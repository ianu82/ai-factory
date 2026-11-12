"""Cheap, deterministic control-plane checks. No provider or GitHub calls.

Detection is read-only. Fixes and Inbox surfacing share a durable, workspace-locked
idempotency boundary; no Supervisor action calls GitHub or grants a monetary budget.
"""
from __future__ import annotations

import hashlib
import json
import re
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Literal

from sqlalchemy import and_, event, or_
from sqlalchemy.orm import Session

from .db import session_scope
from .events import record
from .models import Decision, Event, Message, Outcome, PullReview, Repository, ReviewAttempt, Run, Signal, Task, Workspace
from .onboarding_policy import lock_workspace
from .security import redact
from .supervisor_policy import supervisor_policy


def timestamp(value) -> datetime | None:
    """Compare JSON and DB dates in UTC, independent of the DB's timezone support."""
    try:
        at = datetime.fromisoformat(value) if isinstance(value, str) else value
        if not isinstance(at, datetime):
            return None
        return at.replace(tzinfo=timezone.utc) if at.tzinfo is None else at.astimezone(timezone.utc)
    except ValueError:
        return None


@dataclass(frozen=True)
class Finding:
    kind: str
    subject: str
    title: str
    body: str
    evidence: dict
    confidence: Literal["observed", "inferred"] = "observed"

    def __post_init__(self):
        if self.confidence not in ("observed", "inferred"):
            raise ValueError("confidence must be observed or inferred")

    @property
    def fingerprint(self) -> str:
        # Evidence contains incident identity, not sweep time or elapsed age.
        evidence = dict(self.evidence)
        if self.kind in {"stuck_task", "push_protection"}:
            # Heartbeats/notes must not turn the same blocked incident into a new one.
            evidence.pop("updated_at", None)
        value = json.dumps([self.kind, self.subject, evidence], sort_keys=True, default=str)
        return hashlib.sha256(value.encode()).hexdigest()


def detect(db: Session, workspace_id: str, now: datetime, policy: dict) -> list[Finding]:
    """Read only workspace-scoped durable evidence; callers supply the clock."""
    now = timestamp(now)
    tasks = db.query(Task).filter(Task.workspace_id == workspace_id).all()
    signals = db.query(Signal).filter(Signal.workspace_id == workspace_id).all()
    # History is queried per incident below. A workspace-wide cap or time window
    # can hide durable recovery/revision evidence in busy or long-lived incidents.
    history = db.query(Event).filter(Event.workspace_id == workspace_id)
    repos = {r.id: r for r in db.query(Repository).filter(Repository.workspace_id == workspace_id)}
    findings = []
    for task in tasks:
        expired = (task.status == "running" and timestamp(task.lease_expires_at) is not None
                   and timestamp(task.lease_expires_at) < now)
        ev = task.evidence or {}
        recovered_queue = (task.status == "queued"
                           and ev.get("supervisor_requeues", {}).get("epoch") == ev.get("budget_epoch", 0))
        if not (task.status in {"blocked", "needs_input", "smoke_failed", "failed"} or expired or recovered_queue):
            continue
        if not recovered_queue and timestamp(task.updated_at) >= now - timedelta(minutes=policy["stuck_minutes"]):
            continue
        latest_run = (db.query(Run).filter(Run.workspace_id == workspace_id, Run.task_id == task.id)
                      .order_by(Run.started_at.desc(), Run.id.desc()).first())
        protection = _push_protection(task, latest_run)
        evidence = {"task_id": task.id, "status": task.status, "budget_epoch": ev.get("budget_epoch", 0),
                    "stage": task.stage, "head": task.head_sha, "reason": task.blocked_reason or "",
                    "updated_at": task.updated_at.isoformat()}
        if protection:
            evidence["classification"] = "github_push_protection"
            if latest_run and not _push_protection(task):
                evidence["run_id"] = latest_run.id
                evidence["error_digest"] = hashlib.sha256((latest_run.error or "").encode()).hexdigest()
        findings.append(Finding("push_protection" if protection else "stuck_task", task.id,
            f"{task.key}: GitHub push protection" if protection else f"{task.key}: task is stuck",
            "A person must remove the secret or explicitly allow it on GitHub before the push can proceed."
            if protection else (f"Task {task.key} remains queued after Supervisor lease recovery."
                               if recovered_queue else f"Task {task.key} remains {task.status} beyond the stuck threshold."), evidence))

    open_tasks = {d.task_id for d in db.query(Decision).filter(
        Decision.workspace_id == workspace_id, Decision.status == "open", Decision.kind != "supervisor")}
    outcomes = {o.id: o for o in db.query(Outcome).filter(Outcome.workspace_id == workspace_id)}
    for sig in signals:
        if sig.status not in {"new", "proposed", "accepted"}:
            continue
        data = sig.data or {}
        repo = repos.get(sig.repo_id)
        branch = data.get("branch", data.get("head_branch"))
        main_red = (sig.source == "ci_failure" and repo is not None
                    and (branch is None or branch == repo.default_branch))
        # A later recorded green observation retires a stale failure even when a
        # legacy signal was left open. Never use PR CI as default-branch evidence.
        recovered = _main_recovery(sig, repo, history, signals)
        if main_red and recovered:
            main_red = False
        evidence = {"signal_id": sig.id, "repo_id": sig.repo_id, "first_seen_at": sig.first_seen_at.isoformat()}
        if sig.source == "ci_failure":
            evidence.update({"failure_at": data.get("failure_at"), "workflow": data.get("workflow"),
                             "branch": branch})
        if policy["mode"] == "fix" and sig.source == "ci_failure" and (repo is None or recovered):
            findings.append(Finding("main_red", sig.id, "Stale default-branch CI failure",
                                    "The repository was removed or a newer green run resolved this failure.",
                                    {**evidence, "recovery": recovered or {"repository_removed": True}}))
        if main_red:
            findings.append(Finding("main_red", sig.id, "Default-branch CI is red",
                                    f"Open CI failure in {repo.full_name}: {sig.title}", evidence))
        if main_red or sig.source == "smoke_failure":
            # CI first_seen_at is the poll time, potentially long after the
            # failed run. Merges after that run cannot have caused its failure.
            failure_at = (timestamp(data.get("failure_at")) if main_red else None) or timestamp(sig.first_seen_at)
            repo_tasks = db.query(Task.id).filter(Task.workspace_id == workspace_id, Task.repo_id == sig.repo_id)
            baseline_event = history.filter(
                Event.created_at < failure_at.replace(tzinfo=None),
                or_(Event.data["repo_id"].as_string() == sig.repo_id, Event.task_id.in_(repo_tasks)),
                or_(Event.kind == "smoke_passed", and_(
                    Event.kind.in_({"ci_status", "main_green"}),
                    Event.data["branch"].as_string() == (repo.default_branch if repo else None),
                    Event.data["workflow"].as_string() == data["workflow"] if data.get("workflow") else True,
                    or_(Event.data["state"].as_string() == "success", Event.kind == "main_green"))))
            baseline_event = baseline_event.order_by(Event.created_at.desc(), Event.id.desc()).first()
            baseline = timestamp(baseline_event.created_at) if baseline_event else None
            candidates = []
            for task in tasks:
                merge = (task.evidence or {}).get("merge") or {}
                merged_at = timestamp(merge.get("at"))
                if task.repo_id != sig.repo_id:
                    continue
                if merged_at is None:
                    merge_event = history.filter(Event.task_id == task.id,
                        Event.kind.in_({"merged", "merged_outside"})).order_by(
                            Event.created_at.desc(), Event.id.desc()).first()
                    merged_at = timestamp(merge_event.created_at) if merge_event else None
                green = (merge.get("ci") or (task.evidence or {}).get("ci") or {}).get("state") == "success"
                if not green and not merge.get("ci") and merged_at is not None:
                    green = history.filter(Event.task_id == task.id, Event.kind == "ci_status",
                        Event.data["state"].as_string() == "success",
                        Event.created_at <= merged_at.replace(tzinfo=None)).first() is not None
                if (task.repo_id == sig.repo_id and task.merge_sha and task.pr_number and merged_at
                        and merged_at < failure_at and (baseline is None or merged_at > baseline) and green):
                    candidates.append({"task_id": task.id, "pr": task.pr_number, "merge_sha": task.merge_sha})
            if len(candidates) >= 2:
                candidates.sort(key=lambda c: (c["pr"], c["task_id"]))
                findings.append(Finding("combined_break", sig.id, "Likely combined-merge break",
                    "Likely interaction between individually green PRs: "
                    + ", ".join(f"#{c['pr']} (task {c['task_id']})" for c in candidates)
                    + ". These merges precede the failure; this is a suspicion, not proven causation.",
                    {**evidence, "candidates": candidates, "last_green_at": baseline.isoformat() if baseline else None},
                    "inferred"))
        if (sig.source == "github_issue" and sig.status in {"new", "proposed"}
                and timestamp(sig.first_seen_at) < now - timedelta(hours=policy["unaccepted_issue_hours"])):
            reason = data.get("acceptance_reason")
            findings.append(Finding("unaccepted_issue", sig.id, "Factory issue awaits acceptance",
                f"{sig.title} has not been accepted." + (f" Acceptance reason: {reason}" if reason else ""),
                {**evidence, **({"acceptance_reason": reason} if reason else {})}))
        if sig.source == "smoke_failure" and sig.status == "new":
            linked = {t.id for t in tasks if (t.outcome_id == sig.outcome_id and sig.outcome_id)
                      or (t.pr_url and t.pr_url == sig.url) or t.id == data.get("task_id")
                      or (outcomes.get(t.outcome_id) and outcomes[t.outcome_id].signal_id == sig.id)}
            if not linked.intersection(open_tasks):
                findings.append(Finding("untriaged_smoke", sig.id, "Smoke failure needs triage",
                                        f"{sig.title} has no linked task with an open decision.", evidence))

    for review in db.query(PullReview).filter(PullReview.workspace_id == workspace_id):
        attempt = (db.query(ReviewAttempt).filter(ReviewAttempt.review_id == review.id)
                   .order_by(ReviewAttempt.created_at.desc(), ReviewAttempt.id.desc()).first())
        if not attempt or attempt.status not in {"failed", "blocked"} or not (attempt.verdict or {}).get("findings"):
            continue
        at = timestamp(attempt.finished_at or attempt.updated_at)
        for task in tasks:
            if task.repo_id != review.repo_id or task.pr_number != review.pr_number:
                continue
            relay = db.query(Message).filter(Message.workspace_id == workspace_id, Message.task_id == task.id,
                                             Message.author_kind == "factory").all()
            relayed = next((m for m in relay if (m.attribution or {}).get("source") == "supervisor"
                            and (m.attribution or {}).get("review_attempt_id") == attempt.id), None)
            if relayed and (relayed.attribution or {}).get("requeued"):
                continue
            guidance = [timestamp(m.created_at) for m in db.query(Message).filter(
                Message.workspace_id == workspace_id, Message.outcome_id == task.outcome_id)
                if m.task_id in (None, task.id) and m.author_kind == "user"]
            revision = history.filter(Event.task_id == task.id, Event.kind == "stage",
                Event.data["stage"].as_string() == "revise").order_by(
                    Event.created_at.desc(), Event.id.desc()).first()
            if revision:
                guidance.append(timestamp(revision.created_at))
            if task.stage == "revise":
                guidance.append(timestamp(task.updated_at))
            if relayed or not guidance or max(guidance) < at:
                findings.append(Finding("unrelayed_review", task.id, "Review findings await relay",
                    f"Review of PR #{review.pr_number} has findings newer than the task's guidance or revision.",
                    {"task_id": task.id, "review_id": review.id, "attempt_id": attempt.id,
                     "findings": attempt.verdict["findings"]}))
    return findings


def _push_protection(task: Task, run: Run | None = None) -> bool:
    return bool(re.search(r"GH013|push protection|secret scanning",
                          (task.blocked_reason or "") + "\n" + (run.error or "" if run else ""), re.I))


def _main_recovery(sig: Signal, repo: Repository | None, history, signals) -> dict | None:
    """Return stable recovery identity, without a workspace-wide history cutoff."""
    data = sig.data or {}
    if (repo is None or not data.get("workflow")
            or data.get("branch", data.get("head_branch")) not in {None, repo.default_branch}):
        return None  # Without workflow identity, a green run cannot prove recovery.
    failed_at = timestamp(data.get("failure_at")) or timestamp(sig.last_seen_at)
    green = history.filter(Event.kind.in_({"ci_status", "main_green"}),
        Event.data["repo_id"].as_string() == sig.repo_id,
        Event.data["branch"].as_string() == repo.default_branch,
        or_(Event.data["state"].as_string() == "success", Event.kind == "main_green"),
        Event.created_at > failed_at.replace(tzinfo=None))
    green = green.filter(Event.data["workflow"].as_string() == data["workflow"])
    # The first proving observation remains stable when later green polls arrive.
    event = green.order_by(Event.created_at, Event.id).first()
    if event:
        return {"event_id": event.id, "at": timestamp(event.created_at).isoformat()}
    # Some older importers retain one resolved CI signal per run rather than a green event.
    recovered = []
    for other in signals:
        if (other.id == sig.id or other.source != "ci_failure" or other.status != "resolved"
                or other.repo_id != sig.repo_id
                or (other.data or {}).get("branch", repo.default_branch) != repo.default_branch
                or not ((data.get("workflow") and (other.data or {}).get("workflow") == data["workflow"])
                        or (sig.external_id and other.external_id == sig.external_id))):
            continue
        at = (timestamp((other.data or {}).get("resolved_at")) or timestamp((other.data or {}).get("failure_at"))
              or timestamp(other.last_seen_at))
        if at > failed_at:
            recovered.append((at, other.id))
    if recovered:
        at, signal_id = min(recovered)
        return {"signal_id": signal_id, "at": at.isoformat()}
    return None


def _notification(db: Session, workspace_id: str, finding: Finding) -> Decision | None:
    return next((d for d in db.query(Decision).filter(Decision.workspace_id == workspace_id,
        Decision.kind == "supervisor", Decision.status.in_({"open", "resolved", "dismissed"}))
        if (d.metadata_json or {}).get("fingerprint") == finding.fingerprint), None)


def _can_retry(db: Session, task: Task, settings: dict, now: datetime) -> bool:
    """Never grant new money, override a human gate, or resume a protected operation."""
    from sqlalchemy import func, select
    from .models import ScorerResult
    from .pipeline.orchestrator import orchestrator

    job = orchestrator._tasks.get(task.id)
    if job and not job[0].done():
        return False
    if task.stage in {"merge", "revert", "revert_merge", "release"} or task.merge_sha:
        return False
    latest = (db.query(Run).filter(Run.workspace_id == task.workspace_id, Run.task_id == task.id)
              .order_by(Run.started_at.desc(), Run.id.desc()).first())
    if _push_protection(task, latest):
        return False
    outcome = db.get(Outcome, task.outcome_id)
    if not outcome or outcome.status in {"cancelled", "completed", "failed", "abandoned"}:
        return False
    gates = db.query(Decision).filter(Decision.workspace_id == task.workspace_id, Decision.status == "open")
    if any((d.kind in {"budget", "provider_hold"} and d.outcome_id in {None, task.outcome_id})
           or (d.task_id == task.id and d.kind in {"approve_merge", "approve_release", "question"}) for d in gates):
        return False
    spent, unknown = db.query(func.coalesce(func.sum(Run.cost_usd), 0),
        func.count(Run.id).filter(Run.cost_known.is_(False))).filter(Run.outcome_id == task.outcome_id).one()
    scores, score_unknown = db.query(func.coalesce(func.sum(ScorerResult.cost_usd), 0),
        func.count(ScorerResult.id).filter(ScorerResult.cost_known.is_(False))).filter(
            ScorerResult.run_id.in_(select(Run.id).where(Run.outcome_id == task.outcome_id))).one()
    for run in db.query(Run).filter(Run.outcome_id == task.outcome_id, Run.role == "lead", Run.status == "running",
                                    Run.started_at > now.replace(tzinfo=None) - timedelta(minutes=30)):
        reserved = (run.config_snapshot or {}).get("budget_reserved", 0)
        if isinstance(reserved, (int, float)) and not isinstance(reserved, bool) and reserved > 0:
            spent += max(0, reserved - (run.cost_usd or 0))
    limit = max(settings["budgets"].get("outcome_usd") or 0,
                (task.evidence or {}).get("budget_allowance_usd") or 0)
    return not (unknown or score_unknown) and (not limit or spent + scores < limit)


def _after_commit(db: Session, callback) -> None:
    # Waking before the commit can let the scheduler miss the newly queued row.
    event.listen(db, "after_commit", lambda session: callback(), once=True)


def act(db: Session, workspace_id: str, finding: Finding, policy: dict, now: datetime) -> bool:
    """Apply only allow-listed fixes. False means the caller must surface the finding.

    Intent, mutation and audit event commit together. The workspace lock serializes
    competing sweeps (also on SQLite); re-read state/policy before every action.
    """
    if policy["mode"] != "fix":
        return False
    workspace = lock_workspace(db, workspace_id)
    if workspace.paused or supervisor_policy(workspace.settings)["mode"] != "fix":
        return False
    policy = supervisor_policy(workspace.settings)
    now = timestamp(now)
    notification = _notification(db, workspace_id, finding)
    if notification and notification.status in {"resolved", "dismissed"}:
        return True
    if finding.kind not in {"stuck_task", "untriaged_smoke", "main_red", "unrelayed_review"}:
        return False
    # Task transitions use row-level locking rather than the policy lock. Hold
    # both before re-detection so a lease heartbeat cannot race the requeue.
    subject_model = Task if finding.kind in {"stuck_task", "unrelayed_review"} else Signal
    subject = (db.query(subject_model).filter(subject_model.workspace_id == workspace_id,
        subject_model.id == finding.subject).populate_existing().with_for_update().first())
    if subject is None:
        return True
    # Re-detect while holding the policy lock: a renewed lease, new review head or
    # a person's task transition invalidates the original evidence.
    current = next((f for f in detect(db, workspace_id, now, policy)
                    if f.fingerprint == finding.fingerprint), None)
    if current is None:
        return True
    from .pipeline.orchestrator import orchestrator
    from .pipeline.runtime import merged_settings

    settings = merged_settings(workspace.settings)
    task = None
    action = ""
    handled = True
    if finding.kind == "stuck_task":
        task = subject
        ev = task.evidence or {}
        epoch = ev.get("budget_epoch", 0)
        job = orchestrator._tasks.get(task.id)
        if (task.status != "running" or timestamp(task.lease_expires_at) is None
                or timestamp(task.lease_expires_at) >= timestamp(now)
                or (job and not job[0].done())
                or ev.get("supervisor_requeues", {}).get("epoch") == epoch
                or not _can_retry(db, task, settings, now)):
            return False
        # Reclaim abandoned sessions before approving a replacement. Their
        # recorded cost omits unrecorded in-flight charges: closing them makes
        # that cost unknown. Persist the cleanup but surface instead of requeueing
        # until reconciliation establishes the cost.
        orchestrator._close_interrupted_runs([task.id], session=db)
        db.flush()
        if not _can_retry(db, task, settings, now):
            return False
        task.status, task.lease_owner, task.lease_expires_at = "queued", "", None
        task.evidence = {**ev, "supervisor_requeues": {"epoch": epoch, "at": timestamp(now).isoformat()}}
        action = "requeue_expired_lease"
        _after_commit(db, orchestrator.wake)
    elif finding.kind == "untriaged_smoke":
        sig = subject
        prefix = "Smoke failed after merging "
        if not sig.title.startswith(prefix):
            return False
        matches = db.query(Task).filter(Task.workspace_id == workspace_id, Task.repo_id == sig.repo_id,
            Task.title == sig.title[len(prefix):], Task.merge_sha != "").limit(2).all()
        if len(matches) != 1:
            return False  # Title collisions are ambiguous, never guess a delivery.
        task = matches[0]
        sig.data, sig.status = {**(sig.data or {}), "task_id": task.id}, "proposed"
        existing = db.query(Decision).filter(Decision.workspace_id == workspace_id, Decision.task_id == task.id,
                                             Decision.kind == "unblock", Decision.status == "open").first()
        if existing is None:
            decision = Decision(workspace_id=workspace_id, outcome_id=task.outcome_id, task_id=task.id,
                kind="unblock", title=f"Smoke checks failed after merging {task.key}", body=redact(sig.body),
                metadata_json={"source": "supervisor", "signal_id": sig.id},
                options=[{"value": "revert", "label": "Revert the merge"},
                         {"value": "fix", "label": "Create a fix outcome"},
                         {"value": "acknowledge", "label": "Acknowledge"}])
            db.add(decision)
            db.flush()
            record(workspace_id, "decision_requested", decision.title, actor="supervisor",
                   task_id=task.id, outcome_id=task.outcome_id,
                   data={"decision_id": decision.id, "kind": "unblock"}, session=db)
        action = "triage_smoke"
    elif finding.kind == "main_red":
        sig = subject
        repo = db.query(Repository).filter(Repository.workspace_id == workspace_id, Repository.id == sig.repo_id).first()
        if repo is not None:
            events = db.query(Event).filter(Event.workspace_id == workspace_id,
                Event.kind.in_({"ci_status", "main_green"}), Event.created_at > (
                    timestamp((sig.data or {}).get("failure_at")) or timestamp(sig.last_seen_at)).replace(tzinfo=None))
            signals = db.query(Signal).filter(Signal.workspace_id == workspace_id, Signal.repo_id == sig.repo_id,
                                               Signal.source == "ci_failure", Signal.status == "resolved")
            if not _main_recovery(sig, repo, events, signals):
                return False
        sig.status = "resolved"
        action = "resolve_stale_ci"
    else:
        task = subject
        attempt = db.get(ReviewAttempt, finding.evidence["attempt_id"])
        review = db.get(PullReview, attempt.review_id)
        if review.head_sha != attempt.head_sha:
            return False
        if task.head_sha and task.head_sha != attempt.head_sha:
            return False  # Findings on an older head cannot authorize revising this one.
        relayed = next((m for m in db.query(Message).filter(Message.workspace_id == workspace_id,
            Message.task_id == task.id, Message.author_kind == "factory")
            if (m.attribution or {}).get("source") == "supervisor"
            and (m.attribution or {}).get("review_attempt_id") == attempt.id), None)
        if relayed:
            return bool((relayed.attribution or {}).get("requeued"))
        ev = task.evidence or {}
        used = int(ev.get("auto_retries", 0))
        rounds = max(0, min(int(settings.get("auto_retry_rounds", 1) or 0), 3))
        unblocks = db.query(Decision).filter(Decision.workspace_id == workspace_id,
            Decision.task_id == task.id, Decision.kind == "unblock",
            Decision.status == "open").with_for_update().all()
        review_unblocks = []
        for decision in unblocks:
            metadata = decision.metadata_json or {}
            # Task identity alone (or the card's age) cannot prove this gate
            # belongs to the review. Unknown/other incidents require a person.
            if "review_attempt_id" in metadata:
                related = metadata["review_attempt_id"] == attempt.id
            elif "fingerprint" in metadata:
                related = metadata["fingerprint"] == finding.fingerprint
            else:
                related = (metadata.get("review_id") == review.id
                           and metadata.get("head_sha") == attempt.head_sha)
            if related:
                review_unblocks.append(decision)
        handled = (task.status in {"failed", "blocked"} and used < rounds
                   and len(review_unblocks) == len(unblocks)
                   and _can_retry(db, task, settings, now))
        db.add(Message(workspace_id=workspace_id, outcome_id=task.outcome_id, task_id=task.id,
            author_kind="factory", body=redact("Supervisor: review findings for PR "
                f"#{task.pr_number} at {attempt.head_sha}\n\n" + json.dumps(attempt.verdict["findings"], indent=2)),
            attribution={"source": "supervisor", "review_attempt_id": attempt.id,
                         "guidance": True, "requeued": handled}, created_at=timestamp(now).replace(tzinfo=None)))
        if handled:
            # These cards describe the failed attempt, not the replacement. A
            # stale retry/abandon response must not mutate the active retry.
            for decision in review_unblocks:
                decision.status = "superseded"
                decision.resolved_at = timestamp(now).replace(tzinfo=None)
                decision.attribution = {"source": "supervisor", "review_attempt_id": attempt.id}
                record(workspace_id, "decision_superseded", "Supervisor superseded an unblock decision before retry",
                       actor="supervisor", task_id=task.id, outcome_id=task.outcome_id,
                       data={"decision_id": decision.id, "review_attempt_id": attempt.id}, session=db)
            task.status, task.stage, task.blocked_reason = "queued", "revise", ""
            task.lease_owner, task.lease_expires_at = "", None
            task.evidence = {**ev, "auto_retries": used + 1, "attempt_base": task.attempt}
            _after_commit(db, orchestrator.wake)
        action = "relay_review"
    if task and (action == "requeue_expired_lease" or (action == "relay_review" and handled)):
        task.updated_at = timestamp(now).replace(tzinfo=None)
    record(workspace_id, "supervisor_action", f"Supervisor: {action}", actor="supervisor",
           task_id=task.id if task else None, outcome_id=task.outcome_id if task else None,
           data={"action": action, "finding_kind": finding.kind, "fingerprint": finding.fingerprint,
                 "subject": finding.subject, "requeued": handled if action == "relay_review" else action == "requeue_expired_lease"},
           session=db)
    return handled


def surface(workspace_id: str, finding: Finding, *, now: datetime | None = None) -> str | None:
    from .pipeline.runtime import publish_decision

    body = finding.body
    if finding.confidence == "inferred":
        body = "Likely / suspected: " + body + "\nEvidence: " + json.dumps(finding.evidence, sort_keys=True)
    metadata = {"finding_kind": finding.kind, "subject": finding.subject, "fingerprint": finding.fingerprint}
    # Policy check and decision creation share the same lock. Calling open_decision
    # after releasing this lock would reintroduce the settings-change race.
    with session_scope() as db:
        workspace = lock_workspace(db, workspace_id)
        policy = supervisor_policy(workspace.settings)
        if workspace.paused or policy["mode"] == "off":
            return None
        if now is not None and not any(f.fingerprint == finding.fingerprint
                for f in detect(db, workspace_id, now, policy)):
            return None  # The live thresholds/evidence no longer enable this finding.
        existing = _notification(db, workspace_id, finding)
        if existing:
            return existing.id
        decision = Decision(workspace_id=workspace_id, kind="supervisor", title=redact(finding.title)[:300],
            body=redact(body), task_id=finding.evidence.get("task_id"), metadata_json=metadata,
            options=[{"value": "acknowledge", "label": "Acknowledge"},
                     {"value": "dismiss", "label": "Dismiss"}])
        db.add(decision)
        db.flush()
        decision_id, title, task_id = decision.id, decision.title, decision.task_id
    publish_decision(workspace_id, decision_id, "supervisor", title, task_id=task_id)
    return decision_id


def sweep(workspace_id: str, now: datetime) -> list[Finding]:
    with session_scope() as db:
        workspace = db.get(Workspace, workspace_id)
        if workspace is None or workspace.paused:
            return []
        policy = supervisor_policy(workspace.settings)
        state = (workspace.settings.get("supervisor_state") or {}) if isinstance(workspace.settings, dict) else {}
        if not isinstance(state, dict):
            state = {}
        if not isinstance(workspace.settings, dict):
            previous = (db.query(Event).filter(Event.workspace_id == workspace_id, Event.kind == "supervisor_sweep")
                        .order_by(Event.created_at.desc(), Event.id.desc()).first())
            state = (previous.data or {}).get("state", {}) if previous else {}
        findings = detect(db, workspace_id, now, policy) if policy["mode"] != "off" else []
        previous_fingerprints = {f.get("fingerprint") for f in state.get("findings", [])}
    surfaced = 0
    if policy["mode"] != "off":
        for finding in findings:
            # A transient push block should not interrupt a person. Confirm the
            # identical incident in the immediately preceding sweep first.
            if finding.kind == "push_protection" and finding.fingerprint not in previous_fingerprints:
                continue
            with session_scope() as db:
                handled = act(db, workspace_id, finding, policy, now) if policy["mode"] == "fix" else False
            if not handled and surface(workspace_id, finding, now=now) is not None:
                surfaced += 1
    summary = [{"finding_kind": f.kind, "subject": f.subject, "fingerprint": f.fingerprint,
                "confidence": f.confidence} for f in findings]
    state = {"last_sweep_at": timestamp(now).isoformat(), "findings": summary}
    counts = dict(Counter(f.kind for f in findings))
    with session_scope() as db:
        workspace = db.query(Workspace).filter(Workspace.id == workspace_id).with_for_update().one()
        if isinstance(workspace.settings, dict):
            workspace.settings = {**workspace.settings, "supervisor_state": state}
        record(workspace_id, "supervisor_sweep", f"Supervisor found {len(findings)} condition(s)", actor="supervisor",
               data={"counts": counts, "total": len(findings), "surfaced": surfaced, "state": state}, session=db)
    return findings
