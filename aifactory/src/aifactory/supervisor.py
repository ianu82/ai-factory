"""Cheap, deterministic control-plane checks. No provider or GitHub calls.

Detection is read-only; surfacing uses the Inbox's durable idempotency boundary.
Fix-mode actions belong to the next layer, not to these detectors.
"""
from __future__ import annotations

import hashlib
import json
import re
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Literal

from sqlalchemy.orm import Session

from .db import session_scope
from .events import record
from .models import Decision, Event, Message, Outcome, PullReview, Repository, ReviewAttempt, Run, Signal, Task, Workspace
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
        value = json.dumps([self.kind, self.subject, self.evidence], sort_keys=True, default=str)
        return hashlib.sha256(value.encode()).hexdigest()


def detect(db: Session, workspace_id: str, now: datetime, policy: dict) -> list[Finding]:
    """Read only workspace-scoped durable evidence; callers supply the clock."""
    now = timestamp(now)
    tasks = db.query(Task).filter(Task.workspace_id == workspace_id).all()
    signals = db.query(Signal).filter(Signal.workspace_id == workspace_id).all()
    events = db.query(Event).filter(Event.workspace_id == workspace_id).all()
    repos = {r.id: r for r in db.query(Repository).filter(Repository.workspace_id == workspace_id)}
    findings = []
    for task in tasks:
        expired = (task.status == "running" and timestamp(task.lease_expires_at) is not None
                   and timestamp(task.lease_expires_at) < now)
        if not (task.status in {"blocked", "needs_input", "smoke_failed", "failed"} or expired):
            continue
        if timestamp(task.updated_at) >= now - timedelta(minutes=policy["stuck_minutes"]):
            continue
        latest_run = (db.query(Run).filter(Run.workspace_id == workspace_id, Run.task_id == task.id)
                      .order_by(Run.started_at.desc(), Run.id.desc()).first())
        protection = re.search(r"GH013|push protection|secret scanning",
                               (task.blocked_reason or "") + "\n" + (latest_run.error if latest_run else ""), re.I)
        evidence = {"task_id": task.id, "status": task.status, "updated_at": task.updated_at.isoformat()}
        if protection:
            evidence["classification"] = "github_push_protection"
        findings.append(Finding("push_protection" if protection else "stuck_task", task.id,
            f"{task.key}: GitHub push protection" if protection else f"{task.key}: task is stuck",
            "A person must remove the secret or explicitly allow it on GitHub before the push can proceed."
            if protection else f"Task {task.key} remains {task.status} beyond the stuck threshold.", evidence))

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
        recovered = any(e.kind in {"ci_status", "main_green"} and (e.data or {}).get("repo_id") == sig.repo_id
            and (e.data or {}).get("branch") == (repo.default_branch if repo else None)
            and ((e.data or {}).get("state") == "success" or e.kind == "main_green")
            and (not data.get("workflow") or (e.data or {}).get("workflow") == data["workflow"])
            and timestamp(e.created_at) > (timestamp(data.get("failure_at")) or timestamp(sig.last_seen_at))
            for e in events)
        if main_red and recovered:
            main_red = False
        evidence = {"signal_id": sig.id, "repo_id": sig.repo_id, "first_seen_at": sig.first_seen_at.isoformat()}
        if main_red:
            findings.append(Finding("main_red", sig.id, "Default-branch CI is red",
                                    f"Open CI failure in {repo.full_name}: {sig.title}", evidence))
        if main_red or sig.source == "smoke_failure":
            # CI first_seen_at is the poll time, potentially long after the
            # failed run. Merges after that run cannot have caused its failure.
            failure_at = (timestamp(data.get("failure_at")) if main_red else None) or timestamp(sig.first_seen_at)
            passes = [timestamp(e.created_at) for e in events
                      if timestamp(e.created_at) < failure_at
                      and ((e.data or {}).get("repo_id") == sig.repo_id
                           or any(t.id == e.task_id and t.repo_id == sig.repo_id for t in tasks))
                      and (e.kind == "smoke_passed" or (
                           e.kind in {"ci_status", "main_green"}
                           and (e.data or {}).get("branch") == (repo.default_branch if repo else None)
                           and (not data.get("workflow") or (e.data or {}).get("workflow") == data["workflow"])
                           and ((e.data or {}).get("state") == "success" or e.kind == "main_green")))]
            baseline = max(passes) if passes else None
            candidates = []
            for task in tasks:
                merge = (task.evidence or {}).get("merge") or {}
                merged_at = timestamp(merge.get("at"))
                if merged_at is None:
                    merged_at = next((timestamp(e.created_at) for e in events
                                      if e.task_id == task.id and e.kind in {"merged", "merged_outside"}), None)
                green = ((task.evidence or {}).get("ci") or {}).get("state") == "success"
                green |= any(e.task_id == task.id and e.kind == "ci_status"
                             and (e.data or {}).get("state") == "success"
                             and merged_at is not None and timestamp(e.created_at) <= merged_at for e in events)
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
            guidance = [timestamp(m.created_at) for m in db.query(Message).filter(
                Message.workspace_id == workspace_id, Message.outcome_id == task.outcome_id)
                if m.task_id in (None, task.id) and m.author_kind == "user"]
            guidance += [timestamp(e.created_at) for e in events if e.task_id == task.id
                         and e.kind == "stage" and (e.data or {}).get("stage") == "revise"]
            if task.stage == "revise":
                guidance.append(timestamp(task.updated_at))
            if not guidance or max(guidance) < at:
                findings.append(Finding("unrelayed_review", task.id, "Review findings await relay",
                    f"Review of PR #{review.pr_number} has findings newer than the task's guidance or revision.",
                    {"task_id": task.id, "review_id": review.id, "attempt_id": attempt.id,
                     "findings": attempt.verdict["findings"]}))
    return findings


def surface(workspace_id: str, finding: Finding) -> str:
    from .pipeline.runtime import open_decision

    body = finding.body
    if finding.confidence == "inferred":
        body = "Likely / suspected: " + body + "\nEvidence: " + json.dumps(finding.evidence, sort_keys=True)
    metadata = {"finding_kind": finding.kind, "subject": finding.subject, "fingerprint": finding.fingerprint}
    return open_decision(workspace_id, kind="supervisor", title=redact(finding.title)[:300], body=redact(body),
                         task_id=finding.evidence.get("task_id"), metadata=metadata,
                         dedup_key=finding.fingerprint,
                         options=[{"value": "acknowledge", "label": "Acknowledge"},
                                  {"value": "dismiss", "label": "Dismiss"}])


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
            surface(workspace_id, finding)
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
