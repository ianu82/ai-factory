"""JSON views of the model. Secrets are never serialized."""

from __future__ import annotations

import base64

from datetime import datetime
from typing import Any

from sqlalchemy.orm import Session

from .models import Artifact, Decision, DecisionReview, MemoryEntry, Message, Outcome, Plan, PullReview, ReviewAttempt, Repository, Run, Signal, Task, User, Workspace, Workstream


def ts(value: datetime | None) -> str | None:
    return value.isoformat() + "Z" if value else None


def user(u: User) -> dict[str, Any]:
    return {"id": u.id, "email": u.email, "name": u.name}


def workspace(w: Workspace, role: str | None = None, org_name: str = "") -> dict[str, Any]:
    from .pipeline.runtime import merged_settings

    return {"id": w.id, "name": w.name, "slug": w.slug, "description": w.description, "org_id": w.org_id,
            "org_name": org_name, "role": role, "paused": w.paused, "settings": merged_settings(w.settings),
            "mindshub_workspace_id": w.mindshub_workspace_id, "created_at": ts(w.created_at)}


def workstream(w: Workstream) -> dict[str, Any]:
    return {"id": w.id, "workspace_id": w.workspace_id, "slug": w.slug, "name": w.name,
            "goal": w.goal, "owner_id": w.owner_id, "default_repo_ids": w.default_repo_ids,
            "state": w.state, "worker_override": w.worker_override,
            "plan_approval_override": w.plan_approval_override, "merge_approval_override": w.merge_approval_override,
            "priority_override": w.priority_override, "outcome_budget_override": w.outcome_budget_override,
            "created_at": ts(w.created_at), "updated_at": ts(w.updated_at)}


def repository(r: Repository) -> dict[str, Any]:
    from .steward_policy import steward_policy
    from .release_config import release_public

    config = r.config if isinstance(r.config, dict) else {}
    if "release" in config:
        config = {**config, "release": release_public(config["release"])}
    policy_invalid = not isinstance(r.config, (dict, type(None)))
    try:
        policy = steward_policy(config)
    except ValueError:
        policy = steward_policy()
        policy_invalid = True
    return {"id": r.id, "full_name": r.full_name, "default_branch": r.default_branch, "description": r.description,
            "config": {**config, "steward": policy,
                       "review": {"mode": "off", "allow_untrusted_local": False,
                                  **(config.get("review") if isinstance(config.get("review"), dict) else {})}},
            "steward_policy_invalid": policy_invalid, "created_at": ts(r.created_at)}


def pull_review(r: PullReview) -> dict[str, Any]:
    return {"id": r.id, "workspace_id": r.workspace_id, "repo_id": r.repo_id, "pr_number": r.pr_number,
            "outcome_id": r.outcome_id, "head_sha": r.head_sha, "comment_id": r.comment_id,
            "created_at": ts(r.created_at), "updated_at": ts(r.updated_at)}


def review_attempt(a: ReviewAttempt) -> dict[str, Any]:
    return {"id": a.id, "review_id": a.review_id, "head_sha": a.head_sha, "base_sha": a.base_sha,
            "status": a.status, "verdict": a.verdict or {}, "evidence": a.evidence or {},
            "created_at": ts(a.created_at), "updated_at": ts(a.updated_at),
            "started_at": ts(a.started_at), "finished_at": ts(a.finished_at)}


def run(r: Run) -> dict[str, Any]:
    return {"id": r.id, "task_id": r.task_id, "outcome_id": r.outcome_id, "role": r.role, "harness": r.harness,
            "config_version": r.config_version, "provider": r.provider, "model": r.model, "attempt": r.attempt, "status": r.status, "turns": r.turns,
            "input_tokens": r.input_tokens, "output_tokens": r.output_tokens, "cost_usd": r.cost_usd,
            "cost_known": r.cost_known, "error": r.error, "result": r.result, "started_at": ts(r.started_at),
            "finished_at": ts(r.finished_at), "judged_at": ts(r.finished_at)}


def task(t: Task, runs: list[Run] | None = None, repo_name: str = "") -> dict[str, Any]:
    runs = runs or []
    return {"id": t.id, "outcome_id": t.outcome_id, "key": t.key, "title": t.title, "spec": t.spec,
            "acceptance": t.acceptance, "repo_id": t.repo_id, "repo": repo_name, "depends_on": t.depends_on,
            "harness": t.harness, "verifier_harness": t.verifier_harness, "status": t.status, "stage": t.stage,
            "attempt": t.attempt, "branch": t.branch, "base_sha": t.base_sha, "head_sha": t.head_sha,
            "pr_number": t.pr_number, "pr_url": t.pr_url, "merge_sha": t.merge_sha, "summary": t.summary,
            "blocked_reason": t.blocked_reason, "evidence": t.evidence or {},
            "cost_usd": round(sum(r.cost_usd for r in runs), 4), "cost_known": all(r.cost_known for r in runs),
            "tokens": sum(r.input_tokens + r.output_tokens for r in runs),
            "created_at": ts(t.created_at), "started_at": ts(t.started_at), "finished_at": ts(t.finished_at),
            "updated_at": ts(t.updated_at)}


def plan(p: Plan) -> dict[str, Any]:
    return {"id": p.id, "version": p.version, "status": p.status, "summary": p.summary, "content": p.content,
            "approved_by": p.approved_by, "approved_at": ts(p.approved_at), "created_at": ts(p.created_at)}


def outcome_sources(db: Session, outcomes: list[Outcome]) -> dict[str, Signal]:
    signal_ids = {outcome.signal_id for outcome in outcomes if outcome.signal_id}
    if not signal_ids:
        return {}
    return {signal.id: signal for signal in db.query(Signal).filter(
        Signal.id.in_(signal_ids), Signal.source == "linear_issue")}


def outcome(o: Outcome, tasks: list[Task] | None = None, runs: list[Run] | None = None,
            open_decisions: int = 0, source_signal: Signal | None = None) -> dict[str, Any]:
    tasks = tasks if tasks is not None else []
    runs = runs or []
    done = sum(1 for t in tasks if t.status == "completed")
    # "started": tasks that have left the queue (ran, are running, or wait on a person); with none started, the
    # outcome is planned but every task is still waiting for a free slot or for its dependencies.
    started = sum(1 for t in tasks if t.status not in ("pending", "queued") or t.attempt or t.pr_number)
    active = sum(1 for t in tasks if t.status == "running")
    return {"id": o.id, "number": o.number, "title": o.title, "brief": o.brief, "acceptance": o.acceptance,
            "workstream_id": o.workstream_id, "repo_ids": o.repo_ids, "harness": o.harness, "status": o.status, "priority": o.priority,
            "source": o.source, "summary": o.summary, "blocked_reason": o.blocked_reason,
            "linear_issue": {"id": source_signal.external_id,
                             "identifier": (source_signal.data or {}).get("identifier"),
                             "url": source_signal.url} if source_signal is not None
                            and source_signal.source == "linear_issue"
                            and source_signal.workspace_id == o.workspace_id
                            and source_signal.id == o.signal_id else None,
            "created_by": o.created_by, "created_at": ts(o.created_at), "updated_at": ts(o.updated_at),
            "completed_at": ts(o.completed_at), "progress": {"done": done, "total": len(tasks),
                                                         "started": started, "active": active},
            "cost_usd": round(sum(r.cost_usd for r in runs), 4), "cost_known": all(r.cost_known for r in runs),
            "open_decisions": open_decisions,
            "prs": [{"number": t.pr_number, "url": t.pr_url, "task": t.key} for t in tasks if t.pr_url],
            "track": [{"key": t.key, "status": t.status, "stage": t.stage,
                       "evidence": {k: {"state": (t.evidence or {}).get(k, {}).get("state")}
                                    for k in ("ci", "smoke") if (t.evidence or {}).get(k)}}
                      for t in sorted(tasks, key=lambda t: t.key)]}


def judge(r: DecisionReview) -> dict[str, Any]:
    return {"id": r.id, "state": r.state, "question_revision": r.question_revision,
            "question_fingerprint": r.question_fingerprint, "run_id": r.run_id, "mode": r.mode,
            "proposed_verdict": r.proposed_verdict, "effective_verdict": r.effective_verdict,
            "answer": r.answer, "resolution": r.resolution, "summary": r.summary,
            "citations": r.citations, "closest_decisions": r.closest_decisions, "report": r.report,
            "correction_decision_id": r.correction_decision_id, "created_at": ts(r.created_at),
            "updated_at": ts(r.updated_at), "started_at": ts(r.started_at), "finished_at": ts(r.finished_at), "judged_at": ts(r.finished_at)}


def actionable(d: Decision) -> bool:
    """Pending model work is not human attention; legacy unreviewed work is."""
    r = d.review
    return d.status == "open" and (not r or (r.workspace_id == d.workspace_id
        and r.state not in ("pending", "running") and r.finished_at is not None))


def decision(d: Decision, context: dict[str, Any] | None = None) -> dict[str, Any]:
    attribution = d.attribution or {}
    context = dict(context or {})
    from sqlalchemy.orm import object_session
    db = object_session(d)
    correction = None
    if d.review and d.review.workspace_id == d.workspace_id and d.review.correction_decision_id and db:
        c = db.get(Decision, d.review.correction_decision_id)
        if c and c.workspace_id == d.workspace_id:
            person = db.get(User, c.resolved_by) if c.resolved_by else None
            correction = {"id": c.id, "resolution": c.resolution, "answer": c.comment,
                "resolved_by": c.resolved_by, "resolved_by_name": (person.name or person.email) if person else "",
                "resolved_at": ts(c.resolved_at), "attribution": c.attribution or {}}
    if d.resolved_by and db:
        person = db.get(User, d.resolved_by)
        context["resolved_by_name"] = (person.name or person.email) if person else ""
    elif d.review and d.review.workspace_id == d.workspace_id and d.review.effective_verdict in ("decide", "send_back"):
        context["resolved_by_name"] = "Decision judge"
    if attribution.get("source") == "mind":
        person = db.get(User, d.resolved_by) if db and d.resolved_by else None
        context["resolved_by_name"] = ((person.name or person.email) if person else "Person") + " via Mind"
    if d.kind == "provider_hold":
        from .pipeline.holds import hold_details

        context["hold"] = hold_details(d)  # why the provider refused, and what lifts the hold
    return {"attribution": attribution, "actionable": actionable(d), "correction": correction,
            "supersedes": attribution.get("supersedes"), "id": d.id, "kind": d.kind, "title": d.title, "body": d.body, "options": d.options, "status": d.status,
            "resolution": d.resolution, "comment": d.comment, "resolved_by": d.resolved_by,
            "resolved_at": ts(d.resolved_at), "reusable": d.reusable, "scope": d.scope,
            "judge": judge(d.review) if d.review and d.review.workspace_id == d.workspace_id else None,
            "outcome_id": d.outcome_id, "task_id": d.task_id, "created_at": ts(d.created_at), **(context or {})}


def message(m: Message, author: str = "") -> dict[str, Any]:
    if (m.attribution or {}).get("source") == "mind":
        author += " via Mind"
    return {"author_id": m.author_id, "attribution": m.attribution or {}, "id": m.id, "outcome_id": m.outcome_id, "task_id": m.task_id, "author_kind": m.author_kind,
            "author": author, "body": m.body, "delivered": m.delivered, "created_at": ts(m.created_at)}


def artifact(a: Artifact, content: bool = False) -> dict[str, Any]:
    row = {"id": a.id, "kind": a.kind, "name": a.name, "task_id": a.task_id, "run_id": a.run_id, "data": a.data,
           "size": len(base64.b64decode(a.content or "")) if a.kind == "attachment" else len(a.content or ""), "created_at": ts(a.created_at)}
    if content:
        row["content"] = a.content
    return row


def signal(s: Signal) -> dict[str, Any]:
    return {"id": s.id, "source": s.source, "title": s.title, "body": s.body, "url": s.url, "count": s.count,
            "status": s.status, "outcome_id": s.outcome_id, "repo_id": s.repo_id, "data": s.data or {},
            "first_seen_at": ts(s.first_seen_at), "last_seen_at": ts(s.last_seen_at)}


def memory(entry: MemoryEntry) -> dict[str, Any]:
    return {"id": entry.id, "workspace_id": entry.workspace_id, "repo_id": entry.repo_id,
            "scope": "workspace" if entry.repo_id is None else "repository", "run_id": entry.run_id,
            "kind": entry.kind, "text": entry.text, "topic": entry.topic, "confidence": entry.confidence,
            "source": entry.source, "created_at": ts(entry.created_at), "updated_at": ts(entry.updated_at)}
