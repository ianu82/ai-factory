"""Resolving human decisions. Every resolution is attributed and recorded; reusable ones
become precedent that later planning and building follow."""

from __future__ import annotations

from typing import Any

from sqlalchemy.orm import Session

from .db import utcnow
from .events import notify, record
from .models import Decision, Message, Outcome, Plan, Task, User
from .pipeline.orchestrator import orchestrator
from .pipeline.planning import approve_plan


class DecisionError(ValueError):
    pass


def resolve(db: Session, decision: Decision, user: User, resolution: str, comment: str = "",
            reusable: bool = False, scope: str = "item") -> dict[str, Any]:
    if decision.status != "open":
        raise DecisionError("This decision is already resolved")
    valid = {o.get("value") for o in decision.options or []} | {"dismiss"}
    if resolution not in valid:
        raise DecisionError(f"Choose one of: {', '.join(sorted(v for v in valid if v))}")
    decision.status = "resolved" if resolution != "dismiss" else "dismissed"
    decision.resolution = resolution
    decision.comment = comment.strip()[:8000]
    decision.resolved_by = user.id
    decision.resolved_at = utcnow()
    decision.reusable = bool(reusable and comment.strip())
    decision.scope = scope if scope in ("item", "repository", "workspace") else "item"
    outcome = db.get(Outcome, decision.outcome_id) if decision.outcome_id else None
    task = db.get(Task, decision.task_id) if decision.task_id else None
    take_over: str | None = None

    def guidance(text: str) -> None:
        if text.strip() and outcome:
            db.add(Message(workspace_id=decision.workspace_id, outcome_id=outcome.id, task_id=task.id if task else None,
                           author_id=user.id, author_kind="user", body=text.strip()))

    kind = decision.kind
    if kind == "approve_plan" and outcome:
        plan = (db.query(Plan).filter(Plan.outcome_id == outcome.id, Plan.status == "proposed")
                .order_by(Plan.version.desc()).first())
        if resolution == "approve" and plan:
            db.commit()
            approve_plan(plan.id, approved_by=user.id)
        elif resolution == "replan":
            guidance(comment)
            if plan:
                plan.status = "rejected"
            outcome.status = "planning"
        elif resolution == "cancel":
            outcome.status = "cancelled"
            orchestrator.cancel_outcome(outcome.id)
    elif kind == "question" and outcome and task is None:
        if resolution == "answer":
            guidance(comment)
            outcome.status = "planning"
        elif resolution == "approve":
            guidance(comment)
            plan = (db.query(Plan).filter(Plan.outcome_id == outcome.id, Plan.status == "proposed")
                    .order_by(Plan.version.desc()).first())
            if plan:
                db.commit()
                approve_plan(plan.id, approved_by=user.id)
        elif resolution == "cancel":
            outcome.status = "cancelled"
            orchestrator.cancel_outcome(outcome.id)
    elif kind == "question" and task:
        if resolution == "answer":
            guidance(comment)
            task.status, task.blocked_reason = "queued", ""
        elif resolution == "abandon":
            task.status = "abandoned"
    elif kind == "approve_merge" and task:
        if resolution == "approve":
            task.status, task.stage = "queued", "merge"
        elif resolution == "reject":
            guidance(comment or "A reviewer requested changes before merging.")
            task.status, task.stage, task.attempt = "queued", "revise", 0
    elif kind == "budget" and task:
        if resolution == "retry":
            ev = dict(task.evidence or {})
            ev["budget_override"] = True
            task.evidence = ev
            task.status = "queued"
        else:
            task.status = "abandoned"
    elif kind == "unblock":
        if task:
            if resolution == "retry":
                if task.status == "failed":
                    task.attempt = 0
                    task.stage = "" if not task.pr_number else "revise"
                guidance(comment)
                task.status, task.blocked_reason = "queued", ""
            elif resolution == "abandon":
                task.status = "abandoned"
            elif resolution == "take_over":
                take_over = task.id  # finished after this decision is recorded
            elif resolution == "revert":
                task.status, task.stage, task.blocked_reason = "queued", "revert", ""
            elif resolution in ("acknowledge", "fix"):
                task.blocked_reason = ""
                if resolution == "fix":
                    from .signals import outcome_from_text

                    outcome_from_text(db, decision.workspace_id, user, f"Fix: {decision.title}",
                                      f"{decision.body}\n\n{comment}".strip(), [task.repo_id], source="improvement")
        elif outcome:
            if resolution == "retry":
                guidance(comment)
                outcome.status, outcome.blocked_reason = "planning", ""
            elif resolution == "cancel":
                outcome.status = "cancelled"
                orchestrator.cancel_outcome(outcome.id)
    db.commit()
    record(decision.workspace_id, "decision_resolved", f"{user.name or user.email}: {resolution} — {decision.title}",
           outcome_id=decision.outcome_id, task_id=decision.task_id, actor=user.id, level="success",
           data={"decision_id": decision.id, "resolution": resolution, "comment": comment[:2000],
                 "reusable": decision.reusable, "scope": decision.scope})
    notify(decision.workspace_id, "decision", decision.id)
    if outcome:
        notify(decision.workspace_id, "outcome", outcome.id)
    result: dict[str, Any] = {"ok": True}
    if take_over:
        from .pipeline.handover import HandoverError, mark_handed_over

        try:
            result["handover"] = mark_handed_over(take_over, user)
            orchestrator.cancel_task(take_over)
        except HandoverError as exc:
            result["error"] = str(exc)
    orchestrator.wake()
    return result
