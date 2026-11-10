"""Resolving human decisions. Every resolution is attributed and recorded; reusable ones
become precedent that later planning and building follow."""

from __future__ import annotations

from typing import Any

from sqlalchemy.orm import Session

from .db import utcnow
from .events import record
from .models import Decision, Message, Outcome, Plan, Task, User
from .pipeline.orchestrator import ACTIVE_TASK, HUMAN_TASK, orchestrator
from .pipeline.lifecycle import ACCEPTING, accepted_after_merge, complete_accepted
from .pipeline.planning import approve_plan


class DecisionError(ValueError):
    pass



def fresh_budget(evidence: dict[str, Any] | None) -> dict[str, Any]:
    """Evidence for a task whose attempt counter restarts at 0: attempt numbers are reused, so the epoch moves on
    and earlier records (kept for their review findings) no longer count as this budget's attempts."""
    ev = {k: v for k, v in (evidence or {}).items() if k not in ("attempt_base", "auto_retries")}
    revalidation = ev.get("revalidation") or {}
    if revalidation.get("phase") == "rejected":
        ev["revalidation"] = {**revalidation, "phase": "handled", "human_retry": True}
        ev["rebase_rebuilds"] = 0
    ev["budget_epoch"] = int(ev.get("budget_epoch", 0)) + 1
    return ev

def resolve(db: Session, decision: Decision, user: User, resolution: str, comment: str = "",
            reusable: bool = False, scope: str = "item", *, action_id: str | None = None,
            pending_events: list[dict] | None = None, after: list[tuple] | None = None) -> dict[str, Any]:
    """Apply a person's resolution. The app's endpoint commits and publishes here. A confirmed Mind card
    (action_id) keeps everything in its own transaction instead: plan approval and hand-over join it, events wait
    in pending_events, and what must follow the commit (stopping planners and jobs, publishing a hand-over's
    candidate, closing a dropped pull request) goes to ``after`` for work_services.finish()."""
    if action_id and (pending_events is None or after is None):
        raise DecisionError("A Mind resolution leaves its events and follow-up work to its confirmation")
    if decision.status != "open":
        raise DecisionError("This decision is already resolved")
    valid = {o.get("value") for o in decision.options or []} | {"dismiss"}
    if resolution not in valid:
        raise DecisionError(f"Choose one of: {', '.join(sorted(v for v in valid if v))}")
    decision.status = "resolved" if resolution != "dismiss" else "dismissed"
    decision.resolution = resolution
    decision.comment = comment.strip()[:8000]
    decision.attribution = {"source": "mind", "action_id": action_id} if action_id else None
    decision.resolved_by = user.id
    decision.resolved_at = utcnow()
    decision.reusable = bool(reusable and comment.strip())
    decision.scope = scope if scope in ("item", "repository", "workspace") else "item"
    outcome = db.get(Outcome, decision.outcome_id) if decision.outcome_id else None
    task = db.get(Task, decision.task_id) if decision.task_id else None
    take_over: str | None = None
    dropped: str | None = None
    completed: list[dict] = []  # events of tasks this decision completed
    author = (user.name or user.email) + (" via Mind" if action_id else "")

    def approve(plan: Plan) -> None:
        if after is None:
            db.commit()
            approve_plan(plan.id, approved_by=user.id)
        else:
            approve_plan(plan.id, approved_by=user.id, session=db, pending_events=pending_events, by=author,
                         attribution=decision.attribution)

    def stop(outcome_id: str) -> None:
        if after is None:
            orchestrator.cancel_outcome(outcome_id)
        else:
            after.append(("stop_outcome", outcome_id))

    def guidance(text: str) -> None:
        if text.strip() and outcome:
            db.add(Message(workspace_id=decision.workspace_id, outcome_id=outcome.id, task_id=task.id if task else None,
                           author_id=user.id, author_kind="user", body=text.strip(),
                           attribution=decision.attribution))

    kind = decision.kind
    if kind == "approve_plan" and outcome:
        plan = (db.query(Plan).filter(Plan.outcome_id == outcome.id, Plan.status == "proposed")
                .order_by(Plan.version.desc()).first())
        if resolution == "approve" and plan:
            approve(plan)
        elif resolution == "replan":
            guidance(comment)
            if plan:
                plan.status = "rejected"
            outcome.status = "planning"
        elif resolution == "cancel":
            outcome.status = "cancelled"
            stop(outcome.id)
    elif kind == "question" and outcome and task is None:
        if resolution == "answer":
            guidance(comment)
            outcome.status = "planning"
        elif resolution == "approve":
            guidance(comment)
            plan = (db.query(Plan).filter(Plan.outcome_id == outcome.id, Plan.status == "proposed")
                    .order_by(Plan.version.desc()).first())
            if plan:
                approve(plan)
        elif resolution == "cancel":
            outcome.status = "cancelled"
            stop(outcome.id)
    elif kind == "question" and task:
        if resolution == "answer":
            guidance(comment)
            task.status, task.blocked_reason = "queued", ""
        elif resolution == "abandon":
            task.status = "abandoned"
            dropped = task.id
    elif kind == "approve_merge" and task:
        if resolution == "approve":
            task.status, task.stage = "queued", "merge"
        elif resolution == "reject":
            guidance(comment or "A reviewer requested changes before merging.")
            task.status, task.stage, task.attempt = "queued", "revise", 0
            # A requested revision is a fresh budget: earlier rounds' offsets and automatic retries do not carry over.
            task.evidence = fresh_budget(task.evidence)
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
                wait = (task.evidence or {}).get("ci_wait")
                if wait and wait.get("timed_out"):
                    from datetime import timedelta
                    from .pipeline.runtime import workspace_settings

                    now = utcnow()
                    wait = {**wait, "timed_out": False, "first_wait_at": now.isoformat(),
                            "deadline": (now + timedelta(minutes=workspace_settings(task.workspace_id)[
                                "limits"].get("ci_minutes", 45))).isoformat(), "next_poll_at": now.isoformat()}
                    task.evidence = {**task.evidence, "ci_wait": wait}
                if task.status == "failed":
                    task.attempt = 0
                    task.evidence = fresh_budget(task.evidence)
                    task.stage = "" if not task.pr_number else "revise"
                guidance(comment)
                task.status, task.blocked_reason = "queued", ""
            elif resolution == "abandon":
                task.status = "abandoned"
                dropped = task.id
            elif resolution == "take_over" and after is None:
                take_over = task.id  # finished after this decision is recorded
            elif resolution == "take_over":
                from .pipeline.handover import HandoverError, mark_handed_over

                try:
                    info = mark_handed_over(task.id, user, action_id=action_id, session=db,
                                            pending_events=pending_events)
                except HandoverError as exc:
                    raise DecisionError(str(exc)) from None
                after.extend([("stop_task", task.id, task.workspace_id, task.outcome_id),
                              ("publish_candidate", task.id, info)])
            elif resolution == "revert":
                task.status, task.stage, task.blocked_reason = "queued", "revert", ""
            elif resolution in ACCEPTING:
                task.blocked_reason = ""
                if accepted_after_merge(task):
                    completed.append(complete_accepted(db, task, user.name or user.email))
                if resolution == "fix":
                    from .signals import outcome_from_text

                    outcome_from_text(db, decision.workspace_id, user, f"Fix: {decision.title}",
                                      f"{decision.body}\n\n{comment}".strip(), [task.repo_id], source="improvement",
                                      pending_events=pending_events)
        elif outcome:
            if resolution == "retry":
                guidance(comment)
                outcome.status, outcome.blocked_reason = "planning", ""
            elif resolution == "cancel":
                outcome.status = "cancelled"
                stop(outcome.id)
    payload = record(decision.workspace_id, "decision_resolved", f"{author}: {resolution} — {decision.title}",
                     outcome_id=decision.outcome_id, task_id=decision.task_id, actor=user.id, level="success",
                     data={"decision_id": decision.id, "resolution": resolution, "comment": comment[:2000],
                           "reusable": decision.reusable, "scope": decision.scope, **(decision.attribution or {})},
                     session=db, publish=False)
    if pending_events is not None:
        pending_events.extend(completed)
        pending_events.append(payload)
        # Keep a retry without guidance text on the object timeline. With text, guidance() already posted it.
        if action_id and resolution == "retry" and outcome and not comment.strip():
            db.add(Message(workspace_id=decision.workspace_id, outcome_id=outcome.id,
                           task_id=decision.task_id, author_id=user.id, author_kind="user",
                           body="Retried task." if decision.task_id else "Retried planning.",
                           attribution=decision.attribution))
    else:
        db.commit()
        from .work_services import publish_changes
        publish_changes([*completed, payload])
    result: dict[str, Any] = {"ok": True}
    if dropped and task and task.pr_number:
        if after is None:
            result["dropped"] = dropped
        else:
            after.append(("close_pull", dropped, "Abandoned"))
    if take_over:
        from .pipeline.handover import HandoverError, mark_handed_over

        try:
            result["handover"] = mark_handed_over(take_over, user)
            orchestrator.cancel_task(take_over)
        except HandoverError as exc:
            result["error"] = str(exc)
    if pending_events is None:
        orchestrator.wake()
    return result


def overrule(db: Session, decision: Decision, user: User, resolution: str, answer: str,
             reason: str) -> dict[str, Any]:
    """Correct a completed automatic verdict without replaying its work transitions.

    The review link is a compare-and-swap as well as a row lock: SQLite ignores
    FOR UPDATE, so two stale requests must still never create two corrections.
    All audit, guidance and link writes commit together, before notifications.
    """
    from sqlalchemy import update
    from .models import DecisionReview
    from .work_services import publish_changes

    original = db.query(Decision).filter(Decision.id == decision.id,
        Decision.workspace_id == decision.workspace_id).populate_existing().with_for_update().one()
    review = db.query(DecisionReview).filter(DecisionReview.decision_id == original.id,
        DecisionReview.workspace_id == original.workspace_id).populate_existing().with_for_update().first()
    if (original.status != "resolved" or not review or not review.finished_at
            or review.state in ("pending", "running") or review.mode != "decide"
            or review.effective_verdict not in ("decide", "send_back") or review.correction_decision_id):
        raise DecisionError("This verdict is not available for overruling")
    if not answer.strip() or not reason.strip():
        raise DecisionError("A corrected answer and reason are required")
    from .decision_judges import allowed_resolutions, question_fingerprint
    if review.question_fingerprint and review.question_fingerprint != question_fingerprint(original):
        raise DecisionError("The question has changed since this verdict")
    if resolution not in allowed_resolutions(original):
        raise DecisionError("Choose a safe resolution from the original question")
    outcome = db.get(Outcome, original.outcome_id) if original.outcome_id else None
    task = db.get(Task, original.task_id) if original.task_id else None
    if ((original.outcome_id and (not outcome or outcome.workspace_id != original.workspace_id))
            or (original.task_id and (not task or task.workspace_id != original.workspace_id))
            or (task and outcome and task.outcome_id != outcome.id)):
        raise DecisionError("Decision associations do not belong to this workspace")
    now = utcnow()
    attribution = {"source": "judge_correction", "supersedes": original.id,
                   "review_id": review.id, "actor_id": user.id, "reason": reason.strip(),
                   "corrected_at": now.isoformat() + "Z"}
    correction = Decision(workspace_id=original.workspace_id, outcome_id=original.outcome_id,
        task_id=original.task_id, kind=original.kind, title=original.title, body=original.body,
        options=original.options, metadata_json=original.metadata_json, status="resolved",
        resolution=resolution, comment=answer.strip(), resolved_by=user.id, resolved_at=now,
        reusable=True, scope=original.scope, attribution=attribution)
    db.add(correction)
    db.flush()
    claimed = db.execute(update(DecisionReview).where(DecisionReview.id == review.id,
        DecisionReview.correction_decision_id.is_(None)).values(correction_decision_id=correction.id))
    if claimed.rowcount != 1:
        db.rollback()
        raise DecisionError("This verdict has already been corrected")
    original.status = "superseded"
    # Include work waiting on CI or a person, using the scheduler's lifecycle
    # groups. Posting guidance must not resume it or replay approval transitions.
    # Positive lists also exclude terminal and unknown states.
    active_outcome = outcome and outcome.status in (
        "planning", "running", "attention", "blocked", "needs_input", "awaiting_approval")
    active_task = task is None or task.status in ACTIVE_TASK | HUMAN_TASK | {"pending"}
    if active_outcome and active_task:
        db.add(Message(workspace_id=original.workspace_id, outcome_id=outcome.id,
            task_id=task.id if task else None, author_id=user.id, author_kind="user",
            body=f"Correction to decision {original.id}: {answer.strip()}\nReason: {reason.strip()}",
            attribution=attribution))
    payload = record(original.workspace_id, "decision_overruled",
        f"{user.name or user.email} corrected: {original.title}", actor=user.id,
        outcome_id=original.outcome_id, task_id=original.task_id,
        data={"decision_id": original.id, "correction_decision_id": correction.id, **attribution},
        session=db, publish=False)
    db.commit()
    publish_changes([payload])
    from .events import notify
    notify(original.workspace_id, "decision", correction.id)
    return {"ok": True, "correction_decision_id": correction.id}
