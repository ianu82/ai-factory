"""Validated shared work mutations, used by the app's endpoints and by confirmed Mind cards alike.

Callers own the commit, then publish the returned events (publish_changes) and run what a mutation leaves for
after its commit (finish): stopping planners and jobs, publishing a hand-over, closing a dropped pull request.
"""
from __future__ import annotations

import logging
from typing import Any

from fastapi import HTTPException
from sqlalchemy.orm import Session

from . import events
from .auth import require_member
from .models import Decision, Message, Outcome, Run, Task, User
from .serializers import actionable

log = logging.getLogger(__name__)


def author(user: User, action_id: str | None = None) -> str:
    """How shared work names the person: "Alice", or "Alice via Mind" for a confirmed Mind card."""
    return (user.name or user.email) + (" via Mind" if action_id else "")


def attribution(action_id: str | None) -> dict[str, Any]:
    return {"source": "mind", "action_id": action_id} if action_id else {}


def target(db: Session, user: User, workspace_id: str, outcome_id: str,
           task_id: str | None = None, minimum: str = "viewer") -> tuple[Outcome, Task | None]:
    require_member(db, user, workspace_id, minimum)
    outcome = db.query(Outcome).filter_by(id=outcome_id, workspace_id=workspace_id).populate_existing().one_or_none()
    if outcome is None:
        raise HTTPException(404, "Outcome not found")
    task = None
    if task_id:
        task = db.query(Task).filter_by(id=task_id, workspace_id=workspace_id,
                                        outcome_id=outcome_id).populate_existing().one_or_none()
        if task is None:
            raise HTTPException(404, "Task not found")
    return outcome, task


def send_guidance(db: Session, user: User, workspace_id: str, outcome_id: str,
                  text: str, task_id: str | None = None, *, action_id: str | None = None):
    target(db, user, workspace_id, outcome_id, task_id, "member")
    text = text.strip()
    if not text or len(text) > 8000:
        raise HTTPException(422, "Guidance must contain 1–8000 characters")
    source = attribution(action_id)
    message = Message(workspace_id=workspace_id, outcome_id=outcome_id, task_id=task_id,
                      author_id=user.id, author_kind="user", body=text, attribution=source)
    db.add(message)
    db.flush()
    payload = events.record(workspace_id, "guidance", f"{author(user, action_id)}: {text[:200]}",
                            outcome_id=outcome_id, task_id=task_id, actor=user.id,
                            data={"intervention": True, **source}, session=db, publish=False)
    return message, payload


def create_outcome(db: Session, user: User, workspace_id: str, *, title: str, brief: str, repo_ids: list[str] | None,
                   acceptance: list[str] | None = None, harness: str | None = None, priority: int = 2,
                   attachment_ids: list[str] | None = None, template: str | None = None,
                   signal_id: str | None = None, action_id: str | None = None,
                   pending_events: list[dict] | None = None, workstream_id: str | None = None) -> Outcome:
    """POST /outcomes and a confirmed Mind card: connected repositories, a known harness, staged attachments
    claimed before anything is created. Callers commit and wake the orchestrator. A Mind card's outcome has source
    "mind" (the app names its creator "<person> via Mind") and its creation event waits in pending_events."""
    from . import attachments as A
    from .guided import README_ACCEPTANCE, README_BRIEF, README_TEMPLATE, README_TITLE
    from .models import Artifact, Repository, new_id
    from .signals import outcome_from_text

    require_member(db, user, workspace_id, "member")
    from . import workstreams

    attachment_ids = list(attachment_ids or [])
    if template and (attachment_ids or (repo_ids is not None and len(repo_ids) != 1)):
        raise HTTPException(400, "The README example requires exactly one repository and no attachments")
    try:
        uploads = A.validate_staged(db, workspace_id, user.id, attachment_ids)
    except LookupError as exc:
        raise HTTPException(404, str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc
    workstreams.lock_workspace(db, workspace_id)
    repo_ids = workstreams.creation_repositories(db, workspace_id, repo_ids, workstream_id)
    repos = db.query(Repository).filter(Repository.workspace_id == workspace_id, Repository.id.in_(repo_ids)).all()
    if not repo_ids or len(repos) != len(set(repo_ids)):
        raise HTTPException(400, "Unknown repository")
    if template and (len(repo_ids) != 1 or attachment_ids):
        raise HTTPException(400, "The README example requires exactly one repository and no attachments")
    if harness and harness not in ("native", "codex", "anton"):
        raise HTTPException(400, "Unknown harness")
    # Claim before creating any outcome/events. The conditional UPDATE serializes
    # competing submissions on both SQLite and Postgres; a loser rolls back all claims.
    claim_id = new_id()
    claimed = A.staged_query(db, workspace_id, user.id).filter(Artifact.id.in_(attachment_ids)).update(
        {Artifact.outcome_id: claim_id}, synchronize_session=False)
    if claimed != len(uploads):
        db.rollback()
        raise HTTPException(409, "Attachment is no longer available")
    outcome = outcome_from_text(db, workspace_id, user, README_TITLE if template else title,
                                README_BRIEF if template else brief, [r.id for r in repos],
                                acceptance=README_ACCEPTANCE if template else acceptance,
                                harness=harness, priority=max(0, min(priority, 3)), signal_id=signal_id,
                                guidance={"template": README_TEMPLATE, "repo_id": repos[0].id} if template else None,
                                source="mind" if action_id else "user", action_id=action_id,
                                pending_events=pending_events, workstream_id=workstream_id)
    if uploads:
        db.query(Artifact).filter(Artifact.workspace_id == workspace_id, Artifact.outcome_id == claim_id).update(
            {Artifact.outcome_id: outcome.id}, synchronize_session=False)
        events.record(workspace_id, "attachments_added", f"{user.name or user.email} added {len(uploads)} attachments",
                      outcome_id=outcome.id, actor=user.id,
                      data={"ids": [a.id for a in uploads], "names": [a.name for a in uploads],
                            "count": len(uploads)}, session=db)
    A.cleanup_staged(db)
    return outcome


def cancel_outcome(db: Session, user: User, workspace_id: str, outcome_id: str, *,
                   action_id: str | None = None) -> tuple[dict, list[tuple]]:
    """POST /outcomes/{id}/cancel and a confirmed Mind card: the outcome and its unfinished tasks are cancelled and
    its open decisions superseded. Returns the audit event to publish after the commit, and the follow-ups for
    finish(): stop its planner and jobs, then close the cancelled tasks' pull requests."""
    outcome, _ = target(db, user, workspace_id, outcome_id, None, "member")
    outcome.status = "cancelled"
    later: list[tuple] = [("stop_outcome", outcome.id)]
    for t in db.query(Task).filter(Task.outcome_id == outcome.id, Task.status.notin_(["completed", "abandoned"])):
        t.status = "cancelled"
        if t.pr_number:
            later.append(("close_pull", t.id, "Cancelled"))
    for d in db.query(Decision).filter(Decision.outcome_id == outcome.id, Decision.status == "open"):
        d.status = "superseded"
    payload = events.record(workspace_id, "outcome_cancelled", f"{author(user, action_id)} cancelled #{outcome.number}",
                            outcome_id=outcome.id, actor=user.id, level="warn", data=attribution(action_id),
                            session=db, publish=False)
    return payload, later


def task_dependents(db: Session, task: Task) -> list[Task]:
    """The unfinished tasks of the task's outcome that wait for it, directly or through other tasks."""
    from .pipeline.lifecycle import dependents

    return dependents(db.query(Task).filter(Task.outcome_id == task.outcome_id).all(), task.key)


def cancel_task(db: Session, user: User, workspace_id: str, task_id: str, *, dependents: str = "keep",
                action_id: str | None = None) -> tuple[list[dict], list[tuple]]:
    """POST /tasks/{id}/cancel and a confirmed Mind card: the task is cancelled unless it has completed. Returns the
    audit events and the follow-ups: stop its job, then close its pull request.

    ``dependents``: "drop" cancels the tasks that wait for it as well, each with an event that says why; "keep"
    leaves them waiting, and the scheduler asks a person what to do with them."""
    require_member(db, user, workspace_id, "member")
    t = db.query(Task).filter_by(id=task_id, workspace_id=workspace_id).populate_existing().one_or_none()
    if t is None:
        raise HTTPException(404, "Task not found")
    if dependents not in ("keep", "drop"):
        raise HTTPException(422, "dependents is keep or drop")
    waiting = task_dependents(db, t) if dependents == "drop" else []
    if t.status not in ("completed",):
        t.status = "cancelled"
    payloads = [events.record(workspace_id, "task_cancelled", f"{author(user, action_id)} stopped {t.key}",
                              outcome_id=t.outcome_id, task_id=t.id, actor=user.id, level="warn",
                              data=attribution(action_id), session=db, publish=False)]
    later: list[tuple] = [("stop_task", t.id, workspace_id, t.outcome_id)]
    if t.status == "cancelled" and t.pr_number:
        later.append(("close_pull", t.id, "Cancelled"))
    for w in waiting:
        w.status = "cancelled"
        payloads.append(events.record(
            workspace_id, "task_cancelled", f"{author(user, action_id)} dropped {w.key}: it waits for {t.key}, "
            "which was cancelled", outcome_id=w.outcome_id, task_id=w.id, actor=user.id, level="warn",
            data={**attribution(action_id), "with": t.key}, session=db, publish=False))
        if w.pr_number:
            later.append(("close_pull", w.id, "Cancelled"))
    return payloads, later


async def finish(steps: list[tuple], user: User, action_id: str | None = None) -> dict[str, Any]:
    """Run, in order and on the event loop, what committed changes left for afterwards: stopping planners and jobs
    (which are this loop's tasks), publishing a hand-over's candidate, closing pull requests on GitHub. A failed
    step is logged and reported; it never undoes the committed change, as in the app."""
    from .pipeline.handover import close_dropped_pull, publish_candidate
    from .pipeline.orchestrator import orchestrator

    result: dict[str, Any] = {}
    for step, object_id, *args in steps:
        try:
            if step == "stop_outcome":
                orchestrator.cancel_outcome(object_id)
            elif step == "stop_task":
                orchestrator.cancel_task(object_id)
                events.notify(args[0], "task", object_id, outcome_id=args[1])
            elif step == "publish_candidate":
                result["handover"] = await publish_candidate(object_id, args[0])
            elif step == "close_pull":
                closed = await close_dropped_pull(object_id, author(user, action_id), args[0])
                result["pull_requests_closed"] = result.get("pull_requests_closed", 0) + int(bool(closed))
            else:
                raise ValueError(f"Unknown follow-up {step}")
        except Exception:
            log.exception("Follow-up %s for %s failed", step, object_id)
            result.setdefault("follow_ups_failed", []).append(step)
    return result


def publish_changes(payloads: list[dict]) -> None:
    """Only call after a successful commit, including the saved live audit event."""
    for payload in payloads:
        events.bus.publish({"type": "event", **payload})
        events.notify(payload["workspace_id"], "outcome", payload["outcome_id"])
        if payload["data"].get("decision_id"):
            events.notify(payload["workspace_id"], "decision", payload["data"]["decision_id"])
    if payloads:
        from .pipeline.orchestrator import orchestrator
        orchestrator.wake()


def outcome_rows(db: Session, workspace_id: str, outcome_ids: list[str]) -> tuple[dict, dict, dict]:
    """Shared app/Mind progress inputs; paginate objects, never their aggregate population."""
    tasks, runs, decisions = {}, {}, {}
    if outcome_ids:
        for t in db.query(Task).filter(Task.workspace_id == workspace_id, Task.outcome_id.in_(outcome_ids)):
            tasks.setdefault(t.outcome_id, []).append(t)
        for r in db.query(Run).filter(Run.workspace_id == workspace_id, Run.outcome_id.in_(outcome_ids)):
            runs.setdefault(r.outcome_id, []).append(r)
        for d in db.query(Decision).filter(Decision.workspace_id == workspace_id,
                                           Decision.outcome_id.in_(outcome_ids), Decision.status == "open"):
            if actionable(d):
                decisions[d.outcome_id] = decisions.get(d.outcome_id, 0) + 1
    return tasks, runs, decisions
