"""Validated shared work mutations. Callers own commit and post-commit publication."""
from __future__ import annotations

from fastapi import HTTPException
from sqlalchemy.orm import Session

from . import events
from .auth import require_member
from .models import Decision, Message, Outcome, Run, Task, User


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
    attribution = {"source": "mind", "action_id": action_id} if action_id else {}
    message = Message(workspace_id=workspace_id, outcome_id=outcome_id, task_id=task_id,
                      author_id=user.id, author_kind="user", body=text, attribution=attribution)
    db.add(message)
    db.flush()
    author = (user.name or user.email) + (" via Mind" if action_id else "")
    payload = events.record(workspace_id, "guidance", f"{author}: {text[:200]}", outcome_id=outcome_id,
                            task_id=task_id, actor=user.id,
                            data={"intervention": True, **attribution}, session=db, publish=False)
    return message, payload


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
            decisions[d.outcome_id] = decisions.get(d.outcome_id, 0) + 1
    return tasks, runs, decisions
