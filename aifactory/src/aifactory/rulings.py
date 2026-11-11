"""Team rulings: decisions made anywhere that every agent in the workspace follows.

The Factory already reuses people's decisions: a resolved decision marked reusable, with workspace or repository
scope, is a precedent, and the planner, builders and verifiers are given the applicable ones
(``runtime.precedents``). But only answers to Inbox decisions could become precedents. A rule settled in guidance on
one outcome ("build token-shaped test strings at runtime"), in a Mind chat or in a person's head reached only that
outcome's agents, and the next outcome rediscovered it.

A ruling is a precedent a person states directly, or promotes from guidance they or the Lead posted. It is stored as
a resolved decision of kind ``ruling`` (reusable, workspace or repository scope), so every existing consumer of
precedents follows it with no further plumbing, the Lead reads it in its record, and retiring it uses the existing
precedent switch.
"""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from .auth import current_user, require_member
from .db import get_db, utcnow
from .events import record
from .models import Decision, Message, Outcome, Task, User

KIND = "ruling"
SCOPES = ("workspace", "repository")


def add(db: Session, workspace_id: str, user: User, text: str, *, title: str = "", scope: str = "workspace",
        outcome_id: str | None = None, task_id: str | None = None, source: dict[str, Any] | None = None) -> Decision:
    """Record a ruling. Repository scope applies to the repository of the task (else the outcome) it came from."""
    text = (text or "").strip()
    if not text:
        raise ValueError("A ruling needs text")
    if scope not in SCOPES:
        raise ValueError("Scope must be workspace or repository")
    if scope == "repository" and not (task_id or outcome_id):
        raise ValueError("A repository ruling must come from an outcome or task, to know its repository")
    title = (title or text.splitlines()[0]).strip()[:120]
    ruling = Decision(workspace_id=workspace_id, outcome_id=outcome_id, task_id=task_id, kind=KIND, title=title,
                      body="", options=[{"value": KIND, "label": "Ruling"}], status="resolved", resolution=KIND,
                      comment=text[:4000], resolved_by=user.id, resolved_at=utcnow(), reusable=True, scope=scope,
                      attribution={"source": "ruling", **(source or {})})
    db.add(ruling)
    db.flush()
    return ruling


def view(d: Decision, names: dict[str, str]) -> dict[str, Any]:
    return {"id": d.id, "kind": d.kind, "title": d.title, "text": d.comment if d.kind == KIND else
            f"{d.resolution}{' — ' + d.comment if d.comment else ''}", "scope": d.scope,
            "outcome_id": d.outcome_id, "task_id": d.task_id, "by": names.get(d.resolved_by or "", ""),
            "at": d.resolved_at.isoformat() if d.resolved_at else None, "source": (d.attribution or {}).get("source")}


def active(db: Session, workspace_id: str) -> list[Decision]:
    """Every precedent in force: rulings and reusable decision answers, newest first."""
    return (db.query(Decision).filter(Decision.workspace_id == workspace_id, Decision.status == "resolved",
                                      Decision.reusable.is_(True), Decision.scope.in_(SCOPES))
            .order_by(Decision.resolved_at.desc(), Decision.id).limit(200).all())


# API ------------------------------------------------------------------------------------------------------------

router = APIRouter(prefix="/api")


class RulingIn(BaseModel):
    text: str = Field(default="", max_length=4000)
    title: str = Field(default="", max_length=120)
    scope: str = "workspace"
    outcome_id: str | None = None
    task_id: str | None = None
    message_id: str | None = None  # promote this guidance post; its text is used when no text is given


@router.get("/workspaces/{workspace_id}/rulings")
def list_rulings(workspace_id: str, user: User = Depends(current_user), db: Session = Depends(get_db)):
    require_member(db, user, workspace_id)
    rows = active(db, workspace_id)
    names = {u.id: u.name or u.email or "" for u in db.query(User).filter(User.id.in_({d.resolved_by for d in rows}))}
    return {"rulings": [view(d, names) for d in rows]}


@router.post("/workspaces/{workspace_id}/rulings")
def create_ruling(workspace_id: str, body: RulingIn, user: User = Depends(current_user), db: Session = Depends(get_db)):
    """Make a rule every agent in the workspace (or the repository) follows from now on."""
    require_member(db, user, workspace_id, "member")
    text, outcome_id, task_id, source = body.text, body.outcome_id, body.task_id, {}
    if body.message_id:
        message = db.get(Message, body.message_id)
        if message is None or message.workspace_id != workspace_id or message.author_kind != "user":
            raise HTTPException(404, "Guidance not found")
        text = text or message.body
        outcome_id, task_id = outcome_id or message.outcome_id, task_id or message.task_id
        source = {"message_id": message.id}
        existing = next((d for d in active(db, workspace_id)
                         if d.kind == KIND and (d.attribution or {}).get("message_id") == message.id), None)
        if existing is not None:  # promoting the same guidance again returns the rule it already became
            names = {existing.resolved_by: ""} if existing.resolved_by else {}
            return view(existing, names)
    if outcome_id:
        o = db.get(Outcome, outcome_id)
        if o is None or o.workspace_id != workspace_id:
            raise HTTPException(404, "Outcome not found")
    if task_id:
        t = db.get(Task, task_id)
        if t is None or t.workspace_id != workspace_id:
            raise HTTPException(404, "Task not found")
    try:
        ruling = add(db, workspace_id, user, text, title=body.title, scope=body.scope, outcome_id=outcome_id,
                     task_id=task_id, source=source)
    except ValueError as exc:
        raise HTTPException(400, str(exc))
    db.commit()
    record(workspace_id, "ruling_added", f"{user.name or user.email} made a team rule: {ruling.title}",
           outcome_id=outcome_id, task_id=task_id, actor=user.id, data={"decision_id": ruling.id, "scope": ruling.scope})
    return view(ruling, {user.id: user.name or user.email or ""})


@router.delete("/workspaces/{workspace_id}/rulings/{ruling_id}")
def retire_ruling(workspace_id: str, ruling_id: str, user: User = Depends(current_user), db: Session = Depends(get_db)):
    """Retire a rule: agents stop being given it. The record stays in decision history. Admins retire any rule;
    others only rulings they made."""
    member = require_member(db, user, workspace_id, "member")
    d = db.get(Decision, ruling_id)
    if d is None or d.workspace_id != workspace_id or not d.reusable:
        raise HTTPException(404, "Rule not found")
    if member.role not in ("admin", "owner") and not (d.kind == KIND and d.resolved_by == user.id):
        raise HTTPException(403, "Only an admin, or the person who made this rule, can retire it")
    d.reusable = False
    db.commit()
    record(workspace_id, "ruling_retired", f"{user.name or user.email} retired a team rule: {d.title}",
           actor=user.id, data={"decision_id": d.id})
    return {"ok": True}
