"""Proposal-only model tools and separately authenticated confirmation adapters."""
from __future__ import annotations

import json

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy import update
from sqlalchemy.orm import Session

from ..auth import current_user
from ..db import get_db
from ..decisions import resolve
from ..models import Decision, MindAction, MindActionResult, User, Workspace
from ..security import redact
from ..work_services import publish_changes, send_guidance, target
from .api import PrivateRoute
from .tools import Context, ReadTool, citation, clean, current, validate_arguments

KINDS = frozenset({"answer_question", "send_guidance", "retry_task"})
# The thread API's route class: a 422 never echoes rejected input (which can hold a pasted secret).
router = APIRouter(prefix="/mind/threads", route_class=PrivateRoute)


def fields(kind: str, text: str | None) -> dict:
    text = redact((text or "").strip())
    if len(text) > 8000 or (not text and kind != "retry_task"):
        raise HTTPException(422, "Text must contain 1–8000 characters")
    return {"text": text} if text else {}


def validate(context: Context, kind, workspace_id, outcome_id, task_id, decision_id):
    if kind not in KINDS:
        raise HTTPException(422, "Unsupported action kind")
    db, user = context.db, current(context)
    outcome, task = target(db, user, workspace_id, outcome_id, task_id, "member")
    if outcome.status in ("completed", "cancelled"):
        raise HTTPException(409, "Outcome is no longer active")
    decision = None
    if kind == "send_guidance":
        if decision_id:
            raise HTTPException(422, "Guidance does not resolve decisions")
    else:
        decision = db.query(Decision).filter_by(id=decision_id, workspace_id=workspace_id,
                        outcome_id=outcome_id, task_id=task_id).with_for_update().populate_existing().one_or_none()
        expected_kind, option = ("question", "answer") if kind == "answer_question" else ("unblock", "retry")
        if (decision is None or decision.status != "open" or decision.kind != expected_kind or
                option not in {o.get("value") for o in decision.options or []}):
            raise HTTPException(409, "Decision is no longer eligible")
        if kind == "retry_task" and (task is None or task.status not in ("blocked", "failed")):
            raise HTTPException(409, "Task is no longer retryable")
    snapshot = {"outcome_status": outcome.status, "outcome_title": outcome.title,
                "task_status": task.status if task else None, "task_stage": task.stage if task else None,
                "task_title": task.title if task else None, "task_attempt": task.attempt if task else None,
                "decision": {"title": decision.title, "body": decision.body, "kind": decision.kind,
                             "options": decision.options} if decision else None}
    return outcome, task, decision, snapshot


def card(action: MindAction, context: Context) -> dict:
    db = context.db
    workspace = db.get(Workspace, action.workspace_id)
    task = action.task_id is not None
    label = action.snapshot.get("task_title") if task else action.snapshot.get("outcome_title")
    obj = citation(action.workspace_id, "task" if task else "outcome", action.task_id or action.outcome_id,
                   label or "Work", action.outcome_id, action.task_id)
    if action.kind == "answer_question":
        effect = f'Answer the open question “{(action.snapshot.get("decision") or {}).get("title", "") }” with the exact text below; resume its work.'
        obj = citation(action.workspace_id, "decision", action.decision_id, (action.snapshot.get("decision") or {}).get("title", "Question"), action.outcome_id, action.task_id)
    elif action.kind == "retry_task":
        effect = "Resolve the open unblock decision as retry and requeue this task. Work may run again and incur execution costs."
    else:
        effect = "Post the exact text below as your guidance on this " + ("task." if task else "outcome.")
    return json.loads(redact(json.dumps({"id": action.id, "thread_id": action.thread_id, "revision": action.revision, "kind": action.kind,
                  "workspace": {"id": workspace.id, "name": workspace.name},
                  "object": {k: obj[k] for k in ("kind", "id", "label", "href")},
                  "fields": action.fields, "effect": effect, "status": action.status,
                  **({"result": action.result} if action.result is not None else {})})))


def propose(name: str, args: dict, context: Context) -> dict:
    spec = PROPOSALS.get(name)
    if spec is None:
        raise HTTPException(422, "Unknown proposal")
    validate_arguments(spec.schema, args)
    if not context.thread_id or len(context.thread_id) > 32:
        raise HTTPException(422, "An owner-validated thread context is required")
    kind = name.removeprefix("propose_")
    data = fields(kind, args.get("text"))
    _, _, _, snapshot = validate(context, kind, args["workspace_id"], args["outcome_id"], args.get("task_id"), args.get("decision_id"))
    action = MindAction(owner_id=context.user_id, thread_id=context.thread_id, kind=kind,
                        workspace_id=args["workspace_id"], outcome_id=args["outcome_id"], task_id=args.get("task_id"),
                        decision_id=args.get("decision_id"), fields=data, snapshot=snapshot)
    context.db.add(action)
    context.db.commit()
    return card(action, context)


PROPOSALS = {}
for _kind in sorted(KINDS):
    _properties = {k: {"type": "string"} for k in ("workspace_id", "outcome_id", "task_id", "decision_id")}
    _properties["text"] = {"type": "string", "maxLength": 8000}
    _required = ["workspace_id", "outcome_id"]
    if _kind != "retry_task":
        _required.append("text")
    if _kind != "send_guidance":
        _required.append("decision_id")
    if _kind == "retry_task":
        _required.append("task_id")
    _name = "propose_" + _kind
    PROPOSALS[_name] = ReadTool(_name, f"Propose {_kind}; creates a pending card only, never executes.",
                               {"type": "object", "properties": _properties, "required": _required,
                                "additionalProperties": False}, propose)


def owned(context: Context, action_id: str):
    action = context.db.query(MindAction).filter_by(id=action_id, thread_id=context.thread_id,
                   owner_id=current(context).id).populate_existing().one_or_none()
    if action is None:
        raise HTTPException(404, "Action not found")
    return action


def claim(context, action, expected_revision, **values):
    count = context.db.execute(update(MindAction).where(MindAction.id == action.id,
                 MindAction.owner_id == context.user_id, MindAction.thread_id == context.thread_id,
                 MindAction.revision == expected_revision, MindAction.status == "pending").values(**values),
                 execution_options={"synchronize_session": False}).rowcount
    if count != 1:
        context.db.rollback()
        raise HTTPException(409, "Action changed or is no longer pending")
    context.db.refresh(action)


def edit(context: Context, action_id: str, expected_revision: int, *, text: str | None = None, dismiss=False):
    action = owned(context, action_id)
    if dismiss and text is not None or not dismiss and text is None:
        raise HTTPException(422, "Choose text or dismiss")
    # Dismiss cannot write anything to shared objects, even after membership revocation.
    if dismiss:
        claim(context, action, expected_revision, status="dismissed", revision=expected_revision + 1)
    else:
        validate(context, action.kind, action.workspace_id, action.outcome_id, action.task_id, action.decision_id)
        claim(context, action, expected_revision, fields=fields(action.kind, text), revision=expected_revision + 1)
    context.db.commit()
    return card(action, context)


def confirm(context: Context, action_id: str, expected_revision: int):
    db = context.db
    action = owned(context, action_id)
    payloads = []
    try:
        # Conditional claim serializes confirmations and edits before any shared mutation.
        claim(context, action, expected_revision, status="executing")
        _, _, decision, snapshot = validate(context, action.kind, action.workspace_id, action.outcome_id,
                                            action.task_id, action.decision_id)
        if snapshot != action.snapshot:
            raise HTTPException(409, "Action target changed; propose a new card")
        if action.kind == "send_guidance":
            message, payload = send_guidance(db, current(context), action.workspace_id, action.outcome_id,
                                            action.fields["text"], action.task_id, action_id=action.id)
            payloads.append(payload)
            result = {"ok": True, "message_id": message.id}
        else:
            result = resolve(db, decision, current(context), "answer" if action.kind == "answer_question" else "retry",
                             action.fields.get("text", ""), action_id=action.id, pending_events=payloads)
        action.status = "succeeded"
        action.result = clean({"action_id": action.id, "kind": action.kind, "status": "succeeded", **result})
        db.add(MindActionResult(thread_id=action.thread_id, owner_id=context.user_id,
                               action_id=action.id, content=action.result))
        db.commit()
    except Exception:
        db.rollback()
        raise
    publish_changes(payloads)
    return card(action, context)


def execution_results(context: Context) -> list[dict]:
    """Owner-only continuation projection for T1 to include in the next engine turn."""
    current(context)
    return [clean(row.content) for row in context.db.query(MindActionResult).filter_by(
        thread_id=context.thread_id, owner_id=context.user_id).order_by(MindActionResult.created_at.desc()).limit(20)][::-1]


class EditBody(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)
    expected_revision: int = Field(ge=1)
    text: str | None = Field(default=None, max_length=8000)
    dismiss: bool = False


class ConfirmBody(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)
    expected_revision: int = Field(ge=1)


def _private(db: Session, user: User, thread_id: str) -> tuple:
    """The thread API's own access rule and egress scrub (owner, every scoped workspace still a membership; the
    current secrets of the person's workspaces), applied to card edits and confirmations too."""
    from . import store
    from .api import scrub, secrets_for

    thread = store.owned(db, user.id, thread_id)
    secrets = secrets_for(db, user.id, thread.workspace_ids)
    return (lambda value: scrub(value, secrets)), thread


@router.patch("/{thread_id}/actions/{action_id}")
def patch_action(thread_id: str, action_id: str, body: EditBody,
                 user: User = Depends(current_user), db: Session = Depends(get_db)):
    clean_value, _ = _private(db, user, thread_id)
    text = clean_value(body.text) if body.text is not None else None
    return clean_value(edit(Context(db, user.id, thread_id), action_id, body.expected_revision, text=text,
                            dismiss=body.dismiss))


@router.post("/{thread_id}/actions/{action_id}/confirm")
def confirm_action(thread_id: str, action_id: str, body: ConfirmBody,
                   user: User = Depends(current_user), db: Session = Depends(get_db)):
    clean_value, _ = _private(db, user, thread_id)
    action = db.query(MindAction).filter_by(id=action_id, thread_id=thread_id, owner_id=user.id).one_or_none()
    if action is not None and action.status == "pending":
        # A secret saved after the card was proposed is still never posted to shared work.
        fields = clean_value(action.fields)
        if fields != action.fields:
            action.fields = fields
            db.commit()
    return clean_value(confirm(Context(db, user.id, thread_id), action_id, body.expected_revision))
