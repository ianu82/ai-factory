"""The provider hold: when the model provider refuses a workspace (billing, quota or its credential), every task and
plan of that workspace would fail the same way. One open ``provider_hold`` decision, with no outcome and no task, is
the hold: the scheduler starts nothing in the workspace while it is open, and resolving it lets work start again."""

from __future__ import annotations

from sqlalchemy.orm import Session

from ..db import session_scope
from ..models import Decision
from ..onboarding_policy import lock_workspace
from .runtime import publish_decision

PROVIDER_HOLD = "provider_hold"
TITLE = "Model provider refused: work is on hold"


def open_provider_hold(workspace_id: str, error: str, source: str) -> str:
    """Hold the workspace unless it is held already; returns the hold's decision. ``source`` says what was refused
    first ("T2 of outcome #7, the builder session"). Work that fails while the hold is open finds that decision."""
    with session_scope() as s:
        # First statement of the transaction, so two failures in the same moment take turns: the second one's
        # query below runs after the first one's commit and finds its decision.
        lock_workspace(s, workspace_id)
        existing = s.query(Decision.id).filter(Decision.workspace_id == workspace_id, Decision.kind == PROVIDER_HOLD,
                                               Decision.status == "open").first()
        if existing:
            return existing[0]
        decision = Decision(
            workspace_id=workspace_id, kind=PROVIDER_HOLD, title=TITLE,
            body=f"The model provider refused a request ({source}):\n\n{error[:1500]}\n\n"
                 "Nothing new starts in this workspace, neither planning nor tasks, until someone chooses Resume "
                 "work. Work that is refused the same way waits in the queue; no task is charged an attempt for "
                 "it.\n\nWhat to do: add credits or fix the provider credential in Settings, then choose Resume "
                 "work. If the provider still refuses, this hold opens again.",
            options=[{"value": "retry", "label": "Resume work"}])
        s.add(decision)
        s.flush()
        decision_id = decision.id
    publish_decision(workspace_id, decision_id, PROVIDER_HOLD, TITLE)
    return decision_id


def held_workspaces(session: Session) -> set[str]:
    """The workspaces with an open provider hold."""
    return {workspace_id for (workspace_id,) in session.query(Decision.workspace_id).filter(
        Decision.kind == PROVIDER_HOLD, Decision.status == "open")}
