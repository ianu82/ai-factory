"""Learning is a workspace lifecycle, not a change to operational defaults.

All writes replace JSON values. The requested preset is kept separately from
its effective approval-only policy until an administrator explicitly applies it.
"""
from __future__ import annotations

from copy import deepcopy
from typing import Any

from fastapi import HTTPException
from sqlalchemy import update
from sqlalchemy.orm import Session

from .models import Outcome, Workspace

KEY = "learning"
POLICIES = ("plan", "merge", "release")


class MergeReservationConflict(HTTPException):
    """A short-lived transaction must not invalidate an authorized external effect.

    The fence is durable, not a lease: an uncertain request needs reconciliation,
    never a timeout-based takeover. HTTP callers can retry after reconciliation.
    """

    def __init__(self):
        super().__init__(409, "A merge is authorized or awaiting reconciliation; retry after it settles",
                         headers={"Retry-After": "1"})


def check_merge_reservation(ws: Workspace, *, token: str | None = None) -> None:
    reservation = ws.merge_reservation
    if reservation and (token is None or reservation.get("token") != token):
        raise MergeReservationConflict()


def lock_workspace(db: Session, workspace_id: str, *, reservation_token: str | None = None) -> Workspace:
    """Serialize policy gates and settings writes, then discard any stale snapshot.

    A no-op UPDATE locks the row on PostgreSQL and acquires SQLite's write lock
    (where SELECT FOR UPDATE is ignored). Never compare JSON: PostgreSQL's JSON
    type has no equality operator. The caller owns the transaction/commit.
    """
    with db.no_autoflush:
        db.execute(update(Workspace).where(Workspace.id == workspace_id)
                   .values(outcome_seq=Workspace.outcome_seq)
                   .execution_options(synchronize_session=False))
        ws = (db.query(Workspace).filter(Workspace.id == workspace_id)
              .populate_existing().one())
        check_merge_reservation(ws, token=reservation_token)
        return ws


def active(raw: dict[str, Any] | None) -> bool:
    return ((raw or {}).get(KEY) or {}).get("status") == "learning"


def completed(db: Session, workspace_id: str) -> Outcome | None:
    return (db.query(Outcome).filter(Outcome.workspace_id == workspace_id, Outcome.status == "completed")
            .order_by(Outcome.completed_at, Outcome.created_at, Outcome.id).first())


def begin(raw: dict[str, Any] | None) -> dict[str, Any]:
    from .pipeline.runtime import merged_settings

    result = deepcopy(raw or {})
    if KEY not in result:
        preset = merged_settings(result)["autonomy"]
        result[KEY] = {"status": "learning", "preset": deepcopy(preset)}
        result["autonomy"] = {**preset, "plan": "approval", "merge": "approval"}
    return result


def effective(settings: dict[str, Any]) -> dict[str, Any]:
    if active(settings):
        settings["autonomy"] = {**settings["autonomy"], "plan": "approval", "merge": "approval"}
    return settings


def patch_autonomy(raw: dict[str, Any], patch: dict[str, Any]) -> dict[str, Any]:
    """Partial policy edits target the desired preset while learning is active."""
    result = deepcopy(raw)
    if active(result):
        state = result[KEY]
        preset = {**state["preset"], **patch}
        result[KEY] = {**state, "preset": preset}
        result["autonomy"] = {**preset, "plan": "approval", "merge": "approval"}
    else:
        result["autonomy"] = {**result["autonomy"], **patch}
    return result


def offer(db: Session, ws: Workspace) -> dict[str, Any]:
    from .pipeline.runtime import merged_settings

    settings = merged_settings(ws.settings)
    state = settings.get(KEY) or {}
    before = settings["autonomy"]
    learning = active(settings)
    after = state.get("preset", before) if learning else before
    first = completed(db, ws.id)
    return {
        "learning": learning,
        "eligible": learning and first is not None,
        "applied": state.get("status") == "applied",
        "first_completed_outcome_id": first.id if first else None,
        "before": before,
        "after": after,
        "changes": [{"policy": key, "before": before[key], "after": after[key]}
                    for key in POLICIES if before[key] != after[key]],
    }
