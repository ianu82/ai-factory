"""Durable triage primitives. Each write helper owns a short transaction.

Claim commits the immutable input set before returning. Call the model only after
it returns, then finalize with the returned fencing token. No external effects
belong in these transactions. Admission code must lock the proposal row before
creating/linking an outcome in the same transaction.
"""
from __future__ import annotations

import hashlib
import json
import math
from datetime import datetime, timedelta
from typing import Any

from sqlalchemy import select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.orm import Session

from .db import session_scope, utcnow
from .models import (Run, Signal, SignalRevision, TriageOverride, TriageProposal,
                     TriageState, TriageVerdict, TriageWindow, Workspace, new_id)


def _insert_once(db: Session, model, values: dict, keys: list[str]) -> None:
    insert = sqlite_insert if db.bind.dialect.name == "sqlite" else pg_insert
    db.execute(insert(model).values(**values).on_conflict_do_nothing(index_elements=keys))


def signal_snapshot(signal: Signal) -> dict[str, Any]:
    """Material content only: polling counters, status and outcome links are not revisions.

    Ingestion may pass a normalized snapshot to snapshot_revision instead. Inputs
    must already have passed the source's credential/redaction boundary.
    """
    return {key: getattr(signal, key) for key in
            ("source", "external_id", "repo_id", "title", "body", "url", "data")}


def snapshot_revision(db: Session, signal: Signal, content: dict | None = None) -> SignalRevision:
    content = signal_snapshot(signal) if content is None else content
    encoded = json.dumps(content, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False)
    digest = hashlib.sha256(encoded.encode()).hexdigest()
    _insert_once(db, SignalRevision, dict(id=new_id(), workspace_id=signal.workspace_id,
        signal_id=signal.id, content_hash=digest, snapshot=json.loads(encoded), created_at=utcnow()),
        ["workspace_id", "signal_id", "content_hash"])
    return db.scalars(select(SignalRevision).filter_by(workspace_id=signal.workspace_id,
        signal_id=signal.id, content_hash=digest)).one()


def claim_window(workspace_id: str, *, now: datetime | None = None,
                 lease_seconds: int = 300, limit: int = 300) -> TriageWindow | None:
    """Claim due work, or recover the exact inputs of an expired window.

    A conditional UPDATE serializes claims on both databases, even when the state
    row was just inserted by another process. A new token fences out old workers.
    Scheduler policy (enabled, interval, budgets) is supplied by the caller.
    """
    if not 1 <= limit <= 300 or lease_seconds <= 0:
        raise ValueError("invalid triage claim bounds")
    now = now or utcnow()
    token = new_id()
    with session_scope() as db:
        if db.get(Workspace, workspace_id) is None:
            raise ValueError("unknown workspace")
        _insert_once(db, TriageState, dict(workspace_id=workspace_id, lease_token="", scan_after_id=""), ["workspace_id"])
        claimed = db.execute(update(TriageState).where(TriageState.workspace_id == workspace_id,
            (TriageState.lease_until.is_(None) | (TriageState.lease_until <= now)),
            (TriageState.next_due_at.is_(None) | (TriageState.next_due_at <= now) |
             TriageState.active_window_id.is_not(None))).values(lease_token=token,
                lease_until=now + timedelta(seconds=lease_seconds)))
        if not claimed.rowcount:
            return None
        state = db.get(TriageState, workspace_id)
        if state.active_window_id:
            window = db.get(TriageWindow, state.active_window_id)
            if window is None or window.status != "running":
                raise ValueError("invalid active triage window")
            window.lease_token = token
            return window
        revisions = []
        # Bound both rows loaded and snapshot writes. Rotate through a keyset
        # page so reviewed signals cannot starve later/new signals indefinitely.
        candidates = list(db.scalars(select(Signal).where(
            Signal.workspace_id == workspace_id, Signal.id > state.scan_after_id)
            .order_by(Signal.id).limit(limit)))
        state.scan_after_id = candidates[-1].id if len(candidates) == limit else ""
        for signal in candidates:
            revision = snapshot_revision(db, signal)
            if db.scalar(select(TriageVerdict.id).where(TriageVerdict.revision_id == revision.id)) is None:
                revisions.append(revision.id)
        run = Run(workspace_id=workspace_id, outcome_id=None, role="triage", harness="triage")
        db.add(run)
        db.flush()
        window = TriageWindow(workspace_id=workspace_id, run_id=run.id, revision_ids=revisions,
                              lease_token=token, created_at=now)
        db.add(window)
        db.flush()
        state.active_window_id = window.id
        return window


def proposal_for_signal(db: Session, workspace_id: str, signal_id: str) -> TriageProposal:
    """Reserve one durable identity across revisions. Never unique by outcome:
    GitHub intake can legitimately attach several signals to the same outcome.
    """
    signal = db.get(Signal, signal_id)
    if signal is None or signal.workspace_id != workspace_id:
        raise ValueError("unknown signal")
    if signal.outcome_id:
        from .models import Outcome

        outcome = db.get(Outcome, signal.outcome_id)
        if outcome is None or outcome.workspace_id != workspace_id:
            raise ValueError("signal outcome outside workspace")
    _insert_once(db, TriageProposal, dict(id=new_id(), workspace_id=workspace_id,
        signal_id=signal_id, outcome_id=signal.outcome_id, state="pending", created_at=utcnow()), ["signal_id"])
    return db.scalars(select(TriageProposal).filter_by(workspace_id=workspace_id, signal_id=signal_id)
                      .with_for_update()).one()


def finalize_window(workspace_id: str, window_id: str, token: str, verdicts: list[dict], *,
                    now: datetime | None = None, interval_seconds: int = 1800,
                    run_result: dict | None = None) -> bool:
    """Atomically record exactly one verdict per snapshotted revision and finish.
    Expired or superseded workers cannot publish results. Application is a later
    step; proposal reservation here performs no external effects.
    """
    now = now or utcnow()
    if interval_seconds < 0:
        raise ValueError("invalid triage interval")
    with session_scope() as db:
        locked = db.execute(update(TriageState).where(TriageState.workspace_id == workspace_id,
            TriageState.active_window_id == window_id, TriageState.lease_token == token,
            TriageState.lease_until > now).values(lease_token=token))
        if not locked.rowcount:
            return False
        window = db.get(TriageWindow, window_id)
        if window.status != "running" or window.lease_token != token:
            return False
        if (len(verdicts) != len(window.revision_ids) or
                {v["revision_id"] for v in verdicts} != set(window.revision_ids)):
            raise ValueError("verdicts must match the exact window revisions")
        summary = dict.fromkeys(("signals_seen", "proposed", "attached", "duplicated", "ignored", "asked"), 0)
        counters = dict(propose="proposed", attach="attached", duplicate="duplicated", ignore="ignored", ask="asked")
        for data in verdicts:
            verdict = data["verdict"]
            confidence = data["confidence"]
            if verdict not in counters or isinstance(confidence, bool) or not math.isfinite(confidence) or not 0 <= confidence <= 1:
                raise ValueError("invalid triage verdict or confidence")
            revision = db.get(SignalRevision, data["revision_id"])
            if revision.workspace_id != workspace_id:
                raise ValueError("revision outside workspace")
            goal_kind = data.get("goal_kind")
            goal_id = data.get("goal_id")
            if goal_kind not in (None, "workspace", "workstream") or bool(goal_kind) != bool(goal_id):
                raise ValueError("invalid normalized goal reference")
            db.add(TriageVerdict(workspace_id=workspace_id, signal_id=revision.signal_id,
                revision_id=revision.id, run_id=window.run_id, verdict=verdict, created_at=now,
                reasons=data.get("reasons", []), confidence=confidence, goal_kind=goal_kind,
                goal_id=goal_id, goal_snapshot=data.get("goal_snapshot", {}),
                payload=data.get("payload", {})))
            if verdict == "propose":
                proposal_for_signal(db, workspace_id, revision.signal_id)
            summary["signals_seen"] += 1
            summary[counters[verdict]] += 1
        window.status = "completed"
        window.completed_at = now
        state = db.get(TriageState, workspace_id)
        state.active_window_id = None
        state.lease_until = None
        state.lease_token = ""
        state.last_completed_at = now
        state.next_due_at = now + timedelta(seconds=interval_seconds)
        run = db.get(Run, window.run_id)
        run.status = "succeeded"
        run.finished_at = now
        run.result = {**(run_result or {}), "summary": summary}
        return True


def latest_verdicts(db: Session, workspace_id: str, signal_ids: list[str]) -> dict[str, TriageVerdict]:
    """One bounded bulk query; latest means completion time, not mutable signal time."""
    if not signal_ids:
        return {}
    from sqlalchemy import func
    ranked = select(TriageVerdict.id, func.row_number().over(
        partition_by=TriageVerdict.signal_id,
        order_by=(TriageVerdict.created_at.desc(), TriageVerdict.id.desc())).label("position")).where(
            TriageVerdict.workspace_id == workspace_id, TriageVerdict.signal_id.in_(signal_ids)).subquery()
    return {v.signal_id: v for v in db.scalars(select(TriageVerdict).join(
        ranked, ranked.c.id == TriageVerdict.id).where(ranked.c.position == 1))}


def override_history(db: Session, verdict_ids: list[str]) -> dict[str, list[TriageOverride]]:
    result: dict[str, list[TriageOverride]] = {}
    if verdict_ids:
        for override in db.scalars(select(TriageOverride).where(TriageOverride.verdict_id.in_(verdict_ids))
                                   .order_by(TriageOverride.created_at, TriageOverride.id)):
            result.setdefault(override.verdict_id, []).append(override)
    return result


def bind_proposal_outcome(db: Session, workspace_id: str, signal_id: str, outcome_id: str) -> str:
    """Bind admission in the caller's outcome-creation transaction, or fail closed.

    Reserve/lock with proposal_for_signal *before* creating an outcome. Roll back
    that transaction if this helper rejects a competing identity.
    """
    from .models import Outcome

    proposal = proposal_for_signal(db, workspace_id, signal_id)
    outcome = db.get(Outcome, outcome_id)
    if outcome is None or outcome.workspace_id != workspace_id:
        raise ValueError("unknown outcome")
    bound = db.execute(update(TriageProposal).where(TriageProposal.id == proposal.id,
        (TriageProposal.outcome_id.is_(None) | (TriageProposal.outcome_id == outcome_id)))
        .values(outcome_id=outcome_id, state="admitted"))
    if not bound.rowcount:
        raise ValueError("signal already admitted to another outcome")
    return proposal.id
