"""Durable tracing contracts, with no network calls or implicit commits.

Callers own the transaction (normally session_scope). Helpers serialize on the
workspace row, including first insert, on both SQLite and Postgres. Never hold
this lock across agent/provider calls. Naive dates are legacy UTC; wire dates
are ISO UTC. An episode is eligible while open and ingestion > processed.

Observation ids identify *source events*, not ingestion attempts. Without one,
the canonical normalized payload is the replay key. Sources which can emit two
identical reopen events must supply distinct observation ids.
"""
from __future__ import annotations

import hashlib
import json
import math
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator
from sqlalchemy import update
from sqlalchemy.orm import Session

from .db import utcnow
from .models import (
    Decision, ErrorObservation, ErrorRecord, Outcome, Repository, SuspectedCause,
    Task, TraceRun, TraceState, Workspace, new_id,
)


def timestamp(value: datetime) -> datetime:
    return value.astimezone(UTC).replace(tzinfo=None) if value.tzinfo else value


def _iso(value: datetime | None) -> str | None:
    return timestamp(value).isoformat() + "Z" if value is not None else None


class Contract(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class StackFrame(Contract):
    file: str = Field(min_length=1, max_length=2000)
    line: int = Field(gt=0, strict=True)
    function: str = Field(default="", max_length=500)

    @field_validator("file")
    @classmethod
    def nonblank(cls, value: str) -> str:
        if not value.strip() or "\x00" in value:
            raise ValueError("Frame file must be non-empty and contain no NUL")
        return value


class ErrorInput(Contract):
    workspace_id: str = Field(min_length=1, max_length=32)
    repo_id: str = Field(min_length=1, max_length=32)
    source: str = Field(min_length=1, max_length=40)
    external_id: str = Field(min_length=1, max_length=200)
    title: str = Field(min_length=1, max_length=300)
    body: str = Field(default="", max_length=65536)
    stack_frames: list[StackFrame] = Field(default_factory=list, max_length=200)
    first_seen_at: datetime
    last_seen_at: datetime
    release: str | None = Field(default=None, min_length=1, max_length=200)
    commit_sha: str | None = Field(default=None, min_length=1, max_length=64)
    lifecycle: Literal["open", "resolved"] = "open"
    regressed: bool = False
    observation_id: str | None = Field(default=None, min_length=1, max_length=200)

    @field_validator("source", "external_id", "title")
    @classmethod
    def nonblank(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("Value must not be blank")
        return value

    @field_validator("first_seen_at", "last_seen_at")
    @classmethod
    def utc(cls, value: datetime) -> datetime:
        return timestamp(value)

    @model_validator(mode="after")
    def dates(self) -> ErrorInput:
        if self.last_seen_at < self.first_seen_at:
            raise ValueError("last_seen_at must not precede first_seen_at")
        if self.regressed and self.lifecycle != "open":
            raise ValueError("A regression must be open")
        return self


class CauseInput(Contract):
    repo_id: str = Field(min_length=1, max_length=32)
    pr_number: int = Field(gt=0, strict=True)
    merge_sha: str = Field(min_length=1, max_length=64)
    confidence: float = Field(ge=0, le=1, allow_inf_nan=False)
    evidence: dict[str, Any] = Field(default_factory=dict)
    task_id: str | None = None
    outcome_id: str | None = None


class TraceRunResult(Contract):
    status: Literal["succeeded", "failed", "cancelled", "error", "interrupted"]
    turns: int = Field(default=0, ge=0, strict=True)
    input_tokens: int = Field(default=0, ge=0, strict=True)
    output_tokens: int = Field(default=0, ge=0, strict=True)
    cost_usd: float = Field(default=0, ge=0, allow_inf_nan=False)
    cost_known: bool = False
    result: dict[str, Any] = Field(default_factory=dict)
    error: str = Field(default="", max_length=65536)
    finished_at: datetime

    @field_validator("finished_at")
    @classmethod
    def utc(cls, value: datetime) -> datetime:
        return timestamp(value)


@dataclass(frozen=True)
class ObservationResult:
    """Stable receipt: replay returns the original episode/revision, even later."""

    error_id: str
    episode: int
    revision: int


def _json(value: Any) -> str:
    # Bound collections/strings at ingress; also reject NaN and non-JSON evidence.
    def check(item: Any, depth: int = 0) -> None:
        if depth > 20:
            raise ValueError("Evidence nesting exceeds 20")
        if isinstance(item, dict):
            if len(item) > 1000 or any(not isinstance(k, str) or len(k) > 2000 for k in item):
                raise ValueError("Evidence object exceeds limits")
            for child in item.values():
                check(child, depth + 1)
        elif isinstance(item, (list, tuple)):
            if len(item) > 1000:
                raise ValueError("Evidence array exceeds 1000 entries")
            for child in item:
                check(child, depth + 1)
        elif isinstance(item, str) and len(item) > 65536:
            raise ValueError("Evidence string exceeds 65536 characters")
    check(value)
    encoded = json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)
    if len(encoded.encode()) > 262144:
        raise ValueError("Evidence exceeds 256 KiB")
    return encoded


def _hash(value: Any) -> str:
    return hashlib.sha256(_json(value).encode()).hexdigest()


def _lock(db: Session, workspace_id: str) -> None:
    # UPDATE obtains the SQLite writer lock as well as a Postgres row lock.
    result = db.execute(update(Workspace).where(Workspace.id == workspace_id)
                        .values(id=Workspace.id))
    if result.rowcount != 1:
        raise ValueError("Workspace not found")


def _repo(db: Session, workspace_id: str, repo_id: str) -> Repository:
    repo = db.get(Repository, repo_id, populate_existing=True)
    if repo is None or repo.workspace_id != workspace_id:
        raise ValueError("Repository does not belong to workspace")
    return repo


def _error(db: Session, workspace_id: str, error_id: str) -> ErrorRecord:
    row = db.get(ErrorRecord, error_id, populate_existing=True)
    if row is None or row.workspace_id != workspace_id:
        raise ValueError("Error does not belong to workspace")
    _repo(db, workspace_id, row.repo_id)
    return row


def _context(db: Session, row: ErrorRecord, task_id: str | None, outcome_id: str | None) -> str | None:
    if task_id is not None:
        task = db.get(Task, task_id)
        if task is None or task.workspace_id != row.workspace_id or task.repo_id != row.repo_id:
            raise ValueError("Task does not belong to repository/workspace")
        if outcome_id is not None and task.outcome_id != outcome_id:
            raise ValueError("Outcome does not belong to task")
        outcome_id = task.outcome_id
    if outcome_id is not None:
        outcome = db.get(Outcome, outcome_id)
        if (outcome is None or outcome.workspace_id != row.workspace_id
                or row.repo_id not in (outcome.repo_ids or [])):
            raise ValueError("Outcome does not belong to repository/workspace")
    return outcome_id


def eligible(row: ErrorRecord) -> bool:
    return row.lifecycle == "open" and row.ingestion_revision > row.processed_revision


def upsert_error(db: Session, observation: ErrorInput) -> ObservationResult:
    """Duplicate sightings only advance last-seen. Evidence changes advance revision.

    A reopen or new evidence after confirmation starts a separate episode. A
    resolution always closes the current episode, but cannot change its frozen
    evidence or attribution. Stale observations cannot reopen/resolve newer ones.
    """
    data = observation.model_dump(mode="json")
    evidence = {key: data[key] for key in ("title", "body", "stack_frames", "release", "commit_sha")}
    fingerprint = _hash(evidence)
    key = _hash({"event": observation.observation_id}) if observation.observation_id else _hash(data)
    _lock(db, observation.workspace_id)
    _repo(db, observation.workspace_id, observation.repo_id)
    scope = dict(workspace_id=observation.workspace_id, repo_id=observation.repo_id,
                 source=observation.source, external_id=observation.external_id)
    receipt = db.query(ErrorObservation).filter_by(**scope, observation_key=key).first()
    if receipt:
        row = db.get(ErrorRecord, receipt.error_id)
        return ObservationResult(row.id, row.episode, receipt.revision)
    row = (db.query(ErrorRecord).filter_by(**scope).order_by(ErrorRecord.episode.desc())
           .populate_existing().first())
    episode = 1
    if row is not None:
        stale = observation.last_seen_at < row.last_seen_at
        changed = fingerprint != row.evidence_fingerprint
        confirmed = db.query(SuspectedCause.id).filter_by(error_id=row.id, disposition="confirmed").first() is not None
        reopen = observation.regressed or (row.lifecycle == "resolved" and observation.lifecycle == "open")
        if not stale and observation.lifecycle == "open" and (reopen or (confirmed and changed)):
            episode = row.episode + 1
            # Closing is lifecycle bookkeeping, never an evidence/review rewrite.
            row.lifecycle = "resolved"
            row = None
        else:
            row.last_seen_at = max(row.last_seen_at, observation.last_seen_at)
            if not stale:
                row.lifecycle = observation.lifecycle
                if changed and not confirmed:
                    for name, value in evidence.items():
                        setattr(row, name, value)
                    row.evidence_fingerprint = fingerprint
                    row.ingestion_revision += 1
            row.updated_at = utcnow()
    if row is None:
        row = ErrorRecord(**scope, episode=episode, **evidence,
                          first_seen_at=observation.first_seen_at, last_seen_at=observation.last_seen_at,
                          lifecycle=observation.lifecycle, evidence_fingerprint=fingerprint)
        db.add(row)
        db.flush()
    db.add(ErrorObservation(**scope, observation_key=key, error_id=row.id, revision=row.ingestion_revision))
    db.flush()
    return ObservationResult(row.id, row.episode, row.ingestion_revision)


def mark_processed(db: Session, workspace_id: str, error_id: str, revision: int) -> bool:
    """An obsolete reviewer must not consume a newer ingestion revision."""
    _lock(db, workspace_id)
    row = _error(db, workspace_id, error_id)
    if revision != row.ingestion_revision:
        return False
    row.processed_revision = revision
    db.flush()
    return True


def upsert_cause(db: Session, workspace_id: str, error_id: str, revision: int,
                 candidate: CauseInput) -> SuspectedCause:
    evidence = json.loads(_json(candidate.evidence))
    _lock(db, workspace_id)
    row = _error(db, workspace_id, error_id)
    if row.repo_id != candidate.repo_id:
        raise ValueError("Candidate repository does not belong to error")
    outcome_id = _context(db, row, candidate.task_id, candidate.outcome_id)
    cause = db.query(SuspectedCause).filter_by(error_id=error_id, repo_id=candidate.repo_id,
                                             pr_number=candidate.pr_number, merge_sha=candidate.merge_sha).first()
    if cause is not None:
        # Unreviewed candidates may be refreshed for a new evidence revision;
        # reviewed evidence/dispositions remain precedents, never ingestion edits.
        if (cause.disposition == "pending" and revision == row.ingestion_revision
                and eligible(row) and revision > cause.error_revision):
            cause.error_revision, cause.confidence, cause.evidence = revision, candidate.confidence, evidence
            cause.updated_at = utcnow()
            db.flush()
        return cause
    if revision != row.ingestion_revision or not eligible(row):
        raise ValueError("Error revision is not eligible")
    cause = SuspectedCause(workspace_id=workspace_id, error_id=error_id, error_revision=revision,
                           **candidate.model_dump(exclude={"outcome_id", "evidence"}),
                           outcome_id=outcome_id, evidence=evidence)
    db.add(cause)
    db.flush()
    return cause


def review_cause(db: Session, workspace_id: str, cause_id: str, *,
                 disposition: Literal["confirmed", "rejected", "uncertain"], reason: str,
                 decision_id: str | None = None) -> SuspectedCause:
    if disposition not in {"confirmed", "rejected", "uncertain"} or not reason.strip() or len(reason) > 65536:
        raise ValueError("Review requires a disposition and a bounded reason")
    _lock(db, workspace_id)
    cause = db.get(SuspectedCause, cause_id, populate_existing=True)
    if cause is None or cause.workspace_id != workspace_id:
        raise ValueError("Cause does not belong to workspace")
    if decision_id is not None:
        decision = db.get(Decision, decision_id)
        if decision is None or decision.workspace_id != workspace_id:
            raise ValueError("Decision does not belong to workspace")
    if cause.disposition in {"confirmed", "rejected"}:
        if (cause.disposition, cause.reason, cause.decision_id) != (disposition, reason, decision_id):
            raise ValueError("Historical review is immutable")
        return cause
    row = _error(db, workspace_id, cause.error_id)
    if row.ingestion_revision != cause.error_revision or row.lifecycle != "open":
        raise ValueError("Review is for an obsolete error revision")
    cause.disposition, cause.reason, cause.decision_id = disposition, reason, decision_id
    cause.updated_at = utcnow()
    db.flush()
    return cause


def create_trace_run(db: Session, workspace_id: str, error_id: str, revision: int, *,
                     cycle_id: str, provider: str = "", model: str = "", task_id: str | None = None,
                     outcome_id: str | None = None, started_at: datetime | None = None) -> TraceRun:
    for name, value, limit in (("cycle_id", cycle_id, 64), ("provider", provider, 20), ("model", model, 100)):
        if len(value) > limit or (name == "cycle_id" and not value.strip()):
            raise ValueError(f"Invalid {name}")
    _lock(db, workspace_id)
    row = _error(db, workspace_id, error_id)
    if revision != row.ingestion_revision or not eligible(row):
        raise ValueError("Error revision is not eligible")
    outcome_id = _context(db, row, task_id, outcome_id)
    run = TraceRun(workspace_id=workspace_id, error_id=error_id, error_revision=revision,
                   cycle_id=cycle_id, provider=provider, model=model, task_id=task_id, outcome_id=outcome_id,
                   started_at=timestamp(started_at) if started_at else utcnow())
    db.add(run)
    db.flush()
    return run


def finish_trace_run(db: Session, workspace_id: str, run_id: str, result: TraceRunResult) -> TraceRun:
    """Persist returned usage even for failed sessions; terminal results are replay-safe."""
    payload = result.model_dump()
    payload["result"] = json.loads(_json(result.result))
    _lock(db, workspace_id)
    run = db.get(TraceRun, run_id, populate_existing=True)
    if run is None or run.workspace_id != workspace_id:
        raise ValueError("Trace run does not belong to workspace")
    if result.finished_at < run.started_at:
        raise ValueError("finished_at must not precede started_at")
    if run.status != "running":
        if any(getattr(run, key) != value for key, value in payload.items()):
            raise ValueError("Terminal trace result is immutable")
        return run
    for key, value in payload.items():
        setattr(run, key, value)
    db.flush()
    return run


def acquire_lease(db: Session, workspace_id: str, *, owner: str, cycle_id: str,
                  now: datetime, ttl: timedelta, reserve_usd: float = 0) -> TraceState | None:
    """Expiry recovers the same unfinished cycle and its reservation, not a new night.

    The opaque lease token fences an expired worker, including reuse of owner ids.
    A new cycle reserves once; recovery never adds the reservation a second time.
    """
    if (not owner.strip() or len(owner) > 64 or not cycle_id.strip() or len(cycle_id) > 64
            or not math.isfinite(reserve_usd) or reserve_usd < 0 or ttl <= timedelta(0)):
        raise ValueError("Invalid lease or reservation")
    now = timestamp(now)
    _lock(db, workspace_id)
    state = db.get(TraceState, workspace_id, populate_existing=True)
    if state is None:
        state = TraceState(workspace_id=workspace_id)
        db.add(state)
        db.flush()
    if state.lease_expires_at is not None and state.lease_expires_at > now:
        return None
    if state.cycle_id is None:
        if state.completed_cutoff is not None and now <= state.completed_cutoff:
            raise ValueError("Cycle cutoff must advance")
        state.cycle_id, state.cycle_at, state.cycle_cutoff = cycle_id, now, now
        state.reserved_usd, state.spent_usd = reserve_usd, 0
    state.lease_owner, state.lease_token, state.lease_expires_at = owner, new_id(), now + ttl
    db.flush()
    return state


def complete_cycle(db: Session, workspace_id: str, *, token: str, now: datetime) -> bool:
    _lock(db, workspace_id)
    state = db.get(TraceState, workspace_id, populate_existing=True)
    if (state is None or state.lease_token != token or state.lease_expires_at is None
            or state.lease_expires_at <= timestamp(now)):
        return False
    state.completed_cutoff = state.cycle_cutoff
    state.cycle_id = state.cycle_at = state.cycle_cutoff = None
    state.lease_owner = state.lease_token = state.lease_expires_at = None
    state.reserved_usd = 0
    db.flush()
    return True


def cause_dict(row: SuspectedCause) -> dict[str, Any]:
    return {name: getattr(row, name) for name in (
        "id", "error_id", "error_revision", "repo_id", "pr_number", "merge_sha", "task_id", "outcome_id",
        "confidence", "evidence", "disposition", "reason", "decision_id", "side_effect_progress",
    )} | {"created_at": _iso(row.created_at), "updated_at": _iso(row.updated_at)}


def error_dict(row: ErrorRecord) -> dict[str, Any]:
    return {name: getattr(row, name) for name in (
        "id", "workspace_id", "repo_id", "source", "external_id", "episode", "title", "body", "stack_frames",
        "release", "commit_sha", "lifecycle", "ingestion_revision", "processed_revision",
    )} | {"first_seen_at": _iso(row.first_seen_at), "last_seen_at": _iso(row.last_seen_at),
         "created_at": _iso(row.created_at), "updated_at": _iso(row.updated_at), "eligible": eligible(row)}


def production_errors(db: Session, workspace_id: str, *, limit: int = 50, offset: int = 0,
                      repo_id: str | None = None) -> dict[str, Any]:
    """Shared list contract: bounded pagination, all episodes, no cross-workspace rows.

    HTTP adapters must check workspace membership before invoking these helpers.
    """
    if not 1 <= limit <= 100 or offset < 0:
        raise ValueError("Invalid pagination")
    if repo_id is not None:
        _repo(db, workspace_id, repo_id)
    query = db.query(ErrorRecord).filter_by(workspace_id=workspace_id)
    if repo_id is not None:
        query = query.filter_by(repo_id=repo_id)
    rows = query.order_by(ErrorRecord.last_seen_at.desc(), ErrorRecord.id).offset(offset).limit(limit + 1).all()
    return {"items": [error_dict(row) for row in rows[:limit]],
            "next_offset": offset + limit if len(rows) > limit else None}


def production_error_detail(db: Session, workspace_id: str, error_id: str, *,
                            candidate_limit: int = 50, candidate_offset: int = 0) -> dict[str, Any]:
    if not 1 <= candidate_limit <= 100 or candidate_offset < 0:
        raise ValueError("Invalid pagination")
    row = _error(db, workspace_id, error_id)
    causes = (db.query(SuspectedCause).filter_by(workspace_id=workspace_id, error_id=row.id)
              .order_by(SuspectedCause.confidence.desc(), SuspectedCause.id)
              .offset(candidate_offset).limit(candidate_limit + 1).all())
    return error_dict(row) | {"candidates": [cause_dict(c) for c in causes[:candidate_limit]],
                              "next_candidate_offset": candidate_offset + candidate_limit
                              if len(causes) > candidate_limit else None}


def trace_run_dict(row: TraceRun) -> dict[str, Any]:
    """Runs row contract; no fabricated outcome or ordinary Run foreign key."""
    return {name: getattr(row, name) for name in (
        "id", "task_id", "outcome_id", "harness", "provider", "model", "attempt", "status", "turns",
        "input_tokens", "output_tokens", "cost_usd", "cost_known", "error", "result",
        "error_id", "error_revision", "cycle_id",
    )} | {"role": "tracer", "config_version": None, "started_at": _iso(row.started_at),
         "finished_at": _iso(row.finished_at), "judged_at": _iso(row.finished_at),
         "outcome_number": None, "outcome_title": "", "task_key": None}
