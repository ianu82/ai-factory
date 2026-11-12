"""Workspace-scoped workstream validation, authorization and atomic audited mutations."""

from __future__ import annotations

import re
from typing import Literal

from fastapi import HTTPException
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from . import events, serializers as S
from .models import Membership, Repository, User, Workstream


class Overrides(BaseModel):
    model_config = ConfigDict(extra="forbid")

    worker_override: Literal["native", "codex", "anton"] | None = None
    plan_approval_override: Literal["auto", "approval"] | None = None
    merge_approval_override: Literal["auto", "approval"] | None = None
    priority_override: int | None = Field(default=None, ge=0, le=3, strict=True)
    outcome_budget_override: float | None = Field(default=None, gt=0, allow_inf_nan=False)


class WorkstreamUpdate(Overrides):
    name: str | None = Field(default=None, max_length=200)
    goal: str | None = Field(default=None, max_length=2000)
    owner_id: str | None = None
    default_repo_ids: list[str] | None = None
    state: Literal["active", "archived"] | None = None

    @field_validator("name", "goal")
    @classmethod
    def nonblank(cls, value: str | None) -> str | None:
        if value is not None:
            value = value.strip()
            if not value:
                raise ValueError("Must not be blank")
        return value

    @model_validator(mode="after")
    def required_values(self) -> WorkstreamUpdate:
        for key in ("name", "goal", "owner_id", "default_repo_ids", "state"):
            if key in self.model_fields_set and getattr(self, key) is None:
                raise ValueError(f"{key} cannot be null")
        return self


class WorkstreamCreate(WorkstreamUpdate):
    name: str = Field(min_length=1, max_length=200)
    goal: str = Field(min_length=1, max_length=2000)
    state: Literal["active"] = "active"


def get(db: Session, workspace_id: str, workstream_id: str) -> Workstream:
    stream = db.query(Workstream).filter_by(workspace_id=workspace_id, id=workstream_id).one_or_none()
    if stream is None:
        raise HTTPException(404, "Workstream not found")
    return stream


def _authorize(member: Membership, owner_id: str) -> None:
    if member.role in ("owner", "admin"):
        return
    if member.role == "viewer" or member.user_id != owner_id:
        raise HTTPException(403, "Only workspace admins or the workstream owner can manage this workstream")


def _references(db: Session, workspace_id: str, values: dict) -> None:
    if "owner_id" in values:
        owner = db.query(Membership).filter_by(workspace_id=workspace_id, user_id=values["owner_id"]).first()
        if owner is None:
            raise HTTPException(422, "Owner must be a workspace member")
    if "default_repo_ids" in values:
        ids = list(dict.fromkeys(values["default_repo_ids"]))
        count = db.query(Repository).filter(Repository.workspace_id == workspace_id, Repository.id.in_(ids)).count()
        if count != len(ids):
            raise HTTPException(422, "Default repositories must belong to this workspace")
        values["default_repo_ids"] = ids


def _record_change(db: Session, stream: Workstream, user: User, kind: str, before: dict | None) -> dict:
    db.flush()
    return events.record(stream.workspace_id, kind, f"Workstream {stream.name}", actor=user.id,
                         data={"workstream_id": stream.id, "before": before, "after": S.workstream(stream)},
                         session=db, publish=False)


def publish_changes(payloads: list[dict]) -> None:
    """Publish audit events and invalidations only after the caller successfully commits."""
    for payload in payloads:
        events.bus.publish({"type": "event", **payload})
        events.notify(payload["workspace_id"], "workstream", payload["data"]["workstream_id"])


def remove_repository_defaults(db: Session, workspace_id: str, repo_id: str, user: User) -> list[dict]:
    """Prune JSON references, including archived streams, in the disconnect transaction.

    The caller owns the commit and must publish the returned events afterwards.
    """
    payloads = []
    for stream in db.query(Workstream).filter_by(workspace_id=workspace_id):
        if repo_id not in stream.default_repo_ids:
            continue
        before = S.workstream(stream)
        stream.default_repo_ids = [value for value in stream.default_repo_ids if value != repo_id]
        payloads.append(_record_change(db, stream, user, "workstream_updated", before))
    return payloads


def _commit(db: Session, stream: Workstream, user: User, kind: str, before: dict | None) -> dict:
    try:
        payload = _record_change(db, stream, user, kind, before)
        after = S.workstream(stream)
        db.commit()
    except Exception:
        db.rollback()
        raise
    # Neither the audit event nor the invalidation may escape a failed transaction.
    publish_changes([payload])
    return after


def create(db: Session, workspace_id: str, member: Membership, user: User, body: WorkstreamCreate) -> dict:
    values = body.model_dump(exclude_unset=True)
    values.setdefault("owner_id", user.id)
    _authorize(member, values["owner_id"])
    _references(db, workspace_id, values)
    base = re.sub(r"[^a-z0-9]+", "-", body.name.lower()).strip("-")[:60] or "workstream"
    # sqlite legacy transaction control does not BEGIN for SELECT or SAVEPOINT.
    # Ensure releasing the insertion savepoint cannot commit ahead of the audit row.
    connection = db.connection()
    if connection.dialect.name == "sqlite" and not connection.connection.driver_connection.in_transaction:
        connection.exec_driver_sql("BEGIN IMMEDIATE")
    n = 1
    while True:
        slug = base if n == 1 else f"{base}-{n}"
        if db.query(Workstream.id).filter_by(workspace_id=workspace_id, slug=slug).first():
            n += 1
            continue
        stream = Workstream(workspace_id=workspace_id, slug=slug, **values)
        try:
            # The unique constraint also arbitrates concurrent creators, not only sequential requests.
            with db.begin_nested():
                db.add(stream)
                db.flush()
            break
        except IntegrityError:
            if not db.query(Workstream.id).filter_by(workspace_id=workspace_id, slug=slug).first():
                raise
            n += 1
    return _commit(db, stream, user, "workstream_created", None)


def update(db: Session, stream: Workstream, member: Membership, user: User, body: WorkstreamUpdate) -> dict:
    from .onboarding_policy import lock_workspace

    lock_workspace(db, stream.workspace_id)
    db.refresh(stream)
    _authorize(member, stream.owner_id)
    values = body.model_dump(exclude_unset=True)
    # Members cannot hand their streams to someone else to bypass ownership controls.
    if "owner_id" in values:
        _authorize(member, values["owner_id"])
    _references(db, stream.workspace_id, values)
    before = S.workstream(stream)
    kind = "workstream_archived" if values.get("state") == "archived" and stream.state != "archived" else "workstream_updated"
    for key, value in values.items():
        setattr(stream, key, value)
    return _commit(db, stream, user, kind, before)
