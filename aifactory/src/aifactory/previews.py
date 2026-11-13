"""Preview persistence contracts. No VM calls, agent sessions or delivery work here."""
from __future__ import annotations

from datetime import timedelta
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy import JSON, case, column, func, literal, or_, select, update
from sqlalchemy.orm import Session

from . import events
from .auth import current_user, require_member
from .bootstrap import BOOTSTRAP_MARKER
from .db import get_db, utcnow
from .models import (Outcome, Preview, PreviewPolicy, Repository, Task, User,
                     Workspace, WorkspacePreviewPolicy)

router = APIRouter(prefix="/api/workspaces/{workspace_id}/previews")
STATES = frozenset({"queued", "waking", "starting", "running", "asleep", "stopped", "failed"})
ACTIVE_STATES = frozenset({"starting", "waking", "running"})
PositiveInt = Annotated[int, Field(strict=True, ge=1, le=2147483647)]


class FactoryPolicyUpdate(BaseModel):
    model_config = ConfigDict(extra="forbid")
    active_cap: PositiveInt = 5
    idle_minutes: PositiveInt = 30
    lifetime_hours: Annotated[int, Field(strict=True, ge=1, le=8760)] = 72


class LocalPolicyUpdate(BaseModel):
    model_config = ConfigDict(extra="forbid")
    idle_minutes: PositiveInt | None = None
    lifetime_hours: PositiveInt | None = None


def factory_policy(db: Session, *, lock: bool = False) -> PreviewPolicy:
    if lock:
        # Both supported engines implement ON CONFLICT. Insert first so concurrent first
        # writers cannot race creating the singleton; UPDATE also locks on SQLite.
        if db.bind.dialect.name == "postgresql":
            from sqlalchemy.dialects.postgresql import insert
        else:
            from sqlalchemy.dialects.sqlite import insert
        db.execute(insert(PreviewPolicy).values(id=1, active_cap=5, idle_minutes=30,
                                               lifetime_hours=72).on_conflict_do_nothing())
        db.execute(update(PreviewPolicy).where(PreviewPolicy.id == 1).values(id=1))
        return db.scalars(select(PreviewPolicy).execution_options(populate_existing=True)).one()
    return db.get(PreviewPolicy, 1) or PreviewPolicy(id=1, active_cap=5, idle_minutes=30, lifetime_hours=72)


def effective_policy(db: Session, workspace_id: str) -> dict:
    global_policy = factory_policy(db)
    local = db.get(WorkspacePreviewPolicy, workspace_id)
    return {"active_cap": global_policy.active_cap,
            "idle_minutes": min(global_policy.idle_minutes, local.idle_minutes or global_policy.idle_minutes)
            if local else global_policy.idle_minutes,
            "lifetime_hours": min(global_policy.lifetime_hours, local.lifetime_hours or global_policy.lifetime_hours)
            if local else global_policy.lifetime_hours}


class Source(BaseModel):
    model_config = ConfigDict(extra="forbid")
    repository_id: Annotated[str, Field(min_length=1, max_length=32)]
    commit_sha: Annotated[str, Field(pattern=r"^(?:[0-9a-f]{40}|[0-9a-f]{64})$")]


class PreviewRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    repository_id: Annotated[str, Field(min_length=1, max_length=32)] | None = None
    pr_number: PositiveInt | None = None
    outcome_id: Annotated[str, Field(min_length=1, max_length=32)] | None = None
    task_id: Annotated[str, Field(min_length=1, max_length=32)] | None = None
    desired_manifest: Annotated[list[Source], Field(min_length=1, max_length=100)]


def request_preview(db: Session, workspace_id: str, body: PreviewRequest) -> Preview:
    """Caller authorizes membership and commits. Serialize before reading identity.

    Duplicate requests return the original identity, without changing source or expiry.
    Source refresh and waking are separate reconciler operations in later tasks.
    """
    is_pr = body.repository_id is not None and body.pr_number is not None and body.outcome_id is None
    is_outcome = body.repository_id is None and body.pr_number is None and body.outcome_id is not None
    if not (is_pr or is_outcome):
        raise HTTPException(422, "Specify a repository and PR, or an outcome")
    if not db.execute(update(Workspace).where(Workspace.id == workspace_id).values(id=workspace_id)).rowcount:
        raise HTTPException(404, "Workspace not found")
    for repo_id in {s.repository_id for s in body.desired_manifest} | ({body.repository_id} if is_pr else set()):
        repo = db.get(Repository, repo_id)
        if repo is None or repo.workspace_id != workspace_id:
            raise HTTPException(404, "Repository not found")
    if len({s.repository_id for s in body.desired_manifest}) != len(body.desired_manifest):
        raise HTTPException(422, "Duplicate manifest repository")
    if is_pr and {s.repository_id for s in body.desired_manifest} != {body.repository_id}:
        raise HTTPException(422, "PR manifest must contain only its repository")
    if is_outcome:
        outcome = db.get(Outcome, body.outcome_id)
        if outcome is None or outcome.workspace_id != workspace_id:
            raise HTTPException(404, "Outcome not found")
        if not {s.repository_id for s in body.desired_manifest}.issubset(set(outcome.repo_ids)):
            raise HTTPException(422, "Manifest repositories must belong to the outcome")
    if body.task_id:
        task = db.get(Task, body.task_id)
        if task is None or task.workspace_id != workspace_id:
            raise HTTPException(404, "Task not found")
        if (is_pr and (task.repo_id != body.repository_id or task.pr_number != body.pr_number)
                or is_outcome and task.outcome_id != body.outcome_id):
            raise HTTPException(422, "Task does not match preview identity")
    query = select(Preview).where(Preview.workspace_id == workspace_id)
    query = query.where(Preview.repository_id == body.repository_id, Preview.pr_number == body.pr_number) if is_pr else query.where(Preview.outcome_id == body.outcome_id)
    existing = db.scalars(query).one_or_none()
    if existing:
        return existing
    now = utcnow()
    preview = Preview(workspace_id=workspace_id, repository_id=body.repository_id, pr_number=body.pr_number,
                      outcome_id=body.outcome_id, task_id=body.task_id,
                      desired_manifest=[s.model_dump() for s in body.desired_manifest],
                      created_at=now, expires_at=now + timedelta(hours=effective_policy(db, workspace_id)["lifetime_hours"]))
    db.add(preview)
    db.flush()
    return preview


def guard_repository_disconnect(db: Session, workspace_id: str, repository_id: str) -> None:
    """Retain PR and outcome source references, including terminal previews.

    Caller holds the workspace lock shared with request_preview until disconnect commits.
    Match manifests in SQL so both the diagnostic and rows loaded remain bounded.
    Source refresh writers must also take this lock before validating repositories.
    """
    def manifest_references(manifest):
        # JSON (not JSONB) on PostgreSQL; SQLite exposes each array entry as JSON text.
        # running_manifest may be SQL NULL or JSON null before the first allocation.
        if db.bind.dialect.name == "postgresql":
            array = case((func.json_typeof(manifest) == "array", manifest), else_=literal([], type_=JSON))
            entries = func.json_array_elements(array).table_valued(column("value", JSON))
            repository = entries.c.value["repository_id"].as_string()
        else:
            entries = func.json_each(manifest).table_valued("value")
            repository = func.json_extract(entries.c.value, "$.repository_id")
        return select(1).select_from(entries).where(repository == repository_id).exists()

    references = db.execute(
        select(Preview.id, Preview.pr_number, Preview.outcome_id, Preview.state)
        .where(Preview.workspace_id == workspace_id, or_(
            Preview.repository_id == repository_id,
            Preview.outcome_id.is_not(None) & (
                manifest_references(Preview.desired_manifest) | manifest_references(Preview.running_manifest))))
        .order_by(Preview.id).limit(21)
    ).all()
    if references:
        summary = ", ".join(
            f"{row.id} ({'PR #' + str(row.pr_number) if row.pr_number else 'outcome ' + row.outcome_id}, {row.state})"
            for row in references[:20])
        if len(references) > 20:
            summary += ", and more"
        raise HTTPException(409, f"Repository is referenced by retained previews: {summary}. "
                            "Stopping a preview does not remove its durable identity or usage history.")


@router.get("/policy")
def read_policy(workspace_id: str, user: User = Depends(current_user), db: Session = Depends(get_db)):
    require_member(db, user, workspace_id)
    policy = factory_policy(db)
    local = db.get(WorkspacePreviewPolicy, workspace_id)
    return {"factory": {k: getattr(policy, k) for k in ("active_cap", "idle_minutes", "lifetime_hours")},
            "overrides": {k: getattr(local, k) if local else None for k in ("idle_minutes", "lifetime_hours")},
            "effective": effective_policy(db, workspace_id)}


@router.put("/policy")
def update_local_policy(workspace_id: str, body: LocalPolicyUpdate, user: User = Depends(current_user),
                        db: Session = Depends(get_db)):
    require_member(db, user, workspace_id, "admin")
    policy = factory_policy(db, lock=True)
    for key, value in body.model_dump().items():
        if value is not None and value > getattr(policy, key):
            raise HTTPException(422, f"{key} exceeds the Factory limit")
    local = db.get(WorkspacePreviewPolicy, workspace_id)
    if local is None:
        local = WorkspacePreviewPolicy(workspace_id=workspace_id)
        db.add(local)
    for key, value in body.model_dump().items():
        setattr(local, key, value)
    events.record(workspace_id, "preview_policy_changed", actor=user.id,
                  data={"scope": "workspace", **body.model_dump()}, session=db)
    db.commit()
    return read_policy(workspace_id, user, db)


@router.put("/policy/factory")
def update_factory_policy(workspace_id: str, body: FactoryPolicyUpdate, user: User = Depends(current_user),
                          db: Session = Depends(get_db)):
    require_member(db, user, workspace_id, "admin")
    ws = db.get(Workspace, workspace_id)
    if ws is None or (ws.settings or {}).get(BOOTSTRAP_MARKER) is not True:
        raise HTTPException(403, "Only bootstrap workspace administrators may change Factory preview policy")
    policy = factory_policy(db, lock=True)
    for key, value in body.model_dump().items():
        setattr(policy, key, value)
    events.record(workspace_id, "preview_policy_changed", actor=user.id,
                  data={"scope": "factory", **body.model_dump()}, session=db)
    db.commit()
    return read_policy(workspace_id, user, db)


@router.get("/{preview_id}")
def read_preview(workspace_id: str, preview_id: str, user: User = Depends(current_user),
                 db: Session = Depends(get_db)):
    require_member(db, user, workspace_id)
    preview = db.get(Preview, preview_id)
    if preview is None or preview.workspace_id != workspace_id:
        raise HTTPException(404, "Preview not found")
    # Internal VM/lease identifiers are intentionally not exposed to workspace clients.
    return {key: getattr(preview, key) for key in (
        "id", "workspace_id", "repository_id", "pr_number", "outcome_id", "task_id", "desired_manifest",
        "running_manifest", "state", "reason", "generation", "expires_at", "created_at", "updated_at",
        "state_changed_at", "last_human_visit_at", "started_at", "stopped_at", "destroyed_at")}
