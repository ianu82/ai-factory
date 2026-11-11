"""Workspace goals and optional, fail-closed triage capability resolution.

Optional integrations register callables on app.state.triage_capabilities (or pass
TriageCapabilities to resolve_goals). Admission-level adapters MUST translate their
own levels to Admission; raw/unknown levels are denied, never interpreted as the
unrelated autonomy.plan/merge/release modes. None means no installed adapter.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Literal

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator, model_validator
from sqlalchemy.orm import Session

from .auth import current_user
from .db import Base, get_db
from .events import record, bus, notify
from .models import Goal, Membership, Repository, User

router = APIRouter(prefix="/api/workspaces/{workspace_id}")


class Admission(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)
    enabled: bool = False
    repo_ids: list[str] = Field(default_factory=list, max_length=200)
    # P0 is most urgent: admit only priorities >= this floor (0..3).
    max_urgency: int = Field(default=3, ge=0, le=3)


class GoalInput(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)
    name: str = Field(min_length=1, max_length=200)
    description: str = Field(min_length=1, max_length=4000)
    owner_id: str = Field(min_length=1, max_length=32)
    repo_ids: list[str] = Field(max_length=200)
    admission: Admission = Field(default_factory=Admission)
    workstream_id: str | None = Field(default=None, max_length=32)
    state: Literal["active", "archived"] = "active"

    @model_validator(mode="after")
    def active_scope(self) -> GoalInput:
        if self.state == "active" and not self.repo_ids:
            raise ValueError("Active goals require repositories")
        return self

    @field_validator("name", "description")
    @classmethod
    def nonblank(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("Must not be blank")
        return value.strip()


class GoalUpdate(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)
    name: str | None = None
    description: str | None = None
    owner_id: str | None = None
    repo_ids: list[str] | None = None
    admission: Admission | None = None
    workstream_id: str | None = None
    state: Literal["active", "archived"] | None = None


def _workstream_model():
    # Discover a registered model, never import an optional implementation.
    return next((m.class_ for m in Base.registry.mappers if m.class_.__name__ == "Workstream"), None)


def _references(db: Session, workspace_id: str, values: dict) -> None:
    if not db.query(Membership).filter_by(workspace_id=workspace_id, user_id=values["owner_id"]).first():
        raise HTTPException(422, "Owner must be a workspace member")
    ids = list(dict.fromkeys(values["repo_ids"]))
    count = db.query(Repository).filter(Repository.workspace_id == workspace_id, Repository.id.in_(ids)).count()
    if count != len(ids):
        raise HTTPException(422, "Repositories must belong to this workspace")
    values["repo_ids"] = ids
    if not set(values["admission"]["repo_ids"]).issubset(ids):
        raise HTTPException(422, "Admission repositories must be within the goal repositories")
    if values["workstream_id"] is not None:
        model = _workstream_model()
        if model is None or not db.query(model).filter_by(workspace_id=workspace_id, id=values["workstream_id"]).first():
            raise HTTPException(422, "Workstream must belong to this workspace")


def serialize(goal: Goal) -> dict:
    return {key: getattr(goal, key) for key in ("id", "workspace_id", "name", "description", "owner_id",
                                              "repo_ids", "admission", "workstream_id", "state")}


def _save(db: Session, goal: Goal, user: User, kind: str) -> dict:
    db.flush()
    result = serialize(goal)
    payload = record(goal.workspace_id, kind, f"Goal {goal.name}", actor=user.id,
                     data={"goal": result}, session=db, publish=False)
    db.commit()
    bus.publish({"type": "event", **payload})
    notify(goal.workspace_id, "goal", goal.id)
    return result


def remove_repository(db: Session, workspace_id: str, repo_id: str) -> None:
    """Prune both scopes in the disconnect transaction; empty goals are archived."""
    for goal in db.query(Goal).filter_by(workspace_id=workspace_id):
        if repo_id in goal.repo_ids:
            goal.repo_ids = [v for v in goal.repo_ids if v != repo_id]
            goal.admission = {**goal.admission, "repo_ids": [v for v in goal.admission.get("repo_ids", []) if v != repo_id]}
            if not goal.repo_ids:
                goal.state = "archived"
                goal.admission = {**goal.admission, "enabled": False}


@dataclass(frozen=True)
class TriageCapabilities:
    # Return bounded workspace-scoped stream dictionaries (id, state, goal,
    # name, owner_id, default_repo_ids). The resolver rechecks tenancy and state.
    workstreams: Callable[[Session, str], list[dict]] | None = None
    admission_policy: Callable[[Session, str, dict], object] | None = None


def _installed_workstreams(db: Session, workspace_id: str) -> list[dict]:
    model = _workstream_model()
    if model is None:
        return []
    rows = db.query(model).filter_by(workspace_id=workspace_id, state="active").order_by(model.id).limit(201).all()
    return [{key: getattr(row, key) for key in ("id", "workspace_id", "state", "goal", "name", "owner_id", "default_repo_ids")}
            for row in rows]


def effective_admission(goal: dict, policy: object = None, *, installed: bool = False) -> dict:
    """A level adapter overrides the toggle but can only narrow the goal scope.

    Invalid stored or installed policies deny admission. With an adapter installed,
    None is unknown (deny), not permission to fall back to a more permissive toggle.
    """
    try:
        chosen = Admission.model_validate(policy if installed else goal.get("admission", {}))
        scope = set(goal["repo_ids"])
        chosen.repo_ids = [v for v in dict.fromkeys(chosen.repo_ids) if v in scope]
        if not chosen.repo_ids:
            chosen.enabled = False
        return {**chosen.model_dump(), "source": "admission-level" if installed else "goal"}
    except (ValidationError, TypeError, ValueError):
        return {**Admission().model_dump(), "source": "admission-level" if installed else "goal", "invalid": True}


def resolve_goals(db: Session, workspace_id: str, capabilities: TriageCapabilities | None = None) -> dict:
    caps = capabilities or TriageCapabilities(workstreams=_installed_workstreams)
    try:
        streams = caps.workstreams(db, workspace_id) if caps.workstreams else []
        streams = [s for s in streams if s.get("workspace_id") == workspace_id and s.get("state") == "active"]
        if len(streams) > 200:
            raise ValueError("Too many active workstreams")
        candidates = []
        if streams:
            for stream in streams:
                # Historical goals must not consume the active candidate cap.
                attached = db.query(Goal).filter_by(workspace_id=workspace_id, workstream_id=stream["id"], state="active").order_by(Goal.id).limit(201).all()
                if len(attached) > 200:
                    raise ValueError("Too many attached goals")
                if attached:
                    candidates.extend(serialize(g) for g in attached)
                else:
                    candidates.append({"id": f"workstream:{stream['id']}", "workspace_id": workspace_id,
                                       "name": stream["name"], "description": stream["goal"],
                                       "owner_id": stream["owner_id"], "repo_ids": stream["default_repo_ids"],
                                       "admission": {}, "workstream_id": stream["id"], "state": "active"})
        else:
            # Attached goals in inactive streams cannot leak into the fallback.
            rows = db.query(Goal).filter_by(workspace_id=workspace_id, state="active", workstream_id=None).order_by(Goal.id).limit(201).all()
            candidates = [serialize(g) for g in rows]
        if len(candidates) > 200:
            raise ValueError("Too many active goals")
        valid_repos = {r.id for r in db.query(Repository.id).filter_by(workspace_id=workspace_id)}
        for goal in candidates:
            goal["repo_ids"] = [v for v in goal["repo_ids"] if v in valid_repos]
            policy = caps.admission_policy(db, workspace_id, goal) if caps.admission_policy else None
            goal["effective_admission"] = effective_admission(goal, policy, installed=caps.admission_policy is not None)
        return {"source": "workstream" if streams else "workspace", "goals": candidates}
    except Exception as exc:
        raise HTTPException(503, "Goal/policy integration unavailable; triage is disabled") from exc


@router.get("/triage/goals")
def effective_goals(workspace_id: str, request: Request, user: User = Depends(current_user), db: Session = Depends(get_db)):
    from .api import _ws
    _ws(db, user, workspace_id)
    return resolve_goals(db, workspace_id, getattr(request.app.state, "triage_capabilities", None))


@router.get("/goals")
def list_goals(workspace_id: str, state: Literal["active", "archived", "all"] = "active",
               limit: int = Query(100, ge=1, le=200), offset: int = Query(0, ge=0),
               user: User = Depends(current_user), db: Session = Depends(get_db)):
    from .api import _ws
    _ws(db, user, workspace_id)
    query = db.query(Goal).filter_by(workspace_id=workspace_id)
    if state != "all":
        query = query.filter_by(state=state)
    return [serialize(g) for g in query.order_by(Goal.created_at, Goal.id).offset(offset).limit(limit)]


@router.post("/goals")
def create_goal(workspace_id: str, body: GoalInput, user: User = Depends(current_user), db: Session = Depends(get_db)):
    from .api import _ws
    _ws(db, user, workspace_id, "admin")
    values = body.model_dump()
    _references(db, workspace_id, values)
    goal = Goal(workspace_id=workspace_id, **values)
    db.add(goal)
    return _save(db, goal, user, "goal_created")


@router.patch("/goals/{goal_id}")
def update_goal(workspace_id: str, goal_id: str, body: GoalUpdate, user: User = Depends(current_user), db: Session = Depends(get_db)):
    from .api import _ws
    _ws(db, user, workspace_id, "admin")
    goal = db.query(Goal).filter_by(workspace_id=workspace_id, id=goal_id).one_or_none()
    if goal is None:
        raise HTTPException(404, "Goal not found")
    values = {k: v for k, v in serialize(goal).items() if k in GoalInput.model_fields}
    values.update(body.model_dump(exclude_unset=True))
    # Disconnected goals may remain archived with empty scope, but reactivation
    # requires a valid repository selection.
    try:
        checked = GoalInput.model_validate(values).model_dump()
    except ValidationError:
        raise HTTPException(422, "Invalid goal fields") from None
    _references(db, workspace_id, checked)
    for key, value in checked.items():
        setattr(goal, key, value)
    return _save(db, goal, user, "goal_archived" if goal.state == "archived" else "goal_updated")
