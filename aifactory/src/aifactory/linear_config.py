"""Typed, workspace-scoped Linear configuration and provider-backed validation."""

from __future__ import annotations

from typing import Annotated, Any

from pydantic import BaseModel, ConfigDict, Field, StringConstraints, model_validator
from sqlalchemy.orm import Session

from .events import record
from .linear import Linear, LinearProvider, resolve_token
from .models import Repository, Secret, Workspace
from .pipeline.runtime import merged_settings
from .preview_config import redaction_values
from .security import redact

Identifier = Annotated[str, StringConstraints(strict=True, strip_whitespace=True, min_length=1, max_length=200)]


class LinearStates(BaseModel):
    model_config = ConfigDict(extra="forbid")

    queued: Identifier | None = None
    building: Identifier | None = None
    verifying: Identifier | None = None
    merging: Identifier | None = None
    done: Identifier | None = None
    blocked: Identifier | None = None


class LinearConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    enabled: bool = Field(default=False, strict=True)
    team_id: Identifier | None = None
    intake_label: Identifier = "factory-auto"
    repo_ids: list[Identifier] = Field(default_factory=list)
    states: LinearStates = Field(default_factory=LinearStates)

    @model_validator(mode="after")
    def required_when_enabled(self) -> LinearConfig:
        if self.enabled and (not self.team_id or not self.repo_ids or not all(self.states.model_dump().values())):
            raise ValueError("Enabling Linear requires a team, repositories and all six state mappings")
        if any(self.states.model_dump().values()) and not self.team_id:
            raise ValueError("State mappings require a team")
        return self


class LinearConfigError(ValueError):
    pass


def provider(workspace_id: str) -> LinearProvider:
    return Linear(resolve_token(workspace_id))


def saved_config(workspace: Workspace) -> LinearConfig:
    return LinearConfig.model_validate((workspace.settings or {}).get("linear") or {})


async def validate_config(db: Session, workspace_id: str, config: LinearConfig) -> None:
    repositories = {repo.id for repo in db.query(Repository).filter(Repository.workspace_id == workspace_id)}
    if not set(config.repo_ids) <= repositories:
        raise LinearConfigError("Linear repositories must belong to this workspace")
    if config.enabled:
        linear = provider(workspace_id)
        if config.team_id not in {team["id"] for team in await linear.teams()}:
            raise LinearConfigError("Select a team returned by Linear discovery")
        state_ids = {state["id"] for state in await linear.team_states(config.team_id)}
        if any(state_id is not None and state_id not in state_ids for state_id in config.states.model_dump().values()):
            raise LinearConfigError("All mapped states must belong to the selected Linear team")


def _sanitize_changes(value: Any, secrets: tuple[str, ...]) -> Any:
    if isinstance(value, str):
        return redact(value, secrets)
    if isinstance(value, dict):
        return {key: _sanitize_changes(item, secrets) for key, item in value.items()}
    if isinstance(value, list):
        return [_sanitize_changes(item, secrets) for item in value]
    return value


def save_config(db: Session, workspace: Workspace, config: LinearConfig, actor: str, *, commit: bool = True) -> None:
    from .onboarding_policy import lock_workspace

    workspace = lock_workspace(db, workspace.id)
    value = config.model_dump()
    if (workspace.settings or {}).get("linear") == value:
        return
    secrets = tuple(value for secret in db.query(Secret).filter(Secret.workspace_id == workspace.id)
                    for value in redaction_values(secret))
    changes = _sanitize_changes({"linear": value}, secrets)
    workspace.settings = {**merged_settings(workspace.settings), "linear": value}
    record(workspace.id, "settings_changed", "Updated workspace Linear configuration", actor=actor,
           data={"changes": changes}, session=db)
    if commit:
        db.commit()
