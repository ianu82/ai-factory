"""Workspace onboarding progress; reads are local, side-effect-free snapshots.

Operational readiness follows runtime credential compatibility (including untested
and operator bootstrap keys). Completion is stricter: identity-only/untested keys
are not called validated. Neither acknowledgments nor an outcome gate readiness.
"""

from __future__ import annotations

from typing import Annotated, Literal

from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel, ConfigDict, Field, StrictBool, TypeAdapter, ValidationError
from sqlalchemy.orm import Session

from .auth import current_user, require_member
from .config import get_settings
from .db import get_db
from .models import Outcome, Repository, Secret, User, Workspace

Step = Literal["models", "github", "repositories", "autonomy", "team", "outcome"]
STEPS: tuple[Step, ...] = ("models", "github", "repositories", "autonomy", "team", "outcome")
router = APIRouter(prefix="/api/workspaces/{workspace_id}/onboarding", tags=["onboarding"])


class Action(BaseModel):
    model_config = ConfigDict(extra="forbid")


class Navigate(Action):
    action: Literal["navigate"]
    step: Step


class Defer(Action):
    action: Literal["defer"]
    deferred: StrictBool


class Complete(Action):
    action: Literal["complete"]
    step: Step
    acknowledged: StrictBool | None = None
    first_outcome_id: str | None = Field(default=None, min_length=1, max_length=32)


OnboardingAction = Annotated[Navigate | Defer | Complete, Field(discriminator="action")]
_action_adapter = TypeAdapter(OnboardingAction)


class OnboardingState(BaseModel):
    current_step: Step
    completed_steps: list[Step]
    deferred: bool
    first_outcome_id: str | None
    ready: bool
    # Operational prerequisites only, not acknowledgments or the first outcome.
    missing_steps: list[Step]
    # Configured credentials without successful validation evidence (bootstrap
    # and sign-in identity checks included). This is not a live health check or
    # a guarantee of repository-scoped GitHub permissions.
    validation_uncertain_steps: list[Step]


def _workspace(db: Session, user: User, workspace_id: str, minimum: str = "viewer") -> Workspace:
    require_member(db, user, workspace_id, minimum)
    ws = db.get(Workspace, workspace_id)
    if ws is None:
        raise HTTPException(404, "Workspace not found")
    return ws


def snapshot(db: Session, ws: Workspace) -> OnboardingState:
    state = ws.onboarding if isinstance(ws.onboarding, dict) else {}
    secrets = {s.kind: s for s in db.query(Secret).filter(Secret.workspace_id == ws.id)}
    preferred = (ws.settings or {}).get("provider") or "mindshub"
    providers = [preferred] + [p for p in ("mindshub", "openai") if p != preferred]
    # Match runtime's preference for workspace credentials over bootstrap keys.
    model = next((secrets.get(f"{p}_api_key") for p in providers
                  if secrets.get(f"{p}_api_key") is not None
                  and secrets[f"{p}_api_key"].status != "invalid"), None)
    github = secrets.get("github_token")
    from .fallbacks import available

    fallback = available(ws.id)  # server credentials count only for the operator's bootstrap workspace
    model_usable = model is not None or fallback["mindshub_api_key"] or fallback["openai_api_key"]
    github_fallback = fallback["github_token"]
    github_usable = bool(github and github.status != "invalid") or github_fallback
    repo_connected = db.query(Repository.id).filter(Repository.workspace_id == ws.id).first() is not None
    from .github_app import INSTALLATION_SETTING, missing_permissions
    from .models import GitHubInstallation

    binding = (ws.settings or {}).get(INSTALLATION_SETTING)
    evidence = db.get(GitHubInstallation, str(binding)) if binding else None
    app_ready = bool(get_settings().github_app_available and evidence and evidence.status == "active"
                     and not missing_permissions(evidence.permissions))
    if binding:
        github_usable = app_ready
    usable = {"models": model_usable, "github": github_usable, "repositories": repo_connected}
    # Catalog evidence comes from credential validation (including seeded keys),
    # never from an identity-only sign-in or this local snapshot.
    from .github import REQUIRED_PERMISSIONS

    github_meta = (github.metadata_json or {}) if github else {}
    permissions = {p.get("permission"): p.get("state") for p in github_meta.get("permissions", [])
                   if isinstance(p, dict)}
    # Read-only probes cannot prove every fine-grained grant, particularly on
    # public repositories. Unknown is honest, not a known denial. The repository
    # itself must have been fetched successfully before this rule applies.
    github_validated = bool(
        github and github.status == "valid" and github_meta.get("login") and github_meta.get("repository")
        and github_meta.get("test_status") == "valid"
        and github_meta.get("diagnostic_code") in ("github_verified", "github_permissions_unverified")
        and all(permissions.get(name) in ("present", "unverified") for name in REQUIRED_PERMISSIONS)
    )
    if binding:
        github_validated = app_ready
    validated = {
        "models": bool(model and model.status == "valid" and (model.metadata_json or {}).get("models")),
        "github": github_validated,
        "repositories": repo_connected,
    }
    completed = {step for step, valid in validated.items() if valid}
    acks = state.get("completed_steps")
    if isinstance(acks, list):
        completed.update(step for step in ("autonomy", "team") if step in acks)
    outcome_id = state.get("first_outcome_id")
    outcome = db.get(Outcome, outcome_id) if isinstance(outcome_id, str) else None
    first_outcome_id = outcome.id if outcome and outcome.workspace_id == ws.id else None
    if first_outcome_id:
        completed.add("outcome")
    current = state.get("current_step")
    if current not in STEPS:
        current = next((step for step in STEPS if step not in completed), "outcome")
    return OnboardingState(
        current_step=current, completed_steps=[s for s in STEPS if s in completed],
        deferred=state.get("deferred") is True, first_outcome_id=first_outcome_id,
        ready=all(usable.values()), missing_steps=[s for s in STEPS if s in usable and not usable[s]],
        validation_uncertain_steps=[s for s in ("models", "github") if usable[s] and not validated[s]],
    )


@router.get("", response_model=OnboardingState)
def get_onboarding(workspace_id: str, user: User = Depends(current_user), db: Session = Depends(get_db)):
    return snapshot(db, _workspace(db, user, workspace_id))


@router.post("/start", response_model=OnboardingState)
def start_onboarding(workspace_id: str, user: User = Depends(current_user), db: Session = Depends(get_db)):
    """Explicit entry transition; snapshots remain side-effect free."""
    from .onboarding_policy import begin, completed, lock_workspace

    _workspace(db, user, workspace_id, "admin")
    ws = lock_workspace(db, workspace_id)
    if completed(db, ws.id) is None:
        settings = begin(ws.settings)
        if settings != ws.settings:
            ws.settings = settings
            db.commit()
    return snapshot(db, ws)


@router.patch("", response_model=OnboardingState)
async def update_onboarding(workspace_id: str, request: Request, user: User = Depends(current_user),
                            db: Session = Depends(get_db)):
    ws = _workspace(db, user, workspace_id, "admin")
    try:
        body = _action_adapter.validate_python(await request.json(), strict=True)
    except (ValidationError, ValueError):
        # Do not echo rejected arbitrary JSON: it might contain a pasted secret.
        raise HTTPException(422, "Invalid onboarding action") from None
    from .onboarding_policy import begin, completed, lock_workspace

    ws = lock_workspace(db, workspace_id)
    before = snapshot(db, ws)
    # Replace progress JSON; policy initialization below also uses copy-on-write.
    state = before.model_dump(include={"current_step", "deferred", "first_outcome_id"})
    state["completed_steps"] = [s for s in before.completed_steps if s in ("autonomy", "team")]
    if isinstance(body, Navigate):
        state["current_step"] = body.step
    elif isinstance(body, Defer):
        state["deferred"] = body.deferred
    else:
        if body.step in ("autonomy", "team"):
            if body.acknowledged is not True or body.first_outcome_id is not None:
                raise HTTPException(422, "Explicit acknowledgment required")
            state["completed_steps"] = list(dict.fromkeys([*state["completed_steps"], body.step]))
        elif body.step == "outcome":
            if body.acknowledged is not None:
                raise HTTPException(422, "Supply a workspace outcome instead of an acknowledgment")
            outcome = db.get(Outcome, body.first_outcome_id) if body.first_outcome_id else None
            if outcome is None or outcome.workspace_id != ws.id:
                raise HTTPException(404, "Outcome not found")
            state["first_outcome_id"] = outcome.id
        else:
            if body.acknowledged is not None or body.first_outcome_id is not None:
                raise HTTPException(422, "Prerequisites are derived from workspace configuration")
            if body.step not in before.completed_steps:
                raise HTTPException(409, "Complete and validate this prerequisite first")
    if completed(db, ws.id) is None:
        ws.settings = begin(ws.settings)
    ws.onboarding = state
    db.commit()
    return snapshot(db, ws)


# Only these static strings may enter the copyable prompt. Never format workspace
# names, settings, credential hints, diagnostics, repository context, or secrets.
SETUP_PROMPT_STEPS = {
    "models": "AI models: choose <MODEL_PROVIDER> and <MODEL>. Enter <MODEL_API_KEY> only in Factory's AI models step, never in this chat.",
    "github": "GitHub: install the GitHub App if offered and review its repository access. Otherwise use the PAT fallback: enter <GITHUB_PAT> only in Factory, never in this chat. Never share a token, private key, or client secret with an assistant.",
    "repositories": "Repositories: choose <REPOSITORY> visible to the GitHub connection; review <SETUP_COMMANDS>, <TEST_COMMANDS> and <START_COMMAND_OR_NO_APP> against repository facts in Factory.",
    "autonomy": "How autonomous: review <PLAN_APPROVAL> and <MERGE_APPROVAL> in Factory. Prefer Approve first while learning; review exactly what automatic runs can do.",
    "team": "Team: review <TEAM_MEMBERS_AND_ROLES> in Factory and confirm who can approve plans and merges. Do not paste personal team details here.",
    "outcome": "First outcome: select one repository and try the guided README CONTRIBUTING example (README only, preserve unrelated documentation, use repository facts; no code, dependency or workflow changes), or choose a blank form. Review the plan, checks, independent verification, PR, CI, approval and smoke evidence in Factory.",
}


def format_setup_prompt(remaining_steps) -> str:
    lines = ["Help me complete Factory setup one step at a time. Ask for choices, not secrets. "
             "Keys must be entered only in Factory; never ask me to paste credentials here."]
    lines.extend(SETUP_PROMPT_STEPS[step] for step in STEPS if step in remaining_steps)
    if len(lines) == 1:
        lines.append("All setup steps are complete; no setup values are needed.")
    return "\n\n".join(lines)


@router.get("/prompt")
def setup_prompt(workspace_id: str, user: User = Depends(current_user), db: Session = Depends(get_db)):
    state = snapshot(db, _workspace(db, user, workspace_id))
    remaining = [step for step in STEPS if step not in state.completed_steps]
    return {"remaining_steps": remaining, "text": format_setup_prompt(remaining)}
