"""HTTP API for the Factory app."""

from __future__ import annotations

import asyncio
import base64
import json
import math
import re
from datetime import datetime, timezone
from typing import Any, Literal
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field, field_validator
from sqlalchemy import case, update
from sqlalchemy.orm import Session

from . import definitions
from . import attachments as A
from . import insights, linear_config, onboarding_policy, preview_config, workstreams
from . import serializers as S
from .auth import ROLE_RANK, auth_methods, current_user, require_member
from .credential_diagnostics import MESSAGES, safe_metadata, test_models
from .db import get_db, utcnow
from .decisions import DecisionError, overrule, resolve
from .events import bus, event_dict, notify, record
from .fallbacks import available as fallback_credentials
from .github import GitHub, GitHubError, PERMISSION_LABELS, REQUIRED_PERMISSIONS, resolve_credential as resolve_token
from .guided import README_TEMPLATE, README_TITLE, README_BRIEF, README_ACCEPTANCE, outcome_guidance
from .harness import capabilities
from .inference import PROVIDERS, InferenceError, ModelClient, resolve_provider
from .linear import Linear, LinearError
from .memory import MemoryScopeError, MemoryService
from .model_checks import check_workspace_models
from .models import (Alert, Artifact, Decision, DecisionReview, Event, Membership, Message, Organization, Outcome, Plan, Repository, Run,
                     Secret, Signal, Task, User, Workspace, new_id)
from .pipeline.orchestrator import orchestrator
from .pipeline.runtime import DEFAULT_SETTINGS, merged_settings
from .review import review_policy, validate_review
from .repo_detect import detect
from .sandbox import isolation_available
from .security import decrypt, encrypt, mask, redact
from .signals import outcome_from_text, sync_github, upsert_signal
from .steward_policy import steward_policy, validate_steward

router = APIRouter(prefix="/api")
SECRET_KINDS = {"mindshub_api_key", "openai_api_key", "github_token", "linear_api_key"}


def _ws(db: Session, user: User, workspace_id: str, minimum: str = "viewer") -> tuple[Workspace, Membership]:
    member = require_member(db, user, workspace_id, minimum)
    ws = db.get(Workspace, workspace_id)
    if ws is None:
        raise HTTPException(404, "Workspace not found")
    return ws, member


# --- identity ---------------------------------------------------------------
# When this process started: the release checker uses it to tell a soak from a restart.
STARTED_AT = datetime.now(timezone.utc).isoformat(timespec="seconds")


@router.get("/health")
def health():
    import os

    from . import __version__

    return {"ok": True, "version": __version__, "commit": os.environ.get("RAILWAY_GIT_COMMIT_SHA", ""),
            "started_at": STARTED_AT, "orchestrator": orchestrator.status()}


@router.get("/meta")
def meta():
    return {"auth": auth_methods(), "harnesses": capabilities(), "capabilities": {"reverify": True},
            "sandbox": {"isolation": "per-job identity" if isolation_available() else "shared (development)"},
            "version": "2.0.0"}


@router.get("/me")
def me(user: User = Depends(current_user), db: Session = Depends(get_db)):
    rows = (db.query(Membership, Workspace, Organization).join(Workspace, Membership.workspace_id == Workspace.id)
            .join(Organization, Workspace.org_id == Organization.id).filter(Membership.user_id == user.id).all())
    return {"user": S.user(user),
            "workspaces": [S.workspace(w, m.role, o.name) for m, w, o in rows],
            "organizations": [{"id": o.id, "name": o.name} for o in {o.id: o for _, _, o in rows}.values()]}


# --- workstreams -------------------------------------------------------------
@router.get("/workspaces/{workspace_id}/workstreams")
def list_workstreams(workspace_id: str, state: Literal["active", "archived", "all"] = "active",
                     user: User = Depends(current_user), db: Session = Depends(get_db)):
    _ws(db, user, workspace_id)
    from .models import Workstream

    query = db.query(Workstream).filter_by(workspace_id=workspace_id)
    if state != "all":
        query = query.filter_by(state=state)
    return [S.workstream(w) for w in query.order_by(Workstream.created_at, Workstream.id)]


@router.get("/workspaces/{workspace_id}/workstreams/{workstream_id}")
def get_workstream(workspace_id: str, workstream_id: str, user: User = Depends(current_user),
                   db: Session = Depends(get_db)):
    _ws(db, user, workspace_id)
    return S.workstream(workstreams.get(db, workspace_id, workstream_id))


@router.post("/workspaces/{workspace_id}/workstreams")
def create_workstream(workspace_id: str, body: workstreams.WorkstreamCreate,
                      user: User = Depends(current_user), db: Session = Depends(get_db)):
    _, member = _ws(db, user, workspace_id, "member")
    return workstreams.create(db, workspace_id, member, user, body)


@router.patch("/workspaces/{workspace_id}/workstreams/{workstream_id}")
def update_workstream(workspace_id: str, workstream_id: str, body: workstreams.WorkstreamUpdate,
                      user: User = Depends(current_user), db: Session = Depends(get_db)):
    _, member = _ws(db, user, workspace_id, "member")
    stream = workstreams.get(db, workspace_id, workstream_id)
    return workstreams.update(db, stream, member, user, body)


# --- workspaces ---------------------------------------------------------------
class WorkspaceCreate(BaseModel):
    org_id: str
    name: str = Field(min_length=1, max_length=120)
    description: str = ""


@router.post("/workspaces")
def create_workspace(body: WorkspaceCreate, user: User = Depends(current_user), db: Session = Depends(get_db)):
    admin = (db.query(Membership).join(Workspace, Membership.workspace_id == Workspace.id)
             .filter(Membership.user_id == user.id, Workspace.org_id == body.org_id,
                     Membership.role.in_(["owner", "admin"])).first())
    if admin is None:
        raise HTTPException(403, "Only organization admins can create workspaces")
    slug = re.sub(r"[^a-z0-9]+", "-", body.name.lower()).strip("-")[:60] or "workspace"
    base, n = slug, 2
    while db.query(Workspace).filter(Workspace.org_id == body.org_id, Workspace.slug == slug).first():
        slug, n = f"{base}-{n}", n + 1
    ws = Workspace(org_id=body.org_id, name=body.name, slug=slug, description=body.description,
                   settings=onboarding_policy.begin(DEFAULT_SETTINGS))
    db.add(ws)
    db.flush()
    db.add(Membership(workspace_id=ws.id, user_id=user.id, role="owner"))
    db.commit()
    return S.workspace(ws, "owner")


class WorkspaceUpdate(BaseModel):
    name: str | None = None
    description: str | None = None
    settings: dict[str, Any] | None = None
    paused: bool | None = None


def _mind_settings(value: Any) -> dict[str, Any]:
    """Mind's daily budget: a finite, nonnegative number of US dollars (0 turns Mind off)."""
    if value is None:
        return dict(DEFAULT_SETTINGS["mind"])
    if not isinstance(value, dict) or set(value) - {"daily_budget_usd"}:
        raise HTTPException(422, "mind accepts only daily_budget_usd")
    budget = value.get("daily_budget_usd", DEFAULT_SETTINGS["mind"]["daily_budget_usd"])
    if type(budget) not in (int, float) or not math.isfinite(budget) or budget < 0:
        raise HTTPException(422, "mind.daily_budget_usd must be a finite, nonnegative number")
    return {"daily_budget_usd": float(budget)}


def _lead_settings(value: Any) -> dict[str, Any]:
    """The Lead: on or off, its spend per run in US dollars and its turns per run."""
    default = DEFAULT_SETTINGS["lead"]
    if value is None:
        return dict(default)
    if not isinstance(value, dict) or set(value) - set(default):
        raise HTTPException(422, "lead accepts only enabled, run_usd and turns")
    enabled = value.get("enabled", default["enabled"])
    usd = value.get("run_usd", default["run_usd"])
    turns = value.get("turns", default["turns"])
    if type(enabled) is not bool:
        raise HTTPException(422, "lead.enabled must be true or false")
    if type(usd) not in (int, float) or not math.isfinite(usd) or not 0 <= usd <= 20:
        raise HTTPException(422, "lead.run_usd must be a number from 0 to 20")
    if type(turns) is not int or not 2 <= turns <= 80:  # one to act, and the last for the summary
        raise HTTPException(422, "lead.turns must be a whole number from 2 to 80")
    return {"enabled": enabled, "run_usd": float(usd), "turns": turns}


def _alert_settings(value: Any) -> dict[str, Any]:
    """Alerts: how long a decision may wait before it alerts, and the outside channels. Slack and email aren't
    available yet, so they can only be off: alerts show in the app's Inbox."""
    default = DEFAULT_SETTINGS["alerts"]
    if value is None:
        return dict(default)
    if not isinstance(value, dict) or set(value) - set(default):
        raise HTTPException(422, "alerts accepts only after_minutes, slack and email")
    after = value.get("after_minutes", default["after_minutes"])
    if type(after) is not int or not 15 <= after <= 1440:
        raise HTTPException(422, "alerts.after_minutes must be a whole number from 15 to 1440")
    for channel, name in (("slack", "Slack"), ("email", "Email")):
        on = value.get(channel, False)
        if type(on) is not bool:
            raise HTTPException(422, f"alerts.{channel} must be true or false")
        if on:
            raise HTTPException(422, f"{name} alerts aren't available yet: alerts show in the Inbox")
    return {"after_minutes": after, "slack": False, "email": False}


def _clean_settings(raw: dict[str, Any]) -> dict[str, Any]:
    try:
        review = review_policy(raw)
    except ValueError as exc:
        raise HTTPException(422, str(exc)) from None
    mind = _mind_settings(raw.get("mind"))
    lead = _lead_settings(raw.get("lead"))  # before merging: a lead that isn't an object is refused, not a 500
    alerts = _alert_settings(raw.get("alerts"))
    merged = merged_settings({**raw, "lead": lead, "alerts": alerts})
    if "decision_judges" in raw and raw["decision_judges"] not in ("off", "suggest", "decide"):
        raise HTTPException(422, "decision_judges must be off, suggest or decide")
    merged["review"] = review
    merged["mind"] = mind
    merged["lead"] = lead
    merged["alerts"] = alerts
    merged["max_attempts"] = max(1, min(int(merged.get("max_attempts", 3)), 8))
    merged["auto_retry_rounds"] = max(0, min(int(merged.get("auto_retry_rounds", 1)), 3))
    for role in ("builder", "verifier", "planner"):
        if merged["harness"].get(role) not in ("native", "codex", "anton"):
            merged["harness"][role] = "native"
    for key in ("plan", "merge", "release"):
        if merged["autonomy"].get(key) not in ("auto", "approval"):
            merged["autonomy"][key] = DEFAULT_SETTINGS["autonomy"][key]
    if merged.get("provider") not in PROVIDERS:
        merged["provider"] = "mindshub"
    from .scorers import definitions
    try:
        definitions(merged)
    except ValueError:
        raise HTTPException(422, "Invalid scorer configuration") from None
    return merged


@router.patch("/workspaces/{workspace_id}")
def update_workspace(workspace_id: str, body: WorkspaceUpdate, user: User = Depends(current_user),
                     db: Session = Depends(get_db)):
    ws, _ = _ws(db, user, workspace_id, "admin")
    if body.settings is not None and "github_app_installation_id" in body.settings:
        # Only T4's verified installation callback may bind/unbind an installation.
        raise HTTPException(422, "Use the GitHub App connection flow to manage GitHub access")
    if body.settings is not None and "linear" in body.settings:
        raise HTTPException(422, "Use PUT /api/workspaces/{workspace_id}/linear to configure Linear")
    ws = onboarding_policy.lock_workspace(db, workspace_id)
    changes = {}
    if body.name is not None:
        ws.name = body.name.strip()[:120] or ws.name
    if body.description is not None:
        ws.description = body.description
    if body.settings is not None:
        # Keys starting with "_" are the operator bootstrap's own bookkeeping (including the marker that
        # earns server fallback credentials); people can never set or clear them through the API.
        patch = {k: v for k, v in body.settings.items() if not str(k).startswith("_")}
        if onboarding_policy.KEY in patch:
            raise HTTPException(422, "Learning lifecycle is managed by the onboarding policy endpoint")
        before = merged_settings(ws.settings)
        updated = {**before, **patch}
        if "review" in patch:
            try:
                updated["review"] = {**before["review"], **validate_review(patch["review"])}
            except ValueError as exc:
                raise HTTPException(422, str(exc)) from None
        if "autonomy" in patch:
            autonomy = patch["autonomy"]
            if not isinstance(autonomy, dict) or any(
                    k not in onboarding_policy.POLICIES or v not in ("auto", "approval")
                    for k, v in autonomy.items()):
                raise HTTPException(422, "Invalid autonomy policy")
            updated = onboarding_policy.patch_autonomy({**updated, "autonomy": before["autonomy"]}, autonomy)
        ws.settings = _clean_settings(updated)
        changes = {k: ws.settings[k] for k in patch if before.get(k) != ws.settings.get(k)}
    if body.paused is not None:
        ws.paused = body.paused
        changes["paused"] = body.paused
    db.commit()
    record(ws.id, "settings_changed", f"{user.name or user.email} updated workspace settings", actor=user.id,
           data={"changes": changes})
    orchestrator.wake()
    return S.workspace(ws, require_member(db, user, workspace_id).role)


@router.get("/workspaces/{workspace_id}/onboarding/policy")
def get_learning_policy(workspace_id: str, user: User = Depends(current_user), db: Session = Depends(get_db)):
    ws, _ = _ws(db, user, workspace_id)
    return onboarding_policy.offer(db, ws)


@router.post("/workspaces/{workspace_id}/onboarding/policy/apply")
def apply_learning_preset(workspace_id: str, user: User = Depends(current_user), db: Session = Depends(get_db)):
    _ws(db, user, workspace_id, "admin")
    ws = onboarding_policy.lock_workspace(db, workspace_id)
    preview = onboarding_policy.offer(db, ws)
    if preview["applied"]:
        return preview
    if not preview["eligible"]:
        raise HTTPException(409, "Complete an outcome before applying the preset")
    old = ws.settings
    state = old[onboarding_policy.KEY]
    new = {**old, "autonomy": dict(preview["after"]),
           onboarding_policy.KEY: {**state, "status": "applied", "applied_by": user.id,
                                   "applied_at": utcnow().isoformat()}}
    ws.settings = new
    record(ws.id, "learning_preset_applied", f"{user.name or user.email} applied the post-learning preset",
           actor=user.id, data={"before": preview["before"], "after": preview["after"],
                                "changes": preview["changes"],
                                "outcome_id": preview["first_completed_outcome_id"]}, session=db, publish=False)
    db.commit()
    db.refresh(ws)
    notify(ws.id, "workspace", ws.id)
    orchestrator.wake()
    return onboarding_policy.offer(db, ws)


@router.get("/workspaces/{workspace_id}/linear")
def get_linear_config(workspace_id: str, user: User = Depends(current_user), db: Session = Depends(get_db)):
    ws, _ = _ws(db, user, workspace_id)
    return linear_config.saved_config(ws)


@router.put("/workspaces/{workspace_id}/linear")
async def put_linear_config(workspace_id: str, body: linear_config.LinearConfig,
                            user: User = Depends(current_user), db: Session = Depends(get_db)):
    ws, _ = _ws(db, user, workspace_id, "admin")
    try:
        await linear_config.validate_config(db, workspace_id, body)
    except linear_config.LinearConfigError as exc:
        raise HTTPException(422, str(exc)) from None
    except LinearError as exc:
        raise HTTPException(503, str(exc)) from None
    linear_config.save_config(db, ws, body, user.id)
    return body


@router.get("/workspaces/{workspace_id}/linear/teams")
async def linear_teams(workspace_id: str, user: User = Depends(current_user), db: Session = Depends(get_db)):
    _ws(db, user, workspace_id, "admin")
    try:
        return {"items": await linear_config.provider(workspace_id).teams()}
    except LinearError as exc:
        raise HTTPException(503, str(exc)) from None


@router.get("/workspaces/{workspace_id}/linear/teams/{team_id}/states")
async def linear_team_states(workspace_id: str, team_id: str, user: User = Depends(current_user),
                             db: Session = Depends(get_db)):
    _ws(db, user, workspace_id, "admin")
    try:
        return {"items": await linear_config.provider(workspace_id).team_states(team_id)}
    except LinearError as exc:
        raise HTTPException(503, str(exc)) from None


@router.get("/workspaces/{workspace_id}/members")
def members(workspace_id: str, user: User = Depends(current_user), db: Session = Depends(get_db)):
    _ws(db, user, workspace_id)
    rows = db.query(Membership, User).join(User, Membership.user_id == User.id).filter(
        Membership.workspace_id == workspace_id).all()
    return [{"user": S.user(u), "role": m.role} for m, u in rows]


class RoleUpdate(BaseModel):
    role: str


@router.patch("/workspaces/{workspace_id}/members/{user_id}")
def set_role(workspace_id: str, user_id: str, body: RoleUpdate, user: User = Depends(current_user),
             db: Session = Depends(get_db)):
    _, mine = _ws(db, user, workspace_id, "admin")
    if body.role not in ROLE_RANK or ROLE_RANK[body.role] > ROLE_RANK[mine.role]:
        raise HTTPException(400, "Invalid role")
    target = db.query(Membership).filter(Membership.workspace_id == workspace_id, Membership.user_id == user_id).one_or_none()
    if target is None:
        raise HTTPException(404, "Member not found")
    target.role = body.role
    db.commit()
    record(workspace_id, "member_role", f"Role changed to {body.role}", actor=user.id, data={"user_id": user_id})
    return {"ok": True}


@router.post("/workspaces/{workspace_id}/seen")
def mark_seen(workspace_id: str, user: User = Depends(current_user), db: Session = Depends(get_db)):
    _, member = _ws(db, user, workspace_id)
    member.last_viewed_at = utcnow()
    db.commit()
    return {"ok": True}


# --- credentials ----------------------------------------------------------------
def _secret_view(kind: str, sec: Secret | None) -> dict[str, Any]:
    labels = {"mindshub_api_key": "MindsHub Inference", "openai_api_key": "OpenAI", "github_token": "GitHub",
              "linear_api_key": "Linear"}
    if sec is None:
        return {"kind": kind, "label": labels[kind], "configured": False, "status": "missing"}
    value = decrypt(sec.ciphertext)
    metadata = safe_metadata(sec.metadata_json or {}, value)
    detail = MESSAGES.get(metadata.get("diagnostic_code"))
    if detail is None:
        detail = (redact(sec.status_detail or "", (value,)) if kind == "linear_api_key"
                  else "Credential saved. Test it to verify current access.")
    return {"kind": kind, "label": labels[kind], "configured": True, "hint": mask(value), "status": sec.status,
            "detail": detail, "tested_at": S.ts(sec.tested_at), "updated_at": S.ts(sec.updated_at),
            "metadata": metadata}


@router.get("/workspaces/{workspace_id}/credentials")
def credentials(workspace_id: str, user: User = Depends(current_user), db: Session = Depends(get_db)):
    _ws(db, user, workspace_id)
    secrets = {s.kind: s for s in db.query(Secret).filter(Secret.workspace_id == workspace_id)}
    return {"github_permissions": [{"permission": name, "label": PERMISSION_LABELS[name], "required": level}
                                   for name, level in REQUIRED_PERMISSIONS.items()],
            "items": [_secret_view(k, secrets.get(k)) for k in ("mindshub_api_key", "github_token", "openai_api_key", "linear_api_key")],
            "server_fallbacks": fallback_credentials(workspace_id)}


async def _test_secret(kind: str, value: str) -> tuple[str, str, dict[str, Any]]:
    try:
        if kind == "linear_api_key":
            viewer = await Linear(value).viewer()
            name = redact(viewer["name"], (value,))
            return "valid", redact(f"Connected as {name}", (value,)), {"name": name}
        if kind in ("mindshub_api_key", "openai_api_key"):
            return await test_models(kind, value)
        if kind == "github_token":
            return await GitHub(value).diagnose()
    except LinearError as exc:
        return "invalid" if exc.category == "authentication" else "untested", redact(str(exc), (value,)), {}
    except Exception:
        return "untested", MESSAGES["service_error"], {"diagnostic_code": "service_error", "test_status": "untested"}
    return "untested", "", {}


class SecretBody(BaseModel):
    value: str = Field(min_length=8, max_length=8192)


@router.put("/workspaces/{workspace_id}/credentials/{kind}")
async def set_credential(workspace_id: str, kind: str, body: SecretBody, user: User = Depends(current_user),
                         db: Session = Depends(get_db)):
    _ws(db, user, workspace_id, "admin")
    if kind not in SECRET_KINDS:
        raise HTTPException(404, "Unknown credential")
    value = body.value.strip()
    if len(value) < 8:
        raise HTTPException(422, "Invalid credential request")
    if kind == "mindshub_api_key" and not value.startswith("mdb_"):
        raise HTTPException(400, "MindsHub API keys start with mdb_")
    status, detail, metadata = await _test_secret(kind, value)
    sec = db.query(Secret).filter(Secret.workspace_id == workspace_id, Secret.kind == kind).one_or_none()
    if sec is None:
        sec = Secret(workspace_id=workspace_id, kind=kind, ciphertext="")
        db.add(sec)
    sec.ciphertext = encrypt(value)
    sec.hint = mask(value)
    sec.status, sec.status_detail, sec.metadata_json = status, redact(detail, (value,)), safe_metadata(metadata, value)
    sec.tested_at = utcnow()
    sec.updated_by = user.id
    db.commit()
    record(workspace_id, "credential_saved", f"{user.name or user.email} saved the {kind.replace('_', ' ')} ({status})",
           actor=user.id, level="success" if status == "valid" else "warn", data={"kind": kind, "status": status})
    return _secret_view(kind, sec)


@router.post("/workspaces/{workspace_id}/credentials/{kind}/test")
async def test_credential(workspace_id: str, kind: str,
                          repository: str | None = Query(default=None, max_length=200,
                                                        pattern=r"^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$"),
                          user: User = Depends(current_user), db: Session = Depends(get_db)):
    _ws(db, user, workspace_id, "admin")
    if kind.startswith("preview_"):
        raise HTTPException(404, "Unknown credential")
    sec = db.query(Secret).filter(Secret.workspace_id == workspace_id, Secret.kind == kind).one_or_none()
    if sec is None:
        raise HTTPException(404, "Not configured")
    value = decrypt(sec.ciphertext)
    # Settings and automatic tests have no repository parameter. Reuse the last
    # selected repository instead of downgrading its evidence to identity-only.
    if kind == "github_token":
        recorded = (sec.metadata_json or {}).get("repository")
        if repository is None and isinstance(recorded, str) and re.fullmatch(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+", recorded):
            repository = recorded
        status, detail, metadata = await GitHub(value).diagnose(repository)
    else:
        status, detail, metadata = await _test_secret(kind, value)
    # Only transient diagnostic failures retain the last-known-good status.
    # Definitive rejection must still turn Settings' status indicator red.
    if not (status == "untested" and sec.status == "valid"
            and metadata.get("diagnostic_code") in ("network_error", "rate_limited", "service_error")):
        sec.status = status
    sec.status_detail, sec.tested_at = redact(detail, (value,)), utcnow()
    sec.metadata_json = safe_metadata(metadata, value)
    db.commit()
    return _secret_view(kind, sec)


@router.delete("/workspaces/{workspace_id}/credentials/{kind}")
def delete_credential(workspace_id: str, kind: str, user: User = Depends(current_user), db: Session = Depends(get_db)):
    _ws(db, user, workspace_id, "admin")
    if kind.startswith("preview_"):
        raise HTTPException(404, "Unknown credential")
    sec = db.query(Secret).filter(Secret.workspace_id == workspace_id, Secret.kind == kind).one_or_none()
    if sec:
        db.delete(sec)
        db.commit()
        record(workspace_id, "credential_removed", f"{user.name or user.email} removed the {kind.replace('_', ' ')}",
               actor=user.id)
    return {"ok": True}


@router.get("/workspaces/{workspace_id}/models")
async def models(workspace_id: str, user: User = Depends(current_user), db: Session = Depends(get_db)):
    _ws(db, user, workspace_id)
    try:
        provider = resolve_provider(workspace_id)
        names = await ModelClient(provider).list_models()
        return {"provider": provider.provider, "label": provider.label, "models": names}
    except InferenceError as exc:
        return {"provider": None, "models": [], "error": str(exc)}


@router.post("/workspaces/{workspace_id}/models/check")
async def check_models(workspace_id: str, user: User = Depends(current_user), db: Session = Depends(get_db)):
    ws, _ = _ws(db, user, workspace_id, "admin")
    return await check_workspace_models(workspace_id, merged_settings(ws.settings or {}))


# --- memory -----------------------------------------------------------------------
@router.get("/workspaces/{workspace_id}/memory")
def memories(workspace_id: str, repo_id: str | None = None, run_id: str | None = None,
             limit: int = Query(100, ge=1, le=200), offset: int = Query(0, ge=0, le=1_000_000),
             user: User = Depends(current_user), db: Session = Depends(get_db)):
    _ws(db, user, workspace_id)
    try:
        return [S.memory(entry) for entry in MemoryService(workspace_id, repo_id, run_id).list(limit=limit, offset=offset)]
    except MemoryScopeError:
        raise HTTPException(404, "Memory scope not found") from None


@router.delete("/workspaces/{workspace_id}/memory/{entry_id}")
def delete_memory(workspace_id: str, entry_id: str, user: User = Depends(current_user),
                  db: Session = Depends(get_db)):
    _ws(db, user, workspace_id, "admin")
    try:
        deleted = MemoryService(workspace_id).delete(entry_id, actor=user.id)
    except MemoryScopeError:
        deleted = False
    if not deleted:
        raise HTTPException(404, "Memory entry not found")
    return {"ok": True}


# --- repositories -----------------------------------------------------------------
@router.get("/workspaces/{workspace_id}/repositories")
def repositories(workspace_id: str, user: User = Depends(current_user), db: Session = Depends(get_db)):
    _ws(db, user, workspace_id)
    return [S.repository(r) for r in db.query(Repository).filter(Repository.workspace_id == workspace_id)
            .order_by(Repository.full_name)]


@router.get("/workspaces/{workspace_id}/github/repos")
async def github_repos(workspace_id: str, page: int = Query(1, ge=1), q: str = Query("", max_length=200),
                       per_page: int = Query(100, ge=1, le=100),
                       user: User = Depends(current_user), db: Session = Depends(get_db)):
    _ws(db, user, workspace_id, "admin")
    try:
        gh = GitHub(resolve_token(workspace_id))
        # Search only repositories reachable by this credential, not public GitHub
        # search results. Continue past nonmatching pages, including the old cap.
        while True:
            result = await gh.accessible_repos_page(page, per_page)
            result["items"] = [r for r in result["items"] if q.strip().casefold() in r["full_name"].casefold()]
            if result["items"] or result["next_page"] is None:
                return result
            page = result["next_page"]
    except GitHubError as exc:
        return {"items": [], "error": str(exc), "next_page": None}


class RepoQACommands(BaseModel):
    start: str | None = None


class RepoReleaseSelection(BaseModel):
    mode: Literal["on_merge"] | None = None


class RepoCommands(BaseModel):
    preview_qa: Any = None
    review: dict[str, Any] | None = None
    setup: list[str] | None = None
    checks: list[str] | None = None
    qa: RepoQACommands | None = None
    release: RepoReleaseSelection | None = None

    @field_validator("review")
    @classmethod
    def valid_review(cls, value):
        return validate_review(value, repository=True)


class RepoConnect(BaseModel):
    full_name: str
    config: RepoCommands | None = None


def _repo_name(full_name: str) -> str:
    name = full_name.strip().removeprefix("https://github.com/").strip("/").removesuffix(".git")
    if not re.fullmatch(r"[\w.-]+/[\w.-]+", name):
        raise HTTPException(400, "Use owner/name")
    return name


async def _detect_repository(workspace_id: str, full_name: str):
    name = _repo_name(full_name)
    try:
        gh = GitHub(resolve_token(workspace_id))
        info = await gh.repo(name)
        if not (info.get("permissions") or {}).get("push"):
            raise HTTPException(400, "The workspace's GitHub access cannot push to this repository")
        config = await detect(gh, info["full_name"], info.get("default_branch") or "main")
    except GitHubError as exc:
        raise HTTPException(400, str(exc))
    return info, config


@router.post("/workspaces/{workspace_id}/repositories/preview")
async def preview_repo(workspace_id: str, body: RepoConnect, user: User = Depends(current_user),
                       db: Session = Depends(get_db)):
    _ws(db, user, workspace_id, "admin")
    info, config = await _detect_repository(workspace_id, body.full_name)
    return {"full_name": info["full_name"], "default_branch": info.get("default_branch") or "main",
            "description": info.get("description") or "", "detected": config.get("detected", []), "config": config,
            "release_selection": {"mode": "on_merge", "confirmed": False,
                                  "description": "Select only if this repository deploys when main changes; not automatically detected."}}


def _clean_repo_config(current: dict[str, Any], patch: dict[str, Any]) -> dict[str, Any]:
    cfg = dict(current)
    if "preview_qa" in patch:
        try:
            cfg["preview_qa"] = preview_config.validate_config(patch["preview_qa"], current.get("preview_qa"))
        except ValueError:
            raise HTTPException(422, "Invalid Preview QA configuration") from None
    if "require_trusted_issue_author" in patch:
        if type(patch["require_trusted_issue_author"]) is not bool:
            raise HTTPException(422, "require_trusted_issue_author must be a boolean")
        cfg["require_trusted_issue_author"] = patch["require_trusted_issue_author"]
    if "steward" in patch:
        try:
            update = validate_steward(patch["steward"])
            try:
                policy = steward_policy(current)
            except ValueError:
                policy = steward_policy()  # Allow an admin to replace an invalid stored policy.
            cfg["steward"] = {**policy, **update}
        except ValueError as exc:
            raise HTTPException(422, str(exc)) from None
    if "review" in patch:
        try:
            cfg["review"] = {"mode": "off", "allow_untrusted_local": False,
                             **(current.get("review") or {}), **validate_review(patch["review"], repository=True)}
        except ValueError as exc:
            raise HTTPException(422, str(exc)) from None
    for key in ("setup", "checks", "smoke", "protected_paths"):
        if key in patch:
            cfg[key] = [str(x).strip() for x in patch[key] or [] if str(x).strip()]
    for key in ("context", "signal_label"):
        if key in patch:
            cfg[key] = str(patch[key] or "")[:8000]
    if "merge" in patch:
        if not isinstance(patch["merge"], dict):
            raise HTTPException(422, "merge must be an object")
        merge = {**(current.get("merge") or {}), **patch["merge"]}
        if "reverify" in merge and merge["reverify"] not in ("overlap", "always", "never"):
            raise HTTPException(422, "merge.reverify must be overlap, always or never")
        cfg["merge"] = merge
    for key in ("release", "qa", "recovery", "build"):
        if key in patch and isinstance(patch[key], dict):
            cfg[key] = patch[key]
    for key in ("require_ci", "max_changed_lines"):
        if key in patch:
            cfg[key] = patch[key]
    return cfg


@router.post("/workspaces/{workspace_id}/repositories")
async def connect_repo(workspace_id: str, body: RepoConnect, user: User = Depends(current_user),
                       db: Session = Depends(get_db)):
    _ws(db, user, workspace_id, "admin")
    name = _repo_name(body.full_name)
    if db.query(Repository).filter(Repository.workspace_id == workspace_id, Repository.full_name == name).first():
        raise HTTPException(409, "Already connected")
    info, config = await _detect_repository(workspace_id, name)
    if db.query(Repository).filter(Repository.workspace_id == workspace_id,
                                   Repository.full_name == info["full_name"]).first():
        raise HTTPException(409, "Already connected")
    # Detection is a proposal, not consent to execute a long-running server.
    qa = dict(config.get("qa") or {})
    qa.pop("start", None)
    config["qa"] = qa
    if body.config is not None:
        patch = body.config.model_dump(exclude_unset=True)
        # Connection reviews only edit commands; retain all unrelated QA fields.
        if isinstance(patch.get("qa"), dict):
            reviewed_qa = patch["qa"]
            patch["qa"] = dict(qa)
            if "start" in reviewed_qa:
                patch["qa"]["start"] = (reviewed_qa["start"] or "").strip()
        if isinstance(patch.get("release"), dict):
            patch["release"] = {**(config.get("release") or {}), **patch["release"]}
        config = _clean_repo_config(config, patch)
    repo = Repository(workspace_id=workspace_id, full_name=info["full_name"], default_branch=info.get("default_branch") or "main",
                      description=info.get("description") or "", config=config)
    try:
        definitions.lock_workspace(db, workspace_id)
        definitions.lock_repositories(db, workspace_id)
        db.add(repo)
        definitions.validate_repository_change(db, workspace_id, author=user.id)
        db.commit()
    except definitions.DefinitionError as exc:
        db.rollback()
        raise HTTPException(422, str(exc)) from exc
    record(workspace_id, "repository_connected", f"Connected {repo.full_name}", actor=user.id,
           data={"detected": config.get("detected", [])})
    return S.repository(repo)


class RepoUpdate(BaseModel):
    config: dict[str, Any]


@router.patch("/workspaces/{workspace_id}/repositories/{repo_id}")
def update_repo(workspace_id: str, repo_id: str, body: RepoUpdate, user: User = Depends(current_user),
                db: Session = Depends(get_db)):
    _ws(db, user, workspace_id, "admin")
    definitions.lock_workspace(db, workspace_id)
    definitions.lock_repositories(db, workspace_id)
    repo = db.get(Repository, repo_id)
    if repo is None or repo.workspace_id != workspace_id:
        raise HTTPException(404, "Repository not found")
    try:
        repo.config = _clean_repo_config(repo.config or {}, body.config)
        definitions.validate_repository_change(db, workspace_id, author=user.id)
        db.commit()
    except definitions.DefinitionError as exc:
        db.rollback()
        raise HTTPException(422, str(exc)) from exc
    record(workspace_id, "repository_configured", f"Updated {repo.full_name} configuration", actor=user.id)
    return S.repository(repo)


def _preview_repository(db: Session, user: User, workspace_id: str, repo_id: str,
                        minimum: str = "viewer") -> Repository:
    _ws(db, user, workspace_id, minimum)
    # Always query again after the workspace lock: db.get may reuse a cached
    # repository that a concurrent disconnect has already removed.
    repo = db.query(Repository).filter_by(id=repo_id, workspace_id=workspace_id).populate_existing().one_or_none()
    if repo is None:
        raise HTTPException(404, "Repository not found")
    return repo


@router.get("/workspaces/{workspace_id}/repositories/{repo_id}/preview-qa/account")
def preview_account_metadata(workspace_id: str, repo_id: str, user: User = Depends(current_user),
                             db: Session = Depends(get_db)):
    _preview_repository(db, user, workspace_id, repo_id)
    return preview_config.account_metadata(preview_config.account(db, workspace_id, repo_id))


@router.put("/workspaces/{workspace_id}/repositories/{repo_id}/preview-qa/account")
async def put_preview_account(workspace_id: str, repo_id: str, request: Request,
                              user: User = Depends(current_user), db: Session = Depends(get_db)):
    _preview_repository(db, user, workspace_id, repo_id, "admin")
    # Bound bytes before decoding. Never let framework validation echo input values.
    raw = bytearray()
    async for chunk in request.stream():
        if len(raw) + len(chunk) > preview_config.MAX_ACCOUNT_BYTES:
            raise HTTPException(422, "Invalid preview account request")
        raw.extend(chunk)
    try:
        fields = preview_config.validate_account(json.loads(raw))
    except (ValueError, TypeError, UnicodeError, RecursionError):
        raise HTTPException(422, "Invalid preview account request") from None
    definitions.lock_workspace(db, workspace_id)
    _preview_repository(db, user, workspace_id, repo_id, "admin")
    sec = preview_config.save_account(db, workspace_id, repo_id, fields, user.id)
    db.commit()
    record(workspace_id, "preview_account_saved", "Updated Preview QA account", actor=user.id)
    return preview_config.account_metadata(sec)


@router.delete("/workspaces/{workspace_id}/repositories/{repo_id}/preview-qa/account")
def delete_preview_account(workspace_id: str, repo_id: str, user: User = Depends(current_user),
                           db: Session = Depends(get_db)):
    _preview_repository(db, user, workspace_id, repo_id, "admin")
    definitions.lock_workspace(db, workspace_id)
    _preview_repository(db, user, workspace_id, repo_id, "admin")
    sec = preview_config.account(db, workspace_id, repo_id)
    if sec is not None:
        db.delete(sec)
    db.commit()
    record(workspace_id, "preview_account_deleted", "Deleted Preview QA account", actor=user.id)
    return preview_config.account_metadata(None)


@router.delete("/workspaces/{workspace_id}/repositories/{repo_id}")
def disconnect_repo(workspace_id: str, repo_id: str, user: User = Depends(current_user), db: Session = Depends(get_db)):
    _ws(db, user, workspace_id, "admin")
    # Use the same serialization point as account PUT/DELETE before reading
    # repository state, so a late account write cannot leave an orphan secret.
    definitions.lock_workspace(db, workspace_id)
    repo = _preview_repository(db, user, workspace_id, repo_id, "admin")
    if db.query(Task).filter(Task.repo_id == repo_id, Task.status.in_(["queued", "running", "waiting_ci"])).first():
        raise HTTPException(409, "Work is running on this repository")
    try:
        definitions.guard_disconnect(db, workspace_id, repo_id)
    except definitions.DefinitionError as exc:
        db.rollback()
        raise HTTPException(409, str(exc)) from exc
    MemoryService(workspace_id, repo_id).delete_repository_entries(session=db, actor=user.id)
    workspace = db.get(Workspace, workspace_id)
    config = (workspace.settings or {}).get("linear")
    if config and repo_id in config.get("repo_ids", []):
        config = {**config, "repo_ids": [value for value in config["repo_ids"] if value != repo_id]}
        if not config["repo_ids"]:
            config["enabled"] = False
        linear_config.save_config(db, workspace, linear_config.LinearConfig.model_validate(config), user.id,
                                  commit=False)
    preview_account = preview_config.account(db, workspace_id, repo_id)
    if preview_account is not None:
        db.delete(preview_account)
    workstream_events = workstreams.remove_repository_defaults(db, workspace_id, repo_id, user)
    db.delete(repo)
    definitions.validate_repository_change(db, workspace_id, author=user.id)
    db.commit()
    workstreams.publish_changes(workstream_events)
    return {"ok": True}


# --- staged attachments -----------------------------------------------------------
class AttachmentUpload(BaseModel):
    name: str = Field(min_length=1, max_length=1000)
    content: str


@router.post("/workspaces/{workspace_id}/attachments")
def upload_attachment(workspace_id: str, body: AttachmentUpload, user: User = Depends(current_user),
                      db: Session = Depends(get_db)):
    _ws(db, user, workspace_id, "member")
    try:
        name, data = A.validate_upload(body.name, body.content)
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc
    try:
        A.check_staged_quota(db, workspace_id, user.id, data["size_bytes"])
    except ValueError as exc:
        db.rollback()
        raise HTTPException(413, str(exc)) from exc
    row = Artifact(workspace_id=workspace_id, kind="attachment", name=name, content=body.content,
                   data={**data, "uploaded_by": user.id})
    db.add(row)
    db.commit()
    return S.artifact(row)


@router.delete("/workspaces/{workspace_id}/attachments/{attachment_id}")
def delete_attachment(workspace_id: str, attachment_id: str, user: User = Depends(current_user),
                      db: Session = Depends(get_db)):
    _ws(db, user, workspace_id, "member")
    # A conditional delete cannot race an outcome's conditional claim.
    deleted = A.staged_query(db, workspace_id, user.id).filter(Artifact.id == attachment_id).delete(
        synchronize_session=False)
    A.cleanup_staged(db)
    db.commit()
    if not deleted:
        raise HTTPException(404, "Attachment not found")
    return {"ok": True}


# --- outcomes ---------------------------------------------------------------------
class OutcomeCreate(BaseModel):
    title: str = Field(min_length=3, max_length=300)
    brief: str = ""
    acceptance: list[str] = []
    repo_ids: list[str] = Field(min_length=1)
    harness: str | None = None
    priority: int = 2
    attachment_ids: list[str] = Field(default_factory=list)
    request_id: UUID | None = None
    template: Literal["readme-contributing"] | None = None


def _outcome_rows(db: Session, workspace_id: str, outcome_ids: list[str]) -> tuple[dict, dict, dict]:
    from .work_services import outcome_rows

    return outcome_rows(db, workspace_id, outcome_ids)


@router.get("/workspaces/{workspace_id}/outcomes")
def list_outcomes(workspace_id: str, status: str = "", limit: int = 200, user: User = Depends(current_user),
                  db: Session = Depends(get_db)):
    _ws(db, user, workspace_id)
    query = db.query(Outcome).filter(Outcome.workspace_id == workspace_id)
    if status:
        query = query.filter(Outcome.status.in_(status.split(",")))
    rows = query.order_by(Outcome.updated_at.desc()).limit(max(1, min(limit, 500))).all()
    tasks, runs, decisions = _outcome_rows(db, workspace_id, [o.id for o in rows])
    sources = S.outcome_sources(db, rows)
    return [S.outcome(o, tasks.get(o.id, []), runs.get(o.id, []), decisions.get(o.id, 0),
                      sources.get(o.signal_id)) for o in rows]


@router.post("/workspaces/{workspace_id}/outcomes")
def create_outcome(workspace_id: str, body: OutcomeCreate, user: User = Depends(current_user),
                   db: Session = Depends(get_db)):
    _ws(db, user, workspace_id, "member")
    marker = None
    if body.request_id is not None:
        # UUID bytes encoded without padding fit the existing VARCHAR(32) signal_id.
        marker = "request:" + base64.urlsafe_b64encode(body.request_id.bytes).decode().rstrip("=")
        # Serialize requests for this workspace on SQLite and Postgres. A row lock
        # alone would be ignored by SQLite. This write precedes the replay lookup.
        db.execute(update(Workspace).where(Workspace.id == workspace_id).values(outcome_seq=Workspace.outcome_seq))
        existing = db.query(Outcome).filter_by(workspace_id=workspace_id, created_by=user.id, signal_id=marker).first()
        if existing is not None:
            tasks, runs, decisions = _outcome_rows(db, workspace_id, [existing.id])
            result = S.outcome(existing, tasks.get(existing.id, []), runs.get(existing.id, []), decisions.get(existing.id, 0))
            db.commit()
            return result
    from .work_services import create_outcome as create

    # The same service creates a confirmed Mind card's outcome.
    outcome = create(db, user, workspace_id, title=body.title, brief=body.brief, repo_ids=body.repo_ids,
                     acceptance=body.acceptance, harness=body.harness, priority=body.priority,
                     attachment_ids=body.attachment_ids, template=body.template, signal_id=marker)
    db.commit()
    orchestrator.wake()
    return S.outcome(outcome, [], [])


@router.get("/workspaces/{workspace_id}/outcomes/{outcome_id}/conversation")
def outcome_conversation(workspace_id: str, outcome_id: str, user: User = Depends(current_user),
                         db: Session = Depends(get_db)):
    """The outcome as a conversation: the request, the Lead's posts with the steps behind each, people's messages and
    decisions, in order (conversation.py)."""
    from .conversation import conversation

    _ws(db, user, workspace_id)
    o = db.get(Outcome, outcome_id)
    if o is None or o.workspace_id != workspace_id:
        raise HTTPException(404, "Outcome not found")
    return conversation(db, o, user.id)


@router.get("/workspaces/{workspace_id}/outcomes/{outcome_id}")
def outcome_detail(workspace_id: str, outcome_id: str, user: User = Depends(current_user), db: Session = Depends(get_db)):
    _ws(db, user, workspace_id)
    o = db.get(Outcome, outcome_id)
    if o is None or o.workspace_id != workspace_id:
        raise HTTPException(404, "Outcome not found")
    tasks = db.query(Task).filter(Task.outcome_id == o.id).order_by(Task.key).all()
    runs = db.query(Run).filter(Run.outcome_id == o.id).order_by(Run.started_at).all()
    repos = {r.id: r.full_name for r in db.query(Repository).filter(Repository.workspace_id == workspace_id)}
    plans = db.query(Plan).filter(Plan.outcome_id == o.id).order_by(Plan.version).all()
    decisions = db.query(Decision).filter(Decision.outcome_id == o.id).order_by(Decision.created_at).all()
    users = {u.id: (u.name or u.email or "") for u in db.query(User).filter(
        User.id.in_({m.author_id for m in db.query(Message).filter(Message.outcome_id == o.id)} |
                    {d.resolved_by for d in decisions if d.resolved_by} | ({o.created_by} if o.created_by else set())))}
    messages = db.query(Message).filter(Message.outcome_id == o.id).order_by(Message.created_at).all()
    events = (db.query(Event).filter(Event.outcome_id == o.id).order_by(Event.id.desc()).limit(400).all())[::-1]
    open_count = sum(S.actionable(d) for d in decisions)
    return {
        "guidance": outcome_guidance(db, workspace_id, o.id),
        "outcome": S.outcome(o, tasks, runs, open_count, S.outcome_sources(db, [o]).get(o.signal_id))
                   | {"created_by_name": _creator(db, users.get(o.created_by or "", ""), o)},
        "repositories": [{"id": rid, "full_name": repos.get(rid, "?")} for rid in o.repo_ids],
        "attachments": [S.artifact(a) for a in db.query(Artifact).filter(
            Artifact.workspace_id == workspace_id, Artifact.outcome_id == o.id, Artifact.kind == "attachment")
            .order_by(Artifact.created_at, Artifact.id)],
        "plans": [S.plan(p) for p in plans],
        "tasks": [S.task(t, [r for r in runs if r.task_id == t.id], repos.get(t.repo_id, "")) for t in tasks],
        "runs": [S.run(r) for r in runs],
        "decisions": [S.decision(d, {"resolved_by_name": users.get(d.resolved_by or "", "")}) for d in decisions],
        "messages": [S.message(m, users.get(m.author_id or "", "Factory")) for m in messages],
        "events": [event_dict(e) for e in events],
    }


def _creator(db: Session, name: str, outcome: Outcome) -> str:
    """The creator's name, "<person> via Mind" when a confirmed Mind card created the outcome (its creation event
    carries the card's attribution, whatever the outcome's source: a Mind fix outcome is still an improvement)."""
    created = (db.query(Event).filter(Event.outcome_id == outcome.id, Event.kind == "outcome_created")
               .order_by(Event.id).first()) if name else None
    return f"{name} via Mind" if created is not None and (created.data or {}).get("source") == "mind" else name


@router.post("/workspaces/{workspace_id}/outcomes/{outcome_id}/cancel")
async def cancel_outcome(workspace_id: str, outcome_id: str, user: User = Depends(current_user),
                         db: Session = Depends(get_db)):
    # Shared with Mind's cancel card: cancel and supersede, commit, publish, then stop the work and close its PRs.
    from .work_services import cancel_outcome as cancel, finish, publish_changes

    _ws(db, user, workspace_id, "member")
    payload, later = cancel(db, user, workspace_id, outcome_id)
    db.commit()
    publish_changes([payload])
    await finish(later, user)
    return {"ok": True}


@router.post("/workspaces/{workspace_id}/tasks/{task_id}/cancel")
async def cancel_task(workspace_id: str, task_id: str, user: User = Depends(current_user), db: Session = Depends(get_db)):
    from .work_services import cancel_task as cancel, finish, publish_changes

    _ws(db, user, workspace_id, "member")
    payload, later = cancel(db, user, workspace_id, task_id)
    db.commit()
    publish_changes([payload])
    await finish(later, user)
    return {"ok": True}


@router.post("/workspaces/{workspace_id}/tasks/{task_id}/take-over")
async def take_over_task(workspace_id: str, task_id: str, user: User = Depends(current_user),
                         db: Session = Depends(get_db)):
    """Stop the Factory on this task and hand its latest candidate to the person asking."""
    from .pipeline.handover import HandoverError, mark_handed_over, publish_candidate

    _ws(db, user, workspace_id, "member")
    t = db.get(Task, task_id)
    if t is None or t.workspace_id != workspace_id:
        raise HTTPException(404, "Task not found")
    try:
        info = mark_handed_over(task_id, user)
    except HandoverError as exc:
        raise HTTPException(409, str(exc))
    orchestrator.cancel_task(task_id)
    notify(workspace_id, "task", task_id, outcome_id=t.outcome_id)
    result = await publish_candidate(task_id, info)
    orchestrator.wake()
    return result


class MessageBody(BaseModel):
    body: str = Field(min_length=1, max_length=8000)
    task_id: str | None = None


@router.post("/workspaces/{workspace_id}/outcomes/{outcome_id}/messages")
def post_message(workspace_id: str, outcome_id: str, body: MessageBody, user: User = Depends(current_user),
                 db: Session = Depends(get_db)):
    from .work_services import publish_changes, send_guidance

    m, payload = send_guidance(db, user, workspace_id, outcome_id, body.body, body.task_id)
    db.commit()
    publish_changes([payload])
    return S.message(m, user.name or user.email or "")


@router.get("/workspaces/{workspace_id}/tasks/{task_id}/artifacts")
def task_artifacts(workspace_id: str, task_id: str, user: User = Depends(current_user), db: Session = Depends(get_db)):
    _ws(db, user, workspace_id)
    task = db.get(Task, task_id)
    if task is None or task.workspace_id != workspace_id:
        raise HTTPException(404, "Task not found")
    rows = db.query(Artifact).filter(Artifact.workspace_id == workspace_id, Artifact.task_id == task_id).order_by(
        Artifact.created_at).all()
    return [S.artifact(a) for a in rows]


@router.get("/workspaces/{workspace_id}/artifacts/{artifact_id}")
def artifact(workspace_id: str, artifact_id: str, user: User = Depends(current_user), db: Session = Depends(get_db)):
    _ws(db, user, workspace_id)
    a = db.get(Artifact, artifact_id)
    if a is None or a.workspace_id != workspace_id:
        raise HTTPException(404, "Artifact not found")
    if a.kind == "attachment":
        if a.outcome_id is None:
            if (a.data or {}).get("uploaded_by") != user.id or a.created_at <= utcnow() - A.STAGED_TTL:
                raise HTTPException(404, "Artifact not found")
        else:
            outcome = db.get(Outcome, a.outcome_id)
            if outcome is None or outcome.workspace_id != workspace_id:
                raise HTTPException(404, "Artifact not found")
    return S.artifact(a, content=True)


# --- attention & decisions -----------------------------------------------------------
@router.get("/workspaces/{workspace_id}/inbox")
def inbox(workspace_id: str, user: User = Depends(current_user), db: Session = Depends(get_db)):
    _, member = _ws(db, user, workspace_id)
    decisions = db.query(Decision).filter(Decision.workspace_id == workspace_id, Decision.status == "open").order_by(
        Decision.created_at).all()
    decisions = [d for d in decisions if S.actionable(d)]
    outcomes = {o.id: o for o in db.query(Outcome).filter(Outcome.workspace_id == workspace_id, Outcome.id.in_({d.outcome_id for d in decisions if d.outcome_id}))}
    tasks = {t.id: t for t in db.query(Task).filter(Task.workspace_id == workspace_id, Task.id.in_({d.task_id for d in decisions if d.task_id}))}
    since = member.last_viewed_at
    recent = []
    if since:
        recent = (db.query(Event).filter(Event.workspace_id == workspace_id, Event.created_at > since,
                                         Event.kind.in_(["outcome_completed", "merged", "plan_proposed", "published",
                                                         "decision_resolved", "task_completed", "outcome_created"]))
                  .order_by(Event.id.desc()).limit(50).all())
    change_outcomes = {o.id: o for o in db.query(Outcome).filter(Outcome.id.in_({e.outcome_id for e in recent if e.outcome_id}))}
    from .pipeline.lead import notes_for

    lead_notes = notes_for(db, decisions)
    alerts = db.query(Alert).filter(Alert.workspace_id == workspace_id, Alert.status == "open") \
        .order_by(Alert.opened_at.desc()).limit(50).all()
    about = {o.id: o for o in db.query(Outcome).filter(Outcome.id.in_({a.outcome_id for a in alerts if a.outcome_id}))}
    alert_tasks = {t.id: t.key for t in db.query(Task).filter(Task.id.in_({a.task_id for a in alerts if a.task_id}))}
    return {
        "alerts": [{"id": a.id, "kind": a.kind, "title": a.title, "body": a.body, "since": S.ts(a.since),
                    "opened_at": S.ts(a.opened_at), "outcome_id": a.outcome_id, "decision_id": a.decision_id,
                    "outcome_number": about[a.outcome_id].number if a.outcome_id in about else None,
                    "outcome_title": about[a.outcome_id].title if a.outcome_id in about else "",
                    "task_key": alert_tasks.get(a.task_id)} for a in alerts],
        "decisions": [S.decision(d, {
            "outcome_number": outcomes[d.outcome_id].number if d.outcome_id in outcomes else None,
            "outcome_title": outcomes[d.outcome_id].title if d.outcome_id in outcomes else "",
            "task_key": tasks[d.task_id].key if d.task_id in tasks else None,
            "pr_url": tasks[d.task_id].pr_url if d.task_id in tasks else "",
            "pr_number": tasks[d.task_id].pr_number if d.task_id in tasks else None,
            "priority": outcomes[d.outcome_id].priority if d.outcome_id in outcomes else 2,
            "lead_note": lead_notes.get(d.id),
        }) for d in decisions],
        "since": S.ts(since),
        "changes": [event_dict(e) | ({"outcome_number": change_outcomes[e.outcome_id].number,
                                       "outcome_title": change_outcomes[e.outcome_id].title}
                                      if e.outcome_id in change_outcomes else {}) for e in recent],
    }


@router.post("/workspaces/{workspace_id}/alerts/{alert_id}/dismiss")
def dismiss_alert(workspace_id: str, alert_id: str, user: User = Depends(current_user), db: Session = Depends(get_db)):
    """Hide an alert while its condition lasts. If the condition clears and comes back, that is a new alert."""
    _ws(db, user, workspace_id, "member")
    alert = db.get(Alert, alert_id)
    if alert is None or alert.workspace_id != workspace_id:
        raise HTTPException(404, "Alert not found")
    if alert.status == "open":
        alert.status, alert.dismissed_by, alert.dismissed_at = "dismissed", user.id, utcnow()
        record(workspace_id, "alert_dismissed", f"{user.name or user.email} dismissed an alert: {alert.title}",
               outcome_id=alert.outcome_id, task_id=alert.task_id, actor=user.id,
               data={"alert_id": alert.id, "kind": alert.kind}, session=db)
        db.commit()
        notify(workspace_id, "alert", workspace_id)
    return {"status": alert.status}


class Resolution(BaseModel):
    resolution: str
    comment: str = ""
    reusable: bool = False
    scope: str = "item"


@router.post("/workspaces/{workspace_id}/decisions/{decision_id}/resolve")
async def resolve_decision(workspace_id: str, decision_id: str, body: Resolution, user: User = Depends(current_user),
                           db: Session = Depends(get_db)):
    _ws(db, user, workspace_id, "member")
    d = db.get(Decision, decision_id)
    if d is None or d.workspace_id != workspace_id:
        raise HTTPException(404, "Decision not found")
    try:
        result = resolve(db, d, user, body.resolution, body.comment, body.reusable, body.scope)
    except DecisionError as exc:
        raise HTTPException(400, str(exc))
    info = result.pop("handover", None)
    if info:
        from .pipeline.handover import publish_candidate

        result["handover"] = await publish_candidate(d.task_id, info)
    dropped = result.pop("dropped", None)
    if dropped:
        from .pipeline.handover import close_dropped_pull

        result["pr_closed"] = await close_dropped_pull(dropped, user.name or user.email, "Abandoned")
    return result


class Overrule(BaseModel):
    resolution: str = Field(min_length=1, max_length=60)
    answer: str = Field(min_length=1, max_length=8000)
    reason: str = Field(min_length=1, max_length=8000)


@router.post("/workspaces/{workspace_id}/decisions/{decision_id}/overrule")
def overrule_decision(workspace_id: str, decision_id: str, body: Overrule,
                      user: User = Depends(current_user), db: Session = Depends(get_db)):
    _ws(db, user, workspace_id, "member")
    d = db.get(Decision, decision_id)
    if d is None or d.workspace_id != workspace_id:
        raise HTTPException(404, "Decision not found")
    try:
        return overrule(db, d, user, body.resolution, body.answer, body.reason)
    except DecisionError as exc:
        raise HTTPException(400, str(exc)) from None


@router.get("/workspaces/{workspace_id}/decisions")
def decision_library(workspace_id: str, user: User = Depends(current_user), db: Session = Depends(get_db)):
    _ws(db, user, workspace_id)
    rows = db.query(Decision).filter(Decision.workspace_id == workspace_id,
        (Decision.status != "open") | Decision.review.has(
            (DecisionReview.workspace_id == workspace_id) & (DecisionReview.finished_at.is_not(None)) &
            (DecisionReview.effective_verdict == "hold") &
            DecisionReview.state.notin_(("pending", "running")))).order_by(
        # Explicit NULL ordering keeps open held reviews ahead of history on
        # both SQLite and Postgres, before the history limit is applied.
        case((Decision.resolved_at.is_(None), 0), else_=1),
        Decision.resolved_at.desc(), Decision.created_at.desc(), Decision.id).limit(300).all()
    names = {u.id: (u.name or u.email) for u in db.query(User).filter(User.id.in_({d.resolved_by for d in rows if d.resolved_by}))}
    return [S.decision(d, {"resolved_by_name": names.get(d.resolved_by or "", "")}) for d in rows]


class PrecedentUpdate(BaseModel):
    reusable: bool
    scope: str = "workspace"


@router.patch("/workspaces/{workspace_id}/decisions/{decision_id}")
def update_precedent(workspace_id: str, decision_id: str, body: PrecedentUpdate, user: User = Depends(current_user),
                     db: Session = Depends(get_db)):
    _ws(db, user, workspace_id, "admin")
    d = db.get(Decision, decision_id)
    if d is None or d.workspace_id != workspace_id or d.status == "open":
        raise HTTPException(404, "Decision not found")
    d.reusable = body.reusable
    d.scope = body.scope if body.scope in ("item", "repository", "workspace") else "workspace"
    db.commit()
    record(workspace_id, "precedent_updated", f"{user.name or user.email} {'kept' if body.reusable else 'retired'} a precedent",
           actor=user.id, data={"decision_id": d.id})
    return {"ok": True}


class ScoreRequest(BaseModel):
    scorer_id: str = Field(min_length=1, max_length=80)


@router.get("/workspaces/{workspace_id}/scorers")
def scorer_definitions(workspace_id: str, user: User = Depends(current_user), db: Session = Depends(get_db)):
    from .scorers import definitions
    ws, _ = _ws(db, user, workspace_id)
    try:
        return [d.model_dump() | {"version": d.version} for d in definitions(ws.settings or {})]
    except ValueError:
        raise HTTPException(422, "Invalid scorer configuration") from None


def _score_run(db, user, workspace_id, run_id, minimum="viewer"):
    ws, _ = _ws(db, user, workspace_id, minimum)
    run = db.get(Run, run_id)
    if not run or run.workspace_id != workspace_id:
        raise HTTPException(404, "Run not found")
    return ws, run


@router.get("/workspaces/{workspace_id}/runs/{run_id}/scores")
def run_scores(workspace_id: str, run_id: str, limit: int = Query(20, ge=1, le=50),
               offset: int = Query(0, ge=0), user: User = Depends(current_user), db: Session = Depends(get_db)):
    from .models import ScorerResult
    from .scorers import result_dict
    _score_run(db, user, workspace_id, run_id)
    return [result_dict(r) for r in db.query(ScorerResult).filter_by(workspace_id=workspace_id, run_id=run_id)
            .order_by(ScorerResult.created_at.desc(), ScorerResult.id.desc()).offset(offset).limit(limit)]


@router.post("/workspaces/{workspace_id}/runs/{run_id}/scores")
async def score_run(workspace_id: str, run_id: str, body: ScoreRequest,
                    user: User = Depends(current_user), db: Session = Depends(get_db)):
    from .models import ScorerResult
    from .scorers import definitions, execute, reserve, result_dict
    ws, run = _score_run(db, user, workspace_id, run_id, "member")
    try:
        configured = definitions(ws.settings or {})
    except ValueError:
        raise HTTPException(422, "Invalid scorer configuration") from None
    definition = next((d for d in configured if d.id == body.scorer_id), None)
    if definition is None:
        raise HTTPException(404, "Scorer not found")
    try:
        row = reserve(db, run, definition)
    except ValueError as exc:
        raise HTTPException(422, str(exc)) from None
    result_id = row.id
    db.commit()
    await execute(result_id)
    db.expire_all()
    return result_dict(db.get(ScorerResult, result_id))


class ScorerCostRequest(BaseModel):
    cost_usd: float = Field(ge=0, allow_inf_nan=False)
    evidence: str = Field(min_length=1, max_length=2000, pattern=r".*\S.*")


def _score_attempt(db, workspace_id, run_id, score_id, attempt_id=None):
    from .models import ScorerAttempt, ScorerResult
    score = db.get(ScorerResult, score_id)
    if not score or score.workspace_id != workspace_id or score.run_id != run_id:
        raise HTTPException(404, "Score not found")
    if attempt_id is None:
        return score
    attempt = db.get(ScorerAttempt, attempt_id)
    if not attempt or attempt.workspace_id != workspace_id or attempt.result_id != score_id:
        raise HTTPException(404, "Scorer attempt not found")
    return attempt


@router.get("/workspaces/{workspace_id}/runs/{run_id}/scores/{score_id}/attempts")
def scorer_attempts(workspace_id: str, run_id: str, score_id: str,
                    limit: int = Query(20, ge=1, le=50), offset: int = Query(0, ge=0),
                    user: User = Depends(current_user), db: Session = Depends(get_db)):
    from .models import ScorerAttempt
    from .scorers import attempt_dict
    _score_run(db, user, workspace_id, run_id)
    _score_attempt(db, workspace_id, run_id, score_id)
    return [attempt_dict(a) for a in db.query(ScorerAttempt).filter_by(
        workspace_id=workspace_id, result_id=score_id).order_by(
            ScorerAttempt.started_at.desc(), ScorerAttempt.id.desc()).offset(offset).limit(limit)]


@router.post("/workspaces/{workspace_id}/runs/{run_id}/scores/{score_id}/attempts/{attempt_id}/cost")
def reconcile_scorer_cost(workspace_id: str, run_id: str, score_id: str, attempt_id: str,
                          body: ScorerCostRequest, user: User = Depends(current_user), db: Session = Depends(get_db)):
    """Explicit, audited billing reconciliation, never guess zero for a possibly paid call."""
    from .scorers import aggregate, attempt_dict
    _score_run(db, user, workspace_id, run_id, "admin")
    attempt = _score_attempt(db, workspace_id, run_id, score_id, attempt_id)
    if attempt.finished_at is None or attempt.cost_known:
        raise HTTPException(409, "Only finished attempts with unknown cost can be reconciled")
    attempt.cost_usd, attempt.cost_known = round(body.cost_usd, 6), True
    attempt.cost_reconciliation = {"actor": user.id, "at": utcnow().isoformat(), "evidence": body.evidence}
    db.flush()
    aggregate(db, score_id)
    record(workspace_id, "scorer_cost_reconciled", "Scorer charge reconciled from provider billing",
           actor=user.id, run_id=run_id, session=db,
           data={"attempt_id": attempt_id, "provider": attempt.provider, "cost_usd": attempt.cost_usd})
    db.commit()
    return attempt_dict(attempt)


# --- runs, activity, signals -----------------------------------------------------------
@router.get("/workspaces/{workspace_id}/runs")
def list_runs(workspace_id: str, limit: int = 100, config_version: str | None = None, user: User = Depends(current_user), db: Session = Depends(get_db)):
    _ws(db, user, workspace_id)
    query = db.query(Run).filter(Run.workspace_id == workspace_id)
    if config_version is not None:
        query = query.filter(Run.config_version == config_version)
    rows = query.order_by(Run.started_at.desc()).limit(max(1, min(limit, 500))).all()
    outcomes = {o.id: o for o in db.query(Outcome).filter(Outcome.id.in_({r.outcome_id for r in rows}))}
    tasks = {t.id: t for t in db.query(Task).filter(Task.id.in_({r.task_id for r in rows if r.task_id}))}
    from .scorers import run_summaries
    scores, totals = run_summaries(db, workspace_id, [r.id for r in rows])
    return [S.run(r) | {"scores": scores.get(r.id, []),
                        "score_summary": totals.get(r.id, {"count": 0, "cost_usd": 0, "cost_known": True}),
                        "outcome_number": outcomes[r.outcome_id].number if r.outcome_id in outcomes else None,
                        "outcome_title": outcomes[r.outcome_id].title if r.outcome_id in outcomes else "",
                        "task_key": tasks[r.task_id].key if r.task_id in tasks else None} for r in rows]


@router.get("/workspaces/{workspace_id}/stats")
def stats(workspace_id: str, user: User = Depends(current_user), db: Session = Depends(get_db)):
    _ws(db, user, workspace_id)
    from .metrics import benchmark, first_attempt_result, human_message, workspace_metrics
    from .decision_judges import judge_metrics

    outcomes = [o for o in db.query(Outcome).filter(Outcome.workspace_id == workspace_id) if not benchmark(o)]
    outcome_ids = {o.id for o in outcomes}
    tasks = [t for t in db.query(Task).filter(Task.workspace_id == workspace_id)
             if t.outcome_id in outcome_ids and not benchmark(t)]
    task_ids = {t.id for t in tasks}
    runs = [r for r in db.query(Run).filter(Run.workspace_id == workspace_id)
            if r.outcome_id in outcome_ids and (not r.task_id or r.task_id in task_ids) and not benchmark(r)]
    from .models import ScorerResult
    from sqlalchemy import func

    # Score history is unbounded; aggregate numeric usage without loading judge
    # definitions or reasoning into this frequently refreshed endpoint.
    score_cost, score_tokens, score_unknown = db.query(
        func.coalesce(func.sum(ScorerResult.cost_usd), 0),
        func.coalesce(func.sum(ScorerResult.input_tokens + ScorerResult.output_tokens), 0),
        func.count(ScorerResult.id).filter(ScorerResult.cost_known.is_(False)),
    ).filter(ScorerResult.workspace_id == workspace_id,
             ScorerResult.run_id.in_([r.id for r in runs])).one()
    total_cost = sum(r.cost_usd for r in runs) + score_cost
    cost_known = all(r.cost_known for r in runs) and score_unknown == 0
    decisions = db.query(Decision).filter(Decision.workspace_id == workspace_id).all()
    merged = [t for t in tasks if t.merge_sha]
    by_status: dict[str, int] = {}
    for o in outcomes:
        by_status[o.status] = by_status.get(o.status, 0) + 1
    metrics = workspace_metrics(db, workspace_id)
    # The legacy Runs UI divides this counter by tasks.merged. Keep that
    # population while using durable attempt/check history instead of list length.
    merged_ids = {t.id for t in merged}
    journals: dict[str, list[Event]] = {}
    if merged_ids:
        for e in db.query(Event).filter(Event.workspace_id == workspace_id, Event.task_id.in_(merged_ids)).order_by(Event.id):
            journals.setdefault(e.task_id, []).append(e)
    repos = {r.id: r for r in db.query(Repository).filter(Repository.workspace_id == workspace_id)}
    first_pass = sum(first_attempt_result(t, journals.get(t.id, []),
                                         (repos[t.repo_id].config or {}).get("checks", [])
                                         if t.repo_id in repos else None) is True for t in merged)
    return {
        "metrics": metrics,
        "decision_judges": judge_metrics(db, workspace_id),
        "outcomes": by_status,
        "tasks": {"total": len(tasks), "completed": sum(t.status == "completed" for t in tasks), "merged": len(merged),
                  "first_attempt": first_pass, "smoke_failed": sum(t.status == "smoke_failed" for t in tasks)},
        "cost": {"usd": round(total_cost, 2), "known": cost_known,
                 "per_completed_task": round(total_cost / max(1, sum(t.status == "completed" for t in tasks)), 2)},
        "tokens": sum(r.input_tokens + r.output_tokens for r in runs) + score_tokens,
        "human": {"decisions": len(decisions), "open": sum(d.status == "open" for d in decisions),
                  "interventions": sum(human_message(m) for m in db.query(Message).filter(
                      Message.workspace_id == workspace_id, Message.author_kind == "user"))},
        # A session a restart interrupted says nothing about the harness: it counts neither for nor against it.
        "harnesses": {h: {"runs": sum(r.harness == h and r.status != "interrupted" for r in runs),
                          "succeeded": sum(r.harness == h and r.status == "succeeded" for r in runs)}
                      for h in ("native", "codex", "anton")},
    }


@router.get("/workspaces/{workspace_id}/events")
def events(workspace_id: str, after: int = 0, limit: int = 200, user: User = Depends(current_user),
           db: Session = Depends(get_db)):
    _ws(db, user, workspace_id)
    query = db.query(Event).filter(Event.workspace_id == workspace_id)
    if after:
        query = query.filter(Event.id > after).order_by(Event.id).limit(min(limit, 1000))
        return [event_dict(e) for e in query]
    return [event_dict(e) for e in query.order_by(Event.id.desc()).limit(min(limit, 1000))][::-1]


@router.get("/workspaces/{workspace_id}/messages")
def list_messages(workspace_id: str, before: int | None = None, limit: int = 50, party: str | None = None,
                  intent: str | None = None, outcome_id: str | None = None, user: User = Depends(current_user),
                  db: Session = Depends(get_db)):
    """The Message Log: who told whom what, newest first (see messages.py)."""
    from . import messages

    _ws(db, user, workspace_id)
    if party is not None and party not in messages.GROUPS:
        raise HTTPException(422, f"party must be one of {', '.join(messages.GROUPS)}")
    if intent is not None and intent not in messages.INTENTS:
        raise HTTPException(422, f"intent must be one of {', '.join(messages.INTENTS)}")
    return messages.log(db, workspace_id, before=before, limit=limit, group=party, intent=intent,
                        outcome_id=outcome_id)


@router.get("/workspaces/{workspace_id}/signals")
def list_signals(workspace_id: str, user: User = Depends(current_user), db: Session = Depends(get_db)):
    _ws(db, user, workspace_id)
    return [S.signal(s) for s in db.query(Signal).filter(Signal.workspace_id == workspace_id).order_by(
        Signal.last_seen_at.desc()).limit(300)]


@router.post("/workspaces/{workspace_id}/signals/sync")
async def sync_signals(workspace_id: str, user: User = Depends(current_user), db: Session = Depends(get_db)):
    _ws(db, user, workspace_id, "member")
    try:
        return await sync_github(db, workspace_id, resolve_token(workspace_id))
    except GitHubError as exc:
        raise HTTPException(400, str(exc))


class SignalAction(BaseModel):
    action: str  # accept|ignore
    title: str | None = None
    brief: str | None = None
    note: str | None = Field(default=None, max_length=8000)  # edited team note for an improvement


@router.post("/workspaces/{workspace_id}/signals/analyze")
def analyze_signals(workspace_id: str, user: User = Depends(current_user), db: Session = Depends(get_db)):
    _ws(db, user, workspace_id, "member")
    result = insights.analyze(db, workspace_id)
    db.commit()
    return result


class FeedbackIn(BaseModel):
    title: str = Field(min_length=3, max_length=300)
    body: str = Field(default="", max_length=8000)
    url: str = Field(default="", max_length=400)
    repo: str | None = Field(default=None, max_length=200)  # owner/name of a connected repository
    source_id: str | None = Field(default=None, max_length=200)  # the product's own id; repeats are counted
    reporter: str | None = Field(default=None, max_length=200)


def _record_feedback(db: Session, workspace_id: str, body: FeedbackIn, via: str) -> Signal:
    repo_id = None
    if body.repo:
        repo = db.query(Repository).filter(Repository.workspace_id == workspace_id,
                                           Repository.full_name == body.repo).one_or_none()
        repo_id = repo.id if repo else None
    text = body.body.strip()
    if body.reporter:
        text += f"\n\nReported by {body.reporter} via {via}."
    key = body.source_id or " ".join(body.title.lower().split())
    sig, _created = upsert_signal(db, workspace_id, source="feedback", title=body.title.strip(), body=text,
                                  url=body.url, external_id=key, repo_id=repo_id)
    if sig.status in ("ignored", "resolved"):
        sig.status = "new"  # it came back
    db.commit()
    notify(workspace_id, "signal", sig.id)
    return sig


@router.post("/workspaces/{workspace_id}/signals")
def add_feedback(workspace_id: str, body: FeedbackIn, user: User = Depends(current_user),
                 db: Session = Depends(get_db)):
    _ws(db, user, workspace_id, "member")
    return S.signal(_record_feedback(db, workspace_id, body, user.name or user.email))


def _feedback_hash(key: str) -> str:
    import hashlib

    return hashlib.sha256(key.encode()).hexdigest()


@router.post("/workspaces/{workspace_id}/feedback-key")
def rotate_feedback_key(workspace_id: str, user: User = Depends(current_user), db: Session = Depends(get_db)):
    """Create (or replace) the key a product uses to send user feedback here. Shown once."""
    import secrets as pysecrets

    _ws(db, user, workspace_id, "admin")
    key = "aff_" + pysecrets.token_urlsafe(32)
    sec = db.query(Secret).filter(Secret.workspace_id == workspace_id, Secret.kind == "feedback_key").one_or_none()
    if sec is None:
        sec = Secret(workspace_id=workspace_id, kind="feedback_key", ciphertext="")
        db.add(sec)
    sec.ciphertext, sec.hint, sec.status = _feedback_hash(key), mask(key), "valid"
    db.commit()
    record(workspace_id, "feedback_key_rotated", "Product feedback key created", actor=user.id)
    return {"key": key, "hint": sec.hint, "endpoint": f"/api/feedback/{workspace_id}"}


@router.get("/workspaces/{workspace_id}/feedback-key")
def feedback_key_status(workspace_id: str, user: User = Depends(current_user), db: Session = Depends(get_db)):
    _ws(db, user, workspace_id)
    sec = db.query(Secret).filter(Secret.workspace_id == workspace_id, Secret.kind == "feedback_key").one_or_none()
    return {"configured": sec is not None, "hint": sec.hint if sec else "", "endpoint": f"/api/feedback/{workspace_id}",
            "updated_at": S.ts(sec.updated_at) if sec else None}


@router.post("/feedback/{workspace_id}")
def ingest_feedback(workspace_id: str, body: FeedbackIn, request: Request, db: Session = Depends(get_db)):
    """Product feedback from a team's own app, authenticated with the workspace feedback key."""
    import hmac

    supplied = request.headers.get("authorization", "").removeprefix("Bearer ").strip()
    sec = db.query(Secret).filter(Secret.workspace_id == workspace_id, Secret.kind == "feedback_key").one_or_none()
    if not supplied or sec is None or not hmac.compare_digest(sec.ciphertext, _feedback_hash(supplied)):
        raise HTTPException(401, "Invalid feedback key")
    sig = _record_feedback(db, workspace_id, body, "the product")
    return {"id": sig.id, "count": sig.count, "status": sig.status}


@router.post("/workspaces/{workspace_id}/signals/{signal_id}")
def act_on_signal(workspace_id: str, signal_id: str, body: SignalAction, user: User = Depends(current_user),
                  db: Session = Depends(get_db)):
    _ws(db, user, workspace_id, "member")
    sig = db.get(Signal, signal_id)
    if sig is None or sig.workspace_id != workspace_id:
        raise HTTPException(404, "Signal not found")
    if sig.source == "linear_issue":
        if body.action == "accept" and sig.outcome_id:
            return S.signal(sig)
        raise HTTPException(409, "Linear signals are managed by configured Linear intake")
    if body.action == "ignore":
        sig.data = {key: value for key, value in (sig.data or {}).items() if key != "acceptance_reason"}
        sig.status = "ignored"
        db.commit()
        return S.signal(sig)
    if body.action != "accept":
        raise HTTPException(400, "Unknown action")
    if sig.source == "improvement":
        _ws(db, user, workspace_id, "admin")
        try:
            insights.apply(db, sig, user, note=body.note)
        except ValueError as exc:
            raise HTTPException(400, str(exc))
        db.commit()
        notify(workspace_id, "signal", sig.id)
        return S.signal(sig)
    sig.data = {key: value for key, value in (sig.data or {}).items() if key != "acceptance_reason"}
    if sig.outcome_id:
        db.commit()
        return S.signal(sig)  # accepting again must not create duplicate work
    worker = (sig.data or {}).get("worker") if sig.source == "github_issue" else None
    if worker not in ("native", "codex", "anton"):
        worker = None
    repo_ids = [sig.repo_id] if sig.repo_id else [r.id for r in db.query(Repository).filter(
        Repository.workspace_id == workspace_id).limit(1)]
    outcome = outcome_from_text(db, workspace_id, user, body.title or sig.title,
                                (body.brief or sig.body or "") + (f"\n\nSource: {sig.url}" if sig.url else ""),
                                repo_ids, source="signal", signal_id=sig.id, harness=worker)
    sig.status, sig.outcome_id = "accepted", outcome.id
    db.commit()
    orchestrator.wake()
    return S.signal(sig)


# --- live updates -----------------------------------------------------------------------
@router.get("/workspaces/{workspace_id}/stream")
async def stream(workspace_id: str, request: Request, user: User = Depends(current_user),
                 db: Session = Depends(get_db)):
    _ws(db, user, workspace_id)
    db.close()
    sub_id, queue = bus.subscribe(workspace_id)

    async def gen():
        try:
            yield "retry: 3000\n\n"
            while True:
                if await request.is_disconnected():
                    break
                try:
                    item = await asyncio.wait_for(queue.get(), timeout=20)
                    yield f"data: {json.dumps(item, default=str)}\n\n"
                except asyncio.TimeoutError:
                    yield ": keepalive\n\n"
        finally:
            bus.unsubscribe(sub_id)

    return StreamingResponse(gen(), media_type="text/event-stream",
                             headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


# Confirmation adapters are deliberately not model tools.
from .mind.actions import router as mind_actions_router
router.include_router(mind_actions_router)
