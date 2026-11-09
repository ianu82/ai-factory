"""HTTP API for the Factory app."""

from __future__ import annotations

import asyncio
import json
import re
from typing import Any, Literal

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from . import insights
from . import serializers as S
from .auth import ROLE_RANK, auth_methods, current_user, require_member
from .config import get_settings
from .db import get_db, utcnow
from .decisions import DecisionError, resolve
from .events import bus, event_dict, notify, record
from .github import GitHub, GitHubError, resolve_token
from .harness import capabilities
from .inference import PROVIDERS, InferenceError, ModelClient, ProviderConfig, resolve_provider
from .linear import Linear, LinearError
from .memory import MemoryScopeError, MemoryService
from .models import (Artifact, Decision, Event, Membership, Message, Organization, Outcome, Plan, Repository, Run,
                     Secret, Signal, Task, User, Workspace)
from .pipeline.orchestrator import orchestrator
from .pipeline.runtime import DEFAULT_SETTINGS, merged_settings
from .repo_detect import detect
from .sandbox import isolation_available
from .security import encrypt, mask, redact
from .signals import outcome_from_text, sync_github, upsert_signal

router = APIRouter(prefix="/api")
SECRET_KINDS = {"mindshub_api_key", "openai_api_key", "github_token", "linear_api_key"}


def _ws(db: Session, user: User, workspace_id: str, minimum: str = "viewer") -> tuple[Workspace, Membership]:
    member = require_member(db, user, workspace_id, minimum)
    ws = db.get(Workspace, workspace_id)
    if ws is None:
        raise HTTPException(404, "Workspace not found")
    return ws, member


# --- identity ---------------------------------------------------------------
@router.get("/health")
def health():
    import os

    from . import __version__

    return {"ok": True, "version": __version__, "commit": os.environ.get("RAILWAY_GIT_COMMIT_SHA", ""),
            "orchestrator": orchestrator.status()}


@router.get("/meta")
def meta():
    return {"auth": auth_methods(), "harnesses": capabilities(),
            "sandbox": {"isolation": "per-job identity" if isolation_available() else "shared (development)"},
            "version": "2.0.0"}


@router.get("/me")
def me(user: User = Depends(current_user), db: Session = Depends(get_db)):
    rows = (db.query(Membership, Workspace, Organization).join(Workspace, Membership.workspace_id == Workspace.id)
            .join(Organization, Workspace.org_id == Organization.id).filter(Membership.user_id == user.id).all())
    return {"user": S.user(user),
            "workspaces": [S.workspace(w, m.role, o.name) for m, w, o in rows],
            "organizations": [{"id": o.id, "name": o.name} for o in {o.id: o for _, _, o in rows}.values()]}


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
                   settings=json.loads(json.dumps(DEFAULT_SETTINGS)))
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


def _clean_settings(raw: dict[str, Any]) -> dict[str, Any]:
    merged = merged_settings(raw)
    merged["concurrency"] = max(1, min(int(merged.get("concurrency", 3)), 50))
    merged["max_attempts"] = max(1, min(int(merged.get("max_attempts", 3)), 8))
    for role in ("builder", "verifier", "planner"):
        if merged["harness"].get(role) not in ("native", "codex", "anton"):
            merged["harness"][role] = "native"
    for key in ("plan", "merge", "release"):
        if merged["autonomy"].get(key) not in ("auto", "approval"):
            merged["autonomy"][key] = DEFAULT_SETTINGS["autonomy"][key]
    if merged.get("provider") not in PROVIDERS:
        merged["provider"] = "mindshub"
    return merged


@router.patch("/workspaces/{workspace_id}")
def update_workspace(workspace_id: str, body: WorkspaceUpdate, user: User = Depends(current_user),
                     db: Session = Depends(get_db)):
    ws, _ = _ws(db, user, workspace_id, "admin")
    changes = {}
    if body.name is not None:
        ws.name = body.name.strip()[:120] or ws.name
    if body.description is not None:
        ws.description = body.description
    if body.settings is not None:
        before = merged_settings(ws.settings)
        ws.settings = _clean_settings({**before, **body.settings})
        changes = {k: ws.settings[k] for k in body.settings if before.get(k) != ws.settings.get(k)}
    if body.paused is not None:
        ws.paused = body.paused
        changes["paused"] = body.paused
    db.commit()
    record(ws.id, "settings_changed", f"{user.name or user.email} updated workspace settings", actor=user.id,
           data={"changes": changes})
    orchestrator.wake()
    return S.workspace(ws, require_member(db, user, workspace_id).role)


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
    return {"kind": kind, "label": labels[kind], "configured": True, "hint": sec.hint, "status": sec.status,
            "detail": sec.status_detail, "tested_at": S.ts(sec.tested_at), "updated_at": S.ts(sec.updated_at),
            "metadata": sec.metadata_json or {}}


@router.get("/workspaces/{workspace_id}/credentials")
def credentials(workspace_id: str, user: User = Depends(current_user), db: Session = Depends(get_db)):
    _ws(db, user, workspace_id)
    secrets = {s.kind: s for s in db.query(Secret).filter(Secret.workspace_id == workspace_id)}
    settings = get_settings()
    return {"items": [_secret_view(k, secrets.get(k)) for k in ("mindshub_api_key", "github_token", "openai_api_key", "linear_api_key")],
            "server_fallbacks": {"github_token": bool(settings.bootstrap_github_token),
                                 "openai_api_key": bool(settings.bootstrap_openai_key),
                                 "mindshub_api_key": bool(settings.bootstrap_mindshub_key)}}


async def _test_secret(kind: str, value: str) -> tuple[str, str, dict[str, Any]]:
    settings = get_settings()
    try:
        if kind == "linear_api_key":
            viewer = await Linear(value).viewer()
            name = redact(viewer["name"], (value,))
            return "valid", redact(f"Connected as {name}", (value,)), {"name": name}
        if kind == "mindshub_api_key":
            from .auth import AuthError, mindshub_identity

            try:
                identity = await mindshub_identity(value)
            except AuthError as exc:
                return "invalid", str(exc), {}
            client = ModelClient(ProviderConfig("mindshub", settings.mindshub_api_base, value, "MindsHub Inference"))
            models = await client.list_models()
            return "valid", f"Connected to MindsHub · {len(models)} models available", {
                "organization_id": identity.get("organization_id"), "organization_name": identity.get("organization_name"),
                "workspace_id": identity.get("workspace_id"), "models": models[:80]}
        if kind == "openai_api_key":
            client = ModelClient(ProviderConfig("openai", settings.openai_api_base, value, "OpenAI"))
            models = await client.list_models()
            return "valid", f"Connected to OpenAI · {len(models)} models", {"models": models[:80]}
        if kind == "github_token":
            gh = GitHub(value)
            me = await gh.user()
            return "valid", f"Connected as {me.get('login')}", {"login": me.get("login")}
    except LinearError as exc:
        return "invalid" if exc.category == "authentication" else "untested", redact(str(exc), (value,)), {}
    except (InferenceError, GitHubError) as exc:
        return "invalid", str(exc), {}
    except Exception as exc:
        detail = f"Could not test right now: {type(exc).__name__}"
        return "untested", redact(detail, (value,)) if kind == "linear_api_key" else detail, {}
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
    if kind == "mindshub_api_key" and not value.startswith("mdb_"):
        raise HTTPException(400, "MindsHub API keys start with mdb_")
    status, detail, metadata = await _test_secret(kind, value)
    sec = db.query(Secret).filter(Secret.workspace_id == workspace_id, Secret.kind == kind).one_or_none()
    if sec is None:
        sec = Secret(workspace_id=workspace_id, kind=kind, ciphertext="")
        db.add(sec)
    sec.ciphertext = encrypt(value)
    sec.hint = mask(value)
    sec.status, sec.status_detail, sec.metadata_json = status, detail, metadata
    sec.tested_at = utcnow()
    sec.updated_by = user.id
    db.commit()
    record(workspace_id, "credential_saved", f"{user.name or user.email} saved the {kind.replace('_', ' ')} ({status})",
           actor=user.id, level="success" if status == "valid" else "warn", data={"kind": kind, "status": status})
    return _secret_view(kind, sec)


@router.post("/workspaces/{workspace_id}/credentials/{kind}/test")
async def test_credential(workspace_id: str, kind: str, user: User = Depends(current_user), db: Session = Depends(get_db)):
    _ws(db, user, workspace_id, "admin" if kind == "linear_api_key" else "member")
    sec = db.query(Secret).filter(Secret.workspace_id == workspace_id, Secret.kind == kind).one_or_none()
    if sec is None:
        raise HTTPException(404, "Not configured")
    from .security import decrypt

    status, detail, metadata = await _test_secret(kind, decrypt(sec.ciphertext))
    sec.status, sec.status_detail, sec.tested_at = status, detail, utcnow()
    if metadata:
        sec.metadata_json = metadata
    db.commit()
    return _secret_view(kind, sec)


@router.delete("/workspaces/{workspace_id}/credentials/{kind}")
def delete_credential(workspace_id: str, kind: str, user: User = Depends(current_user), db: Session = Depends(get_db)):
    _ws(db, user, workspace_id, "admin")
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
    setup: list[str] | None = None
    checks: list[str] | None = None
    qa: RepoQACommands | None = None
    release: RepoReleaseSelection | None = None


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
    for key in ("setup", "checks", "smoke", "protected_paths"):
        if key in patch:
            cfg[key] = [str(x).strip() for x in patch[key] or [] if str(x).strip()]
    for key in ("context", "signal_label"):
        if key in patch:
            cfg[key] = str(patch[key] or "")[:8000]
    for key in ("merge", "release", "qa", "recovery", "build"):
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
    db.add(repo)
    db.commit()
    record(workspace_id, "repository_connected", f"Connected {repo.full_name}", actor=user.id,
           data={"detected": config.get("detected", [])})
    return S.repository(repo)


class RepoUpdate(BaseModel):
    config: dict[str, Any]


@router.patch("/workspaces/{workspace_id}/repositories/{repo_id}")
def update_repo(workspace_id: str, repo_id: str, body: RepoUpdate, user: User = Depends(current_user),
                db: Session = Depends(get_db)):
    _ws(db, user, workspace_id, "admin")
    repo = db.get(Repository, repo_id)
    if repo is None or repo.workspace_id != workspace_id:
        raise HTTPException(404, "Repository not found")
    repo.config = _clean_repo_config(repo.config or {}, body.config)
    db.commit()
    record(workspace_id, "repository_configured", f"Updated {repo.full_name} configuration", actor=user.id)
    return S.repository(repo)


@router.delete("/workspaces/{workspace_id}/repositories/{repo_id}")
def disconnect_repo(workspace_id: str, repo_id: str, user: User = Depends(current_user), db: Session = Depends(get_db)):
    _ws(db, user, workspace_id, "admin")
    repo = db.get(Repository, repo_id)
    if repo is None or repo.workspace_id != workspace_id:
        raise HTTPException(404, "Repository not found")
    if db.query(Task).filter(Task.repo_id == repo_id, Task.status.in_(["queued", "running"])).first():
        raise HTTPException(409, "Work is running on this repository")
    MemoryService(workspace_id, repo_id).delete_repository_entries(session=db, actor=user.id)
    db.delete(repo)
    db.commit()
    return {"ok": True}


# --- outcomes ---------------------------------------------------------------------
class OutcomeCreate(BaseModel):
    title: str = Field(min_length=3, max_length=300)
    brief: str = ""
    acceptance: list[str] = []
    repo_ids: list[str] = Field(min_length=1)
    harness: str | None = None
    priority: int = 2


def _outcome_rows(db: Session, outcome_ids: list[str]) -> tuple[dict, dict, dict]:
    tasks: dict[str, list[Task]] = {}
    runs: dict[str, list[Run]] = {}
    decisions: dict[str, int] = {}
    if outcome_ids:
        for t in db.query(Task).filter(Task.outcome_id.in_(outcome_ids)):
            tasks.setdefault(t.outcome_id, []).append(t)
        for r in db.query(Run).filter(Run.outcome_id.in_(outcome_ids)):
            runs.setdefault(r.outcome_id, []).append(r)
        for d in db.query(Decision).filter(Decision.outcome_id.in_(outcome_ids), Decision.status == "open"):
            decisions[d.outcome_id] = decisions.get(d.outcome_id, 0) + 1
    return tasks, runs, decisions


@router.get("/workspaces/{workspace_id}/outcomes")
def list_outcomes(workspace_id: str, status: str = "", limit: int = 200, user: User = Depends(current_user),
                  db: Session = Depends(get_db)):
    _ws(db, user, workspace_id)
    query = db.query(Outcome).filter(Outcome.workspace_id == workspace_id)
    if status:
        query = query.filter(Outcome.status.in_(status.split(",")))
    rows = query.order_by(Outcome.updated_at.desc()).limit(min(limit, 500)).all()
    tasks, runs, decisions = _outcome_rows(db, [o.id for o in rows])
    return [S.outcome(o, tasks.get(o.id, []), runs.get(o.id, []), decisions.get(o.id, 0)) for o in rows]


@router.post("/workspaces/{workspace_id}/outcomes")
def create_outcome(workspace_id: str, body: OutcomeCreate, user: User = Depends(current_user),
                   db: Session = Depends(get_db)):
    _ws(db, user, workspace_id, "member")
    repos = db.query(Repository).filter(Repository.workspace_id == workspace_id, Repository.id.in_(body.repo_ids)).all()
    if len(repos) != len(set(body.repo_ids)):
        raise HTTPException(400, "Unknown repository")
    if body.harness and body.harness not in ("native", "codex", "anton"):
        raise HTTPException(400, "Unknown harness")
    outcome = outcome_from_text(db, workspace_id, user, body.title, body.brief, [r.id for r in repos],
                                acceptance=body.acceptance, harness=body.harness, priority=max(0, min(body.priority, 3)))
    db.commit()
    orchestrator.wake()
    return S.outcome(outcome, [], [])


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
    open_count = sum(1 for d in decisions if d.status == "open")
    return {
        "outcome": S.outcome(o, tasks, runs, open_count) | {"created_by_name": users.get(o.created_by or "", "")},
        "repositories": [{"id": rid, "full_name": repos.get(rid, "?")} for rid in o.repo_ids],
        "plans": [S.plan(p) for p in plans],
        "tasks": [S.task(t, [r for r in runs if r.task_id == t.id], repos.get(t.repo_id, "")) for t in tasks],
        "runs": [S.run(r) for r in runs],
        "decisions": [S.decision(d, {"resolved_by_name": users.get(d.resolved_by or "", "")}) for d in decisions],
        "messages": [S.message(m, users.get(m.author_id or "", "Factory")) for m in messages],
        "events": [event_dict(e) for e in events],
    }


@router.post("/workspaces/{workspace_id}/outcomes/{outcome_id}/cancel")
async def cancel_outcome(workspace_id: str, outcome_id: str, user: User = Depends(current_user),
                         db: Session = Depends(get_db)):
    _ws(db, user, workspace_id, "member")
    o = db.get(Outcome, outcome_id)
    if o is None or o.workspace_id != workspace_id:
        raise HTTPException(404, "Outcome not found")
    o.status = "cancelled"
    with_prs = []
    for t in db.query(Task).filter(Task.outcome_id == o.id, Task.status.notin_(["completed", "abandoned"])):
        t.status = "cancelled"
        if t.pr_number:
            with_prs.append(t.id)
    for d in db.query(Decision).filter(Decision.outcome_id == o.id, Decision.status == "open"):
        d.status = "superseded"
    db.commit()
    orchestrator.cancel_outcome(o.id)
    record(workspace_id, "outcome_cancelled", f"{user.name or user.email} cancelled #{o.number}", outcome_id=o.id,
           actor=user.id, level="warn")
    notify(workspace_id, "outcome", o.id)
    if with_prs:
        from .pipeline.handover import close_dropped_pull

        for task_id in with_prs:
            await close_dropped_pull(task_id, user.name or user.email, "Cancelled")
    return {"ok": True}


@router.post("/workspaces/{workspace_id}/tasks/{task_id}/cancel")
async def cancel_task(workspace_id: str, task_id: str, user: User = Depends(current_user), db: Session = Depends(get_db)):
    _ws(db, user, workspace_id, "member")
    t = db.get(Task, task_id)
    if t is None or t.workspace_id != workspace_id:
        raise HTTPException(404, "Task not found")
    if t.status not in ("completed",):
        t.status = "cancelled"
    db.commit()
    orchestrator.cancel_task(task_id)
    record(workspace_id, "task_cancelled", f"{user.name or user.email} stopped {t.key}", outcome_id=t.outcome_id,
           task_id=t.id, actor=user.id, level="warn")
    notify(workspace_id, "task", t.id, outcome_id=t.outcome_id)
    orchestrator.wake()
    if t.status == "cancelled" and t.pr_number:
        from .pipeline.handover import close_dropped_pull

        await close_dropped_pull(t.id, user.name or user.email, "Cancelled")
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
    _ws(db, user, workspace_id, "member")
    o = db.get(Outcome, outcome_id)
    if o is None or o.workspace_id != workspace_id:
        raise HTTPException(404, "Outcome not found")
    m = Message(workspace_id=workspace_id, outcome_id=outcome_id, task_id=body.task_id, author_id=user.id,
                author_kind="user", body=body.body.strip())
    db.add(m)
    db.commit()
    record(workspace_id, "guidance", f"{user.name or user.email}: {body.body[:200]}", outcome_id=outcome_id,
           task_id=body.task_id, actor=user.id, data={"intervention": True})
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
    return S.artifact(a, content=True)


# --- attention & decisions -----------------------------------------------------------
@router.get("/workspaces/{workspace_id}/inbox")
def inbox(workspace_id: str, user: User = Depends(current_user), db: Session = Depends(get_db)):
    _, member = _ws(db, user, workspace_id)
    decisions = db.query(Decision).filter(Decision.workspace_id == workspace_id, Decision.status == "open").order_by(
        Decision.created_at).all()
    outcomes = {o.id: o for o in db.query(Outcome).filter(Outcome.id.in_({d.outcome_id for d in decisions if d.outcome_id}))}
    tasks = {t.id: t for t in db.query(Task).filter(Task.id.in_({d.task_id for d in decisions if d.task_id}))}
    since = member.last_viewed_at
    recent = []
    if since:
        recent = (db.query(Event).filter(Event.workspace_id == workspace_id, Event.created_at > since,
                                         Event.kind.in_(["outcome_completed", "merged", "plan_proposed", "published",
                                                         "decision_resolved", "task_completed", "outcome_created"]))
                  .order_by(Event.id.desc()).limit(50).all())
    change_outcomes = {o.id: o for o in db.query(Outcome).filter(Outcome.id.in_({e.outcome_id for e in recent if e.outcome_id}))}
    return {
        "decisions": [S.decision(d, {
            "outcome_number": outcomes[d.outcome_id].number if d.outcome_id in outcomes else None,
            "outcome_title": outcomes[d.outcome_id].title if d.outcome_id in outcomes else "",
            "task_key": tasks[d.task_id].key if d.task_id in tasks else None,
            "pr_url": tasks[d.task_id].pr_url if d.task_id in tasks else "",
            "pr_number": tasks[d.task_id].pr_number if d.task_id in tasks else None,
            "priority": outcomes[d.outcome_id].priority if d.outcome_id in outcomes else 2,
        }) for d in decisions],
        "since": S.ts(since),
        "changes": [event_dict(e) | ({"outcome_number": change_outcomes[e.outcome_id].number,
                                       "outcome_title": change_outcomes[e.outcome_id].title}
                                      if e.outcome_id in change_outcomes else {}) for e in recent],
    }


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


@router.get("/workspaces/{workspace_id}/decisions")
def decision_library(workspace_id: str, user: User = Depends(current_user), db: Session = Depends(get_db)):
    _ws(db, user, workspace_id)
    rows = db.query(Decision).filter(Decision.workspace_id == workspace_id, Decision.status != "open").order_by(
        Decision.resolved_at.desc()).limit(300).all()
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


# --- runs, activity, signals -----------------------------------------------------------
@router.get("/workspaces/{workspace_id}/runs")
def list_runs(workspace_id: str, limit: int = 100, user: User = Depends(current_user), db: Session = Depends(get_db)):
    _ws(db, user, workspace_id)
    rows = db.query(Run).filter(Run.workspace_id == workspace_id).order_by(Run.started_at.desc()).limit(min(limit, 500)).all()
    outcomes = {o.id: o for o in db.query(Outcome).filter(Outcome.id.in_({r.outcome_id for r in rows}))}
    tasks = {t.id: t for t in db.query(Task).filter(Task.id.in_({r.task_id for r in rows if r.task_id}))}
    return [S.run(r) | {"outcome_number": outcomes[r.outcome_id].number if r.outcome_id in outcomes else None,
                        "outcome_title": outcomes[r.outcome_id].title if r.outcome_id in outcomes else "",
                        "task_key": tasks[r.task_id].key if r.task_id in tasks else None} for r in rows]


@router.get("/workspaces/{workspace_id}/stats")
def stats(workspace_id: str, user: User = Depends(current_user), db: Session = Depends(get_db)):
    _ws(db, user, workspace_id)
    outcomes = db.query(Outcome).filter(Outcome.workspace_id == workspace_id).all()
    tasks = db.query(Task).filter(Task.workspace_id == workspace_id).all()
    runs = db.query(Run).filter(Run.workspace_id == workspace_id).all()
    decisions = db.query(Decision).filter(Decision.workspace_id == workspace_id).all()
    merged = [t for t in tasks if t.merge_sha]
    by_status: dict[str, int] = {}
    for o in outcomes:
        by_status[o.status] = by_status.get(o.status, 0) + 1
    first_pass = sum(1 for t in merged if len((t.evidence or {}).get("attempts", [])) <= 1)
    return {
        "outcomes": by_status,
        "tasks": {"total": len(tasks), "completed": sum(t.status == "completed" for t in tasks), "merged": len(merged),
                  "first_attempt": first_pass, "smoke_failed": sum(t.status == "smoke_failed" for t in tasks)},
        "cost": {"usd": round(sum(r.cost_usd for r in runs), 2), "known": all(r.cost_known for r in runs),
                 "per_completed_task": round(sum(r.cost_usd for r in runs) / max(1, sum(t.status == "completed" for t in tasks)), 2)},
        "tokens": sum(r.input_tokens + r.output_tokens for r in runs),
        "human": {"decisions": len(decisions), "open": sum(d.status == "open" for d in decisions),
                  "interventions": db.query(Message).filter(Message.workspace_id == workspace_id,
                                                            Message.author_kind == "user").count()},
        "harnesses": {h: {"runs": sum(r.harness == h for r in runs),
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
    if body.action == "ignore":
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
    repo_ids = [sig.repo_id] if sig.repo_id else [r.id for r in db.query(Repository).filter(
        Repository.workspace_id == workspace_id).limit(1)]
    outcome = outcome_from_text(db, workspace_id, user, body.title or sig.title,
                                (body.brief or sig.body or "") + (f"\n\nSource: {sig.url}" if sig.url else ""),
                                repo_ids, source="signal", signal_id=sig.id)
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
