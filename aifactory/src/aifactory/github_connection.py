"""Browser-bound GitHub App authorization. Only installation evidence is persisted.

App operators configure the setup URL as PUBLIC_URL/api/github/return, the OAuth
callback as PUBLIC_URL/api/github/oauth, and webhook as PUBLIC_URL/api/github/webhook.
Repository grants are configured on GitHub, not encoded in installation links.
"""
from __future__ import annotations

import hashlib
import hmac
import json
import re
from datetime import timedelta
from typing import Literal
from urllib.parse import urlencode

import httpx
from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import RedirectResponse
from sqlalchemy import delete, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from .auth import COOKIE, current_user, require_member
from .config import get_settings
from .db import get_db, utcnow
from .github import API, GitHubError
from .github_app import APP_PERMISSIONS, INSTALLATION_SETTING, missing_permissions, sign_jwt
from .models import GitHubConnectionState, GitHubInstallation, User, Workspace, new_id
from .security import new_token, sign, token_hash, unsign

router = APIRouter(prefix="/api")
ROOT = "/workspaces/{workspace_id}/github/connection"
LIMIT = 1024 * 1024


def configured():
    if not get_settings().github_app_available:
        raise HTTPException(503, "GitHub App is not configured")


def identifier(value) -> str:
    if isinstance(value, bool) or not re.fullmatch(r"[1-9][0-9]{0,19}", str(value)):
        raise HTTPException(400, "Invalid GitHub installation")
    return str(value)


async def github_json(method: str, url: str, **kwargs):
    """Bound responses and never relay provider bodies (including OAuth errors)."""
    try:
        async with httpx.AsyncClient(timeout=20, follow_redirects=False) as client:
            async with client.stream(method, url, **kwargs) as response:
                if response.status_code != 200:
                    raise HTTPException(400, "GitHub authorization unavailable. Reconnect or manage GitHub access.")
                body = bytearray()
                async for chunk in response.aiter_bytes():
                    if len(body) + len(chunk) > LIMIT:
                        raise HTTPException(502, "GitHub response exceeds limit")
                    body.extend(chunk)
                data = json.loads(body)
                if not isinstance(data, dict):
                    raise ValueError("Expected object")
                return data
    except (httpx.HTTPError, ValueError, TypeError):
        raise HTTPException(502, "GitHub authorization unavailable. Please retry.") from None


def headers(token: str) -> dict:
    return {"Authorization": f"Bearer {token}", "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28", "User-Agent": "ai-factory"}


def callback(workspace_id: str, stage: str) -> str:
    return get_settings().public_url + "/api/github/" + stage


def intent(db, request, user, workspace_id, stage, destination, installation=""):
    token = sign(new_token())
    db.query(GitHubConnectionState).filter(GitHubConnectionState.expires_at < utcnow()).delete()
    db.query(GitHubConnectionState).filter_by(
        workspace_id=workspace_id, session_hash=token_hash(request.cookies.get(COOKIE, "")), stage=stage).delete()
    db.add(GitHubConnectionState(token_hash=token_hash(token), user_id=user.id, workspace_id=workspace_id,
                                session_hash=token_hash(request.cookies.get(COOKIE, "")), stage=stage,
                                destination=destination, installation_id=installation,
                                expires_at=utcnow() + timedelta(minutes=10)))
    db.commit()
    return token


def consume(db, request, user, workspace_id, state, stage):
    if len(state) > 200 or not unsign(state):
        raise HTTPException(400, "Invalid or expired GitHub state")
    require_member(db, user, workspace_id, "admin")
    row = db.get(GitHubConnectionState, token_hash(state))
    if (row is None or row.expires_at <= utcnow() or row.user_id != user.id or
            row.workspace_id != workspace_id or row.stage != stage or
            row.session_hash != token_hash(request.cookies.get(COOKIE, ""))):
        raise HTTPException(400, "Invalid or expired GitHub state")
    # Conditional deletion is atomic across processes, unlike a read then ORM delete.
    result = db.execute(delete(GitHubConnectionState).where(GitHubConnectionState.token_hash == row.token_hash))
    if result.rowcount != 1:
        db.rollback()
        raise HTTPException(400, "GitHub state already used")
    db.commit()  # consumed even when the subsequent provider request fails
    return row


@router.get(ROOT)
def connection(workspace_id: str, user: User = Depends(current_user), db: Session = Depends(get_db)):
    require_member(db, user, workspace_id)
    ws = db.get(Workspace, workspace_id)
    installation = (ws.settings or {}).get(INSTALLATION_SETTING)
    evidence = db.get(GitHubInstallation, str(installation)) if installation else None
    granted = evidence.permissions if evidence else {}
    missing = missing_permissions(granted)
    configured_now = get_settings().github_app_available
    return {"configured": configured_now, "installation_id": installation,
            "status": evidence.status if evidence else "disconnected",
            "ready": bool(configured_now and evidence and evidence.status == "active" and not missing),
            "account": evidence.account if evidence else "", "granted_permissions": granted,
            "required_permissions": APP_PERMISSIONS, "missing_permissions": missing,
            "webhook_received_at": evidence.webhook_received_at if evidence else None,
            "manage_url": f"https://github.com/settings/installations/{identifier(installation)}" if installation else None}


@router.post(ROOT + "/start")
async def start(workspace_id: str, request: Request, destination: Literal["setup", "settings"] = "setup",
                user: User = Depends(current_user), db: Session = Depends(get_db)):
    require_member(db, user, workspace_id, "admin")
    configured()
    try:
        app = await github_json("GET", API + "/app", headers=headers(sign_jwt()))
    except GitHubError:
        raise HTTPException(503, "GitHub App signing is unavailable") from None
    slug = app.get("slug", "")
    if not re.fullmatch(r"[A-Za-z0-9-]{1,100}", slug):
        raise HTTPException(502, "Invalid GitHub App identity")
    state = intent(db, request, user, workspace_id, "install", destination)
    return {"url": f"https://github.com/apps/{slug}/installations/new?" + urlencode({"state": state})}


@router.get(ROOT + "/return")
async def installed(workspace_id: str, request: Request, state: str = "", installation_id: str = "",
                    user: User = Depends(current_user), db: Session = Depends(get_db)):
    configured()
    row = consume(db, request, user, workspace_id, state, "install")
    selected = identifier(installation_id)
    oauth_state = intent(db, request, user, workspace_id, "oauth", row.destination, selected)
    return RedirectResponse("https://github.com/login/oauth/authorize?" + urlencode({
        "client_id": get_settings().github_app_client_id, "state": oauth_state,
        "redirect_uri": callback(workspace_id, "oauth")}), status_code=303,
        headers={"Cache-Control": "no-store", "Referrer-Policy": "no-referrer"})


@router.get(ROOT + "/oauth")
async def authorized(workspace_id: str, request: Request, state: str = "", code: str = "",
                     user: User = Depends(current_user), db: Session = Depends(get_db)):
    configured()
    row = consume(db, request, user, workspace_id, state, "oauth")
    if not code or len(code) > 1000:
        raise HTTPException(400, "GitHub authorization code required")
    settings = get_settings()
    auth = await github_json("POST", "https://github.com/login/oauth/access_token",
                             headers={"Accept": "application/json"}, json={
                                 "client_id": settings.github_app_client_id,
                                 "client_secret": settings.github_app_client_secret, "code": code,
                                 "redirect_uri": callback(workspace_id, "oauth")})
    token = auth.get("access_token")
    if not isinstance(token, str) or not token or len(token) > 4096:
        raise HTTPException(400, "GitHub user authorization failed")
    found = None
    # Bound pagination; exceeding this limit fails closed rather than guessing access.
    for page in range(1, 101):
        data = await github_json("GET", API + "/user/installations", headers=headers(token),
                                 params={"per_page": 100, "page": page})
        installations = data.get("installations")
        if not isinstance(installations, list) or len(installations) > 100:
            raise HTTPException(502, "Invalid GitHub installations response")
        found = next((i for i in installations if str(i.get("id")) == row.installation_id), None)
        if found or len(installations) < 100:
            break
    if not found:
        raise HTTPException(403, "Installation is not accessible to the authorized GitHub user")
    # Snapshot before the live lookup, then release the read transaction during I/O.
    # A lifecycle/repository webhook arriving after this point must win over reconnect.
    snapshot = db.query(GitHubInstallation.version).filter_by(id=row.installation_id).one_or_none()
    db.commit()
    try:
        selected = await github_json("GET", API + f"/app/installations/{row.installation_id}", headers=headers(sign_jwt()))
    except GitHubError:
        raise HTTPException(503, "GitHub App signing is unavailable") from None
    if (str(selected.get("id")) != row.installation_id or selected.get("suspended_at") or
            found.get("suspended_at") or str(selected.get("app_id")) != str(settings.github_app_id)):
        raise HTTPException(403, "Installation is unavailable or suspended")
    permissions = selected.get("permissions") or {}
    if not isinstance(permissions, dict) or missing_permissions(permissions):
        raise HTTPException(403, "GitHub App is missing required permissions. Manage GitHub access.")
    # Re-check the live session and admin role after network I/O, under the workspace lock.
    db.expire_all()
    current_user(request, db)
    require_member(db, user, workspace_id, "admin")
    ws = db.query(Workspace).filter_by(id=workspace_id).with_for_update().one()
    values = {
        "status": "active",
        "permissions": {k: v for k, v in permissions.items() if k in APP_PERMISSIONS},
        "account": str((selected.get("account") or {}).get("login", ""))[:200],
        "version": new_id(),
    }
    # CAS also protects SQLite, where SELECT FOR UPDATE does not lock a row.
    # Unchanged stale suspension evidence can be repaired by the verified live response.
    if snapshot is not None:
        changed = db.execute(update(GitHubInstallation).where(
            GitHubInstallation.id == row.installation_id,
            GitHubInstallation.version == snapshot.version,
        ).values(**values).execution_options(synchronize_session=False))
        if changed.rowcount != 1:
            db.rollback()
            raise HTTPException(409, "GitHub installation changed during authorization. Reconnect to verify access.")
    else:
        db.add(GitHubInstallation(id=row.installation_id, **values))
        try:
            db.flush()
        except IntegrityError:
            db.rollback()
            raise HTTPException(409, "GitHub installation changed during authorization. Reconnect to verify access.") from None
    ws.settings = {**(ws.settings or {}), INSTALLATION_SETTING: row.installation_id}
    if row.destination == "setup":
        ws.onboarding = {**(ws.onboarding or {}), "current_step": "github"}
    db.commit()
    path = f"/w/{workspace_id}/" + ("settings/credentials" if row.destination == "settings" else "onboarding")
    return RedirectResponse(path, status_code=303, headers={"Cache-Control": "no-store", "Referrer-Policy": "no-referrer"})


@router.delete(ROOT)
def disconnect(workspace_id: str, user: User = Depends(current_user), db: Session = Depends(get_db)):
    require_member(db, user, workspace_id, "admin")
    ws = db.query(Workspace).filter_by(id=workspace_id).with_for_update().one()
    ws.settings = {k: v for k, v in (ws.settings or {}).items() if k != INSTALLATION_SETTING}
    db.query(GitHubConnectionState).filter_by(workspace_id=workspace_id).delete()
    db.commit()
    return {"ok": True}


@router.post("/github/webhook")
async def webhook(request: Request, db: Session = Depends(get_db)):
    configured()
    body = bytearray()
    async for chunk in request.stream():
        if len(body) + len(chunk) > LIMIT:
            raise HTTPException(413, "Webhook too large")
        body.extend(chunk)
    signature = "sha256=" + hmac.new(get_settings().github_app_webhook_secret.encode(), body, hashlib.sha256).hexdigest()
    if not hmac.compare_digest(signature, request.headers.get("x-hub-signature-256", "")):
        raise HTTPException(401, "Invalid webhook signature")
    event = request.headers.get("x-github-event", "")
    if event == "ping":
        return {"ok": True}
    if event not in ("installation", "installation_repositories"):
        return {"ok": True}
    try:
        payload = json.loads(body)
        installation = payload["installation"]
        selected = identifier(installation["id"])
    except (ValueError, KeyError, TypeError):
        raise HTTPException(400, "Invalid installation webhook") from None
    evidence = db.query(GitHubInstallation).filter_by(id=selected).with_for_update().one_or_none()
    if evidence is None:
        evidence = GitHubInstallation(id=selected, status="active", permissions={})
        db.add(evidence)
    if evidence:
        evidence.version = new_id()
        evidence.webhook_received_at = utcnow()
        action = payload.get("action")
        if action in ("deleted", "suspend"):
            evidence.status = "deleted" if action == "deleted" else "suspended"
        elif action == "unsuspend" and evidence.status != "deleted":
            evidence.status = "active"
        if isinstance(installation.get("permissions"), dict):
            evidence.permissions = {k: v for k, v in installation["permissions"].items() if k in APP_PERMISSIONS}
        db.commit()
    # Invalidate short-lived credentials after repository access or lifecycle changes.
    from . import github_app
    with github_app._lock:
        for key in list(github_app._cache):
            if key[0] == selected:
                github_app._cache.pop(key, None)
    return {"ok": True}


# GitHub App setup/callback URLs are server-wide, not per-workspace. Resolve only
# the hashed intent here; consume() still authenticates user, role and browser.
def return_workspace(db: Session, state: str) -> str:
    if len(state) > 200 or not unsign(state):
        raise HTTPException(400, "Invalid or expired GitHub state")
    row = db.get(GitHubConnectionState, token_hash(state))
    if row is None:
        raise HTTPException(400, "Invalid or expired GitHub state")
    return row.workspace_id


@router.get("/github/return")
async def installation_return(request: Request, state: str = "", installation_id: str = "",
                              user: User = Depends(current_user), db: Session = Depends(get_db)):
    return await installed(return_workspace(db, state), request, state, installation_id, user, db)


@router.get("/github/oauth")
async def oauth_return(request: Request, state: str = "", code: str = "",
                       user: User = Depends(current_user), db: Session = Depends(get_db)):
    return await authorized(return_workspace(db, state), request, state, code, user, db)
