"""Sign-in with MindsHub and team access.

Identity always comes from MindsHub Auth (``/v1/authenticate/``), which accepts either a
browser-session JWT (from the MindsHub OIDC sign-in) or a MindsHub API key. Its answer gives
the user, their organization and workspace; Factory organizations and default workspaces
mirror those, and organization members get access to their organization's workspaces.
"""

from __future__ import annotations

import base64
import hashlib
import json
import secrets
from datetime import timedelta
from typing import Any
from urllib.parse import urlencode

import httpx
from fastapi import APIRouter, Depends, HTTPException, Request, Response
from fastapi.responses import RedirectResponse
from pydantic import BaseModel
from sqlalchemy.orm import Session

from .config import get_settings
from .db import get_db, utcnow
from .models import Membership, Organization, Secret, SessionRecord, User, Workspace
from .security import encrypt, mask, new_token, sign, token_hash, unsign

router = APIRouter()
COOKIE = "aif_session"
SESSION_DAYS = 14
ROLE_RANK = {"viewer": 0, "member": 1, "admin": 2, "owner": 3}


class AuthError(Exception):
    def __init__(self, message: str, status: int | None = None):
        super().__init__(message)
        self.status = status


async def mindshub_identity(bearer: str) -> dict[str, Any]:
    """Ask MindsHub Auth who this bearer (JWT or mdb_ key) belongs to."""
    settings = get_settings()
    try:
        async with httpx.AsyncClient(timeout=10.0, follow_redirects=False) as client:
            response = await client.get(settings.mindshub_auth_url,
                                        headers={"Authorization": f"Bearer {bearer}", "Accept": "application/json"})
    except httpx.HTTPError:
        raise AuthError("MindsHub sign-in is unavailable right now. Try again shortly.")
    if response.status_code in (401, 403):
        raise AuthError("MindsHub did not accept these credentials.", response.status_code)
    if response.status_code == 429:
        raise AuthError("MindsHub is rate limiting sign-in. Try again in a minute.", 429)
    if response.status_code != 200:
        raise AuthError("MindsHub sign-in is unavailable right now. Try again shortly.", response.status_code)
    try:
        data = response.json()
    except ValueError:
        raise AuthError("MindsHub returned an invalid response.", 502) from None
    # Explicit rejection takes precedence over absent/malformed identity fields.
    if isinstance(data, dict) and data.get("valid") is False:
        raise AuthError("MindsHub did not accept these credentials.", 401)
    if not isinstance(data, dict) or not data.get("user_id"):
        raise AuthError("MindsHub returned an invalid identity.", 502)
    return data


def _org_role(identity: dict[str, Any]) -> str:
    roles = identity.get("roles") or []
    if isinstance(roles, str):
        roles = [roles]
    text = " ".join(str(r).lower() for r in roles)
    if "owner" in text:
        return "owner"
    if "manage-organization" in text or "admin" in text:
        return "admin"
    return "member"


def upsert_identity(db: Session, identity: dict[str, Any]) -> User:
    settings = get_settings()
    email = (identity.get("email") or "").lower() or None
    org_id = str(identity.get("organization_id") or "")
    if settings.allowed_emails or settings.allowed_org_ids:
        allowed = (email and (email in settings.allowed_emails or any(
            email.endswith("@" + d.lstrip("@")) for d in settings.allowed_emails if d.startswith("@"))))
        allowed = allowed or (org_id and org_id in settings.allowed_org_ids)
        if not allowed:
            raise AuthError("This Factory is not open to your account yet. Ask an administrator for access.")
    user = db.query(User).filter(User.mindshub_user_id == str(identity["user_id"])).one_or_none()
    if user is None and email:
        # Adopt an operator-created account for the same (MindsHub-verified) email.
        user = db.query(User).filter(User.email == email, User.mindshub_user_id.is_(None)).first()
        if user is not None:
            user.mindshub_user_id = str(identity["user_id"])
    if user is None:
        user = User(mindshub_user_id=str(identity["user_id"]), email=email,
                    name=identity.get("name") or (email.split("@")[0] if email else "MindsHub user"))
        db.add(user)
        db.flush()
    else:
        user.email = email or user.email
    user.last_seen_at = utcnow()
    # Operator invitations (bootstrap): grant the invited role and link the bootstrap
    # organization to the person's real MindsHub organization the first time they sign in.
    if email:
        from .bootstrap import invited_role

        for ws in db.query(Workspace).all():
            role = invited_role(email, ws)
            if not role:
                continue
            org = db.get(Organization, ws.org_id)
            if org is not None and not org.mindshub_org_id and org_id and not db.query(Organization).filter(
                    Organization.mindshub_org_id == org_id).first():
                org.mindshub_org_id = org_id
                ws.mindshub_workspace_id = ws.mindshub_workspace_id or identity.get("workspace_id")
            if not db.query(Membership).filter(Membership.workspace_id == ws.id, Membership.user_id == user.id).first():
                db.add(Membership(workspace_id=ws.id, user_id=user.id, role=role))
        db.flush()
    if org_id:
        org = db.query(Organization).filter(Organization.mindshub_org_id == org_id).one_or_none()
        if org is None:
            org = Organization(mindshub_org_id=org_id, name=identity.get("organization_name") or "My organization")
            db.add(org)
            db.flush()
        elif identity.get("organization_name"):
            org.name = identity["organization_name"]
        workspaces = db.query(Workspace).filter(Workspace.org_id == org.id).all()
        if not workspaces:
            from .pipeline.runtime import new_settings
            from .onboarding_policy import begin

            ws = Workspace(org_id=org.id, name=f"{org.name}", slug="default",
                           mindshub_workspace_id=identity.get("workspace_id"), settings=begin(new_settings()))
            db.add(ws)
            db.flush()
            workspaces = [ws]
            role_for_new = "owner"
        else:
            role_for_new = _org_role(identity)
        for ws in workspaces:
            member = db.query(Membership).filter(Membership.workspace_id == ws.id, Membership.user_id == user.id).one_or_none()
            if member is None:
                db.add(Membership(workspace_id=ws.id, user_id=user.id, role=role_for_new))
    db.flush()
    return user


def start_session(db: Session, response: Response, user: User, method: str) -> None:
    token = new_token("aifs_")
    db.add(SessionRecord(token_hash=token_hash(token), user_id=user.id, method=method,
                         expires_at=utcnow() + timedelta(days=SESSION_DAYS)))
    db.commit()
    response.set_cookie(COOKIE, token, max_age=SESSION_DAYS * 86400, httponly=True, samesite="lax",
                        secure=get_settings().secure_cookies, path="/")


def current_user(request: Request, db: Session = Depends(get_db)) -> User:
    token = request.cookies.get(COOKIE, "")
    if not token:
        raise HTTPException(401, "Sign in required")
    record = db.get(SessionRecord, token_hash(token))
    if record is None or record.expires_at < utcnow():
        raise HTTPException(401, "Session expired")
    user = db.get(User, record.user_id)
    if user is None:
        raise HTTPException(401, "Sign in required")
    return user


def require_member(db: Session, user: User, workspace_id: str, minimum: str = "viewer") -> Membership:
    member = db.query(Membership).filter(Membership.workspace_id == workspace_id, Membership.user_id == user.id).one_or_none()
    if member is None:
        raise HTTPException(404, "Workspace not found")
    if ROLE_RANK.get(member.role, 0) < ROLE_RANK[minimum]:
        raise HTTPException(403, f"This action requires the {minimum} role")
    return member


def auth_methods() -> dict[str, Any]:
    settings = get_settings()
    return {
        "oidc": bool(settings.oidc_client_id),
        "github_app": settings.github_app_available,
        "api_key": settings.allow_api_key_sign_in,
        "dev": settings.allow_dev_sign_in,
    }


# ---------------------------------------------------------------------------
class ApiKeySignIn(BaseModel):
    key: str
    use_for_inference: bool = True


@router.post("/auth/api-key")
async def sign_in_with_api_key(body: ApiKeySignIn, response: Response, db: Session = Depends(get_db)):
    if not get_settings().allow_api_key_sign_in:
        raise HTTPException(404, "Not available")
    key = body.key.strip()
    if not key.startswith("mdb_") or len(key) > 4096:
        raise HTTPException(400, "Enter a MindsHub API key (it starts with mdb_).")
    try:
        identity = await mindshub_identity(key)
        user = upsert_identity(db, identity)
    except AuthError as exc:
        status = 401 if exc.status in (401, 403) else 429 if exc.status == 429 else 503
        raise HTTPException(status, str(exc))
    if body.use_for_inference:
        from .credential_diagnostics import safe_metadata, test_models

        status, detail, metadata = await test_models("mindshub_api_key", key, identity)
        metadata = safe_metadata(metadata, key)
        # Seed the inference credential for workspaces this user owns that have none yet.
        for member in db.query(Membership).filter(Membership.user_id == user.id, Membership.role.in_(["owner", "admin"])):
            existing = db.query(Secret).filter(Secret.workspace_id == member.workspace_id,
                                               Secret.kind == "mindshub_api_key").one_or_none()
            if existing is None:
                db.add(Secret(workspace_id=member.workspace_id, kind="mindshub_api_key", ciphertext=encrypt(key),
                              hint=mask(key), status=status, status_detail=detail,
                              tested_at=utcnow(), updated_by=user.id,
                              metadata_json=metadata))
    start_session(db, response, user, "api_key")
    return {"ok": True}


def _pkce() -> tuple[str, str]:
    verifier = secrets.token_urlsafe(64)
    challenge = base64.urlsafe_b64encode(hashlib.sha256(verifier.encode()).digest()).rstrip(b"=").decode()
    return verifier, challenge


@router.get("/auth/login")
async def oidc_login(next: str = "/"):
    settings = get_settings()
    if not settings.oidc_client_id:
        raise HTTPException(404, "MindsHub single sign-on is not configured")
    verifier, challenge = _pkce()
    state = secrets.token_urlsafe(24)
    params = {
        "client_id": settings.oidc_client_id,
        "redirect_uri": f"{settings.public_url}/auth/callback",
        "response_type": "code",
        "scope": "openid email profile",
        "state": state,
        "code_challenge": challenge,
        "code_challenge_method": "S256",
    }
    redirect = RedirectResponse(f"{settings.oidc_issuer}/protocol/openid-connect/auth?{urlencode(params)}")
    safe_next = next if next.startswith("/") and not next.startswith("//") else "/"
    redirect.set_cookie("aif_oidc", sign(json.dumps({"s": state, "v": verifier, "n": safe_next})), max_age=600,
                        httponly=True, samesite="lax", secure=settings.secure_cookies, path="/auth")
    return redirect


@router.get("/auth/callback")
async def oidc_callback(request: Request, code: str = "", state: str = "", db: Session = Depends(get_db)):
    settings = get_settings()
    raw = unsign(request.cookies.get("aif_oidc", ""))
    if not raw:
        return RedirectResponse("/signin?error=expired")
    stored = json.loads(raw)
    if not code or not secrets.compare_digest(stored.get("s", ""), state):
        return RedirectResponse("/signin?error=state")
    data = {"grant_type": "authorization_code", "code": code, "client_id": settings.oidc_client_id,
            "redirect_uri": f"{settings.public_url}/auth/callback", "code_verifier": stored["v"]}
    if settings.oidc_client_secret:
        data["client_secret"] = settings.oidc_client_secret
    try:
        async with httpx.AsyncClient(timeout=15.0) as client:
            tokens = await client.post(f"{settings.oidc_issuer}/protocol/openid-connect/token", data=data)
        if tokens.status_code != 200:
            return RedirectResponse("/signin?error=token")
        identity = await mindshub_identity(tokens.json()["access_token"])
        user = upsert_identity(db, identity)
    except (AuthError, httpx.HTTPError, KeyError):
        return RedirectResponse("/signin?error=identity")
    redirect = RedirectResponse(stored.get("n") or "/")
    redirect.delete_cookie("aif_oidc", path="/auth")
    start_session(db, redirect, user, "oidc")
    return redirect


class DevSignIn(BaseModel):
    email: str
    name: str = ""
    organization: str = "Local development"


@router.post("/auth/dev")
async def dev_sign_in(body: DevSignIn, response: Response, db: Session = Depends(get_db)):
    if not get_settings().allow_dev_sign_in:
        raise HTTPException(404, "Not available")
    identity = {"user_id": "dev-" + hashlib.sha256(body.email.lower().encode()).hexdigest()[:24], "email": body.email,
                "name": body.name, "organization_id": "dev-" + hashlib.sha256(body.organization.encode()).hexdigest()[:24],
                "organization_name": body.organization, "roles": ["owner"]}
    try:
        user = upsert_identity(db, identity)
    except AuthError as exc:
        raise HTTPException(403, str(exc))
    start_session(db, response, user, "dev")
    return {"ok": True}


@router.post("/auth/logout")
async def logout(request: Request, response: Response, db: Session = Depends(get_db)):
    token = request.cookies.get(COOKIE, "")
    if token:
        record = db.get(SessionRecord, token_hash(token))
        if record:
            db.delete(record)
            db.commit()
    response.delete_cookie(COOKIE, path="/")
    settings = get_settings()
    logout_url = None
    if settings.oidc_client_id:
        logout_url = (f"{settings.oidc_issuer}/protocol/openid-connect/logout?"
                      + urlencode({"client_id": settings.oidc_client_id, "post_logout_redirect_uri": settings.public_url + "/signin"}))
    return {"ok": True, "logout_url": logout_url}
