"""Server-only GitHub App signing and process-local installation credentials.

T4 stores only ``github_app_installation_id`` in Workspace.settings. A credential
reference captures that workspace binding, not a token; every use revalidates it.
Nothing in this module persists credentials or includes provider bodies in errors.
"""
from __future__ import annotations

import base64
import hashlib
import json
import threading
import time
from dataclasses import dataclass
from datetime import datetime

import httpx
from cryptography.exceptions import UnsupportedAlgorithm
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding, rsa

from .config import get_settings
from .db import session_scope
from .models import Workspace

INSTALLATION_SETTING = "github_app_installation_id"
APP_PERMISSIONS = {"contents": "write", "pull_requests": "write", "issues": "write",
                   "checks": "read", "actions": "read", "metadata": "read"}
EXPIRY_SKEW = 60
_cache: dict[tuple[str, str], tuple[str, float]] = {}
_lock = threading.Lock()


def installation_id(workspace_id: str) -> str | None:
    from .github import GitHubError

    with session_scope() as db:
        workspace = db.get(Workspace, workspace_id)
        settings = (workspace.settings or {}) if workspace else {}
        if INSTALLATION_SETTING not in settings:
            return None
        value = settings[INSTALLATION_SETTING]
    # A present but malformed binding must never select the PAT fallback.
    if isinstance(value, bool) or not str(value).isascii() or not str(value).isdigit() or int(value) <= 0:
        raise GitHubError("Invalid GitHub App installation binding. Manage GitHub access.")
    return str(int(value))


def _identity() -> str:
    settings = get_settings()
    return hashlib.sha256(json.dumps([settings.github_app_id, settings.github_app_private_key,
                                     settings.github_app_client_id, settings.github_app_client_secret,
                                     settings.github_app_webhook_secret]).encode()).hexdigest()


def sign_jwt() -> str:
    """RS256, backdated by 60 seconds; total validity is at most ten minutes."""
    from .github import GitHubError

    settings = get_settings()
    if not settings.github_app_available:
        raise GitHubError("GitHub App is not configured. Contact the server administrator.")

    def encode(value: bytes) -> str:
        return base64.urlsafe_b64encode(value).rstrip(b"=").decode()

    now = int(time.time())
    payload = {"iss": settings.github_app_id, "iat": now - 60, "exp": now + 540}
    message = encode(b'{"alg":"RS256","typ":"JWT"}') + "." + encode(json.dumps(payload).encode())
    try:
        key = serialization.load_pem_private_key(settings.github_app_private_key.encode(), password=None)
        if not isinstance(key, rsa.RSAPrivateKey):
            raise ValueError("Not an RSA key")
        signature = key.sign(message.encode(), padding.PKCS1v15(), hashes.SHA256())
    except (ValueError, TypeError, UnsupportedAlgorithm):
        raise GitHubError("GitHub App signing key is invalid. Contact the server administrator.") from None
    return message + "." + encode(signature)


def installation_token(installation: str) -> str:
    from .github import API, GitHubError

    settings = get_settings()
    if not settings.github_app_available:
        raise GitHubError("GitHub App is not configured. Contact the server administrator.")
    key = (installation, _identity())
    # Sync callers and async callers (via to_thread) share the same lock/cache.
    # The lock covers minting, not just insertion: duplicate requests cannot mint twice.
    with _lock:
        cached = _cache.get(key)
        if cached and cached[1] > time.time() + EXPIRY_SKEW:
            return cached[0]
        _cache.pop(key, None)
        try:
            with httpx.Client(timeout=30.0) as client:
                response = client.post(f"{API}/app/installations/{installation}/access_tokens",
                                       headers={"Authorization": f"Bearer {sign_jwt()}",
                                                "Accept": "application/vnd.github+json",
                                                "X-GitHub-Api-Version": "2022-11-28", "User-Agent": "ai-factory"},
                                       json={"permissions": APP_PERMISSIONS})
            if response.status_code != 201:
                raise GitHubError(f"GitHub App installation access failed ({response.status_code}). Manage GitHub access.",
                                  response.status_code)
            data = response.json()
            token = data["token"]
            expires = datetime.fromisoformat(data["expires_at"].replace("Z", "+00:00"))
            if not isinstance(token, str) or not token or expires.tzinfo is None or expires.timestamp() <= time.time():
                raise ValueError("Invalid token response")
        except httpx.HTTPError:
            raise GitHubError("GitHub App installation access is unavailable. Retry or manage GitHub access.") from None
        except (ValueError, KeyError, TypeError, AttributeError, OverflowError):
            raise GitHubError("GitHub App returned an invalid installation credential.") from None
        _cache[key] = (token, expires.timestamp())
        return token


@dataclass(frozen=True, repr=False)
class InstallationCredential:
    workspace_id: str
    installation: str

    def resolve(self) -> str:
        from .github import GitHubError

        if installation_id(self.workspace_id) != self.installation:
            raise GitHubError("GitHub App installation binding changed. Reconnect GitHub access.")
        return installation_token(self.installation)

    def invalidate(self) -> None:
        with _lock:
            _cache.pop((self.installation, _identity()), None)
