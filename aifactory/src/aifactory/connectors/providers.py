"""Transient connector token resolution; no bootstrap credentials or token caching."""

from __future__ import annotations

import json
from dataclasses import dataclass, field

import httpx
from sqlalchemy.orm import Session

from ..models import Connector
from .core import ConnectorError, STATUS_DETAILS, validate_scope, workspace_secret

OAUTH_BASE = "https://auth.mindshub.ai/v1/oauth"
OAUTH_TIMEOUT = httpx.Timeout(10.0, connect=3.0, pool=3.0)
MAX_RESPONSE_BYTES = 16384
TOKEN_KINDS = {"slack": "slack_bot_token", "sentry": "sentry_token"}


@dataclass(frozen=True)
class TokenResolution:
    token: str | None = field(default=None, repr=False)
    provider: str = "workspace"
    status: str = "ready"
    fallback_reason: str | None = None

    @property
    def detail(self) -> str:
        if self.fallback_reason:
            return "Using workspace token. " + STATUS_DETAILS[self.fallback_reason]
        return STATUS_DETAILS[self.status]

    def require_token(self) -> str:
        if self.token is None:
            raise ConnectorError(self.status)
        return self.token


async def _oauth_token(service: str, key: str) -> tuple[str | None, str]:
    # Both service and URL are fixed internally; neither config nor env can redirect
    # a workspace key. No redirect following, response logging or error-body storage.
    try:
        async with httpx.AsyncClient(timeout=OAUTH_TIMEOUT, follow_redirects=False) as client:
            async with client.stream("POST", f"{OAUTH_BASE}/{service}/token",
                                     headers={"Authorization": f"Bearer {key}"}) as response:
                if response.status_code == 401:
                    return None, "oauth_unauthorized"
                if response.status_code in (404, 409):
                    return None, "missing_connection"
                if not 200 <= response.status_code < 300:
                    return None, "oauth_http"
                content = bytearray()
                async for chunk in response.aiter_bytes(chunk_size=MAX_RESPONSE_BYTES + 1):
                    if len(content) + len(chunk) > MAX_RESPONSE_BYTES:
                        return None, "oauth_response"
                    content.extend(chunk)
                try:
                    data = json.loads(content)
                except (ValueError, UnicodeError):
                    return None, "oauth_response"
                if not isinstance(data, dict):
                    return None, "oauth_response"
                token, expires = data.get("access_token"), data.get("expires_in")
                if (not isinstance(token, str) or not token.strip() or len(token) > 8192
                        or any(c.isspace() or ord(c) < 33 or ord(c) > 126 for c in token)
                        or type(expires) not in (int, float) or not 0 < expires <= 86400):
                    return None, "oauth_response"
                return token, "ready"
    except httpx.TimeoutException:
        return None, "oauth_timeout"
    except (httpx.RequestError, ValueError):
        return None, "oauth_network"


async def resolve_token(db: Session, workspace_id: str, connector: Connector) -> TokenResolution:
    """Resolve a Slack/Sentry API token; signing secrets always remain local.

    Requires BOTH connector.provider='mindshub' and the literal workspace setting
    connectors_mindshub_enabled=True to make a request. Call before acquiring intake
    locks; never persist the returned token or serialize the dataclass with asdict.
    Missing fallback credentials return an actionable failure, not false success.
    """
    workspace = validate_scope(db, workspace_id, connector)
    kind = TOKEN_KINDS.get(connector.kind)
    if kind is None:
        raise ConnectorError("invalid_input")
    local_error = None
    try:
        local = workspace_secret(db, workspace_id, kind)
    except ConnectorError as exc:
        local, local_error = None, exc.code
    if connector.provider == "workspace":
        return TokenResolution(local, status="ready" if local else local_error or "missing_token")
    reason = "oauth_disabled"
    if (workspace.settings or {}).get("connectors_mindshub_enabled") is True:
        try:
            key = workspace_secret(db, workspace_id, "mindshub_api_key")
        except ConnectorError:
            key = None
        if key:
            token, reason = await _oauth_token(connector.kind, key)
            if token:
                return TokenResolution(token, provider="mindshub")
        else:
            reason = "missing_key"
    if local:
        return TokenResolution(local, status="ready", fallback_reason=reason)
    return TokenResolution(provider="mindshub", status=reason)
