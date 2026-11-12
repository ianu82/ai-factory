"""The one way an alert leaves the product: a workspace's webhook (``alerts.webhook_url``), POSTed ``{"text": …}``,
which Slack's incoming webhooks accept as is.

The URL is a secret, kept like the release webhook secret: encrypted as the workspace's ``alert_webhook`` secret and
never returned by the API, which says only whether it is set and its host (view). It must be HTTPS, carry no
credentials, and name a public host: an address that is loopback, link-local or private is refused when it is saved,
and again when a name resolves to one at sending time.
"""
from __future__ import annotations

import ipaddress
import logging
import re
import socket
from typing import Any
from urllib.parse import urlsplit, urlunsplit

import httpx
from sqlalchemy.orm import Session

from .models import Secret
from .security import decrypt, encrypt

log = logging.getLogger(__name__)

KIND = "alert_webhook"
TIMEOUT = 10.0
REFUSED = "The webhook must be an HTTPS URL to a public host, without a user name or password"
_TLD = re.compile(r"[a-z]{2,63}|xn--[a-z0-9-]{1,59}")


def validate(value: Any) -> str:
    """The URL, stripped, or ValueError(REFUSED)."""
    if not isinstance(value, str) or len(value.strip()) > 2000:
        raise ValueError(REFUSED)
    value = value.strip()
    try:
        url = urlsplit(value)
        if (url.scheme != "https" or not url.hostname or url.username is not None or url.password is not None
                or "@" in url.netloc or any(c.isspace() or ord(c) < 32 or ord(c) == 127 for c in value)):
            raise ValueError
        _ = url.port  # a malformed or out-of-range port
    except ValueError:
        raise ValueError(REFUSED) from None
    if not public_host(url.hostname):
        raise ValueError(REFUSED)
    return value


def public_host(host: str) -> bool:
    """Whether a host, as written, may be public: a global IP address, or a name under a real top-level domain (not
    localhost, .local or .internal; a numeric last label is an IP address in disguise, such as 127.1)."""
    host = host.rstrip(".").lower()
    try:
        return ipaddress.ip_address(host).is_global
    except ValueError:
        pass
    if host == "localhost" or host.endswith((".localhost", ".local", ".internal")) or "." not in host:
        return False
    return bool(_TLD.fullmatch(host.rsplit(".", 1)[1]))


def url_for(db: Session, workspace_id: str) -> str:
    """The workspace's webhook URL, or "" when none is set."""
    sec = db.query(Secret).filter(Secret.workspace_id == workspace_id, Secret.kind == KIND).one_or_none()
    return decrypt(sec.ciphertext) if sec is not None and sec.ciphertext else ""


def view(db: Session, workspace_id: str) -> dict[str, Any]:
    """What the API shows of the webhook: whether it is set, and its host. Never the URL."""
    sec = db.query(Secret).filter(Secret.workspace_id == workspace_id, Secret.kind == KIND).one_or_none()
    return {"set": bool(sec is not None and sec.ciphertext), "host": sec.hint if sec is not None else ""}


def save(db: Session, workspace_id: str, value: Any, user_id: str) -> dict[str, Any]:
    """Set the webhook (validated), or clear it with "". In the caller's transaction. Returns the new view."""
    sec = db.query(Secret).filter(Secret.workspace_id == workspace_id, Secret.kind == KIND).one_or_none()
    if isinstance(value, str) and not value.strip():
        if sec is not None:
            db.delete(sec)
        return {"set": False, "host": ""}
    url = validate(value)
    if sec is None:
        sec = Secret(workspace_id=workspace_id, kind=KIND, ciphertext="")
        db.add(sec)
    sec.ciphertext, sec.hint = encrypt(url), (urlsplit(url).hostname or "")[:64]
    sec.status, sec.updated_by = "valid", user_id
    return {"set": True, "host": sec.hint}


def send(url: str, text: str) -> str:
    """POST {"text": text} to the webhook. Returns "" when it was accepted, else why not. Never raises, and never
    names the URL (a secret) in what it returns or logs."""
    try:
        host = urlsplit(validate(url)).hostname or ""
        status = _post(url, {"text": text})
    except Exception as exc:
        return f"the webhook could not be reached ({type(exc).__name__})"
    if not 200 <= status < 300:
        log.warning("alert webhook at %s answered %s", host, status)
        return f"the webhook answered HTTP {status}"
    return ""


def _post(url: str, payload: dict[str, Any]) -> int:
    """Send it, once its host resolves only to public addresses. The connection goes to one of the addresses that
    were checked, not to a second lookup of the name (which could answer differently); the name is kept for the
    certificate and the Host header. Redirects are not followed."""
    parts = urlsplit(url)
    host, port = parts.hostname or "", parts.port or 443
    addresses = [str(info[4][0]).split("%")[0] for info in socket.getaddrinfo(host, port, type=socket.SOCK_STREAM)]
    if not addresses or not all(ipaddress.ip_address(address).is_global for address in addresses):
        raise PermissionError("the webhook's host resolves to a private address")
    literal = f"[{addresses[0]}]" if ":" in addresses[0] else addresses[0]
    target = urlunsplit(("https", f"{literal}:{port}", parts.path or "/", parts.query, ""))
    with httpx.Client(timeout=TIMEOUT, follow_redirects=False) as client:
        return client.post(target, json=payload, headers={"Host": host if port == 443 else f"{host}:{port}"},
                           extensions={"sni_hostname": host}).status_code
