"""Secret encryption, opaque tokens and masking.

Workspace credentials are encrypted at rest with a key derived from AIFACTORY_SECRET_KEY.
Plaintext only exists transiently in the control-plane process; it is never written to
job sandboxes, events, artifacts, logs or API responses.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import os
import re
import secrets
from functools import lru_cache
from pathlib import Path

from cryptography.fernet import Fernet, InvalidToken

from .config import get_settings


class SecretError(RuntimeError):
    pass


def _master_key() -> bytes:
    settings = get_settings()
    material = settings.secret_key
    if not material:
        # Development fallback: a persistent random key in the data directory.
        path = settings.data_dir / ".secret-key"
        settings.data_dir.mkdir(parents=True, exist_ok=True)
        if not path.exists():
            path.write_text(secrets.token_urlsafe(48))
            os.chmod(path, 0o600)
        material = path.read_text().strip()
    return hashlib.sha256(("aifactory-v2:" + material).encode()).digest()


@lru_cache(maxsize=1)
def _fernet() -> Fernet:
    return Fernet(base64.urlsafe_b64encode(_master_key()))


def reset_crypto_cache() -> None:
    _fernet.cache_clear()


def encrypt(value: str) -> str:
    return _fernet().encrypt(value.encode()).decode()


def decrypt(token: str) -> str:
    try:
        return _fernet().decrypt(token.encode()).decode()
    except InvalidToken as exc:  # pragma: no cover - key rotation problem
        raise SecretError("Stored credential cannot be decrypted with the current server key") from exc


def new_token(prefix: str = "") -> str:
    return prefix + secrets.token_urlsafe(32)


def token_hash(token: str) -> str:
    return hashlib.sha256(token.encode()).hexdigest()


def sign(value: str) -> str:
    mac = hmac.new(_master_key(), value.encode(), hashlib.sha256).hexdigest()[:32]
    return f"{value}.{mac}"


def unsign(signed: str) -> str | None:
    value, _, mac = signed.rpartition(".")
    if not value or not hmac.compare_digest(sign(value), signed):
        return None
    return value


def mask(value: str) -> str:
    value = value.strip()
    if len(value) < 16:
        return "••••…" + value[-2:] if len(value) > 2 else "••••"
    prefix = value.split(".", 1)[0][:12] if value.startswith("mdb_") else value[:6]
    return f"{prefix}…{value[-4:]}"


_SECRET_PATTERNS = [
    re.compile(r"-----BEGIN (?:[A-Z0-9]+ )*PRIVATE KEY-----[\s\S]*?-----END (?:[A-Z0-9]+ )*PRIVATE KEY-----"),
    re.compile(r"\b(?:AKIA|ASIA)[A-Z0-9]{16}\b"),
    re.compile(r"\bxox[baprs]-[A-Za-z0-9-]{10,}"),
    re.compile(r"\bmdb_[A-Za-z0-9_.-]{8,}"),
    re.compile(r"\beyJ[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+"),
    re.compile(r"lin_(?:api|oauth)_[A-Za-z0-9_-]+"),
    re.compile(r"mdb_[A-Za-z0-9_-]+\.[A-Za-z0-9_\-.]+"),
    re.compile(r"sk-[A-Za-z0-9_\-]{16,}"),
    re.compile(r"gh[pousr]_[A-Za-z0-9]{20,}"),
    re.compile(r"github_pat_[A-Za-z0-9_]{20,}"),
    re.compile(r"x-access-token:[^@\s]+@"),
    re.compile(r"(?i)(authorization:\s*bearer\s+)[A-Za-z0-9._\-]+"),
]


def redact(text: str, extra: tuple[str, ...] = ()) -> str:
    """Remove anything that looks like a credential from text that will be stored or shown."""
    if not text:
        return text
    for value in extra:
        if value:
            text = text.replace(value, "[redacted]")
    for pattern in _SECRET_PATTERNS:
        text = pattern.sub(lambda m: (m.group(1) if m.groups() else "") + "[redacted]", text)
    return text


def ensure_private_dir(path: Path) -> Path:
    path.mkdir(parents=True, exist_ok=True)
    try:
        os.chmod(path, 0o700)
    except PermissionError:  # pragma: no cover
        pass
    return path
