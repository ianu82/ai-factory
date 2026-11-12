"""Declarative Preview QA policy and private repository test-account storage.

No provider credential testing or hint generation belongs on this path. Consumers
must keep decrypted account fields inside the trusted browser helper.
"""
from __future__ import annotations

import ipaddress
import json
import re
from typing import Annotated, Any, Literal
from urllib.parse import quote, unquote, urlsplit

from pydantic import BaseModel, ConfigDict, Field, field_validator
from sqlalchemy.orm import Session

from .models import Secret
from .security import SecretError, decrypt, encrypt

MAX_ACCOUNT_BYTES = 65536
# GitHub PR numbers fit a signed 64-bit integer; commit IDs are SHA-1 hex.
MAX_PR_NUMBER = 2**63 - 1
MAX_HEAD_SHA_LENGTH = 40
PLACEHOLDER = re.compile(r"\{(pr_number|head_sha|branch)\}")
FIELD_NAME = r"^[A-Za-z][A-Za-z0-9_]{0,63}$"
Text = Annotated[str, Field(strict=True, min_length=1, max_length=2048)]
Selector = Annotated[str, Field(strict=True, min_length=1, max_length=512)]
SecretField = Annotated[str, Field(strict=True, pattern=FIELD_NAME)]


def _text(value: str, *, encoded: bool = True) -> None:
    if any(ord(c) <= 32 or ord(c) == 127 for c in value) or "\\" in value:
        raise ValueError("Invalid preview location")
    if encoded and re.search(r"%(?![0-9a-fA-F]{2})", value):
        raise ValueError("Invalid preview location")


def _path(value: str, *, nested: bool = True) -> str:
    _text(value)
    # Parse URL structure before decoding: escaped '#' and '?' are data, not
    # fragment/query delimiters, and an escaped '%' need not start another escape.
    parts = urlsplit(value)
    if (not value.startswith("/") or value.startswith("//") or parts.scheme or parts.netloc
            or "#" in value):
        raise ValueError("Invalid preview path")
    _text(unquote(parts.query), encoded=False)
    path = parts.path if nested else unquote(parts.path)
    # Raw policy paths reject nested traversal/control escapes. Rendered values
    # have already been quoted: decode them only once so a literal branch "%0a"
    # is data, not a second layer of URL syntax.
    while True:
        _text(path, encoded=False)
        if path.startswith("//") or any(p in (".", "..") for p in path.split("/")):
            raise ValueError("Invalid preview path")
        decoded = unquote(path)
        if not nested or decoded == path:
            break
        path = decoded
    return value


def _url(value: str, *, nested: bool = True) -> str:
    _text(value)
    parts = urlsplit(value)
    if (parts.scheme not in ("http", "https") or not parts.hostname or parts.username is not None
            or parts.password is not None or "@" in parts.netloc or "#" in value
            or not re.fullmatch(r"[A-Za-z0-9.\-:\[\]]+", parts.hostname)):
        raise ValueError("Invalid preview URL")
    host = parts.hostname
    if ":" in host:
        ipaddress.IPv6Address(host)
    elif (len(host.rstrip(".")) > 253 or any(
            not re.fullmatch(r"[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?", label)
            for label in host.rstrip(".").split("."))):
        raise ValueError("Invalid preview URL")
    # Accessing port also checks malformed and out-of-range ports.
    parts.port
    _path((parts.path or "/") + ("?" + parts.query if parts.query else ""), nested=nested)
    return value


def validate_url_template(value: str) -> str:
    # Validate DNS labels against the longest revision we can render, not short
    # stand-ins that let adjacent placeholders exceed the 63-byte DNS limit.
    maximums = {"pr_number": str(MAX_PR_NUMBER), "head_sha": "a" * MAX_HEAD_SHA_LENGTH,
                "branch": "preview"}
    remainder = PLACEHOLDER.sub(lambda m: maximums[m[1]], value)
    if "{" in remainder or "}" in remainder:
        raise ValueError("Invalid preview URL template")
    # Only PR numbers and SHAs can be safely rendered into a host label.
    authority = urlsplit(value).netloc
    if "{branch}" in authority or any("{" in part for part in (urlsplit(value).scheme,)):
        raise ValueError("Invalid preview URL template")
    if "{" in authority:
        host = authority.rsplit(":", 1)[0] if ":" in authority else authority
        if "{" in authority[len(host):]:
            raise ValueError("Invalid preview URL template")
    _url(remainder)
    return value


def render_url_template(template: str, *, pr_number: int, head_sha: str, branch: str) -> str:
    validate_url_template(template)
    if (type(pr_number) is not int or not 1 <= pr_number <= MAX_PR_NUMBER
            or not re.fullmatch(r"[0-9a-fA-F]{7,40}", head_sha)):
        raise ValueError("Invalid preview revision")
    values = {"pr_number": str(pr_number), "head_sha": head_sha, "branch": branch}
    rendered = PLACEHOLDER.sub(lambda m: quote(values[m[1]], safe=""), template)
    return _url(rendered, nested=False)


class _Model(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)


class Goto(_Model):
    action: Literal["goto"]
    path: Text

    _valid_path = field_validator("path")(_path)


class Fill(_Model):
    action: Literal["fill"]
    selector: Selector
    secret_field: SecretField


class Click(_Model):
    action: Literal["click"]
    selector: Selector


class WaitForSelector(_Model):
    action: Literal["wait_for_selector"]
    selector: Selector


LoginStep = Annotated[Goto | Fill | Click | WaitForSelector, Field(discriminator="action")]


class Probe(_Model):
    method: Literal["GET", "HEAD", "POST", "PUT", "PATCH", "DELETE"] = "GET"
    path: Text
    expect_status: Annotated[int, Field(strict=True, ge=100, le=599)] = 200

    _valid_path = field_validator("path")(_path)


class PreviewQA(_Model):
    enabled: bool = False
    required: bool = False
    environment_pattern: Annotated[str, Field(min_length=1, max_length=200)] = "*"
    timeout_seconds: Annotated[int, Field(strict=True, ge=1, le=3600)] = 600
    url_template: Text | None = None
    pages: Annotated[list[Text], Field(max_length=50)] = Field(default_factory=list)
    probes: Annotated[list[Probe], Field(max_length=50)] = Field(default_factory=list)
    login_steps: Annotated[list[LoginStep], Field(max_length=50)] = Field(default_factory=list)

    @field_validator("url_template")
    @classmethod
    def valid_template(cls, value):
        return validate_url_template(value) if value is not None else None

    @field_validator("pages")
    @classmethod
    def valid_pages(cls, value):
        return [_path(path) for path in value]

    @field_validator("environment_pattern")
    @classmethod
    def valid_pattern(cls, value):
        if any(ord(c) < 32 or ord(c) == 127 for c in value):
            raise ValueError("Invalid environment pattern")
        return value


def preview_qa(config: dict[str, Any] | None = None) -> dict[str, Any]:
    """Read default-off policy without mutating legacy repository JSON."""
    return PreviewQA.model_validate((config or {}).get("preview_qa", {})).model_dump()


def validate_config(value: Any, current: Any = None) -> dict[str, Any]:
    try:
        if not isinstance(value, dict):
            raise ValueError
        return PreviewQA.model_validate({**(current or {}), **value}).model_dump()
    except (ValueError, TypeError):
        raise ValueError("Invalid Preview QA configuration") from None


def account_kind(repo_id: str) -> str:
    # Repository ids are 32 hex characters, so the complete kind fits String(40).
    if not re.fullmatch(r"[0-9a-f]{32}", repo_id):
        raise ValueError("Invalid repository id")
    return "preview_" + repo_id


def account(db: Session, workspace_id: str, repo_id: str) -> Secret | None:
    return db.query(Secret).filter_by(workspace_id=workspace_id, kind=account_kind(repo_id)).one_or_none()


def validate_account(body: Any) -> dict[str, str]:
    if not isinstance(body, dict) or set(body) != {"fields"}:
        raise ValueError("Invalid preview account request")
    fields = body["fields"]
    if (not isinstance(fields, dict) or not 1 <= len(fields) <= 32
            or any(not isinstance(k, str) or not re.fullmatch(FIELD_NAME, k)
                   or not isinstance(v, str) or not 1 <= len(v) <= 8192 for k, v in fields.items())):
        raise ValueError("Invalid preview account request")
    return fields


def account_metadata(sec: Secret | None) -> dict[str, Any]:
    # Names are metadata; values are never decrypted for API responses.
    return {"configured": sec is not None, "status": "configured" if sec else "missing",
            "fields": sorted((sec.metadata_json or {}).get("fields", [])) if sec else []}


def save_account(db: Session, workspace_id: str, repo_id: str, fields: dict[str, str], actor: str) -> Secret:
    sec = account(db, workspace_id, repo_id)
    if sec is None:
        sec = Secret(workspace_id=workspace_id, kind=account_kind(repo_id))
        db.add(sec)
    sec.ciphertext = encrypt(json.dumps(fields, ensure_ascii=True))
    sec.hint, sec.status, sec.status_detail = "", "untested", ""
    sec.metadata_json, sec.updated_by, sec.tested_at = {"fields": sorted(fields)}, actor, None
    return sec


def load_account(db: Session, workspace_id: str, repo_id: str) -> dict[str, str]:
    """Trusted helper only: do not place this result in agent context or evidence."""
    sec = account(db, workspace_id, repo_id)
    if sec is None:
        raise ValueError("Preview account is not configured")
    return validate_account({"fields": json.loads(decrypt(sec.ciphertext))})


def redaction_values(sec: Secret) -> tuple[str, ...]:
    """Exact-match egress tokens, including each private account field.

    Accounts remain one encrypted JSON document at rest, but that document is
    not an adequate redaction token for a password appearing on its own. Keep
    ordinary provider secrets unchanged and fail closed on malformed accounts.
    """
    plaintext = decrypt(sec.ciphertext)
    if not sec.kind.startswith("preview_"):
        return (plaintext,)
    try:
        fields = validate_account({"fields": json.loads(plaintext)})
    except (ValueError, TypeError, RecursionError):
        raise SecretError("Stored preview account is invalid") from None
    return (plaintext, *fields.values())
