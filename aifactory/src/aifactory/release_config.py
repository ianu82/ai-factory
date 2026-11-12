"""Validated repository release settings. Legacy continuous settings are lossless.

Secret references are internal: public configuration exposes only configured state.
A webhook must explicitly promise synchronous completion or describe bounded polling;
acceptance of a request alone is never evidence of a successful deployment.
"""
from __future__ import annotations

import hashlib
import re
from copy import deepcopy
from typing import Annotated, Any, Literal
from urllib.parse import urlsplit
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from pydantic import BaseModel, ConfigDict, Field, StringConstraints, field_validator, model_validator
from sqlalchemy.orm import Session

from .models import Secret
from .security import decrypt, redact

Name = Annotated[str, StringConstraints(strict=True, min_length=1, max_length=200, pattern=r"\S")]
Command = Annotated[str, StringConstraints(strict=True, min_length=1, max_length=4000)]
Weekday = Literal["mon", "tue", "wed", "thu", "fri", "sat", "sun"]
ApprovalPolicy = Literal["inherit", "approval", "auto"]


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


def safe_url(value: str) -> str:
    try:
        url = urlsplit(value)
        if (url.scheme != "https" or not url.hostname or url.username or url.password
                or url.query or url.fragment or any(c.isspace() for c in value)):
            raise ValueError
        _ = url.port
    except ValueError:
        raise ValueError("Use an HTTPS URL without credentials, query or fragment") from None
    return value


class ReleaseSchedule(StrictModel):
    weekdays: set[Weekday] = Field(default_factory=lambda: {"mon", "tue", "wed", "thu", "fri"}, min_length=1)
    time: str = "09:00"
    timezone: str = "UTC"

    @field_validator("time")
    @classmethod
    def local_time(cls, value: str) -> str:
        if not re.fullmatch(r"(?:[01][0-9]|2[0-3]):[0-5][0-9]", value):
            raise ValueError("Use local HH:MM time")
        return value

    @field_validator("timezone")
    @classmethod
    def timezone_name(cls, value: str) -> str:
        try:
            ZoneInfo(value)
        except (ValueError, ZoneInfoNotFoundError):
            raise ValueError("Unknown ZoneInfo timezone") from None
        return value

    @property
    def zone(self) -> ZoneInfo:
        return ZoneInfo(self.timezone)


class VersionFile(StrictModel):
    path: Name
    format: Literal["plain", "json"] = "plain"
    key: Name | None = None  # one literal JSON object key; no parser or expression language

    @field_validator("path")
    @classmethod
    def safe_path(cls, value: str) -> str:
        if (value.startswith("/") or "\\" in value or ":" in value or "\x00" in value
                or any(part in ("", ".", "..", ".git") for part in value.split("/"))):
            raise ValueError("Version file must be a safe repository-relative path")
        return value

    @model_validator(mode="after")
    def json_key(self):
        if (self.format == "json") != (self.key is not None):
            raise ValueError("JSON version files require a key; plain files must not have a key")
        return self


class ReleaseCut(StrictModel):
    mode: Literal["tag", "version_pr"] = "tag"
    tag_prefix: str = Field(default="v", max_length=40)
    initial_version: str = "0.1.0"
    increment: Literal["patch"] = "patch"
    version_file: VersionFile | None = None

    @field_validator("initial_version")
    @classmethod
    def semantic_version(cls, value: str) -> str:
        if not re.fullmatch(r"(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)", value):
            raise ValueError("Use a major.minor.patch semantic version")
        return value

    @field_validator("tag_prefix")
    @classmethod
    def safe_prefix(cls, value: str) -> str:
        if not re.fullmatch(r"[A-Za-z0-9_-]*", value):
            raise ValueError("Unsafe tag prefix")
        return value

    @model_validator(mode="after")
    def file_required(self):
        if (self.mode == "version_pr") != (self.version_file is not None):
            raise ValueError("Only version_pr cutting requires a version file")
        return self

    def next_version(self, previous: str | None = None) -> str:
        if previous is None:
            return self.initial_version
        self.semantic_version(previous)
        major, minor, patch = map(int, previous.split("."))
        return f"{major}.{minor}.{patch + 1}"


class WorkflowTarget(StrictModel):
    kind: Literal["workflow"]
    workflow: Name
    inputs: dict[Name, Annotated[str, StringConstraints(strict=True, max_length=2000)]] = Field(default_factory=dict, max_length=30)
    correlation_input: Name

    @model_validator(mode="after")
    def correlation_reserved(self):
        if self.correlation_input in self.inputs:
            raise ValueError("Correlation input is reserved for the stable operation key")
        return self


class SynchronousCompletion(StrictModel):
    mode: Literal["synchronous"]
    success_statuses: list[int] = Field(default_factory=lambda: [200, 204], min_length=1, max_length=10)

    @field_validator("success_statuses")
    @classmethod
    def terminal_statuses(cls, value):
        if any(type(code) is not int or code < 200 or code > 299 or code == 202 for code in value):
            raise ValueError("Synchronous completion requires final 2xx statuses, never 202")
        return value


class PollingCompletion(StrictModel):
    mode: Literal["poll"]
    status_url: str = Field(max_length=2000)
    handle_key: Name
    state_key: Name
    success_values: list[Name] = Field(min_length=1, max_length=20)
    failure_values: list[Name] = Field(min_length=1, max_length=20)
    interval_seconds: int = Field(default=10, ge=1, le=300, strict=True)
    timeout_seconds: int = Field(default=1800, ge=1, le=86400, strict=True)

    _url = field_validator("status_url")(safe_url)

    @model_validator(mode="after")
    def distinct_states(self):
        if set(self.success_values) & set(self.failure_values):
            raise ValueError("Polling terminal states must be disjoint")
        return self


class WebhookTarget(StrictModel):
    kind: Literal["webhook"]
    endpoint: str = Field(max_length=2000)
    secret_ref: Annotated[str, StringConstraints(pattern=r"^[a-f0-9]{32}$")]
    timeout_seconds: int = Field(default=30, ge=1, le=120, strict=True)
    completion: Annotated[SynchronousCompletion | PollingCompletion, Field(discriminator="mode")]

    _url = field_validator("endpoint")(safe_url)


DeployTarget = Annotated[WorkflowTarget | WebhookTarget, Field(discriminator="kind")]


class SmokeDefinition(StrictModel):
    commands: list[Command] = Field(default_factory=list, max_length=20)
    urls: list[Annotated[str, StringConstraints(max_length=2000)]] = Field(default_factory=list, max_length=20)
    timeout_seconds: int = Field(default=60, ge=1, le=600, strict=True)

    @field_validator("urls")
    @classmethod
    def safe_urls(cls, value):
        return [safe_url(url) for url in value]

    @model_validator(mode="after")
    def smoke_required(self):
        if not self.commands and not self.urls:
            raise ValueError("Smoke requires at least one command or URL")
        if any(not command.strip() for command in self.commands):
            raise ValueError("Smoke commands cannot be blank")
        return self


class TrainReleaseConfig(StrictModel):
    mode: Literal["train"]
    schedule: ReleaseSchedule = Field(default_factory=ReleaseSchedule)
    cut: ReleaseCut = Field(default_factory=ReleaseCut)
    staging: DeployTarget
    production: DeployTarget
    staging_smoke: SmokeDefinition
    production_smoke: SmokeDefinition
    rollback: DeployTarget
    approval: ApprovalPolicy = "inherit"


class ContinuousReleaseConfig(BaseModel):
    # Older detectors and Settings clients own additional fields; never drop them.
    model_config = ConfigDict(extra="allow")
    mode: Literal["continuous", "on_merge"] | None = None
    health_url: str | None = None


ReleaseConfig = Annotated[TrainReleaseConfig | ContinuousReleaseConfig, Field(union_mode="left_to_right")]


def webhook_secret_kind(repo_id: str) -> str:
    # Stable, repository-specific and within Secret.kind's String(40).
    return "relhook:" + hashlib.sha256(repo_id.encode()).hexdigest()[:32]


def _secret_key(key: str) -> bool:
    normalized = re.sub(r"[^a-z0-9]", "", key.lower())
    return normalized in {"secret", "secretref", "webhooksecret", "token", "password", "authorization",
                          "apikey", "accesstoken", "ciphertext"}


def release_public(value: Any) -> Any:
    """Hide secret references only. Do not rewrite URLs or unrelated configuration."""
    if isinstance(value, dict):
        result = {}
        for key, item in value.items():
            if _secret_key(key):
                result["secret_configured" if key == "secret_ref" else key + "_configured"] = bool(item)
            else:
                result[key] = release_public(item)
        return result
    if isinstance(value, list):
        return [release_public(item) for item in value]
    return value


def validate_release_patch(value: Any, *, db: Session, workspace_id: str, repo_id: str) -> dict | None:
    """Validate before assignment. Errors deliberately contain no submitted values."""
    try:
        if value is None:
            return None
        if not isinstance(value, dict):
            raise ValueError
        if value.get("mode") != "train":
            ContinuousReleaseConfig.model_validate(value)
            # Legacy settings may contain URLs with query strings and arbitrary fields.
            return deepcopy(value)
        data = deepcopy(value)
        stored = db.query(Secret).filter(Secret.workspace_id == workspace_id,
                                        Secret.kind == webhook_secret_kind(repo_id)).one_or_none()
        for name in ("staging", "production", "rollback"):
            target = data.get(name)
            if isinstance(target, dict) and target.get("kind") == "webhook":
                target.pop("secret_configured", None)
                if "secret_ref" not in target and stored:
                    target["secret_ref"] = stored.id
        parsed = TrainReleaseConfig.model_validate(data)
        for target in (parsed.staging, parsed.production, parsed.rollback):
            if isinstance(target, WebhookTarget) and (not stored or target.secret_ref != stored.id):
                raise ValueError
        # Inputs and commands must use CI-owned credentials, never inline credentials.
        def reject_inline(item):
            if isinstance(item, dict):
                for key, child in item.items():
                    if _secret_key(key) and key != "secret_ref":
                        raise ValueError
                    reject_inline(child)
            elif isinstance(item, list):
                for child in item:
                    reject_inline(child)
            elif isinstance(item, str) and redact(item) != item:
                raise ValueError
        reject_inline(data)
        credentials = tuple(decrypt(secret.ciphertext) for secret in db.query(Secret).filter(
            Secret.workspace_id == workspace_id))
        def reject_known(item):
            if isinstance(item, str) and redact(item, credentials) != item:
                raise ValueError
            if isinstance(item, dict):
                for key, child in item.items():
                    reject_known(key)
                    reject_known(child)
            elif isinstance(item, list):
                for child in item:
                    reject_known(child)
        reject_known(data)
        # Store only what was configured, not model defaults; references remain internal.
        return data
    except (ValueError, TypeError):
        raise ValueError("Invalid release configuration or webhook secret reference") from None


def effective_release_approval(config: TrainReleaseConfig, settings: dict) -> str:
    if config.approval != "inherit":
        return config.approval
    return (settings.get("autonomy") or {}).get("release", "approval")


def _redact_evidence_url(match: re.Match) -> str:
    try:
        url = urlsplit(match.group())
        if url.username or url.password or url.query or url.fragment:
            return "[redacted URL]"
    except ValueError:
        return "[redacted URL]"
    return match.group()


def release_redacted(value: Any, secrets: tuple[str, ...] = (), *, depth: int = 0) -> Any:
    """Bounded snapshot/log scrubber, shared by all durable release egress fields."""
    if depth > 12:
        return "[truncated]"
    if isinstance(value, str):
        text = redact(value, secrets)
        text = re.sub(r"(?i)(\b(?:password|secret|token|api[_-]?key|authorization)\s*[:=]\s*)([^\s,;]+)",
                      r"\1[redacted]", text)
        # URLs in evidence may be external handles with signed query strings.
        # This scrubber is intentionally NOT used for editable legacy config.
        text = re.sub(r"https?://[^\s<>\"]+", _redact_evidence_url, text)
        return text[:16000]
    if isinstance(value, dict):
        return {redact(str(key), secrets)[:200]: (bool(item) if _secret_key(str(key)) else
                release_redacted(item, secrets, depth=depth + 1)) for key, item in list(value.items())[:100]}
    if isinstance(value, list):
        return [release_redacted(item, secrets, depth=depth + 1) for item in value[:100]]
    return value
