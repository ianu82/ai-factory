from __future__ import annotations

import hashlib
import hmac
import json
import os
import re
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Iterator
from urllib import error as urllib_error
from urllib import request as urllib_request
from uuid import uuid4

from .automation import AutomationError, FactoryAutomationCoordinator
from .contracts import load_validators, validation_errors_for
from .intake import build_manual_intake_item, normalize_whitespace, repo_root, utc_now

DEFAULT_LINEAR_GRAPHQL_URL = "https://api.linear.app/graphql"
DEFAULT_WEBHOOK_MAX_AGE_SECONDS = 60
CHECKLIST_ITEM_PATTERN = re.compile(r"^[-*]\s*\[[ xX]\]\s+(.+)$")
PLAIN_BULLET_PATTERN = re.compile(r"^[-*]\s+(.+)$")


class LinearTriggerError(RuntimeError):
    """Base class for Linear trigger failures."""


class LinearConfigurationError(LinearTriggerError):
    """Raised when Linear trigger configuration is incomplete or invalid."""


class LinearWebhookVerificationError(LinearTriggerError):
    """Raised when a webhook request cannot be trusted."""


class LinearTriggerStoreError(LinearTriggerError):
    """Raised when persisted Linear trigger artifacts cannot be saved or loaded."""


class LinearTriggerLeaseBusyError(LinearTriggerError):
    """Raised when another worker already owns a Linear trigger lease."""


class LinearGraphQLClientError(LinearTriggerError):
    """Raised when Linear GraphQL fetches or mutations fail."""


def parse_utc_timestamp(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)


def _bool_from_env(name: str, *, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    normalized = raw.strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    raise LinearConfigurationError(
        f"{name} must be one of true/false, yes/no, on/off, or 1/0."
    )


def _header_value(headers: dict[str, str], name: str) -> str | None:
    target = name.lower()
    for key, value in headers.items():
        if key.lower() == target:
            return value
    return None


def verify_linear_signature(raw_body: bytes, header_signature: str | None, secret: str) -> bool:
    if not header_signature:
        return False
    computed = hmac.new(
        secret.encode("utf-8"),
        raw_body,
        hashlib.sha256,
    ).hexdigest()
    return hmac.compare_digest(computed, header_signature.strip().lower())


def webhook_timestamp_is_fresh(
    webhook_timestamp_ms: int,
    *,
    now_ms: int | None = None,
    max_age_seconds: int = DEFAULT_WEBHOOK_MAX_AGE_SECONDS,
) -> bool:
    reference_ms = now_ms if now_ms is not None else int(datetime.now(timezone.utc).timestamp() * 1000)
    return abs(reference_ms - webhook_timestamp_ms) <= max_age_seconds * 1000


@dataclass(slots=True)
class LinearTriggerConfig:
    target_team_id: str
    target_state_id: str
    webhook_secret: str | None = None
    api_key: str | None = None
    trigger_base_url: str | None = None
    comment_on_accept: bool = True
    comment_on_reject: bool = True
    graphql_url: str = DEFAULT_LINEAR_GRAPHQL_URL
    webhook_max_age_seconds: int = DEFAULT_WEBHOOK_MAX_AGE_SECONDS

    @classmethod
    def from_env(
        cls,
        *,
        require_webhook_secret: bool = False,
        require_api_key: bool = False,
    ) -> "LinearTriggerConfig":
        target_team_id = os.environ.get("LINEAR_TARGET_TEAM_ID", "").strip()
        target_state_id = os.environ.get("LINEAR_TARGET_STATE_ID", "").strip()
        webhook_secret = os.environ.get("LINEAR_WEBHOOK_SECRET", "").strip() or None
        api_key = os.environ.get("LINEAR_API_KEY", "").strip() or None
        trigger_base_url = os.environ.get("FACTORY_TRIGGER_BASE_URL", "").strip() or None
        graphql_url = os.environ.get("LINEAR_GRAPHQL_URL", "").strip() or DEFAULT_LINEAR_GRAPHQL_URL
        raw_max_age = os.environ.get("LINEAR_WEBHOOK_MAX_AGE_SECONDS", "").strip()
        if not target_team_id:
            raise LinearConfigurationError("LINEAR_TARGET_TEAM_ID is required.")
        if not target_state_id:
            raise LinearConfigurationError("LINEAR_TARGET_STATE_ID is required.")
        if require_webhook_secret and not webhook_secret:
            raise LinearConfigurationError("LINEAR_WEBHOOK_SECRET is required.")
        if require_api_key and not api_key:
            raise LinearConfigurationError("LINEAR_API_KEY is required.")
        if raw_max_age:
            try:
                webhook_max_age_seconds = int(raw_max_age)
            except ValueError as exc:
                raise LinearConfigurationError(
                    "LINEAR_WEBHOOK_MAX_AGE_SECONDS must be an integer."
                ) from exc
            if webhook_max_age_seconds < 1:
                raise LinearConfigurationError(
                    "LINEAR_WEBHOOK_MAX_AGE_SECONDS must be >= 1."
                )
        else:
            webhook_max_age_seconds = DEFAULT_WEBHOOK_MAX_AGE_SECONDS
        return cls(
            target_team_id=target_team_id,
            target_state_id=target_state_id,
            webhook_secret=webhook_secret,
            api_key=api_key,
            trigger_base_url=trigger_base_url,
            comment_on_accept=_bool_from_env("LINEAR_COMMENT_ON_ACCEPT", default=True),
            comment_on_reject=_bool_from_env("LINEAR_COMMENT_ON_REJECT", default=True),
            graphql_url=graphql_url,
            webhook_max_age_seconds=webhook_max_age_seconds,
        )


@dataclass(slots=True)
class LinearTriggerState:
    version: int = 1
    processed_delivery_ids: list[str] = field(default_factory=list)
    processed_logical_trigger_keys: list[str] = field(default_factory=list)
    updated_at: str = field(default_factory=utc_now)

    @classmethod
    def from_document(cls, document: dict[str, Any]) -> "LinearTriggerState":
        return cls(
            version=int(document["version"]),
            processed_delivery_ids=list(document["processed_delivery_ids"]),
            processed_logical_trigger_keys=list(document["processed_logical_trigger_keys"]),
            updated_at=document["updated_at"],
        )

    def to_document(self) -> dict[str, Any]:
        return {
            "version": self.version,
            "processed_delivery_ids": list(self.processed_delivery_ids),
            "processed_logical_trigger_keys": list(self.processed_logical_trigger_keys),
            "updated_at": self.updated_at,
        }


@dataclass(slots=True)
class LinearWebhookEnvelope:
    delivery_id: str
    event_type: str
    received_at: str
    logical_trigger_key: str
    issue_id: str
    team_id: str
    state_id: str
    payload: dict[str, Any]

    @classmethod
    def from_payload(
        cls,
        *,
        delivery_id: str,
        event_type: str,
        received_at: str,
        payload: dict[str, Any],
    ) -> "LinearWebhookEnvelope":
        data = payload["data"]
        logical_trigger_key = (
            f"linear:{data['id']}:{data['stateId']}:{payload['createdAt']}"
        )
        return cls(
            delivery_id=delivery_id,
            event_type=event_type,
            received_at=received_at,
            logical_trigger_key=logical_trigger_key,
            issue_id=str(data["id"]),
            team_id=str(data["teamId"]),
            state_id=str(data["stateId"]),
            payload=payload,
        )

    def to_document(self) -> dict[str, Any]:
        return {
            "delivery_id": self.delivery_id,
            "event_type": self.event_type,
            "received_at": self.received_at,
            "logical_trigger_key": self.logical_trigger_key,
            "issue_id": self.issue_id,
            "team_id": self.team_id,
            "state_id": self.state_id,
            "payload": self.payload,
        }


@dataclass(slots=True)
class LinearIssueSnapshot:
    id: str
    identifier: str
    title: str
    description: str
    url: str
    team: dict[str, str | None]
    state: dict[str, str | None]
    labels: list[str]
    priority: int | None
    project: dict[str, str | None] | None
    creator: dict[str, str | None] | None
    assignee: dict[str, str | None] | None
    created_at: str
    updated_at: str
    comments: list[dict[str, str | None]]

    def to_document(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "identifier": self.identifier,
            "title": self.title,
            "description": self.description,
            "url": self.url,
            "team": dict(self.team),
            "state": dict(self.state),
            "labels": list(self.labels),
            "priority": self.priority,
            "project": None if self.project is None else dict(self.project),
            "creator": None if self.creator is None else dict(self.creator),
            "assignee": None if self.assignee is None else dict(self.assignee),
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "comments": [dict(comment) for comment in self.comments],
        }


@dataclass(slots=True)
class LinearWebhookResponse:
    status_code: int
    document: dict[str, Any]


@dataclass(slots=True)
class LinearTriggerCycleResult:
    processed_events: list[dict[str, Any]]
    skipped_events: list[dict[str, str]]
    failed_events: list[dict[str, str]]
    trigger_state: LinearTriggerState

    def failed_handoffs(self) -> list[dict[str, Any]]:
        return [
            event
            for event in self.processed_events
            if isinstance(event.get("handoff"), dict)
            and str(event["handoff"].get("status", "")).lower() == "failed"
        ]

    def to_document(self) -> dict[str, Any]:
        return {
            "cycle": "linear-trigger",
            "processed_events": list(self.processed_events),
            "skipped_events": list(self.skipped_events),
            "failed_events": list(self.failed_events),
            "trigger_state": self.trigger_state.to_document(),
        }


class LinearTriggerStore:
    """Persist incoming Linear trigger envelopes and worker state."""

    def __init__(
        self,
        root: Path,
        *,
        repo_root_override: Path | None = None,
    ) -> None:
        self.root = root.resolve()
        self.repo_root = repo_root(repo_root_override)
        self.inbox_dir = self.root / "linear-trigger-inbox"
        self.state_path = self.root / "linear-trigger-state.json"
        validators = load_validators(self.repo_root)
        self.state_validator = validators["linear-trigger-state"]
        self.envelope_validator = validators["linear-webhook-envelope"]

    def load_state(self) -> LinearTriggerState:
        document = self._load_state_document()
        if document is None:
            return LinearTriggerState()
        return LinearTriggerState.from_document(document)

    def save_state(
        self,
        state: LinearTriggerState,
        *,
        expected_previous_updated_at: str | None = None,
    ) -> Path:
        self.root.mkdir(parents=True, exist_ok=True)
        document = state.to_document()
        self._validate_state_document(document)
        if expected_previous_updated_at is not None:
            current_document = self._load_state_document()
            current_updated_at = (
                None if current_document is None else current_document.get("updated_at")
            )
            if current_updated_at != expected_previous_updated_at:
                raise LinearTriggerStoreError(
                    "linear-trigger-state changed during update: expected previous updated_at "
                    f"'{expected_previous_updated_at}', found '{current_updated_at}'."
                )
        self._write_json_atomic(self.state_path, document)
        return self.state_path

    @contextmanager
    def state_transaction(self) -> Iterator[LinearTriggerState]:
        with self._lease(self.root / ".linear-trigger-state.lock", "linear-trigger-state"):
            state_previously_existed = self.state_path.exists()
            state = self.load_state()
            expected_previous_updated_at = state.updated_at if state_previously_existed else None
            yield state
            self.save_state(
                state,
                expected_previous_updated_at=expected_previous_updated_at,
            )

    @contextmanager
    def event_lease(self, delivery_id: str) -> Iterator[None]:
        lock_path = self.inbox_dir / f".{delivery_id}.lock"
        with self._lease(lock_path, delivery_id):
            yield

    def save_envelope(self, envelope: LinearWebhookEnvelope) -> Path:
        self.inbox_dir.mkdir(parents=True, exist_ok=True)
        path = self.inbox_dir / f"{envelope.delivery_id}.json"
        document = envelope.to_document()
        self._validate_envelope_document(document)
        if path.exists():
            return path
        self._write_json_atomic(path, document)
        return path

    def load_envelope(self, path: Path) -> LinearWebhookEnvelope:
        try:
            document = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise LinearTriggerStoreError(
                f"Could not read Linear trigger envelope at {path}: {exc}"
            ) from exc
        self._validate_envelope_document(document)
        return LinearWebhookEnvelope(
            delivery_id=document["delivery_id"],
            event_type=document["event_type"],
            received_at=document["received_at"],
            logical_trigger_key=document["logical_trigger_key"],
            issue_id=document["issue_id"],
            team_id=document["team_id"],
            state_id=document["state_id"],
            payload=document["payload"],
        )

    def iter_envelope_paths(self) -> list[Path]:
        if not self.inbox_dir.exists():
            return []
        return sorted(
            path
            for path in self.inbox_dir.iterdir()
            if path.is_file() and path.suffix == ".json"
        )

    def mark_processed(
        self,
        state: LinearTriggerState,
        envelope: LinearWebhookEnvelope,
    ) -> None:
        changed = False
        if envelope.delivery_id not in state.processed_delivery_ids:
            state.processed_delivery_ids.append(envelope.delivery_id)
            state.processed_delivery_ids.sort()
            changed = True
        if envelope.logical_trigger_key not in state.processed_logical_trigger_keys:
            state.processed_logical_trigger_keys.append(envelope.logical_trigger_key)
            state.processed_logical_trigger_keys.sort()
            changed = True
        if changed:
            state.updated_at = utc_now()

    def mark_delivery_processed(
        self,
        state: LinearTriggerState,
        delivery_id: str,
    ) -> None:
        if delivery_id not in state.processed_delivery_ids:
            state.processed_delivery_ids.append(delivery_id)
            state.processed_delivery_ids.sort()
            state.updated_at = utc_now()

    @staticmethod
    def remove_envelope(path: Path) -> None:
        path.unlink(missing_ok=True)

    def _load_state_document(self) -> dict[str, Any] | None:
        if not self.state_path.exists():
            return None
        try:
            document = json.loads(self.state_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise LinearTriggerStoreError(
                f"Could not load linear trigger state at {self.state_path}: {exc}"
            ) from exc
        self._validate_state_document(document)
        return document

    def _validate_state_document(self, document: dict[str, Any]) -> None:
        errors = validation_errors_for(self.state_validator, document)
        if errors:
            raise LinearTriggerStoreError(
                f"linear-trigger-state failed validation: {'; '.join(errors)}"
            )

    def _validate_envelope_document(self, document: dict[str, Any]) -> None:
        errors = validation_errors_for(self.envelope_validator, document)
        if errors:
            raise LinearTriggerStoreError(
                f"linear-webhook-envelope failed validation: {'; '.join(errors)}"
            )

    @staticmethod
    def _write_json_atomic(path: Path, document: dict[str, Any]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = path.with_name(f".{path.name}.{uuid4().hex}.tmp")
        try:
            tmp_path.write_text(f"{json.dumps(document, indent=2)}\n", encoding="utf-8")
            os.replace(tmp_path, path)
        finally:
            tmp_path.unlink(missing_ok=True)

    @contextmanager
    def _lease(
        self,
        lock_path: Path,
        resource_id: str,
        *,
        ttl_seconds: float = 300.0,
    ) -> Iterator[None]:
        if ttl_seconds <= 0:
            raise LinearTriggerStoreError("Linear trigger lease ttl_seconds must be > 0.")
        lock_path.parent.mkdir(parents=True, exist_ok=True)
        self._clear_expired_lease(lock_path)
        lease_document = {
            "resource_id": resource_id,
            "lease_id": uuid4().hex,
            "acquired_at": utc_now(),
            "expires_at": (
                datetime.now(timezone.utc) + timedelta(seconds=ttl_seconds)
            ).isoformat().replace("+00:00", "Z"),
        }
        try:
            fd = os.open(lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o644)
        except FileExistsError as exc:
            raise LinearTriggerLeaseBusyError(
                f"Linear trigger resource '{resource_id}' is currently locked."
            ) from exc
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as handle:
                handle.write(f"{json.dumps(lease_document, indent=2)}\n")
            yield
        finally:
            lock_path.unlink(missing_ok=True)

    @staticmethod
    def _clear_expired_lease(lock_path: Path) -> None:
        if not lock_path.exists():
            return
        try:
            document = json.loads(lock_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            lock_path.unlink(missing_ok=True)
            return
        expires_at = document.get("expires_at")
        if not isinstance(expires_at, str):
            lock_path.unlink(missing_ok=True)
            return
        if parse_utc_timestamp(expires_at) <= datetime.now(timezone.utc):
            lock_path.unlink(missing_ok=True)


class LinearWebhookReceiver:
    """Verify, filter, and persist incoming Linear issue webhook events."""

    def __init__(
        self,
        config: LinearTriggerConfig,
        store: LinearTriggerStore,
    ) -> None:
        self.config = config
        self.store = store

    def handle_request(
        self,
        *,
        path: str,
        headers: dict[str, str],
        raw_body: bytes,
        received_at: str | None = None,
        now_ms: int | None = None,
    ) -> LinearWebhookResponse:
        if path != "/hooks/linear":
            return LinearWebhookResponse(
                status_code=404,
                document={"status": "not_found"},
            )
        if self.config.webhook_secret is None:
            raise LinearConfigurationError("LINEAR_WEBHOOK_SECRET is required.")
        if not verify_linear_signature(
            raw_body,
            _header_value(headers, "Linear-Signature"),
            self.config.webhook_secret,
        ):
            return LinearWebhookResponse(
                status_code=401,
                document={"status": "rejected", "reason": "invalid_signature"},
            )
        try:
            payload = json.loads(raw_body.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            return LinearWebhookResponse(
                status_code=400,
                document={"status": "rejected", "reason": "invalid_json"},
            )
        if not isinstance(payload, dict):
            return LinearWebhookResponse(
                status_code=400,
                document={"status": "rejected", "reason": "invalid_payload_shape"},
            )
        webhook_timestamp = payload.get("webhookTimestamp")
        if not isinstance(webhook_timestamp, int):
            return LinearWebhookResponse(
                status_code=400,
                document={"status": "rejected", "reason": "invalid_webhook_timestamp"},
            )
        if not webhook_timestamp_is_fresh(
            webhook_timestamp,
            now_ms=now_ms,
            max_age_seconds=self.config.webhook_max_age_seconds,
        ):
            return LinearWebhookResponse(
                status_code=401,
                document={"status": "rejected", "reason": "stale_webhook_timestamp"},
            )
        delivery_id = _header_value(headers, "Linear-Delivery")
        if not delivery_id:
            return LinearWebhookResponse(
                status_code=400,
                document={"status": "rejected", "reason": "missing_delivery_id"},
            )
        event_type = _header_value(headers, "Linear-Event") or str(payload.get("type", ""))
        accepted = self._accepted_envelope(
            payload=payload,
            delivery_id=delivery_id,
            event_type=event_type,
            received_at=received_at or utc_now(),
        )
        if accepted is None:
            return LinearWebhookResponse(
                status_code=200,
                document={"status": "ignored", "delivery_id": delivery_id},
            )
        stored_path = self.store.save_envelope(accepted)
        return LinearWebhookResponse(
            status_code=200,
            document={
                "status": "accepted",
                "delivery_id": accepted.delivery_id,
                "logical_trigger_key": accepted.logical_trigger_key,
                "stored_path": str(stored_path),
            },
        )

    def _accepted_envelope(
        self,
        *,
        payload: dict[str, Any],
        delivery_id: str,
        event_type: str,
        received_at: str,
    ) -> LinearWebhookEnvelope | None:
        if event_type != "Issue":
            return None
        if payload.get("type") != "Issue":
            return None
        action = payload.get("action")
        if action not in {"create", "update"}:
            return None
        data = payload.get("data")
        if not isinstance(data, dict):
            return None
        team_id = data.get("teamId")
        state_id = data.get("stateId")
        issue_id = data.get("id")
        if not all(isinstance(value, str) and value for value in (team_id, state_id, issue_id)):
            return None
        if team_id != self.config.target_team_id:
            return None
        if state_id != self.config.target_state_id:
            return None
        if action == "update":
            updated_from = payload.get("updatedFrom")
            previous_state_id = (
                updated_from.get("stateId")
                if isinstance(updated_from, dict)
                else None
            )
            if previous_state_id == self.config.target_state_id:
                return None
            if previous_state_id is None:
                return None
        return LinearWebhookEnvelope.from_payload(
            delivery_id=delivery_id,
            event_type=event_type,
            received_at=received_at,
            payload=payload,
        )


class LinearGraphQLClient:
    """Fetch issue snapshots and post comments through the Linear GraphQL API."""

    def __init__(self, config: LinearTriggerConfig) -> None:
        if config.api_key is None:
            raise LinearConfigurationError("LINEAR_API_KEY is required.")
        self.config = config

    def fetch_issue_snapshot(self, issue_id: str) -> LinearIssueSnapshot:
        document = self._execute(
            """
            query FactoryLinearIssue($id: String!) {
              issue(id: $id) {
                id
                identifier
                title
                description
                url
                createdAt
                updatedAt
                priority
                team {
                  id
                  name
                }
                state {
                  id
                  name
                  type
                }
                labels {
                  nodes {
                    id
                    name
                  }
                }
                project {
                  id
                  name
                }
                creator {
                  id
                  name
                  displayName
                }
                assignee {
                  id
                  name
                  displayName
                }
                comments(last: 10) {
                  nodes {
                    id
                    body
                    createdAt
                    user {
                      id
                      name
                      displayName
                    }
                  }
                }
              }
            }
            """,
            {"id": issue_id},
        )
        issue = document.get("issue")
        if not isinstance(issue, dict):
            raise LinearGraphQLClientError(
                f"Linear issue '{issue_id}' could not be loaded."
            )
        return LinearIssueSnapshot(
            id=str(issue["id"]),
            identifier=str(issue["identifier"]),
            title=str(issue["title"]),
            description=str(issue.get("description") or ""),
            url=str(issue["url"]),
            team={
                "id": _optional_str(issue.get("team"), "id"),
                "name": _optional_str(issue.get("team"), "name"),
            },
            state={
                "id": _optional_str(issue.get("state"), "id"),
                "name": _optional_str(issue.get("state"), "name"),
                "type": _optional_str(issue.get("state"), "type"),
            },
            labels=[
                str(node["name"])
                for node in _connection_nodes(issue.get("labels"))
                if isinstance(node, dict) and isinstance(node.get("name"), str)
            ],
            priority=issue.get("priority") if isinstance(issue.get("priority"), int) else None,
            project=_optional_entity(issue.get("project")),
            creator=_optional_person(issue.get("creator")),
            assignee=_optional_person(issue.get("assignee")),
            created_at=str(issue["createdAt"]),
            updated_at=str(issue["updatedAt"]),
            comments=[
                {
                    "id": _optional_str(node, "id"),
                    "body": str(node.get("body") or ""),
                    "created_at": _optional_str(node, "createdAt"),
                    "author": _display_name(node.get("user")),
                }
                for node in _connection_nodes(issue.get("comments"))
                if isinstance(node, dict)
            ],
        )

    def find_factory_issue_by_work_item(
        self,
        *,
        team_id: str,
        work_item_id: str,
    ) -> dict[str, Any] | None:
        marker = f"Work item: `{work_item_id}`"
        matches: list[dict[str, Any]] = []
        after: str | None = None
        while True:
            document = self._execute(
                """
                query FactoryLinearIssuesForWorkItem($teamId: String!, $after: String) {
                  team(id: $teamId) {
                    id
                    issues(first: 100, after: $after, includeArchived: false) {
                      pageInfo {
                        hasNextPage
                        endCursor
                      }
                      nodes {
                        id
                        identifier
                        title
                        description
                        url
                        createdAt
                        state {
                          id
                          name
                        }
                      }
                    }
                  }
                }
                """,
                {"teamId": team_id, "after": after},
            )
            team = document.get("team")
            if not isinstance(team, dict):
                raise LinearGraphQLClientError(
                    f"Linear team '{team_id}' could not be loaded."
                )
            issues = team.get("issues")
            if not isinstance(issues, dict):
                raise LinearGraphQLClientError(
                    f"Linear team '{team_id}' did not return an issue connection."
                )
            for node in _connection_nodes(issues):
                description = str(node.get("description") or "")
                if (
                    marker in description
                    and "synchronized automatically by the AI Factory" in description
                    and isinstance(node.get("id"), str)
                ):
                    matches.append(node)

            page_info = issues.get("pageInfo")
            if not isinstance(page_info, dict) or not page_info.get("hasNextPage"):
                break
            next_cursor = page_info.get("endCursor")
            if not isinstance(next_cursor, str) or not next_cursor:
                break
            after = next_cursor

        if not matches:
            return None
        matches.sort(key=lambda issue: str(issue.get("createdAt") or ""))
        issue = matches[0]
        return {
            "id": str(issue["id"]),
            "identifier": _optional_str(issue, "identifier"),
            "title": _optional_str(issue, "title"),
            "url": _optional_str(issue, "url"),
            "state": _optional_entity(issue.get("state")),
        }

    def create_comment(self, issue_id: str, body: str) -> str | None:
        document = self._execute(
            """
            mutation FactoryLinearComment($issueId: String!, $body: String!) {
              commentCreate(input: { issueId: $issueId, body: $body }) {
                success
                comment {
                  id
                }
              }
            }
            """,
            {"issueId": issue_id, "body": body},
        )
        payload = document.get("commentCreate")
        if not isinstance(payload, dict) or not payload.get("success"):
            raise LinearGraphQLClientError("Linear commentCreate mutation failed.")
        comment = payload.get("comment")
        if isinstance(comment, dict) and isinstance(comment.get("id"), str):
            return str(comment["id"])
        return None

    def fetch_team_labels(self, team_id: str) -> list[dict[str, Any]]:
        document = self._execute(
            """
            query FactoryLinearTeamLabels($id: String!) {
              team(id: $id) {
                id
                labels(first: 250, includeArchived: false) {
                  nodes {
                    id
                    name
                  }
                }
              }
            }
            """,
            {"id": team_id},
        )
        team = document.get("team")
        if not isinstance(team, dict):
            raise LinearGraphQLClientError(
                f"Linear team '{team_id}' could not be loaded."
            )
        return [
            {"id": str(node["id"]), "name": str(node["name"])}
            for node in _connection_nodes(team.get("labels"))
            if isinstance(node.get("id"), str) and isinstance(node.get("name"), str)
        ]

    def create_issue_label(
        self,
        *,
        team_id: str,
        name: str,
        color: str,
        description: str,
    ) -> dict[str, Any]:
        document = self._execute(
            """
            mutation FactoryLinearIssueLabelCreate(
              $teamId: String!,
              $name: String!,
              $color: String!,
              $description: String!
            ) {
              issueLabelCreate(
                input: {
                  teamId: $teamId,
                  name: $name,
                  color: $color,
                  description: $description
                }
              ) {
                success
                issueLabel {
                  id
                  name
                }
              }
            }
            """,
            {
                "teamId": team_id,
                "name": name,
                "color": color,
                "description": description,
            },
        )
        payload = document.get("issueLabelCreate")
        if not isinstance(payload, dict) or not payload.get("success"):
            raise LinearGraphQLClientError("Linear issueLabelCreate mutation failed.")
        label = payload.get("issueLabel")
        if not isinstance(label, dict):
            raise LinearGraphQLClientError("Linear issueLabelCreate did not return an issueLabel.")
        return {"id": str(label["id"]), "name": str(label["name"])}

    def add_issue_label(self, issue_id: str, label_id: str) -> None:
        self._update_issue_labels(issue_id, added_label_ids=[label_id])

    def remove_issue_label(self, issue_id: str, label_id: str) -> None:
        self._update_issue_labels(issue_id, removed_label_ids=[label_id])

    def _update_issue_labels(
        self,
        issue_id: str,
        *,
        added_label_ids: list[str] | None = None,
        removed_label_ids: list[str] | None = None,
    ) -> None:
        document = self._execute(
            """
            mutation FactoryLinearIssueLabelsUpdate(
              $id: String!,
              $addedLabelIds: [String!],
              $removedLabelIds: [String!]
            ) {
              issueUpdate(
                id: $id,
                input: {
                  addedLabelIds: $addedLabelIds,
                  removedLabelIds: $removedLabelIds
                }
              ) {
                success
                issue {
                  id
                }
              }
            }
            """,
            {
                "id": issue_id,
                "addedLabelIds": added_label_ids or [],
                "removedLabelIds": removed_label_ids or [],
            },
        )
        payload = document.get("issueUpdate")
        if not isinstance(payload, dict) or not payload.get("success"):
            raise LinearGraphQLClientError("Linear issue label update failed.")

    def fetch_team_states(self, team_id: str) -> list[dict[str, Any]]:
        document = self._execute(
            """
            query FactoryLinearTeamStates($id: String!) {
              team(id: $id) {
                id
                name
                key
                states {
                  nodes {
                    id
                    name
                    type
                    position
                  }
                }
              }
            }
            """,
            {"id": team_id},
        )
        team = document.get("team")
        if not isinstance(team, dict):
            raise LinearGraphQLClientError(
                f"Linear team '{team_id}' could not be loaded."
            )
        states = team.get("states")
        nodes = _connection_nodes(states)
        return [
            {
                "id": str(node["id"]),
                "name": str(node["name"]),
                "type": _optional_str(node, "type"),
                "position": node.get("position"),
            }
            for node in nodes
            if isinstance(node.get("id"), str) and isinstance(node.get("name"), str)
        ]

    def create_workflow_state(
        self,
        *,
        team_id: str,
        name: str,
        state_type: str,
        color: str,
        description: str,
        position: float,
    ) -> dict[str, Any]:
        document = self._execute(
            """
            mutation FactoryLinearWorkflowStateCreate(
              $teamId: String!,
              $name: String!,
              $type: String!,
              $color: String!,
              $description: String!,
              $position: Float!
            ) {
              workflowStateCreate(
                input: {
                  teamId: $teamId,
                  name: $name,
                  type: $type,
                  color: $color,
                  description: $description,
                  position: $position
                }
              ) {
                success
                workflowState {
                  id
                  name
                  type
                  position
                }
              }
            }
            """,
            {
                "teamId": team_id,
                "name": name,
                "type": state_type,
                "color": color,
                "description": description,
                "position": float(position),
            },
        )
        payload = document.get("workflowStateCreate")
        if not isinstance(payload, dict) or not payload.get("success"):
            raise LinearGraphQLClientError("Linear workflowStateCreate mutation failed.")
        workflow_state = payload.get("workflowState")
        if not isinstance(workflow_state, dict):
            raise LinearGraphQLClientError("Linear workflowStateCreate did not return a workflowState.")
        return {
            "id": str(workflow_state["id"]),
            "name": str(workflow_state["name"]),
            "type": _optional_str(workflow_state, "type"),
            "position": workflow_state.get("position"),
        }

    def create_issue(
        self,
        *,
        team_id: str,
        title: str,
        description: str,
        state_id: str | None = None,
    ) -> dict[str, Any]:
        document = self._execute(
            """
            mutation FactoryLinearIssueCreate(
              $teamId: String!,
              $title: String!,
              $description: String!,
              $stateId: String
            ) {
              issueCreate(
                input: {
                  teamId: $teamId,
                  title: $title,
                  description: $description,
                  stateId: $stateId
                }
              ) {
                success
                issue {
                  id
                  identifier
                  title
                  url
                  state {
                    id
                    name
                  }
                }
              }
            }
            """,
            {
                "teamId": team_id,
                "title": title,
                "description": description,
                "stateId": state_id,
            },
        )
        payload = document.get("issueCreate")
        if not isinstance(payload, dict) or not payload.get("success"):
            raise LinearGraphQLClientError("Linear issueCreate mutation failed.")
        issue = payload.get("issue")
        if not isinstance(issue, dict):
            raise LinearGraphQLClientError("Linear issueCreate did not return an issue.")
        return {
            "id": str(issue["id"]),
            "identifier": _optional_str(issue, "identifier"),
            "title": _optional_str(issue, "title"),
            "url": _optional_str(issue, "url"),
            "state": _optional_entity(issue.get("state")),
        }

    def update_issue_state(self, issue_id: str, state_id: str) -> dict[str, Any]:
        document = self._execute(
            """
            mutation FactoryLinearIssueUpdate($id: String!, $stateId: String!) {
              issueUpdate(id: $id, input: { stateId: $stateId }) {
                success
                issue {
                  id
                  identifier
                  title
                  url
                  state {
                    id
                    name
                  }
                }
              }
            }
            """,
            {
                "id": issue_id,
                "stateId": state_id,
            },
        )
        payload = document.get("issueUpdate")
        if not isinstance(payload, dict) or not payload.get("success"):
            raise LinearGraphQLClientError("Linear issueUpdate mutation failed.")
        issue = payload.get("issue")
        if not isinstance(issue, dict):
            raise LinearGraphQLClientError("Linear issueUpdate did not return an issue.")
        return {
            "id": str(issue["id"]),
            "identifier": _optional_str(issue, "identifier"),
            "title": _optional_str(issue, "title"),
            "url": _optional_str(issue, "url"),
            "state": _optional_entity(issue.get("state")),
        }

    def _execute(self, query: str, variables: dict[str, Any]) -> dict[str, Any]:
        request_payload = json.dumps({"query": query, "variables": variables}).encode("utf-8")
        request = urllib_request.Request(
            self.config.graphql_url,
            data=request_payload,
            headers={
                "Authorization": str(self.config.api_key),
                "Content-Type": "application/json",
                "User-Agent": "auto-mindsdb-factory/linear-trigger",
            },
        )
        try:
            with urllib_request.urlopen(request, timeout=30) as response:
                raw = response.read()
        except urllib_error.HTTPError as exc:
            body = exc.read().decode("utf-8", errors="replace")
            raise LinearGraphQLClientError(
                f"Linear GraphQL request failed with HTTP {exc.code}: {body}"
            ) from exc
        except OSError as exc:
            raise LinearGraphQLClientError(
                f"Linear GraphQL request failed: {exc}"
            ) from exc
        try:
            payload = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise LinearGraphQLClientError(
                "Linear GraphQL response was not valid JSON."
            ) from exc
        errors = payload.get("errors")
        if isinstance(errors, list) and errors:
            messages = ", ".join(
                str(error.get("message", "unknown GraphQL error"))
                for error in errors
                if isinstance(error, dict)
            )
            raise LinearGraphQLClientError(f"Linear GraphQL errors: {messages}")
        data = payload.get("data")
        if not isinstance(data, dict):
            raise LinearGraphQLClientError("Linear GraphQL response missing data.")
        return data


class LinearTriggerWorker:
    """Convert persisted Linear trigger envelopes into Stage 1 manual intake runs."""

    def __init__(
        self,
        store_dir: Path,
        *,
        repo_root_override: Path | None = None,
        config: LinearTriggerConfig | None = None,
        linear_client: LinearGraphQLClient | None = None,
        coordinator: FactoryAutomationCoordinator | None = None,
    ) -> None:
        self.repo_root = repo_root(repo_root_override)
        self.config = config or LinearTriggerConfig.from_env(require_api_key=True)
        self.trigger_store = LinearTriggerStore(store_dir, repo_root_override=self.repo_root)
        self.linear_client = linear_client or LinearGraphQLClient(self.config)
        self.coordinator = coordinator or FactoryAutomationCoordinator(
            store_dir,
            repo_root_override=self.repo_root,
        )

    def run_cycle(
        self,
        *,
        repository: str,
        max_events: int | None = None,
    ) -> LinearTriggerCycleResult:
        from .linear_workflow import LinearWorkflowError

        if max_events is not None and max_events < 1:
            raise LinearTriggerError("max_events must be >= 1 when provided.")
        processed_events: list[dict[str, Any]] = []
        skipped_events: list[dict[str, str]] = []
        failed_events: list[dict[str, str]] = []

        for path in self.trigger_store.iter_envelope_paths():
            if max_events is not None and len(processed_events) + len(failed_events) >= max_events:
                break
            delivery_id = path.stem
            try:
                with self.trigger_store.event_lease(delivery_id):
                    envelope = self.trigger_store.load_envelope(path)
                    with self.trigger_store.state_transaction() as trigger_state:
                        if delivery_id in trigger_state.processed_delivery_ids:
                            self.trigger_store.remove_envelope(path)
                            skipped_events.append(
                                {
                                    "delivery_id": delivery_id,
                                    "reason": "delivery_already_processed",
                                }
                            )
                            continue
                        if envelope.logical_trigger_key in trigger_state.processed_logical_trigger_keys:
                            self.trigger_store.mark_delivery_processed(trigger_state, delivery_id)
                            self.trigger_store.remove_envelope(path)
                            skipped_events.append(
                                {
                                    "delivery_id": delivery_id,
                                    "reason": "logical_trigger_already_processed",
                                }
                            )
                            continue

                    snapshot = self.linear_client.fetch_issue_snapshot(envelope.issue_id)
                    stage1_result = self._create_stage1_result(snapshot, envelope)
                    stage1_document = stage1_result.to_document()
                    stored_path, _ = self.coordinator.register_bundle("stage1", stage1_document)
                    with self.trigger_store.state_transaction() as trigger_state:
                        self.trigger_store.mark_processed(trigger_state, envelope)

                    if self.coordinator.linear_workflow_sync is None:
                        comment_result = self._maybe_comment(snapshot, stage1_result, stored_path)
                    else:
                        comment_result = {
                            "status": "skipped",
                            "reason": "managed_by_linear_workflow_sync",
                        }
                    handoff_result = None
                    if stage1_result.policy_decision["decision"] == "active_build_candidate":
                        handoff_result = self.coordinator.run_immediate_handoff(
                            stage1_result.work_item.work_item_id,
                            raise_on_failure=False,
                            repository=repository,
                        ).to_document()
                    self.trigger_store.remove_envelope(path)
                    processed_events.append(
                        {
                            "delivery_id": envelope.delivery_id,
                            "logical_trigger_key": envelope.logical_trigger_key,
                            "issue_id": snapshot.id,
                            "issue_identifier": snapshot.identifier,
                            "work_item_id": stage1_result.work_item.work_item_id,
                            "decision": stage1_result.spec_packet["relevance"]["decision"],
                            "stored_path": str(stored_path),
                            "comment": comment_result,
                            "handoff": handoff_result,
                        }
                    )
            except LinearTriggerLeaseBusyError:
                skipped_events.append(
                    {
                        "delivery_id": delivery_id,
                        "reason": "delivery_locked",
                    }
                )
            except (LinearTriggerError, AutomationError, LinearWorkflowError) as exc:
                failed_events.append(
                    {
                        "delivery_id": delivery_id,
                        "reason": str(exc),
                    }
                )

        return LinearTriggerCycleResult(
            processed_events=processed_events,
            skipped_events=skipped_events,
            failed_events=failed_events,
            trigger_state=self.trigger_store.load_state(),
        )

    def _create_stage1_result(
        self,
        snapshot: LinearIssueSnapshot,
        envelope: LinearWebhookEnvelope,
    ):
        item = build_manual_intake_item(
            title=snapshot.title,
            body=render_linear_manual_intake_body(snapshot, envelope),
            url=snapshot.url,
            provider="linear",
            external_id=envelope.logical_trigger_key,
            detected_at=envelope.received_at,
            published_at=str(envelope.payload["createdAt"]),
        )
        try:
            return self.coordinator.stage1_pipeline.process_item(item)
        except Exception as exc:
            raise LinearTriggerError(
                f"Linear Stage 1 intake failed for issue '{snapshot.identifier}': {exc}"
            ) from exc

    def _maybe_comment(
        self,
        snapshot: LinearIssueSnapshot,
        stage1_result,
        stored_path: Path,
    ) -> dict[str, Any]:
        decision = stage1_result.spec_packet["relevance"]["decision"]
        should_comment = (
            self.config.comment_on_accept
            if decision == "active_build_candidate"
            else self.config.comment_on_reject
        )
        if not should_comment:
            return {"status": "skipped", "reason": "comment_disabled"}
        try:
            comment_id = self.linear_client.create_comment(
                snapshot.id,
                render_linear_comment_body(
                    config=self.config,
                    stage1_result=stage1_result,
                ),
            )
        except LinearGraphQLClientError as exc:
            return {"status": "failed", "reason": str(exc)}
        return {"status": "posted", "comment_id": comment_id}


def _optional_entity(entity: Any) -> dict[str, str | None] | None:
    if not isinstance(entity, dict):
        return None
    return {
        "id": _optional_str(entity, "id"),
        "name": _optional_str(entity, "name"),
    }


def _optional_person(entity: Any) -> dict[str, str | None] | None:
    if not isinstance(entity, dict):
        return None
    return {
        "id": _optional_str(entity, "id"),
        "name": _display_name(entity),
    }


def _optional_str(entity: Any, key: str) -> str | None:
    if not isinstance(entity, dict):
        return None
    value = entity.get(key)
    return value if isinstance(value, str) and value else None


def _display_name(entity: Any) -> str | None:
    if not isinstance(entity, dict):
        return None
    for key in ("displayName", "name"):
        value = entity.get(key)
        if isinstance(value, str) and value:
            return value
    return None


def _connection_nodes(connection: Any) -> list[dict[str, Any]]:
    if not isinstance(connection, dict):
        return []
    nodes = connection.get("nodes")
    if not isinstance(nodes, list):
        return []
    return [node for node in nodes if isinstance(node, dict)]


def _extract_acceptance_hints(description: str) -> list[str]:
    hints: list[str] = []
    in_acceptance_section = False
    for raw_line in description.splitlines():
        line = raw_line.strip()
        if not line:
            if in_acceptance_section:
                in_acceptance_section = False
            continue
        lowered = line.lower().rstrip(":")
        if "acceptance criteria" in lowered or "definition of done" in lowered:
            in_acceptance_section = True
            continue
        if match := CHECKLIST_ITEM_PATTERN.match(line):
            hints.append(normalize_whitespace(match.group(1)))
            continue
        if in_acceptance_section and (match := PLAIN_BULLET_PATTERN.match(line)):
            hints.append(normalize_whitespace(match.group(1)))
    deduped: list[str] = []
    for hint in hints:
        if hint not in deduped:
            deduped.append(hint)
    return deduped[:8]


def render_linear_manual_intake_body(
    snapshot: LinearIssueSnapshot,
    envelope: LinearWebhookEnvelope,
) -> str:
    acceptance_hints = _extract_acceptance_hints(snapshot.description)
    label_summary = ", ".join(snapshot.labels) if snapshot.labels else "none"
    project_name = snapshot.project["name"] if snapshot.project and snapshot.project["name"] else "none"
    creator_name = snapshot.creator["name"] if snapshot.creator and snapshot.creator["name"] else "unknown"
    assignee_name = snapshot.assignee["name"] if snapshot.assignee and snapshot.assignee["name"] else "unassigned"
    state_name = snapshot.state["name"] or envelope.state_id
    recent_discussion = (
        " | ".join(
            (
                f"{comment['created_at'] or 'unknown time'} by "
                f"{comment['author'] or 'unknown author'}: "
                f"{normalize_whitespace(comment['body'] or '')}"
            )
            for comment in snapshot.comments
            if comment.get("body")
        )
        or "No recent comments."
    )
    description = normalize_whitespace(snapshot.description) or "No description provided."
    acceptance_summary = (
        "; ".join(acceptance_hints)
        if acceptance_hints
        else "No explicit checklist or acceptance section was found in the issue body."
    )
    return (
        f"Issue summary: {description} "
        f"Acceptance hints: {acceptance_summary} "
        f"Issue metadata: identifier={snapshot.identifier}; team={snapshot.team['name'] or envelope.team_id}; "
        f"state={state_name}; priority={snapshot.priority if snapshot.priority is not None else 'unset'}; "
        f"project={project_name}; labels={label_summary}; creator={creator_name}; assignee={assignee_name}; "
        f"created_at={snapshot.created_at}; updated_at={snapshot.updated_at}. "
        f"Recent discussion: {recent_discussion} "
        f"Source metadata: trigger_action={envelope.payload['action']}; "
        f"trigger_created_at={envelope.payload['createdAt']}; "
        f"logical_trigger_key={envelope.logical_trigger_key}; linear_url={snapshot.url}."
    )


def render_linear_comment_body(
    *,
    config: LinearTriggerConfig,
    stage1_result,
) -> str:
    decision = stage1_result.spec_packet["relevance"]["decision"]
    work_item_id = stage1_result.work_item.work_item_id
    rationale = stage1_result.spec_packet["relevance"]["rationale"]
    lines = [
        f"AI Factory intake result: `{decision}`",
        "",
        f"- Work item: `{work_item_id}`",
        f"- Source external id: `{stage1_result.source_item.external_id}`",
        f"- Rationale: {rationale}",
    ]
    lane = stage1_result.policy_decision.get("lane_assignment", {}).get("lane")
    if isinstance(lane, str):
        lines.append(f"- Execution lane: `{lane}`")
    if config.trigger_base_url:
        lines.append(
            f"- Cockpit: {config.trigger_base_url.rstrip('/')} (filter for `{work_item_id}`)"
        )
    else:
        lines.append("- Cockpit: not configured for external linking on this factory instance")
    return "\n".join(lines)


class LinearWebhookHTTPServer(ThreadingHTTPServer):
    def __init__(
        self,
        server_address: tuple[str, int],
        receiver: LinearWebhookReceiver,
    ) -> None:
        self.receiver = receiver
        super().__init__(server_address, LinearWebhookHandler)


class LinearWebhookHandler(BaseHTTPRequestHandler):
    server: LinearWebhookHTTPServer

    def do_POST(self) -> None:  # noqa: N802
        content_length = int(self.headers.get("Content-Length", "0") or "0")
        raw_body = self.rfile.read(content_length)
        headers = {key: value for key, value in self.headers.items()}
        try:
            response = self.server.receiver.handle_request(
                path=self.path,
                headers=headers,
                raw_body=raw_body,
            )
        except LinearTriggerError as exc:
            response = LinearWebhookResponse(
                status_code=500,
                document={"status": "error", "reason": str(exc)},
            )
        payload = json.dumps(response.document).encode("utf-8")
        self.send_response(response.status_code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def do_GET(self) -> None:  # noqa: N802
        self.send_response(405)
        self.end_headers()

    def log_message(self, format: str, *args: Any) -> None:
        return None


def serve_linear_webhooks(
    *,
    store_dir: Path,
    host: str,
    port: int,
    repo_root_override: Path | None = None,
    config: LinearTriggerConfig | None = None,
) -> None:
    runtime_config = config or LinearTriggerConfig.from_env(require_webhook_secret=True)
    receiver = LinearWebhookReceiver(
        runtime_config,
        LinearTriggerStore(store_dir, repo_root_override=repo_root_override),
    )
    server = LinearWebhookHTTPServer((host, port), receiver)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        return
    finally:
        server.server_close()
