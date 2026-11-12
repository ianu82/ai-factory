"""Shared connector storage and intake primitives.

No helper commits. Callers commit only after *all* delivery effects succeed. Network
calls must finish before entering intake: the connector row is locked until commit.
"""

from __future__ import annotations

import hashlib
from typing import Any
from urllib.parse import urlsplit

from sqlalchemy import update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from .. import signals
from ..db import utcnow
from ..models import Connector, ConnectorReceipt, Repository, Secret, Signal, Workspace
from ..security import SecretError, decrypt, encrypt, redact

CONNECTOR_KINDS = frozenset({"slack", "sentry", "incident"})
CREDENTIAL_KINDS = {
    "slack": frozenset({"slack_bot_token", "slack_signing_secret"}),
    "sentry": frozenset({"sentry_token", "sentry_webhook_secret"}),
    "incident": frozenset({"incident_webhook_secret"}),
}
STATUS_DETAILS = {
    "untested": "Test the connection before enabling intake.",
    "ready": "Connection ready.",
    "disabled": "Enable the connector to receive events.",
    "invalid_scope": "Choose a connector and repository in this workspace.",
    "invalid_input": "Supply a valid, bounded connector identifier and normalized fields.",
    "missing_token": "Save a workspace credential in Connections and test again.",
    "unreadable_secret": "Re-save the workspace credential with the current server key.",
    "oauth_disabled": "Enable experimental MindsHub connections for this workspace.",
    "missing_key": "Save this workspace's MindsHub API key or use a workspace token.",
    "missing_connection": "Connect this service in MindsHub or save a workspace token.",
    "oauth_unauthorized": "Reconnect MindsHub or replace this workspace's MindsHub API key.",
    "oauth_timeout": "MindsHub timed out. Retry or save a workspace token.",
    "oauth_network": "MindsHub is unreachable. Retry or save a workspace token.",
    "oauth_response": "MindsHub returned an invalid token response. Reconnect or use a workspace token.",
    "oauth_http": "MindsHub could not provide a token. Reconnect or use a workspace token.",
}


class ConnectorError(ValueError):
    """Only allowlisted messages may reach connector status, logs or API responses."""

    def __init__(self, code: str):
        self.code = code if code in STATUS_DETAILS else "invalid_input"
        super().__init__(STATUS_DETAILS[self.code])


def set_status(connector: Connector, code: str, *, tested: bool = False) -> None:
    connector.status = code if code in STATUS_DETAILS else "invalid_input"
    connector.status_detail = STATUS_DETAILS[connector.status]
    if tested:
        connector.tested_at = utcnow()


def connector_view(connector: Connector) -> dict[str, Any]:
    """Public foundation state: never echo free-form detail, config or runtime state."""
    status = connector.status if connector.status in STATUS_DETAILS else "invalid_input"
    return {"id": connector.id, "workspace_id": connector.workspace_id, "kind": connector.kind,
            "enabled": connector.enabled, "provider": connector.provider, "status": status,
            "status_detail": STATUS_DETAILS[status], "tested_at": connector.tested_at,
            "last_received_at": connector.last_received_at}


def validate_scope(db: Session, workspace_id: str, connector: Connector,
                   repo_id: str | None = None) -> Workspace:
    workspace = db.get(Workspace, workspace_id)
    if (workspace is None or connector.workspace_id != workspace_id or connector.kind not in CONNECTOR_KINDS
            or connector.provider not in ("workspace", "mindshub")):
        raise ConnectorError("invalid_scope")
    stored = db.get(Connector, connector.id) if connector.id else None
    if stored is None or stored.workspace_id != workspace_id:
        raise ConnectorError("invalid_scope")
    if repo_id is not None:
        repo = db.get(Repository, repo_id)
        if repo is None or repo.workspace_id != workspace_id:
            raise ConnectorError("invalid_scope")
    return workspace


def store_credential(db: Session, connector: Connector, kind: str, value: str) -> None:
    validate_scope(db, connector.workspace_id, connector)
    # Match the credential API's minimum after stripping. Very short values would
    # cause exact-value intake redaction to corrupt unrelated source text.
    if (kind not in CREDENTIAL_KINDS[connector.kind] or not isinstance(value, str)
            or len(value.strip()) < 8 or len(value) > 16384):
        raise ConnectorError("invalid_input")
    row = db.query(Secret).filter_by(workspace_id=connector.workspace_id, kind=kind).one_or_none()
    if row is None:
        row = Secret(workspace_id=connector.workspace_id, kind=kind)
        db.add(row)
    row.ciphertext = encrypt(value.strip())
    # Even a partial hint is inappropriate for arbitrary signing secrets.
    row.hint, row.status, row.status_detail = "", "untested", ""


def workspace_secret(db: Session, workspace_id: str, kind: str) -> str | None:
    row = db.query(Secret).filter_by(workspace_id=workspace_id, kind=kind).one_or_none()
    if row is None:
        return None
    try:
        return decrypt(row.ciphertext).strip() or None
    except SecretError:
        raise ConnectorError("unreadable_secret") from None


def _identifier(value: str) -> str:
    if not isinstance(value, str) or not value.strip() or len(value) > 2048:
        raise ConnectorError("invalid_input")
    return hashlib.sha256(value.encode()).hexdigest()


def claim_delivery(db: Session, workspace_id: str, connector: Connector, delivery_key: str) -> bool:
    """Serialize deliveries per connector, then claim within the caller's transaction.

    The UPDATE takes a row lock on Postgres and starts a real outer write transaction
    on SQLite (a SAVEPOINT alone can otherwise commit independently of session rollback).
    A conflict rolls back only the savepoint, not unrelated caller work.
    """
    key = _identifier(delivery_key)
    result = db.execute(update(Connector).where(
        Connector.id == connector.id, Connector.workspace_id == workspace_id,
        Connector.enabled.is_(True)).values(last_received_at=Connector.last_received_at))
    if result.rowcount != 1:
        raise ConnectorError("disabled")
    validate_scope(db, workspace_id, connector)
    try:
        with db.begin_nested():
            db.add(ConnectorReceipt(connector_id=connector.id, delivery_key=key))
            db.flush()
    except IntegrityError:
        if db.query(ConnectorReceipt).filter_by(connector_id=connector.id, delivery_key=key).first() is None:
            raise
        return False
    return True


# Only normalized source metadata is admitted; never accept an arbitrary payload dict.
_METADATA_FIELDS = frozenset({"channel_id", "thread_ts", "message_ts", "project_id", "issue_id",
                              "service", "incident_id", "culprit", "stack_summary", "first_seen",
                              "last_seen", "events", "users_affected", "release"})


def signal_priority(signal: Signal) -> int:
    value = (signal.data or {}).get("priority")
    return value if type(value) is int and 0 <= value <= 3 else 2


def ingest_signal(db: Session, workspace_id: str, connector: Connector, *, delivery_key: str,
                  external_id: str, title: str, body: str = "", url: str = "", repo_id: str | None = None,
                  metadata: dict[str, Any] | None = None, priority: int | None = None) -> tuple[Signal | None, bool]:
    """Deduplicate delivery, then upsert a connector/external-issue namespaced signal.

    Returns (None, False) on an already committed delivery; new deliveries for an
    existing issue refresh its fields. Failure rolls the receipt back too.
    """
    validate_scope(db, workspace_id, connector, repo_id)
    issue_key = _identifier(external_id)
    if (not isinstance(title, str) or not title.strip() or len(title) > 3000
            or not isinstance(body, str) or len(body) > 32000
            or not isinstance(url, str) or len(url) > 400
            or (priority is not None and (type(priority) is not int or not 0 <= priority <= 3))):
        raise ConnectorError("invalid_input")
    # Exact-value scrubbing, not just pattern redaction, protects arbitrary signing
    # secrets in free text and source metadata. Fail closed on undecryptable secrets.
    secret_values = tuple(workspace_secret(db, workspace_id, row.kind) or "" for row in
                          db.query(Secret).filter(Secret.workspace_id == workspace_id,
                                                  Secret.kind != "feedback_key").all())

    def clean(value: str, limit: int) -> str:
        return redact(value, extra=secret_values)[:limit]

    try:
        parsed = urlsplit(url)
        safe_url = url if (parsed.scheme in ("https", "http") and parsed.netloc
                           and parsed.username is None and parsed.password is None) else ""
    except ValueError:
        safe_url = ""
    data: dict[str, Any] = {"connector_id": connector.id, "origin": connector.kind}
    if priority is not None:
        data["priority"] = priority
    if metadata is not None:
        if not isinstance(metadata, dict) or len(metadata) > 32:
            raise ConnectorError("invalid_input")
        for key, value in metadata.items():
            if key in _METADATA_FIELDS:
                if isinstance(value, str):
                    if len(value) > 8000:
                        raise ConnectorError("invalid_input")
                    data[key] = clean(value, 2000)
                elif type(value) is int and 0 <= value <= 2**63 - 1:
                    data[key] = value

    # Start the outer transaction before creating a savepoint (SQLite legacy mode).
    db.execute(update(Connector).where(Connector.id == connector.id,
                                      Connector.workspace_id == workspace_id).values(
                                          last_received_at=Connector.last_received_at))
    with db.begin_nested():
        if not claim_delivery(db, workspace_id, connector, delivery_key):
            return None, False
        # The connector lock serializes distinct deliveries for the same issue.
        # The Signal unique constraint is a final guard against duplicate inserts.
        namespaced_id = signals.fingerprint(connector.id, issue_key)
        row, created = signals.upsert_signal(
            db, workspace_id, source=connector.kind, external_id=namespaced_id,
            title=clean(title, 300), body=clean(body, 8000), url=clean(safe_url, 400), repo_id=repo_id)
        # Connector deliveries are source snapshots, including an explicitly empty
        # body; upsert_signal's keep-old-body behavior is for other intake sources.
        row.title, row.body, row.url, row.repo_id = clean(title, 300), clean(body, 8000), clean(safe_url, 400), repo_id
        row.data = {**(row.data or {}), **data}
        connector.last_received_at = utcnow()
        db.flush()
        return row, created
