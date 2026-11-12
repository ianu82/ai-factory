from concurrent.futures import ThreadPoolExecutor

import pytest
from sqlalchemy import inspect
from sqlalchemy.exc import IntegrityError

from aifactory.connectors.core import (
    ConnectorError, claim_delivery, connector_view, ingest_signal, set_status, store_credential, validate_scope,
)
from aifactory.db import engine, init_engine, session_scope
from aifactory.models import Connector, ConnectorReceipt, Event, Organization, Repository, Secret, Signal, Workspace
from aifactory.security import decrypt, redact


def setup_connector(db, *, kind="slack", opted_in=False):
    org = Organization(name="test")
    db.add(org)
    db.flush()
    workspace = Workspace(org_id=org.id, name="test", slug="test",
                          settings={"connectors_mindshub_enabled": opted_in})
    db.add(workspace)
    db.flush()
    connector = Connector(workspace_id=workspace.id, kind=kind, enabled=True)
    repo = Repository(workspace_id=workspace.id, full_name="org/repo")
    db.add_all([connector, repo])
    db.flush()
    return workspace, connector, repo


def test_create_all_existing_database():
    with session_scope() as db:
        workspace, connector, _ = setup_connector(db)
        workspace_id = workspace.id
    # Simulate an existing pre-connector schema, retaining workspace data.
    ConnectorReceipt.__table__.drop(engine())
    Connector.__table__.drop(engine())
    init_engine()
    assert {"connectors", "connector_receipts"} <= set(inspect(engine()).get_table_names())
    with session_scope() as db:
        assert db.get(Workspace, workspace_id).name == "test"
        for kind in ("slack", "sentry", "incident"):
            db.add(Connector(workspace_id=workspace_id, kind=kind))
        db.flush()
        with pytest.raises(IntegrityError), db.begin_nested():
            db.add(Connector(workspace_id=workspace_id, kind="slack"))
            db.flush()
    init_engine()  # idempotent startup


def test_secrets_public_state_and_intake_are_safe():
    secret = "arbitrary-signing-secret-no-recognizable-prefix"
    with session_scope() as db:
        ws, connector, repo = setup_connector(db)
        store_credential(db, connector, "slack_signing_secret", secret)
        db.flush()
        row = db.query(Secret).one()
        assert secret not in row.ciphertext and not row.hint
        assert decrypt(row.ciphertext) == secret
        connector.status_detail = secret
        connector.config = {"token": secret}
        connector.runtime_state = {"secret": secret}
        assert secret not in repr(connector_view(connector))
        set_status(connector, secret, tested=True)
        assert secret not in connector.status_detail
        sig, created = ingest_signal(db, ws.id, connector, delivery_key=secret, external_id=secret,
                                     title=secret, body=f"text {secret}", url=f"https://example.com/{secret}",
                                     repo_id=repo.id, metadata={"culprit": secret, "accessToken": secret,
                                                               "payload": {"secret": secret}})
        assert created
        assert secret not in repr((sig.title, sig.body, sig.url, sig.external_id, sig.data))
        assert "accessToken" not in sig.data and "payload" not in sig.data
        assert db.query(Event).count() == 0
        assert secret not in db.query(ConnectorReceipt).one().delivery_key
        with pytest.raises(ConnectorError):
            store_credential(db, connector, "github_token", secret)


@pytest.mark.parametrize("kind,credential_kind", [
    ("slack", "slack_bot_token"), ("slack", "slack_signing_secret"),
    ("sentry", "sentry_token"), ("sentry", "sentry_webhook_secret"),
    ("incident", "incident_webhook_secret"),
])
@pytest.mark.parametrize("value", ["", " ", "a", "1234567", "   1234567   "])
def test_short_credentials_do_not_create_or_overwrite_secrets(kind, credential_kind, value):
    with session_scope() as db:
        ws, connector, _ = setup_connector(db, kind=kind)
        with pytest.raises(ConnectorError, match="normalized fields"):
            store_credential(db, connector, credential_kind, value)
        assert db.query(Secret).count() == 0

        # Exactly eight non-padding characters is the API's accepted boundary.
        store_credential(db, connector, credential_kind, "  12345678  ")
        db.flush()
        row = db.query(Secret).one()
        ciphertext = row.ciphertext
        assert decrypt(ciphertext) == "12345678"
        with pytest.raises(ConnectorError, match="normalized fields"):
            store_credential(db, connector, credential_kind, value)
        db.flush()
        assert db.query(Secret).one().ciphertext == ciphertext
        sig, created = ingest_signal(db, ws.id, connector, delivery_key="delivery", external_id="issue",
                                     title="A database alarm", body="An ordinary database incident",
                                     url="https://example.com/alarm", metadata={"service": "database"})
        assert created
        assert (sig.title, sig.body, sig.url, sig.data["service"]) == (
            "A database alarm", "An ordinary database incident", "https://example.com/alarm", "database")


def test_delivery_retries_refresh_fields_and_isolate_workspaces():
    with session_scope() as db:
        ws, connector, repo = setup_connector(db)
        other, other_connector, other_repo = setup_connector(db)
        sig, new = ingest_signal(db, ws.id, connector, delivery_key="d1", external_id="same",
                                 title="before", repo_id=repo.id)
        assert new
        assert ingest_signal(db, ws.id, connector, delivery_key="d1", external_id="same", title="retry") == (None, False)
        updated, new = ingest_signal(db, ws.id, connector, delivery_key="d2", external_id="same",
                                     title="after", body="new body", url="https://example.com/updated",
                                     repo_id=None, priority=0)
        assert not new and updated.id == sig.id and updated.count == 2
        assert (updated.title, updated.body, updated.url, updated.repo_id) == (
            "after", "new body", "https://example.com/updated", None)
        isolated, new = ingest_signal(db, other.id, other_connector, delivery_key="d1", external_id="same",
                                      title="other", repo_id=other_repo.id)
        assert new and isolated.id != sig.id
        with pytest.raises(ConnectorError):
            validate_scope(db, ws.id, connector, other_repo.id)
        with pytest.raises(ConnectorError):
            ingest_signal(db, other.id, connector, delivery_key="d3", external_id="same", title="wrong")
        assert db.query(ConnectorReceipt).count() == 3


def test_receipt_rolls_back_on_ingest_failure(monkeypatch):
    from aifactory import signals

    with session_scope() as db:
        ws, connector, _ = setup_connector(db)
        ids = ws.id, connector.id
    original = signals.upsert_signal

    def fail(*args, **kwargs):
        original(*args, **kwargs)
        raise RuntimeError("ingestion failed")

    with session_scope() as db:
        monkeypatch.setattr(signals, "upsert_signal", fail)
        with pytest.raises(RuntimeError):
            ingest_signal(db, ids[0], db.get(Connector, ids[1]), delivery_key="retry",
                          external_id="issue", title="test")
        assert db.query(ConnectorReceipt).count() == db.query(Signal).count() == 0
        monkeypatch.setattr(signals, "upsert_signal", original)
        assert ingest_signal(db, ids[0], db.get(Connector, ids[1]), delivery_key="retry",
                             external_id="issue", title="test")[1]
    # Outer rollback must also undo a successful nested receipt/signal insertion.
    with pytest.raises(RuntimeError), session_scope() as db:
        ingest_signal(db, ids[0], db.get(Connector, ids[1]), delivery_key="rollback",
                      external_id="other", title="test")
        raise RuntimeError()
    with session_scope() as db:
        assert db.query(ConnectorReceipt).count() == db.query(Signal).count() == 1


@pytest.mark.parametrize("distinct_deliveries", [False, True])
def test_concurrent_intake(distinct_deliveries):
    with session_scope() as db:
        ws, connector, _ = setup_connector(db)
        ids = ws.id, connector.id

    def receive(index):
        with session_scope() as db:
            return ingest_signal(db, ids[0], db.get(Connector, ids[1]),
                                 delivery_key=str(index) if distinct_deliveries else "same",
                                 external_id="same", title="issue")[1]

    with ThreadPoolExecutor(max_workers=4) as pool:
        results = list(pool.map(receive, range(8)))
    assert sum(results) == 1
    with session_scope() as db:
        assert db.query(Signal).count() == 1
        assert db.query(Signal).one().count == (8 if distinct_deliveries else 1)
        assert db.query(ConnectorReceipt).count() == (8 if distinct_deliveries else 1)


def test_claim_delivery_rolls_back():
    with session_scope() as db:
        ws, connector, _ = setup_connector(db)
        ids = ws.id, connector.id
    with pytest.raises(RuntimeError), session_scope() as db:
        assert claim_delivery(db, ids[0], db.get(Connector, ids[1]), "delivery")
        raise RuntimeError()
    with session_scope() as db:
        assert claim_delivery(db, ids[0], db.get(Connector, ids[1]), "delivery")
        assert not claim_delivery(db, ids[0], db.get(Connector, ids[1]), "delivery")


@pytest.mark.parametrize("token", ["xoxb-1234567890-abcdefghijkl", "xoxp-1234567890-abcdefghijkl",
                                   "xapp-1-ABCDEFGHIJ-1234567890", "xoxe.xoxb-1-abcdefghijklmno",
                                   "xoxe-1-abcdefghijklmno"])
def test_slack_redaction(token):
    assert token not in redact(f"token: {token}")


@pytest.mark.parametrize("priority,expected", [(0, 0), (None, 2)])
@pytest.mark.parametrize("body", ["updated", ""])
def test_acceptance_refreshes_mapping_and_priority(client, signed_in, priority, expected, body):
    workspace_id = signed_in["workspaces"][0]["id"]
    with session_scope() as db:
        connector = Connector(workspace_id=workspace_id, kind="incident", enabled=True)
        old_repo = Repository(workspace_id=workspace_id, full_name="org/old")
        new_repo = Repository(workspace_id=workspace_id, full_name="org/new")
        db.add_all([connector, old_repo, new_repo])
        db.flush()
        sig, _ = ingest_signal(db, workspace_id, connector, delivery_key="first", external_id="incident",
                               title="before", body="obsolete description", repo_id=old_repo.id)
        refreshed, created = ingest_signal(db, workspace_id, connector, delivery_key="second", external_id="incident",
                                          title="after", url="https://example.com/new", body=body, repo_id=new_repo.id,
                                          priority=priority)
        assert not created and refreshed.id == sig.id
        # Check durable state in a new session below, not just the identity map.
        signal_id, repo_id = sig.id, new_repo.id
    with session_scope() as db:
        refreshed = db.get(Signal, signal_id)
        assert refreshed.body == body
        assert refreshed.count == 2
        assert db.query(ConnectorReceipt).filter_by(connector_id=refreshed.data["connector_id"]).count() == 2
    response = client.post(f"/api/workspaces/{workspace_id}/signals/{signal_id}", json={"action": "accept"})
    assert response.status_code == 200, response.text
    from aifactory.models import Outcome
    with session_scope() as db:
        outcome = db.get(Outcome, db.get(Signal, signal_id).outcome_id)
        assert outcome.priority == expected
        assert outcome.repo_ids == [repo_id]
        assert outcome.title == "after" and "https://example.com/new" in outcome.brief
        assert "obsolete description" not in outcome.brief
        if body:
            assert body in outcome.brief


def test_disabled_intake_and_feedback_hash():
    with session_scope() as db:
        ws, connector, _ = setup_connector(db)
        # feedback_key is deliberately a one-way hash, not decryptable ciphertext.
        db.add(Secret(workspace_id=ws.id, kind="feedback_key", ciphertext="hash-only"))
        connector.enabled = False
        db.flush()
        with pytest.raises(ConnectorError, match="Enable the connector"):
            ingest_signal(db, ws.id, connector, delivery_key="delivery", external_id="issue", title="test")
        assert db.query(ConnectorReceipt).count() == 0
        connector.enabled = True
        db.flush()
        assert ingest_signal(db, ws.id, connector, delivery_key="delivery", external_id="issue", title="test")[1]


@pytest.mark.parametrize("priority", [4, -1, True, 1.5, "0"])
def test_invalid_priority_rejected_before_claim(priority):
    with session_scope() as db:
        ws, connector, _ = setup_connector(db)
        with pytest.raises(ConnectorError, match="normalized fields"):
            ingest_signal(db, ws.id, connector, delivery_key="delivery", external_id="issue",
                          title="incident", priority=priority)
        assert db.query(ConnectorReceipt).count() == db.query(Signal).count() == 0


@pytest.mark.parametrize("priority,expected", [(0, 0), (1, 1), (2, 2), (3, 3), (4, 2),
                                              (-1, 2), (True, 2), (None, 2)])
def test_signal_priority_bounds(priority, expected):
    from aifactory.connectors.core import signal_priority

    assert signal_priority(Signal(data={"priority": priority})) == expected


@pytest.mark.parametrize("url", ["https://:password@example.com/path",
                                  "https://user:password@example.com/path",
                                  "https://user@example.com/path", "https://@example.com/path"])
def test_url_userinfo_is_not_persisted_or_accepted(client, signed_in, url):
    workspace_id = signed_in["workspaces"][0]["id"]
    with session_scope() as db:
        connector = Connector(workspace_id=workspace_id, kind="incident", enabled=True)
        db.add(connector)
        db.flush()
        sig, _ = ingest_signal(db, workspace_id, connector, delivery_key="delivery", external_id="issue",
                               title="incident", url=url)
        assert sig.url == ""
        signal_id = sig.id
    response = client.post(f"/api/workspaces/{workspace_id}/signals/{signal_id}", json={"action": "accept"})
    assert response.status_code == 200, response.text
    from aifactory.models import Outcome
    with session_scope() as db:
        outcome = db.get(Outcome, db.get(Signal, signal_id).outcome_id)
        assert url not in outcome.brief and "password" not in outcome.brief
        assert "password" not in repr([(event.message, event.data) for event in db.query(Event).all()])
