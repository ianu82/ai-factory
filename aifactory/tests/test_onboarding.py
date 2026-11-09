"""Onboarding stores navigation, not credentials or client-asserted readiness."""

from __future__ import annotations

import json

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, inspect, text

from aifactory import config, db
from aifactory.github import REQUIRED_PERMISSIONS
from aifactory.models import Membership, Outcome, Repository, Secret, Workspace
from aifactory.onboarding import STEPS


def url(signed_in):
    return f"/api/workspaces/{signed_in['workspaces'][0]['id']}/onboarding"


def seed_ready(ws_id, *, provider="mindshub", status="valid", metadata=True):
    with db.session_scope() as s:
        s.add_all([
            Secret(workspace_id=ws_id, kind=f"{provider}_api_key", ciphertext="encrypted-model-secret",
                   hint="secret-hint", status=status, status_detail="private error detail",
                   metadata_json={"models": ["model"], "secret": "private metadata"} if metadata else {}),
            Secret(workspace_id=ws_id, kind="github_token", ciphertext="encrypted-github-secret", status=status,
                   metadata_json={"login": "octocat", "repository": "acme/app", "test_status": "valid",
                                  "diagnostic_code": "github_verified",
                                  "permissions": [{"permission": name, "state": "present"} for name in REQUIRED_PERMISSIONS]} if metadata else {}),
            Repository(workspace_id=ws_id, full_name="acme/app", config={"checks": ["pytest"]}),
        ])


def test_null_get_is_local_safe_and_does_not_mutate(client, signed_in, monkeypatch):
    def forbidden(*args, **kwargs):
        raise AssertionError("GET must not test or decrypt credentials")

    monkeypatch.setattr("httpx.AsyncClient.request", forbidden)
    monkeypatch.setattr("aifactory.security.decrypt", forbidden)
    ws_id = signed_in["workspaces"][0]["id"]
    with db.session_scope() as s:
        settings = s.get(Workspace, ws_id).settings
    response = client.get(url(signed_in))
    assert response.status_code == 200
    assert response.json() == {
        "current_step": "models", "completed_steps": [], "deferred": False, "first_outcome_id": None,
        "ready": False, "missing_steps": ["models", "github", "repositories"], "validation_uncertain_steps": [],
    }
    with db.session_scope() as s:
        ws = s.get(Workspace, ws_id)
        assert ws.onboarding is None
        assert ws.settings == settings


def test_progress_survives_sign_out_and_new_database_sessions(client, signed_in):
    path = url(signed_in)
    ws_id = signed_in["workspaces"][0]["id"]
    with db.session_scope() as s:
        settings = s.get(Workspace, ws_id).settings
    for action in [
        {"action": "navigate", "step": "team"},
        {"action": "complete", "step": "autonomy", "acknowledged": True},
        {"action": "complete", "step": "team", "acknowledged": True},
        {"action": "defer", "deferred": True},
    ]:
        assert client.patch(path, json=action).status_code == 200
    expected = client.get(path).json()
    assert expected["current_step"] == "team"
    assert expected["completed_steps"] == ["autonomy", "team"]
    assert expected["deferred"] is True
    assert expected["ready"] is False
    with db.session_scope() as s:
        assert s.get(Workspace, ws_id).settings == settings
        assert s.get(Workspace, ws_id).onboarding["completed_steps"] == ["autonomy", "team"]
    assert client.post("/auth/logout").status_code == 200
    assert client.get(path).status_code == 401
    assert client.patch(path, json={"action": "defer", "deferred": False}).status_code == 401
    with TestClient(client.app) as fresh:
        assert fresh.post("/auth/dev", json={"email": "ian@example.com", "name": "Ian", "organization": "MindsDB"}).status_code == 200
        assert fresh.get(path).json() == expected
        assert fresh.patch(path, json={"action": "defer", "deferred": False}).json()["deferred"] is False
        assert fresh.patch(path, json={"action": "navigate", "step": "models"}).json()["current_step"] == "models"
    with db.session_scope() as s:
        assert s.get(Workspace, ws_id).settings == settings


@pytest.mark.parametrize("role", ["owner", "admin", "member", "viewer"])
def test_roles(client, signed_in, role):
    ws_id = signed_in["workspaces"][0]["id"]
    with db.session_scope() as s:
        s.query(Membership).filter_by(workspace_id=ws_id, user_id=signed_in["user"]["id"]).one().role = role
    assert client.get(url(signed_in)).status_code == 200
    result = client.patch(url(signed_in), json={"action": "navigate", "step": "github"})
    assert result.status_code == (200 if role in ("owner", "admin") else 403)


def test_workspace_isolation_and_outcome_ownership(client, signed_in):
    path = url(signed_in)
    ws_id = signed_in["workspaces"][0]["id"]
    # Even within the same organization, ownership is workspace-scoped.
    with db.session_scope() as s:
        own_ws = s.get(Workspace, ws_id)
        other = Workspace(org_id=own_ws.org_id, name="Private", slug="private")
        s.add(other)
        s.flush()
        other_id = other.id
        foreign = Outcome(workspace_id=other_id, number=1, title="Private outcome")
        local = Outcome(workspace_id=ws_id, number=1, title="First outcome")
        s.add_all([foreign, local])
        s.flush()
        foreign_id, local_id = foreign.id, local.id
    other_path = f"/api/workspaces/{other_id}/onboarding"
    assert client.get(other_path).status_code == 404
    assert client.patch(other_path, json={"action": "defer", "deferred": True}).status_code == 404
    for outcome_id in (foreign_id, "missing"):
        assert client.patch(path, json={"action": "complete", "step": "outcome", "first_outcome_id": outcome_id}).status_code == 404
    result = client.patch(path, json={"action": "complete", "step": "outcome", "first_outcome_id": local_id})
    assert result.status_code == 200
    assert result.json()["first_outcome_id"] == local_id
    assert result.json()["completed_steps"] == ["outcome"]
    with db.session_scope() as s:
        assert s.get(Workspace, other_id).onboarding is None
        s.delete(s.get(Outcome, local_id))
    assert client.get(path).json()["first_outcome_id"] is None


@pytest.mark.parametrize("action", [
    {}, {"action": "reset"}, {"action": "navigate", "step": "unknown"},
    {"action": "navigate", "step": "models", "credentials": "do-not-echo-this-secret"},
    {"action": "defer", "deferred": "true"}, {"action": "complete", "step": "team"},
    {"action": "complete", "step": "autonomy", "acknowledged": False},
    {"action": "complete", "step": "team", "acknowledged": "true"},
    {"action": "complete", "step": "team", "acknowledged": 1},
    {"action": "complete", "step": "github", "acknowledged": True},
    {"action": "complete", "step": "models", "first_outcome_id": "anything"},
    {"completed_steps": list(STEPS), "ready": True},
    ["do-not-echo-this-secret"], None,
])
def test_invalid_actions_are_rejected_without_echoing_input(client, signed_in, action):
    response = client.patch(url(signed_in), content=json.dumps(action), headers={"Content-Type": "application/json"})
    assert response.status_code == 422
    assert "do-not-echo-this-secret" not in response.text
    with db.session_scope() as s:
        assert s.get(Workspace, signed_in["workspaces"][0]["id"]).onboarding is None


def test_false_completion_and_stale_state_cannot_hide_prerequisites(client, signed_in):
    path = url(signed_in)
    ws_id = signed_in["workspaces"][0]["id"]
    for step in ("models", "github", "repositories"):
        assert client.patch(path, json={"action": "complete", "step": step}).status_code == 409
    seed_ready(ws_id)
    for step in ("models", "github", "repositories"):
        assert client.patch(path, json={"action": "complete", "step": step}).status_code == 200
    with db.session_scope() as s:
        s.query(Secret).filter_by(workspace_id=ws_id).update({"status": "invalid"})
        s.query(Repository).filter_by(workspace_id=ws_id).delete()
        # Simulate a legacy client having stored bogus completion flags.
        s.get(Workspace, ws_id).onboarding = {"completed_steps": list(STEPS), "ready": True}
    result = client.get(path).json()
    assert result["ready"] is False
    assert result["missing_steps"] == ["models", "github", "repositories"]
    assert result["completed_steps"] == ["autonomy", "team"]
    for step in result["missing_steps"]:
        assert client.patch(path, json={"action": "complete", "step": step}).status_code == 409


@pytest.mark.parametrize("provider", ["mindshub", "openai"])
def test_ready_legacy_workspace_does_not_require_acknowledgments(client, signed_in, provider):
    ws_id = signed_in["workspaces"][0]["id"]
    seed_ready(ws_id, provider=provider)
    result = client.get(url(signed_in))
    assert result.status_code == 200
    assert result.json()["ready"] is True
    assert result.json()["missing_steps"] == []
    assert result.json()["completed_steps"] == ["models", "github", "repositories"]
    assert result.json()["validation_uncertain_steps"] == []
    for private in ("encrypted-model-secret", "encrypted-github-secret", "secret-hint", "private metadata", "private error detail", "ciphertext"):
        assert private not in result.text
    with db.session_scope() as s:
        assert s.get(Workspace, ws_id).onboarding is None


@pytest.mark.parametrize("status", ["valid", "untested"])
def test_identity_only_and_untested_credentials_report_uncertainty(client, signed_in, status):
    ws_id = signed_in["workspaces"][0]["id"]
    seed_ready(ws_id, status=status, metadata=False)
    result = client.get(url(signed_in)).json()
    assert result["ready"] is True  # preserve runtime and legacy compatibility
    assert result["validation_uncertain_steps"] == ["models", "github"]
    assert result["completed_steps"] == ["repositories"]
    assert client.patch(url(signed_in), json={"action": "complete", "step": "models"}).status_code == 409


def test_bootstrap_compatibility_is_not_reported_as_validated(client, signed_in, monkeypatch):
    monkeypatch.setenv("AIFACTORY_BOOTSTRAP_OPENAI_API_KEY", "bootstrap-model-secret")
    monkeypatch.setenv("AIFACTORY_BOOTSTRAP_GITHUB_TOKEN", "bootstrap-github-secret")
    config.reset_settings_cache()
    ws_id = signed_in["workspaces"][0]["id"]
    with db.session_scope() as s:
        s.add(Repository(workspace_id=ws_id, full_name="acme/bootstrap"))
    result = client.get(url(signed_in))
    assert result.json()["ready"] is True
    assert result.json()["validation_uncertain_steps"] == ["models", "github"]
    assert "bootstrap-model-secret" not in result.text
    assert "bootstrap-github-secret" not in result.text


def test_new_and_existing_sqlite_schema_use_additive_evolution(tmp_path):
    # A fresh database has the nullable column, without a migration or server default.
    fresh = db.init_engine(f"sqlite:///{tmp_path / 'fresh.db'}")
    column = next(c for c in inspect(fresh).get_columns("workspaces") if c["name"] == "onboarding")
    assert column["nullable"] is True
    assert column["default"] is None
    legacy_url = f"sqlite:///{tmp_path / 'legacy.db'}"
    legacy = create_engine(legacy_url)
    with legacy.begin() as conn:
        conn.execute(text("CREATE TABLE workspaces (id VARCHAR(32) PRIMARY KEY, settings JSON NOT NULL)"))
        conn.execute(text("INSERT INTO workspaces (id, settings) VALUES ('old', '{\"provider\": \"openai\"}')"))
    legacy.dispose()
    evolved = db.init_engine(legacy_url)
    with evolved.connect() as conn:
        row = conn.execute(text("SELECT settings, onboarding FROM workspaces WHERE id='old'")).one()
        assert json.loads(row.settings) == {"provider": "openai"}
        assert row.onboarding is None
    db.init_engine(legacy_url)  # repeated startup is idempotent
