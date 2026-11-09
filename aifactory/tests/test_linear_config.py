from __future__ import annotations

import copy
import json

import httpx
import pytest
from sqlalchemy.orm import Session

from aifactory import linear_config
from aifactory.db import session_scope
from aifactory.linear import Linear
from aifactory.models import Event, Membership, Repository, Secret, Workspace
from aifactory.security import encrypt
from .linear_fake import FakeLinear, connection

STAGES = ("queued", "building", "verifying", "merging", "done", "blocked")
TEAMS = [{"id": "team-1", "name": "Engineering", "key": "ENG"},
         {"id": "team-2", "name": "Other team", "key": "OTHER"}]
STATES = [{"id": f"state-{stage}", "name": f"Workflow {stage}", "type": "unstarted"} for stage in STAGES]
TOKEN = "short-secret-token"


@pytest.fixture
def configured(client, signed_in, monkeypatch):
    workspace_id = signed_in["workspaces"][0]["id"]
    with session_scope() as db:
        workspace = db.get(Workspace, workspace_id)
        workspace.settings = {**workspace.settings, "unrelated": {"keep": [1, 2]}, "concurrency": 7}
        repo = Repository(workspace_id=workspace_id, full_name="org/project")
        other = Workspace(org_id=workspace.org_id, name="Other", slug="other", settings={})
        db.add_all([repo, other, Secret(workspace_id=workspace_id, kind="linear_api_key",
                                        ciphertext=encrypt(TOKEN), status="valid")])
        db.flush()
        foreign = Repository(workspace_id=other.id, full_name="org/private")
        db.add(foreign)
        db.flush()
        repo_id, foreign_id = repo.id, foreign.id

    fake = FakeLinear()

    def handle(request):
        payload = json.loads(request.content)
        variables = payload["variables"]
        if "teams(" in payload["query"]:
            fake.data = {"teams": connection(TEAMS[1:] if variables["after"] else TEAMS[:1],
                                            None if variables["after"] else "teams-next")}
        else:
            states = STATES if variables["id"] == "team-1" else [{"id": "foreign-state", "name": "Queued",
                                                                  "type": "unstarted"}]
            fake.data = {"team": {"states": connection(states[3:] if variables["after"] else states[:3],
                                                        None if variables["after"] else "states-next")}}
        return fake.handle(request)

    monkeypatch.setattr(linear_config, "Linear", lambda token: Linear(token, transport=httpx.MockTransport(handle)))
    config = {"enabled": True, "team_id": "team-1", "repo_ids": [repo_id],
              "states": {stage: f"state-{stage}" for stage in STAGES}}
    return workspace_id, config, fake, foreign_id


def snapshot(workspace_id):
    with session_scope() as db:
        return copy.deepcopy(db.get(Workspace, workspace_id).settings), [
            (event.actor, event.data) for event in db.query(Event).filter_by(
                workspace_id=workspace_id, kind="settings_changed").order_by(Event.created_at)]


@pytest.mark.parametrize("role", ["owner", "admin"])
def test_save_reload_defaults_custom_label_and_audit(client, signed_in, configured, role):
    workspace_id, config, fake, _ = configured
    path = f"/api/workspaces/{workspace_id}/linear"
    with session_scope() as db:
        db.query(Membership).filter_by(workspace_id=workspace_id).one().role = role
    before, _ = snapshot(workspace_id)
    assert client.get(path).json() == linear_config.LinearConfig().model_dump()
    response = client.put(path, json=config)
    assert response.status_code == 200, response.text
    expected = {**config, "intake_label": "factory-auto"}
    assert response.json() == expected == client.get(path).json()
    stored, events = snapshot(workspace_id)
    assert stored == {**before, "linear": expected}
    assert events == [(signed_in["user"]["id"], {"changes": {"linear": expected}})]
    assert TOKEN not in json.dumps(events)
    assert client.put(path, json=config).status_code == 200
    assert snapshot(workspace_id) == (stored, events)
    config["intake_label"] = "ready-for-factory"
    assert client.put(path, json=config).status_code == 200
    assert client.get(path).json()["intake_label"] == config["intake_label"]
    assert len(snapshot(workspace_id)[1]) == 2
    assert len(fake.requests) == 12
    assert client.patch(path.removesuffix("/linear"), json={"settings": {"concurrency": 9}}).status_code == 200
    assert client.get(path).json() == {**config}


def test_discovery_uses_real_ids_names_and_all_pages(client, configured):
    workspace_id, _, fake, _ = configured
    path = f"/api/workspaces/{workspace_id}/linear/teams"
    assert client.get(path).json() == {"items": TEAMS}
    assert client.get(path + "/team-1/states").json() == {"items": STATES}
    assert [payload["variables"]["after"] for _, payload in fake.requests] == [
        None, "teams-next", None, "states-next"]
    assert all(request.headers["Authorization"] == TOKEN for request, _ in fake.requests)


@pytest.mark.parametrize("invalid", ["repository", "foreign_state", "state_name", "team", "other_team", "no_team", "no_repos",
                                      *[f"missing_{stage}" for stage in STAGES], "label", "extra", "enabled_type"])
def test_invalid_save_is_atomic(client, configured, invalid):
    workspace_id, config, _, foreign_id = configured
    path = f"/api/workspaces/{workspace_id}/linear"
    assert client.put(path, json=config).status_code == 200
    before = snapshot(workspace_id)
    if invalid == "repository":
        config["repo_ids"].append(foreign_id)
    elif invalid in ("foreign_state", "state_name"):
        config["states"]["queued"] = "foreign-state" if invalid == "foreign_state" else "Workflow queued"
    elif invalid == "team":
        config["team_id"] = "unknown-team"
    elif invalid == "other_team":
        config["team_id"] = "team-2"
    elif invalid == "no_team":
        config["team_id"] = None
    elif invalid == "no_repos":
        config["repo_ids"] = []
    elif invalid.startswith("missing_"):
        del config["states"][invalid.removeprefix("missing_")]
    elif invalid == "label":
        config["intake_label"] = "  "
    elif invalid == "extra":
        config["api_key"] = "lin_api_do_not_store"
    else:
        config["enabled"] = "true"
    assert client.put(path, json=config).status_code == 422
    assert snapshot(workspace_id) == before


@pytest.mark.parametrize("invalid", ["api_key", "nested", "type", "required"])
def test_invalid_configuration_does_not_echo_inputs(client, configured, invalid, caplog):
    workspace_id, config, _, _ = configured
    before = snapshot(workspace_id)
    credential = 'private-"credential"-\\value'
    if invalid == "api_key":
        config["api_key"] = credential
    elif invalid == "nested":
        config["states"]["api_key"] = credential
    elif invalid == "type":
        config["enabled"] = credential
    else:
        config["intake_label"] = credential
        config["repo_ids"] = []
    response = client.put(f"/api/workspaces/{workspace_id}/linear", json=config)
    assert response.status_code == 422
    assert response.json() == {"detail": "Invalid Linear configuration request"}
    assert credential not in response.text + caplog.text
    assert snapshot(workspace_id) == before


@pytest.mark.parametrize("role", ["member", "viewer", "outsider"])
def test_permissions(client, configured, role):
    workspace_id, config, fake, _ = configured
    path = f"/api/workspaces/{workspace_id}/linear"
    assert client.put(path, json=config).status_code == 200
    before = snapshot(workspace_id)
    fake.requests.clear()
    with session_scope() as db:
        member = db.query(Membership).filter_by(workspace_id=workspace_id).one()
        if role == "outsider":
            db.delete(member)
        else:
            member.role = role
    expected = 404 if role == "outsider" else 403
    assert client.get(path).status_code == (404 if role == "outsider" else 200)
    assert client.put(path, json=config).status_code == expected
    assert client.get(path + "/teams").status_code == expected
    assert client.get(path + "/teams/team-1/states").status_code == expected
    assert not fake.requests
    assert snapshot(workspace_id) == before


@pytest.mark.parametrize("value", [None, {}, {"enabled": True}, {"enabled": False}])
def test_generic_patch_cannot_write_linear(client, configured, value):
    workspace_id, _, _, _ = configured
    before = snapshot(workspace_id)
    response = client.patch(f"/api/workspaces/{workspace_id}", json={"name": "Do not change",
                            "settings": {"linear": value, "concurrency": 20}})
    assert response.status_code == 422
    assert "/linear" in response.json()["detail"]
    assert snapshot(workspace_id) == before
    with session_scope() as db:
        assert db.get(Workspace, workspace_id).name != "Do not change"


@pytest.mark.parametrize("failure", ["missing", "invalid", 401, 429, 503, "timeout", "late_page"])
@pytest.mark.parametrize("previously_saved", [False, True])
def test_discovery_failure_never_persists_enabled_config(client, configured, failure, previously_saved, caplog):
    workspace_id, config, fake, _ = configured
    path = f"/api/workspaces/{workspace_id}/linear"
    if previously_saved:
        assert client.put(path, json=config).status_code == 200
    before = snapshot(workspace_id)
    config["intake_label"] = "new-label"
    if failure in ("missing", "invalid"):
        with session_scope() as db:
            secret = db.query(Secret).filter_by(workspace_id=workspace_id).one()
            if failure == "missing":
                db.delete(secret)
            else:
                secret.status = "invalid"
    elif failure == "timeout":
        fake.failure = httpx.ReadTimeout(TOKEN)
    elif failure == "late_page":
        original = fake.handle

        def fail_late(request):
            payload = json.loads(request.content)
            if payload["variables"]["after"] == "states-next":
                return httpx.Response(200, json={"errors": [{"message": TOKEN}]})
            return original(request)

        fake.handle = fail_late
    else:
        fake.status = failure
    response = client.put(path, json=config)
    assert response.status_code == 503
    assert TOKEN not in response.text + caplog.text
    assert snapshot(workspace_id) == before
    if failure != "late_page":
        assert client.get(path + "/teams").status_code == 503
        assert client.get(path + "/teams/team-1/states").status_code == 503
    assert client.put(path, json={"enabled": False}).status_code == 200
    assert client.get(path).json()["enabled"] is False


@pytest.mark.parametrize("failure", ["missing", "invalid", 401, 429, 503, "timeout"])
def test_disable_retains_configuration_without_discovery(client, signed_in, configured, failure):
    workspace_id, config, fake, foreign_id = configured
    path = f"/api/workspaces/{workspace_id}/linear"
    assert client.put(path, json=config).status_code == 200
    before, events = snapshot(workspace_id)
    fake.requests.clear()
    if failure in ("missing", "invalid"):
        with session_scope() as db:
            secret = db.query(Secret).filter_by(workspace_id=workspace_id).one()
            if failure == "missing":
                db.delete(secret)
            else:
                secret.status = "invalid"
    elif failure == "timeout":
        fake.failure = httpx.ReadTimeout(TOKEN)
    else:
        fake.status = failure
    config["enabled"] = False
    invalid = {**config, "repo_ids": [foreign_id]}
    assert client.put(path, json=invalid).status_code == 422
    assert snapshot(workspace_id) == (before, events)
    response = client.put(path, json=config)
    assert response.status_code == 200, response.text
    expected = {**config, "intake_label": "factory-auto"}
    assert client.get(path).json() == expected
    stored, saved_events = snapshot(workspace_id)
    assert stored == {**before, "linear": expected}
    assert saved_events == [*events, (signed_in["user"]["id"], {"changes": {"linear": expected}})]
    assert not fake.requests


def test_event_redacts_workspace_credentials(client, configured):
    workspace_id, config, _, _ = configured
    config["intake_label"] = TOKEN
    assert client.put(f"/api/workspaces/{workspace_id}/linear", json=config).status_code == 200
    _, events = snapshot(workspace_id)
    assert events[0][1]["changes"]["linear"]["intake_label"] == "[redacted]"
    assert TOKEN not in json.dumps(events)


@pytest.mark.parametrize("credential", [
    'token-with-"quotes"',
    "token-with-\\backslashes\\",
    "token-with-unicode-\u00e9-\u2603",
    "token-with-\nnewline-\ttab",
    'token-with-"quotes"-\\backslash-\u00e9-\nnewline',
])
@pytest.mark.parametrize("enabled", [False, True])
@pytest.mark.parametrize("embedded", [False, True])
def test_event_redacts_credentials_before_json_escaping(client, signed_in, configured, credential, enabled, embedded):
    workspace_id, config, _, _ = configured
    with session_scope() as db:
        db.add(Secret(workspace_id=workspace_id, kind="github_token", ciphertext=encrypt(credential), status="valid"))
    label = f"prefix-{credential}-suffix" if embedded else credential
    config = {**config, "intake_label": label} if enabled else {"enabled": False, "intake_label": label}
    path = f"/api/workspaces/{workspace_id}/linear"
    assert client.put(path, json=config).status_code == 200
    assert client.get(path).json()["intake_label"] == label
    stored, events = snapshot(workspace_id)
    assert stored["linear"]["intake_label"] == label
    expected = copy.deepcopy(stored["linear"])
    expected["intake_label"] = "prefix-[redacted]-suffix" if embedded else "[redacted]"
    assert events == [(signed_in["user"]["id"], {"changes": {"linear": expected}})]
    assert credential not in json.dumps(events, ensure_ascii=False)
    assert client.put(path, json=config).status_code == 200
    assert snapshot(workspace_id) == (stored, events)


def test_event_and_configuration_share_transaction(client, configured, monkeypatch):
    workspace_id, config, _, _ = configured
    before = snapshot(workspace_id)

    def fail_commit(session):
        raise RuntimeError("commit failed")

    with monkeypatch.context() as patch:
        patch.setattr(Session, "commit", fail_commit)
        with pytest.raises(RuntimeError, match="commit failed"):
            client.put(f"/api/workspaces/{workspace_id}/linear", json=config)
    assert snapshot(workspace_id) == before
