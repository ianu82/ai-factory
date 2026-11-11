from __future__ import annotations

import copy

import pytest
from fastapi.testclient import TestClient

from aifactory.db import session_scope
from aifactory.models import Event, Membership, Repository
from aifactory.steward_policy import steward_policy, validate_steward


DEFAULT = {"mode": "propose", "fix_minutes": 60}
# Serialized repository config also shows the PR-review opt-in defaults (never written).
REVIEW_DEFAULT = {"mode": "off", "allow_untrusted_local": False}
INVALID = [
    None, False, [], "fix", 1,
    {"fix_minute": 30}, {"mode": "fix", "fix_minutes": 30, "extra": True},
    *({"mode": value} for value in (None, True, False, 1, "auto", "FIX", "", [], {})),
    *({"fix_minutes": value} for value in (None, True, False, 0, -1, 1.5, 60.0, "60", "", [], {})),
]


def test_normalization_defaults_partial_values_and_no_mutation():
    for config in (None, {}, {"steward": {}}):
        assert steward_policy(config) == DEFAULT
    config = {"steward": {"mode": "off"}, "checks": ["pytest"]}
    original = copy.deepcopy(config)
    assert steward_policy(config) == {**DEFAULT, "mode": "off"}
    assert steward_policy({"steward": {"fix_minutes": 5}}) == {**DEFAULT, "fix_minutes": 5}
    policy = steward_policy(config)
    policy["mode"] = "fix"
    assert config == original
    assert steward_policy() == DEFAULT
    patch = {"mode": "fix", "fix_minutes": 1}
    assert validate_steward(patch) == patch
    assert validate_steward(patch) is not patch


@pytest.mark.parametrize("invalid", INVALID)
def test_policy_rejects_invalid_values(invalid):
    with pytest.raises(ValueError):
        validate_steward(invalid)
    with pytest.raises(ValueError):
        steward_policy({"steward": invalid})


@pytest.fixture
def repository(client, signed_in):
    workspace_id = signed_in["workspaces"][0]["id"]
    with session_scope() as db:
        repo = Repository(workspace_id=workspace_id, full_name="acme/app", default_branch="trunk", config={})
        db.add(repo)
        db.flush()
        repo_id = repo.id
    return workspace_id, repo_id


def snapshot(repo_id):
    with session_scope() as db:
        return copy.deepcopy(db.get(Repository, repo_id).config), db.query(Event).filter_by(
            kind="repository_configured").count()


def test_legacy_empty_config_exposes_defaults_without_writing(client, repository):
    ws, repo_id = repository
    response = client.get(f"/api/workspaces/{ws}/repositories")
    assert response.status_code == 200
    assert response.json()[0]["config"]["steward"] == DEFAULT
    assert response.json()[0]["default_branch"] == "trunk"
    assert snapshot(repo_id) == ({}, 0)


@pytest.mark.parametrize("role", ["owner", "admin"])
def test_admin_round_trip_all_modes_and_partial_updates(client, repository, role):
    ws, repo_id = repository
    base = f"/api/workspaces/{ws}/repositories"
    unrelated = {"checks": ["pytest"], "custom": {"keep": [1, 2]}, "merge": {"mode": "approval"}}
    with session_scope() as db:
        db.query(Membership).filter_by(workspace_id=ws).one().role = role
        db.get(Repository, repo_id).config = unrelated
    for mode, minutes in (("off", 1), ("propose", 60), ("fix", 120)):
        policy = {"mode": mode, "fix_minutes": minutes}
        response = client.patch(f"{base}/{repo_id}", json={"config": {"steward": policy}})
        assert response.status_code == 200, response.text
        assert response.json()["config"] == {**unrelated, "steward": policy, "review": REVIEW_DEFAULT}
        assert client.get(base).json()[0]["config"] == response.json()["config"]
        assert snapshot(repo_id)[0] == {**unrelated, "steward": policy}
    response = client.patch(f"{base}/{repo_id}", json={"config": {"steward": {"mode": "off"}}})
    assert response.json()["config"]["steward"] == {"mode": "off", "fix_minutes": 120}
    response = client.patch(f"{base}/{repo_id}", json={"config": {"steward": {"fix_minutes": 2}}})
    assert response.json()["config"]["steward"] == {"mode": "off", "fix_minutes": 2}
    response = client.patch(f"{base}/{repo_id}", json={"config": {"context": "notes"}})
    assert response.json()["config"] == {**unrelated, "steward": {"mode": "off", "fix_minutes": 2}, "context": "notes",
                                         "review": REVIEW_DEFAULT}


@pytest.mark.parametrize("invalid", INVALID)
def test_invalid_patch_is_atomic_and_not_audited(client, repository, invalid):
    ws, repo_id = repository
    with session_scope() as db:
        db.get(Repository, repo_id).config = {"context": "keep", "steward": {"mode": "fix", "fix_minutes": 25}}
    before = snapshot(repo_id)
    response = client.patch(f"/api/workspaces/{ws}/repositories/{repo_id}", json={
        "config": {"require_trusted_issue_author": False, "context": "changed", "steward": invalid}})
    assert response.status_code == 422, response.text
    assert "steward" in response.json()["detail"]
    assert snapshot(repo_id) == before


@pytest.mark.parametrize("unknown", ["fix_minute", "extra"])
def test_unknown_policy_field_is_named_and_patch_is_atomic(client, repository, unknown):
    ws, repo_id = repository
    with session_scope() as db:
        db.get(Repository, repo_id).config = {"context": "keep", "steward": {"mode": "off", "fix_minutes": 25}}
    before = snapshot(repo_id)
    response = client.patch(f"/api/workspaces/{ws}/repositories/{repo_id}", json={
        "config": {"context": "changed", "steward": {"mode": "fix", "fix_minutes": 10, unknown: 30}}})
    assert response.status_code == 422, response.text
    assert unknown in response.json()["detail"]
    assert snapshot(repo_id) == before
    assert client.get(f"/api/workspaces/{ws}/repositories").json()[0]["config"] == {**before[0], "review": REVIEW_DEFAULT}


@pytest.mark.parametrize("role", ["member", "viewer"])
def test_non_admins_can_read_but_cannot_write(client, repository, role):
    ws, repo_id = repository
    with session_scope() as db:
        db.query(Membership).filter_by(workspace_id=ws).one().role = role
    before = snapshot(repo_id)
    base = f"/api/workspaces/{ws}/repositories"
    assert client.get(base).json()[0]["config"]["steward"] == DEFAULT
    assert client.patch(f"{base}/{repo_id}", json={"config": {"steward": {"mode": "fix"}}}).status_code == 403
    assert snapshot(repo_id) == before


def test_cross_workspace_policy_access_is_denied(client, repository):
    ws, repo_id = repository
    before = snapshot(repo_id)
    with TestClient(client.app) as other:
        assert other.post("/auth/dev", json={"email": "other@other.test", "organization": "Other"}).status_code == 200
        other_ws = other.get("/api/me").json()["workspaces"][0]["id"]
        assert other_ws != ws
        assert other.get(f"/api/workspaces/{ws}/repositories").status_code == 404
        for target_ws in (ws, other_ws):
            response = other.patch(f"/api/workspaces/{target_ws}/repositories/{repo_id}",
                                   json={"config": {"steward": {"mode": "fix", "fix_minutes": 1}}})
            assert response.status_code == 404, response.text
    assert snapshot(repo_id) == before


@pytest.mark.parametrize("invalid", INVALID)
def test_invalid_stored_policy_lists_with_defaults_and_can_be_replaced(client, repository, invalid):
    ws, repo_id = repository
    stored = {"context": "keep", "steward": invalid}
    with session_scope() as db:
        db.get(Repository, repo_id).config = stored
        db.add(Repository(workspace_id=ws, full_name="acme/healthy", config={}))
    base = f"/api/workspaces/{ws}/repositories"
    response = client.get(base)
    assert response.status_code == 200, response.text
    repos = {r["id"]: r for r in response.json()}
    assert len(repos) == 2
    assert repos[repo_id]["steward_policy_invalid"] is True
    assert repos[repo_id]["config"] == {"context": "keep", "steward": DEFAULT, "review": REVIEW_DEFAULT}
    healthy = next(r for r in repos.values() if r["id"] != repo_id)
    assert healthy["steward_policy_invalid"] is False
    assert snapshot(repo_id)[0] == stored  # Reading never rewrites operator data.
    # Unrelated updates still work and retain the warning and invalid stored data.
    response = client.patch(f"{base}/{repo_id}", json={"config": {"context": "updated"}})
    assert response.status_code == 200, response.text
    assert response.json()["steward_policy_invalid"] is True
    assert snapshot(repo_id)[0] == {**stored, "context": "updated"}
    response = client.patch(f"{base}/{repo_id}", json={"config": {"steward": {"mode": "off"}}})
    assert response.status_code == 200, response.text
    assert response.json()["steward_policy_invalid"] is False
    assert response.json()["config"] == {"context": "updated", "steward": {**DEFAULT, "mode": "off"},
                                         "review": REVIEW_DEFAULT}


@pytest.mark.parametrize("policy", [
    {"fix_minute": 30}, {"mode": "auto"}, {"fix_minutes": True}, None,
    {"mode": "fix", "fix_minutes": 15}, {"mode": "off"},
])
def test_bootstrap_existing_repository_validates_policy(client, repository, monkeypatch, caplog, policy):
    import asyncio
    import json

    from aifactory.bootstrap import apply_bootstrap
    from aifactory.models import Workspace

    ws, repo_id = repository
    original_policy = {"mode": "propose", "fix_minutes": 25}
    with session_scope() as db:
        workspace = db.get(Workspace, ws)
        workspace.settings = {**workspace.settings, "_bootstrap_workspace": True}
        db.get(Repository, repo_id).config = {"steward": original_policy, "checks": ["pytest"]}
    monkeypatch.setenv("AIFACTORY_BOOTSTRAP", json.dumps({"repositories": [
        {"full_name": "acme/app", "config": {"steward": policy, "context": "operator notes"}},
    ]}))

    async def run():
        return apply_bootstrap()

    assert asyncio.run(run()) == ws
    config = snapshot(repo_id)[0]
    assert config["checks"] == ["pytest"]
    assert config["context"] == "operator notes"
    if policy in INVALID:
        assert config["steward"] == original_policy
        assert "steward" not in config["_bootstrap"]
        assert "invalid steward policy for acme/app" in caplog.text
    else:
        assert config["steward"] == policy
        assert "steward" in config["_bootstrap"]
    assert client.get(f"/api/workspaces/{ws}/repositories").status_code == 200


@pytest.mark.parametrize("policy", [
    {"fix_minute": 30}, {"mode": "auto"}, {"fix_minutes": True}, None,
    {"mode": "fix", "fix_minutes": 15}, {"fix_minutes": 5},
])
def test_bootstrap_connect_validates_policy(client, repository, monkeypatch, caplog, policy):
    import asyncio

    from aifactory import github, repo_detect
    from aifactory.bootstrap import _connect

    ws, _ = repository

    async def repo(self, name):
        return {"full_name": name, "default_branch": "trunk"}

    async def detect(gh, name, branch):
        assert branch == "release"
        return {"checks": ["pytest"]}

    monkeypatch.setattr(github, "resolve_credential", lambda workspace_id: "test-token")
    monkeypatch.setattr(github.GitHub, "repo", repo)
    monkeypatch.setattr(repo_detect, "detect", detect)
    overrides = {"steward": policy, "context": "operator notes", "_base_branch": "release"}
    original = copy.deepcopy(overrides)
    asyncio.run(_connect(ws, [("acme/new", overrides)]))
    assert overrides == original
    with session_scope() as db:
        connected = db.query(Repository).filter_by(full_name="acme/new").one()
        config = connected.config
        assert connected.default_branch == "release"
    assert config["checks"] == ["pytest"]
    assert config["context"] == "operator notes"
    assert "_base_branch" not in config
    if policy in INVALID:
        assert "steward" not in config
        assert "steward" not in config["_bootstrap"]
        assert "invalid steward policy for acme/new" in caplog.text
    else:
        assert config["steward"] == policy
        assert "steward" in config["_bootstrap"]
    assert client.get(f"/api/workspaces/{ws}/repositories").status_code == 200
