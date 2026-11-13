from __future__ import annotations

import pytest

from aifactory import api as api_module
from aifactory.db import session_scope
from aifactory.models import Membership, Repository


@pytest.fixture
def repo_detection(monkeypatch):
    async def detect(workspace_id, full_name):
        return {"full_name": full_name, "default_branch": "main"}, {
            "setup": ["npm ci"], "checks": ["npm test"], "detected": ["Node"],
        }

    monkeypatch.setattr(api_module, "_detect_repository", detect)


def test_network_connect_update_roundtrip_and_unrelated_edits(client, signed_in, repo_detection):
    path = f"/api/workspaces/{signed_in['workspaces'][0]['id']}/repositories"
    network = {"mode": "restricted", "registries": [
        {"host": "REGISTRY.NPMJS.ORG.", "port": 443}, {"host": "registry.npmjs.org", "port": 443},
        {"host": "2001:0db8::1", "port": 8443},
    ]}
    expected = {"mode": "restricted", "registries": [
        {"host": "2001:db8::1", "port": 8443}, {"host": "registry.npmjs.org", "port": 443},
    ]}
    response = client.post(path, json={"full_name": "acme/app", "config": {"network": network}})
    assert response.status_code == 200, response.text
    repo = response.json()
    assert repo["config"]["network"] == expected
    assert repo["config"]["setup"] == ["npm ci"]
    with session_scope() as db:
        assert db.get(Repository, repo["id"]).config["network"] == expected
    assert client.get(path).json()[0]["config"]["network"] == expected
    detail = f"{path}/{repo['id']}"
    response = client.patch(detail, json={"config": {"context": "Keep policy unchanged"}})
    assert response.status_code == 200
    assert response.json()["config"]["network"] == expected
    # Explicit unrestricted is retained, including registries for future benchmark selection.
    expected["mode"] = "unrestricted"
    response = client.patch(detail, json={"config": {"network": expected}})
    assert response.status_code == 200
    assert response.json()["config"]["network"] == expected
    assert client.get(path).json()[0]["config"]["network"] == expected
    with session_scope() as db:
        assert db.get(Repository, repo["id"]).config["network"] == expected


def test_connect_without_network_does_not_add_restriction(client, signed_in, repo_detection):
    path = f"/api/workspaces/{signed_in['workspaces'][0]['id']}/repositories"
    response = client.post(path, json={"full_name": "acme/legacy"})
    assert response.status_code == 200
    assert "network" not in response.json()["config"]


@pytest.mark.parametrize("network", [
    None, "restricted", {}, {"mode": "invalid"},
    {"mode": "restricted", "registries": ["https://pypi.org"]},
    {"mode": "restricted", "registries": [{"host": "*.example.org", "port": 443}]},
    {"mode": "restricted", "registries": [{"host": "192.0.2.0/24", "port": 443}]},
    {"mode": "restricted", "registries": [{"host": "pypi.org", "port": 0}]},
    {"mode": "restricted", "registries": [{"host": "pypi.org", "port": True}]},
    {"mode": "restricted", "registries": [{"host": "user:password@pypi.org", "port": 443}]},
    {"mode": "restricted", "gateway": {"host": "attacker.example", "port": 443}},
])
def test_invalid_network_is_422_on_connect_and_patch(client, signed_in, monkeypatch, network):
    ws = signed_in["workspaces"][0]["id"]
    path = f"/api/workspaces/{ws}/repositories"

    async def unexpected(*args):
        pytest.fail("Malformed connection policy must fail before external detection")

    monkeypatch.setattr(api_module, "_detect_repository", unexpected)
    response = client.post(path, json={"full_name": "acme/app", "config": {"network": network}})
    assert response.status_code == 422, response.text
    assert "user:password" not in response.text
    with session_scope() as db:
        assert db.query(Repository).filter_by(workspace_id=ws).count() == 0
        repo = Repository(workspace_id=ws, full_name="acme/app", config={"network": {"mode": "restricted", "registries": []}})
        db.add(repo)
        db.flush()
        rid = repo.id
    response = client.patch(f"{path}/{rid}", json={"config": {"network": network}})
    assert response.status_code == 422, response.text
    with session_scope() as db:
        assert db.get(Repository, rid).config["network"] == {"mode": "restricted", "registries": []}


@pytest.mark.parametrize("role", ["member", "viewer"])
def test_non_admin_cannot_connect_or_update_network(client, signed_in, repo_detection, role):
    ws = signed_in["workspaces"][0]["id"]
    path = f"/api/workspaces/{ws}/repositories"
    network = {"mode": "restricted", "registries": []}
    created = client.post(path, json={"full_name": "acme/app", "config": {"network": network}})
    assert created.status_code == 200
    rid = created.json()["id"]
    client.post("/auth/logout")
    client.cookies.clear()
    assert client.post("/auth/dev", json={"email": "member@example.com", "organization": "MindsDB"}).status_code == 200
    uid = client.get("/api/me").json()["user"]["id"]
    with session_scope() as db:
        db.query(Membership).filter_by(workspace_id=ws, user_id=uid).one().role = role
    assert client.post(path, json={"full_name": "acme/other", "config": {"network": network}}).status_code == 403
    assert client.patch(f"{path}/{rid}", json={"config": {"network": {"mode": "unrestricted"}}}).status_code == 403
    assert client.get(path).json()[0]["config"]["network"] == network


def test_admin_can_configure_network(client, signed_in, repo_detection):
    ws = signed_in["workspaces"][0]["id"]
    with session_scope() as db:
        db.query(Membership).filter_by(workspace_id=ws).one().role = "admin"
    path = f"/api/workspaces/{ws}/repositories"
    created = client.post(path, json={"full_name": "acme/app", "config": {"network": {"mode": "restricted"}}})
    assert created.status_code == 200
    response = client.patch(f"{path}/{created.json()['id']}", json={"config": {"network": {"mode": "unrestricted"}}})
    assert response.status_code == 200
    assert response.json()["config"]["network"] == {"mode": "unrestricted", "registries": []}


def test_network_validation_can_be_exercised_without_github_credentials(client, signed_in):
    # Runtime QA can validate requests without connecting to GitHub or seeding a repository.
    path = "/api/workspaces/qa-network-policy/repositories"
    for network in (
        {"mode": "restricted", "registries": [{"host": "*.example.org", "port": 443}]},
        {"mode": "restricted", "registries": [{"host": "pypi.org", "port": 0}]},
        {"mode": "restricted", "gateway": {"host": "attacker.example", "port": 443}},
    ):
        response = client.post(path, json={"full_name": "acme/app", "config": {"network": network}})
        assert response.status_code == 422
        assert response.json() == {"detail": "Invalid repository request"}
