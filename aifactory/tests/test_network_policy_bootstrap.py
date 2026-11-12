from __future__ import annotations

import asyncio
import copy
import json

import pytest

from aifactory import bootstrap, github, repo_detect
from aifactory.db import session_scope
from aifactory.models import Repository, Workspace
from aifactory.network_policy import NetworkPolicyError


@pytest.fixture
def bootstrap_workspace(client, signed_in):
    ws = signed_in["workspaces"][0]["id"]
    with session_scope() as db:
        workspace = db.get(Workspace, ws)
        workspace.settings = {**workspace.settings, "_bootstrap_workspace": True}
    return ws


@pytest.mark.parametrize("path", ["existing", "new", "connect"])
@pytest.mark.parametrize("network", [
    None,
    {"mode": "restricted", "registries": [{"host": "*.example.org", "port": 443}]},
    {"mode": "restricted", "registries": [{"host": "user:secret@pypi.org", "port": 443}]},
    {"mode": "restricted", "registries": [{"host": "pypi.org", "port": 0}]},
])
def test_invalid_bootstrap_network_raises_without_persisting(
    bootstrap_workspace, monkeypatch, path, network,
):
    ws = bootstrap_workspace
    original = {"network": {"mode": "restricted", "registries": []}, "context": "unchanged"}
    if path == "existing":
        with session_scope() as db:
            db.add(Repository(workspace_id=ws, full_name="acme/app", config=original))
    overrides = {"network": network, "context": "must not persist"}
    before = copy.deepcopy(overrides)
    monkeypatch.setenv("AIFACTORY_BOOTSTRAP", json.dumps({"repositories": [
        {"full_name": "acme/app", "config": overrides},
    ]}))

    def unexpected(*args):
        pytest.fail("Invalid network override must fail before GitHub access")

    monkeypatch.setattr(github, "resolve_credential", unexpected)

    async def run():
        if path == "connect":
            await bootstrap._connect(ws, [("acme/app", overrides)])
        else:
            bootstrap.apply_bootstrap()

    with pytest.raises(NetworkPolicyError) as error:
        asyncio.run(run())
    assert "secret" not in str(error.value)
    assert overrides == before
    with session_scope() as db:
        repo = db.query(Repository).filter_by(workspace_id=ws, full_name="acme/app").one_or_none()
        if path == "existing":
            assert repo.config == original
        else:
            assert repo is None


@pytest.mark.parametrize("path", ["existing", "new", "connect"])
def test_bootstrap_network_stores_canonical_policy(bootstrap_workspace, monkeypatch, path):
    ws = bootstrap_workspace
    if path == "existing":
        with session_scope() as db:
            db.add(Repository(workspace_id=ws, full_name="acme/app", config={"checks": ["pytest"]}))
    network = {"mode": "restricted", "registries": [
        {"host": "PYPI.ORG.", "port": 443}, {"host": "pypi.org", "port": 443},
        {"host": "2001:0db8::1", "port": 8443},
    ]}
    expected = {"mode": "restricted", "registries": [
        {"host": "2001:db8::1", "port": 8443}, {"host": "pypi.org", "port": 443},
    ]}
    overrides = {"network": network, "context": "operator notes"}
    before = copy.deepcopy(overrides)
    monkeypatch.setenv("AIFACTORY_BOOTSTRAP", json.dumps({"repositories": [
        {"full_name": "acme/app", "config": overrides},
    ]}))

    async def repo(self, name):
        return {"full_name": name, "default_branch": "main"}

    async def detect(*args):
        return {"checks": ["pytest"]}

    monkeypatch.setattr(github, "resolve_credential", lambda workspace_id: "test-token")
    monkeypatch.setattr(github.GitHub, "repo", repo)
    monkeypatch.setattr(repo_detect, "detect", detect)

    async def run():
        if path == "connect":
            await bootstrap._connect(ws, [("acme/app", overrides)])
        else:
            assert bootstrap.apply_bootstrap() == ws
            # The declared new repository is connected by a background task.
            await asyncio.gather(*asyncio.all_tasks() - {asyncio.current_task()})

    asyncio.run(run())
    assert overrides == before
    with session_scope() as db:
        config = db.query(Repository).filter_by(workspace_id=ws, full_name="acme/app").one().config
    assert config["network"] == expected
    assert config["checks"] == ["pytest"]
    assert config["context"] == "operator notes"
    applied = {}
    bootstrap._changed(applied, "network", expected)
    assert config["_bootstrap"]["network"] == applied["network"]
