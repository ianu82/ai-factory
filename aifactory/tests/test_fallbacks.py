"""Server fallback credentials serve the operator's bootstrap workspace only."""

from __future__ import annotations

import asyncio
import json
from datetime import timedelta

import pytest

from aifactory import config, db
from aifactory.bootstrap import apply_bootstrap
from aifactory.fallbacks import set_bootstrap_workspace
from aifactory.github import GitHubError, resolve_token
from aifactory.inference import InferenceError, resolve_provider
from aifactory.models import Organization, Workspace

OPERATOR = {"AIFACTORY_BOOTSTRAP_GITHUB_TOKEN": "operator-github-secret",
            "AIFACTORY_BOOTSTRAP_MINDSHUB_API_KEY": "operator-mindshub-secret",
            "AIFACTORY_BOOTSTRAP_OPENAI_API_KEY": "operator-openai-secret"}


@pytest.fixture
def operator_keys(monkeypatch):
    for name, value in OPERATOR.items():
        monkeypatch.setenv(name, value)
    config.reset_settings_cache()


def boot(monkeypatch, cfg):
    monkeypatch.setenv("AIFACTORY_BOOTSTRAP", json.dumps(cfg))

    async def run():
        return apply_bootstrap()

    return asyncio.run(run())


def add_workspace(org_name: str, name: str, *, after: str | None = None) -> str:
    with db.session_scope() as s:
        org = s.query(Organization).filter(Organization.name == org_name).one()
        ws = Workspace(org_id=org.id, name=name, slug=name.lower().replace(" ", "-"), settings={})
        if after:  # created later than the given workspace, whatever the clock resolution
            ws.created_at = s.get(Workspace, after).created_at + timedelta(seconds=1)
        s.add(ws)
        s.flush()
        return ws.id


def test_only_the_bootstrap_workspace_gets_server_credentials(monkeypatch, operator_keys):
    boot_id = boot(monkeypatch, {"organization": "MindsDB", "workspace": "Factory"})
    second = add_workspace("MindsDB", "MindsHub products")
    namesake = add_workspace("MindsDB", "Factory", after=boot_id)  # same name, created later

    assert resolve_token(boot_id) == "operator-github-secret"
    assert resolve_provider(boot_id).api_key == "operator-mindshub-secret"
    assert resolve_provider(boot_id, required_provider="openai").api_key == "operator-openai-secret"
    for other in (second, namesake):
        with pytest.raises(GitHubError, match="Add a GitHub token"):
            resolve_token(other)
        for required in (None, "mindshub", "openai"):
            with pytest.raises(InferenceError):
                resolve_provider(other, required_provider=required)

    # A restart keeps the oldest workspace of that name; the namesake never takes over.
    assert boot(monkeypatch, {"organization": "MindsDB", "workspace": "Factory"}) == boot_id
    with pytest.raises(GitHubError):
        resolve_token(namesake)


def test_without_a_bootstrap_no_workspace_gets_server_credentials(monkeypatch, operator_keys):
    monkeypatch.delenv("AIFACTORY_BOOTSTRAP", raising=False)
    with db.session_scope() as s:
        org = Organization(name="Solo")
        s.add(org)
        s.flush()
        ws = Workspace(org_id=org.id, name="Only", slug="only", settings={})
        s.add(ws)
        s.flush()
        ws_id = ws.id

    async def run():
        return apply_bootstrap()

    assert asyncio.run(run()) is None
    with pytest.raises(GitHubError):
        resolve_token(ws_id)
    with pytest.raises(InferenceError):
        resolve_provider(ws_id)


def test_a_new_workspace_shows_its_own_credentials_as_missing(client, signed_in, operator_keys):
    ws_id = signed_in["workspaces"][0]["id"]
    org_name = signed_in["workspaces"][0]["org_name"]
    set_bootstrap_workspace(add_workspace(org_name, "Factory"))  # the operator's workspace is another one

    setup = client.get(f"/api/workspaces/{ws_id}/onboarding")
    assert setup.status_code == 200
    assert {"models", "github"} <= set(setup.json()["missing_steps"])
    assert setup.json()["ready"] is False
    creds = client.get(f"/api/workspaces/{ws_id}/credentials")
    assert creds.status_code == 200
    assert creds.json()["server_fallbacks"] == {"github_token": False, "openai_api_key": False,
                                                "mindshub_api_key": False}
    for secret in OPERATOR.values():
        assert secret not in setup.text and secret not in creds.text
