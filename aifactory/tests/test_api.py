from __future__ import annotations

import asyncio
import json

import httpx
import pytest

from aifactory import api as api_module
from aifactory import auth as auth_module
from aifactory import gateway
from aifactory import signals as signals_module
from aifactory.db import SessionLocal, session_scope
from aifactory.github import GitHubError, resolve_token_with_generation
from aifactory.inference import ProviderConfig, UsageLedger
from aifactory.models import Event, IssueAcknowledgementState, Repository, Run, Secret, Workspace


def test_sign_in_creates_org_workspace_and_owner(client, signed_in):
    assert signed_in["user"]["email"] == "ian@example.com"
    assert len(signed_in["workspaces"]) == 1
    ws = signed_in["workspaces"][0]
    assert ws["role"] == "owner" and ws["org_name"] == "MindsDB"
    assert ws["settings"]["autonomy"]["merge"] == "auto"


def test_second_org_member_joins_existing_workspace_as_member(client, signed_in):
    client.post("/auth/logout")
    client.cookies.clear()
    r = client.post("/auth/dev", json={"email": "colleague@example.com", "organization": "MindsDB"})
    assert r.status_code == 200
    me = client.get("/api/me").json()
    assert [w["id"] for w in me["workspaces"]] == [signed_in["workspaces"][0]["id"]]


def test_unauthenticated_requests_are_refused(client):
    assert client.get("/api/me").status_code == 401


def test_api_key_sign_in_uses_mindshub_identity_and_stores_masked_key(client, monkeypatch):
    async def identity(bearer):
        assert bearer == "mdb_test123.supersecretvalue"
        return {"valid": True, "user_id": "u-1", "email": "lead@mindsdb.com", "organization_id": "org-1",
                "organization_name": "MindsDB", "workspace_id": "w-1", "roles": ["manage-organization"]}

    monkeypatch.setattr(auth_module, "mindshub_identity", identity)
    r = client.post("/auth/api-key", json={"key": "mdb_test123.supersecretvalue", "use_for_inference": True})
    assert r.status_code == 200, r.text
    me = client.get("/api/me").json()
    ws_id = me["workspaces"][0]["id"]
    creds = client.get(f"/api/workspaces/{ws_id}/credentials").json()
    mindshub = next(c for c in creds["items"] if c["kind"] == "mindshub_api_key")
    assert mindshub["configured"] and mindshub["status"] == "valid"
    assert "supersecret" not in json.dumps(creds)
    with session_scope() as s:
        stored = s.query(Secret).one()
        assert "supersecret" not in stored.ciphertext


def test_credentials_are_tested_masked_and_role_protected(client, signed_in, monkeypatch):
    ws_id = signed_in["workspaces"][0]["id"]

    async def fake_test(kind, value):
        return "valid", "Connected as tester", {"login": "tester"}

    monkeypatch.setattr(api_module, "_test_secret", fake_test)
    r = client.put(f"/api/workspaces/{ws_id}/credentials/github_token", json={"value": "ghp_" + "x" * 36})
    assert r.status_code == 200
    body = r.json()
    assert body["status"] == "valid" and body["hint"].startswith("ghp_xx") and "x" * 20 not in body["hint"]
    r = client.put(f"/api/workspaces/{ws_id}/credentials/mindshub_api_key", json={"value": "not-a-mindshub-key"})
    assert r.status_code == 400


def test_outcome_lifecycle_through_api(client, signed_in, monkeypatch):
    ws_id = signed_in["workspaces"][0]["id"]

    class FakeGH:
        def __init__(self, token):
            pass

        async def repo(self, name):
            return {"full_name": name, "default_branch": "main", "permissions": {"push": True}, "description": "demo"}

    async def fake_detect(gh, name, ref):
        return {"setup": ["npm ci"], "checks": ["npm test"], "smoke": ["npm test"], "detected": ["Node project (npm)"]}

    monkeypatch.setattr(api_module, "GitHub", FakeGH)
    monkeypatch.setattr(api_module, "resolve_token", lambda ws: "t")
    monkeypatch.setattr(api_module, "detect", fake_detect)
    repo = client.post(f"/api/workspaces/{ws_id}/repositories", json={"full_name": "https://github.com/acme/app"}).json()
    assert repo["full_name"] == "acme/app" and repo["config"]["checks"] == ["npm test"]
    r = client.post(f"/api/workspaces/{ws_id}/outcomes", json={
        "title": "Add CSV export", "brief": "Users can export entries as CSV", "acceptance": ["Download works"],
        "repo_ids": [repo["id"]]})
    assert r.status_code == 200, r.text
    outcome = r.json()
    assert outcome["number"] == 1 and outcome["status"] == "planning"
    detail = client.get(f"/api/workspaces/{ws_id}/outcomes/{outcome['id']}").json()
    assert detail["outcome"]["title"] == "Add CSV export"
    msg = client.post(f"/api/workspaces/{ws_id}/outcomes/{outcome['id']}/messages", json={"body": "Use UTF-8"})
    assert msg.status_code == 200
    assert client.post(f"/api/workspaces/{ws_id}/outcomes/{outcome['id']}/cancel").json()["ok"]
    listed = client.get(f"/api/workspaces/{ws_id}/outcomes").json()
    assert listed[0]["status"] == "cancelled"
    stats = client.get(f"/api/workspaces/{ws_id}/stats").json()
    assert stats["outcomes"] == {"cancelled": 1}


def test_settings_are_validated(client, signed_in):
    ws_id = signed_in["workspaces"][0]["id"]
    r = client.patch(f"/api/workspaces/{ws_id}", json={"settings": {"concurrency": 500, "harness": {"builder": "evil"},
                                                                    "autonomy": {"merge": "approval"}}})
    assert r.status_code == 200
    settings = r.json()["settings"]
    assert settings["concurrency"] == 50 and settings["harness"]["builder"] == "native"
    assert settings["autonomy"]["merge"] == "approval" and settings["autonomy"]["plan"] == "auto"


def test_gateway_pins_model_enforces_budget_and_records_usage(client, signed_in, monkeypatch):
    ws_id = signed_in["workspaces"][0]["id"]
    with session_scope() as s:
        from aifactory.models import Outcome

        o = Outcome(workspace_id=ws_id, number=1, title="t", repo_ids=[])
        s.add(o)
        s.flush()
        run = Run(workspace_id=ws_id, outcome_id=o.id, role="builder", harness="codex", model="gpt-terra")
        s.add(run)
        s.flush()
        run_id = run.id
    seen = {}

    def upstream(request: httpx.Request) -> httpx.Response:
        seen["auth"] = request.headers["authorization"]
        seen["body"] = json.loads(request.content)
        return httpx.Response(200, json={"id": "r", "output": [], "usage": {"input_tokens": 120, "output_tokens": 30}})

    real_client = httpx.AsyncClient
    monkeypatch.setattr(gateway.httpx, "AsyncClient",
                        lambda **kw: real_client(transport=httpx.MockTransport(upstream), **kw))
    ledger = UsageLedger(run_id=run_id, workspace_settings={}, max_tokens=200)
    token = gateway.issue(gateway.GatewayGrant(run_id=run_id, workspace_id=ws_id, model="gpt-terra",
                                               provider=ProviderConfig("mindshub", "https://api.test/v1", "mdb_real.key",
                                                                       "MindsHub"), ledger=ledger))
    headers = {"Authorization": f"Bearer {token}"}
    assert client.post("/gateway/v1/responses", json={"model": "other", "input": "x"}, headers=headers).status_code == 400
    r = client.post("/gateway/v1/responses", json={"model": "gpt-terra", "input": "x", "client_metadata": {"a": 1}},
                    headers=headers)
    assert r.status_code == 200, r.text
    assert seen["auth"] == "Bearer mdb_real.key" and "client_metadata" not in seen["body"]
    assert ledger.input_tokens == 120 and ledger.output_tokens == 30
    r = client.post("/gateway/v1/responses", json={"model": "gpt-terra", "input": "x"}, headers=headers)
    assert r.status_code == 200
    r = client.post("/gateway/v1/responses", json={"model": "gpt-terra", "input": "x"}, headers=headers)
    assert r.status_code == 400 and r.json()["error"]["code"] == "budget_exhausted"
    gateway.revoke(run_id)
    assert client.post("/gateway/v1/responses", json={"input": "x"}, headers=headers).status_code == 401
    assert client.post("/gateway/v1/responses", json={"input": "x"}, headers={"Authorization": "Bearer nope"}).status_code == 401


def test_ci_checks_fall_back_to_actions_when_checks_api_is_forbidden(monkeypatch):
    from aifactory.github import GitHub, GitHubError

    async def fake_request(self, method, path, **kwargs):
        if path.endswith("/check-runs"):
            raise GitHubError("Resource not accessible by personal access token", 403)
        if path.endswith("/status"):
            return {"statuses": [{"context": "ai-factory/verification", "state": "success"}]}
        if path.endswith("/actions/runs"):
            return {"workflow_runs": [{"id": 9, "name": "Validate", "status": "completed", "conclusion": "success"}]}
        if path.endswith("/jobs"):
            return {"jobs": [{"id": 1, "name": "validate", "status": "completed", "conclusion": "success", "html_url": "u"},
                             {"id": 2, "name": "sandbox", "status": "in_progress", "conclusion": None, "html_url": "u"}]}
        raise AssertionError(path)

    monkeypatch.setattr(GitHub, "request", fake_request)
    summary = asyncio.run(GitHub("t").checks("o/r", "abc"))
    assert summary.state == "pending" and [c["name"] for c in summary.checks] == ["validate", "sandbox"]


def test_github_credential_save_reenables_acknowledgements_and_rejects_stale_failure(client, signed_in, monkeypatch):
    ws_id = signed_in["workspaces"][0]["id"]
    endpoint = f"/api/workspaces/{ws_id}/credentials"

    async def fake_test(kind, value):
        return ("untested" if value.endswith("new") else "valid"), "", {}

    monkeypatch.setattr(api_module, "_test_secret", fake_test)
    assert client.put(f"{endpoint}/github_token", json={"value": "old-token"}).status_code == 200
    with session_scope() as s:
        s.add(Repository(workspace_id=ws_id, full_name="acme/app", default_branch="main", config={}))
    issue = {"number": 1}
    attempts = []

    class FakeGH:
        def __init__(self, token):
            self.token = token

        async def issues(self, full_name, labels="", since=None):
            number = issue["number"]
            return [{"number": number, "title": str(number), "html_url": f"https://gh/{number}"}] if labels == "factory:auto" else []

        async def comment(self, full_name, number, body):
            attempts.append((self.token, number))
            if number == 1:
                raise GitHubError("forbidden", 403)

        async def request(self, *args, **kwargs):
            return {}

    monkeypatch.setattr(signals_module, "GitHub", FakeGH)
    assert client.post(f"/api/workspaces/{ws_id}/signals/sync").json()["accepted"] == 1
    assert client.put(f"{endpoint}/openai_api_key", json={"value": "other-key"}).status_code == 200
    issue["number"] = 2
    assert client.post(f"/api/workspaces/{ws_id}/signals/sync").json()["accepted"] == 1
    assert attempts == [("old-token", 1)]
    _, old_generation = resolve_token_with_generation(ws_id)
    response = client.put(f"{endpoint}/github_token", json={"value": "token-new"})
    assert response.status_code == 200 and response.json()["status"] == "untested"
    with SessionLocal() as db:
        signals_module._disable_acknowledgements(db, ws_id, old_generation)
        signals_module._disable_acknowledgements(db, ws_id, old_generation)
    issue["number"] = 3
    assert client.post(f"/api/workspaces/{ws_id}/signals/sync").json()["accepted"] == 1
    assert attempts == [("old-token", 1), ("token-new", 3)]
    with session_scope() as s:
        state = s.get(IssueAcknowledgementState, ws_id)
        assert not state.disabled and state.generation == old_generation + 1
        assert s.query(Event).filter_by(workspace_id=ws_id, kind="issue_acknowledgements_disabled").count() == 1


def test_concurrent_failures_only_emit_one_warning(client, signed_in):
    ws_id = signed_in["workspaces"][0]["id"]
    from concurrent.futures import ThreadPoolExecutor

    def fail():
        with SessionLocal() as db:
            signals_module._disable_acknowledgements(db, ws_id, 0)

    with ThreadPoolExecutor(max_workers=4) as pool:
        list(pool.map(lambda _: fail(), range(8)))
    with session_scope() as s:
        assert s.get(IssueAcknowledgementState, ws_id).disabled
        assert s.query(Event).filter_by(workspace_id=ws_id, kind="issue_acknowledgements_disabled").count() == 1


def test_inflight_old_credential_403_does_not_disable_replacement(client, signed_in, monkeypatch):
    ws_id = signed_in["workspaces"][0]["id"]
    endpoint = f"/api/workspaces/{ws_id}/credentials/github_token"

    async def fake_test(kind, value):
        return "valid", "", {}

    monkeypatch.setattr(api_module, "_test_secret", fake_test)
    assert client.put(endpoint, json={"value": "old-token"}).status_code == 200
    with session_scope() as s:
        s.add(Repository(workspace_id=ws_id, full_name="acme/app", default_branch="main", config={}))
    old_token, old_generation = resolve_token_with_generation(ws_id)
    attempts = []

    class FakeGH:
        def __init__(self, token):
            self.token = token

        async def issues(self, full_name, labels="", since=None):
            return [{"number": 1, "title": "First", "html_url": "https://gh/1"},
                    {"number": 2, "title": "Second", "html_url": "https://gh/2"}] if labels == "factory:auto" else []

        async def comment(self, full_name, number, body):
            attempts.append((self.token, number))
            if self.token == "old-token":
                assert client.put(endpoint, json={"value": "new-token"}).status_code == 200
                raise GitHubError("forbidden", 403)

        async def request(self, *args, **kwargs):
            return {}

    monkeypatch.setattr(signals_module, "GitHub", FakeGH)
    with SessionLocal() as db:
        result = asyncio.run(signals_module.sync_github(db, ws_id, old_token, generation=old_generation))
    assert result["accepted"] == 2 and attempts == [("old-token", 1), ("new-token", 1), ("new-token", 2)]
    with session_scope() as s:
        assert not s.get(IssueAcknowledgementState, ws_id).disabled
        assert s.query(Event).filter_by(workspace_id=ws_id, kind="issue_acknowledgements_disabled").count() == 0


def test_credential_replacement_after_sync_snapshot_uses_new_token(client, signed_in, monkeypatch):
    ws_id = signed_in["workspaces"][0]["id"]
    endpoint = f"/api/workspaces/{ws_id}/credentials/github_token"

    async def fake_test(kind, value):
        return "valid", "", {}

    monkeypatch.setattr(api_module, "_test_secret", fake_test)
    assert client.put(endpoint, json={"value": "old-token"}).status_code == 200
    with session_scope() as s:
        s.add(Repository(workspace_id=ws_id, full_name="acme/app", default_branch="main", config={}))
    attempts = []

    class FakeGH:
        def __init__(self, token):
            self.token = token

        async def issues(self, full_name, labels="", since=None):
            return [{"number": 1, "title": "First", "html_url": "https://gh/1"}] if labels == "factory:auto" else []

        async def comment(self, full_name, number, body):
            attempts.append((self.token, number))

        async def request(self, *args, **kwargs):
            return {}

    def replace_after_snapshot(workspace_id):
        from fastapi.testclient import TestClient

        snapshot = resolve_token_with_generation(workspace_id)
        with TestClient(client.app, cookies=client.cookies) as replacement_client:
            assert replacement_client.put(endpoint, json={"value": "new-token"}).status_code == 200
        return snapshot

    monkeypatch.setattr(signals_module, "GitHub", FakeGH)
    monkeypatch.setattr(api_module, "resolve_token_with_generation", replace_after_snapshot)
    assert client.post(f"/api/workspaces/{ws_id}/signals/sync").json()["accepted"] == 1
    assert attempts == [("new-token", 1)]


def test_invalid_github_save_reenables_but_test_and_delete_do_not(client, signed_in, monkeypatch):
    ws_id = signed_in["workspaces"][0]["id"]
    endpoint = f"/api/workspaces/{ws_id}/credentials/github_token"

    async def fake_test(kind, value):
        return "invalid", "invalid token", {}

    monkeypatch.setattr(api_module, "_test_secret", fake_test)
    assert client.put(endpoint, json={"value": "bad-token"}).status_code == 200
    with SessionLocal() as db:
        signals_module._disable_acknowledgements(db, ws_id, 1)
    assert client.post(f"{endpoint}/test").status_code == 200
    with session_scope() as s:
        assert s.get(IssueAcknowledgementState, ws_id).disabled
    assert client.put(endpoint, json={"value": "bad-token-again"}).json()["status"] == "invalid"
    with session_scope() as s:
        assert not s.get(IssueAcknowledgementState, ws_id).disabled
    with SessionLocal() as db:
        signals_module._disable_acknowledgements(db, ws_id, 2)
    assert client.delete(endpoint).status_code == 200
    with session_scope() as s:
        assert s.get(IssueAcknowledgementState, ws_id).disabled
