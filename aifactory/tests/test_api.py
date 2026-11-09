from __future__ import annotations

import asyncio
import json

import httpx
import pytest

from aifactory import api as api_module
from aifactory import auth as auth_module
from aifactory import gateway
from aifactory.db import session_scope
from aifactory.inference import ProviderConfig, UsageLedger
from aifactory.models import Run, Secret, Workspace


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

    async def catalog(self):
        return ["test-model"]

    monkeypatch.setattr(api_module.ModelClient, "list_models", catalog)
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


@pytest.mark.parametrize("value,expected", [(None, 2), ("6", 6), (0, 1), (-5, 1), (500, 50), ("bad", 2), ([], 2)])
def test_planning_settings_default_and_normalization(client, signed_in, value, expected):
    ws = signed_in["workspaces"][0]
    assert ws["settings"]["planning_concurrency"] == 2
    response = client.patch(f"/api/workspaces/{ws['id']}", json={"settings": {"planning_concurrency": value}})
    assert response.status_code == 200, response.text
    assert response.json()["settings"]["planning_concurrency"] == expected
    saved = client.get("/api/me").json()["workspaces"][0]["settings"]
    assert saved["planning_concurrency"] == expected
    assert saved["concurrency"] == 3


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


def _mock_image_provider(monkeypatch, handler):
    from aifactory import model_checks

    monkeypatch.setattr(model_checks, "resolve_provider", lambda _: ProviderConfig(
        "mindshub", "https://provider.test/v1", "mdb_secret-never-expose", "MindsHub"))
    real_client = httpx.AsyncClient
    monkeypatch.setattr(httpx, "AsyncClient", lambda **kw: real_client(
        transport=httpx.MockTransport(handler), **kw))


def test_model_check_sends_valid_png_and_deduplicates_inherited_roles(client, signed_in, monkeypatch):
    import base64
    import struct
    import zlib

    ws_id = signed_in["workspaces"][0]["id"]
    client.patch(f"/api/workspaces/{ws_id}", json={"settings": {"models": {
        "default": "default-model", "builder": "other-model"}}})
    seen = []

    def upstream(request):
        assert request.url.path == "/v1/responses"
        body = json.loads(request.content)
        seen.append(body["model"])
        assert body["max_output_tokens"] == 32 and not body["store"]
        image = body["input"][0]["content"][1]
        assert image["type"] == "input_image"
        prefix, encoded = image["image_url"].split(",")
        assert prefix == "data:image/png;base64"
        png = base64.b64decode(encoded, validate=True)
        assert png[:8] == b"\x89PNG\r\n\x1a\n" and len(png) < 100
        offset = 8
        chunks = {}
        while offset < len(png):
            size = struct.unpack("!I", png[offset:offset + 4])[0]
            kind = png[offset + 4:offset + 8]
            data = png[offset + 8:offset + 8 + size]
            assert zlib.crc32(kind + data) == struct.unpack("!I", png[offset + 8 + size:offset + 12 + size])[0]
            chunks[kind] = data
            offset += 12 + size
        assert struct.unpack("!2I5B", chunks[b"IHDR"]) == (1, 1, 8, 2, 0, 0, 0)
        assert zlib.decompress(chunks[b"IDAT"]) == b"\0\0\0\0"
        assert chunks[b"IEND"] == b""
        return httpx.Response(200, json={"output": []})

    _mock_image_provider(monkeypatch, upstream)
    r = client.post(f"/api/workspaces/{ws_id}/models/check")
    assert r.status_code == 200, r.text
    assert sorted(seen) == ["default-model", "other-model"]
    roles = r.json()["roles"]
    assert roles["planner"]["model"] == roles["verifier"]["model"] == "default-model"
    assert roles["builder"]["model"] == "other-model"
    assert {role["status"] for role in roles.values()} == {"supported"}


@pytest.mark.parametrize(("status", "message", "expected"), [
    (400, "This model does not support image input", "unsupported"),
    (400, "This model does not support images", "unsupported"),
    (422, "Vision is not supported", "unsupported"),
    (400, "This model does not support image input; use a supported image format", "unsupported"),
    (400, "This model does not support image_url; use a supported image format", "unsupported"),
    (400, "This model does not support image input format TIFF", "unknown"),
    (400, "This model does not support image input encoding base64", "unknown"),
    (400, "This model does not support image input MIME type image/tiff", "unknown"),
    (400, "This model does not support image input size: too large", "unknown"),
    (400, "Unsupported image format", "unknown"),
    (400, "TIFF images are not supported", "unknown"),
    (400, "Base64 image input is not supported", "unknown"),
    (400, "Data URL images are not supported", "unknown"),
    (400, "Data URI images are not supported", "unknown"),
    (400, "data URIs are not allowed", "unknown"),
    (400, "data URLs are not allowed", "unknown"),
    (400, "Inline images are not supported", "unknown"),
    (400, "Inline image input is not supported", "unknown"),
    (400, "Base64 images are not supported", "unknown"),
    (400, "Data-URL images are not supported", "unknown"),
    (400, "Provision is not supported", "unknown"),
    (400, "This model does not support image input. Please do not include images.", "unsupported"),
    (401, "This model does not support image input", "unknown"),
    (403, "Invalid credential", "unknown"),
    (404, "Model unavailable", "unknown"),
    (429, "Rate limited", "unknown"),
    (500, "Provider error", "unknown"),
])
def test_model_check_classifies_provider_errors_safely(client, signed_in, monkeypatch, caplog,
                                                       status, message, expected):
    seen = []

    def upstream(request):
        seen.append(request)
        return httpx.Response(status, json={"error": {"message": message + "; mdb_secret-never-expose"}})

    _mock_image_provider(monkeypatch, upstream)
    ws_id = signed_in["workspaces"][0]["id"]
    r = client.post(f"/api/workspaces/{ws_id}/models/check")
    assert r.status_code == 200
    assert {role["status"] for role in r.json()["roles"].values()} == {expected}
    assert len(seen) == 1  # no retries, including include-removal, or text-only fallback
    assert "mdb_secret-never-expose" not in r.text + caplog.text
    assert message not in r.text


@pytest.mark.parametrize("error", [httpx.ReadTimeout, httpx.ConnectError])
def test_model_check_transport_failures_are_unknown(client, signed_in, monkeypatch, error):
    def upstream(request):
        raise error("private provider detail mdb_secret-never-expose", request=request)

    _mock_image_provider(monkeypatch, upstream)
    ws_id = signed_in["workspaces"][0]["id"]
    r = client.post(f"/api/workspaces/{ws_id}/models/check")
    assert {role["status"] for role in r.json()["roles"].values()} == {"unknown"}
    assert "private" not in r.text and "mdb_secret" not in r.text


def test_model_check_missing_credentials_and_builtin_default(client, signed_in, monkeypatch):
    def no_network(*args, **kwargs):
        raise AssertionError("No request should be made without credentials")

    monkeypatch.setattr(httpx, "AsyncClient", no_network)
    ws_id = signed_in["workspaces"][0]["id"]
    r = client.post(f"/api/workspaces/{ws_id}/models/check")
    assert r.status_code == 200
    assert r.json()["provider"] is None
    assert {role["model"] for role in r.json()["roles"].values()} == {"gpt-terra"}
    assert {role["status"] for role in r.json()["roles"].values()} == {"unknown"}


def test_model_check_bootstrap_override_matches_runtime(client, signed_in, monkeypatch):
    from aifactory.config import reset_settings_cache

    monkeypatch.setenv("AIFACTORY_BOOTSTRAP_OPENAI_API_KEY", "sk-bootstrap-secret")
    monkeypatch.setenv("AIFACTORY_BOOTSTRAP_OPENAI_MODEL", "bootstrap-model")
    reset_settings_cache()
    ws_id = signed_in["workspaces"][0]["id"]
    client.patch(f"/api/workspaces/{ws_id}", json={"settings": {"models": {
        "planner": "a", "builder": "b", "verifier": "c"}}})
    seen = []

    def upstream(request):
        seen.append(json.loads(request.content)["model"])
        assert request.headers["authorization"] == "Bearer sk-bootstrap-secret"
        return httpx.Response(200, json={"output": []})

    real_client = httpx.AsyncClient
    monkeypatch.setattr(httpx, "AsyncClient", lambda **kw: real_client(transport=httpx.MockTransport(upstream), **kw))
    r = client.post(f"/api/workspaces/{ws_id}/models/check")
    assert r.status_code == 200 and r.json()["provider"] == "openai"
    assert seen == ["bootstrap-model"]
    assert {role["model"] for role in r.json()["roles"].values()} == {"bootstrap-model"}


def test_model_client_exhausted_include_retry_preserves_provider_error(monkeypatch):
    from aifactory.inference import InferenceError, ModelClient

    message = "This model does not support image input. Please do not include images."

    def upstream(request):
        return httpx.Response(400, json={"error": {"message": message}})

    _mock_image_provider(monkeypatch, upstream)
    client = ModelClient(ProviderConfig("openai", "https://provider.test/v1", "key", "OpenAI"), max_retries=0)
    with pytest.raises(InferenceError, match=message) as error:
        asyncio.run(client._post("/responses", {"include": ["reasoning.encrypted_content"]}))
    assert error.value.status == 400


def test_model_catalog_never_probes_selected_models(client, signed_in, monkeypatch):
    from aifactory import model_checks

    async def forbidden(*args, **kwargs):
        raise AssertionError("GET must not send paid image probes")

    async def catalog(self):
        return ["catalog-model"]

    monkeypatch.setattr(model_checks.ModelClient, "_post", forbidden)
    monkeypatch.setattr(api_module.ModelClient, "list_models", catalog)
    monkeypatch.setattr(api_module, "resolve_provider", lambda _: ProviderConfig(
        "openai", "https://provider.test/v1", "key", "OpenAI"))
    ws_id = signed_in["workspaces"][0]["id"]
    r = client.get(f"/api/workspaces/{ws_id}/models")
    assert r.status_code == 200 and r.json()["models"] == ["catalog-model"]


def test_model_checks_report_distinct_results_independently(client, signed_in, monkeypatch):
    ws_id = signed_in["workspaces"][0]["id"]
    client.patch(f"/api/workspaces/{ws_id}", json={"settings": {"models": {
        "planner": "accepts", "builder": "rejects", "verifier": "unavailable"}}})
    seen = []

    def upstream(request):
        model = json.loads(request.content)["model"]
        seen.append(model)
        if model == "accepts":
            return httpx.Response(200, json={"output": []})
        return httpx.Response(400 if model == "rejects" else 503,
                              json={"error": {"message": "This model does not support images"}})

    _mock_image_provider(monkeypatch, upstream)
    roles = client.post(f"/api/workspaces/{ws_id}/models/check").json()["roles"]
    assert len(seen) == 3
    assert [roles[role]["status"] for role in ("planner", "builder", "verifier")] == [
        "supported", "unsupported", "unknown"]


def test_model_check_overall_timeout_is_bounded(client, signed_in, monkeypatch):
    from aifactory import model_checks

    async def blocked(self, path, body):
        assert self.timeout == 0.01 and self.max_retries == 0
        assert self.log_errors is False
        await asyncio.sleep(60)

    _mock_image_provider(monkeypatch, lambda request: pytest.fail("No real network"))
    monkeypatch.setattr(model_checks, "CHECK_TIMEOUT", 0.01)
    monkeypatch.setattr(model_checks.ModelClient, "_post", blocked)
    ws_id = signed_in["workspaces"][0]["id"]
    r = client.post(f"/api/workspaces/{ws_id}/models/check")
    assert r.status_code == 200
    assert {role["status"] for role in r.json()["roles"].values()} == {"unknown"}


def test_workspace_admin_can_explicitly_check_models(client, signed_in, monkeypatch):
    from aifactory.models import Membership

    ws_id = signed_in["workspaces"][0]["id"]
    with session_scope() as s:
        s.query(Membership).filter(Membership.workspace_id == ws_id).one().role = "admin"
    _mock_image_provider(monkeypatch, lambda request: httpx.Response(200, json={"output": []}))
    r = client.post(f"/api/workspaces/{ws_id}/models/check")
    assert r.status_code == 200
    assert {role["status"] for role in r.json()["roles"].values()} == {"supported"}


def test_parallel_delivery_settings_survive_unrelated_edits(client, signed_in):
    from aifactory.models import Repository

    ws_id = signed_in["workspaces"][0]["id"]
    path = f"/api/workspaces/{ws_id}"
    response = client.patch(path, json={"settings": {"planning_concurrency": 3}})
    assert response.status_code == 200
    assert response.json()["settings"]["planning_concurrency"] == 3
    response = client.patch(path, json={"settings": {"concurrency": 4}})
    assert response.status_code == 200
    assert response.json()["settings"]["planning_concurrency"] == 3
    assert client.get("/api/me").json()["workspaces"][0]["settings"]["planning_concurrency"] == 3

    with session_scope() as db:
        repo = Repository(workspace_id=ws_id, full_name="acme/app", config={})
        db.add(repo)
        db.flush()
        repo_id = repo.id
    repo_path = f"{path}/repositories/{repo_id}"
    for mode in ("overlap", "always", "never"):
        merge = {"mode": "approval", "method": "merge", "revalidate": False, "reverify": mode}
        response = client.patch(repo_path, json={"config": {"merge": merge}})
        assert response.status_code == 200
        assert response.json()["config"]["merge"] == merge
        response = client.patch(repo_path, json={"config": {"context": "Updated team notes"}})
        assert response.status_code == 200
        assert response.json()["config"]["merge"] == merge
        assert client.get(f"{path}/repositories").json()[0]["config"]["merge"] == merge

    client.post("/auth/logout")
    client.cookies.clear()
    client.post("/auth/dev", json={"email": "member@example.com", "organization": "MindsDB"})
    from aifactory.models import Membership

    member_id = client.get("/api/me").json()["user"]["id"]
    with session_scope() as db:
        db.query(Membership).filter_by(workspace_id=ws_id, user_id=member_id).one().role = "member"
    assert client.patch(path, json={"settings": {"planning_concurrency": 5}}).status_code == 403
    assert client.patch(repo_path, json={"config": {"merge": {"reverify": "never"}}}).status_code == 403


def test_waiting_ci_is_serialized_in_task_cards_and_outcome_tracks(client, signed_in):
    from aifactory.models import Outcome, Plan, Repository, Task

    ws_id = signed_in["workspaces"][0]["id"]
    with session_scope() as db:
        repo = Repository(workspace_id=ws_id, full_name="acme/app")
        outcome = Outcome(workspace_id=ws_id, number=1, title="Parked delivery", status="running")
        db.add_all([repo, outcome])
        db.flush()
        plan = Plan(outcome_id=outcome.id)
        db.add(plan)
        db.flush()
        db.add(Task(workspace_id=ws_id, outcome_id=outcome.id, repo_id=repo.id, plan_id=plan.id,
                    key="T1", title="Waiting", status="waiting_ci", stage="ci",
                    evidence={"ci": {"state": "pending"}}))
        outcome_id = outcome.id
    path = f"/api/workspaces/{ws_id}/outcomes"
    detail = client.get(f"{path}/{outcome_id}")
    assert detail.status_code == 200
    task = detail.json()["tasks"][0]
    assert (task["status"], task["stage"], task["evidence"]["ci"]["state"]) == ("waiting_ci", "ci", "pending")
    listing = client.get(path)
    assert listing.status_code == 200
    outcome = listing.json()[0]
    # A parked CI wait has started but is not running (it holds no slot).
    assert outcome["progress"] == {"done": 0, "total": 1, "started": 1, "active": 0}
    assert outcome["track"][0] == {
        "key": "T1", "status": "waiting_ci", "stage": "ci", "evidence": {"ci": {"state": "pending"}},
    }


def test_saved_workspace_planning_limit_controls_dispatch(client, signed_in, monkeypatch):
    """The editable API setting limits actual planners, not just serialized settings."""
    from aifactory.models import Outcome
    from aifactory.pipeline.orchestrator import Orchestrator

    ws_id = signed_in["workspaces"][0]["id"]
    assert client.patch(f"/api/workspaces/{ws_id}", json={"settings": {"planning_concurrency": 1}}).status_code == 200
    with session_scope() as db:
        db.add_all(Outcome(workspace_id=ws_id, number=n, title=f"Plan {n}", status="planning")
                   for n in range(1, 4))
    orch = Orchestrator()
    started = []

    def spawn(outcome_id):
        started.append(outcome_id)
        # Dispatch accounts for an admitted job immediately, before the next candidate.
        orch._planning[outcome_id] = (None, None)

    monkeypatch.setattr(orch, "_spawn_plan", spawn)
    orch._schedule()
    assert len(started) == 1
    orch._schedule()
    assert len(started) == 1
    assert client.patch(f"/api/workspaces/{ws_id}", json={"settings": {"planning_concurrency": 2}}).status_code == 200
    orch._schedule()
    assert len(started) == 2


def test_meta_reverify_capability_is_optional_and_explicit(client):
    """Old servers omit the flag; supporting servers must send a boolean, not a label."""
    response = client.get("/api/meta")
    assert response.status_code == 200
    capabilities = response.json().get("capabilities", {})
    if "reverify" in capabilities:
        assert type(capabilities["reverify"]) is bool
