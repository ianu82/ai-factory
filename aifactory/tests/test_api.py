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
    assert ws["settings"]["autonomy"] == {"plan": "approval", "merge": "approval", "release": "approval"}
    assert ws["settings"]["learning"]["preset"] == {"plan": "auto", "merge": "auto", "release": "approval"}


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
    assert settings["autonomy"]["merge"] == "approval" and settings["autonomy"]["plan"] == "approval"


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
    assert sorted(seen) == ["default-model", "opus-5-5", "other-model"]
    roles = r.json()["roles"]
    assert roles["planner"]["model"] == roles["verifier"]["model"] == "default-model"
    assert roles["builder"]["model"] == "other-model"
    assert roles["plan_reviewer"]["model"] == "opus-5-5"
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
    # One probe per distinct model (the default and the plan reviewer's), with no retries,
    # include-removal, or text-only fallback.
    assert len(seen) == 2
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
    assert {role: result["model"] for role, result in r.json()["roles"].items()} == {
        "planner": "gpt-terra", "plan_reviewer": "opus-5-5", "builder": "gpt-terra", "verifier": "gpt-terra"}
    assert {role["status"] for role in r.json()["roles"].values()} == {"unknown"}


def test_model_check_bootstrap_override_matches_runtime(client, signed_in, monkeypatch):
    from aifactory.config import reset_settings_cache

    monkeypatch.setenv("AIFACTORY_BOOTSTRAP_OPENAI_API_KEY", "sk-bootstrap-secret")
    monkeypatch.setenv("AIFACTORY_BOOTSTRAP_OPENAI_MODEL", "bootstrap-model")
    reset_settings_cache()
    ws_id = signed_in["workspaces"][0]["id"]
    from aifactory.fallbacks import set_bootstrap_workspace

    set_bootstrap_workspace(ws_id)  # server credentials apply to the operator's bootstrap workspace only
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
        "planner": "accepts", "plan_reviewer": "accepts", "builder": "rejects", "verifier": "unavailable"}}})
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
    assert [roles[role]["status"] for role in ("planner", "plan_reviewer", "builder", "verifier")] == [
        "supported", "supported", "unsupported", "unknown"]


def test_model_check_picks_the_plan_reviewer_default_for_the_resolved_provider(client, signed_in, monkeypatch):
    """A MindsHub workspace whose only key is OpenAI's runs on OpenAI, so its reviewer default is the verifier's."""
    from aifactory import model_checks

    ws_id = signed_in["workspaces"][0]["id"]
    client.patch(f"/api/workspaces/{ws_id}", json={"settings": {"provider": "mindshub", "models": {
        "default": "default-model", "verifier": "verifier-model"}}})
    seen = []

    def upstream(request):
        seen.append(json.loads(request.content)["model"])
        return httpx.Response(200, json={"output": []})

    _mock_image_provider(monkeypatch, upstream)
    monkeypatch.setattr(model_checks, "resolve_provider", lambda _: ProviderConfig(
        "openai", "https://provider.test/v1", "key", "OpenAI"))
    r = client.post(f"/api/workspaces/{ws_id}/models/check")
    assert r.status_code == 200 and r.json()["provider"] == "openai"
    assert sorted(seen) == ["default-model", "verifier-model"]
    assert r.json()["roles"]["plan_reviewer"] == {
        "model": "verifier-model", "status": "supported", "explanation": "The model accepted image input."}


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


def test_runs_config_version_filter_is_exact_scoped_and_before_limit(client, signed_in):
    from datetime import timedelta

    from aifactory.db import utcnow
    from aifactory.models import Outcome

    ws_id = signed_in["workspaces"][0]["id"]
    with session_scope() as s:
        ws = s.get(Workspace, ws_id)
        other = Workspace(org_id=ws.org_id, name="Private workspace", slug="private")
        s.add(other)
        s.flush()
        o = Outcome(workspace_id=ws_id, number=1, title="Our outcome")
        theirs = Outcome(workspace_id=other.id, number=1, title="Private outcome")
        s.add_all([o, theirs])
        s.flush()
        now = utcnow()
        matching = Run(workspace_id=ws_id, outcome_id=o.id, role="builder", harness="native",
                       config_version="abc123", started_at=now - timedelta(minutes=3))
        s.add_all([matching,
                   Run(workspace_id=ws_id, outcome_id=o.id, role="planner", harness="native",
                       config_version="abc1234", started_at=now),
                   Run(workspace_id=ws_id, outcome_id=o.id, role="verifier", harness="native",
                       config_version=None, started_at=now - timedelta(minutes=1)),
                   Run(workspace_id=other.id, outcome_id=theirs.id, role="builder", harness="native",
                       config_version="abc123", started_at=now)])
        s.flush()
        expected = matching.id
    response = client.get(f"/api/workspaces/{ws_id}/runs?config_version=abc123&limit=1")
    assert response.status_code == 200
    assert [r["id"] for r in response.json()] == [expected]
    assert response.json()[0]["config_version"] == "abc123"
    assert client.get(f"/api/workspaces/{ws_id}/runs?config_version=abc").json() == []
    assert client.get(f"/api/workspaces/{ws_id}/runs?config_version=").json() == []
    assert any(r["config_version"] is None for r in client.get(f"/api/workspaces/{ws_id}/runs").json())
    client.post("/auth/logout")
    client.cookies.clear()
    client.post("/auth/dev", json={"email": "outsider@example.com", "organization": "Other org"})
    assert client.get(f"/api/workspaces/{ws_id}/runs?config_version=abc123").status_code == 404


def test_created_workspace_enters_learning(client, signed_in):
    org = signed_in["organizations"][0]["id"]
    response = client.post("/api/workspaces", json={"org_id": org, "name": "New team"})
    assert response.status_code == 200
    ws = response.json()
    assert ws["settings"]["autonomy"]["plan"] == ws["settings"]["autonomy"]["merge"] == "approval"
    assert ws["settings"]["learning"]["preset"]["merge"] == "auto"
    assert client.get(f"/api/workspaces/{ws['id']}/onboarding/policy").json()["eligible"] is False


@pytest.mark.parametrize("state", [{"status": "applied"}, None, {"preset": {"merge": "auto"}}])
def test_generic_settings_cannot_modify_learning_lifecycle(client, signed_in, state):
    ws_id = signed_in["workspaces"][0]["id"]
    before = client.get(f"/api/workspaces/{ws_id}/onboarding/policy").json()
    assert client.patch(f"/api/workspaces/{ws_id}", json={"settings": {"learning": state}}).status_code == 422
    assert client.get(f"/api/workspaces/{ws_id}/onboarding/policy").json() == before


def test_partial_merge_settings_preserve_all_keys_and_advertise_capability(client, signed_in):
    from aifactory.models import Repository

    ws_id = signed_in["workspaces"][0]["id"]
    merge = {"mode": "approval", "method": "merge", "revalidate": False, "future_key": "preserved"}
    with session_scope() as db:
        repo = Repository(workspace_id=ws_id, full_name="acme/app", config={"merge": merge})
        db.add(repo)
        db.flush()
        repo_id = repo.id
    path = f"/api/workspaces/{ws_id}/repositories/{repo_id}"
    response = client.patch(path, json={"config": {"merge": {"reverify": "always"}}})
    assert response.status_code == 200
    expected = {**merge, "reverify": "always"}
    assert response.json()["config"]["merge"] == expected
    # Same payload shape as the Settings form, including retained merge keys.
    response = client.patch(path, json={"config": {"context": "New notes", "merge": expected}})
    assert response.status_code == 200 and response.json()["config"]["merge"] == expected
    response = client.patch(path, json={"config": {"merge": {"reverify": "invalid"}}})
    assert response.status_code == 422
    assert client.get("/api/meta").json()["capabilities"]["reverify"] is True


@pytest.mark.parametrize("mode", ["off", "all", "labelled"])
def test_repository_review_policy_roundtrip(client, signed_in, mode):
    from aifactory.models import Repository

    wid = signed_in["workspaces"][0]["id"]
    existing = {"checks": ["pytest"], "custom": {"keep": True}, "review": {"max_rounds": 4}}
    with session_scope() as db:
        repo = Repository(workspace_id=wid, full_name="acme/reviews", config=existing)
        db.add(repo)
        db.flush()
        rid = repo.id
    path = f"/api/workspaces/{wid}/repositories"
    assert client.get(path).json()[0]["config"]["review"] == {
        "mode": "off", "allow_untrusted_local": False, "max_rounds": 4}
    response = client.patch(f"{path}/{rid}", json={"config": {"review": {"mode": mode}}})
    assert response.status_code == 200, response.text
    assert response.json()["config"] == {**existing, "steward": {"mode": "propose", "fix_minutes": 60},
                                         "review": {"mode": mode, "allow_untrusted_local": False, "max_rounds": 4}}
    response = client.patch(f"{path}/{rid}", json={"config": {"review": {"allow_untrusted_local": True}}})
    assert response.status_code == 200
    policy = {"mode": mode, "allow_untrusted_local": True, "max_rounds": 4}
    assert client.get(path).json()[0]["config"]["review"] == policy
    with session_scope() as db:
        assert db.get(Repository, rid).config == {**existing, "review": policy}


@pytest.mark.parametrize("review", [None, [], "all", {"mode": "on"}, {"mode": None}, {"mode": True},
                                    {"allow_untrusted_local": "false"}, {"allow_untrusted_local": 1},
                                    {"allow_untrusted_local": None}, {"unknown": True}])
def test_repository_review_policy_rejects_invalid_atomically(client, signed_in, review):
    from aifactory.models import Repository

    wid = signed_in["workspaces"][0]["id"]
    existing = {"checks": ["pytest"], "review": {"mode": "labelled"}, "custom": "keep"}
    with session_scope() as db:
        repo = Repository(workspace_id=wid, full_name="acme/reviews", config=existing)
        db.add(repo)
        db.flush()
        rid = repo.id
    response = client.patch(f"/api/workspaces/{wid}/repositories/{rid}",
                            json={"config": {"checks": ["bad"], "review": review}})
    assert response.status_code == 422, response.text
    with session_scope() as db:
        assert db.get(Repository, rid).config == existing


@pytest.mark.parametrize("role,expected", [("owner", 200), ("admin", 200), ("member", 403), ("viewer", 403)])
def test_repository_review_policy_authorization(client, signed_in, role, expected):
    from aifactory.models import Membership, Repository

    wid = signed_in["workspaces"][0]["id"]
    with session_scope() as db:
        db.query(Membership).filter_by(workspace_id=wid).one().role = role
        repo = Repository(workspace_id=wid, full_name="acme/reviews")
        db.add(repo)
        db.flush()
        rid = repo.id
    response = client.patch(f"/api/workspaces/{wid}/repositories/{rid}",
                            json={"config": {"review": {"mode": "all", "allow_untrusted_local": True}}})
    assert response.status_code == expected, response.text
    with session_scope() as db:
        assert db.get(Repository, rid).config == ({"review": {"mode": "all", "allow_untrusted_local": True}}
                                                  if expected == 200 else {})


def test_repository_review_policy_workspace_boundary(client, signed_in):
    from aifactory.models import Repository

    wid = signed_in["workspaces"][0]["id"]
    with session_scope() as db:
        org_id = db.get(Workspace, wid).org_id
        other = Workspace(org_id=org_id, name="Other", slug="other")
        db.add(other)
        db.flush()
        repo = Repository(workspace_id=other.id, full_name="acme/private", config={"context": "private"})
        db.add(repo)
        db.flush()
        rid, other_id = repo.id, other.id
    payload = {"config": {"review": {"mode": "all"}}}
    assert client.patch(f"/api/workspaces/{wid}/repositories/{rid}", json=payload).status_code == 404
    assert client.patch(f"/api/workspaces/{other_id}/repositories/{rid}", json=payload).status_code == 404
    with session_scope() as db:
        assert db.get(Repository, rid).config == {"context": "private"}


@pytest.mark.parametrize("mode", ["off", "all", "labelled"])
def test_connect_repository_review_policy(client, signed_in, monkeypatch, mode):
    async def detect(_wid, name):
        return {"full_name": name}, {"checks": ["pytest"], "detected": ["Python"]}

    monkeypatch.setattr(api_module, "_detect_repository", detect)
    wid = signed_in["workspaces"][0]["id"]
    path = f"/api/workspaces/{wid}/repositories"
    r = client.post(path, json={"full_name": "acme/reviews", "config": {"review": {"mode": mode}}})
    assert r.status_code == 200, r.text
    assert r.json()["config"]["review"] == {"mode": mode, "allow_untrusted_local": False}
    assert r.json()["config"]["checks"] == ["pytest"]
    assert client.post(path, json={"full_name": "acme/invalid", "config": {
        "review": {"allow_untrusted_local": "true"}}}).status_code == 422


@pytest.mark.parametrize("field,value", [("mode", "off"), ("mode", "all"),
                                         ("allow_untrusted_local", False), ("allow_untrusted_local", True)])
def test_workspace_rejects_repository_only_review_options(client, signed_in, field, value):
    wid = signed_in["workspaces"][0]["id"]
    with session_scope() as db:
        before = db.get(Workspace, wid).settings
    response = client.patch(f"/api/workspaces/{wid}", json={"settings": {
        "max_attempts": 7, "review": {"max_rounds": 5, field: value}}})
    assert response.status_code == 422, response.text
    assert f"review.{field}" in response.json()["detail"]
    with session_scope() as db:
        assert db.get(Workspace, wid).settings == before


def test_disconnect_repository_deletes_review_history_and_keeps_costs(client, signed_in):
    # Uses foreign-key-enforcing SQLite by default, or Postgres through
    # AIFACTORY_TEST_DATABASE_URL, exactly like the rest of the API suite.
    from aifactory.models import Outcome, PullReview, Repository, ReviewAttempt
    from aifactory.pull_reviews import get_or_create_attempt, get_or_create_review

    wid = signed_in["workspaces"][0]["id"]
    with session_scope() as db:
        repo = Repository(workspace_id=wid, full_name="acme/review-history")
        other = Repository(workspace_id=wid, full_name="acme/keep-history")
        db.add_all([repo, other])
        db.flush()
        review = get_or_create_review(db, repo, 42)
        review_id, outcome_id, repo_id = review.id, review.outcome_id, repo.id
        for head in ("head1", "head2"):
            get_or_create_attempt(db, review, head, "base")
        other_review = get_or_create_review(db, other, 42)
        get_or_create_attempt(db, other_review, "head1", "base")
        other_id = other_review.id
        run = Run(workspace_id=wid, outcome_id=outcome_id, role="reviewer", harness="native", cost_usd=1.5)
        db.add(run)
        db.flush()
        run_id = run.id
    response = client.delete(f"/api/workspaces/{wid}/repositories/{repo_id}")
    assert response.status_code == 200, response.text
    with session_scope() as db:
        assert db.get(Repository, repo_id) is None
        assert db.get(PullReview, review_id) is None
        assert db.query(ReviewAttempt).filter_by(review_id=review_id).count() == 0
        assert db.get(Outcome, outcome_id).source == "review"
        assert db.get(Run, run_id).cost_usd == 1.5
        assert db.get(PullReview, other_id) is not None
        assert db.query(ReviewAttempt).filter_by(review_id=other_id).count() == 1


def test_review_intake_controls_are_deferred_until_intake_exists():
    from pathlib import Path

    settings = (Path(__file__).resolve().parents[1] / "web/src/pages/Settings.tsx").read_text()
    assert "Review pull requests" not in settings
    assert "allow_untrusted_local" not in settings
    assert "review_mode" not in settings


@pytest.mark.parametrize("mode", ["auto", "approval", "github"])
def test_three_merge_policies_round_trip_and_inherit(client, signed_in, mode):
    from aifactory.merge_policy import effective_merge_policy
    from aifactory.models import Repository

    ws_id = signed_in["workspaces"][0]["id"]
    with session_scope() as s:
        ws = s.get(Workspace, ws_id)
        ws.settings = {**ws.settings, "learning": {"status": "applied"}}
        repo = Repository(workspace_id=ws_id, full_name="acme/policy", config={})
        s.add(repo)
        s.flush()
        repo_id = repo.id
    r = client.patch(f"/api/workspaces/{ws_id}", json={"settings": {"autonomy": {"merge": mode}}})
    assert r.status_code == 200, r.text
    assert r.json()["settings"]["autonomy"]["merge"] == mode
    assert effective_merge_policy(r.json()["settings"], {}) == mode
    path = f"/api/workspaces/{ws_id}/repositories/{repo_id}"
    r = client.patch(path, json={"config": {"merge": {"mode": mode, "method": "rebase", "revalidate": False, "reverify": "always"}}})
    assert r.status_code == 200, r.text
    config = r.json()["config"]
    assert config["merge"] == {"mode": mode, "method": "rebase", "revalidate": False, "reverify": "always"}
    assert effective_merge_policy({"autonomy": {"merge": "approval"}}, config) == mode
    for inherited in ("", None):
        r = client.patch(path, json={"config": {"merge": {"mode": inherited}}})
        assert r.status_code == 200
        assert r.json()["config"]["merge"]["method"] == "rebase"
        assert effective_merge_policy({"autonomy": {"merge": mode}}, r.json()["config"]) == mode
    for invalid in ("human", 1, [], {}):
        assert client.patch(path, json={"config": {"merge": {"mode": invalid}}}).status_code == 422


@pytest.mark.parametrize("key,value", [("plan", "github"), ("release", "github"), ("merge", "human")])
def test_invalid_autonomy_policy_rejected(client, signed_in, key, value):
    ws_id = signed_in["workspaces"][0]["id"]
    assert client.patch(f"/api/workspaces/{ws_id}", json={"settings": {"autonomy": {key: value}}}).status_code == 422


def test_github_self_link_verifies_identity_and_never_retains_token(client, signed_in, monkeypatch, caplog):
    from aifactory.github import GitHub, GitHubError
    from aifactory.models import Event, User

    token = "ghp_personal_not_for_storage"
    async def identity(self):
        return {"id": 12345, "login": "proved-owner", "token": token}
    monkeypatch.setattr(GitHub, "user", identity)
    r = client.put("/auth/github", json={"token": token, "login": "someone-else"})
    assert r.status_code == 400 and token not in r.text
    r = client.put("/auth/github", json={"token": token})
    assert r.status_code == 200, r.text
    assert r.json()["github"] == {"id": "12345", "login": "proved-owner"}
    assert client.get("/api/me").json()["user"]["github"] == r.json()["github"]
    assert token not in r.text
    with session_scope() as s:
        user = s.get(User, signed_in["user"]["id"])
        assert user.github_id == "12345"
        assert token not in repr({c.name: getattr(user, c.name) for c in User.__table__.columns})
        assert not s.query(Secret).count()
        assert token not in repr([(e.message, e.data) for e in s.query(Event)])
    async def rejected(self):
        raise GitHubError(f"Provider echo: {token}")
    monkeypatch.setattr(GitHub, "user", rejected)
    r = client.put("/auth/github", json={"token": token})
    assert r.status_code == 400 and token not in r.text
    assert token not in caplog.text
    assert client.delete("/auth/github").json()["github"] is None
    client.post("/auth/logout")
    assert client.put("/auth/github", json={"token": token}).status_code == 401
    assert client.delete("/auth/github").status_code == 401


def test_github_link_is_personal_not_admin_only(client, signed_in, monkeypatch):
    from aifactory.github import GitHub
    from aifactory.models import User

    owner_id = signed_in["user"]["id"]
    with session_scope() as s:
        owner = s.get(User, owner_id)
        owner.github_id, owner.github_login = "55", "original-owner"
    client.post("/auth/logout")
    client.post("/auth/dev", json={"email": "member@example.com", "organization": "MindsDB"})
    async def identity(self):
        return {"id": 99, "login": "member"}
    monkeypatch.setattr(GitHub, "user", identity)
    assert client.put("/auth/github", json={"token": "personal"}).status_code == 200
    assert client.delete("/auth/github").status_code == 200
    with session_scope() as s:
        assert s.get(User, owner_id).github_id == "55"
    ws_id = signed_in["workspaces"][0]["id"]
    from aifactory.models import Membership
    with session_scope() as s:
        member = s.query(User).filter_by(email="member@example.com").one()
        s.query(Membership).filter_by(user_id=member.id, workspace_id=ws_id).one().role = "member"
    assert client.patch(f"/api/workspaces/{ws_id}", json={"settings": {"autonomy": {"merge": "github"}}}).status_code == 403


@pytest.mark.parametrize("identity", [{"login": "unproved"}, {"id": 0, "login": "no"}, {"id": 1, "login": "bad login"}])
def test_github_link_rejects_incomplete_provider_identity(client, signed_in, monkeypatch, identity):
    from aifactory.github import GitHub
    async def user(self):
        return identity
    monkeypatch.setattr(GitHub, "user", user)
    assert client.put("/auth/github", json={"token": "personal"}).status_code == 400
    assert client.get("/api/me").json()["user"]["github"] is None


def test_github_link_bounds_and_sanitizes_all_invalid_payloads(client, signed_in):
    marker = "never-echo-this-token"
    for body in ({"token": {"nested": marker}}, {"login": marker}, [marker], {"token": ""}):
        response = client.put("/auth/github", json=body)
        assert response.status_code == 400 and marker not in response.text
    response = client.put("/auth/github", content=marker * 1000)
    assert response.status_code == 413 and marker not in response.text


def test_github_policy_is_preserved_as_learning_preset(client, signed_in):
    ws_id = signed_in["workspaces"][0]["id"]
    base = f"/api/workspaces/{ws_id}"
    r = client.patch(base, json={"settings": {"autonomy": {"merge": "github"}}})
    assert r.status_code == 200
    assert r.json()["settings"]["autonomy"]["merge"] == "approval"
    assert r.json()["settings"]["learning"]["preset"]["merge"] == "github"
    from aifactory.models import Outcome
    with session_scope() as s:
        s.add(Outcome(workspace_id=ws_id, number=1, title="Learned", status="completed"))
    r = client.post(f"{base}/onboarding/policy/apply")
    assert r.status_code == 200 and r.json()["after"]["merge"] == "github"
    assert client.get("/api/me").json()["workspaces"][0]["settings"]["autonomy"]["merge"] == "github"
