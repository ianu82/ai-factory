"""Credential checks never generate, mutate GitHub, or return upstream errors."""
from __future__ import annotations

import json

import httpx
import pytest

from aifactory.api import _test_secret
from aifactory.auth import AuthError, mindshub_identity
from aifactory.db import session_scope
from aifactory.github import GitHub, REQUIRED_PERMISSIONS
from aifactory.models import Membership, Secret
from aifactory.security import mask

KEY = "mdb_test.secretvalue"
TOKEN = "ghp_sensitive_token_value"
ASYNC_CLIENT = httpx.AsyncClient


def transport(monkeypatch, handler):

    def checked(request):
        assert request.method == "GET"
        return handler(request)

    monkeypatch.setattr(httpx, "AsyncClient", lambda **kwargs: ASYNC_CLIENT(**kwargs, transport=httpx.MockTransport(checked)))


def response(status=200, data=None, headers=None):
    return httpx.Response(status, json=data, headers=headers)


@pytest.mark.parametrize("failure,expected,code", [
    (401, "invalid", "identity_invalid"), (403, "invalid", "identity_invalid"),
    (429, "untested", "rate_limited"), (503, "untested", "service_error"),
    ("network", "untested", "network_error"), ("invalid", "invalid", "identity_invalid"),
    ("malformed", "untested", "service_error"),
])
async def test_identity_categories(monkeypatch, failure, expected, code):
    def handle(request):
        if failure == "network":
            raise httpx.ConnectError(KEY)
        if failure == "invalid":
            return response(data={"valid": False, "message": KEY})
        if failure == "malformed":
            return response(data={"error": KEY})
        return response(failure, {"message": KEY})
    transport(monkeypatch, handle)
    status, detail, metadata = await _test_secret("mindshub_api_key", KEY)
    assert status == expected and metadata["diagnostic_code"] == code
    assert KEY not in detail + json.dumps(metadata)


@pytest.mark.parametrize("status", [401, 403, 429, 503])
async def test_auth_preserves_status(monkeypatch, status):
    transport(monkeypatch, lambda r: response(status, {"error": KEY}))
    with pytest.raises(AuthError) as caught:
        await mindshub_identity(KEY)
    assert caught.value.status == status
    assert KEY not in str(caught.value)


@pytest.mark.parametrize("failure,expected,code", [
    (401, "invalid", "inference_denied"), (403, "invalid", "inference_denied"),
    (402, "invalid", "inference_denied"), (429, "untested", "rate_limited"),
    (503, "untested", "service_error"), ("network", "untested", "network_error"),
    ("empty", "invalid", "models_empty"), ("malformed", "untested", "service_error"),
    (200, "valid", "models_available"),
])
async def test_inference_categories(monkeypatch, failure, expected, code):
    def handle(request):
        if not request.url.path.endswith("/models"):
            return response(data={"valid": True, "user_id": "u"})
        if failure == "network":
            raise httpx.ReadTimeout(KEY)
        if failure == "empty":
            return response(data={"data": []})
        if failure == "malformed":
            return response(data={"data": KEY})
        if failure == 200:
            return response(data={"data": [{"id": "model"}, {"id": "disabled", "enabled": False}]})
        return response(failure, {"error": KEY})
    transport(monkeypatch, handle)
    status, detail, metadata = await _test_secret("mindshub_api_key", KEY)
    assert status == expected and metadata["diagnostic_code"] == code
    assert KEY not in detail + json.dumps(metadata)


def github_handler(request, *, scopes=None, private=True):
    path = request.url.path
    if path == "/user":
        return response(data={"login": "octocat"}, headers={} if scopes is None else {"X-OAuth-Scopes": scopes})
    if path == "/repos/acme/app":
        return response(data={"private": private, "default_branch": "main", "permissions": {"push": True}})
    return response(data=[], headers={"X-Accepted-GitHub-Permissions": "contents=write;issues=read"})


@pytest.mark.parametrize("scopes,private,state", [("repo", True, "present"), ("read:user", True, "missing"),
                                                      (None, True, "unverified"), (None, False, "unverified")])
async def test_permission_evidence(monkeypatch, scopes, private, state):
    transport(monkeypatch, lambda r: github_handler(r, scopes=scopes, private=private))
    status, detail, metadata = await GitHub(TOKEN).diagnose("acme/app")
    assert metadata["login"] == "octocat"
    permissions = {p["permission"]: p for p in metadata["permissions"]}
    assert set(permissions) == set(REQUIRED_PERMISSIONS)
    for name in ("contents", "pull_requests", "issues"):
        assert permissions[name]["state"] == state
    if state == "present":
        assert all(p["state"] == "present" for p in permissions.values())
    if state == "unverified":
        assert permissions["issues"]["instruction"]
        assert permissions["checks"]["state"] == ("present" if private else "unverified")
    assert status == ("invalid" if state == "missing" else "valid")


async def test_pull_requests_read_does_not_prove_issues_read(monkeypatch):
    # A token with only Pull requests:read may successfully list /issues.
    transport(monkeypatch, github_handler)
    _, _, metadata = await GitHub(TOKEN).diagnose("acme/app")
    assert next(p for p in metadata["permissions"] if p["permission"] == "issues")["state"] == "unverified"


@pytest.mark.parametrize("status", [403, 404])
async def test_inaccessible_repository_is_not_missing_permissions(monkeypatch, status):
    transport(monkeypatch, lambda r: response(status, {"message": TOKEN}) if r.url.path == "/repos/acme/app"
              else github_handler(r, scopes="repo"))
    status, _, metadata = await GitHub(TOKEN).diagnose("acme/app")
    assert status == "valid"
    assert metadata["diagnostic_code"] == "github_repository_unverified"
    assert all(p["state"] == "unverified" for p in metadata["permissions"])


RATE_LIMITS = [(429, {}, ""), (403, {"Retry-After": "60", "X-RateLimit-Remaining": "4999"}, ""),
               (403, {"X-RateLimit-Remaining": "0"}, ""), (403, {}, "secondary rate limit"),
               (403, {}, "API rate limit exceeded"), (403, {}, "abuse detection")]


@pytest.mark.parametrize("stage", ["/user", "/repos/acme/app", "/repos/acme/app/commits/main/check-runs", "/repos/acme/app/actions/runs", "/repos/acme/app/deployments"])
@pytest.mark.parametrize("status,headers,message", RATE_LIMITS)
async def test_primary_and_secondary_limits_are_not_permission_evidence(monkeypatch, stage, status, headers, message):
    transport(monkeypatch, lambda r: response(status, {"message": message + TOKEN}, headers) if r.url.path == stage
              else github_handler(r))
    status, detail, metadata = await GitHub(TOKEN).diagnose("acme/app")
    assert status == "untested" and metadata["diagnostic_code"] == "rate_limited"
    assert all(p["state"] == "unverified" for p in metadata["permissions"])
    assert TOKEN not in detail + json.dumps(metadata)


@pytest.mark.parametrize("failure,expected", [(401, "invalid"), (429, "valid"), (503, "valid"), ("network", "valid")])
def test_retest_status_and_metadata_replacement(client, signed_in, monkeypatch, failure, expected):
    ws = signed_in["workspaces"][0]["id"]
    path = f"/api/workspaces/{ws}/credentials/github_token"
    transport(monkeypatch, github_handler)
    assert client.put(path, json={"value": TOKEN}).json()["status"] == "valid"
    assert client.post(path + "/test?repository=acme/app").json()["metadata"]["login"] == "octocat"
    def fail(r):
        if failure == "network":
            raise httpx.ConnectError(TOKEN)
        return response(failure, {"message": TOKEN})
    transport(monkeypatch, fail)
    body = client.post(path + "/test").json()
    assert body["status"] == expected
    assert body["metadata"]["test_status"] == ("invalid" if failure == 401 else "untested")
    assert "login" not in body["metadata"] and body["metadata"]["repository"] == "acme/app"
    assert all(p["state"] == "unverified" for p in body["metadata"]["permissions"])
    assert TOKEN not in json.dumps(body) + client.get(path.rsplit("/", 1)[0]).text


@pytest.mark.parametrize("failure", [401, 403, "invalid"])
def test_definitive_mindshub_retest_invalidates(client, signed_in, monkeypatch, failure):
    ws = signed_in["workspaces"][0]["id"]
    path = f"/api/workspaces/{ws}/credentials/mindshub_api_key"
    def good(r):
        return response(data={"data": [{"id": "model"}]}) if r.url.path.endswith("/models") else response(data={"user_id": "u"})
    transport(monkeypatch, good)
    assert client.put(path, json={"value": KEY}).json()["status"] == "valid"
    transport(monkeypatch, lambda r: response(data={"valid": False}) if failure == "invalid" else
              response(failure, {"error": KEY}) if r.url.path.endswith("/models") else good(r))
    body = client.post(path + "/test").json()
    assert body["status"] == "invalid" and "models" not in body["metadata"]


@pytest.mark.parametrize("failure,expected", [(200, "valid"), (403, "invalid"), (503, "untested")])
def test_seeded_keys_are_inference_tested(client, monkeypatch, failure, expected):
    calls = []
    def handle(r):
        calls.append(r.url.path)
        if r.url.path.endswith("/models"):
            return response(failure, {"data": [{"id": "model"}], "error": KEY})
        return response(data={"user_id": "u", "organization_id": "org", "email": "owner@example.com"})
    transport(monkeypatch, handle)
    assert client.post("/auth/api-key", json={"key": KEY}).status_code == 200
    ws = client.get("/api/me").json()["workspaces"][0]["id"]
    body = client.get(f"/api/workspaces/{ws}/credentials").json()["items"][0]
    assert body["status"] == expected
    assert any(p.endswith("/models") for p in calls)
    onboarding = client.get(f"/api/workspaces/{ws}/onboarding")
    assert ("models" in onboarding.json()["completed_steps"]) == (failure == 200)
    assert KEY not in json.dumps(body) + onboarding.text


@pytest.mark.parametrize("role", ["member", "viewer"])
@pytest.mark.parametrize("kind", ["github_token", "mindshub_api_key", "openai_api_key"])
def test_retests_require_admin(client, signed_in, monkeypatch, role, kind):
    ws = signed_in["workspaces"][0]["id"]
    with session_scope() as db:
        db.query(Membership).filter_by(workspace_id=ws).one().role = role
    assert client.post(f"/api/workspaces/{ws}/credentials/{kind}/test").status_code == 403
    assert client.get(f"/api/workspaces/{ws}/credentials").status_code == 200


@pytest.mark.parametrize("secret", ["12345678", "abcdefghijk", "x" * 11, "x" * 80, "••••••••"])
def test_adversarial_success_and_error_never_echo_secret(client, signed_in, monkeypatch, secret):
    ws = signed_in["workspaces"][0]["id"]
    path = f"/api/workspaces/{ws}/credentials/github_token"
    transport(monkeypatch, lambda r: response(data={"login": secret, "extra": secret}))
    saved = client.put(path, json={"value": secret})
    assert saved.status_code == 200
    assert secret not in saved.text + client.post(path + "/test").text + client.get(path.rsplit("/", 1)[0]).text
    transport(monkeypatch, lambda r: response(401, {"message": secret}))
    assert secret not in client.post(path + "/test").text
    assert secret not in client.get(f"/api/workspaces/{ws}/onboarding").text
    assert secret not in mask(secret)


@pytest.mark.parametrize("value", ["secret", "x" * 8193, {"secret": TOKEN}, [TOKEN], None])
def test_malformed_credential_body_is_safe(client, signed_in, value):
    ws = signed_in["workspaces"][0]["id"]
    response = client.put(f"/api/workspaces/{ws}/credentials/github_token", json={"value": value})
    assert response.status_code == 422
    assert response.json() == {"detail": "Invalid credential request"}
    response = client.post("/auth/api-key", json={"key": {"value": TOKEN}})
    assert response.status_code == 422 and TOKEN not in response.text


def test_legacy_metadata_is_allowlisted(client, signed_in, monkeypatch):
    ws = signed_in["workspaces"][0]["id"]
    transport(monkeypatch, github_handler)
    path = f"/api/workspaces/{ws}/credentials/github_token"
    client.put(path, json={"value": TOKEN})
    with session_scope() as db:
        sec = db.query(Secret).filter_by(workspace_id=ws).one()
        sec.metadata_json = {"upstream_error": TOKEN, "login": TOKEN}
        sec.status_detail = TOKEN
        sec.hint = TOKEN
    assert TOKEN not in client.get(path.rsplit("/", 1)[0]).text


@pytest.mark.parametrize("status,headers,message", RATE_LIMITS)
def test_rate_limited_retest_keeps_last_good_status(client, signed_in, monkeypatch, status, headers, message):
    ws = signed_in["workspaces"][0]["id"]
    path = f"/api/workspaces/{ws}/credentials/github_token"
    transport(monkeypatch, github_handler)
    assert client.put(path, json={"value": TOKEN}).json()["status"] == "valid"
    transport(monkeypatch, lambda r: response(status, {"message": message}, headers)
              if r.url.path.endswith("/check-runs") else github_handler(r))
    body = client.post(path + "/test?repository=acme/app").json()
    assert body["status"] == "valid" and body["metadata"]["test_status"] == "untested"
    assert body["metadata"]["diagnostic_code"] == "rate_limited"
    assert all(p["state"] == "unverified" for p in body["metadata"]["permissions"])


def test_empty_scope_and_accepted_permissions_are_not_token_grants(monkeypatch):
    transport(monkeypatch, lambda r: github_handler(r, scopes=""))
    import asyncio
    status, _, metadata = asyncio.run(GitHub(TOKEN).diagnose("acme/app"))
    assert status == "valid"
    for p in metadata["permissions"]:
        if p["permission"] in ("contents", "pull_requests", "issues"):
            assert p["state"] == "unverified"


@pytest.mark.parametrize("status,expected", [(401, 401), (429, 429), (503, 503)])
def test_sign_in_failures_are_safe_and_preserve_category(client, monkeypatch, status, expected):
    transport(monkeypatch, lambda r: response(status, {"message": KEY}))
    r = client.post("/auth/api-key", json={"key": KEY})
    assert r.status_code == expected and KEY not in r.text


def test_invalid_json_and_repository_validation_are_safe(client, signed_in):
    ws = signed_in["workspaces"][0]["id"]
    path = f"/api/workspaces/{ws}/credentials/github_token"
    r = client.put(path, content='{"value":"' + TOKEN, headers={"Content-Type": "application/json"})
    assert r.status_code == 422 and TOKEN not in r.text
    r = client.post(path + "/test", params={"repository": TOKEN})
    assert r.status_code == 422 and TOKEN not in r.text


@pytest.mark.parametrize("length", [199, 200, 201, 256, 8192])
@pytest.mark.parametrize("wrapped", [False, True])
def test_metadata_redacts_before_truncating(length, wrapped):
    from aifactory.credential_diagnostics import safe_metadata

    secret = "opaque_" + "A" * (length - 11) + "Z9xy"
    echoed = f"account:{secret}:suffix" if wrapped else secret
    fields = ("organization_id", "organization_name", "workspace_id", "login", "name", "repository")
    metadata = {field: echoed for field in fields}
    metadata["models"] = [echoed]
    safe = safe_metadata(metadata, secret)
    expected = "account:[redacted]:suffix" if wrapped else "[redacted]"
    assert safe == {**{field: expected for field in fields}, "models": [expected]}
    # Checking only `secret not in response` would miss a reconstructable prefix.
    assert secret[:190] not in json.dumps(safe)


@pytest.mark.parametrize("length", [199, 200, 201, 256])
@pytest.mark.parametrize("kind", ["github_token", "mindshub_api_key"])
def test_long_upstream_echo_is_not_partially_disclosed(client, signed_in, monkeypatch, length, kind):
    prefix = "mdb_" if kind == "mindshub_api_key" else "opaque_"
    secret = prefix + "A" * (length - len(prefix) - 4) + "Z9xy"
    echoed = f"upstream:{secret}:suffix"

    def handle(request):
        if request.url.path.endswith("/models"):
            return response(data={"data": [{"id": echoed}]})
        if kind == "mindshub_api_key":
            return response(data={"user_id": "u", "organization_id": echoed,
                                  "organization_name": echoed, "workspace_id": echoed})
        return response(data={"login": echoed})

    transport(monkeypatch, handle)
    ws = signed_in["workspaces"][0]["id"]
    path = f"/api/workspaces/{ws}/credentials/{kind}"
    responses = [client.put(path, json={"value": secret}), client.post(path + "/test")]
    listing = client.get(path.rsplit("/", 1)[0])
    assert listing.status_code == 200
    credentials = [r.json() for r in responses]
    credentials.append(next(item for item in listing.json()["items"] if item["kind"] == kind))
    for r in responses:
        assert r.status_code == 200
    for credential in credentials:
        metadata = credential["metadata"]
        if kind == "github_token":
            assert metadata["login"] == "upstream:[redacted]:suffix"
        else:
            assert metadata["models"] == ["upstream:[redacted]:suffix"]
            for field in ("organization_id", "organization_name", "workspace_id"):
                assert metadata[field] == "upstream:[redacted]:suffix"
        assert secret[:190] not in json.dumps(credential)
    onboarding = client.get(f"/api/workspaces/{ws}/onboarding")
    assert onboarding.status_code == 200
    assert secret[:190] not in onboarding.text


@pytest.mark.parametrize("private", [True, False])
def test_fine_grained_completion_and_settings_retest_keep_repository(client, signed_in, monkeypatch, private):
    ws = signed_in["workspaces"][0]["id"]
    path = f"/api/workspaces/{ws}/credentials/github_token"
    calls = []

    def handle(r):
        calls.append(r.url.path)
        return github_handler(r, private=private)

    transport(monkeypatch, handle)
    assert client.put(path, json={"value": TOKEN}).status_code == 200
    progress = f"/api/workspaces/{ws}/onboarding"
    assert "github" not in client.get(progress).json()["completed_steps"]
    checked = client.post(path + "/test?repository=acme/app").json()
    assert any(p["state"] == "unverified" for p in checked["metadata"]["permissions"])
    assert "github" in client.get(progress).json()["completed_steps"]
    calls.clear()
    retest = client.post(path + "/test")
    assert retest.status_code == 200
    assert "/repos/acme/app" in calls
    assert retest.json()["metadata"]["repository"] == "acme/app"
    assert "github" in client.get(progress).json()["completed_steps"]
    assert client.patch(progress, json={"action": "complete", "step": "github"}).status_code == 200


@pytest.mark.parametrize("failure", [403, 404])
def test_inaccessible_repository_retest_removes_completion(client, signed_in, monkeypatch, failure):
    ws = signed_in["workspaces"][0]["id"]
    path = f"/api/workspaces/{ws}/credentials/github_token"
    transport(monkeypatch, github_handler)
    client.put(path, json={"value": TOKEN})
    client.post(path + "/test?repository=acme/app")
    progress = f"/api/workspaces/{ws}/onboarding"
    assert "github" in client.get(progress).json()["completed_steps"]
    transport(monkeypatch, lambda r: response(failure) if r.url.path == "/repos/acme/app" else github_handler(r))
    assert client.post(path + "/test").json()["metadata"]["diagnostic_code"] == "github_repository_unverified"
    state = client.get(progress).json()
    assert "github" not in state["completed_steps"]
    assert "github" in state["validation_uncertain_steps"]
    assert client.patch(progress, json={"action": "complete", "step": "github"}).status_code == 409


def test_permission_contract_and_both_web_recipes(client, signed_in):
    from pathlib import Path

    expected = {"contents": "write", "pull_requests": "write", "issues": "write", "statuses": "write",
                "checks": "read", "actions": "read", "deployments": "read", "metadata": "read"}
    assert REQUIRED_PERMISSIONS == expected
    ws = signed_in["workspaces"][0]["id"]
    recipe = client.get(f"/api/workspaces/{ws}/credentials").json()["github_permissions"]
    assert {p["permission"]: p["required"] for p in recipe} == expected
    assert next(p["label"] for p in recipe if p["permission"] == "statuses") == "Commit statuses"
    web = Path(__file__).resolve().parents[1] / "web" / "src" / "onboarding"
    for step in ("GitHubStep.tsx", "RepositoryStep.tsx"):
        source = (web / step).read_text()
        assert "<GitHubPermissions " in source
        assert "Issues, Checks and Actions: read" not in source
        assert "Contents and Pull requests: write" not in source
    settings = (web.parent / "pages" / "Settings.tsx").read_text()
    assert '<GitHubPermissions workspaceId={ws.id} credential={c}' in settings
    assert "Contents, Pull requests and Commit statuses (read & write)" not in settings
    source = (web / "GitHubPermissions.tsx").read_text()
    assert "github_permissions?.map" in source
    assert 'p.required === "write" ? "Read and write" : "Read-only"' in source


def test_model_choices_persist_without_changing_other_settings(client, signed_in, monkeypatch):
    ws = signed_in["workspaces"][0]["id"]
    transport(monkeypatch, lambda r: response(data={"data": [{"id": "planner"}, {"id": "builder"}]})
              if r.url.path.endswith("/models") else response(data={"user_id": "u"}))
    path = f"/api/workspaces/{ws}"
    client.put(path + "/credentials/mindshub_api_key", json={"value": KEY})
    before = client.get("/api/me").json()["workspaces"][0]["settings"]
    models = {"default": "planner", "planner": "planner", "builder": "builder", "verifier": "planner"}
    assert client.patch(path, json={"settings": {"provider": "mindshub", "models": models}}).status_code == 200
    after = client.get("/api/me").json()["workspaces"][0]["settings"]
    assert after["models"] == models
    assert {k: v for k, v in after.items() if k not in ("provider", "models")} == {
        k: v for k, v in before.items() if k not in ("provider", "models")}
    assert set(client.get(path + "/models").json()["models"]) == {"planner", "builder"}
