"""Installation transport contract against fake GitHub, with real RSA signatures."""
from __future__ import annotations

from types import SimpleNamespace
import base64
import json
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone

import httpx
import pytest
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding, rsa

from aifactory import config, github_app
from aifactory.db import session_scope
from aifactory.github import GitHub, GitHubError, resolve_credential, resolve_token, workspace_client
from aifactory.models import Workspace

SYNC_CLIENT = httpx.Client
ASYNC_CLIENT = httpx.AsyncClient


@pytest.fixture(scope="session")
def signing_key():
    return rsa.generate_private_key(public_exponent=65537, key_size=2048)


@pytest.fixture
def app(monkeypatch, signed_in, signing_key):
    key = signing_key
    pem = key.private_bytes(serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8,
                            serialization.NoEncryption()).decode()
    for name, value in {"ID": "123", "PRIVATE_KEY": pem, "CLIENT_ID": "client-id",
                        "CLIENT_SECRET": "client-secret-sensitive", "WEBHOOK_SECRET": "webhook-sensitive"}.items():
        monkeypatch.setenv("AIFACTORY_GITHUB_APP_" + name, value)
    config.reset_settings_cache()
    github_app._cache.clear()
    ws = signed_in["workspaces"][0]["id"]
    with session_scope() as db:
        row = db.get(Workspace, ws)
        row.settings = {**(row.settings or {}), github_app.INSTALLATION_SETTING: 42}
    yield ws, key
    github_app._cache.clear()


def fake(monkeypatch, key, clock):
    seen = []
    minted = []
    fail = {"mint": False, "api": False}

    def handler(request):
        seen.append(request)
        path = request.url.path
        if path.startswith("/app/installations/"):
            assert request.method == "POST"
            assert path in ("/app/installations/42/access_tokens", "/app/installations/43/access_tokens")
            encoded = request.headers["authorization"].removeprefix("Bearer ")
            header, payload, sig = encoded.split(".")
            decode = lambda v: base64.urlsafe_b64decode(v + "=" * (-len(v) % 4))
            assert json.loads(decode(header))["alg"] == "RS256"
            key.public_key().verify(decode(sig), f"{header}.{payload}".encode(), padding.PKCS1v15(), hashes.SHA256())
            claims = json.loads(decode(payload))
            assert claims["iss"] == "123"
            assert claims["iat"] == clock[0] - 60
            assert 0 < claims["exp"] - claims["iat"] <= 600
            assert claims["exp"] > clock[0]
            assert json.loads(request.content) == {"permissions": github_app.APP_PERMISSIONS}
            assert json.loads(request.content)["permissions"]["statuses"] == "read"
            if fail["mint"]:
                return httpx.Response(403, json={"message": encoded + " client-secret-sensitive"})
            token = f"installation-sensitive-{len(minted)}"
            minted.append(token)
            return httpx.Response(201, json={"token": token, "expires_at": datetime.fromtimestamp(
                clock[0] + 120, timezone.utc).isoformat()})
        assert request.headers["authorization"] == "Bearer " + minted[-1]
        assert path != "/user" and "/statuses/" not in path
        if fail["api"]:
            return httpx.Response(401, json={"message": minted[-1]})
        if path == "/installation/repositories":
            page = int(request.url.params["page"])
            size = int(request.url.params["per_page"])
            return httpx.Response(200, json={"repositories": [
                {"full_name": "acme/demo" if i == 0 else f"acme/r{i}"} for i in range(size if page == 1 else 1)], "total_count": size + 1})
        if path.endswith("/status"):
            return httpx.Response(200, json={"statuses": [{"context": "external-ci", "state": "success"}]})
        if path.endswith("/check-runs"):
            return httpx.Response(200, json={"check_runs": []})
        if path.endswith("/actions/runs"):
            return httpx.Response(200, json={"workflow_runs": []})
        if path == "/repos/acme/demo":
            return httpx.Response(200, json={"full_name": "acme/demo", "default_branch": "main"})
        raise AssertionError(path)

    transport = httpx.MockTransport(handler)
    monkeypatch.setattr(httpx, "Client", lambda **kw: SYNC_CLIENT(**kw, transport=transport))
    monkeypatch.setattr(httpx, "AsyncClient", lambda **kw: ASYNC_CLIENT(**kw, transport=transport))
    monkeypatch.setattr(github_app, "time", SimpleNamespace(time=lambda: clock[0]))
    return seen, minted, fail


async def test_sign_mint_pagination_refresh_and_commit_status(app, monkeypatch):
    ws, key = app
    clock = [1800000000]
    seen, minted, _ = fake(monkeypatch, key, clock)
    gh = workspace_client(ws)
    assert isinstance(resolve_token(ws), str)  # legacy synchronous contract
    page = await gh.accessible_repos_page(per_page=2)
    assert page["next_page"] == 2 and all(r["can_push"] for r in page["items"])
    assert (await gh.accessible_repos_page(2, 2))["next_page"] is None
    assert len(await gh.accessible_repos()) == 101
    assert len(minted) == 1
    clock[0] += 61  # within the expiry safety window; same long-lived client
    assert (await gh.repo("acme/demo"))["permissions"]["push"]
    assert len(minted) == 2
    assert (await gh.checks("acme/demo", "sha")).state == "success"
    before = len(seen)
    await gh.set_status("acme/demo", "sha", "success", "Verified")
    assert len(seen) == before


async def test_revoked_and_changed_bindings_fail_closed(app, monkeypatch):
    ws, key = app
    clock = [1800000000]
    _, minted, fail = fake(monkeypatch, key, clock)
    monkeypatch.setenv("AIFACTORY_BOOTSTRAP_GITHUB_TOKEN", "bootstrap-must-not-be-used")
    config.reset_settings_cache()
    gh = workspace_client(ws)
    await gh.repo("acme/demo")
    fail["api"] = True
    with pytest.raises(GitHubError) as exc:
        await gh.repo("acme/demo")
    assert minted[-1] not in str(exc.value)
    fail["mint"] = True
    with pytest.raises(GitHubError) as exc:
        await gh.repo("acme/demo")
    assert "client-secret-sensitive" not in str(exc.value)
    with session_scope() as db:
        db.get(Workspace, ws).settings = {github_app.INSTALLATION_SETTING: 43}
    with pytest.raises(GitHubError, match="binding changed"):
        await gh.repo("acme/demo")
    with session_scope() as db:
        db.get(Workspace, ws).settings = {github_app.INSTALLATION_SETTING: None}
    with pytest.raises(GitHubError, match="Invalid"):
        resolve_token(ws)


def test_concurrent_mint_and_credential_rotation(app, monkeypatch):
    ws, key = app
    clock = [1800000000]
    _, minted, _ = fake(monkeypatch, key, clock)
    with ThreadPoolExecutor(max_workers=8) as pool:
        tokens = list(pool.map(lambda _: resolve_token(ws), range(16)))
    assert len(set(tokens)) == 1 and len(minted) == 1
    monkeypatch.setenv("AIFACTORY_GITHUB_APP_CLIENT_SECRET", "rotated-sensitive")
    config.reset_settings_cache()
    assert resolve_token(ws) != tokens[0]
    assert len(minted) == 2


async def test_workspace_isolation(app, monkeypatch):
    ws, key = app
    clock = [1800000000]
    _, minted, _ = fake(monkeypatch, key, clock)
    old = workspace_client(ws)
    with session_scope() as db:
        other = Workspace(name="Other", slug="other", org_id=db.get(Workspace, ws).org_id, settings={github_app.INSTALLATION_SETTING: 43})
        db.add(other)
        db.flush()
        other_id = other.id
    await old.repo("acme/demo")
    await workspace_client(other_id).repo("acme/demo")
    assert len(minted) == 2
    with session_scope() as db:
        db.get(Workspace, ws).settings = {}
    with pytest.raises(GitHubError, match="binding changed"):
        await old.repo("acme/demo")


async def test_connect_without_user_or_push_metadata(app, monkeypatch, client):
    from aifactory import api

    ws, key = app
    fake(monkeypatch, key, [1800000000])

    async def detect(*args):
        return {}

    monkeypatch.setattr(api, "detect", detect)
    response = client.post(f"/api/workspaces/{ws}/repositories", json={"full_name": "acme/demo"})
    assert response.status_code == 200, response.text
    assert response.json()["full_name"] == "acme/demo"
    with session_scope() as db:
        assert db.get(Workspace, ws).settings[github_app.INSTALLATION_SETTING] == 42
    assert "sensitive" not in response.text


@pytest.mark.parametrize("missing", ["ID", "PRIVATE_KEY", "CLIENT_ID", "CLIENT_SECRET", "WEBHOOK_SECRET"])
def test_availability_requires_every_setting(app, monkeypatch, client, missing):
    monkeypatch.delenv("AIFACTORY_GITHUB_APP_" + missing)
    config.reset_settings_cache()
    assert not config.get_settings().github_app_available
    assert client.get("/api/meta").json()["auth"]["github_app"] is False
    with pytest.raises(GitHubError, match="not configured"):
        resolve_token(app[0])


@pytest.mark.parametrize("operation", ["publish", "rebase", "revert"])
async def test_git_refreshes_at_push_after_long_local_work(app, monkeypatch, tmp_path, operation):
    from aifactory import gitops

    ws, key = app
    clock = [1800000000]
    _, minted, _ = fake(monkeypatch, key, clock)
    credential = resolve_credential(ws)
    resolve_token(ws)
    captured = []

    async def mirror(*args):
        return tmp_path

    async def clone(*args):
        assert args[2] == {}  # local clones never need authentication

    async def git(*args, **kw):
        captured.append((args, kw.get("env", {})))
        if args[0] in ("commit", "rebase", "revert"):
            clock[0] += 121
        return "sha"

    monkeypatch.setattr(gitops, "mirror", mirror)
    monkeypatch.setattr(gitops, "clone_mirror", clone)
    monkeypatch.setattr(gitops, "git", git)
    if operation == "publish":
        await gitops.publish("acme/demo", credential, base_sha="base", branch="branch",
                             patch=gitops.Patch(b"diff", [], 1, 0), message="change", task_id="test")
    elif operation == "rebase":
        await gitops.rebase_branch("acme/demo", credential, branch="branch", onto="main", task_id="test")
    else:
        await gitops.revert_commit("acme/demo", credential, base="main", sha="sha", branch="branch", task_id="test")
    assert len(minted) == 2
    push = next(env for args, env in captured if args[0] == "push")
    auth = base64.b64decode(push["GIT_CONFIG_VALUE_0"].split()[-1]).decode()
    assert auth == "x-access-token:" + minted[-1]
    assert all("GIT_CONFIG_VALUE_0" not in env for args, env in captured if args[0] not in ("push", "fetch"))


async def test_pat_status_contract(monkeypatch):
    seen = []

    def handle(request):
        seen.append(request)
        if request.method == "POST":
            return httpx.Response(201, json={})
        if request.url.path.endswith("check-runs"):
            return httpx.Response(200, json={"check_runs": []})
        return httpx.Response(200, json={"statuses": []})

    monkeypatch.setattr(httpx, "AsyncClient", lambda **kw: ASYNC_CLIENT(**kw, transport=httpx.MockTransport(handle)))
    gh = GitHub("pat")
    await gh.set_status("acme/demo", "sha", "success", "verified")
    await gh.checks("acme/demo", "sha")
    assert [r.url.path for r in seen] == ["/repos/acme/demo/statuses/sha",
                                         "/repos/acme/demo/commits/sha/check-runs", "/repos/acme/demo/commits/sha/status"]


def test_secrets_never_persist_or_escape_config(app, monkeypatch, client):
    from sqlalchemy import select
    from aifactory.models import Base
    from aifactory.sandbox import LocalSandbox

    ws, key = app
    seen, minted, _ = fake(monkeypatch, key, [1800000000])
    resolve_token(ws)
    response = client.get("/api/meta")
    assert response.status_code == 200 and response.json()["auth"]["github_app"] is True
    # Generic workspace settings cannot bind an installation from another workspace.
    rejected = client.patch(f"/api/workspaces/{ws}", json={"settings": {github_app.INSTALLATION_SETTING: 43}})
    assert rejected.status_code == 422
    secrets = [minted[0], seen[0].headers["authorization"].removeprefix("Bearer "),
               config.get_settings().github_app_private_key, "client-secret-sensitive", "webhook-sensitive"]
    with session_scope() as db:
        rows = repr([list(db.execute(select(table))) for table in Base.metadata.sorted_tables])
    sandbox = LocalSandbox("secret-test")
    outputs = [response.text, client.get("/api/me").text, rows, repr(config.get_settings()),
               repr(sandbox.env())]
    for output in outputs:
        assert all(secret not in output for secret in secrets)


async def test_git_error_scrubs_ephemeral_header(app, monkeypatch):
    from aifactory import gitops
    from aifactory.github import fresh_git_auth_env

    ws, key = app
    fake(monkeypatch, key, [1800000000])
    env = await fresh_git_auth_env(resolve_credential(ws))
    encoded = env["GIT_CONFIG_VALUE_0"].split()[-1]
    decoded = base64.b64decode(encoded).decode()

    class FailedProcess:
        returncode = 1

        async def communicate(self, _):
            return b"", f"{env['GIT_CONFIG_VALUE_0']} {decoded}".encode()

    async def spawn(*args, **kwargs):
        return FailedProcess()

    monkeypatch.setattr(gitops.asyncio, "create_subprocess_exec", spawn)
    with pytest.raises(gitops.GitError) as exc:
        await gitops.git("push", env=env)
    assert encoded not in str(exc.value) and decoded not in str(exc.value)
    assert "installation-sensitive" not in str(exc.value)


async def test_public_metadata_does_not_prove_installation_membership(app, monkeypatch):
    ws, key = app
    fake(monkeypatch, key, [1800000000])
    gh = workspace_client(ws)

    async def unselected(*args, **kwargs):
        return {"items": [{"full_name": "other/repo"}], "next_page": None}

    monkeypatch.setattr(gh, "accessible_repos_page", unselected)
    with pytest.raises(GitHubError, match="not available to this installation"):
        await gh.repo("acme/demo")


async def test_real_git_publish_keeps_credentials_out_of_config(app, monkeypatch, tmp_path):
    from aifactory import gitops
    from aifactory.sandbox import LocalSandbox
    from .fakes import make_remote

    ws, key = app
    _, minted, _ = fake(monkeypatch, key, [1800000000])
    remotes = tmp_path / "remotes"
    make_remote(remotes, "acme/demo", {"README.md": "before\n"})
    monkeypatch.setattr(gitops, "GIT_BASE", str(remotes))
    credential = resolve_credential(ws)
    sandbox = LocalSandbox("app-checkout").create()
    try:
        base = await gitops.checkout(sandbox, "acme/demo", credential, "main", "candidate")
        (sandbox.work / "README.md").write_text("after\n")
        patch = await gitops.extract_patch(sandbox, base)
        head = await gitops.publish("acme/demo", credential, base_sha=base, branch="candidate",
                                    patch=patch, message="docs", task_id="app-publish")
        assert len(head) == 40
        for path in (sandbox.work / ".git/config", gitops._mirror_path("acme/demo") / "config"):
            text = path.read_text()
            assert "extraheader" not in text.lower()
            assert all(token not in text for token in minted)
            assert "x-access-token" not in text
    finally:
        await sandbox.close()


@pytest.fixture
def connection_github(app, monkeypatch):
    """All network calls use a deterministic transport; no retries, sleeps or RSA generation."""
    ws, key = app
    with session_scope() as db:
        db.get(Workspace, ws).settings = {}
    selected = {"id": 42, "app_id": 123, "account": {"login": "acme"},
                "permissions": dict(github_app.APP_PERMISSIONS), "suspended_at": None}
    control = {"authorized": True, "selected": selected}
    seen, minted, _ = fake(monkeypatch, key, [1800000000])
    # Preserve installation transport while routing the connection protocol separately.
    transport_client = httpx.AsyncClient

    def handle(request):
        path = request.url.path
        if path == "/app":
            assert request.headers["authorization"].startswith("Bearer eyJ")
            return httpx.Response(200, json={"slug": "factory-test"})
        if path == "/login/oauth/access_token":
            assert json.loads(request.content)["client_secret"] == "client-secret-sensitive"
            return httpx.Response(200, json={"access_token": "transient-user-sensitive"})
        if path == "/user/installations":
            assert request.headers["authorization"] == "Bearer transient-user-sensitive"
            return httpx.Response(200, json={"installations": [selected] if control["authorized"] else []})
        if path.startswith("/app/installations/"):
            return httpx.Response(200, json=selected)
        raise AssertionError(path)

    def clients(**kw):
        # github_json uses follow_redirects=False; transport uses timeout only.
        if "follow_redirects" in kw:
            return ASYNC_CLIENT(**kw, transport=httpx.MockTransport(handle))
        return transport_client(**kw)

    monkeypatch.setattr(httpx, "AsyncClient", clients)
    return ws, control


def _start(client, ws, destination="setup"):
    from urllib.parse import parse_qs, urlparse

    root = f"/api/workspaces/{ws}/github/connection"
    response = client.post(root + "/start?destination=" + destination)
    assert response.status_code == 200, response.text
    url = response.json()["url"]
    assert url.startswith("https://github.com/apps/factory-test/installations/new?")
    return root, parse_qs(urlparse(url).query)["state"][0]


def _return(client, root, state, installation="42"):
    from urllib.parse import parse_qs, urlparse

    response = client.get(root + "/return", params={"state": state, "installation_id": installation}, follow_redirects=False)
    assert response.status_code == 303, response.text
    return parse_qs(urlparse(response.headers["location"]).query)["state"][0]


def test_install_oauth_roundtrip_repos_and_no_credentials(connection_github, client, signed_in):
    from sqlalchemy import select
    from aifactory.models import Base, GitHubConnectionState

    ws, _ = connection_github
    root, state = _start(client, ws)
    oauth = _return(client, root, state)
    response = client.get(root + "/oauth", params={"state": oauth, "code": "test-code"}, follow_redirects=False)
    assert response.status_code == 303, response.text
    assert response.headers["location"] == f"/w/{ws}/onboarding"
    metadata = client.get(root).json()
    assert metadata["ready"] and metadata["account"] == "acme"
    assert metadata["granted_permissions"] == github_app.APP_PERMISSIONS
    assert metadata["missing_permissions"] == {}
    assert metadata["manage_url"] == "https://github.com/settings/installations/42"
    repos = client.get(f"/api/workspaces/{ws}/github/repos")
    assert repos.status_code == 200, repos.text
    assert repos.json()["items"][0]["full_name"] == "acme/demo"
    assert "github" in client.get(f"/api/workspaces/{ws}/onboarding").json()["completed_steps"]
    with session_scope() as db:
        assert db.query(GitHubConnectionState).count() == 0
        rows = repr([list(db.execute(select(table))) for table in Base.metadata.sorted_tables])
    assert all(s not in rows + response.text + json.dumps(metadata) for s in
               ["transient-user-sensitive", "client-secret-sensitive", "webhook-sensitive", "installation-sensitive"])
    assert client.get(root + "/oauth", params={"state": oauth, "code": "test-code"}).status_code == 400
    assert client.get(root + "/return", params={"state": state, "installation_id": "42"}).status_code == 400


@pytest.mark.parametrize("failure", ["absent", "forged", "suspended", "wrong-app", "missing-permission"])
def test_installation_must_be_authorized_and_available(connection_github, client, failure):
    ws, control = connection_github
    root, state = _start(client, ws)
    oauth = _return(client, root, state, "43" if failure == "forged" else "42")
    if failure == "absent":
        control["authorized"] = False
    if failure == "suspended":
        control["selected"]["suspended_at"] = "2026-01-01T00:00:00Z"
    if failure == "wrong-app":
        control["selected"]["app_id"] = 999
    if failure == "missing-permission":
        control["selected"]["permissions"]["contents"] = "read"
    response = client.get(root + "/oauth", params={"state": oauth, "code": "test"})
    assert response.status_code == 403, response.text
    with session_scope() as db:
        assert github_app.INSTALLATION_SETTING not in db.get(Workspace, ws).settings


@pytest.mark.parametrize("stage", ["return", "oauth"])
def test_expired_forged_and_wrong_session_state(connection_github, client, stage):
    from datetime import timedelta
    from aifactory.db import utcnow
    from aifactory.models import GitHubConnectionState
    from aifactory.security import token_hash

    ws, _ = connection_github
    root, state = _start(client, ws)
    if stage == "oauth":
        state = _return(client, root, state)
    params = {"state": state, "installation_id": "42", "code": "test"}
    assert client.get(root + "/" + stage, params={**params, "state": state + "x"}).status_code == 400
    # Same user, newly signed-in browser session cannot reuse the intent.
    from fastapi.testclient import TestClient
    other = TestClient(client.app)
    other.post("/auth/dev", json={"email": "ian@example.com", "name": "Ian", "organization": "MindsDB"})
    assert other.get(root + "/" + stage, params=params).status_code == 400
    with session_scope() as db:
        db.get(GitHubConnectionState, token_hash(state)).expires_at = utcnow() - timedelta(seconds=1)
    assert client.get(root + "/" + stage, params=params).status_code == 400


def _webhook(client, payload, event="installation", valid=True):
    import hashlib
    import hmac
    body = json.dumps(payload).encode()
    sig = hmac.new(b"webhook-sensitive", body, hashlib.sha256).hexdigest()
    return client.post("/api/github/webhook", content=body, headers={
        "x-github-event": event, "x-hub-signature-256": "sha256=" + (sig if valid else "0" * 64)})


def test_signed_ping_and_installation_lifecycle(connection_github, client):
    from aifactory.models import GitHubInstallation

    ws, _ = connection_github
    root, state = _start(client, ws)
    oauth = _return(client, root, state)
    assert client.get(root + "/oauth", params={"state": oauth, "code": "test"}, follow_redirects=False).status_code == 303
    assert _webhook(client, {"zen": "hello"}, "ping").status_code == 200
    assert _webhook(client, {}, "ping", valid=False).status_code == 401
    payload = {"action": "suspend", "installation": {"id": 42}}
    assert _webhook(client, payload, valid=False).status_code == 401
    assert client.get(root).json()["ready"]
    assert _webhook(client, {"action": "suspend", "installation": {"id": 999}}).status_code == 200
    assert client.get(root).json()["ready"]
    assert _webhook(client, payload).status_code == 200
    assert not client.get(root).json()["ready"]
    with pytest.raises(GitHubError, match="unavailable"):
        resolve_token(ws)
    assert "github" not in client.get(f"/api/workspaces/{ws}/onboarding").json()["completed_steps"]
    with session_scope() as db:
        assert db.get(GitHubInstallation, "42").webhook_received_at is not None
    assert _webhook(client, {"action": "unsuspend", "installation": {"id": 42}}).status_code == 200
    assert client.get(root).json()["ready"]
    github_app._cache[("42", "test")] = ("old", 9999999999)
    assert _webhook(client, {"action": "removed", "installation": {"id": 42}}, "installation_repositories").status_code == 200
    assert ("42", "test") not in github_app._cache
    assert _webhook(client, {"action": "deleted", "installation": {"id": 42}}).status_code == 200
    assert not client.get(root).json()["ready"]
    assert _webhook(client, {"action": "unsuspend", "installation": {"id": 42}}).status_code == 200
    assert not client.get(root).json()["ready"]


def test_unconfigured_disconnect_restores_token_fallback(app, monkeypatch, client):
    from aifactory.models import Secret
    from aifactory.security import encrypt

    ws, _ = app
    root = f"/api/workspaces/{ws}/github/connection"
    monkeypatch.delenv("AIFACTORY_GITHUB_APP_CLIENT_ID")
    config.reset_settings_cache()
    with session_scope() as db:
        db.add(Secret(workspace_id=ws, kind="github_token", ciphertext=encrypt("saved-pat")))
    assert client.get(root).json()["configured"] is False
    assert client.post(root + "/start").status_code == 503
    with pytest.raises(GitHubError, match="not configured"):
        resolve_token(ws)
    assert client.delete(root).status_code == 200
    assert resolve_token(ws) == "saved-pat"
    assert client.get("/api/meta").json()["auth"]["github_app"] is False


def test_unsafe_destination_is_rejected(connection_github, client):
    ws, _ = connection_github
    assert client.post(f"/api/workspaces/{ws}/github/connection/start?destination=https://evil.test").status_code == 422


def test_global_callbacks_preserve_destination_and_only_bind_initiator(connection_github, client):
    from urllib.parse import parse_qs, urlparse

    ws, _ = connection_github
    with session_scope() as db:
        other = Workspace(name="Unconnected", slug="unconnected", org_id=db.get(Workspace, ws).org_id, settings={})
        db.add(other)
        db.flush()
        other_id = other.id
    root, state = _start(client, ws, "settings")
    response = client.get("/api/github/return", params={"state": state, "installation_id": "42"}, follow_redirects=False)
    assert response.status_code == 303
    query = parse_qs(urlparse(response.headers["location"]).query)
    assert query["redirect_uri"] == [config.get_settings().public_url + "/api/github/oauth"]
    response = client.get("/api/github/oauth", params={"state": query["state"][0], "code": "test"}, follow_redirects=False)
    assert response.status_code == 303
    assert response.headers["location"] == f"/w/{ws}/settings/credentials"
    with session_scope() as db:
        assert db.get(Workspace, other_id).settings == {}
        assert db.get(Workspace, ws).settings[github_app.INSTALLATION_SETTING] == "42"
    assert client.get("/api/github/return", params={"state": state, "installation_id": "42"}).status_code == 400


def test_webhook_missing_permission_invalidates_readiness(connection_github, client):
    ws, _ = connection_github
    root, state = _start(client, ws)
    oauth = _return(client, root, state)
    assert client.get(root + "/oauth", params={"state": oauth, "code": "test"}, follow_redirects=False).status_code == 303
    assert _webhook(client, {"action": "new_permissions_accepted", "installation": {
        "id": 42, "permissions": {"contents": "read"}}}).status_code == 200
    info = client.get(root).json()
    assert not info["ready"]
    assert info["missing_permissions"]["contents"] == "write"
    with pytest.raises(GitHubError, match="missing permissions"):
        resolve_token(ws)


@pytest.mark.parametrize("disconnect_first", [False, True])
def test_reconnect_refreshes_missed_unsuspend_evidence(connection_github, client, disconnect_first):
    from aifactory.models import GitHubInstallation

    ws, _ = connection_github
    root, state = _start(client, ws)
    oauth = _return(client, root, state)
    assert client.get(root + "/oauth", params={"state": oauth, "code": "test"}, follow_redirects=False).status_code == 303
    assert _webhook(client, {"action": "suspend", "installation": {"id": 42}}).status_code == 200
    assert not client.get(root).json()["ready"]
    with pytest.raises(GitHubError, match="unavailable"):
        resolve_token(ws)
    if disconnect_first:
        assert client.delete(root).status_code == 200
    # GitHub is now live/unsuspended, but its unsuspend webhook never arrived.
    root, state = _start(client, ws)
    oauth = _return(client, root, state)
    response = client.get(root + "/oauth", params={"state": oauth, "code": "test"}, follow_redirects=False)
    assert response.status_code == 303, response.text
    assert client.get(root).json()["ready"]
    with session_scope() as db:
        evidence = db.get(GitHubInstallation, "42")
        assert evidence.status == "active"
        assert evidence.permissions == github_app.APP_PERMISSIONS
        assert evidence.webhook_received_at is not None
    assert client.get(f"/api/workspaces/{ws}/github/repos").status_code == 200


@pytest.mark.parametrize("existing", [False, True])
@pytest.mark.parametrize("event,action", [
    ("installation", "suspend"),
    ("installation", "deleted"),
    ("installation_repositories", "removed"),
])
def test_webhook_during_live_lookup_wins_over_reconnect(connection_github, client, monkeypatch, existing, event, action):
    """Deliver a real signed webhook after the live response, before the callback's CAS.

    No sleeps or threads: the hook deterministically exercises both an updated
    evidence row and the first-connection insert race on SQLite and Postgres.
    """
    import hashlib
    import hmac
    from aifactory import github_connection
    from aifactory.models import GitHubInstallation

    ws, _ = connection_github
    if existing:
        root, state = _start(client, ws)
        oauth = _return(client, root, state)
        assert client.get(root + "/oauth", params={"state": oauth, "code": "test"}, follow_redirects=False).status_code == 303
        assert _webhook(client, {"action": "suspend", "installation": {"id": 42}}).status_code == 200
        # Include legacy, additively migrated evidence whose version is still NULL.
        with session_scope() as db:
            db.get(GitHubInstallation, "42").version = None
    root, state = _start(client, ws)
    oauth = _return(client, root, state)
    live_json = github_connection.github_json
    delivered = []

    async def deliver_after_lookup(method, url, **kwargs):
        response = await live_json(method, url, **kwargs)
        if url.endswith("/app/installations/42"):
            payload = {"action": action, "installation": {"id": 42}}
            if event == "installation_repositories":
                payload["installation"]["permissions"] = {"contents": "read"}
            body = json.dumps(payload).encode()
            signature = hmac.new(b"webhook-sensitive", body, hashlib.sha256).hexdigest()
            async with ASYNC_CLIENT(transport=httpx.ASGITransport(app=client.app), base_url="http://testserver") as sender:
                receipt = await sender.post("/api/github/webhook", content=body, headers={
                    "x-github-event": event, "x-hub-signature-256": "sha256=" + signature})
            assert receipt.status_code == 200, receipt.text
            with session_scope() as db:
                evidence = db.get(GitHubInstallation, "42")
                delivered.append((evidence.status, evidence.version, evidence.permissions, evidence.webhook_received_at))
        return response

    monkeypatch.setattr(github_connection, "github_json", deliver_after_lookup)
    response = client.get(root + "/oauth", params={"state": oauth, "code": "test"}, follow_redirects=False)
    assert response.status_code == 409, response.text
    assert len(delivered) == 1
    with session_scope() as db:
        evidence = db.get(GitHubInstallation, "42")
        assert (evidence.status, evidence.version, evidence.permissions, evidence.webhook_received_at) == delivered[0]
        binding = db.get(Workspace, ws).settings.get(github_app.INSTALLATION_SETTING)
        assert binding == ("42" if existing else None)
    assert not client.get(root).json()["ready"]
    assert "github" not in client.get(f"/api/workspaces/{ws}/onboarding").json()["completed_steps"]
