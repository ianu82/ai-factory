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


@pytest.fixture
def app(monkeypatch, signed_in):
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
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
            assert "statuses" not in request.content.decode()
            if fail["mint"]:
                return httpx.Response(403, json={"message": encoded + " client-secret-sensitive"})
            token = f"installation-sensitive-{len(minted)}"
            minted.append(token)
            return httpx.Response(201, json={"token": token, "expires_at": datetime.fromtimestamp(
                clock[0] + 120, timezone.utc).isoformat()})
        assert request.headers["authorization"] == "Bearer " + minted[-1]
        assert path != "/user" and not path.endswith("/status") and "/statuses/" not in path
        if fail["api"]:
            return httpx.Response(401, json={"message": minted[-1]})
        if path == "/installation/repositories":
            page = int(request.url.params["page"])
            size = int(request.url.params["per_page"])
            return httpx.Response(200, json={"repositories": [
                {"full_name": "acme/demo" if i == 0 else f"acme/r{i}"} for i in range(size if page == 1 else 1)], "total_count": size + 1})
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


async def test_sign_mint_pagination_refresh_and_no_status(app, monkeypatch):
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
    assert (await gh.checks("acme/demo", "sha")).state == "none"
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
