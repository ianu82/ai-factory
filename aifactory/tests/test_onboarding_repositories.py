"""Repository discovery/review is tenant scoped; detection never implies consent to boot."""
from __future__ import annotations

import asyncio
import base64
import copy
import json

import pytest

from aifactory import api as api_module
from aifactory.db import session_scope
from aifactory.github import GitHub, GitHubError
from aifactory.models import Event, Membership, Repository
from aifactory.repo_detect import detect


DETECTED = {
    "setup": ["npm ci"], "checks": ["npm test"], "smoke": ["npm run build"],
    "protected_paths": ["billing/**"], "context": "Keep these notes", "detected": ["Node project (npm)"],
    "qa": {"start": 'npm run dev -- --host 0.0.0.0 --port "$PORT"', "ready_path": "/login",
           "env": {"MODE": "test"}, "pages": ["/login"]},
    "release": {"health_url": "https://example.test/health"},
    "start_detection": {"available": True, "requires_confirmation": True},
}


@pytest.fixture
def github(monkeypatch):
    class FakeGH:
        push = True
        error = None
        calls = []

        def __init__(self, token):
            pass

        async def repo(self, name):
            self.calls.append(name)
            if self.error:
                raise self.error
            return {"full_name": "Acme/App", "default_branch": "trunk", "permissions": {"push": self.push}}

    async def fake_detect(gh, name, ref):
        assert name == "Acme/App" and ref == "trunk"
        return copy.deepcopy(DETECTED)

    monkeypatch.setattr(api_module, "GitHub", FakeGH)
    monkeypatch.setattr(api_module, "resolve_token", lambda ws: "private-token")
    monkeypatch.setattr(api_module, "detect", fake_detect)
    return FakeGH


def path(signed_in):
    return f"/api/workspaces/{signed_in['workspaces'][0]['id']}"


def test_preview_normalizes_detects_and_does_not_persist(client, signed_in, github):
    with session_scope() as s:
        before = s.query(Event).count()
    for _ in range(2):
        response = client.post(path(signed_in) + "/repositories/preview",
                               json={"full_name": " https://github.com/acme/app.git/ "})
        assert response.status_code == 200, response.text
        result = response.json()
        assert result["full_name"] == "Acme/App" and result["default_branch"] == "trunk"
        assert result["detected"] == DETECTED["detected"]
        assert result["config"] == DETECTED
        assert result["release_selection"]["mode"] == "on_merge"
        assert not result["release_selection"]["confirmed"]
        assert "private-token" not in response.text
    assert github.calls == ["acme/app", "acme/app"]
    with session_scope() as s:
        assert s.query(Repository).count() == 0
        assert s.query(Event).count() == before


@pytest.mark.parametrize("action", ["", "/preview"])
def test_validation_and_access_failures_do_not_persist(client, signed_in, github, action):
    url = path(signed_in) + "/repositories" + action
    for name in ("wrong", "a/b/c", "https://other.test/acme/app", "acme/app?token=x"):
        assert client.post(url, json={"full_name": name}).status_code == 400
    assert github.calls == []
    github.push = False
    assert client.post(url, json={"full_name": "acme/app"}).status_code == 400
    github.push = True
    github.error = GitHubError("Not found", 404)
    assert client.post(url, json={"full_name": "acme/app"}).status_code == 400
    with session_scope() as s:
        assert s.query(Repository).count() == 0


@pytest.mark.parametrize("review", ["accept", "override", "omit", "empty"])
def test_connect_accept_override_or_omit_start_and_preserve_config(client, signed_in, github, review):
    body = {"full_name": "acme/app"}
    if review in ("accept", "override", "empty"):
        start = {"accept": DETECTED["qa"]["start"], "override": "  custom-server --port $PORT  ", "empty": ""}[review]
        body["config"] = {"setup": ["  install  ", ""], "checks": [" test "],
                          "qa": {"start": start}, "release": {"mode": "on_merge"}}
    response = client.post(path(signed_in) + "/repositories", json=body)
    assert response.status_code == 200, response.text
    config = response.json()["config"]
    for key in ("smoke", "context", "protected_paths", "detected"):
        assert config[key] == DETECTED[key]
    for key in ("env", "ready_path", "pages"):
        assert config["qa"][key] == DETECTED["qa"][key]
    if review == "omit":
        assert "start" not in config["qa"]
        assert config["setup"] == DETECTED["setup"] and config["checks"] == DETECTED["checks"]
        assert "mode" not in config["release"]
    else:
        assert config["setup"] == ["install"] and config["checks"] == ["test"]
        assert config["qa"]["start"] == start.strip()
        assert config["release"]["mode"] == "on_merge"
    with session_scope() as s:
        assert s.query(Repository).count() == 1
        assert s.query(Event).filter_by(kind="repository_connected").count() == 1
    assert client.post(path(signed_in) + "/repositories", json=body).status_code == 409
    # Even a differently cased alias resolves to the same canonical identity.
    assert client.post(path(signed_in) + "/repositories", json={"full_name": "ACME/APP"}).status_code == 409


def test_setup_only_override_does_not_accept_start(client, signed_in, github):
    result = client.post(path(signed_in) + "/repositories", json={
        "full_name": "acme/app", "config": {"setup": ["reviewed"]}}).json()
    assert result["config"]["setup"] == ["reviewed"]
    assert "start" not in result["config"]["qa"]


def test_patch_replaces_nested_settings_to_allow_clearing(client, signed_in, github):
    repo = client.post(path(signed_in) + "/repositories", json={"full_name": "acme/app"}).json()
    url = path(signed_in) + "/repositories/" + repo["id"]
    assert client.patch(url, json={"config": {
        "merge": {"mode": "approval"}, "release": {"mode": "on_merge", "health_url": "https://x.test"},
    }}).status_code == 200
    config = client.patch(url, json={"config": {"merge": {}, "release": {}, "qa": {}}}).json()["config"]
    assert config["merge"] == config["release"] == config["qa"] == {}
    assert config["context"] == DETECTED["context"] and config["smoke"] == DETECTED["smoke"]


@pytest.mark.parametrize("role", ["owner", "admin", "member", "viewer"])
def test_admin_only_preview_connect_discovery(client, signed_in, github, monkeypatch, role):
    async def page(self, page, per_page):
        return {"items": [], "next_page": None}
    monkeypatch.setattr(github, "accessible_repos_page", page, raising=False)
    with session_scope() as s:
        s.query(Membership).filter_by(user_id=signed_in["user"]["id"]).one().role = role
    expected = 200 if role in ("owner", "admin") else 403
    assert client.get(path(signed_in) + "/github/repos").status_code == expected
    for suffix in ("/preview", ""):
        assert client.post(path(signed_in) + "/repositories" + suffix, json={"full_name": "acme/app"}).status_code == expected


def test_tenant_and_signed_out_restrictions(client, signed_in, github):
    url = path(signed_in)
    client.post("/auth/logout")
    for suffix in ("/repositories", "/repositories/preview"):
        assert client.post(url + suffix, json={"full_name": "acme/app"}).status_code == 401
    assert client.get(url + "/github/repos").status_code == 401
    client.post("/auth/dev", json={"email": "other@example.test", "organization": "Other"})
    for suffix in ("/repositories", "/repositories/preview"):
        assert client.post(url + suffix, json={"full_name": "acme/app"}).status_code == 404
    assert client.get(url + "/github/repos").status_code == 404
    assert github.calls == []


def test_paging_and_search_find_accessible_repositories_after_200(client, signed_in, monkeypatch):
    calls = []

    async def request(self, method, endpoint, **kwargs):
        assert endpoint == "/user/repos"
        params = kwargs["params"]
        assert params["sort"] == "pushed"
        calls.append(params["page"])
        repos = [{"full_name": f"acme/repo-{i}", "permissions": {"push": True}} for i in range(205)]
        start = (params["page"] - 1) * params["per_page"]
        return repos[start:start + params["per_page"]]

    monkeypatch.setattr(GitHub, "request", request)
    monkeypatch.setattr(api_module, "resolve_token", lambda ws: "token")
    url = path(signed_in) + "/github/repos"
    first = client.get(url).json()
    assert len(first["items"]) == 100 and first["next_page"] == 2
    second = client.get(url + "?page=2").json()
    third = client.get(url + f"?page={second['next_page']}").json()
    assert len(third["items"]) == 5 and third["next_page"] is None
    calls.clear()
    result = client.get(url + "?q=ACME/REPO-204").json()
    assert [r["full_name"] for r in result["items"]] == ["acme/repo-204"]
    assert calls == [1, 2, 3]
    assert result["items"][0]["can_push"]
    assert client.get(url + "?q=not-found").json() == {"items": [], "next_page": None}
    assert client.get(url + "?page=0").status_code == 422


def test_discovery_preserves_items_and_error(client, signed_in):
    result = client.get(path(signed_in) + "/github/repos").json()
    assert result["items"] == [] and result["error"] and result["next_page"] is None


class ManifestGH:
    def __init__(self, files):
        self.files = files

    async def request(self, method, endpoint, **kwargs):
        name = endpoint.split("/contents/")[1]
        assert kwargs["params"]["ref"] == "trunk"
        if name not in self.files:
            raise GitHubError("missing", 404)
        return {"encoding": "base64", "content": base64.b64encode(self.files[name].encode()).decode()}


@pytest.mark.parametrize("lock,manager,install", [
    ("package-lock.json", "npm run", "npm ci --no-audit --no-fund"),
    ("pnpm-lock.yaml", "pnpm", "corepack enable && pnpm install --frozen-lockfile"),
    ("yarn.lock", "yarn", "corepack enable && yarn install --frozen-lockfile"),
])
@pytest.mark.parametrize("script,flags", [("vite", '--host 0.0.0.0 --port "$PORT"'),
                                         ("next dev", '-H 0.0.0.0 -p "$PORT"'),
                                         ("astro dev", '--host 0.0.0.0 --port "$PORT"')])
def test_node_manifest_detects_reviewable_start_and_never_adds_it_to_smoke(lock, manager, install, script, flags):
    files = {"package.json": json.dumps({"scripts": {"dev": script, "test": "vitest run", "build": "build", "lint": "eslint ."}}),
             lock: "lock contents", "AGENTS.md": "Team notes"}
    config = asyncio.run(detect(ManifestGH(files), "acme/app", "trunk"))
    assert config["setup"] == [install]
    assert config["checks"] == [f"{manager} lint", f"{manager} test"]
    separator = " --" if manager == "npm run" else ""
    assert config["qa"]["start"] == f"{manager} dev{separator} {flags}"
    assert config["start_detection"]["available"] and config["start_detection"]["requires_confirmation"]
    assert config["smoke"] == [f"{manager} build", f"{manager} test"]
    assert config["context"] == "Team notes"


@pytest.mark.parametrize("files", [
    {}, {"pyproject.toml": "[project]\nname='lib'"}, {"go.mod": "module lib"},
    {"package.json": '{"scripts":{"start":"node index.js"}}'},
    {"package.json": '{"dependencies":{"react":"*"}}'}, {"package.json": "[]"},
    {"package.json": '{"scripts":{"dev":42}}'}, {"package.json": '{"scripts":[]}'},
    {"package.json": "not json"},
])
def test_unknown_manifests_never_guess_start(files):
    config = asyncio.run(detect(ManifestGH(files), "acme/lib", "trunk"))
    assert not config.get("qa", {}).get("start")
    assert not config["start_detection"]["available"] and config["start_detection"]["reason"]


def test_next_production_start_requires_build_and_is_not_smoke():
    files = {"package.json": json.dumps({"scripts": {"start": "next start", "build": "next build"}})}
    config = asyncio.run(detect(ManifestGH(files), "acme/app", "trunk"))
    assert config["qa"]["start"] == 'npm run build && npm run start -- -H 0.0.0.0 -p "$PORT"'
    assert config["smoke"] == ["npm run build"]
    files["package.json"] = json.dumps({"scripts": {"start": "next start"}})
    config = asyncio.run(detect(ManifestGH(files), "acme/app", "trunk"))
    assert not config["start_detection"]["available"]
    assert not config.get("qa", {}).get("start")


@pytest.mark.parametrize("config", [{"qa": {}}, {"qa": None}, {"checks": []}])
def test_unreviewed_start_is_not_accepted_by_other_overrides(client, signed_in, github, config):
    result = client.post(path(signed_in) + "/repositories", json={"full_name": "acme/app", "config": config})
    assert result.status_code == 200
    assert "start" not in result.json()["config"]["qa"]


@pytest.mark.parametrize("lock,run,separator", [
    ("package-lock.json", "npm run", " --"),
    ("pnpm-lock.yaml", "pnpm", ""),
    ("yarn.lock", "yarn", ""),
])
@pytest.mark.parametrize("script,build,flags", [
    ("vite preview", "vite build", '--host 0.0.0.0 --port "$PORT"'),
    ("astro preview", "astro build", '--host 0.0.0.0 --port "$PORT"'),
    ("next start", "next build", '-H 0.0.0.0 -p "$PORT"'),
    ("nuxt start", "nuxt build", '--host 0.0.0.0 --port "$PORT"'),
])
def test_preview_and_production_servers_build_before_start(lock, run, separator, script, build, flags):
    files = {lock: "lock contents"}
    for build_script in (build, None, "", "   "):
        scripts = {"start": script}
        if build_script is not None:
            scripts["build"] = build_script
        files["package.json"] = json.dumps({"scripts": scripts})
        config = asyncio.run(detect(ManifestGH(files), "acme/app", "trunk"))
        if build_script and build_script.strip():
            assert config["qa"]["start"] == f"{run} build && {run} start{separator} {flags}"
            assert config["start_detection"]["available"]
            assert config["start_detection"]["requires_confirmation"]
            assert config["smoke"] == [f"{run} build"]
        else:
            assert not config.get("qa", {}).get("start")
            assert not config["start_detection"]["available"]
            assert "requires a build script" in config["start_detection"]["reason"]


def test_vite_preview_serves_fresh_checkout_after_detected_build(tmp_path):
    """Execute the suggestion with the web app's Vite, without preexisting dist.

    Manifest-only coverage above always runs; this integration check also runs
    when the existing web dependencies and Node are installed (no downloads).
    """
    import os
    from pathlib import Path
    import shutil
    import signal
    import socket
    import subprocess
    import time
    from urllib.error import URLError
    from urllib.request import urlopen

    vite_bin = Path(__file__).resolve().parents[1] / "web/node_modules/.bin"
    if not (vite_bin / "vite").exists() or not shutil.which("npm") or not shutil.which("node"):
        pytest.skip("Existing web Vite dependencies and Node are required for executable fixture")
    package = json.dumps({"scripts": {"start": "vite preview", "build": "vite build"}, "type": "module"})
    (tmp_path / "package.json").write_text(package)
    (tmp_path / "index.html").write_text("<!doctype html><title>Fresh checkout preview</title><h1>Build succeeded</h1>")
    config = asyncio.run(detect(ManifestGH({"package.json": package}), "acme/app", "trunk"))
    assert not (tmp_path / "dist").exists()
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    env = {**os.environ, "PATH": f"{vite_bin}{os.pathsep}{os.environ['PATH']}", "PORT": str(port)}
    with (tmp_path / "preview.log").open("w+") as log:
        process = subprocess.Popen(config["qa"]["start"], shell=True, cwd=tmp_path, env=env,
                                   stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
        try:
            deadline = time.monotonic() + 30
            while time.monotonic() < deadline:
                if process.poll() is not None:
                    break
                try:
                    with urlopen(f"http://127.0.0.1:{port}/", timeout=1) as response:
                        assert response.status == 200
                        assert "Build succeeded" in response.read().decode()
                    assert (tmp_path / "dist/index.html").exists()
                    return
                except (URLError, TimeoutError):
                    time.sleep(0.1)
            log.seek(0)
            pytest.fail(f"Detected Vite preview did not serve a fresh checkout: {log.read()}")
        finally:
            if process.poll() is None:
                os.killpg(process.pid, signal.SIGTERM)
            process.wait(timeout=5)
