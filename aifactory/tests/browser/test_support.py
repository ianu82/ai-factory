"""Fast harness checks: no browser needed by these tests."""
from __future__ import annotations

import asyncio
import json
import multiprocessing
from pathlib import Path
import re

import pytest

from tests.browser.conftest import Diagnostics, dependencies_current
from tests.browser.seed import KINDS, STATUSES
from tests.browser.server import BrowserServer, isolated_environment


def test_environment_is_allowlisted(tmp_path, monkeypatch):
    for key in ("AIFACTORY_TEST_DATABASE_URL", "DATABASE_URL", "GITHUB_TOKEN", "GH_TOKEN", "OPENAI_API_KEY",
                "AIFACTORY_BOOTSTRAP_OPENAI_API_KEY", "AIFACTORY_BOOTSTRAP_CONFIG", "MINDSHUB_API_KEY",
                "MINDSHUB_OIDC_CLIENT_SECRET", "AIFACTORY_GITHUB_APP_PRIVATE_KEY", "DS_POSTGRES_PROD__PASSWORD"):
        monkeypatch.setenv(key, "must-not-reach-child")
    env = isolated_environment(tmp_path, "http://127.0.0.1:12345")
    assert "must-not-reach-child" not in env.values()
    assert env["DATABASE_URL"] == f"sqlite:///{tmp_path / 'factory.db'}"
    assert env["AIFACTORY_JOBS_DIR"] == str(tmp_path / "jobs")
    assert env["AIFACTORY_PUBLIC_URL"] == env["AIFACTORY_INTERNAL_URL"]
    assert env["AIFACTORY_ORCHESTRATOR"] == "0"


def test_outcome_seed_tracks_ui_states():
    source = (Path(__file__).resolve().parents[2] / "web/src/format.ts").read_text()
    status_block = source.split("export const OUTCOME_STATUS:")[1].split("};")[0]
    assert set(re.findall(r"^  (\w+):", status_block, re.MULTILINE)) == set(STATUSES)
    assert set(KINDS) == {"approve_plan", "approve_merge", "question", "unblock", "budget", "approve_release"}


def test_dependency_detection_requires_matching_lock(tmp_path):
    assert not dependencies_current(tmp_path)
    modules = tmp_path / "node_modules"
    (modules / ".bin").mkdir(parents=True)
    (modules / ".bin/vite").touch()
    (modules / "vite").mkdir()
    (modules / "vite/package.json").write_text("{}")
    packages = {"node_modules/vite": {"version": "1", "integrity": "dummy"}}
    (tmp_path / "package-lock.json").write_text(json.dumps({"packages": {"": {}, **packages}}))
    (modules / ".package-lock.json").write_text(json.dumps({"packages": packages}))
    assert dependencies_current(tmp_path)
    packages["node_modules/vite"]["version"] = "old"
    (modules / ".package-lock.json").write_text(json.dumps({"packages": packages}))
    assert not dependencies_current(tmp_path)


def test_startup_failure_is_diagnostic_and_reaps_child(tmp_path):
    build = tmp_path / "bad-build"
    build.mkdir()
    (build / "index.html").write_text("<html></html>")
    # Missing assets makes StaticFiles fail during create_app in the subprocess.
    before = {child.pid for child in multiprocessing.active_children()}
    with pytest.raises(RuntimeError, match="Startup log:") as error:
        BrowserServer(build, tmp_path / "data", tmp_path / "startup.log", startup_timeout=5)
    assert "assets" in str(error.value)
    assert {child.pid for child in multiprocessing.active_children()} == before
    assert (tmp_path / "startup.log").is_file()


def test_sync_playwright_does_not_leak_into_later_async_tests():
    # This file runs after the browser smoke tests in the normal subtree order.
    # A session-scoped sync_playwright leaves its loop active and breaks asyncio.run.
    async def unrelated_async_operation():
        return "independent"

    assert asyncio.run(unrelated_async_operation()) == "independent"


def test_failure_diagnostics_cover_all_pages_and_unique_checkpoints(tmp_path):
    class Page:
        def __init__(self, url):
            self.url = url

        def is_closed(self):
            return False

        def screenshot(self, *, path, full_page, timeout):
            assert full_page and timeout == 5000
            Path(path).write_bytes(b"captured")

    class Context:
        pages = [Page("http://localhost/inbox"), Page("http://localhost/outcomes")]

    class Node:
        nodeid = "tests/browser/test_flow.py::test_flow[375x812]"

    diagnostics = Diagnostics(Node(), tmp_path, {"width": 375, "height": 812})
    diagnostics.context = Context()
    diagnostics.capture("checkpoint")
    diagnostics.capture("call")
    paths = [path for _, path in diagnostics.captures]
    assert len(set(paths)) == 4
    assert all(path.is_file() and "375x812" in path.name for path in paths)
    report = diagnostics.report()
    assert Node.nodeid in report
    assert "http://localhost/inbox" in report and "http://localhost/outcomes" in report
    assert all(f"PNG: {path}" in report for path in paths)
