"""Release checker: promotes staging's commit to `release` only when it is ready, then confirms production."""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

import httpx
import pytest

from aifactory.release import ReleaseConfig, Releaser, smoke

REPO = "acme/factory"
STAGING = "https://staging.example"
PROD = "https://prod.example"
OLD = "a" * 40
NEW = "b" * 40
NOW = datetime(2026, 11, 9, 22, 0, tzinfo=timezone.utc)
APP = '<html><div id="root"></div><script type="module" src="/assets/index-1.js"></script></html>'


class Clock:
    def __init__(self) -> None:
        self.t = NOW

    def now(self) -> datetime:
        return self.t

    def sleep(self, seconds: float) -> None:
        self.t += timedelta(seconds=seconds)


class World:
    """GitHub, staging and production as the release checker sees them."""

    def __init__(self) -> None:
        self.clock = Clock()
        self.release = OLD
        self.staging = {"ok": True, "commit": NEW, "started_at": (NOW - timedelta(minutes=45)).isoformat()}
        self.staging_down_for = 0  # health calls that fail before staging answers
        self.files = ["aifactory/src/aifactory/api.py"]
        self.status = "ahead"
        self.on_main = "identical"
        self.checks = [{"name": "test", "status": "completed", "conclusion": "success"},
                       {"name": "postgres", "status": "completed", "conclusion": "success"}]
        self.asset_status = 200
        self.me_status = 401
        self.live_after = 600  # seconds after the release that production reports the new commit
        self.restart_after: int | None = None  # seconds after the release that production restarts
        self.released_at: datetime | None = None
        self.patches: list[dict] = []
        self.issues: list[dict] = []
        self.comments: list[dict] = []
        self.open_issues: list[dict] = []
        self.github_calls = 0

    def github(self, request: httpx.Request) -> httpx.Response:
        self.github_calls += 1
        path, method = request.url.path, request.method
        if path == f"/repos/{REPO}/git/ref/heads/release":
            return httpx.Response(200, json={"object": {"sha": self.release}})
        if path == f"/repos/{REPO}/compare/{OLD}...{NEW}":
            return httpx.Response(200, json={"status": self.status, "ahead_by": 3,
                                             "files": [{"filename": f} for f in self.files]})
        if path == f"/repos/{REPO}/compare/{NEW}...main":
            return httpx.Response(200, json={"status": self.on_main})
        if path == f"/repos/{REPO}/commits/{NEW}/check-runs":
            return httpx.Response(200, json={"check_runs": self.checks})
        if path == f"/repos/{REPO}/git/refs/heads/release" and method == "PATCH":
            body = json.loads(request.content)
            self.patches.append(body)
            self.release, self.released_at = body["sha"], self.clock.now()
            return httpx.Response(200, json={"object": {"sha": body["sha"]}})
        if path == f"/repos/{REPO}/issues" and method == "GET":
            return httpx.Response(200, json=self.open_issues)
        if path == f"/repos/{REPO}/issues" and method == "POST":
            self.issues.append(json.loads(request.content))
            return httpx.Response(201, json={"number": 9})
        if path.startswith(f"/repos/{REPO}/issues/") and path.endswith("/comments") and method == "POST":
            self.comments.append(json.loads(request.content))
            return httpx.Response(201, json={})
        return httpx.Response(404, json={"message": f"unexpected {method} {path}"})

    def web(self, request: httpx.Request) -> httpx.Response:
        url = str(request.url)
        if url == f"{STAGING}/api/health":
            if self.staging_down_for > 0:
                self.staging_down_for -= 1
                return httpx.Response(502)
            return httpx.Response(200, json=self.staging)
        if url == f"{PROD}/api/health":
            if self.released_at is None or self.clock.now() < self.released_at + timedelta(seconds=self.live_after):
                return httpx.Response(200, json={"ok": True, "commit": OLD,
                                                 "started_at": (NOW - timedelta(hours=2)).isoformat()})
            started = self.released_at + timedelta(seconds=self.live_after)
            if self.restart_after is not None and self.clock.now() >= self.released_at + timedelta(seconds=self.restart_after):
                started = self.released_at + timedelta(seconds=self.restart_after)
            return httpx.Response(200, json={"ok": True, "commit": NEW, "started_at": started.isoformat()})
        if url == f"{STAGING}/":
            return httpx.Response(200, text=APP)
        if url == f"{STAGING}/assets/index-1.js":
            return httpx.Response(self.asset_status, text="x" * 5000)
        if url == f"{STAGING}/api/meta":
            return httpx.Response(200, json={"auth": {"api_key": True}})
        if url == f"{STAGING}/api/me":
            return httpx.Response(self.me_status, json={"detail": "Not signed in"})
        return httpx.Response(404)

    def clients(self) -> tuple[httpx.Client, httpx.Client]:
        gh = httpx.Client(base_url="https://api.github.com", transport=httpx.MockTransport(self.github))
        return gh, httpx.Client(transport=httpx.MockTransport(self.web))

    def run(self, dry_run: bool = False, **settings):
        cfg = ReleaseConfig(repo=REPO, staging_url=STAGING, production_url=PROD, token="t", **settings)
        gh, web = self.clients()
        return Releaser(cfg, gh, web, sleep=self.clock.sleep, now=self.clock.now).run(dry_run=dry_run)


@pytest.fixture
def world() -> World:
    return World()


def test_releases_a_soaked_candidate_and_confirms_production_stays_up(world):
    result = world.run()
    assert result.status == "released", result.as_dict()
    assert world.patches == [{"sha": NEW, "force": False}]  # fast-forward only
    assert world.issues == [] and world.comments == []
    assert any("Production stayed healthy" in s for s in result.steps)


def test_staging_must_run_the_candidate_for_the_soak_period(world):
    world.staging["started_at"] = (NOW - timedelta(minutes=10)).isoformat()
    result = world.run()
    assert result.status == "held" and "10 of 30 minutes" in result.detail
    assert world.patches == [] and world.issues == []


def test_no_factory_code_changed_means_no_release_and_no_restart(world):
    world.files = ["README.md", "docs/v1-controller.md"]
    result = world.run()
    assert result.status == "nothing" and "no restart needed" in result.detail
    assert world.patches == []


def test_nothing_to_do_when_release_already_runs_staging_commit(world):
    world.release = NEW
    assert world.run().status == "nothing"
    assert world.patches == []


def test_pending_checks_hold_the_release_without_an_issue(world):
    world.checks.append({"name": "codex", "status": "in_progress", "conclusion": None})
    result = world.run()
    assert result.status == "held" and "codex" in result.detail
    assert world.patches == [] and world.issues == []


def test_failed_checks_block_the_release_and_open_an_issue(world):
    world.checks[1]["conclusion"] = "failure"
    result = world.run()
    assert result.status == "failed" and "postgres" in result.detail
    assert world.patches == []
    assert len(world.issues) == 1 and NEW[:7] in world.issues[0]["title"]


def test_a_repeated_failure_comments_on_the_open_issue(world):
    world.checks[1]["conclusion"] = "failure"
    world.open_issues = [{"number": 5, "title": f"Release check failed at {NEW[:7]}"}]
    world.run()
    assert world.issues == [] and len(world.comments) == 1


def test_staging_smoke_failure_blocks_the_release(world):
    world.asset_status = 404
    result = world.run()
    assert result.status == "failed" and "/assets/index-1.js" in result.detail
    assert world.patches == [] and len(world.issues) == 1


def test_smoke_reports_auth_that_lets_anonymous_calls_through(world):
    world.me_status = 200
    gh, web = world.clients()
    problems = smoke(web, STAGING)
    assert problems and "expected 401" in problems[0]


def test_staging_briefly_down_during_a_deploy_is_retried(world):
    world.staging_down_for = 2
    assert world.run().status == "released"


def test_staging_that_stays_down_fails(world):
    world.staging_down_for = 100
    result = world.run()
    assert result.status == "failed" and "Staging is not healthy" in result.detail
    assert world.patches == []


def test_production_that_never_reports_the_release_is_reported(world):
    world.live_after = 3 * 3600
    result = world.run()
    assert result.status == "failed" and "did not report" in result.detail
    assert world.patches == [{"sha": NEW, "force": False}]
    assert len(world.issues) == 1


def test_production_restarting_right_after_the_release_is_reported(world):
    world.restart_after = world.live_after + 120
    result = world.run()
    assert result.status == "failed" and "restarted" in result.detail
    assert len(world.issues) == 1


def test_release_only_moves_forward(world):
    world.status = "diverged"
    result = world.run()
    assert result.status == "failed" and "diverged" in result.detail
    assert world.patches == []


def test_candidate_must_be_on_main(world):
    world.on_main = "diverged"
    assert world.run().status == "failed"
    assert world.patches == []


def test_paused_releases_touch_nothing(world):
    result = world.run(paused=True)
    assert result.status == "held" and world.github_calls == 0


def test_dry_run_checks_everything_but_does_not_move_release(world):
    result = world.run(dry_run=True)
    assert result.status == "would_release"
    assert world.patches == []


def test_health_reports_when_the_process_started(client):
    body = client.get("/api/health").json()
    assert datetime.fromisoformat(body["started_at"]).tzinfo is not None


def test_release_check_needs_its_settings(monkeypatch):
    from aifactory.cli import main

    for name in ("AIFACTORY_STAGING_URL", "AIFACTORY_PRODUCTION_URL", "AIFACTORY_RELEASE_GITHUB_TOKEN"):
        monkeypatch.delenv(name, raising=False)
    with pytest.raises(SystemExit) as exit_:
        main(["release-check"])
    assert exit_.value.code == 2
