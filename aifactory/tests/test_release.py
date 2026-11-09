"""Release checker: short, restart-safe runs that confirm production and promote staging's commit."""

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
OTHER = "c" * 40
NOW = datetime(2026, 11, 9, 22, 0, tzinfo=timezone.utc)
APP = '<html><div id="root"></div><script type="module" src="/assets/index-1.js"></script></html>'
HOLD = "release-hold"


def iso(moment: datetime) -> str:
    return moment.isoformat()


class Clock:
    def __init__(self) -> None:
        self.t = NOW

    def now(self) -> datetime:
        return self.t

    def sleep(self, seconds: float) -> None:
        self.t += timedelta(seconds=seconds)


class World:
    """GitHub, staging and production as the release checker sees them. Defaults: a confirmed release three
    hours old, and staging soaked on a newer commit that changes Factory code."""

    def __init__(self) -> None:
        self.clock = Clock()
        self.release = OLD
        self.moved_at: datetime | None = NOW - timedelta(hours=3)
        self.marks: dict[str, str] = {OLD: "success"}
        self.mark_posts: list[tuple[str, str]] = []
        self.production = {"ok": True, "commit": OLD, "started_at": iso(NOW - timedelta(hours=3))}
        self.staging = {"ok": True, "commit": NEW, "started_at": iso(NOW - timedelta(minutes=45))}
        self.staging_down_for = 0
        self.files = ["aifactory/src/aifactory/api.py"]
        self.status = "ahead"
        self.on_main = "identical"
        self.checks = [{"name": "test", "status": "completed", "conclusion": "success"},
                       {"name": "postgres", "status": "completed", "conclusion": "success"}]
        self.asset_status = 200
        self.me_status = 401
        self.holds: list[dict] = []
        self.open_issues: list[dict] = []
        self.labels: set[str] = set()
        self.issues: list[dict] = []
        self.patches: list[dict] = []
        self.github_calls = 0

    def github(self, request: httpx.Request) -> httpx.Response:
        self.github_calls += 1
        path, method, params = request.url.path, request.method, request.url.params
        base = f"/repos/{REPO}"
        if path == f"{base}/git/ref/heads/release":
            return httpx.Response(200, json={"object": {"sha": self.release}})
        if path == f"{base}/activity":
            return httpx.Response(200, json=[{"timestamp": iso(self.moved_at)}] if self.moved_at else [])
        if path.startswith(f"{base}/commits/") and path.endswith("/status"):
            sha = path.split("/")[-2]
            statuses = [{"context": "factory/release", "state": self.marks[sha]}] if sha in self.marks else []
            return httpx.Response(200, json={"statuses": [{"context": "ci/other", "state": "success"}, *statuses]})
        if path.startswith(f"{base}/statuses/") and method == "POST":
            sha, body = path.split("/")[-1], json.loads(request.content)
            assert body["context"] == "factory/release"
            self.marks[sha] = body["state"]
            self.mark_posts.append((sha, body["state"]))
            return httpx.Response(201, json={})
        if path == f"{base}/issues" and method == "GET":
            if params.get("labels") == HOLD:
                return httpx.Response(200, json=self.holds)
            return httpx.Response(200, json=self.open_issues + self.holds)
        if path == f"{base}/issues" and method == "POST":
            body = json.loads(request.content)
            self.issues.append(body)
            issue = {"number": 100 + len(self.issues), "title": body["title"]}
            (self.holds if HOLD in body.get("labels", []) else self.open_issues).append(issue)
            return httpx.Response(201, json=issue)
        if path == f"{base}/labels/{HOLD}":
            return httpx.Response(200 if HOLD in self.labels else 404, json={})
        if path == f"{base}/labels" and method == "POST":
            self.labels.add(json.loads(request.content)["name"])
            return httpx.Response(201, json={})
        if path == f"{base}/compare/{OLD}...{NEW}":
            return httpx.Response(200, json={"status": self.status, "ahead_by": 3,
                                             "files": [{"filename": f} for f in self.files]})
        if path == f"{base}/compare/{NEW}...main":
            return httpx.Response(200, json={"status": self.on_main})
        if path == f"{base}/commits/{NEW}/check-runs":
            return httpx.Response(200, json={"check_runs": self.checks})
        if path == f"{base}/git/refs/heads/release" and method == "PATCH":
            body = json.loads(request.content)
            self.patches.append(body)
            self.release, self.moved_at = body["sha"], self.clock.now()
            return httpx.Response(200, json={"object": {"sha": body["sha"]}})
        return httpx.Response(404, json={"message": f"unexpected {method} {path}"})

    def web(self, request: httpx.Request) -> httpx.Response:
        url = str(request.url)
        if url == f"{STAGING}/api/health":
            if self.staging_down_for > 0:
                self.staging_down_for -= 1
                return httpx.Response(502)
            return httpx.Response(200, json=self.staging)
        if url == f"{PROD}/api/health":
            return httpx.Response(200, json=self.production)
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


# --- promotion ---------------------------------------------------------------------------------------------------
def test_promotes_staging_commit_and_ends_the_run_there(world):
    result = world.run()
    assert result.status == "promoted", result.as_dict()
    assert world.patches == [{"sha": NEW, "force": False}]  # fast-forward only
    assert world.issues == [] and world.mark_posts == []


def test_the_next_runs_confirm_production_goes_live_on_the_promotion(world):
    world.run()
    world.clock.sleep(5 * 60)
    assert world.run().status == "held"  # production still on the old release, inside the go-live window
    world.production = {"ok": True, "commit": NEW, "started_at": iso(world.clock.now())}
    assert "confirming" in world.run().detail  # live, but only just started
    world.clock.sleep(6 * 60)
    world.staging["commit"] = NEW
    result = world.run()
    assert (NEW, "success") in world.mark_posts
    assert result.status == "held" and "last release" in result.detail  # the 2-hour interval applies next


def test_releases_are_at_least_the_interval_apart(world):
    world.moved_at = NOW - timedelta(minutes=60)
    result = world.run()
    assert result.status == "held" and "60 minutes ago" in result.detail
    assert world.patches == []


def test_staging_must_run_the_candidate_for_the_soak_period(world):
    world.staging["started_at"] = iso(NOW - timedelta(minutes=10))
    result = world.run()
    assert result.status == "held" and "10 of 30 minutes" in result.detail
    assert world.patches == [] and world.issues == []


def test_no_factory_code_changed_means_no_release_and_no_restart(world):
    world.files = ["README.md", "docs/v1-controller.md"]
    result = world.run()
    assert result.status == "nothing" and "no restart needed" in result.detail
    assert world.patches == []


def test_nothing_to_do_when_staging_runs_the_release(world):
    world.staging["commit"] = OLD
    assert world.run().status == "nothing"
    assert world.patches == []


# --- confirming production ---------------------------------------------------------------------------------------
def test_an_unconfirmed_release_is_confirmed_before_anything_else(world):
    world.marks = {}
    result = world.run()
    assert world.mark_posts == [(OLD, "success")]
    assert result.status == "promoted"


def test_production_that_only_just_started_is_not_confirmed_yet(world):
    world.marks = {}
    world.production["started_at"] = iso(NOW - timedelta(minutes=2))
    result = world.run()
    assert result.status == "held" and "confirming" in result.detail
    assert world.mark_posts == [] and world.patches == []


def test_waits_while_production_deploys_the_release(world):
    world.marks, world.moved_at = {}, NOW - timedelta(minutes=10)
    world.production["commit"] = OTHER
    result = world.run()
    assert result.status == "held" and "Waiting for production" in result.detail
    assert world.holds == [] and world.patches == []


def test_a_release_that_never_goes_live_holds_releases_and_marks_the_commit(world):
    world.marks, world.moved_at = {}, NOW - timedelta(minutes=60)
    world.production["commit"] = OTHER
    result = world.run()
    assert result.status == "failed" and "Production reports ccccccc" in result.detail
    assert world.mark_posts == [(OLD, "failure")]
    assert len(world.issues) == 1 and world.issues[0]["labels"] == [HOLD] and HOLD in world.labels
    assert world.patches == []
    # Every later run stops at the hold until a person closes it.
    assert "on hold" in world.run().detail
    assert len(world.issues) == 1


def test_an_open_hold_stops_everything(world):
    world.holds = [{"number": 7, "title": "Release hold: rollback in progress"}]
    result = world.run()
    assert result.status == "held" and "#7" in result.detail
    assert world.patches == [] and world.mark_posts == []


def test_a_rolled_back_release_marked_do_not_release_holds(world):
    world.marks = {OLD: "failure"}
    result = world.run()
    assert result.status == "held" and "do-not-release" in result.detail
    assert world.patches == []


def test_a_commit_marked_do_not_release_is_never_promoted(world):
    world.marks[NEW] = "failure"
    result = world.run()
    assert result.status == "held" and "do-not-release" in result.detail
    assert world.patches == []


# --- gates before a promotion ------------------------------------------------------------------------------------
def test_pending_checks_hold_the_release_without_an_issue(world):
    world.checks.append({"name": "codex", "status": "in_progress", "conclusion": None})
    result = world.run()
    assert result.status == "held" and "codex" in result.detail
    assert world.patches == [] and world.issues == []


def test_failed_checks_block_the_release_and_are_reported_once(world):
    world.checks[1]["conclusion"] = "failure"
    result = world.run()
    assert result.status == "failed" and "postgres" in result.detail
    assert world.patches == []
    assert len(world.issues) == 1 and NEW[:7] in world.issues[0]["title"] and "labels" not in world.issues[0]
    world.run()
    assert len(world.issues) == 1  # the open issue already reports it


def test_staging_smoke_failure_blocks_the_release(world):
    world.asset_status = 404
    result = world.run()
    assert result.status == "failed" and "/assets/index-1.js" in result.detail
    assert world.patches == [] and len(world.issues) == 1


def test_smoke_reports_auth_that_lets_anonymous_calls_through(world):
    world.me_status = 200
    _gh, web = world.clients()
    problems = smoke(web, STAGING)
    assert problems and "expected 401" in problems[0]


def test_staging_briefly_down_during_a_deploy_is_retried(world):
    world.staging_down_for = 2
    assert world.run().status == "promoted"


def test_staging_that_stays_down_fails(world):
    world.staging_down_for = 100
    result = world.run()
    assert result.status == "failed" and "Staging is not healthy" in result.detail
    assert world.patches == []


def test_release_only_moves_forward(world):
    world.status = "diverged"
    result = world.run()
    assert result.status == "failed" and "diverged" in result.detail
    assert world.patches == []


def test_candidate_must_be_on_main(world):
    world.on_main = "diverged"
    assert world.run().status == "failed"
    assert world.patches == []


# --- controls ---------------------------------------------------------------------------------------------------
def test_paused_releases_touch_nothing(world):
    result = world.run(paused=True)
    assert result.status == "held" and world.github_calls == 0


def test_dry_run_changes_nothing(world):
    world.marks = {}
    result = world.run(dry_run=True)
    assert result.status == "would_promote"
    assert world.patches == [] and world.mark_posts == [] and world.issues == []


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
