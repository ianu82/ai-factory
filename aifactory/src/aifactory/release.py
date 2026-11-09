"""Release checker: promote what staging runs to the branch production deploys from.

Merges land on `main` as soon as they are ready, and staging (its own Railway environment) deploys every
commit of `main`. Production deploys only from `release`. A scheduled `aifactory release-check` (a Railway cron
service) fast-forwards `release` to the commit staging runs, and only when all of these hold:

- staging is healthy on that commit and has run it without restarting for the soak period;
- the commit changes the Factory's code since the last release (otherwise a release restarts production for nothing);
- every GitHub check on the commit passed;
- staging passes smoke checks: the web app and its assets load, the API answers, and auth rejects anonymous calls.

It then confirms that production reports the new commit and stays up without restarting. Anything that needs a
person (failed checks, failed smoke, a release that did not go live) opens or updates a GitHub issue.
"""

from __future__ import annotations

import logging
import os
import re
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

import httpx

log = logging.getLogger(__name__)

OK_CONCLUSIONS = {"success", "neutral", "skipped"}
COMPARE_FILE_LIMIT = 300  # GitHub's compare API lists at most this many files
STAGING_RETRIES = 6  # health retries, one poll interval apart, before staging counts as down
ASSET = re.compile(r'src="(/assets/[^"]+\.js)"')


class ReleaseError(Exception):
    pass


@dataclass(frozen=True)
class ReleaseConfig:
    repo: str
    staging_url: str
    production_url: str
    token: str
    branch: str = "release"
    source_branch: str = "main"
    paths: tuple[str, ...] = ("aifactory/",)
    soak_minutes: int = 30
    live_timeout_minutes: int = 40
    stable_minutes: int = 5
    poll_seconds: int = 30
    paused: bool = False

    @classmethod
    def from_env(cls) -> ReleaseConfig:
        env = os.environ.get
        missing = [n for n in ("AIFACTORY_STAGING_URL", "AIFACTORY_PRODUCTION_URL", "AIFACTORY_RELEASE_GITHUB_TOKEN")
                   if not env(n)]
        if missing:
            raise ReleaseError("Missing settings: " + ", ".join(missing))

        def number(name: str, default: int) -> int:
            try:
                return max(0, int(env(name, default)))
            except ValueError:
                return default

        return cls(repo=env("AIFACTORY_RELEASE_REPO", "ianu82/ai-factory"),
                   staging_url=env("AIFACTORY_STAGING_URL", "").rstrip("/"),
                   production_url=env("AIFACTORY_PRODUCTION_URL", "").rstrip("/"),
                   token=env("AIFACTORY_RELEASE_GITHUB_TOKEN", ""),
                   branch=env("AIFACTORY_RELEASE_BRANCH", "release"),
                   source_branch=env("AIFACTORY_RELEASE_SOURCE_BRANCH", "main"),
                   paths=tuple(p.strip() for p in env("AIFACTORY_RELEASE_PATHS", "aifactory/").split(",") if p.strip()),
                   soak_minutes=number("AIFACTORY_RELEASE_SOAK_MINUTES", 30),
                   live_timeout_minutes=number("AIFACTORY_RELEASE_LIVE_TIMEOUT_MINUTES", 40),
                   stable_minutes=number("AIFACTORY_RELEASE_STABLE_MINUTES", 5),
                   poll_seconds=max(1, number("AIFACTORY_RELEASE_POLL_SECONDS", 30)),
                   paused=env("AIFACTORY_RELEASE_PAUSED", "").strip().lower() in ("1", "true", "yes", "on"))


@dataclass
class Result:
    status: str  # released | would_release | nothing | held | failed
    detail: str
    commit: str = ""
    previous: str = ""
    steps: list[str] = field(default_factory=list)

    @property
    def failed(self) -> bool:
        return self.status == "failed"

    def as_dict(self) -> dict[str, Any]:
        return {"status": self.status, "detail": self.detail, "commit": self.commit, "previous": self.previous,
                "steps": self.steps}


def _health(web: httpx.Client, base: str) -> dict[str, Any]:
    try:
        response = web.get(f"{base}/api/health", timeout=20)
    except httpx.HTTPError as exc:
        return {"ok": False, "error": f"unreachable ({type(exc).__name__})"}
    if response.status_code != 200:
        return {"ok": False, "error": f"HTTP {response.status_code}"}
    try:
        body = response.json()
    except ValueError:
        return {"ok": False, "error": "not JSON"}
    return body if isinstance(body, dict) else {"ok": False, "error": "unexpected body"}


def _started(health: dict[str, Any]) -> datetime | None:
    try:
        value = datetime.fromisoformat(str(health.get("started_at") or ""))
    except ValueError:
        return None
    return value if value.tzinfo else value.replace(tzinfo=timezone.utc)


def smoke(web: httpx.Client, base: str) -> list[str]:
    """What a person would notice first: the app does not load, the API is down, or auth is open."""
    problems: list[str] = []
    try:
        page = web.get(f"{base}/", timeout=20)
        has_root = '<div id="root"' in page.text
        if page.status_code != 200 or not has_root:
            problems.append(f"web app page: HTTP {page.status_code}, app root {'found' if has_root else 'missing'}")
        else:
            assets = ASSET.findall(page.text)
            if not assets:
                problems.append("web app page references no script bundle")
            for path in assets[:3]:
                asset = web.get(f"{base}{path}", timeout=20)
                if asset.status_code != 200 or len(asset.content) < 1000:
                    problems.append(f"script {path}: HTTP {asset.status_code}, {len(asset.content)} bytes")
        meta = web.get(f"{base}/api/meta", timeout=20)
        if meta.status_code != 200 or not isinstance((meta.json() if meta.status_code == 200 else {}).get("auth"), dict):
            problems.append(f"/api/meta: HTTP {meta.status_code}")
        me = web.get(f"{base}/api/me", timeout=20)
        if me.status_code != 401:
            problems.append(f"/api/me without a session returned HTTP {me.status_code}, expected 401")
    except (httpx.HTTPError, ValueError) as exc:
        problems.append(f"request failed ({type(exc).__name__})")
    return problems


class Releaser:
    def __init__(self, cfg: ReleaseConfig, gh: httpx.Client, web: httpx.Client, *,
                 sleep: Callable[[float], None] = time.sleep,
                 now: Callable[[], datetime] = lambda: datetime.now(timezone.utc)) -> None:
        self.cfg, self.gh, self.web, self.sleep, self.now = cfg, gh, web, sleep, now
        self.steps: list[str] = []

    # --- GitHub -----------------------------------------------------------------------------------------------
    def _gh(self, method: str, path: str, **kwargs: Any) -> httpx.Response:
        response = self.gh.request(method, path, timeout=30, **kwargs)
        if response.status_code >= 500 or response.status_code == 429:
            raise ReleaseError(f"GitHub {method} {path}: HTTP {response.status_code}")
        return response

    def _ref(self, branch: str) -> str | None:
        response = self._gh("GET", f"/repos/{self.cfg.repo}/git/ref/heads/{branch}")
        if response.status_code == 404:
            return None
        if response.status_code != 200:
            raise ReleaseError(f"Reading branch {branch}: HTTP {response.status_code}")
        return response.json()["object"]["sha"]

    def _compare(self, base: str, head: str) -> dict[str, Any]:
        response = self._gh("GET", f"/repos/{self.cfg.repo}/compare/{base}...{head}")
        if response.status_code != 200:
            raise ReleaseError(f"Comparing {base[:7]}...{head[:7]}: HTTP {response.status_code}")
        return response.json()

    def _check_runs(self, sha: str) -> list[dict[str, Any]]:
        runs: list[dict[str, Any]] = []
        for page in range(1, 11):
            response = self._gh("GET", f"/repos/{self.cfg.repo}/commits/{sha}/check-runs",
                                params={"per_page": 100, "page": page})
            if response.status_code != 200:
                raise ReleaseError(f"Reading checks for {sha[:7]}: HTTP {response.status_code}")
            batch = response.json().get("check_runs") or []
            runs += batch
            if len(batch) < 100:
                break
        return runs

    def report(self, result: Result) -> None:
        """Open an issue for a failure, or comment on the open one with the same title."""
        title = f"Release check failed at {result.commit[:7] or 'unknown commit'}"
        body = (f"{result.detail}\n\nCandidate: `{result.commit or 'unknown'}`\nCurrent release: `{result.previous or 'unknown'}`\n\n"
                "Steps:\n" + "\n".join(f"- {s}" for s in result.steps) +
                "\n\nThe release checker will retry at its next run. Production keeps running the current release.")
        try:
            existing = self._gh("GET", f"/repos/{self.cfg.repo}/issues", params={"state": "open", "per_page": 100})
            match = next((i for i in (existing.json() if existing.status_code == 200 else [])
                          if i.get("title") == title and "pull_request" not in i), None)
            if match:
                self._gh("POST", f"/repos/{self.cfg.repo}/issues/{match['number']}/comments", json={"body": body})
            else:
                self._gh("POST", f"/repos/{self.cfg.repo}/issues", json={"title": title, "body": body})
        except (ReleaseError, httpx.HTTPError) as exc:  # reporting must never hide the result itself
            log.error("Could not report the release failure on GitHub: %s", exc)

    # --- the check ----------------------------------------------------------------------------------------------
    def step(self, text: str) -> None:
        log.info(text)
        self.steps.append(text)

    def run(self, dry_run: bool = False) -> Result:
        try:
            result = self._run(dry_run)
        except (ReleaseError, httpx.HTTPError, KeyError, ValueError) as exc:
            result = Result("failed", f"Release check error: {exc}")
        result.steps = self.steps
        if result.failed:
            self.report(result)
        return result

    def _run(self, dry_run: bool) -> Result:
        cfg = self.cfg
        if cfg.paused:
            return Result("held", "Releases are paused (AIFACTORY_RELEASE_PAUSED).")
        staging = _health(self.web, cfg.staging_url)
        for _ in range(STAGING_RETRIES):  # a deploy briefly takes staging down; don't call that a failure
            if staging.get("ok"):
                break
            self.sleep(cfg.poll_seconds)
            staging = _health(self.web, cfg.staging_url)
        candidate = str(staging.get("commit") or "")
        if not staging.get("ok"):
            return Result("failed", f"Staging is not healthy: {staging.get('error') or staging}", candidate)
        if not candidate:
            return Result("failed", "Staging does not report its commit (RAILWAY_GIT_COMMIT_SHA).")
        started = _started(staging)
        if started is None:
            return Result("held", "Staging does not report when it started yet.", candidate)
        ran = (self.now() - started).total_seconds() / 60
        self.step(f"Staging runs {candidate[:7]}, up {ran:.0f} min")

        previous = self._ref(cfg.branch)
        if previous is None:
            return Result("failed", f"Branch {cfg.branch} does not exist; create it at the commit production runs.",
                          candidate)
        if previous == candidate:
            return Result("nothing", f"{cfg.branch} is already at {candidate[:7]}.", candidate, previous)
        compare = self._compare(previous, candidate)
        status = compare.get("status")
        if status in ("identical", "behind"):
            return Result("nothing", f"Staging's {candidate[:7]} is not ahead of {cfg.branch} ({status}).", candidate, previous)
        if status != "ahead":
            return Result("failed", f"{candidate[:7]} has diverged from {cfg.branch} at {previous[:7]}; "
                                    "release only moves forward.", candidate, previous)
        files = [f.get("filename", "") for f in compare.get("files") or []]
        if len(files) < COMPARE_FILE_LIMIT and not any(f.startswith(cfg.paths) for f in files):
            return Result("nothing", f"No Factory code changed in {compare.get('ahead_by', '?')} commit(s) since "
                                     f"{previous[:7]}; no restart needed.", candidate, previous)
        self.step(f"{compare.get('ahead_by', '?')} commit(s) since {cfg.branch} at {previous[:7]} change Factory code")

        on_source = self._compare(candidate, cfg.source_branch).get("status")
        if on_source not in ("ahead", "identical"):
            return Result("failed", f"{candidate[:7]} is not on {cfg.source_branch}.", candidate, previous)

        if ran < cfg.soak_minutes:
            return Result("held", f"Staging has run {candidate[:7]} for {ran:.0f} of {cfg.soak_minutes} minutes.",
                          candidate, previous)

        runs = self._check_runs(candidate)
        if not runs:
            return Result("held", f"No GitHub checks reported on {candidate[:7]} yet.", candidate, previous)
        pending = sorted({r.get("name", "?") for r in runs if r.get("status") != "completed"})
        if pending:
            return Result("held", "Checks still running: " + ", ".join(pending), candidate, previous)
        failing = sorted({r.get("name", "?") for r in runs if r.get("conclusion") not in OK_CONCLUSIONS})
        if failing:
            return Result("failed", "Checks failed: " + ", ".join(failing), candidate, previous)
        self.step(f"{len(runs)} GitHub check(s) passed")

        problems = smoke(self.web, cfg.staging_url)
        if problems:
            return Result("failed", "Staging smoke failed: " + "; ".join(problems), candidate, previous)
        self.step("Staging smoke checks passed")

        again = _health(self.web, cfg.staging_url)
        if not again.get("ok") or again.get("commit") != candidate or _started(again) != started:
            return Result("held", "Staging changed during the check; trying again next run.", candidate, previous)

        if dry_run:
            return Result("would_release", f"Would move {cfg.branch} from {previous[:7]} to {candidate[:7]}.",
                          candidate, previous)
        moved = self._gh("PATCH", f"/repos/{cfg.repo}/git/refs/heads/{cfg.branch}",
                         json={"sha": candidate, "force": False})
        if moved.status_code != 200:
            return Result("failed", f"Moving {cfg.branch} to {candidate[:7]} was refused: HTTP {moved.status_code}.",
                          candidate, previous)
        self.step(f"Moved {cfg.branch} from {previous[:7]} to {candidate[:7]}")
        return self._confirm_live(candidate, previous)

    def _confirm_live(self, candidate: str, previous: str) -> Result:
        cfg = self.cfg
        deadline = self.now().timestamp() + cfg.live_timeout_minutes * 60
        live: dict[str, Any] = {}
        while True:
            live = _health(self.web, cfg.production_url)
            if live.get("ok") and live.get("commit") == candidate:
                break
            if self.now().timestamp() >= deadline:
                seen = live.get("commit", "")[:7] or live.get("error", "no answer")
                return Result("failed", f"Production did not report {candidate[:7]} within {cfg.live_timeout_minutes} "
                                        f"minutes (last seen: {seen}). Check the Railway deployment.", candidate, previous)
            self.sleep(cfg.poll_seconds)
        started = _started(live)
        self.step(f"Production is live on {candidate[:7]}")
        stable_until = self.now().timestamp() + cfg.stable_minutes * 60
        while self.now().timestamp() < stable_until:
            self.sleep(cfg.poll_seconds)
            check = _health(self.web, cfg.production_url)
            if not check.get("ok") or check.get("commit") != candidate:
                return Result("failed", f"Production became unhealthy after the release: "
                                        f"{check.get('error') or check.get('commit', '')[:7]}.", candidate, previous)
            if started is not None and _started(check) != started:
                return Result("failed", "Production restarted within minutes of the release.", candidate, previous)
        self.step(f"Production stayed healthy for {cfg.stable_minutes} minutes")
        return Result("released", f"Released {candidate[:7]} (previous {previous[:7]}).", candidate, previous)


def release_check(dry_run: bool = False) -> Result:
    cfg = ReleaseConfig.from_env()
    headers = {"Authorization": f"Bearer {cfg.token}", "Accept": "application/vnd.github+json",
               "X-GitHub-Api-Version": "2022-11-28", "User-Agent": "MindsHub-AI-Factory-release-check"}
    with httpx.Client(base_url="https://api.github.com", headers=headers) as gh, \
            httpx.Client(headers={"User-Agent": "MindsHub-AI-Factory-release-check"}, follow_redirects=True) as web:
        return Releaser(cfg, gh, web).run(dry_run=dry_run)
