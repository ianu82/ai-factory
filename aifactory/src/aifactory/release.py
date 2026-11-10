"""Release checker: promote what staging runs to the branch production deploys from.

Merges land on `main` as soon as they are ready, and staging (its own Railway environment) deploys every
commit of `main`. Production deploys only from `release`. `aifactory release-check` runs every 15 minutes (a
Railway cron service). Each run is short and safe to interrupt: it works out the state from GitHub and the two
deployments, does at most one thing, and ends right after moving `release`, so the redeploy it causes never cuts
a run in half.

A run, in order:

1. Stops if releases are paused or an open issue labelled `release-hold` exists.
2. Confirms the current release. Production must report the `release` commit and have run it for a few minutes;
   the commit then gets a green `factory/release` status. If production has not gone live on it in time, the run
   marks that commit "do not release", opens a `release-hold` issue and stops. A person rolls back or fixes, then
   closes the hold. A release confirmed earlier is never marked bad: a production blip is retried, and a longer
   outage opens one issue that becomes a hold only if it lasts.
3. Waits until the last release is at least the release interval old (2 hours by default).
4. Promotes the newest commit staging has run (itself or a later commit containing it) for the soak period,
   when all of these hold:
   - it changes the Factory's code since the last release;
   - it is on `main` and ahead of `release`, and not marked "do not release";
   - every GitHub check on it passed;
   - staging passes smoke checks.

Failures before a promotion open one issue per commit and change nothing. A commit is marked "do not release"
with a failed `factory/release` commit status: by the checker when a release fails in production, or by a person
during a rollback.
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
PRODUCTION_RETRIES = 10  # likewise for production: a restart or a slow answer is not an outage
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
    interval_minutes: int = 120
    live_timeout_minutes: int = 40
    stable_minutes: int = 5
    outage_hold_minutes: int = 45
    poll_seconds: int = 30
    hold_label: str = "release-hold"
    status_context: str = "factory/release"
    staging_context: str = "factory/staging"
    candidate_window: int = 40  # newest commits since the release considered for promotion
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
                   interval_minutes=number("AIFACTORY_RELEASE_INTERVAL_MINUTES", 120),
                   live_timeout_minutes=number("AIFACTORY_RELEASE_LIVE_TIMEOUT_MINUTES", 40),
                   stable_minutes=number("AIFACTORY_RELEASE_STABLE_MINUTES", 5),
                   outage_hold_minutes=number("AIFACTORY_RELEASE_OUTAGE_HOLD_MINUTES", 45),
                   poll_seconds=max(1, number("AIFACTORY_RELEASE_POLL_SECONDS", 30)),
                   paused=env("AIFACTORY_RELEASE_PAUSED", "").strip().lower() in ("1", "true", "yes", "on"))


@dataclass
class Result:
    status: str  # promoted | would_promote | nothing | held | failed
    detail: str
    commit: str = ""
    previous: str = ""
    steps: list[str] = field(default_factory=list)
    reported: bool = False  # already opened its own issue

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


def _time(value: Any) -> datetime | None:
    try:
        parsed = datetime.fromisoformat(str(value or "").replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


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
        self.dry_run = False

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

    def _moved_at(self, branch: str) -> datetime | None:
        """When the branch last moved (push, merge, creation or rollback), from the repository activity log."""
        response = self._gh("GET", f"/repos/{self.cfg.repo}/activity",
                            params={"ref": f"refs/heads/{branch}", "per_page": 1})
        if response.status_code != 200:
            raise ReleaseError(f"Reading activity for {branch}: HTTP {response.status_code}")
        items = response.json()
        return _time(items[0].get("timestamp")) if items else None

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

    def _mark(self, sha: str) -> str | None:
        """This checker's verdict on a commit: success (live in production), failure (do not release) or None."""
        response = self._gh("GET", f"/repos/{self.cfg.repo}/commits/{sha}/status")
        if response.status_code != 200:
            raise ReleaseError(f"Reading statuses for {sha[:7]}: HTTP {response.status_code}")
        for status in response.json().get("statuses") or []:
            if status.get("context") == self.cfg.status_context:
                return status.get("state")
        return None

    def _set_mark(self, sha: str, state: str, description: str) -> None:
        if self.dry_run:
            self.step(f"Dry run: would mark {sha[:7]} {state}")
            return
        response = self._gh("POST", f"/repos/{self.cfg.repo}/statuses/{sha}",
                            json={"state": state, "context": self.cfg.status_context, "description": description[:140]})
        if response.status_code not in (200, 201):
            raise ReleaseError(f"Marking {sha[:7]} {state}: HTTP {response.status_code}")

    def _staging_record(self, sha: str) -> tuple[datetime | None, bool | None]:
        """What an earlier run recorded for this commit on staging: when staging (last) started it, and whether
        staging passed smoke checks while running it (None when never recorded)."""
        response = self._gh("GET", f"/repos/{self.cfg.repo}/commits/{sha}/status")
        if response.status_code != 200:
            raise ReleaseError(f"Reading statuses for {sha[:7]}: HTTP {response.status_code}")
        for status in response.json().get("statuses") or []:  # newest first: the latest record per context
            if status.get("context") == self.cfg.staging_context:
                text = str(status.get("description") or "")
                since = _time(text.removeprefix("Staging since ").split(";")[0].strip())
                smoke_ok = True if "smoke passed" in text else False if "smoke failed" in text else None
                return since, smoke_ok
        return None, None

    def _note_staged(self, sha: str, started: datetime, smoke_ok: bool | None = None) -> None:
        """Record when staging started a commit and whether smoke passed on it. This checker keeps no state
        between runs, and later merges replace staging's commit, so the record lives on the commit as a status. A
        restart of the same commit (a newer start) resets the soak and the smoke result. Within one start a failure
        is sticky: a later pass does not clear it, so a flaky tree is never promoted on its good runs."""
        if self.dry_run:
            return
        since, recorded_smoke = self._staging_record(sha)
        newer = since is None or started > since
        first_result = smoke_ok is not None and recorded_smoke is None
        new_failure = smoke_ok is False and recorded_smoke is True
        if not (newer or first_result or new_failure):
            return
        if not newer and since is not None:
            started = since  # same start: keep its soak, change only the smoke result
        smoke = "" if smoke_ok is None else f"; smoke {'passed' if smoke_ok else 'failed'}"
        state = "failure" if smoke_ok is False else "success"
        response = self._gh("POST", f"/repos/{self.cfg.repo}/statuses/{sha}",
                            json={"state": state, "context": self.cfg.staging_context,
                                  "description": f"Staging since {started.isoformat()}{smoke}"[:140]})
        if response.status_code not in (200, 201):
            raise ReleaseError(f"Recording staging for {sha[:7]}: HTTP {response.status_code}")

    def _recent_commits(self, head: str, stop: str) -> list[str]:
        """Commits from head back to (not including) stop, newest first, at most candidate_window of them.
        The commits list is newest-first and paged, unlike compare, which caps at 250 and orders oldest-first."""
        found: list[str] = []
        for page in range(1, 4):
            response = self._gh("GET", f"/repos/{self.cfg.repo}/commits",
                                params={"sha": head, "per_page": 100, "page": page})
            if response.status_code != 200:
                raise ReleaseError(f"Listing commits from {head[:7]}: HTTP {response.status_code}")
            batch = [c.get("sha") for c in response.json() if c.get("sha")]
            for sha in batch:
                if sha == stop or len(found) >= self.cfg.candidate_window:
                    return found or [head]
                found.append(sha)
            if len(batch) < 100:
                break
        return found or [head]

    def _open_issues(self, **params: Any) -> list[dict[str, Any]]:
        response = self._gh("GET", f"/repos/{self.cfg.repo}/issues", params={"state": "open", "per_page": 100, **params})
        if response.status_code != 200:
            raise ReleaseError(f"Listing issues: HTTP {response.status_code}")
        return [i for i in response.json() if "pull_request" not in i]

    def _open_issue(self, title: str, body: str, labels: list[str] | None = None) -> None:
        """One issue per title: a repeated failure is already reported and is not posted again."""
        if self.dry_run:
            self.step(f"Dry run: would open issue {title!r}")
            return
        try:
            if any(i.get("title") == title for i in self._open_issues()):
                return
            for label in labels or []:
                self._ensure_label(label)
            self._gh("POST", f"/repos/{self.cfg.repo}/issues",
                     json={"title": title, "body": body, **({"labels": labels} if labels else {})})
        except (ReleaseError, httpx.HTTPError) as exc:  # reporting must never hide the result itself
            log.error("Could not open the release issue on GitHub: %s", exc)

    def _ensure_label(self, label: str) -> None:
        if self._gh("GET", f"/repos/{self.cfg.repo}/labels/{label}").status_code == 404:
            self._gh("POST", f"/repos/{self.cfg.repo}/labels",
                     json={"name": label, "color": "b60205", "description": "Stops the release checker until closed"})

    def _body(self, result: Result, extra: str = "") -> str:
        return (f"{result.detail}\n\nCommit: `{result.commit or 'unknown'}`\nRelease branch was at: "
                f"`{result.previous or 'unknown'}`\n\nSteps:\n" + "\n".join(f"- {s}" for s in self.steps) +
                (f"\n\n{extra}" if extra else ""))

    # --- the check ----------------------------------------------------------------------------------------------
    def step(self, text: str) -> None:
        log.info(text)
        self.steps.append(text)

    def run(self, dry_run: bool = False) -> Result:
        self.dry_run = dry_run
        try:
            result = self._run()
        except (ReleaseError, httpx.HTTPError, KeyError, ValueError, TypeError) as exc:
            result = Result("failed", f"Release check error: {exc}")
        result.steps = self.steps
        if result.failed and not result.reported:
            self._open_issue(f"Release check failed at {result.commit[:7] or 'unknown commit'}", self._body(
                result, "Production keeps running the current release. The checker tries again on its next run."))
        return result

    def _minutes_since(self, moment: datetime | None) -> float | None:
        return None if moment is None else (self.now() - moment).total_seconds() / 60

    def _run(self) -> Result:
        cfg = self.cfg
        if cfg.paused:
            return Result("held", "Releases are paused (AIFACTORY_RELEASE_PAUSED).")
        holds = self._open_issues(labels=cfg.hold_label)
        if holds:
            return Result("held", f"Releases are on hold until #{holds[0]['number']} is closed: {holds[0].get('title', '')}")

        current = self._ref(cfg.branch)
        if current is None:
            return Result("failed", f"Branch {cfg.branch} does not exist; create it at the commit production runs.")
        since_move = self._minutes_since(self._moved_at(cfg.branch))
        self._observe_staging()

        confirmed = self._confirm_production(current, since_move)
        if confirmed is not None:
            return confirmed

        if since_move is not None and since_move < cfg.interval_minutes:
            return Result("held", f"The last release was {since_move:.0f} minutes ago; the next can follow after "
                                  f"{cfg.interval_minutes}.", previous=current)
        return self._promote(current)

    def _observe_staging(self) -> None:
        """Every run records when staging started its current commit, including runs that then wait for the
        release interval, so a later run can promote a commit that has since been replaced on staging."""
        staging = _health(self.web, self.cfg.staging_url)
        commit, started = str(staging.get("commit") or ""), _time(staging.get("started_at"))
        if staging.get("ok") and commit and started is not None:
            try:
                # Smoke the exact tree staging runs now, so an older commit is only ever promoted on a smoke result
                # from when that commit itself was deployed. A deploy that starts during the smoke run makes the
                # result belong to neither commit, so only the start is recorded then.
                passed = not smoke(self.web, self.cfg.staging_url)
                self._note_staged(commit, started, smoke_ok=passed if self._still_running(commit, started) else None)
            except (ReleaseError, httpx.HTTPError) as exc:
                self.step(f"Could not record staging's commit: {exc}")

    def _still_running(self, commit: str, started: datetime | None) -> bool:
        """Whether staging still runs the same deployment (commit and start) it reported before a smoke run."""
        again = _health(self.web, self.cfg.staging_url)
        return bool(again.get("ok")) and again.get("commit") == commit and _time(again.get("started_at")) == started

    def _production(self) -> dict[str, Any]:
        """Production's health, asked again over a few minutes while it does not answer: a restart or a slow
        response is not an outage. A healthy answer, on whichever commit, ends the wait."""
        live = _health(self.web, self.cfg.production_url)
        for _ in range(PRODUCTION_RETRIES):
            if live.get("ok"):
                break
            self.sleep(self.cfg.poll_seconds)
            live = _health(self.web, self.cfg.production_url)
        return live

    def _confirm_production(self, current: str, since_move: float | None) -> Result | None:
        """None when production runs the current release, confirmed; otherwise the reason to stop this run."""
        cfg = self.cfg
        mark = self._mark(current)
        if mark == "failure":
            return Result("held", f"{cfg.branch} is at {current[:7]}, which is marked do-not-release. "
                                  f"Roll {cfg.branch} back or ship a fix.", current, current)
        live = self._production()
        on_release = bool(live.get("ok")) and live.get("commit") == current
        if mark == "success":
            return self._watch_confirmed(current, live, on_release)
        if on_release:
            up = self._minutes_since(_time(live.get("started_at")))
            if up is not None and up < cfg.stable_minutes:
                return Result("held", f"Production has run {current[:7]} for {up:.0f} minutes; confirming after "
                                      f"{cfg.stable_minutes}.", current, current)
            self._set_mark(current, "success", "Live in production")
            self.step(f"Production confirmed on {current[:7]}")
            return None
        seen = str(live.get("commit") or "")[:7] or live.get("error", "no answer")
        if since_move is not None and since_move < cfg.live_timeout_minutes:
            return Result("held", f"Waiting for production to go live on {current[:7]} (moved {since_move:.0f} minutes "
                                  f"ago; production reports {seen}).", current, current)
        # Only a release never confirmed live is marked bad: it did not go live, or not for long enough.
        waited = "an unknown time" if since_move is None else f"{since_move:.0f} minutes"
        result = Result("failed", f"Production reports {seen}, but {cfg.branch} has been at {current[:7]} for {waited}.",
                        current, current)
        self._set_mark(current, "failure", "Did not go live or stay healthy in production")
        self._open_issue(f"Release hold: {current[:7]} did not go live in production", self._body(result, (
            f"Releases are on hold while this issue is open, and {current[:7]} is marked do-not-release. Check the "
            f"Railway deployment. To roll back, move `{cfg.branch}` to the last good release. Close this issue once "
            "production is healthy; the checker then releases the next commit staging runs.")), [cfg.hold_label])
        result.reported = True
        return result

    def _watch_confirmed(self, current: str, live: dict[str, Any], on_release: bool) -> Result | None:
        """A confirmed release stays good. If production stops answering, or answers on another commit, the run
        skips and records the outage in one issue; only an outage that lasts puts releases on hold."""
        cfg = self.cfg
        title = f"Production unhealthy on {current[:7]}"
        outage = next((i for i in self._open_issues() if i.get("title") == title), None)
        if on_release:
            self.step(f"Production runs {current[:7]}, confirmed earlier")
            if outage and not self.dry_run and not any(l.get("name") == cfg.hold_label for l in outage.get("labels", [])):
                self._gh("POST", f"/repos/{cfg.repo}/issues/{outage['number']}/comments",
                         json={"body": "Production answers healthy on the release again; closing."})
                self._gh("PATCH", f"/repos/{cfg.repo}/issues/{outage['number']}", json={"state": "closed"})
                self.step(f"Closed #{outage['number']}: production recovered")
            return None
        seen = (f"reports {str(live.get('commit'))[:7]} instead" if live.get("ok")
                else f"does not answer ({live.get('error', 'no answer')})")
        if outage is None:
            result = Result("held", f"Production {seen} of {current[:7]}; skipping this run.", current, current)
            self._open_issue(title, self._body(result, (
                f"{current[:7]} was confirmed live earlier, so it is not marked bad. The checker skips releases while "
                f"production is unhealthy and puts them on hold if this lasts {cfg.outage_hold_minutes} minutes. It "
                "closes this issue itself when production answers again.")))
            return result
        lasted = self._minutes_since(_time(outage.get("created_at"))) or 0.0
        if lasted < cfg.outage_hold_minutes:
            return Result("held", f"Production still {seen} of {current[:7]}, for {lasted:.0f} minutes; skipping "
                                  "this run.", current, current)
        if not self.dry_run:
            self._ensure_label(cfg.hold_label)
            self._gh("POST", f"/repos/{cfg.repo}/issues/{outage['number']}/labels", json={"labels": [cfg.hold_label]})
            self._gh("POST", f"/repos/{cfg.repo}/issues/{outage['number']}/comments", json={"body": (
                f"Production has been unhealthy for {lasted:.0f} minutes, so releases are now on hold until this issue "
                f"is closed. {current[:7]} is not marked bad: it ran healthily before the outage.")})
        result = Result("failed", f"Production has been unhealthy for {lasted:.0f} minutes; releases are on hold "
                                  f"(#{outage['number']}).", current, current)
        result.reported = True
        return result

    def _promote(self, current: str) -> Result:
        cfg = self.cfg
        staging = _health(self.web, cfg.staging_url)
        for _ in range(STAGING_RETRIES):  # a deploy briefly takes staging down; don't call that a failure
            if staging.get("ok"):
                break
            self.sleep(cfg.poll_seconds)
            staging = _health(self.web, cfg.staging_url)
        candidate = str(staging.get("commit") or "")
        if not staging.get("ok"):
            return Result("failed", f"Staging is not healthy: {staging.get('error') or staging}", candidate, current)
        if not candidate:
            return Result("failed", "Staging does not report its commit (RAILWAY_GIT_COMMIT_SHA).", previous=current)
        started = _time(staging.get("started_at"))
        if started is None:
            return Result("held", "Staging does not report when it started yet.", candidate, current)
        ran = self._minutes_since(started) or 0.0
        self.step(f"Staging runs {candidate[:7]}, up {ran:.0f} minutes")

        if candidate == current:
            return Result("nothing", f"{cfg.branch} is already at {candidate[:7]}.", candidate, current)
        compare = self._compare(current, candidate)
        status = compare.get("status")
        if status in ("identical", "behind"):
            return Result("nothing", f"Staging's {candidate[:7]} is not ahead of {cfg.branch} ({status}).", candidate, current)
        if status != "ahead":
            return Result("failed", f"{candidate[:7]} has diverged from {cfg.branch} at {current[:7]}; "
                                    "release only moves forward.", candidate, current)
        files = [f.get("filename", "") for f in compare.get("files") or []]
        if len(files) < COMPARE_FILE_LIMIT and not any(f.startswith(cfg.paths) for f in files):
            return Result("nothing", f"No Factory code changed in {compare.get('ahead_by', '?')} commit(s) since "
                                     f"{current[:7]}; no restart needed.", candidate, current)
        self.step(f"{compare.get('ahead_by', '?')} commit(s) since {cfg.branch} at {current[:7]} change Factory code")
        if self._compare(candidate, cfg.source_branch).get("status") not in ("ahead", "identical"):
            return Result("failed", f"{candidate[:7]} is not on {cfg.source_branch}.", candidate, current)

        # Merges keep replacing staging's commit, so waiting for its current commit to soak would starve releases
        # while the Factory is busy. Promote the newest commit staging actually deployed (it has its own
        # factory/staging record, so it is a tree staging ran, never a side-branch commit) whose code has been on
        # staging for the soak period: since staging started it, every later commit staging ran contains it.
        line = self._recent_commits(candidate, current)
        def staging_record(sha: str) -> tuple[datetime | None, bool | None]:
            if sha != candidate:
                return self._staging_record(sha)
            # Staging's current deployment soaks from the start it reports now; a recorded verdict counts only
            # when it was recorded for that same start.
            since, verdict = self._staging_record(sha)
            return started, verdict if since == started else None

        records = {sha: staging_record(sha) for sha in line}
        soaked_since: dict[str, datetime | None] = {sha: record[0] for sha, record in records.items()}
        chosen, reasons = None, []
        for sha in line:
            since = soaked_since[sha]
            if since is None:
                continue  # never deployed on staging by itself
            minutes = self._minutes_since(since) or 0.0 if since else 0.0
            if minutes < cfg.soak_minutes:
                reasons.append(f"{sha[:7]} {minutes:.0f}/{cfg.soak_minutes} min")
                continue
            if self._mark(sha) == "failure":
                reasons.append(f"{sha[:7]} is marked do-not-release")
                continue
            if records[sha][1] is False:
                reasons.append(f"{sha[:7]} failed smoke while staging ran it (sticky until staging restarts it)")
                continue
            if sha != candidate and records[sha][1] is not True:
                # The smoke run below tests staging's current tree; an older commit needs its own passing result.
                reasons.append(f"{sha[:7]} has no passing smoke result from when staging ran it")
                continue
            if sha != candidate:
                part = self._compare(current, sha)
                if part.get("status") != "ahead":
                    reasons.append(f"{sha[:7]} is not a fast-forward of {cfg.branch}")
                    continue
                part_files = [f.get("filename", "") for f in part.get("files") or []]
                if len(part_files) < COMPARE_FILE_LIMIT and not any(f.startswith(cfg.paths) for f in part_files):
                    return Result("nothing", f"No Factory code changed up to the newest soaked commit {sha[:7]}.",
                                  sha, current)
            runs = self._check_runs(sha)
            pending = sorted({r.get("name", "?") for r in runs if r.get("status") != "completed"})
            failing = sorted({r.get("name", "?") for r in runs if r.get("conclusion") not in OK_CONCLUSIONS})
            if not runs or pending:
                reasons.append(f"{sha[:7]} checks still running: {', '.join(pending)}" if runs
                               else f"No GitHub checks reported on {sha[:7]} yet")
                continue
            if failing:
                if sha == candidate:
                    return Result("failed", "Checks failed: " + ", ".join(failing), candidate, current)
                reasons.append(f"{sha[:7]} checks failed")
                continue
            chosen = sha
            self.step(f"{len(runs)} GitHub check(s) passed on {sha[:7]}, on staging for {minutes:.0f} minutes")
            break
        if chosen is None:
            if len(line) == 1 and ran < cfg.soak_minutes:
                return Result("held", f"Staging has run {candidate[:7]} for {ran:.0f} of {cfg.soak_minutes} minutes.",
                              candidate, current)
            soaked = any((self._minutes_since(since) or 0.0) >= cfg.soak_minutes
                         for since in soaked_since.values() if since is not None)
            lead = "Nothing releasable yet" if soaked else "No commit has been on staging long enough yet"
            return Result("held", f"{lead}: " + "; ".join(reasons[:6]), candidate, current)

        problems = smoke(self.web, cfg.staging_url)
        if problems:
            if self._still_running(candidate, started):
                try:  # sticky: a later passing run of this deployment must not release it
                    self._note_staged(candidate, started, smoke_ok=False)
                except ReleaseError as exc:
                    self.step(f"Could not record the smoke failure: {exc}")
            return Result("failed", "Staging smoke failed: " + "; ".join(problems), chosen, current)
        self.step("Staging smoke checks passed")

        if not self._still_running(candidate, started):
            return Result("held", "Staging changed during the check; trying again next run.", chosen, current)
        candidate = chosen

        if self.dry_run:
            return Result("would_promote", f"Would move {cfg.branch} from {current[:7]} to {candidate[:7]}.",
                          candidate, current)
        # The last action of a run: the deploy this starts may restart this checker, which is then finished.
        moved = self._gh("PATCH", f"/repos/{cfg.repo}/git/refs/heads/{cfg.branch}",
                         json={"sha": candidate, "force": False})
        if moved.status_code != 200:
            return Result("failed", f"Moving {cfg.branch} to {candidate[:7]} was refused: HTTP {moved.status_code}.",
                          candidate, current)
        self.step(f"Moved {cfg.branch} from {current[:7]} to {candidate[:7]}")
        return Result("promoted", f"Moved {cfg.branch} to {candidate[:7]} (from {current[:7]}); the next runs confirm "
                                  "production goes live on it.", candidate, current)


def release_check(dry_run: bool = False) -> Result:
    cfg = ReleaseConfig.from_env()
    headers = {"Authorization": f"Bearer {cfg.token}", "Accept": "application/vnd.github+json",
               "X-GitHub-Api-Version": "2022-11-28", "User-Agent": "MindsHub-AI-Factory-release-check"}
    with httpx.Client(base_url="https://api.github.com", headers=headers) as gh, \
            httpx.Client(headers={"User-Agent": "MindsHub-AI-Factory-release-check"}, follow_redirects=True) as web:
        return Releaser(cfg, gh, web).run(dry_run=dry_run)
