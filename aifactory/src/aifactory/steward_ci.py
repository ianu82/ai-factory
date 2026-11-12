"""Read-only, bounded default-branch CI diagnosis.

No persistence or repair side effects. ``diagnose`` accepts a detached repository
object (full_name/default_branch), or a dictionary snapshot. Consumers must check
``complete`` as well as ``state``; only success is evidence of recovery. Culprit
intervals are first-parent intervals, not guesses based on run completion dates.
"""

from __future__ import annotations

import asyncio
from typing import Any
from urllib.parse import quote

from .github import GitHub, GitHubError
from .security import redact

_FAILURES = {"failure", "timed_out", "cancelled", "action_required", "error", "startup_failure"}


def _state(rows: list[dict]) -> str:
    if any(row.get("conclusion") in _FAILURES for row in rows):
        return "failure"
    if not rows or any(row.get("conclusion") == "stale" for row in rows):
        return "incomplete"
    # Conditional/neutral jobs are legitimate completed jobs, not pending work.
    if any(row.get("status") != "completed" or not row.get("conclusion") for row in rows):
        return "pending"
    return "success"


def _classify(text: str, steps: list[dict], conclusion: str) -> tuple[str, list[str]]:
    """Hypotheses, not verdicts. Environment evidence wins over test keywords."""
    lower = text.lower()
    failed_steps = [str(s.get("name", "")) for s in steps if s.get("conclusion") in _FAILURES]
    dependency_step = any(word in " ".join(failed_steps).lower()
                          for word in ("install", "resolve dependencies", "download dependencies"))
    for category, patterns in (
        ("dependency", ("eresolve", "could not resolve dependencies", "no matching distribution", "npm err! 404",
                        "unable to resolve dependency", "registry.npmjs.org", "pypi.org", "failed to fetch package")),
        ("infrastructure", ("runner lost", "lost communication", "runner disconnected", "no space left on device",
                            "service unavailable", "502 bad gateway", "runner failed", "failed to start container")),
        ("code", ("assertionerror", "assertion failed", "syntaxerror", "compilation failed", "error ts", "cannot find symbol")),
    ):
        matches = [p for p in patterns if p in lower]
        if matches:
            if category == "code" and dependency_step:
                return "dependency", [f"Failing dependency step: {s}" for s in failed_steps]
            return category, [f"Log contains: {p}" for p in matches] + [f"Failing step: {s}" for s in failed_steps]
    if dependency_step:
        return "dependency", [f"Failing dependency step: {s}" for s in failed_steps]
    if conclusion == "startup_failure":
        return "infrastructure", ["GitHub conclusion: startup_failure"]
    return "unknown", ["No diagnostic failure signature; more evidence is required"]


async def diagnose(github: GitHub, repository: Any, *, max_pages: int = 5,
                   max_commits: int = 100, max_attempts: int = 5,
                   max_log_chars: int = 4000, max_jobs: int = 200) -> dict[str, Any]:
    """Return JSON-safe evidence, bounded histories, and an optional bisection interval.

    Bounds are hard ceilings. Completeness covers the inspected head/boundary
    window, not the entire lifetime of the repository.
    External checks/statuses are collected for historical SHAs too. Missing workflow
    or external coverage is explicitly incomplete, including a missing run_attempt.
    ``attempt_history`` retains superseded failures; ``failures`` contains only the
    selected current-head attempts and unmatched current-head checks.
    """
    if not (1 <= max_pages <= 10 and 1 <= max_commits <= 200 and 1 <= max_attempts <= 10
            and 1 <= max_log_chars <= 16000 and 1 <= max_jobs <= 500):
        raise ValueError("Diagnosis bounds exceed supported limits")
    name = repository["full_name"] if isinstance(repository, dict) else repository.full_name
    branch = repository["default_branch"] if isinstance(repository, dict) else repository.default_branch
    base = f"/repos/{name}"
    gaps: list[str] = []
    result: dict[str, Any] = dict(head=None, state="incomplete", complete=False, last_good_sha=None,
                                  first_bad_sha=None, candidate_interval=None, needs_bisection=False,
                                  failures=[], attempt_history=[], classification="unknown",
                                  confidence="low", evidence=[], incomplete=gaps)
    # Installation credentials resolve off the event loop, just as GitHub requests do.
    try:
        token = github.token
        if not isinstance(token, str):
            token = await asyncio.to_thread(token.resolve)
        extra = (token,)
    except Exception:
        gaps.append("Workspace credential unavailable; diagnosis refused")
        return result

    def finish() -> dict[str, Any]:
        # Scrub every egress field, including job names, URLs and provider metadata.
        def scrub(value: Any) -> Any:
            if isinstance(value, str):
                return redact(value, extra)
            if isinstance(value, list):
                return [scrub(item) for item in value]
            if isinstance(value, dict):
                return {scrub(key): scrub(item) for key, item in value.items()}
            return value
        return scrub(result)

    async def get(path: str, params: dict | None = None) -> Any:
        try:
            return await github.request("GET", base + path, params=params)
        except (GitHubError, OSError, ValueError):
            # Provider errors can contain credentials. Never include their message.
            gaps.append(f"Unavailable GitHub evidence: {path}")
            return None

    async def pages(path: str, key: str, params: dict | None = None, *,
                    bounded_history: bool = False) -> tuple[list[dict], bool]:
        rows: list[dict] = []
        seen: set[Any] = set()
        for page in range(1, max_pages + 1):
            data = await get(path, {**(params or {}), "per_page": 100, "page": page})
            if not isinstance(data, dict) or not isinstance(data.get(key), list) or type(data.get("total_count")) is not int:
                break
            batch = data[key]
            if len(batch) > 100 or any(not isinstance(row, dict) for row in batch):
                break
            for row in batch:
                identity = row.get("id")
                if identity is None or identity in seen:
                    continue
                seen.add(identity)
                rows.append(row)
            if len(rows) >= data["total_count"]:
                return rows, True
            # A short page is NOT the end when total_count says more exists.
            if not batch:
                break
        else:
            # Only an exhausted budget of full pages is an intentional window.
            # Short/missing/malformed pages still indicate unavailable evidence.
            if bounded_history and len(rows) == max_pages * 100:
                return rows, False
        gaps.append(f"Incomplete pagination: {path}")
        return rows, False

    try:
        head = await github.branch_sha(name, branch)
    except (GitHubError, OSError, ValueError):
        gaps.append("Default-branch head unavailable")
        return finish()
    result["head"] = head
    result["url"] = f"https://github.com/{name}/commit/{head}"
    history_gap_count = len(gaps)
    runs, history_complete = await pages("/actions/runs", "workflow_runs", {"branch": branch},
                                         bounded_history=True)
    history_window_available = len(gaps) == history_gap_count
    chain: list[str] = []
    merges: set[str] = set()
    if not runs:
        gaps.append("No default-branch workflow history available")
    snapshots: dict[str, dict] = {}
    owned_ids: set[Any] = set()
    owned_urls: set[str] = set()
    run_urls: set[str] = set()
    remaining_jobs = max_jobs

    async def failure(row: dict, metadata: dict, *, external: bool = False) -> dict:
        text = ""
        if external:
            # Third-party check IDs are not Actions job IDs. Their diagnostic
            # output is supplied inline by the Checks API, not the job log API.
            output = row.get("output") or {}
            text = "\n".join(output[key] for key in ("summary", "text") if output.get(key))
        else:
            check = {**row, "kind": "check_run"}
            try:
                # The helper scrubs the WHOLE bounded download before returning a tail.
                text = await github.job_log_tail(name, check, limit=max_log_chars, redact_extra=extra)
            except (GitHubError, OSError, ValueError):
                pass
        text = redact(text, extra)[-max_log_chars:]
        category, evidence = _classify(text, row.get("steps", []), row.get("conclusion", ""))
        if external:
            evidence.append("Source: check-run output.summary and output.text")
        if not text:
            evidence.append("Check output unavailable or empty" if external else "Job log unavailable or empty")
        return {**metadata, "job_id": row.get("id"), "job_name": row.get("name", ""),
                "url": row.get("html_url", ""), "conclusion": row.get("conclusion"),
                "excerpt": text, "classification": category, "confidence": "medium" if category != "unknown" else "low",
                "evidence": evidence}

    async def inspect(sha: str) -> None:
        nonlocal remaining_jobs
        # A capped branch listing is a discovery window, not proof that all of
        # this revision's workflows were returned. Prove its coverage separately.
        revision_runs = runs
        revision_complete = True
        if not history_complete:
            revision_runs, revision_complete = await pages(
                "/actions/runs", "workflow_runs", {"branch": branch, "head_sha": sha})
        selected: dict[tuple[str, Any], dict] = {}
        for run in revision_runs:
            if run.get("head_sha") != sha:
                continue
            workflow = run.get("workflow_id")
            if workflow is None or not run.get("id"):
                gaps.append(f"Workflow identity missing: {sha}")
                revision_complete = False
                continue
            key = (sha, workflow)
            previous = selected.get(key)
            # Execution/attempt identity, never completion timestamps.
            order = lambda r: (r.get("run_number", r["id"]), r["id"], r.get("run_attempt") or 0)
            if previous is None or order(run) > order(previous):
                selected[key] = run
        snapshots[sha] = {"workflows": {}, "complete": revision_complete, "external": {}}
        executions: dict[Any, dict] = {}
        for run in revision_runs:
            if run.get("head_sha") != sha or run.get("workflow_id") is None or not run.get("id"):
                continue
            previous = executions.get(run["id"])
            if previous is None or (run.get("run_attempt") or 0) > (previous.get("run_attempt") or 0):
                executions[run["id"]] = run
        for run in executions.values():
            sha, workflow = run["head_sha"], run["workflow_id"]
            is_selected = selected[sha, workflow]["id"] == run["id"]
            latest = run.get("run_attempt")
            run_urls.add(run.get("html_url", ""))
            snap = snapshots.setdefault(sha, {"workflows": {}, "complete": True, "external": {}})
            if type(latest) is not int or latest < 1:
                gaps.append(f"Missing run_attempt: run {run['id']}")
                if is_selected:
                    snap["complete"] = False
                    snap["workflows"][workflow] = "incomplete"
                continue
            if latest > max_attempts:
                gaps.append(f"Attempt history bounded: run {run['id']}")
            for attempt in range(max(1, latest - max_attempts + 1), latest + 1):
                jobs, complete = await pages(f"/actions/runs/{run['id']}/attempts/{attempt}/jobs", "jobs")
                if len(jobs) > remaining_jobs:
                    jobs, complete = jobs[:remaining_jobs], False
                    gaps.append("Job inspection budget exhausted")
                remaining_jobs -= len(jobs)
                if not jobs:
                    gaps.append(f"No jobs: run {run['id']} attempt {attempt}")
                meta = dict(sha=sha, workflow_id=workflow, workflow_name=run.get("name", ""),
                            run_id=run["id"], attempt=attempt, run_url=run.get("html_url", ""))
                entries = []
                for job in jobs:
                    owned_ids.add(job.get("id"))
                    owned_urls.add(job.get("html_url", ""))
                    entry = {**meta, "job_id": job.get("id"), "job_name": job.get("name", ""),
                             "url": job.get("html_url", ""), "conclusion": job.get("conclusion"),
                             "status": job.get("status")}
                    if job.get("conclusion") in _FAILURES:
                        entry.update(await failure(job, meta))
                    entries.append(entry)
                result["attempt_history"].extend(entries)
                if attempt == latest and is_selected:
                    settled = all(row.get("status") == "completed" and row.get("conclusion")
                                  and row.get("conclusion") != "stale" for row in [run, *jobs])
                    snap["complete"] &= complete and bool(jobs) and settled
                    if not settled:
                        gaps.append(f"Pending run/jobs: run {run['id']} attempt {attempt}")
                    # The workflow itself must also be completed successfully.
                    snap["workflows"][workflow] = _state([run, *jobs]) if jobs else "incomplete"
                    if sha == head:
                        result["failures"].extend(e for e in entries if e.get("classification"))

        # CheckSummary alone is intentionally insufficient: its legacy callers use a
        # first-page/fallback view. Independently prove pagination and ownership here.
        snap = snapshots.setdefault(sha, {"workflows": {}, "complete": True, "external": {}})
        summary = None
        try:
            summary = await github.checks(name, sha)
        except (GitHubError, OSError, ValueError):
            snap["complete"] = False
            gaps.append(f"Check summary unavailable: {sha}")
        checks, checks_ok = await pages(f"/commits/{sha}/check-runs", "check_runs", {"filter": "latest"})
        statuses, statuses_ok = await pages(f"/commits/{sha}/status", "statuses")
        snap["complete"] &= checks_ok and statuses_ok
        # Expectations belong to this revision, not to a union of CI names from
        # other commits. A removed/renamed workflow or external context must not
        # be required forever. The independent summary can witness checks that
        # are missing from the paginated snapshot (including Actions fallbacks).
        if summary is not None:
            check_ids = {check.get("id") for check in checks}
            check_ids.update(entry["job_id"] for entry in result["attempt_history"] if entry["sha"] == sha)
            run_ids = {run["id"] for run in executions.values() if run["head_sha"] == sha}
            status_contexts = {status.get("context") for status in statuses}
            for check in summary.checks:
                if check.get("kind") == "status":
                    present = check.get("name") in status_contexts
                elif check.get("kind") == "workflow":
                    present = check.get("id") in run_ids
                else:
                    present = check.get("id") in check_ids
                if not present:
                    snap["complete"] = False
                    gaps.append(f"Missing CI coverage for {sha}: {check.get('name', 'unnamed check')}")
            if not summary.checks and summary.state != "success":
                snap["complete"] = False
                gaps.append(f"Check summary has no settled evidence: {sha}")
        for check in checks:
            url = check.get("html_url") or ""
            if check.get("conclusion") == "stale":
                snap["complete"] = False
                gaps.append(f"Stale check evidence: {sha}/{check.get('id')}")
            if (check.get("id") in owned_ids or (url and url in owned_urls)
                    or any(u and (url == u or url.startswith(u + "/")) for u in run_urls)):
                continue  # selected or historical Actions ownership, not unknown CI
            # An unmatched Actions check cannot silently count as an external pass.
            if (check.get("app") or {}).get("slug") == "github-actions":
                snap["complete"] = False
                gaps.append(f"Unmatched Actions check: {sha}/{check.get('id')}")
            key = f"check:{(check.get('app') or {}).get('id')}:{check.get('name')}"
            snap["external"][key] = _state([check])
            if sha == head and check.get("conclusion") in _FAILURES:
                result["failures"].append(await failure(check, {"sha": sha, "workflow_id": None,
                                                               "run_id": None, "attempt": None}, external=True))
        # Combined statuses are latest per context; retain the greatest id if a
        # provider returns old duplicate contexts across pages.
        latest_statuses: dict[str, dict] = {}
        for status in statuses:
            context = status.get("context", "")
            if context.startswith("ai-factory/"):
                continue
            if context not in latest_statuses or status["id"] > latest_statuses[context]["id"]:
                latest_statuses[context] = status
        for context, status in latest_statuses.items():
            state = status.get("state")
            snap["external"][f"status:{context}"] = state if state in ("success", "failure", "pending", "error") else "incomplete"
            if sha == head and state in ("failure", "error"):
                result["failures"].append(dict(sha=sha, workflow_id=None, run_id=None, attempt=None,
                    job_name=context, url=status.get("target_url", ""), classification="unknown", confidence="low",
                    excerpt=redact(status.get("description") or "", extra)[-max_log_chars:],
                    evidence=[f"Commit status {context}: {state}; no job log available"]))

    def sha_state(sha: str) -> str:
        snap = snapshots.get(sha)
        if snap is None:
            return "incomplete"
        states = list(snap["workflows"].values()) + list(snap["external"].values())
        if any(s in ("failure", "error") for s in states):
            return "failure"
        if not snap["complete"] or not states or "incomplete" in states:
            return "incomplete"
        if "pending" in states:
            return "pending"
        return "success"

    # Walk only the first-parent window needed to prove the nearest boundary.
    # Old or force-pushed run SHAs are not ancestry requirements. Each inspected
    # revision includes its own Actions jobs, external checks and commit statuses.
    cursor = head
    boundary_found = False
    for _ in range(max_commits):
        chain.append(cursor)
        await inspect(cursor)
        if sha_state(cursor) == "success":
            boundary_found = True
            break
        commit = await get(f"/commits/{quote(cursor, safe='')}")
        if not isinstance(commit, dict) or not isinstance(commit.get("parents"), list):
            gaps.append(f"Missing commit ancestry: {cursor}")
            break
        parents = commit["parents"]
        if len(parents) > 1:
            merges.add(cursor)
        if not parents:
            break
        if not parents[0].get("sha") or parents[0]["sha"] in chain:
            gaps.append(f"Invalid commit ancestry: {cursor}")
            break
        cursor = parents[0]["sha"]
    if not boundary_found:
        gaps.append("Commit ancestry window incomplete: no verified pass/fail boundary")

    result["states"] = {sha: sha_state(sha) for sha in chain}
    result["state"] = sha_state(head)
    if result["state"] == "success" and gaps:
        result["state"] = "incomplete"
    head_snapshot = snapshots[head]
    if "pending" in head_snapshot["workflows"].values() or "pending" in head_snapshot["external"].values():
        gaps.append("Current-head CI is still pending")
    result["complete"] = not gaps and result["state"] not in ("incomplete", "pending")
    if result["state"] == "success" and result["complete"]:
        result["last_good_sha"] = head

    # A fail/pass on the same SHA is flaky *test* evidence only when the failed
    # step/signature points at tests, never when an install step mentions pytest.
    history = result["attempt_history"]
    for entry in history:
        if entry.get("classification") != "code":
            continue
        test_failure = any(p in entry.get("excerpt", "").lower() for p in ("assertionerror", "assertion failed"))
        passes = [e for e in history if e["sha"] == entry["sha"] and e["workflow_id"] == entry["workflow_id"]
                  and e["job_name"] == entry["job_name"] and e.get("conclusion") == "success"]
        if test_failure and passes:
            entry["classification"] = "flaky"
            entry["evidence"].append("Same SHA and workflow/job passed: " + ", ".join(
                f"run {p['run_id']} attempt {p['attempt']} ({p['url']})" for p in passes))
            # failures and history share the same evidence objects.
    active = result["failures"]
    classes = {e["classification"] for e in active}
    if not active:
        classes = {e["classification"] for e in history if e.get("classification") == "flaky" and e["sha"] == head}
    if len(classes) == 1:
        result["classification"] = classes.pop()
    result["confidence"] = "medium" if result["classification"] != "unknown" else "low"
    result["evidence"] = [dict(job_name=e["job_name"], url=e["url"], run_id=e["run_id"], attempt=e["attempt"],
                                classification=e["classification"], reasons=e["evidence"])
                          for e in active or history if e.get("classification")]

    if result["state"] == "failure":
        # Only inspected, revision-local evidence can establish the boundary.
        good_index = next((i for i, sha in enumerate(chain) if i > 0 and sha_state(sha) == "success"), None)
        if history_window_available and boundary_found and good_index is not None:
            good = chain[good_index]
            result["last_good_sha"] = good
            bad_index = max(i for i in range(good_index) if sha_state(chain[i]) == "failure")
            candidates = list(reversed(chain[bad_index:good_index]))
            result["candidate_interval"] = dict(good=good, bad=chain[bad_index], commits=candidates)
            bad_snapshot = snapshots[chain[bad_index]]
            if len(candidates) == 1 and candidates[0] not in merges and bad_snapshot["complete"]:
                result["first_bad_sha"] = candidates[0]
        result["needs_bisection"] = result["first_bad_sha"] is None
        if result["needs_bisection"]:
            result["evidence"].append({"reason": "Request sandbox bisection: CI/ancestry does not prove an exact culprit"})
    return finish()
