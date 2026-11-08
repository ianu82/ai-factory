"""Pull requests across a workspace's repositories, grouped by what each needs next.

GitHub is the source of truth: open (and recently merged) pull requests are read from every
connected repository, classified as ready to merge, needing attention, waiting on checks or
draft, and linked to the Factory task that opened them when there is one. Results are cached
briefly per workspace so the page can poll without spending the token's rate limit.
"""

from __future__ import annotations

import asyncio
import logging
import time
from typing import Any

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from .auth import current_user, require_member
from .db import get_db, session_scope
from .github import GitHub, GitHubError, resolve_token
from .models import Decision, Outcome, Repository, Task, User

log = logging.getLogger(__name__)
router = APIRouter(prefix="/api")

TTL_SECONDS = 45
HUMAN_TASK = {"needs_input", "failed", "blocked", "smoke_failed"}
_cache: dict[tuple[str, str], tuple[float, dict[str, Any]]] = {}
_details: dict[tuple[str, int, str], dict[str, Any]] = {}
_checks: dict[tuple[str, str], tuple[float, dict[str, Any]]] = {}


def _factory_links(workspace_id: str) -> dict[tuple[str, int], dict[str, Any]]:
    """Tasks that opened pull requests, keyed by (repository, PR number)."""
    with session_scope() as s:
        repos = {r.id: r.full_name for r in s.query(Repository).filter(Repository.workspace_id == workspace_id)}
        decisions = {d.task_id: d.id for d in s.query(Decision).filter(
            Decision.workspace_id == workspace_id, Decision.status == "open", Decision.task_id.isnot(None))}
        links = {}
        for task in s.query(Task).filter(Task.workspace_id == workspace_id, Task.pr_number.isnot(None)):
            outcome = s.get(Outcome, task.outcome_id)
            links[(repos.get(task.repo_id, ""), task.pr_number)] = {
                "outcome_id": task.outcome_id, "outcome_number": outcome.number if outcome else None,
                "task_id": task.id, "task_key": task.key, "status": task.status, "stage": task.stage,
                "blocked_reason": (task.blocked_reason or "")[:300], "decision_id": decisions.get(task.id)}
        return links


async def _detail(gh: GitHub, repo: str, pr: dict[str, Any]) -> dict[str, Any]:
    key = (repo, pr["number"], pr.get("updated_at") or "")
    if key in _details:
        return _details[key]
    full = await gh.request("GET", f"/repos/{repo}/pulls/{pr['number']}")
    detail = {"additions": full.get("additions", 0), "deletions": full.get("deletions", 0),
              "changed_files": full.get("changed_files", 0), "mergeable_state": full.get("mergeable_state") or "unknown",
              "changes_requested_by": []}
    if not pr.get("draft") and pr.get("state") == "open":
        try:
            reviews = await gh.request("GET", f"/repos/{repo}/pulls/{pr['number']}/reviews", params={"per_page": 100})
            latest: dict[str, str] = {}
            for review in reviews:
                login = (review.get("user") or {}).get("login", "")
                if review.get("state") in ("APPROVED", "CHANGES_REQUESTED", "DISMISSED"):
                    latest[login] = review["state"]
            detail["changes_requested_by"] = sorted(u for u, st in latest.items() if st == "CHANGES_REQUESTED")
        except GitHubError:
            pass
    if detail["mergeable_state"] != "unknown":  # GitHub computes mergeability lazily; don't cache "unknown"
        _details[key] = detail
    return detail


async def _ci(gh: GitHub, repo: str, sha: str) -> dict[str, Any]:
    cached = _checks.get((repo, sha))
    if cached and (cached[1]["state"] in ("success", "failure") or time.monotonic() - cached[0] < TTL_SECONDS):
        return cached[1]
    summary = await gh.checks(repo, sha)
    failing = [c["name"] for c in summary.checks if c.get("conclusion") in ("failure", "error", "timed_out", "cancelled")]
    pending = [c["name"] for c in summary.checks if c.get("status") != "completed"]
    result = {"state": summary.state, "total": len(summary.checks), "failing": failing, "pending": pending}
    _checks[(repo, sha)] = (time.monotonic(), result)
    return result


def classify(row: dict[str, Any]) -> tuple[str, str]:
    """(group, reason) for an open pull request."""
    factory, ci, detail = row.get("factory") or {}, row.get("ci") or {}, row.get("detail") or {}
    if row.get("draft"):
        return "draft", "Draft"
    if factory.get("status") in HUMAN_TASK:
        return "attention", factory.get("blocked_reason") or "Waiting on a decision in AI Factory"
    if ci.get("state") == "failure":
        names = ", ".join(ci.get("failing") or []) or "a check"
        return "attention", f"Checks failed: {names}"
    if detail.get("mergeable_state") == "dirty":
        return "attention", "Merge conflict with the base branch"
    if detail.get("changes_requested_by"):
        return "attention", f"Changes requested by {', '.join(detail['changes_requested_by'])}"
    if ci.get("state") == "pending":
        waiting = len(ci.get("pending") or []) or ci.get("total") or 0
        return "checks", f"Waiting on {waiting} check{'s' if waiting != 1 else ''}"
    if factory.get("status") == "awaiting_approval":
        return "ready", "Checks passed; merge needs approval in AI Factory"
    if detail.get("mergeable_state") == "blocked":
        return "attention", "GitHub is blocking the merge (branch protection or required review)"
    if factory.get("status") in ("running", "queued") and factory.get("stage") in ("ci", "merge", "publish"):
        return "checks", "The Factory is merging this" if factory.get("stage") == "merge" else "The Factory is watching CI"
    return "ready", "Checks passed" if ci.get("state") == "success" else "No checks reported"


async def workspace_pulls(workspace_id: str, state: str = "open", refresh: bool = False) -> dict[str, Any]:
    key = (workspace_id, state)
    hit = _cache.get(key)
    if hit and not refresh and time.monotonic() - hit[0] < TTL_SECONDS:
        return hit[1]
    with session_scope() as s:
        repos = [r.full_name for r in s.query(Repository).filter(Repository.workspace_id == workspace_id)
                 .order_by(Repository.full_name)]
    links = _factory_links(workspace_id)
    errors: list[str] = []
    try:
        gh = GitHub(resolve_token(workspace_id))
    except GitHubError as exc:
        return {"items": [], "counts": {}, "errors": [str(exc)], "fetched_at": time.time(), "repositories": repos}
    limit = asyncio.Semaphore(6)
    rows: list[dict[str, Any]] = []

    async def build(repo: str, pr: dict[str, Any]) -> None:
        async with limit:
            row = {"repo": repo, "number": pr["number"], "title": pr.get("title", ""), "url": pr.get("html_url", ""),
                   "author": {"login": (pr.get("user") or {}).get("login", ""),
                              "avatar": (pr.get("user") or {}).get("avatar_url", "")},
                   "draft": bool(pr.get("draft")), "created_at": pr.get("created_at"), "updated_at": pr.get("updated_at"),
                   "merged_at": pr.get("merged_at"), "base": (pr.get("base") or {}).get("ref", ""),
                   "factory": links.get((repo, pr["number"]))}
            try:
                row["detail"] = await _detail(gh, repo, pr)
                if pr.get("state") == "open":
                    row["ci"] = await _ci(gh, repo, (pr.get("head") or {}).get("sha", ""))
            except GitHubError as exc:
                row["detail"], row["ci"] = row.get("detail") or {}, {"state": "unknown"}
                errors.append(f"{repo}#{pr['number']}: {str(exc)[:120]}")
            if pr.get("merged_at"):
                row["group"], row["reason"] = "merged", "Merged"
            else:
                row["group"], row["reason"] = classify(row)
            rows.append(row)

    jobs = []
    for repo in repos:
        try:
            if state in ("open", "ready", "all"):
                for pr in await gh.request("GET", f"/repos/{repo}/pulls",
                                           params={"state": "open", "per_page": 50, "sort": "updated", "direction": "desc"}):
                    jobs.append(build(repo, pr))
            if state in ("merged", "all"):
                for pr in await gh.request("GET", f"/repos/{repo}/pulls",
                                           params={"state": "closed", "per_page": 30, "sort": "updated", "direction": "desc"}):
                    if pr.get("merged_at"):
                        jobs.append(build(repo, pr))
        except GitHubError as exc:
            errors.append(f"{repo}: {str(exc)[:160]}")
    await asyncio.gather(*jobs)
    if state == "ready":
        rows = [r for r in rows if r["group"] == "ready"]
    order = {"ready": 0, "attention": 1, "checks": 2, "draft": 3, "merged": 4}
    rows.sort(key=lambda r: (order.get(r["group"], 9), -(_ts(r.get("merged_at") or r.get("updated_at")))))
    counts: dict[str, int] = {}
    for r in rows:
        counts[r["group"]] = counts.get(r["group"], 0) + 1
    result = {"items": rows, "counts": counts, "errors": errors, "fetched_at": time.time(),
              "repositories": repos}
    _cache[key] = (time.monotonic(), result)
    return result


def _ts(value: str | None) -> float:
    from datetime import datetime

    try:
        return datetime.fromisoformat((value or "").replace("Z", "+00:00")).timestamp()
    except ValueError:
        return 0.0


@router.get("/workspaces/{workspace_id}/pulls")
async def list_pulls(workspace_id: str, state: str = "open", refresh: bool = False,
                     user: User = Depends(current_user), db: Session = Depends(get_db)):
    require_member(db, user, workspace_id, "viewer")
    if state not in ("open", "ready", "merged", "all"):
        raise HTTPException(400, "state must be open, ready, merged or all")
    return await workspace_pulls(workspace_id, state, refresh)
