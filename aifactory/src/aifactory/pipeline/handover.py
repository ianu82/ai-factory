"""Take over: a person stops the Factory on a task and continues from its latest candidate.

The task ends as ``handed_over`` (honestly not delivered by the Factory). If the candidate was
never published, the latest attempt's diff is pushed to the task branch and a draft pull request
is opened, so the person starts from the Factory's work instead of from nothing.
"""

from __future__ import annotations

import logging
from typing import Any

from ..db import session_scope, utcnow
from ..events import notify, record
from ..github import GitHub, GitHubError, resolve_credential as resolve_token
from ..gitops import Patch, publish
from ..models import Artifact, Decision, Outcome, Repository, Task, User

log = logging.getLogger(__name__)

NOT_TAKEABLE = {"completed", "abandoned", "cancelled", "reverted", "handed_over"}


class HandoverError(RuntimeError):
    pass


def mark_handed_over(task_id: str, user: User, *, action_id: str | None = None, session=None,
                     pending_events: list[dict] | None = None) -> dict[str, Any]:
    """Stop the Factory's work on the task (status, open decisions). Returns what publishing needs.

    With a session (a confirmed Mind card), the change joins the caller's transaction and its event waits in
    pending_events until the caller has committed; it is attributed "<person> via Mind"."""
    if session is not None:
        return _hand_over(session, task_id, user, action_id, pending_events)
    with session_scope() as s:
        return _hand_over(s, task_id, user, None, None)


def _hand_over(s, task_id: str, user: User, action_id: str | None, pending_events: list[dict] | None) -> dict[str, Any]:
    who = user.name or user.email
    task = s.get(Task, task_id)
    if task is None:
        raise HandoverError("Task not found")
    if task.status in NOT_TAKEABLE:
        raise HandoverError(f"{task.key} is already {task.status.replace('_', ' ')}")
    task.status, task.stage = "handed_over", ""
    task.blocked_reason = f"Handed over to {who}"
    task.finished_at = utcnow()
    for d in s.query(Decision).filter(Decision.task_id == task_id, Decision.status == "open"):
        d.status = "superseded"
    diff = (s.query(Artifact).filter(Artifact.task_id == task_id, Artifact.kind == "diff")
            .order_by(Artifact.created_at.desc()).first())
    outcome = s.get(Outcome, task.outcome_id)
    repo = s.get(Repository, task.repo_id)
    actor = who + (" via Mind" if action_id else "")
    payload = record(task.workspace_id, "task_handed_over", f"{actor} took over {task.key}; the Factory stopped working on it",
                     outcome_id=task.outcome_id, task_id=task.id, actor=user.id, level="warn", session=s,
                     data={"source": "mind", "action_id": action_id} if action_id else None,
                     publish=pending_events is None)
    if pending_events is not None:
        pending_events.append(payload)
    return {"workspace_id": task.workspace_id, "outcome_id": task.outcome_id, "key": task.key, "title": task.title,
            "number": outcome.number if outcome else 0, "pr_url": task.pr_url, "branch": task.branch,
            "base_sha": task.base_sha, "repo": repo.full_name if repo else "",
            "default_branch": repo.default_branch if repo else "main",
            "diff": diff.content if diff else "", "files": (diff.data or {}).get("files", []) if diff else [],
            "who": who}


async def publish_candidate(task_id: str, info: dict[str, Any]) -> dict[str, Any]:
    """Push the latest unpublished candidate and open a draft PR. Best effort; returns what happened."""
    ws = info["workspace_id"]
    if info["pr_url"]:
        return {"pr_url": info["pr_url"], "published": False}
    if not info["diff"].strip() or not info["base_sha"] or not info["repo"]:
        record(ws, "handover_no_candidate", f"{info['key']} had no candidate change yet; nothing was pushed",
               outcome_id=info["outcome_id"], task_id=task_id)
        return {"published": False}
    branch = info["branch"] or f"factory/{info['number']}-{info['key'].lower()}-{task_id[:6]}"
    token = resolve_token(ws)
    try:
        head = await publish(info["repo"], token, base_sha=info["base_sha"], branch=branch,
                             patch=Patch(diff=info["diff"].encode(), files=info["files"], additions=0, deletions=0),
                             message=f"{info['title']} (Factory candidate, handed over)", task_id=task_id + "-handover")
        body = (f"Work in progress handed over to {info['who']} from MindsHub AI Factory outcome #{info['number']} "
                f"({info['key']}). This is the Factory's latest candidate; it has **not** passed independent "
                "verification. The Factory will not touch this branch again.\n\n🤖 MindsHub AI Factory")
        pr = await GitHub(token).create_pull(info["repo"], head=branch, base=info["default_branch"],
                                             title=f"[handed over] {info['title']}", body=body, draft=True)
    except (GitHubError, Exception) as exc:  # noqa: BLE001 - the hand-over itself already happened
        log.warning("handover publish failed for %s: %s", task_id, exc)
        record(ws, "handover_publish_failed", f"Could not push the candidate for {info['key']}: {str(exc)[:300]}",
               outcome_id=info["outcome_id"], task_id=task_id, level="warn")
        return {"published": False, "error": str(exc)[:300]}
    with session_scope() as s:
        task = s.get(Task, task_id)
        task.branch, task.head_sha = branch, head
        task.pr_number, task.pr_url = pr.get("number"), pr.get("html_url", "")
        # GitHub's own flag: an existing open PR may have been reused rather than created as a draft.
        task.evidence = {**(task.evidence or {}), "pr_draft": bool(pr.get("draft"))}
    record(ws, "handover_published", f"Pushed the candidate to {branch} and opened draft PR #{pr.get('number')}",
           outcome_id=info["outcome_id"], task_id=task_id, data={"pr_url": pr.get("html_url", "")})
    notify(ws, "task", task_id, outcome_id=info["outcome_id"])
    return {"published": True, "pr_url": pr.get("html_url", ""), "branch": branch}


async def close_dropped_pull(task_id: str, who: str, why: str) -> bool:
    """Close the open pull request of a task people abandoned or cancelled (reopening it is always possible)."""
    with session_scope() as s:
        task = s.get(Task, task_id)
        if task is None or not task.pr_number or (task.evidence or {}).get("pr_closed"):
            return False
        repo = s.get(Repository, task.repo_id)
        ws, outcome_id, number, full_name = task.workspace_id, task.outcome_id, task.pr_number, repo.full_name
    gh = GitHub(resolve_token(ws))
    try:
        pr = await gh.pull(full_name, number)
        if pr.get("state") != "open":
            return False
        await gh.comment(full_name, number, f"{why} in MindsHub AI Factory by {who}, so the Factory closed this pull request. "
                                            "Reopen it to continue the work yourself.\n\n🤖 MindsHub AI Factory")
        await gh.request("PATCH", f"/repos/{full_name}/pulls/{number}", json={"state": "closed"})
    except GitHubError as exc:
        record(ws, "pr_close_failed", f"Could not close PR #{number}: {str(exc)[:200]}", outcome_id=outcome_id,
               task_id=task_id, level="warn")
        return False
    with session_scope() as s:
        task = s.get(Task, task_id)
        task.evidence = {**(task.evidence or {}), "pr_closed": True}
    record(ws, "pr_closed", f"Closed PR #{number} ({why.lower()} by {who})", outcome_id=outcome_id, task_id=task_id)
    return True
