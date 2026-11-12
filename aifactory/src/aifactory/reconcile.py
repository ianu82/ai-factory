"""Keep the Factory's view of its pull requests in step with GitHub.

People can merge or close a Factory pull request on GitHub while the task waits on a decision
(merge approval, a stuck task). Without this, the Inbox keeps asking about a PR that is already
merged. A merged PR hands the task back to the merge stage, which records who merged it and runs
smoke checks; a PR closed without merging becomes one clear decision.
"""

from __future__ import annotations

import logging

from sqlalchemy import or_

from .db import session_scope, utcnow
from .events import notify, record
from .github import GitHub, GitHubError, resolve_credential as resolve_token
from .models import Decision, Repository, Task

log = logging.getLogger(__name__)
WAITING = ("awaiting_approval", "needs_input", "failed", "blocked")


async def reconcile_pull_requests() -> int:
    """Returns how many tasks changed."""
    with session_scope() as s:
        rows = []
        for task in s.query(Task).filter(or_(
                Task.status.in_(WAITING),
                Task.stage.in_(("revert", "ci", "revert_merge")),
                Task.status.in_(("cancelled", "handed_over", "abandoned")))):
            candidate = (task.evidence or {}).get("revert_candidate")
            is_revert = bool(candidate and not (task.evidence or {}).get("revert"))
            if not is_revert and (not _still_waiting(task, task.pr_number)
                                  or not task.pr_number or (task.evidence or {}).get("pr_closed")):
                continue
            repo = s.get(Repository, task.repo_id)
            if repo:
                rows.append((task.id, task.workspace_id, repo.full_name,
                             candidate["pr"] if is_revert else task.pr_number, is_revert))
    changed = 0
    clients: dict[str, GitHub] = {}
    for task_id, ws, full_name, number, is_revert in rows:
        try:
            gh = clients.get(ws) or clients.setdefault(ws, GitHub(resolve_token(ws)))
            pr = await gh.pull(full_name, number)
        except GitHubError as exc:
            log.debug("reconcile skipped %s#%s: %s", full_name, number, exc)
            continue
        if pr.get("merged"):
            changed += _handle_reverted(task_id, pr) if is_revert else _handle_merged(task_id, pr)
        elif not is_revert and pr.get("state") == "closed":
            changed += _handle_closed(task_id, pr)
    if changed:
        from .pipeline.orchestrator import orchestrator

        orchestrator.wake()
    return changed


def _supersede(s, task_id: str, why: str) -> None:
    for d in s.query(Decision).filter(Decision.task_id == task_id, Decision.status == "open"):
        d.status, d.comment = "superseded", why


def _still_waiting(task: Task | None, number: int) -> bool:
    """People may have resolved the decision while GitHub was being asked: only act on an unchanged wait."""
    return bool(task and task.status in WAITING and task.pr_number == number and not task.merge_sha)


def _handle_merged(task_id: str, pr: dict) -> bool:
    who = (pr.get("merged_by") or {}).get("login") or "someone"
    with session_scope() as s:
        task = s.get(Task, task_id)
        if not _still_waiting(task, pr.get("number")):
            return False
        _supersede(s, task_id, f"PR #{task.pr_number} was merged on GitHub by {who}")
        task.status, task.stage, task.blocked_reason = "queued", "merge", ""
        s.flush()  # commit queue correlation with the reconciliation observation
        record(task.workspace_id, "merged_outside", f"PR #{task.pr_number} was merged on GitHub by {who}; "
               "finishing the task (smoke checks)", outcome_id=task.outcome_id, task_id=task.id,
               data={**(task.evidence or {}).get("telemetry", {}), "pr": task.pr_number,
                     "head": (pr.get("head") or {}).get("sha"), "merge_sha": pr.get("merge_commit_sha")}, session=s)
        ws, outcome_id = task.workspace_id, task.outcome_id
    notify(ws, "decision", task_id)
    notify(ws, "outcome", outcome_id)
    return True


def _handle_closed(task_id: str, pr: dict) -> bool:
    from .pipeline.runtime import open_decision

    with session_scope() as s:
        task = s.get(Task, task_id)
        if not _still_waiting(task, pr.get("number")):
            return False
        number = task.pr_number
        _supersede(s, task_id, f"PR #{number} was closed on GitHub without merging")
        # Forget the closed PR so a rebuild starts fresh and opens a new one (retrying a "failed" task resets attempts).
        task.evidence = {**(task.evidence or {}), "closed_prs": [*(task.evidence or {}).get("closed_prs", []), number]}
        task.status, task.stage, task.pr_number, task.pr_url = "failed", "", None, ""
        task.blocked_reason = f"PR #{number} was closed on GitHub without merging."
        record(task.workspace_id, "pr_closed_outside", f"PR #{number} was closed on GitHub without merging",
               outcome_id=task.outcome_id, task_id=task.id, level="warn", session=s)
        ws, outcome_id, key = task.workspace_id, task.outcome_id, task.key
    open_decision(ws, "unblock", f"{key}: PR #{number} was closed on GitHub",
                  f"PR #{number} was closed on GitHub without merging. Rebuild it, or abandon the task.",
                  outcome_id=outcome_id, task_id=task_id,
                  options=[{"value": "retry", "label": "Rebuild and open a new PR"},
                           {"value": "abandon", "label": "Abandon task"}])
    return True


def _handle_reverted(task_id: str, pr: dict) -> bool:
    """Record an irreversible GitHub result, even if delivery was stopped meanwhile.

    This is bookkeeping, not permission to resume work. Evidence, terminal state,
    decision settlement and the audit event commit together, once per candidate.
    """
    if not pr.get("merged"):
        return False
    with session_scope() as s:
        task = s.query(Task).filter(Task.id == task_id).with_for_update().one_or_none()
        candidate = (task.evidence or {}).get("revert_candidate") if task else None
        if (not candidate or candidate["pr"] != pr.get("number")
                or candidate["head"] != (pr.get("head") or {}).get("sha")
                or (task.evidence or {}).get("revert")):
            return False
        now = utcnow()
        task.evidence = {**task.evidence, "revert": {
            "pr": candidate["pr"], "sha": pr.get("merge_commit_sha", ""),
            "ci": (task.evidence.get("ci") or {}).get("state", "unknown"), "at": now.isoformat()}}
        task.status, task.stage, task.finished_at = "reverted", "done", now
        task.lease_owner, task.blocked_reason = "", ""
        task.summary = f"Reverted after post-merge smoke failed (revert PR #{candidate['pr']})"
        _supersede(s, task_id, f"Revert PR #{candidate['pr']} was merged on GitHub")
        record(task.workspace_id, "reverted", f"Reverted via PR #{candidate['pr']}",
               outcome_id=task.outcome_id, task_id=task_id, level="warn",
               data={"url": candidate.get("url", "")}, session=s)
        ws, outcome_id = task.workspace_id, task.outcome_id
    notify(ws, "task", task_id, outcome_id=outcome_id)
    notify(ws, "decision", task_id)
    notify(ws, "outcome", outcome_id)
    return True
