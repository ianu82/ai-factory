"""Keep the Factory's view of its pull requests in step with GitHub.

People can merge or close a Factory pull request on GitHub while the task waits on a decision
(merge approval, a stuck task). Without this, the Inbox keeps asking about a PR that is already
merged. A merged PR hands the task back to the merge stage, which records who merged it and runs
smoke checks; a PR closed without merging becomes one clear decision.
"""

from __future__ import annotations

import logging

from .db import session_scope
from .events import notify, record
from .github import GitHub, GitHubError, resolve_token
from .models import Decision, Repository, Task

log = logging.getLogger(__name__)
WAITING = ("awaiting_approval", "needs_input", "failed", "blocked")


async def reconcile_pull_requests() -> int:
    """Returns how many tasks changed."""
    with session_scope() as s:
        rows = []
        for task in s.query(Task).filter(Task.status.in_(WAITING), Task.pr_number.isnot(None), Task.merge_sha == ""):
            if (task.evidence or {}).get("pr_closed"):
                continue
            repo = s.get(Repository, task.repo_id)
            if repo:
                rows.append((task.id, task.workspace_id, repo.full_name, task.pr_number))
    changed = 0
    clients: dict[str, GitHub] = {}
    for task_id, ws, full_name, number in rows:
        try:
            gh = clients.get(ws) or clients.setdefault(ws, GitHub(resolve_token(ws)))
            pr = await gh.pull(full_name, number)
        except GitHubError as exc:
            log.debug("reconcile skipped %s#%s: %s", full_name, number, exc)
            continue
        if pr.get("merged"):
            _handle_merged(task_id, pr)
            changed += 1
        elif pr.get("state") == "closed":
            _handle_closed(task_id, pr)
            changed += 1
    if changed:
        from .pipeline.orchestrator import orchestrator

        orchestrator.wake()
    return changed


def _supersede(s, task_id: str, why: str) -> None:
    for d in s.query(Decision).filter(Decision.task_id == task_id, Decision.status == "open"):
        d.status, d.comment = "superseded", why


def _handle_merged(task_id: str, pr: dict) -> None:
    who = (pr.get("merged_by") or {}).get("login") or "someone"
    with session_scope() as s:
        task = s.get(Task, task_id)
        _supersede(s, task_id, f"PR #{task.pr_number} was merged on GitHub by {who}")
        task.status, task.stage, task.blocked_reason = "queued", "merge", ""
        record(task.workspace_id, "merged_outside", f"PR #{task.pr_number} was merged on GitHub by {who}; "
               "finishing the task (smoke checks)", outcome_id=task.outcome_id, task_id=task.id, session=s)
        ws, outcome_id = task.workspace_id, task.outcome_id
    notify(ws, "decision", task_id)
    notify(ws, "outcome", outcome_id)


def _handle_closed(task_id: str, pr: dict) -> None:
    from .pipeline.runtime import open_decision

    with session_scope() as s:
        task = s.get(Task, task_id)
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
