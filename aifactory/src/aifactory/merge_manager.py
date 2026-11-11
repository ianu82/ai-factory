"""The merge manager: one ordered merge queue per repository.

Without it, every task that reached the merge stage raced to merge. Each merge moved main, so every other waiting
task rebased, re-ran its checks and CI, and often re-verified, then raced again. The cost grew with the square of
the number of waiting tasks ("merge hell").

The manager gives one task per repository the merge turn at a time:

- Order: outcome priority (0 is urgent), then the time the task first reached the merge stage, then its id. A task
  that holds the turn keeps it through rebasing, revalidation and CI on the rebased head, so nothing behind it
  rebases for nothing.
- Waiting tasks are parked as queued at the merge stage. The scheduler does not dispatch them (no slot, no sandbox,
  no GitHub calls) until they reach the head of the queue.
- A rebuild leaves the queue: a task sent back to its builder re-enters at the back when it is ready again.
- Main red: while the default branch's latest CI is failing, the queue pauses (except for repair work: reverts and
  tasks of a "steward" or "ci_failure" signal outcome) and retries a few minutes later, so new merges never pile on
  top of a broken main. Repositories can turn this off with ``merge.hold_on_red_main: false``.
- GitHub merge queue: when the default branch has a GitHub merge queue (``merge.queue``: "auto", the default, or
  "github"), the turn holder is added to GitHub's queue instead of merged directly. GitHub tests the merge group
  in order, and the task completes when GitHub merges it.

State lives in task evidence under ``merge_queue`` (``entered_at``, ``turn``, ``granted_at``, ``retry_at``,
``paused``, ``github``), so it survives restarts with no schema change.
"""

from __future__ import annotations

from collections import defaultdict
from datetime import timedelta
from typing import Any

from .db import session_scope, utcnow
from .models import Outcome, Task

KEY = "merge_queue"
MEMBER_STATUSES = ("queued", "running", "waiting_ci")
RED_MAIN_RETRY = timedelta(minutes=3)
GITHUB_QUEUE_RETRY = timedelta(minutes=1)
REPAIR_SOURCES = {"steward", "ci_failure"}


def _entry(task: Task) -> dict[str, Any]:
    return dict((task.evidence or {}).get(KEY) or {})


def _sort_key(task: Task, priority: int) -> tuple:
    entry = _entry(task)
    # The current turn holder stays first; otherwise priority, then arrival.
    # A task paused behind a red main steps aside, so a repair (or anything else) behind it can reach the head.
    return (0 if entry.get("turn") else 1, entry.get("granted_at") or "", 1 if entry.get("paused") else 0, priority,
            entry.get("entered_at") or "", task.id)


def ordered(session, repo_id: str, *, include_github: bool = False) -> list[Task]:
    """The repository's merge queue, head first. Tasks already handed to GitHub's merge queue are ordered by
    GitHub, so they hold no Factory turn (they are listed only for display)."""
    rows = (session.query(Task, Outcome.priority).join(Outcome, Task.outcome_id == Outcome.id)
            .filter(Task.repo_id == repo_id, Task.status.in_(MEMBER_STATUSES)))
    members = [(t, p if p is not None else 2) for t, p in rows if _entry(t).get("entered_at") and not t.merge_sha
               and (include_github or not _entry(t).get("github"))]
    queue = [t for t, p in sorted(members, key=lambda tp: _sort_key(*tp))]
    # A holder that left the queue (e.g. for a new approval) and came back may still carry a stale turn: the most
    # recently granted turn wins, and take_turn drops the stale one when that task finds itself behind.
    holders = sorted((t for t in queue if _entry(t).get("turn")), key=lambda t: _entry(t).get("granted_at") or "",
                     reverse=True)
    return holders + [t for t in queue if not _entry(t).get("turn")]


def held_back(session) -> set[str]:
    """Queued merge-stage tasks the scheduler must not dispatch: not at the head of their queue, or backing off."""
    now = utcnow().isoformat()
    waiting = [t for t in session.query(Task).filter(Task.status == "queued", Task.stage == "merge")
               if _entry(t).get("entered_at")]
    heads: dict[str, str | None] = {}
    held: set[str] = set()
    for task in waiting:
        if task.repo_id not in heads:
            queue = ordered(session, task.repo_id)
            heads[task.repo_id] = queue[0].id if queue else None
        entry = _entry(task)
        if (entry.get("retry_at") or "") > now or (heads[task.repo_id] != task.id and not entry.get("github")):
            held.add(task.id)
    return held


def queue_view(session, repo_id: str) -> list[dict[str, Any]]:
    """The queue for display: position, task, PR and state."""
    out = []
    for position, task in enumerate(ordered(session, repo_id, include_github=True), start=1):
        entry = _entry(task)
        out.append({"position": position, "task_id": task.id, "outcome_id": task.outcome_id, "key": task.key,
                    "title": task.title, "pr_number": task.pr_number, "status": task.status, "stage": task.stage,
                    "turn": bool(entry.get("turn")), "paused": entry.get("paused") or "",
                    "github_queue": bool(entry.get("github")), "entered_at": entry.get("entered_at")})
    return out


def leave(runner) -> None:
    """Drop the task out of the queue (it is going back to its builder)."""
    if KEY in (runner.task.evidence or {}):
        with session_scope() as s:
            task = s.get(Task, runner.task_id)
            ev = dict(task.evidence or {})
            ev.pop(KEY, None)
            task.evidence = ev
            runner.task.evidence = ev


def _write(runner, entry: dict[str, Any]) -> None:
    runner.evidence(KEY, entry)


def _is_repair(runner) -> bool:
    ev = runner.task.evidence or {}
    if ev.get("revert_candidate"):
        return True
    with session_scope() as s:
        outcome = s.get(Outcome, runner.task.outcome_id)
        if outcome is None:
            return False
        if outcome.source in REPAIR_SOURCES:
            return True
        from .models import Signal

        if outcome.source == "signal":
            signal = s.query(Signal).filter(Signal.outcome_id == outcome.id).first()
            return bool(signal and signal.source in REPAIR_SOURCES)
    return False


def _park(runner, entry: dict[str, Any], *, retry_after: timedelta | None = None) -> None:
    from .pipeline.delivery import Stop

    entry = {**entry, "retry_at": (utcnow() + retry_after).isoformat() if retry_after else None}
    runner.update(status="queued", stage="merge", lease_owner="", blocked_reason="",
                  evidence={**(runner.task.evidence or {}), KEY: entry})
    raise Stop()


async def main_is_red(gh, runner) -> list[str]:
    """Names of failing checks on the default branch's head (empty when green, pending or unknown)."""
    try:
        sha = await gh.branch_sha(runner.repo.full_name, runner.repo.default_branch)
        summary = await gh.checks(runner.repo.full_name, sha)
    except Exception:
        return []  # never block merging on an observation failure
    return [c["name"] for c in summary.failing] if summary.state == "failure" else []


async def take_turn(runner, gh) -> None:
    """Return when this task may merge now; otherwise park it in the queue (raises Stop)."""
    cfg = runner.cfg.get("merge") or {}
    entry = _entry(runner.task)
    if entry.get("github"):
        return  # GitHub's merge queue orders it now
    first = not entry.get("entered_at")
    if first:
        entry["entered_at"] = utcnow().isoformat()
        _write(runner, entry)
    with session_scope() as s:
        queue = ordered(s, runner.repo.id)
    ids = [t.id for t in queue]
    position = ids.index(runner.task_id) + 1 if runner.task_id in ids else len(ids) + 1
    repair = _is_repair(runner)
    # A repair for a broken main (a revert, a CI-failure fix) never waits behind the queue: the holder rebases onto it.
    if position > 1 and not repair:
        if first or entry.get("position") != position:
            runner.event("merge_queued", f"Waiting to merge: position {position} of {len(ids)} in the "
                         f"{runner.repo.full_name} merge queue", position=position, length=len(ids))
        _park(runner, {**entry, "position": position, "paused": "", "turn": False, "granted_at": None})
    if cfg.get("hold_on_red_main", True) and not repair:
        failing = await main_is_red(gh, runner)
        runner._guard()
        if failing:
            if entry.get("paused") != "main_red":
                runner.event("merge_queue_paused", f"Merge queue paused: {runner.repo.default_branch} is failing "
                             f"({', '.join(failing[:3])}). Merging resumes when it is green.", level="warn",
                             failing=failing[:10])
            # Give up any turn, so a repair behind this task can reach the head.
            _park(runner, {**entry, "position": 1, "paused": "main_red", "turn": False, "granted_at": None},
                  retry_after=RED_MAIN_RETRY)
        with session_scope() as s:  # the queue may have changed while main was being checked
            queue = ordered(s, runner.repo.id)
        if queue and queue[0].id != runner.task_id and not entry.get("turn"):
            _park(runner, {**entry, "position": 2, "paused": ""})
    if not entry.get("turn"):
        entry.update(turn=True, granted_at=utcnow().isoformat())
        runner.event("merge_turn", f"Merge turn for PR #{runner.task.pr_number}", position=1, length=len(ids))
    entry.update(position=1, paused="", retry_at=None)
    _write(runner, entry)


# GitHub merge queue -------------------------------------------------------------------------------------------

QUEUE_QUERY = ("query($owner:String!,$name:String!,$branch:String!,$number:Int!){repository(owner:$owner,name:$name)"
               "{mergeQueue(branch:$branch){id} pullRequest(number:$number){id merged mergeQueueEntry{state position}}}}")
ENQUEUE = ("mutation($id:ID!,$head:GitObjectID!){enqueuePullRequest(input:{pullRequestId:$id,expectedHeadOid:$head})"
           "{mergeQueueEntry{state position}}}")


async def github_queue_state(gh, runner) -> dict[str, Any] | None:
    """The PR's GitHub merge queue facts, or None when the branch has no merge queue (or it cannot be read)."""
    mode = (runner.cfg.get("merge") or {}).get("queue", "auto")
    if mode == "factory" or not hasattr(gh, "graphql"):
        return None
    owner, name = runner.repo.full_name.split("/", 1)
    try:
        data = await gh.graphql(QUEUE_QUERY, {"owner": owner, "name": name, "branch": runner.repo.default_branch,
                                              "number": runner.task.pr_number})
    except Exception:
        if mode == "github" or _entry(runner.task).get("github"):
            # Explicit GitHub mode, or already handed to GitHub: never fall back to a direct merge on a lookup failure.
            _park(runner, _entry(runner.task), retry_after=GITHUB_QUEUE_RETRY)
        return None
    repo = (data.get("data") or data).get("repository") or {}
    if not repo.get("mergeQueue"):
        if mode == "github":
            runner.block("unblock", f"{runner.task.key}: no GitHub merge queue on {runner.repo.default_branch}",
                         "This repository is set to merge through GitHub's merge queue (merge.queue: github), but "
                         f"{runner.repo.default_branch} has none. Add one in branch protection, or set merge.queue "
                         "to auto, then retry.", status="needs_input")
        return None
    pr = repo.get("pullRequest") or {}
    return {"pr_id": pr.get("id"), "merged": bool(pr.get("merged")), "entry": pr.get("mergeQueueEntry")}


async def merge_via_github_queue(runner, gh, state: dict[str, Any]) -> None:
    """Add the turn holder to GitHub's merge queue, or keep waiting on it. Always parks (raises Stop)."""
    entry = _entry(runner.task)
    if state["entry"] or state["merged"]:
        # Still queued, or GitHub merged it just now: the next pass sees the merge and completes the task.
        _park(runner, {**entry, "github": True, "paused": ""}, retry_after=GITHUB_QUEUE_RETRY)
    if entry.get("github"):
        # It was in GitHub's queue and is neither queued nor merged: GitHub removed it (failed checks or a conflict).
        leave(runner)
        runner.block("unblock", f"{runner.task.key}: GitHub's merge queue removed the PR",
                     f"PR #{runner.task.pr_number} left the GitHub merge queue without merging, usually because its "
                     "merge group failed CI or conflicted. Retry to rebase and re-run the gates, or abandon.",
                     status="needs_input")
    await gh.graphql(ENQUEUE, {"id": state["pr_id"], "head": runner.task.head_sha})
    runner.event("merge_enqueued", f"Added PR #{runner.task.pr_number} to GitHub's merge queue for "
                 f"{runner.repo.default_branch}", level="success")
    # GitHub orders it from here, so the Factory turn passes to the next task.
    _park(runner, {**entry, "github": True, "turn": False, "paused": ""}, retry_after=GITHUB_QUEUE_RETRY)


def summary(session, workspace_id: str) -> dict[str, list[dict[str, Any]]]:
    """Every repository's queue in a workspace, keyed by repository full name."""
    from .models import Repository

    out: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for repo in session.query(Repository).filter(Repository.workspace_id == workspace_id):
        view = queue_view(session, repo.id)
        if view:
            out[repo.full_name] = view
    return dict(out)


# API ------------------------------------------------------------------------------------------------------------

from fastapi import APIRouter, Depends  # noqa: E402
from sqlalchemy.orm import Session  # noqa: E402

from .auth import current_user, require_member  # noqa: E402
from .db import get_db  # noqa: E402
from .models import User  # noqa: E402

router = APIRouter(prefix="/api")


@router.get("/workspaces/{workspace_id}/merge-queue")
def merge_queue(workspace_id: str, user: User = Depends(current_user), db: Session = Depends(get_db)):
    """Each repository's merge queue, head first."""
    require_member(db, user, workspace_id)
    return {"queues": summary(db, workspace_id)}
