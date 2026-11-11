"""Coordination between outcomes: notice when two outcomes' work changes the same files, before they collide.

Each outcome has a Lead, and within an outcome the planner orders tasks that share files. Across outcomes nothing
did: two outcomes could rewrite the same module in parallel and only find out at merge time, when the second one
rebases, conflicts or breaks the first. Here the Factory compares the files each in-flight task changes (its latest
candidate diff, plus any files its plan declared) with every other outcome's in-flight work in the same repository.

A new overlap is recorded once as a ``work_overlap`` event on both outcomes. That wakes both Leads (it is one of
their milestones), and each Lead's record lists the overlapping work, so the Leads can steer their builders: build
on the other change, keep shared edits small, and let the one further along merge first. The merge manager then
merges them one at a time, and the later one rebases onto the earlier.
"""

from __future__ import annotations

import hashlib
import time
from collections import defaultdict
from typing import Any

from .db import session_scope
from .events import record
from .models import Artifact, Event, Outcome, Task

DONE = ("completed", "cancelled", "abandoned", "reverted", "handed_over", "failed")
SYNC_EVERY = 60.0  # seconds between sweeps
MAX_FILES = 20  # files listed per overlap
_last_sync = 0.0


def _files(session, tasks: list[Task]) -> dict[str, set[str]]:
    """The files each task changes: its latest candidate diff, plus files its plan declared."""
    out: dict[str, set[str]] = {t.id: set(getattr(t, "files", None) or []) for t in tasks}
    if not tasks:
        return out
    rows = (session.query(Artifact.task_id, Artifact.data).filter(Artifact.kind == "diff",
                                                                  Artifact.task_id.in_(list(out)))
            .order_by(Artifact.created_at.desc()).all())
    latest: set[str] = set()
    for task_id, data in rows:
        if task_id in latest:
            continue  # only the newest candidate counts: an earlier attempt's files may no longer be touched
        latest.add(task_id)
        out[task_id] |= {f for f in (data or {}).get("files") or [] if isinstance(f, str)}
    return out


def overlaps(session, workspace_id: str) -> list[dict[str, Any]]:
    """Pairs of in-flight tasks from different outcomes, in one repository, that change at least one same file."""
    tasks = (session.query(Task).join(Outcome, Task.outcome_id == Outcome.id)
             .filter(Task.workspace_id == workspace_id, Task.status.notin_(DONE),
                     Outcome.status.notin_(("completed", "cancelled", "partially_completed")))
             .all())
    tasks = [t for t in tasks if not t.merge_sha]
    files = _files(session, tasks)
    by_repo: dict[str, list[Task]] = defaultdict(list)
    for t in tasks:
        if files[t.id]:
            by_repo[t.repo_id].append(t)
    found = []
    for repo_tasks in by_repo.values():
        repo_tasks.sort(key=lambda t: t.id)
        for i, a in enumerate(repo_tasks):
            for b in repo_tasks[i + 1:]:
                if a.outcome_id == b.outcome_id:
                    continue  # the planner orders tasks within an outcome
                shared = sorted(files[a.id] & files[b.id])
                if shared:
                    key = hashlib.sha256("\x1f".join([a.id, b.id, *shared]).encode()).hexdigest()[:24]
                    found.append({"key": key, "repo_id": a.repo_id, "tasks": (a, b), "files": shared})
    return found


def _describe(task: Task, outcome: Outcome | None) -> dict[str, Any]:
    return {"outcome_id": task.outcome_id, "outcome_number": outcome.number if outcome else None,
            "outcome_title": outcome.title if outcome else "", "task_id": task.id, "task_key": task.key,
            "task_title": task.title, "status": task.status, "stage": task.stage, "pr_number": task.pr_number}


def for_outcome(session, outcome_id: str) -> list[dict[str, Any]]:
    """Other outcomes' in-flight work that changes the same files as this outcome's, for its Lead's record."""
    o = session.get(Outcome, outcome_id)
    if o is None:
        return []
    outcomes = {x.id: x for x in session.query(Outcome).filter(Outcome.workspace_id == o.workspace_id)}
    out = []
    for item in overlaps(session, o.workspace_id):
        a, b = item["tasks"]
        if outcome_id not in (a.outcome_id, b.outcome_id):
            continue
        mine, other = (a, b) if a.outcome_id == outcome_id else (b, a)
        out.append({"your_task": mine.key, "other": _describe(other, outcomes.get(other.outcome_id)),
                    "shared_files": item["files"][:MAX_FILES], "shared_file_count": len(item["files"])})
    return out


def summary(session, workspace_id: str) -> list[dict[str, Any]]:
    outcomes = {x.id: x for x in session.query(Outcome).filter(Outcome.workspace_id == workspace_id)}
    return [{"key": i["key"], "work": [_describe(t, outcomes.get(t.outcome_id)) for t in i["tasks"]],
             "files": i["files"][:MAX_FILES], "file_count": len(i["files"])}
            for i in overlaps(session, workspace_id)]


def sync(workspace_id: str | None = None) -> int:
    """Record each new overlap once on both outcomes (a ``work_overlap`` event). Returns how many were new."""
    new = 0
    with session_scope() as s:
        workspaces = [workspace_id] if workspace_id else [w for (w,) in s.query(Task.workspace_id).filter(
            Task.status.notin_(DONE)).distinct()]
        pending = []
        for ws in workspaces:
            found = overlaps(s, ws)
            if not found:
                continue
            # Per outcome: if recording one side fails, the next sweep still records it.
            seen = {((e.data or {}).get("key"), e.outcome_id) for e in s.query(Event).filter(
                Event.workspace_id == ws, Event.kind == "work_overlap")}
            outcomes = {x.id: x for x in s.query(Outcome).filter(Outcome.workspace_id == ws)}
            for item in found:
                a, b = item["tasks"]
                if (item["key"], a.outcome_id) in seen and (item["key"], b.outcome_id) in seen:
                    continue
                files = item["files"]
                shown = ", ".join(files[:3]) + (f" and {len(files) - 3} more" if len(files) > 3 else "")
                for mine, other in ((a, b), (b, a)):
                    if (item["key"], mine.outcome_id) in seen:
                        continue
                    o = outcomes.get(other.outcome_id)
                    pending.append((ws, mine.outcome_id, mine.id, f"{mine.key} and {other.key} of outcome #{o.number if o else '?'} "
                                    f"both change {shown}", {"key": item["key"], "files": files[:MAX_FILES],
                                                             "other": _describe(other, o)}))
                new += 1
    for ws, outcome_id, task_id, message, data in pending:
        record(ws, "work_overlap", message, outcome_id=outcome_id, task_id=task_id, level="warn", data=data)
    return new


def maybe_sync() -> None:
    """Sweep at most once a minute (the Lead's wake check calls this every few seconds)."""
    global _last_sync
    now = time.monotonic()
    if now - _last_sync < SYNC_EVERY:
        return
    _last_sync = now
    try:
        sync()
    except Exception:  # coordination must never stop the Leads
        import logging

        logging.getLogger(__name__).exception("coordination sync failed")


# API ------------------------------------------------------------------------------------------------------------

from fastapi import APIRouter, Depends  # noqa: E402
from sqlalchemy.orm import Session  # noqa: E402

from .auth import current_user, require_member  # noqa: E402
from .db import get_db  # noqa: E402
from .models import User  # noqa: E402

router = APIRouter(prefix="/api")


@router.get("/workspaces/{workspace_id}/overlaps")
def list_overlaps(workspace_id: str, user: User = Depends(current_user), db: Session = Depends(get_db)):
    """In-flight work from different outcomes that changes the same files."""
    require_member(db, user, workspace_id)
    return {"overlaps": summary(db, workspace_id)}
