"""Atomic lifecycle transitions shared by delivery and its scheduler.

Network responses are only observations: they cannot revive stopped work. The
predicate belongs on the UPDATE, not just on the preceding ORM read.
"""
from __future__ import annotations

from sqlalchemy import exists, update

from ..models import Outcome, Task, Workspace


STOPPED = {"cancelled", "abandoned", "handed_over", "completed", "reverted"}


def transition(session, task: Task, **fields) -> bool:
    result = session.execute(
        update(Task).where(
            Task.id == task.id, Task.status == task.status, Task.stage == task.stage,
            Task.status.notin_(STOPPED),
            exists().where(Workspace.id == Task.workspace_id, Workspace.paused.is_(False)),
            exists().where(Outcome.id == Task.outcome_id, Outcome.status != "cancelled"),
        ).values(**fields).execution_options(synchronize_session=False)
    )
    return result.rowcount == 1


# A person acknowledging a failed post-merge check, or opening a fix outcome for it, accepts the merged change.
ACCEPTING = ("acknowledge", "fix")


def accepted_after_merge(task: Task) -> bool:
    """Whether accepting the open failure completes this task: it is merged and held only by its post-merge smoke or
    release check, with no revert under way. A failed revert stays with a person."""
    return (task.status == "smoke_failed" and bool(task.merge_sha) and task.stage in ("smoke", "release")
            and not (task.evidence or {}).get("revert_candidate"))


def complete_accepted(session, task: Task, by: str) -> dict:
    """Complete a merged task whose failed post-merge check a person accepted, so the tasks depending on it start.
    Returns the unpublished event; the caller publishes it after committing."""
    from ..db import utcnow
    from ..events import record

    check = "release check" if task.stage == "release" else "smoke checks"
    summary = f"Merged; {by} accepted the failed post-merge {check}"
    task.status, task.stage, task.blocked_reason, task.lease_owner = "completed", "done", "", ""
    task.finished_at = utcnow()
    task.summary = summary
    return record(task.workspace_id, "task_completed", f"{task.key} complete: {summary}", outcome_id=task.outcome_id,
                  task_id=task.id, level="success", session=session, publish=False)
