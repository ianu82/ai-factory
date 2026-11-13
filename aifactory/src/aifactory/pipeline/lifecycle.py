"""Atomic lifecycle transitions shared by delivery and its scheduler.

Network responses are only observations: they cannot revive stopped work. The
predicate belongs on the UPDATE, not just on the preceding ORM read.
"""
from __future__ import annotations

from sqlalchemy import exists, update

from ..models import Outcome, Task, Workspace


STOPPED = {"cancelled", "abandoned", "handed_over", "completed", "reverted"}
# Ended without being delivered by the Factory: a task that depends on one of these never starts by itself.
ENDED = ("abandoned", "cancelled", "reverted", "handed_over")
ENDED_AS = {"abandoned": "was abandoned", "cancelled": "was cancelled", "reverted": "was merged and then reverted",
            "handed_over": "was handed over to a person"}


def stranded(tasks: list[Task], gone: set[str] | None = None) -> tuple[list[Task], list[Task]]:
    """The pending tasks that can never start, because something they depend on (directly, or through another such
    task) ended without being delivered; and the ended tasks they wait behind. A task behind one that waits on a
    person is not stranded: that person's answer moves it.

    ``gone``: the keys to count as ended instead of the tasks that have ended, to ask what ending them would strand."""
    by_key = {t.key: t for t in tasks}

    def over(t: Task) -> bool:
        return t.key in gone if gone is not None else t.status in ENDED

    waiting: dict[str, Task] = {}
    changed = True
    while changed:
        changed = False
        for t in tasks:
            if t.status != "pending" or t.key in waiting:
                continue
            behind = [by_key[k] for k in t.depends_on or [] if k in by_key]
            if any(over(b) or b.key in waiting for b in behind):
                waiting[t.key] = t
                changed = True
    ended = {k: by_key[k] for t in waiting.values() for k in t.depends_on or []
             if k in by_key and over(by_key[k])}

    def order(t: Task) -> tuple[int, str]:
        return len(t.key), t.key  # T2 before T10

    return sorted(waiting.values(), key=order), sorted(ended.values(), key=order)


def dependents(tasks: list[Task], key: str) -> list[Task]:
    """The unfinished tasks that wait for ``key``, directly or through other tasks: what ending it would strand."""
    if not any(t.key == key and t.status not in STOPPED for t in tasks):
        return []
    return stranded(tasks, {key})[0]


def transition(session, task: Task, *, owner: str | None = None, exclusive: bool = False,
               expected: dict | None = None, guards: tuple = (), **fields) -> bool:
    """`owner` is the instance making the change: the row must still be leased to it, or to nobody (the scheduler
    tidies up after a shutdown released its leases). A runner whose task another instance took changes nothing.

    `exclusive`: the row must be leased to `owner` itself. For what cannot be taken back (a push, a comment, a merge):
    a lease that is nobody's may have been another instance's a moment ago."""
    if fields.get("lease_owner") == "":
        fields["lease_expires_at"] = None  # an expiry never outlives its owner
    held = () if owner is None else (Task.lease_owner.in_((owner,) if exclusive else ("", owner)),)
    result = session.execute(
        update(Task).where(
            Task.id == task.id, Task.status == task.status, Task.stage == task.stage,
            Task.status.notin_(STOPPED),
            exists().where(Workspace.id == Task.workspace_id, Workspace.paused.is_(False)),
            exists().where(Outcome.id == Task.outcome_id, Outcome.status != "cancelled"),
            *held, *guards,
            *(getattr(Task, key) == value for key, value in (expected or {}).items()),
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
