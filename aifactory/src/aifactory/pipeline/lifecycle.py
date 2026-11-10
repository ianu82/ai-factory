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
