"""Shared row lock and JSON merge contract for delivery and background observers.

Never hold this lock across network I/O. Review observers patch only their own
keys; delivery patches evidence keys rather than replacing a detached snapshot.
"""
from __future__ import annotations

from copy import deepcopy

from sqlalchemy import text

from .models import Task


def locked_task(session, task_id: str) -> Task | None:
    # SQLite has no SELECT FOR UPDATE. Acquire its write reservation before the
    # read, not after constructing a JSON value from potentially stale evidence.
    if session.get_bind().dialect.name == "sqlite" and not session.in_transaction():
        session.execute(text("BEGIN IMMEDIATE"))
    return session.query(Task).filter(Task.id == task_id).with_for_update().one_or_none()


def merge_evidence(task: Task, values: dict) -> dict:
    evidence = deepcopy(task.evidence or {})
    values = deepcopy(values)
    if "review" in values:
        values["review"] = {**evidence.get("review", {}), **values["review"]}
    if "failure_facts" in values:
        values["failure_facts"] = [*evidence.get("failure_facts", []), *values["failure_facts"]]
    evidence.update(values)
    task.evidence = evidence
    return evidence
