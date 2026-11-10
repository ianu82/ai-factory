"""Accepting a failed post-merge check completes the merged task, so the tasks that depend on it start."""
from __future__ import annotations

import asyncio

import pytest

from aifactory.db import session_scope, utcnow
from aifactory.decisions import resolve
from aifactory.models import Decision, Event, Outcome, Plan, Task, User
from aifactory.pipeline.delivery import TaskRunner
from aifactory.pipeline.lifecycle import SETTLE, complete_accepted
from aifactory.pipeline.orchestrator import Orchestrator, orchestrator

from .test_pipeline import factory, new_outcome  # noqa: F401 - workspace, repository and user fixture

OPTIONS = [{"value": "revert", "label": "Revert the merge"}, {"value": "fix", "label": "Create a fix outcome"},
           {"value": "acknowledge", "label": "Acknowledge"}]


def merged_with_dependent(ids, *, stage="smoke", evidence=None, decision_status="open", resolution="",
                          resolved_by=None):
    """T1 merged and held by a failed post-merge check, T2 waiting on it, and the open (or resolved) decision."""
    oid = new_outcome(ids)
    with session_scope() as s:
        s.get(Outcome, oid).status = "attention"
        plan = Plan(outcome_id=oid)
        s.add(plan)
        s.flush()
        first = Task(workspace_id=ids["ws"], outcome_id=oid, plan_id=plan.id, repo_id=ids["repo"], key="T1",
                     title="Dock", status="smoke_failed", stage=stage, pr_number=7, merge_sha="abc123",
                     blocked_reason="Smoke checks failed", evidence=evidence or {"smoke": {"state": "failed"}})
        second = Task(workspace_id=ids["ws"], outcome_id=oid, plan_id=plan.id, repo_id=ids["repo"], key="T2",
                      title="Rollout", status="pending", depends_on=["T1"])
        s.add_all([first, second])
        s.flush()
        decision = Decision(workspace_id=ids["ws"], outcome_id=oid, task_id=first.id, kind="unblock",
                            title="Smoke checks failed after merging T1", options=OPTIONS, status=decision_status,
                            resolution=resolution, resolved_by=resolved_by,
                            resolved_at=utcnow() if decision_status == "resolved" else None)
        s.add(decision)
        s.flush()
        return oid, first.id, second.id, decision.id


def state(tid):
    with session_scope() as s:
        t = s.get(Task, tid)
        return t.status, t.stage


def completions(tid):
    with session_scope() as s:
        return s.query(Event).filter(Event.task_id == tid, Event.kind == "task_completed").count()


@pytest.mark.parametrize("resolution", ["acknowledge", "fix"])
def test_accepting_a_smoke_failure_completes_the_task_and_starts_its_dependents(factory, resolution):
    oid, first, second, did = merged_with_dependent(factory)
    with session_scope() as s:
        resolve(s, s.get(Decision, did), s.get(User, factory["user"]), resolution)
    assert state(first) == ("completed", "done")
    with session_scope() as s:
        task = s.get(Task, first)
        assert task.finished_at is not None and task.blocked_reason == ""
        assert "Dev accepted the failed post-merge smoke checks" in task.summary
        assert task.evidence["smoke"]["state"] == "failed"  # the failure stays on record
    assert completions(first) == 1
    Orchestrator()._promote()
    assert state(second)[0] == "queued"
    with session_scope() as s:
        assert s.get(Outcome, oid).status == "running"


def test_accepting_a_release_check_failure_completes_the_task(factory):
    _, first, second, did = merged_with_dependent(factory, stage="release",
                                                  evidence={"release": {"state": "unhealthy"}})
    with session_scope() as s:
        resolve(s, s.get(Decision, did), s.get(User, factory["user"]), "acknowledge")
    assert state(first) == ("completed", "done")
    with session_scope() as s:
        assert "accepted the failed post-merge release check" in s.get(Task, first).summary
    Orchestrator()._promote()
    assert state(second)[0] == "queued"


def test_acknowledging_a_failed_revert_leaves_the_task_with_a_person(factory):
    # The person asked for a revert and it could not finish: the change they rejected must not unblock anything.
    _, first, second, did = merged_with_dependent(factory, stage="revert",
                                                  evidence={"revert_candidate": {"pr": 8, "head": "def"}})
    with session_scope() as s:
        resolve(s, s.get(Decision, did), s.get(User, factory["user"]), "acknowledge")
    assert state(first) == ("smoke_failed", "revert")
    Orchestrator()._promote()
    assert state(second)[0] == "pending"


def test_startup_completes_tasks_accepted_before_this_fix(factory):
    _, first, second, _ = merged_with_dependent(factory, decision_status="resolved", resolution="acknowledge",
                                                resolved_by=factory["user"])
    _, waiting, waiting_dependent, _ = merged_with_dependent(factory)  # its decision is still open
    orch = Orchestrator()
    orch._recover()
    orch._recover()  # a second restart changes nothing
    assert state(first) == ("completed", "done")
    assert orch._settle == {first}  # its source issues are settled on the first tick
    assert completions(first) == 1
    with session_scope() as s:
        assert "Dev accepted" in s.get(Task, first).summary
    assert state(waiting) == ("smoke_failed", "smoke")
    orch._promote()
    assert state(second)[0] == "queued"
    assert state(waiting_dependent)[0] == "pending"


def test_startup_repair_skips_a_later_open_failure(factory):
    # An earlier failure was acknowledged, but the newest decision on the task is still open.
    oid, first, _, _ = merged_with_dependent(factory, decision_status="resolved", resolution="acknowledge",
                                             resolved_by=factory["user"])
    with session_scope() as s:
        s.add(Decision(workspace_id=factory["ws"], outcome_id=oid, task_id=first, kind="unblock",
                       title="Release check failed after T1", options=OPTIONS[2:]))
    Orchestrator()._recover()
    assert state(first) == ("smoke_failed", "smoke")


def test_completion_leaves_a_cancelled_outcome_alone(factory):
    oid, first, _, _ = merged_with_dependent(factory)
    with session_scope() as s:
        s.get(Outcome, oid).status = "cancelled"
    with session_scope() as s:
        assert complete_accepted(s, s.get(Task, first), "Dev") is None
    assert state(first) == ("smoke_failed", "smoke")
    assert completions(first) == 0


def test_accepted_completion_settles_source_issues(factory, monkeypatch):
    settled = []

    async def settle(self):
        settled.append(self.task_id)

    monkeypatch.setattr(TaskRunner, "_settle_issues", settle)
    _, first, _, did = merged_with_dependent(factory)
    orchestrator._settle.clear()
    with session_scope() as s:
        resolve(s, s.get(Decision, did), s.get(User, factory["user"]), "acknowledge")
    assert orchestrator._settle == {first}
    with session_scope() as s:
        assert s.get(Task, first).evidence[SETTLE]  # stored with the completion
    asyncio.run(orchestrator._settle_accepted())
    assert settled == [first]
    assert not orchestrator._settle
    with session_scope() as s:
        assert SETTLE not in s.get(Task, first).evidence
        assert s.get(Task, first).evidence["smoke"]["state"] == "failed"


def test_a_restart_before_settlement_still_settles(factory, monkeypatch):
    _, first, _, did = merged_with_dependent(factory)
    with session_scope() as s:
        resolve(s, s.get(Decision, did), s.get(User, factory["user"]), "acknowledge")
    orchestrator._settle.clear()  # the process stopped before its next tick
    restarted = Orchestrator()
    restarted._recover()
    assert restarted._settle == {first}

    async def settle(self):
        pass

    monkeypatch.setattr(TaskRunner, "_settle_issues", settle)
    asyncio.run(restarted._settle_accepted())
    again = Orchestrator()
    again._recover()
    assert not again._settle  # settled once, never again


def test_a_failed_settlement_is_retried_at_the_next_startup(factory, monkeypatch):
    _, first, _, did = merged_with_dependent(factory)

    async def fail(self):
        raise RuntimeError("GitHub unreachable")

    monkeypatch.setattr(TaskRunner, "_settle_issues", fail)
    orchestrator._settle.clear()
    with session_scope() as s:
        resolve(s, s.get(Decision, did), s.get(User, factory["user"]), "acknowledge")
    asyncio.run(orchestrator._settle_accepted())
    assert not orchestrator._settle  # no retry loop within the process
    restarted = Orchestrator()
    restarted._recover()
    assert restarted._settle == {first}


def test_outcome_summary_says_a_failed_check_was_accepted(factory):
    oid, first, second, did = merged_with_dependent(factory)
    with session_scope() as s:
        task = s.get(Task, second)
        task.status, task.stage, task.evidence = "completed", "done", {"smoke": {"state": "passed"}}
    with session_scope() as s:
        resolve(s, s.get(Decision, did), s.get(User, factory["user"]), "acknowledge")
    orchestrator._settle.clear()
    Orchestrator()._promote()
    with session_scope() as s:
        outcome = s.get(Outcome, oid)
        assert outcome.status == "completed"
        assert outcome.summary == "All 2 task(s) merged; 1 with a failed post-merge check that a person accepted."
