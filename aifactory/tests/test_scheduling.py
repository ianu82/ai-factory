"""Scheduler ticks with real dispatch bookkeeping and explicitly released fake jobs."""

from __future__ import annotations

import asyncio
from collections import Counter
from datetime import datetime, timedelta

import pytest

from aifactory.config import get_settings, reset_settings_cache
from aifactory.db import session_scope
from aifactory.models import Decision, Organization, Outcome, Plan, Repository, Task, User, Workspace
from aifactory.pipeline import orchestrator as module
from aifactory.pipeline.orchestrator import Orchestrator
from aifactory.pipeline.runtime import merged_settings


@pytest.fixture
def queue(monkeypatch):
    with session_scope() as s:
        org = Organization(name="Test")
        s.add(org)
        s.flush()
        s.add_all([User(id="a", email="a@test"), User(id="b", email="b@test"), User(id="c", email="c@test")])
        for wid in ("w1", "w2", "w3", "paused"):
            s.add(Workspace(id=wid, org_id=org.id, name=wid, slug=wid, paused=wid == "paused"))
            s.flush()
            s.add(Repository(id=f"repo-{wid}", workspace_id=wid, full_name=f"test/{wid}"))
    sequence = 0

    def add(phase="planning", *, ws="w1", requester="a", priority=2, age=None, stage="", started=False):
        nonlocal sequence
        sequence += 1
        oid, tid = f"o{sequence:03}", f"t{sequence:03}"
        created = datetime(2025, 1, 1) + timedelta(seconds=sequence if age is None else age)
        with session_scope() as s:
            s.add(Outcome(id=oid, workspace_id=ws, number=sequence, title=oid, priority=priority,
                          created_by=requester, created_at=created,
                          status="planning" if phase == "planning" else "running"))
            if phase == "delivery":
                s.add(Plan(id=oid, outcome_id=oid))
                s.flush()
                s.add(Task(id=tid, workspace_id=ws, outcome_id=oid, plan_id=oid, repo_id=f"repo-{ws}",
                           key="T1", title=tid, status="queued", created_at=created, stage=stage,
                           started_at=created if started else None))
        return oid if phase == "planning" else tid

    return add


class Jobs:
    def __init__(self, monkeypatch):
        self.orch = Orchestrator()
        self.started = []
        self.release = {}
        owner = self

        async def run(item_id, phase):
            owner.started.append(item_id)
            owner.release[item_id] = asyncio.Event()
            await owner.release[item_id].wait()
            with session_scope() as s:
                item = s.get(Outcome if phase == "planning" else Task, item_id)
                item.status = "running" if phase == "planning" else "completed"

        async def planner(item_id, cancel):
            await run(item_id, "planning")

        class Runner:
            def __init__(self, item_id, cancel, repo_lock, owner=None):
                self.id = item_id

            async def run(self):
                await run(self.id, "delivery")

        monkeypatch.setattr(module, "plan_outcome", planner)
        monkeypatch.setattr(module, "TaskRunner", Runner)

    async def tick(self):
        self.orch._schedule()
        await asyncio.sleep(0)

    async def complete(self, item_id):
        self.release[item_id].set()
        await asyncio.sleep(0)

    async def stop(self):
        jobs = [job for job, _ in [*self.orch._planning.values(), *self.orch._tasks.values()]]
        await self.orch.stop()
        await asyncio.gather(*jobs)


def set_limit(monkeypatch, phase, value):
    key = "AIFACTORY_GLOBAL_PLANNING_CONCURRENCY" if phase == "planning" else "AIFACTORY_GLOBAL_CONCURRENCY"
    monkeypatch.setenv(key, str(value))
    reset_settings_cache()


@pytest.mark.parametrize("phase", ["planning", "delivery"])
def test_priority_and_requester_turns_persist_one_slot_per_tick(queue, monkeypatch, phase):
    set_limit(monkeypatch, phase, 1)
    low = queue(phase, priority=3, age=-100)
    a = [queue(phase, requester="a", priority=0) for _ in range(3)]
    b = [queue(phase, requester="b", priority=0) for _ in range(2)]
    # Null-source work shares a bucket, not a turn per outcome.
    anonymous = [queue(phase, requester=None, priority=0) for _ in range(2)]
    expected = [anonymous[0], a[0], b[0], anonymous[1], a[1], b[1], a[2], low]

    async def go():
        jobs = Jobs(monkeypatch)
        try:
            for item_id in expected:
                await jobs.tick()
                assert jobs.started[-1] == item_id
                assert len(jobs.orch._planning if phase == "planning" else jobs.orch._tasks) == 1
                await jobs.complete(item_id)
            assert jobs.started == expected
        finally:
            await jobs.stop()

    asyncio.run(go())


@pytest.mark.parametrize("phase", ["planning", "delivery"])
def test_committed_priority_promotion_dispatches_next_without_preempting(queue, monkeypatch, phase):
    set_limit(monkeypatch, phase, 1)
    active = queue(phase, priority=2)

    async def go():
        jobs = Jobs(monkeypatch)
        try:
            await jobs.tick()
            assert jobs.started == [active]
            running = jobs.orch._planning if phase == "planning" else jobs.orch._tasks
            job, cancel = running[active]
            older = queue(phase, priority=2, age=-100)
            promoted = queue(phase, priority=3)
            with session_scope() as s:
                oid = promoted if phase == "planning" else s.get(Task, promoted).outcome_id
                s.get(Outcome, oid).priority = 0
            # A committed edit is visible next tick, but the occupied slot is not reclaimed.
            await jobs.tick()
            assert jobs.started == [active]
            assert running == {active: (job, cancel)}
            assert not job.done() and not job.cancelled() and not job.cancelling()
            assert not cancel.is_set() and not jobs.release[active].is_set()
            with session_scope() as s:
                if phase == "delivery":
                    assert s.get(Task, active).status == "running"
                    assert s.get(Task, promoted).status == s.get(Task, older).status == "queued"
                else:
                    assert s.get(Outcome, active).status == "planning"
                assert s.get(Outcome, oid).priority == 0
            await jobs.complete(active)
            await jobs.tick()
            assert jobs.started == [active, promoted]
            assert set(running) == {promoted}
            assert not job.cancelled() and not cancel.is_set()
            await jobs.complete(promoted)
            await jobs.tick()
            assert jobs.started == [active, promoted, older]
        finally:
            await jobs.stop()

    asyncio.run(go())


def test_planners_default_workspace_and_global_caps_and_waiting_state(queue, monkeypatch):
    monkeypatch.delenv("AIFACTORY_GLOBAL_PLANNING_CONCURRENCY", raising=False)
    reset_settings_cache()
    for ws in ("w1", "w2", "w3", "paused"):
        for _ in range(4):
            queue(ws=ws)

    async def go():
        jobs = Jobs(monkeypatch)
        try:
            await jobs.tick()
            assert len(jobs.orch._planning) == 4
            with session_scope() as s:
                counts = Counter(s.get(Outcome, oid).workspace_id for oid in jobs.orch._planning)
                assert counts == {"w1": 2, "w2": 2}
                assert all(o.status == "planning" for o in s.query(Outcome))
            await jobs.tick()
            assert len(jobs.started) == 4
            await jobs.complete(jobs.started[0])
            await jobs.tick()
            assert len(jobs.started) == 5
            assert len(jobs.orch._planning) == 4
        finally:
            await jobs.stop()

    asyncio.run(go())


@pytest.mark.parametrize("phase", ["planning", "delivery"])
def test_blocked_and_paused_candidates_skip_without_consuming_turns(queue, monkeypatch, phase):
    set_limit(monkeypatch, phase, 2)
    key = "planning_concurrency" if phase == "planning" else "concurrency"
    with session_scope() as s:
        s.get(Workspace, "w1").settings = {key: 1}
    a1 = queue(phase, requester="a")
    a2 = queue(phase, requester="a", ws="w2")
    b1 = queue(phase, requester="b")  # blocked by a1's workspace, must not consume b's turn
    c1 = queue(phase, requester="c", ws="w2")
    paused = queue(phase, requester=None, ws="paused", priority=0)

    async def go():
        jobs = Jobs(monkeypatch)
        try:
            await jobs.tick()
            assert jobs.started == [a1, c1]
            # c was actually dispatched, so the next turn wraps to a, then b.
            await jobs.complete(a1)
            await jobs.tick()
            assert jobs.started == [a1, c1, a2]
            await jobs.complete(c1)
            await jobs.tick()
            assert jobs.started == [a1, c1, a2, b1]
            assert paused not in jobs.started
        finally:
            await jobs.stop()

    asyncio.run(go())


@pytest.mark.parametrize("phase", ["planning", "delivery"])
def test_a_workspace_on_provider_hold_gets_nothing_started_until_it_is_resumed(queue, monkeypatch, phase):
    set_limit(monkeypatch, phase, 4)
    started = queue(phase)

    async def go():
        jobs = Jobs(monkeypatch)
        try:
            await jobs.tick()
            assert jobs.started == [started]
            with session_scope() as s:
                s.add(Decision(id="hold", workspace_id="w1", kind="provider_hold",
                               title="Model provider refused: work is on hold",
                               options=[{"value": "retry", "label": "Resume work"}]))
            held = queue(phase, priority=0)
            other = queue(phase, ws="w2")
            await jobs.tick()
            # What was already running is left alone; another workspace's work still starts.
            assert jobs.started == [started, other]
            assert set(jobs.orch._planning if phase == "planning" else jobs.orch._tasks) == {started, other}
            with session_scope() as s:
                s.get(Decision, "hold").status = "resolved"
            await jobs.tick()
            assert jobs.started == [started, other, held]
        finally:
            await jobs.stop()

    asyncio.run(go())


@pytest.mark.parametrize("phase", ["planning", "delivery"])
def test_configured_workspace_and_global_overrides(queue, monkeypatch, phase):
    set_limit(monkeypatch, phase, 6)
    key = "planning_concurrency" if phase == "planning" else "concurrency"
    with session_scope() as s:
        s.get(Workspace, "w1").settings = {key: 4}
        s.get(Workspace, "w2").settings = {key: 2}
    for ws in ("w1", "w2"):
        for _ in range(6):
            queue(phase, ws=ws)

    async def go():
        jobs = Jobs(monkeypatch)
        try:
            await jobs.tick()
            assert len(jobs.started) == 6
            with session_scope() as s:
                model = Outcome if phase == "planning" else Task
                assert Counter(s.get(model, item_id).workspace_id for item_id in jobs.started) == {"w1": 4, "w2": 2}
        finally:
            await jobs.stop()

    asyncio.run(go())


def test_priority_first_then_resumptions_without_exceeding_caps(queue, monkeypatch):
    """Work starts by priority; within one priority, resumed work precedes new work."""
    set_limit(monkeypatch, "delivery", 1)
    fresh = queue("delivery", priority=0, age=-100)
    repair = queue("delivery", requester="b", priority=2, stage="ci_repair")
    merge = queue("delivery", requester="a", priority=0, stage="merge")
    restarted = queue("delivery", requester="a", priority=3, started=True)
    fresh_normal = queue("delivery", requester="c", priority=2, age=-200)

    async def go():
        jobs = Jobs(monkeypatch)
        try:
            for item_id in [merge, fresh, repair, fresh_normal, restarted]:
                await jobs.tick()
                assert jobs.started[-1] == item_id
                await jobs.tick()
                assert len(jobs.orch._tasks) == 1  # no preemption on another tick
                await jobs.complete(item_id)
        finally:
            await jobs.stop()

    asyncio.run(go())


@pytest.mark.parametrize("stage", ["merge", "ci_repair", "ci_refresh", "revert_merge"])
def test_ready_ci_continuation_precedes_urgent_fresh_work(queue, monkeypatch, stage):
    set_limit(monkeypatch, "delivery", 1)
    fresh = queue("delivery", priority=0, age=-100)
    continuation = queue("delivery", priority=3, stage=stage)
    with session_scope() as s:
        s.get(Task, continuation).evidence = {"ci_wait": {"continuation": "delivery"}}

    async def go():
        jobs = Jobs(monkeypatch)
        try:
            await jobs.tick()
            assert jobs.started == [continuation]
            await jobs.tick()
            assert len(jobs.orch._tasks) == 1
            await jobs.complete(continuation)
            await jobs.tick()
            assert jobs.started == [continuation, fresh]
        finally:
            await jobs.stop()

    asyncio.run(go())


def test_oldest_and_id_tie_breakers_for_both_queues(queue, monkeypatch):
    async def go(phase):
        set_limit(monkeypatch, phase, 1)
        newer = queue(phase, age=100)
        older = queue(phase, age=0)
        tied = queue(phase, age=0)
        jobs = Jobs(monkeypatch)
        try:
            for item_id in [older, tied, newer]:
                await jobs.tick()
                assert jobs.started[-1] == item_id
                await jobs.complete(item_id)
        finally:
            await jobs.stop()

    asyncio.run(go("planning"))
    asyncio.run(go("delivery"))


@pytest.mark.parametrize("value,expected", [(None, 4), ("7", 7), ("0", 1), ("-2", 1), ("500", 50), ("bad", 4)])
def test_global_planning_limit_normalization(monkeypatch, value, expected):
    if value is None:
        monkeypatch.delenv("AIFACTORY_GLOBAL_PLANNING_CONCURRENCY", raising=False)
    else:
        monkeypatch.setenv("AIFACTORY_GLOBAL_PLANNING_CONCURRENCY", value)
    reset_settings_cache()
    assert get_settings().global_planning_concurrency == expected


@pytest.mark.parametrize("value,expected", [(None, 2), ("7", 7), (0, 1), (-2, 1), (500, 50), ("bad", 2), ([], 2)])
def test_legacy_workspace_planning_limit_normalization(value, expected):
    assert merged_settings({"planning_concurrency": value})["planning_concurrency"] == expected


def test_planning_and_delivery_keep_independent_per_priority_cursors(queue, monkeypatch):
    set_limit(monkeypatch, "planning", 1)
    set_limit(monkeypatch, "delivery", 1)
    plan_a = queue(priority=0)
    plan_b = queue(priority=0, requester="b")
    build_a = queue("delivery", priority=2)
    build_b = queue("delivery", priority=2, requester="b")

    async def go():
        jobs = Jobs(monkeypatch)
        try:
            await jobs.tick()
            assert jobs.started == [plan_a, build_a]
            await jobs.complete(plan_a)
            await jobs.complete(build_a)
            # Changing priority must not inherit the last requester at another priority.
            plan_low_a = queue(priority=1)
            plan_low_b = queue(priority=1, requester="b")
            build_urgent_a = queue("delivery", priority=0)
            build_urgent_b = queue("delivery", priority=0, requester="b")
            await jobs.tick()
            assert jobs.started[-2:] == [plan_b, build_urgent_a]
            await jobs.complete(plan_b)
            await jobs.complete(build_urgent_a)
            await jobs.tick()
            assert jobs.started[-2:] == [plan_low_a, build_urgent_b]
            await jobs.complete(plan_low_a)
            await jobs.complete(build_urgent_b)
            await jobs.tick()
            assert jobs.started[-2:] == [plan_low_b, build_b]
        finally:
            await jobs.stop()

    asyncio.run(go())


def lease(task_id):
    with session_scope() as s:
        task = s.get(Task, task_id)
        return task.status, task.lease_owner, task.lease_expires_at


def test_a_dispatched_task_is_leased_to_this_instance_until_its_job_ends(queue, monkeypatch):
    from aifactory.db import utcnow

    set_limit(monkeypatch, "delivery", 1)
    task_id = queue("delivery")
    term = timedelta(seconds=module.LEASE_SECONDS)

    async def go():
        jobs = Jobs(monkeypatch)
        try:
            before = utcnow()
            await jobs.tick()
            status, owner, expires = lease(task_id)
            assert (status, owner) == ("running", jobs.orch.instance)
            assert before + term <= expires <= utcnow() + term
            await jobs.complete(task_id)
            assert lease(task_id) == ("completed", "", None)  # the expiry goes with the owner
        finally:
            await jobs.stop()

    asyncio.run(go())


@pytest.mark.parametrize("ending", ["shutdown", "returns", "crashes"])
def test_a_job_whose_task_another_instance_took_leaves_it_alone(queue, monkeypatch, ending):
    """Its lease ran out while this instance was stalled, and another one started the task: same status, another
    owner. However this instance's job ends, the task is no longer its to requeue or block."""
    set_limit(monkeypatch, "delivery", 1)
    task_id = queue("delivery")

    async def go():
        jobs = Jobs(monkeypatch)
        release = asyncio.Event()

        class Runner:
            def __init__(self, item_id, cancel, repo_lock, owner=None):
                pass

            async def run(self):
                await release.wait()
                if ending == "crashes":
                    raise RuntimeError("the sandbox went away")

        monkeypatch.setattr(module, "TaskRunner", Runner)
        try:
            await jobs.tick()
            expires = lease(task_id)[2]
            with session_scope() as s:
                s.get(Task, task_id).lease_owner = "other"
            if ending != "shutdown":
                release.set()
                await asyncio.sleep(0)
                assert not jobs.orch._tasks  # the job has ended
        finally:
            await jobs.stop()
        assert lease(task_id) == ("running", "other", expires)

    asyncio.run(go())
