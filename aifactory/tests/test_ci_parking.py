"""Durable CI observations and adversarial lifecycle races (no external services)."""
from __future__ import annotations

import asyncio
from datetime import timedelta

import pytest

from aifactory.db import session_scope, utcnow
from aifactory.github import GitHubError
from aifactory.models import Decision, Outcome, Plan, Repository, Task, User, Workspace
from aifactory.pipeline import delivery
from aifactory.pipeline.delivery import Stop, TaskRunner
from aifactory.pipeline.orchestrator import Orchestrator

from .fakes import FakeGitHub, ScriptedModel, git
from .test_pipeline import factory, new_outcome  # noqa: F401 - local remote/model fixture


def candidate(ids, *, stage="ci"):
    oid = new_outcome(ids)
    head = git("rev-parse", "main", cwd=ids["remotes"] / "acme/demo.git")
    with session_scope() as s:
        s.get(Outcome, oid).status = "running"
        plan = Plan(outcome_id=oid)
        s.add(plan)
        s.flush()
        task = Task(workspace_id=ids["ws"], outcome_id=oid, plan_id=plan.id, repo_id=ids["repo"],
                    key="T1", title="Candidate", status="running", stage=stage, head_sha=head, base_sha=head,
                    pr_number=1, lease_owner="old")
        s.add(task)
        s.flush()
        return task.id


def runner(tid, orch=None):
    return TaskRunner(tid, asyncio.Event(), (orch or Orchestrator()).repo_lock)


def snapshot(tid):
    with session_scope() as s:
        t = s.get(Task, tid)
        return t.status, t.stage, t.evidence, t.lease_owner


async def park(tid):
    with pytest.raises(Stop):
        await runner(tid).ci_then_merge()


def age(tid, seconds=200, *, expired=False):
    with session_scope() as s:
        t = s.get(Task, tid)
        wait = {**t.evidence["ci_wait"], "first_wait_at": (utcnow() - timedelta(seconds=seconds)).isoformat(),
                "next_poll_at": utcnow().isoformat()}
        if expired:
            wait["deadline"] = (utcnow() - timedelta(seconds=1)).isoformat()
        t.evidence = {**t.evidence, "ci_wait": wait}


def test_pending_restart_success_preserves_candidate_deadline_and_no_agents(factory):
    tid = candidate(factory)
    FakeGitHub.check_state = "pending"
    asyncio.run(park(tid))
    status, stage, ev, lease = snapshot(tid)
    assert (status, stage, lease) == ("waiting_ci", "ci", "")
    orch = Orchestrator()
    orch._recover()
    assert orch.status()["waiting_ci"] == [tid]
    assert orch.status()["running"] == []
    FakeGitHub.check_state = "success"
    asyncio.run(runner(tid, orch).poll_ci())
    status, stage, after, _ = snapshot(tid)
    assert (status, stage) == ("queued", "merge")
    assert after["ci_wait"]["deadline"] == ev["ci_wait"]["deadline"]
    assert after["ci_wait"]["head"] == ev["ci_wait"]["head"]
    assert ScriptedModel.calls == []


def test_health_reports_parked_tasks_separately(factory, client):
    tid = candidate(factory)
    asyncio.run(park(tid))

    response = client.get("/api/health")

    assert response.status_code == 200
    state = response.json()["orchestrator"]
    assert tid in state["waiting_ci"]
    assert tid not in state["running"]


@pytest.mark.parametrize("state,required,expected", [("none", False, "merge"), ("none", True, "blocked"),
                                                    ("failure", False, "ci_repair")])
def test_grace_required_and_failure(factory, state, required, expected):
    tid = candidate(factory)
    FakeGitHub.check_state = "none"
    asyncio.run(park(tid))
    assert snapshot(tid)[0] == "waiting_ci"
    asyncio.run(runner(tid).poll_ci())
    age(tid)
    with session_scope() as s:
        r = s.get(Repository, factory["repo"])
        r.config = {**r.config, "require_ci": required}
    FakeGitHub.check_state = state
    async def go():
        try:
            await runner(tid).poll_ci()
        except Stop:
            pass
    asyncio.run(go())
    status, stage, ev, _ = snapshot(tid)
    assert (status if expected == "blocked" else stage) == expected
    assert ev["ci_wait"]["head"] == ev["ci"]["head"]


def test_failure_reconstructs_logs(factory, monkeypatch):
    tid = candidate(factory)
    FakeGitHub.check_state = "failure"
    asyncio.run(park(tid))
    asyncio.run(runner(tid).poll_ci())
    seen = []
    async def deliver(self, continue_from_branch=False):
        seen.extend(self.feedback)
        assert continue_from_branch
    monkeypatch.setattr(TaskRunner, "deliver", deliver)
    asyncio.run(runner(tid).run())
    assert any("Error: test failed in CI" in text for text in seen)


def test_timeout_keep_waiting_is_only_explicit_reset(factory):
    from aifactory.decisions import resolve

    tid = candidate(factory)
    FakeGitHub.check_state = "pending"
    asyncio.run(park(tid))
    age(tid, expired=True)
    deadline = snapshot(tid)[2]["ci_wait"]["deadline"]
    async def go():
        with pytest.raises(Stop):
            await runner(tid).poll_ci()
    asyncio.run(go())
    assert snapshot(tid)[0] == "blocked"
    assert snapshot(tid)[2]["ci_wait"]["deadline"] == deadline
    with session_scope() as s:
        decision = s.query(Decision).filter(Decision.task_id == tid).one()
        resolve(s, decision, s.get(User, factory["user"]), "retry")
    assert snapshot(tid)[2]["ci_wait"]["deadline"] > deadline
    new_deadline = snapshot(tid)[2]["ci_wait"]["deadline"]
    asyncio.run(park(tid))
    assert snapshot(tid)[2]["ci_wait"]["deadline"] == new_deadline


@pytest.mark.parametrize("initial", [True, False])
@pytest.mark.parametrize("stop", ["cancelled", "handed_over", "abandoned", "pause"])
@pytest.mark.parametrize("error", [True, False])
def test_stop_during_observation_never_resurrects(factory, monkeypatch, initial, stop, error):
    tid = candidate(factory)
    FakeGitHub.check_state = "pending"
    if not initial:
        asyncio.run(park(tid))
    async def checks(*args):
        with session_scope() as s:
            if stop == "pause":
                s.get(Workspace, factory["ws"]).paused = True
            else:
                s.get(Task, tid).status = stop
        if error:
            raise GitHubError(503, "offline")
        return await original(*args)
    original = FakeGitHub.checks
    monkeypatch.setattr(FakeGitHub, "checks", checks)
    async def go():
        with pytest.raises(Stop):
            if initial:
                await park(tid)
            await runner(tid).poll_ci()
    asyncio.run(go())
    assert snapshot(tid)[0] == ("waiting_ci" if stop == "pause" else stop)
    with session_scope() as s:
        assert s.query(Decision).filter(Decision.task_id == tid).count() == 0


def test_token_resolution_cancel_and_generic_error_are_guarded(factory, monkeypatch):
    tid = candidate(factory)
    with session_scope() as s:
        s.get(Task, tid).status = "queued"
    def token(ws):
        with session_scope() as s:
            s.get(Task, tid).status = "cancelled"
        raise GitHubError(401, "missing token")
    monkeypatch.setattr(delivery, "resolve_token", token)
    async def go():
        orch = Orchestrator()
        orch._spawn_task(tid)
        await orch._tasks[tid][0]
        assert not orch._tasks
        orch._schedule_ci()
        await asyncio.gather(*list(orch._ci_polls.values()))
    asyncio.run(go())
    assert snapshot(tid)[0] == "cancelled"
    with session_scope() as s:
        assert s.query(Decision).filter(Decision.task_id == tid).count() == 0


def test_atomic_update_rejects_cancel_between_read_and_write(factory, monkeypatch):
    tid = candidate(factory)
    FakeGitHub.check_state = "pending"
    asyncio.run(park(tid))
    original = delivery.transition
    fired = False
    def race(session, task, **fields):
        nonlocal fired
        if fields.get("status") == "queued" and not fired:
            fired = True
            with session_scope() as other:
                other.get(Task, tid).status = "cancelled"
        return original(session, task, **fields)
    monkeypatch.setattr(delivery, "transition", race)
    FakeGitHub.check_state = "success"
    async def go():
        with pytest.raises(Stop):
            await runner(tid).poll_ci()
    asyncio.run(go())
    assert fired and snapshot(tid)[0] == "cancelled"


def test_cancel_before_poll_starts_does_not_leak_capacity(factory):
    ids = [candidate(factory) for _ in range(5)]
    FakeGitHub.check_state = "pending"
    for tid in ids:
        asyncio.run(park(tid))
        age(tid)
    async def go():
        orch = Orchestrator()
        orch._schedule_ci()
        jobs = list(orch._ci_polls.values())
        assert len(jobs) == 4
        for job in jobs:
            job.cancel()
        await asyncio.gather(*jobs, return_exceptions=True)
        await asyncio.sleep(0)
        assert not orch._ci_polls
        orch._schedule_ci()
        assert len(orch._ci_polls) == 4
        await asyncio.gather(*list(orch._ci_polls.values()))
        orch._schedule_ci()
        await asyncio.gather(*list(orch._ci_polls.values()))
        assert not orch._ci_polls
    asyncio.run(go())


def test_paused_workspace_not_polled(factory, monkeypatch):
    tid = candidate(factory)
    FakeGitHub.check_state = "pending"
    asyncio.run(park(tid))
    age(tid)
    with session_scope() as s:
        s.get(Workspace, factory["ws"]).paused = True
    async def go():
        orch = Orchestrator()
        orch._schedule_ci()
        assert not orch._ci_polls
    asyncio.run(go())
    assert snapshot(tid)[0] == "waiting_ci"


@pytest.mark.parametrize("fresh_priority", [0, 1, 2])
def test_one_slot_freed_then_ready_merge_precedes_new_build(factory, monkeypatch, fresh_priority):
    from aifactory.config import reset_settings_cache

    monkeypatch.setenv("AIFACTORY_GLOBAL_CONCURRENCY", "1")
    reset_settings_cache()
    with session_scope() as s:
        w = s.get(Workspace, factory["ws"])
        w.settings = {**w.settings, "concurrency": 1}
    a, b, c = [candidate(factory) for _ in range(3)]
    with session_scope() as s:
        for tid in (a, b, c):
            t = s.get(Task, tid)
            t.status = "queued"
            if tid != a:
                t.stage = ""
    FakeGitHub.check_state = "pending"
    original = TaskRunner.run
    begun = asyncio.Event()
    release = asyncio.Event()
    order = []
    async def run(self):
        order.append((self.task_id, self.task.stage))
        if self.task_id == a and self.task.stage == "ci":
            await original(self)
        else:
            begun.set()
            await release.wait()
            self.update(status="completed", stage="done")
    monkeypatch.setattr(TaskRunner, "run", run)
    async def go():
        orch = Orchestrator()
        orch._schedule()
        await orch._tasks[a][0]
        assert a not in orch._tasks
        assert orch.status()["waiting_ci"] == [a]
        orch._promote()
        with session_scope() as s:
            assert s.get(Outcome, runner(a).task.outcome_id).status == "running"
        orch._schedule()
        await begun.wait()
        assert orch.status()["running"] == [b]
        assert len(orch._tasks) == 1
        with session_scope() as s:
            # A lower-priority CI continuation must still precede a fresh
            # urgent build once B releases the only execution slot.
            s.get(Outcome, s.get(Task, a).outcome_id).priority = 3
            s.get(Outcome, s.get(Task, c).outcome_id).priority = fresh_priority
        FakeGitHub.check_state = "success"
        await runner(a, orch).poll_ci()
        orch._schedule()
        assert a not in orch._tasks  # resumed work must still wait for the slot
        release.set()
        await orch._tasks[b][0]
        orch._schedule()
        assert list(orch._tasks) == [a]
        await orch._tasks[a][0]
        assert order == [(a, "ci"), (b, ""), (a, "merge")]
    try:
        asyncio.run(go())
    finally:
        reset_settings_cache()


def prepare_revert(ids):
    from .test_pipeline import _land_on_main

    _land_on_main(ids, "revert-me.txt", "landed change\n")
    tid = candidate(ids, stage="revert")
    head = git("rev-parse", "main", cwd=ids["remotes"] / "acme/demo.git")
    FakeGitHub.pulls[1] = {"number": 1, "head": {"ref": "main", "sha": head}, "state": "closed", "merged": True}
    with session_scope() as s:
        s.get(Task, tid).merge_sha = head
    FakeGitHub.check_state = "pending"
    asyncio.run(runner(tid).run())
    assert snapshot(tid)[:2] == ("waiting_ci", "ci")
    assert snapshot(tid)[2]["ci_wait"]["continuation"] == "revert"
    return tid


def test_revert_restart_merges_existing_pr_sha_pinned_under_repo_lock(factory, monkeypatch):
    tid = prepare_revert(factory)
    original_deadline = snapshot(tid)[2]["ci_wait"]["deadline"]
    FakeGitHub.check_state = "success"
    orig_merge = FakeGitHub.merge
    orch = Orchestrator()
    async def merge(self, full_name, number, **kwargs):
        assert orch.repo_lock(factory["repo"]).locked()
        assert kwargs["sha"] == snapshot(tid)[2]["revert_candidate"]["head"]
        return await orig_merge(self, full_name, number, **kwargs)
    monkeypatch.setattr(FakeGitHub, "merge", merge)
    async def go():
        orch._recover()
        await runner(tid, orch).poll_ci()
        assert snapshot(tid)[:2] == ("queued", "revert_merge")
        assert snapshot(tid)[2]["ci_wait"]["deadline"] == original_deadline
        orch._spawn_task(tid)
        await orch._tasks[tid][0]
    asyncio.run(go())
    assert snapshot(tid)[:2] == ("reverted", "done")
    assert len(FakeGitHub.pulls) == 2 and FakeGitHub.merged == [2]
    assert "revert-me.txt" not in git("ls-tree", "--name-only", "main", cwd=factory["remotes"] / "acme/demo.git")


@pytest.mark.parametrize("stop", ["cancelled", "pause"])
@pytest.mark.parametrize("where", ["lock", "pull"])
def test_resumed_revert_revalidates_after_lock_and_pull(factory, monkeypatch, stop, where):
    tid = prepare_revert(factory)
    FakeGitHub.check_state = "success"
    asyncio.run(runner(tid).poll_ci())
    def halt():
        with session_scope() as s:
            if stop == "pause":
                s.get(Workspace, factory["ws"]).paused = True
            else:
                s.get(Task, tid).status = stop
    orig_pull = FakeGitHub.pull
    async def pull(*args):
        result = await orig_pull(*args)
        if where == "pull":
            halt()
        return result
    monkeypatch.setattr(FakeGitHub, "pull", pull)
    async def go():
        orch = Orchestrator()
        lock = orch.repo_lock(factory["repo"])
        if where == "lock":
            await lock.acquire()
        orch._spawn_task(tid)
        job = orch._tasks[tid][0]
        if where == "lock":
            await asyncio.sleep(0)
            halt()
            lock.release()
        await job
    asyncio.run(go())
    assert not FakeGitHub.merged
    assert snapshot(tid)[0] == ("queued" if stop == "pause" else stop)
    assert not snapshot(tid)[3]


def test_generic_failure_handler_does_not_block_cancelled_task(factory, monkeypatch):
    tid = candidate(factory)
    with session_scope() as s:
        s.get(Task, tid).status = "queued"
    async def crash(self):
        with session_scope() as s:
            s.get(Task, tid).status = "cancelled"
        raise GitHubError(503, "late error")
    monkeypatch.setattr(TaskRunner, "run", crash)
    async def go():
        orch = Orchestrator()
        orch._spawn_task(tid)
        await orch._tasks[tid][0]
    asyncio.run(go())
    assert snapshot(tid)[0] == "cancelled"
    with session_scope() as s:
        assert not s.query(Decision).filter(Decision.task_id == tid).count()


def test_health_and_disconnect_include_parked_tasks(factory, client):
    tid = candidate(factory)
    FakeGitHub.check_state = "pending"
    asyncio.run(park(tid))
    health = client.get("/api/health")
    assert health.status_code == 200
    assert tid in health.json()["orchestrator"]["waiting_ci"]
    assert tid not in health.json()["orchestrator"]["running"]
    with session_scope() as s:
        email = s.get(User, factory["user"]).email
    assert client.post("/auth/dev", json={"email": email}).status_code == 200
    response = client.delete(f"/api/workspaces/{factory['ws']}/repositories/{factory['repo']}")
    assert response.status_code == 409


def test_cancelling_inflight_poll_and_shutdown_release_poll_resources(factory, monkeypatch):
    tid = candidate(factory)
    FakeGitHub.check_state = "pending"
    asyncio.run(park(tid))
    age(tid)
    entered = asyncio.Event()
    ended = asyncio.Event()
    async def checks(*args):
        entered.set()
        try:
            await asyncio.Event().wait()
        finally:
            ended.set()
    monkeypatch.setattr(FakeGitHub, "checks", checks)
    async def go():
        orch = Orchestrator()
        orch._schedule_ci()
        await entered.wait()
        with session_scope() as s:
            s.get(Task, tid).status = "cancelled"
        orch.cancel_task(tid)
        await asyncio.gather(*list(orch._ci_polls.values()))
        await asyncio.sleep(0)
        assert ended.is_set() and not orch._ci_polls
        assert snapshot(tid)[0] == "cancelled"
        # Shutdown interrupts observations, not the durable parked continuation.
        with session_scope() as s:
            s.get(Task, tid).status = "waiting_ci"
        entered.clear()
        orch._schedule_ci()
        await entered.wait()
        await orch.stop()
        await asyncio.sleep(0)
        assert not orch._ci_polls
        assert snapshot(tid)[0] == "waiting_ci"
        recovered = Orchestrator()
        recovered._recover()
        assert recovered.status()["waiting_ci"] == [tid]
    asyncio.run(go())


def test_new_candidate_resets_deadline_but_same_candidate_does_not(factory):
    tid = candidate(factory)
    FakeGitHub.check_state = "pending"
    asyncio.run(park(tid))
    age(tid)
    old = snapshot(tid)[2]["ci_wait"]
    asyncio.run(park(tid))
    assert snapshot(tid)[2]["ci_wait"]["first_wait_at"] == old["first_wait_at"]
    with session_scope() as s:
        t = s.get(Task, tid)
        t.head_sha = "new-candidate"
    asyncio.run(park(tid))
    new = snapshot(tid)[2]["ci_wait"]
    assert new["head"] == "new-candidate"
    assert new["first_wait_at"] > old["first_wait_at"]
    assert new["deadline"] > old["deadline"]


@pytest.mark.parametrize("continuation", ["delivery", "revert"])
def test_first_slow_ci_observation_releases_slot_and_sandbox(factory, monkeypatch, continuation):
    """Even the first GitHub request runs only after execution resources close."""
    from aifactory.config import reset_settings_cache

    monkeypatch.setenv("AIFACTORY_GLOBAL_CONCURRENCY", "1")
    reset_settings_cache()
    with session_scope() as s:
        w = s.get(Workspace, factory["ws"])
        w.settings = {**w.settings, "concurrency": 1}
    a = prepare_revert(factory) if continuation == "revert" else candidate(factory)
    b = candidate(factory)
    with session_scope() as s:
        t = s.get(Task, a)
        t.status = "queued"
        t.stage = "revert" if continuation == "revert" else "ci"
        t = s.get(Task, b)
        t.status, t.stage = "queued", ""
    closed = asyncio.Event()
    entered = asyncio.Event()
    building = asyncio.Event()
    release = asyncio.Event()
    observations = []

    class Sandbox:
        async def close(self):
            closed.set()

    original_run = TaskRunner.run

    async def run(self):
        if self.task_id == a:
            self.builder = Sandbox()
            await original_run(self)
        else:
            building.set()
            await release.wait()
            self.update(status="completed", stage="done")

    async def checks(*args):
        observations.append(args)
        assert closed.is_set()
        assert a not in orch._tasks
        entered.set()
        await release.wait()
        return await original_checks(*args)

    original_checks = FakeGitHub.checks
    FakeGitHub.check_state = "pending"
    monkeypatch.setattr(TaskRunner, "run", run)
    monkeypatch.setattr(FakeGitHub, "checks", checks)
    orch = Orchestrator()

    async def go():
        orch._schedule()
        # An inline check would hang this runner and fail before polling starts.
        await asyncio.wait_for(orch._tasks[a][0], timeout=2)
        assert closed.is_set() and not observations
        assert a not in orch._tasks
        assert orch.status()["running"] == []
        assert orch.status()["waiting_ci"] == [a]
        orch._schedule()
        await asyncio.wait_for(entered.wait(), timeout=2)
        await asyncio.wait_for(building.wait(), timeout=2)
        assert orch.status()["running"] == [b]
        assert list(orch._ci_polls) == [a]
        assert len(orch._tasks) == 1
        release.set()
        await asyncio.gather(orch._tasks[b][0], *list(orch._ci_polls.values()))
        assert snapshot(a)[0] == "waiting_ci"

    try:
        asyncio.run(go())
    finally:
        reset_settings_cache()


@pytest.mark.parametrize("source", ["gate", "crash"])
@pytest.mark.parametrize("commit_fails", [False, True])
def test_atomic_decision_announced_only_after_commit(factory, monkeypatch, source, commit_fails):
    from contextlib import contextmanager

    from aifactory import events
    from aifactory.models import Event
    from aifactory.pipeline import orchestrator as scheduler

    tid = candidate(factory)
    with session_scope() as s:
        s.get(Task, tid).status = "queued" if source == "crash" else "running"
    published = []

    def publish(payload):
        if payload.get("entity") == "decision" or payload.get("kind") == "decision_requested":
            # The row must already be visible to another transaction when clients
            # receive either the audit event or the invalidation notification.
            with session_scope() as s:
                decision = s.query(Decision).filter(Decision.task_id == tid).one()
                assert (payload.get("data") or {}).get("decision_id", payload.get("id")) == decision.id
            published.append(payload)

    monkeypatch.setattr(events.bus, "publish", publish)

    @contextmanager
    def scope():
        with session_scope() as s:
            yield s
            if commit_fails and s.query(Decision).filter(Decision.task_id == tid).count():
                raise RuntimeError("commit failed")

    if source == "gate":
        r = runner(tid)
        monkeypatch.setattr(delivery, "session_scope", scope)
        with pytest.raises(RuntimeError if commit_fails else Stop):
            r.block("unblock", "Cannot check CI", "Offline")
    else:
        async def crash(self):
            raise GitHubError(503, "offline")
        monkeypatch.setattr(TaskRunner, "run", crash)
        monkeypatch.setattr(scheduler, "session_scope", scope)

        async def go():
            orch = Orchestrator()
            orch._spawn_task(tid)
            await orch._tasks[tid][0]
        asyncio.run(go())
    with session_scope() as s:
        assert s.query(Decision).filter(Decision.task_id == tid).count() == (0 if commit_fails else 1)
        assert s.query(Event).filter(Event.task_id == tid, Event.kind == "decision_requested").count() == (
            0 if commit_fails else 1)
    assert len(published) == (0 if commit_fails else 2)


@pytest.mark.parametrize("expired", [False, True])
@pytest.mark.parametrize("continuation", ["delivery", "revert"])
@pytest.mark.parametrize("slow_call", ["checks", "branch_sha"])
def test_slow_poll_opens_durable_decision(factory, monkeypatch, expired, continuation, slow_call):
    from aifactory.decisions import resolve

    tid = candidate(factory)
    async def start():
        with pytest.raises(Stop):
            await runner(tid)._wait_ci(continuation=continuation)
    asyncio.run(start())
    age(tid)
    original = snapshot(tid)[2]["ci_wait"]
    FakeGitHub.check_state = "none"
    with session_scope() as s:
        repo = s.get(Repository, factory["repo"])
        repo.config = {**repo.config, "require_ci": True}
    ended = asyncio.Event()
    clock = [utcnow()]
    monkeypatch.setattr(delivery, "utcnow", lambda: clock[0])
    async def slow(*args):
        try:
            await asyncio.Event().wait()
        finally:
            # Cross the persisted deadline *during* the network request. Even
            # an observation that never returns must enforce elapsed CI time.
            if expired:
                clock[0] += timedelta(minutes=2)
            ended.set()
    monkeypatch.setattr(FakeGitHub, slow_call, slow)
    monkeypatch.setattr(delivery, "CI_POLL_TIMEOUT_SECONDS", 0.02)
    async def go():
        orch = Orchestrator()
        orch._schedule_ci()
        await asyncio.gather(*list(orch._ci_polls.values()))
        await asyncio.sleep(0)
        assert ended.is_set() and not orch._ci_polls
        assert not orch._tasks
        # Not eligible again: repeated slow calls cannot silently park forever.
        orch._schedule_ci()
        assert not orch._ci_polls
    asyncio.run(go())
    status, stage, ev, lease = snapshot(tid)
    assert (status, stage, lease) == ("blocked", "ci", "")
    assert ev["ci_wait"]["head"] == original["head"]
    assert ev["ci_wait"]["continuation"] == continuation
    assert ev["ci_wait"]["first_wait_at"] == original["first_wait_at"]
    with session_scope() as s:
        decision = s.query(Decision).filter(Decision.task_id == tid).one()
        if expired:
            assert decision.options[0] == {"value": "retry", "label": "Keep waiting"}
            assert ev["ci_wait"]["timed_out"] is True
            resolve(s, decision, s.get(User, factory["user"]), "retry")
        else:
            assert "could not check CI" in decision.title
            assert "timed out" in decision.body
            assert ev["ci_wait"]["deadline"] == original["deadline"]
    if expired:
        reset = snapshot(tid)[2]["ci_wait"]["deadline"]
        assert reset > ev["ci_wait"]["deadline"]
        asyncio.run(start())
        assert snapshot(tid)[2]["ci_wait"]["deadline"] == reset


@pytest.mark.parametrize("expired", [False, True])
@pytest.mark.parametrize("continuation", ["delivery", "revert"])
@pytest.mark.parametrize("error_source", ["checks", "branch_sha", "credential"])
def test_failed_observation_after_restart_honors_deadline(factory, monkeypatch, expired, continuation, error_source):
    tid = candidate(factory)

    async def start():
        with pytest.raises(Stop):
            await runner(tid)._wait_ci(continuation=continuation)
    asyncio.run(start())
    age(tid, expired=expired)
    original = snapshot(tid)[2]["ci_wait"]
    FakeGitHub.check_state = "none"
    with session_scope() as s:
        repo = s.get(Repository, factory["repo"])
        repo.config = {**repo.config, "require_ci": True}

    async def offline(*args):
        raise GitHubError("GitHub is unavailable", status=503)

    def credential(*args):
        raise GitHubError("No GitHub access", status=401)

    if error_source == "credential":
        monkeypatch.setattr(delivery, "resolve_token", credential)
    else:
        monkeypatch.setattr(FakeGitHub, error_source, offline)

    async def go():
        orch = Orchestrator()
        orch._recover()
        orch._schedule_ci()
        await asyncio.gather(*list(orch._ci_polls.values()))
        await asyncio.sleep(0)
        assert not orch._ci_polls and not orch._tasks
        orch._schedule_ci()
        assert not orch._ci_polls
    asyncio.run(go())
    status, stage, ev, lease = snapshot(tid)
    assert (status, stage, lease) == ("blocked", "ci", "")
    for key in ("head", "pr", "continuation", "first_wait_at", "deadline"):
        assert ev["ci_wait"][key] == original[key]
    with session_scope() as s:
        decision = s.query(Decision).filter(Decision.task_id == tid).one()
        if expired:
            assert decision.options == [{"value": "retry", "label": "Keep waiting"},
                                        {"value": "abandon", "label": "Abandon"}]
            assert ev["ci_wait"]["timed_out"] is True
        else:
            assert "could not check CI" in decision.title
            assert ("No GitHub access" if error_source == "credential" else "GitHub is unavailable") in decision.body


@pytest.mark.parametrize("stop", ["cancelled", "handed_over", "abandoned", "pause"])
def test_poll_timeout_cannot_overwrite_concurrent_stop(factory, monkeypatch, stop):
    tid = candidate(factory)
    asyncio.run(park(tid))
    age(tid, expired=True)
    async def slow(*args):
        try:
            await asyncio.Event().wait()
        finally:
            with session_scope() as s:
                if stop == "pause":
                    s.get(Workspace, factory["ws"]).paused = True
                else:
                    s.get(Task, tid).status = stop
    monkeypatch.setattr(FakeGitHub, "checks", slow)
    monkeypatch.setattr(delivery, "CI_POLL_TIMEOUT_SECONDS", 0.02)
    async def go():
        orch = Orchestrator()
        orch._schedule_ci()
        await asyncio.gather(*list(orch._ci_polls.values()))
    asyncio.run(go())
    assert snapshot(tid)[0] == ("waiting_ci" if stop == "pause" else stop)
    with session_scope() as s:
        assert s.query(Decision).filter(Decision.task_id == tid).count() == 0


@pytest.mark.parametrize("stop", ["cancelled", "handed_over", "pause"])
@pytest.mark.parametrize("where", ["merge", "already_merged_pull"])
def test_completed_revert_survives_concurrent_stop(factory, monkeypatch, stop, where):
    from aifactory.models import Event

    tid = prepare_revert(factory)
    FakeGitHub.check_state = "success"
    asyncio.run(runner(tid).poll_ci())
    original_pull = FakeGitHub.pull

    def halt():
        with session_scope() as s:
            if stop == "pause":
                s.get(Workspace, factory["ws"]).paused = True
            else:
                s.get(Task, tid).status = stop

    original_locked_merge = delivery.TaskRunner._revert_under_workspace_lock

    def merge(*args, **kwargs):
        # Policy writes and merge calls now serialize. Simulate the stop after
        # the irreversible call releases its settings transaction, but before
        # the runner records recovery (the same bookkeeping race).
        result = original_locked_merge(*args, **kwargs)
        halt()
        return result

    async def pull(*args, **kwargs):
        result = await original_pull(*args, **kwargs)
        halt()
        return result

    if where == "already_merged_pull":
        ci_head = snapshot(tid)[2]["revert_candidate"]["head"]
        asyncio.run(FakeGitHub("t").merge("acme/demo", 2, sha=ci_head, method="squash", title="revert"))
        monkeypatch.setattr(FakeGitHub, "pull", pull)
    else:
        monkeypatch.setattr(delivery.TaskRunner, "_revert_under_workspace_lock", merge)
    asyncio.run(runner(tid).run())
    status, stage, ev, lease = snapshot(tid)
    assert (status, stage, lease) == ("reverted", "done", "")
    assert ev["revert"]["sha"] == FakeGitHub.pulls[2]["merge_commit_sha"]
    assert ev["revert"]["pr"] == 2
    assert ev["revert"]["ci"] == "success"
    assert FakeGitHub.merged == [2]
    with session_scope() as s:
        assert s.query(Event).filter_by(task_id=tid, kind="reverted").count() == 1
        assert s.query(Decision).filter_by(task_id=tid, status="open").count() == 0


@pytest.mark.parametrize("stop", ["cancelled", "handed_over", "abandoned", "blocked"])
def test_reconcile_recovers_completed_revert_for_stopped_task(factory, monkeypatch, stop):
    from aifactory import reconcile
    from aifactory.models import Event

    tid = prepare_revert(factory)
    candidate = snapshot(tid)[2]["revert_candidate"]
    asyncio.run(FakeGitHub("t").merge("acme/demo", candidate["pr"], sha=candidate["head"],
                                    method="squash", title="revert"))
    with session_scope() as s:
        s.get(Task, tid).status = stop
        s.add(Decision(workspace_id=factory["ws"], task_id=tid, kind="unblock", title="Stale revert decision"))
    monkeypatch.setattr(reconcile, "GitHub", FakeGitHub)
    monkeypatch.setattr(reconcile, "resolve_token", lambda ws: "token")
    assert asyncio.run(reconcile.reconcile_pull_requests()) == 1
    assert asyncio.run(reconcile.reconcile_pull_requests()) == 0
    assert snapshot(tid)[:2] == ("reverted", "done")
    assert snapshot(tid)[2]["revert"]["sha"] == FakeGitHub.pulls[2]["merge_commit_sha"]
    assert FakeGitHub.merged == [2]
    with session_scope() as s:
        assert s.query(Decision).filter_by(task_id=tid).one().status == "superseded"
        assert s.query(Event).filter_by(task_id=tid, kind="reverted").count() == 1


def test_revert_record_commits_evidence_and_terminal_state_together(factory, monkeypatch):
    from contextlib import contextmanager
    from aifactory import reconcile
    from aifactory.models import Event

    tid = prepare_revert(factory)
    ci_head = snapshot(tid)[2]["revert_candidate"]["head"]
    asyncio.run(FakeGitHub("t").merge("acme/demo", 2, sha=ci_head, method="squash", title="revert"))
    before = snapshot(tid)
    notifications = []

    @contextmanager
    def failed_commit():
        with session_scope() as s:
            yield s
            s.flush()
            assert s.get(Task, tid).status == "reverted"
            assert s.get(Task, tid).evidence["revert"]["pr"] == 2
            raise RuntimeError("commit failed")

    with monkeypatch.context() as m:
        m.setattr(reconcile, "session_scope", failed_commit)
        m.setattr(reconcile, "notify", lambda *args, **kwargs: notifications.append(args))
        with pytest.raises(RuntimeError, match="commit failed"):
            reconcile._handle_reverted(tid, FakeGitHub.pulls[2])
    assert snapshot(tid) == before
    assert not notifications
    with session_scope() as s:
        assert not s.query(Event).filter_by(task_id=tid, kind="reverted").count()
    assert reconcile._handle_reverted(tid, FakeGitHub.pulls[2])
    assert snapshot(tid)[:2] == ("reverted", "done")
