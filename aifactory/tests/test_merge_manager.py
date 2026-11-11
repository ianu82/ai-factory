"""The merge manager: one merge turn per repository, parked waiters, red-main pause, GitHub merge queue."""
from __future__ import annotations

import asyncio
from datetime import timedelta

import pytest

from aifactory import merge_manager
from aifactory.db import session_scope, utcnow
from aifactory.models import Decision, Outcome, Plan, Signal, Task
from aifactory.pipeline import delivery
from aifactory.pipeline.delivery import Stop, TaskRunner
from aifactory.pipeline.orchestrator import Orchestrator

from .fakes import FakeGitHub, git
from .test_pipeline import factory, new_outcome  # noqa: F401 - local remote/model fixture


def merge_task(ids, *, priority=2, key="T1", source="user", pr=1):
    oid = new_outcome(ids, title=f"Outcome {key}")
    head = git("rev-parse", "main", cwd=ids["remotes"] / "acme/demo.git")
    with session_scope() as s:
        outcome = s.get(Outcome, oid)
        outcome.status, outcome.priority, outcome.source = "running", priority, source
        plan = Plan(outcome_id=oid)
        s.add(plan)
        s.flush()
        task = Task(workspace_id=ids["ws"], outcome_id=oid, plan_id=plan.id, repo_id=ids["repo"], key=key,
                    title=f"Task {key}", status="running", stage="merge", head_sha=head, base_sha=head,
                    pr_number=pr, lease_owner="orch")
        s.add(task)
        s.flush()
        return task.id


def runner(tid):
    return TaskRunner(tid, asyncio.Event(), Orchestrator().repo_lock)


def state(tid):
    with session_scope() as s:
        t = s.get(Task, tid)
        return t.status, t.stage, dict((t.evidence or {}).get("merge_queue") or {})


def take(tid, gh=None):
    r = runner(tid)
    asyncio.run(merge_manager.take_turn(r, gh or FakeGitHub("token")))
    return r


def held():
    with session_scope() as s:
        return merge_manager.held_back(s)


def requeue(tid):
    """What the scheduler does before dispatching a parked task again."""
    with session_scope() as s:
        t = s.get(Task, tid)
        t.status, t.lease_owner = "running", "orch"


def test_one_turn_per_repository_and_waiters_park_without_a_slot(factory):
    first, second = merge_task(factory, key="T1"), merge_task(factory, key="T2", pr=2)
    take(first)
    assert state(first)[2]["turn"] is True
    with pytest.raises(Stop):
        take(second)
    status, stage, entry = state(second)
    assert (status, stage, entry["position"]) == ("queued", "merge", 2)
    assert held() == {second}

    # The head merges: the next task is at the head and becomes dispatchable.
    with session_scope() as s:
        s.get(Task, first).merge_sha = "abc"
    assert held() == set()
    requeue(second)
    take(second)
    assert state(second)[2]["turn"] is True


def test_priority_orders_waiters_but_never_takes_the_turn_from_its_holder(factory):
    holder = merge_task(factory, key="T1", priority=2)
    normal = merge_task(factory, key="T2", priority=2, pr=2)
    urgent = merge_task(factory, key="T3", priority=0, pr=3)
    take(holder)
    for tid in (normal, urgent):
        with pytest.raises(Stop):
            take(tid)
    with session_scope() as s:
        assert [t.id for t in merge_manager.ordered(s, factory["repo"])] == [holder, urgent, normal]
    assert state(urgent)[2]["position"] == 2


def test_red_main_pauses_the_queue_except_for_repairs(factory, monkeypatch):
    FakeGitHub.check_state = "failure"

    async def failing_checks(self, full_name, sha):
        from aifactory.github import CheckSummary

        return CheckSummary("failure", [{"name": "tests", "status": "completed", "conclusion": "failure"}])

    monkeypatch.setattr(FakeGitHub, "checks", failing_checks)
    tid = merge_task(factory, key="T1")
    with pytest.raises(Stop):
        take(tid)
    status, _, entry = state(tid)
    assert status == "queued" and entry["paused"] == "main_red" and entry["retry_at"] > utcnow().isoformat()
    assert held() == {tid}  # backing off: the scheduler leaves it alone until retry_at

    # A fix for the failing main is never held back by the pause.
    with session_scope() as s:
        fix = s.get(Task, merge_task(factory, key="T2", source="signal", pr=2))
        s.add(Signal(workspace_id=factory["ws"], source="ci_failure", fingerprint="main-red", title="CI failing",
                     status="accepted", outcome_id=fix.outcome_id))
        fix_id = fix.id
        s.get(Task, tid).evidence = {}  # let the fix reach the head of the queue first
    take(fix_id)
    assert state(fix_id)[2]["turn"] is True

    # A repository can opt out of the pause.
    with session_scope() as s:
        s.get(Task, fix_id).merge_sha = "fixed"
    other = merge_task(factory, key="T3", pr=3)
    r = runner(other)
    r.cfg = {**r.cfg, "merge": {"hold_on_red_main": False}}
    asyncio.run(merge_manager.take_turn(r, FakeGitHub("token")))
    assert state(other)[2]["turn"] is True


def test_a_rebuild_leaves_the_queue_and_frees_the_turn(factory):
    first, second = merge_task(factory, key="T1"), merge_task(factory, key="T2", pr=2)
    r = take(first)
    with pytest.raises(Stop):
        take(second)
    merge_manager.leave(r)  # what deliver() does when the candidate goes back to its builder
    assert "merge_queue" not in (r.task.evidence or {})
    assert held() == set()


class QueueGitHub(FakeGitHub):
    has_queue = True
    entry: dict | None = None
    enqueued: list = []

    async def graphql(self, query, variables):
        if query.startswith("mutation"):
            QueueGitHub.enqueued.append(variables)
            QueueGitHub.entry = {"state": "QUEUED", "position": 1}
            return {"data": {"enqueuePullRequest": {"mergeQueueEntry": QueueGitHub.entry}}}
        return {"data": {"repository": {"mergeQueue": {"id": "MQ"} if QueueGitHub.has_queue else None,
                                        "pullRequest": {"id": f"PR{variables['number']}", "merged": False,
                                                        "mergeQueueEntry": QueueGitHub.entry}}}}


def test_github_merge_queue_takes_the_turn_holder_and_the_next_task_moves_up(factory):
    QueueGitHub.entry, QueueGitHub.enqueued = None, []
    first, second = merge_task(factory, key="T1"), merge_task(factory, key="T2", pr=2)
    r = take(first, QueueGitHub("token"))
    gh = QueueGitHub("token")
    queue_state = asyncio.run(merge_manager.github_queue_state(gh, r))
    with pytest.raises(Stop):
        asyncio.run(merge_manager.merge_via_github_queue(r, gh, queue_state))
    assert QueueGitHub.enqueued == [{"id": "PR1", "head": r.task.head_sha}]
    status, _, entry = state(first)
    assert status == "queued" and entry["github"] is True and entry["turn"] is False
    # GitHub orders it now: the next Factory task is at the head and can take its turn.
    take(second, QueueGitHub("token"))
    assert state(second)[2]["turn"] is True
    with session_scope() as s:
        assert [row["github_queue"] for row in merge_manager.queue_view(s, factory["repo"])] == [False, True]


def test_github_removing_the_pr_from_its_queue_asks_a_person(factory):
    QueueGitHub.entry, QueueGitHub.enqueued = None, []
    tid = merge_task(factory, key="T1")
    with session_scope() as s:
        s.get(Task, tid).evidence = {"merge_queue": {"entered_at": utcnow().isoformat(), "github": True}}
    r = runner(tid)
    gh = QueueGitHub("token")
    queue_state = asyncio.run(merge_manager.github_queue_state(gh, r))
    with pytest.raises(Stop):
        asyncio.run(merge_manager.merge_via_github_queue(r, gh, queue_state))
    with session_scope() as s:
        assert s.get(Task, tid).status == "needs_input"
        assert s.query(Decision).filter(Decision.task_id == tid).count() == 1
    assert QueueGitHub.enqueued == []


def test_without_a_github_queue_the_factory_merges_itself(factory):
    QueueGitHub.has_queue = False
    try:
        tid = merge_task(factory, key="T1")
        assert asyncio.run(merge_manager.github_queue_state(QueueGitHub("token"), runner(tid))) is None
        r = runner(tid)
        r.cfg = {**r.cfg, "merge": {"queue": "factory"}}
        QueueGitHub.has_queue = True
        assert asyncio.run(merge_manager.github_queue_state(QueueGitHub("token"), r)) is None
    finally:
        QueueGitHub.has_queue = True


def test_the_api_lists_each_repository_queue(factory, client, signed_in):
    ws = signed_in["workspaces"][0]["id"]
    response = client.get(f"/api/workspaces/{ws}/merge-queue")
    assert response.status_code == 200 and response.json() == {"queues": {}}


def test_backoff_expires(factory):
    tid = merge_task(factory, key="T1")
    with session_scope() as s:
        s.get(Task, tid).status = "queued"
        s.get(Task, tid).evidence = {"merge_queue": {"entered_at": utcnow().isoformat(),
                                                     "retry_at": (utcnow() - timedelta(seconds=1)).isoformat()}}
    assert held() == set()


def test_two_outcomes_deliver_through_the_queue_end_to_end(factory):
    from .test_pipeline import drive, outcome_status

    first, second = new_outcome(factory, "Greeting one"), new_outcome(factory, "Greeting two")
    assert drive(lambda: all(outcome_status(o) in ("completed", "attention", "blocked") for o in (first, second)),
                 timeout=120)
    with session_scope() as s:
        tasks = s.query(Task).filter(Task.outcome_id.in_([first, second])).all()
        merged = [t for t in tasks if t.merge_sha]
        assert merged, [(t.status, t.stage, t.blocked_reason) for t in tasks]
        for t in merged:
            assert t.status == "completed" and (t.evidence or {}).get("merge_queue", {}).get("turn") is True
        assert merge_manager.held_back(s) == set()


def test_a_paused_head_steps_aside_for_a_repair_behind_it(factory, monkeypatch):
    async def failing_checks(self, full_name, sha):
        from aifactory.github import CheckSummary

        return CheckSummary("failure", [{"name": "tests", "status": "completed", "conclusion": "failure"}])

    monkeypatch.setattr(FakeGitHub, "checks", failing_checks)
    normal = merge_task(factory, key="T1")
    with pytest.raises(Stop):
        take(normal)  # paused at the head: main is red
    with session_scope() as s:
        fix = s.get(Task, merge_task(factory, key="T2", source="signal", pr=2))
        s.add(Signal(workspace_id=factory["ws"], source="ci_failure", fingerprint="main-red-2", title="CI failing",
                     status="accepted", outcome_id=fix.outcome_id))
        fix_id = fix.id
    take(fix_id)
    assert state(fix_id)[2]["turn"] is True


def test_github_queue_rebases_a_stale_head_before_enqueueing(factory, monkeypatch):
    QueueGitHub.entry, QueueGitHub.enqueued = None, []
    tid = merge_task(factory, key="T1")
    r = runner(tid)
    r.task.base_sha = "stale"
    refreshed = []

    async def refresh(self, token, gh, main_sha):
        refreshed.append(main_sha)

    monkeypatch.setattr(TaskRunner, "_refresh_onto_main", refresh)
    async def open_pull(self, full_name, number):
        return {"merged": False, "state": "open"}

    monkeypatch.setattr(QueueGitHub, "pull", open_pull)
    monkeypatch.setattr(delivery, "GitHub", QueueGitHub)
    monkeypatch.setattr(TaskRunner, "_require_merge_approval", lambda self: None)
    with session_scope() as s:
        s.get(Task, tid).base_sha = "stale"
    r = runner(tid)
    asyncio.run(r.merge())
    assert refreshed and QueueGitHub.enqueued == []


def test_a_stale_turn_yields_to_the_newer_holder(factory):
    old, new = merge_task(factory, key="T1"), merge_task(factory, key="T2", pr=2)
    with session_scope() as s:
        s.get(Task, old).evidence = {"merge_queue": {"entered_at": "2026-01-01", "turn": True, "granted_at": "2026-01-01"}}
        s.get(Task, new).evidence = {"merge_queue": {"entered_at": "2026-01-02", "turn": True, "granted_at": "2026-01-03"}}
    with session_scope() as s:
        assert merge_manager.ordered(s, factory["repo"])[0].id == new
    with pytest.raises(Stop):
        take(old)
    assert state(old)[2]["turn"] is False


def test_explicit_github_mode_never_falls_back_on_a_lookup_failure(factory):
    class Broken(QueueGitHub):
        async def graphql(self, query, variables):
            raise RuntimeError("boom")

    r = runner(merge_task(factory, key="T1"))
    r.cfg = {**r.cfg, "merge": {"queue": "github"}}
    with pytest.raises(Stop):
        asyncio.run(merge_manager.github_queue_state(Broken("token"), r))


def test_a_task_handed_to_github_stays_parked_when_lookup_fails_or_it_just_merged(factory):
    class Broken(QueueGitHub):
        async def graphql(self, query, variables):
            raise RuntimeError("boom")

    tid = merge_task(factory, key="T1")
    with session_scope() as s:
        s.get(Task, tid).evidence = {"merge_queue": {"entered_at": utcnow().isoformat(), "github": True}}
    with pytest.raises(Stop):
        asyncio.run(merge_manager.github_queue_state(Broken("token"), runner(tid)))
    r = runner(tid)
    with pytest.raises(Stop):
        asyncio.run(merge_manager.merge_via_github_queue(r, QueueGitHub("token"),
                                                         {"pr_id": "PR1", "merged": True, "entry": None}))
    assert state(tid)[0] == "queued"


def test_explicit_github_mode_without_a_queue_asks_a_person(factory):
    QueueGitHub.has_queue = False
    try:
        tid = merge_task(factory, key="T1")
        r = runner(tid)
        r.cfg = {**r.cfg, "merge": {"queue": "github"}}
        with pytest.raises(Stop):
            asyncio.run(merge_manager.github_queue_state(QueueGitHub("token"), r))
        assert state(tid)[0] == "needs_input"
    finally:
        QueueGitHub.has_queue = True


def test_a_turn_holder_paused_by_red_main_gives_up_its_turn(factory, monkeypatch):
    async def failing_checks(self, full_name, sha):
        from aifactory.github import CheckSummary

        return CheckSummary("failure", [{"name": "tests", "status": "completed", "conclusion": "failure"}])

    tid = merge_task(factory, key="T1")
    take(tid)
    assert state(tid)[2]["turn"] is True
    monkeypatch.setattr(FakeGitHub, "checks", failing_checks)
    requeue(tid)
    with pytest.raises(Stop):
        take(tid)
    assert state(tid)[2]["turn"] is False


def test_a_repair_bypasses_a_turn_holder(factory):
    holder = merge_task(factory, key="T1")
    take(holder)
    with session_scope() as s:
        fix = s.get(Task, merge_task(factory, key="T2", source="signal", pr=2))
        s.add(Signal(workspace_id=factory["ws"], source="ci_failure", fingerprint="main-red-3", title="CI failing",
                     status="accepted", outcome_id=fix.outcome_id))
        fix_id = fix.id
    take(fix_id)  # does not park behind the holder
    assert state(fix_id)[0] != "queued"


def test_red_main_starts_one_urgent_repair_and_waits_for_it(factory, monkeypatch):
    async def failing_checks(self, full_name, sha):
        from aifactory.github import CheckSummary

        return CheckSummary("failure", [{"name": "tests", "status": "completed", "conclusion": "failure"}])

    monkeypatch.setattr(FakeGitHub, "checks", failing_checks)
    tid = merge_task(factory, key="T1")
    for _ in range(2):  # retries after the back-off never start a second repair
        requeue(tid)
        with pytest.raises(Stop):
            take(tid)
    with session_scope() as s:
        repairs = s.query(Outcome).filter(Outcome.priority == 0, Outcome.source == "signal").all()
        assert len(repairs) == 1 and "Fix failing" in repairs[0].title
        signal = s.query(Signal).filter(Signal.outcome_id == repairs[0].id).one()
        assert signal.source == "ci_failure" and signal.status == "accepted"
        repairs[0].status = "completed"
    # Main goes red again later: a new repair starts.
    requeue(tid)
    with pytest.raises(Stop):
        take(tid)
    with session_scope() as s:
        assert s.query(Outcome).filter(Outcome.priority == 0, Outcome.source == "signal").count() == 2


def test_auto_repair_can_be_turned_off(factory, monkeypatch):
    async def failing_checks(self, full_name, sha):
        from aifactory.github import CheckSummary

        return CheckSummary("failure", [{"name": "tests", "status": "completed", "conclusion": "failure"}])

    monkeypatch.setattr(FakeGitHub, "checks", failing_checks)
    r = runner(merge_task(factory, key="T1"))
    r.cfg = {**r.cfg, "merge": {"auto_repair": False}}
    with pytest.raises(Stop):
        asyncio.run(merge_manager.take_turn(r, FakeGitHub("token")))
    with session_scope() as s:
        assert s.query(Outcome).filter(Outcome.priority == 0).count() == 0
