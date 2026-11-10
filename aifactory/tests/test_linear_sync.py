"""Lifecycle recovery and scheduler isolation use only fake GraphQL endpoints."""

import asyncio
import json

import httpx
import pytest

from aifactory import linear_config, linear_sync
from aifactory.db import engine, session_scope, utcnow
from aifactory.linear import Linear
from aifactory.linear_outbox import Dispatcher
from aifactory.linear_sync import LinearSync
from aifactory.models import Decision, Event, LinearOperation as Operation, Outcome, Plan, Repository, Run, Secret, Signal, Task, Workspace
from aifactory.pipeline.orchestrator import Orchestrator
from aifactory.security import encrypt
from .linear_fake import connection
from .test_linear_intake import configured, issue
from .test_linear_outbox import Remote, retry


class Endpoint(Remote):
    def __init__(self):
        super().__init__()
        self.states["issue-1"] = "queued"
        self.graphql_error = None
        self.intake = []

    def handle(self, request):
        if self.graphql_error:
            self.requests.append(json.loads(request.content))
            return httpx.Response(200, json={"errors": [{"message": "sensitive remote detail",
                "extensions": {"code": self.graphql_error}}]})
        if "issues(" in json.loads(request.content)["query"] and not self.failure:
            assert engine().pool.checkedout() == 0
            self.requests.append(json.loads(request.content))
            return httpx.Response(200, json={"data": {"issues": connection(self.intake)}})
        return super().handle(request)


@pytest.fixture
def linked(configured):
    workspace_id, repo_ids = configured
    with session_scope() as db:
        db.get(Workspace, workspace_id).outcome_seq = 1
        outcome = Outcome(workspace_id=workspace_id, number=1, title="Linked", status="planning")
        db.add(outcome)
        db.flush()
        signal = Signal(workspace_id=workspace_id, source="linear_issue", external_id="issue-1",
                        fingerprint="linked", title="Linked", outcome_id=outcome.id)
        db.add(signal)
        db.flush()
        outcome.signal_id = signal.id
        plan = Plan(outcome_id=outcome.id)
        db.add(plan)
        db.flush()
        tasks = [Task(workspace_id=workspace_id, outcome_id=outcome.id, plan_id=plan.id,
                      repo_id=repo_ids[0], key=f"T{index}", title="Task", status="pending")
                 for index in range(2)]
        db.add_all(tasks)
        db.flush()
        return workspace_id, outcome.id, [task.id for task in tasks], plan.id


def cycle(remote):
    retry()
    asyncio.run(LinearSync(remote.provider).cycle())


def test_lifecycle_and_earliest_parallel_phase(linked, monkeypatch):
    monkeypatch.setenv("AIFACTORY_SIGNAL_INTERVAL_SECONDS", "0")
    workspace_id, outcome_id, task_ids, _plan_id = linked
    remote = Endpoint()
    cycle(remote)
    assert remote.states["issue-1"] == "queued"
    for stages, expected in [(("prepare", "merge"), "building"), (("build", "publish"), "building"),
                             (("checks", "verify"), "building"), (("verify", "ci"), "verifying"),
                             (("publish", "release"), "verifying"), (("ci", "release"), "merging")]:
        with session_scope() as db:
            db.get(Outcome, outcome_id).status = "running"
            for task_id, stage in zip(task_ids, stages):
                task = db.get(Task, task_id)
                task.status, task.stage = "running", stage
        cycle(remote)
        assert remote.states["issue-1"] == expected
    with session_scope() as db:
        db.get(Task, task_ids[1]).status = "awaiting_approval"
        decision = Decision(workspace_id=workspace_id, outcome_id=outcome_id,
                            task_id=task_ids[1], kind="approve_merge", title="Approve")
        db.add(decision)
        db.flush()
        decision_id = decision.id
    cycle(remote)
    assert remote.states["issue-1"] == "blocked"
    with session_scope() as db:
        db.get(Task, task_ids[1]).status = "running"
    cycle(remote)
    assert remote.states["issue-1"] == "blocked"
    with session_scope() as db:
        db.get(Decision, decision_id).status = "resolved"
        db.get(Task, task_ids[1]).stage = "build"
    cycle(remote)
    assert remote.states["issue-1"] == "building"
    with session_scope() as db:
        db.get(Outcome, outcome_id).status = "completed"
        for task_id in task_ids:
            db.get(Task, task_id).status = "completed"
    cycle(remote)
    assert remote.states["issue-1"] == "done"
    assert sum("Outcome delivered" in comment["body"] for comment in remote.comments.values()) == 1


@pytest.mark.parametrize("other_status,other_stage,expected", [
    ("waiting_ci", "ci", "merging"),
    ("completed", "done", "merging"),
    ("pending", "prepare", "merging"),
    ("running", "build", "building"),
    ("queued", "verify", "verifying"),
    ("awaiting_approval", "merge", "blocked"),
])
def test_waiting_ci_is_an_active_delivery_phase(linked, monkeypatch, other_status, other_stage, expected):
    monkeypatch.setenv("AIFACTORY_SIGNAL_INTERVAL_SECONDS", "0")
    remote = Endpoint()
    with session_scope() as db:
        db.get(Outcome, linked[1]).status = "running"
        waiting = db.get(Task, linked[2][0])
        waiting.status, waiting.stage = "waiting_ci", "ci"
        other = db.get(Task, linked[2][1])
        other.status, other.stage = other_status, other_stage
    for _attempt in range(2):
        cycle(remote)
    assert remote.states["issue-1"] == expected
    assert remote.effects == [("issue-1", expected)]
    with session_scope() as db:
        assert db.get(Task, linked[2][0]).status == "waiting_ci"
        assert db.get(Outcome, linked[1]).status == "running"


@pytest.mark.parametrize("status", ["cancelled", "partially_completed", "reverted", "handed_over",
                                    "abandoned", "blocked", "attention", "needs_input"])
def test_non_delivery_is_blocked(linked, monkeypatch, status):
    monkeypatch.setenv("AIFACTORY_SIGNAL_INTERVAL_SECONDS", "0")
    with session_scope() as db:
        db.get(Outcome, linked[1]).status = status
    remote = Endpoint()
    cycle(remote)
    assert remote.states["issue-1"] == "blocked"
    assert not any("delivered" in comment["body"] for comment in remote.comments.values())


def test_restart_reconstructs_comments_and_historical_prs(linked, monkeypatch):
    monkeypatch.setenv("AIFACTORY_SIGNAL_INTERVAL_SECONDS", "0")
    workspace_id, outcome_id, task_ids, plan_id = linked
    old_pr, current_pr = "https://github.com/org/repo/pull/1", "https://github.com/org/repo/pull/2"
    with session_scope() as db:
        db.get(Task, task_ids[0]).pr_url = current_pr
        db.add(Event(workspace_id=workspace_id, outcome_id=outcome_id, task_id=task_ids[0],
                     kind="published", message="Opened PR", data={"url": old_pr}))
        db.add(Plan(outcome_id=outcome_id, version=2))
        decision = Decision(workspace_id=workspace_id, outcome_id=outcome_id, kind="question", title="Help")
        db.add(decision)
        db.flush()
        decision_id = decision.id
    remote = Endpoint()
    cycle(remote)
    bodies = [comment["body"] for comment in remote.comments.values()]
    base = f"/w/{workspace_id}/outcomes/{outcome_id}"
    assert sum("Plan ready" in body and base in body for body in bodies) == 2
    assert sum(old_pr in body for body in bodies) == sum(current_pr in body for body in bodies) == 1
    assert sum(base + f"#decision-{decision_id}" in body for body in bodies) == 1
    with session_scope() as db:
        assert db.query(Event).filter_by(kind="plan_proposed").count() == 0
        assert db.query(Event).filter_by(kind="decision_requested").count() == 0
        db.get(Decision, decision_id).status = "resolved"
        db.get(Outcome, outcome_id).status = "completed"
        for task_id in task_ids:
            db.get(Task, task_id).status = "completed"
        db.add_all([Run(workspace_id=workspace_id, outcome_id=outcome_id, role="builder", harness="native", cost_usd=2),
                    Run(workspace_id=workspace_id, outcome_id=outcome_id, role="planner", harness="native", cost_usd=3)])
    for _attempt in range(3):
        cycle(remote)
    assert len(remote.comments) == 6
    assert sum("Total cost: $5.00" in comment["body"] for comment in remote.comments.values()) == 1
    with session_scope() as db:
        assert db.query(Operation).filter_by(kind="comment", status="sent").count() == 6


@pytest.mark.parametrize("intermediate", ["reverted", "blocked", "failed", "abandoned", "running"])
@pytest.mark.parametrize("lost_ack", [False, True])
def test_completion_recovery_revalidates_and_can_complete_again(linked, monkeypatch, intermediate, lost_ack):
    monkeypatch.setenv("AIFACTORY_SIGNAL_INTERVAL_SECONDS", "0")
    remote = Endpoint()
    cycle(remote)
    with session_scope() as db:
        db.get(Outcome, linked[1]).status = "completed"
        for task_id in linked[2]:
            db.get(Task, task_id).status = "completed"
    if lost_ack:
        remote.loss = "commentCreate"
    else:
        remote.failure = 401
    cycle(remote)
    with session_scope() as db:
        completion_id = db.query(Operation).filter_by(kind="comment").filter(
            Operation.payload["comment_kind"].as_string() == "completion").one().remote_id
        db.get(Outcome, linked[1]).status = intermediate
        db.get(Task, linked[2][0]).status = intermediate
    remote.failure = None
    cycle(remote)
    expected = "building" if intermediate == "running" else "blocked"
    assert remote.states["issue-1"] == expected
    assert sum("delivered" in comment["body"] for comment in remote.comments.values()) == int(lost_ack)
    with session_scope() as db:
        assert db.query(Operation).filter_by(kind="comment", status="superseded").count() == 1
        db.get(Outcome, linked[1]).status = "completed"
        for task_id in linked[2]:
            db.get(Task, task_id).status = "completed"
    for _attempt in range(3):
        cycle(remote)
    assert remote.states["issue-1"] == "done"
    assert sum("delivered" in comment["body"] for comment in remote.comments.values()) == 1
    assert completion_id in remote.comments
    with session_scope() as db:
        assert db.query(Operation).filter_by(remote_id=completion_id, status="sent").count() == 1


@pytest.mark.parametrize("legacy", [False, True])
def test_completion_revalidated_after_remote_read(linked, monkeypatch, legacy):
    monkeypatch.setenv("AIFACTORY_SIGNAL_INTERVAL_SECONDS", "0")
    remote = Endpoint()
    with session_scope() as db:
        db.get(Outcome, linked[1]).status = "completed"
        for task_id in linked[2]:
            db.get(Task, task_id).status = "completed"
    remote.failure = 401
    cycle(remote)
    if legacy:
        with session_scope() as db:
            for operation in db.query(Operation).filter_by(kind="comment"):
                operation.payload = {"body": operation.payload["body"]}

    class Reverting(Linear):
        async def comments(self, issue_id):
            comments = await super().comments(issue_id)
            with session_scope() as db:
                db.get(Task, linked[2][0]).status = "reverted"
            return comments

    remote.failure = None
    retry()
    asyncio.run(Dispatcher(lambda workspace_id: Reverting("fake", transport=remote)).dispatch())
    assert not any("delivered" in comment["body"] for comment in remote.comments.values())
    cycle(remote)
    assert remote.states["issue-1"] == "blocked"


@pytest.mark.parametrize("failure", [401, 429, "network", "RATELIMITED"])
def test_warning_deduplication_revision_and_recovery(linked, monkeypatch, failure):
    monkeypatch.setenv("AIFACTORY_SIGNAL_INTERVAL_SECONDS", "180")
    remote = Endpoint()
    remote.intake = [issue(2)]
    if failure == "RATELIMITED":
        remote.graphql_error = failure
    else:
        remote.failure = httpx.ConnectError("sensitive credential") if failure == "network" else failure
    for _attempt in range(3):
        cycle(remote)
    with session_scope() as db:
        warnings = db.query(Event).filter_by(workspace_id=linked[0], level="warn").all()
        assert len(warnings) == 1
        assert "sensitive" not in str([(event.message, event.data) for event in warnings])
        db.add(Secret(workspace_id=linked[0], kind="linear_api_key", ciphertext=encrypt("replacement-secret")))
    cycle(remote)
    with session_scope() as db:
        assert db.query(Event).filter_by(workspace_id=linked[0], level="warn").count() == 2
        workspace = db.get(Workspace, linked[0])
        workspace.settings = {**workspace.settings,
                              "linear": {**workspace.settings["linear"], "intake_label": "new-label"}}
    cycle(remote)
    with session_scope() as db:
        assert db.query(Event).filter_by(workspace_id=linked[0], level="warn").count() == 3
    remote.failure = remote.graphql_error = None
    cycle(remote)
    assert remote.states["issue-1"] == "queued"
    with session_scope() as db:
        assert db.query(Outcome).filter_by(workspace_id=linked[0]).count() == 2
    assert len(remote.comments) == 1


@pytest.mark.parametrize("pause", ["paused", "auto_sync"])
def test_pause_only_disables_intake(linked, monkeypatch, pause):
    remote = Endpoint()
    remote.intake = [issue(2)]
    monkeypatch.setenv("AIFACTORY_SIGNAL_INTERVAL_SECONDS", "180")
    with session_scope() as db:
        workspace = db.get(Workspace, linked[0])
        if pause == "paused":
            workspace.paused = True
        else:
            workspace.settings = {**workspace.settings, "signals": {"auto_sync": False}}
    cycle(remote)
    assert len(remote.comments) == 1
    assert not any("issues(" in request["query"] for request in remote.requests)
    with session_scope() as db:
        assert db.query(Outcome).filter_by(workspace_id=linked[0]).count() == 1


def test_stalled_endpoint_does_not_delay_scheduler_or_shutdown(linked, monkeypatch):
    monkeypatch.setenv("AIFACTORY_SIGNAL_INTERVAL_SECONDS", "180")

    async def scenario():
        entered, cancelled, ticked, delivered = (asyncio.Event() for _index in range(4))

        async def stalled(request):
            assert engine().pool.checkedout() == 0
            entered.set()
            try:
                await asyncio.Event().wait()
            finally:
                cancelled.set()

        monkeypatch.setattr(linear_config, "provider", lambda workspace_id:
                            Linear("fake", transport=httpx.MockTransport(stalled)))
        orchestrator = Orchestrator()
        monkeypatch.setattr(orchestrator, "_recover", lambda: None)
        monkeypatch.setattr(orchestrator, "_schedule", ticked.set)
        monkeypatch.setattr(orchestrator, "_maybe_analyze", lambda: None)

        async def github_failure():
            raise RuntimeError("GitHub unavailable")

        monkeypatch.setattr(orchestrator, "_maybe_sync_signals", github_failure)
        with session_scope() as db:
            db.get(Outcome, linked[1]).status = "running"
            for task_id in linked[2]:
                db.get(Task, task_id).status = "running"
        orchestrator.start()
        try:
            await asyncio.wait_for(entered.wait(), 2)
            await asyncio.wait_for(ticked.wait(), 2)
            worker = orchestrator._linear_task
            orchestrator.wake()
            ticked.clear()
            await asyncio.wait_for(ticked.wait(), 2)
            assert orchestrator._linear_task is worker
            with session_scope() as db:
                assert db.get(Outcome, linked[1]).status == "running"
                for task_id in linked[2]:
                    assert db.get(Task, task_id).status == "running"
                db.get(Task, linked[2][1]).status = "pending"
            await asyncio.to_thread(orchestrator._promote)
            with session_scope() as db:
                assert db.get(Task, linked[2][1]).status == "queued"

            async def delivery():
                with session_scope() as db:
                    for task_id in linked[2]:
                        db.get(Task, task_id).status = "completed"
                await asyncio.to_thread(orchestrator._promote)
                delivered.set()

            await asyncio.wait_for(asyncio.create_task(delivery()), 2)
            assert delivered.is_set()
            with session_scope() as db:
                assert db.get(Outcome, linked[1]).status == "completed"
        finally:
            await asyncio.wait_for(orchestrator.stop(), 2)
        assert cancelled.is_set() and worker.done()

    asyncio.run(scenario())


def test_concurrent_cycles_do_not_overlap(linked, monkeypatch):
    monkeypatch.setenv("AIFACTORY_SIGNAL_INTERVAL_SECONDS", "0")

    async def scenario():
        entered, release = asyncio.Event(), asyncio.Event()
        remote = Endpoint()

        async def handle(request):
            entered.set()
            await release.wait()
            return remote.handle(request)

        worker = LinearSync(lambda workspace_id: Linear("fake", transport=httpx.MockTransport(handle)))
        first = asyncio.create_task(worker.cycle())
        await asyncio.wait_for(entered.wait(), 2)
        await asyncio.wait_for(worker.cycle(), 0.1)
        release.set()
        await first
        assert len(remote.comments) == 1

    asyncio.run(scenario())


@pytest.mark.parametrize("stalled_phase", ["outbound", "intake"])
def test_workspace_deadlines_isolate_dispatch_and_intake(linked, monkeypatch, stalled_phase):
    monkeypatch.setenv("AIFACTORY_SIGNAL_INTERVAL_SECONDS", "180")
    monkeypatch.setattr(linear_sync, "WORKSPACE_TIMEOUT_SECONDS", 2)
    stalled_id = linked[0]
    healthy_ids = []
    with session_scope() as db:
        original = db.get(Workspace, stalled_id)
        for index in range(2):
            workspace = Workspace(org_id=original.org_id, name=f"Healthy {index}", slug=f"healthy-{index}")
            db.add(workspace)
            db.flush()
            repo = Repository(workspace_id=workspace.id, full_name=f"org/healthy-{index}")
            db.add(repo)
            db.flush()
            workspace.settings = {"linear": {**original.settings["linear"], "repo_ids": [repo.id]}}
            outcome = Outcome(workspace_id=workspace.id, number=1, title="Healthy", status="blocked")
            db.add(outcome)
            db.flush()
            db.add(Signal(workspace_id=workspace.id, source="linear_issue", external_id="issue-1",
                          fingerprint="linked", title="Healthy", outcome_id=outcome.id))
            db.add(Plan(outcome_id=outcome.id))
            workspace.outcome_seq = 1
            healthy_ids.append(workspace.id)

    async def scenario():
        entered, cancelled = asyncio.Event(), asyncio.Event()
        remotes = {workspace_id: Endpoint() for workspace_id in [stalled_id, *healthy_ids]}
        for remote in remotes.values():
            remote.intake = [issue(2)]

        async def stalled(request):
            query = json.loads(request.content)["query"]
            if stalled_phase == "outbound" or "issues(" in query:
                assert engine().pool.checkedout() == 0
                entered.set()
                try:
                    await asyncio.Event().wait()
                finally:
                    cancelled.set()
            return remotes[stalled_id].handle(request)

        def provider(workspace_id):
            return (Linear("fake", transport=httpx.MockTransport(stalled)) if workspace_id == stalled_id
                    else remotes[workspace_id].provider(workspace_id))

        worker = LinearSync(provider)
        cycle_task = asyncio.create_task(worker.cycle())

        async def healthy_complete():
            while True:
                with session_scope() as db:
                    if all(db.query(Outcome).filter_by(workspace_id=workspace_id).count() == 2
                           for workspace_id in healthy_ids):
                        return
                await asyncio.sleep(0)

        try:
            await asyncio.wait_for(entered.wait(), 1)
            await asyncio.wait_for(healthy_complete(), 1)
            assert not cancelled.is_set()
            await asyncio.wait_for(cycle_task, 5)
        finally:
            cycle_task.cancel()
            await asyncio.gather(cycle_task, return_exceptions=True)
        assert entered.is_set() and cancelled.is_set()
        with session_scope() as db:
            warnings = db.query(Event).filter_by(level="warn").all()
            assert [(event.workspace_id, event.data) for event in warnings] == [
                (stalled_id, {"category": "timeout"})]
            for workspace_id in healthy_ids:
                assert len(remotes[workspace_id].comments) == 1
                assert remotes[workspace_id].states["issue-1"] == "blocked"
                assert db.query(Outcome).filter_by(workspace_id=workspace_id).count() == 2
            if stalled_phase == "outbound":
                operation = db.query(Operation).filter_by(workspace_id=stalled_id, kind="issue").one()
                assert operation.error == "timeout" and operation.next_attempt > utcnow()
                assert not operation.claim
                assert db.query(Operation).filter_by(workspace_id=stalled_id, kind="comment",
                                                     status="pending").count() == 1
                db.get(Outcome, linked[1]).status = "blocked"
        entered.clear()
        await worker.cycle()
        assert not entered.is_set()
        if stalled_phase == "outbound":
            await Dispatcher(provider).dispatch(stalled_id)
            assert not entered.is_set()
        retry()
        worker._retry_after.clear()
        worker.provider = lambda workspace_id: remotes[workspace_id].provider(workspace_id)
        await worker.cycle()
        with session_scope() as db:
            assert db.query(Event).filter_by(level="warn").count() == 1
            assert db.query(Outcome).filter_by(workspace_id=stalled_id).count() == 2
        assert len(remotes[stalled_id].comments) == 1

    asyncio.run(scenario())


def test_intake_releases_database_and_honors_pause_during_fetch(configured, monkeypatch):
    workspace_id, _repo_ids = configured
    monkeypatch.setenv("AIFACTORY_SIGNAL_INTERVAL_SECONDS", "180")

    async def handle(request):
        assert engine().pool.checkedout() == 0
        with session_scope() as db:
            db.get(Workspace, workspace_id).paused = True
        return httpx.Response(200, json={"data": {"issues": connection([issue()])}})

    asyncio.run(LinearSync(lambda workspace_id: Linear("fake", transport=httpx.MockTransport(handle))).cycle())
    with session_scope() as db:
        assert not db.query(Outcome).filter_by(workspace_id=workspace_id).first()
