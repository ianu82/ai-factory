"""Operational journal contracts at real boundaries, using existing fake harnesses."""
import asyncio
import json

import httpx
import pytest

from aifactory import gateway, telemetry
from aifactory.db import session_scope, utcnow
from aifactory.inference import InferenceError, ModelClient, ProviderConfig
from aifactory.models import Event, Run, Task, Workspace
from aifactory.pipeline.delivery import TaskRunner
from aifactory.pipeline.lifecycle import transition
from aifactory.pipeline.orchestrator import Orchestrator
from .test_inference_limits import fake_http, grant_for, request
from .test_pipeline import factory, new_outcome, drive, outcome_status
from aifactory.pipeline.planning import plan_outcome


def plan(oid):
    asyncio.run(plan_outcome(oid, asyncio.Event()))


def events(kind):
    with session_scope() as s:
        return s.query(Event).filter(Event.kind == kind).order_by(Event.id).all()


def run_for(factory):
    oid = new_outcome(factory)
    with session_scope() as s:
        run = Run(workspace_id=factory["ws"], outcome_id=oid, role="builder", harness="native")
        s.add(run)
        s.flush()
        return oid, run.id


@pytest.mark.parametrize("responses,categories", [
    ([200], [None]), ([429, 200], ["rate_limit", None]),
    (["transport"], ["transport"]), ([204], ["request"]), ([307], ["request"]),
])
async def test_native_attempts(factory, monkeypatch, responses, categories):
    _, rid = run_for(factory)
    provider = ProviderConfig("openai", "https://test/v1", "secret-provider-key", "Test")
    clock = [0.0]
    monkeypatch.setattr(telemetry.time, "monotonic", lambda: clock[0])

    async def sleep(_):
        clock[0] += 1000
    monkeypatch.setattr(asyncio, "sleep", sleep)
    remaining = list(responses)

    def handler(req):
        clock[0] += 0.25
        status = remaining.pop(0)
        if status == "transport":
            raise httpx.ConnectError("secret response text", request=req)
        return httpx.Response(status, json={"output": [], "error": "secret response text"})

    fake_http(monkeypatch, handler)
    client = ModelClient(provider, session_id=rid, max_retries=len(responses) - 1)
    try:
        await client._post("/responses", {"model": "test-model", "input": "private prompt"})
    except InferenceError:
        assert categories[-1] is not None
    rows = events("provider_attempt")
    assert [e.data["error_category"] for e in rows] == categories
    assert all(e.data["elapsed_seconds"] == .25 for e in rows)
    assert all(e.run_id == rid and e.workspace_id == factory["ws"] for e in rows)
    assert all(e.data["source"] == "native" for e in rows)
    assert not any(word in json.dumps([e.data for e in rows]) for word in
                   ("secret-provider-key", "secret response text", "private prompt"))


@pytest.mark.parametrize("mode,category", [
    ("completed", None), ("error", "stream_error"), ("eof", "incomplete"),
    ("cancel", "cancelled"), ("transport", "transport"),
])
async def test_gateway_stream_attempt(factory, monkeypatch, mode, category):
    _, rid = run_for(factory)
    provider = ProviderConfig("openai", "https://test/v1", "secret-key", "Test")
    grant = grant_for(provider, monkeypatch)
    grant.run_id, grant.workspace_id = rid, factory["ws"]

    class Stream(httpx.AsyncByteStream):
        async def __aiter__(self):
            yield b'data: {"type":"response.created"}\n\n'
            if mode == "completed":
                yield b'data: {"type":"response.completed","response":{"usage":{"input_tokens":3,"output_tokens":2}}}\n\n'
            elif mode == "error":
                yield b'data: {"type":"response.failed","error":{"message":"private provider text"}}\n\n'
            elif mode == "transport":
                raise httpx.ReadError("private provider text")

    fake_http(monkeypatch, lambda req: httpx.Response(200, stream=Stream()))
    response = await gateway.proxy("responses", request({"stream": True, "input": "private prompt"}))
    if mode == "cancel":
        await anext(response.body_iterator)
        await response.body_iterator.aclose()
    else:
        try:
            async for _ in response.body_iterator:
                pass
        except httpx.ReadError:
            assert mode == "transport"
    rows = events("provider_attempt")
    assert len(rows) == 1
    data = rows[0].data
    assert data["error_category"] == category
    assert data["success"] == (category is None)
    assert data["provider_failure"] == (category not in (None, "cancelled"))
    assert rows[0].run_id == rid and data["source"] == "gateway"
    assert not any(x in json.dumps(data) for x in ("secret-key", "private prompt", "private provider text"))


def test_queue_stage_restart_and_run_accounting(factory):
    oid = new_outcome(factory)
    plan(oid)
    Orchestrator()._promote()
    with session_scope() as s:
        task = s.query(Task).filter(Task.outcome_id == oid).one()
        tid = task.id
        assert transition(s, task, status="running", attempt=1, stage="build")
        stale = dict(task.evidence)
        assert transition(s, task, stage="checks")
        check_id = task.evidence["telemetry"]["stage_id"]
        assert transition(s, task, evidence=stale, stage="verify")
        assert task.evidence["telemetry"]["stage_id"] != check_id
        for role in ("planner", "builder", "judge"):
            s.add(Run(workspace_id=factory["ws"], outcome_id=oid, task_id=tid if role == "builder" else None,
                      role=role, harness="native", config_snapshot={"budget_admitted": True}))
    assert events("stage_transition")[-1].data["previous_stage_id"] == check_id
    original_queue = events("task_queued")[-1].data["queue_id"]
    original_attempt = events("attempt_started")[-1].data["attempt_id"]
    orch = Orchestrator()
    orch._recover()
    orch._recover()
    interrupted = events("run_interrupted")
    assert len(interrupted) == 3
    assert {e.data["role"] for e in interrupted} == {"planner", "builder", "judge"}
    assert len({e.run_id for e in interrupted}) == 3
    builder = next(e for e in interrupted if e.data["role"] == "builder")
    assert builder.data["queue_id"] == original_queue
    assert builder.data["attempt_id"] == original_attempt
    assert len(events("attempt_interrupted")) == 1
    assert events("attempt_interrupted")[0].data["attempt_id"] == original_attempt
    assert len(events("task_queued")) == 2
    with session_scope() as s:
        judge = s.query(Run).filter(Run.role == "judge").one()
        assert judge.status == "interrupted" and judge.finished_at == judge.started_at
        assert judge.cost_known is False
        task = s.get(Task, tid)
        assert transition(s, task, status="running")
    assert len(events("task_started")) == 2
    queues = [e.data["queue_id"] for e in events("task_queued")]
    assert len(set(queues)) == 2
    assert [e.data["queue_id"] for e in events("task_started")] == queues
    assert len(events("attempt_started")) == 2


def test_runner_updates_preserve_latest_stage_identity(factory):
    oid = new_outcome(factory)
    plan(oid)
    Orchestrator()._promote()
    with session_scope() as s:
        task = s.query(Task).filter_by(outcome_id=oid).one()
        tid = task.id
        assert transition(s, task, status="running", attempt=1, stage="build")
    runner = TaskRunner(tid, asyncio.Event(), lambda _: asyncio.Lock())
    runner._load()
    stale = dict(runner.task.evidence)
    runner.update(stage="checks")
    checks_id = runner.task.evidence["telemetry"]["stage_id"]
    runner.update(stage="verify", evidence={**stale, "ci": {"state": "success"}})
    verify_id = runner.task.evidence["telemetry"]["stage_id"]
    runner.update(stage="ci", evidence=runner.task.evidence)
    rows = events("stage_transition")
    assert rows[-2].data["previous_stage_id"] == checks_id
    assert rows[-1].data["previous_stage_id"] == verify_id
    with session_scope() as s:
        from aifactory.serializers import task as task_view

        task = s.get(Task, tid)
        assert task.evidence["telemetry"] == runner.task.evidence["telemetry"]
        assert task_view(task)["evidence"] == {"ci": {"state": "success"}}


def test_pipeline_quality_and_activity_filter(factory, client):
    oid = new_outcome(factory)
    assert drive(lambda: outcome_status(oid) in ("completed", "attention", "blocked"))
    assert events("verified")[-1].data["rejection_categories"] == []
    assert events("ci_result")[-1].data["head"]
    assert events("smoke_result")[-1].data["ref"]
    assert events("task_terminal")[-1].data["status"] == "completed"
    # Authenticate as the existing fake owner, then verify filtering precedes the limit.
    client.post("/auth/dev", json={"email": "dev@acme.test", "name": "Dev"})
    from aifactory.events import record
    from aifactory.conversation import MAX_EVENTS
    with session_scope() as s:
        for _ in range(MAX_EVENTS + 1):
            record(factory["ws"], "execution_error", outcome_id=oid,
                   data={"error_cause": "unknown"}, session=s, publish=False)
    response = client.get(f"/api/workspaces/{factory['ws']}/outcomes/{oid}")
    assert response.status_code == 200, response.text
    activity = response.json()["events"]
    assert activity and not any(e["kind"] in telemetry.METRIC_ONLY for e in activity)
    assert any(e["kind"] == "plan_proposed" for e in activity)
    response = client.get(f"/api/workspaces/{factory['ws']}/outcomes/{oid}/conversation")
    assert response.status_code == 200, response.text
    steps = [step for item in response.json()["items"] for step in item.get("steps", [])]
    assert steps and all(step["event"] not in telemetry.METRIC_ONLY for step in steps)
    assert any(step["event"] == "plan_proposed" for step in steps)


def test_event_fixture_is_deterministic():
    from .event_fixture import event_fixture
    rows = ((0, "task_queued", {"queue_id": "q1"}), (10, "task_started", {"queue_id": "q1"}))
    assert event_fixture(*rows) == event_fixture(*rows)
    assert event_fixture(*rows)[1]["created_at"] == "2026-01-01T00:00:10Z"


async def test_pause_requeue_has_one_new_episode(factory, monkeypatch):
    oid = new_outcome(factory)
    await plan_outcome(oid, asyncio.Event())
    orch = Orchestrator()
    orch._promote()
    with session_scope() as s:
        tid = s.query(Task).filter_by(outcome_id=oid).one().id

    async def paused_run(self):
        with session_scope() as s:
            s.get(Workspace, factory['ws']).paused = True
    monkeypatch.setattr(TaskRunner, 'run', paused_run)
    orch._spawn_task(tid)
    await orch._tasks[tid][0]
    with session_scope() as s:
        task = s.get(Task, tid)
        assert task.status == 'queued'
        s.get(Workspace, factory['ws']).paused = False
    async def finished_run(self):
        with session_scope() as s:
            assert transition(s, s.get(Task, tid), status='completed', stage='done')
    monkeypatch.setattr(TaskRunner, 'run', finished_run)
    orch._spawn_task(tid)
    await orch._tasks[tid][0]
    queued, started = events('task_queued'), events('task_started')
    assert len(queued) == len(started) == 2
    assert queued[0].data['queue_id'] != queued[1].data['queue_id']
    assert [e.data['queue_id'] for e in queued] == [e.data['queue_id'] for e in started]


@pytest.mark.parametrize('role', ['build', 'qa'])
@pytest.mark.parametrize('result', ['hit', 'miss', 'failed'])
async def test_vm_boot_measurements(factory, monkeypatch, role, result):
    from types import SimpleNamespace
    from aifactory import vm
    from aifactory.gitops import Patch
    import aifactory.pipeline.delivery as delivery

    oid = new_outcome(factory)
    await plan_outcome(oid, asyncio.Event())
    with session_scope() as s:
        tid = s.query(Task).filter_by(outcome_id=oid).one().id
    runner = TaskRunner(tid, asyncio.Event(), lambda _: asyncio.Lock())
    runner._load()
    runner.cfg = {**runner.cfg, 'setup': ['true'], 'build': {'isolation': 'vm'}}
    clock = [0.0]
    monkeypatch.setattr(delivery.time, 'monotonic', lambda: clock[0])
    monkeypatch.setattr(vm, 'vm_available', lambda: (True, ''))
    monkeypatch.setattr(vm, 'environment_key', lambda *args: 'prepared-key')
    async def boot(*args, **kwargs):
        clock[0] += 3.5
        if result == 'failed':
            raise RuntimeError('boot failed')
        return SimpleNamespace(vm_id='vm-1', region='test', prepared='prepared-key' if result == 'hit' else None)
    monkeypatch.setattr(vm.RailwayVMSandbox, 'from_local', boot)
    async def noop(*args, **kwargs):
        pass
    monkeypatch.setattr(delivery, 'checkout', noop)
    monkeypatch.setattr(delivery, 'apply_in_sandbox', noop)
    if role == 'build':
        local = SimpleNamespace(work=None, close=noop)
        await runner._isolate_builder(local)
    else:
        await runner._verifier_sandbox('token', Patch(b'', [], 0, 0), 1, 0, {'isolation': 'vm'})
    row = events(f'{role}_vm_unavailable' if result == 'failed' else f'{role}_vm_ready')[-1]
    assert row.data['elapsed_boot_seconds'] == 3.5
    assert row.data['prepared_eligible'] is True
    assert row.data['prepared_hit'] == (result == 'hit')
    assert row.data['success'] == (result != 'failed')


def test_decision_retry_and_reconciliation_queue_episodes(factory):
    from aifactory.decisions import resolve
    from aifactory.models import Decision, User
    from aifactory.reconcile import _handle_merged

    oid = new_outcome(factory)
    plan(oid)
    orch = Orchestrator()
    orch._promote()
    with session_scope() as s:
        task = s.query(Task).filter_by(outcome_id=oid).one()
        tid = task.id
        task.status, task.stage = 'awaiting_approval', 'merge'
        decision = Decision(workspace_id=factory['ws'], outcome_id=oid, task_id=tid,
                            kind='approve_merge', title='Merge?', body='', options=[{'value': 'reject'}])
        s.add(decision)
        s.flush()
        resolve(s, decision, s.get(User, factory['user']), 'reject', reusable=False)
    assert len(events('review_revision')) == 1
    assert events('decision_resolved')[-1].data['queue_id'] == events('task_queued')[-1].data['queue_id']
    with session_scope() as s:
        task = s.get(Task, tid)
        task.status, task.pr_number = 'awaiting_approval', 12
    pr = {'number': 12, 'merged': True, 'head': {'sha': 'head-12'}, 'merge_commit_sha': 'merged-12'}
    assert _handle_merged(tid, pr)
    assert not _handle_merged(tid, pr)
    queues = events('task_queued')
    assert len(queues) == 3 and len({e.data['queue_id'] for e in queues}) == 3
    assert events('merged_outside')[-1].data['queue_id'] == queues[-1].data['queue_id']


async def test_delayed_run_pricing_and_structured_failure(factory, monkeypatch):
    from aifactory import inference
    oid, rid = run_for(factory)
    with session_scope() as s:
        run = s.get(Run, rid)
        run.provider, run.model = 'mindshub', 'test-model'
        run.status, run.finished_at = 'failed', utcnow()
        run.cost_usd, run.cost_known = .2, False
    monkeypatch.setattr(inference, 'resolve_provider', lambda _: ProviderConfig('mindshub', 'https://test', 'key', 'Test'))
    async def failed(*args):
        raise InferenceError('private provider text')
    monkeypatch.setattr(inference.ModelClient, 'session_cost', failed)
    await Orchestrator()._maybe_reconcile_costs()
    error = events('cost_reconciliation_error')[-1]
    assert error.data['error_cause'] == 'provider'
    assert 'private provider text' not in json.dumps(error.data)
    async def priced(*args):
        return .35
    monkeypatch.setattr(inference.ModelClient, 'session_cost', priced)
    await Orchestrator()._maybe_reconcile_costs()
    adjustment = events('cost_adjusted')[-1]
    assert adjustment.run_id == rid and adjustment.outcome_id == oid
    assert adjustment.data['old_cost_usd'] == .2 and adjustment.data['cost_usd'] == .35
    assert adjustment.data['old_cost_known'] is False and adjustment.data['cost_known'] is True
    await Orchestrator()._maybe_reconcile_costs()
    assert len(events('cost_adjusted')) == 1


async def test_gateway_retry_is_one_measurement_per_send(factory, monkeypatch):
    _, rid = run_for(factory)
    provider = ProviderConfig('openai', 'https://test/v1', 'secret-key', 'Test')
    grant = grant_for(provider, monkeypatch)
    grant.run_id, grant.workspace_id = rid, factory['ws']
    statuses = iter([429, 200])
    clock = [0.0]
    monkeypatch.setattr(telemetry.time, 'monotonic', lambda: clock[0])
    async def sleep(_):
        clock[0] += 1000
    monkeypatch.setattr(asyncio, 'sleep', sleep)
    def handler(req):
        clock[0] += .5
        return httpx.Response(next(statuses), json={'output': []})
    fake_http(monkeypatch, handler)
    assert (await gateway.proxy('responses', request({}))).status_code == 200
    rows = events('provider_attempt')
    assert len(rows) == 2
    assert [r.data['error_category'] for r in rows] == ['rate_limit', None]
    assert [r.data['elapsed_seconds'] for r in rows] == [.5, .5]


async def test_stream_cancelled_before_iterator_starts(factory, monkeypatch):
    _, rid = run_for(factory)
    provider = ProviderConfig('openai', 'https://test/v1', 'secret-key', 'Test')
    grant = grant_for(provider, monkeypatch)
    grant.run_id, grant.workspace_id = rid, factory['ws']
    fake_http(monkeypatch, lambda req: httpx.Response(200, content=b''))
    response = await gateway.proxy('responses', request({'stream': True}))
    async def send(_):
        raise asyncio.CancelledError()
    async def receive():
        return {'type': 'http.disconnect'}
    with pytest.raises(asyncio.CancelledError):
        await response({'type': 'http', 'asgi': {'spec_version': '2.4'}}, receive, send)
    rows = events('provider_attempt')
    assert len(rows) == 1 and rows[0].data['error_category'] == 'cancelled'
    assert rows[0].data['provider_failure'] is False


def test_error_causes_are_type_based():
    from aifactory.inference import BudgetExceeded
    from aifactory.gitops import GitError
    from aifactory.github import GitHubError
    from aifactory.sandbox import SandboxError

    for error, cause in [(BudgetExceeded('secret'), 'budget'), (InferenceError('secret'), 'provider'),
                         (SandboxError('secret'), 'sandbox_vm'), (GitError('secret'), 'git'),
                         (GitHubError('secret'), 'github_api'), (RuntimeError('secret'), 'unknown')]:
        assert telemetry.error_cause(error) == cause


def test_recovery_only_measures_interruption_after_reclaiming_a_lease(factory, monkeypatch):
    from datetime import datetime, timedelta

    from aifactory.pipeline import orchestrator as scheduler
    from .test_pipeline import leased_task

    clock = [datetime(2026, 1, 1, 12)]
    monkeypatch.setattr(scheduler, "utcnow", lambda: clock[0])
    tid = leased_task(factory, lease_owner="another-instance",
                      lease_expires_at=clock[0] + timedelta(seconds=60))
    with session_scope() as s:
        task = s.get(Task, tid)
        original = dict(task.evidence["telemetry"])
        run = Run(workspace_id=task.workspace_id, outcome_id=task.outcome_id,
                  task_id=tid, role="builder", harness="native")
        s.add(run)
        s.flush()
        rid = run.id
    orch = Orchestrator()
    orch._recover()
    assert not events("run_interrupted") and not events("attempt_interrupted")
    with session_scope() as s:
        assert s.get(Run, rid).status == "running"
        assert s.get(Task, tid).evidence["telemetry"] == original
    clock[0] += timedelta(seconds=61)
    assert orch._reclaim() == [tid]
    orch._recover()
    assert len(events("run_interrupted")) == len(events("attempt_interrupted")) == 1
    assert events("run_interrupted")[0].data["attempt_id"] == original["attempt_id"]
    assert events("attempt_interrupted")[0].data["stage_id"] == original["stage_id"]
    assert len(events("task_queued")) == 1
    with session_scope() as s:
        task = s.get(Task, tid)
        assert transition(s, task, status="running")
        assert task.evidence["telemetry"]["attempt_id"] != original["attempt_id"]
    assert events("task_started")[0].data["queue_id"] == events("task_queued")[0].data["queue_id"]


@pytest.mark.parametrize("ok,exit_code", [(True, 0), (False, 1), (False, None)])
async def test_smoke_result_is_durable_before_evidence(factory, monkeypatch, ok, exit_code):
    """A crash before the separate evidence commit cannot hide a smoke failure."""
    from aifactory.pipeline import delivery

    oid = new_outcome(factory)
    await plan_outcome(oid, asyncio.Event())
    with session_scope() as s:
        task = s.query(Task).filter_by(outcome_id=oid).one()
        tid = task.id
        task.merge_sha = "merged-head"
    runner = TaskRunner(tid, asyncio.Event(), lambda _: asyncio.Lock())
    runner._load()
    runner.cfg = {**runner.cfg, "smoke": ["check"], "setup": []}

    async def noop(*args, **kwargs):
        pass

    async def commands(*args, **kwargs):
        return [{"command": "check", "ok": ok, "exit_code": exit_code, "duration": 0.25}]

    def interrupted(*args, **kwargs):
        raise RuntimeError("simulated stop before evidence commit")

    monkeypatch.setattr(delivery, "checkout", noop)
    monkeypatch.setattr(runner, "_run_commands", commands)
    monkeypatch.setattr(runner, "evidence", interrupted)
    with pytest.raises(RuntimeError, match="simulated stop"):
        await runner.smoke()
    row, = events("smoke_result")
    assert row.task_id == tid and row.outcome_id == oid
    assert row.data["ok"] is ok
    assert row.data["state"] == ("passed" if ok else "failed")
    assert row.data["exit_code"] == exit_code
    assert row.data["checks_total"] == 1
    assert row.data["checks_failed"] == int(not ok)
    assert row.data["ref"] == "merged-head" and row.data["smoke_id"]
    with session_scope() as s:
        assert "smoke" not in s.get(Task, tid).evidence
