"""Remote persistence survives dispatcher instances and lost acknowledgements."""

import asyncio
import json
from datetime import timedelta
from uuid import UUID

import httpx
import pytest

from aifactory.db import engine, session_scope, utcnow
from aifactory.linear import Linear
from aifactory.linear_outbox import Dispatcher, enqueue_comment, enqueue_state
from aifactory.models import Decision, Event, LinearOperation as Operation, Outcome, Plan, Run, Secret, Signal, Task
from aifactory.security import encrypt, token_hash
from .test_linear_intake import configured


class Remote(httpx.MockTransport):
    def __init__(self):
        self.states, self.comments, self.effects, self.requests = {}, {}, [], []
        self.failure, self.loss, self.state_error = None, None, None
        super().__init__(self.handle)

    def provider(self, workspace_id):
        return Linear("lin_api_testsecret", transport=self)

    def handle(self, request):
        assert engine().pool.checkedout() == 0
        payload = json.loads(request.content)
        query, variables = payload["query"], payload["variables"]
        self.requests.append(payload)
        if self.failure:
            if isinstance(self.failure, int):
                return httpx.Response(self.failure)
            raise self.failure
        if "issue0:" in query:
            data = {alias: {"state": {"id": self.states.get(issue, "queued"), "name": "State"}}
                    for alias, issue in variables.items()}
            errors = []
            for alias, issue in variables.items():
                if issue == self.state_error:
                    data[alias] = None
                    errors.append({"path": [alias], "message": "secret response"})
            return httpx.Response(200, json={"data": data, "errors": errors})
        if "issueUpdate" in query:
            issue, target = variables["id"], variables["input"]["stateId"]
            self.states[issue] = target
            self.effects.append((issue, target))
            result = {"issueUpdate": {"success": True}}
        elif "commentCreate" in query:
            value = variables["input"]
            assert UUID(value["id"]).version == 4
            assert value["id"] not in self.comments
            with session_scope() as db:
                assert db.query(Operation).filter_by(remote_id=value["id"], status="pending").one()
            self.comments[value["id"]] = value
            result = {"commentCreate": {"success": True, "comment": {"id": value["id"], "body": value["body"]}}}
        else:
            result = {"issue": {"comments": {"nodes": [
                {"id": identity, "body": value["body"]} for identity, value in self.comments.items()
                if value["issueId"] == variables["id"]], "pageInfo": {"hasNextPage": False}}}}
        if self.loss and self.loss in query:
            self.loss = None
            raise httpx.ReadTimeout("lin_api_testsecret")
        return httpx.Response(200, json={"data": result})


@pytest.fixture
def work(configured):
    workspace_id, repos = configured
    with session_scope() as db:
        outcome = Outcome(workspace_id=workspace_id, number=1, title="Test", status="running")
        db.add(outcome)
        db.flush()
        outcome_id = outcome.id
    return workspace_id, outcome_id, "issue-1"


def dispatch(remote):
    return asyncio.run(Dispatcher(remote.provider).dispatch())


def retry():
    with session_scope() as db:
        db.query(Operation).update({"next_attempt": utcnow() - timedelta(seconds=1)})


@pytest.mark.parametrize("kind", ["plan", "pr", "decision", "completion"])
def test_semantic_comments(work, kind):
    remote = Remote()
    with session_scope() as db:
        db.add(Secret(workspace_id=work[0], kind="feedback_key", ciphertext=token_hash("feedback")))
        if kind == "decision":
            db.add(Decision(id="semantic-identity", workspace_id=work[0], outcome_id=work[1],
                            kind="question", title="Help"))
        if kind == "completion":
            db.get(Outcome, work[1]).status = "completed"
    first = enqueue_comment(*work, kind, "semantic-identity", "Body")
    assert enqueue_comment(*work, kind, "semantic-identity", "Changed event") == first
    assert dispatch(remote) == dispatch(remote) == []
    assert len(remote.comments) == 1
    with session_scope() as db:
        assert db.query(Operation).filter_by(kind="comment").count() == 1


@pytest.mark.parametrize("status", ["resolved", "dismissed", "superseded"])
def test_closed_decisions_are_not_captured(work, status):
    remote = Remote()
    with session_scope() as db:
        db.add(Signal(workspace_id=work[0], source="linear_issue", external_id=work[2],
                      fingerprint="test", title="Test", outcome_id=work[1]))
        db.add(Decision(workspace_id=work[0], outcome_id=work[1], kind="question", title="Help", status=status))
    assert dispatch(remote) == []
    assert not remote.comments
    with session_scope() as db:
        assert db.query(Operation).filter_by(kind="comment").count() == 0


@pytest.mark.parametrize("status", ["resolved", "dismissed", "superseded"])
@pytest.mark.parametrize("legacy", [False, True])
@pytest.mark.parametrize("during_lookup", [False, True])
def test_decision_closed_before_delivery_is_superseded(work, status, legacy, during_lookup):
    remote = Remote()
    with session_scope() as db:
        decision = Decision(workspace_id=work[0], outcome_id=work[1], kind="question", title="Help")
        db.add(decision)
        db.flush()
        decision_id = decision.id
    operation_id = enqueue_comment(*work, "decision", decision_id, "A person is needed")
    if legacy:
        with session_scope() as db:
            db.get(Operation, operation_id).payload = {"body": "A person is needed"}
    if not during_lookup:
        with session_scope() as db:
            db.get(Decision, decision_id).status = status

    class Closing(Linear):
        async def comments(self, issue_id):
            comments = await super().comments(issue_id)
            if during_lookup:
                with session_scope() as db:
                    db.get(Decision, decision_id).status = status
            return comments

    assert asyncio.run(Dispatcher(lambda _: Closing("key", transport=remote)).dispatch()) == []
    assert not remote.comments
    with session_scope() as db:
        assert db.get(Operation, operation_id).status == "superseded"
    retry()
    assert dispatch(remote) == []
    assert not remote.comments


@pytest.mark.parametrize("failure", ["response", "ack", "crash"])
def test_comment_survives_lost_ack(work, failure, monkeypatch):
    remote = Remote()
    enqueue_comment(*work, "plan", "plan-1", "Body")
    if failure == "response":
        remote.loss = "commentCreate"
    else:
        def fail_ack(self, operation_id):
            raise RuntimeError("lin_api_testsecret") if failure == "ack" else SystemExit("crash")
        monkeypatch.setattr(Dispatcher, "_ack", fail_ack)
    if failure == "crash":
        with pytest.raises(SystemExit):
            dispatch(remote)
    else:
        assert dispatch(remote)
    monkeypatch.undo()
    retry()
    assert dispatch(remote) == []
    assert len(remote.comments) == 1
    with session_scope() as db:
        assert db.query(Operation).filter_by(kind="comment").one().status == "sent"


def test_states_coalesce_and_reconcile_late_request(work):
    remote = Remote()
    enqueue_state(*work, "building")
    remote.loss = "issueUpdate"
    assert dispatch(remote) == ["timeout"]
    retry()
    assert dispatch(remote) == []
    assert remote.effects == [(work[2], "building")]
    enqueue_state(*work, "verifying")
    enqueue_state(*work, "done")
    assert dispatch(remote) == []
    remote.states[work[2]] = "building"
    retry()
    assert dispatch(remote) == []
    assert remote.effects == [(work[2], "building"), (work[2], "done"), (work[2], "done")]
    with session_scope() as db:
        assert db.query(Operation).filter_by(kind="state", status="superseded").count() == 1
        assert db.query(Event).filter_by(kind="linear_state_reconciled").count() == 2


@pytest.mark.parametrize("failure,category", [(httpx.ConnectTimeout("secret"), "timeout"), (401, "authentication"),
                                               (429, "rate_limit")])
def test_errors_pending_sanitized_and_retry(work, failure, category):
    remote = Remote()
    with session_scope() as db:
        db.add(Secret(workspace_id=work[0], kind="linear_api_key", ciphertext=encrypt("arbitrary-secret")))
        db.add(Secret(workspace_id=work[0], kind="feedback_key", ciphertext=token_hash("feedback")))
    enqueue_comment(*work, "plan", "plan", "arbitrary-secret lin_api_testsecret")
    enqueue_state(*work, "building")
    remote.failure = failure
    assert category in dispatch(remote)
    with session_scope() as db:
        assert db.query(Operation).filter_by(kind="comment").one().status == "pending"
        stored = json.dumps([operation.payload for operation in db.query(Operation)])
        assert "arbitrary-secret" not in stored and "lin_api_testsecret" not in stored
        assert all(operation.error in {"", category} for operation in db.query(Operation))
    if category == "rate_limit":
        count = len(remote.requests)
        for _cycle in range(20):
            dispatch(remote)
        assert len(remote.requests) == count
    remote.failure = None
    retry()
    enqueue_state(*work, "done")
    assert dispatch(remote) == []
    assert remote.states[work[2]] == "done" and len(remote.comments) == 1


def test_partial_states_do_not_starve_comments_or_other_issues(work):
    remote = Remote()
    other = (*work[:2], "issue-2")
    for link in (work, other):
        enqueue_state(*link, "done")
        enqueue_comment(*link, "plan", "plan", "Body")
    remote.state_error = work[2]
    assert dispatch(remote) == ["graphql"]
    assert len(remote.comments) == 2
    assert remote.states[other[2]] == "done"


def test_idle_issue_request_budget(work, monkeypatch):
    from aifactory import linear_outbox

    remote, now = Remote(), utcnow()
    monkeypatch.setattr(linear_outbox, "utcnow", lambda: now)
    enqueue_state(*work, "done")
    for _cycle in range(1200):
        dispatch(remote)
        now += timedelta(seconds=3)
    assert len(remote.requests) < 12
    assert len(remote.effects) == 1


def test_concurrent_dispatch_serialized(work):
    remote = Remote()
    enqueue_state(*work, "done")
    enqueue_comment(*work, "plan", "plan", "Body")
    started, release = asyncio.Event(), asyncio.Event()

    async def run():
        class Paused(Linear):
            async def issue_states(self, ids):
                started.set()
                await release.wait()
                return await super().issue_states(ids)
        first = asyncio.create_task(Dispatcher(lambda _: Paused("key", transport=remote)).dispatch())
        await started.wait()
        assert await Dispatcher(remote.provider).dispatch() == []
        release.set()
        assert await first == []
    asyncio.run(run())
    assert len(remote.comments) == len(remote.effects) == 1


def test_crashed_claim_recovers_after_expiry(work):
    remote = Remote()
    enqueue_comment(*work, "plan", "plan", "Body")
    with session_scope() as db:
        issue = db.query(Operation).filter_by(kind="issue").one()
        issue.claim, issue.lease_until = "crashed-worker", utcnow() + timedelta(seconds=120)
    assert dispatch(remote) == [] and not remote.requests
    with session_scope() as db:
        db.query(Operation).filter_by(kind="issue").one().lease_until = utcnow() - timedelta(seconds=1)
    assert dispatch(remote) == [] and len(remote.comments) == 1


@pytest.mark.parametrize("stage,status,outcome_status,expected", [
    ("build", "running", "running", "building"), ("verify", "running", "running", "verifying"),
    ("ci", "running", "running", "merging"), ("merge", "running", "running", "merging"),
    ("build", "needs_input", "running", "blocked"), ("", "pending", "needs_input", "blocked"),
    ("", "pending", "planning", "queued"), ("done", "completed", "completed", "done")])
def test_lifecycle_reads_factory_not_queue(work, configured, stage, status, outcome_status, expected):
    remote = Remote()
    enqueue_state(*work, "obsolete-state")
    with session_scope() as db:
        outcome = db.get(Outcome, work[1])
        signal = Signal(workspace_id=work[0], source="linear_issue", external_id=work[2],
                        fingerprint="test", title="Test", outcome_id=work[1])
        db.add(signal)
        db.flush()
        outcome.signal_id, outcome.status = signal.id, outcome_status
        plan = Plan(outcome_id=outcome.id)
        db.add(plan)
        db.flush()
        db.add(Task(workspace_id=work[0], outcome_id=work[1], plan_id=plan.id, repo_id=configured[1][0],
                    key="T1", title="Task", status=status, stage=stage))
    assert dispatch(remote) == []
    assert remote.states.get(work[2], "queued") == expected
    assert all(target != "obsolete-state" for issue, target in remote.effects)


def test_factory_completion_feedback_key_and_disable(work, configured):
    from aifactory.models import Workspace
    from aifactory.pipeline.orchestrator import Orchestrator

    remote = Remote()
    with session_scope() as db:
        signal = Signal(workspace_id=work[0], source="linear_issue", external_id=work[2],
                        fingerprint="test", title="Test", outcome_id=work[1])
        db.add(signal)
        db.flush()
        outcome = db.get(Outcome, work[1])
        outcome.signal_id = signal.id
        plan = Plan(outcome_id=outcome.id)
        db.add(plan)
        db.flush()
        db.add(Task(workspace_id=work[0], outcome_id=work[1], plan_id=plan.id, repo_id=configured[1][0],
                    key="T1", title="Task", status="running", pr_url="https://github.com/org/repo/pull/1"))
        db.add(Decision(workspace_id=work[0], outcome_id=work[1], kind="question", title="Help"))
        db.add(Run(workspace_id=work[0], outcome_id=work[1], role="builder", harness="native", cost_usd=1.23))
    assert dispatch(remote) == []
    assert remote.states[work[2]] == "blocked" and len(remote.comments) == 3
    with session_scope() as db:
        db.add(Secret(workspace_id=work[0], kind="feedback_key", ciphertext=token_hash("feedback")))
        db.query(Task).filter_by(outcome_id=work[1]).one().status = "completed"
        db.query(Decision).filter_by(outcome_id=work[1]).one().status = "resolved"
    Orchestrator()._promote()
    assert dispatch(remote) == []
    assert remote.states[work[2]] == "done" and len(remote.comments) == 4
    assert any("$1.23" in comment["body"] for comment in remote.comments.values())
    with session_scope() as db:
        workspace = db.get(Workspace, work[0])
        workspace.settings = {**workspace.settings, "linear": {**workspace.settings["linear"], "enabled": False}}
    enqueue_comment(*work, "decision", "disabled", "Do not send")
    retry()
    remote.states[work[2]] = "building"
    count = len(remote.requests)
    assert dispatch(remote) == []
    assert remote.states[work[2]] == "building" and len(remote.requests) == count


@pytest.mark.parametrize("pricing_arrives", [True, False])
def test_completion_waits_for_final_cost_across_restarts(work, monkeypatch, pricing_arrives):
    from aifactory import linear_outbox

    remote = Remote()
    with session_scope() as db:
        signal = Signal(workspace_id=work[0], source="linear_issue", external_id=work[2],
                        fingerprint="test", title="Test", outcome_id=work[1])
        db.add(signal)
        db.flush()
        outcome = db.get(Outcome, work[1])
        outcome.signal_id, outcome.status = signal.id, "completed"
        db.add(Run(workspace_id=work[0], outcome_id=work[1], role="builder", harness="native",
                   cost_usd=0.01, cost_known=False))
        db.add(Run(workspace_id=work[0], outcome_id=work[1], role="planner", harness="native",
                   cost_usd=1.23, cost_known=True))
    assert dispatch(remote) == []
    assert remote.states[work[2]] == "done" and not remote.comments
    now = utcnow()
    monkeypatch.setattr(linear_outbox, "utcnow", lambda: now)
    with session_scope() as db:
        assert db.query(Operation).filter_by(kind="comment").count() == 0
    now += timedelta(minutes=59)
    assert dispatch(remote) == [] and not remote.comments
    if pricing_arrives:
        with session_scope() as db:
            run = db.query(Run).filter_by(outcome_id=work[1], cost_known=False).one()
            run.cost_usd, run.cost_known = 4.56, True
    else:
        now += timedelta(minutes=1)
    remote.loss = "commentCreate"
    assert dispatch(remote) == ["timeout"]
    assert len(remote.comments) == 1
    body = next(iter(remote.comments.values()))["body"]
    assert body == ("Outcome delivered. Total cost: $5.79" if pricing_arrives
                    else "Outcome delivered. Total cost: unknown (cost still being reconciled).")
    now += timedelta(minutes=1)
    assert dispatch(remote) == []
    assert len(remote.comments) == 1
    with session_scope() as db:
        for run in db.query(Run).filter_by(outcome_id=work[1]):
            run.cost_usd, run.cost_known = 10.0, True
    assert dispatch(remote) == []
    assert len(remote.comments) == 1 and next(iter(remote.comments.values()))["body"] == body


def test_disabled_pending_work_resumes_only_after_reenable(work):
    from aifactory.models import Workspace

    remote = Remote()
    enqueue_state(*work, "building")
    enqueue_comment(*work, "plan", "plan", "Body")
    with session_scope() as db:
        workspace = db.get(Workspace, work[0])
        settings = workspace.settings
        workspace.settings = {**settings, "linear": {**settings["linear"], "enabled": False}}
        db.add(Secret(workspace_id=work[0], kind="feedback_key", ciphertext=token_hash("feedback")))
    for _cycle in range(3):
        assert dispatch(remote) == []
    assert not remote.requests
    with session_scope() as db:
        assert db.query(Operation).filter_by(status="pending").count() == 2
        db.get(Workspace, work[0]).settings = settings
    assert dispatch(remote) == []
    assert remote.effects == [(work[2], "building")] and len(remote.comments) == 1
