"""Budgeted control-plane judge sessions; no execution environment required."""
import asyncio
import json
from datetime import timedelta

import pytest

from aifactory import db, decision_judge_runner as runner
from aifactory.inference import InferenceError, ProviderConfig
from aifactory.models import Decision, DecisionReview, Event, Run, Workspace
from tests.fakes import ScriptedModel, call
from tests.test_decision_judges import data, report  # shared T1 record fixture


@pytest.fixture
def model(data, monkeypatch):
    with db.session_scope() as s:
        s.get(Workspace, data[0]).settings = {
            "models": {"judge": "judge-custom"}, "pricing": {"judge-custom": {"input": 1, "output": 2}},
            "budgets": {"run_usd": 1, "outcome_usd": 2, "run_tokens": 100000}}
    ScriptedModel.judge_calls = []
    ScriptedModel.calls = []
    monkeypatch.setattr(runner, "ModelClient", ScriptedModel)
    monkeypatch.setattr(runner, "resolve_provider", lambda ws: ProviderConfig("mindshub", "unused", "SECRET", "Test"))
    ScriptedModel.scripts = {"judge": lambda messages: {"content": json.dumps(report(data[-2]))}}
    return ScriptedModel


def run(data):
    return asyncio.run(runner.run_judge(data[0], data[-1]))


@pytest.mark.parametrize("verdict", ["decide", "send_back", "hold"])
def test_reports_finished_costed_no_application(data, model, verdict, monkeypatch):
    # Any accidental entry to execution infrastructure must fail the test.
    from aifactory import sandbox, gateway, harness
    def forbidden(*args, **kwargs):
        pytest.fail("judge entered execution infrastructure")
    monkeypatch.setattr(sandbox.LocalSandbox, "__init__", forbidden)
    monkeypatch.setattr(gateway, "issue", forbidden)
    monkeypatch.setattr(harness, "get_harness", forbidden)
    model.scripts["judge"] = lambda _: {"content": json.dumps({**report(data[-2]), "verdict": verdict})}
    row, result, error = run(data)
    assert result["verdict"] == verdict and not error
    assert row.role == "judge" and row.harness == "control_plane" and row.status == "succeeded"
    assert row.finished_at and row.cost_known and row.cost_usd == .0014
    assert (row.input_tokens, row.output_tokens, row.turns) == (1000, 200, 1)
    assert row.provider == "mindshub" and row.model == "judge-custom"
    assert model.calls == [] and len(model.judge_calls) == 1
    request = model.judge_calls[0]
    assert request["tools"] is None and request["model"] == "judge-custom"
    assert [m["role"] for m in request["messages"]] == ["system", "user"]
    assert "UNTRUSTED DATA" in request["messages"][0]["content"]
    assert "superseded_by" in request["messages"][0]["content"]
    assert "SECRET" not in str(row.config_snapshot)
    with db.session_scope() as s:
        assert s.get(Decision, data[-1]).status == "open"
        assert s.get(Decision, data[-1]).review is None
        assert [e.kind for e in s.query(Event).filter_by(run_id=row.id)] == ["run_started", "run_finished"]
        assert s.get(Run, row.id).result == result


@pytest.mark.parametrize("response,reason", [
    ({"content": "not json"}, "Malformed"),
    ({"content": "[]"}, "verdict"),
    ({"content": json.dumps({"verdict": "decide", "summary": "No evidence", "answer": "Guess", "resolution": "answer"})}, "citations"),
    ({"content": "{}", "tool_calls": [call("exec", {})]}, "tool/function"),
    ({"content": "{}", "function_call": {"name": "exec"}}, "tool/function"),
])
def test_invalid_output_safe_hold(data, model, response, reason):
    model.scripts["judge"] = lambda _: response
    row, result, error = run(data)
    assert result["verdict"] == "hold" and reason in error
    assert row.status == "failed" and row.finished_at and row.result == result


@pytest.mark.parametrize("field,value", [("citations", [{"decision_id": "fake", "passage": "fake"}]),
                                          ("resolution", "approve")])
def test_invalid_reference_or_option(data, model, field, value):
    model.scripts["judge"] = lambda _: {"content": json.dumps({**report(data[-2]), field: value})}
    assert run(data)[1]["verdict"] == "hold"


@pytest.mark.parametrize("failure", ["missing", "provider", "timeout", "context", "cancelled"])
def test_provider_failures_return_auditable_hold(data, model, monkeypatch, failure):
    if failure == "missing":
        def missing(ws):
            raise InferenceError("SECRET provider credential")
        monkeypatch.setattr(runner, "resolve_provider", missing)
    else:
        async def fail(self, **kwargs):
            if failure == "timeout":
                raise TimeoutError()
            if failure == "cancelled":
                raise asyncio.CancelledError()
            raise InferenceError("SECRET context rejected", status=400 if failure == "context" else 402)
        monkeypatch.setattr(model, "chat", fail)
    row, result, error = run(data)
    assert result["verdict"] == "hold" and error
    assert row.finished_at and row.status in ("failed", "cancelled")
    assert "SECRET" not in str(row.result) + row.error


@pytest.mark.parametrize("budget", ["exhausted", "tokens", "unknown_spend", "unknown_price", "off", "governance"])
def test_preflight_never_calls_model_or_opens_budget_approval(data, model, budget):
    with db.session_scope() as s:
        ws = s.get(Workspace, data[0])
        settings = dict(ws.settings)
        if budget == "exhausted":
            settings["budgets"] = {**settings["budgets"], "outcome_usd": 0}
        elif budget == "tokens":
            settings["budgets"] = {**settings["budgets"], "run_tokens": 10}
        elif budget == "unknown_price":
            settings["pricing"] = {}
        elif budget == "unknown_spend":
            s.add(Run(workspace_id=data[0], outcome_id=data[3], role="builder", harness="native", cost_known=False))
        elif budget == "off":
            settings["decision_judges"] = "off"
        else:
            s.get(Decision, data[-1]).kind = "approve_plan"
        ws.settings = settings
    row, result, error = run(data)
    assert result["verdict"] == "hold" and error and row.finished_at
    assert not model.judge_calls
    with db.session_scope() as s:
        assert not s.query(Decision).filter_by(workspace_id=data[0], kind="budget").count()


def test_remaining_budget_caps_call_and_override(data, model, monkeypatch):
    with db.session_scope() as s:
        ws = s.get(Workspace, data[0])
        ws.settings = {**ws.settings, "pricing": {"override": {"input": 0, "output": 100}},
                       "budgets": {"run_usd": 1, "outcome_usd": .3, "run_tokens": 100000}}
        s.add(Run(workspace_id=data[0], outcome_id=data[3], role="builder", harness="native", cost_usd=.2))
    monkeypatch.setattr(runner, "resolve_provider", lambda ws: ProviderConfig("openai", "unused", "SECRET", "Test", "override"))
    row, result, error = run(data)
    assert not error and row.model == "override" and row.provider == "openai"
    assert 998 <= model.judge_calls[0]["max_tokens"] <= 1000


def test_measured_cost_and_cost_lookup_failure(data, model, monkeypatch):
    async def measured(self, session):
        return .03
    monkeypatch.setattr(model, "session_cost", measured)
    row, _, _ = run(data)
    assert row.cost_usd == .03 and row.cost_known
    async def broken(self, session):
        raise RuntimeError("lookup unavailable")
    monkeypatch.setattr(model, "session_cost", broken)
    row, result, error = run(data)
    assert row.cost_usd == .0014 and row.finished_at and not error


def test_corrections_in_input_and_validation(data, model):
    with db.session_scope() as s:
        q = s.get(Decision, data[-1])
        corrected = Decision(workspace_id=data[0], outcome_id=data[3], kind="question", title="Correction",
                             status="resolved", resolution="answer", comment="Use a database.",
                             created_at=q.created_at - timedelta(hours=3), resolved_at=q.created_at - timedelta(hours=2))
        s.add(corrected)
        s.flush()
        correction_id = corrected.id
        s.add(DecisionReview(workspace_id=data[0], decision_id=data[-2], correction_decision_id=correction_id))
    row, result, error = run(data)
    assert result["verdict"] == "hold" and "corrected" in error
    ctx = json.loads(model.judge_calls[0]["messages"][1]["content"])["context"]
    assert next(r for r in ctx["records"] if r["id"] == data[-2])["superseded_by"] == correction_id
    candidate = report(correction_id)
    candidate["citations"][0]["passage"] = "Use a database."
    model.scripts["judge"] = lambda _: {"content": json.dumps(candidate)}
    assert run(data)[1]["verdict"] == "decide"


def test_deadline_interrupts_stalled_provider(data, model, monkeypatch):
    with db.session_scope() as s:
        ws = s.get(Workspace, data[0])
        ws.settings = {**ws.settings, "limits": {"run_minutes": .0001}}
    async def stalled(self, **kwargs):
        await asyncio.sleep(60)
    monkeypatch.setattr(model, "chat", stalled)
    row, result, error = run(data)
    assert result["verdict"] == "hold" and error == "Judge deadline exceeded"
    assert row.finished_at


def test_full_record_is_retained_when_too_large(data, model):
    text = "Complete record. " * 10000
    with db.session_scope() as s:
        s.get(Decision, data[-2]).comment = text
    row, result, error = run(data)
    assert result["verdict"] == "hold" and "Full judge record" in error
    assert not model.judge_calls
    assert next(r for r in row.config_snapshot["context"]["records"] if r["id"] == data[-2])["comment"] == text


def test_measured_overspend_is_accounted_and_held(data, model, monkeypatch):
    async def measured(self, session):
        return 1.5
    monkeypatch.setattr(model, "session_cost", measured)
    row, result, error = run(data)
    assert row.cost_known and row.cost_usd == 1.5
    assert row.status == "failed" and result["verdict"] == "hold"
    assert "exceeded" in error


@pytest.mark.parametrize("real_client", [False, True], ids=["scripted", "model-client"])
@pytest.mark.parametrize("failure", ["timeout", "cancelled", "transport"])
@pytest.mark.parametrize("recovery", ["measured", "unavailable", "error"])
def test_interrupted_request_cost_recovery(data, model, monkeypatch, real_client, failure, recovery):
    from aifactory.inference import ModelClient

    client_class = ModelClient if real_client else model
    monkeypatch.setattr(runner, "ModelClient", client_class)

    async def interrupted(*args, **kwargs):
        if failure == "timeout":
            raise TimeoutError()
        if failure == "cancelled":
            raise asyncio.CancelledError()
        raise InferenceError("SECRET transport failure")

    # Exercise the real client's pre-ledger transport failure as well as the fake.
    monkeypatch.setattr(client_class, "_post" if real_client else "chat", interrupted)
    lookups = []

    async def session_cost(self, session_id):
        lookups.append(session_id)
        if recovery == "error":
            raise InferenceError("SECRET lookup failure")
        return .03 if recovery == "measured" else None

    monkeypatch.setattr(client_class, "session_cost", session_cost)
    row, result, error = run(data)
    assert lookups == [row.id]
    assert result["verdict"] == "hold" and error
    assert row.status == ("cancelled" if failure == "cancelled" else "failed")
    assert row.finished_at and row.turns == 0
    assert row.cost_known == (recovery == "measured")
    assert row.cost_usd == (.03 if recovery == "measured" else 0)
    with db.session_scope() as s:
        stored = s.get(Run, row.id)
        assert stored.cost_known == row.cost_known and stored.cost_usd == row.cost_usd
        event = s.query(Event).filter_by(run_id=row.id, kind="run_finished").one()
        assert event.data["cost_known"] == row.cost_known
        assert event.data["cost_usd"] == row.cost_usd
    # Unrecovered spend must block the next bounded outcome request, not silently
    # replenish its budget. Recovered spend must reduce its remaining allowance.
    if recovery == "measured":
        with db.session_scope() as s:
            ws = s.get(Workspace, data[0])
            ws.settings = {**ws.settings, "budgets": {**ws.settings["budgets"], "outcome_usd": .03}}
    next_row, next_result, next_error = run(data)
    assert next_result["verdict"] == "hold"
    assert "budget" in next_error.lower()
    assert lookups == [row.id]  # the second run fails before attempting inference
    assert next_row.cost_known and next_row.cost_usd == 0


def test_interrupted_non_mindshub_cost_is_unknown(data, model, monkeypatch):
    monkeypatch.setattr(runner, "resolve_provider", lambda ws: ProviderConfig("openai", "unused", "SECRET", "Test"))

    async def interrupted(self, **kwargs):
        raise TimeoutError()

    async def forbidden_lookup(self, session_id):
        pytest.fail("MindsHub cost lookup used for another provider")

    monkeypatch.setattr(model, "chat", interrupted)
    monkeypatch.setattr(model, "session_cost", forbidden_lookup)
    row, result, _ = run(data)
    assert result["verdict"] == "hold" and not row.cost_known


@pytest.mark.parametrize("output_type", ["web_search_call", "computer_call", "file_search_call", "unknown_tool_call", None])
def test_real_client_non_content_output_is_held(data, model, monkeypatch, output_type):
    from aifactory.inference import ModelClient

    monkeypatch.setattr(runner, "ModelClient", ModelClient)
    requests = []

    async def post(self, path, body):
        requests.append(body)
        return {"output": [
            {"type": output_type, "status": "completed"},
            {"type": "message", "role": "assistant", "content": [
                {"type": "output_text", "text": json.dumps(report(data[-2]))}]}],
            "usage": {"input_tokens": 1000, "output_tokens": 200}}

    async def session_cost(self, session_id):
        return .03

    monkeypatch.setattr(ModelClient, "_post", post)
    monkeypatch.setattr(ModelClient, "session_cost", session_cost)
    row, result, error = run(data)
    assert row.status == "failed" and result["verdict"] == "hold"
    assert "non-content" in error
    assert row.cost_known and row.cost_usd == .03
    assert len(requests) == 1 and "tools" not in requests[0]


@pytest.mark.parametrize("threaded", [False, True], ids=["same-loop", "separate-loops"])
def test_concurrent_judges_cannot_share_remaining_budget(data, model, monkeypatch, threaded):
    import threading

    with db.session_scope() as s:
        ws = s.get(Workspace, data[0])
        ws.settings = {**ws.settings, "budgets": {**ws.settings["budgets"], "run_usd": .1, "outcome_usd": .1}}
    recovering = threading.Event()
    release = threading.Event()
    calls = []

    async def chat(self, **kwargs):
        calls.append(kwargs)
        kwargs["ledger"].add(kwargs["model"], (1000, 200))
        return {"content": json.dumps(report(data[-2]))}

    async def session_cost(self, session_id):
        # Admission must cover measured-cost recovery, not only the model call.
        recovering.set()
        await asyncio.to_thread(release.wait, 5)
        return .06

    monkeypatch.setattr(model, "chat", chat)
    monkeypatch.setattr(model, "session_cost", session_cost)

    async def concurrent():
        first = asyncio.create_task(asyncio.to_thread(run, data) if threaded else runner.run_judge(data[0], data[-1]))
        try:
            assert await asyncio.to_thread(recovering.wait, 5)
            second = await runner.run_judge(data[0], data[-1])
            assert second[1]["verdict"] == "hold" and "reserved" in second[2]
            assert second[0].finished_at and second[0].cost_known and second[0].cost_usd == 0
        finally:
            release.set()
        return await first

    first = asyncio.run(concurrent())
    assert first[0].status == "succeeded" and first[1]["verdict"] == "decide"
    assert len(calls) == 1
    assert runner.outcome_cost(data[3]) == (.06, True)
    # Once finalized, a subsequent review uses only the unspent allowance.
    next_row, next_result, next_error = run(data)
    assert next_result["verdict"] == "hold" and "exceeded" in next_error
    assert next_row.config_snapshot["limits"]["run_usd"] == pytest.approx(.04)


def test_simultaneous_budget_admission_is_atomic(data, model, monkeypatch):
    import threading

    barrier = threading.Barrier(2)
    release = threading.Event()
    admit = runner._admit_budget
    calls = []

    def simultaneous_admit(*args):
        barrier.wait(timeout=5)
        return admit(*args)

    async def chat(self, **kwargs):
        calls.append(kwargs)
        kwargs["ledger"].add(kwargs["model"], (1000, 200))
        return {"content": json.dumps(report(data[-2]))}

    async def session_cost(self, session_id):
        await asyncio.to_thread(release.wait, 10)
        return .06

    with db.session_scope() as s:
        ws = s.get(Workspace, data[0])
        ws.settings = {**ws.settings, "budgets": {**ws.settings["budgets"], "run_usd": .1, "outcome_usd": .1}}
    monkeypatch.setattr(runner, "_admit_budget", simultaneous_admit)
    monkeypatch.setattr(model, "chat", chat)
    monkeypatch.setattr(model, "session_cost", session_cost)

    async def race():
        tasks = [asyncio.create_task(asyncio.to_thread(run, data)) for _ in range(2)]
        try:
            done, _ = await asyncio.wait(tasks, timeout=5, return_when=asyncio.FIRST_COMPLETED)
            assert len(done) == 1
            assert next(iter(done)).result()[1]["verdict"] == "hold"
        finally:
            release.set()
        return await asyncio.gather(*tasks)

    results = asyncio.run(race())
    assert sorted(r[1]["verdict"] for r in results) == ["decide", "hold"]
    assert len(calls) == 1 and runner.outcome_cost(data[3]) == (.06, True)
