import asyncio
import copy
import json
import time
from types import SimpleNamespace

import pytest

from auto_mindsdb_factory.codex_inference import CodexInference, CodexStopped
from auto_mindsdb_factory.connectors import OpenAIResponsesAgentConfig
from auto_mindsdb_factory.run_journal import RunJournal


MODEL = "gpt-6.1-terra"
BODY = {"model": MODEL, "stream": True, "store": False, "tool_choice": "auto", "parallel_tool_calls": False,
        "reasoning": {"effort": "high"}, "include": ["reasoning.encrypted_content"], "input": [
            {"type": "additional_tools", "tools": [{"type": "namespace", "name": "functions", "tools": [
                {"type": "custom", "name": "exec", "format": {"type": "grammar", "syntax": "lark", "definition": "start: /.+/"}}]}]}]}
RESPONSE = {"id": "resp_test", "model": MODEL, "status": "completed", "output": [
    {"id": "ctc_test", "call_id": "call_test", "type": "custom_tool_call", "name": "exec", "namespace": "functions",
     "input": 'text(await tools.run({"command":["true"]}))'}],
    "usage": {"input_tokens": 7, "output_tokens": 3, "total_tokens": 10, "private": "never journal"}}


def boundary(tmp_path, **options):
    return CodexInference(OpenAIResponsesAgentConfig(model=MODEL, reasoning_effort="high", max_output_tokens=32),
        RunJournal(tmp_path, "codex"), role="builder", **{"max_requests": 2, "max_total_tokens": 100,
            "deadline": time.monotonic() + 10, **options})


def client(response=RESPONSE, failure=None, waiting=False):
    calls, settings, drained = [], [], []
    async def post(path, **kwargs):
        calls.append((path, kwargs))
        if waiting:
            try:
                await asyncio.Event().wait()
            finally:
                drained.append(True)
        if failure:
            raise failure
        return copy.deepcopy(response)
    def with_options(**kwargs):
        settings.append(kwargs)
        return SimpleNamespace(post=post)
    return SimpleNamespace(with_options=with_options), calls, settings, drained


def test_accounts_actual_body_and_preserves_untrusted_context(tmp_path):
    gate = boundary(tmp_path)
    sdk, calls, settings, _ = client()
    before = copy.deepcopy(BODY)
    stream = asyncio.run(gate.request(sdk, BODY))
    assert type(stream) is bytes and b"response.completed" in stream
    path, kwargs = calls[0]
    assert path == "/responses" and kwargs["cast_to"] is dict
    assert kwargs["body"] == {**BODY, "stream": False, "max_output_tokens": 32} and BODY == before
    assert settings[0]["max_retries"] == 0 and settings[0]["timeout"] <= 10
    assert gate.journal.summary()["total_tokens"] == 10 and gate.journal.summary()["usage_complete"]
    assert gate.journal.summary()["cost_usd"] is None
    assert "never journal" not in json.dumps(gate.journal.events())


def test_counts_other_roles_and_requests_before_this_adapter(tmp_path):
    gate = boundary(tmp_path, max_total_tokens=27)
    gate.journal.append("model_requested", role="planner")
    gate.journal.append("model_response", role="planner", usage={"total_tokens": 10})
    sdk, calls, _, _ = client()
    asyncio.run(gate.request(sdk, BODY))
    assert calls[0][1]["body"]["max_output_tokens"] == 17
    assert gate.journal.summary()["total_tokens"] == 20


@pytest.mark.parametrize("change", [{"model": "other"}, {"stream": False}, {"max_output_tokens": 33},
                                    {"tools": [{"type": "web_search"}]}])
def test_policy_rejection_never_calls_provider_or_retries(tmp_path, change):
    gate = boundary(tmp_path)
    sdk, calls, _, _ = client()
    for body in [{**BODY, **change}, BODY]:
        with pytest.raises(CodexStopped): asyncio.run(gate.request(sdk, body))
    assert not calls


@pytest.mark.parametrize("change", [{"usage": None}, {"usage": {"total_tokens": 10}},
    {"usage": {"input_tokens": 7, "output_tokens": True, "total_tokens": 8}},
    {"model": "other"}, {"output": []}, {"status": "incomplete"}])
def test_unknown_usage_or_invalid_action_returns_no_stream_and_stops(tmp_path, change):
    gate = boundary(tmp_path)
    sdk, calls, _, _ = client({**RESPONSE, **change})
    for _ in range(2):
        with pytest.raises(CodexStopped): asyncio.run(gate.request(sdk, BODY))
    assert len(calls) == 1
    assert gate.journal.summary()["usage_complete"] is ("usage" not in change)


@pytest.mark.parametrize("mode", ["cancel", "deadline", "provider-error"])
def test_active_failure_drains_and_cannot_admit_recovery(tmp_path, mode):
    gate = boundary(tmp_path)
    sdk, calls, _, drained = client(waiting=mode != "provider-error", failure=RuntimeError("private provider body"))
    if mode == "cancel": gate.cancelled = lambda: bool(calls)
    if mode == "deadline": gate.deadline = time.monotonic() + 0.03
    started = time.monotonic()
    with pytest.raises(CodexStopped): asyncio.run(gate.request(sdk, BODY))
    assert time.monotonic() - started < 1 and drained == ([] if mode == "provider-error" else [True])
    with pytest.raises(CodexStopped): asyncio.run(gate.request(sdk, BODY))
    assert len(calls) == 1 and not gate.journal.summary()["usage_complete"]
    assert "private provider body" not in json.dumps(gate.journal.events())


def test_request_limit_and_evidence_failure_stop_before_provider(tmp_path, monkeypatch):
    gate = boundary(tmp_path, max_requests=1)
    sdk, calls, _, _ = client()
    asyncio.run(gate.request(sdk, BODY))
    with pytest.raises(CodexStopped): asyncio.run(gate.request(sdk, BODY))
    assert len(calls) == 1
    gate = boundary(tmp_path / "failure")
    monkeypatch.setattr(gate.journal, "append", lambda *a, **k: (_ for _ in ()).throw(OSError("private evidence")))
    with pytest.raises(CodexStopped): asyncio.run(gate.request(sdk, BODY))
    assert len(calls) == 1


def test_stop_reason_cannot_disclose_provider_text(tmp_path):
    gate = boundary(tmp_path)
    with pytest.raises(CodexStopped): gate.stop("private provider body")
    assert gate.journal.events()[-1]["data"]["reason"] == "unknown"


def test_exhausted_total_is_recorded_but_releases_no_executable_bytes(tmp_path):
    gate = boundary(tmp_path, max_total_tokens=10)
    sdk, calls, _, _ = client()
    with pytest.raises(CodexStopped): asyncio.run(gate.request(sdk, BODY))
    assert len(calls) == 1 and gate.journal.summary()["total_tokens"] == 10


def test_overlapping_helper_cannot_escape_accounting(tmp_path):
    gate = boundary(tmp_path)
    sdk, calls, _, drained = client(waiting=True)
    async def run():
        first = asyncio.create_task(gate.request(sdk, BODY))
        while not calls: await asyncio.sleep(0)
        with pytest.raises(CodexStopped): await gate.request(sdk, BODY)
        with pytest.raises(CodexStopped): await first
    asyncio.run(run())
    assert len(calls) == 1 and drained == [True]


def test_two_actions_require_consumption_and_completed_tool(tmp_path):
    gate = boundary(tmp_path)
    sdk, calls, _, _ = client()
    for _ in range(2):
        asyncio.run(gate.request(sdk, BODY))
        supplied = {"command": ["true"]}
        name, approved = gate.take_action("run", supplied)
        supplied["command"][0] = "changed"
        assert (name, approved) == ("run", {"command": ["true"]})
        gate.action_finished()
    assert len(calls) == 2 and gate.journal.summary()["total_tokens"] == 20 and not gate.stopped


@pytest.mark.parametrize("phase", ["pending", "executing"])
def test_helper_request_cannot_overtake_an_approved_action(tmp_path, phase):
    gate = boundary(tmp_path)
    sdk, calls, _, _ = client()
    asyncio.run(gate.request(sdk, BODY))
    if phase == "executing": gate.take_action("run", {"command": ["true"]})
    with pytest.raises(CodexStopped): asyncio.run(gate.request(sdk, BODY))
    assert len(calls) == 1 and gate.stopped


@pytest.mark.parametrize("change", ["tool", "arguments", "duplicate", "unsolicited", "nonfinite", "bool"])
def test_callback_must_match_once_without_argument_aliases(tmp_path, change):
    gate = boundary(tmp_path)
    response = copy.deepcopy(RESPONSE)
    sdk, calls, _, _ = client(response)
    args = {"command": ["true"]}
    if change != "unsolicited": asyncio.run(gate.request(sdk, BODY))
    if change == "duplicate": gate.take_action("run", args); gate.action_finished()
    if change == "arguments": args["command"] = ["false"]
    if change == "nonfinite": args["command"] = [float("nan")]
    if change == "bool": args["command"] = [True]
    with pytest.raises(CodexStopped): gate.take_action("finish" if change == "tool" else "run", args)
    assert gate.stopped and len(calls) == (0 if change == "unsolicited" else 1)
    with pytest.raises(CodexStopped): asyncio.run(gate.request(sdk, BODY))


def test_unsolicited_tool_completion_and_cancelled_callback_stop(tmp_path):
    gate = boundary(tmp_path)
    with pytest.raises(CodexStopped): gate.action_finished()
    gate = boundary(tmp_path / "cancel")
    sdk, calls, _, _ = client()
    asyncio.run(gate.request(sdk, BODY))
    gate.cancelled = lambda: True
    with pytest.raises(CodexStopped): gate.take_action("run", {"command": ["true"]})
    assert len(calls) == 1 and gate.stopped


@pytest.mark.parametrize("cached,cost", [(5, 10.0), (8, None)])
def test_explicit_cost_requires_valid_observed_cache_coverage(tmp_path, cached, cost):
    gate = boundary(tmp_path, pricing={MODEL: {"input": 1000000, "cached_input": 1000000, "output": 1000000, "max_input_tokens": 100}})
    response = copy.deepcopy(RESPONSE)
    response["usage"]["input_tokens_details"] = {"cached_tokens": cached, "private": "excluded"}
    sdk, _, _, _ = client(response)
    asyncio.run(gate.request(sdk, BODY))
    assert gate.journal.summary()["cost_usd"] == cost
    assert "excluded" not in json.dumps(gate.journal.events())
