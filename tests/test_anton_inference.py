import asyncio
import time
from types import SimpleNamespace

import pytest

from auto_mindsdb_factory.anton_inference import AntonInference, AntonStopped
from auto_mindsdb_factory.run_journal import RunJournal


def boundary(tmp_path, *, tokens=100, **options):
    config = SimpleNamespace(model="pinned", reasoning_effort="low", max_output_tokens=32, timeout_seconds=10)
    return AntonInference(config, RunJournal(tmp_path, "anton"), role="builder", max_total_tokens=tokens,
                          deadline=time.monotonic() + 30, **options)


def client(usage=None, status="completed", failure=None, delay=0, output=None):
    calls, settings = [], []
    async def create(**body):
        calls.append(body)
        if delay:
            await asyncio.sleep(delay)
        if failure:
            raise failure
        return SimpleNamespace(id="response", model="pinned", status=status,
            output=output or [],
            usage=SimpleNamespace(model_dump=lambda: usage) if usage is not None else None)
    transport = SimpleNamespace(responses=SimpleNamespace(create=create))
    def with_options(**options):
        settings.append(options)
        return transport
    return SimpleNamespace(with_options=with_options), calls, settings


USAGE = {"input_tokens": 7, "output_tokens": 3, "total_tokens": 10,
         "input_tokens_details": {"cached_tokens": 5}}
BODY = {"model": "pinned", "input": [], "max_output_tokens": 64000}


def test_actual_request_is_clamped_and_accounted_once(tmp_path):
    guard = boundary(tmp_path)
    sdk, calls, settings = client(USAGE)
    asyncio.run(guard.request(sdk, BODY))
    assert calls[0]["max_output_tokens"] == 32 and calls[0]["store"] is False
    assert calls[0]["parallel_tool_calls"] is False
    assert calls[0]["reasoning"] == {"effort": "low"}
    assert settings[0]["max_retries"] == 0
    assert guard.journal.summary()["total_tokens"] == 10
    assert guard.journal.summary()["cost_usd"] is None
    assert guard.journal.events()[-1]["data"]["usage"]["input_tokens_details"]["cached_tokens"] == 5


@pytest.mark.parametrize("usage", [None, {"total_tokens": 0}, {**USAGE, "total_tokens": True},
                                    {**USAGE, "total_tokens": 1}])
def test_unknown_usage_blocks_all_following_calls(tmp_path, usage):
    guard = boundary(tmp_path)
    sdk, calls, _ = client(usage)
    for _ in range(2):
        with pytest.raises(AntonStopped):
            asyncio.run(guard.request(sdk, BODY))
    assert len(calls) == 1 and guard.journal.summary()["usage_complete"] is False


@pytest.mark.parametrize("change", [{"model": "other"}, {"reasoning": {"effort": "high"}},
    {"stream": True}, {"tools": [{"type": "web_search"}]},
    {"tools": [{"type": "function", "name": "read_host_file"}]}])
def test_unapproved_helpers_models_and_native_tools_never_reach_provider(tmp_path, change):
    guard = boundary(tmp_path)
    sdk, calls, _ = client(USAGE)
    with pytest.raises(AntonStopped):
        asyncio.run(guard.request(sdk, {**BODY, **change}))
    assert not calls and not guard.journal.events()


def test_failed_request_cannot_be_retried_by_sdk_recovery(tmp_path):
    guard = boundary(tmp_path)
    sdk, calls, _ = client(failure=RuntimeError("provider body must not enter journal"))
    with pytest.raises(RuntimeError):
        asyncio.run(guard.request(sdk, BODY))
    with pytest.raises(AntonStopped):
        asyncio.run(guard.request(sdk, BODY))
    assert len(calls) == 1 and not guard.journal.summary()["usage_complete"]
    assert "provider body" not in str(guard.journal.events())


def test_exhaustion_and_cancellation_stop_before_another_request(tmp_path):
    guard = boundary(tmp_path, tokens=10)
    sdk, calls, _ = client(USAGE)
    with pytest.raises(AntonStopped):
        asyncio.run(guard.request(sdk, BODY))
    assert guard.journal.summary()["total_tokens"] == 10
    with pytest.raises(AntonStopped):
        asyncio.run(guard.request(sdk, BODY))
    assert len(calls) == 1
    guard.cancelled = lambda: True
    with pytest.raises(AntonStopped):
        guard.admit()


def test_request_timeout_interrupts_active_work_and_retains_unknown_usage(tmp_path):
    guard = boundary(tmp_path)
    guard.config.timeout_seconds = 0.02
    sdk, calls, _ = client(USAGE, delay=10)
    with pytest.raises(TimeoutError):
        asyncio.run(guard.request(sdk, BODY))
    assert len(calls) == 1 and not guard.journal.summary()["usage_complete"]


def test_cancel_stops_active_sdk_work(tmp_path):
    guard = boundary(tmp_path)
    started = time.monotonic()
    guard.cancelled = lambda: time.monotonic() - started > 0.02
    sdk, calls, _ = client(USAGE, delay=10)
    with pytest.raises(AntonStopped):
        asyncio.run(guard.request(sdk, BODY))
    assert time.monotonic() - started < 1
    assert len(calls) == 1 and not guard.journal.summary()["usage_complete"]


@pytest.mark.parametrize("output", [
    [SimpleNamespace(type="function_call", name="scratchpad", arguments='{"code":"read_host()"}')],
    [SimpleNamespace(type="function_call", name="run", arguments='{"command":["cat"')],
    [SimpleNamespace(type="function_call", name="run", arguments='{"command":"shell"}')],
    [SimpleNamespace(type="function_call", name="run", arguments='{"command":["true"]}')] * 2,
])
def test_malformed_or_undeclared_calls_are_rejected_before_sdk_dispatch(tmp_path, output):
    guard = boundary(tmp_path)
    sdk, calls, _ = client(USAGE, output=output)
    with pytest.raises(AntonStopped):
        asyncio.run(guard.request(sdk, BODY))
    assert len(calls) == 1 and guard.journal.summary()["total_tokens"] == 10


def test_evidence_failure_cannot_trigger_sdk_recovery(tmp_path, monkeypatch):
    guard = boundary(tmp_path)
    sdk, calls, _ = client(USAGE)
    def fail(*args, **kwargs):
        raise OSError("diagnostic write failed")
    monkeypatch.setattr(guard.journal, "append", fail)
    with pytest.raises(AntonStopped):
        asyncio.run(guard.request(sdk, BODY))
    assert not calls
