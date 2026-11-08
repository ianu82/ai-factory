"""Keyless real-child lifecycle tests; no network or candidate execution."""

import os
import sys
import time

import pytest

from auto_mindsdb_factory import mindshub_native_process as process
from test_product_inference_runtime import inputs


PROGRAM = '''import asyncio,json,os,sys,time
print(json.dumps({"kind":"ready"}),flush=True)
packet=json.load(sys.stdin)
if MODE == "request-stall": time.sleep(30)
summary={"model":"pinned","status":"completed","usage":{"input_tokens":7,"output_tokens":3,"total_tokens":10}}
print(json.dumps({"kind":"observed","summary":summary,"gateway":{"single_attempt":True}}),flush=True)
print(json.dumps({"kind":"payload","payload":summary}),flush=True)
if MODE == "cleanup-stall":
 async def stubborn():
  async with asyncio.timeout(.01):
   while True:
    try: await asyncio.sleep(30)
    except asyncio.CancelledError: continue
 asyncio.run(stubborn())
print(json.dumps({"kind":"retired"}),flush=True)
'''


@pytest.mark.parametrize("mode", ["success", "request-stall", "cleanup-stall"])
def test_owned_process_retirement_is_bounded_and_preserves_observed_usage(monkeypatch, mode):
    monkeypatch.setattr(process, "_command", lambda: [sys.executable, "-c", "MODE=" + repr(mode) + "\n" + PROGRAM])
    start = time.monotonic()
    observed, payload, complete, facts = process.exchange(
        {"api_key": "synthetic-private-pipe-value", "body": {}, "deadline": start + .35},
        deadline=start + .35, cancelled=lambda: False)
    assert time.monotonic() - start < .7 and facts["reaped"] and facts["packet_sent"]
    assert complete is (mode == "success") and facts["forced"] is (mode != "success")
    assert (observed is None) is (mode == "request-stall")
    if observed is not None:
        assert observed["summary"]["usage"]["total_tokens"] == 10
    with pytest.raises(ChildProcessError): os.waitpid(facts["pid"], os.WNOHANG)
    assert "synthetic-private-pipe-value" not in str((observed, payload, facts))


def test_cancelled_process_cannot_complete_or_remain_live(monkeypatch):
    monkeypatch.setattr(process, "_command", lambda: [sys.executable, "-c", "MODE='request-stall'\n" + PROGRAM])
    start = time.monotonic()
    _, _, complete, facts = process.exchange({"api_key": "synthetic", "body": {}, "deadline": start + 1},
        deadline=start + 1, cancelled=lambda: time.monotonic() - start > .15)
    assert not complete and facts["forced"] and facts["reaped"]
    assert time.monotonic() - start < .5


def test_expired_or_oversized_packet_never_spawns(monkeypatch):
    def denied(): raise AssertionError("must not spawn")
    monkeypatch.setattr(process, "_command", denied)
    for packet, deadline in (({}, time.monotonic() - 1), ({"data": "x" * process.MAX_BYTES}, time.monotonic() + 1)):
        with pytest.raises(RuntimeError, match="admission"):
            process.exchange(packet, deadline=deadline, cancelled=lambda: False)


def test_utf8_response_below_body_limit_stays_within_owned_pipe_limit(monkeypatch):
    program = r'''import json,sys
from auto_mindsdb_factory.mindshub_native_worker import emit
emit({"kind":"ready"})
json.load(sys.stdin)
payload={"output":"\U0001f600"*400000,"usage":{"input_tokens":7,"output_tokens":3,"total_tokens":10}}
assert len(json.dumps(payload,ensure_ascii=False).encode()) < 2*1024*1024
emit({"kind":"observed","summary":{"usage":payload["usage"]}})
emit({"kind":"payload","payload":payload})
emit({"kind":"retired"})
'''
    monkeypatch.setattr(process, "_command", lambda: [sys.executable, "-c", program])
    start = time.monotonic()
    observed, payload, complete, facts = process.exchange({}, deadline=start + 3, cancelled=lambda: False)
    assert complete and facts["reaped"] and not facts["forced"]
    assert payload["output"] == "\U0001f600" * 400000
    assert observed["summary"]["usage"]["total_tokens"] == 10


@pytest.mark.parametrize("attempt", range(3))
def test_late_spawn_is_rejected_with_truthful_retirement_without_sending_packet(monkeypatch, attempt):
    original, children = process.subprocess.Popen, []
    monkeypatch.setattr(process, "_command", lambda: [sys.executable, "-I", "-c", "import time;time.sleep(30)"])
    def late_spawn(*args, **kwargs):
        child = original(*args, **kwargs)
        children.append(child)
        time.sleep(.08)
        return child
    monkeypatch.setattr(process.subprocess, "Popen", late_spawn)
    start = time.monotonic()
    try:
        observed, payload, complete, facts = process.exchange({"api_key": "synthetic"},
            deadline=start + .02, cancelled=lambda: False)
        assert not complete and observed is payload is None
        assert facts["forced"] and not facts["packet_sent"]
        assert facts["retirement_unconfirmed"] is (not facts["reaped"])
        assert facts["process_group"] == facts["pid"]
        assert facts["spawn_late"] and facts["deadline_exceeded"]
        assert facts["deadline_overrun_ms"] >= 50 and facts["retirement_ms"] >= 0
        if facts["reaped"]:
            with pytest.raises(ChildProcessError): os.waitpid(facts["pid"], os.WNOHANG)
    finally:
        for child in children:
            if child.poll() is None: child.kill()
            child.wait(timeout=1)


@pytest.fixture
def unretirable_child(monkeypatch):
    original, children = process.subprocess.Popen, []
    monkeypatch.setattr(process, "_command", lambda: [sys.executable, "-I", "-c", "import time;time.sleep(30)"])
    def create(*args, **kwargs):
        child = original(*args, **kwargs)
        wait = child.wait
        def bounded_wait(timeout=None):
            assert timeout is not None, "Retirement must retain the original deadline"
            return wait(timeout=timeout)
        child.wait = bounded_wait
        children.append((child, wait))
        return child
    def denied(group, signal): raise OSError("private kill failure")
    monkeypatch.setattr(process.subprocess, "Popen", create)
    monkeypatch.setattr(process.os, "killpg", denied)
    yield children
    # Capture/assert production facts first; only this fixture retires its test child.
    for child, wait in children:
        if child.poll() is None: child.kill()
        wait(timeout=1)


def test_failed_kill_returns_bounded_unconfirmed_ownership(unretirable_child):
    start = time.monotonic()
    observed, payload, complete, facts = process.exchange({"api_key": "synthetic-private"},
        deadline=start + .1, cancelled=lambda: False)
    assert time.monotonic() - start < .5
    assert observed is payload is None and not complete and not facts["packet_sent"]
    assert facts["forced"] and not facts["reaped"] and facts["retirement_unconfirmed"]
    assert facts["deadline_exceeded"] and facts["process_group"] == facts["pid"]
    assert "private" not in str(facts)


@pytest.mark.parametrize("harness", ["native", "anton"])
def test_normal_callers_journal_unconfirmed_retirement_and_block_recovery(inputs, tmp_path, unretirable_child, harness):
    import asyncio
    import json
    from auto_mindsdb_factory.anton_inference import AntonInference, AntonStopped
    from auto_mindsdb_factory.connectors import FactoryConnectorError
    from auto_mindsdb_factory.mindshub_native import MindsHubNativeConnector
    from auto_mindsdb_factory.product_agent import ProductAgent, AgentSessionError, TOOLS
    from auto_mindsdb_factory.product_inference_runtime import resolve_qualification_config
    from auto_mindsdb_factory.product_roles import snapshot_roles
    from auto_mindsdb_factory.run_journal import RunJournal
    settings, options = inputs
    config = resolve_qualification_config(settings, "builder", **{**options, "timeout_seconds": 1})
    journal = RunJournal(tmp_path, "unconfirmed-" + harness)
    journal.create({"role_snapshots": snapshot_roles()})
    deadline, commands = time.monotonic() + 3, []
    if harness == "native":
        agent = ProductAgent(MindsHubNativeConnector(config, deadline=deadline), journal,
            max_turns=3, max_total_tokens=1000, deadline=deadline)
        invoke = lambda: agent.run("builder", {}, commands.append)
        stop = AgentSessionError
    else:
        gate = AntonInference(config, journal, role="builder", max_total_tokens=1000, deadline=deadline)
        invoke = lambda: asyncio.run(gate.request(None, {"model": config.model,
            "reasoning": {"effort": config.reasoning_effort}, "tools": TOOLS}))
        stop = AntonStopped
    with pytest.raises(FactoryConnectorError): invoke()
    with pytest.raises(stop): invoke()
    events = RunJournal(tmp_path, "unconfirmed-" + harness).events()
    facts = [event["data"] for event in events if event["kind"] == "inference_process"]
    assert len(facts) == len(unretirable_child) == 1 and not commands
    assert facts[0]["retirement_unconfirmed"] and not facts[0]["reaped"] and not facts[0]["packet_sent"]
    assert facts[0]["pid"] == facts[0]["process_group"]
    assert sum(event["kind"] == "model_requested" for event in events) == 1
    assert not any(event["kind"] in {"agent_tool_requested", "agent_command", "agent_report"} for event in events)
    assert not journal.summary()["usage_complete"]
    assert config.api_key not in json.dumps(events) and "private kill failure" not in json.dumps(events)
