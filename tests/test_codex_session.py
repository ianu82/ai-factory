"""Owned session with the actual CLI/client and local mock inference only."""

import asyncio
from contextlib import contextmanager
import json
import os
from pathlib import Path
import threading
import time

import pytest

from auto_mindsdb_factory import codex_session
from auto_mindsdb_factory.codex_rpc import ACTION_INSTRUCTIONS
from auto_mindsdb_factory.product_agent import AgentSessionError, HarnessStopped
from auto_mindsdb_factory.connectors import OpenAIResponsesAgentConfig
from auto_mindsdb_factory.product_roles import snapshot_roles
from auto_mindsdb_factory.product_execution import codex_execution_snapshot
from auto_mindsdb_factory.run_journal import RunJournal


@pytest.mark.parametrize("mode", [
    "ok", "builder", "frozen", "slow", "slow-unknown-usage", "no-investigation", "missing", "findings",
    "provider-cancel", "command-cancel", "command-deadline", "metadata-actor", "metadata-other", "provider-envelope",
])
def test_owned_session_acceptance_and_cleanup(tmp_path, monkeypatch, mode):
    configured = os.environ.get("AI_FACTORY_CODEX_TEST_BINARY")
    binary = Path(configured or "src/auto_mindsdb_factory/codex_bridge/node_modules/.bin/codex").resolve()
    if not configured and not binary.is_file(): pytest.skip("Pinned CLI is optional outside dedicated CI")
    if not configured: pytest.importorskip("openai")
    import openai
    import httpx2

    requests, clients, owners, commands, drained = [], [], [], [], []
    aborted = threading.Event()
    config = OpenAIResponsesAgentConfig(api_key="controller-test-only", reasoning_effort="high", max_output_tokens=10000)
    frozen_model, role = config.model, "builder" if mode == "builder" else "verifier"
    journal = RunJournal(tmp_path, "session"); journal.create({"role_snapshots": snapshot_roles(),
        "execution_snapshots": {role: codex_execution_snapshot(config) for role in ("builder", "verifier")}})
    report = {"status": "passed", "summary": "Checked", "findings": [],
              "criteria": [{"id": "a", "status": "passed", "evidence": "Executed the job"}]}
    if mode == "missing": report["criteria"] = []
    if mode == "findings": report["findings"] = ["Blocking defect"]
    async def upstream(request):
        requests.append(json.loads(request.content))
        texts = [part["text"] for item in requests[-1]["input"] if item["type"] == "message"
                 for part in item["content"] if part["type"] == "input_text"]
        frozen = snapshot_roles()[role]["instructions"]
        assert any(frozen + "\n\n" + ACTION_INSTRUCTIONS in text for text in texts)
        if mode.startswith("slow"):
            # Cross several real RPC polling intervals with an unmatched request.
            await asyncio.sleep(0.2)
        if mode == "provider-cancel":
            try: await asyncio.Event().wait()
            finally: drained.append(True)
        finish = len(requests) == 2 or mode == "no-investigation"
        code = ("text(await tools.finish(" + json.dumps(report) + "))" if finish
                else 'text(await tools.run({"command":["inspect"]}))')
        rejected_metadata = ({"created_by": "private actor"} if mode == "metadata-actor" else
                             {"private field": "private value", "created_by": None} if mode == "metadata-other" else {})
        envelope = ({"metadata": {"turn_id": "turn_provider"},
            "internal_chat_message_metadata_passthrough": {"turn_id": "turn_provider", "create_time": 123.5},
            "status": "completed"} if mode == "provider-envelope" else {"namespace": "functions"})
        return httpx2.Response(200, json={"id": "resp_session", "status": "completed", "model": frozen_model,
            "output": [{"id": "ctc_session", "call_id": "call_session", "type": "custom_tool_call",
                "name": "exec", "input": code, **envelope, **rejected_metadata}],
            "usage": None if mode == "slow-unknown-usage" else {
                "input_tokens": 7, "output_tokens": 3, "total_tokens": 10}})
    actual_client, actual_process = openai.AsyncOpenAI, codex_session.open_codex_process
    def client(**kwargs):
        assert kwargs["max_retries"] == 0 and kwargs["api_key"] == config.api_key
        instance = actual_client(**kwargs, http_client=httpx2.AsyncClient(transport=httpx2.MockTransport(upstream)))
        clients.append(instance); return instance
    @contextmanager
    def process(*args, **kwargs):
        with actual_process(*args, **kwargs, binary=binary) as owner:
            owners.append(owner)
            assert config.api_key not in str(owner.process.args)
            yield owner
    monkeypatch.setattr(openai, "AsyncOpenAI", client)
    monkeypatch.setattr(codex_session, "open_codex_process", process)
    class Command:
        def __call__(self, arguments):
            commands.append(arguments)
            if mode == "frozen":
                config.model = "changed"; context["acceptance_criteria"].clear()
            if mode in {"command-cancel", "command-deadline"}: assert aborted.wait(10)
            return {"command": arguments, "exit_code": 0, "stdout": "Checked", "stderr": ""}
        def abort(self): aborted.set()
    cancelled = lambda: (bool(requests) if mode == "provider-cancel" else bool(commands) if mode == "command-cancel" else False)
    agent = codex_session.CodexAgent(config, journal, max_turns=3, max_total_tokens=50000,
        deadline=time.monotonic() + (5 if mode == "command-deadline" else 15), cancelled=cancelled)
    context = {"acceptance_criteria": [{"id": "a", "description": "Check the source"}]}
    accepted = mode in {"ok", "builder", "frozen", "slow", "provider-envelope"}
    if accepted: assert agent.run(role, context, Command()) == report
    else:
        with pytest.raises(HarnessStopped if mode.endswith(("cancel", "deadline")) else AgentSessionError):
            agent.run(role, context, Command())
    assert len(owners) == 1 and owners[0].process.poll() is not None and not owners[0].home.parent.exists()
    assert len(clients) == 1 and clients[0].is_closed()
    assert drained == ([True] if mode == "provider-cancel" else [])
    assert aborted.is_set() is (mode in {"command-cancel", "command-deadline"})
    assert len(requests) == (1 if mode.startswith("metadata-") or mode in {
        "no-investigation", "provider-cancel", "command-cancel", "command-deadline", "slow-unknown-usage"} else 2)
    events = journal.events()
    assert any(e["kind"] == "agent_report" for e in events) is accepted
    assert any(e["kind"] == "harness_closed" for e in events)
    assert f"You are the AI Factory {role}." in json.dumps(requests[0])
    assert all(request["model"] == frozen_model for request in requests)
    assert config.api_key not in json.dumps(events)
    assert journal.summary()["usage_complete"] is (mode not in {"provider-cancel", "slow-unknown-usage"})
    if mode.startswith("metadata-"):
        assert commands == [] and journal.summary()["total_tokens"] == 10
        stops = [e["data"] for e in events if e["kind"] == "harness_policy_stopped"]
        assert stops == [{"role": "verifier", "harness": "codex", "reason": "response_policy",
            "response_rejection": "action_metadata", "response_rejection_detail":
                "fields_created_by" if mode == "metadata-actor" else "fields_other_two"}]
        assert "private actor" not in json.dumps(events) and "private field" not in json.dumps(events)
    if mode == "slow":
        assert commands == [["inspect"]] and journal.summary()["total_tokens"] == 20
        assert not any(e["kind"] == "harness_policy_stopped" for e in events)
    if mode == "slow-unknown-usage":
        assert commands == []
        responses = [e["data"] for e in events if e["kind"] == "model_response"]
        assert len(responses) == 1 and responses[0]["usage"] is None
        assert any(e["kind"] == "harness_policy_stopped" and e["data"]["reason"] == "usage_unknown_or_exhausted"
                   for e in events)


def test_unsupported_command_and_dependency_drift_fail_before_start(tmp_path, monkeypatch):
    config = OpenAIResponsesAgentConfig(api_key="test")
    journal = RunJournal(tmp_path, "preflight"); journal.create({"role_snapshots": snapshot_roles(),
        "execution_snapshots": {"builder": codex_execution_snapshot(config)}})
    agent = codex_session.CodexAgent(config, journal, max_turns=3, max_total_tokens=500, deadline=time.monotonic()+5)
    with pytest.raises(AgentSessionError, match="abortable"): agent.run("builder", {}, lambda args: {})
    class Command:
        def __call__(self, args): raise AssertionError("No execution")
        def abort(self): raise AssertionError("No cleanup needed")
    monkeypatch.setattr(codex_session, "distribution", lambda _: type("Package", (), {"version": "unreviewed"})())
    with pytest.raises(AgentSessionError, match="pinned"): agent.run("builder", {}, Command())
    assert not journal.events()


def test_unreviewed_sdk_environment_is_rejected(monkeypatch):
    monkeypatch.setenv("OPENAI_CUSTOM_HEADERS", "test-only")
    with pytest.raises(AgentSessionError, match="environment overrides"):
        codex_session.validate_codex_dependencies()


def test_repeated_task_cancellation_waits_for_owned_cleanup():
    async def run():
        done = []
        async def close():
            await asyncio.sleep(0.06); done.append(True)
        task = asyncio.create_task(codex_session._drain(close()))
        await asyncio.sleep(0.01); task.cancel()
        await asyncio.sleep(0.01); task.cancel()
        with pytest.raises(asyncio.CancelledError): await task
        assert done == [True]
    asyncio.run(run())
