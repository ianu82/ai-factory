"""Pinned CLI -> scoped proxy -> accounting gate -> pinned client, local upstream only."""

import asyncio
import json
import os
from contextlib import ExitStack
from pathlib import Path
import queue
import threading
import time

import pytest

from auto_mindsdb_factory.codex_inference import CodexInference
from auto_mindsdb_factory.codex_process import open_codex_process
from auto_mindsdb_factory.codex_proxy import open_codex_proxy
from auto_mindsdb_factory.connectors import OpenAIResponsesAgentConfig
from auto_mindsdb_factory.product_agent import REPORT_SCHEMA
from auto_mindsdb_factory.run_journal import RunJournal


REPORT = {"status": "unverified", "summary": "Local combined transport fixture", "findings": [],
          "criteria": [{"id": "probe", "status": "unverified", "evidence": "No live provider"}]}


@pytest.mark.parametrize("mode", ["report", "builtin", "cancel"])
def test_real_cli_proxy_and_pinned_client_are_one_guarded_path(tmp_path, mode):
    configured = os.environ.get("AI_FACTORY_CODEX_TEST_BINARY")
    binary = Path(configured or "src/auto_mindsdb_factory/codex_bridge/node_modules/.bin/codex").resolve()
    if not configured and not binary.is_file(): pytest.skip("Combined CLI/SDK transport requires the pinned CLI")
    assert binary.is_file()
    if not configured: pytest.importorskip("anton.core.llm.openai")
    from anton.core.llm.openai import openai
    import httpx2

    requests, callbacks, reader_errors, cancelled = [], [], [], [False]
    config = OpenAIResponsesAgentConfig(api_key="controller-test-only", reasoning_effort="high", max_output_tokens=10000)
    gate = CodexInference(config, RunJournal(tmp_path, "combined"), role="verifier", max_requests=1,
        max_total_tokens=50000, deadline=time.monotonic() + 15, cancelled=lambda: cancelled[0])
    async def run():
        started, drained = asyncio.Event(), asyncio.Event()
        async def upstream(request):
            requests.append(request); started.set()
            if mode == "cancel":
                try:
                    await asyncio.Event().wait()
                finally:
                    drained.set()
            code = ("text(await tools.finish(" + json.dumps(REPORT) + "))" if mode == "report"
                    else 'text(await tools.apply_patch("forbidden"))')
            return httpx2.Response(200, json={"id": "resp_combined", "object": "response", "created_at": 0,
                "status": "completed", "model": config.model, "output": [{"id": "ctc_combined", "call_id": "call_combined",
                    "type": "custom_tool_call", "name": "exec", "namespace": "functions", "input": code}],
                "usage": {"input_tokens": 7, "output_tokens": 3, "total_tokens": 10}})
        http = httpx2.AsyncClient(transport=httpx2.MockTransport(upstream))
        client = openai.AsyncOpenAI(api_key=config.api_key, base_url="https://provider.invalid/v1",
            organization="test-organization", project="test-project", http_client=http)
        try:
            async with open_codex_proxy(gate, client) as proxy:
                ownership, reader = ExitStack(), None
                try:
                    owner = ownership.enter_context(open_codex_process(proxy.base_url,
                        proxy_token=proxy.token, deadline=gate.deadline, binary=binary))
                    process, messages = owner.process, queue.Queue()
                    assert config.api_key not in str(process.args)
                    def read_messages():
                        try:
                            while True:
                                try:
                                    line = process.stdout.readline()
                                except ValueError:
                                    if process.poll() is None: raise
                                    break
                                if not line: break
                                messages.put(json.loads(line))
                        except Exception as error:
                            reader_errors.append(type(error).__name__)
                    reader = threading.Thread(target=read_messages)
                    reader.start()
                    def send(message):
                        process.stdin.write(json.dumps(message) + "\n"); process.stdin.flush()
                    send({"id": 1, "method": "initialize", "params": {"clientInfo": {
                        "name": "factory_proxy_fixture", "version": "1"}, "capabilities": {"experimentalApi": True}}})
                    terminal, settings = None, None
                    while time.monotonic() < gate.deadline:
                        if mode == "cancel" and started.is_set(): cancelled[0] = True
                        try:
                            message = messages.get_nowait()
                        except queue.Empty:
                            assert process.poll() is None
                            await asyncio.sleep(0.01); continue
                        assert "error" not in message
                        if message.get("id") == 1:
                            send({"method": "initialized", "params": {}})
                            send({"id": 2, "method": "thread/start", "params": {"model": config.model,
                                "modelProvider": "factory_guarded", "allowProviderModelFallback": False,
                                "ephemeral": True, "cwd": str(owner.work), "approvalPolicy": "never", "sandbox": "read-only",
                                "baseInstructions": "Use the supplied dynamic tool.", "dynamicTools": [{"type": "function",
                                "name": "finish", "description": "Finish fixture", "inputSchema": REPORT_SCHEMA}]}})
                        elif message.get("id") == 2:
                            send({"id": 3, "method": "turn/start", "params": {"threadId": message["result"]["thread"]["id"],
                                "model": config.model, "effort": "high", "input": [{"type": "text", "text": "Call finish."}]}})
                        elif message.get("method") == "thread/settings/updated": settings = message["params"]["threadSettings"]
                        elif message.get("method") == "item/tool/call": callbacks.append(message["params"]); break
                        elif message.get("method") == "turn/completed": terminal = message["params"]["turn"]["status"]; break
                    assert len(requests) == 1 and settings["model"] == config.model and settings["effort"] == "high"
                    assert settings["sandboxPolicy"] == {"type": "readOnly", "networkAccess": False}
                    if mode == "report":
                        assert len(callbacks) == 1
                        assert gate.take_action(callbacks[0]["tool"], callbacks[0]["arguments"]) == ("finish", REPORT)
                    else: assert not callbacks and terminal == "failed" and gate.stopped
                    if mode == "cancel": assert drained.is_set()
                    assert proxy.token not in str(requests[0].headers) + requests[0].content.decode() + json.dumps(gate.journal.events())
                finally:
                    await asyncio.to_thread(ownership.close)
                    if reader is not None: reader.join(timeout=2)
                assert reader is not None and not reader.is_alive() and not reader_errors
                assert process.poll() is not None and not owner.home.parent.exists()
        finally:
            await client.close()
        assert http.is_closed
    asyncio.run(run())
    assert requests[0].headers["authorization"] == "Bearer controller-test-only"
    assert "x-factory-session" not in requests[0].headers
    assert json.loads(requests[0].content)["stream"] is False
    assert gate.journal.summary()["usage_complete"] is (mode != "cancel")
    assert gate.journal.summary()["total_tokens"] == (0 if mode == "cancel" else 10)
    assert gate.journal.summary()["cost_usd"] is None
    assert config.api_key not in json.dumps(gate.journal.events())
