"""Real pinned CLI tool-result round trip through the production session path."""

import asyncio
from contextlib import ExitStack
import json
import os
from pathlib import Path
import subprocess
import sys
import time
from types import SimpleNamespace

import pytest

from auto_mindsdb_factory.codex_inference import CodexInference, CodexStopped
from auto_mindsdb_factory.codex_process import open_codex_process
from auto_mindsdb_factory.codex_proxy import open_codex_proxy
from auto_mindsdb_factory.codex_rpc import codex_turn
from auto_mindsdb_factory.connectors import OpenAIResponsesAgentConfig
from auto_mindsdb_factory.run_journal import RunJournal


@pytest.mark.parametrize("exit_code", [0, 1])
def test_real_cli_receives_job_result_then_finishes(tmp_path, exit_code):
    configured = os.environ.get("AI_FACTORY_CODEX_TEST_BINARY")
    binary = Path(configured or "src/auto_mindsdb_factory/codex_bridge/node_modules/.bin/codex").resolve()
    if not configured and not binary.is_file(): pytest.skip("Pinned CLI is optional outside dedicated CI")
    if not configured: pytest.importorskip("anton.core.llm.openai")
    from anton.core.llm.openai import openai
    import httpx2

    config = OpenAIResponsesAgentConfig(api_key="controller-test-only", reasoning_effort="high", max_output_tokens=10000)
    gate = CodexInference(config, RunJournal(tmp_path, "rpc"), role="verifier", max_requests=2,
        max_total_tokens=50000, deadline=time.monotonic() + 15)
    report = {"status": "unverified", "summary": "Local RPC round trip", "findings": [],
              "criteria": [{"id": "probe", "status": "unverified", "evidence": "No live provider"}]}
    requests, commands = [], []
    async def run():
        async def upstream(request):
            requests.append(json.loads(request.content))
            number = len(requests)
            assert number <= 2
            if number == 2:
                history = json.dumps(requests[-1]["input"])
                assert "custom_tool_call_output" in history and "job-result-marker" in history
                assert str(exit_code) in history
            code = ('text(await tools.run({"command":["test-job"]}))' if number == 1
                    else "text(await tools.finish(" + json.dumps(report) + "))")
            return httpx2.Response(200, json={"id": f"resp_rpc_{number}", "object": "response", "created_at": 0,
                "status": "completed", "model": config.model, "output": [{"id": f"ctc_rpc_{number}",
                    "call_id": f"call_rpc_{number}", "type": "custom_tool_call", "name": "exec", "namespace": "functions", "input": code}],
                "usage": {"input_tokens": 7, "output_tokens": 3, "total_tokens": 10}})
        async def command(arguments):
            commands.append(arguments)
            return {"exit_code": exit_code, "stdout": "job-result-marker", "stderr": "", "duration_seconds": 0}
        http = httpx2.AsyncClient(transport=httpx2.MockTransport(upstream))
        client = openai.AsyncOpenAI(api_key=config.api_key, base_url="https://provider.invalid/v1", http_client=http)
        try:
            async with open_codex_proxy(gate, client) as proxy:
                ownership = ExitStack()
                try:
                    owner = ownership.enter_context(open_codex_process(proxy.base_url, proxy_token=proxy.token,
                        deadline=gate.deadline, binary=binary))
                    assert await codex_turn(owner, gate, instructions="Use only the supplied tools.",
                        context={"task": "Run the job and report honestly."}, command=command) == report
                finally:
                    await asyncio.to_thread(ownership.close)
                assert owner.process.poll() is not None and not owner.home.parent.exists()
        finally:
            await client.close()
        assert http.is_closed
    asyncio.run(run())
    assert commands == [["test-job"]] and len(requests) == 2
    assert gate.journal.summary()["total_tokens"] == 20 and gate.journal.summary()["usage_complete"]
    assert not gate.stopped and gate.journal.summary()["cost_usd"] is None


@pytest.mark.parametrize("fault", ["thread", "turn", "boolean-id", "numeric-network", "model", "approval", "duplicate-json", "eof"])
def test_rpc_boundary_rejects_before_dispatch(tmp_path, fault):
    config = OpenAIResponsesAgentConfig(reasoning_effort="high")
    gate = CodexInference(config, RunJournal(tmp_path, "bad-rpc"), role="verifier", max_requests=2,
        max_total_tokens=100, deadline=time.monotonic() + 5)
    called = []
    gate.take_action = lambda *args: called.append(args)
    settings = {"model": config.model, "effort": "high", "sandboxPolicy": {"type": "readOnly", "networkAccess": False}}
    callback = {"id": 10, "method": "item/tool/call", "params": {"threadId": "thread", "turnId": "turn",
        "callId": "call", "tool": "finish", "arguments": {}}}
    if fault == "thread": callback["params"]["threadId"] = "unowned"
    if fault == "turn": callback["params"]["turnId"] = "unowned"
    if fault == "boolean-id": callback["id"] = True
    if fault == "numeric-network": settings["sandboxPolicy"]["networkAccess"] = 0
    if fault == "model": settings["model"] = "fallback"
    if fault == "approval": callback["method"] = "item/commandExecution/requestApproval"
    packets = [json.dumps({"method": "thread/settings/updated", "params": {"threadId": "thread", "threadSettings": settings}}),
               json.dumps(callback)]
    if fault == "duplicate-json": packets[-1] = '{"method":"notification","method":"item/tool/call"}'
    if fault == "eof": packets = []
    script = '''import json,sys
def send(value): print(json.dumps(value),flush=True)
sys.stdin.readline();send({"id":1,"result":{}})
sys.stdin.readline();sys.stdin.readline();send({"id":2,"result":{"thread":{"id":"thread"}}})
sys.stdin.readline();send({"id":3,"result":{"turn":{"id":"turn"}}})
for packet in PACKETS: print(packet,flush=True)
'''.replace("PACKETS", repr(packets))
    process = subprocess.Popen([sys.executable, "-c", script], stdin=subprocess.PIPE, stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL, text=True, env={"HOME": str(tmp_path), "PATH": os.defpath})
    owner = SimpleNamespace(process=process, work=tmp_path)
    async def run():
        with pytest.raises(CodexStopped):
            await codex_turn(owner, gate, instructions="Frozen instructions", context={}, command=None)
    try:
        asyncio.run(run())
    finally:
        process.wait(timeout=2)
        process.stdin.close(); process.stdout.close()
    assert not called and gate.stopped
    assert gate.journal.events()[-1]["data"]["reason"] == "rpc_policy"
