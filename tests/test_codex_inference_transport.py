"""Actual pinned OpenAI client serialization/cancellation, with no provider calls."""

import asyncio
import json
import time

import pytest

from auto_mindsdb_factory.codex_inference import CodexInference, CodexStopped
from auto_mindsdb_factory.connectors import OpenAIResponsesAgentConfig
from auto_mindsdb_factory.run_journal import RunJournal


@pytest.mark.parametrize("mode", ["success", "unsafe-action", "unknown-usage", "provider-error", "cancel", "deadline"])
def test_actual_client_request_accounting_and_transport_drain(tmp_path, mode):
    pytest.importorskip("anton.core.llm.openai")
    from anton.core.llm.openai import openai
    import httpx2

    requests, drained = [], []
    async def respond(request):
        requests.append(request)
        if mode in {"cancel", "deadline"}:
            try:
                await asyncio.Event().wait()
            finally:
                drained.append(True)
        if mode == "provider-error":
            return httpx2.Response(503, json={"error": {"message": "private upstream text"}})
        program = ('text(await tools.run({"command":["true"]}))' if mode != "unsafe-action"
                   else 'text(await tools.apply_patch("forbidden"))')
        return httpx2.Response(200, json={"id": "resp_actual", "model": "gpt-6.1-terra", "status": "completed",
            "output": [{"id": "ctc_actual", "call_id": "call_actual", "type": "custom_tool_call",
                        "name": "exec", "namespace": "functions", "input": program}],
            "usage": None if mode == "unknown-usage" else {"input_tokens": 7, "output_tokens": 3, "total_tokens": 10}})
    config = OpenAIResponsesAgentConfig(api_key="controller-test-only", reasoning_effort="high", max_output_tokens=1000)
    gate = CodexInference(config, RunJournal(tmp_path, "actual"), role="verifier", max_requests=2, max_total_tokens=100,
        deadline=time.monotonic() + 10,
        cancelled=lambda: mode == "cancel" and bool(requests))
    body = {"model": config.model, "stream": True, "store": False, "tool_choice": "auto", "parallel_tool_calls": False,
        "reasoning": {"effort": "high"}, "include": ["reasoning.encrypted_content"], "input": [{
            "type": "additional_tools", "tools": [{"type": "namespace", "name": "functions", "tools": [{
                "type": "custom", "name": "exec", "format": {"type": "grammar", "syntax": "lark", "definition": "start: /.+/"}}]}]}]}
    http = httpx2.AsyncClient(transport=httpx2.MockTransport(respond))
    async def run():
        sdk = openai.AsyncOpenAI(api_key=config.api_key, base_url="https://provider.invalid/v1",
            organization="test-organization", project="test-project", http_client=http)
        try:
            if mode == "deadline": gate.deadline = time.monotonic() + 0.2
            if mode == "success":
                stream = await gate.request(sdk, body)
                assert b"response.completed" in stream and b"tools.run" in stream
            else:
                for _ in range(2):
                    with pytest.raises(CodexStopped): await gate.request(sdk, body)
        finally:
            await sdk.close()
    started = time.monotonic()
    asyncio.run(run())
    assert time.monotonic() - started < 2 and http.is_closed and len(requests) == 1
    request = requests[0]
    assert str(request.url) == "https://provider.invalid/v1/responses"
    assert request.headers["authorization"] == "Bearer controller-test-only"
    assert json.loads(request.content) == {**body, "stream": False, "max_output_tokens": 100}
    assert drained == ([True] if mode in {"cancel", "deadline"} else [])
    summary = gate.journal.summary()
    assert summary["usage_complete"] is (mode in {"success", "unsafe-action"})
    assert summary["total_tokens"] == (10 if mode in {"success", "unsafe-action"} else 0)
    assert summary["cost_usd"] is None
    assert not any(text in json.dumps(gate.journal.events()) for text in [config.api_key, "private upstream text", "forbidden"])
