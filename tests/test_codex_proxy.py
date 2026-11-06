import asyncio
import copy
import json
import time
from types import SimpleNamespace
from urllib.parse import urlsplit

import pytest

from auto_mindsdb_factory.codex_inference import CodexInference
from auto_mindsdb_factory.codex_proxy import open_codex_proxy
from auto_mindsdb_factory.connectors import OpenAIResponsesAgentConfig
from auto_mindsdb_factory.run_journal import RunJournal


BODY = {"model": "gpt-6.1-terra", "stream": True, "store": False, "parallel_tool_calls": False, "tool_choice": "auto",
    "reasoning": {"effort": "high"}, "include": ["reasoning.encrypted_content"], "input": [{"type": "additional_tools",
    "tools": [{"type": "namespace", "name": "functions", "tools": [{"type": "custom", "name": "exec",
    "format": {"type": "grammar", "syntax": "lark", "definition": "start: /.+/"}}]}]}]}
RESPONSE = {"id": "resp_proxy", "model": "gpt-6.1-terra", "status": "completed", "output": [{"id": "ctc_proxy",
    "call_id": "call_proxy", "type": "custom_tool_call", "name": "exec", "namespace": "functions",
    "input": 'text(await tools.run({"command":["true"]}))'}],
    "usage": {"input_tokens": 7, "output_tokens": 3, "total_tokens": 10}}


def boundary(tmp_path):
    return CodexInference(OpenAIResponsesAgentConfig(reasoning_effort="high", max_output_tokens=32),
        RunJournal(tmp_path, "proxy"), role="builder", max_requests=2, max_total_tokens=100,
        deadline=time.monotonic() + 10)


async def connect(proxy, *, token=None, body=BODY):
    url = urlsplit(proxy.base_url)
    reader, writer = await asyncio.open_connection(url.hostname, url.port)
    data = json.dumps(body).encode()
    capability = proxy.token if token is None else token
    writer.write((f"POST /v1/responses HTTP/1.1\r\nHost: {url.netloc}\r\nX-Factory-Session: {capability}\r\n"
                  f"Content-Type: application/json\r\nContent-Length: {len(data)}\r\n\r\n").encode() + data)
    await writer.drain()
    return reader, writer


def test_socket_requests_require_capability_and_only_expose_validated_accounted_sse(tmp_path):
    gate, calls = boundary(tmp_path), []
    async def post(path, **kwargs):
        calls.append(kwargs["body"])
        return copy.deepcopy(RESPONSE)
    client = SimpleNamespace(with_options=lambda **k: SimpleNamespace(post=post))
    async def run():
        before = asyncio.all_tasks()
        async with open_codex_proxy(gate, client) as proxy:
            assert len(proxy.token) == 43 and proxy.token not in repr(proxy)
            reader, writer = await connect(proxy, token="incorrect")
            assert (await reader.read()).startswith(b"HTTP/1.1 400") and not calls and not gate.stopped
            writer.close(); await writer.wait_closed()
            reader, writer = await connect(proxy)
            response = await reader.read()
            assert response.startswith(b"HTTP/1.1 200") and b"response.completed" in response
            assert len(calls) == 1 and proxy.token not in json.dumps(calls) + json.dumps(gate.journal.events())
            assert calls[0]["stream"] is False and calls[0]["max_output_tokens"] == 32
            writer.close(); await writer.wait_closed()
        url = urlsplit(proxy.base_url)
        with pytest.raises(OSError): await asyncio.open_connection(url.hostname, url.port)
        assert asyncio.all_tasks() == before
    asyncio.run(run())
    assert gate.journal.summary()["total_tokens"] == 10 and gate.journal.summary()["cost_usd"] is None


@pytest.mark.parametrize("mode", ["request-policy", "unsafe-action", "provider-error"])
def test_policy_or_provider_failure_releases_no_executable_stream(tmp_path, mode):
    gate, calls = boundary(tmp_path), []
    async def post(path, **kwargs):
        calls.append(True)
        if mode == "provider-error": raise RuntimeError("private upstream body")
        response = copy.deepcopy(RESPONSE)
        response["output"][0]["input"] = 'text(await tools.apply_patch("private injected program"))'
        return response
    client = SimpleNamespace(with_options=lambda **k: SimpleNamespace(post=post))
    async def run():
        async with open_codex_proxy(gate, client) as proxy:
            reader, writer = await connect(proxy, body={**BODY, "model": "other"} if mode == "request-policy" else BODY)
            response = await reader.read()
            assert b"event:" not in response and b"private" not in response
            writer.close(); await writer.wait_closed()
    asyncio.run(run())
    assert len(calls) == (0 if mode == "request-policy" else 1) and gate.stopped
    assert "private" not in json.dumps(gate.journal.events())


@pytest.mark.parametrize("mode", ["cancel", "deadline", "disconnect", "context-exit", "cancel-and-exit"])
def test_real_socket_stop_disconnect_and_context_exit_drain_active_provider_work(tmp_path, mode):
    gate, calls, cancelled = boundary(tmp_path), [], [False]
    gate.cancelled = lambda: cancelled[0]
    async def run():
        before = asyncio.all_tasks()
        started, drained = asyncio.Event(), asyncio.Event()
        async def post(path, **kwargs):
            calls.append(True); started.set()
            try:
                await asyncio.Event().wait()
            finally:
                await asyncio.sleep(0.1)  # A second cancellation must not interrupt cleanup.
                drained.set()
        client = SimpleNamespace(with_options=lambda **k: SimpleNamespace(post=post))
        async with open_codex_proxy(gate, client) as proxy:
            reader, writer = await connect(proxy)
            await asyncio.wait_for(started.wait(), timeout=1)
            if mode in {"cancel", "cancel-and-exit"}: cancelled[0] = True
            if mode == "deadline": gate.deadline = time.monotonic() + 0.01
            if mode == "disconnect": writer.close(); await writer.wait_closed()
            if mode == "cancel-and-exit": await asyncio.sleep(0.06)
            if mode not in {"context-exit", "cancel-and-exit"}:
                await asyncio.wait_for(drained.wait(), timeout=1)
                assert b"event:" not in await asyncio.wait_for(reader.read(), timeout=1)
        writer.close(); await writer.wait_closed()
        assert drained.is_set() and asyncio.all_tasks() == before
    started_at = time.monotonic()
    asyncio.run(run())
    assert time.monotonic() - started_at < 2
    assert len(calls) == 1 and gate.stopped and not gate.journal.summary()["usage_complete"]
    assert gate.journal.summary()["total_tokens"] == 0 and gate.journal.summary()["cost_usd"] is None
