"""Authenticated catalog and native-stream readiness, entirely local."""
import asyncio
import json
import socket
import threading
import time
from contextlib import contextmanager
from types import SimpleNamespace

import pytest
from fastapi import FastAPI, Request
from fastapi.responses import StreamingResponse

from aifactory.inference import BudgetExceeded, ProviderConfig
from aifactory.inference_limits import provider_guard
from aifactory.mind import provider as mp

MODEL = "anthropic/claude-opus-5.5"
KEY = "private-fixture-bearer"


class Ledger:
    def __init__(self, limit=20):
        self.entries = []
        self.limit = limit
        self.checks = 0

    def check(self):
        self.checks += 1
        if len(self.entries) >= self.limit:
            raise BudgetExceeded("budget")

    def add(self, model, usage):
        self.entries.append((model, usage))


@contextmanager
def fake_provider(*, catalog=None, mode="normal", forced=None, pause=None):
    import uvicorn

    app = FastAPI()
    requests = []
    headers = []

    @app.get("/v1/models")
    async def models(request: Request):
        headers.append(dict(request.headers))
        return {"data": [{"id": name} for name in (catalog if catalog is not None else [MODEL])]}

    @app.get("/v1/usage/sessions/{session}")
    async def cost(session: str):
        return {"estimated_cost_usd": 0.003}

    @app.post("/v1/responses")
    async def responses(request: Request):
        body = await request.json()
        requests.append(body)
        headers.append(dict(request.headers))
        outputs = [i for i in body["input"] if i.get("type") == "function_call_output"]
        name = forced or body["tools"][0]["name"]
        call = not outputs and mode != "no_tools"
        response = {"id": "resp_test", "object": "response", "created_at": 1, "status": "completed",
                    "model": "claude-opus-5" if mode == "substitute" else MODEL,
                    "output": [], "usage": {"input_tokens": 10, "output_tokens": 5,
                    "total_tokens": 15, "input_tokens_details": {"cached_tokens": 0},
                    "output_tokens_details": {"reasoning_tokens": 0}}}

        async def stream():
            def event(kind, **kw):
                return "data: " + json.dumps({"type": kind, **kw}) + "\n\n"
            if mode == "error":
                yield event("error", message=KEY, code="secret")
                return
            yield event("response.created", response={**response, "status": "in_progress", "usage": None})
            yield event("response.reasoning_summary_text.delta", delta="hidden reasoning")
            if call:
                item = {"type": "function_call", "id": "fc_1", "call_id": "call_1", "name": name,
                        "arguments": "", "status": "in_progress"}
                yield event("response.output_item.added", output_index=0, item=item)
                yield event("response.function_call_arguments.delta", output_index=0, delta="{}", item_id="fc_1")
                yield event("response.function_call_arguments.done", output_index=0, arguments="{}", item_id="fc_1")
            else:
                for text in ([KEY[:8], KEY[8:]] if mode == "key" else ["Ready ", "now."]):
                    yield event("response.output_text.delta", output_index=0, content_index=0, item_id="msg_1", delta=text)
                if pause:
                    await asyncio.to_thread(pause.wait, 5)
            yield event("response.completed", response=response)
            yield "data: [DONE]\n\n"
        return StreamingResponse(stream(), media_type="text/event-stream")

    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    server = uvicorn.Server(uvicorn.Config(app, host="127.0.0.1", port=port, log_level="error"))
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()
    for _ in range(100):
        if server.started:
            break
        time.sleep(0.02)
    credential = mp.Credential(ProviderConfig("mindshub", f"http://127.0.0.1:{port}/v1", KEY, "Fake"), "v1")
    try:
        yield credential, requests, headers
    finally:
        if pause:
            pause.set()
        server.should_exit = True
        thread.join(5)


@pytest.mark.parametrize("names,reason", [
    (["claude-opus-5", "claude-opus-5.6", "claude-opus-5.50"], "model_absent"),
    ([MODEL, "claude-opus-5-5"], "ambiguous_catalog"),
])
def test_exact_catalog(names, reason):
    with pytest.raises(mp.MindUnavailable) as exc:
        mp.select_model(names)
    assert exc.value.reason == reason
    assert mp.select_model(["gpt", "claude-opus-4.6", MODEL]) == MODEL


@pytest.mark.parametrize("mode,reason,count", [
    ("normal", None, 2), ("no_tools", "unsupported_tools", 1),
    ("substitute", "model_substitution", 1), ("error", "provider_failure", 1),
])
async def test_readiness_native_probe(monkeypatch, mode, reason, count):
    pytest.importorskip("anton.core.session")
    with fake_provider(mode=mode) as (credential, requests, headers):
        monkeypatch.setattr(mp, "saved_credential", lambda _: credential)
        ledger = Ledger()
        adapter = mp.MindProvider()
        result = await adapter.readiness("ws", ledger)
        assert result.reason == reason
        assert result.available == (reason is None)
        assert len(requests) == count
        assert len(ledger.entries) == count
        assert result.cost_usd == 0.003
        assert all(body["model"] == MODEL and body["stream"] for body in requests)
        assert all(h["authorization"] == f"Bearer {KEY}" and h["x-mindshub-allow-failover"] == "false" for h in headers)
        assert requests[0]["tool_choice"] == {"type": "function", "name": "mind_readiness_ping"}
        if result.available:
            assert requests[1]["tool_choice"] == "none"
            assert await adapter.readiness("ws", ledger) == result
            assert len(requests) == 2
            provider = await adapter.open("ws", result, "chat", ledger)
            assert not provider.export_connection_info().api_key
            assert await provider.current_api_key() is None
            assert provider.trace_headers() == {}
            assert not provider._client._client.follow_redirects
            assert provider._client.max_retries == 0
            await provider.aclose()
            rotated = mp.Credential(credential.config, "v2")
            monkeypatch.setattr(mp, "saved_credential", lambda _: rotated)
            with pytest.raises(mp.MindUnavailable):
                await adapter.open("ws", result, "chat", ledger)
            assert (await adapter.readiness("ws", ledger)).available
            assert len(requests) == 4
        assert KEY not in repr(result)
        guard = provider_guard(credential.config)
        assert guard.active == 0
        assert len(guard._sends) == len(requests)


@pytest.mark.parametrize("catalog,reason", [([], "model_absent"), ([MODEL, "claude-opus-5-5"], "ambiguous_catalog")])
async def test_catalog_failure_never_sends_inference(monkeypatch, catalog, reason):
    pytest.importorskip("anton.core.session")
    with fake_provider(catalog=catalog) as (credential, requests, _):
        monkeypatch.setattr(mp, "saved_credential", lambda _: credential)
        result = await mp.MindProvider().readiness("ws", Ledger())
        assert result.reason == reason
        assert not requests


@pytest.mark.parametrize("secret", [None, SimpleNamespace(status="invalid", ciphertext="DO NOT DECRYPT")])
def test_missing_or_invalid_saved_key_never_uses_bootstrap(monkeypatch, secret):
    class DB:
        def query(self, *_): return self
        def filter_by(self, **_): return self
        def one_or_none(self): return secret
    @contextmanager
    def scope(): yield DB()
    monkeypatch.setattr(mp, "session_scope", scope)
    monkeypatch.setattr(mp, "decrypt", lambda _: pytest.fail("must not decrypt"))
    with pytest.raises(mp.MindUnavailable) as exc:
        mp.saved_credential("ws")
    assert exc.value.reason == "missing_key"


async def test_budget_prevents_second_probe(monkeypatch):
    pytest.importorskip("anton.core.session")
    with fake_provider() as (credential, requests, _):
        monkeypatch.setattr(mp, "saved_credential", lambda _: credential)
        ledger = Ledger(limit=1)
        result = await mp.MindProvider().readiness("ws", ledger)
        assert not result.available
        assert result.reason == "budget_exceeded"
        assert len(requests) == len(ledger.entries) == 1


async def test_missing_sdk_and_missing_key_are_specific(monkeypatch):
    import builtins

    def missing(_):
        raise mp.MindUnavailable("missing_key")
    monkeypatch.setattr(mp, "saved_credential", missing)
    assert (await mp.MindProvider().readiness("ws", Ledger())).reason == "missing_key"
    real_import = builtins.__import__
    def without_sdk(name, *args, **kwargs):
        if name.startswith("anton"):
            raise ImportError("SDK missing; sensitive details")
        return real_import(name, *args, **kwargs)
    credential = mp.Credential(ProviderConfig("mindshub", "http://unused", KEY, "Fake"), "v1")
    monkeypatch.setattr(mp, "saved_credential", lambda _: credential)
    async def no_cost(*_): return None
    monkeypatch.setattr(mp.ModelClient, "session_cost", no_cost)
    monkeypatch.setattr(builtins, "__import__", without_sdk)
    result = await mp.MindProvider().readiness("ws", Ledger())
    assert result.reason == "missing_sdk"
    assert "sensitive" not in repr(result)


async def test_failed_readiness_invalidates_previous_success(monkeypatch):
    pytest.importorskip("anton.core.session")
    with fake_provider() as (credential, _, _):
        monkeypatch.setattr(mp, "saved_credential", lambda _: credential)
        adapter = mp.MindProvider()
        ready = await adapter.readiness("ws", Ledger())
        assert ready.available
        def revoked(_):
            raise mp.MindUnavailable("missing_key")
        monkeypatch.setattr(mp, "saved_credential", revoked)
        assert (await adapter.readiness("ws", Ledger())).reason == "missing_key"
        monkeypatch.setattr(mp, "saved_credential", lambda _: credential)
        with pytest.raises(mp.MindUnavailable):
            await adapter.open("ws", ready, "chat", Ledger())


def test_mindshub_catalog_ids_select_opus_5_5_and_nothing_else():
    # MindsHub's catalog (10 Nov 2026) names Opus 5.5 "opus-5-5", next to the aliases "opus" and "opus-5".
    assert mp.select_model(["opus", "opus-5", "opus-5-5", "gpt-sol-6-1", "fable-5-1"]) == "opus-5-5"
    with pytest.raises(mp.MindUnavailable) as exc:
        mp.select_model(["opus", "opus-5", "opus-5-6", "opus-5-50"])
    assert exc.value.reason == "model_absent"


@pytest.mark.parametrize("reported,selected,same", [
    ("opus-5-5", "opus-5-5", True),
    ("claude-opus-5-5-20261101", "opus-5-5", True),
    ("anthropic/claude-opus-5.5", "opus-5-5", True),
    ("claude-opus-5", "opus-5-5", False),
    ("claude-opus-5-6", "opus-5-5", False),
    ("opus-5-50", "opus-5-5", False),
    ("gpt-opus-5-5-fast", "opus-5-5", False),
    ("claude-opus-5-5.1", "opus-5-5", False),
    ("gpt-sol-6-1", "opus-5-5", False),
    (None, "opus-5-5", False),
])
def test_a_response_naming_the_same_generation_is_not_a_substitution(reported, selected, same):
    assert mp.same_model(reported, selected) is same
