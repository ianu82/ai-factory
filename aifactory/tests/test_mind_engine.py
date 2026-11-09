"""Real pinned ChatSession against local SSE, plus scripted lifecycle tests."""
import asyncio
import json
import threading

import pytest

from aifactory.mind import engine as me
from aifactory.mind import provider as mp
from aifactory.inference_limits import provider_guard
from tests.test_mind_provider import fake_provider, Ledger, MODEL, KEY


class Tools:
    def __init__(self):
        self.calls = []

    def definitions(self):
        return [{"type": "function", "function": {"name": name, "description": "Factory test tool",
                 "parameters": {"type": "object", "properties": {}, "additionalProperties": False}}}
                for name in ("factory_read", "factory_propose")]

    async def call(self, name, arguments):
        self.calls.append((name, arguments))
        return {"source": "outcome:39", "status": "unverified", "secret": KEY}


class Adapter:
    """T1 readiness/provider contract double; still uses the real SDK transport."""
    def __init__(self, credential):
        self.credential = credential
        self.invalidated = False

    async def open(self, workspace_id, readiness, session_id, ledger):
        return mp.make_provider(self.credential, MODEL, session_id, ledger)

    async def session_cost(self, *_):
        return None

    def invalidate(self, *_):
        self.invalidated = True


async def test_native_deltas_arrive_before_completion_and_tool_order():
    pytest.importorskip("anton.core.session")
    paused = threading.Event()
    with fake_provider(pause=paused) as (credential, requests, headers):
        adapter, ledger, tools = Adapter(credential), Ledger(), Tools()
        seen = []
        two_deltas = asyncio.Event()
        async def emit(event):
            seen.append(event)
            if sum(e["type"] == "text_delta" for e in seen) >= 2:
                two_deltas.set()
        task = asyncio.create_task(me.MindEngine(adapter).run(
            workspace_id="ws", prompt="What happened? " + KEY,
            history=[{"role": "user", "content": "old " + KEY}],
            tools=tools, ledger=ledger, emit=emit, readiness=mp.Readiness(True, model=MODEL)))
        try:
            await asyncio.wait_for(two_deltas.wait(), 10)
            assert not task.done(), "text must arrive while provider completion is paused"
            assert seen[:2] == [{"type": "tool_start", "name": "factory_read"},
                                {"type": "tool_result", "name": "factory_read"}]
            assert [e["text"] for e in seen if e["type"] == "text_delta"] == ["Ready ", "now."]
            assert provider_guard(credential.config).active == 1
            paused.set()
            result = await task
        finally:
            paused.set()
            if not task.done():
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
        assert result.text == "Ready now."
        assert result.cost_usd is None
        assert tools.calls == [("factory_read", {})]
        assert len(requests) == len(ledger.entries) == 2
        assert ledger.entries == [(MODEL, (10, 5)), (MODEL, (10, 5))]
        assert ledger.checks >= 4
        for body in requests:
            assert {t["name"] for t in body["tools"]} == {"factory_read", "factory_propose"}
            assert all(t["type"] == "function" for t in body["tools"])
            assert KEY not in json.dumps(body)
            assert "hidden reasoning" not in json.dumps(body)
            assert body["model"] == MODEL
        assert KEY not in json.dumps(seen)
        assert "hidden reasoning" not in json.dumps(seen)
        assert all(h["x-mindshub-allow-failover"] == "false" for h in headers)
        assert provider_guard(credential.config).active == 0
        assert len(provider_guard(credential.config)._sends) == 2


@pytest.mark.parametrize("tool", ["scratchpad", "run", "web_search", "web_fetch", "read_text_file",
                                  "read_image", "write_file", "recall_skill", "memorize", "recall"])
async def test_forced_builtin_cannot_execute(tool, monkeypatch):
    pytest.importorskip("anton.core.session")
    monkeypatch.setattr(me, "no_runtime", lambda **_: pytest.fail("runtime must never be constructed"))
    with fake_provider(forced=tool) as (credential, requests, _):
        tools, ledger = Tools(), Ledger()
        async def emit(_): pass
        with pytest.raises(mp.MindUnavailable) as exc:
            await me.MindEngine(Adapter(credential)).run(
                workspace_id="ws", prompt="Do it", history=[], tools=tools, ledger=ledger,
                emit=emit, readiness=mp.Readiness(True, model=MODEL))
        assert exc.value.reason == "unsupported_tools"
        assert tools.calls == []
        assert len(requests) == 1
        assert {t["name"] for t in requests[0]["tools"]} == {"factory_read", "factory_propose"}



async def test_provider_error_is_safe_and_never_retried():
    pytest.importorskip("anton.core.session")
    with fake_provider(mode="error") as (credential, requests, _):
        events, ledger, adapter = [], Ledger(), Adapter(credential)
        async def emit(event): events.append(event)
        with pytest.raises(mp.MindUnavailable) as exc:
            await me.MindEngine(adapter).run(workspace_id="ws", prompt="status", history=[], tools=Tools(),
                ledger=ledger, emit=emit, readiness=mp.Readiness(True, model=MODEL))
        assert exc.value.reason == "provider_failure"
        assert KEY not in str(exc.value) + json.dumps(events)
        assert len(requests) == len(ledger.entries) == 1
        assert adapter.invalidated
        assert provider_guard(credential.config).active == 0


async def test_cancel_closes_stream_and_accounts_unknown_usage():
    pytest.importorskip("anton.core.session")
    pause = threading.Event()
    with fake_provider(mode="no_tools", pause=pause) as (credential, requests, _):
        ledger, seen = Ledger(), asyncio.Event()
        async def emit(event):
            if event["type"] == "text_delta": seen.set()
        task = asyncio.create_task(me.MindEngine(Adapter(credential)).run(
            workspace_id="ws", prompt="status", history=[], tools=Tools(), ledger=ledger,
            emit=emit, readiness=mp.Readiness(True, model=MODEL)))
        await asyncio.wait_for(seen.wait(), 10)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert len(ledger.entries) == len(requests) == 1
        assert ledger.entries[0][1] is None
        assert provider_guard(credential.config).active == 0
        pause.set()


async def test_split_bearer_never_emitted():
    pytest.importorskip("anton.core.session")
    with fake_provider(mode="key") as (credential, requests, _):
        events = []
        async def emit(event): events.append(event)
        result = await me.MindEngine(Adapter(credential)).run(
            workspace_id="ws", prompt="status", history=[], tools=Tools(), ledger=Ledger(),
            emit=emit, readiness=mp.Readiness(True, model=MODEL))
        assert KEY not in result.text + json.dumps(events) + json.dumps(requests)
        assert "[redacted]" in result.text


async def test_exact_registry_rebuilt_without_global_mutation(tmp_path):
    pytest.importorskip("anton.core.session")
    from anton.core.llm.client import LLMClient
    from anton.core.settings import CoreSettings
    from anton.core.tools.tool_defs import SCRATCHPAD_TOOL, ToolDef
    description = SCRATCHPAD_TOOL.description
    with fake_provider() as (credential, _, _):
        provider = mp.make_provider(credential, MODEL, "registry", Ledger())
        client = LLMClient(planning_provider=provider, planning_model=MODEL,
                           coding_provider=provider, coding_model=MODEL, router_provider=provider, router_model=MODEL)
        async def handler(*_): return "ok"
        tools = [ToolDef("factory_read", "read", {"type": "object", "properties": {}}, handler)]
        settings = CoreSettings.model_construct(analytics_enabled=False, skills_root=tmp_path / "skills",
                                                skills_extra_roots=[], skills_artifact_roots=[])
        chat = me.restricted_chat(client, settings, tools, [], str(tmp_path), "registry")
        try:
            for _ in range(3):
                chat.tool_registry.register_tool(SCRATCHPAD_TOOL)
                assert [t["name"] for t in chat._build_tools()] == ["factory_read"]
                assert [t.name for t in chat.tool_registry.get_tool_defs()] == ["factory_read"]
            assert SCRATCHPAD_TOOL.description == description
            assert await chat._build_system_prompt() == me.PERSONA
            with pytest.raises(mp.MindUnavailable): me.no_runtime()
        finally:
            await chat.close()
            await client.aclose()


async def test_scripted_history_rejection_and_failure_cleanup(monkeypatch):
    pytest.importorskip("anton.core.session")
    from anton.core.llm.provider import StreamTextDelta
    events = []
    closed = []
    class Chat:
        async def turn_stream(self, prompt, **_):
            assert KEY not in prompt
            try:
                yield StreamTextDelta(text="Live")
                raise mp._ProviderStop("model_substitution")
            finally:
                closed.append("stream")
        async def close(self): closed.append("chat")
    def create(client, settings, tools, history, *_):
        assert history == [{"role": "user", "content": "[redacted]"}]
        assert not settings.analytics_enabled
        assert {t.name for t in tools} == {"factory_read", "factory_propose"}
        return Chat()
    monkeypatch.setattr(me, "restricted_chat", create)
    with fake_provider() as (credential, requests, _):
        adapter = Adapter(credential)
        async def emit(event): events.append(event)
        args = dict(workspace_id="ws", prompt=KEY, tools=Tools(), ledger=Ledger(),
                    emit=emit, readiness=mp.Readiness(True, model=MODEL))
        with pytest.raises(ValueError):
            await me.MindEngine(adapter).run(history=[{"role": "system", "content": "unsafe"}], **args)
        with pytest.raises(mp.MindUnavailable) as exc:
            await me.MindEngine(adapter).run(history=[{"role": "user", "content": KEY}], **args)
        assert exc.value.reason == "model_substitution"
        assert events == [{"type": "text_delta", "text": "Live"}]
        assert closed == ["stream", "chat"]
        assert adapter.invalidated
        assert not requests
