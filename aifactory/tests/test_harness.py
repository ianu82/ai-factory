from __future__ import annotations

import asyncio
import json
import time

from aifactory.harness import tools
from aifactory.harness.base import HarnessSession
from aifactory.harness.native import NativeHarness
from aifactory.harness.schemas import BUILDER_REPORT
from aifactory.inference import ProviderConfig, UsageLedger
from aifactory.sandbox import COMMAND_TIMEOUT, ExecResult, LocalSandbox

from .fakes import call, step


class Script:
    def __init__(self, fn):
        self.fn = fn
        self.provider = ProviderConfig("openai", "http://x", "k", "T")
        self.seen = []

    async def chat(self, *, messages, ledger=None, **_):
        self.seen.append([dict(m) for m in messages])
        return self.fn(messages)


def make_session(tmp_path, fn, **kw):
    sb = LocalSandbox("h", root=tmp_path).create()
    sb.work.mkdir()
    (sb.work / "a.txt").write_text("one\ntwo\n")
    client = Script(fn)
    session = HarnessSession(run_id="r", role="builder", system="sys", prompt="do it", sandbox=sb, model="m",
                             client=client, ledger=UsageLedger(run_id="r", workspace_settings={}),
                             report_schema=BUILDER_REPORT, writable=True, **kw)
    return session, client, sb


def test_native_recovers_from_bad_tool_calls_and_bad_report(tmp_path, monkeypatch):
    monkeypatch.setattr(UsageLedger, "persist", lambda self: None)

    def fn(messages):
        n = step(messages)
        if n == 0:
            bad = {"id": "x", "type": "function", "function": {"name": "read_file", "arguments": "{not json"}}
            return {"content": "", "tool_calls": [bad, call("nope", {}, 1), call("read_file", {"path": "../etc/passwd"}, 2)]}
        if n == 1:
            return {"content": "", "tool_calls": [call("edit_file", {"path": "a.txt", "old_text": "two", "new_text": "2"})]}
        if n == 2:
            return {"content": "", "tool_calls": [call("finish", {"summary": "missing status"})]}
        return {"content": "", "tool_calls": [call("finish", {"status": "done", "summary": "edited"})]}

    session, client, sb = make_session(tmp_path, fn)
    result = asyncio.run(NativeHarness().run(session))
    assert result.report["status"] == "done"
    assert (sb.work / "a.txt").read_text() == "one\n2\n"
    tool_outputs = [m["content"] for m in client.seen[1] if m["role"] == "tool"]
    assert "not valid JSON" in tool_outputs[0] and "unknown" in tool_outputs[1] and "escapes" in tool_outputs[2]
    final_outputs = [m["content"] for m in client.seen[3] if m["role"] == "tool"]
    assert any("missing required fields: status" in o for o in final_outputs)


def test_native_delivers_team_guidance_mid_run(tmp_path, monkeypatch):
    monkeypatch.setattr(UsageLedger, "persist", lambda self: None)
    notes = [["Prefer small functions"]]

    def fn(messages):
        if step(messages) == 0:
            return {"content": "", "tool_calls": [call("run", {"command": "echo hi"})]}
        return {"content": "", "tool_calls": [call("finish", {"status": "done", "summary": "ok"})]}

    session, client, _ = make_session(tmp_path, fn, take_guidance=lambda: notes.pop() if notes else [])
    asyncio.run(NativeHarness().run(session))
    assert any("Prefer small functions" in str(m.get("content")) for m in client.seen[0])


def test_native_warns_once_under_ten_turns_or_ten_minutes_are_left(tmp_path, monkeypatch):
    from time import monotonic

    monkeypatch.setattr(UsageLedger, "persist", lambda self: None)

    def fn(messages):
        if step(messages) < 2:
            return {"content": "", "tool_calls": [call("run", {"command": "true"}, step(messages))]}
        return {"content": "", "tool_calls": [call("finish", {"status": "done", "summary": "ok"})]}

    def warnings(**limits):
        """Per model call of a three-call session: the budget warning the agent was sent just before it, if any."""
        session, client, _ = make_session(tmp_path / "-".join(limits), fn, **limits)
        asyncio.run(NativeHarness().run(session))
        last = [str(messages[-1].get("content")) for messages in client.seen]
        return [text.split(" in this session.")[0] if text.startswith("Budget: ") else "" for text in last]

    assert warnings() == ["", "", ""]  # 80 turns and an hour: nothing to say
    by_turns = warnings(max_turns=11)
    assert by_turns[:2] == ["", ""] and by_turns[2].endswith(" min and 9 turns are left")
    by_time = warnings(deadline=monotonic() + 300)
    assert by_time == [f"Budget: about 4 min and {n} turns are left" for n in (80, 79, 78)]


def test_native_forces_a_final_report_when_out_of_turns(tmp_path, monkeypatch):
    monkeypatch.setattr(UsageLedger, "persist", lambda self: None)
    seen_tools = []

    def fn(messages):
        return {"content": "", "tool_calls": [call("run", {"command": "true"}, step(messages))]}

    def wrapped(messages):
        if "out of turns" in str(messages[-1].get("content")):
            return {"content": "", "tool_calls": [call("finish", {"status": "blocked", "summary": "ran out",
                                                                  "blocked_reason": "time"})]}
        return fn(messages)

    session, client, _ = make_session(tmp_path, wrapped, max_turns=3)
    result = asyncio.run(NativeHarness().run(session))
    assert result.report["status"] == "blocked"


def test_run_tool_allows_a_command_as_long_as_a_check_and_says_so(tmp_path, monkeypatch):
    limits, progress = [], []

    async def exec_(self, command, *, timeout=600, **_):
        limits.append(timeout)
        slow = command == "sleep 9999"
        return ExecResult(command, 124 if slow else 0, "so far", 1.0, timed_out=slow)

    monkeypatch.setattr(LocalSandbox, "exec", exec_)
    session, _, _ = make_session(tmp_path, None,
                                 on_progress=lambda kind, message, data: progress.append((kind, message)))
    assert session.command_timeout == COMMAND_TIMEOUT  # what one of the repository's checks is allowed
    box = tools.ToolBox(session)

    async def go():
        await box.call("run", {"command": "pytest -q", "timeout_seconds": 99_999})
        await box.call("run", {"command": "pytest -q"})
        out = await box.call("run", {"command": "sleep 9999"})
        session.deadline = time.monotonic() + 200  # a session about to end allows less
        await box.call("run", {"command": "pytest -q", "timeout_seconds": 99_999})
        return out

    out = asyncio.run(go())
    assert limits[:3] == [COMMAND_TIMEOUT, 300, 300] and 30 <= limits[3] <= 140
    assert "[timed out, " in out
    results = [message for kind, message in progress if kind == "command_result"]
    assert results[1].startswith("exit 0 in ")
    assert results[2].startswith("timed out after ") and results[2].endswith("(limit 300s)")  # not "exit 124"
    run = next(t["function"] for t in tools.tool_specs(False) if t["function"]["name"] == "run")
    assert f"Default 300, max {COMMAND_TIMEOUT} " in run["parameters"]["properties"]["timeout_seconds"]["description"]


def test_model_client_speaks_responses_api_and_round_trips_reasoning(monkeypatch):
    import httpx

    from aifactory import inference
    from aifactory.inference import ModelClient

    seen = []

    def upstream(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content)
        seen.append(body)
        assert request.url.path.endswith("/responses")
        if len(seen) == 1:
            return httpx.Response(200, json={"status": "completed", "usage": {"input_tokens": 50, "output_tokens": 5}, "output": [
                {"type": "reasoning", "id": "rs_1", "encrypted_content": "enc", "summary": []},
                {"type": "function_call", "id": "fc_1", "call_id": "call_1", "name": "run", "arguments": "{\"command\": \"ls\"}"}]})
        return httpx.Response(200, json={"status": "completed", "usage": {"input_tokens": 60, "output_tokens": 7}, "output": [
            {"type": "message", "role": "assistant", "content": [{"type": "output_text", "text": "done"}]}]})

    real = httpx.AsyncClient
    monkeypatch.setattr(inference.httpx, "AsyncClient", lambda **kw: real(transport=httpx.MockTransport(upstream), **kw))
    client = ModelClient(ProviderConfig("openai", "https://api.test/v1", "sk-x", "OpenAI"))
    tools = [{"type": "function", "function": {"name": "run", "description": "d", "parameters": {"type": "object"}}}]
    msgs = [{"role": "system", "content": "sys"}, {"role": "user", "content": "go"}]
    first = asyncio.run(client.chat(model="m", messages=msgs, tools=tools, reasoning_effort="high"))
    assert first["tool_calls"][0]["id"] == "call_1" and first["tool_calls"][0]["function"]["name"] == "run"
    assert seen[0]["tools"][0]["name"] == "run" and seen[0]["reasoning"] == {"effort": "high"}
    assert seen[0]["input"][0] == {"role": "developer", "content": "sys"}
    msgs += [first, {"role": "tool", "tool_call_id": "call_1", "content": "a.txt"}]
    second = asyncio.run(client.chat(model="m", messages=msgs, tools=tools))
    assert second["content"] == "done"
    replay = seen[1]["input"]
    assert {"type": "reasoning", "id": "rs_1", "encrypted_content": "enc", "summary": []} in replay
    assert replay[-1] == {"type": "function_call_output", "call_id": "call_1", "output": "a.txt"}
