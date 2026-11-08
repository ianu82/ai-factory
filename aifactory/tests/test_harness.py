from __future__ import annotations

import asyncio
import json

from aifactory.harness.base import HarnessSession
from aifactory.harness.native import NativeHarness
from aifactory.harness.schemas import BUILDER_REPORT
from aifactory.inference import ProviderConfig, UsageLedger
from aifactory.sandbox import LocalSandbox

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
