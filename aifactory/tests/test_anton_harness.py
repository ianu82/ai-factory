"""Anton harness against a local fake Responses API (runs only where the Anton SDK is installed)."""

from __future__ import annotations

import asyncio
import json
import socket
import threading
import time

import pytest

anton = pytest.importorskip("anton.core.session")

from fastapi import FastAPI, Request  # noqa: E402

from aifactory import inference  # noqa: E402
from aifactory.inference_limits import provider_guard  # noqa: E402
from aifactory.harness.anton import AntonHarness  # noqa: E402
from aifactory.harness.base import HarnessFailed, HarnessSession  # noqa: E402
from aifactory.harness.schemas import BUILDER_REPORT  # noqa: E402
from aifactory.inference import ModelClient, ProviderConfig, UsageLedger  # noqa: E402
from aifactory.sandbox import LocalSandbox  # noqa: E402


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _fake_server(port: int, requests: list, web_enabled=False, input_tokens=100, cached_tokens=0, errors=None):
    """``errors``: what the provider answers with first, one per request, before it serves any: an HTTP status, or
    (status, headers, body)."""
    import uvicorn

    app = FastAPI()

    def response(output, n):
        return {"id": f"resp_{n}", "object": "response", "created_at": int(time.time()), "status": "completed",
                "model": "fake-model", "output": output, "parallel_tool_calls": True, "tool_choice": "auto", "tools": [],
                "usage": {"input_tokens": input_tokens, "output_tokens": 10, "total_tokens": input_tokens + 10,
                          "input_tokens_details": {"cached_tokens": cached_tokens},
                          "output_tokens_details": {"reasoning_tokens": 0}}}

    @app.post("/v1/responses")
    async def responses(request: Request):
        body = await request.json()
        if errors:
            from fastapi.responses import JSONResponse
            answer = errors.pop(0)
            status, headers, content = answer if isinstance(answer, tuple) else (
                answer, {}, {"error": {"message": "the fake provider refused"}})
            return JSONResponse(content, status_code=status, headers=headers)
        requests.append(body)
        outputs = [i for i in body.get("input", []) if isinstance(i, dict) and i.get("type") == "function_call_output"]
        n = len(requests)
        if not outputs:
            call = {"type": "function_call", "id": "fc_1", "call_id": "call_1", "name": "write_file", "status": "completed",
                    "arguments": json.dumps({"path": "hello.txt", "content": "hi from anton\n"})}
        elif len(outputs) == 1:
            call = {"type": "function_call", "id": "fc_image", "call_id": "call_image", "name": "read_image",
                    "status": "completed", "arguments": json.dumps({"file_path": ".factory/screen.jpg"})}
        elif web_enabled and len(outputs) == 2:
            call = {"type": "function_call", "id": "fc_web", "call_id": "call_web", "name": "web_fetch",
                    "status": "completed", "arguments": json.dumps({"url": f"http://127.0.0.1:{port}/page"})}
        else:
            call = {"type": "function_call", "id": f"fc_{n}", "call_id": f"call_{n}", "name": "finish", "status": "completed",
                    "arguments": json.dumps({"status": "done", "summary": "Wrote hello.txt"})}
        return response([call], n)

    @app.get("/page")
    async def page():
        from fastapi.responses import PlainTextResponse
        return PlainTextResponse("Sandbox web integration fixture")

    server = uvicorn.Server(uvicorn.Config(app, host="127.0.0.1", port=port, log_level="error"))
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()
    for _ in range(100):
        if server.started:
            break
        time.sleep(0.05)
    return server


def _sandbox(tmp_path):
    import io
    from PIL import Image

    sb = LocalSandbox("anton", root=tmp_path).create()
    sb.work.mkdir()
    image = io.BytesIO()
    Image.new("RGB", (2, 2), "red").save(image, format="PNG")
    (sb.work / ".factory").mkdir()
    (sb.work / ".factory" / "screen.jpg").write_bytes(image.getvalue())
    return sb


@pytest.mark.parametrize("web_enabled", [False, True])
def test_anton_harness_runs_factory_tools_and_returns_validated_report(tmp_path, monkeypatch, web_enabled):
    monkeypatch.setattr(UsageLedger, "persist", lambda self: None)
    port = _free_port()
    requests: list = []
    server = _fake_server(port, requests, web_enabled)
    progress = []
    try:
        sb = _sandbox(tmp_path)
        provider = ProviderConfig("openai", f"http://127.0.0.1:{port}/v1", "sk-test", "Fake")
        ledger = UsageLedger(run_id="r", workspace_settings={})
        session = HarnessSession(run_id="r", role="builder", system="You are a builder.", prompt="Create hello.txt",
                                 sandbox=sb, model="fake-model", client=ModelClient(provider), ledger=ledger,
                                 report_schema=BUILDER_REPORT, writable=True, max_turns=10,
                                 tool_policy={"web": web_enabled},
                                 on_progress=lambda *event: progress.append(event))
        result = asyncio.run(AntonHarness().run(session))
    finally:
        server.should_exit = True
    assert result.report == {"status": "done", "summary": "Wrote hello.txt"}
    assert (sb.work / "hello.txt").read_text() == "hi from anton\n"
    assert ledger.input_tokens >= 200 and ledger.calls >= 2
    guard = provider_guard(provider)
    assert guard.active == 0
    assert len(guard._sends) == len(requests)
    tool_names = {t.get("name") for t in requests[0].get("tools", [])}
    assert {"write_file", "finish", "run", "read_image"} <= tool_names
    assert "scratchpad" not in tool_names
    if web_enabled:
        assert {"web_search", "web_fetch"} <= tool_names
        assert any(event[0] == "web_request" and event[2]["outcome"] == "success" for event in progress)
        assert any("Sandbox web integration fixture" in json.dumps(body) for body in requests[3:])
    else:
        assert not tool_names & {"web_search", "web_fetch"}
    # All advertised tools are function tools, never provider-native web tools.
    assert all(tool.get("type") == "function" for body in requests for tool in body.get("tools", []))

    # The actual ChatSession dispatch must use the replacement and pass vision
    # content to the provider, not just advertise an otherwise unreachable tool.
    assert any("data:image/png;base64," in json.dumps(body) for body in requests[2:])


def test_anton_retries_a_short_outage_inside_the_session(tmp_path, monkeypatch):
    # The real SDK and real HTTP 503s: the request is sent again in place, and the session goes on to its report.
    monkeypatch.setattr(UsageLedger, "persist", lambda self: None)
    monkeypatch.setattr(inference, "RETRY_WAIT_CAP_SECONDS", 0.01)
    port = _free_port()
    requests: list = []
    errors = [503, 503]
    server = _fake_server(port, requests, errors=errors)
    progress = []
    try:
        sb = _sandbox(tmp_path)
        provider = ProviderConfig("openai", f"http://127.0.0.1:{port}/v1", "sk-test", "Fake")
        session = HarnessSession(run_id="r", role="builder", system="You are a builder.", prompt="Create hello.txt",
                                 sandbox=sb, model="fake-model", client=ModelClient(provider),
                                 ledger=UsageLedger(run_id="r", workspace_settings={}), report_schema=BUILDER_REPORT,
                                 writable=True, max_turns=10, on_progress=lambda *event: progress.append(event))
        result = asyncio.run(AntonHarness().run(session))
    finally:
        server.should_exit = True
    assert result.report == {"status": "done", "summary": "Wrote hello.txt"}
    assert errors == [] and (sb.work / "hello.txt").read_text() == "hi from anton\n"
    warnings = [event[1] for event in progress if event[0] == "warn"]
    assert len(warnings) == 1, warnings  # one line for the request that was retried, not one per try
    assert warnings[0].startswith("Model provider unavailable (503); retried for ")
    assert warnings[0].endswith(" s, then succeeded")
    guard = provider_guard(provider)
    assert guard.active == 0
    assert len(guard._sends) == len(requests) + 2  # each refused send took a permit of its own, and gave it back


def test_a_provider_refusal_ends_an_anton_session_with_its_cause(tmp_path, monkeypatch):
    # HTTP 402 through the real SDK: never retried here, and the failure itself says the provider refused.
    monkeypatch.setattr(UsageLedger, "persist", lambda self: None)
    monkeypatch.setattr(inference, "RETRY_WAIT_CAP_SECONDS", 0.01)
    port = _free_port()
    requests: list = []
    server = _fake_server(port, requests, errors=[402] * 100)
    progress = []
    try:
        provider = ProviderConfig("openai", f"http://127.0.0.1:{port}/v1", "sk-test", "Fake")
        session = HarnessSession(run_id="r", role="builder", system="You are a builder.", prompt="Create hello.txt",
                                 sandbox=_sandbox(tmp_path), model="fake-model", client=ModelClient(provider),
                                 ledger=UsageLedger(run_id="r", workspace_settings={}), report_schema=BUILDER_REPORT,
                                 writable=True, max_turns=2, deadline=time.monotonic() + 60,
                                 on_progress=lambda *event: progress.append(event))
        with pytest.raises(HarnessFailed) as failed:
            asyncio.run(AntonHarness().run(session))
    finally:
        server.should_exit = True
    assert failed.value.cause == "provider", str(failed.value)
    assert requests == []  # no request was served
    assert not [event for event in progress if event[0] == "warn"]  # the retry loop never waited on a refusal
    assert provider_guard(provider).active == 0


def test_an_overload_marked_not_worth_retrying_ends_an_anton_session_as_transient(tmp_path, monkeypatch):
    # 12 Nov 2026: a gateway's overload marked "x-should-retry: false" was taken for a billing refusal, and the
    # planner's failure held the whole workspace until a person resumed it. An overload is retried, and when it
    # outlasts the retries the session fails as transient: the work is redone later, nothing is held.
    monkeypatch.setattr(UsageLedger, "persist", lambda self: None)
    monkeypatch.setattr(inference, "RETRY_WAIT_CAP_SECONDS", 0.01)
    monkeypatch.setattr(inference, "RETRY_BUDGET_SECONDS", 0.2)
    port = _free_port()
    requests: list = []
    overloaded = (529, {"x-should-retry": "false"}, {"error": {"type": "overloaded_error", "message": "Overloaded"}})
    server = _fake_server(port, requests, errors=[overloaded] * 1000)
    try:
        provider = ProviderConfig("openai", f"http://127.0.0.1:{port}/v1", "sk-test", "Fake")
        session = HarnessSession(run_id="r", role="builder", system="You are a builder.", prompt="Create hello.txt",
                                 sandbox=_sandbox(tmp_path), model="fake-model", client=ModelClient(provider),
                                 ledger=UsageLedger(run_id="r", workspace_settings={}), report_schema=BUILDER_REPORT,
                                 writable=True, max_turns=2, deadline=time.monotonic() + 60)
        with pytest.raises(HarnessFailed) as failed:
            asyncio.run(AntonHarness().run(session))
    finally:
        server.should_exit = True
    assert failed.value.cause == "transient", str(failed.value)
    assert requests == [] and provider_guard(provider).active == 0


@pytest.mark.parametrize("seconds_left", [300, 60])
def test_anton_tool_results_warn_near_the_time_limit_and_only_finish_runs_past_it(tmp_path, monkeypatch, seconds_left):
    monkeypatch.setattr(UsageLedger, "persist", lambda self: None)
    port = _free_port()
    requests: list = []
    server = _fake_server(port, requests)
    try:
        sb = _sandbox(tmp_path)
        provider = ProviderConfig("openai", f"http://127.0.0.1:{port}/v1", "sk-test", "Fake")
        session = HarnessSession(run_id="r", role="builder", system="You are a builder.", prompt="Create hello.txt",
                                 sandbox=sb, model="fake-model", client=ModelClient(provider),
                                 ledger=UsageLedger(run_id="r", workspace_settings={}), report_schema=BUILDER_REPORT,
                                 writable=True, deadline=time.monotonic() + seconds_left)
        result = asyncio.run(AntonHarness().run(session))
    finally:
        server.should_exit = True
    # finish is never refused: the report is how a session that is out of budget ends.
    assert result.report == {"status": "done", "summary": "Wrote hello.txt"}
    # What the agent was shown for its two tool calls (write_file, then read_image) when it reported.
    outputs = [str(item["output"]) for item in requests[-1]["input"]
               if isinstance(item, dict) and item.get("type") == "function_call_output"]
    assert len(outputs) == 2
    if seconds_left == 300:  # under ten minutes: the tool ran, and its result says how much is left
        assert (sb.work / "hello.txt").read_text() == "hi from anton\n"
        assert "Budget: about 4 min and 79 turns are left in this session. Wrap up" in outputs[0]
        # read_image returns an image and a line of text, not a string: the notice is added to its text.
        assert "Loaded .factory/screen.jpg" in outputs[1]
        assert "turns are left in this session. Wrap up" in outputs[1]
    else:  # under 90 seconds: nothing but finish runs
        assert not (sb.work / "hello.txt").exists()
        assert all("so this tool did not run. Call finish now" in output for output in outputs)


def test_a_run_is_not_paused_by_antons_per_turn_token_ceiling(tmp_path, monkeypatch):
    # Reworks of large PRs reread a big cached context on every call. Anton's own ceiling (1.25M tokens a turn, cache
    # reads at full weight) stopped those turns to ask a user who isn't there; the run's ledger bounds spend instead.
    monkeypatch.setattr(UsageLedger, "persist", lambda self: None)
    port = _free_port()
    requests: list = []
    # 600k a call, nearly all cached: past Anton's ceiling by the second call. gpt-4.1's 1M window keeps Anton's
    # context-pressure compaction (0.7 of the window) out of the way.
    server = _fake_server(port, requests, input_tokens=600_000, cached_tokens=590_000)
    try:
        provider = ProviderConfig("openai", f"http://127.0.0.1:{port}/v1", "sk-test", "Fake")
        ledger = UsageLedger(run_id="r", workspace_settings={}, max_tokens=6_000_000)
        session = HarnessSession(run_id="r", role="builder", system="You are a builder.", prompt="Create hello.txt",
                                 sandbox=_sandbox(tmp_path), model="gpt-4.1", client=ModelClient(provider),
                                 ledger=ledger, report_schema=BUILDER_REPORT, writable=True, max_turns=10)
        result = asyncio.run(AntonHarness().run(session))
    finally:
        server.should_exit = True
    assert result.report == {"status": "done", "summary": "Wrote hello.txt"}
    assert ledger.input_tokens >= 1_800_000
    sent = json.dumps(requests)
    assert "counts against the user's plan" not in sent  # Anton's hand-back notice at the ceiling
    assert "Continue. When the work is complete" not in sent  # the agent never stopped short of finishing
