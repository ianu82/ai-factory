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

from aifactory.harness.anton import AntonHarness  # noqa: E402
from aifactory.harness.base import HarnessSession  # noqa: E402
from aifactory.harness.schemas import BUILDER_REPORT  # noqa: E402
from aifactory.inference import ModelClient, ProviderConfig, UsageLedger  # noqa: E402
from aifactory.sandbox import LocalSandbox  # noqa: E402


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _fake_server(port: int, requests: list):
    import uvicorn

    app = FastAPI()

    def response(output, n):
        return {"id": f"resp_{n}", "object": "response", "created_at": int(time.time()), "status": "completed",
                "model": "fake-model", "output": output, "parallel_tool_calls": True, "tool_choice": "auto", "tools": [],
                "usage": {"input_tokens": 100, "output_tokens": 10, "total_tokens": 110,
                          "input_tokens_details": {"cached_tokens": 0}, "output_tokens_details": {"reasoning_tokens": 0}}}

    @app.post("/v1/responses")
    async def responses(request: Request):
        body = await request.json()
        requests.append(body)
        outputs = [i for i in body.get("input", []) if isinstance(i, dict) and i.get("type") == "function_call_output"]
        n = len(requests)
        if not outputs:
            call = {"type": "function_call", "id": "fc_1", "call_id": "call_1", "name": "write_file", "status": "completed",
                    "arguments": json.dumps({"path": "hello.txt", "content": "hi from anton\n"})}
        elif len(outputs) == 1:
            call = {"type": "function_call", "id": "fc_image", "call_id": "call_image", "name": "read_image",
                    "status": "completed", "arguments": json.dumps({"file_path": ".factory/screen.jpg"})}
        else:
            call = {"type": "function_call", "id": f"fc_{n}", "call_id": f"call_{n}", "name": "finish", "status": "completed",
                    "arguments": json.dumps({"status": "done", "summary": "Wrote hello.txt"})}
        return response([call], n)

    server = uvicorn.Server(uvicorn.Config(app, host="127.0.0.1", port=port, log_level="error"))
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()
    for _ in range(100):
        if server.started:
            break
        time.sleep(0.05)
    return server


def test_anton_harness_runs_factory_tools_and_returns_validated_report(tmp_path, monkeypatch):
    monkeypatch.setattr(UsageLedger, "persist", lambda self: None)
    port = _free_port()
    requests: list = []
    server = _fake_server(port, requests)
    try:
        sb = LocalSandbox("anton", root=tmp_path).create()
        sb.work.mkdir()
        import io
        from PIL import Image

        image = io.BytesIO()
        Image.new("RGB", (2, 2), "red").save(image, format="PNG")
        (sb.work / ".factory").mkdir()
        (sb.work / ".factory" / "screen.jpg").write_bytes(image.getvalue())
        provider = ProviderConfig("openai", f"http://127.0.0.1:{port}/v1", "sk-test", "Fake")
        ledger = UsageLedger(run_id="r", workspace_settings={})
        session = HarnessSession(run_id="r", role="builder", system="You are a builder.", prompt="Create hello.txt",
                                 sandbox=sb, model="fake-model", client=ModelClient(provider), ledger=ledger,
                                 report_schema=BUILDER_REPORT, writable=True, max_turns=10)
        result = asyncio.run(AntonHarness().run(session))
    finally:
        server.should_exit = True
    assert result.report == {"status": "done", "summary": "Wrote hello.txt"}
    assert (sb.work / "hello.txt").read_text() == "hi from anton\n"
    assert ledger.input_tokens >= 200 and ledger.calls >= 2
    tool_names = {t.get("name") for t in requests[0].get("tools", [])}
    assert {"write_file", "finish", "run", "read_image"} <= tool_names
    assert not tool_names & {"scratchpad", "web_search", "web_fetch"}

    # The actual ChatSession dispatch must use the replacement and pass vision
    # content to the provider, not just advertise an otherwise unreachable tool.
    assert any("data:image/png;base64," in json.dumps(body) for body in requests[2:])
