"""Codex CLI → Factory gateway → fake upstream (runs only where the Codex CLI is installed)."""

from __future__ import annotations

import asyncio
import json
import shutil
import socket
import threading
import time

import pytest

if not shutil.which("codex"):
    pytest.skip("Codex CLI not installed", allow_module_level=True)

from fastapi import FastAPI, Request  # noqa: E402
from fastapi.responses import StreamingResponse  # noqa: E402

from aifactory import gateway  # noqa: E402
from aifactory.db import session_scope  # noqa: E402
from aifactory.harness.base import HarnessSession  # noqa: E402
from aifactory.harness.codex import CodexHarness  # noqa: E402
from aifactory.harness.schemas import BUILDER_REPORT  # noqa: E402
from aifactory.inference import ModelClient, ProviderConfig, UsageLedger  # noqa: E402
from aifactory.models import Organization, Outcome, Run, Workspace  # noqa: E402
from aifactory.sandbox import LocalSandbox  # noqa: E402


def _port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _serve(app, port):
    import uvicorn

    server = uvicorn.Server(uvicorn.Config(app, host="127.0.0.1", port=port, log_level="error"))
    threading.Thread(target=server.run, daemon=True).start()
    for _ in range(200):
        if server.started:
            break
        time.sleep(0.05)
    return server


def _upstream(seen: list):
    app = FastAPI()
    report = json.dumps({"status": "done", "summary": "Nothing to change", "tests": [], "criteria": [], "blocked_reason": ""})

    @app.get("/v1/models")
    async def models():
        return {"object": "list", "data": [{"id": "gpt-terra", "object": "model"}]}

    @app.post("/v1/responses")
    async def responses(request: Request):
        body = await request.json()
        seen.append({"auth": request.headers.get("authorization"), "model": body.get("model"),
                     "client_metadata": "client_metadata" in body, "stream": body.get("stream")})
        item = {"type": "message", "id": "msg_1", "role": "assistant", "status": "completed",
                "content": [{"type": "output_text", "text": report, "annotations": []}]}
        usage = {"input_tokens": 321, "output_tokens": 45, "total_tokens": 366,
                 "input_tokens_details": {"cached_tokens": 0}, "output_tokens_details": {"reasoning_tokens": 0}}
        resp = {"id": "resp_1", "object": "response", "status": "completed", "model": body.get("model"), "output": [item],
                "usage": usage}

        async def events():
            for event in (
                {"type": "response.created", "response": {**resp, "status": "in_progress", "output": []}},
                {"type": "response.output_item.added", "output_index": 0, "item": {**item, "status": "in_progress", "content": []}},
                {"type": "response.output_text.delta", "output_index": 0, "content_index": 0, "item_id": "msg_1", "delta": report},
                {"type": "response.output_item.done", "output_index": 0, "item": item},
                {"type": "response.completed", "response": resp},
            ):
                yield f"event: {event['type']}\ndata: {json.dumps(event)}\n\n"

        if body.get("stream"):
            return StreamingResponse(events(), media_type="text/event-stream")
        return resp

    return app


def test_codex_reaches_models_only_through_the_gateway(tmp_path, monkeypatch):
    monkeypatch.setattr(UsageLedger, "persist", lambda self: None)
    from aifactory.app import create_app

    seen: list = []
    up_port, gw_port = _port(), _port()
    upstream = _serve(_upstream(seen), up_port)
    factory = _serve(create_app(start_orchestrator=False), gw_port)
    try:
        with session_scope() as s:
            org = Organization(name="o")
            s.add(org)
            s.flush()
            ws = Workspace(org_id=org.id, name="w", slug="w", settings={})
            s.add(ws)
            s.flush()
            o = Outcome(workspace_id=ws.id, number=1, title="t", repo_ids=[])
            s.add(o)
            s.flush()
            run = Run(workspace_id=ws.id, outcome_id=o.id, role="builder", harness="codex", model="gpt-terra")
            s.add(run)
            s.flush()
            ws_id, run_id = ws.id, run.id
        provider = ProviderConfig("mindshub", f"http://127.0.0.1:{up_port}/v1", "mdb_real.secret", "MindsHub")
        ledger = UsageLedger(run_id=run_id, workspace_settings={})
        token = gateway.issue(gateway.GatewayGrant(run_id=run_id, workspace_id=ws_id, model="gpt-terra",
                                                   provider=provider, ledger=ledger))
        sb = LocalSandbox("codex", root=tmp_path).create()
        sb.work.mkdir()
        (sb.work / "README.md").write_text("demo\n")
        session = HarnessSession(run_id=run_id, role="builder", system="You are a builder.", prompt="Inspect and report.",
                                 sandbox=sb, model="gpt-terra", client=ModelClient(provider), ledger=ledger,
                                 report_schema=BUILDER_REPORT, writable=True, max_turns=5,
                                 gateway_url=f"http://127.0.0.1:{gw_port}/gateway/v1", gateway_token=token)
        result = asyncio.run(CodexHarness().run(session))
    finally:
        upstream.should_exit = True
        factory.should_exit = True
    assert result.report["status"] == "done"
    assert seen and all(r["auth"] == "Bearer mdb_real.secret" for r in seen), seen
    assert all(r["model"] == "gpt-terra" and not r["client_metadata"] for r in seen)
    assert ledger.input_tokens >= 321 and ledger.output_tokens >= 45
    env_dump = (sb.home / ".codex" / "config.toml").read_text()
    assert "mdb_real.secret" not in env_dump
