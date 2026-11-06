"""Exercise the pinned real app-server against a credential-free loopback peer."""

import http.server
from contextlib import ExitStack
import json
import os
from pathlib import Path
import queue
import threading
import time

import pytest

from auto_mindsdb_factory.codex_actions import CodexActionError, codex_action
from auto_mindsdb_factory.codex_stream import codex_sse
from auto_mindsdb_factory.codex_request import codex_request
from auto_mindsdb_factory.codex_process import open_codex_process
from auto_mindsdb_factory.product_agent import REPORT_SCHEMA


MODEL = "gpt-6.1-terra"
REPORT = {"status": "unverified", "summary": "Local transport fixture",
          "criteria": [{"id": "probe", "status": "unverified", "evidence": "No live inference"}], "findings": []}


@pytest.mark.parametrize("permitted", [True, False], ids=["report-callback", "builtin-rejected-before-stream"])
def test_real_codex_response_is_gated_before_tool_execution(permitted):
    configured = os.environ.get("AI_FACTORY_CODEX_TEST_BINARY")
    binary = Path(configured or "src/auto_mindsdb_factory/codex_bridge/node_modules/.bin/codex").resolve()
    if not configured and not binary.is_file():
        pytest.skip("Pinned Codex CLI unavailable; dedicated Codex CI requires it")
    assert binary.is_file()
    messages, requests, decisions = queue.Queue(), [], []

    class Peer(http.server.BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass

        def do_POST(self):
            requests.append(json.loads(self.rfile.read(int(self.headers["Content-Length"]))))
            assert self.path == "/v1/responses"
            assert requests[-1]["model"] == MODEL
            assert requests[-1]["reasoning"]["effort"] == "high"
            prepared = codex_request(requests[-1], model=MODEL, effort="high", max_output_tokens=10000, remaining_tokens=50000)
            assert prepared.pop("max_output_tokens") == 10000 and prepared == requests[-1]
            code = "text(await tools.finish(" + json.dumps(REPORT) + "))" if permitted else 'text(await tools.apply_patch("forbidden"))'
            call = {"type": "custom_tool_call", "id": "ctc_probe", "call_id": "call_probe",
                    "name": "exec", "namespace": "functions", "input": code, "status": "completed"}
            response = {"id": "resp_probe", "object": "response", "created_at": 0,
                        "status": "completed", "model": MODEL, "output": [
                            {"type": "reasoning", "id": "rs_probe", "summary": [], "encrypted_content": "opaque"},
                            {"type": "message", "id": "msg_probe", "role": "assistant", "status": "completed",
                             "content": [{"type": "output_text", "text": "Bounded transport fixture", "annotations": []}]}, call],
                        "usage": {"input_tokens": 0, "output_tokens": 0, "total_tokens": 0}}
            try:
                assert len(requests) == 1
                action = codex_action(response, model=MODEL)
                out = codex_sse(response, model=MODEL)
            except CodexActionError:
                decisions.append("rejected")
                out = b'{"error":{"message":"Factory policy stopped the offline response","type":"invalid_request_error"}}'
                self.send_response(400); self.send_header("Content-Type", "application/json")
            else:
                decisions.append("allowed")
                assert action == ("finish", REPORT, "call_probe")
                self.send_response(200); self.send_header("Content-Type", "text/event-stream")
            self.send_header("Content-Length", str(len(out))); self.end_headers(); self.wfile.write(out)

    peer = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Peer)
    server_thread = threading.Thread(target=peer.serve_forever, daemon=True)
    server_thread.start()
    process, reader = None, None
    ownership = ExitStack()
    reader_errors = []
    try:
        owned = ownership.enter_context(open_codex_process(f"http://127.0.0.1:{peer.server_port}/v1",
            deadline=time.monotonic() + 20, binary=binary))
        process, work = owned.process, owned.work

        def read_messages():
            try:
                while True:
                    try:
                        line = process.stdout.readline()
                    except ValueError:
                        if process.poll() is None: raise
                        break
                    if not line: break
                    messages.put(json.loads(line))
            except Exception as error:
                reader_errors.append(type(error).__name__)

        reader = threading.Thread(target=read_messages, daemon=True)
        reader.start()

        def send(message):
            process.stdin.write(json.dumps(message) + "\n"); process.stdin.flush()

        send({"id": 1, "method": "initialize", "params": {
            "clientInfo": {"name": "factory_transport_fixture", "version": "1"}, "capabilities": {"experimentalApi": True}}})
        callbacks, terminal, settings = [], None, None
        deadline = time.monotonic() + 20
        while time.monotonic() < deadline:
            try:
                message = messages.get(timeout=.1)
            except queue.Empty:
                assert process.poll() is None, "App-server exited before the observed outcome"
                continue
            assert "error" not in message, message
            method = message.get("method")
            if message.get("id") == 1:
                send({"method": "initialized", "params": {}})
                send({"id": 2, "method": "thread/start", "params": {
                    "model": MODEL, "modelProvider": "factory_guarded", "allowProviderModelFallback": False,
                    "ephemeral": True, "cwd": str(work), "approvalPolicy": "never", "sandbox": "read-only",
                    "baseInstructions": "Use the supplied dynamic tool.", "dynamicTools": [{"type": "function",
                    "name": "finish", "description": "Finish the fixture", "inputSchema": REPORT_SCHEMA}]}})
            elif message.get("id") == 2:
                send({"id": 3, "method": "turn/start", "params": {"threadId": message["result"]["thread"]["id"],
                    "model": MODEL, "effort": "high", "input": [{"type": "text", "text": "Call finish."}]}})
            elif method == "thread/settings/updated":
                settings = message["params"]["threadSettings"]
            elif method == "item/tool/call":
                callbacks.append(message["params"]); break
            elif method == "turn/completed":
                terminal = message["params"]["turn"]["status"]; break
        assert len(requests) == 1
        assert settings["model"] == MODEL and settings["effort"] == "high"
        assert settings["sandboxPolicy"] == {"type": "readOnly", "networkAccess": False}
        if permitted:
            assert decisions == ["allowed"] and len(callbacks) == 1
            assert callbacks[0]["tool"] == "finish" and callbacks[0]["arguments"] == REPORT
        else:
            assert decisions == ["rejected"] and not callbacks and terminal == "failed"
    finally:
        try:
            ownership.close()
        finally:
            if reader is not None: reader.join(timeout=2)
            peer.shutdown(); peer.server_close(); server_thread.join(timeout=2)
    assert reader is not None and not reader.is_alive() and not server_thread.is_alive() and not reader_errors
    assert process.poll() is not None and process.stdin.closed and process.stdout.closed
    assert not owned.home.parent.exists()
