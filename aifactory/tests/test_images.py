from __future__ import annotations

import asyncio
import base64
import io
import json
import os
import time

import httpx
import pytest
from PIL import Image

from aifactory.harness.base import HarnessFailed
from aifactory.harness.native import NativeHarness
from aifactory.harness.tools import ToolBox, tool_specs
from aifactory.images import (DiagnosticStream, ImageRedactor, ImageResult, MAX_IMAGE_BYTES,
                              is_image_rejection)
from aifactory.inference import InferenceError, ModelClient, UsageLedger, _error_detail
from aifactory.sandbox import SandboxError, read_bytes, read_image_bytes
from .fakes import call
from .test_harness import make_session


def png():
    out = io.BytesIO()
    Image.new("RGB", (1, 1)).save(out, format="PNG")
    return out.getvalue()


@pytest.mark.parametrize("writable", [False, True])
def test_image_registration(writable):
    assert "view_image" in [t["function"]["name"] for t in tool_specs(writable)]


@pytest.mark.parametrize("fmt,suffix,mime", [("PNG", "png", "image/png"), ("JPEG", "jpg", "image/jpeg"),
                                             ("GIF", "gif", "image/gif"), ("WEBP", "webp", "image/webp")])
def test_formats(tmp_path, fmt, suffix, mime):
    session, _, sb = make_session(tmp_path, None)
    out = io.BytesIO()
    Image.new("RGB", (1, 1)).save(out, format=fmt)
    data = out.getvalue()
    (sb.work / f"a.{suffix}").write_bytes(data)
    result = asyncio.run(ToolBox(session).call("view_image", {"path": f"a.{suffix}"}))
    assert isinstance(result, ImageResult)
    assert result.mime_type == mime
    assert base64.b64decode(result.data) == data
    assert result.description == f"a.{suffix} ({mime}, {len(data)} bytes)"
    assert result.data not in repr(result)


def test_limits_refusals_and_bounded_local_reads(tmp_path, monkeypatch):
    session, _, sb = make_session(tmp_path, None)
    (sb.work / "a.png").write_bytes(png())
    (sb.work / "fake.jpg").write_bytes(png())
    (sb.work / "bad.png").write_bytes(b"not an image")
    (sb.work / "folder").mkdir()
    outside = tmp_path / "external.png"
    outside.write_bytes(png())
    (sb.work / "link.png").symlink_to(outside)
    os.mkfifo(sb.work / "fifo.png")
    box = ToolBox(session)
    for path in ("missing.png", "folder", "fifo.png", "../external.png", str(outside), "link.png", "bad.png", "fake.jpg"):
        result = asyncio.run(box.call("view_image", {"path": path}))
        assert isinstance(result, str) and result.startswith("Error:")
        assert base64.b64encode(png()).decode() not in result
    boundary = png() + b"\0" * (MAX_IMAGE_BYTES - len(png()))
    (sb.work / "boundary.png").write_bytes(boundary)
    assert isinstance(asyncio.run(box.call("view_image", {"path": "boundary.png"})), ImageResult)
    (sb.work / "large.png").write_bytes(boundary + b"x")
    assert "large" in asyncio.run(box.call("view_image", {"path": "large.png"}))
    for _ in range(11):
        assert isinstance(asyncio.run(box.call("view_image", {"path": "a.png"})), ImageResult)
    assert "12 images" in asyncio.run(box.call("view_image", {"path": "a.png"}))
    assert "12 images" in asyncio.run(ToolBox(session).call("view_image", {"path": "a.png"}))
    from dataclasses import replace
    fresh = replace(session, image_deliveries=0, image_redactor=ImageRedactor())
    assert isinstance(asyncio.run(ToolBox(fresh).call("view_image", {"path": "a.png"})), ImageResult)
    # A hard guard against loading the full file before slicing.
    from pathlib import Path
    monkeypatch.setattr(Path, "read_bytes", lambda _: pytest.fail("unbounded read"))
    assert asyncio.run(read_bytes(sb, "large.png", 8)) == png()[:8]
    assert asyncio.run(read_bytes(sb, "large.png", 1, True)) == b"x"
    assert len(asyncio.run(read_image_bytes(sb, "boundary.png", MAX_IMAGE_BYTES))) == MAX_IMAGE_BYTES


def test_vm_reader_uses_remote_descriptor_checks(tmp_path):
    from .test_vm import FakeBridge
    from aifactory.vm import RailwayVMSandbox

    root = tmp_path / "vm"
    root.mkdir()
    bridge = FakeBridge(root)
    vm = RailwayVMSandbox("images", bridge=bridge)
    vm.vm_id = "fake"
    vm._closed = False
    (root / "work").mkdir()
    (root / "work" / "a.png").write_bytes(png())
    (root / "work" / "link.png").symlink_to(tmp_path / "outside.png")
    (tmp_path / "outside.png").write_bytes(png())
    assert asyncio.run(read_image_bytes(vm, "a.png", MAX_IMAGE_BYTES)) == png()
    for path in ("../outside.png", "link.png", str(tmp_path / "outside.png"), "missing.png", "."):
        with pytest.raises(SandboxError):
            asyncio.run(read_image_bytes(vm, path, MAX_IMAGE_BYTES))
    (root / "work" / "boundary.png").write_bytes(png() + b"x" * (MAX_IMAGE_BYTES - len(png())))
    assert len(asyncio.run(read_image_bytes(vm, "boundary.png", MAX_IMAGE_BYTES))) == MAX_IMAGE_BYTES
    (root / "work" / "big.png").write_bytes(png() + b"x" * MAX_IMAGE_BYTES)
    with pytest.raises(SandboxError):
        asyncio.run(read_image_bytes(vm, "big.png", MAX_IMAGE_BYTES))
    assert "read" not in bridge.calls


@pytest.mark.parametrize("wire", ["responses", "chat"])
@pytest.mark.parametrize("reject", [False, True])
def test_native_wire_images_batch_order_and_validated_retry(tmp_path, monkeypatch, wire, reject):
    from aifactory import inference

    monkeypatch.setattr(UsageLedger, "persist", lambda _: None)
    session, _, sb = make_session(tmp_path, None)
    (sb.work / "a.png").write_bytes(png())
    requests = []
    warnings = []
    session.on_progress = lambda *args: warnings.append(args)
    payload = base64.b64encode(png()).decode()
    summary = "Verified commit d939b2e28844b6b117251021bf8d35e5e916d1a9 https://example.com/repository"

    def reply(calls):
        if wire == "responses":
            return {"output": [{"type": "function_call", "call_id": c["id"], **c["function"]} for c in calls]}
        return {"choices": [{"message": {"tool_calls": calls}}]}

    def upstream(request):
        body = json.loads(request.content)
        requests.append(body)
        if len(requests) == 1:
            return httpx.Response(200, json=reply([call("view_image", {"path": "a.png"}, 1),
                                                   call("read_file", {"path": "a.txt"}, 2)]))
        messages = body["input"] if wire == "responses" else body["messages"]
        image_messages = [m for m in messages if isinstance(m.get("content"), list) and any(
            p["type"] in ("input_image", "image_url") for p in m["content"])]
        if len(requests) == 2:
            assert len(image_messages) == 1
            parts = image_messages[0]["content"]
            image = parts[1]
            url = image["image_url"] if wire == "responses" else image["image_url"]["url"]
            assert url == "data:image/png;base64," + payload
            assert parts[0]["text"] == f"a.png (image/png, {len(png())} bytes)"
            index = messages.index(image_messages[0])
            assert all(m.get("type") == "function_call_output" if wire == "responses" else m["role"] == "tool"
                       for m in messages[index - 2:index])
            if reject:
                return httpx.Response(400, json={"error": {"message": "This model does not support images. " + payload}})
        else:
            assert not image_messages
            assert payload not in json.dumps(body)
            assert "files and text only" in json.dumps(body)
        return httpx.Response(200, json=reply([call("finish", {"status": "done", "summary": summary})]))

    real = httpx.AsyncClient
    monkeypatch.setattr(inference.httpx, "AsyncClient", lambda **kw: real(transport=httpx.MockTransport(upstream), **kw))
    session.client = ModelClient(session.client.provider, wire=wire, max_retries=0)
    result = asyncio.run(NativeHarness().run(session))
    assert result.report["summary"] == summary
    assert len(requests) == (3 if reject else 2)
    assert len([w for w in warnings if w[0] == "image_fallback"]) == int(reject)
    assert payload not in json.dumps(result.transcript) + result.log + str(warnings)


@pytest.mark.parametrize("text,expected", [
    ("Invalid image_url in tool definition", False),
    ("Invalid image_url in function definition", False),
    ("Invalid tool schema: property image_url is not allowed in function parameters", False),
    ("This model does not support images.", True),
    ("This model doesn't support image inputs", True),
    ("Invalid content type. image_url is only supported by certain models.", True),
    ("This model does not support images larger than 2048 pixels", False),
])
def test_shared_rejection_rule(text, expected):
    assert is_image_rejection(InferenceError(text, status=400)) == expected
    for status in (None, 401, 402, 403, 429, 500, 502):
        assert not is_image_rejection(InferenceError(text, status=status))


def test_redaction_before_clip_and_stream_boundaries():
    payload = base64.b64encode(png()).decode()
    redactor = ImageRedactor()
    redactor.add(payload)
    response = httpx.Response(400, json={"error": {"message": "x " * 246 + payload}})
    detail = _error_detail(response, redactor)
    assert payload[:8] not in detail
    stream = DiagnosticStream(redactor)
    stream.feed(payload[:-10])
    stream.feed(payload[-10:])
    assert not any(c in stream.finish() for c in set(payload))
    for _ in range(100):
        stream.feed("ordinary output\n" * 1000)
        assert len(stream.tail) <= stream.LIMIT
        assert len(stream.pending) <= stream.WINDOW
    sha = "d939b2e28844b6b117251021bf8d35e5e916d1a9"
    text = f"Verified commit {sha} https://example.com/{sha}"
    assert redactor(text) == text
    assert ImageRedactor()(payload) == payload


def test_anchor_collisions_keep_every_payload_and_large_fragment():
    redactor = ImageRedactor()
    first = "A" * 32 + base64.b64encode(os.urandom(600)).decode()
    second = "A" * 32 + base64.b64encode(os.urandom(600)).decode()
    redactor.add(first)
    redactor.add(second)
    assert len(redactor.anchors["A" * 32]) == 2
    for payload in (first, second):
        fragment = payload[12:108]
        assert fragment not in redactor(fragment)
    stream = DiagnosticStream(redactor)
    stream.feed(first[:-10])
    stream.feed(first[-10:])
    assert first[:32] not in stream.finish()


def test_large_matcher_and_redaction_budgets():
    import resource

    redactor = ImageRedactor()
    payload = base64.b64encode(os.urandom(MAX_IMAGE_BYTES)).decode()
    before = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    started = time.perf_counter()
    redactor.add(payload)
    assert time.perf_counter() - started < 0.5
    extra = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss - before
    assert extra < 64 * 1024  # Linux KiB
    fragment = payload[100:300100]
    text = "ordinary output " * 24000 + fragment + " more text " * 24000
    started = time.perf_counter()
    assert fragment not in redactor(text)
    assert time.perf_counter() - started < 0.5


@pytest.mark.parametrize("upstream,retry", [(400, True), (415, True), (422, True), (401, False),
                                            (403, False), (429, False), (500, False), (None, False)])
def test_codex_startup_images_original_status_and_files_only_retry(tmp_path, monkeypatch, upstream, retry):
    from aifactory.harness.codex import CodexHarness
    from aifactory.sandbox import ExecResult

    session, _, sb = make_session(tmp_path, None)
    session.gateway_url = "http://gateway/v1"
    session.gateway_token = "token"
    session.image_paths = ["missing.png", "a.png"]
    (sb.work / "a.png").write_bytes(png())
    payload = base64.b64encode(png()).decode()
    warnings, commands = [], []
    session.on_progress = lambda *args: warnings.append(args)
    monkeypatch.setattr(CodexHarness, "availability", lambda _: (True, ""))

    async def execute(command, *, on_output, **kwargs):
        commands.append(command)
        if len(commands) == 1:
            assert "--image" in command and str(sb.work / "a.png") in command
            assert "missing.png" not in command
            original = f"Provider returned {upstream}: " if upstream is not None else ""
            text = "HTTP 502 " + original + "This model does not support images"
            line = json.dumps({"type": "turn.failed", "error": {"message": text}})
            # Large/invalid records do not hide a following valid failure event.
            on_output(b"x" * (70 * 1024))
            on_output(b"x" * 100 + b"\n[]\n" + line.encode() + b"\n")
            return ExecResult(command, 1, line, 0)
        assert "--image" not in command
        (sb.work / ".factory/codex/last.json").write_text(json.dumps({"status": "done", "summary": "ok"}))
        on_output(b'{"type":"turn.completed"}\n')
        return ExecResult(command, 0, "ok", 0)

    monkeypatch.setattr(sb, "exec", execute)
    if retry:
        result = asyncio.run(CodexHarness().run(session))
        assert result.report["status"] == "done"
        assert payload not in result.log
    else:
        with pytest.raises(HarnessFailed) as error:
            asyncio.run(CodexHarness().run(session))
        assert payload not in str(error.value)
    assert len(commands) == (2 if retry else 1)
    assert len([w for w in warnings if w[0] == "image_fallback"]) == int(retry)


def test_runtime_fallback_is_one_persisted_run_warning(tmp_path, monkeypatch):
    from aifactory.pipeline import runtime
    from aifactory.models import Event, Organization, Outcome, Workspace
    from aifactory.db import session_scope
    from aifactory.harness.base import HarnessResult
    from aifactory.sandbox import LocalSandbox

    with session_scope() as s:
        org = Organization(name="Images")
        s.add(org)
        s.flush()
        ws = Workspace(name="Images", slug="images", org_id=org.id)
        s.add(ws)
        s.flush()
        workspace_id = ws.id
        outcome = Outcome(workspace_id=ws.id, title="Images", number=1)
        s.add(outcome)
        s.flush()
        outcome_id = outcome.id
    from aifactory.inference import ProviderConfig
    monkeypatch.setattr(runtime, "resolve_provider", lambda *a, **k: ProviderConfig("openai", "http://x", "key", "test"))

    class Harness:
        label = "Test"

        async def run(self, session):
            session.on_progress("thought", "chatter", {})
            for _ in range(4):
                session.on_progress("image_fallback", "untrusted payload", {"content": "do not persist"})
            return HarnessResult(report={"status": "done", "summary": "ok"}, turns=1)

    monkeypatch.setattr(runtime, "get_harness", lambda _: Harness())
    sb = LocalSandbox("warning", root=tmp_path).create()
    sb.work.mkdir()
    spec = runtime.SessionSpec(workspace_id=workspace_id, outcome_id=outcome_id, task_id=None,
                               role="builder", harness="native", prompt="go", system="sys", sandbox=sb)
    run, result, error = asyncio.run(runtime.run_agent(spec, asyncio.Event()))
    assert not error and result
    with session_scope() as s:
        rows = s.query(Event).filter(Event.run_id == run.id, Event.kind == "agent_image_fallback").all()
        assert len(rows) == 1 and rows[0].level == "warn"
        assert "untrusted" not in rows[0].message
        assert rows[0].data == {"role": "builder"}


@pytest.mark.parametrize("status,text", [
    (403, "This model does not support images."),
    (429, "This model does not support images."),
    (500, "This model does not support images."),
    (400, "Invalid tool schema: property image_url is not allowed in function parameters"),
])
def test_native_does_not_hide_unrelated_errors(tmp_path, status, text):
    session, client, sb = make_session(tmp_path, None)
    (sb.work / "a.png").write_bytes(png())
    calls = 0
    warnings = []
    session.on_progress = lambda *args: warnings.append(args)

    async def chat(**kwargs):
        nonlocal calls
        calls += 1
        if calls == 1:
            return {"tool_calls": [call("view_image", {"path": "a.png"})]}
        raise InferenceError(text, status=status)

    client.chat = chat
    with pytest.raises(HarnessFailed):
        asyncio.run(NativeHarness().run(session))
    assert calls == 2
    assert not session.images_disabled
    assert not any(w[0] == "image_fallback" for w in warnings)


def test_two_large_images_with_address_space_cap():
    import subprocess
    import sys

    # A separate process avoids changing the test runner's address-space policy.
    script = '''
import base64, os, resource
from aifactory.images import ImageRedactor
resource.setrlimit(resource.RLIMIT_AS, (1536 * 1024 * 1024, 1536 * 1024 * 1024))
r = ImageRedactor()
for _ in range(2):
    r.add(base64.b64encode(os.urandom(5 * 1024 * 1024)).decode())
assert len(r.payloads) == 2
'''
    result = subprocess.run([sys.executable, "-c", script], capture_output=True, text=True, timeout=15)
    assert result.returncode == 0, result.stderr


def test_stream_redacts_large_split_payload_and_preserves_sha_urls():
    payload = base64.b64encode(os.urandom(300_000)).decode()
    redactor = ImageRedactor()
    redactor.add(payload)
    stream = DiagnosticStream(redactor)
    stream.feed(payload[:-10])
    stream.feed(payload[-10:])
    output = stream.finish()
    assert all(character == "*" for character in output)
    text = "https://example.com/" + "a" * 600 + " " + "d" * 600
    assert redactor(text) == text


@pytest.mark.parametrize("status", [400, 403, 500])
def test_gateway_preserves_original_status_and_redacts_before_clip(monkeypatch, status):
    from aifactory import gateway
    from aifactory.harness.codex import _provider_status
    from aifactory.inference import ProviderConfig
    from .test_inference_limits import fake_http, request

    ledger = UsageLedger("missing", {})
    redactor = ImageRedactor()
    payload = base64.b64encode(png()).decode()
    grant = gateway.GatewayGrant("r", "ws", "m", ProviderConfig("openai", "http://provider", "key", "test"),
                                 ledger, image_redactor=redactor)
    monkeypatch.setattr(gateway, "_authenticate", lambda _: grant)
    fake_http(monkeypatch, lambda _: httpx.Response(status, json={"error": {
        "message": "x " * 390 + payload + " This model does not support images."}}))
    response = asyncio.run(gateway.proxy("responses", request({"input": [{"role": "user", "content": [
        {"type": "input_image", "image_url": "data:image/png;base64," + payload}]}]})))
    text = response.body.decode()
    assert _provider_status(text) == status
    assert payload[:8] not in text
    assert payload in redactor.payloads


def test_sandbox_redacts_full_output_before_clipping(tmp_path):
    import shlex
    import sys

    session, _, sb = make_session(tmp_path, None)
    (sb.work / "a.png").write_bytes(png())
    box = ToolBox(session)
    image = asyncio.run(box.call("view_image", {"path": "a.png"}))
    script = "import sys; sys.stdout.write('x '*2995 + " + repr(image.data) + " + ' tail'*10000)"
    result = asyncio.run(sb.exec(shlex.quote(sys.executable) + " -c " + shlex.quote(script)))
    assert image.data[:16] not in result.output
    assert image.data[-16:] not in result.output
