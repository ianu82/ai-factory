from __future__ import annotations

import asyncio
import base64
import copy
import json
import random
import re
import textwrap
import time
import tracemalloc

import httpx
import pytest

from aifactory.harness.base import HarnessFailed, HarnessSession
from aifactory.harness.native import NativeHarness
from aifactory.harness.schemas import BUILDER_REPORT
from aifactory.image_input import ImageOutputRedactor, ImagePayloadRedactor, rejects_image_input
from aifactory.inference import BudgetExceeded, InferenceError, ModelClient, ProviderConfig, UsageLedger
from aifactory.model_checks import TEST_IMAGE
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

IMAGE = {"name": "screen.png", "mime_type": "image/png", "content": TEST_IMAGE.split(",")[1]}

CAPABILITIES = [
    ("This model does not support images.", True),
    ("This model does not support image inputs.", True),
    ("This model does not support image input", True),
    ("This model can not support image input", True),
    ("Models do not support image input", True),
    ("Image inputs are not supported by this model.", True),
    ("Vision is not supported for this model", True),
    ("Invalid content type. image_url is only supported by certain models.", True),
    ("This model does not support images larger than 2048 pixels", False),
    ("Too many images: at most 4 per request", False),
    ("Unsupported image format: TIFF", False),
    ("Invalid image data", False),
    ("Invalid tool schema: property image_url is not allowed in function parameters", False),
    ("This model does not support image input; use a supported image format", True),
    ("This model does not support image input. Function calling remains available.", True),
    ("Invalid tool schema: this model does not support images", False),
    ("Base64 image inputs are not supported", False),
]


@pytest.mark.parametrize("message,expected", CAPABILITIES)
@pytest.mark.parametrize("status", [400, 415, 422, 401, 403, 402, 429, 500, None])
def test_shared_image_capability_table(message, expected, status):
    from aifactory.model_checks import rejects_image_input as probe_rule
    from aifactory.inference import rejects_image_input as client_rule

    variants = [message]
    for source, replacements in [
        ("does not", ["doesn't", "does n't", "doesn t"]),
        ("do not", ["don't"]), ("can not", ["can't", "cannot"]),
        ("is not", ["isn't"]), ("are not", ["aren't"]),
    ]:
        variants.extend(message.replace(source, replacement) for replacement in replacements)
    for variant in variants:
        error = InferenceError(variant, status=status)
        assert probe_rule is client_rule is rejects_image_input
        assert rejects_image_input(error) == (expected and status in (400, 415, 422))


@pytest.mark.parametrize("wire", ["responses", "chat"])
def test_native_image_wire_and_files_fallback(tmp_path, monkeypatch, wire):
    from aifactory import inference
    seen, progress = [], []
    monkeypatch.setattr(UsageLedger, "persist", lambda self: None)

    def upstream(request):
        body = json.loads(request.content)
        seen.append(body)
        messages = body["input" if wire == "responses" else "messages"]
        parts = messages[1]["content"]
        assert parts[0] == {"type": "input_text" if wire == "responses" else "text", "text": session.prompt}
        if len(seen) == 1:
            assert parts[1]["type"] == ("input_image" if wire == "responses" else "image_url")
            url = parts[1]["image_url"]
            url = url if wire == "responses" else url["url"]
            assert url == TEST_IMAGE
            assert base64.b64decode(url.split(",")[1]) == base64.b64decode(IMAGE["content"])
            return httpx.Response(400, json={"error": {"message": "This model doesn't support images; " + TEST_IMAGE}})
        assert len(parts) == 1
        args = {"status": "done", "summary": "safe " + IMAGE["content"]}
        if wire == "responses":
            return httpx.Response(200, json={"output": [{"type": "function_call", "call_id": "f", "name": "finish",
                                                        "arguments": json.dumps(args)}]})
        return httpx.Response(200, json={"choices": [{"message": {"tool_calls": [call("finish", args)]}}]})

    real = httpx.AsyncClient
    monkeypatch.setattr(inference.httpx, "AsyncClient", lambda **kw: real(transport=httpx.MockTransport(upstream), **kw))
    session, _, _ = make_session(tmp_path, None, image_inputs=[IMAGE], on_progress=lambda *p: progress.append(p))
    session.prompt = "Inspect .factory/attachments/screen.png (image/png); keep this text"
    session.client = ModelClient(ProviderConfig("openai", "https://test/v1", "k", "T"), wire=wire)
    result = asyncio.run(NativeHarness().run(session))
    assert len(seen) == 2 and session.ledger.calls == 1 and result.turns == 1
    assert progress == [("warning", "The provider does not support image input; continuing with attachment files and text only.", {})]
    assert result.report["summary"] == "safe [image omitted]"
    assert IMAGE["content"] not in json.dumps([progress, result.report, result.transcript, result.log])
    assert "data:image" not in json.dumps([progress, result.report, result.transcript, result.log])


@pytest.mark.parametrize("error", [
    InferenceError("does not support images", status=s) for s in (401, 403, 402, 429, 500)
] + [InferenceError("invalid tool schema", status=400), InferenceError("unreachable", retryable=True),
     BudgetExceeded("does not support images", status=400)])
def test_native_does_not_hide_other_failures(tmp_path, error):
    def fail(_):
        raise error
    session, client, _ = make_session(tmp_path, fail, image_inputs=[IMAGE])
    with pytest.raises(HarnessFailed):
        asyncio.run(NativeHarness().run(session))
    assert len(client.seen) == 1


def test_failed_image_retry_is_once_and_redacted_before_clipping(tmp_path):
    def fail(_):
        raise InferenceError("This model does not support images; " + IMAGE["content"], status=422)
    session, client, _ = make_session(tmp_path, fail, image_inputs=[IMAGE])
    with pytest.raises(HarnessFailed) as error:
        asyncio.run(NativeHarness().run(session))
    assert len(client.seen) == 2
    assert IMAGE["content"] not in str(error.value)


def test_images_do_not_consume_text_trim_budget():
    messages = [{"role": "system", "content": "sys"}, {"role": "user", "content": [
        {"type": "text", "text": "prompt"}, {"type": "image_url", "image_url": {"url": "x" * 600000}}]}]
    messages += [{"role": "tool", "content": "evidence" * 1000}] * 12
    before = copy.deepcopy(messages)
    NativeHarness()._trim(messages)
    assert messages == before


def test_redacted_report_is_validated_before_acceptance(tmp_path, monkeypatch):
    from aifactory.harness import native
    real = native.validate_report
    checked = []
    def validate(role, args):
        checked.append(args)
        # A duplicate-key planner report must be rejected *after* redaction.
        return real(role, args)
    monkeypatch.setattr(native, "validate_report", validate)
    from aifactory.harness.schemas import PLANNER_REPORT
    def finish(_):
        return {"tool_calls": [call("finish", {"summary": "plan", "questions": [], "tasks": [
            {"key": key, "title": "task", "repo": "org/repo", "spec": "work", "acceptance": ["ok"], "depends_on": []}
            for key in (IMAGE["content"], TEST_IMAGE)]})]}
    session, _, _ = make_session(tmp_path, finish, image_inputs=[IMAGE])
    session.role, session.report_schema = "planner", PLANNER_REPORT
    with pytest.raises(HarnessFailed, match="rejected repeatedly"):
        asyncio.run(NativeHarness().run(session))
    assert "Duplicate task key" in real("planner", checked[0])[1]
    assert checked[0]["tasks"][0]["key"] == checked[0]["tasks"][1]["key"] == "[image omitted]"


@pytest.mark.parametrize("width", [53, 76])
def test_wrapped_fragments_stream_and_upstream_truncation(width):
    payload = base64.b64encode(random.Random(7).randbytes(1600000)).decode()
    matcher = ImagePayloadRedactor([{"content": payload}])
    wrapped = "\n".join(textwrap.wrap(payload, width))[:2000000]
    assert matcher.text(wrapped) == "[image omitted]"
    stream = ImageOutputRedactor(matcher)
    output = "".join(stream.feed(wrapped[i:i + 701]) for i in range(0, len(wrapped), 701))
    output += stream.feed("", final=True)
    assert not re.search(r"[A-Za-z0-9+/=]{32}", output)


def test_unknown_wrapped_data_url_is_fully_redacted():
    payload = base64.b64encode(random.Random(1).randbytes(10000)).decode()
    text = "data:image/png;base64," + "\n".join(textwrap.wrap(payload, 76))
    redactor = ImagePayloadRedactor()
    assert redactor.text(text) == "[image omitted]"
    stream = ImageOutputRedactor(redactor)
    output = "".join(stream.feed(text[i:i + 71]) for i in range(0, len(text), 71)) + stream.feed("", final=True)
    assert not re.search(r"[A-Za-z0-9+/=]{32}", output)


@pytest.mark.parametrize("prefix_length", [1, 50, 95])
def test_redaction_precedes_sandbox_head_tail_clipping(tmp_path, prefix_length):
    from aifactory.harness.tools import ToolBox
    payload = base64.b64encode(random.Random(2).randbytes(10000)).decode()
    session, _, sb = make_session(tmp_path, None, image_inputs=[{"content": payload}])
    (sb.work / "payload").write_text(payload)
    command = f"printf '%{6000 - prefix_length}s' x; cat payload"
    chunks = []
    result = asyncio.run(sb.exec(command, output_redactor=ImageOutputRedactor(ImagePayloadRedactor(session.image_inputs)),
                                 on_output=chunks.append))
    output = asyncio.run(ToolBox(session).call("run", {"command": command}))
    for text in (output, result.for_model(), b"".join(chunks).decode()):
        assert "x" + payload[:prefix_length] not in text
        assert payload[:96] not in text and payload[-96:] not in text


def test_image_matcher_performance_budget():
    payload = base64.b64encode(random.Random(3).randbytes(5 * 1024 * 1024)).decode()
    started = time.process_time()
    matcher = ImagePayloadRedactor([{"content": payload}])
    assert time.process_time() - started < 0.5
    tracemalloc.start()
    measured = ImagePayloadRedactor([{"content": payload}])
    assert measured.anchors
    _, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    assert peak < 64 * 1024 * 1024
    wrapped = "\n".join(textwrap.wrap(payload[:2100000], 53))[:2000000]
    started = time.process_time()
    assert matcher.text(wrapped) == "[image omitted]"
    assert time.process_time() - started < 0.5


def test_native_warning_is_one_attributed_warn_event(tmp_path, monkeypatch):
    from aifactory.db import session_scope
    from aifactory.models import Event, Organization, Outcome, Run, Workspace
    from aifactory.pipeline import runtime
    from aifactory.pipeline.runtime import SessionSpec, run_agent

    with session_scope() as db:
        org = Organization(name="Image test")
        db.add(org)
        db.flush()
        ws = Workspace(org_id=org.id, name="Image test", slug="image-test")
        db.add(ws)
        db.flush()
        outcome = Outcome(workspace_id=ws.id, title="See screenshot", brief="Inspect it", number=1)
        db.add(outcome)
        db.flush()
        ws_id, outcome_id = ws.id, outcome.id
    session, _, sb = make_session(tmp_path, None)
    seen = []
    class Client(Script):
        def __init__(self, *args, **kwargs):
            super().__init__(self.respond)
        def respond(self, messages):
            seen.append(copy.deepcopy(messages))
            if len(seen) == 1:
                raise InferenceError("This model doesn't support images", status=400)
            return {"tool_calls": [call("finish", {"status": "done", "summary": "done " + TEST_IMAGE})]}
    async def prepare(*args):
        return "\n.factory/attachments/screen.png", [IMAGE]
    monkeypatch.setattr(runtime, "ModelClient", Client)
    monkeypatch.setattr(runtime, "resolve_provider", lambda _: ProviderConfig("openai", "http://test", "k", "T"))
    monkeypatch.setattr(runtime, "prepare_attachments", prepare)
    run, result, error = asyncio.run(run_agent(SessionSpec(ws_id, outcome_id, None, "builder", "native",
                                                          "Inspect it", "system", sb), asyncio.Event()))
    assert not error and result and run.status == "succeeded" and len(seen) == 2
    with session_scope() as db:
        events = db.query(Event).filter_by(kind="agent_warning").all()
        assert len(events) == 1
        event = events[0]
        assert event.level == "warn" and event.workspace_id == ws_id
        assert event.run_id == run.id and event.outcome_id == outcome_id and event.data["role"] == "builder"
        assert IMAGE["content"] not in json.dumps([event.message, event.data, db.get(Run, run.id).result])


def test_retry_error_uses_original_images_before_error_truncation(tmp_path, monkeypatch):
    from aifactory import inference
    payload = base64.b64encode(random.Random(8).randbytes(4000)).decode()
    # Provider errors may echo a different delivered image, not only the probe-sized fixture.
    seen = []
    def upstream(request):
        seen.append(request)
        message = "This model does not support images" if len(seen) == 1 else payload + "; invalid request"
        return httpx.Response(400, json={"error": {"message": message}})
    real = httpx.AsyncClient
    monkeypatch.setattr(inference.httpx, "AsyncClient", lambda **kw: real(transport=httpx.MockTransport(upstream), **kw))
    session, _, _ = make_session(tmp_path, None, image_inputs=[IMAGE])
    session.client = ModelClient(ProviderConfig("openai", "https://test", "k", "T"))
    # Include the larger known payload in the per-session matcher (no invalid image delivery).
    from aifactory import image_input
    original = image_input.ImagePayloadRedactor.__init__
    def init(self, images=()):
        original(self, [*images, {"content": payload}])
    monkeypatch.setattr(ImagePayloadRedactor, "__init__", init)
    with pytest.raises(HarnessFailed) as exc:
        asyncio.run(NativeHarness().run(session))
    assert payload[:96] not in str(exc.value)
    assert "[image omitted]; invalid request" in str(exc.value)


@pytest.mark.parametrize("wire", ["responses", "chat"])
@pytest.mark.parametrize("mime", ["image/png", "image/jpeg", "image/gif", "image/webp"])
def test_native_accepts_each_image_mime_and_exact_bytes(tmp_path, monkeypatch, wire, mime):
    from .test_attachments import VALID_IMAGES
    from aifactory import inference
    raw = VALID_IMAGES[mime]
    image = {"name": "reference", "mime_type": mime, "content": base64.b64encode(raw).decode()}
    seen = []
    def upstream(request):
        body = json.loads(request.content)
        seen.append(body)
        parts = body["input" if wire == "responses" else "messages"][1]["content"]
        url = parts[1]["image_url"]
        url = url if wire == "responses" else url["url"]
        assert url.split(",")[0] == f"data:{mime};base64"
        assert base64.b64decode(url.split(",")[1], validate=True) == raw
        if wire == "responses":
            return httpx.Response(200, json={"output": [{"type": "function_call", "call_id": "f", "name": "finish",
                "arguments": json.dumps({"status": "done", "summary": "accepted"})}]})
        return httpx.Response(200, json={"choices": [{"message": {"tool_calls": [call("finish", {
            "status": "done", "summary": "accepted"})]}}]})
    real = httpx.AsyncClient
    monkeypatch.setattr(inference.httpx, "AsyncClient", lambda **kw: real(transport=httpx.MockTransport(upstream), **kw))
    monkeypatch.setattr(UsageLedger, "persist", lambda _: None)
    session, _, _ = make_session(tmp_path, None, image_inputs=[image])
    session.client = ModelClient(ProviderConfig("openai", "https://test", "k", "T"), wire=wire)
    result = asyncio.run(NativeHarness().run(session))
    assert result.report["summary"] == "accepted" and len(seen) == 1


def test_stream_output_and_retained_head_tail_have_no_wrapped_payload(tmp_path):
    payload = base64.b64encode(random.Random(19).randbytes(100000)).decode()
    matcher = ImagePayloadRedactor([{"content": payload}])
    _, _, sb = make_session(tmp_path, None)
    (sb.work / "wrapped").write_text("\n".join(textwrap.wrap(payload, 76)))
    chunks = []
    result = asyncio.run(sb.exec("cat wrapped", output_redactor=ImageOutputRedactor(matcher),
                                 on_output=chunks.append))
    for output in (result.output, b"".join(chunks).decode()):
        assert not re.search(r"[A-Za-z0-9+/=]{32}", output)


def test_vm_truncated_wrapped_output_redacted_before_callbacks_and_clip(tmp_path):
    from aifactory.vm import RailwayVMSandbox
    payload = base64.b64encode(random.Random(21).randbytes(1600000)).decode()
    raw = "\n".join(textwrap.wrap(payload, 53))[:2000000]
    class Bridge:
        async def call(self, op, args, **kwargs):
            return {"stdout": raw, "exitCode": 0, "truncated": True}
    sb = RailwayVMSandbox(vm_id="fake", bridge=Bridge())
    chunks = []
    result = asyncio.run(sb.exec("base64 -w 53 reference.png", on_output=chunks.append,
        output_redactor=ImageOutputRedactor(ImagePayloadRedactor([{"content": payload}]))))
    assert result.output == "[image omitted]"
    assert b"".join(chunks) == b"[image omitted]"


def test_filtered_short_command_keeps_text_and_utf8_at_eof(tmp_path):
    _, _, sb = make_session(tmp_path, None)
    result = asyncio.run(sb.exec("printf 'small output café'", output_redactor=ImageOutputRedactor(ImagePayloadRedactor())))
    assert result.output == "small output café"


def test_duplicate_anchor_candidates_and_arbitrary_fragment_offsets():
    payload = "A" * 32 + "B" * 32 + "A" * 32 + "C" * 100 + "D" * 100
    matcher = ImagePayloadRedactor([{"content": payload}])
    for offset in range(100):
        fragment = payload[offset:offset + 96]
        assert matcher.text("\n".join(textwrap.wrap(fragment, 17))) == "[image omitted]"


@pytest.mark.parametrize("image", [
    {"content": "not-base64", "mime_type": "image/png"},
    {"content": IMAGE["content"], "mime_type": "image/jpeg"},
    {"content": IMAGE["content"], "mime_type": "image/svg+xml"},
])
def test_invalid_session_images_fail_before_request(tmp_path, image):
    session, client, _ = make_session(tmp_path, None, image_inputs=[image])
    with pytest.raises(HarnessFailed, match="Invalid session image input"):
        asyncio.run(NativeHarness().run(session))
    assert not client.seen


@pytest.mark.parametrize("wire", ["responses", "chat"])
def test_no_image_requests_keep_string_content(tmp_path, monkeypatch, wire):
    from aifactory import inference
    def upstream(request):
        body = json.loads(request.content)
        user = body["input" if wire == "responses" else "messages"][1]
        assert user == {"role": "user", "content": "do it"}
        if wire == "responses":
            return httpx.Response(200, json={"output": [{"type": "function_call", "call_id": "f", "name": "finish",
                "arguments": '{"status":"done","summary":"done"}'}]})
        return httpx.Response(200, json={"choices": [{"message": {"tool_calls": [call("finish", {
            "status": "done", "summary": "done"})]}}]})
    real = httpx.AsyncClient
    monkeypatch.setattr(inference.httpx, "AsyncClient", lambda **kw: real(transport=httpx.MockTransport(upstream), **kw))
    monkeypatch.setattr(UsageLedger, "persist", lambda _: None)
    session, _, _ = make_session(tmp_path, None)
    session.client = ModelClient(ProviderConfig("openai", "https://test", "k", "T"), wire=wire)
    assert asyncio.run(NativeHarness().run(session)).report["status"] == "done"


def test_stream_redactor_bounds_whitespace_overlap():
    stream = ImageOutputRedactor(ImagePayloadRedactor())
    chunks = []
    block = " \t\n" * 22000
    for _ in range(128):
        chunks.append(stream.feed(block))
        assert len(stream.pending) <= 8192
    chunks.append(stream.feed("", final=True))
    assert "".join(chunks) == block * 128
    assert stream.pending == ""


def test_timeout_flushes_redacted_diagnostic_tail(tmp_path, monkeypatch):
    _, _, sb = make_session(tmp_path, None)
    chunks = []

    async def run():
        ready = asyncio.Event()
        stream = ImageOutputRedactor(ImagePayloadRedactor([IMAGE]))
        real_feed = stream.feed
        real_wait_for = asyncio.wait_for

        def feed(text, *, final=False):
            output = real_feed(text, final=final)
            if text:
                ready.set()
            return output

        async def wait_for(awaitable, timeout):
            if isinstance(awaitable, asyncio.Future):
                # Trigger timeout only after the pump has buffered the diagnostic;
                # no wall-clock race or arbitrary sleep is involved.
                await real_wait_for(ready.wait(), timeout=10)
                awaitable.cancel()
                try:
                    await awaitable
                except asyncio.CancelledError:
                    pass
                raise asyncio.TimeoutError
            return await real_wait_for(awaitable, timeout)

        monkeypatch.setattr(stream, "feed", feed)
        monkeypatch.setattr(asyncio, "wait_for", wait_for)
        result = await sb.exec("printf '%s' 'diagnostic café " + IMAGE["content"] + "'; exec tail -f /dev/null",
                               output_redactor=stream, on_output=chunks.append)
        assert stream.pending == ""
        return result

    result = asyncio.run(run())
    assert result.timed_out and result.exit_code == 124
    assert result.output == "diagnostic café [image omitted]"
    assert b"".join(chunks).decode() == result.output
    assert result.total_bytes == len(result.output.encode())
