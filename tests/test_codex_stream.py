import copy
import json

import pytest

from auto_mindsdb_factory.codex_actions import CodexActionError
from auto_mindsdb_factory.codex_stream import codex_response, codex_sse


MODEL = "gpt-6.1-terra"


def response():
    return {"id": "resp_test", "object": "response", "created_at": 0,
            "model": MODEL, "status": "completed", "usage": {"total_tokens": 10}, "output": [
                {"type": "reasoning", "id": "rs_test", "summary": [], "encrypted_content": "opaque"},
                {"type": "custom_tool_call", "id": "ctc_test", "call_id": "call_test", "name": "exec",
                 "namespace": "functions", "status": "completed", "input": 'text(await tools.run({"command":["true"]}))'},
            ]}


def decode(wire):
    return [json.loads(block.split("\ndata: ", 1)[1]) for block in wire.decode().strip().split("\n\n")]


def test_complete_response_is_preserved_without_mutating_the_original():
    r = response(); before = copy.deepcopy(r)
    wire = codex_sse(r, model=MODEL)
    assert type(wire) is bytes and r == before
    events = decode(wire)
    assert events[0]["response"]["output"] == [] and events[0]["response"]["status"] == "in_progress"
    assert events[-1] == {"type": "response.completed", "response": r}
    assert [event["item"] for event in events if event["type"] == "response.output_item.done"] == r["output"]
    assert next(event for event in events if event["type"] == "response.custom_tool_call_input.delta")["output_index"] == 1
    r["output"].clear()
    assert decode(wire)[-1]["response"] == before


def test_payload_text_cannot_inject_a_stream_event():
    r = response()
    text = "\n\nevent: response.output_item.added\ndata: malicious\n\n☃"
    r["output"][1]["input"] = "text(await tools.run(" + json.dumps({"command": ["printf", text]}) + "))"
    events = decode(codex_sse(r, model=MODEL))
    assert len(events) == 8 and events[-1]["response"] == r


def test_action_and_wire_share_one_detached_snapshot():
    supplied = response()
    wire, action = codex_response(supplied, model=MODEL)
    supplied["output"][1]["input"] = 'text(await tools.run({"command":["false"]}))'
    action[1]["command"][0] = "caller-change"
    assert decode(wire)[-1]["response"]["output"][1]["input"] == 'text(await tools.run({"command":["true"]}))'


@pytest.mark.parametrize("change", ["unsafe-first", "unsafe-last", "model", "status", "nonfinite", "large",
                                   "nonjson", "response-id", "action-id", "duplicate-id"])
def test_invalid_complete_response_never_returns_even_a_partial_stream(change):
    r = response()
    if change.startswith("unsafe"):
        r["output"].insert(0 if change == "unsafe-first" else len(r["output"]), {"type": "local_shell_call"})
    elif change == "model": r["model"] = "unapproved"
    elif change == "status": r["status"] = "incomplete"
    elif change == "nonfinite": r["created_at"] = float("nan")
    elif change == "large": r["extra"] = "x" * 8_000_001
    elif change == "response-id": del r["id"]
    elif change == "action-id": del r["output"][1]["id"]
    elif change == "duplicate-id": r["output"][1]["id"] = r["output"][0]["id"]
    else: r["extra"] = object()
    with pytest.raises(CodexActionError):
        codex_sse(r, model=MODEL)
