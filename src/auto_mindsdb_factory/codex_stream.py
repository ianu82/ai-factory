"""Buffer a validated response before app-server can observe executable items."""

import json

from .codex_actions import CodexActionError, codex_action


def codex_sse(response: dict, *, model: str) -> bytes:
    return codex_response(response, model=model)[0]


def _provider_envelope(snapshot, request_namespace):
    """Discard only the observed passive envelope, bound to the validated request.

    No provider metadata reaches app-server or selects an execution namespace.
    The unchanged action parser still validates every execution-bearing field.
    """
    if request_namespace != "functions" or type(snapshot.get("output")) is not list:
        return
    fields = {"type", "id", "status", "call_id", "name", "input", "metadata",
              "internal_chat_message_metadata_passthrough"}
    def turn_id(value):
        return type(value) is str and 1 <= len(value) <= 200
    for item in snapshot["output"]:
        if type(item) is not dict or item.get("type") != "custom_tool_call" or set(item) != fields:
            continue
        metadata, internal = item["metadata"], item["internal_chat_message_metadata_passthrough"]
        if (type(metadata) is not dict or set(metadata) != {"turn_id"} or not turn_id(metadata["turn_id"])
                or type(internal) is not dict or set(internal) != {"turn_id", "create_time"}
                or not turn_id(internal["turn_id"]) or type(internal["create_time"]) not in (int, float)
                or not 0 <= internal["create_time"] <= 2 ** 53):
            continue
        del item["metadata"], item["internal_chat_message_metadata_passthrough"]
        item["namespace"] = request_namespace


def codex_response(response: dict, *, model: str, request_namespace=None) -> tuple[bytes, tuple[str, dict, str]]:
    """Return immutable SSE and its action from one validated detached snapshot.

    This is deliberately not a generator. Inference admission, observed usage,
    cancellation and lifetime enforcement remain the caller's prerequisites.
    """
    if type(response) is not dict:
        raise CodexActionError("response_object")
    try:
        document = json.dumps(response, allow_nan=False)
        if len(document) > 8_000_000:
            raise ValueError("Response exceeds the transport bound")
        snapshot = json.loads(document)
    except (ValueError, TypeError, RecursionError):
        raise CodexActionError("response_encoding") from None
    _provider_envelope(snapshot, request_namespace)
    action = codex_action(snapshot, model=model)
    if type(snapshot.get("id")) is not str or not 1 <= len(snapshot["id"]) <= 200:
        raise CodexActionError("response_identity")
    identities = [item.get("id") for item in snapshot["output"]]
    if (any(type(value) is not str or not 1 <= len(value) <= 200 for value in identities)
            or len(set(identities)) != len(identities)):
        raise CodexActionError("item_identities")
    events = [{"type": "response.created", "response": {**snapshot, "status": "in_progress", "output": []}}]
    for index, item in enumerate(snapshot["output"]):
        added = {**item, "input": "", "status": "in_progress"} if item["type"] == "custom_tool_call" else item
        events.append({"type": "response.output_item.added", "output_index": index, "item": added})
        if item["type"] == "custom_tool_call":
            events.extend([
                {"type": "response.custom_tool_call_input.delta", "output_index": index,
                 "item_id": item["id"], "delta": item["input"]},
                {"type": "response.custom_tool_call_input.done", "output_index": index,
                 "item_id": item["id"], "input": item["input"]},
            ])
        events.append({"type": "response.output_item.done", "output_index": index, "item": item})
    events.append({"type": "response.completed", "response": snapshot})
    wire = "".join("event: " + event["type"] + "\ndata: " + json.dumps(event) + "\n\n" for event in events).encode()
    return wire, action
