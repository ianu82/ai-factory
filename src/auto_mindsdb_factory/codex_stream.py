"""Buffer a validated response before app-server can observe executable items."""

import json

from .codex_actions import CodexActionError, codex_action


def codex_sse(response: dict, *, model: str) -> bytes:
    """Return immutable SSE only after the complete detached response passes.

    This is deliberately not a generator. Inference admission, observed usage,
    cancellation and lifetime enforcement remain the caller's prerequisites.
    """
    if type(response) is not dict:
        raise CodexActionError("Codex response is not a JSON object")
    try:
        document = json.dumps(response, allow_nan=False)
        if len(document) > 8_000_000:
            raise ValueError("Response exceeds the transport bound")
        snapshot = json.loads(document)
    except (ValueError, TypeError, RecursionError):
        raise CodexActionError("Codex response cannot be encoded safely") from None
    codex_action(snapshot, model=model)
    if type(snapshot.get("id")) is not str or not 1 <= len(snapshot["id"]) <= 200:
        raise CodexActionError("Codex stream response identity is invalid")
    identities = [item.get("id") for item in snapshot["output"]]
    if (any(type(value) is not str or not 1 <= len(value) <= 200 for value in identities)
            or len(set(identities)) != len(identities)):
        raise CodexActionError("Codex stream item identities are invalid")
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
    return "".join("event: " + event["type"] + "\ndata: " + json.dumps(event) + "\n\n" for event in events).encode()
