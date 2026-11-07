"""Decode one bounded Codex Code Mode action without evaluating JavaScript.

The future inference bridge must validate the complete response before releasing
any tool-call stream item to app-server. This module alone enables no backend.
"""

import json
import re

from jsonschema import ValidationError, validate

from .product_agent import COMMAND_SCHEMA, REPORT_SCHEMA


_REJECTIONS = {
    "response_object": "Codex response is not a JSON object",
    "response_encoding": "Codex response cannot be encoded safely",
    "response_identity": "Codex stream response identity is invalid",
    "item_identities": "Codex stream item identities are invalid",
    "model_or_status": "Codex response model or status is invalid",
    "output_items": "Codex response contains unsupported items",
    "action_count": "Codex response must contain one action",
    "action_metadata": "Codex response action is invalid",
    "action_expression": "Codex action is outside the permitted expression",
    "action_arguments": "Codex action arguments are invalid",
}
REJECTION_REASONS = frozenset(_REJECTIONS)


class CodexActionError(ValueError):
    """A fixed diagnostic code; never include rejected provider content."""

    def __init__(self, reason):
        self.reason = reason if type(reason) is str and reason in REJECTION_REASONS else "unknown"
        super().__init__(_REJECTIONS.get(self.reason, "Codex response rejected"))


_ACTION = re.compile(
    r"text[ \t]*\([ \t]*await[ \t]+tools\.(run|finish)[ \t]*"
    r"\((.*)\)[ \t]*\)", re.DOTALL,
)


def _unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("Duplicate JSON key")
        result[key] = value
    return result


def _invalid_constant(value):
    raise ValueError("Non-JSON constant")


def codex_action(response: dict, *, model: str) -> tuple[str, dict, str]:
    """Require the exact model and one run/finish JSON expression, or fail closed.

    Only `text(await tools.run(JSON))` or `text(await tools.finish(JSON))` is
    accepted. There are no aliases, assignments, comments, extra expressions,
    arbitrary programs, alternate tools or direct function-call compatibility.
    Returned JSON is detached. Existing execution policy still owns commands.
    """
    if (type(response) is not dict or type(model) is not str or not model
            or type(response.get("model")) is not str or response["model"] != model
            or type(response.get("status")) is not str or response["status"] != "completed"):
        raise CodexActionError("model_or_status")
    output = response.get("output")
    if type(output) is not list or any(
        type(item) is not dict or type(item.get("type")) is not str
        or item["type"] not in {"message", "reasoning", "custom_tool_call"} for item in output
    ):
        raise CodexActionError("output_items")
    calls = [item for item in output if item["type"] == "custom_tool_call"]
    if len(calls) != 1:
        raise CodexActionError("action_count")
    call = calls[0]
    if (set(call) - {"type", "name", "namespace", "call_id", "input", "id", "status"}
            or ("status" in call and (type(call["status"]) is not str or call["status"] != "completed"))
            or ("id" in call and (type(call["id"]) is not str or not 1 <= len(call["id"]) <= 200))
            or any(type(call.get(key)) is not str or call[key] != value
            for key, value in {"name": "exec", "namespace": "functions"}.items())
            or type(call.get("call_id")) is not str or not 1 <= len(call["call_id"]) <= 200
            or type(call.get("input")) is not str or len(call["input"]) > 1_000_000):
        raise CodexActionError("action_metadata")
    # Trim the suffix separately: adjacent optional whitespace patterns would
    # allow quadratic regex backtracking on a large rejected program.
    program = call["input"].strip(" \t\r\n")
    if program.endswith(";"):
        program = program[:-1].rstrip(" \t")
    match = _ACTION.fullmatch(program)
    if match is None:
        raise CodexActionError("action_expression")
    name, raw = match.groups()
    try:
        arguments = json.loads(raw, object_pairs_hook=_unique_object, parse_constant=_invalid_constant)
        validate(arguments, REPORT_SCHEMA if name == "finish" else COMMAND_SCHEMA)
    except (ValueError, TypeError, RecursionError, ValidationError):
        raise CodexActionError("action_arguments") from None
    return name, arguments, call["call_id"]
