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
_EXTRA_FIELD_DETAILS = {
    frozenset({"async"}): "fields_async",
    frozenset({"caller"}): "fields_caller",
    frozenset({"async", "caller"}): "fields_async_and_caller",
    frozenset({"created_by"}): "fields_created_by",
    frozenset({"async", "created_by"}): "fields_async_and_created_by",
    frozenset({"caller", "created_by"}): "fields_caller_and_created_by",
    frozenset({"async", "caller", "created_by"}): "fields_async_caller_and_created_by",
}
METADATA_REJECTION_DETAILS = frozenset(_EXTRA_FIELD_DETAILS.values()) | frozenset({
    "fields_other", "fields_other_one", "fields_other_two", "fields_other_many",
    "status", "item_id", "name", "namespace", "call_id", "input"})
_ACTION_FIELDS = frozenset({"type", "name", "namespace", "call_id", "input", "id", "status"})


class CodexActionError(ValueError):
    """A fixed diagnostic code; never include rejected provider content."""

    def __init__(self, reason, *, detail=None):
        self.reason = reason if type(reason) is str and reason in REJECTION_REASONS else "unknown"
        self.detail = (detail if self.reason == "action_metadata" and type(detail) is str
                       and detail in METADATA_REJECTION_DETAILS else None)
        super().__init__(_REJECTIONS.get(self.reason, "Codex response rejected"))


def _metadata_detail(call):
    """Classify an already rejected action; never serialize its keys or values."""
    extra = set(call) - _ACTION_FIELDS
    if extra:
        known = _EXTRA_FIELD_DETAILS.get(frozenset(extra))
        if known is not None:
            return known
        # Count all extra fields, including any known ones, without retaining names.
        return ("fields_other_one", "fields_other_two", "fields_other_many")[min(len(extra), 3) - 1]
    if "status" in call and (type(call["status"]) is not str or call["status"] != "completed"):
        return "status"
    if "id" in call and (type(call["id"]) is not str or not 1 <= len(call["id"]) <= 200):
        return "item_id"
    for key, value in {"name": "exec", "namespace": "functions"}.items():
        if type(call.get(key)) is not str or call[key] != value:
            return key
    if type(call.get("call_id")) is not str or not 1 <= len(call["call_id"]) <= 200:
        return "call_id"
    return "input"


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
    if (set(call) - _ACTION_FIELDS
            or ("status" in call and (type(call["status"]) is not str or call["status"] != "completed"))
            or ("id" in call and (type(call["id"]) is not str or not 1 <= len(call["id"]) <= 200))
            or any(type(call.get(key)) is not str or call[key] != value
            for key, value in {"name": "exec", "namespace": "functions"}.items())
            or type(call.get("call_id")) is not str or not 1 <= len(call["call_id"]) <= 200
            or type(call.get("input")) is not str or len(call["input"]) > 1_000_000):
        raise CodexActionError("action_metadata", detail=_metadata_detail(call))
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
