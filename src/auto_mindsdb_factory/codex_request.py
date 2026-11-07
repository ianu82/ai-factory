"""Validate the pinned CLI's request before a future bridge admits inference."""

import json
import re

from .codex_grammar import ACTION_GRAMMAR


class CodexRequestError(ValueError):
    pass


_FIELDS = {"model", "stream", "input", "tool_choice", "parallel_tool_calls", "reasoning",
           "store", "include", "prompt_cache_key", "text", "client_metadata", "max_output_tokens"}
_INPUT_TYPES = {"message", "reasoning", "custom_tool_call", "custom_tool_call_output", "additional_tools"}


def _tools(tools, namespace=None):
    if type(tools) is not list or len(tools) > 200:
        raise CodexRequestError("Codex tool catalogue is invalid")
    found = False
    for tool in tools:
        if type(tool) is not dict or type(tool.get("type")) is not str:
            raise CodexRequestError("Codex tool catalogue is invalid")
        kind = tool["type"]
        if kind == "namespace" and namespace is None and tool.get("name") == "functions":
            if set(tool) - {"type", "name", "description", "tools"}:
                raise CodexRequestError("Codex namespace contains unsupported policy")
            found |= _tools(tool.get("tools"), "functions")
        elif kind == "function" and namespace == "functions":
            if (set(tool) - {"type", "name", "description", "parameters", "strict", "defer_loading"}
                    or type(tool.get("name")) is not str or re.fullmatch(r"[A-Za-z0-9_.-]{1,128}", tool["name"]) is None
                    or type(tool.get("parameters")) is not dict):
                raise CodexRequestError("Codex function declaration is invalid")
        elif kind == "custom" and namespace == "functions" and tool.get("name") == "exec":
            fmt = tool.get("format")
            if (set(tool) - {"type", "name", "description", "format"} or type(fmt) is not dict
                    or set(fmt) != {"type", "syntax", "definition"} or fmt["type"] != "grammar"
                    or fmt["syntax"] != "lark" or type(fmt["definition"]) is not str or not fmt["definition"]):
                raise CodexRequestError("Codex Code Mode declaration is invalid")
            # Narrow the detached producer declaration. The original action
            # parser, callback ownership and report validation remain mandatory.
            tool["format"] = {"type": "grammar", "syntax": "lark", "definition": ACTION_GRAMMAR}
            found = True
        else:
            # Hosted tools could execute at the provider before response validation.
            raise CodexRequestError("Codex requested an unsupported tool surface")
    return found


def codex_request(body: dict, *, model: str, effort: str, max_output_tokens: int, remaining_tokens: int) -> dict:
    """Detach the request, preserve smaller explicit caps and reject policy drift.

    The caller must derive remaining_tokens from complete aggregate accounting
    and enforce cancellation, deadlines, request limits and retained ownership.
    No inference, tool execution or evidence write occurs here.
    """
    if (type(body) is not dict or type(model) is not str or not model or type(effort) is not str or not effort
            or type(max_output_tokens) is not int or max_output_tokens < 1
            or type(remaining_tokens) is not int or remaining_tokens < 1):
        raise CodexRequestError("Codex request configuration is invalid")
    try:
        encoded = json.dumps(body, allow_nan=False)
        if len(encoded) > 8_000_000:
            raise ValueError("Request exceeds the transport bound")
        request = json.loads(encoded)
    except (ValueError, TypeError, RecursionError):
        raise CodexRequestError("Codex request cannot be encoded safely") from None
    reasoning, items = request.get("reasoning"), request.get("input")
    if (set(request) - _FIELDS or request.get("model") != model or request.get("stream") is not True
            or request.get("store") is not False or request.get("parallel_tool_calls") is not False
            or type(request.get("tool_choice")) is not str or request["tool_choice"] not in {"auto", "required"}
            or type(reasoning) is not dict or reasoning.get("effort") != effort
            or set(reasoning) - {"effort", "context", "summary"}
            or request.get("include") != ["reasoning.encrypted_content"] or type(items) is not list
            or any(type(item) is not dict or type(item.get("type")) is not str
                   or item["type"] not in _INPUT_TYPES for item in items)):
        raise CodexRequestError("Codex request differs from the permitted execution policy")
    found = False
    for item in items:
        if item["type"] == "additional_tools":
            found |= _tools(item.get("tools"))
    if not found:
        raise CodexRequestError("Codex request has no permitted Code Mode declaration")
    cap = min(max_output_tokens, remaining_tokens)
    if "max_output_tokens" in request:
        explicit = request["max_output_tokens"]
        if type(explicit) is not int or not 1 <= explicit <= cap:
            raise CodexRequestError("Codex request output limit exceeds the permitted budget")
    else:
        request["max_output_tokens"] = cap
    if len(json.dumps(request, allow_nan=False)) > 8_000_000:
        raise CodexRequestError("Codex request exceeds the transport bound")
    return request
