import copy

import pytest

from auto_mindsdb_factory.codex_request import CodexRequestError, codex_request


MODEL = "gpt-6.1-terra"


def body():
    return {"model": MODEL, "stream": True, "store": False, "tool_choice": "auto", "parallel_tool_calls": False,
            "reasoning": {"effort": "high", "context": "all_turns"}, "include": ["reasoning.encrypted_content"],
            "input": [{"type": "additional_tools", "tools": [{"type": "namespace", "name": "functions", "tools": [
                {"type": "custom", "name": "exec", "description": "Controller code tool", "format": {
                    "type": "grammar", "syntax": "lark", "definition": "start: /[\\s\\S]+/"}},
            ]}]}, {"type": "message", "role": "user", "content": "Untrusted task context"}]}


def prepare(request, **overrides):
    return codex_request(request, **{"model": MODEL, "effort": "high", "max_output_tokens": 10000,
                                     "remaining_tokens": 50000, **overrides})


def test_applies_only_the_trusted_missing_cap_without_mutating_context():
    b = body(); before = copy.deepcopy(b)
    actual = prepare(b, remaining_tokens=17)
    assert actual.pop("max_output_tokens") == 17 and actual == before and b == before
    actual["input"].clear()
    assert b == before


def test_preserves_a_smaller_explicit_limit():
    b = body(); b["max_output_tokens"] = 12
    assert prepare(b, remaining_tokens=17) == b


@pytest.mark.parametrize("field,value", [("model", "fallback"), ("store", True), ("stream", False),
    ("parallel_tool_calls", True), ("tool_choice", "none"), ("tool_choice", {}), ("input", None),
    ("reasoning", {"effort": "low"}), ("reasoning", {"effort": "high", "helper_model": "other"}),
    ("include", []), ("previous_response_id", "hidden-history"), ("max_output_tokens", 10001),
    ("max_output_tokens", True), ("max_output_tokens", 0), ("max_output_tokens", None)])
def test_rejects_drift_and_never_increases_explicit_limits(field, value):
    b = body(); b[field] = value
    with pytest.raises(CodexRequestError): prepare(b)


@pytest.mark.parametrize("tool", [{"type": "web_search"}, {"type": "code_interpreter"},
    {"type": "image_generation"}, {"type": "mcp", "server_url": "https://unapproved.invalid"},
    {"type": "namespace", "name": "other", "tools": []}])
def test_rejects_hosted_or_unknown_tool_surfaces(tool):
    b = body(); b["input"][0]["tools"].append(tool)
    with pytest.raises(CodexRequestError): prepare(b)
    b = body(); b["tools"] = [tool]
    with pytest.raises(CodexRequestError): prepare(b)


@pytest.mark.parametrize("kind", ["compaction", "web_search_call", "function_call", "unknown"])
def test_unknown_history_or_compaction_cannot_hide_a_new_request(kind):
    b = body(); b["input"].append({"type": kind})
    with pytest.raises(CodexRequestError): prepare(b)


@pytest.mark.parametrize("setting", [{"remaining_tokens": 0}, {"remaining_tokens": False},
                                    {"max_output_tokens": -1}, {"max_output_tokens": True}, {"effort": ""}])
def test_invalid_or_empty_budget_stops_before_a_request(setting):
    with pytest.raises(CodexRequestError): prepare(body(), **setting)


def test_missing_or_malformed_catalogue_fails_closed():
    for value in [None, [], [{"type": "namespace", "name": "functions", "tools": []}]]:
        b = body(); b["input"][0]["tools"] = value
        with pytest.raises(CodexRequestError): prepare(b)


@pytest.mark.parametrize("name", ["", "bad\nname", "x" * 129])
def test_malformed_function_names_stop_before_provider_validation(name):
    b = body()
    b["input"][0]["tools"][0]["tools"].append({"type": "function", "name": name, "parameters": {}})
    with pytest.raises(CodexRequestError): prepare(b)


def test_empty_code_mode_grammar_is_invalid():
    b = body(); b["input"][0]["tools"][0]["tools"][0]["format"]["definition"] = ""
    with pytest.raises(CodexRequestError): prepare(b)
