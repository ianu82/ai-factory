import copy
import json

import pytest

from auto_mindsdb_factory.codex_actions import CodexActionError, codex_action


MODEL = "gpt-6.1-terra"
REPORT = {"status": "unverified", "summary": "Needs independent checks",
          "criteria": [{"id": "one", "status": "unverified", "evidence": "Not established"}], "findings": []}


def response(code):
    return {"model": MODEL, "status": "completed", "output": [
        {"type": "reasoning", "encrypted_content": "opaque"},
        {"type": "custom_tool_call", "name": "exec", "namespace": "functions",
         "call_id": "call_one", "input": code},
    ]}


@pytest.mark.parametrize("name,args", [("run", {"command": ["printf", "spaces; $HOME `x` \\ ' \" ☃"]}),
                                       ("finish", REPORT)])
def test_decodes_only_schema_valid_json_without_evaluating_it(name, args):
    r = response(f"text(await tools.{name}({json.dumps(args)}))")
    before = copy.deepcopy(r)
    actual_name, actual, call = codex_action(r, model=MODEL)
    assert (actual_name, actual, call) == (name, args, "call_one")
    actual.clear()
    assert r == before


def test_permits_only_trivial_expression_whitespace():
    assert codex_action(response(' \ntext ( await tools.run (\n{"command": ["true"]}\n) );\r\n'),
                        model=MODEL) == ("run", {"command": ["true"]}, "call_one")


@pytest.mark.parametrize("code", [
    'text(await tools.apply_patch("patch"))',
    'text(await tools.exec_command({"command": "id"}))',
    'text(await tools.read_mcp_resource({}))',
    'text(await tools.create_goal({"objective": "extra work"}))',
    'text(await tools.run({"command": ["true"]})); text(await tools.finish({}))',
    'const result = await tools.run({"command": ["true"]}); text(result)',
    'text(await tools["run"]({"command": ["true"]}))',
    'text(await tools.run((()=>({command:["true"]}))()))',
    'text(await tools.run({command: ["true"]}))',
    'text(await tools.run({"command": [String(globalThis)]}))',
    'text(await tools.run({"command": [`${process.env}`]}))',
    'text(await tools.run({"command": ["true"], "command": ["false"]}))',
    'text(await tools.run({"command": ["true"], "__proto__": {}}))',
    'text(await tools.finish({"status": NaN}))',
    '/* prefix */ text(await tools.run({"command": ["true"]}))',
    'text(await tools.run({"command": ["true"]})) // suffix',
    'text(await tools.run({"command": []}))',
    'text(await tools.run({"command": [1]}))',
    'text(await tools.finish({"status": "passed"}))',
    'text(await tools.run({"command": ["true"]}))\nprocess.exit()',
])
def test_rejects_arbitrary_javascript_tools_and_invalid_arguments(code):
    with pytest.raises(CodexActionError):
        codex_action(response(code), model=MODEL)


def test_rejection_never_runs_embedded_file_write(tmp_path):
    target = tmp_path / "forbidden"
    code = f'text(await tools.run({{"command": ["true"]}})); require("fs").writeFileSync({json.dumps(str(target))}, "bad")'
    with pytest.raises(CodexActionError):
        codex_action(response(code), model=MODEL)
    assert not target.exists()


@pytest.mark.parametrize("field,value", [("model", "fallback-model"), ("model", None),
                                         ("status", "incomplete"), ("status", None), ("output", None)])
def test_rejects_unapproved_response(field, value):
    r = response('text(await tools.run({"command":["true"]}))')
    r[field] = value
    with pytest.raises(CodexActionError):
        codex_action(r, model=MODEL)


@pytest.mark.parametrize("item", [{"type": "function_call", "name": "run", "arguments": "{}"},
                                  {"type": "local_shell_call"}, {"type": "web_search_call"},
                                  {"type": "compaction"}, {"type": "custom_tool_call"}, None])
def test_rejects_other_action_items_and_multiple_calls(item):
    r = response('text(await tools.run({"command":["true"]}))')
    r["output"].append(item)
    with pytest.raises(CodexActionError):
        codex_action(r, model=MODEL)


@pytest.mark.parametrize("field,value", [("namespace", "other"), ("namespace", None),
                                         ("name", "functions.exec"), ("call_id", ""),
                                         ("call_id", 1), ("input", None), ("input", "x" * 1_000_001),
                                         ("status", "in_progress"), ("async", True),
                                         ("caller", {"type": "program"}), ("id", None)])
def test_rejects_forged_or_unbounded_action_metadata(field, value):
    r = response('text(await tools.run({"command":["true"]}))')
    r["output"][1][field] = value
    with pytest.raises(CodexActionError):
        codex_action(r, model=MODEL)


@pytest.mark.parametrize("change,detail", [
    ({"async": None}, "fields_async"),
    ({"caller": {"type": "private caller"}}, "fields_caller"),
    ({"async": False, "caller": None}, "fields_async_and_caller"),
    ({"private field name": "private value", "async": False}, "fields_other"),
    ({"status": "private status"}, "status"),
    ({"id": None}, "item_id"),
    ({"name": "private name"}, "name"),
    ({"namespace": "private namespace"}, "namespace"),
    ({"call_id": []}, "call_id"),
    ({"input": None}, "input"),
])
def test_metadata_diagnostic_is_fixed_and_changes_no_rejection(change, detail):
    r = response('text(await tools.run({"command":["true"]}))')
    r["output"][1].update(change)
    before = copy.deepcopy(r)
    with pytest.raises(CodexActionError) as caught:
        codex_action(r, model=MODEL)
    assert caught.value.reason == "action_metadata" and caught.value.detail == detail
    assert "private" not in str(caught.value) and r == before


def test_missing_action_and_excessive_json_nesting_fail_closed():
    with pytest.raises(CodexActionError):
        codex_action({"model": MODEL, "status": "completed", "output": []}, model=MODEL)
    with pytest.raises(CodexActionError, match="arguments are invalid"):
        codex_action(response('text(await tools.run(' + '[' * 2000 + '0' + ']' * 2000 + '))'), model=MODEL)


def test_large_whitespace_suffix_does_not_hide_extra_code():
    with pytest.raises(CodexActionError, match="permitted expression"):
        codex_action(response('text(await tools.run({"command":["true"]}))' + ' ' * 100_000 + 'x'), model=MODEL)
