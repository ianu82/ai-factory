import json
import time
from types import SimpleNamespace

import pytest

from auto_mindsdb_factory.product_agent import ProductAgent, AgentSessionError
from auto_mindsdb_factory.run_journal import RunJournal


CONTEXT = {"acceptance_criteria": [{"id": "a", "description": "works"}]}
REPORT = {"status": "passed", "summary": "done", "criteria": [{"id": "a", "status": "passed", "evidence": "observed"}], "findings": []}


def response(name, args):
    return {"id": "response-1", "status": "completed", "model": "configured-model", "usage": {"total_tokens": 10},
            "output": [{"type": "reasoning", "id": "r", "summary": [], "encrypted_content": "opaque"},
                       {"type": "function_call", "name": name, "call_id": "c", "arguments": json.dumps(args)}]}


def session(tmp_path, responses, **kwargs):
    requests = []
    def request(body, **kwargs):
        requests.append(json.loads(json.dumps(body)))
        return responses.pop(0)
    connector = SimpleNamespace(config=SimpleNamespace(model="configured-model", max_output_tokens=1000, reasoning_effort="low", timeout_seconds=10), request_response=request)
    journal = RunJournal(tmp_path, "session")
    return ProductAgent(connector, journal, max_turns=3, max_total_tokens=kwargs.get("tokens", 100), deadline=time.monotonic() + 30), requests, journal


def test_tool_loop_preserves_reasoning_and_records_usage(tmp_path):
    agent, requests, journal = session(tmp_path, [response("run", {"command": ["cat", "app.py"]}), response("finish", REPORT)])
    assert agent.run("verifier", CONTEXT, lambda cmd: {"command": cmd, "exit_code": 0}) == REPORT
    assert requests[1]["input"][1]["encrypted_content"] == "opaque"
    assert requests[1]["input"][-1]["type"] == "function_call_output"
    assert requests[0]["store"] is False
    assert journal.summary()["total_tokens"] == 20


@pytest.mark.parametrize("responses", [
    [response("finish", REPORT)],
    [response("run", {"command": ["true"]}), response("finish", {**REPORT, "criteria": []})],
    [response("run", {"command": ["true"]}), response("finish", {**REPORT, "findings": ["bug"]})],
    [{**response("run", {"command": ["true"]}), "status": "incomplete"}],
    [{**response("run", {"command": ["true"]}), "usage": None}],
])
def test_false_or_incomplete_success_is_blocked(tmp_path, responses):
    agent, _, _ = session(tmp_path, responses)
    with pytest.raises(AgentSessionError):
        agent.run("verifier", CONTEXT, lambda cmd: {"exit_code": 0})


def test_token_budget_is_shared_across_roles(tmp_path):
    agent, requests, _ = session(tmp_path, [response("run", {"command": ["true"]})], tokens=5)
    with pytest.raises(AgentSessionError, match="budget"):
        agent.run("builder", CONTEXT, lambda cmd: {})
    assert len(requests) == 1
