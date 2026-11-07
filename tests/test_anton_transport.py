"""Exercise the pinned SDK and actual OpenAI client through an in-memory HTTP transport."""

import asyncio
import json
import time

import pytest

from auto_mindsdb_factory.anton_session import AntonAgent
from auto_mindsdb_factory.connectors import OpenAIResponsesAgentConfig
from auto_mindsdb_factory.product_agent import HarnessStopped
from auto_mindsdb_factory.product_agent import TOOLS
from auto_mindsdb_factory.product_roles import snapshot_roles
from auto_mindsdb_factory.run_journal import RunJournal


REPORT = {"status": "passed", "summary": "Parser checked", "findings": [],
          "criteria": [{"id": "a", "status": "passed", "evidence": "Ran parser cases"}]}
CONTEXT = {"acceptance_criteria": [{"id": "a", "description": "Reject invalid input"}]}


@pytest.mark.parametrize("mode", ["success", "provider-error", "cancel-active", "host-tool", "multiple-calls"])
def test_real_sdk_http_protocol_accounting_and_cleanup(tmp_path, monkeypatch, mode):
    pytest.importorskip("anton.core.session")
    import httpx2
    import anton.core.llm.openai as provider

    actual_client = provider.openai.AsyncOpenAI
    requests, clients, commands, cancellations = [], [], [], []
    async def respond(request):
        requests.append(request)
        if mode == "provider-error":
            return httpx2.Response(503, json={"error": {"message": "private provider failure"}})
        if mode == "cancel-active":
            try:
                await asyncio.Event().wait()
            finally:
                cancellations.append(True)
        name, arguments = ("finish", REPORT) if len(requests) == 2 else ("run", {"command": ["cat", "parser.py"]})
        if mode == "host-tool":
            name, arguments = "scratchpad", {"code": "read_host_secret()"}
        output = [{"id": "fc_" + str(len(requests)), "type": "function_call", "status": "completed",
                   "call_id": "call_" + str(len(requests)), "name": name, "arguments": json.dumps(arguments)}]
        if mode == "multiple-calls":
            output.append({**output[0], "id": "second", "call_id": "second"})
        return httpx2.Response(200, json={"id": "resp_" + str(len(requests)), "object": "response",
            "created_at": 1, "status": "completed", "model": "pinned", "parallel_tool_calls": False,
            "output": output,
            "usage": {"input_tokens": 7, "output_tokens": 3, "total_tokens": 10}})
    def create_client(**kwargs):
        http = httpx2.AsyncClient(transport=httpx2.MockTransport(respond))
        clients.append(http)
        return actual_client(**kwargs, http_client=http)
    monkeypatch.setattr(provider.openai, "AsyncOpenAI", create_client)
    config = OpenAIResponsesAgentConfig(api_key="controller-only-http-test", model="pinned", max_output_tokens=1000)
    journal = RunJournal(tmp_path, "http")
    journal.create({"role_snapshots": snapshot_roles()})
    agent = AntonAgent(config, journal, max_turns=4, max_total_tokens=1000, deadline=time.monotonic() + 10,
                       cancelled=lambda: mode == "cancel-active" and bool(requests))
    def command(argv):
        commands.append(argv)
        return {"command": argv, "exit_code": 0, "stdout": "Observed parser"}
    if mode == "success":
        assert agent.run("builder", CONTEXT, command) == REPORT
        assert commands == [["cat", "parser.py"]] and len(requests) == 2
        assert journal.summary()["total_tokens"] == 20 and journal.summary()["usage_complete"]
        bodies = [json.loads(request.content) for request in requests]
        assert all(body["model"] == "pinned" and body["store"] is False for body in bodies)
        assert all(body["parallel_tool_calls"] is False and body["max_output_tokens"] <= 1000 for body in bodies)
        assert all({tool["name"] for tool in body["tools"]} == {"run", "finish"} for body in bodies)
        assert all(body["tools"] == TOOLS and body["tool_choice"] == "required" for body in bodies)
        assert any(item.get("type") == "function_call_output" for item in bodies[1]["input"])
    else:
        with pytest.raises(HarnessStopped):
            agent.run("builder", CONTEXT, command)
        assert len(requests) == 1 and not commands
        assert journal.summary()["usage_complete"] is (mode in {"host-tool", "multiple-calls"})
        assert cancellations == ([True] if mode == "cancel-active" else [])
    assert clients and all(client.is_closed for client in clients)
    assert all(request.headers["authorization"] == "Bearer controller-only-http-test" for request in requests)
    assert "controller-only-http-test" not in json.dumps(journal.events()) + str([r.content for r in requests])
    assert "private provider failure" not in json.dumps(journal.events())


@pytest.mark.parametrize("change", [None, "dropped-params", "dropped-content", "clamped-params",
    "chain-truncated", "failover", "attempts", "missing-attempts", "missing-failover", "malformed-attempts", "malformed-failover", "redirect"])
def test_gateway_changes_stop_before_tool_with_usage_and_no_header_leak(tmp_path, monkeypatch, change):
    from auto_mindsdb_factory.product_inference_runtime import MindsHubQualificationConfig
    pytest.importorskip("anton.core.session")
    import httpx2
    import anton.core.llm.openai as provider

    actual_client = provider.openai.AsyncOpenAI
    requests, clients, commands = [], [], []
    report = {"status": "passed", "summary": "Checked parser", "findings": [],
              "criteria": [{"id": "a", "status": "passed", "evidence": "Read parser"}]}
    headers = {"x-mindshub-attempts": "1", "x-mindshub-failover": "false"}
    if change in {"dropped-params", "dropped-content", "clamped-params", "chain-truncated"}:
        headers["x-mindshub-" + change] = "untrusted-private-header"
    elif change == "failover": headers["x-mindshub-failover"] = "true"
    elif change == "attempts": headers["x-mindshub-attempts"] = "2"
    elif change == "malformed-attempts": headers["x-mindshub-attempts"] = "private-attempt-value"
    elif change == "malformed-failover": headers["x-mindshub-failover"] = "private-failover-value"
    elif change and change.startswith("missing-"): del headers["x-mindshub-" + change.removeprefix("missing-")]

    async def respond(request):
        requests.append(request)
        assert str(request.url) == MindsHubQualificationConfig.base_url
        if change == "redirect":
            return httpx2.Response(307, headers={"location": "https://other.invalid/v1/responses"})
        name, args = ("finish", report) if len(requests) == 2 else ("run", {"command": ["cat", "parser.py"]})
        return httpx2.Response(200, headers=headers, json={"id": "r", "object": "response", "created_at": 1,
            "status": "completed", "model": "pinned", "parallel_tool_calls": False,
            "output": [{"type": "function_call", "id": "f", "call_id": "c" + str(len(requests)),
                        "name": name, "arguments": json.dumps(args)}],
            "usage": {"input_tokens": 7, "output_tokens": 3, "total_tokens": 10}})

    def create_client(**kwargs):
        http = httpx2.AsyncClient(transport=httpx2.MockTransport(respond), follow_redirects=True)
        clients.append(http)
        return actual_client(**kwargs, http_client=http)
    monkeypatch.setattr(provider.openai, "AsyncOpenAI", create_client)
    config = MindsHubQualificationConfig(("mdb_synthetic.private", '{"policy":"synthetic"}'),
        "pinned", "high", 1000, 10)
    journal = RunJournal(tmp_path, "gateway")
    journal.create({"role_snapshots": snapshot_roles()})
    agent = AntonAgent(config, journal, max_turns=4, max_total_tokens=1000, deadline=time.monotonic() + 10)
    def command(argv):
        commands.append(argv)
        return {"command": argv, "exit_code": 0, "stdout": "Observed parser"}
    context = {"acceptance_criteria": [{"id": "a", "description": "Inspect parser"}]}
    if change is None:
        assert agent.run("builder", context, command) == report
        assert commands == [["cat", "parser.py"]] and len(requests) == 2
    else:
        with pytest.raises(HarnessStopped):
            agent.run("builder", context, command)
        assert len(requests) == 1 and not commands
    assert clients and all(c.is_closed and not c.follow_redirects for c in clients)
    assert all(r.headers["authorization"] == "Bearer mdb_synthetic.private" for r in requests)
    events = journal.events()
    assert all(value not in json.dumps(events) for value in
        ("untrusted-private-header", "private-attempt-value", "private-failover-value"))
    assert "mdb_synthetic.private" not in json.dumps(events)
    if change != "redirect":
        assert journal.summary()["total_tokens"] == (20 if change is None else 10)
        assert journal.summary()["usage_complete"] is (change not in {
            "failover", "attempts", "missing-attempts", "missing-failover",
            "malformed-attempts", "malformed-failover"})
        evidence = [e["data"] for e in events if e["kind"] == "inference_gateway"]
        assert len(evidence) == (2 if change is None else 1)
        if change:
            assert events[-1]["data"]["reason"] == "gateway_policy_mismatch"
    else:
        assert not journal.summary()["usage_complete"]
