import json
import time
from types import SimpleNamespace

import pytest

from auto_mindsdb_factory import anton_session
from auto_mindsdb_factory.anton_session import ANTON_COMMIT, AntonAgent, validate_anton_installation
from auto_mindsdb_factory.connectors import OpenAIResponsesAgentConfig
from auto_mindsdb_factory.product_agent import AgentSessionError
from auto_mindsdb_factory.product_roles import snapshot_roles
from auto_mindsdb_factory.run_journal import RunJournal


@pytest.mark.parametrize("metadata", [None, "[]", "{}", json.dumps({"url": "https://github.com/mindsdb/anton",
    "vcs_info": {"commit_id": "0" * 40}})])
def test_unpinned_installations_fail_before_import(monkeypatch, metadata):
    monkeypatch.setattr(anton_session, "distribution", lambda name: SimpleNamespace(read_text=lambda path: metadata))
    with pytest.raises(AgentSessionError, match="pinned"):
        validate_anton_installation()


REPORT = {"status": "passed", "summary": "Checked parser", "findings": [],
          "criteria": [{"id": "a", "status": "passed", "evidence": "Executed parser regression"}]}
CONTEXT = {"acceptance_criteria": [{"id": "a", "description": "Parser handles invalid input"}]}


def harness(tmp_path, monkeypatch, actions, **overrides):
    pytest.importorskip("anton.core.session")
    import anton.core.llm.openai as provider
    requests, transports, commands = [], [], []
    class Client:
        def __init__(self, **kwargs):
            self.closed = False
            self.responses = SimpleNamespace(create=self.create)
            transports.append(self)
        def with_options(self, **kwargs):
            if "max_retries" in kwargs:
                assert kwargs["max_retries"] == 0
            return self
        async def create(self, **body):
            requests.append(body)
            name, arguments = actions.pop(0)
            return SimpleNamespace(id=str(len(requests)), model="pinned", status="completed",
                usage=SimpleNamespace(model_dump=lambda: {"input_tokens": 7, "output_tokens": 3, "total_tokens": 10}),
                output=[SimpleNamespace(type="function_call", call_id=str(len(requests)), name=name,
                                        arguments=json.dumps(arguments))])
        async def close(self):
            self.closed = True
    monkeypatch.setattr(provider.openai, "AsyncOpenAI", Client)
    config = OpenAIResponsesAgentConfig(api_key="controller-only-test-secret", model="pinned", max_output_tokens=1000)
    journal = RunJournal(tmp_path, "session")
    journal.create({"role_snapshots": snapshot_roles()})
    agent = AntonAgent(config, journal, max_turns=4, max_total_tokens=10000,
                       deadline=time.monotonic() + 5, **overrides)
    def command(argv):
        commands.append(argv)
        return {"command": argv, "exit_code": 0, "output": "Observed parser behavior"}
    return agent, journal, requests, transports, commands, command


@pytest.mark.parametrize("role", ["builder", "verifier"])
def test_real_sdk_uses_only_job_tools_and_validated_finish(tmp_path, monkeypatch, role):
    agent, journal, requests, transports, commands, command = harness(tmp_path, monkeypatch,
        [("run", {"command": ["cat", "parser.py"]}), ("finish", REPORT)])
    assert agent.run(role, CONTEXT, command) == REPORT
    assert commands == [["cat", "parser.py"]]
    assert all({t["name"] for t in r["tools"]} == {"run", "finish"} for r in requests)
    assert all(r["store"] is False and r["model"] == "pinned" for r in requests)
    assert all(transports) and all(t.closed for t in transports)
    assert journal.summary()["total_tokens"] == 20
    assert journal.events()[-1]["kind"] == "agent_report"
    assert "controller-only-test-secret" not in json.dumps(requests) + json.dumps(journal.events())
    starts = [e for e in journal.events() if e["kind"] == "harness_started"]
    assert len(starts) == 1 and starts[0]["data"]["version"] == ANTON_COMMIT


def test_sdk_cannot_finish_without_investigating(tmp_path, monkeypatch):
    agent, journal, requests, transports, commands, command = harness(tmp_path, monkeypatch,
        [("finish", REPORT)] * 5)
    with pytest.raises(AgentSessionError):
        agent.run("verifier", CONTEXT, command)
    assert not commands and all(t.closed for t in transports)
    assert not any(e["kind"] == "agent_report" for e in journal.events())


def test_sdk_cannot_invoke_undeclared_host_tools(tmp_path, monkeypatch):
    agent, journal, requests, transports, commands, command = harness(tmp_path, monkeypatch,
        [("scratchpad", {"action": "exec", "code": "read_controller_secret()"})])
    with pytest.raises(AgentSessionError):
        agent.run("verifier", CONTEXT, command)
    assert not commands and len(requests) == 1 and all(t.closed for t in transports)
    assert not any(e["kind"] == "agent_report" for e in journal.events())


def test_home_environment_overrides_fail_before_execution(monkeypatch):
    monkeypatch.setenv("ANTON_SETTINGS", "candidate-controlled")
    with pytest.raises(AgentSessionError, match="environment"):
        validate_anton_installation()
