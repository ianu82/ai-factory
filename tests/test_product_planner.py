import json
import time
from types import SimpleNamespace

import pytest

from auto_mindsdb_factory.product_agent import AgentSessionError, RoleSnapshotError
from auto_mindsdb_factory.product_planner import ProductPlanner, PLANNER_VERSION
from auto_mindsdb_factory.product_profile import ProductProfile
from auto_mindsdb_factory.product_roles import select_role, snapshot_roles
from auto_mindsdb_factory.product_task import ProductTask
from auto_mindsdb_factory.run_journal import RunJournal


def setup(tmp_path, responses, **settings):
    requests = []
    def request(body, **kwargs):
        requests.append(body)
        return {"id": str(len(requests)), "status": "completed", "model": "pinned", "usage": {"total_tokens": 10},
            "output": [{"type": "function_call", "call_id": str(len(requests)),
                        "name": responses[0][0], "arguments": json.dumps(responses.pop(0)[1])}]}
    connector = SimpleNamespace(config=SimpleNamespace(model="pinned", max_output_tokens=1000,
        reasoning_effort="low", timeout_seconds=10, fallback_model=None), request_response=request)
    profile = ProductProfile.from_document({"version": 1, "name": "planner", "repository": "owner/repo",
        "image": "python@sha256:" + "a" * 64, "checks": {"test": ["pytest"]}, "smoke": [["pytest"]],
        "builder_model": "pinned", "max_total_tokens": 1000, **settings})
    task = ProductTask.from_document({"id": "task", "title": "Parser", "goal": "Reject invalid input",
        "acceptance_criteria": [{"id": "reject", "description": "Reject invalid input", "command": ["pytest"]}]})
    journal = RunJournal(tmp_path, "planner")
    journal.create({"role_snapshots": snapshot_roles(), "planner_role_snapshot": select_role("planner", PLANNER_VERSION)})
    return ProductPlanner(connector, journal, task=task, base_sha="b" * 40, profile=profile,
                          deadline=time.monotonic() + 300), requests, journal


def plan():
    return {"status": "ready", "reason": "Inspected parser", "goal": "Reject invalid input",
        "constraints": ["Keep API"], "source_paths": ["parser.py"], "changes": ["Validate parser inputs"],
        "risks": ["Preserve valid input"], "questions": [],
        "validation": [{"criterion_id": "reject", "evidence": "Run invalid and valid parser cases"}]}


def test_planner_uses_frozen_role_and_shared_accounting(tmp_path):
    agent, requests, journal = setup(tmp_path, [("run", {"command": ["cat", "parser.py"]}), ("finish", plan())])
    assert agent.run("planner", agent.task.public_context, lambda cmd: {"command": cmd, "exit_code": 0}) == plan()
    assert journal.summary()["total_tokens"] == 20
    assert journal.events()[-1]["kind"] == "planner_report"
    assert all(r["instructions"] == select_role("planner", PLANNER_VERSION)["instructions"] for r in requests)
    assert all([t["name"] for t in r["tools"]] == ["run", "finish"] for r in requests)
    assert all(r["model"] == "pinned" and r["max_output_tokens"] <= 1000 for r in requests)
    assert agent.max_turns == 8 and agent.max_total_tokens == 1000
    assert 0 < agent.deadline - time.monotonic() <= 120


@pytest.mark.parametrize("mutation", ["missing", "historical", "previous-planner", "report-planner", "readiness-planner", "window-planner", "portable-planner", "forged"])
def test_planner_rejects_missing_or_nonexecuting_roles_before_request(tmp_path, mutation):
    agent, requests, journal = setup(tmp_path, [])
    manifest = json.loads((journal.root / "manifest.json").read_text())
    if mutation == "missing":
        del manifest["planner_role_snapshot"]
    elif mutation == "historical":
        manifest["planner_role_snapshot"] = select_role("planner", "1")
    elif mutation == "previous-planner":
        manifest["planner_role_snapshot"] = select_role("planner", "2")
    elif mutation == "report-planner":
        manifest["planner_role_snapshot"] = select_role("planner", "3")
    elif mutation == "readiness-planner":
        manifest["planner_role_snapshot"] = select_role("planner", "4")
    elif mutation == "window-planner":
        manifest["planner_role_snapshot"] = select_role("planner", "5")
    elif mutation == "portable-planner":
        manifest["planner_role_snapshot"] = select_role("planner", "6")
    else:
        manifest["planner_role_snapshot"]["instructions"] = "write code"
    (journal.root / "manifest.json").write_text(json.dumps(manifest))
    with pytest.raises(RoleSnapshotError):
        agent.run("planner", agent.task.public_context, lambda cmd: pytest.fail("command invoked"))
    assert not requests


def test_capture_policy_is_supplied_before_first_command_and_on_report(tmp_path):
    from auto_mindsdb_factory.docker_runner import DockerSandbox
    agent, requests, journal = setup(tmp_path, [("run", {"command": ["cat", "parser.py"]}),
                                               ("finish", plan())], max_agent_turns=2)
    before = (journal.root / "manifest.json").read_bytes()
    def investigate(argv):
        assert len(requests) == 1
        assert json.loads(requests[0]["input"][-1]["content"])["command_output"] == {
            "max_bytes_per_stream": DockerSandbox.MAX_CAPTURED_OUTPUT_BYTES,
            "retains": "tail", "text_encoding": "utf-8", "decode_errors": "replace"}
        return {"exit_code": 0}
    assert agent.run("planner", agent.task.public_context, investigate) == plan()
    policies = [json.loads(r["input"][-1]["content"]) for r in requests]
    assert policies[0]["command_output"] == policies[1]["command_output"]
    assert policies[0]["planning_budget"]["report_only"] is False
    assert policies[1]["planning_budget"]["report_only"] is True
    assert (journal.root / "manifest.json").read_bytes() == before


@pytest.mark.parametrize("mode", ["no-investigation", "bad-mapping", "unknown-usage", "exhausted", "deadline"])
def test_planning_cannot_waive_admission_or_report_requirements(tmp_path, mode):
    proposed = plan()
    if mode == "bad-mapping":
        proposed["validation"][0]["criterion_id"] = "other"
    responses = ([] if mode == "no-investigation" else [("run", {"command": ["cat", "parser.py"]})])
    agent, requests, journal = setup(tmp_path, responses + [("finish", proposed)])
    if mode == "unknown-usage":
        journal.append("model_failed", role="builder")
    elif mode == "exhausted":
        journal.append("model_response", usage={"total_tokens": 1000})
    elif mode == "deadline":
        agent.deadline = time.monotonic() - 1
    with pytest.raises(ValueError if mode == "bad-mapping" else AgentSessionError):
        agent.run("planner", agent.task.public_context, lambda cmd: {"command": cmd, "exit_code": 0})
    if mode in {"unknown-usage", "exhausted", "deadline"}:
        assert not requests


@pytest.mark.parametrize("limit", [2, 8])
def test_last_request_reserves_report_without_extending_limits(tmp_path, limit):
    response = ("run", {"command": ["cat", "parser.py"]})
    agent, requests, journal = setup(tmp_path, [response] * (limit - 1) + [("finish", plan())],
                                     max_agent_turns=limit)
    manifest = (journal.root / "manifest.json").read_bytes()
    deadline = agent.deadline
    commands = []
    result = agent.run("planner", agent.task.public_context,
                      lambda argv: commands.append(argv) or {"exit_code": 0})
    assert result == plan() and len(commands) == limit - 1 and len(requests) == limit
    assert [t["name"] for t in requests[-1]["tools"]] == ["finish"]
    guidance = json.loads(requests[-1]["input"][-1]["content"])["planning_budget"]
    assert guidance["requests_remaining"] == 1 and guidance["report_only"] is True
    assert agent.deadline == deadline and agent.max_turns == limit
    assert (journal.root / "manifest.json").read_bytes() == manifest
    assert journal.summary()["total_tokens"] == limit * 10


def test_time_reservation_requires_report_before_the_last_request(tmp_path, monkeypatch):
    agent, requests, journal = setup(tmp_path, [("run", {"command": ["cat", "parser.py"]}),
                                               ("finish", plan())])
    clock = {"now": agent.deadline - 100}
    monkeypatch.setattr(time, "monotonic", lambda: clock["now"])
    def investigate(argv):
        clock["now"] = agent.deadline - 20
        return {"exit_code": 0}
    assert agent.run("planner", agent.task.public_context, investigate) == plan()
    assert [t["name"] for t in requests[-1]["tools"]] == ["finish"]
    assert json.loads(requests[-1]["input"][-1]["content"])["planning_budget"] == {
        "requests_remaining": 7, "seconds_remaining": 20, "report_only": True}
    assert journal.summary()["usage_complete"] is True


def test_setup_and_investigation_leave_time_for_a_slow_final_report(tmp_path, monkeypatch):
    agent, requests, journal = setup(tmp_path, [("run", {"command": ["cat", "parser.py"]}),
                                               ("finish", plan())])
    deadline = agent.deadline
    # Setup has consumed 48 seconds of the 100-second work window. A source
    # request and read leave 45 seconds; another request/command could use 26.
    clock = {"now": deadline - 52}
    monkeypatch.setattr(time, "monotonic", lambda: clock["now"])
    original_request = agent.connector.request_response
    def request(body, **kwargs):
        clock["now"] += 6 if not requests else 20
        return original_request(body, **kwargs)
    agent.connector.request_response = request
    commands = []
    def investigate(argv):
        commands.append(argv)
        clock["now"] += 1
        return {"exit_code": 0}
    assert agent.run("planner", agent.task.public_context, investigate) == plan()
    assert len(commands) == 1 and len(requests) == 2
    assert [tool["name"] for tool in requests[0]["tools"]] == ["run", "finish"]
    assert [tool["name"] for tool in requests[1]["tools"]] == ["finish"]
    assert clock["now"] == deadline - 25 and agent.deadline == deadline
    assert agent.max_turns == 8 and agent.max_total_tokens == 1000
    assert journal.summary()["total_tokens"] == 20 and journal.summary()["usage_complete"]


def test_provider_cannot_execute_an_unoffered_last_turn_command(tmp_path):
    response = ("run", {"command": ["cat", "parser.py"]})
    agent, requests, journal = setup(tmp_path, [response, response], max_agent_turns=2)
    commands = []
    with pytest.raises(AgentSessionError, match="unavailable for this turn"):
        agent.run("planner", agent.task.public_context,
                  lambda argv: commands.append(argv) or {"exit_code": 0})
    assert len(commands) == 1 and len(requests) == 2
    assert journal.summary()["total_tokens"] == 20 and journal.summary()["usage_complete"]
    assert not any(e["kind"] == "planner_report" for e in journal.events())


def test_one_request_cannot_authorize_investigation_and_a_report(tmp_path):
    with pytest.raises(AgentSessionError, match="investigation request and a report"):
        setup(tmp_path, [], max_agent_turns=1)
