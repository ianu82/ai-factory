"""Budget checks at the actual delivery retry boundary, with a fake provider/sandbox."""
import json
import tempfile
from pathlib import Path
from types import SimpleNamespace

import pytest

from auto_mindsdb_factory.product_agent import ProductAgent
from auto_mindsdb_factory.product_delivery import DeliveryError, ProductDelivery
from auto_mindsdb_factory.product_profile import ProductProfile
from auto_mindsdb_factory.product_task import ProductTask
from auto_mindsdb_factory.product_workspace import git
from auto_mindsdb_factory.run_journal import RunJournal


def response(tokens, tool="run", args=None, **extra):
    return {"id": "response", "model": "fake", "status": "completed",
            "usage": tokens, "output": [{"type": "function_call", "name": tool,
            "call_id": "call", "arguments": json.dumps(args or {"command": ["edit"]})}], **extra}


def usage(total):
    return {"total_tokens": total, "input_tokens": total - 1, "output_tokens": 1,
            "input_tokens_details": {"cached_tokens": total - 1}}


def report(status):
    return {"status": status, "summary": "checked", "findings": ["defect"] if status == "failed" else [],
            "criteria": [{"id": "a", "status": status, "evidence": "observed"}]}


@pytest.fixture
def product_root():
    # Keep test Git metadata out of the source tree even with --basetemp.
    with tempfile.TemporaryDirectory(prefix="delivery-budget-", dir="/tmp") as root:
        yield Path(root)


@pytest.fixture
def delivery_case(product_root, monkeypatch):
    tmp_path = product_root
    import auto_mindsdb_factory.product_delivery as module

    target = tmp_path / "target"
    target.mkdir()
    git(target, "init", "-q")
    git(target, "config", "user.name", "test")
    git(target, "config", "user.email", "test@example.com")
    git(target, "remote", "add", "origin", "https://github.com/owner/product.git")
    (target / "app.txt").write_text("original")
    git(target, "add", "app.txt")
    git(target, "commit", "-qm", "base")
    base = git(target, "rev-parse", "HEAD")
    # Source exports do not have controller Git metadata. All target Git operations remain real.
    real_git = module.git
    monkeypatch.setattr(module, "git", lambda root, *args: base if root == module.repo_root()
                        and args == ("rev-parse", "HEAD") else real_git(root, *args))
    profile = ProductProfile.from_document({"version": 1, "name": "p", "repository": "owner/product",
        "image": "test@sha256:" + "a" * 64, "setup": [["setup"]], "checks": {"unit": ["true"]},
        "smoke": [["true"]], "max_attempts": 2, "max_total_tokens": 1000})
    task = ProductTask.from_document({"id": "budget", "title": "Fix", "goal": "fix",
        "acceptance_criteria": [{"id": "a", "description": "works", "command": ["true"]}]})
    requests, responses, sandboxes, commands, stages, prepares = [], [], [], [], [], []

    class Sandbox:
        def __init__(self, workspace, image, **kwargs):
            self.workspace = workspace
            sandboxes.append(kwargs)
        def __enter__(self): return self
        def __exit__(self, *args): pass
        def run(self, command):
            commands.append(command)
            if command == ["edit"]:
                (self.workspace / "app.txt").write_text("changed")
            return {"command": command, "exit_code": 0}

    def agent(role, journal):
        def request(body, **kwargs):
            requests.append((role, body))
            assert responses, "unexpected model request"
            result = responses.pop(0)
            if isinstance(result, Exception):
                raise result
            return result
        connector = SimpleNamespace(config=SimpleNamespace(model="fake", max_output_tokens=1000,
            reasoning_effort="low", timeout_seconds=10), request_response=request)
        return ProductAgent(connector, journal, max_turns=5, max_total_tokens=1000, deadline=delivery.deadline)

    delivery = ProductDelivery(profile, target, tmp_path / "store", agent_factory=agent, sandbox_factory=Sandbox)
    real_prepare = delivery._prepare
    def prepare(sha, workspace, journal):
        prepares.append(sha)
        return real_prepare(sha, workspace, journal)
    monkeypatch.setattr(delivery, "_prepare", prepare)
    journal = RunJournal(tmp_path / "store", "budget")
    return SimpleNamespace(delivery=delivery, task=task, base=base, journal=journal, requests=requests,
        responses=responses, sandboxes=sandboxes, commands=commands, stages=stages, prepares=prepares)


def run(case):
    return case.delivery.run(case.task, base_sha=case.base, on_stage=case.stages.append)


def builder():
    return [response(usage(10)), response(usage(10), "finish", report("passed"))]


@pytest.mark.parametrize("prior", [usage(1000), usage(1001), None])
def test_preexisting_exhausted_or_unknown_usage_prevents_first_attempt(delivery_case, monkeypatch, prior):
    case = delivery_case
    real_append = RunJournal.append
    def append(journal, kind, **data):
        result = real_append(journal, kind, **data)
        if kind == "base_resolved":
            real_append(journal, "model_requested", role="prior")
            real_append(journal, "model_response", role="prior", usage=prior)
        return result
    monkeypatch.setattr(RunJournal, "append", append)
    with pytest.raises(DeliveryError, match="unknown" if prior is None else "token budget exhausted"):
        run(case)
    assert not case.prepares and not case.sandboxes and not case.commands and not case.requests and not case.stages
    assert case.journal.summary()["status"] == "blocked"
    assert case.journal.summary()["attempts"] == 0


@pytest.mark.parametrize("tokens,status,expected_reason", [
    (usage(985), "completed", "Run token budget exhausted after response"),
    (usage(980), "incomplete", "Model response did not complete"),
    (None, "completed", "Model usage unavailable; cannot enforce run token budget"),
    (usage(985), "incomplete", "Model response did not complete"),
])
def test_verifier_response_stops_retry_without_losing_evidence(delivery_case, tokens, status, expected_reason):
    case = delivery_case
    last = response(tokens, status=status)
    case.responses.extend(builder() + [last])
    with pytest.raises(DeliveryError, match="unknown" if tokens is None else "token budget exhausted"):
        run(case)
    events = case.journal.events()
    recorded = [e["data"] for e in events if e["kind"] == "model_response"]
    assert len(recorded) == 3 and recorded[-1]["usage"] == tokens and recorded[-1]["status"] == status
    assert recorded[-1]["role"] == "verifier" and recorded[-1]["response_id"] == last["id"]
    failures = [e["data"] for e in events if e["kind"] == "attempt_finished"]
    assert failures == [{"attempt": 1, "status": "failed", "reason": expected_reason}]
    assert case.journal.summary()["status"] == "blocked" and case.journal.summary()["attempts"] == 1
    assert case.journal.summary()["total_tokens"] == 20 + (tokens["total_tokens"] if tokens else 0)
    assert case.journal.summary()["usage_complete"] is (tokens is not None)
    assert len(case.requests) == 3 and len(case.prepares) == 2
    assert len(case.sandboxes) == 4 and case.commands.count(["setup"]) == 2
    assert case.stages == ["building", "verifying"]


def test_failed_candidate_with_remaining_budget_retries_and_passes(delivery_case):
    case = delivery_case
    case.responses.extend(builder() + [response(usage(10)), response(usage(10), "finish", report("failed"))]
        + builder() + [response(usage(10)), response(usage(10), "finish", report("passed"))])
    result = run(case)
    assert result["status"] == "verified" and result["usage"]["attempts"] == 2
    assert result["usage"]["total_tokens"] == 80
    events = case.journal.events()
    failures = [e["data"] for e in events if e["kind"] == "attempt_finished"]
    assert failures[0]["status"] == "failed" and "defect" in failures[0]["reason"]
    assert failures[1]["status"] == "verified"
    assert len(case.prepares) == 4 and len(case.sandboxes) == 8 and len(case.requests) == 8
    assert case.commands.count(["setup"]) == 4
    assert case.stages == ["building", "verifying", "building", "verifying"]
    assert "defect" in json.loads(case.requests[4][1]["input"][0]["content"])["revision_feedback"]


def test_failed_verifier_at_exact_cap_keeps_report_and_stops_retry(delivery_case):
    case = delivery_case
    case.responses.extend(builder() + [response(usage(10)), response(usage(970), "finish", report("failed"))])
    with pytest.raises(DeliveryError, match="token budget exhausted"):
        run(case)
    events = case.journal.events()
    verifier_report = next(e["data"]["report"] for e in events if e["kind"] == "agent_report"
                           and e["data"]["role"] == "verifier")
    failure = next(e["data"] for e in events if e["kind"] == "attempt_finished")
    assert verifier_report == report("failed") and "defect" in failure["reason"]
    assert case.journal.summary()["total_tokens"] == 1000
    assert case.journal.summary()["status"] == "blocked" and case.journal.summary()["attempts"] == 1
    assert len(case.requests) == 4 and len(case.prepares) == 2 and len(case.sandboxes) == 4
    assert case.commands.count(["setup"]) == 2 and case.stages == ["building", "verifying"]


def test_provider_failure_unknown_usage_stops_retry(delivery_case):
    case = delivery_case
    case.responses.extend(builder() + [RuntimeError("provider private data")])
    with pytest.raises(DeliveryError, match="unknown"):
        run(case)
    events = case.journal.events()
    assert sum(e["kind"] == "model_failed" for e in events) == 1
    failure = next(e["data"] for e in events if e["kind"] == "attempt_finished")
    assert failure["reason"] == "RuntimeError"
    assert case.journal.summary()["usage_complete"] is False
    assert case.journal.summary()["status"] == "blocked" and case.journal.summary()["attempts"] == 1
    assert len(case.requests) == 3 and len(case.prepares) == 2 and len(case.sandboxes) == 4
    assert case.commands.count(["setup"]) == 2 and case.stages == ["building", "verifying"]
