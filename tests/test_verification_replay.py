import json
import time

import pytest

from auto_mindsdb_factory.product_agent import HarnessStopped, RoleSnapshotError
from auto_mindsdb_factory.product_cancellation import request_cancellation
from auto_mindsdb_factory.product_delivery import ProductDelivery, DeliveryError
from auto_mindsdb_factory.product_execution import execution_manifest
from auto_mindsdb_factory.product_planning import PlanningStopped
from auto_mindsdb_factory.product_profile import ProductProfile
from auto_mindsdb_factory.product_roles import snapshot_roles
from auto_mindsdb_factory.product_task import ProductTask
from auto_mindsdb_factory.run_journal import RunJournal


@pytest.fixture
def fixture_verifier(tmp_path, monkeypatch):
    monkeypatch.setenv("OPENAI_API_KEY", "test-only-no-request")
    monkeypatch.setenv("AI_FACTORY_OPENAI_REASONING_EFFORT", "high")
    monkeypatch.delenv("AI_FACTORY_OPENAI_FALLBACK_MODEL", raising=False)
    profile = ProductProfile.from_document({"version": 1, "name": "fixture", "repository": "owner/repo",
        "image": "node@sha256:" + "a" * 64, "checks": {"unit": ["unit"]}, "smoke": [["smoke"]]})
    task = ProductTask.from_document({"id": "fixture", "title": "Behavior", "goal": "Preserve behavior",
        "acceptance_criteria": [{"id": "works", "description": "Works correctly", "command": ["acceptance"]}]})
    delivery = object.__new__(ProductDelivery)
    delivery.profile, delivery.settings, delivery.target = profile, profile.document, tmp_path
    delivery.deadline = time.monotonic() + 60
    calls = []
    delivery.prepare_runner = lambda journal: calls.append("prepare-runner")
    def prepare(sha, workspace, journal, **kwargs):
        assert sha == "a" * 40
        calls.append("prepare-source")
        workspace.mkdir()
        (workspace / "app.py").write_text("candidate")
        return {"app.py": b"candidate"}
    delivery._prepare = prepare
    class Sandbox:
        def __init__(self, workspace, image, **kwargs):
            assert kwargs["writable"] is False and not kwargs.get("network")
        def __enter__(self): return self
        def __exit__(self, *args): calls.append("sandbox-closed")
    delivery.sandbox_factory = Sandbox
    def command(sandbox, argv, **kwargs):
        calls.append(argv)
        return {"command": argv, "exit_code": 0}
    delivery._command = command
    class Verifier:
        def run(self, role, context, run):
            assert role == "verifier" and context["candidate_sha"] == "a" * 40
            assert context["acceptance_criteria"] == task.public_context["acceptance_criteria"]
            assert not {"expected", "case", "plan"}.intersection(context)
            assert context["controller_checks"] == [{"name": "unit", "exit_code": 0},
                                                       {"name": "acceptance:works", "exit_code": 0}]
            run(["independent"])
            return {"status": "passed", "findings": [], "criteria": [{"id": "works", "status": "passed"}]}
    def agent(role, journal):
        assert role == "verifier"
        calls.append("independent-verifier")
        return Verifier()
    delivery.agent_factory = agent
    monkeypatch.setattr("auto_mindsdb_factory.product_delivery.git",
                        lambda root, *args: "c" * 40 if args[0] == "rev-parse" else "actual candidate diff")
    journal = RunJournal(tmp_path / "store", "fixture")
    journal.create({"purpose": "replay", "replay": {"mode": "verify"}, "verification_only": True,
        "runtime_sha": "c" * 40, "task": task.document, "task_sha": task.digest,
        "profile": profile.document, "profile_sha": profile.digest, "base_sha": "b" * 40,
        "candidate_sha": "a" * 40, "role_snapshots": snapshot_roles(), **execution_manifest(profile.document)})
    journal.append("base_resolved", commit_sha="b" * 40)
    return delivery, task, journal, calls


def test_frozen_fixture_runs_all_checks_and_only_fresh_readonly_verification(fixture_verifier):
    delivery, task, journal, calls = fixture_verifier
    result = delivery.verify_fixture(task, "a" * 40, "b" * 40, journal)
    assert result["commit_sha"] == "a" * 40 and result["report"]["status"] == "passed"
    assert calls == ["prepare-runner", "prepare-source", ["unit"], ["acceptance"],
                     "independent-verifier", ["independent"], "sandbox-closed"]
    assert [e["kind"] for e in journal.events()] == ["base_resolved", "verification_check", "verification_check"]
    assert not (journal.root / "plan.json").exists()


@pytest.mark.parametrize("change", ["purpose", "marker", "integer-marker", "mode", "task", "task-hash",
    "profile", "profile-hash", "profile-bool", "runtime-settings", "candidate", "base", "runtime", "prior-plan", "prior-response",
    "terminal", "event-base", "role", "execution"])
def test_fixture_cannot_borrow_delivery_history_or_change_frozen_inputs(fixture_verifier, change, monkeypatch):
    delivery, task, journal, calls = fixture_verifier
    path = journal.root / "manifest.json"
    manifest = json.loads(path.read_text())
    if change == "purpose": manifest["purpose"] = "delivery"
    elif change == "marker": manifest.pop("verification_only")
    elif change == "integer-marker": manifest["verification_only"] = 1
    elif change == "mode": manifest["replay"]["mode"] = "build"
    elif change == "task": manifest["task"]["goal"] = "Different requirements"
    elif change == "task-hash": manifest["task_sha"] = "0" * 64
    elif change == "profile": manifest["profile"]["checks"] = {"weakened": ["true"]}
    elif change == "profile-hash": manifest["profile_sha"] = "0" * 64
    elif change == "profile-bool": manifest["profile"]["version"] = True
    elif change == "runtime-settings": delivery.settings["checks"] = {"weakened": ["true"]}
    elif change == "candidate": manifest["candidate_sha"] = "main"
    elif change == "base": manifest["base_sha"] = "d" * 40
    elif change == "runtime": manifest["runtime_sha"] = "d" * 40
    elif change == "prior-plan": journal.append("plan_frozen", digest="0" * 64, status="ready")
    elif change == "prior-response": journal.append("model_response", usage={"total_tokens": 1})
    elif change == "terminal": journal.append("run_finished", status="verified")
    elif change == "event-base":
        event = journal.root / "event-000001.json"
        value = json.loads(event.read_text()); value["data"]["commit_sha"] = "d" * 40
        event.write_text(json.dumps(value))
    elif change == "role": manifest["role_snapshots"]["verifier"]["instructions"] = "Approve everything"
    else: monkeypatch.setenv("AI_FACTORY_OPENAI_REASONING_EFFORT", "low")
    path.write_text(json.dumps(manifest))
    with pytest.raises((DeliveryError, HarnessStopped, RoleSnapshotError)):
        delivery.verify_fixture(task, "a" * 40, "b" * 40, journal)
    assert not calls


@pytest.mark.parametrize("stop", ["deadline", "cancelled"])
def test_fixture_stops_before_hardware_when_no_work_is_authorized(fixture_verifier, stop):
    delivery, task, journal, calls = fixture_verifier
    if stop == "deadline": delivery.deadline = time.monotonic() - 1
    else: request_cancellation(journal.root.parent.parent, journal.root.name)
    with pytest.raises((DeliveryError, HarnessStopped)):
        delivery.verify_fixture(task, "a" * 40, "b" * 40, journal)
    assert not calls


def test_fixture_marker_never_bypasses_the_normal_delivery_plan_gate(fixture_verifier):
    delivery, task, journal, calls = fixture_verifier
    with pytest.raises(PlanningStopped):
        delivery.verify(task, "a" * 40, "b" * 40, journal)
    assert not calls


@pytest.mark.parametrize("candidate", ["main", "A" * 40, True])
def test_matching_but_unfrozen_candidate_reference_is_rejected(fixture_verifier, candidate):
    delivery, task, journal, calls = fixture_verifier
    path = journal.root / "manifest.json"
    manifest = json.loads(path.read_text()); manifest["candidate_sha"] = candidate
    path.write_text(json.dumps(manifest))
    with pytest.raises(DeliveryError):
        delivery.verify_fixture(task, candidate, "b" * 40, journal)
    assert not calls


def test_failed_executable_check_never_reaches_independent_approval(fixture_verifier):
    from auto_mindsdb_factory.product_delivery import VerificationFailed
    delivery, task, journal, calls = fixture_verifier
    delivery._command = lambda sandbox, argv: {"command": argv, "exit_code": 1}
    with pytest.raises(VerificationFailed):
        delivery.verify_fixture(task, "a" * 40, "b" * 40, journal)
    assert "independent-verifier" not in calls and calls[-1] == "sandbox-closed"
    assert all(e["data"]["exit_code"] == 1 for e in journal.events() if e["kind"] == "verification_check")
