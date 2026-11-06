import json
import time
from contextlib import contextmanager
from types import SimpleNamespace

import pytest

from auto_mindsdb_factory.product_planning import PlanningStopped, plan_delivery, planning_manifest, retained_plan
from auto_mindsdb_factory.product_profile import ProductProfile
from auto_mindsdb_factory.product_task import ProductTask
from auto_mindsdb_factory.run_journal import RunJournal


@pytest.fixture
def phase(tmp_path):
    profile = ProductProfile.from_document({"version": 1, "name": "planning", "repository": "owner/repo",
        "image": "python@sha256:" + "a" * 64, "checks": {"unit": ["test"]}, "smoke": [["test"]]})
    task = ProductTask.from_document({"id": "plan", "title": "Parser", "goal": "Fix invalid input",
        "acceptance_criteria": [{"id": "reject", "description": "Reject invalid input", "command": ["test"]}]})
    journal = RunJournal(tmp_path / "store", "plan")
    journal.create({**planning_manifest(profile), "task": task.document})
    calls, contexts = [], []
    def prepare(base, workspace, journal, **kwargs):
        calls.append(("prepare", kwargs))
        workspace.mkdir()
        (workspace / "parser.py").write_text("original")
        return {"parser.py": b"original"}
    @contextmanager
    def sandbox(workspace, image, **kwargs):
        calls.append(("sandbox", kwargs))
        yield SimpleNamespace(workspace=workspace)
        calls.append(("closed", {}))
    def command(sandbox, argv, **kwargs):
        calls.append(("command", kwargs))
        assert (sandbox.workspace / "parser.py").read_text() == "original"
        return {"exit_code": 0}
    delivery = SimpleNamespace(profile=profile, settings=profile.document, deadline=time.monotonic() + 300,
        _prepare=prepare, sandbox_factory=sandbox, _command=command, prepare_runner=lambda journal: None)
    content = {"status": "ready", "reason": "Inspected parser", "goal": "Reject invalid input",
        "constraints": ["Keep API"], "source_paths": ["parser.py"], "changes": ["Validate input"],
        "risks": ["Preserve valid input"], "questions": [],
        "validation": [{"criterion_id": "reject", "evidence": "Run parser regression"}]}
    def factory(task, base, journal, deadline):
        assert deadline <= delivery.deadline and deadline - time.monotonic() <= 120
        def run(role, context, command):
            contexts.append(context)
            command(["cat", "parser.py"])
            return content
        return SimpleNamespace(run=run)
    return SimpleNamespace(delivery=delivery, task=task, base="b" * 40, journal=journal,
        calls=calls, contexts=contexts, content=content, factory=factory)


def execute(p):
    return plan_delivery(p.delivery, p.task, p.base, p.journal, planner_factory=p.factory)


def test_phase_freezes_after_readonly_investigation_and_reuses_without_writes(phase):
    p = phase
    result = execute(p)
    assert [kind for kind, _ in p.calls] == ["prepare", "sandbox", "command", "closed"]
    assert p.calls[1][1]["writable"] is False
    assert p.calls[0][1]["deadline"] == p.calls[2][1]["deadline"]
    assert p.contexts[0]["acceptance_criteria"] == p.task.public_context["acceptance_criteria"]
    before = {file.name: file.read_bytes() for file in p.journal.root.iterdir()}
    assert retained_plan(p.delivery, p.task, p.base, p.journal) == result
    assert before == {file.name: file.read_bytes() for file in p.journal.root.iterdir()}
    with pytest.raises(PlanningStopped):
        execute(p)
    assert len(p.contexts) == 1


@pytest.mark.parametrize("status", ["blocked", "unverified"])
def test_unresolved_plan_retained_but_never_admitted(phase, status):
    p = phase
    p.content.update(status=status, questions=["What is the intended contract?"])
    with pytest.raises(PlanningStopped):
        execute(p)
    assert json.loads((p.journal.root / "plan.json").read_text())["plan"]["content"]["status"] == status


def test_planning_exposes_controller_binding_without_manufacturing_validation(phase):
    p = phase
    p.content["risks"] = ["Git metadata is absent; full implementation checks remain unrun"]
    result = execute(p)
    assert p.contexts[0]["source_environment"] == {
        "source_access": "read_only", "git_metadata": "absent", "base_binding": "controller_export"}
    assert p.contexts[0]["base_sha"] == p.base
    assert p.contexts[0]["required_checks"] == p.delivery.settings["checks"]
    assert result["guidance"] == p.content
    assert not any(e["kind"] in {"verification_check", "agent_report", "candidate_verified",
                                  "merge_confirmed", "post_merge_smoke"} for e in p.journal.events())


@pytest.mark.parametrize("mode", ["unknown", "exhausted", "expired", "missing-role"])
def test_admission_stops_before_environment_or_provider(phase, mode):
    p = phase
    if mode == "unknown":
        p.journal.append("model_failed")
    elif mode == "exhausted":
        p.journal.append("model_response", usage={"total_tokens": p.delivery.settings["max_total_tokens"]})
    elif mode == "expired":
        p.delivery.deadline = time.monotonic() - 1
    else:
        (p.journal.root / "manifest.json").write_text("{}")
    with pytest.raises(PlanningStopped):
        execute(p)
    assert not p.calls and not p.contexts


def test_worker_bootstrap_uses_existing_run_budget_before_the_bounded_planning_phase(phase, monkeypatch):
    p = phase
    clock = {"now": 0.0}
    monkeypatch.setattr(time, "monotonic", lambda: clock["now"])
    p.delivery.deadline = 300
    def bootstrap(journal):
        assert journal is p.journal
        clock["now"] = 150
    p.delivery.prepare_runner = bootstrap
    execute(p)
    assert p.delivery.deadline == 300  # Hardware preparation never extends the run.
    assert p.calls[0][1]["deadline"] == 250  # Reserve finalization within the same 120 seconds.
    start = next(e["data"] for e in p.journal.events() if e["kind"] == "planning_started")
    assert start == {"max_seconds": 120, "remaining_run_seconds": 150, "finalization_reserve_seconds": 20}


def test_bootstrap_cannot_reset_an_exhausted_global_deadline(phase, monkeypatch):
    p = phase
    clock = {"now": 0.0}
    monkeypatch.setattr(time, "monotonic", lambda: clock["now"])
    p.delivery.deadline = 100
    p.delivery.prepare_runner = lambda journal: clock.update(now=101)
    with pytest.raises(PlanningStopped, match="during runner preparation"):
        execute(p)
    assert p.delivery.deadline == 100 and not p.calls and not p.contexts


def test_no_preparation_is_admitted_after_usage_exhaustion(phase):
    p = phase
    p.delivery.prepare_runner = lambda journal: pytest.fail("Exhausted run allocated a worker")
    p.journal.append("model_response", usage={"total_tokens": p.delivery.settings["max_total_tokens"]})
    with pytest.raises(PlanningStopped):
        execute(p)


def test_one_request_stops_before_allocating_hardware(phase):
    phase.delivery.settings = {**phase.delivery.settings, "max_agent_turns": 1}
    phase.delivery.prepare_runner = lambda journal: pytest.fail("Infeasible planner allocated hardware")
    with pytest.raises(PlanningStopped, match="investigation request and a report"):
        execute(phase)
    assert not phase.calls and not phase.contexts


@pytest.mark.parametrize("cleanup_seconds,admitted", [(4, True), (20, False)])
def test_report_finalization_is_reserved_but_the_phase_deadline_still_blocks(phase, monkeypatch, cleanup_seconds, admitted):
    p = phase
    clock = {"now": 0.0}
    monkeypatch.setattr(time, "monotonic", lambda: clock["now"])
    p.delivery.deadline = 300
    @contextmanager
    def sandbox(*args, **kwargs):
        yield SimpleNamespace(workspace=args[0])
        clock["now"] += cleanup_seconds
    p.delivery.sandbox_factory = sandbox
    def factory(task, base, journal, deadline):
        assert deadline == 100  # Total phase still ends at 120.
        def run(role, context, command):
            command(["cat", "parser.py"])
            clock["now"] = deadline
            return p.content
        return SimpleNamespace(run=run)
    p.factory = factory
    if admitted:
        execute(p)
        assert (p.journal.root / "plan.json").exists()
    else:
        with pytest.raises(PlanningStopped):
            execute(p)
        assert not (p.journal.root / "plan.json").exists()
    validation = next(e["data"] for e in p.journal.events() if e["kind"] == "planning_validation")
    assert validation == {"investigated": True, "within_deadline": admitted,
                          "source_unchanged": True, "modes_unchanged": True}
    assert p.delivery.deadline == 300


def test_finalization_reserve_stops_infeasible_work_before_setup(phase, monkeypatch):
    monkeypatch.setattr(time, "monotonic", lambda: 0)
    phase.delivery.deadline = 20
    with pytest.raises(PlanningStopped, match="safe finalization"):
        execute(phase)
    assert not phase.calls and not phase.contexts


@pytest.mark.parametrize("mode", ["self-consistent-tamper", "missing-ack", "changed-task", "legacy"])
def test_execution_requires_original_ready_acknowledgement(phase, mode):
    p = phase
    if mode != "legacy":
        execute(p)
    if mode == "self-consistent-tamper":
        from auto_mindsdb_factory.product_plan import ProductPlan
        path = p.journal.root / "plan.json"
        record = json.loads(path.read_text())
        record["plan"]["content"]["changes"] = ["different guidance"]
        record["digest"] = ProductPlan(json.dumps(record["plan"], sort_keys=True, separators=(",", ":"))).digest
        path.write_text(json.dumps(record))
    elif mode == "missing-ack":
        (p.journal.root / "event-000001.json").unlink()
    elif mode == "changed-task":
        p.task = ProductTask.from_document({**p.task.document, "goal": "different task"})
    with pytest.raises(PlanningStopped):
        retained_plan(p.delivery, p.task, p.base, p.journal)


@pytest.mark.parametrize("mode", ["no-investigation", "source-write", "mode-write", "ack-write"])
def test_investigation_and_durable_acknowledgement_are_required(phase, monkeypatch, mode):
    p = phase
    if mode == "no-investigation":
        p.factory = lambda *args: SimpleNamespace(run=lambda *args: p.content)
    elif mode in {"source-write", "mode-write"}:
        def mutate(sandbox, argv, **kwargs):
            path = sandbox.workspace / "parser.py"
            path.write_text("changed") if mode == "source-write" else path.chmod(0o755)
            return {"exit_code": 0}
        p.delivery._command = mutate
    else:
        monkeypatch.setattr(p.journal, "append", lambda *args, **kwargs: (_ for _ in ()).throw(OSError()))
    with pytest.raises(PlanningStopped):
        execute(p)
    with pytest.raises(PlanningStopped):
        retained_plan(p.delivery, p.task, p.base, p.journal)
