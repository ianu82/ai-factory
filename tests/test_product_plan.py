import copy
import json

import pytest

from auto_mindsdb_factory.product_plan import PlanError, ProductPlan, freeze_plan, read_plan
from auto_mindsdb_factory.product_roles import select_role
from auto_mindsdb_factory.product_task import ProductTask
from auto_mindsdb_factory.run_journal import RunJournal


@pytest.fixture
def inputs():
    task = ProductTask.from_document({"id": "plan-test", "title": "Fix parser", "goal": "Reject malformed input",
        "acceptance_criteria": [{"id": "reject", "description": "Reject invalid input", "command": ["pytest"]}]})
    return {"task": task, "base_sha": "a" * 40, "profile_sha": "b" * 64}


def content():
    return {"status": "ready", "reason": "The parser and regression entry point were inspected",
        "goal": "Reject invalid input without changing valid parsing", "constraints": ["Keep public API"],
        "source_paths": ["src/parser.py", "tests/test_parser.py"], "changes": ["Add validation and regression"],
        "risks": ["Preserve empty-input compatibility"], "questions": [],
        "validation": [{"criterion_id": "reject", "evidence": "Run malformed and valid-input regression cases"}]}


def make(inputs, proposed=None):
    return ProductPlan.create(proposed or content(), planner_role=select_role("planner"), **inputs)


def test_plan_is_deterministic_detached_and_bound(inputs):
    body = content()
    plan = make(inputs, body)
    assert plan == make(inputs, dict(reversed(list(body.items()))))
    body["changes"].clear()
    detached = plan.document
    detached["content"]["changes"].clear()
    assert plan.document["content"]["changes"]
    for key, value in [("base_sha", "c" * 40), ("profile_sha", "d" * 64)]:
        with pytest.raises(PlanError):
            ProductPlan.from_document(plan.document, **{**inputs, key: value})
    other = ProductTask.from_document({**inputs["task"].document, "goal": "Different authorization"})
    with pytest.raises(PlanError):
        ProductPlan.from_document(plan.document, **{**inputs, "task": other})


@pytest.mark.parametrize("field,value", [
    ("goal", "  "), ("constraints", []), ("source_paths", ["../controller"]),
    ("source_paths", ["/etc/passwd"]), ("source_paths", ["..\\controller"]),
    ("questions", ["Does the task authorize changing the API?"]), ("changes", []),
    ("validation", []), ("validation", [{"criterion_id": "other", "evidence": "pytest"}]),
    ("status", "approved"), ("risks", "none"), ("reason", "x" * 2001),
])
def test_malformed_or_ambiguous_plan_cannot_be_ready(inputs, field, value):
    body = content()
    body[field] = value
    with pytest.raises(PlanError):
        make(inputs, body)


def test_duplicate_mapping_unknown_role_and_forged_digest_fail(inputs):
    body = content()
    body["validation"] *= 2
    with pytest.raises(PlanError):
        make(inputs, body)
    for role in [select_role("builder"), {**select_role("planner"), "version": "999"},
                 {**select_role("planner"), "digest": "0" * 64}]:
        with pytest.raises(PlanError):
            ProductPlan.create(content(), planner_role=role, **inputs)


@pytest.mark.parametrize("status", ["blocked", "unverified"])
def test_blocked_guidance_keeps_reason_and_questions(inputs, status):
    body = {**content(), "status": status, "reason": "Public contract is ambiguous", "changes": [],
            "questions": ["Which empty-input behavior is intended?"]}
    assert make(inputs, body).document["content"] == body


def test_freeze_is_immutable_idempotent_and_does_not_rewrite_legacy(inputs, tmp_path):
    journal = RunJournal(tmp_path, "planning")
    journal.create({"legacy": True})
    journal.append("old_event", value="unchanged")
    original = {p.name: p.read_bytes() for p in journal.root.glob("*.json")}
    with pytest.raises(PlanError):
        read_plan(journal, **inputs)
    plan = make(inputs)
    assert freeze_plan(journal, plan, **inputs) == plan
    path = journal.root / "plan.json"
    before = (path.read_bytes(), path.stat().st_mtime_ns)
    assert freeze_plan(journal, plan, **inputs) == plan
    assert before == (path.read_bytes(), path.stat().st_mtime_ns)
    assert all((journal.root / name).read_bytes() == raw for name, raw in original.items())
    with pytest.raises(PlanError):
        freeze_plan(journal, make(inputs, {**content(), "changes": ["Different implementation"]}), **inputs)
    record = json.loads(path.read_text())
    record["plan"]["content"]["changes"] = ["Forged guidance"]
    path.write_text(json.dumps(record))
    with pytest.raises(PlanError):
        read_plan(journal, **inputs)


def test_no_candidate_path_or_direct_constructor_bypass(inputs, tmp_path):
    journal = RunJournal(tmp_path, "planning")
    outside = tmp_path / "candidate.json"
    outside.write_text("{}")
    (journal.root / "plan.json").symlink_to(outside)
    with pytest.raises(PlanError):
        freeze_plan(journal, make(inputs), **inputs)
    assert outside.read_text() == "{}"
    forged = copy.deepcopy(make(inputs).document)
    forged["task_sha"] = "0" * 64
    with pytest.raises(PlanError):
        freeze_plan(journal, ProductPlan(json.dumps(forged)), **inputs)
    with pytest.raises(PlanError):
        freeze_plan(journal, ProductPlan("invalid JSON"), **inputs)


@pytest.mark.parametrize("field,value", [("version", True), ("version", 2), ("planner_role", None),
    ("content", []), ("task_sha", "0" * 64), ("extra", "unexpected")])
def test_invalid_envelopes_fail_closed(inputs, field, value):
    document = make(inputs).document
    document[field] = value
    with pytest.raises(PlanError):
        ProductPlan.from_document(document, **inputs)
