"""Security constraints and changed files are a required, carried-through part of every plan."""
from __future__ import annotations

import asyncio
from datetime import timedelta
from types import SimpleNamespace

from aifactory import prompts, serializers
from aifactory.db import session_scope, utcnow
from aifactory.harness.schemas import validate_report
from aifactory.models import Message, Plan, Task
from aifactory.pipeline import planning
from aifactory.pipeline.runtime import guidance_history

from .test_pipeline import factory, new_outcome  # noqa: F401 - workspace, repository and user fixture


def task(key, files, depends_on=(), repo="acme/demo", constraints=("Rejects input over 1 MiB",)):
    return {"key": key, "title": key, "repo": repo, "spec": "s", "acceptance": ["a"], "constraints": list(constraints),
            "files": list(files), "depends_on": list(depends_on)}


def plan(*tasks):
    return {"summary": "s", "tasks": list(tasks), "questions": [], "risks": []}


def test_plan_requires_constraints_and_files():
    for missing in ("constraints", "files"):
        bad = task("T1", ["a.py"])
        bad[missing] = []
        ok, err = validate_report("planner", plan(bad))
        assert ok is None and missing in err
    no_field = task("T1", ["a.py"])
    del no_field["constraints"]
    assert validate_report("planner", plan(no_field))[0] is None
    blank = task("T1", ["a.py"], constraints=[" "])
    assert validate_report("planner", plan(blank))[0] is None


def test_tasks_sharing_a_file_run_one_after_the_other():
    ok, err = validate_report("planner", plan(task("T1", ["src/models.py", "a.py"]), task("T2", ["./src/models.py"]),
                                              task("T3", ["b.py"]), task("T4", ["src/models.py"], repo="acme/other")))
    assert err == ""
    deps = {t["key"]: t["depends_on"] for t in ok["tasks"]}
    assert deps == {"T1": [], "T2": ["T1"], "T3": [], "T4": []}  # another repository never conflicts


def test_existing_order_is_kept_and_no_cycle_is_added():
    ok, _ = validate_report("planner", plan(task("T1", ["x.py"], depends_on=["T2"]), task("T2", ["x.py"])))
    assert {t["key"]: t["depends_on"] for t in ok["tasks"]} == {"T1": ["T2"], "T2": []}
    ok, _ = validate_report("planner", plan(task("T1", ["x.py"]), task("T2", ["y.py"], depends_on=["T1"]),
                                            task("T3", ["x.py"], depends_on=["T2"])))
    assert ok["tasks"][2]["depends_on"] == ["T2"]  # already after T1 through T2


def test_approved_plan_carries_constraints_and_files(factory):
    ids = factory
    oid = new_outcome(ids)
    content = plan(task("T1", ["greet.sh"], constraints=["Never echoes the token", "Fails closed without a sandbox"]))
    with session_scope() as s:
        p = Plan(outcome_id=oid, version=1, summary="s", content=content)
        s.add(p)
        s.flush()
        plan_id = p.id
    assert planning.approve_plan(plan_id, approved_by=ids["user"])
    with session_scope() as s:
        t = s.query(Task).filter_by(outcome_id=oid).one()
        assert t.constraints == ["Never echoes the token", "Fails closed without a sandbox"]
        assert t.files == ["greet.sh"]
        out = serializers.task(t)
        assert out["constraints"] == t.constraints and out["files"] == ["greet.sh"]


def test_builder_and_verifier_see_numbered_constraints():
    t = SimpleNamespace(key="T1", title="Add", spec="Do it", acceptance=["Works"], constraints=["Bounded to 1 MiB"],
                        files=["a.py"])
    repo = SimpleNamespace(full_name="acme/demo", config={})
    outcome = SimpleNamespace(number=1, title="O", brief="")
    built = prompts.build_prompt(t, outcome, repo, [], [])
    assert "C1. Bounded to 1 MiB" in built and "- a.py" in built
    verify = prompts.verify_prompt(t, repo, "abc", {"summary": "x"}, [])
    assert "C1. Bounded to 1 MiB" in verify and "probe each adversarially" in verify
    legacy = SimpleNamespace(key="T1", title="Add", spec="Do it", acceptance=["Works"])  # tasks planned before
    assert "Constraints" not in prompts.verify_prompt(legacy, repo, "abc", {"summary": "x"}, [])
    assert "constraints" in prompts.PLANNER and "fail closed" in prompts.PLANNER
    assert "C1, C2" in prompts.VERIFIER


def test_planner_sees_protected_paths_and_task_guidance(factory):
    ids = factory
    oid = new_outcome(ids)
    with session_scope() as s:
        p = Plan(outcome_id=oid, version=1, summary="s", content=plan(task("T1", ["greet.sh"])))
        s.add(p)
        s.flush()
        s.add(Task(workspace_id=ids["ws"], outcome_id=oid, plan_id=p.id, key="T1", title="t", repo_id=ids["repo"]))
        s.flush()
        tid = s.query(Task).filter_by(outcome_id=oid).one().id
        now = utcnow()
        s.add_all([Message(workspace_id=ids["ws"], outcome_id=oid, author_kind="user", body="Outcome note",
                           created_at=now - timedelta(seconds=2)),
                   Message(workspace_id=ids["ws"], outcome_id=oid, task_id=tid, author_kind="user",
                           body="Operator decision: fail closed", created_at=now - timedelta(seconds=1))])
    assert guidance_history(oid) == ["Outcome note"]
    assert guidance_history(oid, all_tasks=True) == ["Outcome note", "T1: Operator decision: fail closed"]
    repo = SimpleNamespace(full_name="acme/demo", default_branch="main", config={"protected_paths": [".github/**"]})
    outcome = SimpleNamespace(number=1, title="O", brief="b", acceptance=[])
    text = prompts.plan_prompt(outcome, [repo], [], guidance_history(oid, all_tasks=True))
    assert "Never changed by the factory: .github/**" in text and "T1: Operator decision: fail closed" in text


def test_planning_end_to_end_stores_constraints(factory):
    ids = factory
    oid = new_outcome(ids)
    asyncio.run(planning.plan_outcome(oid, asyncio.Event()))
    with session_scope() as s:
        tasks = s.query(Task).filter_by(outcome_id=oid).all()
        assert tasks and tasks[0].constraints == ["Only greet.sh changes"] and tasks[0].files == ["greet.sh"]


def test_globs_count_as_overlapping_files():
    ok, _ = validate_report("planner", plan(task("T1", ["src/*.py"]), task("T2", ["src/models.py"]),
                                            task("T3", ["web/**"])))
    assert {t["key"]: t["depends_on"] for t in ok["tasks"]} == {"T1": [], "T2": ["T1"], "T3": []}


def test_a_pass_must_judge_every_constraint_and_criterion():
    from aifactory.harness.schemas import missing_coverage

    full = [{"criterion": "Works", "status": "pass"}, {"criterion": "C1. Bounded", "status": "pass"},
            {"criterion": "C2", "status": "pass"}]
    assert missing_coverage(["Works"], ["Bounded to 1 MiB", "Never logs the token"], full) == []
    assert missing_coverage(["Works"], ["Bounded to 1 MiB", "Never logs the token"], full[:2]) == ["C2"]
    assert missing_coverage(["Works", "Tested"], [], full[:1]) == ["1 of 2 acceptance criteria"]
    by_text = [{"criterion": "Works"}, {"criterion": "never logs the token: checked with a probe"}]
    assert missing_coverage(["Works"], ["Never logs the token"], by_text) == []
    assert missing_coverage(["Works"], ["Never logs the token"], []) == ["C1", "1 of 1 acceptance criteria"]
