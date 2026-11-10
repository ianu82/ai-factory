from __future__ import annotations

import json

import pytest
from fastapi import HTTPException

from aifactory.db import SessionLocal
from aifactory.mind.tools import MAX_ITEMS, Context, REGISTRY, clean, read_call
from aifactory.models import Decision, Event, Membership, Organization, Outcome, Plan, Repository, Run, Task, User, Workspace

SECRET = "sk-" + "s" * 40


@pytest.fixture
def world():
    with SessionLocal() as db:
        org = Organization(name="Org")
        users = [User(name="Alice", email="a@t"), User(name="Bob", email="b@t")]
        db.add_all([org, *users]); db.flush()
        work = []
        for i in range(3):
            ws = Workspace(org_id=org.id, name=f"Workspace {i}", slug=str(i))
            db.add(ws); db.flush()
            if i < 2:
                for u in users:
                    db.add(Membership(workspace_id=ws.id, user_id=u.id, role="member"))
            repo = Repository(workspace_id=ws.id, full_name=f"org/repo{i}")
            o = Outcome(workspace_id=ws.id, number=1, title=f"Outcome {SECRET}")
            db.add_all([repo, o]); db.flush()
            plan = Plan(outcome_id=o.id, content={"secret": SECRET})
            db.add(plan); db.flush()
            t = Task(workspace_id=ws.id, outcome_id=o.id, plan_id=plan.id, repo_id=repo.id,
                     key="T1", title="Task", status="blocked", spec=SECRET,
                     evidence={"ci": {"state": "passed"}})
            db.add(t); db.flush()
            run = Run(workspace_id=ws.id, outcome_id=o.id, task_id=t.id, role="builder", harness="native", cost_known=False)
            db.add(run); db.flush()
            question = Decision(workspace_id=ws.id, outcome_id=o.id, task_id=t.id, kind="question", title="Question",
                                body=SECRET, options=[{"value": "answer"}])
            unblock = Decision(workspace_id=ws.id, outcome_id=o.id, task_id=t.id, kind="unblock", title="Retry?",
                               options=[{"value": "retry"}])
            db.add_all([question, unblock])
            # This is the runtime's real storage format, not synthetic transcript artifacts.
            for j in range(8):
                db.add(Event(workspace_id=ws.id, outcome_id=o.id, task_id=t.id, run_id=run.id,
                             kind="agent_thought", message=f"{j} {SECRET} " + "x" * 2500,
                             data={"nested": {SECRET: [SECRET]}}))
            work.append((ws, o, t, question, unblock, run, plan))
        db.commit()
        yield db, users, work


@pytest.mark.parametrize("name", sorted(REGISTRY))
@pytest.mark.parametrize("person", [0, 1])
def test_read_family_authorization_and_redaction(world, name, person):
    db, users, work = world
    context = Context(db, users[person].id)
    for i, (ws, o, *_) in enumerate(work):
        args = {"workspace_id": ws.id}
        if name == "outcome_detail":
            args["outcome_id"] = o.id
        if i == 2:
            with pytest.raises(HTTPException) as exc:
                read_call(name, args, context)
            assert exc.value.status_code == 404
        else:
            result = read_call(name, args, context)
            assert result["items"] and result["provenance"]
            assert SECRET not in json.dumps(result)
            assert all(c["workspace_id"] == ws.id and c["href"].startswith(f"/w/{ws.id}/") for c in result["citations"])
    member = db.query(Membership).filter_by(user_id=users[person].id, workspace_id=work[0][0].id).one()
    db.delete(member); db.commit()
    with pytest.raises(HTTPException):
        read_call(name, {"workspace_id": work[0][0].id, **({"outcome_id": work[0][1].id} if name == "outcome_detail" else {})}, context)


def test_production_transcripts_bounded_cursor_and_progress(world):
    db, users, work = world
    ws, o, t, _, _, run, plan = work[0]
    context = Context(db, users[0].id)
    args = {"workspace_id": ws.id, "limit": 1}
    result = read_call("runs", args, context)
    excerpt = result["items"][0]["excerpt"]
    assert excerpt and len(excerpt) <= 2000 and SECRET not in excerpt
    assert result["truncated"] and result["items"][0]["excerpt_truncated"]
    assert result["items"][0]["cost_known"] is False
    first = read_call("recent_events", args, context)
    second = read_call("recent_events", {**args, "cursor": first["next_cursor"]}, context)
    assert second["items"][0]["id"] < first["items"][0]["id"]
    assert SECRET not in json.dumps(first)
    db.add(Task(workspace_id=ws.id, outcome_id=o.id, plan_id=plan.id, repo_id=t.repo_id, key="T2",
                title="Second", status="waiting_ci", stage="ci"))
    db.commit()
    detail = read_call("outcome_detail", {**args, "outcome_id": o.id}, context)
    assert len(detail["items"][0]["tasks"]) == 1 and detail["truncated"]
    assert detail["items"][0]["outcome"]["progress"]["total"] == 2
    assert read_call("overview", args, context)["items"][0]["counts"]["waiting"] == 2


@pytest.mark.parametrize("name", ["runs", "list_tasks", "open_decisions", "outcome_detail"])
def test_forged_nested_ids(world, name):
    db, users, work = world
    with pytest.raises(HTTPException):
        read_call(name, {"workspace_id": work[0][0].id, "outcome_id": work[1][1].id}, Context(db, users[0].id))
    with pytest.raises(HTTPException):
        read_call("runs", {"workspace_id": work[0][0].id, "outcome_id": work[0][1].id,
                           "task_id": work[1][2].id}, Context(db, users[0].id))


def test_read_validation(world):
    db, users, work = world
    ctx = Context(db, users[0].id)
    for name, args in [("confirm", {}), ("recent_events", {"cursor": "12"}), ("runs", {"limit": 51}),
                       ("overview", {"url": "https://example.com"}),
                       ("recent_events", {"since": "2020-01-01", "until": "2026-01-01"})]:
        with pytest.raises(HTTPException):
            read_call(name, {"workspace_id": work[0][0].id, **args}, ctx)


def test_running_ci_is_waiting_and_explicit_run_scope(world):
    db, users, work = world
    ws, o, t, _, _, run, _ = work[0]
    t.status, t.stage = "running", "ci"
    db.commit()
    context = Context(db, users[0].id)
    counts = read_call("overview", {"workspace_id": ws.id}, context)["items"][0]["counts"]
    assert counts["waiting"] == counts["waiting_ci"] == 1 and counts["running"] == 0
    with pytest.raises(HTTPException):
        read_call("runs", {"workspace_id": ws.id, "run_id": work[1][5].id}, context)


def test_objects_are_bounded_like_lists():
    state = [False]
    result = clean({f"key{i}": {"nested": list(range(3))} for i in range(5000)}, state)
    assert len(result) == MAX_ITEMS and state[0]
    small = [False]
    assert clean({"a": 1}, small) == {"a": 1} and not small[0]


def test_a_task_with_huge_evidence_reads_bounded_and_truncated(world):
    db, users, work = world
    ws, o, t, _, _, _, _ = work[0]
    t.evidence = {f"check{i}": "x" * 100 for i in range(5000)}
    db.commit()
    detail = read_call("outcome_detail", {"workspace_id": ws.id, "outcome_id": o.id}, Context(db, users[0].id))
    evidence = detail["items"][0]["tasks"][0]["evidence"]
    assert len(evidence) == MAX_ITEMS
    assert detail["truncated"]
