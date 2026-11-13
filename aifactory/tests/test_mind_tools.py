from __future__ import annotations

import json
from datetime import datetime, timedelta

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
            ws = Workspace(org_id=org.id, name=f"Workspace {i}", slug=str(i), outcome_seq=1)  # outcome #1 below
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
            db.add_all([question, unblock, Decision(workspace_id=ws.id, kind="supervisor", title="Finding", body=SECRET)])
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
    if name == "pull_requests":  # the fixture's tasks have not opened pull requests yet
        for _, _, t, *_ in work:
            t.pr_number, t.pr_url = 12, "https://github.com/org/repo/pull/12"
        db.commit()
    def scope(ws, o, t):  # the object a detail read needs, in that workspace
        return {"outcome_id": o.id} if name == "outcome_detail" else (
            {"outcome_id": o.id, "task_id": t.id} if name == "task_detail" else {})
    for i, (ws, o, t, *_) in enumerate(work):
        args = {"workspace_id": ws.id, **scope(ws, o, t)}
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
        read_call(name, {"workspace_id": work[0][0].id, **scope(*work[0][:3])}, context)


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


@pytest.mark.parametrize("name", ["runs", "list_tasks", "open_decisions", "outcome_detail", "pull_requests"])
def test_forged_nested_ids(world, name):
    db, users, work = world
    with pytest.raises(HTTPException):
        read_call(name, {"workspace_id": work[0][0].id, "outcome_id": work[1][1].id}, Context(db, users[0].id))
    with pytest.raises(HTTPException):
        read_call("runs", {"workspace_id": work[0][0].id, "outcome_id": work[0][1].id,
                           "task_id": work[1][2].id}, Context(db, users[0].id))


@pytest.mark.parametrize("kind", ["stuck_task", "push_protection", "unrelayed_review"])
def test_supervisor_findings_resolve_task_only_links_without_leaking_other_workspaces(world, kind):
    db, users, work = world
    ws, outcome, task, *_ = work[0]
    finding = Decision(workspace_id=ws.id, task_id=task.id, kind="supervisor", title="Task finding",
                       metadata_json={"finding_kind": kind})
    invalid = [
        Decision(workspace_id=ws.id, task_id=work[1][2].id, kind="supervisor", title="Other workspace"),
        Decision(workspace_id=ws.id, task_id="missing-task", kind="supervisor", title="Missing task"),
        Decision(workspace_id=ws.id, outcome_id=work[1][1].id, task_id=task.id,
                 kind="supervisor", title="Mismatched outcome"),
    ]
    db.add_all([finding, *invalid])
    db.commit()

    result = read_call("supervisor_findings", {"workspace_id": ws.id}, Context(db, users[0].id))
    item = next(d for d in result["items"] if d["id"] == finding.id)
    assert item["task_id"] == task.id and item["outcome_id"] == outcome.id
    cite = next(c for c in result["citations"] if c["id"] == finding.id)
    assert cite["href"] == f"/w/{ws.id}/outcomes/{outcome.id}?task={task.id}"
    assert not {d.id for d in invalid} & {d["id"] for d in result["items"]}
    assert not {d.id for d in invalid} & {c["id"] for c in result["citations"]}
    assert work[1][2].id not in json.dumps(result)
    # Reading resolves links without rewriting the durable decision.
    db.refresh(finding)
    assert finding.outcome_id is None


@pytest.mark.parametrize("valid_count", [0, 1, 2, 3, 5])
@pytest.mark.parametrize("same_timestamp", [False, True])
def test_supervisor_findings_fill_page_past_invalid_links(world, valid_count, same_timestamp):
    db, users, work = world
    ws, outcome, task, *_ = work[0]
    db.query(Decision).filter_by(workspace_id=ws.id, kind="supervisor").update({"status": "resolved"})
    now = datetime(2026, 11, 12)
    rows, valid = [], []

    def add(**links):
        row = Decision(id=f"finding-{len(rows):03d}", workspace_id=ws.id, kind="supervisor",
                       title="Finding", created_at=now if same_timestamp else now + timedelta(seconds=len(rows)),
                       **links)
        rows.append(row)
        return row

    # More than one batch of stale/foreign links precedes and follows the valid findings.
    for _ in range(4):
        add(task_id="missing-task")
        add(task_id=work[1][2].id)
    for _ in range(valid_count):
        valid.append(add(task_id=task.id))  # task-only findings still get the correct outcome citation
        add(outcome_id=work[1][1].id, task_id=task.id)
    for _ in range(4):
        add(outcome_id="missing-outcome")
    db.add_all(rows)
    db.commit()

    result = read_call("supervisor_findings", {"workspace_id": ws.id, "limit": 2}, Context(db, users[0].id))
    assert [d["id"] for d in result["items"]] == [d.id for d in valid[:2]]
    assert [c["id"] for c in result["citations"]] == [d.id for d in valid[:2]]
    assert all(c["href"] == f"/w/{ws.id}/outcomes/{outcome.id}?task={task.id}" for c in result["citations"])
    assert result["truncated"] is (valid_count > 2)
    assert work[1][2].id not in json.dumps(result)


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


def test_lists_carry_brief_tasks_and_outcome_detail_the_full_ones(world):
    db, users, work = world
    ws, o, t, _, _, _, _ = work[0]
    t.evidence = {f"check{i}": "x" * 1500 for i in range(40)}
    t.spec = "s" * 1900
    db.commit()
    ctx = Context(db, users[0].id)
    for name in ("overview", "list_tasks"):
        result = read_call(name, {"workspace_id": ws.id}, ctx)
        task = (result["items"][0]["work"] if name == "overview" else result["items"])[0]
        assert task["key"] == t.key and "evidence" not in task and "spec" not in task
        assert len(json.dumps(result)) < 10_000
    detail = read_call("outcome_detail", {"workspace_id": ws.id, "outcome_id": o.id}, ctx)
    # Over the budget, so shortened: every field of the task survives, only its texts are cut.
    task = detail["items"][0]["tasks"][0]
    assert detail["truncated"] and detail["narrow"]
    assert set(task) >= {"evidence", "spec", "acceptance", "status"} and len(task["evidence"]) == 40
    assert all(len(v) <= 401 for v in task["evidence"].values())


def test_an_oversized_result_is_shortened_to_the_budget_and_says_so(world):
    from aifactory.mind.tools import RESULT_BUDGET
    db, users, work = world
    ws, o, t, _, _, _, _ = work[0]
    for i in range(30):
        db.add(Decision(workspace_id=ws.id, outcome_id=o.id, task_id=t.id, kind="question", title=f"Q{i}",
                        body="b" * 1900, options=[], status="open"))
    db.commit()
    result = read_call("open_decisions", {"workspace_id": ws.id}, Context(db, users[0].id))
    assert len(json.dumps(result, ensure_ascii=False)) <= RESULT_BUDGET
    assert result["truncated"] and "narrow" in result


def test_nested_objects_are_narrowed_last_and_the_budget_always_holds(world):
    from aifactory.mind.tools import RESULT_BUDGET
    db, users, work = world
    ws, o, t, _, _, _, _ = work[0]
    # About 1 MB, nested three levels: over the budget after every shortening step, so the hard fallback applies.
    t.evidence = {f"k{i}": {f"n{j}": {f"m{k}": "z" * 40 for k in range(10)} for j in range(50)} for i in range(50)}
    db.commit()
    ctx = Context(db, users[0].id)
    for name, args in (("task_detail", {"outcome_id": o.id, "task_id": t.id}), ("outcome_detail", {"outcome_id": o.id})):
        result = read_call(name, {"workspace_id": ws.id, **args}, ctx)
        assert len(json.dumps(result, ensure_ascii=False)) <= RESULT_BUDGET
        assert result["truncated"] and "narrow" in result


def test_brief_tasks_redact_before_they_cut(world):
    from aifactory.mind.tools import BRIEF_TEXT
    db, users, work = world
    ws, o, t, _, _, _, _ = work[0]
    t.blocked_reason = "x" * (BRIEF_TEXT - 10) + SECRET  # the credential crosses the cut
    db.commit()
    result = read_call("overview", {"workspace_id": ws.id}, Context(db, users[0].id))
    assert "sk-" not in json.dumps(result)


def test_a_shortened_event_page_continues_after_the_last_event_shown(world):
    db, users, work = world
    ws, o, t, _, _, run, _ = work[0]
    for j in range(20):
        db.add(Event(workspace_id=ws.id, outcome_id=o.id, task_id=t.id, kind="note", message=f"{j} " + "y" * 1900,
                     data={"pad": ["z" * 1900] * 3}))
    db.commit()
    ctx = Context(db, users[0].id)
    first = read_call("recent_events", {"workspace_id": ws.id}, ctx)
    shown = [e["id"] for e in first["items"]]
    assert first["truncated"] and "narrow" in first and len(shown) < 20
    assert first["next_cursor"] == shown[-1]
    second = read_call("recent_events", {"workspace_id": ws.id, "cursor": first["next_cursor"]}, ctx)
    following = db.query(Event.id).filter(Event.workspace_id == ws.id, Event.id < shown[-1]).order_by(Event.id.desc()).first()[0]
    assert second["items"][0]["id"] == following  # nothing skipped between the pages


def test_task_detail_reads_any_task_in_full_and_only_in_its_outcome(world):
    db, users, work = world
    ws, o, t, _, _, _, _ = work[0]
    other = Outcome(workspace_id=ws.id, number=2, title="Other")
    db.add(other)
    db.commit()
    ctx = Context(db, users[0].id)
    task = read_call("task_detail", {"workspace_id": ws.id, "outcome_id": o.id, "task_id": t.id}, ctx)["items"][0]
    assert task["key"] == t.key and "evidence" in task and "spec" in task
    with pytest.raises(HTTPException) as exc:  # a task is read only through its own outcome
        read_call("task_detail", {"workspace_id": ws.id, "outcome_id": other.id, "task_id": t.id}, ctx)
    assert exc.value.status_code == 404


def test_pull_requests_report_recorded_ci_decisions_and_the_page_snapshot(world, monkeypatch):
    import time

    from aifactory import pulls
    db, users, work = world
    ws, o, t, question, unblock, *_ = work[0]
    t.pr_number, t.pr_url, t.head_sha, t.status = 309, "https://github.com/org/repo0/pull/309", "c" * 40, "awaiting_approval"
    t.evidence = {"ci": {"state": "failure", "head": "c" * 40, "at": "2026-11-10T10:00:00",
                         "checks": [{"name": "tests", "status": "completed", "conclusion": "failure"},
                                    {"name": "lint", "status": "completed", "conclusion": "success"},
                                    {"name": "postgres", "status": "in_progress"}]}, "smoke": {"state": "passed"}}
    db.commit()
    context = Context(db, users[0].id)
    result = read_call("pull_requests", {"workspace_id": ws.id}, context)
    item = result["items"][0]
    assert (item["repository"], item["number"], item["merged"]) == ("org/repo0", 309, False)
    assert item["ci"]["state"] == "failure" and item["ci"]["failing"] == ["tests"] and item["ci"]["pending"] == ["postgres"]
    assert item["smoke"] == "passed" and item["task"]["key"] == "T1" and item["outcome"]["number"] == 1
    assert item["decision"]["id"] in (question.id, unblock.id) and item["decision"]["options"]
    assert "github" not in item and "github_fetched_at" not in result
    assert read_call("pull_requests", {"workspace_id": ws.id, "state": "merged"}, context)["items"] == []
    with pytest.raises(HTTPException):
        read_call("pull_requests", {"workspace_id": ws.id, "state": "closed"}, context)
    # The app's Pull requests page snapshot (never a GitHub call from Mind) adds review threads when it is recent.
    snapshot = {"fetched_at": time.time(), "items": [{"repo": "org/repo0", "number": 309, "group": "attention",
                "reason": "2 unresolved review comments from codex", "detail": {"open_threads": 2,
                "open_thread_authors": ["codex"], "mergeable_state": "clean"}, "ci": {"state": "failure"}}]}
    monkeypatch.setitem(pulls._cache, (ws.id, "open"), (time.monotonic(), snapshot))
    item = read_call("pull_requests", {"workspace_id": ws.id}, context)["items"][0]
    assert item["github"]["open_threads"] == 2 and item["github"]["open_thread_authors"] == ["codex"]
    monkeypatch.setitem(pulls._cache, (ws.id, "open"), (time.monotonic(), {**snapshot, "fetched_at": time.time() - 3600}))
    assert "github" not in read_call("pull_requests", {"workspace_id": ws.id}, context)["items"][0]
    t.status = "handed_over"  # its pull request stays open for the person who took it over
    db.commit()
    assert read_call("pull_requests", {"workspace_id": ws.id}, context)["items"][0]["task"]["status"] == "handed_over"
    t.status, t.evidence = "cancelled", {**t.evidence, "pr_closed": True}
    db.commit()
    assert read_call("pull_requests", {"workspace_id": ws.id}, context)["items"] == []
    assert read_call("pull_requests", {"workspace_id": ws.id, "state": "all"}, context)["items"][0]["closed_by_factory"]
    t.merge_sha, t.status, t.evidence = "d" * 40, "completed", {"ci": {"state": "success"}}
    db.commit()
    assert read_call("pull_requests", {"workspace_id": ws.id}, context)["items"] == []
    assert read_call("pull_requests", {"workspace_id": ws.id, "state": "merged"}, context)["items"][0]["merged"]


def test_reads_name_what_proposals_need(world):
    db, users, work = world
    ws, o, t, question, *_ = work[0]
    t.pr_number = 5
    db.commit()
    context = Context(db, users[0].id)
    repos = read_call("repositories", {"workspace_id": ws.id}, context)
    assert [r["full_name"] for r in repos["items"]] == ["org/repo0"]
    assert repos["citations"][0]["href"] == f"/w/{ws.id}/settings/repositories"
    decisions = read_call("open_decisions", {"workspace_id": ws.id}, context)["items"]
    asked = next(d for d in decisions if d["id"] == question.id)
    assert asked["options"] == [{"value": "answer"}] and asked["task_key"] == "T1" and asked["pr_number"] == 5
    assert asked["outcome_number"] == 1 and asked["actionable"] is True
    from aifactory.models import Message
    db.add(Message(workspace_id=ws.id, outcome_id=o.id, author_id=users[1].id, body="Prefer the cache"))
    db.commit()
    detail = read_call("outcome_detail", {"workspace_id": ws.id, "outcome_id": o.id}, context)["items"][0]
    assert {d["id"] for d in detail["decisions"]} == {question.id, work[0][4].id}
    assert detail["messages"][0]["body"] == "Prefer the cache" and detail["messages"][0]["author"] == "Bob"


def test_reads_take_outcome_numbers_and_task_keys(world):
    # People name work as the app shows it (#53, T1); Mind passes that on instead of looking the ids up first.
    db, users, work = world
    ws, o, t, *_, run, _ = work[0]
    context = Context(db, users[0].id)
    for ref in ("#1", "1", " # 1 ", o.id):  # workspace 1 has an outcome #1 too: only this workspace's counts
        detail = read_call("outcome_detail", {"workspace_id": ws.id, "outcome_id": ref}, context)
        assert detail["citations"][0]["id"] == o.id
    for outcome_ref, task_ref in (("#1", "T1"), ("1", "t1"), (o.id, "T1"), ("#1", t.id), ("#1", "#1 T1")):
        task = read_call("task_detail", {"workspace_id": ws.id, "outcome_id": outcome_ref, "task_id": task_ref}, context)
        assert task["items"][0]["id"] == t.id
    runs = read_call("runs", {"workspace_id": ws.id, "task_id": "#1 T1"}, context)  # the label names the outcome
    assert [r["id"] for r in runs["items"]] == [run.id]
    for workspace_id, outcome_ref, task_ref, status in [
            (ws.id, "#2", "T1", 404),  # no outcome #2 here
            (ws.id, work[1][1].id, "T1", 404),  # another workspace's outcome
            (ws.id, "#1", "T9", 404),  # no such key in #1
            (ws.id, "#1", "#2 T1", 404),  # a label for another outcome
            (work[2][0].id, "#1", "T1", 404)]:  # a workspace the person is not in
        with pytest.raises(HTTPException) as exc:
            read_call("task_detail", {"workspace_id": workspace_id, "outcome_id": outcome_ref, "task_id": task_ref},
                      context)
        assert exc.value.status_code == status
    db.add(Task(workspace_id=ws.id, outcome_id=o.id, plan_id=t.plan_id, repo_id=t.repo_id, key="t1", title="Again"))
    db.commit()
    with pytest.raises(HTTPException) as exc:  # two tasks with that key: never a guess
        read_call("task_detail", {"workspace_id": ws.id, "outcome_id": "#1", "task_id": "T1"}, context)
    assert exc.value.status_code == 422 and "list_tasks" in exc.value.detail
    task = read_call("task_detail", {"workspace_id": ws.id, "outcome_id": "#1", "task_id": t.id}, context)
    assert task["items"][0]["id"] == t.id


def test_nested_tool_arguments_are_bounded_and_closed():
    from aifactory.mind.tools import validate_arguments
    schema = {"type": "object", "properties": {"workspace_id": {"type": "string"}, "tasks": {"type": "array", "maxItems": 2,
              "minItems": 1, "items": {"type": "object", "properties": {"key": {"type": "string", "maxLength": 3},
              "acceptance": {"type": "array", "items": {"type": "string"}}}, "required": ["key"],
              "additionalProperties": False}}}, "required": ["workspace_id"], "additionalProperties": False}
    validate_arguments(schema, {"workspace_id": "w", "tasks": [{"key": "T1", "acceptance": ["ok"]}]})
    for bad, where in [({"workspace_id": "w", "tasks": []}, "tasks"),
                       ({"workspace_id": "w", "tasks": [{"key": "T1"}] * 3}, "tasks"),
                       ({"workspace_id": "w", "tasks": [{"key": "TOOLONG"}]}, "tasks[0].key"),
                       ({"workspace_id": "w", "tasks": [{"acceptance": []}]}, "missing key"),
                       ({"workspace_id": "w", "tasks": [{"key": "T1", "sneaky": SECRET}]}, "unknown argument"),
                       ({"workspace_id": "w", "tasks": [{"key": "T1", "acceptance": [" "]}]}, "tasks[0].acceptance[0]"),
                       ({"workspace_id": "w", "tasks": {"key": "T1"}}, "tasks"),
                       ("not an object", "Invalid tool arguments")]:
        with pytest.raises(HTTPException) as exc:
            validate_arguments(schema, bad)
        assert exc.value.status_code == 422 and where in exc.value.detail and SECRET not in exc.value.detail
