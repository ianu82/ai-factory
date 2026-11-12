from __future__ import annotations

import asyncio
import re

import pytest
from fastapi import HTTPException

from aifactory import serializers as S
from aifactory.db import SessionLocal
from aifactory.mind.actions import KINDS, PROPOSALS, confirm, edit, execution_results, planner_task_schema, propose
from aifactory.mind.tools import Context, Tools
from aifactory.models import (Decision, DecisionReview, Event, Membership, Message, MindAction, MindThread, Outcome,
                              Plan, Repository, Task, User, Workspace)
from tests.test_mind_tools import world  # noqa: F401  (fixture)


def plan_task(**values):
    """A task in the planner's terms, with whatever else the planner's schema requires of it."""
    extra = {"constraints": ["Reject unauthenticated requests"], "files": [f"src/{values['key'].lower()}.py"]}
    return {"depends_on": [], **values, **{k: v for k, v in extra.items() if k in planner_task_schema()["properties"]}}


def scenario(world, name):
    """(tool, arguments, change the target, check what confirming did) for each kind of card."""
    db, users, work = world
    ws, o, t, question, unblock, run, plan = work[0]
    repo = db.query(Repository).filter_by(workspace_id=ws.id).one()
    base = {"workspace_id": ws.id}
    if name == "guidance":
        def done():
            assert db.query(Message).one().body == "Please continue"
        return "send_guidance", {**base, "outcome_id": o.id, "task_id": t.id, "text": "Please continue"}, \
            lambda: setattr(t, "title", "Renamed"), done
    if name in ("answer", "retry"):
        decision = question if name == "answer" else unblock
        args = {**base, "decision_id": decision.id, "option": name, **({"text": "Use the cache"} if name == "answer" else {})}
        def done():
            assert decision.status == "resolved" and decision.resolution == name and t.status == "queued"
        return "resolve_decision", args, lambda: setattr(decision, "body", "A different question"), done
    if name in ("merge", "request_changes"):
        t.status, t.pr_number, t.head_sha = "awaiting_approval", 309, "a" * 40
        decision = Decision(workspace_id=ws.id, outcome_id=o.id, task_id=t.id, kind="approve_merge", title="Merge?",
                            options=[{"value": "approve", "label": "Merge"}, {"value": "reject", "label": "Request changes"}])
        db.add(decision); db.commit()
        option = "approve" if name == "merge" else "Request changes"  # a label is matched too
        args = {**base, "decision_id": decision.id, "option": option, **({"text": "Split the module"} if name != "merge" else {})}
        def done():
            assert decision.resolution == ("approve" if name == "merge" else "reject") and t.status == "queued"
            assert t.stage == ("merge" if name == "merge" else "revise")
            if name != "merge":
                assert db.query(Message).one().body == "Split the module"
        return "resolve_decision", args, lambda: setattr(t, "head_sha", "b" * 40), done
    if name == "plan":
        plan.status = "superseded"
        proposed = Plan(outcome_id=o.id, version=2, content={"tasks": [plan_task(
            key="T9", title="Planned", repo=repo.full_name, spec="Do it", acceptance=["It works"])]})
        o.status = "awaiting_approval"
        decision = Decision(workspace_id=ws.id, outcome_id=o.id, kind="approve_plan", title="Approve plan?",
                            options=[{"value": "approve", "label": "Approve"}, {"value": "replan", "label": "Revise"},
                                     {"value": "cancel", "label": "Cancel"}])
        db.add_all([proposed, decision]); db.commit()
        def done():
            db.expire_all()
            assert db.get(Plan, proposed.id).status == "approved" and db.get(Plan, proposed.id).approved_by == users[0].id
            assert db.get(Outcome, o.id).status == "running"
            assert db.query(Task).filter_by(outcome_id=o.id, key="T9").one().status == "pending"
        return "resolve_decision", {**base, "decision_id": decision.id, "option": "approve"}, \
            lambda: setattr(decision, "title", "Another plan"), done
    if name == "fix":
        decision = Decision(workspace_id=ws.id, outcome_id=o.id, task_id=t.id, kind="unblock", title="Smoke failed",
                            body="The health check returned 500.",
                            options=[{"value": "fix", "label": "Create a fix outcome"},
                                     {"value": "acknowledge", "label": "Acknowledge"}])
        db.add(decision); db.commit()
        def done():
            fix = db.query(Outcome).filter_by(workspace_id=ws.id, number=2).one()
            assert fix.title == "Fix: Smoke failed" and fix.source == "improvement"  # still an improvement
            assert fix.created_by == users[0].id and decision.resolution == "fix"
        return "resolve_decision", {**base, "decision_id": decision.id, "option": "fix"}, \
            lambda: setattr(decision, "body", "Another failure"), done
    if name == "create":
        def done():
            new = db.query(Outcome).filter_by(workspace_id=ws.id, number=2).one()
            assert new.title == "Add search" and new.brief == "Users can search outcomes."
            assert new.source == "mind" and new.created_by == users[0].id and new.repo_ids == [repo.id]
            assert new.priority == 1 and new.status == "planning"
        return "create_outcome", {**base, "title": "Add search", "text": "Users can search outcomes.", "priority": 1}, \
            lambda: setattr(repo, "full_name", "org/renamed"), done
    if name == "build":
        tasks = [plan_task(key="T1", title="API", repo=repo.full_name.upper(), spec="Add the endpoint",
                           acceptance=["GET /search returns 200"]),
                 plan_task(key="T2", title="Page", repo=repo.full_name, spec="Add the page",
                           acceptance=["The page lists results"], depends_on=["T1"])]
        def done():
            new = db.query(Outcome).filter_by(workspace_id=ws.id, number=2).one()
            assert new.status == "running" and new.source == "mind" and new.brief == "Search, then a page."
            adopted = db.query(Plan).filter_by(outcome_id=new.id).one()
            assert adopted.status == "approved" and adopted.approved_by == users[0].id
            assert [x["key"] for x in adopted.content["tasks"]] == ["T1", "T2"]
            built = {x.key: x for x in db.query(Task).filter_by(outcome_id=new.id)}
            assert set(built) == {"T1", "T2"} and built["T2"].depends_on == ["T1"]
            assert all(x.status == "pending" and x.repo_id == repo.id and x.plan_id == adopted.id for x in built.values())
            assert built["T1"].spec == "Add the endpoint" and built["T1"].acceptance == ["GET /search returns 200"]
        return "build_plan", {**base, "title": "Search", "text": "Search, then a page.", "tasks": tasks}, \
            lambda: setattr(repo, "full_name", "org/renamed"), done
    if name == "cancel_outcome":
        def done():
            assert o.status == "cancelled" and t.status == "cancelled"
            assert question.status == unblock.status == "superseded"
        return "cancel_outcome", {**base, "outcome_id": o.id}, lambda: setattr(o, "title", "Renamed"), done
    if name == "cancel_task":
        def done():
            assert t.status == "cancelled" and o.status == "planning"
        return "cancel_task", {**base, "outcome_id": o.id, "task_id": t.id}, lambda: setattr(t, "title", "Renamed"), done
    assert name == "take_over"
    def done():
        assert t.status == "handed_over" and question.status == unblock.status == "superseded"
    return "take_over_task", {**base, "outcome_id": o.id, "task_id": t.id}, lambda: setattr(t, "title", "Renamed"), done


SCENARIOS = ["guidance", "answer", "retry", "merge", "request_changes", "plan", "fix", "create", "build",
             "cancel_outcome", "cancel_task", "take_over"]


def shared(db):
    """Everything a confirmation may change in shared work."""
    db.expire_all()
    return (sorted((o.id, o.status) for o in db.query(Outcome)), sorted((t.id, t.status, t.stage) for t in db.query(Task)),
            sorted((d.id, d.status) for d in db.query(Decision)), sorted((p.id, p.status) for p in db.query(Plan)),
            db.query(Message).count(), db.query(Event).count())


def proposal(world, name="guidance"):
    db, users, _ = world
    tool, args, _, _ = scenario(world, name)
    ctx = Context(db, users[0].id, "thread")
    return ctx, propose("propose_" + tool, args, ctx)


@pytest.mark.parametrize("name", SCENARIOS)
def test_each_kind_proposes_then_executes_once_only_for_its_member(world, name):
    db, users, work = world
    tool, args, _, done = scenario(world, name)
    ctx = Context(db, users[0].id, "thread")
    before = shared(db)
    card = propose("propose_" + tool, args, ctx)
    assert card["status"] == "pending" and card["kind"] == tool and card["effect"]
    assert set(card) >= {"id", "revision", "kind", "workspace", "object", "fields", "effect", "status"}
    assert set(card["object"]) == {"kind", "id", "label", "href"} and card["object"]["href"].startswith(f"/w/{work[0][0].id}/")
    assert all(isinstance(v, str) for v in card["fields"].values())
    assert shared(db) == before  # a proposal never executes
    with pytest.raises(HTTPException) as exc:  # nobody else can confirm it
        confirm(Context(db, users[1].id, "thread"), card["id"], 1)
    assert exc.value.status_code == 404
    member = db.query(Membership).filter_by(user_id=users[0].id, workspace_id=work[0][0].id).one()
    member.role = "viewer"
    db.commit()
    with pytest.raises(HTTPException) as exc:  # nor can its owner without the member role
        confirm(ctx, card["id"], 1)
    assert exc.value.status_code == 403 and shared(db) == before
    member.role = "member"
    db.commit()
    result = confirm(ctx, card["id"], 1)
    assert result["status"] == "succeeded" and result["result"]["ok"]
    db.expire_all()
    done()
    with pytest.raises(HTTPException) as exc:  # exactly once
        confirm(ctx, card["id"], 1)
    assert exc.value.status_code == 409
    assert execution_results(ctx) == [db.get(MindAction, card["id"]).result]
    attributed = db.query(Event).filter(Event.actor == users[0].id).all()
    assert attributed and all("Alice via Mind" in e.message for e in attributed)
    assert all(e.data.get("source") == "mind" and e.data.get("action_id") == card["id"] for e in attributed)
    for message in db.query(Message):
        assert S.message(message, users[0].name)["author"] == "Alice via Mind"


@pytest.mark.parametrize("name", SCENARIOS)
def test_each_kind_refuses_a_changed_target(world, name):
    db, users, _ = world
    tool, args, change, _ = scenario(world, name)
    ctx = Context(db, users[0].id, "thread")
    card = propose("propose_" + tool, args, ctx)
    change()
    db.commit()
    before = shared(db)
    with pytest.raises(HTTPException) as exc:
        confirm(ctx, card["id"], 1)
    assert exc.value.status_code in (404, 409)
    assert shared(db) == before and db.get(MindAction, card["id"]).status == "pending"


@pytest.mark.parametrize("name", SCENARIOS)
def test_a_viewer_cannot_propose(world, name):
    db, users, work = world
    tool, args, _, _ = scenario(world, name)
    db.query(Membership).filter_by(user_id=users[0].id, workspace_id=work[0][0].id).one().role = "viewer"
    db.commit()
    with pytest.raises(HTTPException) as exc:
        propose("propose_" + tool, args, Context(db, users[0].id, "thread"))
    assert exc.value.status_code == 403 and db.query(MindAction).count() == 0


def test_card_effects_say_what_will_happen(world):
    db, users, work = world
    ctx = Context(db, users[0].id, "thread")
    effects = {}
    for name in ("merge", "request_changes", "retry", "cancel_task", "build"):
        tool, args, _, _ = scenario(world, name)
        effects[name] = propose("propose_" + tool, args, ctx)
    assert effects["merge"]["effect"].startswith("Approve the merge of PR #309 (#1 T1)")
    assert effects["merge"]["object"]["kind"] == "decision" and effects["merge"]["fields"]["option"] == "Merge"
    assert effects["request_changes"]["fields"] == {"option": "Request changes", "text": "Split the module"}
    assert effects["retry"]["effect"].startswith("Retry #1 T1") and effects["retry"]["fields"] == {"option": "retry", "text": ""}
    assert "closes PR #309" in effects["cancel_task"]["effect"]
    build = effects["build"]
    assert re.match(r"Create a new outcome “Search” in Workspace 0 with this 2-task plan \(T1, T2\) and start building", build["effect"])
    assert build["fields"]["plan"].splitlines() == ["T1 API — org/repo0", "T2 Page — org/repo0 (after T1)"]
    assert build["fields"]["text"] == "Search, then a page." and build["object"]["kind"] == "workspace"
    done = confirm(ctx, build["id"], 1)
    assert done["effect"].startswith("Create #2 “Search”") and done["object"]["kind"] == "outcome"
    assert done["object"]["href"] == f"/w/{work[0][0].id}/outcomes/{done['result']['outcome_id']}"


def test_only_the_text_is_editable_and_options_needing_text_keep_it(world):
    db, users, _ = world
    ctx, card = proposal(world, "request_changes")
    with pytest.raises(HTTPException):
        edit(ctx, card["id"], 1, text="  ")  # a change request needs its comment
    edited = edit(ctx, card["id"], 1, text="Rename the helper")
    assert edited["revision"] == 2 and edited["fields"] == {"option": "Request changes", "text": "Rename the helper"}
    _, cancel = proposal(world, "cancel_outcome")
    with pytest.raises(HTTPException) as exc:
        edit(ctx, cancel["id"], 1, text="anything")
    assert exc.value.status_code == 422
    _, created = proposal(world, "create")
    edited = edit(ctx, created["id"], 1, text="A sharper brief")
    assert edited["fields"]["title"] == "Add search" and edited["fields"]["text"] == "A sharper brief"
    confirm(ctx, created["id"], 2)
    assert db.query(Outcome).filter_by(number=2).one().brief == "A sharper brief"


@pytest.mark.parametrize("tasks,reason", [
    ([{"key": "T1", "depends_on": ["T9"]}], "depends on unknown tasks"),
    ([{"key": "T1", "depends_on": ["T2"]}, {"key": "T2", "depends_on": ["T1"]}], "cycle"),
    ([{"key": "T1"}, {"key": "T1"}], "Duplicate task key"),
    ([{"key": "T1", "acceptance": []}], "missing acceptance"),
    ([{"key": "T1", "repo": "someone/else"}], "not a connected repository"),
])
def test_an_invalid_plan_is_refused_with_the_planners_reason(world, tasks, reason):
    db, users, work = world
    ws = work[0][0]
    full = [plan_task(**{"title": "Work", "repo": "org/repo0", "spec": "Do it", "acceptance": ["It works"], **t})
            for t in tasks]
    with pytest.raises(HTTPException) as exc:
        propose("propose_build_plan", {"workspace_id": ws.id, "title": "Search", "text": "Summary", "tasks": full},
                Context(db, users[0].id, "thread"))
    assert exc.value.status_code == 422 and reason in exc.value.detail
    assert db.query(MindAction).count() == 0 and db.query(Outcome).filter_by(workspace_id=ws.id).count() == 1


def test_a_plan_needing_approval_is_approved_by_the_persons_confirmation(world):
    db, users, work = world
    db.get(Workspace, work[0][0].id).settings = {"autonomy": {"plan": "approval"}}
    db.commit()
    ctx, card = proposal(world, "build")
    confirm(ctx, card["id"], 1)
    new = db.query(Outcome).filter_by(workspace_id=work[0][0].id, number=2).one()
    assert new.status == "running" and db.query(Task).filter_by(outcome_id=new.id).count() == 2
    approved = db.query(Event).filter_by(kind="plan_approved", outcome_id=new.id).one()
    assert approved.message == "Plan approved by Alice via Mind" and approved.actor == users[0].id
    assert db.query(Decision).filter_by(outcome_id=new.id).count() == 0  # nothing left waiting for a person


def test_resolution_options_and_judged_decisions(world):
    db, users, work = world
    ws, o, t, question, *_ = work[0]
    ctx = Context(db, users[0].id, "thread")
    with pytest.raises(HTTPException) as exc:
        propose("propose_resolve_decision", {"workspace_id": ws.id, "decision_id": question.id, "option": "approve"}, ctx)
    assert exc.value.status_code == 422 and "answer" in exc.value.detail
    with pytest.raises(HTTPException):  # an answer needs its text
        propose("propose_resolve_decision", {"workspace_id": ws.id, "decision_id": question.id, "option": "answer"}, ctx)
    db.add(DecisionReview(workspace_id=ws.id, decision_id=question.id, state="running"))
    db.commit()
    db.expire_all()  # as a new request would see it
    with pytest.raises(HTTPException) as exc:  # the app offers no options while a judge reviews it
        propose("propose_resolve_decision", {"workspace_id": ws.id, "decision_id": question.id, "option": "answer",
                                             "text": "Yes"}, ctx)
    assert exc.value.status_code == 409


def test_a_cancel_card_names_the_tasks_that_wait_and_drops_them_only_when_asked(world, monkeypatch):
    from aifactory.pipeline import orchestrator as orchestration
    monkeypatch.setattr(orchestration.orchestrator, "cancel_task", lambda task_id: None)
    db, users, work = world
    ws, o, t, _, _, _, plan = work[0]
    waiting = Task(workspace_id=ws.id, outcome_id=o.id, plan_id=plan.id, repo_id=t.repo_id, key="T2", title="Page",
                   status="pending", depends_on=["T1"])
    db.add(waiting); db.commit()
    ctx = Context(db, users[0].id, "thread")
    base = {"workspace_id": ws.id, "outcome_id": o.id, "task_id": t.id}
    kept = propose("propose_cancel_task", base, ctx)
    assert kept["effect"].endswith(" The tasks that wait for it are kept, and will ask what to do: T2 Page.")
    dropped = propose("propose_cancel_task", {**base, "dependents": "drop"}, ctx)
    assert dropped["effect"].endswith(" The tasks that wait for it are dropped too: T2 Page.")
    with pytest.raises(HTTPException) as exc:
        propose("propose_cancel_task", {**base, "dependents": "all"}, ctx)
    assert exc.value.status_code == 422
    confirm(ctx, dropped["id"], 1)
    db.expire_all()
    assert db.get(Task, t.id).status == "cancelled" and db.get(Task, waiting.id).status == "cancelled"
    [event] = db.query(Event).filter_by(kind="task_cancelled", task_id=waiting.id).all()
    assert event.message == "Alice via Mind dropped T2: it waits for T1, which was cancelled"
    assert event.data["source"] == "mind" and event.data["with"] == "T1"
    with pytest.raises(HTTPException) as exc:  # the kept card's target changed: T1 is no longer open
        confirm(ctx, kept["id"], 1)
    assert exc.value.status_code == 409


def test_take_over_and_cancel_finish_after_the_commit(world, monkeypatch):
    from aifactory.pipeline import orchestrator as orchestration
    stopped = []
    monkeypatch.setattr(orchestration.orchestrator, "cancel_task", stopped.append)
    db, users, work = world
    ctx, card = proposal(world, "take_over")
    result = confirm(ctx, card["id"], 1)
    t = work[0][2]
    assert stopped == [t.id] and result["result"]["handover"] == {"published": False}  # no candidate to push
    assert db.query(Event).filter_by(kind="handover_no_candidate", task_id=t.id).count() == 1
    assert execution_results(ctx)[-1]["handover"] == {"published": False}


def test_a_follow_up_that_did_not_happen_marks_the_result_partial(world, monkeypatch):
    from aifactory.mind import actions
    from aifactory.pipeline import orchestrator as orchestration
    monkeypatch.setattr(orchestration.orchestrator, "cancel_task", lambda task_id: None)
    async def finish(later, user, action_id):
        return {"handover": {"published": False, "error": "push refused"}, "follow_ups_failed": ["stop_task"]}
    monkeypatch.setattr(actions, "finish", finish)
    ctx, card = proposal(world, "take_over")
    result = confirm(ctx, card["id"], 1)["result"]
    assert result["ok"] is True and result["partial"] is True
    assert result["incomplete"] == ["stopping the task's job", "opening the hand-over's draft pull request (push refused)"]


def test_incomplete_counts_pull_requests_github_refused_to_close():
    from aifactory.mind.actions import incomplete
    later = [("stop_task", "t"), ("close_pull", "a", None), ("close_pull", "b", None)]
    assert incomplete(later, {"pull_requests_closed": 2}) == []
    assert incomplete(later, {"pull_requests_closed": 1}) == ["closing 1 pull request (GitHub refused)"]
    assert incomplete(later, {"follow_ups_failed": ["close_pull"], "pull_requests_closed": 1}) == ["closing a pull request"]
    assert incomplete([("publish_candidate", "t", {})], {"handover": {"published": False}}) == []  # nothing to push


def test_dismiss_typed_yes_and_unsupported_tools(world):
    ctx, card = proposal(world)
    tools = Tools(ctx)
    names = {d["function"]["name"] for d in tools.definitions()}
    assert set(PROPOSALS) == {"propose_" + kind for kind in KINDS} and len(PROPOSALS) == 7
    for forbidden in ("confirm", "yes", "approve_plan", "approve_merge", "approve_release", "cancel", "budget", "http", "sql",
                      "delete_workspace", "set_credential"):
        assert forbidden not in names
        with pytest.raises(HTTPException):
            asyncio.run(tools.call(forbidden, {}))
        with pytest.raises(HTTPException):
            propose("propose_" + forbidden, {}, ctx)
    assert edit(ctx, card["id"], 1, dismiss=True)["status"] == "dismissed"
    with pytest.raises(HTTPException):
        confirm(ctx, card["id"], 2)
    assert ctx.db.query(Message).count() == 0


@pytest.mark.parametrize("change", ["viewer", "revoked", "owner", "body", "resolved", "task", "outcome"])
def test_stale_and_authorization(world, change):
    db, users, work = world
    ctx, card = proposal(world, "answer")
    ws, o, t, decision, *_ = work[0]
    if change in ("viewer", "revoked"):
        m = db.query(Membership).filter_by(user_id=users[0].id, workspace_id=ws.id).one()
        if change == "viewer": m.role = "viewer"
        else: db.delete(m)
    elif change == "owner": ctx = Context(db, users[1].id, "thread")
    elif change == "body": decision.body = "A completely different question"
    elif change == "resolved": decision.status = "resolved"
    elif change == "task":
        t.key = "moved"
        t.outcome_id = work[1][1].id
    elif change == "outcome": o.status = "cancelled"
    db.commit()
    with pytest.raises(HTTPException):
        confirm(ctx, card["id"], 1)
    assert db.query(Message).count() == 0
    assert db.query(Event).filter_by(actor=users[0].id).count() == 0


def test_legacy_cards_still_render_and_ask_for_a_new_proposal(world):
    db, users, work = world
    ws, o, t, question, *_ = work[0]
    legacy = MindAction(owner_id=users[0].id, thread_id="thread", kind="answer_question", workspace_id=ws.id,
                        outcome_id=o.id, task_id=t.id, decision_id=question.id, fields={"text": "Old"},
                        snapshot={"outcome_title": o.title, "task_title": t.title, "decision": {"title": "Question"}})
    db.add(legacy); db.commit()
    ctx = Context(db, users[0].id, "thread")
    from aifactory.mind.actions import card
    view = card(legacy, ctx)
    assert view["kind"] == "answer_question" and "Question" in view["effect"] and view["object"]["kind"] == "decision"
    with pytest.raises(HTTPException) as exc:
        confirm(ctx, legacy.id, 1)
    assert exc.value.status_code == 409 and question.status == "open"
    assert edit(ctx, legacy.id, 1, dismiss=True)["status"] == "dismissed"


def test_proposals_take_outcome_numbers_and_task_keys(world):
    db, users, work = world
    ws, o, t, *_ = work[0]
    ctx = Context(db, users[0].id, "thread")
    guidance = propose("propose_send_guidance", {"workspace_id": ws.id, "outcome_id": "#1", "text": "Keep going"}, ctx)
    assert guidance["object"]["id"] == o.id and guidance["object"]["label"].startswith("#1 ")
    cancel = propose("propose_cancel_task", {"workspace_id": ws.id, "outcome_id": "1", "task_id": "t1"}, ctx)
    take = propose("propose_take_over_task", {"workspace_id": ws.id, "outcome_id": o.id, "task_id": "#1 T1"}, ctx)
    for card in (cancel, take):
        action = db.get(MindAction, card["id"])
        assert (action.outcome_id, action.task_id) == (o.id, t.id)
    # No #7 here; and a workspace the person is not in, though it has a #1.
    for workspace_id, number in ((ws.id, "#7"), (work[2][0].id, "#1")):
        with pytest.raises(HTTPException) as exc:
            propose("propose_send_guidance", {"workspace_id": workspace_id, "outcome_id": number, "text": "x"}, ctx)
        assert exc.value.status_code == 404
    assert db.query(MindAction).count() == 3


def test_cross_workspace_and_task_proposals(world):
    db, users, work = world
    ws, o, *_ = work[0]
    for workspace_id, outcome_id, task_id in [(ws.id, work[1][1].id, work[1][2].id),
                                             (ws.id, o.id, work[1][2].id),
                                             (work[2][0].id, work[2][1].id, work[2][2].id)]:
        for tool, args in [("send_guidance", {"text": "hello"}), ("cancel_task", {}), ("take_over_task", {})]:
            with pytest.raises(HTTPException):
                propose("propose_" + tool, {"workspace_id": workspace_id, "outcome_id": outcome_id, "task_id": task_id,
                                            **args}, Context(db, users[0].id, "thread"))
    with pytest.raises(HTTPException):  # a decision of another workspace
        propose("propose_resolve_decision", {"workspace_id": ws.id, "decision_id": work[1][3].id, "option": "answer",
                                             "text": "x"}, Context(db, users[0].id, "thread"))
    assert db.query(MindAction).count() == 0


def test_guidance_publication_after_commit_and_rollback(world, monkeypatch):
    from aifactory import events
    from aifactory.work_services import publish_changes, send_guidance
    db, users, work = world
    ctx, card = proposal(world)
    published = []
    def capture(payload):
        if payload["type"] == "event":
            with SessionLocal() as reader:
                assert reader.get(Event, payload["id"]) is not None
            published.append(payload)
    monkeypatch.setattr(events.bus, "publish", capture)
    confirm(ctx, card["id"], 1)
    assert len(published) == 1
    _, payload = send_guidance(db, users[0], work[0][0].id, work[0][1].id, "App guidance")
    assert len(published) == 1
    db.commit(); publish_changes([payload])
    assert len(published) == 2 and "via Mind" not in published[-1]["message"]
    ctx, card = proposal(world)
    def fail(): raise RuntimeError("commit failed")
    monkeypatch.setattr(db, "commit", fail)
    with pytest.raises(RuntimeError): confirm(ctx, card["id"], 1)
    assert len(published) == 2 and db.get(MindAction, card["id"]).status == "pending"
    assert db.query(Message).count() == 2


@pytest.mark.parametrize("name", ["answer", "retry", "plan", "build", "cancel_outcome", "take_over"])
def test_a_failed_commit_rolls_back_everything(world, monkeypatch, name):
    from aifactory import events
    db, users, work = world
    ctx, card = proposal(world, name)
    before = shared(db)
    published = []
    monkeypatch.setattr(events.bus, "publish", published.append)
    def fail(): raise RuntimeError("commit failed")
    monkeypatch.setattr(db, "commit", fail)
    with pytest.raises(RuntimeError): confirm(ctx, card["id"], 1)
    monkeypatch.undo()
    assert db.get(MindAction, card["id"]).status == "pending"
    assert shared(db) == before and execution_results(ctx) == [] and not published


def test_http_adapters_and_app_guidance(client, signed_in, world, monkeypatch):
    from aifactory import events
    db, users, work = world
    # Authenticate the seeded owner with the normal current_user dependency.
    from aifactory.auth import current_user
    client.app.dependency_overrides[current_user] = lambda: users[0]
    # The HTTP routes serve only a thread the person owns, scoped to workspaces they are still a member of.
    db.add(MindThread(id="thread", owner_id=users[0].id, billing_workspace_id=work[0][0].id,
                      workspace_ids=sorted([work[0][0].id, work[1][0].id])))
    db.commit()
    ctx, card = proposal(world)
    path = f'/api/mind/threads/thread/actions/{card["id"]}'
    assert client.patch(path, json={"expected_revision": 1, "kind": "retry_task", "text": "bad"}).status_code == 422
    assert client.post(path + "/confirm", json={"expected_revision": 1, "text": "yes"}).status_code == 422
    r = client.patch(path, json={"expected_revision": 1, "text": "edited"})
    assert r.status_code == 200 and r.json()["revision"] == 2
    assert client.post(path + "/confirm", json={"expected_revision": 1}).status_code == 409
    assert client.post(path + "/confirm", json={"expected_revision": 2}).json()["status"] == "succeeded"
    assert client.post(path + "/confirm", json={"expected_revision": 2}).status_code == 409
    ws, o, *_ = work[0]
    path = f"/api/workspaces/{ws.id}/outcomes/{o.id}/messages"
    assert client.post(path, json={"body": "forged", "task_id": work[1][2].id}).status_code == 404
    published = []
    def capture(payload):
        if payload["type"] == "event":
            with SessionLocal() as reader: assert reader.get(Event, payload["id"]) is not None
            published.append(payload)
    monkeypatch.setattr(events.bus, "publish", capture)
    r = client.post(path, json={"body": "app"})
    assert r.status_code == 200 and r.json()["author"] == "Alice" and len(published) == 1
    def fail(self): raise RuntimeError("commit failed")
    monkeypatch.setattr("sqlalchemy.orm.Session.commit", fail)
    with pytest.raises(RuntimeError): client.post(path, json={"body": "not committed"})
    assert len(published) == 1


def test_http_confirmation_runs_follow_ups_on_the_event_loop(client, world, monkeypatch):
    from aifactory.auth import current_user
    from aifactory.pipeline import orchestrator as orchestration
    db, users, work = world
    client.app.dependency_overrides[current_user] = lambda: users[0]
    db.add(MindThread(id="thread", owner_id=users[0].id, billing_workspace_id=work[0][0].id,
                      workspace_ids=sorted([work[0][0].id, work[1][0].id])))
    db.commit()
    stops = []

    def stop(outcome_id):
        try:
            asyncio.get_running_loop()
            stops.append((outcome_id, "event loop"))
        except RuntimeError:  # a worker thread has no running loop
            stops.append((outcome_id, "worker thread"))

    monkeypatch.setattr(orchestration.orchestrator, "cancel_outcome", stop)
    _, card = proposal(world, "cancel_outcome")
    response = client.post(f'/api/mind/threads/thread/actions/{card["id"]}/confirm', json={"expected_revision": 1})
    assert response.status_code == 200 and response.json()["status"] == "succeeded"
    # The app's loop (where planners and jobs are tasks) stopped the work, not the request's worker thread.
    assert stops == [(work[0][1].id, "event loop")]
    db.expire_all()
    assert db.get(Outcome, work[0][1].id).status == "cancelled"


def test_app_endpoints_share_the_services(client, signed_in):
    ws = signed_in["workspaces"][0]["id"]
    with SessionLocal() as db:
        repo = Repository(workspace_id=ws, full_name="acme/app")
        db.add(repo); db.commit()
        repo_id = repo.id
    created = client.post(f"/api/workspaces/{ws}/outcomes", json={"title": "App outcome", "brief": "b", "repo_ids": [repo_id]})
    assert created.status_code == 200 and created.json()["source"] == "user"
    outcome_id = created.json()["id"]
    assert client.get(f"/api/workspaces/{ws}/outcomes/{outcome_id}").json()["outcome"]["created_by_name"] == "Ian"
    assert client.post(f"/api/workspaces/{ws}/outcomes/{outcome_id}/cancel").json() == {"ok": True}
    with SessionLocal() as db:
        assert db.get(Outcome, outcome_id).status == "cancelled"
        event = db.query(Event).filter_by(kind="outcome_cancelled", outcome_id=outcome_id).one()
        assert re.fullmatch(r"Ian cancelled #\d+", event.message) and event.data == {}


def test_concurrent_confirmation_claims_once(world):
    from concurrent.futures import ThreadPoolExecutor
    from threading import Barrier
    db, users, _ = world
    ctx, card = proposal(world)
    owner = users[0].id
    db.rollback()
    barrier = Barrier(2)
    def click():
        with SessionLocal() as session:
            barrier.wait(timeout=10)
            try:
                return confirm(Context(session, owner, "thread"), card["id"], 1)["status"]
            except HTTPException as exc:
                return exc.status_code
    with ThreadPoolExecutor(max_workers=2) as executor:
        results = list(executor.map(lambda _: click(), range(2)))
    assert sorted(results, key=str) == [409, "succeeded"]
    assert db.query(Message).count() == 1
    assert len(execution_results(ctx)) == 1


def test_card_keeps_full_exact_edit_text(world):
    ctx, card = proposal(world)
    text = "Full guidance. " * 400
    edited = edit(ctx, card["id"], 1, text=text)
    assert edited["fields"]["text"] == text.strip()
    confirm(ctx, card["id"], 2)
    assert ctx.db.query(Message).one().body == edited["fields"]["text"]


def test_object_http_views_keep_real_actor_with_mind_label(client, world):
    from aifactory.auth import current_user
    db, users, work = world
    client.app.dependency_overrides[current_user] = lambda: users[0]
    ctx, card = proposal(world, "answer")
    confirm(ctx, card["id"], 1)
    _, created = proposal(world, "create")
    confirm(ctx, created["id"], 1)
    ws, o, *_ = work[0]
    view = client.get(f"/api/workspaces/{ws.id}/outcomes/{o.id}").json()
    answer = next(d for d in view["decisions"] if d["id"] == work[0][3].id)
    assert answer["resolved_by"] == users[0].id and answer["resolved_by_name"] == "Alice via Mind"
    assert view["messages"][0]["author"] == "Alice via Mind"
    event = next(e for e in view["events"] if e["kind"] == "decision_resolved")
    assert event["actor"] == users[0].id and "Alice via Mind" in event["message"]
    new = db.query(Outcome).filter_by(workspace_id=ws.id, number=2).one()
    view = client.get(f"/api/workspaces/{ws.id}/outcomes/{new.id}").json()
    assert view["outcome"]["created_by_name"] == "Alice via Mind" and view["outcome"]["created_by"] == users[0].id
    _, fix = proposal(world, "fix")  # a fix outcome keeps its improvement source and its Mind attribution
    confirm(ctx, fix["id"], 1)
    fixed = db.query(Outcome).filter_by(workspace_id=ws.id, number=3).one()
    view = client.get(f"/api/workspaces/{ws.id}/outcomes/{fixed.id}").json()
    assert view["outcome"]["source"] == "improvement" and view["outcome"]["created_by_name"] == "Alice via Mind"


@pytest.mark.parametrize("text", ["Please continue", ""])
def test_confirmed_retry_records_one_timeline_message(world, text):
    db, users, work = world
    ws, o, t, _, unblock, *_ = work[0]
    ctx = Context(db, users[0].id, "thread")
    args = {"workspace_id": ws.id, "decision_id": unblock.id, "option": "retry"}
    card = propose("propose_resolve_decision", {**args, "text": text} if text else args, ctx)
    assert confirm(ctx, card["id"], 1)["status"] == "succeeded"
    assert [m.body for m in db.query(Message)] == [text or "Retried task."]


def test_legacy_mind_actions_table_allows_workspace_cards(tmp_path):
    """An existing SQLite database (production's) keeps its cards when outcome_id becomes nullable."""
    import json

    from sqlalchemy import create_engine, inspect, text
    from sqlalchemy.schema import CreateIndex, CreateTable

    from aifactory import db as database
    url = f"sqlite:///{tmp_path / 'legacy.db'}"
    engine = create_engine(url)
    ddl = str(CreateTable(MindAction.__table__).compile(engine))
    ddl, count = re.subn(r"(outcome_id VARCHAR\(32\))", r"\1 NOT NULL", ddl, count=1)
    ddl, dropped = re.subn(r"\n\s*digest VARCHAR\(64\),", "", ddl, count=1)  # production's table predates it
    assert count == dropped == 1
    with engine.begin() as conn:
        conn.execute(text(ddl))
        for index in MindAction.__table__.indexes:
            conn.execute(CreateIndex(index))
        conn.execute(text("INSERT INTO mind_actions (id, thread_id, owner_id, workspace_id, outcome_id, kind, revision, "
                          "status, fields, snapshot, created_at) VALUES ('old', 't', 'u', 'w', 'o', 'send_guidance', 1, "
                          "'pending', :fields, :snapshot, '2026-11-10 00:00:00')"),
                     {"fields": json.dumps({"text": "kept"}), "snapshot": json.dumps({})})
    engine.dispose()
    database.init_engine(url)
    database.init_engine(url)  # idempotent
    inspector = inspect(database.engine())
    columns = {c["name"]: c for c in inspector.get_columns("mind_actions")}
    assert columns["outcome_id"]["nullable"] and columns["digest"]["nullable"]
    assert {i["name"] for i in inspector.get_indexes("mind_actions")} >= {i.name for i in MindAction.__table__.indexes}
    assert "mind_actions" in {k["referred_table"] for k in inspector.get_foreign_keys("mind_action_results")}
    with database.engine().connect() as conn:
        assert conn.exec_driver_sql("PRAGMA foreign_keys").scalar() == 1
        assert conn.exec_driver_sql("SELECT name FROM sqlite_master WHERE name = 'mind_actions_rebuild'").first() is None
    with database.session_scope() as s:
        assert s.get(MindAction, "old").fields == {"text": "kept"}


def test_mind_action_outcome_becomes_nullable_on_postgres():
    from sqlalchemy import inspect, text

    from aifactory import db as database
    if database.engine().dialect.name != "postgresql":
        pytest.skip("SQLite is covered by the legacy-file upgrade")
    with database.engine().begin() as conn:
        conn.execute(text("ALTER TABLE mind_actions ALTER COLUMN outcome_id SET NOT NULL"))
    database.init_engine()
    column = next(c for c in inspect(database.engine()).get_columns("mind_actions") if c["name"] == "outcome_id")
    assert column["nullable"]
