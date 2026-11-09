from __future__ import annotations

import asyncio

import pytest
from fastapi import HTTPException

from aifactory import serializers as S
from aifactory.db import SessionLocal
from aifactory.mind.actions import PROPOSALS, confirm, edit, execution_results, propose
from aifactory.mind.tools import Context, Tools
from aifactory.models import Decision, Event, Membership, Message, MindAction, User
from tests.test_mind_tools import world


def proposal(world, kind="send_guidance"):
    db, users, work = world
    ws, o, t, question, unblock, *_ = work[0]
    ctx = Context(db, users[0].id, "thread")
    args = {"workspace_id": ws.id, "outcome_id": o.id, "task_id": t.id, "text": "Please continue"}
    if kind != "send_guidance":
        args["decision_id"] = (question if kind == "answer_question" else unblock).id
    return ctx, propose("propose_" + kind, args, ctx)


@pytest.mark.parametrize("kind", ["send_guidance", "answer_question", "retry_task"])
def test_confirmation_gate_shared_service_attribution_and_result(world, kind):
    db, users, work = world
    ctx, card = proposal(world, kind)
    t, question, unblock = work[0][2:5]
    assert card["status"] == "pending" and t.status == "blocked"
    assert question.status == unblock.status == "open" and db.query(Message).count() == 0
    edited = edit(ctx, card["id"], 1, text="Updated exact text")
    assert edited["revision"] == 2 and edited["fields"] == {"text": "Updated exact text"}
    assert db.query(Message).count() == 0
    with pytest.raises(HTTPException):
        confirm(ctx, card["id"], 1)
    result = confirm(ctx, card["id"], 2)
    assert result["status"] == "succeeded" and result["result"]["ok"]
    assert execution_results(ctx) == [result["result"]]
    assert execution_results(Context(db, users[1].id, "thread")) == []
    with pytest.raises(HTTPException):
        confirm(ctx, card["id"], 2)
    for message in db.query(Message):
        assert message.author_id == users[0].id
        assert S.message(message, users[0].name)["author"] == "Alice via Mind"
    events = db.query(Event).filter(Event.actor == users[0].id).all()
    assert len(events) == 1 and "Alice via Mind" in events[0].message
    assert events[0].data["source"] == "mind" and events[0].data["action_id"] == card["id"]
    if kind != "send_guidance":
        decision = question if kind == "answer_question" else unblock
        assert decision.resolved_by == users[0].id
        assert S.decision(decision)["resolved_by_name"] == "Alice via Mind"
        assert t.status == "queued"
    assert db.get(User, users[0].id).name == "Alice"


def test_dismiss_typed_yes_and_unsupported_tools(world):
    ctx, card = proposal(world)
    tools = Tools(ctx)
    names = {d["function"]["name"] for d in tools.definitions()}
    assert len(PROPOSALS) == 3
    for forbidden in ("confirm", "yes", "approve_plan", "approve_merge", "approve_release", "cancel", "budget", "http", "sql"):
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
    ctx, card = proposal(world, "answer_question")
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


@pytest.mark.parametrize("kind", ["approve_plan", "approve_merge", "approve_release", "budget", "unblock"])
def test_answer_cannot_resolve_other_decision_kinds(world, kind):
    db, users, work = world
    ws, o, t, decision, *_ = work[0]
    decision.kind = kind
    decision.options = [{"value": "answer"}, {"value": "retry"}]
    db.commit()
    with pytest.raises(HTTPException):
        proposal(world, "answer_question")
    decision.kind = "budget"
    db.commit()
    with pytest.raises(HTTPException):
        propose("propose_retry_task", {"workspace_id": ws.id, "outcome_id": o.id, "task_id": t.id,
                                      "decision_id": decision.id}, Context(db, users[0].id, "thread"))


def test_cross_workspace_and_task_proposals(world):
    db, users, work = world
    ws, o, *_ = work[0]
    for workspace_id, outcome_id, task_id in [(ws.id, work[1][1].id, work[1][2].id),
                                             (ws.id, o.id, work[1][2].id),
                                             (work[2][0].id, work[2][1].id, work[2][2].id)]:
        with pytest.raises(HTTPException):
            propose("propose_send_guidance", {"workspace_id": workspace_id, "outcome_id": outcome_id,
                    "task_id": task_id, "text": "hello"}, Context(db, users[0].id, "thread"))


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


def test_http_adapters_and_app_guidance(client, signed_in, world, monkeypatch):
    from aifactory import events
    db, users, work = world
    # Authenticate the seeded owner with the normal current_user dependency.
    from aifactory.auth import current_user
    client.app.dependency_overrides[current_user] = lambda: users[0]
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


@pytest.mark.parametrize("kind", ["answer_question", "retry_task"])
def test_decision_action_commit_failure_rolls_back_everything(world, monkeypatch, kind):
    from aifactory import events
    db, users, work = world
    ctx, card = proposal(world, kind)
    published = []
    monkeypatch.setattr(events.bus, "publish", published.append)
    def fail(): raise RuntimeError("commit failed")
    monkeypatch.setattr(db, "commit", fail)
    with pytest.raises(RuntimeError): confirm(ctx, card["id"], 1)
    assert db.get(MindAction, card["id"]).status == "pending"
    assert work[0][2].status == "blocked"
    assert work[0][3].status == work[0][4].status == "open"
    assert db.query(Message).count() == 0 and execution_results(ctx) == []
    assert not published


def test_object_http_views_keep_real_actor_with_mind_label(client, world):
    from aifactory.auth import current_user
    db, users, work = world
    client.app.dependency_overrides[current_user] = lambda: users[0]
    ctx, card = proposal(world, "answer_question")
    confirm(ctx, card["id"], 1)
    ws, o, *_ = work[0]
    view = client.get(f"/api/workspaces/{ws.id}/outcomes/{o.id}").json()
    answer = next(d for d in view["decisions"] if d["id"] == work[0][3].id)
    assert answer["resolved_by"] == users[0].id and answer["resolved_by_name"] == "Alice via Mind"
    assert view["messages"][0]["author"] == "Alice via Mind"
    event = next(e for e in view["events"] if e["kind"] == "decision_resolved")
    assert event["actor"] == users[0].id and "Alice via Mind" in event["message"]
