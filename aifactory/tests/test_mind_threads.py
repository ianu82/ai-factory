"""Thread integration and security/accounting regressions, with no paid services."""
import asyncio
import json
from datetime import datetime, timedelta

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from aifactory.auth import current_user
from aifactory.db import SessionLocal, init_engine
from aifactory.mind import api, store
from aifactory.models import (Event, Membership, Message, MindCharge, MindMessage, MindThread,
                             Organization, Secret, User, Workspace)
from aifactory.security import encrypt


class Runtime:
    def __init__(self):
        self.calls = 0
        self.history = []
        self.total = .25
        self.output = [{"type": "token", "data": {"text": "Hello "}},
                       {"type": "token", "data": {"text": "world"}}]

    async def readiness(self, workspace_id):
        return {"status": "ready", "model_id": "scripted"}

    async def session_cost(self, workspace_id, session_id):
        return self.total

    async def run_turn(self, context, history, emit, tools, ledger):
        self.history = history
        await ledger.before_call()
        self.calls += 1
        for event in self.output:
            await emit(event)
        await ledger.after_call(self.total)


@pytest.fixture
def world():
    with SessionLocal() as db:
        org = Organization(name="Org")
        alice, bob = User(name="Alice"), User(name="Bob")
        db.add_all([org, alice, bob]); db.flush()
        a = Workspace(org_id=org.id, name="A", slug="a")
        b = Workspace(org_id=org.id, name="B", slug="b")
        db.add_all([a, b]); db.flush()
        db.add_all([Membership(user_id=alice.id, workspace_id=a.id),
                    Membership(user_id=bob.id, workspace_id=a.id, role="admin")])
        db.commit()
        ids = alice.id, bob.id, a.id, b.id
    runtime = Runtime()
    app = FastAPI()
    app.include_router(api.router, prefix="/api")
    actor = [ids[0]]
    def user():
        with SessionLocal() as db:
            return db.get(User, actor[0])
    app.dependency_overrides[current_user] = user
    app.state.mind_runtime = runtime
    with TestClient(app) as client:
        yield client, runtime, actor, ids


def create(world, ws=None):
    client, _, _, ids = world
    response = client.post("/api/mind/threads", json={"billing_workspace_id": ws or ids[2]})
    assert response.status_code == 200, response.text
    return response.json()["id"]


def send(world, thread, key="one", text="What's happening?", **extra):
    return world[0].post(f"/api/mind/threads/{thread}/messages", json={
        "text": text, "client_message_id": key, **extra})


def events(response):
    assert response.status_code == 200, response.text
    return [json.loads(line[6:]) for line in response.text.splitlines() if line.startswith("data: ")]


def test_lifecycle_personal_scope_and_private_bus(world, monkeypatch):
    from aifactory.events import bus
    monkeypatch.setattr(bus, "publish", lambda *_: pytest.fail("Private chat published"))
    client, runtime, actor, ids = world
    thread = create(world)
    es = events(send(world, thread))
    assert [e["type"] for e in es] == ["message_started", "token", "token", "usage", "done"]
    assert [e["seq"] for e in es] == list(range(1, 6))
    assert es[-1]["data"] == {"status": "complete"}  # what web/src/mindHttp.ts accepts as a finished reply
    assert es[-2]["data"]["daily_spend"] == .25 and es[-2]["data"]["daily_limit"] == 10  # number | null on the wire
    view = client.get(f"/api/mind/threads/{thread}").json()
    assert view["messages"][1]["text"] == "Hello world"
    assert view["thread"]["title"] == "What's happening?"
    assert view["thread"]["cost"] == {"usd": .25, "known": True}
    assert client.get("/api/mind/threads").json()[0]["id"] == thread
    assert send(world, thread).status_code == 409
    assert runtime.calls == 1
    actor[0] = ids[1]
    assert client.get(f"/api/mind/threads/{thread}").status_code == 404
    assert send(world, thread, "bob").status_code == 404
    assert client.get("/api/mind/threads").json() == []
    actor[0] = ids[0]
    with SessionLocal() as db:
        db.query(Membership).filter_by(user_id=ids[0]).delete(); db.commit()
        assert db.query(Event).count() == db.query(Message).count() == 0
    assert client.get(f"/api/mind/threads/{thread}").status_code == 404
    assert send(world, thread, "revoked").status_code == 404
    assert client.get("/api/mind/threads").json() == []


def test_validation_and_unavailable(world):
    client, _, _, ids = world
    assert client.post("/api/mind/threads", json={"billing_workspace_id": ids[3]}).status_code == 404
    thread = create(world)
    assert send(world, thread, text="").status_code == 422
    assert send(world, thread, text="x" * 8001).status_code == 422
    assert send(world, thread, context={"workspace_id": ids[3], "kind": "workspace", "label": "B"}).status_code == 404
    assert client.get("/api/mind/status", params={"workspace_id": ids[2]}).json()["status"] == "ready"


@pytest.mark.parametrize("value", [float("inf"), float("nan"), -1, "10", True])
def test_invalid_limits(world, value):
    with SessionLocal() as db:
        ws = db.get(Workspace, world[3][2]); ws.settings = {"mind": {"daily_budget_usd": value}}
        # PostgreSQL JSON rejects NaN: validate the in-memory value without flushing it first.
        with db.no_autoflush, pytest.raises(store.AccountingError, match="finite"):
            store.effective_limit(db, world[3][0])


def test_budget_across_workspaces_and_process_recreation(world):
    client, runtime, _, ids = world
    first = create(world)
    with SessionLocal() as db:
        db.add(Membership(user_id=ids[0], workspace_id=ids[3]))
        db.get(Workspace, ids[3]).settings = {"mind": {"daily_budget_usd": .25}}
        db.commit()
    second = create(world, ids[3])
    events(send(world, first))
    init_engine()  # Durable accounting, no process-local lock/counter.
    es = events(send(world, second))
    assert next(e for e in es if e["type"] == "error")["data"]["code"] == "budget_exceeded"
    assert runtime.calls == 1
    with SessionLocal() as db:
        assert store.usage(db, ids[0])["daily_spend"]["usd"] == .25


async def test_midnight_continuation_and_exact_once(world, monkeypatch):
    ids, runtime = world[3], world[1]
    now = [datetime(2026, 11, 10, 23, 59)]
    monkeypatch.setattr(store, "utcnow", lambda: now[0])
    # SQLAlchemy model defaults use their imported clock; explicitly mirror
    # the admission timestamp here by changing the persisted first row.
    ledger = store.TurnLedger(ids[0], ids[2], runtime=runtime)
    await ledger.before_call()
    with SessionLocal() as db:
        db.query(MindCharge).update({"created_at": now[0]}); db.commit()
    await ledger.after_call(1)
    now[0] += timedelta(minutes=2)
    await ledger.before_call()
    with SessionLocal() as db:
        db.query(MindCharge).filter_by(settled=False).update({"created_at": now[0]}); db.commit()
    await ledger.after_call(20)
    await ledger.after_call(20)
    await ledger.reconcile()
    with SessionLocal() as db:
        assert store.usage(db, ids[0])["daily_spend"] == {"usd": 19, "known": True}
        assert sum(r.usd for r in db.query(MindCharge)) == 20
    with pytest.raises(store.AccountingError, match="exhausted"):
        await ledger.before_call()


async def test_unknown_failed_usage_blocks_then_reconciles_once(world):
    ids, runtime = world[3], world[1]
    runtime.total = None
    first = store.TurnLedger(ids[0], ids[2], runtime=runtime)
    await first.before_call()
    await first.after_call(None)
    second = store.TurnLedger(ids[0], ids[2], runtime=runtime)
    with pytest.raises(store.AccountingError, match="unresolved"):
        await second.before_call()
    runtime.total = .75
    await second.reconcile()
    await second.reconcile()
    with SessionLocal() as db:
        assert store.usage(db, ids[0])["daily_spend"] == {"usd": .75, "known": True}
    await second.before_call()
    await second.after_call(0)


async def test_concurrent_paid_claims(world):
    ids = world[3]
    first = store.TurnLedger(ids[0], ids[2])
    second = store.TurnLedger(ids[0], ids[2])
    await first.before_call()
    with pytest.raises(store.AccountingError, match="Another"):
        await second.before_call()
    await first.after_call(.1)
    await second.before_call()
    await second.after_call(.2)


def save_secret(ws, text):
    with SessionLocal() as db:
        db.add(Secret(workspace_id=ws, kind="github_token", ciphertext=encrypt(text)))
        db.commit()


def test_scrubber_original_spans_and_structured_boundaries():
    secret = "opaqueFirstHalfopaqueSecondHalf"
    value = [{"text": "opaqueFirstHalf", "context": {"label": "opaqueSecondHalf"}},
             {"text": "opaqueSecondHalf"}]
    clean = api.scrub(value, (secret,))
    assert clean[0]["text"] == clean[0]["context"]["label"] == clean[1]["text"] == "[redacted]"
    assert api.scrub({"first": "opaqueFirstHalf", "second": "opaqueSecondHalf", "other": "safe"}, (secret,)) == {
        "first": "[redacted]", "second": "[redacted]", "other": "safe"}


def test_new_secret_read_scrub_original_transcript_and_runtime_history(world):
    thread = create(world)
    with SessionLocal() as db:
        row = db.get(MindThread, thread); row.title = "opaqueFirstHalfopaqueSecondHalf"
        db.add_all([MindMessage(thread_id=thread, role="user", text="opaqueFirstHalf",
            context={"label": "opaqueSecondHalf"}, turn_id="old", position=1),
            MindMessage(thread_id=thread, role="assistant", text="opaqueSecondHalf", turn_id="old", position=2)])
        db.commit()
    save_secret(world[3][2], "opaqueFirstHalfopaqueSecondHalf")
    view = world[0].get(f"/api/mind/threads/{thread}").json()
    assert view["messages"][0]["text"] == view["messages"][0]["context"]["label"] == "[redacted]"
    assert view["messages"][1]["text"] == "[redacted]"
    assert world[0].get("/api/mind/threads").json()[0]["title"] == "[redacted]"
    events(send(world, thread))
    assert "opaqueFirstHalf" not in json.dumps(world[1].history)
    assert "opaqueSecondHalf" not in json.dumps(world[1].history)
    with SessionLocal() as db:
        assert db.query(MindMessage).filter_by(thread_id=thread, position=1).one().text == "opaqueFirstHalf"


def test_all_sse_event_kinds_share_scrub_buffer(world):
    thread = create(world)
    save_secret(world[3][2], "opaqueFirstHalfopaqueSecondHalf")
    world[1].output = [
        {"type": "token", "data": {"text": "opaqueFirstHalf"}},
        {"type": "action_card", "data": {"card": {"first": "opaqueSecondHalf"}}},
        {"type": "tool_finished", "data": {"call_id": "1", "name": "read", "summary": "opaqueFirstHalf",
            "citations": [{"label": "opaqueSecondHalf"}]}},
    ]
    # Exact concatenation of string values includes envelope metadata; place
    # credential across structured leaves in each event, and tokens across events.
    world[1].output[1]["data"]["card"] = {"first": "opaqueFirstHalf", "second": "opaqueSecondHalf"}
    world[1].output += [{"type": "token", "data": {"text": "opaqueSecondHalf"}}]
    response = send(world, thread)
    es = events(response)
    assert es[1]["data"]["text"] == "[redacted]"
    assert es[-3]["data"]["text"] == "[redacted]"
    assert es[2]["data"]["card"] == {"first": "[redacted]", "second": "[redacted]"}
    assert "opaqueFirstHalf" not in world[0].get(f"/api/mind/threads/{thread}").text


def test_new_workspace_secrets_cover_tool_results_and_sse(world):
    client, runtime, _, ids = world
    thread = create(world)
    with SessionLocal() as db:
        db.add(Membership(user_id=ids[0], workspace_id=ids[3]))
        from aifactory.models import Outcome
        db.add(Outcome(workspace_id=ids[3], number=1, title="opaque-cross-workspace-value"))
        db.commit()
    save_secret(ids[3], "opaque-cross-workspace-value")
    async def run_turn(context, history, emit, tools, ledger):
        result = await tools.call("list_outcomes", {"workspace_id": ids[3]})
        assert "opaque-cross-workspace-value" not in json.dumps(result)
        await emit({"type": "token", "text": "opaque-cross-workspace-value"})
    runtime.run_turn = run_turn
    response = send(world, thread)
    assert "opaque-cross-workspace-value" not in response.text
    assert "opaque-cross-workspace-value" not in client.get(f"/api/mind/threads/{thread}").text


def test_schema_initialization_existing_database(world):
    thread = create(world)
    init_engine(); init_engine()
    assert world[0].get(f"/api/mind/threads/{thread}").status_code == 200


def test_expired_turn_recovers_truthful_status(world):
    thread = create(world)
    with SessionLocal() as db:
        row = db.get(MindThread, thread)
        row.active_turn, row.lease_until = "dead", datetime(2000, 1, 1)
        db.add(MindMessage(thread_id=thread, role="assistant", text="partial", status="streaming", turn_id="dead", position=1))
        db.commit()
    result = world[0].get(f"/api/mind/threads/{thread}").json()
    assert result["thread"]["status"] == result["messages"][0]["status"] == "interrupted"
    assert result["messages"][0]["text"] == "partial"


async def test_simultaneous_submissions_claim_thread_and_disconnect_persists(world):
    thread = create(world)
    client, runtime, _, ids = world
    started = asyncio.Event()
    async def run_turn(context, history, emit, tools, ledger):
        await ledger.before_call()
        started.set()
        await emit({"type": "token", "text": "partial " * 100})
        await asyncio.Event().wait()
    runtime.run_turn = run_turn
    body = api.SubmitMessage(text="hello", client_message_id="once")
    with SessionLocal() as db:
        response = api.submit(thread, body, db.get(User, ids[0]), db, runtime)
    iterator = response.body_iterator
    first = await asyncio.wait_for(anext(iterator), 3)
    assert "message_started" in first
    await started.wait()
    with SessionLocal() as db:
        from fastapi import HTTPException
        for key in ("once", "different"):
            with pytest.raises(HTTPException) as exc:
                api.submit(thread, api.SubmitMessage(text="hello", client_message_id=key),
                           db.get(User, ids[0]), db, runtime)
            assert exc.value.status_code == 409
    await iterator.aclose()
    result = client.get(f"/api/mind/threads/{thread}").json()
    assert result["messages"][1]["text"] == "partial " * 100
    assert result["messages"][1]["status"] == "interrupted"
    assert result["thread"]["status"] == "interrupted"
    assert send(world, thread, "once").status_code == 409


def test_charged_failed_readiness_and_zero_budget(world):
    client, runtime, _, ids = world
    async def probe(workspace_id, ledger):
        await ledger.before_call()
        await ledger.after_call(.4)
        raise RuntimeError("opaque-provider-error-key")
    runtime.readiness = probe
    response = client.get("/api/mind/status", params={"workspace_id": ids[2]})
    assert response.status_code == 200
    assert response.json()["status"] == "unavailable"
    assert "opaque-provider-error-key" not in response.text
    with SessionLocal() as db:
        assert store.usage(db, ids[0])["daily_spend"]["usd"] == .4
        db.get(Workspace, ids[2]).settings = {"mind": {"daily_budget_usd": 0}}
        db.commit()
    response = client.get("/api/mind/status", params={"workspace_id": ids[2]})
    assert "exhausted" in response.json()["reason"]


def test_full_transcript_response_cross_message_and_field_egress(world):
    thread = create(world)
    with SessionLocal() as db:
        db.add_all([MindMessage(thread_id=thread, role="user", text="opaqueFirstHalf", turn_id="old", position=1),
                    MindMessage(thread_id=thread, role="assistant", text="opaqueSecondHalf", turn_id="old", position=2)])
        db.commit()
    save_secret(world[3][2], "opaqueFirstHalfopaqueSecondHalf")
    response = world[0].get(f"/api/mind/threads/{thread}")
    assert "opaqueFirstHalf" not in response.text and "opaqueSecondHalf" not in response.text


def test_structured_sse_cross_event_boundary(world):
    thread = create(world)
    save_secret(world[3][2], "opaqueFirstHalfopaqueSecondHalf")
    world[1].output = [
        {"type": "action_card", "data": {"card": {"field": "opaqueFirstHalf"}}},
        {"type": "action_card", "data": {"card": {"field": "opaqueSecondHalf"}}},
    ]
    response = send(world, thread)
    assert "opaqueFirstHalf" not in response.text and "opaqueSecondHalf" not in response.text


async def test_utc_midnight_resets_exhausted_budget(world, monkeypatch):
    ids = world[3]
    now = [datetime(2026, 11, 10, 23, 59)]
    monkeypatch.setattr(store, "utcnow", lambda: now[0])
    first = store.TurnLedger(ids[0], ids[2])
    await first.before_call()
    await first.after_call(10)
    with pytest.raises(store.AccountingError, match="exhausted"):
        await first.before_call()
    now[0] += timedelta(minutes=2)
    second = store.TurnLedger(ids[0], ids[2])
    await second.before_call()
    await second.after_call(.1)
    with SessionLocal() as db:
        assert store.usage(db, ids[0])["daily_spend"]["usd"] == .1


def test_unregistered_application_router():
    from aifactory.app import create_app
    app = create_app(start_orchestrator=False)
    assert not any(getattr(route, "path", "") == "/api/mind/threads" for route in app.routes)


@pytest.mark.parametrize("endpoint,payload", [
    ("messages", {"text": "x" * 8001 + "opaqueFirstHalfopaqueSecondHalf", "client_message_id": "bad"}),
    ("messages", {"text": "hello", "client_message_id": "bad", "first": "opaqueFirstHalf", "second": "opaqueSecondHalf"}),
    ("threads", {"billing_workspace_id": "opaqueFirstHalf", "extra": "opaqueSecondHalf"}),
    ("messages", {"text": "hello", "client_message_id": "bad", "context": {
        "workspace_id": "opaqueFirstHalf", "kind": "workspace", "label": "opaqueSecondHalf", "extra": True}}),
])
def test_validation_errors_never_echo_credentials(world, endpoint, payload):
    thread = create(world)
    save_secret(world[3][2], "opaqueFirstHalfopaqueSecondHalf")
    path = f"/api/mind/threads/{thread}/messages" if endpoint == "messages" else "/api/mind/threads"
    response = world[0].post(path, json=payload)
    assert response.status_code == 422
    assert "opaqueFirstHalf" not in response.text
    assert "opaqueSecondHalf" not in response.text
    assert response.json() == {"detail": "Invalid Mind request"}
    assert world[1].calls == 0


async def test_asgi_disconnect_shields_partial_persistence(world):
    thread = create(world)
    client, runtime, _, ids = world
    emitted, stopped = asyncio.Event(), asyncio.Event()
    partial = "partial " * 100

    async def run_turn(context, history, emit, tools, ledger):
        try:
            await ledger.before_call()
            await emit({"type": "token", "text": partial})
            emitted.set()
            await asyncio.Event().wait()
        finally:
            stopped.set()

    runtime.run_turn = run_turn
    with SessionLocal() as db:
        response = api.submit(thread, api.SubmitMessage(text="hello", client_message_id="disconnect"),
                              db.get(User, ids[0]), db, runtime)

    async def receive():
        await emitted.wait()
        return {"type": "http.disconnect"}

    sent = []

    async def send_asgi(message):
        sent.append(message)

    # Spec 2.0 runs Starlette's real disconnect listener/cancel scope, rather
    # than just aclosing the iterator outside the response lifecycle.
    await asyncio.wait_for(response({"type": "http", "asgi": {"spec_version": "2.0"}},
                                    receive, send_asgi), 3)
    assert stopped.is_set()
    view = client.get(f"/api/mind/threads/{thread}").json()
    assert view["messages"][1]["text"] == partial
    assert view["messages"][1]["status"] == view["thread"]["status"] == "interrupted"
    assert send(world, thread, "disconnect").status_code == 409
    with SessionLocal() as db:
        assert db.get(MindThread, thread).active_turn is None
        assert store.usage(db, ids[0])["daily_spend"]["known"] is False
        assert store.wire_usage(db, ids[0])["daily_spend"] is None  # unknown spend is null on the wire


async def test_late_cumulative_growth_after_midnight_counts_today_once(world, monkeypatch):
    ids = world[3]
    now = [datetime(2026, 11, 10, 23, 59)]
    monkeypatch.setattr(store, "utcnow", lambda: now[0])
    ledger = store.TurnLedger(ids[0], ids[2])
    await ledger.before_call()
    await ledger.after_call(1)
    now[0] += timedelta(minutes=2)
    # Cumulative usage grows with no new admission (late provider report).
    await ledger.after_call(20)
    await ledger.after_call(20)
    with SessionLocal() as db:
        assert store.usage(db, ids[0])["daily_spend"] == {"usd": 19, "known": True}
        assert store.cost(db.query(MindCharge).filter_by(session_id=ledger.session_id)) == {"usd": 20, "known": True}
    with pytest.raises(store.AccountingError, match="exhausted"):
        await store.TurnLedger(ids[0], ids[2]).before_call()


async def test_same_session_midnight_cumulative_usage_is_allocated_per_call(world, monkeypatch):
    ids = world[3]
    now = [datetime(2026, 11, 10, 23, 59)]
    monkeypatch.setattr(store, "utcnow", lambda: now[0])
    ledger = store.TurnLedger(ids[0], ids[2])
    await ledger.before_call()
    await ledger.after_call(1)
    now[0] += timedelta(minutes=2)
    await ledger.before_call()
    await ledger.after_call(20)
    await ledger.after_call(20)  # Repeated cumulative report must not double count.
    init_engine()
    with SessionLocal() as db:
        assert store.usage(db, ids[0])["daily_spend"] == {"usd": 19, "known": True}
        assert store.cost(db.query(MindCharge).filter_by(session_id=ledger.session_id)) == {"usd": 20, "known": True}
    with pytest.raises(store.AccountingError, match="exhausted"):
        await ledger.before_call()
