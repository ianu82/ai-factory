"""Mind end to end through the real app: routers, the runtime adapter, the restricted engine, the provider and
durable accounting, against a local fake MindsHub. Nothing here calls a paid service."""
import asyncio
import json

import pytest
from fastapi.testclient import TestClient

from aifactory.app import create_app
from aifactory.auth import current_user
from aifactory.db import SessionLocal
from aifactory.mind import provider as mp
from aifactory.mind import runtime as mr
from aifactory.mind.contracts import TurnContext
from aifactory.mind.store import AccountingError
from aifactory.models import (Decision, Event, Membership, MindAction, MindCharge, MindThread, Organization, Outcome,
                             Plan, Repository, Secret, Task, User, Workspace)
from aifactory.security import encrypt
from tests.test_mind_provider import KEY, MODEL, fake_provider


@pytest.fixture
def people():
    """Alice is a member of two workspaces; Bob is only a viewer of the first."""
    with SessionLocal() as db:
        org = Organization(name="Org")
        alice, bob = User(name="Alice", email="alice@t"), User(name="Bob", email="bob@t")
        db.add_all([org, alice, bob]); db.flush()
        first = Workspace(org_id=org.id, name="Platform", slug="platform")
        second = Workspace(org_id=org.id, name="Mobile", slug="mobile")
        db.add_all([first, second]); db.flush()
        db.add_all([Membership(user_id=alice.id, workspace_id=first.id, role="member"),
                    Membership(user_id=alice.id, workspace_id=second.id, role="member"),
                    Membership(user_id=bob.id, workspace_id=first.id, role="viewer")])
        # The workspace's saved MindsHub key: Mind redacts it everywhere, as it would in production.
        db.add(Secret(workspace_id=first.id, kind="mindshub_api_key", ciphertext=encrypt(KEY)))
        repo = Repository(workspace_id=first.id, full_name="acme/platform")
        outcome = Outcome(workspace_id=first.id, number=7, title="Ship the thing", status="running")
        db.add_all([repo, outcome]); db.flush()
        plan = Plan(outcome_id=outcome.id, content={})
        db.add(plan); db.flush()
        task = Task(workspace_id=first.id, outcome_id=outcome.id, plan_id=plan.id, repo_id=repo.id, key="T1",
                    title="Build it", status="blocked", spec="s")
        db.add(task); db.flush()
        unblock = Decision(workspace_id=first.id, outcome_id=outcome.id, task_id=task.id, kind="unblock",
                           title="Retry?", options=[{"value": "retry"}])
        db.add(unblock)
        db.commit()
        yield {"alice": alice.id, "bob": bob.id, "first": first.id, "second": second.id,
               "outcome": outcome.id, "task": task.id, "unblock": unblock.id}


@pytest.fixture
def app_as(people):
    """The production app (create_app), signed in as whoever `actor[0]` names."""
    app = create_app(start_orchestrator=False)
    actor = [people["alice"]]

    def user():
        with SessionLocal() as db:
            return db.get(User, actor[0])

    app.dependency_overrides[current_user] = user
    with TestClient(app) as client:
        yield client, app, actor


def stream(response):
    assert response.status_code == 200, response.text
    return [json.loads(line[6:]) for line in response.text.splitlines() if line.startswith("data: ")]


def send(client, thread, text, context=None, key="m1"):
    return stream(client.post(f"/api/mind/threads/{thread}/messages",
                              json={"text": text, "client_message_id": key, "context": context}))


def test_real_engine_answers_through_the_app(app_as, people, monkeypatch):
    pytest.importorskip("anton.core.session")
    client, app, _ = app_as
    assert isinstance(app.state.mind_runtime, mr.EngineRuntime)
    with fake_provider() as (credential, requests, headers):
        monkeypatch.setattr(mp, "saved_credential", lambda workspace_id: credential)
        thread = client.post("/api/mind/threads", json={"billing_workspace_id": people["first"]}).json()
        assert sorted(thread["workspace_ids"]) == sorted([people["first"], people["second"]])
        status = client.get("/api/mind/status", params={"workspace_id": people["first"]}).json()
        assert status == {"status": "ready", "model_id": MODEL, "reason": None}
        probes = len(requests)
        context = {"workspace_id": people["first"], "kind": "inbox", "id": people["first"], "label": "Inbox"}
        events = send(client, thread["id"], "What needs me today? " + KEY, context)

    types = [e["type"] for e in events]
    assert types[0] == "message_started" and types[-2:] == ["usage", "done"]
    assert events[-1]["data"] == {"status": "complete"}
    assert "tool_started" in types and "tool_finished" in types
    assert "".join(e["data"]["text"] for e in events if e["type"] == "token") == "Ready now."
    assert all(KEY not in json.dumps(e) for e in events)
    # The turn reused the cached readiness probe and sent two requests (tool call, then the answer).
    turn = requests[probes:]
    assert len(turn) == 2
    prompt = json.dumps(turn[0])
    assert "Workspaces this person can ask about" in prompt and people["second"] in prompt
    assert "The person is looking at" in prompt and KEY not in prompt
    sessions = {h.get("langfuse-session-id") for h in headers if h.get("langfuse-session-id")}
    view = client.get(f"/api/mind/threads/{thread['id']}").json()
    assert view["messages"][1]["text"] == "Ready now." and view["messages"][1]["status"] == "complete"
    assert view["thread"]["cost"]["known"] is True
    assert KEY not in json.dumps(view)
    with SessionLocal() as db:
        charges = db.query(MindCharge).filter_by(user_id=people["alice"]).all()
        assert charges and all(c.settled for c in charges)  # every paid send admitted and reconciled
        assert {c.session_id for c in charges} <= sessions  # charges name the MindsHub sessions actually used
        assert db.query(Event).filter(Event.message.contains("Ready now")).count() == 0  # never on activity


class ProposingRuntime:
    """A Mind-specific scripted runtime: proposes a retry card through the real private tool dispatcher."""

    def __init__(self, ids):
        self.ids, self.histories = ids, []

    async def readiness(self, workspace_id, *, ledger):
        return {"status": "ready", "model_id": "scripted"}

    async def session_cost(self, workspace_id, session_id):
        return 0.01

    async def run_turn(self, context, history, emit, tools, ledger):
        self.histories.append(history)
        await ledger.before_call()
        if len(self.histories) > 1:
            await emit({"type": "text_delta", "text": "T1 is queued again."})
            await ledger.after_call(0.02)
            return
        await tools.call("propose_retry_task", {"workspace_id": self.ids["first"], "outcome_id": self.ids["outcome"],
                                                "task_id": self.ids["task"], "decision_id": self.ids["unblock"]})
        await emit({"type": "text_delta", "text": "I can retry T1 once you confirm."})
        await ledger.after_call(0.01)


def test_a_proposed_action_runs_once_only_after_confirmation(app_as, people):
    client, app, actor = app_as
    app.state.mind_runtime = runtime = ProposingRuntime(people)
    thread = client.post("/api/mind/threads", json={"billing_workspace_id": people["first"]}).json()["id"]
    events = send(client, thread, "Can T1 run again?")
    card = next(e["data"]["card"] for e in events if e["type"] == "action_card")
    assert card["status"] == "pending" and card["kind"] == "retry_task"
    with SessionLocal() as db:
        assert db.get(Task, people["task"]).status == "blocked"  # a proposal never executes
    base = f"/api/mind/threads/{thread}/actions/{card['id']}"

    actor[0] = people["bob"]  # another person (a viewer here) cannot see or confirm Alice's card
    assert client.post(f"{base}/confirm", json={"expected_revision": 1}).status_code == 404
    assert client.get(f"/api/mind/threads/{thread}").status_code == 404
    actor[0] = people["alice"]

    confirmed = client.post(f"{base}/confirm", json={"expected_revision": 1})
    assert confirmed.status_code == 200, confirmed.text
    assert confirmed.json()["status"] == "succeeded"
    assert client.post(f"{base}/confirm", json={"expected_revision": 1}).status_code == 409  # exactly once
    with SessionLocal() as db:
        assert db.get(Task, people["task"]).status == "queued"
        decision = db.get(Decision, people["unblock"])
        assert decision.status != "open" and decision.resolved_by == people["alice"]
        assert db.query(MindAction).filter_by(id=card["id"]).one().status == "succeeded"
    # The confirmed result is on the saved card after a reload, and the next turn still sees the conversation.
    view = client.get(f"/api/mind/threads/{thread}").json()
    assert view["messages"][1]["action_cards"][0]["id"] == card["id"]
    events = send(client, thread, "Did it work?", key="m2")
    assert events[-1]["data"] == {"status": "complete"}
    assert any(m["role"] == "assistant" and "retry T1" in m["text"] for m in runtime.histories[-1])


class FakeProvider:
    def __init__(self, ready=True):
        self.ready = mp.Readiness(ready, None if ready else "model_absent", model=MODEL if ready else None)
        self.sessions = []

    async def readiness(self, workspace_id, ledger, session_id=None):
        self.sessions.append(session_id)
        return self.ready

    async def session_cost(self, workspace_id, session_id):
        return 0.0


class FakeEngine:
    def __init__(self, provider, fail_with=None):
        self.provider, self.fail_with, self.calls = provider, fail_with, []

    async def run(self, **kwargs):
        self.calls.append(kwargs)
        await kwargs["emit"]({"type": "tool_start", "name": "overview"})
        await kwargs["emit"]({"type": "tool_result", "name": "overview"})
        await kwargs["emit"]({"type": "text_delta", "text": "ok"})
        if self.fail_with is not None:
            try:  # as the real engine does with a provider stop
                await kwargs["ledger"].admit()
            except mp._ProviderStop as exc:
                raise mp.MindUnavailable(exc.reason) from None


class FakeLedger:
    session_id = "mind-turn"

    def __init__(self, refuse=None):
        self.refuse, self.reconciled, self.before, self.after = refuse, 0, 0, 0

    async def before_call(self):
        self.before += 1
        if self.refuse:
            raise AccountingError(self.refuse)

    async def after_call(self, usage):
        self.after += 1

    async def reconcile(self):
        self.reconciled += 1


def test_the_runtime_adapter_builds_one_turn(people):
    provider = FakeProvider()
    engine = FakeEngine(provider)
    runtime = mr.EngineRuntime(engine)
    with SessionLocal() as db:
        thread = MindThread(owner_id=people["alice"], billing_workspace_id=people["first"],
                            workspace_ids=[people["first"], people["second"]])
        db.add(thread); db.commit()
        thread_id = thread.id
    context = TurnContext(people["alice"], thread_id, people["first"], (people["first"], people["second"]))
    history = [{"role": "user", "text": "first"}, {"role": "assistant", "text": ""},  # an interrupted reply
               {"role": "tool", "text": "x"}, {"role": "assistant", "text": "answer"},
               {"role": "user", "text": "next", "context": {"workspace_id": people["first"], "kind": "outcome",
                                                            "id": "o1", "label": "Outcome 7", "evil": "x"}}]
    seen = []

    async def emit(event):
        seen.append(event)

    ledger = FakeLedger()
    asyncio.run(runtime.run_turn(context, history, emit, tools=None, ledger=ledger))
    call = engine.calls[0]
    assert call["history"] == [{"role": "user", "content": "first"}, {"role": "assistant", "content": "answer"}]
    assert call["session_id"] == provider.sessions[0] == "mind-turn"  # one MindsHub session for the turn
    assert call["prompt"].endswith("next") and "Outcome 7" in call["prompt"] and "evil" not in call["prompt"]
    assert "Mobile" in call["prompt"] and "Platform" in call["prompt"]
    assert [e["type"] for e in seen] == ["tool_start", "text_delta"]  # finished tools come from the dispatcher
    assert ledger.reconciled == 1

    with pytest.raises(mp.MindUnavailable):
        asyncio.run(mr.EngineRuntime(FakeEngine(FakeProvider(ready=False))).run_turn(
            context, history, emit, tools=None, ledger=FakeLedger()))


def test_the_ledger_bridge_waits_briefly_for_late_cost_then_stops(people):
    class Late(FakeLedger):
        async def before_call(self):
            self.before += 1
            if self.before < 3:
                raise AccountingError("accounting_pending")

    late = Late()
    bridge = mr.LedgerBridge(late, wait=0)
    asyncio.run(bridge.admit())
    assert late.before == 3 and bridge.error is None
    stuck = mr.LedgerBridge(FakeLedger(refuse="accounting_pending"), wait=0)
    with pytest.raises(mp._ProviderStop):
        asyncio.run(stuck.admit())
    assert stuck.ledger.before == mr.RECONCILE_TRIES and stuck.error.code == "accounting_pending"
    spent = mr.LedgerBridge(FakeLedger(refuse="budget_exceeded"), wait=0)
    with pytest.raises(mp._ProviderStop):
        asyncio.run(spent.admit())
    assert spent.ledger.before == 1  # a spent budget is final, never retried
    with pytest.raises(mp._ProviderStop):
        spent.check()  # and every later send in the turn stops before it is sent

    # The engine's generic stop becomes the ledger's own error, so the dock shows its budget state.
    with SessionLocal() as db:
        thread = MindThread(owner_id=people["alice"], billing_workspace_id=people["first"],
                            workspace_ids=[people["first"]])
        db.add(thread); db.commit()
        context = TurnContext(people["alice"], thread.id, people["first"], (people["first"],))

    async def emit(_):
        pass

    with pytest.raises(AccountingError) as exc:
        asyncio.run(mr.EngineRuntime(FakeEngine(FakeProvider(), fail_with=True)).run_turn(
            context, [{"role": "user", "text": "hi"}], emit, tools=None, ledger=FakeLedger(refuse="budget_exceeded")))
    assert exc.value.code == "budget_exceeded"


@pytest.mark.parametrize("value", ["NaN", "Infinity", -1, "10", True, {"daily_budget_usd": 1, "x": 2}, [1]])
def test_mind_budget_setting_is_validated(client, signed_in, value):
    ws = signed_in["workspaces"][0]["id"]
    body = value if isinstance(value, (dict, list)) else {"daily_budget_usd": value}
    raw = json.dumps({"settings": {"mind": body}}).replace('"NaN"', "NaN").replace('"Infinity"', "Infinity")
    before = client.get("/api/me").json()["workspaces"][0]
    r = client.patch(f"/api/workspaces/{ws}", content=raw, headers={"Content-Type": "application/json"})
    assert r.status_code == 422, r.text
    assert client.get("/api/me").json()["workspaces"][0] == before  # nothing else was replaced


def test_mind_budget_setting_defaults_and_accepts_zero(client, signed_in):
    ws = signed_in["workspaces"][0]["id"]
    r = client.patch(f"/api/workspaces/{ws}", json={"settings": {"concurrency": 4}})
    assert r.json()["settings"]["mind"] == {"daily_budget_usd": 10.0}
    r = client.patch(f"/api/workspaces/{ws}", json={"settings": {"mind": {"daily_budget_usd": 0}}})
    assert r.status_code == 200 and r.json()["settings"]["mind"] == {"daily_budget_usd": 0.0}
    assert r.json()["settings"]["concurrency"] == 4
