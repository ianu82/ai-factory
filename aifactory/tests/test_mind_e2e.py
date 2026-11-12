"""Mind end to end through the real app: routers, the runtime adapter, the restricted engine, the provider and
durable accounting, against a local fake MindsHub. Nothing here calls a paid service."""
import asyncio
import json
from datetime import datetime, timedelta

import pytest
from fastapi.testclient import TestClient

from aifactory.app import create_app
from aifactory.auth import current_user
from aifactory.db import SessionLocal
from aifactory.mind import provider as mp
from aifactory.mind import runtime as mr
from aifactory.mind.contracts import TurnContext
from aifactory.mind.store import AccountingError
from aifactory.models import (Decision, Event, Membership, MindAction, MindActionResult, MindCharge, MindThread,
                             MindMessage, Organization, Outcome, Plan, Repository, Secret, Task, User, Workspace)
from aifactory.security import encrypt
from tests.test_mind_provider import KEY, MODEL, fake_provider


@pytest.fixture
def people():
    """Alice is a member of two workspaces; Bob is only a viewer of the first."""
    with SessionLocal() as db:
        org = Organization(name="Org")
        alice, bob = User(name="Alice", email="alice@t"), User(name="Bob", email="bob@t")
        db.add_all([org, alice, bob]); db.flush()
        first = Workspace(org_id=org.id, name="Platform", slug="platform", outcome_seq=7)  # outcome #7 below
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
        assert thread["workspace_ids"] == [people["first"]]
        status = client.get("/api/mind/status", params={"workspace_id": people["first"]}).json()
        assert status == {"status": "ready", "model_id": MODEL, "reason": None, "accounting": "settled"}
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
    assert "This conversation belongs to the Factory workspace" in prompt
    assert "Platform" in prompt and people["first"] in prompt
    assert "Mobile" not in prompt and people["second"] not in prompt
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
        await tools.call("propose_resolve_decision", {"workspace_id": self.ids["first"],
                                                      "decision_id": self.ids["unblock"], "option": "retry"})
        await emit({"type": "text_delta", "text": "I can retry T1 once you confirm."})
        await ledger.after_call(0.01)


def test_a_proposed_action_runs_once_only_after_confirmation(app_as, people):
    client, app, actor = app_as
    app.state.mind_runtime = runtime = ProposingRuntime(people)
    thread = client.post("/api/mind/threads", json={"billing_workspace_id": people["first"]}).json()["id"]
    events = send(client, thread, "Can T1 run again?")
    card = next(e["data"]["card"] for e in events if e["type"] == "action_card")
    assert card["status"] == "pending" and card["kind"] == "resolve_decision"
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
    saved = view["messages"][1]["action_cards"][0]
    assert saved["id"] == card["id"] and saved["status"] == "succeeded"  # as it is now, not as streamed
    assert saved["result"]["ok"] is True
    events = send(client, thread, "Did it work?", key="m2")
    assert events[-1]["data"] == {"status": "complete"}
    assert any(m["role"] == "assistant" and "retry T1" in m["text"] for m in runtime.histories[-1])


class ScriptedCalls(ProposingRuntime):
    """Makes the given tool calls through the real private dispatcher and keeps what each returned to the model."""

    def __init__(self, ids, calls):
        super().__init__(ids)
        self.calls, self.results = calls, []

    async def run_turn(self, context, history, emit, tools, ledger):
        await ledger.before_call()
        for name, arguments in self.calls:
            self.results.append(await tools.call(name, arguments))
        await emit({"type": "text_delta", "text": "Here is what I can do once you confirm."})
        await ledger.after_call(0.01)


def planned(**values):
    from aifactory.mind.actions import planner_task_schema
    extra = {"constraints": ["Only members can search"], "files": ["src/search.py"]}
    return {"depends_on": [], **values, **{k: v for k, v in extra.items() if k in planner_task_schema()["properties"]}}


def test_a_plan_worked_out_with_mind_starts_building_once_confirmed(app_as, people):
    client, app, actor = app_as
    task = planned(key="T1", title="Search endpoint", repo="acme/platform", spec="Add GET /search with paging.",
                   acceptance=["GET /search?q=x returns matching outcomes"])
    args = {"workspace_id": people["first"], "title": "Search", "text": "Search outcomes by title."}
    app.state.mind_runtime = runtime = ScriptedCalls(people, [
        ("propose_build_plan", {**args, "tasks": [{**task, "depends_on": ["T9"]}]}),
        ("propose_build_plan", {**args, "tasks": [task]})])
    thread = client.post("/api/mind/threads", json={"billing_workspace_id": people["first"]}).json()["id"]
    events = send(client, thread, "Let's plan search together")
    refused, card = runtime.results
    # The planner's validator explains itself, so the model can fix the plan; nothing was saved for it.
    assert refused["refused"] is True and refused["status"] == 422 and "depends on unknown tasks" in refused["reason"]
    assert [e["data"]["card"]["id"] for e in events if e["type"] == "action_card"] == [card["id"]]
    assert any(e["type"] == "tool_finished" and e["data"]["summary"] == "Factory refused the call" for e in events)
    assert card["kind"] == "build_plan" and card["fields"]["plan"] == "T1 Search endpoint — acme/platform"
    assert card["effect"].startswith("Create a new outcome “Search” in Platform with this 1-task plan (T1)")
    with SessionLocal() as db:
        assert db.query(Outcome).filter_by(workspace_id=people["first"]).count() == 1  # a proposal creates nothing
        assert db.query(MindAction).count() == 1
    base = f"/api/mind/threads/{thread}/actions/{card['id']}"
    actor[0] = people["bob"]
    assert client.post(f"{base}/confirm", json={"expected_revision": 1}).status_code == 404
    actor[0] = people["alice"]
    confirmed = client.post(f"{base}/confirm", json={"expected_revision": 1})
    assert confirmed.status_code == 200, confirmed.text
    assert confirmed.json()["status"] == "succeeded" and confirmed.json()["effect"].startswith("Create #8 “Search”")
    assert client.post(f"{base}/confirm", json={"expected_revision": 1}).status_code == 409
    with SessionLocal() as db:
        new = db.query(Outcome).filter_by(workspace_id=people["first"], number=8).one()
        assert new.status == "running" and new.source == "mind" and new.created_by == people["alice"]
        plan = db.query(Plan).filter_by(outcome_id=new.id).one()
        assert plan.status == "approved" and plan.approved_by == people["alice"]
        built = db.query(Task).filter_by(outcome_id=new.id).one()
        assert (built.key, built.spec, built.status) == ("T1", "Add GET /search with paging.", "pending")
        assert db.query(Event).filter_by(outcome_id=new.id, kind="planning_started").count() == 0  # no planner ran


def test_a_secret_saved_after_a_plan_card_never_reaches_the_plan(app_as, people):
    client, app, _ = app_as
    opaque = "opaque-plan-secret-0007"  # no credential pattern would recognize it
    task = planned(key="T1", title="Search endpoint", repo="acme/platform", spec=f"Call the index with {opaque}",
                   acceptance=[f"Search works with {opaque}"])
    app.state.mind_runtime = runtime = ScriptedCalls(people, [("propose_build_plan", {
        "workspace_id": people["first"], "title": "Search", "text": "Search outcomes.", "tasks": [task],
        "risks": [f"Rotating {opaque}"]})])
    thread = client.post("/api/mind/threads", json={"billing_workspace_id": people["first"]}).json()["id"]
    send(client, thread, "Plan search")
    card = runtime.results[0]
    with SessionLocal() as db:
        assert opaque in json.dumps(db.get(MindAction, card["id"]).snapshot)  # not a secret when proposed
        db.add(Secret(workspace_id=people["first"], kind="github_token", ciphertext=encrypt(opaque)))
        db.commit()
    confirmed = client.post(f"/api/mind/threads/{thread}/actions/{card['id']}/confirm", json={"expected_revision": 1})
    assert confirmed.status_code == 200, confirmed.text
    assert confirmed.json()["status"] == "succeeded" and opaque not in confirmed.text
    with SessionLocal() as db:
        new = db.query(Outcome).filter_by(workspace_id=people["first"], number=8).one()
        plan = db.query(Plan).filter_by(outcome_id=new.id).one()
        built = db.query(Task).filter_by(outcome_id=new.id).one()
        assert opaque not in json.dumps(plan.content) and "[redacted]" in built.spec
        assert opaque not in built.spec + json.dumps(built.acceptance)
        assert opaque not in json.dumps(db.get(MindAction, card["id"]).snapshot)


def test_a_viewer_is_refused_and_its_thread_keeps_its_scope(app_as, people):
    client, app, actor = app_as
    actor[0] = people["bob"]  # a viewer of the first workspace, not a member of the second
    app.state.mind_runtime = runtime = ScriptedCalls(people, [
        ("propose_cancel_task", {"workspace_id": people["first"], "outcome_id": people["outcome"],
                                 "task_id": people["task"]}),
        ("propose_create_outcome", {"workspace_id": people["second"], "title": "Not mine", "text": "No."})])
    thread = client.post("/api/mind/threads", json={"billing_workspace_id": people["first"]}).json()["id"]
    events = send(client, thread, "Stop T1")
    assert events[-1]["data"] == {"status": "complete"}
    viewer, stranger = runtime.results
    assert viewer["refused"] and viewer["status"] == 403
    assert stranger["refused"] and stranger["status"] == 403
    assert people["first"] in stranger["reason"]
    with SessionLocal() as db:
        assert db.query(MindAction).count() == 0 and db.get(Task, people["task"]).status == "blocked"
        assert people["second"] not in db.get(MindThread, thread).workspace_ids
    assert client.get(f"/api/mind/threads/{thread}").status_code == 200  # a refusal never widens (and hides) it


def test_take_over_through_the_app_hands_the_task_over(app_as, people):
    client, app, _ = app_as
    app.state.mind_runtime = ScriptedCalls(people, [("propose_take_over_task", {
        "workspace_id": people["first"], "outcome_id": people["outcome"], "task_id": people["task"]})])
    thread = client.post("/api/mind/threads", json={"billing_workspace_id": people["first"]}).json()["id"]
    card = next(e["data"]["card"] for e in send(client, thread, "I'll take T1") if e["type"] == "action_card")
    assert card["kind"] == "take_over_task" and card["fields"] == {}
    confirmed = client.post(f"/api/mind/threads/{thread}/actions/{card['id']}/confirm", json={"expected_revision": 1})
    assert confirmed.status_code == 200, confirmed.text
    assert confirmed.json()["result"]["handover"] == {"published": False}  # the follow-up ran after the commit
    with SessionLocal() as db:
        assert db.get(Task, people["task"]).status == "handed_over"
        assert db.get(Decision, people["unblock"]).status == "superseded"
        event = db.query(Event).filter_by(kind="task_handed_over", task_id=people["task"]).one()
        assert event.message.startswith("Alice via Mind took over T1") and event.actor == people["alice"]
    saved = client.get(f"/api/mind/threads/{thread}").json()["messages"][1]["action_cards"][0]
    assert saved["status"] == "succeeded" and saved["result"]["handover"] == {"published": False}


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


def test_card_edits_and_confirmations_use_the_workspace_secret_scrub(app_as, people):
    client, app, _ = app_as
    app.state.mind_runtime = ProposingRuntime(people)
    opaque = "opaque-workspace-secret-0001"  # no credential pattern would recognize it
    with SessionLocal() as db:
        db.add(Secret(workspace_id=people["first"], kind="github_token", ciphertext=encrypt(opaque)))
        db.commit()
    thread = client.post("/api/mind/threads", json={"billing_workspace_id": people["first"]}).json()["id"]
    card = next(e["data"]["card"] for e in send(client, thread, "Retry T1?") if e["type"] == "action_card")
    base = f"/api/mind/threads/{thread}/actions/{card['id']}"
    edited = client.patch(base, json={"expected_revision": 1, "text": f"Use {opaque} please"})
    assert edited.status_code == 200, edited.text
    assert opaque not in edited.text and "[redacted]" in edited.json()["fields"]["text"]
    with SessionLocal() as db:
        assert opaque not in json.dumps(db.get(MindAction, card["id"]).fields)
    confirmed = client.post(f"{base}/confirm", json={"expected_revision": 2})
    assert confirmed.status_code == 200 and opaque not in confirmed.text
    # Rejected input is never echoed back, as on the thread API.
    for bad in ({"expected_revision": 3, "text": "x" * 8001 + opaque}, {"expected_revision": 3, "extra": opaque}):
        response = client.patch(base, json=bad)
        assert response.status_code == 422 and opaque not in response.text


@pytest.mark.parametrize("legacy", [False, True])
def test_the_runtime_adapter_builds_one_turn(people, legacy):
    provider = FakeProvider()
    engine = FakeEngine(provider)
    runtime = mr.EngineRuntime(engine)
    with SessionLocal() as db:
        thread = MindThread(owner_id=people["alice"], billing_workspace_id=people["first"],
                            workspace_ids=[people["first"], people["second"]] if legacy else [people["first"]])
        db.add(thread); db.commit()
        thread_id = thread.id
    # Even legacy threads with a wider stored scope may only use their billing workspace.
    context = TurnContext(people["alice"], thread_id, people["first"], (people["first"],))
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
    assert call["history"] == ([] if legacy else [
        {"role": "user", "content": "first"}, {"role": "assistant", "content": "answer"}])
    assert call["session_id"] == provider.sessions[0] == "mind-turn"  # one MindsHub session for the turn
    assert call["prompt"].endswith("next") and "Outcome 7" in call["prompt"] and "evil" not in call["prompt"]
    assert "Platform" in call["prompt"] and people["first"] in call["prompt"]
    assert "Mobile" not in call["prompt"] and people["second"] not in call["prompt"]
    with SessionLocal() as db:
        assert db.get(MindThread, thread_id).workspace_ids == (
            [people["first"], people["second"]] if legacy else [people["first"]])
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


def test_workspace_names_reach_the_model_only_scrubbed(people):
    opaque = "opaque-name-secret-0042"
    with SessionLocal() as db:
        db.get(Workspace, people["first"]).name = f"Platform {opaque}"
        db.add(Secret(workspace_id=people["first"], kind="github_token", ciphertext=encrypt(opaque)))
        thread = MindThread(owner_id=people["alice"], billing_workspace_id=people["first"],
                            workspace_ids=[people["first"]])
        db.add(thread); db.commit()
        context = TurnContext(people["alice"], thread.id, people["first"], (people["first"],))
    engine = FakeEngine(FakeProvider())

    async def emit(_):
        pass

    asyncio.run(mr.EngineRuntime(engine).run_turn(context, [{"role": "user", "text": "hi"}], emit, tools=None,
                                                  ledger=FakeLedger()))
    prompt = engine.calls[0]["prompt"]
    assert opaque not in prompt and "Platform [redacted]" in prompt
    assert "Mobile" not in prompt and people["second"] not in prompt


@pytest.mark.parametrize("module", ["aifactory.mind.actions", "aifactory.mind.api", "aifactory.mind.runtime"])
def test_each_mind_module_imports_on_its_own(module):
    """No import cycle: a fresh interpreter can import any Mind module first (tests import actions directly)."""
    import os
    import subprocess
    import sys
    from pathlib import Path

    src = str(Path(__file__).resolve().parents[1] / "src")
    env = {**os.environ, "PYTHONPATH": os.pathsep.join(filter(None, [src, os.environ.get("PYTHONPATH")]))}
    result = subprocess.run([sys.executable, "-c", f"import {module}"], capture_output=True, text=True, env=env,
                            timeout=120)
    assert result.returncode == 0, result.stderr[-2000:]


@pytest.mark.parametrize("code,expected", [("missing_key", "Add a saved MindsHub Inference key"),
                                           ("model_absent", "Claude Opus 5.5 is not available"),
                                           ("raw provider text sk-abcdefghijklmnopqrstu", "Check workspace inference settings")])
def test_status_names_what_to_fix_with_fixed_public_reasons_only(app_as, people, code, expected):
    client, app, _ = app_as

    class Unready:
        async def readiness(self, workspace_id, *, ledger):
            return mp.Readiness(False, code)

    app.state.mind_runtime = Unready()
    status = client.get("/api/mind/status", params={"workspace_id": people["first"]}).json()
    assert status["status"] == "unavailable" and expected in status["reason"] and "sk-" not in status["reason"]


def test_a_failed_turn_keeps_its_status_when_the_reader_stops_at_the_error(app_as, people):
    client, app, _ = app_as

    class Failing(ProposingRuntime):
        async def run_turn(self, context, history, emit, tools, ledger):
            await emit({"type": "text_delta", "text": "Partial answer"})
            raise mp.MindUnavailable("provider_failure")

    app.state.mind_runtime = Failing(people)
    thread = client.post("/api/mind/threads", json={"billing_workspace_id": people["first"]}).json()["id"]
    with client.stream("POST", f"/api/mind/threads/{thread}/messages",
                       json={"text": "Status?", "client_message_id": "fail-1"}) as response:
        for line in response.iter_lines():  # stop reading at the error frame, as the browser used to
            if line.startswith("data: ") and json.loads(line[6:])["type"] == "error":
                break
    import time
    for _ in range(100):  # the server saves the turn as its stream ends, which can follow the reader stopping
        saved = client.get(f"/api/mind/threads/{thread}").json()
        if saved["messages"][1]["status"] != "streaming":
            break
        time.sleep(0.05)
    assert saved["messages"][1]["status"] == "failed" and saved["messages"][1]["text"] == "Partial answer"


def test_a_finished_turn_is_saved_before_its_done_frame(app_as, people):
    client, app, _ = app_as

    class Answering(ProposingRuntime):
        async def run_turn(self, context, history, emit, tools, ledger):
            await ledger.before_call()
            await emit({"type": "text_delta", "text": "Saved before done."})
            await ledger.after_call(0.01)

    app.state.mind_runtime = Answering(people)
    thread = client.post("/api/mind/threads", json={"billing_workspace_id": people["first"]}).json()["id"]
    with client.stream("POST", f"/api/mind/threads/{thread}/messages",
                       json={"text": "Hello", "client_message_id": "saved-1"}) as response:
        for line in response.iter_lines():
            if line.startswith("data: ") and json.loads(line[6:])["type"] == "done":
                # What the page does on "done": reload. The reply is already saved, with no polling.
                saved = client.get(f"/api/mind/threads/{thread}").json()["messages"][1]
                assert saved["status"] == "complete" and saved["text"] == "Saved before done."
                break


@pytest.mark.parametrize("invalid", ["missing_thread", "revoked_membership", "wrong_billing_workspace"])
def test_runtime_refuses_invalid_thread_scope_before_provider_access(people, invalid):
    from fastapi import HTTPException

    with SessionLocal() as db:
        thread = MindThread(owner_id=people["alice"], billing_workspace_id=people["first"],
                            workspace_ids=[people["first"]])
        db.add(thread); db.commit()
        thread_id = thread.id
        if invalid == "revoked_membership":
            # Even a malformed legacy scope omitting the billing workspace must fail closed.
            thread.workspace_ids = []
            db.query(Membership).filter_by(user_id=people["alice"], workspace_id=people["first"]).delete()
            db.commit()
    context = TurnContext(people["alice"], "missing" if invalid == "missing_thread" else thread_id,
                          people["second"] if invalid == "wrong_billing_workspace" else people["first"], ())
    provider = FakeProvider()
    engine = FakeEngine(provider)

    async def emit(_):
        pytest.fail("A refused turn emitted an event")

    with pytest.raises(HTTPException) as exc:
        asyncio.run(mr.EngineRuntime(engine).run_turn(context, [{"role": "user", "text": "hello"}], emit,
                                                    tools=None, ledger=FakeLedger()))
    assert exc.value.status_code == 404 and exc.value.detail == "Thread not found"
    assert provider.sessions == [] and engine.calls == []


def test_legacy_action_results_only_continue_in_the_billing_workspace(people):
    opaque = "opaque-result-secret-0063"
    start = datetime(2026, 11, 1)
    with SessionLocal() as db:
        thread = MindThread(owner_id=people["alice"], billing_workspace_id=people["first"],
                            workspace_ids=[people["first"], people["second"]])
        db.add(thread); db.flush()
        thread_id = thread.id
        db.add(Secret(workspace_id=people["first"], kind="github_token", ciphertext=encrypt(opaque)))
        # More than a page of newer B results must neither reach the provider nor crowd out A's results.
        for index in range(23):
            workspace_id = people["first"] if index < 2 else people["second"]
            action = MindAction(owner_id=people["alice"], thread_id=thread_id, workspace_id=workspace_id,
                                kind="send_guidance", status="succeeded")
            db.add(action); db.flush()
            content = ({"message": f"Billing result {index}: {opaque}"} if index < 2 else
                       {"workspace_id": people["second"], "message": "Mobile private result"})
            db.add(MindActionResult(owner_id=people["alice"], thread_id=thread_id, action_id=action.id,
                                    content=content, created_at=start + timedelta(seconds=index)))
        db.commit()

    engine = FakeEngine(FakeProvider())

    async def emit(_):
        pass

    context = TurnContext(people["alice"], thread_id, people["first"], (people["first"], people["second"]))
    asyncio.run(mr.EngineRuntime(engine).run_turn(context, [{"role": "user", "text": "What's next?"}], emit,
                                                tools=None, ledger=FakeLedger()))
    prompt = engine.calls[0]["prompt"]
    assert "Confirmed action results" in prompt
    assert prompt.index("Billing result 0") < prompt.index("Billing result 1")
    assert "[redacted]" in prompt and opaque not in prompt
    assert "Mobile" not in prompt and people["second"] not in prompt
    with SessionLocal() as db:
        assert db.get(MindThread, thread_id).workspace_ids == [people["first"], people["second"]]
        assert db.query(MindActionResult).filter_by(thread_id=thread_id).count() == 23  # no migration


def test_legacy_cross_workspace_transcript_never_reaches_later_model_turns(app_as, people):
    client, app, _ = app_as
    engine = FakeEngine(FakeProvider())
    app.state.mind_runtime = mr.EngineRuntime(engine)
    thread_id = client.post("/api/mind/threads", json={"billing_workspace_id": people["first"]}).json()["id"]
    with SessionLocal() as db:
        db.get(MindThread, thread_id).workspace_ids = [people["first"], people["second"]]
        # Plain messages can carry B's data too: context/citation filtering alone cannot establish provenance.
        old = [
            MindMessage(thread_id=thread_id, role="user", text=f"Check Mobile {people['second']}",
                        context={"workspace_id": people["second"], "kind": "workspace"},
                        turn_id="legacy-1", position=1),
            MindMessage(thread_id=thread_id, role="assistant", text="Mobile private outcome: Hidden launch",
                        turn_id="legacy-1", position=2),
            MindMessage(thread_id=thread_id, role="user", text="Tell me about Hidden launch",
                        turn_id="legacy-2", position=3),
            MindMessage(thread_id=thread_id, role="assistant", text="Private launch details",
                        turn_id="legacy-2", position=4),
        ]
        db.add_all(old); db.commit()
        saved = [(m.id, m.text, m.context) for m in old]

    for index in range(2):
        events = send(client, thread_id, "What is next here?", key=f"scoped-{index}", context={
            "workspace_id": people["first"], "kind": "workspace", "label": "Platform overview"})
        assert events[-1]["data"] == {"status": "complete"}
        call = engine.calls[index]
        assert call["history"] == []
        prompt = call["prompt"]
        assert "Platform overview" in prompt and prompt.endswith("What is next here?")
        assert "Mobile" not in prompt and people["second"] not in prompt
        assert "Hidden launch" not in prompt and "Private launch details" not in prompt

    with SessionLocal() as db:
        assert db.get(MindThread, thread_id).workspace_ids == [people["first"], people["second"]]
        for message_id, text, context in saved:
            message = db.get(MindMessage, message_id)
            assert (message.text, message.context) == (text, context)  # no transcript migration
