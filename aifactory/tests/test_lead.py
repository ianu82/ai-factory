"""The Lead: one agent per outcome, woken by what needs judgement, acting only within the delegated safe set."""
from __future__ import annotations

import asyncio
from datetime import timedelta

import pytest

from aifactory import db
from aifactory.inference import ProviderConfig
from aifactory.models import (Decision, DecisionReview, Event, Message, Outcome, Plan, Repository, Run, Task, User,
                              Workspace)
from aifactory.pipeline import lead
from tests.fakes import ScriptedModel, call, step


@pytest.fixture
def world(signed_in):
    ws, uid = signed_in["workspaces"][0]["id"], signed_in["user"]["id"]
    with db.session_scope() as s:
        w = s.get(Workspace, ws)
        w.settings = {**(w.settings or {}), "models": {"lead": "lead-model"},
                      "pricing": {"lead-model": {"input": 1, "output": 2}},
                      "budgets": {"run_usd": 5, "outcome_usd": 40, "run_tokens": 1_000_000}}
        repo = Repository(workspace_id=ws, full_name="team/app")
        o = Outcome(workspace_id=ws, number=7, title="Search", brief="Add search", status="running")
        s.add_all([repo, o])
        s.flush()
        o.repo_ids = [repo.id]
        plan = Plan(outcome_id=o.id, status="approved", content={"tasks": [{"key": "T1", "title": "Endpoint"}]})
        s.add(plan)
        s.flush()
        t = Task(workspace_id=ws, outcome_id=o.id, plan_id=plan.id, repo_id=repo.id, key="T1", title="Endpoint",
                 status="needs_input", stage="build")
        s.add(t)
        s.flush()
        question = Decision(workspace_id=ws, outcome_id=o.id, task_id=t.id, kind="question", title="Which queue?",
                            body="Durable or in-memory?", options=[{"value": "answer", "label": "Answer"}])
        merge = Decision(workspace_id=ws, outcome_id=o.id, task_id=t.id, kind="approve_merge", title="Merge PR #12?",
                         options=[{"value": "approve", "label": "Merge"}, {"value": "reject", "label": "Request changes"}])
        s.add_all([question, merge])
        s.flush()
        return {"ws": ws, "uid": uid, "outcome": o.id, "task": t.id, "question": question.id, "merge": merge.id,
                "repo": repo.id, "plan": plan.id}


def woken():
    return {d["outcome_id"]: d for d in lead.due()}


def lead_run(world, seen, *, ago=timedelta(minutes=5), status="succeeded"):
    with db.session_scope() as s:
        s.add(Run(workspace_id=world["ws"], outcome_id=world["outcome"], role="lead", harness="control_plane",
                  status=status, started_at=db.utcnow() - ago, config_snapshot={"seen": seen}))


def test_waiting_decisions_and_peoples_posts_wake_the_lead(world):
    due = woken()[world["outcome"]]
    assert due["reasons"] == ["2 decisions waiting on someone"]
    assert set(due["decisions"]) == {world["question"], world["merge"]}
    lead_run(world, due["seen"])
    assert world["outcome"] not in woken()  # nothing new since its last run

    with db.session_scope() as s:  # a changed question wakes it again
        s.get(Decision, world["question"]).body = "Durable, in-memory, or Redis?"
    due = woken()[world["outcome"]]
    assert due["reasons"] == ["1 decision waiting on someone"] and due["decisions"] == [world["question"]]
    lead_run(world, due["seen"], ago=timedelta(minutes=4))

    with db.session_scope() as s:  # the Lead's own guidance never wakes it
        s.add(Message(workspace_id=world["ws"], outcome_id=world["outcome"], author_id=lead.lead_user(s).id,
                      author_kind="user", body="Use the durable queue."))
    assert world["outcome"] not in woken()
    with db.session_scope() as s:  # a person's post does
        s.add(Message(workspace_id=world["ws"], outcome_id=world["outcome"], author_id=world["uid"],
                      author_kind="user", body="Why is T1 stuck?"))
    assert woken()[world["outcome"]]["reasons"] == ["1 new post from people"]


def test_a_decision_under_judge_review_reaches_the_lead_only_once_the_judge_holds_it(world):
    with db.session_scope() as s:
        s.add(DecisionReview(workspace_id=world["ws"], decision_id=world["question"], state="pending"))
    assert woken()[world["outcome"]]["decisions"] == [world["merge"]]  # the judge goes first
    with db.session_scope() as s:
        review = s.query(DecisionReview).one()
        review.state, review.finished_at, review.report = "finished", db.utcnow(), {"verdict": "hold"}
    assert set(woken()[world["outcome"]]["decisions"]) == {world["question"], world["merge"]}


def test_the_lead_waits_its_turn(world):
    seen = woken()[world["outcome"]]["seen"]
    lead_run(world, {}, ago=timedelta(seconds=10))
    assert world["outcome"] not in woken()  # cooldown after its last run
    with db.session_scope() as s:
        s.query(Run).delete()
    lead_run(world, {}, ago=timedelta(minutes=5), status="running")
    assert world["outcome"] not in woken()  # one run per outcome at a time
    with db.session_scope() as s:
        s.query(Run).delete()
    lead_run(world, {}, ago=timedelta(hours=1), status="running")
    assert world["outcome"] in woken()  # a run cut off by a restart doesn't block it
    assert world["outcome"] not in lead.due(running=frozenset({world["outcome"]}))
    with db.session_scope() as s:
        s.query(Run).delete()
    lead_run(world, seen, ago=timedelta(minutes=5), status="failed")
    assert world["outcome"] not in woken()  # a failed run waits before trying again
    with db.session_scope() as s:
        s.query(Run).delete()
    lead_run(world, seen, ago=timedelta(minutes=20), status="failed")
    assert woken()[world["outcome"]]["decisions"]  # and what it failed on is still waiting
    with db.session_scope() as s:
        s.query(Run).delete()
        for n in range(lead.DAILY_RUNS):
            s.add(Run(workspace_id=world["ws"], outcome_id=world["outcome"], role="lead", harness="control_plane",
                      status="succeeded", started_at=db.utcnow() - timedelta(minutes=5 + n), config_snapshot={}))
    assert world["outcome"] not in woken()  # daily cap
    assert seen["decisions"]


@pytest.mark.parametrize("change", ["finished", "paused", "disabled", "review"])
def test_no_lead_for_finished_paused_disabled_or_review_work(world, change):
    with db.session_scope() as s:
        o, w = s.get(Outcome, world["outcome"]), s.get(Workspace, world["ws"])
        if change == "finished":
            o.status = "completed"
        elif change == "paused":
            w.paused = True
        elif change == "disabled":
            w.settings = {**w.settings, "lead": {"enabled": False}}
        else:
            o.source = "review"
    assert world["outcome"] not in woken()


def test_its_first_run_reads_old_posts_without_waking_for_them(world):
    with db.session_scope() as s:
        s.query(Decision).delete()
        s.add(Message(workspace_id=world["ws"], outcome_id=world["outcome"], author_id=world["uid"],
                      author_kind="user", body="Old note", created_at=db.utcnow() - timedelta(days=3)))
    assert world["outcome"] not in woken()
    with db.session_scope() as s:
        s.add(Message(workspace_id=world["ws"], outcome_id=world["outcome"], author_id=world["uid"],
                      author_kind="user", body="Recent note", created_at=db.utcnow() - timedelta(hours=1)))
    assert woken()[world["outcome"]]["reasons"] == ["1 new post from people"]


def test_the_lead_acts_only_within_the_safe_set_and_its_own_outcome(world):
    tools = lead.Tools(world["ws"], world["outcome"], "run-1")
    assert "may not choose" in tools.call("resolve_decision", {"decision_id": world["merge"], "option": "approve"})
    assert "comment is required" in tools.call("resolve_decision", {"decision_id": world["merge"], "option": "reject"})
    with db.session_scope() as s:  # another outcome's decision is out of reach
        other = Outcome(workspace_id=world["ws"], number=8, title="Other", status="running")
        s.add(other)
        s.flush()
        elsewhere = Decision(workspace_id=world["ws"], outcome_id=other.id, kind="question", title="Elsewhere?",
                             options=[{"value": "answer"}])
        s.add(elsewhere)
        s.flush()
        elsewhere_id = elsewhere.id
        merging = Task(workspace_id=world["ws"], outcome_id=world["outcome"], plan_id=world["plan"],
                       repo_id=world["repo"], key="T2", title="Merging", status="blocked", stage="merge")
        s.add(merging)
        s.flush()
        unblock = Decision(workspace_id=world["ws"], outcome_id=world["outcome"], task_id=merging.id, kind="unblock",
                           title="T2 is stuck", options=[{"value": "retry"}, {"value": "abandon"}])
        s.add(unblock)
        s.flush()
        unblock_id = unblock.id
    assert "No open decision" in tools.call("resolve_decision", {"decision_id": elsewhere_id, "option": "answer",
                                                                 "comment": "x"})
    assert "would resume a merge" in tools.call("resolve_decision", {"decision_id": unblock_id, "option": "retry"})
    assert "may not choose" in tools.call("resolve_decision", {"decision_id": unblock_id, "option": "abandon"})
    assert "Unknown tool" in tools.call("approve_everything", {})
    assert "Invalid arguments" in tools.call("note_for_people", {"txt": "typo"})
    assert "No task" in tools.call("post_guidance", {"text": "Hi", "task": "T9"})
    assert "No task" in tools.call("task_detail", {"task": "T9"})
    assert "Search" in tools.call("outcome_record", {}) and "Endpoint" in tools.call("task_detail", {"task": "t1"})
    assert "Posted" in tools.call("post_guidance", {"text": "Prefer the durable queue.", "task": "T1"})
    with db.session_scope() as s:
        assert all(d.status == "open" for d in s.query(Decision))
        posted = s.query(Message).one()
        assert (posted.task_id, posted.author_kind, posted.attribution) == (
            world["task"], "user", {"source": "lead", "run_id": "run-1"})
        assert s.get(User, posted.author_id).is_system


def test_each_tool_has_a_limit_per_run(world):
    tools = lead.Tools(world["ws"], world["outcome"], "run-1")
    for _ in range(lead.PER_RUN["note_for_people"]):
        assert "Noted" in tools.call("note_for_people", {"text": "Waiting on review."})
    assert "the limit" in tools.call("note_for_people", {"text": "One more."})


@pytest.fixture
def model(world, monkeypatch):
    ScriptedModel.calls = []
    monkeypatch.setattr(lead, "ModelClient", ScriptedModel)
    monkeypatch.setattr(lead, "resolve_provider", lambda ws: ProviderConfig("mindshub", "unused", "SECRET", "Test"))
    return ScriptedModel


def test_a_run_answers_the_question_and_recommends_on_the_merge(world, model):
    seen_requests = []

    def script(messages):
        seen_requests.append(messages)
        if step(messages) == 0:
            return {"content": "", "tool_calls": [
                call("resolve_decision", {"decision_id": world["question"], "option": "answer",
                                          "comment": "Use the durable queue: jobs must survive restarts."}, 0),
                call("resolve_decision", {"decision_id": world["merge"], "option": "approve"}, 1),
                call("note_for_people", {"decision_id": world["merge"],
                                         "text": "PR #12 is ready: CI and the verifier passed. I recommend merging."}, 2)]}
        return {"content": "", "tool_calls": [call("finish", {"summary": "Answered T1's question; merge needs you."})]}

    model.scripts = {"lead": script}
    due = woken()[world["outcome"]]
    row = asyncio.run(lead.run_lead(world["ws"], world["outcome"], due["reasons"], due["seen"], due["decisions"]))

    assert row.role == "lead" and row.harness == "control_plane" and row.status == "succeeded", row.error
    assert row.model == "lead-model" and row.provider == "mindshub" and row.turns == 2
    assert row.cost_known and row.cost_usd == round(2 * (1000 * 1 + 200 * 2) / 1e6, 6)
    assert row.result["summary"] == "Answered T1's question; merge needs you."
    assert [a["tool"] for a in row.result["actions"]] == ["resolve_decision", "note_for_people"]
    assert model.calls == ["lead", "lead"]
    first = seen_requests[0]
    assert first[0]["content"].startswith("You are the Lead") and "UNTRUSTED DATA" in first[0]["content"]
    assert "Which queue?" in first[1]["content"] and "2 decisions waiting on someone" in first[1]["content"]
    outputs = [m["content"] for m in seen_requests[1] if m["role"] == "tool"]
    assert outputs[0].startswith("Applied: answer") and "may not choose" in outputs[1]

    with db.session_scope() as s:
        question, merge = s.get(Decision, world["question"]), s.get(Decision, world["merge"])
        the_lead = s.query(User).filter_by(email=lead.LEAD_EMAIL).one()
        assert question.status == "resolved" and question.resolution == "answer"
        assert question.resolved_by == the_lead.id and question.attribution == {"source": "lead", "run_id": row.id}
        assert merge.status == "open"  # approving a merge stays with people
        guidance = s.query(Message).filter_by(author_kind="user").one()
        assert guidance.body.startswith("Use the durable queue") and guidance.author_id == the_lead.id
        note = s.query(Message).filter_by(author_kind="lead").one()
        assert note.attribution == {"source": "lead", "run_id": row.id, "decision_id": world["merge"]}
        kinds = [e.kind for e in s.query(Event).filter(Event.outcome_id == world["outcome"]).order_by(Event.id)]
        assert kinds[0] == "run_started" and kinds[-1] == "run_finished"
        assert "decision_resolved" in kinds and "lead_note" in kinds
        assert s.get(Run, row.id).config_snapshot["seen"] == due["seen"]
    assert world["outcome"] not in woken()  # what it was woken for is handled


def test_a_spent_outcome_budget_stops_the_run_before_any_model_call(world, model):
    with db.session_scope() as s:
        w = s.get(Workspace, world["ws"])
        w.settings = {**w.settings, "budgets": {**w.settings["budgets"], "outcome_usd": 1}}
        s.add(Run(workspace_id=world["ws"], outcome_id=world["outcome"], role="builder", harness="native",
                  status="succeeded", cost_usd=2.0, cost_known=True))
    row = asyncio.run(lead.run_lead(world["ws"], world["outcome"], ["test"], {}))
    assert row.status == "failed" and "budget is spent" in row.error and model.calls == []


def test_a_restart_ends_runs_in_progress(world):
    lead_run(world, {}, status="running")
    assert lead.recover() == 1
    with db.session_scope() as s:
        run = s.query(Run).one()
        assert run.status == "failed" and run.error == "Interrupted by a restart" and run.finished_at


def test_lead_settings_are_validated(client, signed_in):
    ws = signed_in["workspaces"][0]["id"]
    r = client.patch(f"/api/workspaces/{ws}", json={"settings": {"lead": {"enabled": False, "run_usd": 1, "turns": 10}}})
    assert r.status_code == 200 and r.json()["settings"]["lead"] == {"enabled": False, "run_usd": 1.0, "turns": 10}
    for bad in ({"run_usd": 50}, {"turns": 0}, {"enabled": "yes"}, {"model": "x"}, "on"):
        assert client.patch(f"/api/workspaces/{ws}", json={"settings": {"lead": bad}}).status_code == 422
    with db.session_scope() as s:  # the refused changes left the saved settings alone
        assert s.get(Workspace, ws).settings["lead"] == {"enabled": False, "run_usd": 1.0, "turns": 10}


def test_the_orchestrator_runs_due_leads_beside_the_scheduler(monkeypatch):
    from aifactory.pipeline import orchestrator as orchestration

    started = []

    async def fake_run(workspace_id, outcome_id, reasons, seen, decisions):
        started.append(outcome_id)
        await asyncio.sleep(0)

    monkeypatch.setattr(lead, "due", lambda running: [
        {"workspace_id": "w", "outcome_id": f"o{n}", "reasons": ["r"], "seen": {}, "decisions": []}
        for n in range(3) if f"o{n}" not in running])
    monkeypatch.setattr(lead, "run_lead", fake_run)
    monkeypatch.setenv("AIFACTORY_LEAD_CONCURRENCY", "2")

    async def go():
        orchestrator = orchestration.Orchestrator()
        await orchestrator._maybe_run_leads()
        assert set(orchestrator._leads) == {"o0", "o1"}  # the server-wide limit
        await asyncio.gather(*orchestrator._leads.values())
        await asyncio.sleep(0)
        assert orchestrator._leads == {}

    asyncio.run(go())
    assert started == ["o0", "o1"]
