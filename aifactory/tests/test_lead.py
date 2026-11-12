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
        if status == "succeeded":  # a run that succeeded posted its update (a closing one if it was woken for that)
            s.add(Message(workspace_id=world["ws"], outcome_id=world["outcome"], author_id=lead.lead_user(s).id,
                          author_kind="lead", body="I'm the Lead for #7.", created_at=db.utcnow() - ago,
                          attribution={"source": "lead", "kind": "update",
                                       **({"closing": True} if (seen or {}).get("closing") else {})}))


def test_waiting_decisions_and_peoples_posts_wake_the_lead(world):
    due = woken()[world["outcome"]]
    assert due["reasons"] == ["2 decisions waiting on someone", "you've just joined this outcome"]
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
        if change == "finished":  # long enough ago that its ending was reported
            o.status, o.completed_at = "completed", db.utcnow() - timedelta(hours=2)
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
        s.get(Outcome, world["outcome"]).created_at = db.utcnow() - timedelta(days=3)  # not new: nothing to join
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


def test_the_lead_leaves_a_decision_marked_for_people_alone(world):
    tools = lead.Tools(world["ws"], world["outcome"], "run-1")
    with db.session_scope() as s:
        s.get(Decision, world["merge"]).metadata_json = {"people_only": "A person approves changes to the rails"}
    out = tools.call("resolve_decision", {"decision_id": world["merge"], "option": "reject",
                                          "comment": "Keep the old rule."})
    assert "may not choose" in out
    with db.session_scope() as s:
        assert s.get(Decision, world["merge"]).status == "open"


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
                                         "text": "PR #12 is ready: CI and the verifier passed. I recommend merging."}, 2),
                call("post_update", {"text": "I'm the Lead for #7. T1's question is answered; the merge waits on you."}, 3)]}
        return {"content": "", "tool_calls": [call("finish", {"summary": "Answered T1's question; merge needs you."})]}

    model.scripts = {"lead": script}
    due = woken()[world["outcome"]]
    row = asyncio.run(lead.run_lead(world["ws"], world["outcome"], due["reasons"], due["seen"], due["decisions"]))

    assert row.role == "lead" and row.harness == "control_plane" and row.status == "succeeded", row.error
    assert row.model == "lead-model" and row.provider == "mindshub" and row.turns == 2
    assert row.cost_known and row.cost_usd == round(2 * (1000 * 1 + 200 * 2) / 1e6, 6)
    assert row.result["summary"] == "Answered T1's question; merge needs you."
    assert [a["tool"] for a in row.result["actions"]] == ["resolve_decision", "note_for_people", "post_update"]
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
        assert guidance.attribution == {"source": "lead", "run_id": row.id}  # attributed in the same transaction
        resolved = s.query(Event).filter_by(kind="decision_resolved").one()
        assert resolved.data["source"] == "lead" and resolved.data["run_id"] == row.id
        note = next(m for m in s.query(Message).filter_by(author_kind="lead") if (m.attribution or {}).get("decision_id"))
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
    for bad in ({"run_usd": 50}, {"turns": 1}, {"enabled": "yes"}, {"model": "x"}, "on"):
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


class MeteredModel(ScriptedModel):
    """MindsHub reporting what the run's session has cost so far, one reading per call."""
    readings: list = []

    async def session_cost(self, _session_id):
        return MeteredModel.readings.pop(0) if MeteredModel.readings else None


def unpriced(world):
    with db.session_scope() as s:
        w = s.get(Workspace, world["ws"])
        w.settings = {**w.settings, "pricing": {}}


def busy(messages):
    return {"content": "", "tool_calls": [call("outcome_record", {}, step(messages))]}


def test_without_a_price_mindshubs_estimate_caps_the_run(world, model, monkeypatch):
    unpriced(world)
    monkeypatch.setattr(lead, "ModelClient", MeteredModel)
    model.scripts = {"lead": busy}
    MeteredModel.readings = [0.5, 2.5]
    row = asyncio.run(lead.run_lead(world["ws"], world["outcome"], ["test"], {}))
    assert row.status == "failed" and row.error == "The Lead's budget of $2.00 is spent" and row.turns == 2
    MeteredModel.readings = []  # no reading: stop rather than spend blind
    row = asyncio.run(lead.run_lead(world["ws"], world["outcome"], ["test"], {}))
    assert row.status == "failed" and "couldn't be measured" in row.error and row.turns == 1


def test_without_a_price_or_a_metered_provider_the_lead_does_not_start(world, model, monkeypatch):
    unpriced(world)
    monkeypatch.setattr(lead, "resolve_provider", lambda ws: ProviderConfig("openai", "unused", "SECRET", "Test"))
    row = asyncio.run(lead.run_lead(world["ws"], world["outcome"], ["test"], {}))
    assert row.status == "failed" and "can't be capped" in row.error and model.calls == []


def test_the_model_follows_the_provider_that_serves_the_run(world, model, monkeypatch):
    with db.session_scope() as s:
        w = s.get(Workspace, world["ws"])
        w.settings = {**w.settings, "models": {"default": "gpt-x"}, "pricing": {"gpt-x": {"input": 1, "output": 1}}}
    monkeypatch.setattr(lead, "resolve_provider", lambda ws: ProviderConfig("openai", "unused", "SECRET", "Test"))
    model.scripts = {"lead": lambda messages: {"content": "", "tool_calls": [call("finish", {"summary": "Done"})]}}
    row = asyncio.run(lead.run_lead(world["ws"], world["outcome"], ["test"], {}))
    assert row.status == "succeeded" and (row.provider, row.model) == ("openai", "gpt-x")  # not MindsHub's Opus


def test_the_leads_guidance_is_not_counted_as_a_persons_intervention(world, client):
    tools = lead.Tools(world["ws"], world["outcome"], "run-1")
    tools.call("post_guidance", {"text": "Prefer the durable queue."})
    assert client.get(f"/api/workspaces/{world['ws']}/stats").json()["human"]["interventions"] == 0
    with db.session_scope() as s:
        s.add(Message(workspace_id=world["ws"], outcome_id=world["outcome"], author_id=world["uid"],
                      author_kind="user", body="Please hurry."))
    assert client.get(f"/api/workspaces/{world['ws']}/stats").json()["human"]["interventions"] == 1


def test_known_spend_alone_can_exhaust_the_outcome_budget(world, model):
    with db.session_scope() as s:
        w = s.get(Workspace, world["ws"])
        w.settings = {**w.settings, "budgets": {**w.settings["budgets"], "outcome_usd": 1}}
        s.add(Run(workspace_id=world["ws"], outcome_id=world["outcome"], role="builder", harness="native",
                  status="succeeded", cost_usd=1.5, cost_known=True))
        s.add(Run(workspace_id=world["ws"], outcome_id=world["outcome"], role="verifier", harness="native",
                  status="succeeded", cost_usd=0.0, cost_known=False))  # still being priced
    row = asyncio.run(lead.run_lead(world["ws"], world["outcome"], ["test"], {}))
    assert row.status == "failed" and "budget is spent" in row.error and model.calls == []


def test_a_decision_on_stopped_work_is_left_alone(world):
    with db.session_scope() as s:
        s.get(Task, world["task"]).status = "cancelled"  # a person stopped T1 while the Lead was working
    tools = lead.Tools(world["ws"], world["outcome"], "run-1")
    out = tools.call("resolve_decision", {"decision_id": world["question"], "option": "answer", "comment": "Use it."})
    assert "no longer applies" in out
    with db.session_scope() as s:
        assert s.get(Decision, world["question"]).status == "open" and s.get(Task, world["task"]).status == "cancelled"
        s.get(Task, world["task"]).status = "needs_input"
        s.get(Outcome, world["outcome"]).status = "cancelled"
    out = tools.call("resolve_decision", {"decision_id": world["question"], "option": "answer", "comment": "Use it."})
    assert "no longer applies" in out


def test_runs_in_progress_hold_their_share_of_the_outcome_budget(world, model):
    def budget(outcome_usd):
        with db.session_scope() as s:
            w = s.get(Workspace, world["ws"])
            w.settings = {**w.settings, "budgets": {**w.settings["budgets"], "outcome_usd": outcome_usd}}

    with db.session_scope() as s:  # a builder still running may spend up to the run cap ($5 here)
        s.add(Run(workspace_id=world["ws"], outcome_id=world["outcome"], role="builder", harness="native",
                  status="running", cost_usd=0.0))
        s.add(Run(workspace_id=world["ws"], outcome_id=world["outcome"], role="verifier", harness="native",
                  status="running", started_at=db.utcnow() - timedelta(hours=5)))  # cut off by a restart: holds nothing
    budget(4)
    row = asyncio.run(lead.run_lead(world["ws"], world["outcome"], ["test"], {}))
    assert row.status == "failed" and "held by runs in progress" in row.error and model.calls == []
    budget(6)
    model.scripts = {"lead": lambda messages: {"content": "", "tool_calls": [call("finish", {"summary": "Done"})]}}
    row = asyncio.run(lead.run_lead(world["ws"], world["outcome"], ["test"], {}))
    assert row.status == "succeeded" and row.config_snapshot["budget_reserved"] == 1.0  # $6 less the builder's $5


def test_lead_turns_counts_every_model_call_including_the_summary(world, model):
    with db.session_scope() as s:
        w = s.get(Workspace, world["ws"])
        w.settings = {**w.settings, "lead": {"enabled": True, "run_usd": 2.0, "turns": 2}}
    model.scripts = {"lead": busy}
    row = asyncio.run(lead.run_lead(world["ws"], world["outcome"], ["test"], {}))
    assert row.status == "succeeded" and row.turns == 2 and model.calls == ["lead", "lead"]


def test_a_failed_final_cost_reading_keeps_the_last_one(world, model, monkeypatch):
    unpriced(world)
    monkeypatch.setattr(lead, "ModelClient", MeteredModel)
    model.scripts = {"lead": lambda messages: busy(messages) if step(messages) == 0
                     else {"content": "", "tool_calls": [call("finish", {"summary": "Done"})]}}
    MeteredModel.readings = [0.5, 0.7]  # read after each call; the final read after the run finds nothing
    row = asyncio.run(lead.run_lead(world["ws"], world["outcome"], ["test"], {}))
    assert row.status == "succeeded" and row.cost_usd == 0.7 and row.cost_known is False


def test_every_admission_counts_what_a_running_lead_has_reserved(world):
    from aifactory.pipeline.planning import _over_budget
    from aifactory.pipeline.runtime import outcome_committed

    with db.session_scope() as s:
        s.add(Run(workspace_id=world["ws"], outcome_id=world["outcome"], role="builder", harness="native",
                  status="succeeded", cost_usd=1.0, cost_known=True))
        s.add(Run(workspace_id=world["ws"], outcome_id=world["outcome"], role="lead", harness="control_plane",
                  status="running", cost_usd=0.5, config_snapshot={"budget_reserved": 2.0}))
        s.add(Run(workspace_id=world["ws"], outcome_id=world["outcome"], role="lead", harness="control_plane",
                  status="succeeded", cost_usd=0.25, config_snapshot={"budget_reserved": 2.0}))  # finished: holds nothing
    assert outcome_committed(world["outcome"]) == (1.0 + 0.5 + 0.25 + 1.5, True)  # spent, plus $1.50 still held
    settings = {"budgets": {"outcome_usd": 3.0}}
    assert "reached its $3 budget" in _over_budget(world["outcome"], settings)  # planning can't take the Lead's share
    assert _over_budget(world["outcome"], {"budgets": {"outcome_usd": 4.0}}) == ""


def test_a_lead_always_has_a_turn_to_act(world):
    assert lead.settings_for({"lead": {"turns": 1}})["turns"] == 2  # one to act, and the summary


class CapturingModel(ScriptedModel):
    """Records each request's output limit."""
    limits: list = []

    async def chat(self, *, model, messages, tools=None, ledger=None, **kwargs):
        CapturingModel.limits.append(kwargs.get("max_tokens"))
        return await super().chat(model=model, messages=messages, tools=tools, ledger=ledger, **kwargs)


def lead_settings(world, **values):
    with db.session_scope() as s:
        w = s.get(Workspace, world["ws"])
        w.settings = {**w.settings, **values}


def test_with_a_price_each_request_is_sized_to_the_dollars_left(world, model, monkeypatch):
    monkeypatch.setattr(lead, "ModelClient", CapturingModel)
    CapturingModel.limits = []
    lead_settings(world, pricing={"lead-model": {"input": 0, "output": 1000}})  # $1 per 1,000 output tokens
    model.scripts = {"lead": lambda messages: {"content": "", "tool_calls": [call("finish", {"summary": "Done"})]}}
    row = asyncio.run(lead.run_lead(world["ws"], world["outcome"], ["test"], {}))
    assert row.status == "succeeded" and CapturingModel.limits == [2000]  # $2 left buys 2,000 output tokens
    lead_settings(world, lead={"enabled": True, "run_usd": 0.0003, "turns": 30})  # less than one useful reply
    CapturingModel.limits = []
    row = asyncio.run(lead.run_lead(world["ws"], world["outcome"], ["test"], {}))
    assert row.status == "failed" and "can't cover another request" in row.error and CapturingModel.limits == []


def test_without_a_price_the_lead_needs_room_for_a_request(world, model, monkeypatch):
    unpriced(world)
    monkeypatch.setattr(lead, "ModelClient", MeteredModel)
    lead_settings(world, lead={"enabled": True, "run_usd": 0.1, "turns": 30})
    row = asyncio.run(lead.run_lead(world["ws"], world["outcome"], ["test"], {}))
    assert row.status == "failed" and "needs at least $0.25" in row.error and model.calls == []
    lead_settings(world, lead={"enabled": True, "run_usd": 2.0, "turns": 30})
    model.scripts = {"lead": busy}
    MeteredModel.readings = [1.7]  # $0.30 of $2 left after the first request: too little for another
    row = asyncio.run(lead.run_lead(world["ws"], world["outcome"], ["test"], {}))
    assert row.status == "failed" and "nearly spent" in row.error and row.turns == 1


def test_a_request_that_fails_before_its_usage_is_recorded_leaves_the_cost_unknown(world, model, monkeypatch):
    from aifactory.inference import InferenceError

    class Down(ScriptedModel):
        async def chat(self, **_kwargs):
            raise InferenceError("Gateway timeout", status=504)

    monkeypatch.setattr(lead, "ModelClient", Down)
    row = asyncio.run(lead.run_lead(world["ws"], world["outcome"], ["test"], {}))
    assert row.status == "failed" and "status 504" in row.error
    assert row.cost_known is False  # it may have been billed, and MindsHub couldn't say


def test_a_lead_that_never_uses_its_tools_fails_so_it_is_retried(world, model):
    model.scripts = {"lead": lambda messages: {"content": "I think it's fine."}}
    row = asyncio.run(lead.run_lead(world["ws"], world["outcome"], ["test"], {}))
    assert row.status == "failed" and "without using its tools" in row.error


def test_a_spent_outcome_does_not_wake_its_lead(world):
    assert world["outcome"] in woken()
    with db.session_scope() as s:
        w = s.get(Workspace, world["ws"])
        w.settings = {**w.settings, "budgets": {**w.settings["budgets"], "outcome_usd": 1}}
        s.add(Run(workspace_id=world["ws"], outcome_id=world["outcome"], role="builder", harness="native",
                  status="succeeded", cost_usd=1.5, cost_known=True))
    assert world["outcome"] not in woken()  # people raise the budget or continue it; until then the Lead stays out
    with db.session_scope() as s:
        w = s.get(Workspace, world["ws"])
        w.settings = {**w.settings, "budgets": {**w.settings["budgets"], "outcome_usd": 5}}
    assert world["outcome"] in woken()  # a raised budget wakes it again


def test_a_person_letting_a_spent_outcome_continue_lets_its_lead_work(world, model):
    with db.session_scope() as s:
        w = s.get(Workspace, world["ws"])
        w.settings = {**w.settings, "budgets": {**w.settings["budgets"], "outcome_usd": 1}}
        s.add(Run(workspace_id=world["ws"], outcome_id=world["outcome"], role="builder", harness="native",
                  status="succeeded", cost_usd=1.5, cost_known=True))
        s.add(Decision(workspace_id=world["ws"], outcome_id=world["outcome"], task_id=world["task"], kind="budget",
                       title="Outcome #7 reached its $1 budget", status="resolved", resolution="abandon",
                       resolved_by=world["uid"], options=[{"value": "retry"}, {"value": "abandon"}]))
    assert world["outcome"] not in woken()  # stopping isn't permission to continue
    with db.session_scope() as s:
        s.add(Decision(workspace_id=world["ws"], outcome_id=world["outcome"], task_id=world["task"], kind="budget",
                       title="Outcome #7 reached its $1 budget", status="resolved", resolution="retry",
                       resolved_by=world["uid"], options=[{"value": "retry"}, {"value": "abandon"}]))
    assert world["outcome"] in woken()  # "Continue anyway": the budget no longer stops its Lead
    model.scripts = {"lead": lambda messages: {"content": "", "tool_calls": [call("finish", {"summary": "Done"})]}}
    row = asyncio.run(lead.run_lead(world["ws"], world["outcome"], ["test"], {}))
    assert row.status == "succeeded" and row.config_snapshot["budget_reserved"] == 2.0  # only its own cap applies


def test_a_cut_off_lead_run_holds_no_budget(world):
    from aifactory.pipeline.runtime import outcome_committed

    with db.session_scope() as s:
        w = s.get(Workspace, world["ws"])
        w.settings = {**w.settings, "budgets": {**w.settings["budgets"], "outcome_usd": 2}}
        s.add(Run(workspace_id=world["ws"], outcome_id=world["outcome"], role="lead", harness="control_plane",
                  status="running", started_at=db.utcnow() - timedelta(hours=1),
                  config_snapshot={"budget_reserved": 2.0}))  # crashed holding the whole budget
    assert outcome_committed(world["outcome"]) == (0.0, True)  # stale: it holds nothing
    assert world["outcome"] in woken()  # so its Lead can run again


def test_the_inbox_shows_the_leads_recommendation_on_its_decision(world, client):
    tools = lead.Tools(world["ws"], world["outcome"], "run-1")
    tools.call("note_for_people", {"decision_id": world["merge"], "text": "Earlier view: wait for CI."})
    tools.call("note_for_people", {"decision_id": world["merge"], "text": "CI and the verifier passed. Merge it."})
    tools.call("note_for_people", {"text": "A note about the outcome, not a decision."})
    inbox = {d["id"]: d for d in client.get(f"/api/workspaces/{world['ws']}/inbox").json()["decisions"]}
    note = inbox[world["merge"]]["lead_note"]
    assert note["text"] == "CI and the verifier passed. Merge it." and note["run_id"] == "run-1" and note["created_at"]
    assert inbox[world["question"]]["lead_note"] is None


def milestone(world, kind, ago=timedelta(0), message="", task=True):
    with db.session_scope() as s:
        event = Event(workspace_id=world["ws"], outcome_id=world["outcome"], task_id=world["task"] if task else None,
                      kind=kind, message=message or kind, created_at=db.utcnow() - ago)
        s.add(event)
        s.flush()
        return event.id


def digests(world):
    with db.session_scope() as s:
        return [(m.body, m.attribution["event_id"]) for m in s.query(Message).filter(
            Message.outcome_id == world["outcome"], Message.author_kind == "factory").order_by(Message.created_at)]


def test_progress_alone_is_told_as_a_digest_and_does_not_wake_the_lead(world):
    lead_run(world, woken()[world["outcome"]]["seen"], ago=timedelta(minutes=5))
    published = milestone(world, "published", message="Opened PR #12")
    milestone(world, "stage", message="Stage: ci")  # not a milestone
    assert world["outcome"] not in woken()  # nothing to judge: no Lead run, whatever the time since its last one
    assert digests(world) == [("Progress since the last update:\n- T1: Opened PR #12", published)]
    with db.session_scope() as s:
        assert s.query(Run).filter(Run.role == "lead").count() == 1  # the one before the progress
    merged = milestone(world, "merged", message="T1 merged as PR #12")
    assert world["outcome"] not in woken() and len(digests(world)) == 1  # at most one digest every half hour
    with db.session_scope() as s:
        s.query(Event).filter(Event.kind == "progress_digest").one().created_at = db.utcnow() - timedelta(minutes=31)
    assert world["outcome"] not in woken()
    # Only what the last digest had not told, and a task's key is not repeated in front of a message that has it.
    assert digests(world)[1] == ("Progress since the last update:\n- T1 merged as PR #12", merged)
    assert world["outcome"] not in woken() and len(digests(world)) == 2  # told once


def test_progress_is_still_told_when_the_lead_has_used_its_runs_for_the_day(world):
    seen = woken()[world["outcome"]]["seen"]
    for n in range(lead.DAILY_RUNS):
        lead_run(world, seen, ago=timedelta(minutes=5 + n))
    published = milestone(world, "published", message="Opened PR #12")
    with db.session_scope() as s:  # a person's post would wake the Lead, had it a run left today
        s.add(Message(workspace_id=world["ws"], outcome_id=world["outcome"], author_id=world["uid"],
                      author_kind="user", body="How is it going?"))
    assert world["outcome"] not in woken()
    assert digests(world) == [("Progress since the last update:\n- T1: Opened PR #12", published)]


def test_a_lead_that_something_else_woke_reports_the_progress_too(world):
    lead_run(world, woken()[world["outcome"]]["seen"], ago=timedelta(minutes=5))
    published = milestone(world, "published", message="Opened PR #12")
    with db.session_scope() as s:
        s.add(Message(workspace_id=world["ws"], outcome_id=world["outcome"], author_id=world["uid"],
                      author_kind="user", body="How is it going?"))
    due = woken()[world["outcome"]]
    assert due["reasons"] == ["1 new post from people", "1 step of progress to report"]
    assert due["seen"]["update_due"] is True and due["seen"]["event_id"] == published and digests(world) == []
    lead_run(world, due["seen"], ago=timedelta(minutes=2))
    assert world["outcome"] not in woken() and digests(world) == []  # its update covered the step: no digest of it


def test_a_finished_outcome_reports_how_it_ended_within_the_hour(world):
    lead_run(world, woken()[world["outcome"]]["seen"], ago=timedelta(minutes=20))
    with db.session_scope() as s:
        o = s.get(Outcome, world["outcome"])
        o.status, o.completed_at = "completed", db.utcnow() - timedelta(minutes=5)
    milestone(world, "outcome_completed", task=False)
    due = woken()[world["outcome"]]
    assert due["reasons"] == ["the outcome has ended (completed)"]  # its open decisions don't count
    assert due["seen"]["update_due"] is True
    assert due["seen"]["closing"] is True
    lead_run(world, {**due["seen"], "closing": False}, ago=timedelta(minutes=2))  # a progress update after the end
    with db.session_scope() as s:
        s.query(Run).filter(Run.started_at > db.utcnow() - timedelta(minutes=3)).one().started_at = db.utcnow() - timedelta(minutes=3)
    assert world["outcome"] in woken()  # isn't the closing report
    lead_run(world, due["seen"], ago=timedelta(minutes=1, seconds=30))
    assert world["outcome"] not in woken()  # a run woken for the ending reported it
    with db.session_scope() as s:
        s.query(Run).delete()
        s.get(Outcome, world["outcome"]).completed_at = db.utcnow() - timedelta(hours=2)
    assert world["outcome"] not in woken()  # too long ago


def test_a_cancelled_outcome_reports_how_it_ended(world):
    lead_run(world, woken()[world["outcome"]]["seen"], ago=timedelta(minutes=20))
    with db.session_scope() as s:  # cancelling records no completion time: when it ended is its last update
        o = s.get(Outcome, world["outcome"])
        o.status, o.updated_at = "cancelled", db.utcnow() - timedelta(minutes=3)
    assert woken()[world["outcome"]]["reasons"] == ["the outcome has ended (cancelled)"]


def test_a_run_woken_to_report_fails_if_it_posts_no_update(world, model):
    model.scripts = {"lead": lambda messages: {"content": "", "tool_calls": [call("finish", {"summary": "Nothing to add"})]}}
    due = woken()[world["outcome"]]  # a new outcome: the Lead introduces itself
    row = asyncio.run(lead.run_lead(world["ws"], world["outcome"], due["reasons"], due["seen"], due["decisions"]))
    assert row.status == "failed" and "didn't post the update" in row.error
    with db.session_scope() as s:
        s.query(Run).delete()
    assert "you've just joined this outcome" in woken()[world["outcome"]]["reasons"]  # still owed


def test_post_update_adds_to_the_outcomes_conversation(world):
    tools = lead.Tools(world["ws"], world["outcome"], "run-1")
    assert "Posted" in tools.call("post_update", {"text": "T1 is open as PR #12; Codex is reviewing it."})
    with db.session_scope() as s:
        update = s.query(Message).one()
        assert update.author_kind == "lead" and update.attribution == {"source": "lead", "run_id": "run-1",
                                                                      "kind": "update"}
        assert s.query(Event).filter_by(kind="lead_update").one().data["source"] == "lead"


def test_the_outcome_reads_as_a_conversation(world, client):
    base = db.utcnow() - timedelta(hours=1)
    with db.session_scope() as s:
        s.get(Outcome, world["outcome"]).created_at = base
        s.get(Decision, world["question"]).created_at = base + timedelta(minutes=5)
        s.get(Decision, world["merge"]).created_at = base + timedelta(minutes=6)
        s.add_all([
            Event(workspace_id=world["ws"], outcome_id=world["outcome"], kind="plan_approved",
                  message="Plan approved by the plan reviewer", created_at=base + timedelta(minutes=1)),
            Event(workspace_id=world["ws"], outcome_id=world["outcome"], task_id=world["task"], kind="run_finished",
                  message="Builder succeeded", data={"role": "builder", "cost_usd": 0.38},
                  created_at=base + timedelta(minutes=2)),
            Event(workspace_id=world["ws"], outcome_id=world["outcome"], kind="run_finished", message="Lead finished",
                  data={"role": "lead"}, created_at=base + timedelta(minutes=2, seconds=30)),
            Event(workspace_id=world["ws"], outcome_id=world["outcome"], task_id=world["task"], kind="agent_thought",
                  message="thinking", created_at=base + timedelta(minutes=2, seconds=40)),
        ])
    tools = lead.Tools(world["ws"], world["outcome"], "run-1")
    tools.call("post_update", {"text": "The plan is approved and T1 is built."})
    tools.call("post_guidance", {"text": "Prefer the durable queue.", "task": "T1"})
    tools.call("note_for_people", {"decision_id": world["merge"], "text": "Ready to merge."})
    with db.session_scope() as s:
        for m, minutes in zip(s.query(Message).order_by(Message.created_at), (3, 4, 7)):
            m.created_at = base + timedelta(minutes=minutes)
        s.add(Message(workspace_id=world["ws"], outcome_id=world["outcome"], author_id=world["uid"], author_kind="user",
                      body="Why is the merge waiting?", created_at=base + timedelta(minutes=8)))
        s.add(Event(workspace_id=world["ws"], outcome_id=world["outcome"], task_id=world["task"], kind="published",
                    message="Opened PR #12", data={"url": "https://github.com/team/app/pull/12"},
                    created_at=base + timedelta(minutes=9)))
        s.add(Run(workspace_id=world["ws"], outcome_id=world["outcome"], role="lead", harness="control_plane",
                  status="succeeded", model="opus-5-5", cost_usd=0.21, started_at=base + timedelta(minutes=3)))

    r = client.get(f"/api/workspaces/{world['ws']}/outcomes/{world['outcome']}/conversation")
    assert r.status_code == 200, r.text
    items = r.json()["items"]
    assert [i["kind"] for i in items] == ["request", "lead", "lead_guidance", "decision", "decision", "lead", "person",
                                          "working"]
    request, update, guidance, question, merge, note, person, working = items
    assert request["title"] == "Search" and request["from"] == "the app"
    assert update["update"] is True and update["text"] == "The plan is approved and T1 is built."
    assert [(s_["who"], s_["text"]) for s_ in update["steps"]] == [
        ("Planner", "Plan approved by the plan reviewer"), ("Builder · T1", "Builder succeeded")]
    assert update["steps"][1]["cost_usd"] == 0.38  # the Lead's own run and agent thoughts aren't steps
    assert guidance["task_key"] == "T1" and guidance["text"] == "Prefer the durable queue."
    assert question["decision"]["id"] == world["question"] and question["decision"]["lead_note"] is None
    assert merge["decision"]["lead_note"]["text"] == "Ready to merge." and merge["decision"]["task_key"] == "T1"
    assert note["update"] is False and note["decision_id"] == world["merge"] and note["steps"] == []
    assert person["author"] == "Ian" and person["operator"] is False
    assert [s_["text"] for s_ in working["steps"]] == ["Opened PR #12"]
    assert working["steps"][0]["url"] == "https://github.com/team/app/pull/12"  # evidence, one click away
    assert r.json()["lead"] == {"model": "opus-5-5", "running": False, "last_active": r.json()["lead"]["last_active"],
                                "cost_usd": 0.21, "cost_known": True}  # the Lead's own spend, its own line

    with db.session_scope() as s:
        other = Workspace(org_id=s.get(Workspace, world["ws"]).org_id, name="Other", slug="other")
        s.add(other)
        s.flush()
        elsewhere = Outcome(workspace_id=other.id, number=1, title="Elsewhere")
        s.add(elsewhere)
        s.flush()
        elsewhere_id = elsewhere.id
    assert client.get(f"/api/workspaces/{world['ws']}/outcomes/{elsewhere_id}/conversation").status_code == 404


def test_a_mind_conversation_that_became_the_outcome_continues_with_its_lead(world):
    from aifactory.conversation import conversation
    from aifactory.mind.api import thread_view
    from aifactory.models import MindAction, MindThread

    with db.session_scope() as s:
        thread = MindThread(owner_id=world["uid"], billing_workspace_id=world["ws"], workspace_ids=[world["ws"]])
        s.add(thread)
        s.flush()
        s.add(MindAction(thread_id=thread.id, owner_id=world["uid"], workspace_id=world["ws"], kind="create_outcome",
                         status="pending"))  # a card not yet confirmed hands nothing off
        s.flush()
        assert thread_view(s, thread)["outcome"] is None
        s.add(MindAction(thread_id=thread.id, owner_id=world["uid"], workspace_id=world["ws"], kind="create_outcome",
                         status="succeeded", result={"ok": True, "outcome_id": world["outcome"], "number": 7}))
        s.flush()
        handed = thread_view(s, thread)["outcome"]
        assert (handed["id"], handed["number"], handed["title"]) == (world["outcome"], 7, "Search")
        o = s.get(Outcome, world["outcome"])
        mine = conversation(s, o, world["uid"])
        assert mine["started_in_mind"] == {"thread_id": thread.id}  # its owner gets the way back
        assert mine["items"][0]["from"] == "a Mind conversation"
        assert conversation(s, o, "someone-else")["started_in_mind"] == {"thread_id": None}  # private before hand-off
        other = Outcome(workspace_id=world["ws"], number=8, title="Unrelated", status="running")
        s.add(other)
        s.flush()
        assert conversation(s, other, world["uid"])["started_in_mind"] is None


def test_a_spent_outcome_that_just_finished_still_posts_its_closing_update(world, model):
    with db.session_scope() as s:
        w = s.get(Workspace, world["ws"])
        w.settings = {**w.settings, "budgets": {**w.settings["budgets"], "outcome_usd": 1}}
        s.add(Run(workspace_id=world["ws"], outcome_id=world["outcome"], role="builder", harness="native",
                  status="succeeded", cost_usd=1.5, cost_known=True))
        o = s.get(Outcome, world["outcome"])
        o.status, o.completed_at = "completed", db.utcnow() - timedelta(minutes=5)
    milestone(world, "outcome_completed", task=False)
    assert world["outcome"] in woken()  # the budget stops work, not the closing report
    model.scripts = {"lead": lambda messages: {"content": "", "tool_calls": [call("finish", {"summary": "Done"})]}}
    row = asyncio.run(lead.run_lead(world["ws"], world["outcome"], ["the outcome has ended"], {}))
    assert row.status == "succeeded"  # within the Lead's own run cap
