"""The Director: off by default; when on, it decides what waits on a person, within its limits."""
from __future__ import annotations

import asyncio
from datetime import timedelta

import pytest

from aifactory import db
from aifactory.inference import ProviderConfig
from aifactory.models import Alert, Decision, Event, Outcome, Plan, Repository, Run, Signal, Task, User, Workspace
from aifactory.pipeline import director
from aifactory.pipeline.holds import open_provider_hold
from tests.fakes import ScriptedModel, call, step

ON = {"mode": "on", "run_usd": 4.0, "daily_usd": 40.0, "max_outcome_usd": 120.0, "after_minutes": 5}


@pytest.fixture
def world(signed_in):
    ws, uid = signed_in["workspaces"][0]["id"], signed_in["user"]["id"]
    old = db.utcnow() - timedelta(minutes=10)
    with db.session_scope() as s:
        w = s.get(Workspace, ws)
        w.settings = {**(w.settings or {}), "models": {"lead": "lead-model"},
                      "pricing": {"lead-model": {"input": 1, "output": 2}},
                      "budgets": {"run_usd": 5, "outcome_usd": 40, "run_tokens": 1_000_000}, "director": dict(ON)}
        repo = Repository(workspace_id=ws, full_name="team/app")
        o = Outcome(workspace_id=ws, number=7, title="Search", brief="Add search", status="running")
        s.add_all([repo, o])
        s.flush()
        o.repo_ids = [repo.id]
        plan = Plan(outcome_id=o.id, status="approved", content={"tasks": [{"key": "T1"}, {"key": "T2"}]})
        s.add(plan)
        s.flush()
        t1 = Task(workspace_id=ws, outcome_id=o.id, plan_id=plan.id, repo_id=repo.id, key="T1", title="Endpoint",
                  status="awaiting_approval", stage="merge", pr_number=12)
        t2 = Task(workspace_id=ws, outcome_id=o.id, plan_id=plan.id, repo_id=repo.id, key="T2", title="Index",
                  status="failed", stage="build", attempt=3)
        s.add_all([t1, t2])
        s.flush()
        merge = Decision(workspace_id=ws, outcome_id=o.id, task_id=t1.id, kind="approve_merge", title="Merge PR #12?",
                         options=[{"value": "approve", "label": "Merge"},
                                  {"value": "reject", "label": "Request changes"}], created_at=old)
        stuck = Decision(workspace_id=ws, outcome_id=o.id, task_id=t2.id, kind="unblock", title="T2 did not pass",
                         options=[{"value": "retry", "label": "Retry"}, {"value": "abandon", "label": "Abandon"},
                                  {"value": "take_over", "label": "Take over"}], created_at=old)
        fresh = Decision(workspace_id=ws, outcome_id=o.id, task_id=t2.id, kind="question", title="Which index?",
                         options=[{"value": "answer", "label": "Answer"}])
        smoke = Signal(workspace_id=ws, repo_id=repo.id, source="smoke_failure", fingerprint="smoke-1",
                       title="Smoke failed after merging Search API", body="`pytest -k smoke` failed",
                       url="https://github.com/team/app/pull/9")
        alert = Alert(workspace_id=ws, outcome_id=o.id, kind="stalled", key=f"stalled:{o.id}",
                      title="Nothing is moving", status="open", opened_at=old)
        s.add_all([merge, stuck, fresh, smoke, alert])
        s.flush()
        return {"ws": ws, "uid": uid, "outcome": o.id, "t1": t1.id, "t2": t2.id, "merge": merge.id,
                "stuck": stuck.id, "fresh": fresh.id, "smoke": smoke.id, "alert": alert.id, "repo": repo.id}


def woken(now=None):
    return {d["workspace_id"]: d for d in director.due(now=now)}


def past_run(world, *, ago=timedelta(minutes=5), status="succeeded", seen=None, cost=0.1, known=True):
    """A Director run recorded as its two events, started ``ago``."""
    at = db.utcnow() - ago
    run_id = f"run{at.timestamp()}"
    with db.session_scope() as s:
        s.add(Event(workspace_id=world["ws"], kind="director_run_started", created_at=at,
                    data={"run_id": run_id, "reserved_usd": 4.0}))
        if status is not None:
            s.add(Event(workspace_id=world["ws"], kind="director_run_finished", created_at=at + timedelta(seconds=30),
                        data={"run_id": run_id, "status": status, "cost_usd": cost, "cost_known": known,
                              **({"seen": seen} if seen is not None else {})}))


def test_off_by_default_and_its_settings_are_validated(client, signed_in):
    ws = signed_in["workspaces"][0]["id"]
    settings = next(w for w in client.get("/api/me").json()["workspaces"] if w["id"] == ws)["settings"]
    assert settings["director"] == {**ON, "mode": "off"}
    with db.session_scope() as s:
        s.add(Decision(workspace_id=ws, kind="question", title="Q?", options=[{"value": "answer", "label": "Answer"}],
                       created_at=db.utcnow() - timedelta(hours=1)))
    assert director.due() == []  # off: decisions wait for a person

    r = client.patch(f"/api/workspaces/{ws}", json={"settings": {"director": {"mode": "on", "daily_usd": 25}}})
    assert r.status_code == 200, r.text
    assert r.json()["settings"]["director"] == {**ON, "daily_usd": 25.0}
    for bad in ({"mode": "maybe"}, {"run_usd": 50}, {"daily_usd": -1}, {"max_outcome_usd": "lots"},
                {"after_minutes": 1.5}, {"model": "x"}, "on"):
        assert client.patch(f"/api/workspaces/{ws}", json={"settings": {"director": bad}}).status_code == 422
    with db.session_scope() as s:  # the refused changes left the saved settings alone
        assert s.get(Workspace, ws).settings["director"] == {**ON, "daily_usd": 25.0}
    assert [d["workspace_id"] for d in director.due()] == [ws]


def test_new_items_wake_it_once_the_judge_and_lead_have_had_their_turn(world):
    due = woken()[world["ws"]]
    keys = {f"d:{world['merge']}", f"d:{world['stuck']}", f"s:{world['smoke']}", f"a:{world['alert']}"}
    assert set(due["new"]) == keys  # the question is too new: its judge and Lead go first
    assert due["reasons"] == ["2 new decisions waiting on a person", "1 new alert waiting on a person",
                              "1 new signal waiting on a person"]

    with db.session_scope() as s:  # while the outcome's Lead runs, its decisions are the Lead's
        s.add(Run(workspace_id=world["ws"], outcome_id=world["outcome"], role="lead", harness="control_plane",
                  status="running", started_at=db.utcnow() - timedelta(minutes=1)))
    due = woken()[world["ws"]]
    assert set(due["new"]) == {f"s:{world['smoke']}", f"a:{world['alert']}"}
    assert {f"d:{world['merge']}", f"d:{world['stuck']}"} <= set(due["marks"])  # remembered, not forgotten

    past_run(world, seen={"marks": due["marks"], "skips": {}})
    assert world["ws"] not in woken()  # nothing new since its last run

    with db.session_scope() as s:  # a changed question wakes it again
        s.query(Run).delete()
        s.get(Decision, world["stuck"]).body = "T2 failed its checks three times."
    assert woken()[world["ws"]]["new"] == [f"d:{world['stuck']}"]
    assert world["ws"] not in director.due(running=frozenset({world["ws"]}))  # one run per workspace


def test_it_waits_its_turn_and_stays_within_its_daily_spend(world):
    past_run(world, ago=timedelta(seconds=30))
    assert world["ws"] not in woken()  # cooldown after its last run
    with db.session_scope() as s:
        s.query(Event).filter(Event.kind.like("director_run%")).delete()
    past_run(world, ago=timedelta(minutes=5), status="failed")
    assert world["ws"] not in woken()  # a failed run waits longer before trying again
    with db.session_scope() as s:
        s.query(Event).filter(Event.kind.like("director_run%")).delete()
    past_run(world, ago=timedelta(minutes=20), status=None)
    assert world["ws"] not in woken()  # a run started without finishing may still be going on another instance
    with db.session_scope() as s:
        s.query(Event).filter(Event.kind.like("director_run%")).delete()
    past_run(world, ago=timedelta(minutes=35), status=None)
    assert world["ws"] in woken()  # one a restart cut off doesn't block it for long
    assert woken()[world["ws"]]["usd"] == 4.0
    past_run(world, ago=timedelta(hours=2), cost=34.0)
    assert woken()[world["ws"]]["usd"] == pytest.approx(40.0 - 34.0 - 4.0)  # the cut-off run counts what it reserved
    past_run(world, ago=timedelta(hours=3), cost=1.0, known=False)
    assert world["ws"] not in woken()  # an unknown cost counts what it reserved: under $0.25 is left today
    past_run(world, ago=timedelta(days=2), cost=500.0)  # older than a day: no longer counted
    with db.session_scope() as s:
        s.query(Event).filter(Event.kind.like("director_run%"), Event.created_at > db.utcnow() - timedelta(days=1)) \
            .delete(synchronize_session=False)
    assert world["ws"] in woken()


def test_a_provider_hold_and_taking_work_over_stay_with_people(world):
    with db.session_scope() as s:
        cfg = director.settings_for(s.get(Workspace, world["ws"]).settings)
        assert director.choices(s, s.get(Decision, world["stuck"]), cfg) == (
            ["retry", "abandon"], "Taking work over is left to people")
    open_provider_hold(world["ws"], "402 Payment Required", "a test")
    assert world["ws"] not in woken()  # no model call can succeed until a person lifts it
    with db.session_scope() as s:
        hold = s.query(Decision).filter_by(kind="provider_hold").one()
        allowed, why = director.choices(s, hold, cfg)
        assert allowed == [] and "billing" in why


def spend(world, usd):
    """Recorded and committed, so budget checks (in their own sessions) count it."""
    with db.session_scope() as s:
        s.add(Run(workspace_id=world["ws"], outcome_id=world["outcome"], role="builder", harness="native",
                  status="succeeded", cost_usd=usd, cost_known=True))


def test_two_scheduler_instances_never_both_start_a_run(world):
    item = woken()[world["ws"]]
    first = director.claim(item)
    assert first is not None and first[1] == 4.0
    assert director.claim(item) is None  # the first run is going: the other instance stands down
    assert world["ws"] not in woken()


def test_a_claim_uses_the_per_run_cap_saved_since_due(world):
    item = woken()[world["ws"]]
    with db.session_scope() as s:
        w = s.get(Workspace, world["ws"])
        w.settings = {**w.settings, "director": {**ON, "run_usd": 1.5}}
    assert director.claim(item)[1] == 1.5


def test_triage_creates_one_outcome_and_the_director_never_overrides_a_persons_triage(world):
    from aifactory.signals import triage

    with db.session_scope() as s:
        person = s.get(User, world["uid"])
        triage(s, s.get(Signal, world["smoke"]), person, "accept")
    with db.session_scope() as s:
        sig = s.get(Signal, world["smoke"])
        first = sig.outcome_id
        triage(s, sig, s.get(User, world["uid"]), "accept")  # accepting again creates nothing
        assert sig.outcome_id == first and s.query(Outcome).filter_by(signal_id=sig.id).count() == 1
        with pytest.raises(ValueError, match="triaged this signal already"):
            triage(s, sig, director.director_user(s), "ignore", pending_only=True)


@pytest.mark.parametrize("change", ["paused", "off", "hold"])
def test_a_claim_rechecks_what_may_have_changed_since_due(world, change):
    item = woken()[world["ws"]]
    if change == "hold":
        open_provider_hold(world["ws"], "402 Payment Required", "a test")
    else:
        with db.session_scope() as s:
            w = s.get(Workspace, world["ws"])
            if change == "paused":
                w.paused = True
            else:
                w.settings = {**w.settings, "director": {**ON, "mode": "off"}}
    assert director.claim(item) is None


def test_it_lets_an_outcome_continue_past_its_budget_only_up_to_the_ceiling(world):

    with db.session_scope() as s:
        budget = Decision(workspace_id=world["ws"], outcome_id=world["outcome"], task_id=world["t2"], kind="budget",
                          title="Outcome #7 reached $40", options=[{"value": "retry", "label": "Continue with $10 more"},
                                                                   {"value": "abandon", "label": "Stop"}])
        s.add(budget)
        s.flush()
        budget_id = budget.id
    cfg = director.settings_for({"director": ON})
    spend(world, 100.0)
    with db.session_scope() as s:  # $100 committed plus the $10 extension fits under $120
        assert director.choices(s, s.get(Decision, budget_id), cfg) == (["retry", "abandon"], "")
    spend(world, 15.0)
    with db.session_scope() as s:
        budget = s.get(Decision, budget_id)
        allowed, why = director.choices(s, budget, cfg)
        assert allowed == ["abandon"] and "to $125.00" in why and "ceiling of $120.00" in why
        assert director.choices(s, budget, {**cfg, "max_outcome_usd": 200.0})[0] == ["retry", "abandon"]


def test_continuing_planning_must_fit_a_whole_planning_run_under_the_ceiling(world):
    with db.session_scope() as s:  # planning stopped at the budget: a decision on the outcome, with no task
        budget = Decision(workspace_id=world["ws"], outcome_id=world["outcome"], kind="budget",
                          title="Outcome #7 reached its budget", options=[{"value": "retry", "label": "Continue anyway"},
                                                                          {"value": "cancel", "label": "Cancel"}])
        s.add(budget)
        s.flush()
        budget_id = budget.id
    cfg = director.settings_for({"director": ON})
    spend(world, 90.0)
    with db.session_scope() as s:  # 3 rounds of a planner and a reviewer session at $5 each: $30 fits under $120
        assert director.choices(s, s.get(Decision, budget_id), cfg)[0] == ["retry", "cancel"]
    spend(world, 5.0)
    with db.session_scope() as s:
        allowed, why = director.choices(s, s.get(Decision, budget_id), cfg)
        assert allowed == ["cancel"] and "to $125.00" in why


def test_a_continuation_the_director_granted_holds_only_under_its_ceiling(world):
    from aifactory.pipeline import lead

    def granted(attribution):
        return Decision(workspace_id=world["ws"], outcome_id=world["outcome"], task_id=world["t2"], kind="budget",
                        title="Outcome #7 reached $40", status="resolved", resolution="retry",
                        options=[{"value": "retry", "label": "Continue"}], attribution=attribution)

    with db.session_scope() as s:
        assert not lead.continued_past_budget(s, world["outcome"])
        s.add(granted({"source": "director", "run_id": "r"}))
    with db.session_scope() as s:
        assert lead.continued_past_budget(s, world["outcome"])  # later Lead runs may continue, under the ceiling
    spend(world, 119.0)
    with db.session_scope() as s:  # a Lead run reserves no more than what is left under the ceiling
        run = Run(workspace_id=world["ws"], outcome_id=world["outcome"], role="lead", harness="control_plane",
                  status="running")
        s.add(run)
        s.flush()
        run_id = run.id
    assert lead._reserve(world["outcome"], run_id, 2.0, 40.0, 5.0, timedelta(minutes=60)) == pytest.approx(1.0)
    with db.session_scope() as s:
        s.query(Run).filter_by(id=run_id).delete()
    spend(world, 11.0)
    with db.session_scope() as s:
        assert not lead.continued_past_budget(s, world["outcome"])  # past it, the outcome's budget stops them again
        s.add(granted(None))  # a person's grant has no ceiling
        s.flush()
        assert lead.continued_past_budget(s, world["outcome"])


@pytest.fixture
def model(world, monkeypatch):
    ScriptedModel.calls = []
    monkeypatch.setattr(director, "ModelClient", ScriptedModel)
    monkeypatch.setattr(director, "resolve_provider", lambda ws: ProviderConfig("mindshub", "unused", "SECRET", "Test"))
    return ScriptedModel


def test_a_run_decides_what_waits_and_leaves_the_rest_to_people_with_a_note(world, model):
    requests = []

    def script(messages):
        requests.append(messages)
        if step(messages) == 0:
            return {"content": "", "tool_calls": [call("inbox", {}, 0)]}
        if step(messages) == 1:
            return {"content": "", "tool_calls": [
                call("resolve_decision", {"decision_id": world["merge"], "option": "approve",
                                          "comment": "CI and the verifier passed; no open review findings."}, 1),
                call("resolve_decision", {"decision_id": world["stuck"], "option": "take_over", "comment": "Mine."}, 2),
                call("note_for_people", {"decision_id": world["stuck"],
                                         "text": "T2 keeps failing the same check; a person should look."}, 3),
                call("resolve_decision", {"decision_id": world["fresh"], "option": "answer", "comment": "B-tree."}, 4),
                call("triage_signal", {"signal_id": world["smoke"], "action": "accept",
                                       "reason": "No task handles this smoke failure yet."}, 5),
                call("dismiss_alert", {"alert_id": world["alert"], "reason": "T1 merges now; T2 has a note."}, 6),
                call("post_guidance", {"outcome": "#7", "task": "T2", "text": "Run the index migration first."}, 7)]}
        return {"content": "", "tool_calls": [call("finish", {"summary": "Merged T1; T2 is left to people."})]}

    model.scripts = {"director": script}
    item = woken()[world["ws"]]
    result = asyncio.run(director.run_director(item))

    assert result["status"] == "succeeded", result["reason"]
    assert result["model"] == "lead-model" and result["cost_known"] and result["turns"] == 3
    assert result["cost_usd"] == round(3 * (1000 * 1 + 200 * 2) / 1e6, 6)
    assert [a["tool"] for a in result["actions"]] == ["resolve_decision", "note_for_people", "triage_signal",
                                                      "dismiss_alert", "post_guidance"]
    first = requests[0]
    assert first[0]["content"].startswith("You are the Director") and "UNTRUSTED DATA" in first[0]["content"]
    inbox = next(m["content"] for m in requests[1] if m["role"] == "tool")
    assert "Merge PR #12?" in inbox and "Which index?" not in inbox  # too new for it yet
    outputs = [m["content"] for m in requests[2] if m["role"] == "tool"][1:]
    assert outputs[0].startswith("Applied: approve") and "may not choose 'take_over'" in outputs[1]
    assert outputs[2] == "Noted for people." and "Lead takes this first" in outputs[3]
    assert outputs[4].startswith("Accepted as outcome #") and outputs[5] == "Dismissed."

    with db.session_scope() as s:
        the_director = s.query(User).filter_by(email=director.DIRECTOR_EMAIL).one()
        assert the_director.is_system and the_director.name == "Director"
        merge = s.get(Decision, world["merge"])
        assert merge.status == "resolved" and merge.resolution == "approve" and merge.resolved_by == the_director.id
        assert merge.attribution == {"source": "director", "run_id": result["run_id"]}
        assert s.get(Task, world["t1"]).stage == "merge"
        assert s.get(Decision, world["stuck"]).status == "open" and s.get(Decision, world["fresh"]).status == "open"
        sig = s.get(Signal, world["smoke"])
        assert sig.status == "accepted" and s.get(Outcome, sig.outcome_id).source == "signal"
        alert = s.get(Alert, world["alert"])
        assert alert.status == "dismissed" and alert.dismissed_by == the_director.id
        kinds = [e.kind for e in s.query(Event).filter_by(workspace_id=world["ws"]).order_by(Event.id)]
        kinds = kinds[kinds.index("director_run_started"):]
        assert kinds[-1] == "director_run_finished"
        assert {"decision_resolved", "director_note", "director_triage", "alert_dismissed", "guidance"} <= set(kinds)
        note = s.query(Event).filter_by(kind="director_note").one()
        assert note.data["decision_id"] == world["stuck"] and note.outcome_id == world["outcome"]

    seen = result["seen"]["marks"]
    assert f"d:{world['stuck']}" in seen and result["seen"]["skips"] == {}
    later = db.utcnow() + timedelta(minutes=3)
    assert world["ws"] not in woken(later)  # the noted decision stays seen; nothing else is new
    assert woken(db.utcnow() + timedelta(minutes=6))[world["ws"]]["new"] == [f"d:{world['fresh']}"]  # now it's ripe


@pytest.mark.parametrize("change", ["off", "hold"])
def test_switching_it_off_or_a_provider_hold_stops_a_run_before_its_next_change(world, model, change):
    def script(messages):
        if step(messages) == 0:
            if change == "hold":  # another task's model request was refused while the Director was thinking
                open_provider_hold(world["ws"], "402 Payment Required", "a test")
            else:
                with db.session_scope() as s:  # a person switches it off while the model is thinking
                    w = s.get(Workspace, world["ws"])
                    w.settings = {**w.settings, "director": {**ON, "mode": "off"}}
            return {"content": "", "tool_calls": [
                call("resolve_decision", {"decision_id": world["merge"], "option": "approve", "comment": "Green."})]}
        return {"content": "", "tool_calls": [call("finish", {"summary": "Done."})]}

    model.scripts = {"director": script}
    result = asyncio.run(director.run_director(woken()[world["ws"]]))
    assert result["status"] == "stopped" and "during the run" in result["reason"]
    with db.session_scope() as s:
        assert s.get(Decision, world["merge"]).status == "open"  # decisions wait for a person again


def test_when_two_resolvers_act_at_once_only_the_first_applies(world):
    from aifactory.decisions import DecisionError, resolve

    person, other = db.SessionLocal(), db.SessionLocal()
    try:
        mine, theirs = person.get(Decision, world["merge"]), other.get(Decision, world["merge"])
        assert mine.status == theirs.status == "open"  # both loaded it while it was open
        resolve(person, mine, person.get(User, world["uid"]), "approve")
        with pytest.raises(DecisionError, match="already resolved"):
            resolve(other, theirs, director.director_user(other), "reject", "Not yet.")
    finally:
        person.close()
        other.close()
    with db.session_scope() as s:
        merge = s.get(Decision, world["merge"])
        assert merge.resolution == "approve" and merge.resolved_by == world["uid"]
        assert s.get(Task, world["t1"]).stage == "merge"


def test_an_item_it_passes_over_comes_back_until_it_has_been_skipped_three_times(world):
    item = woken()[world["ws"]]
    tools = director.Tools(world["ws"], "r", ON, set(item["new"]))
    assert director.seen_after(item, tools) == {"marks": {}, "skips": {}}  # shown nothing: nothing counts as skipped
    tools.shown = set(item["new"])
    tools.handled = {f"d:{world['merge']}"}
    seen = director.seen_after(item, tools)
    assert set(seen["marks"]) == {f"d:{world['merge']}"}
    assert seen["skips"] == {key: 1 for key in item["new"] if key != f"d:{world['merge']}"}
    for _ in range(2):
        past_run(world, seen=seen, ago=timedelta(minutes=10))
        item = woken()[world["ws"]]
        tools = director.Tools(world["ws"], "r", ON, set(item["new"]))
        tools.shown = set(item["new"])
        seen = director.seen_after(item, tools)
    assert set(seen["marks"]) == set(item["marks"]) and seen["skips"] == {}
    past_run(world, seen=seen, ago=timedelta(minutes=10))
    assert world["ws"] not in woken()


def test_the_inbox_shows_every_item_a_page_at_a_time_and_each_in_full_on_request(world):
    import json

    with db.session_scope() as s:  # a long backlog, each question longer than the inbox shows
        for n in range(70):
            s.add(Decision(workspace_id=world["ws"], outcome_id=world["outcome"], kind="question",
                           title=f"Question {n}", body=f"Context {n}: " + "x" * 5000,
                           options=[{"value": "answer", "label": "Answer"}],
                           created_at=db.utcnow() - timedelta(minutes=30 + n)))
    item = woken()[world["ws"]]
    tools = director.Tools(world["ws"], "r", ON, set(item["new"]))
    first = json.loads(tools._inbox())
    assert first["waiting"] == {"decisions": 72, "alerts": 1, "signals": 1}
    assert 0 < len(first["items"]) <= director.PAGE_ITEMS and first["not_shown_yet"] == 74 - len(first["items"])
    assert all(len(row.get("body") or "") <= director.BRIEF + 1 for row in first["items"])

    def ids(rows):
        return [r.get("decision_id") or r.get("alert_id") or r.get("signal_id") for r in rows]

    # Even when the run can neither decide nor note what it saw, each call moves on to items not yet shown.
    second = json.loads(tools._inbox())
    unseen = 74 - len(first["items"])
    assert set(ids(second["items"][:unseen])).isdisjoint(ids(first["items"]))
    calls = 2
    while len(tools.shown) < 74:  # every item reaches a page
        before = len(tools.shown)
        tools._inbox()
        calls += 1
        assert len(tools.shown) > before
    assert calls <= 3 and json.loads(tools._inbox())["not_shown_yet"] == 0
    noted = set(ids(first["items"]))  # what it noted goes after what it was shown but left
    tools.handled |= {key for key in tools.shown if key.split(":", 1)[1] in noted}
    assert not set(ids(json.loads(tools._inbox())["items"][:1])) & noted
    with db.session_scope() as s:
        question = s.query(Decision).filter_by(title="Question 0").one().id
    full = json.loads(tools._inbox_item("decision", question))
    assert full["body"] == "Context 0: " + "x" * 5000  # whole: no per-text shortening
    assert tools._inbox_item("alert", "nope") == "No alert with that id in this workspace."


def test_an_inbox_brief_never_carries_part_of_a_credential(world):
    token = "ghp_" + "A" * 36
    with db.session_scope() as s:  # the token straddles the brief's cut: cutting first would leave "ghp_AAAAAA"
        s.get(Decision, world["merge"]).body = "x" * (director.BRIEF - 10) + token + " and more context"
        s.get(Decision, world["stuck"]).title = "y" * 190 + token
    tools = director.Tools(world["ws"], "r", ON, set(woken()[world["ws"]]["new"]))
    page = tools._inbox()
    assert "ghp_" not in page and "[redacted]" in page


def test_the_inbox_and_message_log_show_the_directors_note(world, client):
    with db.session_scope() as s:
        s.add(Event(workspace_id=world["ws"], outcome_id=world["outcome"], kind="director_note",
                    message="Director: T2 needs a person", data={"source": "director", "run_id": "r",
                                                                  "decision_id": world["stuck"],
                                                                  "text": "T2 needs a person."}))
    decisions = {d["id"]: d for d in client.get(f"/api/workspaces/{world['ws']}/inbox").json()["decisions"]}
    assert decisions[world["stuck"]]["director_note"]["text"] == "T2 needs a person."
    assert decisions[world["merge"]]["director_note"] is None
    items = client.get(f"/api/workspaces/{world['ws']}/messages", params={"party": "director"}).json()["items"]
    assert [(m["from"]["role"], m["to"]["role"], m["intent"]) for m in items] == [("director", "people", "propose")]


def test_the_orchestrator_runs_due_directors_beside_the_scheduler(monkeypatch):
    from aifactory.pipeline import orchestrator as orchestration

    started = []

    async def fake_run(item):
        started.append(item["workspace_id"])
        await asyncio.sleep(0)

    monkeypatch.setattr(director, "due", lambda running: [
        {"workspace_id": f"w{n}", "reasons": ["r"], "new": [], "marks": {}, "seen": {}, "usd": 1.0}
        for n in range(3) if f"w{n}" not in running])
    monkeypatch.setattr(director, "run_director", fake_run)
    monkeypatch.setenv("AIFACTORY_DIRECTOR_CONCURRENCY", "2")

    async def go():
        orchestrator = orchestration.Orchestrator()
        await orchestrator._maybe_run_directors()
        assert set(orchestrator._directors) == {"w0", "w1"}  # the server-wide limit
        await asyncio.gather(*orchestrator._directors.values())
        await asyncio.sleep(0)
        assert orchestrator._directors == {}

    asyncio.run(go())
    assert started == ["w0", "w1"]
