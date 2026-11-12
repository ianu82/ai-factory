"""The watch: stuck work wakes the Lead and then alerts people in the app; repeated failures alert at once."""
from __future__ import annotations

from datetime import timedelta

import pytest
from sqlalchemy.exc import IntegrityError

from aifactory import db
from aifactory.models import Alert, Decision, DecisionReview, Event, Outcome, Plan, Repository, Run, Task
from aifactory.pipeline import lead, watch


@pytest.fixture
def world(signed_in):
    ws = signed_in["workspaces"][0]["id"]
    with db.session_scope() as s:
        repo = Repository(workspace_id=ws, full_name="team/app")
        o = Outcome(workspace_id=ws, number=7, title="Search", brief="Add search", status="running")
        s.add_all([repo, o])
        s.flush()
        plan = Plan(outcome_id=o.id, status="approved", content={"tasks": []})
        s.add(plan)
        s.flush()
        t = Task(workspace_id=ws, outcome_id=o.id, plan_id=plan.id, repo_id=repo.id, key="T1", title="Endpoint",
                 status="running", stage="build")
        s.add(t)
        s.flush()
        return {"ws": ws, "outcome": o.id, "task": t.id, "plan": plan.id, "repo": repo.id}


def now():
    return db.utcnow()


def alerts(client, world):
    return client.get(f"/api/workspaces/{world['ws']}/inbox").json()["alerts"]


def live(world):
    with db.session_scope() as s:
        return {a.kind: a.status for a in s.query(Alert).filter(Alert.workspace_id == world["ws"],
                                                                 Alert.status != "cleared")}


def task(world, **values):
    with db.session_scope() as s:
        t = s.get(Task, world["task"])
        for k, v in values.items():
            setattr(t, k, v)


def decision(world, *, kind="question", age=timedelta(0), task_id=None):
    with db.session_scope() as s:
        d = Decision(workspace_id=world["ws"], outcome_id=world["outcome"], task_id=task_id, kind=kind,
                     title="Which queue?", options=[{"value": "answer"}], created_at=now() - age)
        s.add(d)
        s.flush()
        return d.id


def test_a_decision_waiting_past_the_alert_time_alerts_until_it_is_answered(world, client):
    decision(world, age=timedelta(hours=1))
    late = decision(world, age=timedelta(hours=3))
    assert watch.run()["shown"] == 1  # the default alert time is two hours
    [shown] = alerts(client, world)
    assert shown["kind"] == "overdue" and shown["title"] == "Question has waited over 2 hours"
    assert shown["body"] == "Which queue?" and shown["decision_id"] == late and shown["outcome_number"] == 7
    assert watch.run() == {"raised": 0, "shown": 0, "cleared": 0, "woken": 0}  # one alert while it lasts
    with db.session_scope() as s:
        s.get(Decision, late).status = "resolved"
    assert watch.run()["cleared"] == 1 and alerts(client, world) == []


def test_a_decision_waits_on_people_from_when_its_judge_review_finished(world, client):
    question = decision(world, age=timedelta(hours=3))
    with db.session_scope() as s:  # a judge held it for nearly all that time, then handed it to people
        s.add(DecisionReview(workspace_id=world["ws"], decision_id=question, state="held",
                             finished_at=now() - timedelta(minutes=10)))
    watch.run()
    assert alerts(client, world) == []
    with db.session_scope() as s:
        s.query(DecisionReview).filter_by(decision_id=question).one().finished_at = now() - timedelta(hours=2, minutes=1)
    watch.run()
    assert [a["decision_id"] for a in alerts(client, world)] == [question]


def test_a_task_waiting_with_nothing_to_answer_wakes_the_lead_then_alerts(world, client):
    task(world, status="blocked", blocked_reason="Could not push the branch")
    with db.session_scope() as s:
        s.get(Outcome, world["outcome"]).status = "attention"
    assert watch.run()["raised"] == 0  # the scheduler asks about it within minutes: nothing to wake the Lead for yet
    task(world, updated_at=now() - watch.UNASKED_AFTER)
    assert watch.run() == {"raised": 1, "shown": 0, "cleared": 0, "woken": 1}
    assert live(world) == {"unasked": "watching"} and alerts(client, world) == []  # the Lead looks first
    [due] = [d for d in lead.due() if d["outcome_id"] == world["outcome"]]
    assert "the watch found work stuck: T1 is blocked with no decision to answer" in due["reasons"]
    assert watch.run(now() + timedelta(minutes=10))["shown"] == 0
    assert watch.run(now() + watch.GRACE + timedelta(minutes=1))["shown"] == 1  # still stuck: people are told
    [shown] = alerts(client, world)
    assert shown["kind"] == "unasked" and shown["task_key"] == "T1" and shown["body"] == "Could not push the branch"
    decision(world, kind="unblock", task_id=world["task"])  # now there is something to answer
    assert watch.run()["cleared"] == 1 and live(world) == {}


def test_without_a_lead_stuck_work_still_waits_before_it_alerts(world, client):
    client.patch(f"/api/workspaces/{world['ws']}", json={"settings": {"lead": {"enabled": False}}})
    task(world, status="needs_input", updated_at=now() - watch.UNASKED_AFTER)
    assert watch.run() == {"raised": 1, "shown": 0, "cleared": 0, "woken": 0}
    with db.session_scope() as s:
        assert s.query(Event).filter_by(kind="lead_watch").count() == 0
    assert watch.run(now() + watch.GRACE)["shown"] == 1


def test_an_outcome_waiting_on_a_dropped_task_is_stalled(world, client):
    task(world, status="cancelled")
    with db.session_scope() as s:
        s.add(Task(workspace_id=world["ws"], outcome_id=world["outcome"], plan_id=world["plan"], repo_id=world["repo"],
                   key="T2", title="UI", status="pending", depends_on=["T1"]))
        s.get(Outcome, world["outcome"]).status = "attention"
    watch.run()
    watch.run(now() + watch.GRACE)
    [shown] = alerts(client, world)
    assert shown["kind"] == "stalled" and shown["body"] == "T2 waits on T1, which was dropped."


def test_an_active_outcome_with_no_activity_is_quiet_only_when_nothing_runs(world):
    with db.session_scope() as s:
        s.add(Event(workspace_id=world["ws"], outcome_id=world["outcome"], kind="stage", message="Building",
                    created_at=now() - timedelta(hours=2)))
        busy = Run(workspace_id=world["ws"], outcome_id=world["outcome"], role="builder", harness="native",
                   status="running", started_at=now() - timedelta(hours=3))
        s.add(busy)
        s.flush()
        busy_id = busy.id
    watch.run()
    assert live(world) == {}  # the workspace is running something: work may be waiting its turn
    with db.session_scope() as s:
        run = s.get(Run, busy_id)
        run.status, run.finished_at = "succeeded", now() - timedelta(minutes=90)
    watch.run()
    assert live(world) == {"quiet": "watching"}
    with db.session_scope() as s:  # the Lead the watch woke is looking into it
        s.add(Run(workspace_id=world["ws"], outcome_id=world["outcome"], role="lead", harness="control_plane",
                  status="running", started_at=now()))
    watch.run()  # the watch's own records, and the Lead it woke, are not the work moving
    assert live(world) == {"quiet": "watching"}
    with db.session_scope() as s:  # the provider refused the workspace: nothing starts until a person resumes
        hold = Decision(workspace_id=world["ws"], kind="provider_hold", title="Model provider refused: work is on hold",
                        options=[{"value": "retry", "label": "Resume work"}])
        s.add(hold)
    watch.run()
    assert live(world) == {}  # the hold is what to answer, not each outcome it stopped


def test_a_persons_guidance_is_activity_and_the_leads_is_not(world):
    def guidance(**about):
        with db.session_scope() as s:
            s.add(Event(workspace_id=world["ws"], outcome_id=world["outcome"], kind="guidance",
                        message="Use the durable queue.", **about))

    with db.session_scope() as s:
        s.add(Event(workspace_id=world["ws"], outcome_id=world["outcome"], kind="stage", message="Building",
                    created_at=now() - timedelta(hours=2)))
    watch.run()
    assert live(world) == {"quiet": "watching"}
    guidance(run_id="lead-run", actor="lead")  # the Lead it woke steering the agents
    watch.run()
    assert live(world) == {"quiet": "watching"}
    guidance(actor="a-member", data={"intervention": True})  # a person stepping in
    watch.run()
    assert live(world) == {}


def test_the_same_failure_across_outcomes_is_one_alert(world, client):
    with db.session_scope() as s:
        other = Outcome(workspace_id=world["ws"], number=8, title="Other", status="running")
        s.add(other)
        s.flush()
        for outcome_id, error, started, finished in (
                (world["outcome"], "VM refused: slot 12 busy (id abc1234def)\nTraceback...", now(), None),
                (other.id, "VM refused: slot 7 busy (id 99ffee0011)", now() - timedelta(hours=30),
                 now() - timedelta(hours=1))):  # it started before the day began but failed within it
            s.add(Run(workspace_id=world["ws"], outcome_id=outcome_id, role="builder", harness="native",
                      status="failed", error=error, started_at=started, finished_at=finished))
            s.add(Decision(workspace_id=world["ws"], outcome_id=outcome_id, kind="unblock", status="resolved",
                           title="T1 did not pass after 3 attempts", options=[]))  # the task's own failure
            s.add(Run(workspace_id=world["ws"], outcome_id=outcome_id, role="lead", harness="control_plane",
                      status="failed", error="Interrupted by a restart"))  # a deploy, not a failure
        for outcome_id, crash in ((world["outcome"], "KeyError: 'repo'"), (other.id, "TimeoutError: VM 12 boot")):
            s.add(Decision(workspace_id=world["ws"], outcome_id=outcome_id, kind="unblock", status="resolved",
                           title="A task stopped unexpectedly", body=crash, options=[]))  # different crashes
            # The scheduler asking about work nothing asked about isn't a failure, however many outcomes have it.
            s.add(Decision(workspace_id=world["ws"], outcome_id=outcome_id, kind="unblock", options=[],
                           title="T2 is blocked, and nothing was asking what to do", metadata_json={"unasked": "blocked"}))
    assert watch.run()["shown"] == 1
    [shown] = alerts(client, world)
    assert shown["kind"] == "repeated" and shown["body"] == "VM refused: slot # busy (id …)"
    assert shown["title"] == "The same failure on 2 outcomes in the last day"


def test_one_outage_in_different_phases_is_one_repeated_failure(world, client):
    with db.session_scope() as s:
        other = Outcome(workspace_id=world["ws"], number=8, title="Other", status="running")
        s.add(other)
        s.flush()
        for outcome_id, phase in ((world["outcome"], "Implementation"), (other.id, "Independent verification")):
            s.add(Decision(workspace_id=world["ws"], outcome_id=outcome_id, kind="unblock", status="resolved",
                           title="T1: infrastructure problem", options=[],
                           body=f"{phase} failed 3 times in a row for reasons outside the change. Last error:\n\n"
                                "Sandbox 4f3a2b1c9d did not start"))
    assert watch.run()["shown"] == 1
    [shown] = alerts(client, world)
    assert shown["body"] == "infrastructure problem: Sandbox … did not start"


def test_a_dismissed_alert_stays_dismissed_while_its_condition_lasts(world, client):
    late = decision(world, age=timedelta(hours=3))
    watch.run()
    [shown] = alerts(client, world)
    r = client.post(f"/api/workspaces/{world['ws']}/alerts/{shown['id']}/dismiss")
    assert r.status_code == 200 and r.json() == {"status": "dismissed"}
    watch.run()
    assert alerts(client, world) == [] and live(world) == {"overdue": "dismissed"}
    with db.session_scope() as s:
        s.get(Decision, late).status = "resolved"
    watch.run()
    assert live(world) == {}
    with db.session_scope() as s:
        s.get(Decision, late).status = "open"  # the same condition again is a new alert
    watch.run()
    assert [a["id"] != shown["id"] for a in alerts(client, world)] == [True]


def test_alerts_show_in_the_app_and_slack_and_email_stay_off(world, client):
    url = f"/api/workspaces/{world['ws']}"
    for bad in ({"slack": True}, {"email": True}, {"after_minutes": 10}, {"after_minutes": 60.5}, {"sms": False}):
        assert client.patch(url, json={"settings": {"alerts": bad}}).status_code == 422, bad
    r = client.patch(url, json={"settings": {"alerts": {"after_minutes": 60}}})
    assert r.status_code == 200 and r.json()["settings"]["alerts"] == {"after_minutes": 60, "slack": False,
                                                                       "email": False}
    decision(world, age=timedelta(minutes=61))
    watch.run()
    assert [a["title"] for a in alerts(client, world)] == ["Question has waited over 1 hour"]


def test_a_failure_reason_drops_what_differs_between_occurrences():
    assert watch.reason_of("T3: could not push the branch") == "could not push the branch"
    assert watch.reason_of("\n  Sandbox 4f3a2b1c9d timed out after 600s\nmore") == "Sandbox … timed out after #s"
    assert watch.reason_of("") == ""


def test_the_inbox_shows_every_open_alert(world, client):
    for _ in range(55):
        decision(world, age=timedelta(hours=3))
    assert watch.run()["shown"] == 55
    assert len(alerts(client, world)) == 55


def test_one_live_alert_per_condition_even_from_two_schedulers(world):
    def alert(status):
        with db.session_scope() as s:
            s.add(Alert(workspace_id=world["ws"], key="quiet:x", kind="quiet", title="Quiet", status=status))

    alert("open")
    with pytest.raises(IntegrityError):  # the second scheduler's insert is refused
        alert("watching")
    with db.session_scope() as s:
        s.query(Alert).filter_by(key="quiet:x").one().status = "cleared"
    alert("open")  # once it has cleared, the same condition can be raised again


def test_an_alerts_table_from_before_the_index_keeps_the_first_alert_raised(world):
    from sqlalchemy import text

    with db.engine().begin() as conn:  # as it was before one live alert per condition was enforced
        conn.execute(text("DROP INDEX ux_alerts_live_key"))
    with db.session_scope() as s:  # a dismissed alert, then a duplicate a second scheduler raised later
        s.add_all([Alert(id="f" * 32, workspace_id=world["ws"], key="quiet:x", kind="quiet", title="Quiet",
                         status="dismissed", created_at=now() - timedelta(hours=1)),
                   Alert(id="0" * 32, workspace_id=world["ws"], key="quiet:x", kind="quiet", title="Quiet",
                         status="open", created_at=now())])
    db._migrate_alert_live_key(db.engine())
    assert live(world) == {"quiet": "dismissed"}  # what a person dismissed stays dismissed
    with pytest.raises(IntegrityError):  # and the index is back
        with db.session_scope() as s:
            s.add(Alert(workspace_id=world["ws"], key="quiet:x", kind="quiet", title="Quiet", status="open"))
