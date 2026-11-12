"""The watch: stuck work wakes the Lead and then alerts people in the app; repeated failures alert at once."""
from __future__ import annotations

from datetime import timedelta

import pytest

from aifactory import alert_webhook, db
from aifactory.models import Alert, Decision, Event, Outcome, Plan, Repository, Run, Secret, Task, Workspace
from aifactory.pipeline import lead, watch
from aifactory.pipeline.holds import CAUSES, open_provider_hold


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


def raised(world):
    """The open alerts as the watch left them (the Inbox folds an overdue one into its decision)."""
    with db.session_scope() as s:
        return [{"id": a.id, "kind": a.kind, "title": a.title, "body": a.body, "decision_id": a.decision_id}
                for a in s.query(Alert).filter(Alert.workspace_id == world["ws"], Alert.status == "open")]


def live(world):
    with db.session_scope() as s:
        return {a.kind: a.status for a in s.query(Alert).filter(Alert.workspace_id == world["ws"],
                                                                 Alert.status != "cleared")}


def task(world, **values):
    with db.session_scope() as s:
        t = s.get(Task, world["task"])
        for k, v in values.items():
            setattr(t, k, v)


def working(world, role="builder"):
    """An agent at work: the workspace has not stopped."""
    with db.session_scope() as s:
        run = Run(workspace_id=world["ws"], outcome_id=world["outcome"], role=role, harness="native", status="running",
                  started_at=now())
        s.add(run)
        s.flush()
        return run.id


def decision(world, *, kind="question", age=timedelta(0), task_id=None):
    with db.session_scope() as s:
        d = Decision(workspace_id=world["ws"], outcome_id=world["outcome"], task_id=task_id, kind=kind,
                     title="Which queue?", options=[{"value": "answer"}], created_at=now() - age)
        s.add(d)
        s.flush()
        return d.id


def test_a_decision_waiting_past_the_alert_time_alerts_until_it_is_answered(world, client):
    working(world)
    decision(world, age=timedelta(hours=1))
    late = decision(world, age=timedelta(hours=3))
    assert watch.run()["shown"] == 1  # the default alert time is two hours
    [shown] = raised(world)
    assert shown["kind"] == "overdue" and shown["title"] == "Question has waited over 2 hours"
    assert shown["body"] == "Which queue?" and shown["decision_id"] == late
    assert watch.run() == {"raised": 0, "shown": 0, "cleared": 0, "woken": 0}  # one alert while it lasts
    with db.session_scope() as s:
        s.get(Decision, late).status = "resolved"
    assert watch.run()["cleared"] == 1 and raised(world) == []


def test_an_overdue_decision_is_one_item_in_the_inbox_marked_on_the_decision(world, client):
    working(world)
    recent = decision(world, age=timedelta(minutes=5))
    late = decision(world, age=timedelta(hours=4))
    with db.session_scope() as s:  # and a repeated failure: an alert about no listed decision
        other = Outcome(workspace_id=world["ws"], number=8, title="Other", brief="", status="running")
        s.add(other)
        s.flush()
        for outcome_id in (world["outcome"], other.id):
            s.add(Run(workspace_id=world["ws"], outcome_id=outcome_id, role="builder", harness="native",
                      status="failed", error="Sandbox refused", started_at=now(), finished_at=now()))
    watch.run()
    [overdue] = [a for a in raised(world) if a["kind"] == "overdue"]
    inbox = client.get(f"/api/workspaces/{world['ws']}/inbox").json()
    assert [a["kind"] for a in inbox["alerts"]] == ["repeated"]  # the overdue alert is not a second item
    assert [d["id"] for d in inbox["decisions"]] == [late, recent]  # overdue first
    first, second = inbox["decisions"]
    assert first["overdue"] == {"alert_id": overdue["id"], "title": "Question has waited over 2 hours"}
    assert 239 <= first["waited_minutes"] <= 241 and second["overdue"] is None and 4 <= second["waited_minutes"] <= 6
    assert len(inbox["alerts"]) + len(inbox["decisions"]) == 3  # what the navigation counts is what the list shows
    detail = client.get(f"/api/workspaces/{world['ws']}/outcomes/{world['outcome']}").json()
    assert {d["id"]: d["overdue"] for d in detail["decisions"]}[late] == first["overdue"]  # the card shows it too


def test_a_task_waiting_with_nothing_to_answer_wakes_the_lead_then_alerts(world, client):
    task(world, status="blocked", blocked_reason="Could not push the branch")
    with db.session_scope() as s:
        s.get(Outcome, world["outcome"]).status = "attention"
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
    task(world, status="needs_input")
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
    watch.run()  # the watch's own records, and the Lead it woke, are not the work moving
    assert live(world) == {"quiet": "watching"}
    with db.session_scope() as s:  # the provider refused the workspace: nothing starts until a person resumes
        hold = Decision(workspace_id=world["ws"], kind="provider_hold", title="Model provider refused: work is on hold",
                        options=[{"value": "retry", "label": "Resume work"}])
        s.add(hold)
    watch.run()
    assert live(world) == {}  # the hold is what to answer, not each outcome it stopped


def test_an_outcome_waiting_for_capacity_is_neither_stalled_nor_quiet(world):
    reason = "10 outcomes are open in this workspace (limit 10). This one is planned when one finishes."
    with db.session_scope() as s:
        waiting = Outcome(workspace_id=world["ws"], number=8, title="Export", status="planning", blocked_reason=reason)
        s.add(waiting)
        s.flush()
        s.add(Event(workspace_id=world["ws"], outcome_id=waiting.id, kind="waiting_for_capacity", message=reason,
                    created_at=now() - timedelta(days=2)))
        waiting_id = waiting.id
    for later in (timedelta(0), watch.GRACE + timedelta(hours=3)):
        watch.run(now() + later)
        assert live(world) == {}
    with db.session_scope() as s:
        assert s.query(Event).filter(Event.outcome_id == waiting_id, Event.kind == "lead_watch").count() == 0

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
    assert watch.run()["shown"] == 1
    [shown] = alerts(client, world)
    assert shown["kind"] == "repeated" and shown["body"] == "VM refused: slot # busy (id …)"
    assert shown["title"] == "The same failure on 2 outcomes in the last day"


def test_a_dismissed_alert_stays_dismissed_while_its_condition_lasts(world, client):
    working(world)
    late = decision(world, age=timedelta(hours=3))
    watch.run()
    [shown] = raised(world)
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
    assert [a["id"] != shown["id"] for a in raised(world)] == [True]


def test_alerts_show_in_the_app_and_slack_and_email_stay_off(world, client):
    url = f"/api/workspaces/{world['ws']}"
    for bad in ({"slack": True}, {"email": True}, {"after_minutes": 10}, {"after_minutes": 60.5}, {"sms": False},
                {"stopped_minutes": 4}, {"stopped_minutes": 241}, {"stopped_minutes": 20.5}):
        assert client.patch(url, json={"settings": {"alerts": bad}}).status_code == 422, bad
    r = client.patch(url, json={"settings": {"alerts": {"after_minutes": 60}}})
    assert r.status_code == 200 and r.json()["settings"]["alerts"] == {
        "after_minutes": 60, "stopped_minutes": 20, "slack": False, "email": False}
    assert r.json()["alert_webhook"] == {"set": False, "host": ""}
    working(world)
    decision(world, age=timedelta(minutes=61))
    watch.run()
    assert [a["title"] for a in raised(world)] == ["Question has waited over 1 hour"]


def test_a_failure_reason_drops_what_differs_between_occurrences():
    assert watch.reason_of("T3: could not push the branch") == "could not push the branch"
    assert watch.reason_of("\n  Sandbox 4f3a2b1c9d timed out after 600s\nmore") == "Sandbox … timed out after #s"
    assert watch.reason_of("") == ""


def test_work_that_keeps_running_without_its_vm_is_one_alert(world, client):
    def refused(task_id, kind="build_vm_unavailable", why="no credentials", age=timedelta(0)):
        with db.session_scope() as s:
            s.add(Event(workspace_id=world["ws"], outcome_id=world["outcome"], task_id=task_id, level="warn", kind=kind,
                        message=f"VM isolation requested but unavailable ({why})", created_at=now() - age))

    with db.session_scope() as s:
        tasks = [world["task"]]
        for key in ("T2", "T3"):
            t = Task(workspace_id=world["ws"], outcome_id=world["outcome"], plan_id=world["plan"],
                     repo_id=world["repo"], key=key, title=key, status="running", stage="build")
            s.add(t)
            s.flush()
            tasks.append(t.id)
    refused(tasks[0], age=timedelta(days=2))  # before the day began: not counted
    refused(tasks[0], age=timedelta(minutes=3))
    refused(tasks[1], age=timedelta(minutes=2))
    assert watch.run()["shown"] == 0  # two tasks are not yet a pattern
    refused(tasks[2], "qa_vm_unavailable", "the VM limit is reached")
    assert watch.run()["shown"] == 1
    [shown] = alerts(client, world)
    assert shown["kind"] == "repeated"
    assert shown["title"] == "No VM for 3 tasks in the last day, though VM isolation is asked for"
    assert shown["body"].startswith("3 times in the last day") and "the VM limit is reached" in shown["body"]
    watch.run()
    assert len(alerts(client, world)) == 1  # one alert while it lasts


def stopped_alert(client, world):
    return [a for a in alerts(client, world) if a["kind"] == "stopped"]


@pytest.mark.parametrize("reason", ["hold", "queued", "decision"])
def test_nothing_running_while_work_waits_is_shown_after_the_quiet_period_and_clears_when_a_run_starts(
        world, client, reason):
    began = now()
    if reason == "hold":
        with db.session_scope() as s:
            s.add(Decision(workspace_id=world["ws"], kind="provider_hold", title="Model provider refused",
                           options=[{"value": "retry", "label": "Resume work"}], created_at=began,
                           metadata_json={"cause": "credit", "source": "planning outcome #7", "error": "no credit"}))
        expected = f"A provider hold is open since {watch._when(began)}: {CAUSES['credit'][0]}."
    elif reason == "queued":
        task(world, status="queued")
        expected = "1 task is queued."
    else:
        with db.session_scope() as s:
            s.add(Decision(workspace_id=world["ws"], outcome_id=world["outcome"], kind="question",
                           title="Which queue?", options=[{"value": "answer"}], created_at=began))
        expected = "1 decision is waiting for a person (for 21 minutes)."
    later = began + timedelta(minutes=21)
    assert watch.run(later - timedelta(minutes=2))["shown"] == 0  # not yet quiet for 20 minutes
    assert watch.run(later)["shown"] == 1  # shown at once, like repeated failures
    [shown] = alerts(client, world)
    assert (shown["kind"], shown["title"], shown["body"]) == ("stopped", "Nothing is running", expected)
    assert watch.run(later)["shown"] == 0  # one alert while it lasts
    working(world)
    assert watch.run(later)["cleared"] == 1 and alerts(client, world) == []


def test_nothing_running_counts_every_reason_and_waits_for_the_last_run(world, client):
    began = now()
    with db.session_scope() as s:
        s.add(Run(workspace_id=world["ws"], outcome_id=world["outcome"], role="builder", harness="native",
                  status="succeeded", started_at=began - timedelta(hours=2), finished_at=began + timedelta(minutes=10)))
        for age in (80, 20):  # minutes: not yet overdue
            s.add(Decision(workspace_id=world["ws"], outcome_id=world["outcome"], kind="question", title="Which?",
                           options=[{"value": "answer"}], created_at=began - timedelta(minutes=age)))
    task(world, status="queued")
    assert watch.run(began + timedelta(minutes=29))["shown"] == 0  # the last run ended 19 minutes before
    watch.run(began + timedelta(minutes=30))
    [shown] = stopped_alert(client, world)
    assert shown["body"] == "1 task is queued. 2 decisions are waiting for a person (the oldest for 1 hour)."


def test_nothing_running_is_not_raised_while_an_agent_works_or_for_a_paused_workspace(world, client):
    task(world, status="queued")
    later = now() + timedelta(hours=1)
    builder = working(world)
    watch.run(later)
    assert live(world) == {}
    with db.session_scope() as s:
        run = s.get(Run, builder)
        run.status, run.finished_at = "succeeded", now()
    working(world, role="lead")  # the Lead looking into the work is not the work moving
    client.patch(f"/api/workspaces/{world['ws']}", json={"paused": True})
    watch.run(later)
    assert live(world) == {}  # a paused workspace runs nothing on purpose
    client.patch(f"/api/workspaces/{world['ws']}", json={"paused": False})
    watch.run(later)
    assert live(world) == {"stopped": "open"}


WEBHOOK = "https://hooks.example.com/services/T000/B000/abcdefSECRET"


@pytest.fixture
def posted(monkeypatch):
    """The webhook's HTTP call, faked: each (url, text) sent."""
    sent = []
    monkeypatch.setattr(alert_webhook, "_post", lambda url, payload: sent.append((url, payload["text"])) or 200)
    return sent


def test_the_webhook_carries_a_hold_and_nothing_running_out_once_when_raised_and_once_when_cleared(
        world, client, posted):
    r = client.patch(f"/api/workspaces/{world['ws']}", json={"settings": {"alerts": {"webhook_url": WEBHOOK}}})
    assert r.status_code == 200 and r.json()["alert_webhook"] == {"set": True, "host": "hooks.example.com"}
    with db.session_scope() as s:
        name = s.get(Workspace, world["ws"]).name
    hold = open_provider_hold(world["ws"], "MindsHub: no credit remaining (balance 0)", "planning outcome #7")
    with db.session_scope() as s:
        opened = s.get(Decision, hold).created_at
    watch.run(opened)
    assert posted == [(WEBHOOK, f"Work is on hold in {name} since {watch._when(opened)}: {CAUSES['credit'][0]}. "
                                f"Nothing new starts until the hold is lifted. {CAUSES['credit'][1]}")]
    watch.run(opened + timedelta(minutes=21))
    assert posted[1] == (WEBHOOK, f"Nothing is running in {name} since {watch._when(opened)}. A provider hold is "
                                  f"open since {watch._when(opened)}: {CAUSES['credit'][0]}.")
    watch.run(opened + timedelta(minutes=22))
    assert len(posted) == 2  # once per raise, also after another pass
    r = client.post(f"/api/workspaces/{world['ws']}/decisions/{hold}/resolve", json={"resolution": "retry"})
    assert r.status_code == 200, r.text
    watch.run(opened + timedelta(minutes=23))
    assert posted[2:] == [(WEBHOOK, f"Work has resumed in {name}: an agent is at work again, or nothing waits."),
                          (WEBHOOK, f"Work has resumed in {name}: the provider hold is lifted.")]
    watch.run(opened + timedelta(minutes=24))
    assert len(posted) == 4  # once per clear
    with db.session_scope() as s:
        assert s.query(Event).filter_by(kind="alert_delivery_failed").count() == 0


def test_a_hold_lifted_before_the_watch_first_looked_is_told_once_as_what_happened(world, client, posted):
    client.patch(f"/api/workspaces/{world['ws']}", json={"settings": {"alerts": {"webhook_url": WEBHOOK}}})
    with db.session_scope() as s:
        name = s.get(Workspace, world["ws"]).name
    hold = open_provider_hold(world["ws"], "MindsHub: no credit remaining (balance 0)", "planning outcome #7")
    r = client.post(f"/api/workspaces/{world['ws']}/decisions/{hold}/resolve", json={"resolution": "retry"})
    assert r.status_code == 200, r.text
    with db.session_scope() as s:
        opened, resolved = s.get(Decision, hold).created_at, s.get(Decision, hold).resolved_at
    watch.run(resolved + timedelta(minutes=1))
    assert posted == [(WEBHOOK, f"Work was on hold in {name} from {watch._when(opened)} to {watch._when(resolved)}: "
                                f"{CAUSES['credit'][0]}. The hold is lifted and work has resumed.")]
    watch.run(resolved + timedelta(minutes=6))
    assert len(posted) == 1
    old = open_provider_hold(world["ws"], "MindsHub: no credit remaining (balance 0)", "planning outcome #7")
    client.post(f"/api/workspaces/{world['ws']}/decisions/{old}/resolve", json={"resolution": "retry"})
    watch.run(resolved + timedelta(hours=2))  # lifted long before this pass: old news, not sent
    assert len(posted) == 1
    # That hold was lifted a moment ago, and another opens before the watch looks: only the open one is told,
    # since nothing has resumed.
    again = open_provider_hold(world["ws"], "MindsHub: no credit remaining (balance 0)", "planning outcome #7")
    with db.session_scope() as s:
        reopened = s.get(Decision, again).created_at
    watch.run(reopened + timedelta(minutes=1))
    assert [text for _, text in posted[1:]] == [
        f"Work is on hold in {name} since {watch._when(reopened)}: {CAUSES['credit'][0]}. Nothing new starts until "
        f"the hold is lifted. {CAUSES['credit'][1]}"]


def test_a_failing_webhook_neither_raises_nor_repeats(world, client, monkeypatch):
    calls = []

    def down(url, payload):
        calls.append(payload["text"])
        raise OSError("connection refused")

    monkeypatch.setattr(alert_webhook, "_post", down)
    client.patch(f"/api/workspaces/{world['ws']}", json={"settings": {"alerts": {"webhook_url": WEBHOOK}}})
    hold = open_provider_hold(world["ws"], "MindsHub: no credit remaining (balance 0)", "planning outcome #7")
    watch.run()
    watch.run()
    assert len(calls) == 1
    with db.session_scope() as s:
        [failed] = s.query(Event).filter_by(kind="alert_delivery_failed").all()
        assert failed.level == "warn" and failed.data["decision_id"] == hold and WEBHOOK not in failed.message
    monkeypatch.setattr(alert_webhook, "_post", lambda url, payload: 500)
    assert alert_webhook.send(WEBHOOK, "text") == "the webhook answered HTTP 500"


@pytest.mark.parametrize("bad", ["http://hooks.example.com/x", "https://user:pw@hooks.example.com/x",
                                 "https://127.0.0.1/x", "https://10.1.2.3/x", "https://192.168.0.4/x",
                                 "https://169.254.169.254/latest", "https://[::1]/x", "https://[fe80::1]/x",
                                 "https://localhost/x", "https://box.internal/x", "https://intranet/x",
                                 "https://127.1/x", "https://0x7f.1/x", "ftp://hooks.example.com/x", "not a url", 42])
def test_the_webhook_url_must_be_https_to_a_public_host_without_credentials(world, client, bad):
    r = client.patch(f"/api/workspaces/{world['ws']}", json={"settings": {"alerts": {"webhook_url": bad}}})
    assert r.status_code == 422 and (not isinstance(bad, str) or bad not in r.text)
    with db.session_scope() as s:
        assert s.query(Secret).filter_by(kind=alert_webhook.KIND).count() == 0


def test_the_api_never_returns_the_webhook_url(world, client):
    url = f"/api/workspaces/{world['ws']}"
    r = client.patch(url, json={"settings": {"alerts": {"webhook_url": WEBHOOK}}})
    assert r.status_code == 200 and "SECRET" not in r.text and "/services/" not in r.text
    me = client.get("/api/me")
    assert "SECRET" not in me.text
    [mine] = [w for w in me.json()["workspaces"] if w["id"] == world["ws"]]
    assert mine["alert_webhook"] == {"set": True, "host": "hooks.example.com"}
    with db.session_scope() as s:  # kept encrypted, never in the settings
        secret = s.query(Secret).filter_by(workspace_id=world["ws"], kind=alert_webhook.KIND).one()
        assert "SECRET" not in secret.ciphertext and "SECRET" not in str(s.get(Workspace, world["ws"]).settings)
        assert alert_webhook.url_for(s, world["ws"]) == WEBHOOK
    assert client.post(f"{url}/credentials/{alert_webhook.KIND}/test").status_code == 404
    r = client.patch(url, json={"settings": {"alerts": {"after_minutes": 60}}})  # alerts are patched key by key
    assert r.json()["alert_webhook"]["set"] is True and "SECRET" not in r.text
    r = client.patch(url, json={"settings": {"alerts": {"webhook_url": ""}}})  # Clear
    assert r.json()["alert_webhook"] == {"set": False, "host": ""}
    assert r.json()["settings"]["alerts"]["after_minutes"] == 60


def test_the_webhook_goes_to_the_address_that_was_checked_not_to_a_second_lookup_of_the_name(monkeypatch):
    from types import SimpleNamespace

    from aifactory import alert_webhook

    seen, lookups = {}, []

    class Client:
        def __init__(self, **options):
            seen["client"] = options

        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

        def post(self, url, **request):
            seen.update(url=url, **request)
            return SimpleNamespace(status_code=204)

    def resolve(host, port, **_):
        lookups.append(host)
        return [(2, 1, 6, "", ("93.184.216.34", port))]

    monkeypatch.setattr(alert_webhook.socket, "getaddrinfo", resolve)
    monkeypatch.setattr(alert_webhook.httpx, "Client", Client)
    assert alert_webhook._post("https://hooks.example.com/services/T/B?x=1", {"text": "hi"}) == 204
    # One lookup; the request is addressed to its answer, with the name kept for the certificate and the Host header.
    assert lookups == ["hooks.example.com"] and seen["url"] == "https://93.184.216.34:443/services/T/B?x=1"
    assert seen["headers"] == {"Host": "hooks.example.com"}
    assert seen["extensions"] == {"sni_hostname": "hooks.example.com"}
    assert seen["json"] == {"text": "hi"} and seen["client"]["follow_redirects"] is False

    mixed = [(2, 1, 6, "", ("93.184.216.34", 443)), (2, 1, 6, "", ("10.0.0.5", 443))]
    monkeypatch.setattr(alert_webhook.socket, "getaddrinfo", lambda host, port, **_: mixed)
    with pytest.raises(PermissionError):  # any private answer refuses the send
        alert_webhook._post("https://hooks.example.com/x", {"text": "hi"})
