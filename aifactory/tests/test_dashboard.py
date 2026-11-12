"""The Dashboard's series: hand-built durable histories, a fixed clock, no model/provider or GitHub access."""
from datetime import datetime, timedelta

import pytest
from fastapi.testclient import TestClient

from aifactory.dashboard import floor, series, step
from aifactory.db import session_scope, utcnow
from aifactory.metrics import workspace_metrics
from aifactory.models import Decision, Event, Message, Outcome, Plan, Repository, Run, Task, User

NOW = datetime(2026, 11, 12, 15, 0)  # a Thursday
T0 = datetime(2026, 11, 3, 9, 0)  # the week of Monday 2 November


def passed(n=1):
    return {"n": n, "result": "verified", "checks": [{"command": "pytest -q", "ok": True}],
            "verdict": {"verdict": "pass", "criteria": [{"criterion": "works", "status": "pass"}]}}


def outcome(s, ws, created=T0, completed=None, status="completed", **fields):
    o = Outcome(workspace_id=ws, number=s.query(Outcome).count() + 1, title=f"Outcome {s.query(Outcome).count() + 1}",
                status=status, created_at=created, completed_at=completed, **fields)
    s.add(o)
    s.flush()
    return o


def task(s, ws, o, **fields):
    repo = s.query(Repository).filter_by(workspace_id=ws).first()
    if repo is None:
        repo = Repository(workspace_id=ws, full_name="acme/app", config={"checks": ["pytest -q"]})
        s.add(repo)
        s.flush()
    p = Plan(outcome_id=o.id, status="approved")
    s.add(p)
    s.flush()
    t = Task(workspace_id=ws, outcome_id=o.id, plan_id=p.id, repo_id=repo.id, key=f"T{s.query(Task).count() + 1}",
             title="Task", acceptance=["works"], **fields)
    s.add(t)
    s.flush()
    return t


def event(s, ws, t, kind, at, actor="factory", **data):
    s.add(Event(workspace_id=ws, outcome_id=t.outcome_id, task_id=t.id, kind=kind, actor=actor, created_at=at,
                data=data))
    s.flush()


def run(s, ws, o, at, cost, known=True, role="builder", t=None):
    s.add(Run(workspace_id=ws, outcome_id=o.id, task_id=t.id if t else None, role=role, harness="native",
              status="succeeded", cost_usd=cost, cost_known=known, started_at=at, finished_at=at))
    s.flush()


def delivered(s, ws, o, start=T0, **evidence):
    """A task through every stage: created +1h, started +2h, verified +5h, PR +6h, queued +8h, merged +9h, done +10h."""
    h = lambda n: start + timedelta(hours=n)  # noqa: E731
    t = task(s, ws, o, status="completed", created_at=h(1), started_at=h(2), finished_at=h(10), pr_number=7,
             merge_sha="abc123", evidence={"attempts": [passed()], "merge_queue": {"entered_at": h(8).isoformat()},
                                           "merge": {"sha": "abc123", "at": h(9).isoformat()}, **evidence})
    event(s, ws, t, "stage", h(3), stage="build")
    event(s, ws, t, "stage", h(4), stage="verify")  # a rejected candidate's verification
    event(s, ws, t, "stage", h(5), stage="verify")
    event(s, ws, t, "verified", h(5) + timedelta(minutes=30))
    event(s, ws, t, "published", h(6))
    event(s, ws, t, "merged", h(9))
    return t


def board(ws, period="week", buckets=4, now=NOW):
    with session_scope() as s:
        return series(s, ws, period, buckets, now=now)


@pytest.mark.parametrize("period,at,start,after", [
    ("day", datetime(2026, 11, 12, 15), datetime(2026, 11, 12), datetime(2026, 11, 13)),
    ("week", datetime(2026, 11, 12, 15), datetime(2026, 11, 9), datetime(2026, 11, 16)),
    ("week", datetime(2026, 11, 9), datetime(2026, 11, 9), datetime(2026, 11, 16)),
    ("month", datetime(2026, 12, 31, 23), datetime(2026, 12, 1), datetime(2027, 1, 1)),
])
def test_buckets_are_utc_days_monday_weeks_and_calendar_months(period, at, start, after):
    assert floor(at, period) == start
    assert step(start, period) == after
    assert step(datetime(2026, 1, 1), "month", -2) == datetime(2025, 11, 1)


def test_series_bounds_the_current_bucket_as_partial(client, signed_in):
    ws = signed_in["workspaces"][0]["id"]
    data = board(ws, "month", 3)
    assert [b["start"] for b in data["buckets"]] == ["2026-09-01T00:00:00Z", "2026-10-01T00:00:00Z",
                                                    "2026-11-01T00:00:00Z"]
    assert [b["partial"] for b in data["buckets"]] == [False, False, True]
    assert data["range"]["end"] == "2026-12-01T00:00:00Z"
    assert data["buckets"][0]["days"] == 30 and data["buckets"][2]["days"] == 11.625
    with session_scope() as s:
        with pytest.raises(ValueError):
            series(s, ws, "year")
        with pytest.raises(ValueError):
            series(s, ws, "day", 121)


def test_throughput_cycle_time_and_stages_from_durable_history(client, signed_in):
    ws = signed_in["workspaces"][0]["id"]
    with session_scope() as s:
        o = outcome(s, ws, completed=T0 + timedelta(hours=11))
        delivered(s, ws, o)
        # Opened in the same week, never merged; and a merge whose time was never recorded.
        open_pr = task(s, ws, o, status="waiting_ci", pr_number=8, evidence={})
        event(s, ws, open_pr, "published", T0 + timedelta(hours=7))
        task(s, ws, o, status="completed", merge_sha="def456", finished_at=None, evidence={"attempts": [passed()]})
    data = board(ws)
    week = data["buckets"][2]
    assert week["start"] == "2026-11-02T00:00:00Z"
    assert (week["tasks_delivered"], week["prs_opened"], week["prs_opened_merged"], week["merges"]) == (1, 2, 1, 1)
    assert week["cycle_time"] == {"median_hours": 7.0, "sample_count": 1, "unknown_count": 0}
    assert {k: v["median_hours"] for k, v in week["stages"].items()} == {
        "plan": 1.0, "build": 3.0, "verify": 1.0, "review": 2.0, "merge_queue": 1.0, "release": 1.0}
    assert week["first_attempt"] == {"merged": 1, "passed": 1}
    assert week["lead_time"] == {"median_hours": 11.0, "sample_count": 1}
    assert data["undated"] == {"tasks_delivered": 1, "merges": 1}
    assert all(b["merges"] == 0 for i, b in enumerate(data["buckets"]) if i != 2)
    assert data["range"]["merges"] == 1


def test_merge_stage_event_stands_in_for_a_missing_queue_record(client, signed_in):
    ws = signed_in["workspaces"][0]["id"]
    with session_scope() as s:
        o = outcome(s, ws, completed=T0 + timedelta(hours=11))
        t = delivered(s, ws, o)
        t.evidence = {k: v for k, v in t.evidence.items() if k != "merge_queue"}
        event(s, ws, t, "stage", T0 + timedelta(hours=8, minutes=30), stage="merge")
    stages = board(ws)["buckets"][2]["stages"]
    assert (stages["review"]["median_hours"], stages["merge_queue"]["median_hours"]) == (2.5, 0.5)


def test_costs_are_lower_bounds_when_any_usage_is_unknown(client, signed_in):
    ws = signed_in["workspaces"][0]["id"]
    with session_scope() as s:
        cheap = outcome(s, ws, completed=T0 + timedelta(hours=11))
        delivered(s, ws, cheap)
        run(s, ws, cheap, T0, 4.0)
        dear = outcome(s, ws, completed=T0 + timedelta(days=7))
        delivered(s, ws, dear, start=T0 + timedelta(days=6))
        run(s, ws, dear, T0 + timedelta(days=7), 10.0)
        run(s, ws, dear, T0 + timedelta(days=7), 2.0, known=False, role="verifier")
    data = board(ws)
    last, current = data["buckets"][2], data["buckets"][3]
    assert last["cost"] == {"usd": 4.0, "known": True}
    assert last["cost_per_delivered_task"] == {"usd": 4.0, "known": True}
    assert current["cost"] == {"usd": 12.0, "known": False}
    assert current["runs"] == {"total": 2, "by_role": {"builder": 1, "verifier": 1}}
    assert data["range"]["outcome_cost"] == {"median_usd": 8.0, "known": False, "sample_count": 2}
    assert [(o["number"], o["cost_usd"], o["cost_known"]) for o in data["expensive_outcomes"]] == [
        (dear.number, 12.0, False), (cheap.number, 4.0, True)]


def test_the_director_is_not_a_person_and_is_shown_apart(client, signed_in):
    ws, person = signed_in["workspaces"][0]["id"], signed_in["user"]["id"]
    done = T0 + timedelta(hours=11)
    with session_scope() as s:
        director = User(email="director@factory.local", name="Director", is_system=True)
        lead = User(email="lead@factory.local", name="Lead", is_system=True)
        operator = User(email="operator+abc@factory.local", name="Operator", is_system=True)
        s.add_all([director, lead, operator])
        s.flush()
        alone, directed, guided, helped = (outcome(s, ws, completed=done) for _ in range(4))
        for o in (alone, directed, guided, helped):
            task(s, ws, o, status="completed", finished_at=done)
        s.add(Decision(workspace_id=ws, outcome_id=directed.id, kind="approve_merge", title="Merge?",
                       status="resolved", resolution="approve", resolved_by=director.id, resolved_at=T0 + timedelta(hours=1),
                       attribution={"source": "director", "run_id": "r1"}, created_at=T0))
        # The Director's guidance is a user message so agents receive it; it is still not a person's.
        s.add(Message(workspace_id=ws, outcome_id=guided.id, author_id=director.id, author_kind="user", body="Ship it",
                      attribution={"source": "director", "run_id": "r1"}, created_at=T0 + timedelta(hours=1)))
        s.add(Message(workspace_id=ws, outcome_id=helped.id, author_id=person, author_kind="user", body="Use UTF-8",
                      created_at=T0 + timedelta(hours=1)))
        s.add(Message(workspace_id=ws, outcome_id=alone.id, author_id=lead.id, author_kind="user", body="Retry",
                      attribution={"source": "lead", "run_id": "r2"}, created_at=T0 + timedelta(hours=1)))
        for who, attribution in ((lead.id, {"source": "lead"}), (operator.id, None), (person, {"source": "mind"}),
                                 (person, None), ("policy", None)):
            s.add(Decision(workspace_id=ws, kind="question", title="?", status="resolved", resolution="answer",
                           resolved_by=who, resolved_at=T0 + timedelta(hours=2), attribution=attribution,
                           created_at=T0))
        s.add(Decision(workspace_id=ws, kind="question", title="Still open", created_at=T0))
    week = board(ws)["buckets"][2]
    assert week["autonomy"] == {"unattended": 1, "director": 2, "assisted": 1, "unverified": 0}
    assert week["decisions"] == {"asked": 7, "person": 2, "director": 1, "lead": 1, "operator": 1, "factory": 1}
    assert week["guidance"] == {"person": 1, "director": 1, "lead": 1}
    with session_scope() as s:
        autonomy = workspace_metrics(s, ws)["autonomy"]
    assert (autonomy["numerator"], autonomy["denominator"]) == (3, 4)


def test_series_agrees_with_the_runs_strip(client, signed_in):
    """Summed over buckets that cover all history, the series reproduces the strip's totals."""
    ws, person = signed_in["workspaces"][0]["id"], signed_in["user"]["id"]
    now = utcnow()
    with session_scope() as s:
        o = outcome(s, ws, created=now - timedelta(days=3), completed=now - timedelta(days=1))
        delivered(s, ws, o, start=now - timedelta(days=3))
        run(s, ws, o, now - timedelta(days=2), 3.5)
        run(s, ws, o, now - timedelta(days=2), 1.0, known=False)
        s.add(Message(workspace_id=ws, outcome_id=o.id, author_id=person, author_kind="user", body="Note",
                      created_at=now - timedelta(days=2)))
        s.add(Message(workspace_id=ws, outcome_id=o.id, author_kind="user", body="Directed",
                      attribution={"source": "director"}, created_at=now - timedelta(days=2)))
        s.add(Decision(workspace_id=ws, outcome_id=o.id, kind="question", title="?", created_at=now - timedelta(days=2)))
    strip = client.get(f"/api/workspaces/{ws}/stats").json()
    r = client.get(f"/api/workspaces/{ws}/dashboard", params={"period": "day", "buckets": 7})
    assert r.status_code == 200
    totals = r.json()["range"]
    assert totals["tasks_delivered"] == strip["tasks"]["completed"] == 1
    assert totals["merges"] == strip["tasks"]["merged"] == 1
    assert totals["first_attempt"]["passed"] == strip["tasks"]["first_attempt"] == 1
    assert totals["cost"] == {"usd": strip["cost"]["usd"], "known": strip["cost"]["known"]} == {"usd": 4.5, "known": False}
    assert totals["cost_per_delivered_task"]["usd"] == strip["cost"]["per_completed_task"]
    assert totals["decisions"]["asked"] == strip["human"]["decisions"] == 1
    assert totals["guidance"]["person"] == strip["human"]["interventions"] == 1


def test_change_failures_use_the_regret_accounting(client, signed_in):
    ws = signed_in["workspaces"][0]["id"]
    with session_scope() as s:
        o = outcome(s, ws, completed=T0 + timedelta(hours=11))
        delivered(s, ws, o)
        failed = delivered(s, ws, o)
        failed.status = "smoke_failed"
        failed.merge_sha = failed.evidence["merge"]["sha"] = "fff000"
        failed.evidence = {**failed.evidence, "smoke": {"state": "failed", "ref": "fff000"}}
        pending = delivered(s, ws, o)
        pending.status, pending.merge_sha = "running", "aaa111"
        pending.evidence = {**pending.evidence, "merge": {"sha": "aaa111", "at": (T0 + timedelta(hours=9)).isoformat()}}
    week = board(ws)["buckets"][2]
    assert week["change_failure"] == {"settled": 2, "failed": 1, "pending": 1}
    assert week["merges"] == 3


def test_benchmarks_stay_out(client, signed_in):
    ws = signed_in["workspaces"][0]["id"]
    with session_scope() as s:
        o = outcome(s, ws, completed=T0 + timedelta(hours=11), source="benchmark")
        delivered(s, ws, o)
        run(s, ws, o, T0, 5.0)
    week = board(ws)["buckets"][2]
    assert (week["tasks_delivered"], week["merges"], week["cost"]["usd"], week["outcomes_completed"]) == (0, 0, 0, 0)


def test_dashboard_endpoint_validates_and_keeps_workspaces_apart(client, signed_in):
    ws = signed_in["workspaces"][0]["id"]
    r = client.get(f"/api/workspaces/{ws}/dashboard")
    assert r.status_code == 200
    data = r.json()
    assert data["period"] == "week" and len(data["buckets"]) == 12 and data["buckets"][-1]["partial"]
    empty = data["range"]
    assert empty["cycle_time"]["median_hours"] is None and empty["cost_per_delivered_task"]["usd"] is None
    assert set(data["definitions"]) >= {"cycle_time", "autonomy", "change_failure", "cost_per_delivered_task"}
    assert client.get(f"/api/workspaces/{ws}/dashboard", params={"period": "year"}).status_code == 422
    assert client.get(f"/api/workspaces/{ws}/dashboard", params={"period": "month", "buckets": 37}).status_code == 422
    assert client.get(f"/api/workspaces/{ws}/dashboard", params={"buckets": 0}).status_code == 422
    other = TestClient(client.app)
    assert other.get(f"/api/workspaces/{ws}/dashboard").status_code == 401
    other.post("/auth/dev", json={"email": "other@test.com", "organization": "Other"})
    assert other.get(f"/api/workspaces/{ws}/dashboard").status_code in (403, 404)


def test_director_activity_after_delivery_does_not_reclassify_it(client, signed_in):
    ws = signed_in["workspaces"][0]["id"]
    done = T0 + timedelta(hours=11)
    with session_scope() as s:
        director = User(email="director@factory.local", name="Director", is_system=True)
        s.add(director)
        s.flush()
        o = outcome(s, ws, completed=done)
        t = task(s, ws, o, status="completed", finished_at=done)
        # A note and a decision on the finished outcome: they say nothing about how it was delivered.
        s.add(Message(workspace_id=ws, outcome_id=o.id, author_id=director.id, author_kind="user", body="Later",
                      attribution={"source": "director"}, created_at=done + timedelta(hours=1)))
        s.add(Decision(workspace_id=ws, task_id=t.id, kind="supervisor", title="Finding", status="resolved",
                       resolution="acknowledge", resolved_by=director.id, resolved_at=done + timedelta(hours=2),
                       attribution={"source": "director"}, created_at=done))
    assert board(ws)["buckets"][2]["autonomy"] == {"unattended": 1, "director": 0, "assisted": 0, "unverified": 0}
