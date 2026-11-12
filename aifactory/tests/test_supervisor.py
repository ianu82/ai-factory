"""Supervisor checks use hand-built DB histories and an injected, fixed clock."""
from datetime import datetime, timedelta

import pytest

from aifactory.db import session_scope
from aifactory.models import Decision, Event, Message, Outcome, Plan, PullReview, Repository, ReviewAttempt, Run, Signal, Task, Workspace
from aifactory.supervisor import detect, surface, sweep
from aifactory.supervisor_policy import supervisor_policy, validate_supervisor

NOW = datetime(2026, 11, 11, 12)
OLD = NOW - timedelta(hours=30)


@pytest.fixture
def world(signed_in):
    ws = signed_in["workspaces"][0]["id"]
    with session_scope() as db:
        repo = Repository(workspace_id=ws, full_name="acme/test", default_branch="trunk")
        outcome = Outcome(workspace_id=ws, number=1, title="Delivery", created_at=OLD)
        db.add_all([repo, outcome])
        db.flush()
        plan = Plan(outcome_id=outcome.id)
        db.add(plan)
        db.flush()
        ids = ws, repo.id, outcome.id, plan.id
    return ids


def task(db, world, **fields):
    ws, repo, outcome, plan = world
    row = Task(workspace_id=ws, repo_id=repo, outcome_id=outcome, plan_id=plan,
               key=f"T{db.query(Task).count()}", title="Task", updated_at=fields.pop("updated_at", OLD), **fields)
    db.add(row)
    db.flush()
    return row


def signal(db, world, source, **fields):
    ws, repo, _, _ = world
    row = Signal(workspace_id=ws, repo_id=repo, source=source, title="Failure",
                 fingerprint=f"signal-{db.query(Signal).count()}",
                 first_seen_at=fields.pop("first_seen_at", OLD), last_seen_at=OLD, **fields)
    db.add(row)
    db.flush()
    return row


def findings(world, kind):
    with session_scope() as db:
        return [f for f in detect(db, world[0], NOW, supervisor_policy()) if f.kind == kind]


@pytest.mark.parametrize("status,expired,old,expected", [
    ("blocked", False, True, True), ("needs_input", False, True, True),
    ("smoke_failed", False, True, True), ("failed", False, True, True),
    ("running", True, True, True), ("running", False, True, False),
    ("blocked", False, False, False), ("completed", True, True, False), ("queued", True, True, False),
])
def test_stuck_tasks(world, status, expired, old, expected):
    with session_scope() as db:
        task(db, world, status=status, updated_at=OLD if old else NOW,
             lease_expires_at=NOW - timedelta(minutes=1) if expired else NOW + timedelta(minutes=1))
    assert bool(findings(world, "stuck_task")) is expected


@pytest.mark.parametrize("text,run_error", [("GH013", False), ("push protection", False), ("SECRET SCANNING", True)])
def test_push_protection_two_sweeps(world, text, run_error):
    with session_scope() as db:
        t = task(db, world, status="blocked", blocked_reason="" if run_error else text)
        if run_error:
            db.add(Run(workspace_id=world[0], outcome_id=world[2], task_id=t.id, role="builder", harness="native", error=text))
    assert len(findings(world, "push_protection")) == 1
    sweep(world[0], NOW)
    with session_scope() as db:
        assert db.query(Decision).filter_by(kind="supervisor").count() == 0
    sweep(world[0], NOW + timedelta(minutes=30))
    sweep(world[0], NOW + timedelta(minutes=60))
    with session_scope() as db:
        d = db.query(Decision).filter_by(kind="supervisor").one()
        assert d.status == "open" and "person must remove" in d.body


@pytest.mark.parametrize("branch,status,recovered,expected", [
    ("trunk", "new", False, True), (None, "accepted", False, True),
    ("feature", "new", False, False), ("trunk", "resolved", False, False),
    ("trunk", "ignored", False, False), ("trunk", "new", True, False),
])
def test_main_red(world, branch, status, recovered, expected):
    with session_scope() as db:
        signal(db, world, "ci_failure", status=status, data={"branch": branch} if branch else {})
        if recovered:
            db.add(Event(workspace_id=world[0], kind="ci_status", created_at=NOW,
                         data={"repo_id": world[1], "branch": "trunk", "state": "success"}))
    assert bool(findings(world, "main_red")) is expected


@pytest.mark.parametrize("source", ["ci_failure", "smoke_failure"])
@pytest.mark.parametrize("case,expected", [("two_green", True), ("one", False), ("red", False),
    ("after_failure", False), ("other_repo", False), ("after_baseline", False), ("no_merge_time", False)])
def test_combined_break(world, source, case, expected):
    with session_scope() as db:
        signal(db, world, source, first_seen_at=NOW)
        for n in range(1 if case == "one" else 2):
            t = task(db, world, status="completed", pr_number=n + 10, merge_sha=f"sha{n}", evidence={
                "ci": {"state": "failure" if case == "red" and n == 1 else "success"},
                "merge": {} if case == "no_merge_time" else {"at": (NOW + timedelta(minutes=1) if case == "after_failure"
                          else NOW - timedelta(hours=2)).isoformat()}})
            if case == "other_repo" and n == 1:
                r = Repository(workspace_id=world[0], full_name="acme/other")
                db.add(r)
                db.flush()
                t.repo_id = r.id
        if case == "after_baseline":
            db.add(Event(workspace_id=world[0], kind="smoke_passed", created_at=NOW - timedelta(hours=1),
                         data={"repo_id": world[1]}))
    fs = findings(world, "combined_break")
    assert bool(fs) is expected
    if expected:
        assert fs[0].confidence == "inferred"
        surface(world[0], fs[0])
        with session_scope() as db:
            d = db.query(Decision).filter_by(kind="supervisor").one()
            assert "likely" in d.body.lower() and "#10" in d.body and "#11" in d.body


@pytest.mark.parametrize("status,old,expected", [("new", True, True), ("proposed", True, True),
    ("accepted", True, False), ("new", False, False), ("ignored", True, False)])
def test_unaccepted_issue(world, status, old, expected):
    with session_scope() as db:
        signal(db, world, "github_issue", status=status, first_seen_at=OLD if old else NOW,
               data={"acceptance_reason": "Needs permission"})
    fs = findings(world, "unaccepted_issue")
    assert bool(fs) is expected
    if expected:
        assert "Needs permission" in fs[0].body


@pytest.mark.parametrize("status,decision,expected", [("new", None, True), ("new", "open", False),
    ("new", "resolved", True), ("accepted", None, False)])
def test_untriaged_smoke(world, status, decision, expected):
    with session_scope() as db:
        t = task(db, world)
        signal(db, world, "smoke_failure", status=status, url="https://github.com/acme/test/pull/1")
        t.pr_url = "https://github.com/acme/test/pull/1"
        if decision:
            db.add(Decision(workspace_id=world[0], task_id=t.id, kind="unblock", title="Smoke", status=decision))
    assert bool(findings(world, "untriaged_smoke")) is expected


@pytest.mark.parametrize("case,expected", [("failed", True), ("blocked", True), ("passed", False),
    ("no_findings", False), ("message", False), ("revise", False), ("newer_pass", False), ("other_pr", False)])
def test_unrelayed_review(world, case, expected):
    with session_scope() as db:
        t = task(db, world, pr_number=2 if case == "other_pr" else 1)
        outcome = Outcome(workspace_id=world[0], number=2, title="Review", source="review")
        db.add(outcome)
        db.flush()
        review = PullReview(workspace_id=world[0], repo_id=world[1], outcome_id=outcome.id, pr_number=1)
        db.add(review)
        db.flush()
        db.add(ReviewAttempt(review_id=review.id, head_sha="a", base_sha="b", created_at=OLD,
            finished_at=NOW - timedelta(hours=1), status=case if case in {"passed", "blocked"} else "failed",
            verdict={} if case == "no_findings" else {"findings": [{"body": "Fix it"}]}))
        if case == "message":
            db.add(Message(workspace_id=world[0], outcome_id=world[2], task_id=t.id,
                           author_kind="user", body="Fix it", created_at=NOW))
        if case == "revise":
            db.add(Event(workspace_id=world[0], task_id=t.id, kind="stage", data={"stage": "revise"}, created_at=NOW))
        if case == "newer_pass":
            db.add(ReviewAttempt(review_id=review.id, head_sha="new", base_sha="b", status="passed", created_at=NOW))
    assert bool(findings(world, "unrelayed_review")) is expected


@pytest.mark.parametrize("mode", ["off", "observe", "fix"])
def test_sweep_dedup_and_modes(world, mode):
    with session_scope() as db:
        w = db.get(Workspace, world[0])
        w.settings = {**w.settings, "supervisor": {"mode": mode}}
        task(db, world, status="blocked")
        signal(db, world, "github_issue")
    sweep(world[0], NOW)
    sweep(world[0], NOW)
    with session_scope() as db:
        ds = db.query(Decision).filter_by(kind="supervisor").all()
        assert len(ds) == (0 if mode == "off" else 2)
        for d in ds:
            d.status = "dismissed"
        assert db.query(Event).filter_by(kind="supervisor_sweep", actor="supervisor").count() == 2
        assert db.get(Workspace, world[0]).settings["supervisor_state"]["last_sweep_at"]
    sweep(world[0], NOW)
    with session_scope() as db:
        assert db.query(Decision).filter_by(kind="supervisor").count() == len(ds)


@pytest.mark.parametrize("raw", [None, [], "off", {"mode": "bogus"}, {"mode": True}, {"extra": 1},
    {"interval_minutes": 4}, {"interval_minutes": 5.0}, {"stuck_minutes": 0}, {"stuck_minutes": True},
    {"unaccepted_issue_hours": "24"}, {"unaccepted_issue_hours": -1}])
def test_policy_invalid(raw):
    with pytest.raises(ValueError):
        validate_supervisor(raw)


def test_policy_defaults_and_strict_valid():
    assert supervisor_policy()["mode"] == "observe"
    assert supervisor_policy([]) == supervisor_policy()
    assert supervisor_policy({"supervisor": {"mode": "fix", "interval_minutes": 5}})["interval_minutes"] == 5
    assert validate_supervisor({"stuck_minutes": 1, "unaccepted_issue_hours": 1}) == {"stuck_minutes": 1, "unaccepted_issue_hours": 1}


@pytest.mark.parametrize("raw", [{"mode": "bogus"}, [], {"interval_minutes": True}])
def test_settings_patch_422(client, world, raw):
    response = client.patch(f"/api/workspaces/{world[0]}", json={"settings": {"supervisor": raw}})
    assert response.status_code == 422, response.text
    assert "supervisor" in response.text


def test_settings_patch_persists(client, world):
    policy = {"mode": "fix", "interval_minutes": 5, "stuck_minutes": 10, "unaccepted_issue_hours": 2}
    response = client.patch(f"/api/workspaces/{world[0]}", json={"settings": {"supervisor": policy}})
    assert response.status_code == 200, response.text
    assert response.json()["settings"]["supervisor"] == policy
    with session_scope() as db:
        assert db.get(Workspace, world[0]).settings["supervisor"] == policy


def test_supervisor_metrics_not_assistance(world):
    from aifactory.metrics import workspace_metrics
    with session_scope() as db:
        o = db.get(Outcome, world[2])
        o.status, o.completed_at = "completed", NOW
        db.flush()
        before = workspace_metrics(db, world[0])["autonomy"]
        db.add(Event(workspace_id=world[0], outcome_id=o.id, actor="supervisor", kind="supervisor_sweep",
                     created_at=NOW - timedelta(hours=1)))
        db.flush()
        assert workspace_metrics(db, world[0])["autonomy"] == before
        assert before["value"] == 1


def test_acknowledge_only_resolves(world, signed_in):
    from aifactory.decisions import resolve
    from aifactory.models import User
    with session_scope() as db:
        t = task(db, world, status="blocked")
        task_id = t.id
    sweep(world[0], NOW)
    with session_scope() as db:
        d = db.query(Decision).filter_by(kind="supervisor").one()
        resolve(db, d, db.get(User, signed_in["user"]["id"]), "acknowledge")
        assert d.status == "resolved"
        assert db.get(Task, task_id).status == "blocked"


@pytest.mark.parametrize("settings", [[], "legacy", None, {"supervisor": {"mode": "off"}}])
def test_orchestrator_legacy_and_off(world, monkeypatch, settings):
    from aifactory.pipeline.orchestrator import Orchestrator
    import aifactory.pipeline.orchestrator as module
    monkeypatch.setattr(module, "utcnow", lambda: NOW)
    from aifactory import supervisor_policy as policy_module
    original_policy = policy_module.supervisor_policy
    policy_inputs = []

    def checked_policy(raw):
        policy_inputs.append(raw)
        assert isinstance(raw, dict)
        return original_policy(raw)

    monkeypatch.setattr(policy_module, "supervisor_policy", checked_policy)
    with session_scope() as db:
        db.get(Workspace, world[0]).settings = settings
        task(db, world, status="blocked")
    runner = Orchestrator()
    runner._maybe_supervise()
    assert policy_inputs == [settings if isinstance(settings, dict) else {}]
    with session_scope() as db:
        assert db.query(Decision).filter_by(kind="supervisor").count() == (0 if isinstance(settings, dict) else 1)
    runner._last_supervisor = float("-inf")
    runner._maybe_supervise()
    with session_scope() as db:
        assert db.query(Event).filter_by(kind="supervisor_sweep").count() == (0 if isinstance(settings, dict) else 1)


def test_transient_push_protection_not_surfaced(world):
    with session_scope() as db:
        t = task(db, world, status="blocked", blocked_reason="GH013")
        tid = t.id
    sweep(world[0], NOW)
    with session_scope() as db:
        db.get(Task, tid).status = "completed"
    sweep(world[0], NOW + timedelta(minutes=30))
    with session_scope() as db:
        assert db.query(Decision).filter_by(kind="supervisor").count() == 0


@pytest.mark.parametrize("disabled", ["env", "paused", "recent", "invalid_env"])
def test_orchestrator_skips_and_never_raises(world, monkeypatch, disabled):
    from aifactory.pipeline.orchestrator import Orchestrator
    import aifactory.pipeline.orchestrator as module
    monkeypatch.setattr(module, "utcnow", lambda: NOW)
    monkeypatch.setenv("AIFACTORY_SUPERVISOR_INTERVAL_SECONDS", "0" if disabled == "env" else
                       "bogus" if disabled == "invalid_env" else "180")
    with session_scope() as db:
        w = db.get(Workspace, world[0])
        if disabled == "paused":
            w.paused = True
        if disabled == "recent":
            w.settings = {**w.settings, "supervisor_state": {"last_sweep_at": NOW.isoformat()}}
        task(db, world, status="blocked")
    Orchestrator()._maybe_supervise()
    with session_scope() as db:
        assert db.query(Decision).filter_by(kind="supervisor").count() == 0


@pytest.mark.parametrize("status", ["new", "accepted"])
@pytest.mark.parametrize("same_workflow", [True, False])
def test_sync_retires_only_recovered_workflow(world, monkeypatch, status, same_workflow):
    import asyncio
    from aifactory import signals as module

    class GitHub:
        def __init__(self, token):
            pass

        async def issues(self, full_name, **kwargs):
            return []

        async def request(self, method, path, **kwargs):
            green = kwargs.get("params", {}).get("status") == "success"
            return {"workflow_runs": [{"name": "CI" if not green or same_workflow else "Other",
                "created_at": (NOW if green else OLD).isoformat(), "html_url": "https://github.com/run"}]}

    monkeypatch.setattr(module, "GitHub", GitHub)
    with session_scope() as db:
        sig = signal(db, world, "ci_failure", status=status, external_id="acme/test:ci:CI")
        sig.last_seen_at = NOW + timedelta(hours=1)  # legacy poll time is not failure time
        sid = sig.id
    with session_scope() as db:
        asyncio.run(module.sync_github(db, world[0], "test-token"))
    with session_scope() as db:
        assert db.get(Signal, sid).status == ("resolved" if same_workflow else status)
    assert bool(findings(world, "main_red")) is not same_workflow


def test_surface_concurrent_dedup_and_separate_signal_subjects(world):
    from concurrent.futures import ThreadPoolExecutor
    with session_scope() as db:
        signal(db, world, "github_issue")
        signal(db, world, "github_issue")
    fs = findings(world, "unaccepted_issue")
    with ThreadPoolExecutor(max_workers=2) as pool:
        ids = list(pool.map(lambda _: surface(world[0], fs[0]), range(2)))
    assert ids[0] == ids[1]
    assert surface(world[0], fs[1]) != ids[0]
    with session_scope() as db:
        assert db.query(Decision).filter_by(kind="supervisor", status="open").count() == 2


def test_missing_lease_and_cross_workspace_are_not_findings(world):
    with session_scope() as db:
        task(db, world, status="running", lease_expires_at=None)
        assert detect(db, "another-workspace", NOW, supervisor_policy()) == []
    assert findings(world, "stuck_task") == []


def test_sync_preserves_latest_failure_time(world, monkeypatch):
    """An older run from the same poll cannot make a red workflow look recovered."""
    import asyncio
    from aifactory import signals as module

    class GitHub:
        def __init__(self, token):
            pass

        async def issues(self, full_name, **kwargs):
            return []

        async def request(self, method, path, **kwargs):
            times = ([NOW - timedelta(hours=1)] if kwargs["params"]["status"] == "success"
                     else [NOW, OLD])
            return {"workflow_runs": [{"name": "CI", "created_at": at.isoformat(),
                                      "html_url": "https://github.com/run"} for at in times]}

    monkeypatch.setattr(module, "GitHub", GitHub)
    with session_scope() as db:
        asyncio.run(module.sync_github(db, world[0], "test-token"))
    with session_scope() as db:
        sig = db.query(Signal).filter_by(source="ci_failure").one()
        assert sig.data["failure_at"] == NOW.isoformat()
        assert sig.status == "new"
    assert len(findings(world, "main_red")) == 1


@pytest.mark.parametrize("failure_time,expected", [
    (None, True),  # Legacy signals fall back to first_seen_at.
    (NOW - timedelta(hours=1), True),
    (NOW - timedelta(hours=3), False),  # These merges happened after the failed run.
])
def test_combined_break_uses_run_time_not_poll_time(world, failure_time, expected):
    with session_scope() as db:
        signal(db, world, "ci_failure", first_seen_at=NOW,
               data={"branch": "trunk", **({"failure_at": failure_time.isoformat()} if failure_time else {})})
        for n in range(2):
            task(db, world, status="completed", pr_number=20 + n, merge_sha=f"sha{n}",
                 evidence={"ci": {"state": "success"},
                           "merge": {"at": (NOW - timedelta(hours=2)).isoformat()}})
    assert bool(findings(world, "combined_break")) is expected


@pytest.mark.parametrize("resolution", ["acknowledge", "dismiss"])
@pytest.mark.parametrize("link", ["task", "outcome", "both"])
def test_supervisor_resolution_not_assistance(world, signed_in, monkeypatch, resolution, link):
    from aifactory import decisions
    from aifactory.metrics import workspace_metrics
    from aifactory.models import User

    monkeypatch.setattr(decisions, "utcnow", lambda: NOW - timedelta(minutes=1))
    with session_scope() as db:
        o = db.get(Outcome, world[2])
        o.status, o.completed_at = "completed", NOW
        t = task(db, world, status="completed")
        db.flush()
        before = workspace_metrics(db, world[0])["autonomy"]
        assert before["value"] == 1
        d = Decision(workspace_id=world[0], kind="supervisor", title="Notification",
                     task_id=t.id if link in {"task", "both"} else None,
                     outcome_id=o.id if link in {"outcome", "both"} else None,
                     options=[{"value": "acknowledge"}, {"value": "dismiss"}])  # what the Supervisor offers
        db.add(d)
        db.flush()
        decisions.resolve(db, d, db.get(User, signed_in["user"]["id"]), resolution)
        event = db.query(Event).filter_by(kind="decision_resolved").one()
        event.created_at = NOW - timedelta(minutes=1)
        db.flush()
        assert workspace_metrics(db, world[0])["autonomy"] == before
        # Exempt only Supervisor notifications, not actual human intervention.
        d.kind = "unblock"
        db.flush()
        assert workspace_metrics(db, world[0])["autonomy"]["value"] == 0


@pytest.mark.parametrize("status", ["new", "accepted"])
@pytest.mark.parametrize("stored_failure", [None, OLD])
def test_sync_keeps_new_failure_beyond_first_three_runs(world, monkeypatch, status, stored_failure):
    """Recovery must consider every fetched run, not just the three upserted ones."""
    import asyncio
    from aifactory import signals as module

    class GitHub:
        def __init__(self, token):
            pass

        async def issues(self, full_name, **kwargs):
            return []

        async def request(self, method, path, **kwargs):
            if kwargs["params"]["status"] == "success":
                runs = [("CI", NOW - timedelta(hours=1))]
            else:
                runs = [(f"Other-{n}", NOW + timedelta(minutes=3 - n)) for n in range(3)]
                runs += [("CI", NOW), ("CI", OLD)]
            return {"workflow_runs": [{"name": name, "created_at": at.isoformat(),
                                      "html_url": "https://github.com/run"} for name, at in runs]}

    monkeypatch.setattr(module, "GitHub", GitHub)
    with session_scope() as db:
        sig = signal(db, world, "ci_failure", status=status, external_id="acme/test:ci:CI",
                     data={"branch": "trunk", "workflow": "CI",
                           **({"failure_at": stored_failure.isoformat()} if stored_failure else {})})
        sid = sig.id
    with session_scope() as db:
        asyncio.run(module.sync_github(db, world[0], "test-token"))
    with session_scope() as db:
        sig = db.get(Signal, sid)
        assert sig.status == status
        from aifactory.supervisor import timestamp
        assert timestamp(sig.data["failure_at"]) == timestamp(NOW)
    assert sid in {f.subject for f in findings(world, "main_red")}


@pytest.mark.parametrize("green_workflow,expected", [("Build", False), ("Other", True)])
@pytest.mark.parametrize("green_kind", ["ci_status", "main_green"])
def test_combined_break_baseline_matches_failing_workflow(world, green_workflow, expected, green_kind):
    with session_scope() as db:
        signal(db, world, "ci_failure", first_seen_at=NOW,
               data={"branch": "trunk", "workflow": "Build", "failure_at": NOW.isoformat()})
        for n in range(2):
            task(db, world, status="completed", pr_number=30 + n, merge_sha=f"sha{n}",
                 evidence={"ci": {"state": "success"},
                           "merge": {"at": (NOW - timedelta(hours=2)).isoformat()}})
        db.add(Event(workspace_id=world[0], kind=green_kind, created_at=NOW - timedelta(hours=1),
                     data={"repo_id": world[1], "branch": "trunk", "state": "success",
                           "workflow": green_workflow}))
    assert bool(findings(world, "combined_break")) is expected
