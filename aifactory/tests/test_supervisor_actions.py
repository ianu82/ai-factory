"""Safe Supervisor interventions, incident identity and read-only observation."""
from copy import deepcopy
from datetime import timedelta
from types import SimpleNamespace

import pytest
from sqlalchemy import inspect

from aifactory.db import session_scope
from aifactory.models import Decision, Event, Message, Outcome, PullReview, Repository, ReviewAttempt, Run, Signal, Task, Workspace
from aifactory.pipeline.orchestrator import orchestrator
from aifactory.pipeline.runtime import guidance_history, take_guidance
from aifactory.supervisor import act, detect, surface, sweep
from aifactory.supervisor_policy import supervisor_policy
from tests.fakes import FakeGitHub
from tests.test_supervisor import NOW, OLD, signal, task, world  # noqa: F401 (shared DB fixture)


@pytest.fixture(autouse=True)
def fix_mode(world, monkeypatch):
    with session_scope() as db:
        w = db.get(Workspace, world[0])
        w.settings = {**w.settings, "supervisor": {"mode": "fix"}}
    wakes = []
    monkeypatch.setattr(orchestrator, "wake", lambda: wakes.append(True))
    monkeypatch.setattr(orchestrator, "_tasks", {})
    monkeypatch.setattr("aifactory.github.GitHub", FakeGitHub)
    monkeypatch.setattr("aifactory.pipeline.delivery.GitHub", FakeGitHub)
    monkeypatch.setattr(FakeGitHub, "pulls", {})
    monkeypatch.setattr(FakeGitHub, "merged", [])
    return wakes


def expired_task(db, world, **fields):
    fields.setdefault("status", "running")
    fields.setdefault("lease_expires_at", NOW - timedelta(minutes=10))
    fields.setdefault("lease_owner", "dead-instance")
    return task(db, world, **fields)


def review_task(db, world, **fields):
    fields.setdefault("status", "failed")
    fields.setdefault("attempt", 3)
    fields.setdefault("head_sha", "head")
    t = task(db, world, pr_number=1, **fields)
    o = Outcome(workspace_id=world[0], number=2, title="Review", source="review")
    db.add(o)
    db.flush()
    r = PullReview(workspace_id=world[0], repo_id=world[1], outcome_id=o.id, pr_number=1, head_sha="head")
    db.add(r)
    db.flush()
    a = ReviewAttempt(review_id=r.id, head_sha="head", base_sha="base", status="failed", created_at=OLD,
        finished_at=NOW - timedelta(hours=1), verdict={"findings": [{"id": "R1", "body": "Handle the missing value"}]})
    db.add(a)
    db.flush()
    return t, a


def action_events(db):
    return db.query(Event).filter_by(kind="supervisor_action", actor="supervisor").all()


def snapshot(db, model):
    return {row.id: deepcopy({c.key: getattr(row, c.key) for c in inspect(model).columns})
            for row in db.query(model)}


def test_expired_lease_requeues_once_then_surfaces(world, fix_mode):
    with session_scope() as db:
        t = expired_task(db, world, attempt=2, evidence={"budget_epoch": 4})
        tid = t.id
    sweep(world[0], NOW)
    with session_scope() as db:
        t = db.get(Task, tid)
        assert (t.status, t.lease_owner, t.lease_expires_at, t.attempt) == ("queued", "", None, 2)
        assert t.evidence["supervisor_requeues"]["epoch"] == 4
        assert len(action_events(db)) == 1
        assert db.query(Decision).filter_by(kind="supervisor").count() == 0
    assert fix_mode == [True]
    sweep(world[0], NOW)
    sweep(world[0], NOW)
    with session_scope() as db:
        assert db.get(Task, tid).status == "queued"
        assert len(action_events(db)) == 1
        assert db.query(Decision).filter_by(kind="supervisor", status="open").count() == 1
    assert fix_mode == [True]


def test_lease_recovery_is_once_per_budget_epoch(world, fix_mode):
    with session_scope() as db:
        tid = expired_task(db, world).id
    sweep(world[0], NOW)
    with session_scope() as db:
        t = db.get(Task, tid)
        t.status, t.updated_at, t.lease_expires_at = "running", OLD, OLD
    sweep(world[0], NOW)
    with session_scope() as db:
        assert db.get(Task, tid).status == "running"
        assert len(action_events(db)) == 1
        t = db.get(Task, tid)
        db.query(Task).filter_by(id=tid).update({Task.evidence: {**t.evidence, "budget_epoch": 1},
                                                 Task.updated_at: OLD})
    sweep(world[0], NOW)
    with session_scope() as db:
        assert db.get(Task, tid).status == "queued"
        assert len(action_events(db)) == 2


@pytest.mark.parametrize("case", ["live_job", "renewed_lease", "changed_status", "paused", "observe", "merge", "revert", "release"])
def test_recovery_rechecks_live_state_and_policy(world, fix_mode, case):
    with session_scope() as db:
        tid = expired_task(db, world).id
        f = detect(db, world[0], NOW, supervisor_policy())[0]
    if case == "live_job":
        orchestrator._tasks[tid] = (SimpleNamespace(done=lambda: False), None)
    with session_scope() as db:
        t = db.get(Task, tid)
        w = db.get(Workspace, world[0])
        if case == "renewed_lease":
            t.lease_expires_at = NOW + timedelta(hours=1)
        elif case == "changed_status":
            t.status = "completed"
        elif case == "paused":
            w.paused = True
        elif case == "observe":
            w.settings = {**w.settings, "supervisor": {"mode": "observe"}}
        elif case in {"merge", "revert", "release"}:
            t.stage = case
        db.flush()
        before = snapshot(db, Task)
    with session_scope() as db:
        act(db, world[0], f, {"mode": "fix"}, NOW)
    with session_scope() as db:
        assert snapshot(db, Task) == before
        assert action_events(db) == []
    assert fix_mode == []


@pytest.mark.parametrize("existing", [False, True])
def test_smoke_links_merged_title_and_opens_unblock_without_revert(world, existing):
    with session_scope() as db:
        t = task(db, world, status="smoke_failed", merge_sha="merged", pr_number=1)
        t.title = "Ship a fix"
        tid = t.id
        sig = signal(db, world, "smoke_failure", data={"keep": True})
        sig.title = "Smoke failed after merging Ship a fix"
        sid = sig.id
        if existing:
            db.add(Decision(workspace_id=world[0], task_id=t.id, outcome_id=t.outcome_id,
                            kind="unblock", title="Existing smoke decision"))
        repo = db.get(Repository, world[1])
        repo.config = {"recovery": {"smoke_failure": "revert"}}
    sweep(world[0], NOW)
    sweep(world[0], NOW)
    with session_scope() as db:
        sig = db.get(Signal, sid)
        assert sig.status == "proposed" and sig.data == {"task_id": tid, "keep": True}
        assert db.query(Decision).filter_by(kind="unblock", task_id=tid, status="open").count() == 1
        assert len([e for e in action_events(db) if e.data["action"] == "triage_smoke"]) == 1
        assert db.get(Task, tid).status == "smoke_failed"
        assert not db.get(Task, tid).evidence.get("revert_candidate")
    assert FakeGitHub.pulls == {} and FakeGitHub.merged == []


@pytest.mark.parametrize("case", ["missing_title", "unmerged", "ambiguous", "other_repo"])
def test_smoke_ambiguous_or_unmerged_is_surface_only(world, case):
    with session_scope() as db:
        sig = signal(db, world, "smoke_failure")
        sig.title = "Smoke failed after merging Task"
        sid = sig.id
        if case != "missing_title":
            t = task(db, world, status="completed", merge_sha="" if case == "unmerged" else "merged")
            if case == "other_repo":
                sig.repo_id = "deleted"
            if case == "ambiguous":
                task(db, world, status="completed", merge_sha="another")
    sweep(world[0], NOW)
    with session_scope() as db:
        assert db.get(Signal, sid).status == "new"
        assert db.query(Decision).filter_by(kind="supervisor").count() == 1
        assert action_events(db) == []


@pytest.mark.parametrize("recovery", ["event", "resolved_signal", "deleted_repo"])
def test_stale_main_red_resolves_once(world, recovery):
    with session_scope() as db:
        sig = signal(db, world, "ci_failure", data={"branch": "trunk", "workflow": "CI", "failure_at": OLD.isoformat()})
        sid = sig.id
        if recovery == "deleted_repo":
            sig.repo_id = "no-longer-exists"
        elif recovery == "event":
            db.add(Event(workspace_id=world[0], kind="main_green", created_at=NOW,
                data={"repo_id": world[1], "branch": "trunk", "workflow": "CI"}))
        else:
            green = signal(db, world, "ci_failure", status="resolved", data={"branch": "trunk", "workflow": "CI"})
            green.last_seen_at = NOW
    sweep(world[0], NOW)
    sweep(world[0], NOW)
    with session_scope() as db:
        assert db.get(Signal, sid).status == "resolved"
        assert len(action_events(db)) == 1
        assert db.query(Decision).filter_by(kind="supervisor").count() == 0


@pytest.mark.parametrize("case", ["still_red", "older_green", "other_workflow", "pr_green", "other_repo"])
def test_unproven_main_recovery_surfaces(world, case):
    with session_scope() as db:
        sid = signal(db, world, "ci_failure", data={"branch": "trunk", "workflow": "CI", "failure_at": OLD.isoformat()}).id
        if case != "still_red":
            db.add(Event(workspace_id=world[0], kind="main_green", created_at=OLD - timedelta(hours=1) if case == "older_green" else NOW,
                data={"repo_id": "other" if case == "other_repo" else world[1],
                      "branch": "feature" if case == "pr_green" else "trunk",
                      "workflow": "Other" if case == "other_workflow" else "CI"}))
    sweep(world[0], NOW)
    with session_scope() as db:
        assert db.get(Signal, sid).status == "new"
        assert len(action_events(db)) == 0
        assert db.query(Decision).filter_by(kind="supervisor").count() == 1


def test_review_relay_is_non_user_guidance_exactly_once(world, fix_mode):
    with session_scope() as db:
        t, a = review_task(db, world, evidence={"budget_epoch": 2, "preserve": "value"})
        tid, aid = t.id, a.id
    live = take_guidance(world[2], tid, since=OLD)
    other = take_guidance(world[2], "another-task", since=OLD)
    sweep(world[0], NOW)
    sweep(world[0], NOW)
    with session_scope() as db:
        t = db.get(Task, tid)
        m = db.query(Message).filter_by(task_id=tid).one()
        assert m.author_kind == "factory" and m.author_id is None
        assert m.attribution["review_attempt_id"] == aid
        assert "Handle the missing value" in m.body and "R1" in m.body
        assert (t.status, t.stage, t.attempt) == ("queued", "revise", 3)
        assert t.evidence == {"budget_epoch": 2, "preserve": "value", "auto_retries": 1, "attempt_base": 3}
        assert len(action_events(db)) == 1
        assert db.query(Decision).filter_by(kind="supervisor").count() == 1  # non-actionable stuck-task finding
        body = m.body
    assert guidance_history(world[2], tid) == [body]
    assert guidance_history(world[2], "another-task") == []
    assert live() == [body] and live() == [] and other() == []
    assert fix_mode == [True]


@pytest.mark.parametrize("status,used,rounds,retry", [
    ("blocked", 0, 1, True), ("failed", 1, 2, True), ("failed", 1, 1, False),
    ("failed", 0, 0, False), ("running", 0, 1, False), ("completed", 0, 1, False),
])
def test_review_retry_respects_status_and_shared_rounds(world, status, used, rounds, retry):
    with session_scope() as db:
        t, _ = review_task(db, world, status=status, evidence={"auto_retries": used}, updated_at=NOW)
        tid = t.id
        w = db.get(Workspace, world[0])
        w.settings = {**w.settings, "auto_retry_rounds": rounds}
    sweep(world[0], NOW)
    sweep(world[0], NOW + timedelta(minutes=5))
    with session_scope() as db:
        assert db.query(Message).filter_by(task_id=tid).count() == 1
        t = db.get(Task, tid)
        assert (t.status == "queued") is retry
        assert t.evidence["auto_retries"] == used + int(retry)
        assert db.query(Decision).filter_by(kind="supervisor", status="open").count() == int(not retry)


@pytest.mark.parametrize("case", ["budget_gate", "spent", "unknown_cost", "merge_gate", "provider_hold", "push_protection", "old_head"])
def test_review_cannot_override_safety_gates(world, case):
    with session_scope() as db:
        t, _ = review_task(db, world, updated_at=NOW)
        tid = t.id
        if case in {"budget_gate", "merge_gate", "provider_hold"}:
            db.add(Decision(workspace_id=world[0], task_id=tid, outcome_id=world[2], title="Human gate",
                kind={"budget_gate": "budget", "merge_gate": "approve_merge", "provider_hold": "provider_hold"}[case]))
        elif case in {"spent", "unknown_cost"}:
            db.add(Run(workspace_id=world[0], outcome_id=world[2], task_id=tid, role="builder", harness="native",
                       cost_usd=100, cost_known=case != "unknown_cost"))
        elif case == "old_head":
            t.head_sha = "new-head"
        else:
            t.blocked_reason = "GH013: push protection"
    sweep(world[0], NOW)
    sweep(world[0], NOW)
    with session_scope() as db:
        assert db.get(Task, tid).status == "failed"
        assert db.get(Task, tid).evidence.get("auto_retries", 0) == 0
        assert db.query(Decision).filter_by(kind="supervisor", status="open").count() >= 1
        assert db.query(Message).count() == int(case != "old_head")


@pytest.mark.parametrize("mode", ["observe", "off"])
def test_observe_and_off_change_no_task_or_signal_rows(world, mode):
    with session_scope() as db:
        expired_task(db, world)
        review_task(db, world)
        task(db, world, status="smoke_failed", merge_sha="merge")
        smoke = signal(db, world, "smoke_failure")
        smoke.title = "Smoke failed after merging Task"
        ci = signal(db, world, "ci_failure")
        ci.repo_id = "deleted"
        signal(db, world, "github_issue")
        task(db, world, status="blocked", blocked_reason="GH013")
        w = db.get(Workspace, world[0])
        w.settings = {**w.settings, "supervisor": {"mode": mode}}
        db.flush()
        before = snapshot(db, Task), snapshot(db, Signal), db.query(Outcome).count()
    sweep(world[0], NOW)
    sweep(world[0], NOW + timedelta(minutes=30))
    with session_scope() as db:
        assert (snapshot(db, Task), snapshot(db, Signal), db.query(Outcome).count()) == before
        assert db.query(Message).count() == 0
        assert action_events(db) == []


@pytest.mark.parametrize("mode", ["off", "observe", "fix"])
def test_push_protection_and_unaccepted_issue_never_retry_or_create_outcomes(world, mode):
    with session_scope() as db:
        expired_task(db, world, blocked_reason="GH013 push protection")
        signal(db, world, "github_issue", data={"labels": ["factory"]})
        w = db.get(Workspace, world[0])
        w.settings = {**w.settings, "supervisor": {"mode": mode}}
        before = db.query(Outcome).count()
        db.flush()
        tasks, signals = snapshot(db, Task), snapshot(db, Signal)
    sweep(world[0], NOW)
    sweep(world[0], NOW + timedelta(minutes=30))
    with session_scope() as db:
        assert snapshot(db, Task) == tasks and snapshot(db, Signal) == signals
        assert db.query(Outcome).count() == before
        assert action_events(db) == []
        kinds = {(d.metadata_json or {}).get("finding_kind") for d in db.query(Decision).filter_by(kind="supervisor")}
        assert kinds == (set() if mode == "off" else {"push_protection", "unaccepted_issue"})
    assert FakeGitHub.pulls == {} and FakeGitHub.merged == []


@pytest.mark.parametrize("kind", ["stuck_task", "push_protection"])
def test_timestamp_updates_keep_incident_fingerprint(world, kind):
    with session_scope() as db:
        t = task(db, world, status="blocked", blocked_reason="GH013" if kind == "push_protection" else "Error")
        tid = t.id
        first = next(f for f in detect(db, world[0], NOW, supervisor_policy()) if f.kind == kind)
    with session_scope() as db:
        db.get(Task, tid).updated_at = OLD + timedelta(hours=1)
    with session_scope() as db:
        second = next(f for f in detect(db, world[0], NOW, supervisor_policy()) if f.kind == kind)
    assert first.fingerprint == second.fingerprint
    assert surface(world[0], first) == surface(world[0], second)


@pytest.mark.parametrize("resolution", ["resolved", "dismissed"])
def test_acknowledged_findings_stay_closed_until_material_change(world, resolution):
    with session_scope() as db:
        t = expired_task(db, world)
        tid = t.id
        w = db.get(Workspace, world[0])
        w.settings = {**w.settings, "supervisor": {"mode": "observe"}}
    sweep(world[0], NOW)
    with session_scope() as db:
        d = db.query(Decision).filter_by(kind="supervisor").one()
        d.status = resolution
        t = db.get(Task, tid)
        t.updated_at = OLD + timedelta(hours=1)
        w = db.get(Workspace, world[0])
        w.settings = {**w.settings, "supervisor": {"mode": "fix"}}
    sweep(world[0], NOW)
    with session_scope() as db:
        assert db.get(Task, tid).status == "running"
        assert db.query(Decision).count() == 1
        assert action_events(db) == []
        t = db.get(Task, tid)
        db.query(Task).filter_by(id=tid).update({Task.evidence: {**t.evidence, "budget_epoch": 1},
                                                 Task.updated_at: OLD})
    sweep(world[0], NOW)
    with session_scope() as db:
        assert db.get(Task, tid).status == "queued"
        assert len(action_events(db)) == 1


def test_internal_notes_are_not_guidance(world):
    with session_scope() as db:
        t = task(db, world)
        tid = t.id
        for author, attribution in [("factory", None), ("system", {"source": "supervisor", "guidance": True}),
                                     ("factory", {"source": "supervisor"}), ("factory", {"guidance": True})]:
            db.add(Message(workspace_id=world[0], outcome_id=world[2], task_id=tid, author_kind=author,
                           body="Not guidance", attribution=attribution, created_at=OLD))
    assert guidance_history(world[2], tid) == []
    assert take_guidance(world[2], tid, since=OLD)() == []


def test_supervisor_actions_count_as_assisted_not_unattended(world):
    from aifactory.metrics import workspace_metrics

    with session_scope() as db:
        expired_task(db, world)
    sweep(world[0], NOW)
    with session_scope() as db:
        o = db.get(Outcome, world[2])
        o.status, o.completed_at = "completed", NOW + timedelta(hours=1)
        for e in action_events(db):
            e.created_at = NOW
        db.flush()
        assert workspace_metrics(db, world[0])["autonomy"]["value"] == 0


@pytest.mark.parametrize("kind", ["stuck_task", "unrelayed_review"])
def test_competing_actions_apply_once(world, fix_mode, kind):
    from concurrent.futures import ThreadPoolExecutor

    with session_scope() as db:
        if kind == "stuck_task":
            expired_task(db, world)
        else:
            review_task(db, world, updated_at=NOW)
        f = next(f for f in detect(db, world[0], NOW, supervisor_policy()) if f.kind == kind)

    def apply(_):
        with session_scope() as db:
            return act(db, world[0], f, {"mode": "fix"}, NOW)

    with ThreadPoolExecutor(max_workers=2) as pool:
        list(pool.map(apply, range(2)))
    with session_scope() as db:
        assert len(action_events(db)) == 1
        assert db.query(Message).count() == int(kind == "unrelayed_review")
    assert fix_mode == [True]


def test_rolled_back_action_does_not_wake_or_consume_recovery(world, fix_mode):
    with session_scope() as db:
        tid = expired_task(db, world).id
        f = detect(db, world[0], NOW, supervisor_policy())[0]
    with pytest.raises(RuntimeError, match="transaction failed"):
        with session_scope() as db:
            assert act(db, world[0], f, {"mode": "fix"}, NOW)
            raise RuntimeError("transaction failed")
    with session_scope() as db:
        assert db.get(Task, tid).status == "running"
        assert action_events(db) == []
    assert fix_mode == []
    sweep(world[0], NOW)
    assert fix_mode == [True]


def test_main_red_fingerprint_tracks_new_failure_not_poll_time(world):
    with session_scope() as db:
        sig = signal(db, world, "ci_failure", data={"branch": "trunk", "workflow": "CI", "failure_at": OLD.isoformat()})
        sid = sig.id
        first = next(f for f in detect(db, world[0], NOW, supervisor_policy()) if f.kind == "main_red")
    with session_scope() as db:
        db.get(Signal, sid).last_seen_at = NOW
    with session_scope() as db:
        polled = next(f for f in detect(db, world[0], NOW, supervisor_policy()) if f.kind == "main_red")
        sig = db.get(Signal, sid)
        sig.data = {**sig.data, "failure_at": (NOW - timedelta(hours=1)).isoformat()}
    with session_scope() as db:
        changed = next(f for f in detect(db, world[0], NOW, supervisor_policy()) if f.kind == "main_red")
    assert first.fingerprint == polled.fingerprint
    assert first.fingerprint != changed.fingerprint


def test_combined_break_uses_ci_snapshot_at_merge(world):
    with session_scope() as db:
        signal(db, world, "ci_failure", first_seen_at=NOW,
               data={"branch": "trunk", "failure_at": NOW.isoformat()})
        ids = []
        for n in range(2):
            t = task(db, world, status="completed", pr_number=n + 1, merge_sha=f"merge-{n}",
                evidence={"ci": {"state": "failure"}, "merge": {
                    "at": (NOW - timedelta(hours=2)).isoformat(), "ci": {"state": "success"}}})
            ids.append(t.id)
        assert any(f.kind == "combined_break" for f in detect(db, world[0], NOW, supervisor_policy()))
    with session_scope() as db:
        for tid in ids:
            db.add(Event(workspace_id=world[0], task_id=tid, kind="ci_status", created_at=OLD,
                         data={"state": "success"}))
            t = db.get(Task, tid)
            t.evidence = {**t.evidence, "ci": {"state": "success"}, "merge": {
                **t.evidence["merge"], "ci": {"state": "failure"}}}
    with session_scope() as db:
        assert not any(f.kind == "combined_break" for f in detect(db, world[0], NOW, supervisor_policy()))


def test_review_relay_and_requeue_are_visible_through_api(world, client):
    with session_scope() as db:
        t, _ = review_task(db, world, updated_at=NOW)
        tid = t.id
    sweep(world[0], NOW)
    response = client.get(f"/api/workspaces/{world[0]}/outcomes/{world[2]}")
    assert response.status_code == 200
    visible = next(t for t in response.json()["tasks"] if t["id"] == tid)
    assert visible["status"] == "queued" and visible["stage"] == "revise"
    response = client.get(f"/api/workspaces/{world[0]}/events")
    assert response.status_code == 200
    assert any(e["kind"] == "supervisor_action" and e["actor"] == "supervisor" for e in response.json())
    response = client.get(f"/api/workspaces/{world[0]}/outcomes/{world[2]}/conversation")
    assert response.status_code == 200
    assert "Handle the missing value" in response.text


def test_review_on_superseded_head_is_not_relayed(world):
    with session_scope() as db:
        t, a = review_task(db, world, updated_at=NOW)
        tid = t.id
        db.get(PullReview, a.review_id).head_sha = "new-head-awaiting-review"
    sweep(world[0], NOW)
    with session_scope() as db:
        assert db.get(Task, tid).status == "failed"
        assert db.query(Message).count() == 0
        assert db.query(Decision).filter_by(kind="supervisor").count() == 1


def test_push_protection_fingerprint_keeps_run_evidence(world):
    with session_scope() as db:
        t = task(db, world, status="blocked")
        tid = t.id
        run = Run(workspace_id=world[0], outcome_id=world[2], task_id=tid, role="builder", harness="native",
                  error="GH013: secret scanning blocked the push", started_at=OLD)
        db.add(run)
        db.flush()
        first = next(f for f in detect(db, world[0], NOW, supervisor_policy()) if f.kind == "push_protection")
        assert first.evidence["run_id"] == run.id
    with session_scope() as db:
        db.add(Run(workspace_id=world[0], outcome_id=world[2], task_id=tid, role="builder", harness="native",
                   error="GH013: another secret", started_at=NOW))
    with session_scope() as db:
        second = next(f for f in detect(db, world[0], NOW, supervisor_policy()) if f.kind == "push_protection")
    assert first.fingerprint != second.fingerprint


@pytest.mark.parametrize("rollback", [False, True])
@pytest.mark.parametrize("recorded_cost", [0, 0.5])
def test_lease_recovery_closes_interrupted_runs_atomically(world, fix_mode, rollback, recorded_cost):
    with session_scope() as db:
        t = expired_task(db, world)
        tid = t.id
        other = task(db, world)
        abandoned = Run(workspace_id=world[0], outcome_id=world[2], task_id=tid,
                        role="builder", harness="native", started_at=OLD, cost_usd=recorded_cost)
        unrelated = Run(workspace_id=world[0], outcome_id=world[2], task_id=other.id,
                        role="builder", harness="native", started_at=OLD)
        db.add_all([abandoned, unrelated])
        db.flush()
        rid, unrelated_id = abandoned.id, unrelated.id
        last_seen = OLD + timedelta(minutes=5)
        db.add(Event(workspace_id=world[0], outcome_id=world[2], task_id=tid, run_id=rid,
                     kind="agent_progress", created_at=last_seen))
        finding = next(f for f in detect(db, world[0], NOW, supervisor_policy()) if f.subject == tid)
    if rollback:
        with pytest.raises(RuntimeError, match="rollback"):
            with session_scope() as db:
                assert not act(db, world[0], finding, {"mode": "fix"}, NOW)
                assert db.get(Run, rid).cost_known is False
                raise RuntimeError("rollback")
    else:
        sweep(world[0], NOW)
        sweep(world[0], NOW)
    with session_scope() as db:
        run = db.get(Run, rid)
        assert run.status == ("running" if rollback else "interrupted")
        assert run.cost_known is rollback
        assert run.cost_usd == recorded_cost
        assert run.finished_at == (None if rollback else last_seen.replace(tzinfo=None))
        t = db.get(Task, tid)
        assert (t.status, t.lease_owner, t.lease_expires_at) == ("running", "dead-instance", NOW - timedelta(minutes=10))
        assert "supervisor_requeues" not in t.evidence
        assert db.get(Run, unrelated_id).status == "running"
        assert action_events(db) == []
        assert db.query(Decision).filter_by(kind="supervisor", status="open", task_id=tid).count() == int(not rollback)
        assert db.query(Event).filter_by(kind="runs_interrupted").count() == int(not rollback)
    assert fix_mode == []


@pytest.mark.parametrize("retry", [False, True])
@pytest.mark.parametrize("rollback", [False, True])
def test_review_retry_retires_unblock_decisions_atomically(world, fix_mode, retry, rollback):
    from aifactory.decisions import DecisionError, resolve

    with session_scope() as db:
        t, attempt = review_task(db, world, updated_at=NOW, evidence={"auto_retries": 0 if retry else 1})
        tid = t.id
        cards = [Decision(workspace_id=world[0], outcome_id=world[2], task_id=tid,
                          kind="unblock", title="Worker failed",
                          metadata_json={"review_attempt_id": attempt.id},
                          options=[{"value": "retry"}, {"value": "abandon"}]) for _ in range(2)]
        db.add_all(cards)
        db.flush()
        ids = [d.id for d in cards]
        finding = next(f for f in detect(db, world[0], NOW, supervisor_policy()) if f.kind == "unrelayed_review")
    if rollback:
        with pytest.raises(RuntimeError, match="rollback"):
            with session_scope() as db:
                assert act(db, world[0], finding, {"mode": "fix"}, NOW) is retry
                raise RuntimeError("rollback")
    else:
        sweep(world[0], NOW)
        sweep(world[0], NOW)
    with session_scope() as db:
        for did in ids:
            card = db.get(Decision, did)
            assert card.status == ("superseded" if retry and not rollback else "open")
            if retry and not rollback:
                assert card.resolved_at == NOW.replace(tzinfo=None)
                assert card.resolved_by is None
                assert card.attribution["source"] == "supervisor"
                for choice in ("retry", "abandon"):
                    with pytest.raises(DecisionError, match="already resolved"):
                        resolve(db, card, None, choice)
        assert db.get(Task, tid).status == ("queued" if retry and not rollback else "failed")
        assert db.query(Message).filter_by(task_id=tid).count() == int(not rollback)
    assert fix_mode == ([True] if retry and not rollback else [])


def unrelated_history(db, world):
    """A busy workspace must not evict another incident's durable evidence."""
    db.add_all([Event(workspace_id=world[0], kind="ci_status", created_at=NOW,
                      data={"repo_id": "unrelated", "branch": "trunk", "state": "failure"})
                for _ in range(2001)])


def test_lease_recovery_survives_unrelated_history(world, fix_mode):
    with session_scope() as db:
        tid = expired_task(db, world, evidence={"budget_epoch": 4}).id
    sweep(world[0], NOW)
    with session_scope() as db:
        unrelated_history(db, world)
        t = db.get(Task, tid)
        t.status, t.updated_at, t.lease_expires_at = "running", OLD, OLD
    sweep(world[0], NOW)
    sweep(world[0], NOW)
    with session_scope() as db:
        assert db.get(Task, tid).status == "running"
        assert db.get(Task, tid).evidence["supervisor_requeues"]["epoch"] == 4
        assert len(action_events(db)) == 1
        assert db.query(Decision).filter_by(kind="supervisor", status="open", task_id=tid).count() == 1
    assert fix_mode == [True]


@pytest.mark.parametrize("legacy", [False, True])
def test_revision_history_survives_workspace_noise_and_age(world, legacy):
    with session_scope() as db:
        t, a = review_task(db, world)
        tid = t.id
        revised_at = NOW - timedelta(days=45) if legacy else NOW - timedelta(minutes=30)
        a.finished_at = revised_at - timedelta(hours=1)
        db.add(Event(workspace_id=world[0], task_id=tid, kind="stage", created_at=revised_at,
                     data={"stage": "revise"}))
        unrelated_history(db, world)
    sweep(world[0], NOW)
    sweep(world[0], NOW)
    with session_scope() as db:
        assert db.get(Task, tid).status == "failed"
        assert db.query(Message).count() == 0
        assert action_events(db) == []
        assert not any(d.metadata_json["finding_kind"] == "unrelayed_review"
                       for d in db.query(Decision).filter_by(kind="supervisor"))


@pytest.mark.parametrize("mode", ["observe", "fix"])
@pytest.mark.parametrize("legacy", [False, True])
def test_ci_recovery_history_survives_workspace_noise_and_age(world, mode, legacy):
    with session_scope() as db:
        failed_at = NOW - timedelta(days=50) if legacy else OLD
        sid = signal(db, world, "ci_failure", first_seen_at=failed_at,
                     data={"branch": "trunk", "workflow": "CI", "failure_at": failed_at.isoformat()}).id
        db.add(Event(workspace_id=world[0], kind="main_green", created_at=failed_at + timedelta(hours=1),
                     data={"repo_id": world[1], "branch": "trunk", "workflow": "CI"}))
        unrelated_history(db, world)
        w = db.get(Workspace, world[0])
        w.settings = {**w.settings, "supervisor": {"mode": mode}}
    sweep(world[0], NOW)
    sweep(world[0], NOW)
    with session_scope() as db:
        assert db.get(Signal, sid).status == ("resolved" if mode == "fix" else "new")
        assert db.query(Decision).filter_by(kind="supervisor").count() == 0
        assert len(action_events(db)) == int(mode == "fix")


def test_combined_break_keeps_old_baseline_despite_workspace_noise(world):
    with session_scope() as db:
        signal(db, world, "ci_failure", first_seen_at=NOW,
               data={"branch": "trunk", "workflow": "CI", "failure_at": NOW.isoformat()})
        for n in range(2):
            task(db, world, status="completed", pr_number=n + 1, merge_sha=f"merge-{n}",
                 evidence={"merge": {"at": (NOW - timedelta(days=50)).isoformat(), "ci": {"state": "success"}}})
        db.add(Event(workspace_id=world[0], kind="main_green", created_at=NOW - timedelta(days=45),
                     data={"repo_id": world[1], "branch": "trunk", "workflow": "CI"}))
        unrelated_history(db, world)
        assert not any(f.kind == "combined_break" for f in detect(db, world[0], NOW, supervisor_policy()))


@pytest.mark.parametrize("resolution", ["resolved", "dismissed"])
@pytest.mark.parametrize("recovery", ["event", "resolved_signal", "deleted_repo"])
def test_acknowledged_red_does_not_suppress_new_recovery(world, resolution, recovery):
    with session_scope() as db:
        sid = signal(db, world, "ci_failure", data={
            "branch": "trunk", "workflow": "CI", "failure_at": OLD.isoformat()}).id
    sweep(world[0], NOW)
    with session_scope() as db:
        d = db.query(Decision).filter_by(kind="supervisor").one()
        red_fingerprint = d.metadata_json["fingerprint"]
        d.status = resolution
        if recovery == "event":
            db.add(Event(workspace_id=world[0], kind="main_green", created_at=NOW,
                         data={"repo_id": world[1], "branch": "trunk", "workflow": "CI"}))
        elif recovery == "resolved_signal":
            signal(db, world, "ci_failure", status="resolved", data={
                "branch": "trunk", "workflow": "CI", "resolved_at": NOW.isoformat()})
        else:
            db.get(Signal, sid).repo_id = "deleted-repo"
    with session_scope() as db:
        recovered = next(f for f in detect(db, world[0], NOW, supervisor_policy({"supervisor": {"mode": "fix"}}))
                         if f.kind == "main_red")
        assert recovered.fingerprint != red_fingerprint
        assert recovered.evidence["recovery"]
    sweep(world[0], NOW)
    sweep(world[0], NOW)
    with session_scope() as db:
        assert db.get(Signal, sid).status == "resolved"
        assert len(action_events(db)) == 1
        assert db.query(Decision).filter_by(kind="supervisor").count() == 1
        assert db.query(Decision).filter_by(kind="supervisor").one().status == resolution


def test_red_recovery_then_new_failure_surfaces_new_incident(world):
    with session_scope() as db:
        sid = signal(db, world, "ci_failure", data={
            "branch": "trunk", "workflow": "CI", "failure_at": OLD.isoformat()}).id
    sweep(world[0], NOW)
    with session_scope() as db:
        db.query(Decision).filter_by(kind="supervisor").one().status = "resolved"
        db.add(Event(workspace_id=world[0], kind="main_green", created_at=NOW,
                     data={"repo_id": world[1], "branch": "trunk", "workflow": "CI"}))
    sweep(world[0], NOW)
    with session_scope() as db:
        sig = db.get(Signal, sid)
        assert sig.status == "resolved"
        sig.status = "new"
        sig.data = {**sig.data, "failure_at": (NOW + timedelta(hours=1)).isoformat()}
    sweep(world[0], NOW + timedelta(hours=2))
    with session_scope() as db:
        assert db.get(Signal, sid).status == "new"
        cards = db.query(Decision).filter_by(kind="supervisor").all()
        assert len(cards) == 2
        assert len({d.metadata_json["fingerprint"] for d in cards}) == 2
        assert sum(d.status == "open" for d in cards) == 1


@pytest.mark.parametrize("mode", ["observe", "fix"])
@pytest.mark.parametrize("change", ["off", "paused", "threshold"])
def test_sweep_rechecks_policy_after_detection(world, fix_mode, monkeypatch, mode, change):
    import aifactory.supervisor as module

    with session_scope() as db:
        expired_task(db, world)
        w = db.get(Workspace, world[0])
        w.settings = {**w.settings, "supervisor": {"mode": mode}}
        db.flush()
        before = snapshot(db, Task), snapshot(db, Signal)
    original_detect = module.detect
    changed = False

    def change_after_detection(db, workspace_id, now, policy):
        nonlocal changed
        result = original_detect(db, workspace_id, now, policy)
        if not changed:
            changed = True
            with session_scope() as live:
                w = live.get(Workspace, workspace_id)
                if change == "paused":
                    w.paused = True
                else:
                    w.settings = {**w.settings, "supervisor": {
                        "mode": "off" if change == "off" else mode,
                        "stuck_minutes": 10000 if change == "threshold" else 120}}
        return result

    monkeypatch.setattr(module, "detect", change_after_detection)
    sweep(world[0], NOW)
    with session_scope() as db:
        assert (snapshot(db, Task), snapshot(db, Signal)) == before
        assert db.query(Decision).count() == 0
        assert db.query(Message).count() == 0
        assert action_events(db) == []
    assert fix_mode == []


def test_surface_rechecks_policy_after_unsuccessful_action(world, fix_mode, monkeypatch):
    import aifactory.supervisor as module

    with session_scope() as db:
        task(db, world, status="blocked")  # not allow-listed for retry
    original_act = module.act

    def disable_after_action(db, workspace_id, finding, policy, now):
        assert not original_act(db, workspace_id, finding, policy, now)
        w = db.get(Workspace, workspace_id)
        w.settings = {**w.settings, "supervisor": {"mode": "off"}}
        return False

    monkeypatch.setattr(module, "act", disable_after_action)
    sweep(world[0], NOW)
    with session_scope() as db:
        assert db.query(Decision).count() == 0
        assert action_events(db) == []
    assert fix_mode == []


@pytest.mark.parametrize("status", ["failed", "blocked"])
@pytest.mark.parametrize("gate", ["legacy", "other_attempt", "other_head", "other_fingerprint"])
@pytest.mark.parametrize("later", [False, True])
def test_review_retry_preserves_unrelated_unblock_gate(world, fix_mode, status, gate, later):
    with session_scope() as db:
        t, attempt = review_task(db, world, status=status, updated_at=NOW,
                                 blocked_reason="CI polling timed out")
        tid = t.id
        metadata = {"legacy": None, "other_attempt": {"review_attempt_id": "another-attempt"},
                    "other_head": {"review_id": attempt.review_id, "head_sha": "another-head"},
                    "other_fingerprint": {"fingerprint": "another-incident"}}[gate]
        cards = [Decision(workspace_id=world[0], outcome_id=world[2], task_id=tid,
                          kind="unblock", title="Review failed",
                          metadata_json={"review_attempt_id": attempt.id}),
                 Decision(workspace_id=world[0], outcome_id=world[2], task_id=tid,
                          kind="unblock", title="CI polling timed out", metadata_json=metadata,
                          created_at=attempt.finished_at + timedelta(minutes=10 if later else -10))]
        db.add_all(cards)
        db.flush()
        before = snapshot(db, Task), snapshot(db, Decision)
    sweep(world[0], NOW)
    sweep(world[0], NOW)
    with session_scope() as db:
        assert snapshot(db, Task) == before[0]
        assert {did: snapshot(db, Decision)[did] for did in before[1]} == before[1]
        message = db.query(Message).filter_by(task_id=tid).one()
        assert message.author_kind == "factory" and not message.attribution["requeued"]
        assert db.query(Decision).filter_by(kind="supervisor", status="open", task_id=tid).count() == 1
        assert len(action_events(db)) == 1
        assert db.query(Event).filter_by(kind="decision_superseded").count() == 0
    assert fix_mode == []


@pytest.mark.parametrize("identity", ["attempt", "fingerprint", "review_head"])
def test_review_retry_supersedes_only_matching_incident(world, fix_mode, identity):
    with session_scope() as db:
        t, attempt = review_task(db, world, updated_at=NOW)
        tid = t.id
        finding = next(f for f in detect(db, world[0], NOW, supervisor_policy()) if f.kind == "unrelayed_review")
        metadata = {"attempt": {"review_attempt_id": attempt.id},
                    "fingerprint": {"fingerprint": finding.fingerprint},
                    "review_head": {"review_id": attempt.review_id, "head_sha": attempt.head_sha}}[identity]
        related = Decision(workspace_id=world[0], outcome_id=world[2], task_id=tid,
                           kind="unblock", title="Review failed", metadata_json=metadata)
        other_task = task(db, world, status="blocked", updated_at=NOW)
        unrelated = Decision(workspace_id=world[0], outcome_id=world[2], task_id=other_task.id,
                             kind="unblock", title="GitHub access failed", created_at=NOW)
        db.add_all([related, unrelated])
        db.flush()
        related_id, unrelated_id = related.id, unrelated.id
    sweep(world[0], NOW)
    sweep(world[0], NOW)
    with session_scope() as db:
        assert db.get(Task, tid).status == "queued"
        assert db.get(Decision, related_id).status == "superseded"
        assert db.get(Decision, unrelated_id).status == "open"
        assert db.query(Message).filter_by(task_id=tid).count() == 1
    assert fix_mode == [True]


@pytest.mark.parametrize("mode", ["observe", "fix"])
@pytest.mark.parametrize("recovery", ["event", "resolved_signal"])
def test_legacy_ci_without_workflow_is_surfaced_not_resolved(world, mode, recovery):
    with session_scope() as db:
        sig = signal(db, world, "ci_failure", external_id="legacy-ci", data={
            "branch": "trunk", "failure_at": OLD.isoformat()})
        sid = sig.id
        if recovery == "event":
            db.add(Event(workspace_id=world[0], kind="main_green", created_at=NOW,
                         data={"repo_id": world[1], "branch": "trunk", "workflow": "Unrelated CI"}))
        else:
            signal(db, world, "ci_failure", status="resolved", external_id="legacy-ci", data={
                "branch": "trunk", "workflow": "Unrelated CI", "resolved_at": NOW.isoformat()})
        w = db.get(Workspace, world[0])
        w.settings = {**w.settings, "supervisor": {"mode": mode}}
        db.flush()
        before = snapshot(db, Signal)
    sweep(world[0], NOW)
    sweep(world[0], NOW)
    with session_scope() as db:
        assert snapshot(db, Signal) == before
        assert db.get(Signal, sid).status == "new"
        decision = db.query(Decision).filter_by(kind="supervisor").one()
        assert decision.status == "open" and decision.metadata_json["finding_kind"] == "main_red"
        assert action_events(db) == []
