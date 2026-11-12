"""Tracing contracts use the normal isolated_env database, with no external services."""
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta, timezone
from threading import Barrier

import pytest
from pydantic import ValidationError
from sqlalchemy import create_engine, inspect

from aifactory import db
from aifactory.models import (
    Base, ErrorObservation, ErrorRecord, Organization, Outcome, Plan, Repository,
    Run, SuspectedCause, Task, TraceRun, TraceState, Workspace,
)
from aifactory.tracing_records import (
    CauseInput, ErrorInput, StackFrame, acquire_lease, complete_cycle, create_trace_run,
    eligible, mark_processed, production_error_detail, production_errors, review_cause,
    trace_run_dict, upsert_cause, upsert_error,
)

START = datetime(2026, 1, 1)


def seed(s, name="one"):
    org = Organization(name=name)
    s.add(org)
    s.flush()
    ws = Workspace(org_id=org.id, name=name, slug=name)
    s.add(ws)
    s.flush()
    repo = Repository(workspace_id=ws.id, full_name=f"acme/{name}")
    s.add(repo)
    s.flush()
    return ws, repo


def observation(ws, repo, **kwargs):
    return ErrorInput(**dict(workspace_id=ws.id, repo_id=repo.id, source="webhook", external_id="error-1",
                            title="Crash", stack_frames=[StackFrame(file="src/app.py", line=12, function="run")],
                            first_seen_at=START, last_seen_at=START) | kwargs)


def candidate(repo, **kwargs):
    return CauseInput(**dict(repo_id=repo.id, pr_number=7, merge_sha="abc123", confidence=0.8,
                             evidence={"blame": [12], "release": "v1"}) | kwargs)


def test_empty_and_existing_v2_schema_initialization(tmp_path):
    # Simulate the entire pre-tracing v2 schema, not just an empty runs stub.
    url = f"sqlite:///{tmp_path / 'old-v2.db'}"
    engine = create_engine(url)
    new_tables = {"error_records", "error_observations", "suspected_causes", "trace_runs", "trace_states"}
    old_tables = [t for t in Base.metadata.sorted_tables if t.name not in new_tables]
    Base.metadata.create_all(engine, tables=old_tables)
    with engine.begin() as c:
        c.exec_driver_sql("INSERT INTO organizations (id, name, created_at) VALUES ('legacy', 'Legacy', '2026-01-01')")
    assert not new_tables.intersection(inspect(engine).get_table_names())
    engine.dispose()
    upgraded = db.init_engine(url)
    assert new_tables <= set(inspect(upgraded).get_table_names())
    assert not next(c for c in inspect(upgraded).get_columns("runs") if c["name"] == "outcome_id")["nullable"]
    with upgraded.connect() as c:
        assert c.exec_driver_sql("SELECT name FROM organizations WHERE id='legacy'").scalar_one() == "Legacy"
    db.init_engine(url)  # restart is idempotent
    assert not Run.__table__.c.outcome_id.nullable
    assert TraceRun.__table__.c.outcome_id.nullable


def test_normalization_validation_and_scope():
    with db.session_scope() as s:
        ws, repo = seed(s)
        other_ws, other_repo = seed(s, "two")
        aware = datetime(2026, 1, 1, 2, tzinfo=timezone(timedelta(hours=2)))
        obs = observation(ws, repo, first_seen_at=aware, last_seen_at=aware)
        assert obs.first_seen_at == START
        result = upsert_error(s, obs)
        assert s.get(ErrorRecord, result.error_id).first_seen_at == START
        with pytest.raises(ValueError, match="Repository"):
            upsert_error(s, observation(ws, other_repo))
        with pytest.raises(ValueError, match="Error"):
            production_error_detail(s, other_ws.id, result.error_id)
        with pytest.raises(ValidationError, match="last_seen_at"):
            observation(ws, repo, last_seen_at=START - timedelta(seconds=1))
        for line in (0, -1, 1.5, True):
            with pytest.raises(ValidationError):
                StackFrame(file="app.py", line=line)
        with pytest.raises(ValidationError):
            StackFrame(file="", line=1)
        for confidence in (-0.1, 1.1, float("nan"), float("inf")):
            with pytest.raises(ValidationError):
                candidate(repo, confidence=confidence)
        with pytest.raises(ValidationError):
            observation(ws, repo, external_id="x" * 201)


def test_identity_and_duplicate_observations():
    with db.session_scope() as s:
        ws, repo = seed(s)
        other_ws, other_repo = seed(s, "two")
        repo2 = Repository(workspace_id=ws.id, full_name="acme/second")
        s.add(repo2)
        s.flush()
        obs = observation(ws, repo)
        first = upsert_error(s, obs)
        cause = upsert_cause(s, ws.id, first.error_id, 1, candidate(repo))
        assert upsert_error(s, obs) == first
        newer = observation(ws, repo, last_seen_at=START + timedelta(hours=1))
        assert upsert_error(s, newer) == first
        row = s.get(ErrorRecord, first.error_id)
        assert row.last_seen_at == START + timedelta(hours=1)
        assert row.episode == row.ingestion_revision == 1
        assert upsert_cause(s, ws.id, first.error_id, 1, candidate(repo)).id == cause.id
        ids = {first.error_id, upsert_error(s, observation(ws, repo2)).error_id,
               upsert_error(s, observation(other_ws, other_repo)).error_id,
               upsert_error(s, observation(ws, repo, source="sentry")).error_id}
        assert len(ids) == 4
        assert s.query(SuspectedCause).count() == 1
        assert len(production_errors(s, ws.id)["items"]) == 3
        assert len(production_errors(s, ws.id, repo_id=repo.id)["items"]) == 2
        page = production_errors(s, ws.id, limit=1)
        assert page["next_offset"] == 1
        assert len(production_errors(s, ws.id, offset=1)["items"]) == 2
        detail = production_error_detail(s, ws.id, first.error_id)
        assert detail["candidates"][0]["id"] == cause.id
        assert detail["first_seen_at"].endswith("Z")
        assert len(production_errors(s, other_ws.id)["items"]) == 1


def test_revisions_processing_and_regression_replay():
    with db.session_scope() as s:
        ws, repo = seed(s)
        obs = observation(ws, repo)
        first = upsert_error(s, obs)
        row = s.get(ErrorRecord, first.error_id)
        assert eligible(row)
        assert mark_processed(s, ws.id, row.id, 1)
        assert not eligible(row)
        assert upsert_error(s, observation(ws, repo, last_seen_at=START + timedelta(hours=1))) == first
        assert not eligible(row)
        changed = observation(ws, repo, body="New evidence", last_seen_at=START + timedelta(hours=2))
        revision = upsert_error(s, changed)
        assert revision.error_id == first.error_id and revision.revision == 2
        assert eligible(row)
        assert not mark_processed(s, ws.id, row.id, 1)
        assert row.processed_revision == 1
        assert mark_processed(s, ws.id, row.id, 2)
        regression = observation(ws, repo, body="New evidence", regressed=True,
                                 last_seen_at=START + timedelta(days=1), observation_id="regression-1")
        reopened = upsert_error(s, regression)
        count = s.query(ErrorObservation).count()
        assert reopened.episode == 2 and reopened.revision == 1
        assert upsert_error(s, regression) == reopened
        assert s.query(ErrorObservation).count() == count
        # A replay stays the original receipt even after its episode advances.
        upsert_error(s, observation(ws, repo, body="More evidence", last_seen_at=START + timedelta(days=2)))
        assert upsert_error(s, regression) == reopened
        assert s.query(ErrorRecord).count() == 2
        assert s.query(ErrorObservation).count() == count + 1
        assert upsert_error(s, obs) == first


@pytest.mark.parametrize("changed", [False, True])
def test_confirmed_episode_resolution_closes_without_mutating_evidence(changed):
    with db.session_scope() as s:
        ws, repo = seed(s)
        first = upsert_error(s, observation(ws, repo))
        cause = upsert_cause(s, ws.id, first.error_id, 1, candidate(repo))
        review_cause(s, ws.id, cause.id, disposition="confirmed", reason="Diff proves it")
        cause.side_effect_progress = {"signal_id": "durable-intent"}
        s.flush()
        resolved = upsert_error(s, observation(ws, repo, lifecycle="resolved",
                                body="Changed resolution evidence" if changed else "",
                                last_seen_at=START + timedelta(days=1)))
        row = s.get(ErrorRecord, first.error_id)
        assert resolved == first
        assert row.lifecycle == "resolved"
        assert row.body == "" and row.ingestion_revision == 1
        assert s.query(ErrorRecord).count() == 1
        assert cause.disposition == "confirmed" and cause.reason == "Diff proves it"
        assert cause.side_effect_progress == {"signal_id": "durable-intent"}
        with pytest.raises(ValueError, match="immutable"):
            review_cause(s, ws.id, cause.id, disposition="rejected", reason="Rewrite")
        reopened = upsert_error(s, observation(ws, repo, last_seen_at=START + timedelta(days=2)))
        assert reopened.episode == 2
        assert reopened.error_id != first.error_id
        assert s.get(ErrorRecord, first.error_id).lifecycle == "resolved"


def test_new_evidence_after_confirmation_and_disposition_preservation():
    with db.session_scope() as s:
        ws, repo = seed(s)
        first = upsert_error(s, observation(ws, repo))
        confirmed = upsert_cause(s, ws.id, first.error_id, 1, candidate(repo))
        rejected = upsert_cause(s, ws.id, first.error_id, 1, candidate(repo, pr_number=8))
        uncertain = upsert_cause(s, ws.id, first.error_id, 1, candidate(repo, pr_number=9))
        review_cause(s, ws.id, confirmed.id, disposition="confirmed", reason="Yes")
        review_cause(s, ws.id, rejected.id, disposition="rejected", reason="No")
        review_cause(s, ws.id, uncertain.id, disposition="uncertain", reason="Need human")
        newer = upsert_error(s, observation(ws, repo, commit_sha="new", last_seen_at=START + timedelta(hours=1)))
        assert newer.episode == 2 and newer.error_id != first.error_id
        old = s.get(ErrorRecord, first.error_id)
        assert old.commit_sha is None and old.ingestion_revision == 1
        assert upsert_cause(s, ws.id, first.error_id, 1, candidate(repo, confidence=0.1)).id == confirmed.id
        assert confirmed.confidence == 0.8
        assert (confirmed.disposition, rejected.disposition, uncertain.disposition) == ("confirmed", "rejected", "uncertain")
        assert s.query(SuspectedCause).count() == 3
        assert upsert_cause(s, ws.id, newer.error_id, 1, candidate(repo)).id != confirmed.id


def test_outcome_repository_scope_for_causes_and_runs():
    with db.session_scope() as s:
        ws, repo = seed(s)
        other_ws, other_repo = seed(s, "two")
        same_ws_repo = Repository(workspace_id=ws.id, full_name="acme/b")
        s.add(same_ws_repo)
        s.flush()
        good = Outcome(workspace_id=ws.id, number=1, title="Good", repo_ids=[repo.id])
        wrong_repo = Outcome(workspace_id=ws.id, number=2, title="Wrong repo", repo_ids=[same_ws_repo.id])
        wrong_ws = Outcome(workspace_id=other_ws.id, number=1, title="Wrong workspace", repo_ids=[other_repo.id])
        s.add_all([good, wrong_repo, wrong_ws])
        s.flush()
        plan = Plan(outcome_id=good.id)
        s.add(plan)
        s.flush()
        task = Task(workspace_id=ws.id, repo_id=repo.id, outcome_id=good.id, plan_id=plan.id, key="T1", title="Fix")
        s.add(task)
        s.flush()
        error = upsert_error(s, observation(ws, repo))
        for outcome in (wrong_repo, wrong_ws):
            with pytest.raises(ValueError, match="Outcome"):
                upsert_cause(s, ws.id, error.error_id, 1, candidate(repo, outcome_id=outcome.id))
            with pytest.raises(ValueError, match="Outcome"):
                create_trace_run(s, ws.id, error.error_id, 1, cycle_id="night", outcome_id=outcome.id)
        cause = upsert_cause(s, ws.id, error.error_id, 1, candidate(repo, task_id=task.id))
        assert cause.outcome_id == good.id
        linked = create_trace_run(s, ws.id, error.error_id, 1, cycle_id="night", outcome_id=good.id)
        assert linked.outcome_id == good.id
        run = create_trace_run(s, ws.id, error.error_id, 1, cycle_id="night", started_at=START.replace(tzinfo=UTC))
        wire = trace_run_dict(run)
        assert wire["role"] == "tracer" and wire["outcome_id"] is None and wire["task_id"] is None
        assert wire["cost_known"] is False and wire["started_at"] == "2026-01-01T00:00:00Z"
        assert s.query(Run).count() == 0


def test_expiring_lease_fencing_and_reservation_recovery():
    with db.session_scope() as s:
        ws, _ = seed(s)
        state = acquire_lease(s, ws.id, owner="worker", cycle_id="night-1", now=START,
                              ttl=timedelta(minutes=10), reserve_usd=2)
        ws_id, first_token = ws.id, state.lease_token
        assert state.cycle_at == START and state.reserved_usd == 2
    with db.session_scope() as s:
        assert acquire_lease(s, ws_id, owner="other", cycle_id="night-2", now=START + timedelta(minutes=5),
                             ttl=timedelta(minutes=10)) is None
        assert not complete_cycle(s, ws_id, token=first_token, now=START + timedelta(minutes=10))
        state = acquire_lease(s, ws_id, owner="worker", cycle_id="night-2", now=START + timedelta(minutes=10),
                              ttl=timedelta(minutes=10), reserve_usd=99)
        second_token = state.lease_token
        assert second_token != first_token
        assert state.cycle_id == "night-1" and state.reserved_usd == 2
        assert state.cycle_cutoff == START
        assert not complete_cycle(s, ws_id, token=first_token, now=START + timedelta(minutes=11))
        assert complete_cycle(s, ws_id, token=second_token, now=START + timedelta(minutes=11))
        assert state.completed_cutoff == START and state.reserved_usd == 0
        new = acquire_lease(s, ws_id, owner="worker", cycle_id="night-2", now=START + timedelta(days=1),
                            ttl=timedelta(minutes=10), reserve_usd=3)
        assert new.cycle_id == "night-2" and new.reserved_usd == 3


def test_transaction_rollback_does_not_persist_records():
    with db.session_scope() as s:
        ws, repo = seed(s)
        obs = observation(ws, repo)
    with pytest.raises(RuntimeError):
        with db.session_scope() as s:
            result = upsert_error(s, obs)
            upsert_cause(s, ws.id, result.error_id, 1, candidate(repo))
            acquire_lease(s, ws.id, owner="worker", cycle_id="night", now=START, ttl=timedelta(minutes=1))
            raise RuntimeError("caller rolls back")
    with db.session_scope() as s:
        assert s.query(ErrorRecord).count() == s.query(ErrorObservation).count() == s.query(SuspectedCause).count() == 0
        assert s.query(TraceState).count() == 0


def test_concurrent_ingestion_and_lease_admission():
    with db.session_scope() as s:
        ws, repo = seed(s)
        obs = observation(ws, repo, regressed=True, observation_id="one-event")
        ws_id = ws.id
    barrier = Barrier(2)

    def ingest(_):
        barrier.wait()
        with db.session_scope() as s:
            result = upsert_error(s, obs)
            cause = upsert_cause(s, ws_id, result.error_id, 1, candidate(repo))
            state = acquire_lease(s, ws_id, owner="worker", cycle_id="night", now=START,
                                  ttl=timedelta(minutes=10), reserve_usd=2)
            return result, cause.id, state is not None

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(ingest, range(2)))
    assert results[0][:2] == results[1][:2]
    assert sum(r[2] for r in results) == 1
    with db.session_scope() as s:
        assert s.query(ErrorRecord).count() == s.query(ErrorObservation).count() == s.query(SuspectedCause).count() == 1


def test_trace_run_result_validation_accounting_and_replay():
    from aifactory.tracing_records import TraceRunResult, finish_trace_run

    with db.session_scope() as s:
        ws, repo = seed(s)
        error = upsert_error(s, observation(ws, repo))
        run = create_trace_run(s, ws.id, error.error_id, 1, cycle_id="night", started_at=START)
        result = TraceRunResult(status="failed", input_tokens=100, output_tokens=20, cost_usd=0.25,
                                cost_known=True, result={"verdict": "uncertain"},
                                finished_at=(START + timedelta(minutes=1)).replace(tzinfo=UTC))
        finish_trace_run(s, ws.id, run.id, result)
        assert finish_trace_run(s, ws.id, run.id, result).cost_usd == 0.25
        wire = trace_run_dict(run)
        assert wire["input_tokens"] == 100 and wire["status"] == "failed"
        assert wire["cost_known"] and wire["result"]["verdict"] == "uncertain"
        with pytest.raises(ValueError, match="immutable"):
            finish_trace_run(s, ws.id, run.id, result.model_copy(update={"cost_usd": 0.5}))
        with pytest.raises(ValueError, match="finished_at"):
            finish_trace_run(s, ws.id, run.id, result.model_copy(update={"finished_at": START - timedelta(seconds=1)}))
        for bad in ({"input_tokens": -1}, {"cost_usd": float("inf")}, {"status": "running"}):
            with pytest.raises(ValidationError):
                TraceRunResult(**(result.model_dump() | bad))


def test_obsolete_review_and_pending_candidate_refresh():
    with db.session_scope() as s:
        ws, repo = seed(s)
        error = upsert_error(s, observation(ws, repo))
        cause = upsert_cause(s, ws.id, error.error_id, 1, candidate(repo))
        upsert_error(s, observation(ws, repo, body="New evidence", last_seen_at=START + timedelta(hours=1)))
        with pytest.raises(ValueError, match="obsolete"):
            review_cause(s, ws.id, cause.id, disposition="confirmed", reason="Old review")
        refreshed = upsert_cause(s, ws.id, error.error_id, 2, candidate(repo, confidence=0.9))
        assert refreshed.id == cause.id and refreshed.error_revision == 2 and refreshed.confidence == 0.9
        review_cause(s, ws.id, refreshed.id, disposition="uncertain", reason="Needs human")
        upsert_error(s, observation(ws, repo, body="Further evidence", last_seen_at=START + timedelta(hours=2)))
        assert upsert_cause(s, ws.id, error.error_id, 3, candidate(repo)).disposition == "uncertain"
        assert refreshed.error_revision == 2  # attributed review evidence is not rewritten


def test_regression_payload_replay_without_source_event_id():
    with db.session_scope() as s:
        ws, repo = seed(s)
        first = upsert_error(s, observation(ws, repo))
        event = observation(ws, repo, regressed=True, last_seen_at=START + timedelta(days=1))
        reopened = upsert_error(s, event)
        before = s.query(ErrorObservation).count()
        assert reopened.episode == first.episode + 1
        assert upsert_error(s, event) == reopened
        assert s.query(ErrorObservation).count() == before
        assert s.query(ErrorRecord).count() == 2


def test_stale_observations_do_not_change_lifecycle_or_evidence():
    with db.session_scope() as s:
        ws, repo = seed(s)
        latest = upsert_error(s, observation(ws, repo, body="Latest", last_seen_at=START + timedelta(days=2)))
        upsert_error(s, observation(ws, repo, lifecycle="resolved", body="Stale", last_seen_at=START + timedelta(days=1)))
        row = s.get(ErrorRecord, latest.error_id)
        assert row.lifecycle == "open" and row.body == "Latest" and row.ingestion_revision == 1
        assert row.last_seen_at == START + timedelta(days=2)


def test_disconnect_repository_with_tracing_data(client, signed_in):
    """Disconnect cleans every episode/dependent row under enforced foreign keys."""
    wid = signed_in["workspaces"][0]["id"]
    with db.session_scope() as s:
        ws = s.get(Workspace, wid)
        repo = Repository(workspace_id=wid, full_name="acme/disconnect-tracing")
        other_repo = Repository(workspace_id=wid, full_name="acme/keep-tracing")
        s.add_all([repo, other_repo])
        s.flush()
        other_ws, foreign_repo = seed(s, "foreign")
        repo_id = repo.id
        error_ids, cause_ids, run_ids = [], [], []
        for regressed in (False, True):
            result = upsert_error(s, observation(ws, repo, regressed=regressed,
                                  last_seen_at=START + timedelta(days=int(regressed))))
            cause = upsert_cause(s, wid, result.error_id, 1, candidate(repo))
            review_cause(s, wid, cause.id, disposition="confirmed", reason="Confirmed before disconnect")
            run = create_trace_run(s, wid, result.error_id, 1, cycle_id="night")
            error_ids.append(result.error_id)
            cause_ids.append(cause.id)
            run_ids.append(run.id)
        kept = []
        for keep_ws, keep_repo in ((ws, other_repo), (other_ws, foreign_repo)):
            result = upsert_error(s, observation(keep_ws, keep_repo))
            cause = upsert_cause(s, keep_ws.id, result.error_id, 1, candidate(keep_repo))
            run = create_trace_run(s, keep_ws.id, result.error_id, 1, cycle_id="night")
            kept.append((result.error_id, cause.id, run.id))
        state = acquire_lease(s, wid, owner="worker", cycle_id="night", now=START,
                              ttl=timedelta(minutes=10), reserve_usd=2)
        token = state.lease_token

    # Membership checks must run before any cascade; foreign data is untouched.
    denied = client.delete(f"/api/workspaces/{wid}/repositories/{foreign_repo.id}")
    assert denied.status_code == 404, denied.text
    response = client.delete(f"/api/workspaces/{wid}/repositories/{repo_id}")
    assert response.status_code == 200, response.text
    with db.session_scope() as s:
        assert s.get(Repository, repo_id) is None
        for model, ids in ((ErrorRecord, error_ids), (SuspectedCause, cause_ids), (TraceRun, run_ids)):
            assert all(s.get(model, row_id) is None for row_id in ids)
        assert s.query(ErrorObservation).filter_by(repo_id=repo_id).count() == 0
        for error_id, cause_id, run_id in kept:
            assert s.get(ErrorRecord, error_id) is not None
            assert s.get(SuspectedCause, cause_id) is not None
            assert s.get(TraceRun, run_id) is not None
            assert s.query(ErrorObservation).filter_by(error_id=error_id).count() == 1
        state = s.get(TraceState, wid)
        assert state.lease_token == token and state.reserved_usd == 2


def test_disconnect_tracing_cascade_is_transactional():
    with db.session_scope() as s:
        ws, repo = seed(s)
        result = upsert_error(s, observation(ws, repo))
        cause = upsert_cause(s, ws.id, result.error_id, 1, candidate(repo))
        run = create_trace_run(s, ws.id, result.error_id, 1, cycle_id="night")
        repo_id, cause_id, run_id = repo.id, cause.id, run.id
    with pytest.raises(RuntimeError, match="abort disconnect"):
        with db.session_scope() as s:
            s.delete(s.get(Repository, repo_id))
            s.flush()
            assert s.get(ErrorRecord, result.error_id) is None
            raise RuntimeError("abort disconnect")
    with db.session_scope() as s:
        assert s.get(Repository, repo_id) is not None
        assert s.get(ErrorRecord, result.error_id) is not None
        assert s.get(SuspectedCause, cause_id) is not None
        assert s.get(TraceRun, run_id) is not None
        assert s.query(ErrorObservation).filter_by(error_id=result.error_id).count() == 1
