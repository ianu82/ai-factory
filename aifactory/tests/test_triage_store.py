"""Triage persistence invariants, including real legacy DDL upgrades."""
from datetime import datetime, timedelta

import pytest
from sqlalchemy import event, inspect, select
from sqlalchemy.exc import IntegrityError

from aifactory import db, serializers as S
from aifactory.models import (Organization, Outcome, Run, Signal, SignalRevision,
    TriageOverride, TriageProposal, TriageState, TriageVerdict, TriageWindow, User, Workspace)
from aifactory.triage_store import (claim_window, finalize_window, latest_verdicts,
                                    proposal_for_signal, snapshot_revision)

NOW = datetime(2026, 1, 1)


@pytest.fixture
def workspace():
    with db.session_scope() as s:
        org = Organization(name="Test")
        s.add(org)
        s.flush()
        w = Workspace(org_id=org.id, name="Test", slug="test")
        s.add(w)
        s.flush()
        return w.id


def add_signal(workspace, **extra):
    with db.session_scope() as s:
        signal = Signal(workspace_id=workspace, source="improvement", title="Improve",
                        fingerprint=extra.pop("fingerprint", "one"), **extra)
        s.add(signal)
        s.flush()
        return signal.id


def results(window, verdict="propose"):
    return [dict(revision_id=r, verdict=verdict, confidence=0.8, reasons=["Useful"],
                 goal_kind="workspace", goal_id="reliability", goal_snapshot={"name": "Reliability"},
                 payload={"draft": {"title": "Fix"}}) for r in window.revision_ids]


def finish(workspace, window, verdict="propose", **kwargs):
    return finalize_window(workspace, window.id, window.lease_token, results(window, verdict), **kwargs)


def test_claim_fences_recovers_exact_window_and_never_reopens_completed(workspace):
    sid = add_signal(workspace)
    first = claim_window(workspace, now=NOW)
    assert first.revision_ids and claim_window(workspace, now=NOW) is None
    with db.session_scope() as s:
        s.get(Signal, sid).body = "Changed while model running"
    recovered = claim_window(workspace, now=NOW + timedelta(seconds=301))
    assert recovered.id == first.id and recovered.run_id == first.run_id
    assert recovered.revision_ids == first.revision_ids
    assert recovered.lease_token != first.lease_token
    assert not finish(workspace, first, now=NOW + timedelta(seconds=302))
    assert finish(workspace, recovered, now=NOW + timedelta(seconds=302))
    assert not finish(workspace, recovered, now=NOW + timedelta(seconds=303))
    assert claim_window(workspace, now=NOW + timedelta(seconds=303)) is None
    next_window = claim_window(workspace, now=NOW + timedelta(seconds=2103))
    assert next_window.id != first.id and next_window.revision_ids != first.revision_ids
    with db.session_scope() as s:
        assert s.get(TriageWindow, first.id).status == "completed"
        assert s.get(SignalRevision, first.revision_ids[0]).snapshot["body"] == ""


def test_identical_content_reuses_revision_and_verdict(workspace):
    sid = add_signal(workspace)
    first = claim_window(workspace, now=NOW)
    assert finish(workspace, first, now=NOW)
    with db.session_scope() as s:
        signal = s.get(Signal, sid)
        signal.count += 1
        signal.last_seen_at = NOW + timedelta(days=1)
        signal.status = "accepted"
        assert snapshot_revision(s, signal).id == first.revision_ids[0]
    second = claim_window(workspace, now=NOW + timedelta(hours=1))
    assert second.revision_ids == []
    assert finish(workspace, second, now=NOW + timedelta(hours=1))
    with db.session_scope() as s:
        assert s.query(SignalRevision).count() == s.query(TriageVerdict).count() == 1
        old = s.query(TriageVerdict).one()
        s.add(TriageVerdict(workspace_id=workspace, signal_id=sid, revision_id=old.revision_id,
                           run_id=old.run_id, verdict="ignore", confidence=0.5))
        with pytest.raises(IntegrityError):
            s.flush()
        s.rollback()


def test_proposal_unique_across_sessions_and_revisions_but_not_outcomes(workspace):
    with db.session_scope() as s:
        outcome = Outcome(workspace_id=workspace, number=1, title="Existing")
        s.add(outcome)
        s.flush()
        oid = outcome.id
    sid = add_signal(workspace, outcome_id=oid)
    second = add_signal(workspace, fingerprint="two", outcome_id=oid)
    window = claim_window(workspace, now=NOW)
    assert finish(workspace, window, now=NOW)
    with db.session_scope() as s:
        p = proposal_for_signal(s, workspace, sid)
        pid = p.id
        assert p.outcome_id == oid
        assert proposal_for_signal(s, workspace, second).outcome_id == oid
        s.get(Signal, sid).body = "Revised"
    window = claim_window(workspace, now=NOW + timedelta(hours=1))
    assert finish(workspace, window, now=NOW + timedelta(hours=1))
    with db.session_scope() as s:
        assert proposal_for_signal(s, workspace, sid).id == pid
        assert s.query(TriageProposal).count() == 2
        s.add(TriageProposal(workspace_id=workspace, signal_id=sid))
        with pytest.raises(IntegrityError):
            s.flush()
        s.rollback()


@pytest.mark.parametrize("verdict", ["propose", "attach", "duplicate", "ignore", "ask"])
def test_verdict_serialization_preserves_improvement_and_run_data(workspace, verdict):
    data = {"action": {"kind": "repair"}, "evidence": {"tests": "failed"}}
    sid = add_signal(workspace, data=data)
    window = claim_window(workspace, now=NOW)
    assert finish(workspace, window, verdict, now=NOW, run_result={"model_output": "retained"})
    with db.session_scope() as s:
        user = User(name="Reviewer")
        s.add(user)
        s.flush()
        latest = latest_verdicts(s, workspace, [sid])[sid]
        s.add(TriageOverride(verdict_id=latest.id, actor_id=user.id, verdict="ignore", reason="Human reason"))
        run = s.get(Run, window.run_id)
        run.cost_usd = 0.42
        run.input_tokens = 12
        s.flush()
        row = S.signals(s, [s.get(Signal, sid)])[0]
        assert row["data"] == data
        assert row["latest_triage_verdict"]["verdict"] == verdict
        assert row["latest_triage_verdict"]["overrides"][0]["actor_id"] == user.id
        serialized = S.run(run)
        assert serialized["outcome_id"] is None and serialized["cost_usd"] == 0.42
        assert serialized["result"]["model_output"] == "retained"
        assert serialized["summary"]["signals_seen"] == 1


def test_bulk_serialization_constant_queries(workspace):
    for n in range(10):
        add_signal(workspace, fingerprint=str(n))
    window = claim_window(workspace, now=NOW)
    assert finish(workspace, window, now=NOW)
    statements = []
    def count(_conn, _cursor, statement, _params, _context, _many):
        statements.append(statement)
    with db.session_scope() as s:
        rows = s.query(Signal).all()
        event.listen(db.engine(), "before_cursor_execute", count)
        try:
            assert len(S.signals(s, rows)) == 10
        finally:
            event.remove(db.engine(), "before_cursor_execute", count)
    assert len(statements) == 2


def test_finalize_validation_is_atomic_and_workspace_isolated(workspace):
    sid = add_signal(workspace)
    with db.session_scope() as s:
        w = Workspace(org_id=s.get(Workspace, workspace).org_id, name="Other", slug="other")
        s.add(w)
        s.flush()
        other = w.id
    with db.session_scope() as s:
        assert latest_verdicts(s, other, [sid]) == {}
    window = claim_window(workspace, now=NOW)
    assert not finalize_window(other, window.id, window.lease_token, results(window), now=NOW)
    with pytest.raises(ValueError, match="exact window"):
        finalize_window(workspace, window.id, window.lease_token, [], now=NOW)
    with db.session_scope() as s:
        assert s.get(TriageWindow, window.id).status == "running"
        assert s.query(TriageVerdict).count() == 0
    assert finish(workspace, window, now=NOW)


def test_revisions_immutable(workspace):
    sid = add_signal(workspace)
    with db.session_scope() as s:
        revision = snapshot_revision(s, s.get(Signal, sid))
        s.flush()
        revision.snapshot = {"title": "Changed"}
        with pytest.raises(ValueError, match="immutable"):
            s.flush()
        s.rollback()


def test_sqlite_old_schema_upgrade_twice_preserves_rows_indexes_references(tmp_path):
    from sqlalchemy import create_engine
    from sqlalchemy.schema import CreateTable

    url = f"sqlite:///{tmp_path / 'legacy.db'}"
    engine = create_engine(url)
    # A complete pre-triage schema, with the old NOT NULL runs column.
    ddl = str(CreateTable(Run.__table__).compile(engine)).replace("outcome_id VARCHAR(32)", "outcome_id VARCHAR(32) NOT NULL")
    with engine.begin() as c:
        c.exec_driver_sql("CREATE TABLE organizations (id VARCHAR(32) PRIMARY KEY, name VARCHAR(200))")
        c.exec_driver_sql("CREATE TABLE workspaces (id VARCHAR(32) PRIMARY KEY, org_id VARCHAR(32), name VARCHAR(200), slug VARCHAR(80))")
        c.exec_driver_sql("CREATE TABLE outcomes (id VARCHAR(32) PRIMARY KEY, workspace_id VARCHAR(32))")
        c.exec_driver_sql("CREATE TABLE tasks (id VARCHAR(32) PRIMARY KEY)")
        c.exec_driver_sql(ddl)
        c.exec_driver_sql("CREATE INDEX legacy_runs_role ON runs(role)")
        c.exec_driver_sql("CREATE TABLE legacy_run_audit (run_id VARCHAR(32))")
        c.exec_driver_sql("CREATE TRIGGER legacy_runs_audit AFTER INSERT ON runs "
                          "BEGIN INSERT INTO legacy_run_audit VALUES (NEW.id); END")
        c.exec_driver_sql("CREATE TABLE legacy_run_refs (run_id VARCHAR(32) REFERENCES runs(id))")
        c.exec_driver_sql("INSERT INTO workspaces(id, org_id, name, slug) VALUES ('w', 'o', 'W', 'w')")
        c.exec_driver_sql("INSERT INTO outcomes VALUES ('out', 'w')")
        for role in ("planner", "builder", "verifier"):
            values = dict(id=role, workspace_id="w", outcome_id="out", role=role, harness="codex")
            c.execute(Run.__table__.insert().values(**values))
            c.exec_driver_sql("INSERT INTO legacy_run_refs VALUES (?)", (role,))
    engine.dispose()
    for _ in range(2):
        upgraded = db.init_engine(url)
        with upgraded.connect() as c:
            assert c.exec_driver_sql("SELECT id, outcome_id FROM runs ORDER BY id").all() == [
                ("builder", "out"), ("planner", "out"), ("verifier", "out")]
            assert c.exec_driver_sql("SELECT count(*) FROM legacy_run_refs").scalar() == 3
            assert not c.exec_driver_sql("PRAGMA foreign_key_check").all()
        with upgraded.connect() as c:
            assert c.exec_driver_sql("SELECT count(*) FROM legacy_run_audit").scalar() == 3
            assert c.exec_driver_sql("SELECT sql FROM sqlite_master WHERE name='legacy_runs_audit'").scalar()
            assert c.exec_driver_sql("PRAGMA foreign_keys").scalar() == 1
        assert "legacy_runs_role" in {i["name"] for i in inspect(upgraded).get_indexes("runs")}
        assert next(c for c in inspect(upgraded).get_columns("runs") if c["name"] == "outcome_id")["nullable"]
    with db.session_scope() as s:
        s.add(Run(workspace_id="w", outcome_id=None, role="triage", harness="triage"))


def test_postgres_old_schema_upgrade_twice(workspace):
    if db.engine().dialect.name != "postgresql":
        pytest.skip("requires AIFACTORY_TEST_DATABASE_URL PostgreSQL")
    with db.session_scope() as s:
        outcome = Outcome(workspace_id=workspace, number=1, title="Legacy")
        s.add(outcome)
        s.flush()
        for role in ("planner", "builder", "verifier"):
            s.add(Run(workspace_id=workspace, outcome_id=outcome.id, role=role, harness="codex"))
    with db.engine().begin() as c:
        c.exec_driver_sql("ALTER TABLE runs ALTER COLUMN outcome_id SET NOT NULL")
    for _ in range(2):
        db.init_engine()
        with db.session_scope() as s:
            assert s.query(Run).count() == 3
            assert all(s.get(Outcome, r.outcome_id) for r in s.scalars(select(Run)))
    assert claim_window(workspace, now=NOW).run_id


def test_parallel_claims_only_one_wins(workspace):
    from concurrent.futures import ThreadPoolExecutor
    from threading import Barrier

    add_signal(workspace)
    barrier = Barrier(2)
    def claim():
        barrier.wait()
        return claim_window(workspace, now=NOW)
    with ThreadPoolExecutor(max_workers=2) as pool:
        windows = list(pool.map(lambda _: claim(), range(2)))
    assert sum(w is not None for w in windows) == 1
    with db.session_scope() as s:
        assert s.query(TriageWindow).count() == 1
        assert s.query(Run).count() == 1


def test_admission_binding_is_durable_and_cannot_be_replaced(workspace):
    from aifactory.triage_store import bind_proposal_outcome

    sid = add_signal(workspace)
    with db.session_scope() as s:
        proposal_for_signal(s, workspace, sid)
        first = Outcome(workspace_id=workspace, number=1, title="Admitted")
        s.add(first)
        s.flush()
        oid = first.id
        identity = bind_proposal_outcome(s, workspace, sid, oid)
    with db.session_scope() as s:
        assert bind_proposal_outcome(s, workspace, sid, oid) == identity
    with pytest.raises(ValueError, match="already admitted"):
        with db.session_scope() as s:
            second = Outcome(workspace_id=workspace, number=2, title="Must roll back")
            s.add(second)
            s.flush()
            bind_proposal_outcome(s, workspace, sid, second.id)
    with db.session_scope() as s:
        assert s.query(Outcome).count() == 1
        assert s.query(TriageProposal).one().outcome_id == oid


def test_bounded_keyset_scan_does_not_starve_unreviewed_signals(workspace):
    for n in range(5):
        add_signal(workspace, fingerprint=str(n))
    for n in range(3):
        when = NOW + timedelta(hours=n)
        window = claim_window(workspace, now=when, limit=2)
        assert len(window.revision_ids) <= 2
        assert finish(workspace, window, now=when)
    with db.session_scope() as s:
        assert s.query(TriageVerdict).count() == 5


def test_http_signal_and_run_views(client, signed_in):
    workspace = signed_in["workspaces"][0]["id"]
    sid = add_signal(workspace, data={"action": "repair", "evidence": ["failure"]})
    window = claim_window(workspace, now=NOW)
    assert finish(workspace, window, now=NOW)
    response = client.get(f"/api/workspaces/{workspace}/signals")
    assert response.status_code == 200
    row = next(r for r in response.json() if r["id"] == sid)
    assert row["data"]["evidence"] == ["failure"]
    assert row["latest_triage_verdict"]["verdict"] == "propose"
    response = client.get(f"/api/workspaces/{workspace}/runs")
    assert response.status_code == 200
    row = next(r for r in response.json() if r["id"] == window.run_id)
    assert row["outcome_id"] is None
    assert row["summary"]["proposed"] == 1
    assert "cost_usd" in row
