"""Ledger contracts: all evidence is local and no GitHub/provider is required."""
import json
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from threading import Barrier

import pytest
from sqlalchemy import create_engine, inspect
from sqlalchemy.exc import IntegrityError

from aifactory import db
from aifactory.models import (
    Artifact, Event, GateVerdict, MergeRegret, Organization, Outcome, Plan,
    Repository, Run, Signal, Task, Workspace,
)
from aifactory.regrets import (
    ObservationWindow, RegretScope, merge_gate_passes, record_gate_verdict,
    record_regret, regret_dict, regrets_for_scope,
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
    outcome = Outcome(workspace_id=ws.id, number=1, title="Ship", repo_ids=[repo.id])
    s.add(outcome)
    s.flush()
    plan = Plan(outcome_id=outcome.id)
    s.add(plan)
    s.flush()
    task = Task(workspace_id=ws.id, outcome_id=outcome.id, plan_id=plan.id, key="T1", title="Ship",
                repo_id=repo.id, head_sha="head", merge_sha="merge", pr_number=7, pr_url="https://github/acme/pull/7",
                evidence={"merge": {"at": START.isoformat()}})
    s.add(task)
    s.flush()
    run = Run(workspace_id=ws.id, outcome_id=outcome.id, task_id=task.id, role="verifier", harness="native")
    s.add(run)
    s.flush()
    return ws, repo, outcome, task, run


def fact(s, ws, task, **kwargs):
    return record_regret(s, ws.id, task.id, **{
        "kind": "smoke_failure", "source_key": "smoke:1", "reporter": "factory",
        "occurred_at": START + timedelta(days=1), **kwargs,
    })


def test_attribution_replay_kinds_and_canonical_run_evidence():
    with db.session_scope() as s:
        ws, repo, outcome, task, run = seed(s)
        row, new = fact(s, ws, task, run_id=run.id, reporter="alice", url="https://source/1",
                        detected_at="2026-02-01T12:00:00+02:00", evidence={"reason": "broken"})
        assert new
        sig = s.get(Signal, row.signal_id)
        sig.status, sig.outcome_id = "resolved", outcome.id
        replay, new = fact(s, ws, task, reporter="bob", fix_url="https://fix")
        assert not new and replay.id == row.id
        assert row.reporter == "alice" and row.fix_url == ""
        assert sig.status == "resolved" and sig.count == 1
        e = s.query(Event).filter_by(kind="merge_regret").one()
        a = s.query(Artifact).filter_by(kind="regret").one()
        assert (e.actor, e.task_id, e.outcome_id, e.run_id) == ("alice", task.id, outcome.id, run.id)
        assert (a.task_id, a.run_id, a.data["regret_id"]) == (task.id, run.id, row.id)
        data = json.loads(json.dumps(regrets_for_scope(s, RegretScope(ws.id, run_id=run.id))))[0]
        assert data["occurred_at"] == "2026-01-02T00:00:00Z"
        assert data["detected_at"] == "2026-02-01T10:00:00Z"
        assert data["in_window"] and data["url"] == "https://source/1"
        assert data["evidence"] == {"reason": "broken"} and data["reporter"] == "alice"
        assert run.result == {}
        for kind in ("factory_revert", "github_revert", "linked_cause"):
            assert fact(s, ws, task, kind=kind)[1]
        assert s.query(MergeRegret).count() == 4
        assert s.query(Signal).count() == 4
        assert s.query(Event).filter_by(kind="merge_regret").count() == 4


@pytest.mark.parametrize("offset,status", [(-1, "outside_window"), (0, "in_window"),
                                           (14, "in_window"), (14.001, "outside_window")])
def test_occurrence_bounds_not_detection(offset, status):
    with db.session_scope() as s:
        ws, _, _, task, _ = seed(s)
        row, _ = fact(s, ws, task, occurred_at=START + timedelta(days=offset),
                      detected_at=START + timedelta(days=90))
        assert regret_dict(row)["window_status"] == status
        assert regret_dict(row, ObservationWindow(1))["in_window"] == (0 <= offset <= 1)
        assert s.query(Artifact).count() == 1  # retained even when excluded
        ws.settings = {"regret_window_days": 2}
        assert ObservationWindow.for_workspace(s, ws.id).days == 2


@pytest.mark.parametrize("value", [None, "", "garbage"])
@pytest.mark.parametrize("field", ["merge", "occurrence"])
def test_missing_dates_never_fall_back_to_finished_or_detection(value, field):
    with db.session_scope() as s:
        ws, _, _, task, _ = seed(s)
        task.finished_at = START
        if field == "merge":
            task.evidence = {"merge": {"at": value}}
        row, _ = fact(s, ws, task, occurred_at=value if field == "occurrence" else START)
        assert regret_dict(row)["window_status"] == "unknown"
        assert not regret_dict(row)["in_window"]


@pytest.mark.parametrize("link", ["source", "fix", "url", "source_fix"])
def test_links_reuse_and_do_not_propose_duplicate_fixes(link):
    with db.session_scope() as s:
        ws, repo, fix, task, _ = seed(s)
        sig = Signal(workspace_id=ws.id, repo_id=repo.id, source="github_issue", fingerprint="source",
                     title="Bug", status="accepted", outcome_id=fix.id if link == "source_fix" else None)
        s.add(sig)
        s.flush()
        args = {"source_signal_id": sig.id} if link in {"source", "source_fix"} else (
            {"fix_outcome_id": fix.id} if link == "fix" else {"fix_url": "https://fix"})
        row, _ = fact(s, ws, task, **args)
        assert row.signal_id == (sig.id if link == "source" else None)
        assert sig.status == "accepted" and sig.count == 1
        assert row.fix_outcome_id == (fix.id if link in {"fix", "source_fix"} else None)
        assert not fact(s, ws, task)[1]
        assert s.query(Signal).count() == 1


@pytest.mark.parametrize("foreign", ["task", "repo", "signal", "fix", "run", "fix_repo", "signal_repo"])
def test_ownership_rejected_even_on_replay(foreign):
    with db.session_scope() as s:
        ws, repo, fix, task, run = seed(s)
        other, other_repo, other_fix, other_task, other_run = seed(s, "other")
        sig = Signal(workspace_id=other.id if foreign == "signal" else ws.id, repo_id=other_repo.id,
                     source="feedback", fingerprint="foreign", title="Bug")
        s.add(sig)
        s.flush()
        fact(s, ws, task)
        args = {}
        target = task
        if foreign == "task":
            target = other_task
        elif foreign == "repo":
            args["repo_id"] = other_repo.id
        elif foreign in {"signal", "signal_repo"}:
            args["source_signal_id"] = sig.id
        elif foreign == "run":
            args["run_id"] = other_run.id
        else:
            if foreign == "fix_repo":
                fix.repo_ids = [other_repo.id]
            args["fix_outcome_id"] = other_fix.id if foreign == "fix" else fix.id
        with pytest.raises(ValueError):
            fact(s, ws, target, **args)
        assert s.query(MergeRegret).count() == 1
        with pytest.raises(ValueError):
            regrets_for_scope(s, RegretScope(ws.id, run_id=other_run.id))
        with pytest.raises(ValueError):
            record_gate_verdict(s, ws.id, other_task.id, gate="verifier", attempt=1,
                                verdict="pass", head_sha="head")


def test_sessions_are_distinct_but_merge_gate_population_is_not():
    with db.session_scope() as s:
        ws, _, outcome, task, run = seed(s)
        second = Run(workspace_id=ws.id, outcome_id=outcome.id, task_id=task.id, role="verifier", harness="native")
        s.add(second)
        s.flush()
        args = dict(gate="verifier", attempt=1, verdict="pass", head_sha="head",
                    candidate_fingerprint="diff", score=0.9, at=START, evidence={"passed": True})
        row, new = record_gate_verdict(s, ws.id, task.id, run_id=run.id, **args)
        assert new and row.score == 0.9 and row.at == START
        assert not record_gate_verdict(s, ws.id, task.id, run_id=run.id, **args)[1]
        assert record_gate_verdict(s, ws.id, task.id, run_id=second.id, **args)[1]
        assert record_gate_verdict(s, ws.id, task.id, run_id=second.id, session_key="retry", **args)[1]
        assert s.query(GateVerdict).count() == 3
        assert merge_gate_passes(s, ws.id) == {(task.id, "verifier")}
        record_gate_verdict(s, ws.id, task.id, gate="risk", attempt=1, verdict="pass", head_sha="old-head")
        assert merge_gate_passes(s, ws.id) == {(task.id, "verifier")}
        # Direct inserts are protected too, not only the helper's read-before-write.
        with pytest.raises(IntegrityError), s.begin_nested():
            s.add(GateVerdict(workspace_id=ws.id, task_id=task.id, gate=row.gate, attempt=row.attempt,
                              candidate_key=row.candidate_key, session_key=row.session_key, verdict="fail"))
            s.flush()


def test_atomic_failure_and_caller_rollback(monkeypatch):
    from aifactory import regrets

    with db.session_scope() as s:
        ws, _, _, task, _ = seed(s)
        ids = ws.id, task.id
    original = regrets.record

    def fail(*args, **kwargs):
        raise RuntimeError("journal unavailable")

    with db.SessionLocal() as s:
        monkeypatch.setattr(regrets, "record", fail)
        with pytest.raises(RuntimeError):
            record_regret(s, *ids, kind="linked_cause", source_key="bug", reporter="alice")
        s.commit()
        assert s.query(MergeRegret).count() == s.query(Signal).count() == 0
        monkeypatch.setattr(regrets, "record", original)
        record_regret(s, *ids, kind="linked_cause", source_key="bug", reporter="alice")
        s.rollback()
    with db.SessionLocal() as s:
        assert s.query(MergeRegret).count() == s.query(Signal).count() == s.query(Artifact).count() == 0


def test_concurrent_source_ingestion():
    with db.session_scope() as s:
        ws, _, _, task, _ = seed(s)
        ids = ws.id, task.id
    barrier = Barrier(4)

    def ingest(_):
        with db.session_scope() as s:
            barrier.wait(timeout=10)
            row, new = record_regret(s, *ids, kind="github_revert", source_key="github:commit:abc",
                                     reporter="github", occurred_at=START)
            return row.id, new

    with ThreadPoolExecutor(max_workers=4) as pool:
        results = list(pool.map(ingest, range(4)))
    assert len({row_id for row_id, _ in results}) == 1
    assert sum(new for _, new in results) == 1
    with db.SessionLocal() as s:
        assert s.query(MergeRegret).count() == s.query(Signal).count() == s.query(Artifact).count() == 1
        assert s.query(Event).filter_by(kind="merge_regret").count() == 1


def test_initialization_adds_tables_without_altering_tasks_or_runs(tmp_path):
    url = f"sqlite:///{tmp_path / 'legacy.db'}"
    engine = create_engine(url)
    # Existing product schema minus the two new tables.
    db.Base.metadata.create_all(engine, tables=[t for t in db.Base.metadata.sorted_tables
                                                if t.name not in {"merge_regrets", "gate_verdicts"}])
    before = {name: inspect(engine).get_columns(name) for name in ("tasks", "runs")}
    engine.dispose()
    engine = db.init_engine(url)
    for name in ("tasks", "runs"):
        assert [(c["name"], str(c["type"]), c["nullable"]) for c in inspect(engine).get_columns(name)] == [
            (c["name"], str(c["type"]), c["nullable"]) for c in before[name]]
    for name in ("merge_regrets", "gate_verdicts"):
        assert inspect(engine).has_table(name)
        assert inspect(engine).get_unique_constraints(name)
        assert inspect(engine).get_indexes(name)
    db.init_engine(url)  # repeated initialization is harmless


def test_source_signal_cannot_smuggle_foreign_fix():
    with db.session_scope() as s:
        ws, repo, _, task, _ = seed(s)
        _, _, foreign_fix, _, _ = seed(s, "foreign")
        sig = Signal(workspace_id=ws.id, repo_id=repo.id, source="feedback", fingerprint="linked",
                     title="Bug", outcome_id=foreign_fix.id)
        s.add(sig)
        s.flush()
        with pytest.raises(ValueError, match="Fix outcome"):
            fact(s, ws, task, source_signal_id=sig.id)
        assert s.query(MergeRegret).count() == 0


@pytest.mark.parametrize("kwargs", [{"source_key": ""}, {"source_key": "x" * 501},
                                     {"reporter": " "}, {"kind": "typo"}, {"detected_at": ""}])
def test_invalid_fact_identity_rejected(kwargs):
    with db.session_scope() as s:
        ws, _, _, task, _ = seed(s)
        with pytest.raises(ValueError):
            fact(s, ws, task, **kwargs)
        assert s.query(MergeRegret).count() == 0


def test_database_rejects_duplicate_source_fact():
    with db.session_scope() as s:
        ws, repo, _, task, _ = seed(s)
        row, _ = fact(s, ws, task)
        with pytest.raises(IntegrityError), s.begin_nested():
            s.add(MergeRegret(workspace_id=ws.id, task_id=task.id, repo_id=repo.id,
                              kind=row.kind, source_key=row.source_key, reporter="another"))
            s.flush()


def test_gate_concurrent_replay():
    with db.session_scope() as s:
        ws, _, _, task, _ = seed(s)
        ids = ws.id, task.id
    barrier = Barrier(3)

    def ingest(_):
        with db.session_scope() as s:
            barrier.wait(timeout=10)
            row, new = record_gate_verdict(s, *ids, gate="checks", attempt=1, verdict="pass", head_sha="head")
            return row.id, new

    with ThreadPoolExecutor(max_workers=3) as pool:
        results = list(pool.map(ingest, range(3)))
    assert len({row_id for row_id, _ in results}) == 1
    assert sum(new for _, new in results) == 1
