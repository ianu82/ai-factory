"""Review identity, immutable-head attempts and delivery separation."""

import asyncio

import pytest
from sqlalchemy import create_engine, inspect, select
from sqlalchemy.exc import IntegrityError

from aifactory import db, serializers as S
from aifactory.models import (Base, Organization, Outcome, Plan, PullReview, Repository,
                              REVIEW_STATUSES, ReviewAttempt, Run, Task, Workspace)
from aifactory.pipeline.delivery import TaskRunner
from aifactory.pipeline.orchestrator import Orchestrator
from aifactory.pipeline.planning import approve_plan, plan_outcome
from aifactory.pull_reviews import get_or_create_attempt, get_or_create_review


@pytest.fixture
def repo_id():
    with db.session_scope() as s:
        org = Organization(name="Reviews")
        s.add(org)
        s.flush()
        ws = Workspace(org_id=org.id, name="Reviews", slug="reviews")
        s.add(ws)
        s.flush()
        repo = Repository(workspace_id=ws.id, full_name="acme/app")
        s.add(repo)
        s.flush()
        return repo.id


def test_review_creation_is_idempotent_and_runs_share_outcome(repo_id):
    with db.session_scope() as s:
        repo = s.get(Repository, repo_id)
        review = get_or_create_review(s, repo, 42)
        attempt = get_or_create_attempt(s, review, "head1", "base1")
        review.head_sha, review.comment_id = "head1", 2**40
        attempt.verdict = {"passed": True, "findings": []}
        attempt.evidence = {"checks": [{"command": "pytest", "ok": True}]}
        attempt.status = "passed"
        attempt.started_at = attempt.finished_at = db.utcnow()
        rid, aid, oid = review.id, attempt.id, review.outcome_id
    with db.session_scope() as s:
        repo = s.get(Repository, repo_id)
        review = get_or_create_review(s, repo, 42)
        assert review.id == rid and review.outcome_id == oid
        attempt = get_or_create_attempt(s, review, "head1", "different-base")
        assert attempt.id == aid and attempt.base_sha == "base1"
        assert attempt.verdict == {"passed": True, "findings": []}
        assert S.pull_review(review)["comment_id"] == 2**40
        view = S.review_attempt(attempt)
        assert view["evidence"] == {"checks": [{"command": "pytest", "ok": True}]}
        assert all(view[key].endswith("Z") for key in ("created_at", "updated_at", "started_at", "finished_at"))
        second = get_or_create_attempt(s, review, "head2", "base2")
        assert second.id != aid and second.status == "queued" and second.verdict == second.evidence == {}
        assert S.review_attempt(second)["finished_at"] is None
        assert review.outcome.source == "review" and review.outcome.status != "planning"
        for cost in (1.25, 2.5):
            s.add(Run(workspace_id=repo.workspace_id, outcome_id=oid, role="reviewer", harness="native", cost_usd=cost))
        assert s.query(Outcome).count() == 1
        assert s.get(Workspace, repo.workspace_id).outcome_seq == 1
    with db.session_scope() as s:
        assert sum(r.cost_usd for r in s.query(Run).filter_by(outcome_id=oid)) == 3.75
        assert s.query(ReviewAttempt).count() == 2


def test_database_enforces_identity_attempt_and_outcome_uniqueness(repo_id):
    with db.session_scope() as s:
        review = get_or_create_review(s, s.get(Repository, repo_id), 42)
        get_or_create_attempt(s, review, "head", "base")
        rid, wid, oid = review.id, review.workspace_id, review.outcome_id
    for duplicate in (
        PullReview(repo_id=repo_id, workspace_id=wid, pr_number=42, outcome_id=oid),
        PullReview(repo_id=repo_id, workspace_id=wid, pr_number=43, outcome_id=oid),
        ReviewAttempt(review_id=rid, head_sha="head", base_sha="new-base"),
    ):
        with pytest.raises(IntegrityError), db.session_scope() as s:
            s.add(duplicate)
            s.flush()
    with db.session_scope() as s:
        assert s.query(PullReview).count() == s.query(ReviewAttempt).count() == 1
        # A different PR has its own outcome, and may review the same SHA.
        other = get_or_create_review(s, s.get(Repository, repo_id), 43)
        assert other.outcome_id != oid
        get_or_create_attempt(s, other, "head", "base")


def test_review_attempt_stable_lifecycle(repo_id):
    assert REVIEW_STATUSES == ("queued", "running", "passed", "failed", "blocked", "superseded")
    with db.session_scope() as s:
        review = get_or_create_review(s, s.get(Repository, repo_id), 1)
        attempt = get_or_create_attempt(s, review, "head", "base")
        aid = attempt.id
        for status in REVIEW_STATUSES:
            attempt.status = status
            s.flush()
            assert S.review_attempt(attempt)["status"] == status
    with pytest.raises(IntegrityError), db.session_scope() as s:
        s.get(ReviewAttempt, aid).status = "unknown"
        s.flush()


def test_init_engine_adds_review_tables_to_existing_database(tmp_path):
    url = f"sqlite:///{tmp_path / 'existing.db'}"
    engine = create_engine(url)
    old_tables = [t for t in Base.metadata.sorted_tables if t.name not in ("pull_reviews", "review_attempts")]
    Base.metadata.create_all(engine, tables=old_tables)
    with engine.begin() as conn:
        conn.execute(Organization.__table__.insert().values(id="org", name="Legacy"))
        conn.execute(Workspace.__table__.insert().values(id="ws", org_id="org", name="Legacy", slug="legacy"))
        conn.execute(Repository.__table__.insert().values(id="repo", workspace_id="ws", full_name="acme/old", config={}))
    assert "pull_reviews" not in inspect(engine).get_table_names()
    engine.dispose()
    engine = db.init_engine(url)
    assert {"pull_reviews", "review_attempts"} <= set(inspect(engine).get_table_names())
    with db.session_scope() as s:
        repo = s.get(Repository, "repo")
        assert S.repository(repo)["config"]["review"] == {"mode": "off", "allow_untrusted_local": False}
        assert repo.config == {}  # Defaults do not rewrite old rows.
        get_or_create_attempt(s, get_or_create_review(s, repo, 1), "head", "base")
    db.init_engine(url)  # Repeated startup is additive and safe.
    with db.session_scope() as s:
        assert s.scalar(select(PullReview.pr_number)) == 1


def test_review_outcomes_cannot_be_scheduled_or_directly_executed(repo_id, monkeypatch):
    with db.session_scope() as s:
        review = get_or_create_review(s, s.get(Repository, repo_id), 1)
        # Even an accidentally changed status/stray task must never start delivery.
        review.outcome.status = "planning"
        plan = Plan(outcome_id=review.outcome_id, content={"tasks": []})
        s.add(plan)
        s.flush()
        task = Task(workspace_id=review.workspace_id, outcome_id=review.outcome_id, plan_id=plan.id,
                    repo_id=repo_id, key="T1", title="Must not deliver", status="queued")
        s.add(task)
        s.flush()
        oid, pid, tid = review.outcome_id, plan.id, task.id
    orch = Orchestrator()
    dispatched = []
    monkeypatch.setattr(orch, "_spawn_plan", lambda oid: dispatched.append(oid))
    monkeypatch.setattr(orch, "_spawn_task", lambda tid: dispatched.append(tid))
    orch._schedule()
    assert dispatched == []
    with pytest.raises(ValueError, match="cannot be planned"):
        asyncio.run(plan_outcome(oid, asyncio.Event()))
    with pytest.raises(ValueError, match="cannot be planned"):
        approve_plan(pid, None)
    with pytest.raises(ValueError, match="cannot enter task delivery"):
        TaskRunner(tid, asyncio.Event(), orch.repo_lock)
    with db.session_scope() as s:
        s.get(Outcome, oid).status = "running"
    orch._promote()
    with db.session_scope() as s:
        assert s.get(Outcome, oid).status == "running"
        assert s.get(Plan, pid).status == "proposed"
        assert s.query(Run).count() == 0
