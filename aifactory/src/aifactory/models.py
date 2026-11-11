"""Persistent product model.

Hierarchy: Organization -> Workspace -> (Repository, Secret, Outcome -> Plan, Task -> Run).
Events are the append-only journal; Decisions are attributed human choices (and the
attention queue); Signals are incoming product feedback.
"""

from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any

from sqlalchemy import (
    JSON,
    BigInteger,
    Boolean,
    CheckConstraint,
    DateTime,
    Float,
    ForeignKey,
    Integer,
    String,
    Text,
    UniqueConstraint,
    Index,
)
from sqlalchemy import event, inspect
from sqlalchemy.orm import Mapped, mapped_column, relationship

from .db import Base, utcnow


def new_id() -> str:
    return uuid.uuid4().hex


class Organization(Base):
    __tablename__ = "organizations"
    id: Mapped[str] = mapped_column(String(32), primary_key=True, default=new_id)
    name: Mapped[str] = mapped_column(String(200))
    mindshub_org_id: Mapped[str | None] = mapped_column(String(64), unique=True, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow)


class User(Base):
    __tablename__ = "users"
    id: Mapped[str] = mapped_column(String(32), primary_key=True, default=new_id)
    email: Mapped[str | None] = mapped_column(String(320), nullable=True, index=True)
    name: Mapped[str] = mapped_column(String(200), default="")
    mindshub_user_id: Mapped[str | None] = mapped_column(String(64), unique=True, nullable=True)
    is_system: Mapped[bool] = mapped_column(Boolean, default=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow)
    last_seen_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)


class Workspace(Base):
    __tablename__ = "workspaces"
    id: Mapped[str] = mapped_column(String(32), primary_key=True, default=new_id)
    org_id: Mapped[str] = mapped_column(ForeignKey("organizations.id"), index=True)
    name: Mapped[str] = mapped_column(String(200))
    slug: Mapped[str] = mapped_column(String(80))
    description: Mapped[str] = mapped_column(Text, default="")
    mindshub_workspace_id: Mapped[str | None] = mapped_column(String(64), nullable=True)
    settings: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)
    onboarding: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)
    outcome_seq: Mapped[int] = mapped_column(Integer, default=0)
    paused: Mapped[bool] = mapped_column(Boolean, default=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow)

    __table_args__ = (UniqueConstraint("org_id", "slug"),)


class Membership(Base):
    __tablename__ = "memberships"
    id: Mapped[str] = mapped_column(String(32), primary_key=True, default=new_id)
    workspace_id: Mapped[str] = mapped_column(ForeignKey("workspaces.id"), index=True)
    user_id: Mapped[str] = mapped_column(ForeignKey("users.id"), index=True)
    role: Mapped[str] = mapped_column(String(20), default="member")  # owner|admin|member|viewer
    last_viewed_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow)

    __table_args__ = (UniqueConstraint("workspace_id", "user_id"),)


class Secret(Base):
    """An encrypted workspace credential. Values never leave the control plane."""

    __tablename__ = "secrets"
    id: Mapped[str] = mapped_column(String(32), primary_key=True, default=new_id)
    workspace_id: Mapped[str] = mapped_column(ForeignKey("workspaces.id"), index=True)
    kind: Mapped[str] = mapped_column(String(40))  # mindshub_api_key|openai_api_key|github_token
    ciphertext: Mapped[str] = mapped_column(Text)
    hint: Mapped[str] = mapped_column(String(64), default="")
    status: Mapped[str] = mapped_column(String(20), default="untested")  # untested|valid|invalid
    status_detail: Mapped[str] = mapped_column(Text, default="")
    metadata_json: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)
    tested_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    updated_by: Mapped[str | None] = mapped_column(String(32), nullable=True)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow, onupdate=utcnow)

    __table_args__ = (UniqueConstraint("workspace_id", "kind"),)


class Repository(Base):
    __tablename__ = "repositories"
    id: Mapped[str] = mapped_column(String(32), primary_key=True, default=new_id)
    workspace_id: Mapped[str] = mapped_column(ForeignKey("workspaces.id"), index=True)
    full_name: Mapped[str] = mapped_column(String(200))
    default_branch: Mapped[str] = mapped_column(String(200), default="main")
    description: Mapped[str] = mapped_column(Text, default="")
    config: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow)

    # Delete review identities/evidence on disconnect, retaining outcomes and runs
    # as the cumulative cost history, just like other outcome attribution.
    reviews: Mapped[list["PullReview"]] = relationship(cascade="all, delete-orphan")

    __table_args__ = (UniqueConstraint("workspace_id", "full_name"),)


class Workstream(Base):
    __tablename__ = "workstreams"
    id: Mapped[str] = mapped_column(String(32), primary_key=True, default=new_id)
    workspace_id: Mapped[str] = mapped_column(ForeignKey("workspaces.id"), index=True)
    slug: Mapped[str] = mapped_column(String(80))
    name: Mapped[str] = mapped_column(String(200))
    goal: Mapped[str] = mapped_column(String(2000))
    owner_id: Mapped[str] = mapped_column(ForeignKey("users.id"), index=True)
    default_repo_ids: Mapped[list[str]] = mapped_column(JSON, default=list)
    state: Mapped[str] = mapped_column(String(20), default="active")
    worker_override: Mapped[str | None] = mapped_column(String(20), nullable=True)
    plan_approval_override: Mapped[str | None] = mapped_column(String(20), nullable=True)
    merge_approval_override: Mapped[str | None] = mapped_column(String(20), nullable=True)
    priority_override: Mapped[int | None] = mapped_column(Integer, nullable=True)
    outcome_budget_override: Mapped[float | None] = mapped_column(Float, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow, onupdate=utcnow)

    __table_args__ = (UniqueConstraint("workspace_id", "slug"),)


class Outcome(Base):
    """What a team asked for. Decomposed by a plan into tasks."""

    __tablename__ = "outcomes"
    id: Mapped[str] = mapped_column(String(32), primary_key=True, default=new_id)
    workspace_id: Mapped[str] = mapped_column(ForeignKey("workspaces.id"), index=True)
    workstream_id: Mapped[str | None] = mapped_column(ForeignKey("workstreams.id"), nullable=True, index=True)
    number: Mapped[int] = mapped_column(Integer)
    title: Mapped[str] = mapped_column(String(300))
    brief: Mapped[str] = mapped_column(Text, default="")
    acceptance: Mapped[list[str]] = mapped_column(JSON, default=list)
    repo_ids: Mapped[list[str]] = mapped_column(JSON, default=list)
    harness: Mapped[str | None] = mapped_column(String(20), nullable=True)
    status: Mapped[str] = mapped_column(String(30), default="planning", index=True)
    priority: Mapped[int] = mapped_column(Integer, default=2)  # 0 urgent .. 3 low
    source: Mapped[str] = mapped_column(String(40), default="user")  # user|signal|improvement|review
    signal_id: Mapped[str | None] = mapped_column(String(32), nullable=True)
    summary: Mapped[str] = mapped_column(Text, default="")
    blocked_reason: Mapped[str] = mapped_column(Text, default="")
    created_by: Mapped[str | None] = mapped_column(ForeignKey("users.id"), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow, onupdate=utcnow)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)

    plans: Mapped[list["Plan"]] = relationship(back_populates="outcome", order_by="Plan.version")
    tasks: Mapped[list["Task"]] = relationship(back_populates="outcome", order_by="Task.key")

    __table_args__ = (UniqueConstraint("workspace_id", "number"),)


class LinearOperation(Base):
    __tablename__ = "linear_operations"
    id: Mapped[str] = mapped_column(String(32), primary_key=True, default=new_id)
    workspace_id: Mapped[str] = mapped_column(ForeignKey("workspaces.id"), index=True)
    outcome_id: Mapped[str] = mapped_column(ForeignKey("outcomes.id"), index=True)
    issue_id: Mapped[str] = mapped_column(String(200))
    semantic_key: Mapped[str] = mapped_column(String(64))
    kind: Mapped[str] = mapped_column(String(20))
    payload: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)
    status: Mapped[str] = mapped_column(String(20), default="pending")
    revision: Mapped[int] = mapped_column(Integer, default=0)
    remote_id: Mapped[str | None] = mapped_column(String(36), nullable=True)
    claim: Mapped[str] = mapped_column(String(32), default="")
    lease_until: Mapped[datetime] = mapped_column(DateTime, default=utcnow)
    next_attempt: Mapped[datetime] = mapped_column(DateTime, default=utcnow, index=True)
    attempts: Mapped[int] = mapped_column(Integer, default=0)
    error: Mapped[str] = mapped_column(String(30), default="")
    created_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow)
    sent_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)

    __table_args__ = (UniqueConstraint("workspace_id", "semantic_key"),)


class LinearWarningOperation(Base):
    __tablename__ = "linear_warning_operations"
    key: Mapped[str] = mapped_column(String(64), primary_key=True)
    workspace_id: Mapped[str] = mapped_column(ForeignKey("workspaces.id"), index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow)


# Stable states for one review of one immutable PR head.
REVIEW_STATUSES = ("queued", "running", "passed", "failed", "blocked", "superseded")


class PullReview(Base):
    """One PR identity and its cumulative, review-only budgeting outcome."""

    __tablename__ = "pull_reviews"
    id: Mapped[str] = mapped_column(String(32), primary_key=True, default=new_id)
    workspace_id: Mapped[str] = mapped_column(ForeignKey("workspaces.id"), index=True)
    repo_id: Mapped[str] = mapped_column(ForeignKey("repositories.id"), index=True)
    pr_number: Mapped[int] = mapped_column(Integer)
    outcome_id: Mapped[str] = mapped_column(ForeignKey("outcomes.id"), unique=True)
    head_sha: Mapped[str] = mapped_column(String(64), default="")
    comment_id: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow, onupdate=utcnow)

    outcome: Mapped[Outcome] = relationship()
    attempts: Mapped[list["ReviewAttempt"]] = relationship(
        back_populates="review", order_by="ReviewAttempt.created_at", cascade="all, delete-orphan")

    __table_args__ = (UniqueConstraint("repo_id", "pr_number"),)


class ReviewAttempt(Base):
    """Durable verdict and evidence for a PR head; retries reuse this record."""

    __tablename__ = "review_attempts"
    id: Mapped[str] = mapped_column(String(32), primary_key=True, default=new_id)
    review_id: Mapped[str] = mapped_column(ForeignKey("pull_reviews.id"), index=True)
    head_sha: Mapped[str] = mapped_column(String(64))
    base_sha: Mapped[str] = mapped_column(String(64))
    status: Mapped[str] = mapped_column(String(20), default="queued", index=True)
    verdict: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)
    evidence: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow, onupdate=utcnow)
    started_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)

    review: Mapped[PullReview] = relationship(back_populates="attempts")

    __table_args__ = (
        UniqueConstraint("review_id", "head_sha"),
        CheckConstraint("status IN ('queued', 'running', 'passed', 'failed', 'blocked', 'superseded')",
                        name="ck_review_attempt_status"),
    )


class Plan(Base):
    __tablename__ = "plans"
    id: Mapped[str] = mapped_column(String(32), primary_key=True, default=new_id)
    outcome_id: Mapped[str] = mapped_column(ForeignKey("outcomes.id"), index=True)
    version: Mapped[int] = mapped_column(Integer, default=1)
    status: Mapped[str] = mapped_column(String(20), default="proposed")  # proposed|approved|rejected|superseded
    summary: Mapped[str] = mapped_column(Text, default="")
    content: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)
    run_id: Mapped[str | None] = mapped_column(String(32), nullable=True)
    approved_by: Mapped[str | None] = mapped_column(String(32), nullable=True)
    approved_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow)

    outcome: Mapped[Outcome] = relationship(back_populates="plans")


class Task(Base):
    __tablename__ = "tasks"
    id: Mapped[str] = mapped_column(String(32), primary_key=True, default=new_id)
    workspace_id: Mapped[str] = mapped_column(ForeignKey("workspaces.id"), index=True)
    outcome_id: Mapped[str] = mapped_column(ForeignKey("outcomes.id"), index=True)
    plan_id: Mapped[str] = mapped_column(ForeignKey("plans.id"))
    key: Mapped[str] = mapped_column(String(20))
    title: Mapped[str] = mapped_column(String(300))
    spec: Mapped[str] = mapped_column(Text, default="")
    acceptance: Mapped[list[str]] = mapped_column(JSON, default=list)
    repo_id: Mapped[str] = mapped_column(ForeignKey("repositories.id"))
    depends_on: Mapped[list[str]] = mapped_column(JSON, default=list)
    harness: Mapped[str] = mapped_column(String(20), default="native")
    verifier_harness: Mapped[str] = mapped_column(String(20), default="native")
    status: Mapped[str] = mapped_column(String(30), default="pending", index=True)
    stage: Mapped[str] = mapped_column(String(30), default="")
    attempt: Mapped[int] = mapped_column(Integer, default=0)
    base_sha: Mapped[str] = mapped_column(String(64), default="")
    branch: Mapped[str] = mapped_column(String(200), default="")
    head_sha: Mapped[str] = mapped_column(String(64), default="")
    pr_number: Mapped[int | None] = mapped_column(Integer, nullable=True)
    pr_url: Mapped[str] = mapped_column(String(400), default="")
    merge_sha: Mapped[str] = mapped_column(String(64), default="")
    summary: Mapped[str] = mapped_column(Text, default="")
    blocked_reason: Mapped[str] = mapped_column(Text, default="")
    evidence: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)
    lease_owner: Mapped[str] = mapped_column(String(64), default="")
    lease_expires_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow)
    started_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow, onupdate=utcnow)

    outcome: Mapped[Outcome] = relationship(back_populates="tasks")

    __table_args__ = (UniqueConstraint("outcome_id", "key"),)


class BenchmarkSuite(Base):
    """An immutable, workspace-local snapshot. Changed content creates a new revision."""

    __tablename__ = "benchmark_suites"
    id: Mapped[str] = mapped_column(String(32), primary_key=True, default=new_id)
    workspace_id: Mapped[str] = mapped_column(ForeignKey("workspaces.id"), index=True)
    name: Mapped[str] = mapped_column(String(80))
    revision: Mapped[int] = mapped_column(Integer)
    schema_version: Mapped[int] = mapped_column(Integer)
    content_hash: Mapped[str] = mapped_column(String(64))
    tasks: Mapped[list[dict[str, Any]]] = mapped_column(JSON)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow)

    __table_args__ = (
        UniqueConstraint("workspace_id", "name", "revision"),
        UniqueConstraint("workspace_id", "name", "content_hash"),
    )


@event.listens_for(BenchmarkSuite, "before_update")
def _immutable_benchmark_suite(_mapper, _connection, suite: BenchmarkSuite) -> None:
    if any(inspect(suite).attrs[key].history.has_changes() for key in (
        "workspace_id", "name", "revision", "schema_version", "content_hash", "tasks",
    )):
        raise ValueError("Benchmark suite snapshots are immutable; load a new revision instead")


class BenchmarkTrial(Base):
    """Reserved execution record; loading/importing suites never executes a trial."""

    __tablename__ = "benchmark_trials"
    id: Mapped[str] = mapped_column(String(32), primary_key=True, default=new_id)
    workspace_id: Mapped[str] = mapped_column(ForeignKey("workspaces.id"), index=True)
    suite_id: Mapped[str] = mapped_column(ForeignKey("benchmark_suites.id"), index=True)
    suite_revision: Mapped[int] = mapped_column(Integer)
    task_key: Mapped[str] = mapped_column(String(100))
    trial_number: Mapped[int] = mapped_column(Integer)
    harness: Mapped[str] = mapped_column(String(20), default="")
    model: Mapped[str] = mapped_column(String(100), default="")
    config_version: Mapped[str] = mapped_column(String(64), default="")
    configuration: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)
    outcome_id: Mapped[str | None] = mapped_column(ForeignKey("outcomes.id"), nullable=True)
    task_id: Mapped[str | None] = mapped_column(ForeignKey("tasks.id"), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow)
    started_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    status: Mapped[str] = mapped_column(String(20), default="pending")
    passed: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    checks: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)
    verifier: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)
    cost_usd: Mapped[float | None] = mapped_column(Float, nullable=True)
    cost_known: Mapped[bool] = mapped_column(Boolean, default=False)


class Run(Base):
    """One agent session (planner, builder, verifier) and what it cost."""

    __tablename__ = "runs"
    id: Mapped[str] = mapped_column(String(32), primary_key=True, default=new_id)
    workspace_id: Mapped[str] = mapped_column(ForeignKey("workspaces.id"), index=True)
    outcome_id: Mapped[str] = mapped_column(ForeignKey("outcomes.id"), index=True)
    task_id: Mapped[str | None] = mapped_column(ForeignKey("tasks.id"), nullable=True, index=True)
    benchmark_trial_id: Mapped[str | None] = mapped_column(
        ForeignKey("benchmark_trials.id", name="fk_runs_benchmark_trial_id"), nullable=True, index=True,
    )
    config_version: Mapped[str | None] = mapped_column(String(16), nullable=True)
    config_snapshot: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)
    role: Mapped[str] = mapped_column(String(20))  # planner|builder|verifier
    harness: Mapped[str] = mapped_column(String(20))
    provider: Mapped[str] = mapped_column(String(20), default="")
    model: Mapped[str] = mapped_column(String(100), default="")
    attempt: Mapped[int] = mapped_column(Integer, default=1)
    # running|succeeded|failed|cancelled|error|interrupted (closed after a restart; not a verdict on the work)
    status: Mapped[str] = mapped_column(String(20), default="running")
    turns: Mapped[int] = mapped_column(Integer, default=0)
    input_tokens: Mapped[int] = mapped_column(Integer, default=0)
    output_tokens: Mapped[int] = mapped_column(Integer, default=0)
    cost_usd: Mapped[float] = mapped_column(Float, default=0.0)
    cost_known: Mapped[bool] = mapped_column(Boolean, default=True)
    result: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)
    error: Mapped[str] = mapped_column(Text, default="")
    started_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)


class Event(Base):
    __tablename__ = "events"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    workspace_id: Mapped[str] = mapped_column(String(32), index=True)
    outcome_id: Mapped[str | None] = mapped_column(String(32), nullable=True, index=True)
    task_id: Mapped[str | None] = mapped_column(String(32), nullable=True, index=True)
    run_id: Mapped[str | None] = mapped_column(String(32), nullable=True)
    actor: Mapped[str] = mapped_column(String(64), default="factory")
    level: Mapped[str] = mapped_column(String(10), default="info")  # info|success|warn|error
    kind: Mapped[str] = mapped_column(String(60))
    message: Mapped[str] = mapped_column(Text, default="")
    data: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow, index=True)


class Artifact(Base):
    __tablename__ = "artifacts"
    id: Mapped[str] = mapped_column(String(32), primary_key=True, default=new_id)
    workspace_id: Mapped[str] = mapped_column(String(32), index=True)
    outcome_id: Mapped[str | None] = mapped_column(String(32), nullable=True, index=True)
    task_id: Mapped[str | None] = mapped_column(String(32), nullable=True, index=True)
    run_id: Mapped[str | None] = mapped_column(String(32), nullable=True)
    kind: Mapped[str] = mapped_column(String(30))  # diff|log|report|check|ci|smoke|plan|transcript|attachment
    name: Mapped[str] = mapped_column(String(300))
    content: Mapped[str] = mapped_column(Text, default="")
    data: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow)


class Decision(Base):
    """A human decision: pending ones form the attention queue; resolved ones are precedent."""

    __tablename__ = "decisions"
    id: Mapped[str] = mapped_column(String(32), primary_key=True, default=new_id)
    workspace_id: Mapped[str] = mapped_column(String(32), index=True)
    outcome_id: Mapped[str | None] = mapped_column(String(32), nullable=True, index=True)
    task_id: Mapped[str | None] = mapped_column(String(32), nullable=True, index=True)
    kind: Mapped[str] = mapped_column(String(30))  # approve_plan|approve_merge|question|approve_release|unblock
    title: Mapped[str] = mapped_column(String(300))
    body: Mapped[str] = mapped_column(Text, default="")
    options: Mapped[list[dict[str, Any]]] = mapped_column(JSON, default=list)
    status: Mapped[str] = mapped_column(String(20), default="open", index=True)  # open|resolved|dismissed|superseded
    resolution: Mapped[str] = mapped_column(String(60), default="")
    comment: Mapped[str] = mapped_column(Text, default="")
    resolved_by: Mapped[str | None] = mapped_column(String(32), nullable=True)
    resolved_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    attribution: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)
    reusable: Mapped[bool] = mapped_column(Boolean, default=False)
    scope: Mapped[str] = mapped_column(String(30), default="item")  # item|repository|workspace
    # Absent on legacy questions: judges must not infer that missing safety data means safe.
    metadata_json: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)
    review: Mapped["DecisionReview | None"] = relationship(
        foreign_keys="DecisionReview.decision_id", uselist=False, lazy="selectin")
    created_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow)


class DecisionReview(Base):
    """One durable judge processing slot per question; reports stay structured JSON.

    A correction is a separate attributed Decision, never an edit of the cited answer.
    Question fingerprints let workers discard results for an obsolete revision.
    """

    __tablename__ = "decision_reviews"
    id: Mapped[str] = mapped_column(String(32), primary_key=True, default=new_id)
    workspace_id: Mapped[str] = mapped_column(ForeignKey("workspaces.id"), index=True)
    decision_id: Mapped[str] = mapped_column(ForeignKey("decisions.id"), unique=True)
    state: Mapped[str] = mapped_column(String(20), default="pending")
    question_revision: Mapped[int] = mapped_column(Integer, default=1)
    question_fingerprint: Mapped[str] = mapped_column(String(64), default="")
    run_id: Mapped[str | None] = mapped_column(ForeignKey("runs.id"), nullable=True)
    mode: Mapped[str] = mapped_column(String(20), default="decide")
    proposed_verdict: Mapped[str | None] = mapped_column(String(20), nullable=True)
    effective_verdict: Mapped[str | None] = mapped_column(String(20), nullable=True)
    answer: Mapped[str] = mapped_column(Text, default="")
    resolution: Mapped[str] = mapped_column(String(60), default="")
    summary: Mapped[str] = mapped_column(Text, default="")
    citations: Mapped[list[dict[str, Any]]] = mapped_column(JSON, default=list)
    closest_decisions: Mapped[list[str]] = mapped_column(JSON, default=list)
    report: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)
    correction_decision_id: Mapped[str | None] = mapped_column(ForeignKey("decisions.id"), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow, onupdate=utcnow)
    started_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)


class Message(Base):
    """Conversation scoped to an outcome or task: guidance from people, notes from the Factory."""

    __tablename__ = "messages"
    id: Mapped[str] = mapped_column(String(32), primary_key=True, default=new_id)
    workspace_id: Mapped[str] = mapped_column(String(32), index=True)
    outcome_id: Mapped[str | None] = mapped_column(String(32), nullable=True, index=True)
    task_id: Mapped[str | None] = mapped_column(String(32), nullable=True)
    author_id: Mapped[str | None] = mapped_column(String(32), nullable=True)
    author_kind: Mapped[str] = mapped_column(String(10), default="user")  # user|factory
    body: Mapped[str] = mapped_column(Text)
    attribution: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)
    delivered: Mapped[bool] = mapped_column(Boolean, default=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow)


class Signal(Base):
    __tablename__ = "signals"
    id: Mapped[str] = mapped_column(String(32), primary_key=True, default=new_id)
    workspace_id: Mapped[str] = mapped_column(String(32), index=True)
    repo_id: Mapped[str | None] = mapped_column(String(32), nullable=True)
    source: Mapped[str] = mapped_column(String(30))  # github_issue|ci_failure|feedback|smoke_failure|improvement
    external_id: Mapped[str] = mapped_column(String(200), default="")
    fingerprint: Mapped[str] = mapped_column(String(64), index=True)
    title: Mapped[str] = mapped_column(String(300))
    body: Mapped[str] = mapped_column(Text, default="")
    url: Mapped[str] = mapped_column(String(400), default="")
    count: Mapped[int] = mapped_column(Integer, default=1)
    status: Mapped[str] = mapped_column(String(20), default="new")  # new|proposed|accepted|ignored|resolved
    outcome_id: Mapped[str | None] = mapped_column(String(32), nullable=True)
    data: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True, default=dict)  # improvement proposals
    first_seen_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow)
    last_seen_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow)

    __table_args__ = (UniqueConstraint("workspace_id", "fingerprint"),)


class RunToken(Base):
    """Short-lived, run-scoped credential the sandbox uses to reach the inference gateway."""

    __tablename__ = "run_tokens"
    token_hash: Mapped[str] = mapped_column(String(64), primary_key=True)
    run_id: Mapped[str] = mapped_column(String(32), index=True)
    workspace_id: Mapped[str] = mapped_column(String(32))
    expires_at: Mapped[datetime] = mapped_column(DateTime)
    revoked: Mapped[bool] = mapped_column(Boolean, default=False)


class SessionRecord(Base):
    __tablename__ = "sessions"
    token_hash: Mapped[str] = mapped_column(String(64), primary_key=True)
    user_id: Mapped[str] = mapped_column(ForeignKey("users.id"), index=True)
    method: Mapped[str] = mapped_column(String(20))  # oidc|api_key|dev
    created_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow)
    expires_at: Mapped[datetime] = mapped_column(DateTime)


Index("ix_events_ws_id", Event.workspace_id, Event.id)


class MemoryEntry(Base):
    """Factory-owned long-term memory. A null repository denotes workspace scope."""

    __tablename__ = "memory_entries"
    id: Mapped[str] = mapped_column(String(32), primary_key=True, default=new_id)
    workspace_id: Mapped[str] = mapped_column(ForeignKey("workspaces.id"), index=True)
    repo_id: Mapped[str | None] = mapped_column(ForeignKey("repositories.id"), nullable=True, index=True)
    run_id: Mapped[str] = mapped_column(ForeignKey("runs.id"), index=True)
    kind: Mapped[str] = mapped_column(String(40))
    text: Mapped[str] = mapped_column(Text)
    topic: Mapped[str] = mapped_column(String(200), default="")
    confidence: Mapped[float | None] = mapped_column(Float, nullable=True)
    source: Mapped[str] = mapped_column(Text, default="")
    created_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow, onupdate=utcnow)


class ScorerResult(Base):
    """A judge's verdict; usage includes every attempt, including interrupted calls."""

    __tablename__ = "scorer_results"
    id: Mapped[str] = mapped_column(String(32), primary_key=True, default=new_id)
    workspace_id: Mapped[str] = mapped_column(ForeignKey("workspaces.id"), index=True)
    run_id: Mapped[str] = mapped_column(ForeignKey("runs.id"), index=True)
    scorer_id: Mapped[str] = mapped_column(String(80))
    definition_version: Mapped[str] = mapped_column(String(64))
    definition: Mapped[dict[str, Any]] = mapped_column(JSON)
    automatic_key: Mapped[str | None] = mapped_column(String(180), nullable=True, unique=True)
    status: Mapped[str] = mapped_column(String(20), default="queued", index=True)
    classification: Mapped[str | None] = mapped_column(String(80), nullable=True)
    score: Mapped[float | None] = mapped_column(Float, nullable=True)
    passed: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    reasoning: Mapped[str] = mapped_column(Text, default="")
    error: Mapped[str] = mapped_column(Text, default="")
    input_tokens: Mapped[int] = mapped_column(Integer, default=0)
    output_tokens: Mapped[int] = mapped_column(Integer, default=0)
    cost_usd: Mapped[float] = mapped_column(Float, default=0)
    cost_known: Mapped[bool] = mapped_column(Boolean, default=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow)
    started_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)


class ScorerAttempt(Base):
    """Separate provider sessions retain cost provenance across retries and credential changes."""

    __tablename__ = "scorer_attempts"
    id: Mapped[str] = mapped_column(String(32), primary_key=True, default=new_id)
    result_id: Mapped[str] = mapped_column(ForeignKey("scorer_results.id"), index=True)
    workspace_id: Mapped[str] = mapped_column(ForeignKey("workspaces.id"), index=True)
    provider: Mapped[str] = mapped_column(String(20), default="")
    model: Mapped[str] = mapped_column(String(100), default="")
    # Nullable for additive migration: old attempts do not prove usage was returned.
    usage_known: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    cost_reconciliation: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)
    cost_retry_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True, index=True)
    cost_retry_count: Mapped[int | None] = mapped_column(Integer, nullable=True)
    input_tokens: Mapped[int] = mapped_column(Integer, default=0)
    output_tokens: Mapped[int] = mapped_column(Integer, default=0)
    cost_usd: Mapped[float] = mapped_column(Float, default=0)
    cost_known: Mapped[bool] = mapped_column(Boolean, default=True)
    started_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)


class MergeRegret(Base):
    """One immutable source fact; feedback and fix links are canonical here."""

    __tablename__ = "merge_regrets"
    id: Mapped[str] = mapped_column(String(32), primary_key=True, default=new_id)
    workspace_id: Mapped[str] = mapped_column(ForeignKey("workspaces.id"), index=True)
    task_id: Mapped[str] = mapped_column(ForeignKey("tasks.id"), index=True)
    repo_id: Mapped[str] = mapped_column(ForeignKey("repositories.id"), index=True)
    run_id: Mapped[str | None] = mapped_column(ForeignKey("runs.id"), nullable=True)
    kind: Mapped[str] = mapped_column(String(30))
    source_key: Mapped[str] = mapped_column(String(500))
    occurred_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    detected_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow)
    merge_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    reporter: Mapped[str] = mapped_column(String(200))
    url: Mapped[str] = mapped_column(String(2000), default="")
    source_signal_id: Mapped[str | None] = mapped_column(ForeignKey("signals.id"), nullable=True)
    signal_id: Mapped[str | None] = mapped_column(ForeignKey("signals.id"), nullable=True)
    fix_outcome_id: Mapped[str | None] = mapped_column(ForeignKey("outcomes.id"), nullable=True)
    fix_url: Mapped[str] = mapped_column(String(2000), default="")
    evidence: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)

    __table_args__ = (
        UniqueConstraint("workspace_id", "task_id", "kind", "source_key", name="uq_regret_source"),
        Index("ix_regrets_scope_time", "workspace_id", "repo_id", "occurred_at"),
    )


class GateVerdict(Base):
    """Session-level evidence. Metrics must collapse sessions by task and gate."""

    __tablename__ = "gate_verdicts"
    id: Mapped[str] = mapped_column(String(32), primary_key=True, default=new_id)
    workspace_id: Mapped[str] = mapped_column(ForeignKey("workspaces.id"), index=True)
    task_id: Mapped[str] = mapped_column(ForeignKey("tasks.id"), index=True)
    gate: Mapped[str] = mapped_column(String(100))
    attempt: Mapped[int] = mapped_column(Integer)
    candidate_key: Mapped[str] = mapped_column(String(64))
    candidate_fingerprint: Mapped[str] = mapped_column(String(200), default="")
    head_sha: Mapped[str] = mapped_column(String(64), default="")
    session_key: Mapped[str] = mapped_column(String(200), default="")
    run_id: Mapped[str | None] = mapped_column(ForeignKey("runs.id"), nullable=True)
    verdict: Mapped[str] = mapped_column(String(10))
    at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    evidence: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)
    score: Mapped[float | None] = mapped_column(Float, nullable=True)

    __table_args__ = (
        UniqueConstraint("workspace_id", "task_id", "gate", "attempt", "candidate_key", "session_key",
                         name="uq_gate_candidate_session"),
        Index("ix_gate_scope", "workspace_id", "gate", "verdict"),
    )


class MindAction(Base):
    """Private confirmation ledger; target and kind are immutable after proposal.

    A card for workspace-level work (a new outcome) has no outcome until it is confirmed (db.py relaxes older
    tables)."""
    __tablename__ = "mind_actions"
    id: Mapped[str] = mapped_column(String(32), primary_key=True, default=new_id)
    thread_id: Mapped[str] = mapped_column(String(32), index=True)
    owner_id: Mapped[str] = mapped_column(ForeignKey("users.id"), index=True)
    workspace_id: Mapped[str] = mapped_column(ForeignKey("workspaces.id"))
    outcome_id: Mapped[str | None] = mapped_column(ForeignKey("outcomes.id"), nullable=True)
    task_id: Mapped[str | None] = mapped_column(String(32), nullable=True)
    decision_id: Mapped[str | None] = mapped_column(String(32), nullable=True)
    kind: Mapped[str] = mapped_column(String(30))
    revision: Mapped[int] = mapped_column(Integer, default=1)
    status: Mapped[str] = mapped_column(String(20), default="pending")
    fields: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)
    snapshot: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)
    # A digest of the target's state at proposal, which a confirmation must still match. It lives outside the
    # scrubbed JSON: redaction (which can span neighbouring values) must never alter it. Null on older cards.
    digest: Mapped[str | None] = mapped_column(String(64), nullable=True)
    result: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow)


class MindActionResult(Base):
    """Structured private thread continuation, never a shared object message."""
    __tablename__ = "mind_action_results"
    id: Mapped[str] = mapped_column(String(32), primary_key=True, default=new_id)
    thread_id: Mapped[str] = mapped_column(String(32), index=True)
    owner_id: Mapped[str] = mapped_column(ForeignKey("users.id"))
    action_id: Mapped[str] = mapped_column(ForeignKey("mind_actions.id"), unique=True)
    content: Mapped[dict[str, Any]] = mapped_column(JSON)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow)


class DefinitionState(Base):
    """Workspace-local compare-and-swap head; revisions live in a separate table."""

    __tablename__ = "definition_states"
    workspace_id: Mapped[str] = mapped_column(ForeignKey("workspaces.id"), primary_key=True)
    revision: Mapped[int] = mapped_column(Integer, default=0)


class DefinitionRevision(Base):
    """Complete, attributed immutable bundle (including snapshotted team notes)."""

    __tablename__ = "definition_revisions"
    id: Mapped[str] = mapped_column(String(32), primary_key=True, default=new_id)
    workspace_id: Mapped[str] = mapped_column(ForeignKey("workspaces.id"), index=True)
    revision: Mapped[int] = mapped_column(Integer)
    bundle: Mapped[dict[str, Any]] = mapped_column(JSON)
    author: Mapped[str] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow)
    restored_from: Mapped[int | None] = mapped_column(Integer, nullable=True)
    __table_args__ = (UniqueConstraint("workspace_id", "revision"),)


@event.listens_for(DefinitionRevision, "before_update")
@event.listens_for(DefinitionRevision, "before_delete")
def _immutable_definition_revision(_mapper, _connection, revision: DefinitionRevision) -> None:
    raise ValueError("Definition revisions are immutable; save or restore a new revision")


class MindThread(Base):
    """Person-owned conversation; never part of the workspace activity journal."""
    __tablename__ = "mind_threads"
    id: Mapped[str] = mapped_column(String(32), primary_key=True, default=new_id)
    owner_id: Mapped[str] = mapped_column(ForeignKey("users.id"), index=True)
    title: Mapped[str] = mapped_column(String(200), default="New conversation")
    billing_workspace_id: Mapped[str] = mapped_column(ForeignKey("workspaces.id"))
    workspace_ids: Mapped[list[str]] = mapped_column(JSON)
    model_id: Mapped[str | None] = mapped_column(String(200), nullable=True)
    status: Mapped[str] = mapped_column(String(20), default="idle")
    active_turn: Mapped[str | None] = mapped_column(String(32), nullable=True)
    lease_until: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow)


class MindMessage(Base):
    __tablename__ = "mind_messages"
    id: Mapped[str] = mapped_column(String(32), primary_key=True, default=new_id)
    thread_id: Mapped[str] = mapped_column(ForeignKey("mind_threads.id"), index=True)
    role: Mapped[str] = mapped_column(String(12))
    text: Mapped[str] = mapped_column(Text, default="")
    context: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    citations: Mapped[list] = mapped_column(JSON, default=list)
    action_cards: Mapped[list] = mapped_column(JSON, default=list)
    history: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    status: Mapped[str] = mapped_column(String(20), default="completed")
    client_message_id: Mapped[str | None] = mapped_column(String(128), nullable=True)
    turn_id: Mapped[str] = mapped_column(String(32), index=True)
    position: Mapped[int] = mapped_column(Integer)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow)
    __table_args__ = (UniqueConstraint("thread_id", "client_message_id"),
                      UniqueConstraint("thread_id", "position"))


class MindAccount(Base):
    """Database CAS admission, shared by all of a person's threads and processes."""
    __tablename__ = "mind_accounts"
    user_id: Mapped[str] = mapped_column(ForeignKey("users.id"), primary_key=True)
    claim: Mapped[str | None] = mapped_column(String(32), nullable=True)
    lease_until: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)


class MindCharge(Base):
    """One admitted request, not one cumulative provider session total.

    A request is assigned to its UTC admission day, including late reconciliation.
    Session totals are allocated as deltas to the latest pending request.
    """
    __tablename__ = "mind_charges"
    id: Mapped[str] = mapped_column(String(32), primary_key=True, default=new_id)
    user_id: Mapped[str] = mapped_column(ForeignKey("users.id"), index=True)
    thread_id: Mapped[str | None] = mapped_column(ForeignKey("mind_threads.id"), nullable=True, index=True)
    billing_workspace_id: Mapped[str] = mapped_column(ForeignKey("workspaces.id"))
    session_id: Mapped[str] = mapped_column(String(100), index=True)
    usd: Mapped[float | None] = mapped_column(Float, nullable=True)
    settled: Mapped[bool] = mapped_column(Boolean, default=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow, index=True)
