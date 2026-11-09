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
    Boolean,
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

    __table_args__ = (UniqueConstraint("workspace_id", "full_name"),)


class Outcome(Base):
    """What a team asked for. Decomposed by a plan into tasks."""

    __tablename__ = "outcomes"
    id: Mapped[str] = mapped_column(String(32), primary_key=True, default=new_id)
    workspace_id: Mapped[str] = mapped_column(ForeignKey("workspaces.id"), index=True)
    number: Mapped[int] = mapped_column(Integer)
    title: Mapped[str] = mapped_column(String(300))
    brief: Mapped[str] = mapped_column(Text, default="")
    acceptance: Mapped[list[str]] = mapped_column(JSON, default=list)
    repo_ids: Mapped[list[str]] = mapped_column(JSON, default=list)
    harness: Mapped[str | None] = mapped_column(String(20), nullable=True)
    status: Mapped[str] = mapped_column(String(30), default="planning", index=True)
    priority: Mapped[int] = mapped_column(Integer, default=2)  # 0 urgent .. 3 low
    source: Mapped[str] = mapped_column(String(40), default="user")  # user|signal|improvement
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
    role: Mapped[str] = mapped_column(String(20))  # planner|builder|verifier
    harness: Mapped[str] = mapped_column(String(20))
    provider: Mapped[str] = mapped_column(String(20), default="")
    model: Mapped[str] = mapped_column(String(100), default="")
    attempt: Mapped[int] = mapped_column(Integer, default=1)
    status: Mapped[str] = mapped_column(String(20), default="running")  # running|succeeded|failed|cancelled|error
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
    kind: Mapped[str] = mapped_column(String(30))  # diff|log|report|check|ci|smoke|plan|transcript
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
    reusable: Mapped[bool] = mapped_column(Boolean, default=False)
    scope: Mapped[str] = mapped_column(String(30), default="item")  # item|repository|workspace
    created_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow)


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
