"""Structured, payload-free operational measurements in the existing journal.

Lifecycle IDs live in task evidence and commit with the state they describe. ORM
listeners cover decisions, planning, promotion and reconciliation; the atomic
lifecycle UPDATE calls the same helper explicitly. No scheduling policy lives here.
"""
from __future__ import annotations

import time

from sqlalchemy import event, inspect
from sqlalchemy.orm import Session

from .models import Event, Outcome, Run, Task, new_id

METRIC_ONLY = frozenset({
    "task_queued", "task_started", "stage_transition", "attempt_started", "attempt_finished",
    "task_terminal", "outcome_lifecycle", "run_interrupted", "attempt_interrupted",
    "provider_attempt", "execution_error", "cost_adjusted", "cost_reconciliation_error",
    "smoke_result", "ci_result", "review_revision",
})
TERMINAL = {"completed", "reverted", "cancelled", "abandoned", "handed_over", "failed"}


def emit(session, entity, kind, data):
    # Called during before_flush too: never recursively flush or publish uncommitted data.
    session.add(Event(workspace_id=entity.workspace_id, outcome_id=(entity.id if isinstance(entity, Outcome)
                      else getattr(entity, "outcome_id", None)), task_id=entity.id if isinstance(entity, Task) else
                      getattr(entity, "task_id", None), run_id=entity.id if isinstance(entity, Run) else None,
                      kind=kind, message="", actor="factory", level="info", data=data))


def task_change(session, task, fields, *, previous=None):
    """Return fields with authoritative correlation state, and enqueue journal rows.

    Evidence supplied by a detached runner must never replace the database's newer
    telemetry IDs. All callers use the loaded database copy as the authority.
    """
    previous = previous or {k: getattr(task, k) for k in ("status", "stage", "attempt")}
    old = dict((task.evidence or {}).get("telemetry") or {})
    flow = dict(old)
    status = fields.get("status", task.status)
    stage = fields.get("stage", task.stage) or ""
    attempt = fields.get("attempt", task.attempt) or 0
    evidence = dict(fields.get("evidence", task.evidence) or {})
    epoch = evidence.get("budget_epoch", 0)
    events = []
    if status == "queued" and (previous["status"] != "queued" or not flow.get("queue_id")):
        flow["queue_id"] = new_id()
        events.append(("task_queued", {"previous_status": previous["status"]}))
    if status == "running" and previous["status"] == "queued":
        events.append(("task_started", {}))
    if attempt > 0 and (flow.get("attempt") != attempt or flow.get("budget_epoch") != epoch
                        or (status == "running" and flow.get("interrupted"))):
        if flow.get("attempt_id"):
            events.append(("attempt_finished", {"attempt_id": flow["attempt_id"], "result": "superseded"}))
        flow.update(attempt_id=new_id(), attempt=attempt, budget_epoch=epoch, interrupted=False)
        events.append(("attempt_started", {}))
    if stage != previous["stage"] or flow.get("attempt_id") != old.get("attempt_id"):
        before = flow.get("stage_id")
        flow["stage_id"] = new_id()
        events.append(("stage_transition", {"previous_stage": previous["stage"], "previous_stage_id": before}))
        if stage == "revise" and stage != previous["stage"]:
            events.append(("review_revision", {}))
    if status in TERMINAL and status != previous["status"]:
        events.append(("task_terminal", {}))
        if flow.get("attempt_id"):
            events.append(("attempt_finished", {"result": status}))
    evidence["telemetry"] = flow
    fields = {**fields, "evidence": evidence}
    for kind, extra in events:
        emit(session, task, kind, {**flow, "status": status, "stage": stage, **extra})
    return fields


@event.listens_for(Session, "before_flush")
def lifecycle(session, _context, _instances):
    for obj in list(session.new) + list(session.dirty):
        state = inspect(obj)
        if isinstance(obj, Task):
            keys = ("status", "stage", "attempt", "evidence")
            if obj not in session.new and not any(state.attrs[k].history.has_changes() for k in keys):
                continue
            previous = {}
            for k in ("status", "stage", "attempt"):
                h = state.attrs[k].history
                previous[k] = h.deleted[0] if h.deleted else (None if obj in session.new else getattr(obj, k))
            # Primary keys/defaults have not been assigned on INSERT yet.
            obj.id = obj.id or new_id()
            obj.evidence = task_change(session, obj, {}, previous=previous)["evidence"]
        elif isinstance(obj, Outcome):
            h = state.attrs.status.history
            if obj in session.new or h.has_changes():
                obj.id = obj.id or new_id()
                emit(session, obj, "outcome_lifecycle", {"status": obj.status or "planning",
                     "previous_status": h.deleted[0] if h.deleted else None})


def error_cause(exc):
    # Classify exception types, never serialize error strings/provider response text.
    from .inference import BudgetExceeded, InferenceError
    from .sandbox import SandboxError
    from .gitops import GitError
    from .github import GitHubError
    import httpx

    # Harnesses wrap native/SDK failures with `raise ... from exc`. Preserve the
    # typed underlying cause without consulting potentially sensitive error text.
    # Bound traversal even for malformed or cyclic exception chains.
    for _ in range(8):
        if isinstance(exc, BudgetExceeded):
            return "budget"
        if isinstance(exc, (InferenceError, httpx.HTTPError)):
            return "provider"
        if isinstance(exc, SandboxError):
            return "sandbox_vm"
        if isinstance(exc, GitError):
            return "git"
        if isinstance(exc, GitHubError):
            return "github_api"
        exc = exc.__cause__
        if exc is None:
            break
    return "unknown"


def http_category(status):
    if status == 200:
        return None
    return {401: "authentication", 403: "permission", 402: "budget", 429: "rate_limit"}.get(
        status, "server" if status >= 500 else "request")


class ProviderAttempt:
    """One actual upstream send, excluding admission and retry sleeps. Finish once."""
    def __init__(self, run_id, provider, model, source, workspace_id=None):
        self.run_id, self.provider, self.model = run_id, provider, model
        self.source, self.workspace_id = source, workspace_id
        self.started = time.monotonic()
        self.done = False

    def finish(self, category=None, status=None):
        if self.done:
            return
        self.done = True
        elapsed = max(0.0, time.monotonic() - self.started)
        from .db import session_scope
        from .events import record
        from .models import ScorerAttempt, ScorerResult

        with session_scope() as s:
            run = s.get(Run, self.run_id) if self.run_id else None
            scorer = s.get(ScorerAttempt, self.run_id) if self.run_id and not run else None
            if scorer:
                result = s.get(ScorerResult, scorer.result_id)
                run = s.get(Run, result.run_id) if result else None
            workspace_id = run.workspace_id if run else (scorer.workspace_id if scorer else self.workspace_id)
            if not workspace_id:
                return  # Unattributed catalog/probe clients aren't run traffic.
            record(workspace_id, "provider_attempt", run_id=run.id if run else None,
                   outcome_id=run.outcome_id if run else None, task_id=run.task_id if run else None,
                   data={"provider": self.provider, "model": self.model, "source": self.source,
                         "request_id": new_id(), "session_id": self.run_id,
                         "scorer_attempt_id": scorer.id if scorer else None, "status": status,
                         "success": category is None, "error_category": category,
                         "cancelled": category == "cancelled", "provider_failure": category not in (None, "cancelled"),
                         "elapsed_seconds": elapsed}, session=s, publish=False)


def cost_adjusted(session, row, old_cost, old_known, source):
    from .models import ScorerAttempt, ScorerResult

    entity = row
    if isinstance(row, ScorerAttempt):
        result = session.get(ScorerResult, row.result_id)
        parent = session.get(Run, result.run_id) if result else None
        if parent:
            entity = parent
    emit(session, entity, "cost_adjusted", {"entity_id": row.id, "entity_type": row.__tablename__,
         "provider": row.provider, "model": row.model, "old_cost_usd": old_cost,
         "cost_usd": row.cost_usd, "old_cost_known": old_known, "cost_known": row.cost_known,
         "source": source})
