"""Durable semantic effects; issue leases span requests, never database transactions.

State is level-triggered from Factory records. Recent writes are reconciled at
30/60/120/... seconds, then every 15 minutes, in batches of at most 50 issues.
An expired lease recovers a crashed worker; supplied comment UUIDs survive it.
Completion comments wait up to one hour for final pricing; after that they
report reconciliation in progress instead of a provisional total.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
from datetime import timedelta
from uuid import uuid4

from pydantic import ValidationError
from sqlalchemy import and_, or_, update
from sqlalchemy.exc import IntegrityError

from . import linear_config
from .config import get_settings
from .db import session_scope, utcnow
from .events import record
from .linear import LinearError
from .models import Decision, Event, LinearOperation as Operation, LinearWarningOperation, Outcome, Plan, Run, Secret, Signal, Task, Workspace
from .security import SecretError, decrypt, redact


def _key(*parts: str) -> str:
    return hashlib.sha256(json.dumps(parts).encode()).hexdigest()


def _insert(db, **values):
    existing = db.query(Operation).filter_by(workspace_id=values["workspace_id"],
                                              semantic_key=values["semantic_key"]).one_or_none()
    if existing:
        return existing
    try:
        with db.begin_nested():
            operation = Operation(**values)
            db.add(operation)
            db.flush()
        return operation
    except IntegrityError:
        return db.query(Operation).filter_by(workspace_id=values["workspace_id"],
                                              semantic_key=values["semantic_key"]).one()


def _issue(db, workspace_id, outcome_id, issue_id):
    return _insert(db, workspace_id=workspace_id, outcome_id=outcome_id, issue_id=issue_id,
                   semantic_key=_key(issue_id, "issue"), kind="issue", status="sent")


def _body(db, workspace_id, body):
    secrets = []
    for secret in db.query(Secret).filter(Secret.workspace_id == workspace_id, Secret.kind != "feedback_key"):
        try:
            secrets.append(decrypt(secret.ciphertext))
        except SecretError:
            return "[redacted]"
    return redact(body, tuple(secrets))


def _comment(db, workspace_id, outcome_id, issue_id, kind, identity, body):
    _issue(db, workspace_id, outcome_id, issue_id)
    payload = {"body": _body(db, workspace_id, body)}
    if kind == "decision":
        payload["decision_id"] = identity
    if kind == "completion":
        payload["comment_kind"] = kind
    operation = _insert(db, workspace_id=workspace_id, outcome_id=outcome_id, issue_id=issue_id,
                   semantic_key=_key(issue_id, kind, identity), kind="comment",
                   payload=payload, remote_id=str(uuid4()))
    if kind == "completion" and operation.status == "superseded" and _completed(db, workspace_id, outcome_id):
        operation.status, operation.payload = "pending", payload
        operation.next_attempt, operation.attempts, operation.error = utcnow(), 0, ""
    return operation


def _completed(db, workspace_id, outcome_id):
    outcome = db.get(Outcome, outcome_id)
    return (outcome is not None and outcome.status == "completed"
            and not db.query(Task).filter_by(outcome_id=outcome_id).filter(
                Task.status != "completed").first()
            and not db.query(Decision).filter_by(workspace_id=workspace_id, outcome_id=outcome_id,
                                                status="open").first())


def _obsolete_completion(db, comment):
    if (comment.payload.get("comment_kind") != "completion"
            and comment.semantic_key != _key(comment.issue_id, "completion", comment.outcome_id)):
        return False
    if _completed(db, comment.workspace_id, comment.outcome_id):
        return False
    comment.status = "superseded"
    return True


def _obsolete_decision(db, comment):
    decisions = db.query(Decision).filter_by(workspace_id=comment.workspace_id, outcome_id=comment.outcome_id)
    identity = comment.payload.get("decision_id")
    if identity is not None:
        decision = decisions.filter_by(id=identity).one_or_none()
        obsolete = decision is None or decision.status != "open"
    else:
        obsolete = any(decision.status != "open" and comment.semantic_key ==
                       _key(comment.issue_id, "decision", decision.id) for decision in decisions)
    if obsolete:
        comment.status = "superseded"
    return obsolete


def enqueue_comment(workspace_id: str, outcome_id: str, issue_id: str, kind: str, identity: str, body: str) -> str:
    """Identity is a plan ID, task+PR URL, decision ID, or completed outcome ID."""
    if kind not in {"plan", "pr", "decision", "completion"}:
        raise ValueError("Unsupported Linear comment kind")
    with session_scope() as db:
        return _comment(db, workspace_id, outcome_id, issue_id, kind, identity, body).id


def _state(db, workspace_id, outcome_id, issue_id, target):
    target = _body(db, workspace_id, target)
    issue = _issue(db, workspace_id, outcome_id, issue_id)
    db.execute(update(Operation).where(Operation.id == issue.id).values(revision=Operation.revision))
    db.refresh(issue)
    if issue.payload.get("target") == target:
        return
    db.query(Operation).filter_by(workspace_id=workspace_id, issue_id=issue_id, kind="state", status="pending").update(
        {"status": "superseded"}, synchronize_session=False)
    issue.revision += 1
    issue.payload = {"target": target}
    if issue.error not in {"rate_limit", "timeout"}:
        issue.next_attempt, issue.attempts = utcnow(), 0
    _insert(db, workspace_id=workspace_id, outcome_id=outcome_id, issue_id=issue_id,
            semantic_key=_key(issue_id, "state", str(issue.revision)), kind="state",
            revision=issue.revision, payload={"target": target})


def enqueue_state(workspace_id: str, outcome_id: str, issue_id: str, target: str) -> None:
    with session_scope() as db:
        _state(db, workspace_id, outcome_id, issue_id, target)


def _desired(db, issue):
    workspace = db.get(Workspace, issue.workspace_id)
    config = linear_config.saved_config(workspace)
    if not config.enabled:
        return None
    outcome = db.get(Outcome, issue.outcome_id)
    signal = db.query(Signal).filter_by(workspace_id=issue.workspace_id, outcome_id=outcome.id,
                                      source="linear_issue", external_id=issue.issue_id).one_or_none()
    if not signal or signal.source != "linear_issue" or signal.external_id != issue.issue_id:
        return issue.payload.get("target")
    tasks = db.query(Task).filter_by(outcome_id=outcome.id).all()
    statuses = {task.status for task in tasks}
    stages = {task.stage for task in tasks if task.status in {"running", "queued", "waiting_ci"}}
    if outcome.status in {"attention", "blocked", "failed", "cancelled", "partially_completed",
                          "reverted", "handed_over", "abandoned", "needs_input", "awaiting_approval"} or statuses & {
            "failed", "blocked", "attention", "needs_input", "handed_over", "awaiting_approval", "smoke_failed",
            "cancelled", "abandoned", "reverted", "partially_completed"} or db.query(Decision).filter_by(
                workspace_id=issue.workspace_id, outcome_id=outcome.id, status="open").first():
        stage = "blocked"
    elif outcome.status == "completed" and _completed(db, issue.workspace_id, outcome.id):
        stage = "done"
    elif outcome.status == "planning" or not stages:
        stage = "queued"
    elif stages - {"verify", "publish", "ci", "merge", "smoke", "release", "done"}:
        stage = "building"
    elif stages & {"verify", "publish"}:
        stage = "verifying"
    else:
        stage = "merging"
    return getattr(config.states, stage)


def _capture(db, workspace):
    if not linear_config.saved_config(workspace).enabled:
        return
    links = db.query(Signal, Outcome).join(Outcome, Outcome.id == Signal.outcome_id).filter(
        Signal.workspace_id == workspace.id, Signal.source == "linear_issue").all()
    for signal, outcome in links:
        issue = _issue(db, workspace.id, outcome.id, signal.external_id)
        _state(db, workspace.id, outcome.id, signal.external_id, _desired(db, issue))
        base = f"{get_settings().public_url}/w/{workspace.id}"
        comments = [("plan", plan.id, f"Plan ready: {base}/outcomes/{outcome.id}")
                    for plan in db.query(Plan).filter_by(outcome_id=outcome.id)]
        comments += [("pr", task.id + ":" + task.pr_url, f"Pull request opened: {task.pr_url}")
                     for task in db.query(Task).filter_by(outcome_id=outcome.id) if task.pr_url]
        for event in db.query(Event).filter_by(workspace_id=workspace.id, outcome_id=outcome.id):
            data = event.data or {}
            if event.kind in {"published", "pr_published", "handover_published"} and event.task_id:
                url = data.get("url") or data.get("pr_url")
                if url:
                    comments.append(("pr", event.task_id + ":" + url, f"Pull request opened: {url}"))
        comments += [("decision", decision.id, f"A person is needed: {base}/outcomes/{outcome.id}#decision-{decision.id}")
                     for decision in db.query(Decision).filter_by(outcome_id=outcome.id, status="open")]
        if _completed(db, workspace.id, outcome.id):
            runs = db.query(Run).filter_by(outcome_id=outcome.id).all()
            body = None
            if all(run.cost_known for run in runs):
                body = f"Outcome delivered. Total cost: ${sum(run.cost_usd for run in runs):.2f}"
            else:
                wait = _insert(db, workspace_id=workspace.id, outcome_id=outcome.id, issue_id=signal.external_id,
                               semantic_key=_key(signal.external_id, "completion_cost", outcome.id),
                               kind="cost_wait", status="sent")
                if utcnow() >= wait.created_at + timedelta(hours=1):
                    body = "Outcome delivered. Total cost: unknown (cost still being reconciled)."
            if body:
                comments.append(("completion", outcome.id, body))
        for kind, identity, body in comments:
            _comment(db, workspace.id, outcome.id, signal.external_id, kind, identity, body)


def _category(exc):
    if isinstance(exc, ValidationError):
        return "configuration"
    if isinstance(exc, LinearError) and exc.category in {
            "timeout", "network", "authentication", "rate_limit", "unavailable", "http", "graphql",
            "malformed", "mutation_failed", "not_configured"}:
        return exc.category
    return "internal"


def warning(db, workspace_id, category, *, kind="linear_outbox_warning"):
    workspace = db.get(Workspace, workspace_id)
    secret = db.query(Secret).filter_by(workspace_id=workspace_id, kind="linear_api_key").one_or_none()
    revision = _key(json.dumps((workspace.settings or {}).get("linear"), sort_keys=True),
                    secret.ciphertext if secret else "", str(secret.updated_at) if secret else "")
    key = _key(workspace_id, "warning", category, revision)
    try:
        with db.begin_nested():
            db.add(LinearWarningOperation(key=key, workspace_id=workspace_id))
            db.flush()
            record(workspace_id, kind, f"Linear sync pending ({category})", level="warn",
                   data={"category": category}, session=db)
    except IntegrityError:
        pass


def _failure(db, operation, exc):
    category = _category(exc)
    operation.error = category
    operation.attempts += 1
    operation.next_attempt = utcnow() + timedelta(seconds=max(60 if category == "rate_limit" else 30,
                                                            min(900, 30 * 2 ** min(operation.attempts - 1, 5))))
    warning(db, operation.workspace_id, category)
    return category


class Dispatcher:
    def __init__(self, provider=None):
        self.provider = provider or linear_config.provider

    async def _heartbeat(self, ids, claim):
        while True:
            await asyncio.sleep(30)
            with session_scope() as db:
                db.query(Operation).filter(Operation.id.in_(ids), Operation.claim == claim).update(
                    {"lease_until": utcnow() + timedelta(seconds=120)}, synchronize_session=False)

    def _ack(self, operation_id):
        with session_scope() as db:
            operation = db.get(Operation, operation_id)
            operation.status, operation.sent_at, operation.error = "sent", utcnow(), ""

    async def _deliver(self, issue, remote, provider, errors):
        try:
            with session_scope() as db:
                current = db.get(Operation, issue.id)
                target = _desired(db, current)
                revision = current.revision
            if target and issue.next_attempt <= utcnow():
                try:
                    if isinstance(remote, Exception):
                        raise remote
                    if remote["id"] != target:
                        await provider.update_issue_state(issue.issue_id, target)
                        with session_scope() as db:
                            record(issue.workspace_id, "linear_state_reconciled", "Reconciled Linear state",
                                   outcome_id=issue.outcome_id, session=db)
                    with session_scope() as db:
                        db.query(Operation).filter_by(workspace_id=issue.workspace_id, issue_id=issue.issue_id,
                            kind="state", status="pending").filter(Operation.revision <= revision).update(
                                {"status": "sent", "sent_at": utcnow()}, synchronize_session=False)
                        current = db.get(Operation, issue.id)
                        if current.revision == revision:
                            current.attempts += 1
                            current.error = ""
                            current.next_attempt = utcnow() + timedelta(seconds=min(900, 30 * 2 ** min(current.attempts - 1, 5)))
                except Exception as exc:
                    with session_scope() as db:
                        errors.append(_failure(db, db.get(Operation, issue.id), exc))
            with session_scope() as db:
                comments = db.query(Operation).filter_by(workspace_id=issue.workspace_id, issue_id=issue.issue_id,
                    kind="comment", status="pending").filter(Operation.next_attempt <= utcnow()).all()
            if comments and "rate_limit" not in errors:
                existing = {comment["id"] for comment in await provider.comments(issue.issue_id)}
                for comment in comments:
                    try:
                        with session_scope() as db:
                            current = db.get(Operation, comment.id)
                            if current.status != "pending" or _obsolete_completion(db, current) or _obsolete_decision(db, current):
                                continue
                        if comment.remote_id not in existing:
                            await provider.create_comment(issue.issue_id, comment.payload["body"], comment_id=comment.remote_id)
                        self._ack(comment.id)
                    except Exception as exc:
                        with session_scope() as db:
                            errors.append(_failure(db, db.get(Operation, comment.id), exc))
                        if errors[-1] == "rate_limit":
                            break
        except Exception as exc:
            with session_scope() as db:
                errors.append(_failure(db, db.get(Operation, issue.id), exc))
                db.query(Operation).filter_by(workspace_id=issue.workspace_id, issue_id=issue.issue_id,
                    status="pending").update({"next_attempt": utcnow() + timedelta(seconds=60)}, synchronize_session=False)

    async def dispatch(self, workspace_id: str | None = None) -> list[str]:
        errors = []
        with session_scope() as db:
            workspace_ids = ([workspace_id] if workspace_id is not None
                             else [workspace.id for workspace in db.query(Workspace)])
        for workspace_id in workspace_ids:
            claim, claimed = uuid4().hex, []
            workspace_errors = []
            try:
                with session_scope() as db:
                    workspace = db.get(Workspace, workspace_id)
                    if not linear_config.saved_config(workspace).enabled:
                        continue
                    _capture(db, workspace)
                    pending = db.query(Operation.issue_id).filter_by(workspace_id=workspace_id, kind="comment",
                        status="pending").filter(Operation.next_attempt <= utcnow())
                    candidates = db.query(Operation).filter_by(workspace_id=workspace_id, kind="issue").filter(
                        or_(and_(Operation.payload["target"].as_string().is_not(None),
                                 Operation.next_attempt <= utcnow()), Operation.issue_id.in_(pending)),
                        or_(Operation.error.notin_(["rate_limit", "timeout"]), Operation.next_attempt <= utcnow())).order_by(
                            Operation.next_attempt).limit(50).all()
                    for issue in candidates:
                        changed = db.query(Operation).filter(Operation.id == issue.id, Operation.lease_until <= utcnow()).update(
                            {"claim": claim, "lease_until": utcnow() + timedelta(seconds=120)}, synchronize_session=False)
                        if changed:
                            for comment in db.query(Operation).filter_by(workspace_id=workspace_id,
                                    issue_id=issue.issue_id, kind="comment", status="pending"):
                                _obsolete_completion(db, comment)
                            claimed.append(issue)
                if not claimed:
                    continue
                heartbeat = asyncio.create_task(self._heartbeat([issue.id for issue in claimed], claim))
                try:
                    provider = self.provider(workspace_id)
                    due = [issue.issue_id for issue in claimed if issue.payload.get("target") and issue.next_attempt <= utcnow()]
                    try:
                        states = await provider.issue_states(due) if due else {}
                    except Exception as exc:
                        states = dict.fromkeys(due, exc)
                    for issue in claimed:
                        await self._deliver(issue, states.get(issue.issue_id), provider, workspace_errors)
                        if "rate_limit" in workspace_errors:
                            with session_scope() as db:
                                db.query(Operation).filter_by(workspace_id=workspace_id).update(
                                    {"error": "rate_limit", "next_attempt": utcnow() + timedelta(seconds=60)},
                                    synchronize_session=False)
                            break
                finally:
                    heartbeat.cancel()
                    await asyncio.gather(heartbeat, return_exceptions=True)
            except asyncio.CancelledError:
                with session_scope() as db:
                    for issue in claimed:
                        db.query(Operation).filter_by(workspace_id=workspace_id, issue_id=issue.issue_id).filter(
                            or_(Operation.kind == "issue", Operation.status == "pending")).update(
                                {"error": "timeout", "next_attempt": utcnow() + timedelta(seconds=30)},
                                synchronize_session=False)
                raise
            except Exception as exc:
                if claimed:
                    with session_scope() as db:
                        for issue in claimed:
                            current = db.get(Operation, issue.id)
                            workspace_errors.append(_failure(db, current, exc))
                            db.query(Operation).filter_by(workspace_id=workspace_id, issue_id=issue.issue_id,
                                status="pending").update({"next_attempt": current.next_attempt}, synchronize_session=False)
                else:
                    category = _category(exc)
                    errors.append(category)
                    with session_scope() as db:
                        warning(db, workspace_id, category)
            finally:
                errors.extend(workspace_errors)
                with session_scope() as db:
                    db.query(Operation).filter_by(claim=claim).update(
                        {"claim": "", "lease_until": utcnow()}, synchronize_session=False)
        return errors
