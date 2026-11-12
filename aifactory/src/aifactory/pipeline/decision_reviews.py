"""Durable pre-Inbox review. Transactions never span inference."""
from __future__ import annotations

from sqlalchemy import text

from ..db import session_scope, utcnow
from ..decision_judges import eligible, question_fingerprint, validate_report
from ..models import Decision, DecisionReview, Outcome, Repository, Task, User, Workspace


def lock(db, workspace_id):
    # Reserve SQLite's writer before reading; Postgres serializes per workspace.
    if db.get_bind().dialect.name == "sqlite":
        db.execute(text("BEGIN IMMEDIATE"))
    else:
        db.query(Workspace).filter_by(id=workspace_id).with_for_update().one()


def active(db, decision):
    outcome = db.query(Outcome).filter_by(id=decision.outcome_id).populate_existing().with_for_update().first()
    task = db.query(Task).filter_by(id=decision.task_id).populate_existing().with_for_update().first() if decision.task_id else None
    return (outcome is not None and outcome.workspace_id == decision.workspace_id
            and outcome.status not in ("cancelled", "completed", "partially_completed")
            and (not decision.task_id or (task is not None and task.workspace_id == decision.workspace_id
                 and task.outcome_id == outcome.id and task.status not in
                 ("cancelled", "abandoned", "completed", "handed_over", "reverted"))))


def mode_for(db, workspace_id):
    from .runtime import merged_settings

    raw = db.get(Workspace, workspace_id).settings
    # Legacy workspaces may still carry a non-object settings value. Opening
    # an unrelated human-only decision must not fail at the new review boundary.
    return merged_settings(raw if isinstance(raw, dict) else {})["decision_judges"]


def prepare(db, decision):
    """Queue/update a slot in the caller's state-transition transaction, before publication."""
    mode = mode_for(db, decision.workspace_id)
    task = db.get(Task, decision.task_id) if decision.task_id else None
    outcome = db.get(Outcome, decision.outcome_id) if decision.outcome_id else None
    repo_ids = [task.repo_id] if task else (outcome.repo_ids if outcome else [])
    repos = db.query(Repository).filter(Repository.workspace_id == decision.workspace_id,
                                       Repository.id.in_(repo_ids)).all()
    review = db.query(DecisionReview).filter_by(decision_id=decision.id).first()
    fingerprint = question_fingerprint(decision)
    if mode in ("decide", "suggest") and active(db, decision) and len(repos) == len(set(repo_ids)) and eligible(decision, repos):
        if review and review.mode == mode and review.question_fingerprint == fingerprint:
            return  # Duplicate openings must not replay a completed hold or in-flight review.
        if review is None:
            review = DecisionReview(workspace_id=decision.workspace_id, decision_id=decision.id,
                                    question_revision=0)
            db.add(review)
        review.question_revision += 1
        review.question_fingerprint = fingerprint
        review.mode, review.state = mode, "pending"
        review.started_at = review.finished_at = review.run_id = None
        review.report, review.citations, review.closest_decisions = {}, [], []
        review.proposed_verdict = review.effective_verdict = None
        review.answer = review.resolution = review.summary = ""
        decision.status = "judging"
    else:
        decision.status = "open"
        if review:
            review.state, review.finished_at = "bypassed", utcnow()


def claim(review_id):
    with session_scope() as db:
        # workspace lookup is outside the write transaction on SQLite.
        workspace_id = db.query(DecisionReview.workspace_id).filter_by(id=review_id).scalar()
    if workspace_id is None:
        return None
    with session_scope() as db:
        lock(db, workspace_id)
        review = db.get(DecisionReview, review_id)
        decision = db.get(Decision, review.decision_id)
        if review.state != "pending":
            return None
        if decision.status != "judging":
            # Cancellation may have committed before its in-memory stop hook
            # ran (or the process died before it). Retire the durable slot too.
            review.state, review.finished_at = "superseded", utcnow()
            return None
        mode = mode_for(db, workspace_id)
        if mode != review.mode:
            prepare(db, decision)
            return None
        if not active(db, decision):
            decision.status, review.state, review.finished_at = "superseded", "superseded", utcnow()
            return None
        if question_fingerprint(decision) != review.question_fingerprint:
            prepare(db, decision)
            return None
        review.state, review.started_at = "running", utcnow()
        return (review.workspace_id, decision.id, review.question_revision,
                review.question_fingerprint, review.started_at)


def apply(review_id, claim_token, run, report):
    """Recheck the claim and the question, then commit guidance and safe resumption once."""
    from ..decision_judge_runner import safe_hold
    from ..decisions import resolve
    from ..work_services import publish_changes
    from .runtime import publish_decision

    ws, decision_id, revision, fingerprint, started = claim_token
    events = []
    held = False
    with session_scope() as db:
        lock(db, ws)
        review = db.get(DecisionReview, review_id)
        decision = db.get(Decision, decision_id)
        if (review.state != "running" or review.question_revision != revision
                or review.question_fingerprint != fingerprint or review.started_at != started
                or decision.status != "judging"):
            return
        if not active(db, decision):
            decision.status, review.state, review.finished_at = "superseded", "superseded", utcnow()
            return
        if question_fingerprint(decision) != fingerprint:
            prepare(db, decision)
            return
        if mode_for(db, ws) != review.mode:
            prepare(db, decision)
            return
        # Revalidate citations against the original supplied snapshot and today's durable record.
        try:
            context = (run.config_snapshot or {}).get("context")
            if not context:
                raise ValueError("Missing original judge input snapshot")
            report = validate_report(db, decision, report, context)
        except ValueError as exc:
            report = safe_hold(str(exc))
        proposed = report["verdict"]
        effective = "hold" if review.mode == "suggest" else proposed
        resolution = "answer" if decision.kind == "question" else "retry"
        if effective == "decide" and report["resolution"] != resolution:
            report = safe_hold("Judge may only answer or retry this question")
            proposed = effective = "hold"
        review.run_id, review.report = run.id, report
        review.proposed_verdict, review.effective_verdict = proposed, effective
        for key in ("answer", "resolution", "summary", "citations", "closest_decisions"):
            setattr(review, key, report.get(key, [] if key in ("citations", "closest_decisions") else ""))
        review.state, review.finished_at = "finished", utcnow()
        decision.status = "open"
        if effective == "hold":
            held = True
        else:
            user = db.query(User).filter_by(id="decision-judge").first()
            if user is None:
                user = User(id="decision-judge", name="Decision judge", is_system=True)
                from sqlalchemy.exc import IntegrityError

                try:
                    with db.begin_nested():
                        db.add(user)
                        db.flush()
                except IntegrityError:
                    user = db.get(User, "decision-judge")
            attribution = {"source": "decision_judge", "review_id": review.id,
                           "verdict": effective, "citations": review.citations}
            resolve(db, decision, user, resolution, report["answer"],
                    reusable=effective == "decide", pending_events=events, after=[], attribution=attribution)
        title, kind, outcome_id, task_id = decision.title, decision.kind, decision.outcome_id, decision.task_id
    publish_changes(events)
    if held:
        publish_decision(ws, decision_id, kind, title, outcome_id=outcome_id, task_id=task_id)


def recover(db):
    from ..models import Run

    for run in db.query(Run).filter(Run.role == "judge", Run.status == "running"):
        run.status, run.error, run.finished_at = "failed", "Interrupted by a restart", utcnow()
    for review in db.query(DecisionReview).filter(DecisionReview.state == "running"):
        review.state, review.started_at = "pending", None
