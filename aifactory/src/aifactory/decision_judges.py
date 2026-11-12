"""Control-plane-only judge input and fail-closed policy. No model calls or tools.

Metadata contract: origin.role is planner/builder/verifier; safety explicitly records
approval, budget, takeover, protected_paths booleans and the paths being discussed.
Missing/unknown safety metadata is not evidence of eligibility. Reports are untrusted.
"""
from __future__ import annotations

import hashlib
import json
import posixpath
from typing import Any

from sqlalchemy import or_
from sqlalchemy.orm import Session

from .gitops import DEFAULT_PROTECTED, protected_violations
from .models import Decision, DecisionReview, Message, Outcome, Repository, Task, User
from .serializers import ts

SAFETY_FLAGS = ("approval", "budget", "takeover", "protected_paths")
FORBIDDEN_OPTIONS = {"approve", "approve_plan", "approve_merge", "approve_release",
                     "take_over", "takeover", "handover", "increase_budget", "extend_budget"}


def question_fingerprint(decision: Decision) -> str:
    data = {key: getattr(decision, key) for key in
            ("workspace_id", "outcome_id", "task_id", "kind", "title", "body", "options", "metadata_json")}
    return hashlib.sha256(json.dumps(data, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def eligible(decision: Decision, repositories: list[Repository] | None = None) -> bool:
    """Only ordinary agent questions with explicit, safe provenance may be reviewed.

    The sole mixed-governance exception is the planner's answer-and-replan question;
    allowed_resolutions still removes approve, so reviewing it cannot approve a plan.
    """
    if decision.kind not in ("question", "unblock"):
        return False
    meta = decision.metadata_json
    if not isinstance(meta, dict) or meta.get("people_only"):
        return False
    origin, safety = meta.get("origin"), meta.get("safety")
    if not isinstance(origin, dict) or origin.get("role") not in ("planner", "builder", "verifier"):
        return False
    if not isinstance(safety, dict) or any(safety.get(flag) is not False for flag in SAFETY_FLAGS):
        return False
    paths = safety.get("paths")
    if not isinstance(paths, list) or any(not isinstance(p, str) or not p.strip() for p in paths):
        return False
    patterns = list(DEFAULT_PROTECTED)
    for repo in repositories or []:
        if repo.workspace_id != decision.workspace_id:
            return False
        configured = (repo.config or {}).get("protected_paths", [])
        if not isinstance(configured, list) or any(not isinstance(p, str) for p in configured):
            return False
        patterns.extend(configured)
    normalized = [posixpath.normpath(p.replace("\\", "/")) for p in paths]
    if any(p.startswith(("/", "../")) or p == ".." for p in normalized):
        return False
    patterns += [p.removeprefix("**/") for p in patterns if p.startswith("**/")]
    if protected_violations(normalized, patterns):
        return False
    options = decision.options
    if not isinstance(options, list) or not options or any(
            not isinstance(o, dict) or not isinstance(o.get("value"), str) for o in options):
        return False
    values = {o["value"].strip().lower() for o in options}
    mixed_planner = (decision.kind == "question" and origin["role"] == "planner"
                     and decision.task_id is None and "answer" in values)
    # Unknown action values may encode governance too: only known question actions.
    safe_options = {"answer", "replan", "retry", "cancel", "abandon", "dismiss", "acknowledge", "fix", "revert"}
    if mixed_planner:
        safe_options.add("approve")
    action = "answer" if decision.kind == "question" else "retry"
    return action in values and values <= safe_options


def allowed_resolutions(decision: Decision) -> set[str]:
    return {o["value"] for o in decision.options or []
            if isinstance(o, dict) and isinstance(o.get("value"), str)
            and o["value"] in ({"answer"} if decision.kind == "question" else {"retry"})}


def build_context(db: Session, decision: Decision) -> dict[str, Any]:
    """All recorded answers, not just reusable precedents. No count or text limit.

    Associations are resolved from workspace-filtered maps, never unscoped get/join.
    User attribution is anchored to workspace decisions/messages (users are global).
    """
    ws = decision.workspace_id
    outcomes = {o.id: o for o in db.query(Outcome).filter(Outcome.workspace_id == ws)}
    tasks = {t.id: t for t in db.query(Task).filter(Task.workspace_id == ws)}
    repos = {r.id: r for r in db.query(Repository).filter(Repository.workspace_id == ws)}
    rows = db.query(Decision).filter(Decision.workspace_id == ws, Decision.status.in_(("resolved", "superseded")),
        or_(Decision.resolution != "", Decision.comment != "")).order_by(Decision.created_at, Decision.id).all()
    messages = db.query(Message).filter(Message.workspace_id == ws, Message.author_kind == "user").order_by(
        Message.created_at, Message.id).all()
    author_ids = {d.resolved_by for d in rows} | {m.author_id for m in messages}
    authors = {u.id: {"id": u.id, "name": u.name, "email": u.email}
               for u in db.query(User).filter(User.id.in_([i for i in author_ids if i]))}
    corrections = {r.decision_id: r.correction_decision_id for r in db.query(DecisionReview).filter(
        DecisionReview.workspace_id == ws, DecisionReview.correction_decision_id.is_not(None))}

    def associations(d: Decision) -> tuple[Outcome | None, Task | None, list[Repository]]:
        task = tasks.get(d.task_id)
        outcome = outcomes.get(d.outcome_id) or (outcomes.get(task.outcome_id) if task else None)
        ids = [task.repo_id] if task else (outcome.repo_ids if outcome else [])
        return outcome, task, [repos[i] for i in ids if i in repos]

    records = []
    for d in rows:
        outcome, task, associated = associations(d)
        records.append({"id": d.id, "workspace_id": ws, "kind": d.kind, "question": d.title,
            "body": d.body, "options": d.options, "resolution": d.resolution, "comment": d.comment,
            "author": authors.get(d.resolved_by, {"id": d.resolved_by, "name": "", "email": None}),
            "created_at": ts(d.created_at), "resolved_at": ts(d.resolved_at), "scope": d.scope,
            "outcome": {"id": outcome.id, "number": outcome.number, "title": outcome.title} if outcome else None,
            "task_id": task.id if task else None,
            "repositories": [{"id": r.id, "full_name": r.full_name} for r in associated],
            "superseded_by": corrections.get(d.id), "supersedes": (d.attribution or {}).get("supersedes"),
            "priority": "correction" if (d.attribution or {}).get("source") == "judge_correction" else "original"})
    records.sort(key=lambda r: (r["priority"] != "correction", bool(r["superseded_by"])))
    outcome, task, associated = associations(decision)
    guidance = [{"id": m.id, "body": m.body, "author": authors.get(m.author_id),
                 "created_at": ts(m.created_at), "outcome_id": m.outcome_id, "task_id": m.task_id}
                for m in messages if outcome and m.outcome_id == outcome.id
                and (m.task_id is None or (task and m.task_id == task.id))]
    return {"question": {"id": decision.id, "kind": decision.kind, "title": decision.title,
                         "body": decision.body, "options": decision.options},
            "record_policy": "Superseded records are history only; never follow or cite them. Prefer corrections over original answers.",
            "workspace_id": ws, "question_fingerprint": question_fingerprint(decision), "records": records,
            "outcome": {"id": outcome.id, "brief": outcome.brief, "acceptance": outcome.acceptance} if outcome else None,
            "task": {"id": task.id, "spec": task.spec, "acceptance": task.acceptance} if task else None,
            "repositories": [{"id": r.id, "full_name": r.full_name,
                              "team_notes": (r.config or {}).get("context", "")} for r in associated],
            "guidance": guidance}


def validate_report(db: Session, decision: Decision, report: Any,
                    context: dict[str, Any]) -> dict[str, Any]:
    """Validate untrusted output; callers must turn ValueError into hold, not resolve.

    Citations must match both the supplied snapshot and durable records. Later
    corrections invalidate stale snapshots. Exact passages may not cross fields.
    Call this in the same transaction that applies the verdict, without committing
    in between: the row lock keeps a human resolution from racing that write.
    """
    # Refresh the entire identity-mapped question under the lock: while the
    # model was running, a person may have closed it OR revised its question,
    # options or safety metadata. A status-only read leaves those fields stale.
    current = db.query(Decision).filter(
        Decision.id == decision.id, Decision.workspace_id == decision.workspace_id,
    ).populate_existing().with_for_update().one_or_none()
    if current is None or current.status not in ("open", "judging"):
        raise ValueError("Decision is no longer open")
    decision = current
    if not isinstance(report, dict) or report.get("verdict") not in ("decide", "send_back", "hold"):
        raise ValueError("Invalid judge verdict")
    if context.get("workspace_id") != decision.workspace_id or context.get("question", {}).get("id") != decision.id:
        raise ValueError("Context does not belong to this question")
    if context.get("question_fingerprint") != question_fingerprint(decision):
        raise ValueError("Question changed since review input was assembled")
    # Re-read applicable repositories rather than trusting model-supplied safety data.
    task = db.query(Task).filter(Task.workspace_id == decision.workspace_id, Task.id == decision.task_id).first()
    outcome = db.query(Outcome).filter(Outcome.workspace_id == decision.workspace_id,
                                      Outcome.id == (decision.outcome_id or (task.outcome_id if task else None))).first()
    repo_ids = [task.repo_id] if task else (outcome.repo_ids if outcome else [])
    repositories = db.query(Repository).filter(Repository.workspace_id == decision.workspace_id,
                                               Repository.id.in_(repo_ids)).all()
    if len({r.id for r in repositories}) != len(set(repo_ids)) or not eligible(decision, repositories):
        raise ValueError("Question requires human governance review")
    result = {"verdict": report["verdict"]}
    for key in ("answer", "resolution", "summary"):
        value = report.get(key, "")
        if not isinstance(value, str):
            raise ValueError(f"Invalid {key}")
        result[key] = value
    if not result["summary"].strip():
        raise ValueError("A summary is required")
    supplied = {r["id"]: r for r in context.get("records", [])}
    citations = report.get("citations", [])
    closest = report.get("closest_decisions", [])
    if not isinstance(citations, list) or not isinstance(closest, list) or any(
            not isinstance(i, str) or i not in supplied for i in closest):
        raise ValueError("Invalid record references")
    if report["verdict"] == "decide":
        if not citations or not result["answer"].strip() or result["resolution"] not in allowed_resolutions(decision):
            raise ValueError("Decide requires an answer, safe resolution and citations")
    if report["verdict"] == "send_back" and not result["answer"].strip():
        raise ValueError("Send back requires guidance")
    for citation in citations:
        if not isinstance(citation, dict) or set(citation) != {"decision_id", "passage"}:
            raise ValueError("Invalid citation form")
        ident, passage = citation["decision_id"], citation["passage"]
        if not isinstance(ident, str) or not isinstance(passage, str) or not passage.strip() or ident not in supplied:
            raise ValueError("Missing cited record or passage")
        source = db.query(Decision).filter(Decision.id == ident, Decision.workspace_id == decision.workspace_id).first()
        snapshot = supplied[ident]
        if (source is None or source.id == decision.id or source.status != "resolved"
                or source.resolution == "send_back" or (source.attribution or {}).get("verdict") == "send_back"
                or source.created_at >= decision.created_at or source.resolved_at is None
                or source.resolved_at >= decision.created_at):
            raise ValueError("Citation must be an earlier resolved workspace record")
        if (snapshot.get("workspace_id") != decision.workspace_id or snapshot.get("superseded_by")
                or db.query(DecisionReview).filter(DecisionReview.workspace_id == decision.workspace_id,
                    DecisionReview.decision_id == ident,
                    DecisionReview.correction_decision_id.is_not(None)).first()):
            raise ValueError("Citation has been corrected")
        if source.scope == "item":
            if (source.task_id != decision.task_id or source.outcome_id != decision.outcome_id
                    or not source.outcome_id):
                raise ValueError("Citation is outside this item's scope")
        elif source.scope == "repository":
            source_task = db.query(Task).filter_by(id=source.task_id, workspace_id=decision.workspace_id).first()
            source_outcome = db.query(Outcome).filter_by(id=source.outcome_id, workspace_id=decision.workspace_id).first()
            source_repos = {source_task.repo_id} if source_task else set(source_outcome.repo_ids or []) if source_outcome else set()
            if not repo_ids or not set(repo_ids) <= source_repos:
                raise ValueError("Citation is outside this repository's scope")
        elif source.scope != "workspace":
            raise ValueError("Unknown citation scope")
        if not any(passage in (getattr(source, field) or "") and passage in (snapshot.get(field) or "")
                   for field in ("resolution", "comment")):
            raise ValueError("Citation passage does not match the supplied answer")
    result["citations"], result["closest_decisions"] = citations, closest
    return result



def judge_metrics(db: Session, workspace_id: str, *, now=None) -> dict[str, Any]:
    """UTC inclusive [now-days, now] cohorts by finished_at (judged_at).

    Overruled counts corrections to automatic decide/send_back reviews in that
    cohort, regardless of correction date. Rate is a fraction, not a percentage.
    Suggest reviews count as held; off/bypass and unfinished reviews never count.
    """
    from datetime import timedelta, timezone
    from .db import utcnow

    def utc(value):
        return value.astimezone(timezone.utc).replace(tzinfo=None) if value.tzinfo else value

    now = utc(now or utcnow())
    reviews = db.query(DecisionReview).filter(DecisionReview.workspace_id == workspace_id,
        DecisionReview.finished_at >= now - timedelta(days=30), DecisionReview.finished_at <= now,
        DecisionReview.mode.in_(("decide", "suggest")),
        DecisionReview.state.notin_(("pending", "running", "bypass", "bypassed")),
        DecisionReview.effective_verdict.in_(("decide", "send_back", "hold"))).all()
    result = {}
    for days in (7, 30):
        counts = dict(decided=0, sent_back=0, held=0, overruled=0)
        for r in reviews:
            if not now - timedelta(days=days) <= utc(r.finished_at) <= now:
                continue
            verdict = "hold" if r.mode == "suggest" else r.effective_verdict
            counts[{"decide": "decided", "send_back": "sent_back", "hold": "held"}[verdict]] += 1
            if verdict in ("decide", "send_back") and r.correction_decision_id:
                counts["overruled"] += 1
        denominator = counts["decided"] + counts["sent_back"]
        result[f"days_{days}"] = {**counts, "overrule_rate": counts["overruled"] / denominator if denominator else 0.0}
    return result
