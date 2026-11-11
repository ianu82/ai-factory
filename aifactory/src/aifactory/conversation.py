"""An outcome as a conversation, read from what the Factory already records.

In time order:
- the request;
- the Lead's updates and notes, each carrying the agent steps since its previous post;
- people's messages, and the guidance the Lead sent to agents;
- decisions: open ones with their options and the Lead's recommendation, answered ones with who answered and how.
Steps after the Lead's last post form a trailing "working" item: what the agents are doing right now.
"""
from __future__ import annotations

from datetime import datetime
from typing import Any

from sqlalchemy.orm import Session

from . import serializers as S
from .models import Decision, Event, Message, Outcome, Run, Signal, Task, User

# Shown as their own items, or too fine-grained for the conversation (the full activity view keeps them).
HIDDEN = ("agent_thought", "agent_tool", "agent_command", "decision_requested", "decision_resolved", "guidance",
          "lead_note", "lead_update", "check_slot", "check_slot_wait")
ROLES = {"planner": "Planner", "plan_reviewer": "Plan reviewer", "builder": "Builder", "verifier": "Verifier",
         "judge": "Decision judge", "scorer": "Scorer", "lead": "Lead"}
SOURCES = {"user": "the app", "signal": "a signal", "linear_issue": "a Linear issue", "github_issue": "a GitHub issue",
           "ci_failure": "a CI failure", "improvement": "an improvement proposal", "mind": "a Mind conversation",
           "operator": "the operator", "feedback": "feedback"}
MAX_EVENTS = 3000
MAX_STEPS = 40


def conversation(db: Session, outcome: Outcome, viewer_id: str | None = None) -> dict[str, Any]:
    from .pipeline.lead import notes_for

    tasks = {t.id: t for t in db.query(Task).filter(Task.outcome_id == outcome.id)}
    keys = {task_id: t.key for task_id, t in tasks.items()}
    messages = db.query(Message).filter(Message.outcome_id == outcome.id).order_by(Message.created_at, Message.id).all()
    decisions = db.query(Decision).filter(Decision.outcome_id == outcome.id).order_by(Decision.created_at, Decision.id).all()
    events = db.query(Event).filter(Event.outcome_id == outcome.id, Event.kind.notin_(HIDDEN)) \
        .order_by(Event.id.desc()).limit(MAX_EVENTS).all()[::-1]
    people = {m.author_id for m in messages} | {d.resolved_by for d in decisions} | {outcome.created_by}
    users = {u.id: u for u in db.query(User).filter(User.id.in_({p for p in people if p}))}
    notes = notes_for(db, [d for d in decisions if d.status == "open"])

    mind = _started_in_mind(db, outcome, viewer_id)
    request = _request(db, outcome, users)
    if mind:
        request["from"] = "a Mind conversation"
    items: list[tuple[datetime, int, dict[str, Any]]] = [(outcome.created_at, 0, request)]
    for m in messages:
        items.append((m.created_at, 1, _message(m, users, keys)))
    for d in decisions:
        items.append((d.created_at, 1, _decision(d, outcome, tasks, users, notes)))
    for e in events:
        if e.kind in ("run_started", "run_finished") and (e.data or {}).get("role") == "lead":
            continue  # the Lead's own post says what its run did
        items.append((e.created_at, 2, {"kind": "step", **_step(e, keys)}))
    items.sort(key=lambda row: (row[0], row[1]))

    thread: list[dict[str, Any]] = []
    steps: list[dict[str, Any]] = []
    for _, _, item in items:
        if item["kind"] == "step":
            item.pop("kind")
            steps.append(item)
        elif item["kind"] == "lead":
            thread.append({**item, **_steps(steps)})
            steps = []
        else:
            thread.append(item)
    if steps:
        thread.append({"kind": "working", "at": steps[-1]["at"], **_steps(steps)})

    runs = db.query(Run).filter(Run.outcome_id == outcome.id, Run.role == "lead").order_by(Run.started_at).all()
    last = runs[-1] if runs else None
    return {"items": thread, "started_in_mind": mind,
            "lead": {"model": last.model if last else None, "running": bool(last and last.status == "running"),
                     "last_active": S.ts((last.finished_at or last.started_at) if last else None),
                     # The Lead's narration and decisions are model spend of their own, within the outcome's cost.
                     "cost_usd": round(sum(r.cost_usd or 0.0 for r in runs), 4),
                     "cost_known": all(r.cost_known for r in runs)}}


def _started_in_mind(db: Session, outcome: Outcome, viewer_id: str | None) -> dict[str, Any] | None:
    """Whether a Mind conversation became this outcome. Only its owner gets the link: the conversation before the
    hand-off is private to them."""
    from .mind.api import OUTCOME_KINDS
    from .models import MindAction

    for action in db.query(MindAction).filter(MindAction.workspace_id == outcome.workspace_id,
                                              MindAction.kind.in_(OUTCOME_KINDS), MindAction.status == "succeeded"):
        if (action.result or {}).get("outcome_id") == outcome.id:
            return {"thread_id": action.thread_id if viewer_id and action.owner_id == viewer_id else None}
    return None


def _steps(steps: list[dict[str, Any]]) -> dict[str, Any]:
    return {"steps": steps[-MAX_STEPS:], "steps_omitted": max(0, len(steps) - MAX_STEPS)}


def _name(user: User | None) -> str:
    return (user.name or user.email or "") if user else ""


def _request(db: Session, outcome: Outcome, users: dict[str, User]) -> dict[str, Any]:
    source = outcome.source
    if outcome.signal_id:
        signal = db.get(Signal, outcome.signal_id)
        source = signal.source if signal else source
    return {"kind": "request", "at": S.ts(outcome.created_at), "title": outcome.title, "brief": outcome.brief,
            "acceptance": outcome.acceptance or [], "from": SOURCES.get(source, source),
            "by": _name(users.get(outcome.created_by))}


def _message(m: Message, users: dict[str, User], keys: dict[str, str]) -> dict[str, Any]:
    attribution = m.attribution or {}
    base = {"id": m.id, "at": S.ts(m.created_at), "text": m.body, "task_key": keys.get(m.task_id)}
    if m.author_kind == "lead":
        return {"kind": "lead", **base, "update": attribution.get("kind") == "update",
                "decision_id": attribution.get("decision_id")}
    if attribution.get("source") == "lead":
        return {"kind": "lead_guidance", **base, "delivered": m.delivered}
    if m.author_kind != "user":
        return {"kind": "factory", **base}
    author = users.get(m.author_id)
    return {"kind": "person", **base, "author": _name(author) + (" via Mind" if attribution.get("source") == "mind" else ""),
            "operator": bool(author and author.is_system), "delivered": m.delivered}


def _decision(d: Decision, outcome: Outcome, tasks: dict[str, Task], users: dict[str, User],
              notes: dict[str, dict[str, Any]]) -> dict[str, Any]:
    task = tasks.get(d.task_id) if d.task_id else None
    context = {"outcome_number": outcome.number, "outcome_title": outcome.title,
               "task_key": task.key if task else None, "pr_number": task.pr_number if task else None,
               "pr_url": task.pr_url if task else "", "lead_note": notes.get(d.id),
               "resolved_by_name": _name(users.get(d.resolved_by)) if d.resolved_by else ""}
    view = S.decision(d, context)
    if (d.attribution or {}).get("source") == "lead":
        view["resolved_by_name"] = "Lead"
    return {"kind": "decision", "at": S.ts(d.created_at), "decision": view}


def _step(e: Event, keys: dict[str, str]) -> dict[str, Any]:
    data = e.data or {}
    role = ROLES.get(str(data.get("role") or ""))
    who = role or ("Planner" if e.kind.startswith("plan") or e.kind.startswith("planning") else "Factory")
    key = keys.get(e.task_id)
    step = {"at": S.ts(e.created_at), "who": f"{who} · {key}" if key else who, "text": e.message, "level": e.level,
            "event": e.kind, "run_id": e.run_id}
    if isinstance(data.get("url"), str) and data["url"].startswith("https://"):
        step["url"] = data["url"]
    if e.kind == "run_finished" and isinstance(data.get("cost_usd"), (int, float)):
        step["cost_usd"] = round(float(data["cost_usd"]), 4)
    return step
