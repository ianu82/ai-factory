"""The Message Log: who told whom what, about which object.

Agents and people already talk all day through the event journal (a plan sent for review, a verifier sending a
builder back, a decision asked and answered, an operator's relayed command). This module reads those events as
messages: sender, receiver, intent, one line of what was said, and the object it is about. It adds no writes to the
pipeline and covers history from day one.

New agents (the Supervisor, the Merge Manager, and later Mind-to-Mind) join by recording an event of kind
``message`` whose data carries ``{"message": {"from": {...}, "to": {...}, "intent": ...}}``; see :func:`message_data`.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable

from sqlalchemy.orm import Session

from .models import Event, Outcome, Task, User
from .serializers import ts

INTENTS = ("request", "answer", "propose", "inform", "handoff")
MESSAGE_KIND = "message"
SAYS = 400  # characters of the event's own message shown as what was said
SCAN = 1000  # events read at most per page, so a narrow filter over a long history stays bounded

# Party roles, and the filter groups people pick from on the page.
GROUPS: dict[str, set[str]] = {
    "minds": {"mind"},
    "lead": {"lead"},
    "people": {"person", "people", "operator"},
    "planning": {"planner", "plan_reviewer"},
    "builders": {"builder", "checks"},
    "verifier": {"verifier"},
    "ci": {"ci"},
    "judges": {"judge"},
    "supervisor": {"supervisor"},
    "merge_manager": {"merge_manager"},
    "factory": {"factory"},
}


def party(role: str, label: str, on_behalf_of: str | None = None) -> dict[str, Any]:
    return {"role": role, "label": label, **({"for": on_behalf_of} if on_behalf_of else {})}


FACTORY = party("factory", "Factory")
PEOPLE = party("people", "People")
PLANNER = party("planner", "Planner")
PLAN_REVIEWER = party("plan_reviewer", "Plan reviewer")
OPERATOR = party("operator", "Operator")
LEAD = party("lead", "Lead")  # the agent that owns one outcome end to end (pipeline/lead.py)


def message_data(sender: dict[str, Any], receiver: dict[str, Any], intent: str) -> dict[str, Any]:
    """The data block an agent records with ``kind="message"`` so it appears in the log."""
    if intent not in INTENTS:
        raise ValueError(f"intent must be one of {', '.join(INTENTS)}")
    return {"message": {"from": sender, "to": receiver, "intent": intent}}


@dataclass
class Context:
    names: dict[str, str]  # user id -> display name, for the page's actors
    tasks: dict[str, Task]
    operators: set[str] = field(default_factory=set)  # system users that operator relays act as


def _who(event: Event, ctx: Context) -> dict[str, Any]:
    """The person (or their Mind, or the operator relay, or policy) behind an event that a person caused."""
    data = event.data or {}
    if data.get("source") == "lead":  # the Lead acts as its own system user, with this attribution
        return LEAD
    if event.actor == "operator" or (event.actor or "") in ctx.operators:
        return OPERATOR
    name = ctx.names.get(event.actor or "")
    if name and data.get("source") == "mind":
        return party("mind", f"{name}'s Mind", name)
    if name:
        return party("person", name)
    if data.get("source") in ("judge", "decision_judge"):
        return party("judge", "Decision judge")
    if event.actor == "policy":
        return party("factory", "Policy")
    return FACTORY


def _task_party(role: str, label: str) -> Callable[[Event, Context], dict[str, Any]]:
    def build(event: Event, ctx: Context) -> dict[str, Any]:
        task = ctx.tasks.get(event.task_id or "")
        return party(role, f"{label} · {task.key}" if task else label)
    return build


def _const(value: dict[str, Any]) -> Callable[[Event, Context], dict[str, Any]]:
    return lambda event, ctx: value


Ends = tuple[dict[str, Any], dict[str, Any], str]
Rule = Callable[[Event, Context], Ends | None]  # None: this event is activity after all


def fixed(sender: Callable[[Event, Context], dict[str, Any]], receiver: Callable[[Event, Context], dict[str, Any]],
          intent: str) -> Rule:
    return lambda event, ctx: (sender(event, ctx), receiver(event, ctx), intent)


def _person_acted(event: Event, ctx: Context) -> bool:
    return (event.actor or "") in ctx.names


def _plan_proposed(event: Event, ctx: Context) -> Ends:
    # A plan worked out with a person (adopt_plan, through their Mind) is handed to the Factory to build: no planner
    # or plan reviewer took part. The Planner's own plan is proposed to the Factory: whether the plan reviewer, a
    # person or policy then decides depends on the workspace's autonomy and the plan, and that answer is its own
    # message (plan_reviewed, decision_requested and decision_resolved).
    if _person_acted(event, ctx):
        return _who(event, ctx), FACTORY, "handoff"
    return PLANNER, FACTORY, "propose"


def _plan_reviewed(event: Event, ctx: Context) -> Ends | None:
    # A review that never ran (over budget, could not start, no result) is recorded with verdict "error": no reviewer
    # answered.
    if (event.data or {}).get("verdict") == "error":
        return None
    return PLAN_REVIEWER, PLANNER, "answer"


def _decision_resolved(event: Event, ctx: Context) -> Ends | None:
    # Taking a task over reads once, as the Factory handing the task to the person (task_handed_over), which every
    # completed takeover records: from a decision, the task's page or a Mind. A takeover that could not complete
    # (the task had already finished) records no hand-over, so its answer stays.
    task = ctx.tasks.get(event.task_id or "")
    if (event.data or {}).get("resolution") == "take_over" and task is not None and task.status == "handed_over":
        return None
    return _who(event, ctx), FACTORY, "answer"


def _handed_over(event: Event, ctx: Context) -> Ends:
    # The Factory stops and the person (not their Mind, which only asked) works on the task from here.
    name = ctx.names.get(event.actor or "")
    receiver = party("person", name) if name and event.actor not in ctx.operators else _who(event, ctx)
    return _task_party("factory", "Factory")(event, ctx), receiver, "handoff"


def _lead_note(event: Event, ctx: Context) -> Ends:
    # A note on a decision is the Lead's recommendation to the people who decide it; otherwise it keeps them informed.
    return LEAD, PEOPLE, "propose" if (event.data or {}).get("decision_id") else "inform"


# Recorded events that are messages between parties. Everything else in the journal stays activity, including the
# transitions recorded beside an answer: plan_approved follows the reviewer's plan_reviewed, a person's
# decision_resolved, or a plan handed over through a Mind; operator_decision is the receipt for a relayed
# decision_resolved (recorded as the operator's system user).
RULES: dict[str, Rule] = {
    "plan_proposed": _plan_proposed,
    "plan_reviewed": _plan_reviewed,
    "decision_requested": fixed(_task_party("factory", "Factory"), _const(PEOPLE), "request"),
    "decision_resolved": _decision_resolved,
    "decision_overruled": fixed(_who, _const(FACTORY), "answer"),
    "candidate_rejected": fixed(_task_party("checks", "Checks"), _task_party("builder", "Builder"), "answer"),
    "verification_failed": fixed(_task_party("verifier", "Verifier"), _task_party("builder", "Builder"), "answer"),
    "verified": fixed(_task_party("verifier", "Verifier"), _task_party("builder", "Builder"), "answer"),
    "ci_failed": fixed(_task_party("ci", "CI"), _task_party("builder", "Builder"), "inform"),
    "guidance": fixed(_who, _task_party("builder", "Builders"), "inform"),
    "operator_relay": fixed(_const(OPERATOR), _const(FACTORY), "request"),
    "operator_retry": fixed(_const(OPERATOR), _const(FACTORY), "request"),
    "task_handed_over": _handed_over,
    "lead_note": _lead_note,
    "lead_update": fixed(_const(LEAD), _const(PEOPLE), "inform"),  # the Lead's post in the outcome's conversation
}
KINDS = (*RULES, MESSAGE_KIND)


def _declared(event: Event) -> tuple[dict[str, Any], dict[str, Any], str] | None:
    """A ``message`` event's own sender, receiver and intent, when well formed."""
    block = (event.data or {}).get("message")
    if not isinstance(block, dict):
        return None
    ends = []
    for key in ("from", "to"):
        end = block.get(key)
        if not isinstance(end, dict) or not isinstance(end.get("role"), str) or not isinstance(end.get("label"), str):
            return None
        on_behalf = end.get("for") if isinstance(end.get("for"), str) else None
        ends.append(party(end["role"][:40], end["label"][:80], on_behalf[:80] if on_behalf else None))
    intent = block.get("intent")
    return (ends[0], ends[1], intent) if intent in INTENTS else None


def project(event: Event, ctx: Context) -> dict[str, Any] | None:
    """One journal event as a message, or None when it is not one."""
    if event.kind == MESSAGE_KIND:
        declared = _declared(event)
        if declared is None:
            return None
        sender, receiver, intent = declared
    elif event.kind in RULES:
        ends = RULES[event.kind](event, ctx)
        if ends is None:
            return None
        sender, receiver, intent = ends
    else:
        return None
    task = ctx.tasks.get(event.task_id or "")
    return {"id": event.id, "at": ts(event.created_at), "kind": event.kind, "level": event.level,
            "from": sender, "to": receiver, "intent": intent, "says": (event.message or "")[:SAYS],
            "about": {"outcome_id": event.outcome_id, "task": {"id": task.id, "key": task.key} if task else None,
                      "pr": task.pr_number if task and task.pr_number else None}}


def _matches(message: dict[str, Any], group: str | None, intent: str | None) -> bool:
    if intent and message["intent"] != intent:
        return False
    if group:
        roles = GROUPS.get(group, set())
        return message["from"]["role"] in roles or message["to"]["role"] in roles
    return True


def log(db: Session, workspace_id: str, *, before: int | None = None, limit: int = 50, group: str | None = None,
        intent: str | None = None, outcome_id: str | None = None) -> dict[str, Any]:
    """A page of the workspace's messages, newest first. ``next_cursor`` continues before the last event read."""
    limit = max(1, min(limit, 200))
    query = db.query(Event).filter(Event.workspace_id == workspace_id, Event.kind.in_(KINDS))
    if outcome_id:
        query = query.filter(Event.outcome_id == outcome_id)
    if before:
        query = query.filter(Event.id < before)
    events = query.order_by(Event.id.desc()).limit(SCAN).all()
    actors = {e.actor for e in events if e.actor and e.actor not in ("factory", "operator", "policy")}
    users = db.query(User).filter(User.id.in_(actors)).all() if actors else []
    names = {u.id: u.name or u.email or "Someone" for u in users}
    operators = {u.id for u in users if u.is_system and (u.email or "").startswith("operator")}
    task_ids = {e.task_id for e in events if e.task_id}
    tasks = {t.id: t for t in db.query(Task).filter(Task.id.in_(task_ids))} if task_ids else {}
    ctx = Context(names, tasks, operators)
    items: list[dict[str, Any]] = []
    last_read: int | None = None
    for event in events:
        last_read = event.id
        message = project(event, ctx)
        if message and _matches(message, group, intent):
            items.append(message)
            if len(items) == limit:
                break
    more = len(items) == limit or len(events) == SCAN
    outcome_ids = {m["about"]["outcome_id"] for m in items if m["about"]["outcome_id"]}
    outcomes = {o.id: o for o in db.query(Outcome).filter(Outcome.id.in_(outcome_ids))} if outcome_ids else {}
    for m in items:
        o = outcomes.get(m["about"].pop("outcome_id") or "")
        m["about"]["outcome"] = {"id": o.id, "number": o.number, "title": o.title} if o else None
    return {"items": items, "next_cursor": last_read if more and last_read else None}
