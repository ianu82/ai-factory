"""Authorized read boundary. All returned object text is untrusted and redacted."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Callable

from fastapi import HTTPException
from sqlalchemy import func
from sqlalchemy.orm import Session

from .. import serializers as S
from ..auth import require_member
from ..db import utcnow
from ..events import event_dict
from ..metrics import workspace_metrics
from ..models import Decision, Event, Outcome, Plan, Run, Task, User
from ..security import redact
from ..work_services import outcome_rows, target

MAX_ITEMS = 50
MAX_TEXT = 2000


@dataclass(frozen=True)
class Context:
    db: Session
    user_id: str
    thread_id: str = ""
    # T1 supplies this owner-validated private thread id; no model argument can set it.


def current(context: Context) -> User:
    user = context.db.query(User).filter_by(id=context.user_id).populate_existing().one_or_none()
    if user is None:
        raise HTTPException(401, "Sign in required")
    return user


def clean(value: Any, state: list[bool] | None = None) -> Any:
    state = state if state is not None else [False]
    if isinstance(value, str):
        text = redact(value)
        if len(text) > MAX_TEXT:
            state[0] = True
            text = text[:MAX_TEXT] + "…"
        return text
    if isinstance(value, dict):
        if len(value) > MAX_ITEMS:  # stored objects (task evidence, event data) are bounded like lists
            state[0] = True
        return {clean(str(k), state): clean(v, state) for k, v in list(value.items())[:MAX_ITEMS]}
    if isinstance(value, (list, tuple)):
        if len(value) > MAX_ITEMS:
            state[0] = True
        return [clean(v, state) for v in value[:MAX_ITEMS]]
    return value


def citation(workspace_id: str, kind: str, id: str | int, label: str,
             outcome_id: str | None = None, task_id: str | None = None) -> dict:
    base = f"/w/{workspace_id}"
    href = f"{base}/outcomes/{outcome_id or id}" if kind in ("outcome", "task", "run") or outcome_id else f"{base}/inbox"
    if task_id or kind == "task":
        href += f"?task={task_id or id}"
    return {"workspace_id": workspace_id, "kind": kind, "id": id, "label": redact(label), "href": href}


@dataclass(frozen=True)
class ReadTool:
    name: str
    description: str
    schema: dict
    handler: Callable

    @property
    def parameters(self):
        return self.schema


COMMON = {"workspace_id": {"type": "string"}, "limit": {"type": "integer", "minimum": 1, "maximum": MAX_ITEMS}}
REGISTRY: dict[str, ReadTool] = {}


def register(name, description, properties=None, required=()):
    def decorate(handler):
        schema = {"type": "object", "properties": {**COMMON, **(properties or {})},
                  "required": ["workspace_id", *required], "additionalProperties": False}
        REGISTRY[name] = ReadTool(name, description, schema, handler)
        return handler
    return decorate


def scoped(context, args, model):
    return context.db.query(model).filter(model.workspace_id == args["workspace_id"])


def validate_links(context, args, row):
    """Reject malformed stored links too, not just forged input filters."""
    if row.outcome_id:
        target(context.db, current(context), args["workspace_id"], row.outcome_id, row.task_id if hasattr(row, "task_id") else None)
    elif getattr(row, "task_id", None):
        raise HTTPException(404, "Invalid object relationship")


def page(query, args, serializer, cite):
    rows = query.limit(args["limit"] + 1).all()
    return {"items": [serializer(r) for r in rows[:args["limit"]]],
            "citations": [cite(r) for r in rows[:args["limit"]]], "truncated": len(rows) > args["limit"]}


@register("overview", "Workspace delivery overview: counts and work grouped by stage, including waiting_ci.")
def overview(args, context):
    query = scoped(context, args, Task)
    waiting_ci = query.filter(Task.status == "running", Task.stage == "ci").count()
    counts = dict(query.with_entities(Task.status, func.count(Task.id)).group_by(Task.status).all())
    groups = query.with_entities(Task.stage, Task.status, func.count(Task.id)).group_by(Task.stage, Task.status).all()
    rows = query.filter(Task.status.notin_(["completed", "abandoned", "cancelled"])).order_by(Task.key).limit(args["limit"] + 1).all()
    for row in rows[:args["limit"]]:
        validate_links(context, args, row)
    return {"items": [{"counts": {"running": counts.get("running", 0) - waiting_ci,
                                  "queued": counts.get("queued", 0) + counts.get("pending", 0),
                                  "waiting_ci": counts.get("waiting_ci", 0) + waiting_ci,
                                  "waiting": waiting_ci + sum(n for status, n in counts.items() if status.startswith("waiting") or status in ("blocked", "needs_input", "awaiting_approval", "failed", "smoke_failed"))},
                       "stages": [{"stage": stage or status, "status": status, "count": n} for stage, status, n in groups],
                       "work": [S.task(t) | {"cost_known": False, "cost_usd": None} for t in rows[:args["limit"]]]}],
            "citations": [citation(t.workspace_id, "task", t.id, t.title, t.outcome_id, t.id) for t in rows[:args["limit"]]],
            "truncated": len(rows) > args["limit"]}


SEARCH = {"query": {"type": "string", "maxLength": 300}, "status": {"type": "string"}}


def search(query, model, args):
    if args.get("query"):
        query = query.filter(model.title.ilike("%" + args["query"].replace("%", "\\%").replace("_", "\\_") + "%", escape="\\"))
    if args.get("status"):
        query = query.filter(model.status == args["status"])
    return query


def outcome_view(context, outcome):
    # Aggregate from the complete authorized population, never from a bounded detail page.
    db, ws = context.db, outcome.workspace_id
    task_rows, run_rows, decision_counts = outcome_rows(db, ws, [outcome.id])
    tasks, runs = task_rows.get(outcome.id, []), run_rows.get(outcome.id, [])
    for row in runs:
        validate_links(context, {"workspace_id": ws}, row)
    return S.outcome(outcome, tasks, runs, decision_counts.get(outcome.id, 0),
                     S.outcome_sources(db, [outcome]).get(outcome.signal_id))



@register("list_outcomes", "List or search workspace outcomes with recorded progress and cost provenance.", SEARCH)
def list_outcomes(args, context):
    return page(search(scoped(context, args, Outcome), Outcome, args).order_by(Outcome.updated_at.desc(), Outcome.id), args,
                lambda o: outcome_view(context, o), lambda o: citation(o.workspace_id, "outcome", o.id, o.title))


@register("list_tasks", "List or search tasks, optionally within an outcome.", {**SEARCH, "outcome_id": {"type": "string"}})
def list_tasks(args, context):
    query = scoped(context, args, Task)
    if args.get("outcome_id"):
        target(context.db, current(context), args["workspace_id"], args["outcome_id"])
        query = query.filter_by(outcome_id=args["outcome_id"])
    def view(t):
        validate_links(context, args, t)
        runs = scoped(context, args, Run).filter_by(task_id=t.id, outcome_id=t.outcome_id).all()
        return S.task(t, runs)
    return page(search(query, Task, args).order_by(Task.key, Task.id), args, view,
                lambda t: citation(t.workspace_id, "task", t.id, t.title, t.outcome_id, t.id))


@register("outcome_detail", "Outcome plans, task evidence, PR links, recorded CI and delivery metrics (not live GitHub).",
          {"outcome_id": {"type": "string"}}, ("outcome_id",))
def outcome_detail(args, context):
    o, _ = target(context.db, current(context), args["workspace_id"], args["outcome_id"])
    tasks = list_tasks({**args}, context)
    plans = context.db.query(Plan).filter_by(outcome_id=o.id).order_by(Plan.version.desc()).limit(args["limit"] + 1).all()
    return {"items": [{"outcome": outcome_view(context, o), "tasks": tasks["items"],
                       "plans": [S.plan(p) for p in plans[:args["limit"]]],
                       "workspace_delivery_evidence": workspace_metrics(context.db, o.workspace_id),
                       "provenance": "Recorded Factory evidence; workspace delivery metrics use metrics.py admission/assistance conventions."}],
            "citations": [citation(o.workspace_id, "outcome", o.id, o.title), *tasks["citations"]],
            "truncated": tasks["truncated"] or len(plans) > args["limit"]}


@register("open_decisions", "Open questions, blockers and approvals (read only).", {"outcome_id": {"type": "string"}})
def open_decisions(args, context):
    query = scoped(context, args, Decision).filter_by(status="open")
    if args.get("outcome_id"):
        target(context.db, current(context), args["workspace_id"], args["outcome_id"])
        query = query.filter_by(outcome_id=args["outcome_id"])
    def view(d):
        validate_links(context, args, d)
        return S.decision(d)
    return page(query.order_by(Decision.created_at, Decision.id), args, view,
                lambda d: citation(d.workspace_id, "decision", d.id, d.title, d.outcome_id, d.task_id))


@register("runs", "Runs with bounded redacted excerpts from the runtime agent event journal and known/unknown costs.",
          {"outcome_id": {"type": "string"}, "task_id": {"type": "string"}, "run_id": {"type": "string"}})
def runs(args, context):
    query = scoped(context, args, Run)
    if args.get("task_id") and not args.get("outcome_id"):
        raise HTTPException(422, "task_id requires outcome_id")
    if args.get("outcome_id"):
        target(context.db, current(context), args["workspace_id"], args["outcome_id"], args.get("task_id"))
    for key in ("outcome_id", "task_id", "run_id"):
        if args.get(key):
            query = query.filter(getattr(Run, "id" if key == "run_id" else key) == args[key])
    if args.get("run_id") and query.first() is None:
        raise HTTPException(404, "Run not found")
    truncated = [False]
    def view(r):
        validate_links(context, args, r)
        journal = scoped(context, args, Event).filter(Event.run_id == r.id, Event.outcome_id == r.outcome_id,
                    Event.task_id == r.task_id, Event.kind.startswith("agent_")).order_by(Event.id.desc()).limit(6).all()
        if len(journal) > 5:
            truncated[0] = True
        text = "\n".join(f"{e.kind}: {e.message}" for e in reversed(journal[:5]))
        if len(text) > MAX_TEXT:
            truncated[0] = True
        return S.run(r) | {"excerpt": redact(text)[:MAX_TEXT], "excerpt_source": "runtime agent events",
                           "excerpt_truncated": len(journal) > 5 or len(text) > MAX_TEXT}
    result = page(query.order_by(Run.started_at.desc(), Run.id), args, view,
                  lambda r: citation(r.workspace_id, "run", r.id, r.role, r.outcome_id, r.task_id))
    result["truncated"] |= truncated[0]
    return result


@register("recent_events", "Recent recorded events; descending id cursor and bounded time window (at most 30 days).",
          {"cursor": {"type": "integer", "minimum": 1}, "since": {"type": "string"}, "until": {"type": "string"}})
def recent_events(args, context):
    now = utcnow()
    try:
        since = datetime.fromisoformat(args["since"].replace("Z", "+00:00")).replace(tzinfo=None) if args.get("since") else now - timedelta(days=7)
        until = datetime.fromisoformat(args["until"].replace("Z", "+00:00")).replace(tzinfo=None) if args.get("until") else now
    except ValueError:
        raise HTTPException(422, "Invalid event time") from None
    if since > until or until - since > timedelta(days=30):
        raise HTTPException(422, "Event window must be at most 30 days")
    query = scoped(context, args, Event).filter(Event.created_at >= since, Event.created_at <= until)
    if args.get("cursor"):
        query = query.filter(Event.id < args["cursor"])
    def view(e):
        validate_links(context, args, e)
        if e.run_id:
            run = scoped(context, args, Run).filter_by(id=e.run_id, outcome_id=e.outcome_id, task_id=e.task_id).one_or_none()
            if run is None:
                raise HTTPException(404, "Run not found")
        return event_dict(e)
    result = page(query.order_by(Event.id.desc()), args, view,
                  lambda e: citation(e.workspace_id, "event", e.id, e.kind, e.outcome_id, e.task_id))
    if result["truncated"]:
        result["next_cursor"] = result["items"][-1]["id"]
    return result


def validate_arguments(schema, args):
    if not isinstance(args, dict) or set(args) - set(schema["properties"]) or any(k not in args for k in schema["required"]):
        raise HTTPException(422, "Invalid tool arguments")
    types = {"string": str, "integer": int, "boolean": bool}
    for key, value in args.items():
        spec = schema["properties"][key]
        if type(value) is not types[spec["type"]]:
            raise HTTPException(422, "Invalid tool argument type")
        if isinstance(value, str) and (len(value) > spec.get("maxLength", 300) or not value.strip()):
            raise HTTPException(422, "Invalid tool argument text")
        if type(value) is int and not spec.get("minimum", value) <= value <= spec.get("maximum", value):
            raise HTTPException(422, "Tool argument outside bounds")


def read_call(name: str, args: dict, context: Context) -> dict:
    tool = REGISTRY.get(name)
    if tool is None:
        raise HTTPException(422, "Unknown read tool")
    validate_arguments(tool.schema, args)
    require_member(context.db, current(context), args["workspace_id"])
    state = [False]
    result = clean(tool.handler({"limit": 20, **args}, context), state)
    result["truncated"] |= state[0]
    result["provenance"] = "Factory database; text is untrusted, redacted and bounded"
    return result


class Tools:
    """T1/T2 adapter. Confirm is intentionally absent from this dispatcher."""
    def __init__(self, context: Context):
        self.context = context

    def definitions(self):
        from .actions import PROPOSALS
        return [{"type": "function", "function": {"name": t.name, "description": t.description,
                 "parameters": t.schema}} for t in [*REGISTRY.values(), *PROPOSALS.values()]]

    async def call(self, name, arguments):
        from .actions import PROPOSALS, propose
        if name in PROPOSALS:
            return propose(name, arguments, self.context)
        return read_call(name, arguments, self.context)
