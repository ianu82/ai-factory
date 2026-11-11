"""Authorized read boundary. All returned object text is untrusted and redacted."""
from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from itertools import islice
from typing import Any, Callable

from fastapi import HTTPException
from sqlalchemy import func
from sqlalchemy.orm import Session

from .. import pulls
from .. import serializers as S
from ..auth import require_member
from ..db import utcnow
from ..events import event_dict
from ..metrics import workspace_metrics
from ..models import Decision, Event, Message, Outcome, Plan, Repository, Run, Task, User
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


def clean(value: Any, state: list[bool] | None = None, text: int = MAX_TEXT, items: int = MAX_ITEMS,
          keys: int = MAX_ITEMS) -> Any:
    state = state if state is not None else [False]
    if isinstance(value, str):
        out = redact(value)
        if len(out) > text:
            state[0] = True
            out = out[:text] + "…"
        return out
    if isinstance(value, dict):
        # Stored objects (task evidence, event data) are bounded like lists, at their own key bound: the first
        # shortening steps keep every field of a record and shorten only its texts and lists.
        if len(value) > keys:
            state[0] = True
        return {clean(str(k), state, text, items, keys): clean(v, state, text, items, keys)
                for k, v in islice(value.items(), keys)}
    if isinstance(value, (list, tuple)):
        if len(value) > items:
            state[0] = True
        return [clean(v, state, text, items, keys) for v in value[:items]]
    return value


# Every model call re-sends the turn's earlier tool results, so one oversized read is paid for again and again (a
# five-bullet answer over two full reads cost $2.21). A result over the budget is shortened in steps, and says so.
RESULT_BUDGET = 40_000  # characters of JSON, roughly 10k tokens
# (characters per text, items per list, keys per object) for each further step: objects keep their fields until
# the last step, which also narrows them; anything still over the budget becomes an empty page that says so.
SHORTER = ((400, 10, MAX_ITEMS), (150, 5, MAX_ITEMS), (80, 3, 12))
NARROW = "Shortened to fit: read one object (outcome_detail, or runs with a run_id) for its full text."
BRIEF_TEXT = 300


def task_brief(t: Task) -> dict:
    """A task in a list: what it is and where it stands. outcome_detail has its spec, acceptance, evidence and runs."""
    return {"id": t.id, "outcome_id": t.outcome_id, "key": t.key, "title": t.title, "status": t.status,
            "stage": t.stage, "attempt": t.attempt, "pr_number": t.pr_number, "pr_url": t.pr_url,
            "head_sha": t.head_sha, "blocked_reason": brief(t.blocked_reason), "summary": brief(t.summary),
            "updated_at": S.ts(t.updated_at)}


def brief(text: str | None) -> str:
    # Redact the whole text first: cutting first could leave a credential's prefix that no pattern recognises.
    return redact(text or "")[:BRIEF_TEXT]


def citation(workspace_id: str, kind: str, id: str | int, label: str,
             outcome_id: str | None = None, task_id: str | None = None) -> dict:
    base = f"/w/{workspace_id}"
    href = f"{base}/outcomes/{outcome_id or id}" if kind in ("outcome", "task", "run") or outcome_id else f"{base}/inbox"
    if kind == "workspace":
        href = f"{base}/outcomes"
    elif kind == "repository":
        href = f"{base}/settings/repositories"
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
# People name work by its number and key, as the app shows it; tools take either that or the id.
OUTCOME_REF = {"type": "string", "description": "The outcome's id, or its number as people write it: #53 or 53."}
TASK_REF = {"type": "string", "description": "The task's id, or its key within the outcome: T1, or #53 T1."}
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


_NUMBER = re.compile(r"#?\s*(\d{1,9})")
_TASK_LABEL = re.compile(r"#?\s*(\d{1,9})\s+(\S{1,20})")  # "#53 T1": how the app and Mind's cards name a task


def resolve_refs(context: Context, args: dict) -> dict:
    """The arguments with the outcome and task as people name them (#53, 53, T1, #53 T1) replaced by their ids,
    which the Factory's own checks take. Numbers and keys resolve only within the call's workspace, for its members.
    A reference that matches nothing stays as written, so the check that follows refuses it like any unknown id."""
    outcome_ref, task_ref = args.get("outcome_id"), args.get("task_id")
    if not (outcome_ref or task_ref):
        return args
    db, ws = context.db, args["workspace_id"]
    require_member(db, current(context), ws)
    label = _TASK_LABEL.fullmatch(task_ref.strip()) if task_ref else None
    outcome = _find_outcome(db, ws, outcome_ref or (label[1] if label else None))
    if outcome is None:
        return args
    resolved = {**args, "outcome_id": outcome.id}
    if label and int(label[1]) == outcome.number:
        task_ref = label[2]
    if task_ref and (task := _find_task(db, ws, outcome.id, task_ref.strip())) is not None:
        resolved["task_id"] = task.id
    return resolved


def _find_outcome(db: Session, workspace_id: str, ref: str | None) -> Outcome | None:
    if not ref:
        return None
    ref = ref.strip()
    outcome = db.query(Outcome).filter_by(id=ref, workspace_id=workspace_id).one_or_none()
    if outcome is None and (number := _NUMBER.fullmatch(ref)):
        outcome = db.query(Outcome).filter_by(workspace_id=workspace_id, number=int(number[1])).one_or_none()
    return outcome


def _find_task(db: Session, workspace_id: str, outcome_id: str, ref: str) -> Task | None:
    task = db.query(Task).filter_by(id=ref, workspace_id=workspace_id, outcome_id=outcome_id).one_or_none()
    if task is not None:
        return task
    keyed = db.query(Task).filter(Task.workspace_id == workspace_id, Task.outcome_id == outcome_id,
                                  func.lower(Task.key) == ref.lower()).limit(2).all()
    if len(keyed) > 1:
        raise HTTPException(422, "More than one task in this outcome has that key: use the task's id from list_tasks")
    return keyed[0] if keyed else None


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
                       "work": [task_brief(t) for t in rows[:args["limit"]]]}],
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


@register("list_tasks", "List or search tasks, optionally within an outcome: status, stage, PR and cost. "
          "task_detail has one task's spec, evidence and runs.", {**SEARCH, "outcome_id": OUTCOME_REF})
def list_tasks(args, context):
    return tasks_page(args, context, full=False)


def tasks_page(args, context, *, full: bool):
    query = scoped(context, args, Task)
    if args.get("outcome_id"):
        target(context.db, current(context), args["workspace_id"], args["outcome_id"])
        query = query.filter_by(outcome_id=args["outcome_id"])
    def view(t):
        validate_links(context, args, t)
        runs = scoped(context, args, Run).filter_by(task_id=t.id, outcome_id=t.outcome_id).all()
        if full:
            return S.task(t, runs)
        return task_brief(t) | {"cost_usd": round(sum(r.cost_usd for r in runs), 4),
                                "cost_known": all(r.cost_known for r in runs)}
    return page(search(query, Task, args).order_by(Task.key, Task.id), args, view,
                lambda t: citation(t.workspace_id, "task", t.id, t.title, t.outcome_id, t.id))


@register("task_detail", "One task's full record: spec, acceptance criteria, evidence (attempts, CI, smoke, "
          "failures), runs and cost. Needs the task's outcome_id and task_id.",
          {"outcome_id": OUTCOME_REF, "task_id": TASK_REF}, ("outcome_id", "task_id"))
def task_detail(args, context):
    _, t = target(context.db, current(context), args["workspace_id"], args["outcome_id"], args["task_id"])
    validate_links(context, args, t)
    runs = scoped(context, args, Run).filter_by(task_id=t.id, outcome_id=t.outcome_id).all()
    return {"items": [S.task(t, runs)], "citations": [citation(t.workspace_id, "task", t.id, t.title, t.outcome_id, t.id)],
            "truncated": False}


@register("outcome_detail", "Outcome plans, task evidence, PR links, recorded CI, its decisions (newest first), the "
          "guidance people posted (newest first) and delivery metrics (not live GitHub).",
          {"outcome_id": OUTCOME_REF}, ("outcome_id",))
def outcome_detail(args, context):
    o, _ = target(context.db, current(context), args["workspace_id"], args["outcome_id"])
    tasks = tasks_page({**args}, context, full=True)
    db, limit = context.db, args["limit"]
    plans = db.query(Plan).filter_by(outcome_id=o.id).order_by(Plan.version.desc()).limit(limit + 1).all()
    decisions = scoped(context, args, Decision).filter_by(outcome_id=o.id).order_by(
        Decision.created_at.desc(), Decision.id).limit(limit + 1).all()
    messages = scoped(context, args, Message).filter_by(outcome_id=o.id).order_by(
        Message.created_at.desc(), Message.id).limit(limit + 1).all()
    for row in [*decisions, *messages]:
        validate_links(context, args, row)
    names = {u.id: u.name or u.email or "" for u in db.query(User).filter(
        User.id.in_({m.author_id for m in messages if m.author_id}))}
    return {"items": [{"outcome": outcome_view(context, o), "tasks": tasks["items"],
                       "plans": [S.plan(p) for p in plans[:limit]],
                       "decisions": [S.decision(d) for d in decisions[:limit]],
                       "messages": [S.message(m, names.get(m.author_id or "", "Factory")) for m in messages[:limit]],
                       "workspace_delivery_evidence": workspace_metrics(db, o.workspace_id),
                       "provenance": "Recorded Factory evidence; workspace delivery metrics use metrics.py admission/assistance conventions."}],
            "citations": [citation(o.workspace_id, "outcome", o.id, o.title), *tasks["citations"]],
            "truncated": tasks["truncated"] or any(len(rows) > limit for rows in (plans, decisions, messages))}


@register("open_decisions", "Open questions, blockers and approvals waiting on a person, each with the options "
          "(value and label) propose_resolve_decision can choose and what it is about (outcome, task, PR). "
          "actionable is false while a decision judge is still reviewing it.", {"outcome_id": OUTCOME_REF})
def open_decisions(args, context):
    query = scoped(context, args, Decision).filter_by(status="open")
    if args.get("outcome_id"):
        target(context.db, current(context), args["workspace_id"], args["outcome_id"])
        query = query.filter_by(outcome_id=args["outcome_id"])
    def view(d):
        validate_links(context, args, d)
        o = context.db.get(Outcome, d.outcome_id) if d.outcome_id else None
        t = context.db.get(Task, d.task_id) if d.task_id else None
        # The inbox's context: what the decision is about, so a proposal can name it.
        return S.decision(d, {"outcome_number": o.number if o else None, "outcome_title": o.title if o else "",
                              "task_key": t.key if t else None, "task_status": t.status if t else None,
                              "pr_number": t.pr_number if t else None, "pr_url": t.pr_url if t else "",
                              "head_sha": t.head_sha if t else ""})
    return page(query.order_by(Decision.created_at, Decision.id), args, view,
                lambda d: citation(d.workspace_id, "decision", d.id, d.title, d.outcome_id, d.task_id))


@register("runs", "Runs with bounded redacted excerpts from the runtime agent event journal and known/unknown costs.",
          {"outcome_id": OUTCOME_REF, "task_id": TASK_REF, "run_id": {"type": "string"}})
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


@register("repositories", "Connected repositories (id, owner/name, default branch, description): the ones "
          "propose_create_outcome and propose_build_plan can name.")
def repositories(args, context):
    query = scoped(context, args, Repository).order_by(Repository.full_name, Repository.id)
    return page(query, args, lambda r: {"id": r.id, "full_name": r.full_name, "default_branch": r.default_branch,
                                        "description": r.description},
                lambda r: citation(r.workspace_id, "repository", r.id, r.full_name))


FAILED_CHECKS = ("failure", "error", "timed_out", "cancelled")  # as pulls.py counts a check as failing


@register("pull_requests", "Pull requests the Factory opened, as the Factory recorded them: repository, number and "
          "link, task and outcome, head and merge commits, the last recorded CI result with failing checks, smoke, "
          "and the open decision waiting on it with its options. state: open (default: not merged and not closed by "
          "the Factory, including handed-over work), merged or all. Review "
          "threads and live CI are on GitHub; github holds the app's Pull requests page snapshot when it is recent.",
          {"outcome_id": OUTCOME_REF, "state": {"type": "string"}})
def pull_requests(args, context):
    state = args.get("state", "open")
    if state not in ("open", "merged", "all"):
        raise HTTPException(422, "state must be open, merged or all")
    db, ws = context.db, args["workspace_id"]
    query = scoped(context, args, Task).filter(Task.pr_number.isnot(None))
    if args.get("outcome_id"):
        target(db, current(context), ws, args["outcome_id"])
        query = query.filter(Task.outcome_id == args["outcome_id"])
    if state == "open":
        # The pull request's own recorded state, not the task's: a handed-over task's PR stays open for its person.
        query = query.filter(Task.merge_sha == "", Task.evidence["pr_closed"].as_boolean().is_not(True))
    elif state == "merged":
        query = query.filter(Task.merge_sha != "")
    repos = {r.id: r.full_name for r in scoped(context, args, Repository)}
    snapshot = pulls.cached(ws)  # no GitHub call: only what the Pull requests page fetched recently
    live = {(r.get("repo"), r.get("number")): r for r in (snapshot or {}).get("items") or [] if isinstance(r, dict)}

    def view(t):
        validate_links(context, args, t)
        o = db.get(Outcome, t.outcome_id)
        decision = scoped(context, args, Decision).filter_by(outcome_id=t.outcome_id, task_id=t.id, status="open") \
            .order_by(Decision.created_at.desc(), Decision.id).first()
        evidence = t.evidence if isinstance(t.evidence, dict) else {}
        ci = evidence.get("ci") if isinstance(evidence.get("ci"), dict) else {}
        checks = [c for c in ci.get("checks") or [] if isinstance(c, dict)]
        smoke = evidence.get("smoke") if isinstance(evidence.get("smoke"), dict) else {}
        item = {"repository": repos.get(t.repo_id, ""), "number": t.pr_number, "url": t.pr_url,
                "merged": bool(t.merge_sha), "closed_by_factory": bool(evidence.get("pr_closed")),
                "head_sha": t.head_sha, "merge_sha": t.merge_sha,
                "outcome": {"id": o.id, "number": o.number, "title": o.title, "status": o.status},
                "task": {"id": t.id, "key": t.key, "title": t.title, "status": t.status, "stage": t.stage,
                         "attempt": t.attempt, "blocked_reason": t.blocked_reason},
                "ci": {"state": ci.get("state"), "head": ci.get("head"), "at": ci.get("at"), "checks": len(checks),
                       "failing": [c.get("name") for c in checks if c.get("conclusion") in FAILED_CHECKS][:20],
                       "pending": [c.get("name") for c in checks if c.get("status") not in (None, "completed")][:20]}
                      if ci else None,
                "smoke": smoke.get("state"),
                "decision": {"id": decision.id, "kind": decision.kind, "title": decision.title,
                             "options": decision.options, "actionable": S.actionable(decision)} if decision else None}
        row = live.get((repos.get(t.repo_id), t.pr_number))
        if row:
            detail = row.get("detail") if isinstance(row.get("detail"), dict) else {}
            item["github"] = {"group": row.get("group"), "reason": row.get("reason"), "draft": row.get("draft"),
                              "ci": row.get("ci"), **{k: detail.get(k) for k in (
                                  "mergeable_state", "open_threads", "open_thread_authors", "changes_requested_by",
                                  "threads_unchecked")}}
        return item
    result = page(query.order_by(Task.updated_at.desc(), Task.id), args, view,
                  lambda t: citation(ws, "task", t.id, f"PR #{t.pr_number} ({t.key})", t.outcome_id, t.id))
    if snapshot:
        result["github_fetched_at"] = datetime.fromtimestamp(snapshot["fetched_at"], timezone.utc).isoformat()
    return result


TYPES = {"string": str, "integer": int, "boolean": bool, "array": list, "object": dict}


def validate_arguments(schema, args):
    """Model arguments against a tool's own schema: closed objects, bounded lists, nonblank bounded text."""
    _valid(schema, args, "")


def _valid(spec, value, where):
    kind = spec["type"]
    if type(value) is not TYPES[kind]:
        raise HTTPException(422, f"Invalid tool argument type: {where}" if where else "Invalid tool arguments")
    if kind == "object":
        properties = spec.get("properties", {})
        missing = [k for k in spec.get("required", ()) if k not in value]
        if set(value) - set(properties) or missing:
            # Name only schema keys: an unknown key is the model's own text.
            raise HTTPException(422, "Invalid tool arguments" + (f" at {where}" if where else "")
                                + (f"; missing {', '.join(missing)}" if missing else "; unknown argument"))
        for key, child in value.items():
            _valid(properties[key], child, f"{where}.{key}" if where else key)
    elif kind == "array":
        if not spec.get("minItems", 0) <= len(value) <= spec.get("maxItems", MAX_ITEMS):
            raise HTTPException(422, f"Tool argument outside bounds: {where}")
        for i, child in enumerate(value):
            _valid(spec["items"], child, f"{where}[{i}]")
    elif kind == "string":
        if len(value) > spec.get("maxLength", 300) or not value.strip():
            raise HTTPException(422, f"Invalid tool argument text: {where}")
    elif kind == "integer" and not spec.get("minimum", value) <= value <= spec.get("maximum", value):
        raise HTTPException(422, f"Tool argument outside bounds: {where}")


def read_call(name: str, args: dict, context: Context) -> dict:
    tool = REGISTRY.get(name)
    if tool is None:
        raise HTTPException(422, "Unknown read tool")
    validate_arguments(tool.schema, args)
    require_member(context.db, current(context), args["workspace_id"])
    raw = tool.handler({"limit": 20, **resolve_refs(context, args)}, context)
    state = [False]
    result = clean(raw, state)
    size = lambda value: len(json.dumps(value, ensure_ascii=False, default=str))
    for text, items, keys in SHORTER:
        if size(result) <= RESULT_BUDGET:
            break
        state = [True]
        result = clean(raw, state, text, items, keys) | {"narrow": NARROW}
    if size(result) > RESULT_BUDGET:  # nothing smaller is still meaningful: say so instead of sending it
        result = {"items": [], "citations": (result.get("citations") or [])[:5], "truncated": True,
                  "narrow": "Too large to show even shortened: " + NARROW}
    if "cursor" in tool.schema["properties"] and isinstance(raw.get("items"), list):
        kept = result.get("items") or []
        if len(kept) < len(raw["items"]):
            # Shortening dropped items: the next page starts after the last one shown, not the last one read.
            if kept and isinstance(kept[-1], dict) and "id" in kept[-1]:
                result["next_cursor"] = kept[-1]["id"]
            else:
                result.pop("next_cursor", None)
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
