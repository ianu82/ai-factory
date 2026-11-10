"""Proposal-only model tools and separately authenticated confirmation adapters.

The model only proposes: a proposal checks its target with the person's own role, saves a MindAction and returns a
card. Nothing changes until the person confirms that card over HTTP (never a model tool). A confirmation checks the
target again against the digest of what the card showed, then runs the app's own service for the action, attributed
"<person> via Mind": decisions.resolve for any option a decision offers, work_services for guidance, new outcomes and
cancellations, planning.adopt_plan for a plan worked out with the person, handover for a take-over. What must follow
the commit (stopping planners and jobs, GitHub follow-ups) runs on the event loop through work_services.finish.

Every kind renders as one card wire shape: id, revision, kind, workspace, object {kind, id, label, href}, fields
(strings; only fields.text is the person's to edit), effect (what confirming does, in plain words), status, result.
"""
from __future__ import annotations

import asyncio
import copy
import hashlib
import json
import logging
from dataclasses import dataclass, field
from typing import Any, Callable

import anyio
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy import update
from sqlalchemy.orm import Session

from ..auth import current_user, require_member
from ..db import get_db
from ..decisions import DecisionError, resolve
from ..harness.schemas import PLANNER_REPORT, validate_report
from ..models import Decision, MindAction, MindActionResult, Outcome, Repository, Task, User, Workspace
from ..pipeline.handover import NOT_TAKEABLE, HandoverError, mark_handed_over
from ..pipeline.planning import adopt_plan
from ..security import redact
from ..serializers import actionable
from ..work_services import cancel_outcome, cancel_task, create_outcome, finish, publish_changes, send_guidance, target
from .routing import PrivateRoute
from .tools import Context, ReadTool, citation, clean, current, validate_arguments

# The thread API's route class: a 422 never echoes rejected input (which can hold a pasted secret).
router = APIRouter(prefix="/mind/threads", route_class=PrivateRoute)
log = logging.getLogger(__name__)

NEEDS_TEXT = frozenset({"answer", "replan", "reject"})  # options the app's decision card asks a comment for first
PRIORITY = {0: "Urgent", 1: "High", 2: "Normal", 3: "Low"}  # the app's new-outcome form
STRING = {"type": "string"}
TEXT = {"type": "string", "maxLength": 8000}
PRIORITY_ARG = {"type": "integer", "minimum": 0, "maximum": 3,
                "description": "0 urgent, 1 high, 2 normal (default), 3 low. It cannot be changed later."}
NO_EXECUTION = " Creates a pending card for the person to confirm in the app; it never executes anything itself."


@dataclass(frozen=True)
class Ask:
    """What a card names: from the model's arguments at proposal, from the saved action at confirmation."""
    workspace_id: str
    outcome_id: str | None
    task_id: str | None
    decision_id: str | None
    params: dict


@dataclass
class Target:
    """The checked target: its objects, the card's labels and parameters, and the state the card is bound to."""
    outcome: Outcome | None = None
    task: Task | None = None
    decision: Decision | None = None
    params: dict = field(default_factory=dict)
    labels: dict = field(default_factory=dict)
    state: dict = field(default_factory=dict)


def digest(state: dict) -> str:
    """Only a digest is kept: redacting the saved card can never make an unchanged target look changed."""
    return hashlib.sha256(json.dumps(state, sort_keys=True, default=str).encode()).hexdigest()


def live(context: Context, workspace_id: str, outcome_id: str, task_id: str | None = None):
    """The app's member check and object links (work_services.target), on work that is still active."""
    outcome, task = target(context.db, current(context), workspace_id, outcome_id, task_id, "member")
    if outcome.status in ("completed", "cancelled"):
        raise HTTPException(409, "Outcome is no longer active")
    return outcome, task


def refs(outcome: Outcome | None, task: Task | None) -> dict:
    labels: dict[str, Any] = {}
    if outcome is not None:
        labels |= {"outcome": f"#{outcome.number} “{outcome.title}”", "outcome_label": f"#{outcome.number} {outcome.title}"}
        if task is not None:
            labels |= {"task": f"#{outcome.number} {task.key}", "task_title": task.title,
                       "task_label": f"#{outcome.number} {task.key} · {task.title}"}
            if task.pr_number:
                labels["pr"] = f"PR #{task.pr_number}"
    return labels


def identity(outcome: Outcome, task: Task | None) -> dict:
    state = {"outcome": [outcome.id, outcome.number, outcome.title]}
    if task is not None:
        state["task"] = [task.id, task.key, task.title, task.pr_number]
    return state


def text_of(kind: str, params: dict, text: str | None) -> str:
    text = redact((text or "").strip())
    if len(text) > 8000:
        raise HTTPException(422, "Text must contain at most 8000 characters")
    option = params.get("option")
    if not text and (KINDS[kind].text == "required" or option in NEEDS_TEXT):
        raise HTTPException(422, f"“{option}” needs a comment" if option in NEEDS_TEXT else
                            "Text must contain 1–8000 characters")
    return text


def repository(db: Session, workspace_id: str, wanted: str | None) -> Repository:
    repos = db.query(Repository).filter_by(workspace_id=workspace_id).order_by(Repository.full_name).all()
    if wanted:
        name = wanted.strip().removeprefix("https://github.com/").strip("/").casefold()
        repo = next((r for r in repos if r.id == wanted.strip()), None) or next(
            (r for r in repos if r.full_name.casefold() == name), None)
    else:
        repo = repos[0] if len(repos) == 1 else None
    if repo is None:
        raise HTTPException(422, "Name one of the workspace's connected repositories (the repositories tool lists "
                                 "them)" if repos else "This workspace has no connected repository")
    return repo


def work_object(action: MindAction, labels: dict) -> dict:
    if action.task_id:
        return citation(action.workspace_id, "task", action.task_id, labels.get("task_label") or "Task",
                        action.outcome_id, action.task_id)
    return citation(action.workspace_id, "outcome", action.outcome_id, labels.get("outcome_label") or "Outcome")


def new_outcome_object(action: MindAction, labels: dict) -> dict:
    result = action.result or {}
    if result.get("outcome_id"):
        return citation(action.workspace_id, "outcome", result["outcome_id"],
                        f"#{result.get('number')} {(action.fields or {}).get('title', '')}".strip())
    return citation(action.workspace_id, "workspace", action.workspace_id, labels.get("workspace") or "Workspace")


def created(action: MindAction) -> str:
    number = (action.result or {}).get("number")
    title = (action.fields or {}).get("title", "")
    return f"#{number} “{title}”" if number else f"a new outcome “{title}”"


# --- guidance ---------------------------------------------------------------------------------------------------
def check_guidance(context, ask):
    outcome, task = live(context, ask.workspace_id, ask.outcome_id, ask.task_id)
    return Target(outcome, task, labels=refs(outcome, task), state=identity(outcome, task))


def guidance_effect(labels, action):
    on = labels.get("task") or labels.get("outcome") or "this work"
    return f"Post the text below as your guidance on {on}; the Factory's agents follow it in their next steps."


def run_guidance(context, action, t, user, payloads, later):
    message, payload = send_guidance(context.db, user, action.workspace_id, action.outcome_id, action.fields["text"],
                                     action.task_id, action_id=action.id)
    payloads.append(payload)
    return {"ok": True, "message_id": message.id}


# --- decisions --------------------------------------------------------------------------------------------------
def check_resolution(context, ask):
    db, user = context.db, current(context)
    require_member(db, user, ask.workspace_id, "member")
    decision = db.query(Decision).filter_by(id=ask.decision_id, workspace_id=ask.workspace_id).with_for_update() \
        .populate_existing().one_or_none()
    if decision is None or (decision.task_id and not decision.outcome_id):
        raise HTTPException(404, "Decision not found")
    outcome = task = None
    if decision.outcome_id:
        outcome, task = live(context, ask.workspace_id, decision.outcome_id, decision.task_id)
    if decision.status != "open":
        raise HTTPException(409, "Decision is no longer open")
    if not actionable(decision):  # the app shows no options either while its judge works
        raise HTTPException(409, "A decision judge is still reviewing this decision; wait for its verdict")
    options = [o for o in decision.options or [] if isinstance(o, dict) and isinstance(o.get("value"), str) and o["value"]]
    wanted = str(ask.params.get("option") or "").strip()
    chosen = next((o for o in options if o["value"] == wanted), None) or next(
        (o for o in options if str(o.get("label") or "").casefold() == wanted.casefold()), None)
    if chosen is None:
        raise HTTPException(422, "Choose one of this decision's options: " + ", ".join(o["value"] for o in options))
    labels = {**refs(outcome, task), "decision": decision.title, "decision_kind": decision.kind,
              "option": str(chosen.get("label") or chosen["value"])}
    state = {"decision": [decision.id, decision.kind, decision.title, decision.body, decision.options,
                          decision.outcome_id, decision.task_id]}
    if outcome is not None:
        # A parked task's position is part of what is decided: a new head means a new review.
        state["work"] = [*identity(outcome, task).values(), outcome.status if task is None else None,
                         [task.status, task.stage, task.attempt, task.head_sha] if task else None]
    return Target(outcome, task, decision, params={"option": chosen["value"]}, labels=labels, state=state)


CANCELS = "Cancel outcome {outcome}; the Factory stops its work."
RESOLUTIONS = {  # what decisions.resolve does with each option, in the person's words
    ("approve_merge", "approve"): "Approve the merge of {pr} ({task}); the Factory merges it.",
    ("approve_merge", "reject"): "Request changes on {pr} ({task}): the comment below becomes guidance and the "
                                 "Factory revises it with fresh attempts.",
    ("approve_plan", "approve"): "Approve the proposed plan for {outcome}; its tasks start building and may incur "
                                 "execution costs.",
    ("approve_plan", "replan"): "Reject the proposed plan for {outcome}; the Factory plans it again following the "
                                "comment below.",
    ("approve_plan", "cancel"): CANCELS,
    ("question", "answer"): "Answer “{decision}” for {target} with the comment below; the Factory continues with it.",
    ("question", "approve"): "Approve the plan proposed for {outcome}; its tasks start building and may incur "
                             "execution costs.",
    ("question", "cancel"): CANCELS,
    ("budget", "retry"): "Let {task} continue past its outcome's budget; further work incurs execution costs.",
    ("budget", "abandon"): "Abandon {task} at its outcome's budget; the Factory stops working on it.",
    ("unblock", "retry"): "Retry {target} (“{decision}”); the work runs again and may incur execution costs.",
    ("unblock", "take_over"): "Take over {task} yourself: the Factory stops working on it and hands you its latest "
                              "candidate (opened as a draft pull request when it has none).",
    ("unblock", "revert"): "Revert the merge of {task}; the Factory prepares and merges the revert.",
    ("unblock", "acknowledge"): "Acknowledge “{decision}” for {target} and accept things as they are.",
    ("unblock", "fix"): "Acknowledge “{decision}” for {target} and create a new fix outcome for it.",
    ("unblock", "cancel"): CANCELS,
}
GUIDED = {("question", "approve"), ("unblock", "retry")}  # resolve() posts their optional comment as guidance


def resolution_effect(labels, action):
    kind, value = labels.get("decision_kind"), (action.snapshot.get("params") or {}).get("option")
    template = RESOLUTIONS.get((kind, value))
    if template is None and value == "abandon":
        template = "Abandon {task}; the Factory stops working on it{closing}."
    template = template or "Resolve “{decision}” for {target} as “{option}”."
    target_ref = labels.get("task") or labels.get("outcome") or "this workspace"
    effect = template.format_map({
        "decision": labels.get("decision", ""), "option": labels.get("option", value), "target": target_ref,
        "task": labels.get("task") or target_ref, "outcome": labels.get("outcome") or target_ref,
        "pr": labels.get("pr") or "its pull request",
        "closing": f" and closes {labels['pr']}" if labels.get("pr") and kind in ("question", "unblock") else ""})
    if (action.fields or {}).get("text") and value not in NEEDS_TEXT:
        effect += (" The comment below is posted as guidance." if (kind, value) in GUIDED else
                   " The comment below is recorded with the decision.")
    return effect


def decision_object(action, labels):
    return citation(action.workspace_id, "decision", action.decision_id, labels.get("decision") or "Decision",
                    action.outcome_id, action.task_id)


def run_resolution(context, action, t, user, payloads, later):
    try:
        return resolve(context.db, t.decision, user, t.params["option"], action.fields.get("text", ""),
                       action_id=action.id, pending_events=payloads, after=later)
    except DecisionError as exc:
        raise HTTPException(409, str(exc)) from None


# --- new outcomes -----------------------------------------------------------------------------------------------
def check_new_outcome(context, ask):
    db = context.db
    require_member(db, current(context), ask.workspace_id, "member")
    repo = repository(db, ask.workspace_id, ask.params.get("repository"))
    workspace = db.get(Workspace, ask.workspace_id)
    return Target(params={"repository": repo.id, "priority": ask.params.get("priority", 2)},
                  labels={"workspace": workspace.name, "repository": repo.full_name},
                  state={"repository": [repo.id, repo.full_name]})


def title_of(args: dict) -> str:
    title = redact(args["title"].strip())
    if len(title) < 3:
        raise HTTPException(422, "An outcome title needs 3–300 characters")
    return title


def new_outcome_effect(labels, action):
    priority = (action.fields or {}).get("priority", "Normal").lower()
    return (f"Create {created(action)} in {labels.get('workspace', 'this workspace')} for "
            f"{labels.get('repository', 'its repository')} at {priority} priority, with the brief below; the Factory "
            "starts planning it. Planning and building may incur execution costs.")


def run_new_outcome(context, action, t, user, payloads, later):
    outcome = create_outcome(context.db, user, action.workspace_id, title=action.fields["title"],
                             brief=action.fields["text"], repo_ids=[t.params["repository"]],
                             priority=t.params["priority"], action_id=action.id, pending_events=payloads)
    return {"ok": True, "outcome_id": outcome.id, "number": outcome.number}


# --- a plan worked out with the person --------------------------------------------------------------------------
def planner_task_schema() -> dict:
    """The planner's own task fields (harness/schemas.py), bounded as a tool argument, so the Mind's plans carry
    exactly what the planner's do."""
    item = PLANNER_REPORT["properties"]["tasks"]["items"]
    properties = {}
    for name, spec in item["properties"].items():
        bounded = {k: v for k, v in spec.items() if k in ("type", "description")}
        if spec["type"] == "array":
            items = dict(spec.get("items") or STRING)
            if items.get("type") == "string":
                items["maxLength"] = 2000
            bounded |= {"items": items, "maxItems": 30}
        elif spec["type"] == "string":
            bounded["maxLength"] = {"key": 20, "spec": 8000}.get(name, 300)  # Task.key is 20 characters
        properties[name] = bounded
    return {"type": "object", "properties": properties, "required": list(item["required"]),
            "additionalProperties": False}


def checked_plan(report: dict) -> dict:
    """The planner's validator, exactly as it judges the planner: keys, dependencies (and their cycles), required
    fields, and whatever else harness/schemas.py enforces (shared-file sequencing, constraints)."""
    normalized, error = validate_report("planner", report)
    if normalized is None:
        raise HTTPException(422, f"The plan does not pass the planner's checks: {error}")
    return normalized


def check_plan(context, ask):
    db = context.db
    require_member(db, current(context), ask.workspace_id, "member")
    workspace = db.get(Workspace, ask.workspace_id)
    repos = db.query(Repository).filter_by(workspace_id=ask.workspace_id).all()
    plan = copy.deepcopy(ask.params.get("plan") or {})
    tasks = plan.get("tasks") if isinstance(plan.get("tasks"), list) else []
    known = plan.get("repo_ids") if isinstance(plan.get("repo_ids"), list) else []  # saved by an earlier check
    used: dict[str, Repository] = {}
    chosen: list[str] = []
    for i, task in enumerate(tasks):
        name = str(task.get("repo") or "").strip().removeprefix("https://github.com/").strip("/").casefold() \
            if isinstance(task, dict) else ""
        repo = next((r for r in repos if i < len(known) and r.id == known[i]), None) or next(
            (r for r in repos if r.full_name.casefold() == name or r.id == name), None)
        if repo is None:
            raise HTTPException(422, f"tasks[{i}].repo is not a connected repository (the repositories tool lists them)")
        task["repo"] = repo.full_name  # approve_plan finds each task's repository by this name
        used[repo.id] = repo
        chosen.append(repo.id)
    report = checked_plan({"summary": "", "tasks": tasks, "questions": [], "risks": list(plan.get("risks") or [])})
    # A renamed repository then reads as a changed target (the state below), not as an unknown one.
    return Target(params={"plan": {"tasks": report["tasks"], "risks": report.get("risks") or [], "repo_ids": chosen},
                          "priority": ask.params.get("priority", 2)},
                  labels={"workspace": workspace.name, "repositories": sorted(r.full_name for r in used.values()),
                          "keys": [task["key"] for task in report["tasks"]]},
                  state={"repositories": sorted([r.id, r.full_name] for r in used.values())})


def plan_listing(tasks: list[dict]) -> str:
    return "\n".join(f"{t['key']} {t['title']} — {t['repo']}" + (f" (after {', '.join(t['depends_on'])})"
                                                                 if t.get("depends_on") else "") for t in tasks)


def plan_effect(labels, action):
    keys = labels.get("keys") or []
    size = f"{len(keys)}-task plan" if len(keys) != 1 else "1-task plan"
    return (f"Create {created(action)} in {labels.get('workspace', 'this workspace')} with this {size} "
            f"({', '.join(keys)}) and start building: your confirmation approves the plan, so the Factory skips its own "
            "planning. Building may incur execution costs.")


def run_plan(context, action, t, user, payloads, later):
    plan = t.params["plan"]
    report = checked_plan({"summary": action.fields["text"], "tasks": copy.deepcopy(plan["tasks"]), "questions": [],
                           "risks": list(plan["risks"])})
    names = {r.full_name: r.id for r in context.db.query(Repository).filter_by(workspace_id=action.workspace_id)}
    repo_ids = list(dict.fromkeys(names[task["repo"]] for task in report["tasks"]))
    outcome = create_outcome(context.db, user, action.workspace_id, title=action.fields["title"],
                             brief=action.fields["text"], repo_ids=repo_ids, priority=t.params["priority"],
                             action_id=action.id, pending_events=payloads)
    adopted = adopt_plan(context.db, outcome, report, user, action_id=action.id, pending_events=payloads)
    return {"ok": True, "outcome_id": outcome.id, "number": outcome.number, "plan_id": adopted.id,
            "tasks": [task["key"] for task in report["tasks"]]}


# --- stopping and handing over work -----------------------------------------------------------------------------
def check_outcome_cancel(context, ask):
    outcome, _ = live(context, ask.workspace_id, ask.outcome_id)
    # The pull requests cancel_outcome() will close: unfinished tasks' that are still open.
    pulls = sorted(t.pr_number for t in context.db.query(Task).filter(
        Task.outcome_id == outcome.id, Task.pr_number.isnot(None), Task.status.notin_(["completed", "abandoned"]))
        if not (t.evidence or {}).get("pr_closed"))
    return Target(outcome, labels={**refs(outcome, None), "pulls": pulls},
                  state={**identity(outcome, None), "pulls": pulls})


def outcome_cancel_effect(labels, action):
    pulls = labels.get("pulls") or []
    closing = f", and closes pull request{'s' if len(pulls) > 1 else ''} {', '.join(f'#{n}' for n in pulls)}" if pulls else ""
    return (f"Cancel outcome {labels.get('outcome', '')}: the Factory stops its planner and jobs, cancels its "
            f"unfinished tasks and supersedes its open decisions{closing}.")


def run_outcome_cancel(context, action, t, user, payloads, later):
    payload, steps = cancel_outcome(context.db, user, action.workspace_id, action.outcome_id, action_id=action.id)
    payloads.append(payload)
    later.extend(steps)
    return {"ok": True}


def check_open_task(context, ask):
    outcome, task = live(context, ask.workspace_id, ask.outcome_id, ask.task_id)
    if task.status in NOT_TAKEABLE:
        raise HTTPException(409, f"Task is already {task.status.replace('_', ' ')}")
    return Target(outcome, task, labels=refs(outcome, task), state=identity(outcome, task))


def task_cancel_effect(labels, action):
    closing = f", and closes {labels['pr']}" if labels.get("pr") else ""
    return f"Stop task {labels.get('task', '')} “{labels.get('task_title', '')}”: the Factory cancels it and stops its job{closing}."


def run_task_cancel(context, action, t, user, payloads, later):
    payload, steps = cancel_task(context.db, user, action.workspace_id, action.task_id, action_id=action.id)
    payloads.append(payload)
    later.extend(steps)
    return {"ok": True}


def take_over_effect(labels, action):
    then = (f"{labels['pr']} stays open for you to continue." if labels.get("pr") else
            "its latest candidate, if any, is pushed and opened for you as a draft pull request.")
    return (f"Take over task {labels.get('task', '')} “{labels.get('task_title', '')}” yourself: the Factory stops "
            f"working on it and supersedes its open decisions; {then}")


def run_take_over(context, action, t, user, payloads, later):
    try:
        info = mark_handed_over(t.task.id, user, action_id=action.id, session=context.db, pending_events=payloads)
    except HandoverError as exc:
        raise HTTPException(409, str(exc)) from None
    later.extend([("stop_task", t.task.id, action.workspace_id, action.outcome_id), ("publish_candidate", t.task.id, info)])
    return {"ok": True}


# --- registry ---------------------------------------------------------------------------------------------------
@dataclass(frozen=True)
class Kind:
    description: str
    properties: dict
    required: tuple
    text: str  # "required", "optional" or "none": whether fields.text exists and must be filled
    params: Callable[[dict], dict]
    check: Callable[[Context, Ask], Target]
    fields: Callable[[dict, Target], dict]
    effect: Callable[[dict, MindAction], str]
    object: Callable[[MindAction, dict], dict]
    execute: Callable


def no_params(args):
    return {}


KINDS: dict[str, Kind] = {
    "resolve_decision": Kind(
        "Propose resolving an open decision with one of the options it offers (open_decisions lists them with their "
        "values): approve, or request changes (reject) on a merge; approve or revise (replan) a plan; answer a "
        "question; unblock work: retry a failed task or planning, abandon, take over, revert, acknowledge, create a "
        "fix, cancel. text is the comment, required for answer, replan and reject; omit it otherwise unless the "
        "person wants one." + NO_EXECUTION,
        {"decision_id": STRING, "option": STRING, "text": TEXT}, ("decision_id", "option"), "optional",
        lambda args: {"option": args["option"]}, check_resolution,
        lambda args, t: {"option": t.labels["option"], "text": text_of("resolve_decision", t.params, args.get("text"))},
        resolution_effect, decision_object, run_resolution),
    "send_guidance": Kind(
        "Propose posting the person's guidance (text) on an outcome, or on one of its tasks with task_id; the "
        "Factory's agents follow it in their next steps." + NO_EXECUTION,
        {"outcome_id": STRING, "task_id": STRING, "text": TEXT}, ("outcome_id", "text"), "required",
        no_params, check_guidance, lambda args, t: {"text": text_of("send_guidance", {}, args["text"])},
        guidance_effect, work_object, run_guidance),
    "create_outcome": Kind(
        "Propose a new outcome for the Factory to plan and build: title (3–300 characters), text (the brief: what "
        "and why), repository (an id or owner/name from the repositories tool; optional when the workspace has "
        "exactly one), priority." + NO_EXECUTION,
        {"title": STRING, "text": TEXT, "repository": STRING, "priority": PRIORITY_ARG}, ("title", "text"), "required",
        lambda args: {"repository": args.get("repository"), "priority": args.get("priority", 2)}, check_new_outcome,
        lambda args, t: {"title": title_of(args), "repository": t.labels["repository"],
                         "priority": PRIORITY[t.params["priority"]], "text": text_of("create_outcome", {}, args["text"])},
        new_outcome_effect, new_outcome_object, run_new_outcome),
    "build_plan": Kind(
        "Propose handing a feature you planned with the person to the Factory at the build stage: a new outcome "
        "(title; text is the plan summary, also its brief) with this plan as its approved plan, so the Factory "
        "skips its own planning and starts building. tasks use the planner's own fields: key (T1, T2, …), title, "
        "repo (owner/name of a connected repository), spec, acceptance (verifiable criteria), depends_on (keys), and "
        "any others the planner requires; the planner's validator checks them on proposal and again on "
        "confirmation." + NO_EXECUTION,
        {"title": STRING, "text": TEXT, "tasks": {"type": "array", "items": planner_task_schema(), "minItems": 1,
                                                  "maxItems": 20},
         "risks": {"type": "array", "items": {"type": "string", "maxLength": 2000}, "maxItems": 30,
                   "description": "Assumptions and residual risks of this plan."}, "priority": PRIORITY_ARG},
        ("title", "text", "tasks"), "required",
        lambda args: {"plan": {"tasks": args["tasks"], "risks": args.get("risks") or []},
                      "priority": args.get("priority", 2)}, check_plan,
        lambda args, t: {"title": title_of(args), "repositories": ", ".join(t.labels["repositories"]),
                         "priority": PRIORITY[t.params["priority"]], "plan": plan_listing(t.params["plan"]["tasks"]),
                         "text": text_of("build_plan", {}, args["text"])},
        plan_effect, new_outcome_object, run_plan),
    "cancel_outcome": Kind(
        "Propose cancelling an outcome: the Factory stops its planner and jobs, cancels its unfinished tasks, "
        "supersedes its open decisions and closes the cancelled tasks' pull requests." + NO_EXECUTION,
        {"outcome_id": STRING}, ("outcome_id",), "none", no_params, check_outcome_cancel, lambda args, t: {},
        outcome_cancel_effect, work_object, run_outcome_cancel),
    "cancel_task": Kind(
        "Propose stopping one task: it is cancelled, its job stops and its pull request is closed." + NO_EXECUTION,
        {"outcome_id": STRING, "task_id": STRING}, ("outcome_id", "task_id"), "none", no_params, check_open_task,
        lambda args, t: {}, task_cancel_effect, work_object, run_task_cancel),
    "take_over_task": Kind(
        "Propose handing a task over to the person: the Factory stops working on it; its latest candidate is "
        "pushed and opened as a draft pull request when it has none." + NO_EXECUTION,
        {"outcome_id": STRING, "task_id": STRING}, ("outcome_id", "task_id"), "none", no_params, check_open_task,
        lambda args, t: {}, take_over_effect, work_object, run_take_over),
}

def ask_of(action: MindAction) -> Ask:
    return Ask(action.workspace_id, action.outcome_id, action.task_id, action.decision_id,
               (action.snapshot or {}).get("params") or {})


def kind_of(action: MindAction) -> Kind:
    spec = KINDS.get(action.kind)
    if spec is None or "state" not in (action.snapshot or {}):
        raise HTTPException(409, "This card predates Mind's current actions; ask Mind to propose it again")
    return spec


def legacy_view(action: MindAction) -> tuple[dict, str]:
    """Cards saved before the current kinds (answer_question, retry_task, early guidance) still render."""
    snapshot = action.snapshot or {}
    task = action.task_id is not None
    label = snapshot.get("task_title") if task else snapshot.get("outcome_title")
    obj = citation(action.workspace_id, "task" if task else "outcome", action.task_id or action.outcome_id,
                   label or "Work", action.outcome_id, action.task_id)
    decision = snapshot.get("decision") or {}
    if action.kind == "answer_question":
        effect = f'Answer the open question “{decision.get("title", "")}” with the exact text below; resume its work.'
        obj = citation(action.workspace_id, "decision", action.decision_id, decision.get("title") or "Question",
                       action.outcome_id, action.task_id)
    elif action.kind == "retry_task":
        effect = "Resolve the open unblock decision as retry and requeue this task. Work may run again and incur execution costs."
    else:
        effect = "Post the exact text below as your guidance on this " + ("task." if task else "outcome.")
    return obj, effect


def card(action: MindAction, context: Context) -> dict:
    workspace = context.db.get(Workspace, action.workspace_id)
    spec = KINDS.get(action.kind)
    if spec is not None and "state" in (action.snapshot or {}):
        labels = action.snapshot.get("labels") or {}
        obj, effect = spec.object(action, labels), spec.effect(labels, action)
    else:
        obj, effect = legacy_view(action)
    return json.loads(redact(json.dumps({"id": action.id, "thread_id": action.thread_id, "revision": action.revision,
                  "kind": action.kind, "workspace": {"id": action.workspace_id, "name": workspace.name if workspace else ""},
                  "object": {k: obj[k] for k in ("kind", "id", "label", "href")},
                  "fields": action.fields, "effect": effect, "status": action.status,
                  **({"result": action.result} if action.result is not None else {})})))


def propose(name: str, args: dict, context: Context) -> dict:
    tool = PROPOSALS.get(name)
    if tool is None:
        raise HTTPException(422, "Unknown proposal")
    validate_arguments(tool.schema, args)
    if not context.thread_id or len(context.thread_id) > 32:
        raise HTTPException(422, "An owner-validated thread context is required")
    kind = name.removeprefix("propose_")
    spec = KINDS[kind]
    checked = spec.check(context, Ask(args["workspace_id"], args.get("outcome_id"), args.get("task_id"),
                                      args.get("decision_id"), spec.params(args)))
    fields = spec.fields(args, checked)
    action = MindAction(owner_id=context.user_id, thread_id=context.thread_id, kind=kind,
                        workspace_id=args["workspace_id"], outcome_id=checked.outcome.id if checked.outcome else None,
                        task_id=checked.task.id if checked.task else None,
                        decision_id=checked.decision.id if checked.decision else None, fields=fields,
                        snapshot={"labels": checked.labels, "params": checked.params, "state": digest(checked.state)})
    context.db.add(action)
    context.db.commit()
    return card(action, context)


PROPOSALS = {f"propose_{name}": ReadTool(
    f"propose_{name}", spec.description,
    {"type": "object", "properties": {"workspace_id": STRING, **spec.properties},
     "required": ["workspace_id", *spec.required], "additionalProperties": False}, propose)
    for name, spec in KINDS.items()}


def owned(context: Context, action_id: str):
    action = context.db.query(MindAction).filter_by(id=action_id, thread_id=context.thread_id,
                   owner_id=current(context).id).populate_existing().one_or_none()
    if action is None:
        raise HTTPException(404, "Action not found")
    return action


def claim(context, action, expected_revision, **values):
    count = context.db.execute(update(MindAction).where(MindAction.id == action.id,
                 MindAction.owner_id == context.user_id, MindAction.thread_id == context.thread_id,
                 MindAction.revision == expected_revision, MindAction.status == "pending").values(**values),
                 execution_options={"synchronize_session": False}).rowcount
    if count != 1:
        context.db.rollback()
        raise HTTPException(409, "Action changed or is no longer pending")
    context.db.refresh(action)


def edit(context: Context, action_id: str, expected_revision: int, *, text: str | None = None, dismiss=False):
    action = owned(context, action_id)
    if dismiss and text is not None or not dismiss and text is None:
        raise HTTPException(422, "Choose text or dismiss")
    # Dismiss cannot write anything to shared objects, even after membership revocation.
    if dismiss:
        claim(context, action, expected_revision, status="dismissed", revision=expected_revision + 1)
    else:
        spec = kind_of(action)
        if spec.text == "none":
            raise HTTPException(422, "This card has no editable text")
        spec.check(context, ask_of(action))  # still the person's to change, and still eligible
        # Only the text is editable; the target and every other field stay as proposed.
        fields = {**(action.fields or {}), "text": text_of(action.kind, ask_of(action).params, text)}
        claim(context, action, expected_revision, fields=fields, revision=expected_revision + 1)
    context.db.commit()
    return card(action, context)


def finish_here(fn, *args):
    """Outside a request's worker thread (scripts, tests): run the follow-ups on an event loop of their own."""
    return asyncio.run(fn(*args))


def confirm(context: Context, action_id: str, expected_revision: int, *, finish_on: Callable | None = None):
    """Execute a pending card exactly once. finish_on runs the after-commit follow-ups on the app's event loop (the
    HTTP route passes anyio.from_thread.run); without it they run on a loop of their own."""
    db = context.db
    action = owned(context, action_id)
    payloads: list[dict] = []
    later: list[tuple] = []
    try:
        # Conditional claim serializes confirmations and edits before any shared mutation.
        claim(context, action, expected_revision, status="executing")
        spec = kind_of(action)
        checked = spec.check(context, ask_of(action))
        if digest(checked.state) != action.snapshot.get("state"):
            raise HTTPException(409, "Action target changed; propose a new card")
        user = current(context)
        result = spec.execute(context, action, checked, user, payloads, later)
        action.status = "succeeded"
        action.result = clean({"action_id": action.id, "kind": action.kind, "status": "succeeded", **result})
        db.add(MindActionResult(thread_id=action.thread_id, owner_id=context.user_id,
                               action_id=action.id, content=action.result))
        db.commit()
    except Exception:
        db.rollback()
        raise
    publish_changes(payloads)
    if later:
        try:
            done = (finish_on or finish_here)(finish, later, user, action.id)
        except Exception:  # the confirmed change is committed; say what did not follow rather than fail the request
            log.exception("Follow-ups of Mind action %s did not run", action.id)
            done = {"follow_ups_failed": [step for step, *_ in later]}
        if done:
            # What the follow-ups did (a hand-over's draft pull request, pull requests closed) joins the result.
            action.result = clean({**(action.result or {}), **done})
            row = db.query(MindActionResult).filter_by(action_id=action.id).one_or_none()
            if row is not None:
                row.content = action.result
            db.commit()
    return card(action, context)


def execution_results(context: Context) -> list[dict]:
    """Owner-only continuation projection for T1 to include in the next engine turn."""
    current(context)
    return [clean(row.content) for row in context.db.query(MindActionResult).filter_by(
        thread_id=context.thread_id, owner_id=context.user_id).order_by(MindActionResult.created_at.desc()).limit(20)][::-1]


class EditBody(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)
    expected_revision: int = Field(ge=1)
    text: str | None = Field(default=None, max_length=8000)
    dismiss: bool = False


class ConfirmBody(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)
    expected_revision: int = Field(ge=1)


def _private(db: Session, user: User, thread_id: str) -> tuple:
    """The thread API's own access rule and egress scrub (owner, every scoped workspace still a membership; the
    current secrets of the person's workspaces), applied to card edits and confirmations too."""
    from . import store
    from .api import scrub, secrets_for

    thread = store.owned(db, user.id, thread_id)
    secrets = secrets_for(db, user.id, thread.workspace_ids)
    return (lambda value: scrub(value, secrets)), thread


@router.patch("/{thread_id}/actions/{action_id}")
def patch_action(thread_id: str, action_id: str, body: EditBody,
                 user: User = Depends(current_user), db: Session = Depends(get_db)):
    clean_value, _ = _private(db, user, thread_id)
    text = clean_value(body.text) if body.text is not None else None
    return clean_value(edit(Context(db, user.id, thread_id), action_id, body.expected_revision, text=text,
                            dismiss=body.dismiss))


@router.post("/{thread_id}/actions/{action_id}/confirm")
def confirm_action(thread_id: str, action_id: str, body: ConfirmBody,
                   user: User = Depends(current_user), db: Session = Depends(get_db)):
    clean_value, _ = _private(db, user, thread_id)
    action = db.query(MindAction).filter_by(id=action_id, thread_id=thread_id, owner_id=user.id).one_or_none()
    if action is not None and action.status == "pending":
        # A secret saved after the card was proposed is still never posted to shared work: neither the shown fields
        # nor the saved parameters a card executes from (a plan's specs, acceptance and risks). The target's state
        # is only a digest, so this never makes an unchanged target look changed. The rewrite is a compare-and-swap
        # on the revision the person is confirming, so a concurrent edit is never overwritten (a changed revision
        # then fails the confirmation's own claim with 409).
        fields, snapshot = clean_value(action.fields), clean_value(action.snapshot)
        if fields != action.fields or snapshot != action.snapshot:
            db.execute(update(MindAction).where(MindAction.id == action.id, MindAction.status == "pending",
                                                MindAction.revision == body.expected_revision)
                       .values(fields=fields, snapshot=snapshot), execution_options={"synchronize_session": False})
            db.commit()
            db.expire(action)
    # This route runs in a worker thread: follow-ups (stopping jobs, GitHub) run on the app's event loop.
    return clean_value(confirm(Context(db, user.id, thread_id), action_id, body.expected_revision,
                               finish_on=anyio.from_thread.run))
