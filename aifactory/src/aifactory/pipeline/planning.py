"""Planning: turn an outcome into an approved, dependency-ordered set of tasks."""

from __future__ import annotations

import asyncio
import logging

from sqlalchemy.orm import Session

from ..db import session_scope, utcnow
from ..events import notify, record
from ..github import resolve_credential as resolve_token
from ..gitops import checkout
from ..models import Outcome, Plan, Repository, Task
from ..onboarding_policy import lock_workspace
from ..prompts import PLANNER, plan_prompt
from ..sandbox import LocalSandbox
from .runtime import (SessionSpec, guidance_history, open_decision, precedents, run_agent, set_outcome,
                      merged_settings, workspace_settings)

log = logging.getLogger(__name__)


async def plan_outcome(outcome_id: str, cancelled: asyncio.Event) -> None:
    with session_scope() as s:
        outcome = s.get(Outcome, outcome_id)
        if outcome.source == "review":
            raise ValueError("Review outcomes cannot be planned")
        repos = s.query(Repository).filter(Repository.id.in_(outcome.repo_ids or [])).all()
        workspace_id = outcome.workspace_id
        version = len(outcome.plans) + 1
        snapshot = Outcome(**{c.name: getattr(outcome, c.name) for c in Outcome.__table__.columns})
    if not repos:
        set_outcome(outcome_id, status="blocked", blocked_reason="No repositories are selected for this outcome.")
        return
    settings = workspace_settings(workspace_id)
    harness_name = snapshot.harness or settings["harness"].get("planner") or "native"
    record(workspace_id, "planning_started", f"Planning outcome #{snapshot.number}", outcome_id=outcome_id)
    sandbox = LocalSandbox(f"plan-{outcome_id[:12]}-{version}").create()
    try:
        token = resolve_token(workspace_id)
        sandbox.work.mkdir(parents=True, exist_ok=True)
        for repo in repos:
            await checkout(sandbox, repo.full_name, token, repo.default_branch, f"factory/plan-{outcome_id[:8]}",
                           subdir=repo.full_name.replace("/", "__"), adopt=False)
        sandbox.adopt_work()
        decisions = precedents(workspace_id)
        prompt = plan_prompt(snapshot, repos, decisions, guidance_history(outcome_id))
        run, result, error = await run_agent(SessionSpec(
            workspace_id=workspace_id, outcome_id=outcome_id, task_id=None, role="planner", harness=harness_name,
            prompt=prompt, system=PLANNER, sandbox=sandbox, effective_settings=settings,
            repositories={r.full_name: r.config or {} for r in repos}), cancelled)
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        log.exception("planning setup failed")
        result, error, run = None, str(exc), None
    finally:
        await sandbox.close()

    if result is None:
        set_outcome(outcome_id, status="blocked", blocked_reason=f"Planning failed: {error}")
        open_decision(workspace_id, "unblock", f"Planning failed for #{snapshot.number}",
                      f"The planner could not produce a plan.\n\n{error}",
                      outcome_id=outcome_id, options=[{"value": "retry", "label": "Retry planning"},
                                                      {"value": "cancel", "label": "Cancel outcome"}])
        return

    report = result.report
    by_name = {r.full_name.lower(): r for r in repos}
    problems = [t["repo"] for t in report.get("tasks", []) if t["repo"].lower() not in by_name]
    if problems and len(repos) == 1:
        for task in report["tasks"]:
            task["repo"] = repos[0].full_name
        problems = []
    with session_scope() as s:
        for old in s.query(Plan).filter(Plan.outcome_id == outcome_id, Plan.status == "proposed"):
            old.status = "superseded"
        plan = Plan(outcome_id=outcome_id, version=version, summary=report.get("summary", ""), content=report,
                    run_id=run.id if run else None)
        s.add(plan)
        s.flush()
        plan_id = plan.id
    record(workspace_id, "plan_proposed", f"Plan v{version}: {len(report.get('tasks', []))} task(s)",
           outcome_id=outcome_id, level="success", data={"plan_id": plan_id})
    notify(workspace_id, "plan", plan_id)

    if problems:
        set_outcome(outcome_id, status="blocked", blocked_reason=f"Plan references unknown repositories: {problems}")
        return
    questions = report.get("questions") or []
    if questions:
        body = "\n\n".join(f"**{q['question']}**\n{q.get('why', '')}" +
                           (("\nOptions: " + ", ".join(q["options"])) if q.get("options") else "") for q in questions)
        set_outcome(outcome_id, status="needs_input")
        options = [{"value": "answer", "label": "Answer and re-plan"}]
        if report.get("tasks"):
            body += "\n\nThe planner also proposed a plan with its own assumptions:\n" + report.get("summary", "")
            options.insert(0, {"value": "approve", "label": "Approve plan as proposed"})
        options.append({"value": "cancel", "label": "Cancel"})
        open_decision(workspace_id, "question", f"Planner needs a decision for #{snapshot.number}", body,
                      outcome_id=outcome_id, options=options)
        return
    if not approve_plan(plan_id, approved_by=None):
        set_outcome(outcome_id, status="awaiting_approval")
        open_decision(workspace_id, "approve_plan", f"Approve plan for #{snapshot.number}: {snapshot.title}",
                      report.get("summary", ""), outcome_id=outcome_id,
                      options=[{"value": "approve", "label": "Approve"}, {"value": "replan", "label": "Revise"},
                               {"value": "cancel", "label": "Cancel"}])


def approve_plan(plan_id: str, approved_by: str | None, *, session: Session | None = None,
                 pending_events: list[dict] | None = None, by: str | None = None,
                 attribution: dict | None = None) -> bool:
    """Materialize tasks idempotently; automatic approval holds the settings lock.

    False means a person must approve. The policy check and approval are one
    transaction so onboarding activation cannot slip between them. With a session
    (a confirmed Mind card), the approval joins the caller's transaction and its
    event waits in pending_events until the caller has committed.
    """
    if session is None:
        with session_scope() as s:
            approved, workspace_id, outcome_id = _approve(s, plan_id, approved_by)
    else:
        approved, workspace_id, outcome_id = _approve(session, plan_id, approved_by)
    if outcome_id is None:  # already approved, or waiting for a person
        return approved
    who = by or ("policy (automatic)" if approved_by is None else "a team member")
    payload = record(workspace_id, "plan_approved", f"Plan approved by {who}", outcome_id=outcome_id, level="success",
                     actor=approved_by or "policy", data={"plan_id": plan_id, **(attribution or {})}, session=session,
                     publish=session is None)
    if session is None:
        notify(workspace_id, "outcome", outcome_id)
    else:
        pending_events.append(payload)
    return True


def _approve(s: Session, plan_id: str, approved_by: str | None) -> tuple[bool, str | None, str | None]:
    plan = s.get(Plan, plan_id)
    outcome = s.get(Outcome, plan.outcome_id)
    if outcome.source == "review":
        raise ValueError("Review outcomes cannot be planned")
    workspace_id = outcome.workspace_id
    ws = lock_workspace(s, workspace_id)
    s.refresh(plan)
    if plan.status == "approved":
        return True, None, None
    settings = merged_settings(ws.settings)
    if approved_by is None and settings["autonomy"].get("plan") != "auto":
        return False, None, None
    repos = {r.full_name.lower(): r for r in s.query(Repository).filter(Repository.workspace_id == workspace_id)}
    plan.status = "approved"
    plan.approved_by = approved_by
    plan.approved_at = utcnow()
    builder = outcome.harness or settings["harness"].get("builder") or "native"
    verifier = outcome.harness or settings["harness"].get("verifier") or builder
    for item in plan.content.get("tasks", []):
        repo = repos.get(item["repo"].lower())
        s.add(Task(workspace_id=workspace_id, outcome_id=outcome.id, plan_id=plan.id, key=item["key"][:20],
                   title=item["title"][:300], spec=item["spec"], acceptance=list(item.get("acceptance") or []),
                   repo_id=repo.id, depends_on=list(item.get("depends_on") or []), harness=builder,
                   verifier_harness=verifier, status="pending"))
    outcome.status = "running"
    outcome.blocked_reason = ""
    return True, workspace_id, outcome.id


def adopt_plan(s: Session, outcome: Outcome, report: dict, user, *, action_id: str,
               pending_events: list[dict]) -> Plan:
    """A plan a person worked out with Mind becomes the outcome's approved plan, in the caller's transaction: the
    Factory skips its own planning and starts building. The report has passed validate_report("planner", ...) with
    repository names as connected; the person's confirmation is the plan approval the autonomy policy asks for."""
    by = (user.name or user.email) + " via Mind"
    attribution = {"source": "mind", "action_id": action_id}
    version = s.query(Plan).filter(Plan.outcome_id == outcome.id).count() + 1
    plan = Plan(outcome_id=outcome.id, version=version, summary=report.get("summary", ""), content=report)
    s.add(plan)
    s.flush()
    pending_events.append(record(outcome.workspace_id, "plan_proposed",
                                 f"Plan v{version}: {len(report.get('tasks', []))} task(s), worked out by {by}",
                                 outcome_id=outcome.id, level="success", actor=user.id,
                                 data={"plan_id": plan.id, **attribution}, session=s, publish=False))
    approve_plan(plan.id, approved_by=user.id, session=s, pending_events=pending_events, by=by,
                 attribution=attribution)
    return plan
