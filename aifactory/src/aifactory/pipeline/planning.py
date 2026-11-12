"""Planning: turn an outcome into a reviewed, approved, dependency-ordered set of tasks.

With automatic plan approval the plan reviewer (Opus 5.5 unless the workspace picks another model) checks each
plan against the request and the code. It approves the plan, sends it back to the planner with findings (at most
``PLAN_REVIEW_ROUNDS`` times), or hands it to a person. With "approve first" a person reviews every plan.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import timedelta
from typing import Any

from sqlalchemy.orm import Session

from ..db import session_scope, utcnow
from ..events import notify, record
from ..failures import failure_cause
from ..github import resolve_credential as resolve_token
from ..gitops import checkout
from ..models import Decision, Event, Outcome, Plan, Repository, Run, Task
from ..onboarding_policy import lock_workspace
from ..prompts import PLAN_REVIEWER, PLANNER, describe_review, plan_prompt, plan_review_prompt
from ..sandbox import LocalSandbox
from . import delivery
from .holds import open_provider_hold
from .runtime import (SessionSpec, carried_cause, guidance_history, open_decision, outcome_committed, precedents,
                      run_agent, set_outcome, merged_settings, workspace_settings)

log = logging.getLogger(__name__)

PLAN_REVIEW_ROUNDS = 2  # revisions the reviewer may ask for before a person decides
ACTIVE_OUTCOME = ("planning", "running", "attention", "blocked", "needs_input", "awaiting_approval")
OTHER_OUTCOMES = 20
OTHER_TASKS = 8
CANCEL_EFFECT = {"effect": "Cancels the outcome and stops all of its work."}  # shown under a decision's option
# The outcome's own events that say where its planning stands with infrastructure retries (_infra_retries).
_RETRY_STATE = ("infra_retry", "plan_proposed", "provider_hold", "decision_requested")


def other_work(workspace_id: str, outcome_id: str) -> list[str]:
    """Other outcomes in progress or completed in the last week, with their tasks, so a plan neither repeats
    nor collides with them. Bounded: titles only, at most ``OTHER_OUTCOMES`` outcomes."""
    since = utcnow() - timedelta(days=7)
    lines: list[str] = []
    with session_scope() as s:
        mine = Outcome.workspace_id == workspace_id, Outcome.id != outcome_id
        active = (s.query(Outcome).filter(*mine, Outcome.status.in_(ACTIVE_OUTCOME))
                  .order_by(Outcome.number.desc()).limit(OTHER_OUTCOMES).all())
        done = (s.query(Outcome).filter(*mine, Outcome.status == "completed", Outcome.completed_at >= since)
                .order_by(Outcome.number.desc()).limit(max(0, OTHER_OUTCOMES - len(active))).all())
        repos = dict(s.query(Repository.id, Repository.full_name).filter(Repository.workspace_id == workspace_id))
        for outcome in active + done:
            lines.append(f"- #{outcome.number} {outcome.title[:150]} ({outcome.status})")
            if outcome.status == "completed":
                continue
            tasks = (s.query(Task).filter(Task.outcome_id == outcome.id,
                                          Task.status.notin_(("cancelled", "abandoned")))
                     .order_by(Task.key).limit(OTHER_TASKS).all())
            lines += [f"  - {t.key} {t.title[:150]} [{repos.get(t.repo_id, '?')}] ({t.status})" for t in tasks]
    return lines


def _check_repos(report: dict[str, Any], repos: list[Repository]) -> list[str]:
    """Names of repositories the plan uses that the outcome did not select. A single-repository outcome owns
    every task, so its tasks are corrected rather than rejected."""
    by_name = {r.full_name.lower() for r in repos}
    problems = [t["repo"] for t in report.get("tasks", []) if t["repo"].lower() not in by_name]
    if problems and len(repos) == 1:
        for task in report["tasks"]:
            task["repo"] = repos[0].full_name
        return []
    return problems


def _over_budget(outcome_id: str, settings: dict[str, Any]) -> str:
    """Why planning must not start another session, or "" while the outcome is within its budget. Spending whose
    cost is known is a lower bound, so reaching the budget with it alone is conclusive."""
    budget = settings["budgets"].get("outcome_usd")
    if not budget:
        return ""
    spent, _ = outcome_committed(outcome_id)  # including what a running Lead has reserved
    return f"The outcome reached its ${budget:.0f} budget (${spent:.2f} spent)" if spent >= budget else ""


def _continued_past_budget(outcome_id: str) -> bool:
    """A person chose to continue planning past the budget since the last planning session started. Budget
    decisions are never delegated to judges, so only a person grants this, and each grant covers one planning run."""
    with session_scope() as s:
        decided = (s.query(Decision.resolved_at)
                   .filter(Decision.outcome_id == outcome_id, Decision.task_id.is_(None), Decision.kind == "budget",
                           Decision.status == "resolved", Decision.resolution == "retry")
                   .order_by(Decision.resolved_at.desc()).first())
        if decided is None or decided[0] is None:
            return False
        last = (s.query(Run.started_at).filter(Run.outcome_id == outcome_id, Run.task_id.is_(None),
                                               Run.role.in_(("planner", "plan_reviewer")))
                .order_by(Run.started_at.desc()).first())
        return last is None or decided[0] > last[0]


def _save_plan(workspace_id: str, outcome_id: str, version: int, report: dict[str, Any], run: Any,
               review: dict[str, Any] | None) -> str:
    with session_scope() as s:
        for old in s.query(Plan).filter(Plan.outcome_id == outcome_id, Plan.status == "proposed"):
            old.status = "superseded"
        content = {**report, "review": review} if review else report
        plan = Plan(outcome_id=outcome_id, version=version, summary=report.get("summary", ""), content=content,
                    run_id=run.id if run else None)
        s.add(plan)
        s.flush()
        plan_id = plan.id
    record(workspace_id, "plan_proposed", f"Plan v{version}: {len(report.get('tasks', []))} task(s)",
           outcome_id=outcome_id, level="success", data={"plan_id": plan_id})
    notify(workspace_id, "plan", plan_id)
    return plan_id


def _infra_retries(outcome_id: str) -> tuple[int, float]:
    """Planning's infrastructure retries in a row so far, and the seconds left before the next one may start. Both
    are read from the outcome's last ``infra_retry`` event (an outcome has no evidence field as a task has), so a
    restart during a backoff continues the count and the wait. A plan, a decision or the provider hold ends the row."""
    with session_scope() as s:
        last = (s.query(Event).filter(Event.outcome_id == outcome_id, Event.task_id.is_(None),
                                      Event.kind.in_(_RETRY_STATE)).order_by(Event.id.desc()).first())
        if last is None or last.kind != "infra_retry":
            return 0, 0.0
        data = last.data or {}
        due = last.created_at + timedelta(seconds=float(data.get("delay") or 0))
        return int(data.get("retries") or 1), max((due - utcnow()).total_seconds(), 0.0)


async def plan_outcome(outcome_id: str, cancelled: asyncio.Event) -> None:
    # A retry after an infrastructure failure (below) waits out its backoff here. The outcome stays in planning and
    # keeps its slot; cancelling it ends the wait at once.
    infra_retries, wait = _infra_retries(outcome_id)
    if wait > 0:
        try:
            await asyncio.wait_for(cancelled.wait(), wait)
        except asyncio.TimeoutError:
            pass
        if cancelled.is_set():
            return
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
        open_decision(workspace_id, "unblock", f"#{snapshot.number} has no repositories selected",
                      "Planning cannot start: no repositories are selected for this outcome. Select at least one "
                      "on the outcome, then retry planning.", outcome_id=outcome_id,
                      options=[{"value": "retry", "label": "Retry planning"},
                               {"value": "cancel", "label": "Cancel outcome"}])
        return
    settings = workspace_settings(workspace_id)
    # Replans (a person's revision or answer, a judge's answer, a retry) keep the outcome's earlier spending, so the
    # budget is checked before the first session too. A person who continues anyway allows one more planning run.
    continued = _continued_past_budget(outcome_id)

    def over_budget() -> str:
        return "" if continued else _over_budget(outcome_id, settings)

    if over := over_budget():
        set_outcome(outcome_id, status="blocked", blocked_reason=f"{over}; planning stopped.")
        open_decision(workspace_id, "budget", f"Outcome #{snapshot.number} reached its budget",
                      f"{over}, so planning stopped before starting another session. Raise the outcome budget in "
                      "Settings and continue, continue anyway for one more planning run, or cancel the outcome.",
                      outcome_id=outcome_id,
                      options=[{"value": "retry", "label": "Continue anyway",
                                "effect": "Allows one more planning run past the budget."},
                               {"value": "cancel", "label": "Cancel outcome", **CANCEL_EFFECT}])
        return
    harness_name = snapshot.harness or settings["harness"].get("planner") or "native"
    # Automatic approval means the plan reviewer approves; with "approve first" a person reviews instead.
    reviewed = settings["autonomy"].get("plan") == "auto"
    record(workspace_id, "planning_started", f"Planning outcome #{snapshot.number}", outcome_id=outcome_id)
    sandbox = LocalSandbox(f"plan-{outcome_id[:12]}-{version}").create()
    result = review = last = None
    error, problems, stopped, setup_failed = "", [], "", False
    cause: str | None = None  # what the failed session said it was (runtime.carried_cause); None: read the wording
    who = "the planner"
    try:
        token = resolve_token(workspace_id)
        sandbox.work.mkdir(parents=True, exist_ok=True)
        for repo in repos:
            await checkout(sandbox, repo.full_name, token, repo.default_branch, f"factory/plan-{outcome_id[:8]}",
                           subdir=repo.full_name.replace("/", "__"), adopt=False)
        sandbox.adopt_work()
        decisions = precedents(workspace_id, repo_ids=[r.id for r in repos])
        work = other_work(workspace_id, outcome_id)
        repositories = {r.full_name: r.config or {} for r in repos}
        revision = None
        for revisions in range(PLAN_REVIEW_ROUNDS + 1):
            if revisions and (stopped := over_budget()):
                break  # every round starts two sessions: a revision must not overrun the outcome's budget
            prompt = plan_prompt(snapshot, repos, decisions, guidance_history(outcome_id), work, revision)
            run, result, error = await run_agent(SessionSpec(
                workspace_id=workspace_id, outcome_id=outcome_id, task_id=None, role="planner", harness=harness_name,
                prompt=prompt, system=PLANNER, sandbox=sandbox, effective_settings=settings,
                repositories=repositories), cancelled)
            if result is None:
                cause = carried_cause(run)
                break
            report = result.report
            problems = _check_repos(report, repos)
            review = None
            if reviewed and not problems and not report.get("questions"):
                if over := over_budget():
                    review = {"verdict": "error", "summary": f"{over}, so the plan was not reviewed.", "findings": []}
                else:
                    review, refused = await _review(snapshot, repos, decisions, work, report, harness_name, sandbox,
                                                    settings, repositories, cancelled)
                    if refused and (refused[1] == "provider" or infra_retries < delivery.INFRA_RETRIES):
                        # The provider failed the review, not the plan. Nobody is asked to approve a plan that could
                        # not be reviewed: it is not saved, and planning starts again once the provider answers.
                        result, (error, cause), who = None, refused, "the plan reviewer"
                        break
            plan_id = _save_plan(workspace_id, outcome_id, version, report, run, review)
            last = (plan_id, report, review)
            if review:
                _record_review(workspace_id, outcome_id, version, review)
            version += 1
            if not review or review["verdict"] != "revise" or revisions == PLAN_REVIEW_ROUNDS:
                break
            revision = {"plan": report, "review": review}
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        log.exception("planning setup failed")
        # A provider that refuses before a session starts (no valid credential) says so itself and is handled as a
        # refusal inside a session. Anything else raised here is this outcome's own setup: a checkout, the sandbox.
        cause = "provider" if failure_cause(exc) == "provider" else None
        result, error, setup_failed = None, str(exc), cause is None
    finally:
        await sandbox.close()
    if cancelled.is_set():
        return

    if result is None:
        # As for a task (delivery.infrastructure_problem): the provider or the sandbox failed, not the plan. Every
        # planning session is read this way: the first plan, a revision and the plan review.
        cause = delivery.infrastructure_problem(error, cause)
        if cause == "provider" and not setup_failed:
            # The provider refused this session as it would every model session of the workspace: the outcome stays in
            # planning under the workspace's one hold, and is planned again when a person resumes it. An access
            # problem while setting up (a repository, the GitHub credential) is this outcome's own and blocks it.
            open_provider_hold(workspace_id, error, f"planning outcome #{snapshot.number}, {who}")
            record(workspace_id, "provider_hold", f"The model provider refused {who} ({error[:200]}). Planning "
                   "waits until the provider hold in the inbox is resumed", outcome_id=outcome_id, level="warn")
            return
        if cause == "transient" and infra_retries < delivery.INFRA_RETRIES:
            delay = delivery.INFRA_BACKOFF_SECONDS[min(infra_retries, len(delivery.INFRA_BACKOFF_SECONDS) - 1)]
            # This event is the durable count and retry time: the next call waits out the backoff (_infra_retries).
            record(workspace_id, "infra_retry", f"Planning hit an infrastructure problem ({error[:200]}); retrying in "
                   f"{delay // 60} min ({infra_retries + 1} of {delivery.INFRA_RETRIES})", outcome_id=outcome_id,
                   level="warn", data={"delay": delay, "retries": infra_retries + 1})
            return await plan_outcome(outcome_id, cancelled)
    if last is None:
        failed = "The planner could not produce a plan."
        if cause == "transient":
            failed = (f"Planning failed {infra_retries + 1} times in a row for reasons outside the plan (the model "
                      "provider or the sandbox was unavailable). Last error:")
        set_outcome(outcome_id, status="blocked", blocked_reason=f"Planning failed: {error}")
        open_decision(workspace_id, "unblock", f"Planning failed for #{snapshot.number}", f"{failed}\n\n{error}",
                      outcome_id=outcome_id, options=[{"value": "retry", "label": "Retry planning"},
                                                      {"value": "cancel", "label": "Cancel outcome"}])
        return
    plan_id, report, review = last
    note = f"{stopped} before the planner could revise this plan." if stopped else ""
    if result is None:
        # A revision failed: the last reviewed plan still stands, and a person decides on it.
        note = f"The planner could not revise this plan after the review: {error[:1000]}"
    elif problems:
        set_outcome(outcome_id, status="blocked", blocked_reason=f"Plan references unknown repositories: {problems}")
        open_decision(workspace_id, "unblock", f"The plan for #{snapshot.number} names repositories it may not use",
                      f"The plan references repositories that are not selected for this outcome: {problems}\n\n"
                      "Select them on the outcome, or say in a note which repositories to use, then retry planning.",
                      outcome_id=outcome_id,
                      options=[{"value": "retry", "label": "Retry planning"},
                               {"value": "cancel", "label": "Cancel outcome"}])
        return
    questions = report.get("questions") or []
    if questions and not note:
        body = "\n\n".join(f"**{q['question']}**\n{q.get('why', '')}" +
                           (("\nOptions: " + ", ".join(q["options"])) if q.get("options") else "") for q in questions)
        set_outcome(outcome_id, status="needs_input")
        options = [{"value": "answer", "label": "Answer and re-plan",
                    "effect": "Sends your answer to the planner, which plans the outcome again."}]
        if report.get("tasks"):
            body += "\n\nThe planner also proposed a plan with its own assumptions:\n" + report.get("summary", "")
            options.insert(0, {"value": "approve", "label": "Approve plan as proposed",
                               "effect": "Approves the plan with the planner's assumptions; building starts."})
        options.append({"value": "cancel", "label": "Cancel", **CANCEL_EFFECT})
        open_decision(workspace_id, "question", f"Planner needs a decision for #{snapshot.number}", body,
                      outcome_id=outcome_id, options=options)
        return
    if not note and review and review["verdict"] == "approve" and approve_plan(
            plan_id, approved_by=None, reviewer=review.get("model") or "the plan reviewer"):
        return
    body = report.get("summary", "")
    if review and not note:
        reason = {"revise": f"it still had concerns after {PLAN_REVIEW_ROUNDS} revisions",
                  "escalate": "it needs a person to decide", "error": "the review could not finish"}.get(
                      review["verdict"], "")
        if reason:
            body += f"\n\n**The plan reviewer did not approve this plan: {reason}.**\n" + "\n".join(
                describe_review(review))
    if note:
        body += f"\n\n**{note}**"
        if review:
            body += "\n\nThe review of this plan:\n" + "\n".join(describe_review(review))
    set_outcome(outcome_id, status="awaiting_approval")
    open_decision(workspace_id, "approve_plan", f"Approve plan for #{snapshot.number}: {snapshot.title}", body,
                  outcome_id=outcome_id,
                  options=[{"value": "approve", "label": "Approve", "effect": "Approves the plan; building starts."},
                           {"value": "replan", "label": "Revise",
                            "effect": "Sends your comment to the planner, which proposes a new plan."},
                           {"value": "cancel", "label": "Cancel", **CANCEL_EFFECT}])


async def _review(outcome: Outcome, repos: list[Repository], decisions: list[Any], work: list[str],
                  report: dict[str, Any], harness_name: str, sandbox: LocalSandbox, settings: dict[str, Any],
                  repositories: dict[str, Any], cancelled: asyncio.Event
                  ) -> tuple[dict[str, Any], tuple[str, str] | None]:
    """Review one plan in the planner's checkout. A review that cannot finish never approves. Also returns the error
    and its cause ("provider" or "transient") when the provider or the sandbox failed the review session, as
    delivery.infrastructure_problem reads it; None when the review finished or failed on its own."""
    prompt = plan_review_prompt(outcome, repos, decisions, guidance_history(outcome.id), work, report)
    try:
        run, result, error = await run_agent(SessionSpec(
            workspace_id=outcome.workspace_id, outcome_id=outcome.id, task_id=None, role="plan_reviewer",
            harness=harness_name, prompt=prompt, system=PLAN_REVIEWER, sandbox=sandbox, effective_settings=settings,
            repositories=repositories), cancelled)
    except Exception as exc:  # setup failed before a session started: keep the plan, unless the provider refused
        log.exception("plan review could not start")
        review = {"verdict": "error", "summary": f"The plan review could not start: {str(exc)[:1000]}", "findings": []}
        return review, ((str(exc), "provider") if failure_cause(exc) == "provider" else None)
    if result is None:
        cause = delivery.infrastructure_problem(error, carried_cause(run))
        review = {"verdict": "error", "summary": f"The plan review failed: {error[:1000]}", "findings": [],
                  "run_id": run.id, "model": run.model}
        return review, ((error, cause) if cause else None)
    return {**result.report, "run_id": run.id, "model": run.model}, None


def _record_review(workspace_id: str, outcome_id: str, version: int, review: dict[str, Any]) -> None:
    verdict = review["verdict"]
    text = {"approve": "approved", "revise": "sent back to the planner", "escalate": "referred to a person",
            "error": "not reviewed"}.get(verdict, verdict)
    record(workspace_id, "plan_reviewed", f"Plan v{version} {text} by the plan reviewer: "
           f"{str(review.get('summary', ''))[:300]}", outcome_id=outcome_id,
           level="success" if verdict == "approve" else "warn",
           data={"verdict": verdict, "run_id": review.get("run_id"),
                 "findings": len(review.get("findings") or [])})


def approve_plan(plan_id: str, approved_by: str | None, reviewer: str | None = None, *,
                 session: Session | None = None, pending_events: list[dict] | None = None, by: str | None = None,
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
    who = by or ("a team member" if approved_by else (f"the plan reviewer ({reviewer})" if reviewer
                                                     else "policy (automatic)"))
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
