"""Delivery measurements from workspace-scoped, durable history.

Missing legacy evidence is unknown, not a zero or a pass. Attempt arrays are in
append order: the mutable retry counter is deliberately not used to order them.
"""
from __future__ import annotations

import math
from collections import defaultdict
from datetime import datetime, timezone
from statistics import median

from sqlalchemy.orm import Session

from .models import Artifact, Decision, Event, Message, Outcome, Plan, Repository, Run, Task, User

AUTOMATED_ACTORS = {None, "", "factory", "policy", "system", "scheduler", "supervisor"}


# Factory agents that post guidance as a user message, so agents receive it: the post is theirs, not a person's.
AGENT_SOURCES = {"lead", "director"}


def human_message(m) -> bool:
    """A person's post. The Lead and the Director post guidance as user messages, but those are the Factory's."""
    return m.author_kind == "user" and (m.attribution or {}).get("source") not in AGENT_SOURCES


APPROVALS = {"approve_plan", "approve_merge", "approve_release"}
# Neither failures nor passes: "infra" (the model provider or the sandbox failed; the same attempt is redone) and
# "checks_preexisting" (the configured checks fail on the base commit too; the attempt goes on to verification).
FAILURES = {"builder_failed", "checks_failed", "verification_failed", "policy"}


def benchmark(row) -> bool:
    """The dedicated source marker, never a title/name heuristic."""
    return (getattr(row, "benchmark_trial_id", None) is not None
            or getattr(row, "source", None) == "benchmark"
            or (getattr(row, "evidence", None) or {}).get("source") == "benchmark"
            or (getattr(row, "result", None) or {}).get("source") == "benchmark")


def ratio(definition, numerator, denominator, unknown=0, excluded=0):
    return {"definition": definition, "value": numerator / denominator if denominator else None,
            "numerator": numerator, "denominator": denominator,
            "unknown_count": unknown, "excluded_count": excluded}


def approval(d):
    return (d.kind in APPROVALS and d.status == "resolved" and d.resolution == "approve"
            and not (d.comment or "").strip())


def _failed_check(e):
    return (e.kind == "check_result" and
            (e.data.get("ok") is False or e.data.get("exit_code", 0) != 0))


def first_attempt_result(task, events, configured_checks):
    """True/False = conclusive first pass/failure; None = unknown; 'excluded' = waiting only."""
    attempts = (task.evidence or {}).get("attempts", [])
    conclusive = [a for a in attempts if a.get("result") in FAILURES | {"verified"}]
    # Check failures the base commit shares are not the candidate's failures.
    preexisting = {(a.get("n"), c.get("command")) for a in attempts if a.get("result") == "checks_preexisting"
                   for c in a.get("checks") or [] if c.get("ok") is False}
    # A failed check is conclusive even if a later legacy 'verified' entry has
    # insufficient detail. Inspect the journal before returning unknown.
    first_verified = next((e for e in events if e.kind == "verified"), None)
    prior_failure = any((e.kind in {"verification_failed", "candidate_rejected"}
                         or (_failed_check(e) and ((e.data or {}).get("attempt"), (e.data or {}).get("command"))
                             not in preexisting))
                        and (first_verified is None or e.id < first_verified.id) for e in events)
    if prior_failure:
        return False
    if not conclusive:
        if any(e.kind == "verified" for e in events):
            return None
        # Stage/lease/session events alone do not establish a completed build.
        # Delivered legacy tasks, however, have missing rather than waiting evidence.
        return None if task.pr_number or task.merge_sha or task.status == "completed" else "excluded"
    first = conclusive[0]
    if first.get("result") in FAILURES or first.get("n", 1) != 1:
        return False
    checks, verdict = first.get("checks"), first.get("verdict") or {}
    criteria = verdict.get("criteria")
    # A pre-existing failure is not the candidate's, but the configured checks did not pass either: unknown below.
    if checks is not None and any(c.get("ok") is False and not c.get("preexisting") for c in checks):
        return False
    if verdict.get("verdict") == "fail" or (criteria and any(c.get("status") != "pass" for c in criteria)):
        return False
    if checks is None or criteria is None or verdict.get("verdict") != "pass":
        return None
    passed_commands = {c.get("command") for c in checks if c.get("ok") is True}
    required_checks = first.get("configured_checks", configured_checks)
    if required_checks is None or not set(required_checks).issubset(passed_commands):
        return None
    if any(c.get("ok") is not True for c in checks):
        return None
    passed_criteria = {c.get("criterion") for c in criteria if c.get("status") == "pass"}
    if not set(task.acceptance or []).issubset(passed_criteria):
        return None
    return True


def _timestamp(value):
    """Normalize JSON timestamps and DB datetimes; malformed legacy dates are unknown."""
    try:
        at = datetime.fromisoformat(value) if isinstance(value, str) else value
        if not isinstance(at, datetime):
            return None
        return at.replace(tzinfo=timezone.utc) if at.tzinfo is None else at.astimezone(timezone.utc)
    except ValueError:
        return None


def _false_accept(task, journal):
    ev = task.evidence or {}
    publication = next((e for e in journal if e.kind == "published"), None)
    # evidence.published is mutable: a repaired candidate may be published again.
    # Anchor failure facts to the first durable publication, never the latest
    # candidate's timestamp, so repairs cannot erase an earlier false accept.
    published_at = (_timestamp(publication.created_at) if publication is not None
                    else _timestamp((ev.get("published") or {}).get("at")))
    failure = unknown = False
    for e in journal:
        if not (e.kind in {"ci_failed", "smoke_failed", "reverted"}
                or (e.kind == "ci_status" and (e.data.get("state") == "failure" or e.message == "CI failure"))
                or (e.kind == "smoke_result" and (e.data.get("ok") is False or e.data.get("exit_code", 0) != 0))):
            continue
        if publication is not None:
            failure |= e.id > publication.id
        elif published_at is not None:
            failure |= _timestamp(e.created_at) > published_at
        else:
            unknown = True
    facts = list(ev.get("failure_facts") or [])
    facts += [ev[k] for k in ("ci", "smoke") if (ev.get(k) or {}).get("state") in {"failure", "failed"}]
    if ev.get("revert"):
        facts.append(ev["revert"])
    elif task.status == "reverted":
        facts.append({"at": task.finished_at})
    for fact in facts:
        at = _timestamp(fact.get("at")) if isinstance(fact, dict) else None
        if at is None or published_at is None:
            unknown = True
        else:
            failure |= at > published_at
    return failure, unknown and not failure


def _size(task, artifacts, runs):
    verified = [a for a in (task.evidence or {}).get("attempts", []) if a.get("result") == "verified"]
    if not verified:
        return None
    final = verified[-1]
    diffs = [a for a in artifacts if a.kind == "diff"]
    if final.get("diff_artifact_id"):
        diffs = [a for a in diffs if a.id == final["diff_artifact_id"]]
    else:
        # Legacy artifacts link to builder runs. Select the last verified
        # candidate's run, not the latest (possibly rejected) diff.
        builders = [r for r in runs if r.role == "builder" and r.attempt == final.get("n")]
        if len(builders) != 1:
            return None
        diffs = [a for a in diffs if a.run_id == builders[0].id]
    if len(diffs) != 1:
        return None
    data = diffs[0].data or {}
    values = [data.get("additions"), data.get("deletions")]
    if any(not isinstance(v, int) or isinstance(v, bool) or v < 0 for v in values):
        return None
    lines = sum(values)
    return "small" if lines <= 100 else "medium" if lines <= 400 else "large"


def autonomy(outcomes: dict, tasks: dict, events, messages, decisions, system_users: set, plans: dict) -> dict[str, str]:
    """Each completed outcome's delivery, from its durable history: "unattended" (no person took part), "assisted"
    or "unverified" (the evidence can't tell). Benchmarks are the caller's to leave out of ``outcomes``/``tasks``.
    Factory agents are not people: the Lead and the Director act as system users with their own attribution."""
    def human(actor):
        return actor not in AUTOMATED_ACTORS and actor not in system_users

    # Resolve direct and task-only links once. A row linked both ways belongs
    # only once to each outcome, preserving the admission-window definition
    # without rescanning the entire workspace for every delivery.
    def index_history(history):
        grouped = defaultdict(list)
        for row in history:
            outcome_ids = {row.outcome_id}
            if row.task_id in tasks:
                outcome_ids.add(tasks[row.task_id].outcome_id)
            for outcome_id in outcome_ids:
                if outcome_id in outcomes:
                    grouped[outcome_id].append(row)
        return grouped

    outcome_events = index_history(events)
    outcome_messages = index_history(messages)
    # Reading or dismissing a Supervisor notification is not intervention in
    # delivery. Exempt both the decision row and its human-authored resolution
    # event, including task-only links and decisions outside the time window.
    supervisor_decisions = {d.id for d in decisions if d.kind == "supervisor"}
    outcome_decisions = index_history([d for d in decisions if d.kind != "supervisor"])
    outcome_tasks = defaultdict(list)
    for t in tasks.values():
        outcome_tasks[t.outcome_id].append(t)

    def within(at, outcome):
        at = _timestamp(at)
        return at is not None and _timestamp(outcome.created_at) <= at <= _timestamp(outcome.completed_at)

    classes = {}
    for o in outcomes.values():
        if o.status != "completed":
            continue
        if o.completed_at is None:
            classes[o.id] = "unverified"
            continue
        ds = [d for d in outcome_decisions[o.id] if human(d.resolved_by) and within(d.resolved_at, o)]
        approvals = {d.id for d in ds if approval(d)}
        plan_approvers = {d.resolved_by for d in ds if d.id in approvals and d.kind == "approve_plan"}
        assisted = any(not approval(d) for d in ds)
        assisted |= any(human_message(m) and within(m.created_at, o) for m in outcome_messages[o.id])
        unknown_external_merge = False
        for t in outcome_tasks[o.id]:
            merge = (t.evidence or {}).get("merge") or {}
            if merge.get("outside") is True:
                # TaskRunner records the observation as factory, not as the
                # GitHub user who merged the waiting PR. This is assistance,
                # not an exempt policy approval, regardless of journal actor.
                if _timestamp(merge.get("at")) is None:
                    unknown_external_merge = True
                elif within(merge["at"], o):
                    assisted = True
        for e in outcome_events[o.id]:
            if not within(e.created_at, o):
                continue
            # Reconciliation also observes external merges as factory events.
            if e.kind == "merged_outside":
                assisted = True
                continue
            if not human(e.actor):
                continue
            # Admission is the request itself, not assistance during execution.
            if e.kind == "outcome_created" and e.outcome_id == o.id and not e.task_id:
                continue
            exempt = False
            if e.kind == "decision_resolved":
                exempt = e.data.get("decision_id") in approvals | supervisor_decisions
            elif e.kind == "plan_approved":
                # Production emits only plan_id here, before decision_resolved.
                plan = plans.get(e.data.get("plan_id"))
                exempt = bool(plan and plan.outcome_id == o.id and e.actor in plan_approvers)
            if not exempt:
                assisted = True
        classes[o.id] = ("assisted" if assisted else "unverified" if unknown_external_merge else "unattended")
    return classes


def workspace_metrics(db: Session, workspace_id: str) -> dict:
    def rows(model):
        return db.query(model).filter(model.workspace_id == workspace_id).all()

    all_outcomes, all_tasks, all_runs = rows(Outcome), rows(Task), rows(Run)
    outcomes = {o.id: o for o in all_outcomes if not benchmark(o)}
    tasks = {t.id: t for t in all_tasks if t.outcome_id in outcomes and not benchmark(t)}
    runs = [r for r in all_runs if r.outcome_id in outcomes and not benchmark(r)
            and (not r.task_id or r.task_id in tasks)]
    events, messages, decisions = rows(Event), rows(Message), rows(Decision)
    system_users = {u.id for u in db.query(User).filter(User.is_system.is_(True))}
    plans = {p.id: p for p in db.query(Plan).join(Outcome).filter(Outcome.workspace_id == workspace_id)}
    classes = autonomy(outcomes, tasks, events, messages, decisions, system_users, plans)
    completed = list(classes)
    autonomous = sum(c == "unattended" for c in classes.values())
    unknown_autonomy = sum(c == "unverified" for c in classes.values())

    by_events, by_runs, by_artifacts = defaultdict(list), defaultdict(list), defaultdict(list)
    for e in sorted(events, key=lambda e: e.id):
        by_events[e.task_id].append(e)
    for r in runs:
        by_runs[r.task_id].append(r)
    for a in rows(Artifact):
        by_artifacts[a.task_id].append(a)
    repos = {r.id: r for r in rows(Repository)}
    first_pass = first_total = first_unknown = first_excluded = 0
    false_accepts = published_total = false_unknown = false_excluded = 0
    costs = {size: [] for size in ("small", "medium", "large")}
    unknown_size = unknown_cost = 0
    for t in tasks.values():
        ev, journal = t.evidence or {}, by_events[t.id]
        repo = repos.get(t.repo_id)
        result = first_attempt_result(t, journal, (repo.config or {}).get("checks", []) if repo else None)
        if result == "excluded":
            first_excluded += 1
        elif result is None:
            first_unknown += 1
        else:
            first_total += 1
            first_pass += result

        verified = any(a.get("result") == "verified" for a in ev.get("attempts", []))
        verified |= any(e.kind == "verified" for e in journal)
        published = bool(t.pr_number or ev.get("published") or any(e.kind == "published" for e in journal))
        if published and verified:
            failure, unknown = _false_accept(t, journal)
            # An unordered failure is not evidence of a clean publication.
            # Keep unknown tasks out of the measured population, just as for
            # tasks whose verifier evidence is missing.
            published_total += not unknown
            false_accepts += failure
            false_unknown += unknown
        elif published:
            false_unknown += 1
        else:
            false_excluded += 1
        if t.status != "completed":
            continue
        size = _size(t, by_artifacts[t.id], by_runs[t.id])
        execution = [r for r in by_runs[t.id] if r.role in {"builder", "verifier"}]
        known = bool(execution) and all(r.cost_known and r.cost_usd is not None
                                       and math.isfinite(r.cost_usd) and r.cost_usd >= 0 for r in execution)
        unknown_size += size is None
        unknown_cost += not known
        if size and known:
            costs[size].append(sum(r.cost_usd for r in execution))

    benchmark_tasks = len(all_tasks) - len(tasks)
    sizes = {size: {"value": median(values) if values else None, "sample_count": len(values)}
             for size, values in costs.items()}
    sample_count = sum(len(v) for v in costs.values())
    delivered = sum(t.status == "completed" for t in tasks.values())
    return {
        "autonomy": ratio({"population": "completed outcomes", "admission": "Outcome.created_at",
                           "window": "admission through completed_at inclusive; outcome_created admission event excluded",
                           "success": "no human messages, attributed non-policy decisions, human-action events or external merges; only affirmative approve_plan/approve_merge/approve_release without guidance are exempt; automated retries/restarts are allowed",
                           "unknown": "missing completion time or external merge evidence without a timestamp and no other recorded assistance"},
                          autonomous, len(completed) - unknown_autonomy, unknown_autonomy,
                          len(all_outcomes) - len(completed)),
        "first_attempt_pass_rate": ratio({"population": "tasks with a conclusive build/check/verification attempt",
                                          "success": "earliest attempt passed configured checks and every acceptance criterion on attempt 1; retry counter resets do not erase history"},
                                         first_pass, first_total, first_unknown, first_excluded + benchmark_tasks),
        "verifier_false_accept_rate": ratio({"population": "unique published verifier-passed tasks",
                                            "failure": "subsequent recorded PR CI failure, post-merge smoke failure or revert; each task counted once, including repaired failures",
                                            "unknown": "published tasks lacking verifier evidence, or failures whose order relative to publication is unknown"},
                                           false_accepts, published_total, false_unknown, false_excluded + benchmark_tasks),
        "median_delivered_task_cost": {"definition": {"population": "completed tasks with known size and execution cost",
                                                     "cost": "USD sum of all task-linked builder/verifier runs, excluding planner/judge",
                                                     "size": "final verified diff artifact additions + deletions",
                                                     "boundaries": {"small": [0, 100], "medium": [101, 400], "large": [401, None]}},
                                       "value": sizes, "sample_count": sample_count,
                                       "unknown_count": delivered - sample_count,
                                       "unknown_size_count": unknown_size, "unknown_cost_count": unknown_cost,
                                       "excluded_count": len(all_tasks) - delivered},
    }
