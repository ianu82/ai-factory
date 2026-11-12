"""The watch: stuck work and repeated failures, found every few minutes and shown to people in the app.

Every WATCH_EVERY the orchestrator calls run(), which looks in each workspace that isn't paused for:
- overdue: a decision that has waited on a person longer than the workspace's alert time (alerts.after_minutes);
- unasked: a task waiting on a person with no open decision, so there is nothing anyone can answer;
- stalled: an outcome where nothing is moving and nothing asks a person (its open tasks wait on dropped ones);
- quiet: an outcome with work marked active but no activity for QUIET, while the workspace runs nothing at all
  and isn't on a provider hold;
- repeated: the same failure on REPEAT_TASKS tasks or REPEAT_OUTCOMES outcomes within REPEAT_WINDOW.

Each condition is one Alert row while it lasts, keyed by what it is about: raised once and cleared when the
condition goes away, so a condition that lasts all day is one alert, not one per pass. Stuck work (unasked,
stalled, quiet) first wakes the outcome's Lead, which reads the record and tells people why in a note; it is
shown as an alert only once it has lasted GRACE, which also keeps a passing state from alerting anyone. Overdue
decisions and repeated failures are shown at once: the Lead had its turn when the decision opened, and a failure
across outcomes is no single Lead's.

Open alerts show at the top of the Inbox, where people can dismiss them; a dismissed alert stays dismissed while
its condition lasts. Slack and email are delivery options a workspace will be able to turn on. Neither exists
yet, so both are off and alerts reach people only in the app.
"""
from __future__ import annotations

import hashlib
import re
from datetime import datetime, timedelta
from typing import Any

from sqlalchemy import func, or_
from sqlalchemy.exc import IntegrityError

from ..db import session_scope, utcnow
from ..events import notify, record
from ..models import Alert, Decision, Event, Outcome, Run, Task, Workspace
from ..serializers import actionable
from .holds import held_workspaces
from .orchestrator import ACTIVE_TASK, HUMAN_TASK
from .planning import ACTIVE_OUTCOME
from .runtime import DEFAULT_SETTINGS, merged_settings

WATCH_EVERY = timedelta(minutes=5)
GRACE = timedelta(minutes=30)  # how long stuck work lasts, with its Lead woken, before people are alerted
QUIET = timedelta(hours=1)
# The scheduler opens a decision for a task left waiting on a person within minutes (scheduler's UNASKED_AFTER): the
# watch looks only at tasks that have waited longer, so it doesn't wake the Lead for what is about to be asked.
UNASKED_AFTER = timedelta(minutes=10)
REPEAT_WINDOW = timedelta(days=1)
REPEAT_TASKS, REPEAT_OUTCOMES = 3, 2
STUCK = ("unasked", "stalled", "quiet")
LIVE = ("watching", "open", "dismissed")
DECISIONS = {"approve_merge": "Merge approval", "approve_plan": "Plan approval", "approve_release": "Release approval",
             "question": "Question", "unblock": "Unblock decision", "budget": "Budget decision"}
WAITING = {"needs_input": "waiting for an answer", "awaiting_approval": "waiting for approval", "blocked": "blocked",
           "failed": "failed", "smoke_failed": "failed its post-merge check"}
DROPPED = ("abandoned", "cancelled")
# Not the work moving: the watch's own records, the Lead's posts, and session starts and ends (sessions are read
# from runs, where the Lead's are left out). The Lead's guidance is left out in _last_activity: it carries the Lead's
# run, and a person's guidance, which does count, carries none.
NOT_ACTIVITY = ("lead_watch", "alert_raised", "alert_dismissed", "lead_note", "lead_update", "run_started",
                "run_finished")
# A task out of attempts is the task's own failure, not the pipeline's; the failure classes of review item L2 will
# tell them apart properly.
OWN_FAILURE = re.compile(r"did not pass after", re.I)
# A session a restart ended didn't fail: the next start picks the work up again.
RESTARTED = re.compile(r"^\s*Interrupted by a restart", re.I)
# Unblock titles that name no cause: the decision's body carries it.
NO_CAUSE = re.compile(r"^(A task stopped unexpectedly|infrastructure problem)$", re.I)


def alert_settings(settings: dict[str, Any]) -> dict[str, Any]:
    default = DEFAULT_SETTINGS["alerts"]
    raw = settings.get("alerts") if isinstance(settings.get("alerts"), dict) else {}
    after = raw.get("after_minutes")
    return {"after_minutes": after if type(after) is int and 15 <= after <= 1440 else default["after_minutes"],
            "slack": False, "email": False}  # not available yet


def run(now: datetime | None = None) -> dict[str, int]:
    """One pass over every workspace that isn't paused. Returns how many alerts were raised, shown and cleared,
    and how many Leads were woken."""
    from .lead import settings_for

    now = now or utcnow()
    counts = {"raised": 0, "shown": 0, "cleared": 0, "woken": 0}
    with session_scope() as s:
        workspaces = [(w.id, merged_settings(w.settings)) for w in s.query(Workspace).filter(Workspace.paused.isnot(True))]
    for workspace_id, settings in workspaces:
        with session_scope() as s:
            found = find(s, workspace_id, settings, now)
            shown = _apply(s, workspace_id, found, settings_for(settings)["enabled"], now, counts)
        if shown:  # after the commit, so the Inbox refetches what was written
            notify(workspace_id, "alert", workspace_id)
    return counts


def find(s, workspace_id: str, settings: dict[str, Any], now: datetime) -> dict[str, dict[str, Any]]:
    """What is stuck or failing in a workspace now, by condition key."""
    after = timedelta(minutes=alert_settings(settings)["after_minutes"])
    outcomes = {o.id: o for o in s.query(Outcome).filter(Outcome.workspace_id == workspace_id,
                                                         Outcome.source != "review",
                                                         Outcome.status.in_(ACTIVE_OUTCOME))}
    decisions = s.query(Decision).filter(Decision.workspace_id == workspace_id, Decision.status == "open").all()
    tasks = s.query(Task).filter(Task.outcome_id.in_(list(outcomes))).all() if outcomes else []
    keys = {t.id: t.key for t in tasks}
    found: dict[str, dict[str, Any]] = {}

    def add(key: str, kind: str, title: str, body: str = "", since: datetime | None = None, **about: Any) -> None:
        found[key] = {"kind": kind, "title": title[:300], "body": body[:2000], "since": since or now,
                      "outcome_id": about.get("outcome_id"), "task_id": about.get("task_id"),
                      "decision_id": about.get("decision_id"), "data": about.get("data")}

    for d in decisions:
        if d.outcome_id and d.outcome_id not in outcomes:
            continue  # its outcome ended: nothing to chase
        if actionable(d) and now - handed_off(d) >= after:
            add(f"overdue:{d.id}", "overdue", f"{DECISIONS.get(d.kind, 'Decision')} has waited over {_span(after)}",
                d.title, handed_off(d), outcome_id=d.outcome_id, task_id=d.task_id, decision_id=d.id)

    asked_tasks = {d.task_id for d in decisions if d.task_id}  # any open decision, even one a judge is reviewing
    asked_outcomes = {d.outcome_id for d in decisions if d.outcome_id}
    asked_whole = {d.outcome_id for d in decisions if d.outcome_id and not d.task_id}  # e.g. an outcome's budget
    unasked: set[str] = set()
    for t in tasks:
        if t.status in HUMAN_TASK and t.id not in asked_tasks and t.outcome_id not in asked_whole \
                and now - (t.updated_at or now) >= UNASKED_AFTER:
            unasked.add(t.outcome_id)
            add(f"unasked:{t.id}", "unasked", f"{t.key} is {WAITING.get(t.status, t.status)} with no decision to answer",
                t.blocked_reason or t.title, outcome_id=t.outcome_id, task_id=t.id)

    by_outcome: dict[str, list[Task]] = {}
    for t in tasks:
        by_outcome.setdefault(t.outcome_id, []).append(t)
    # Agent sessions only: a Lead the watch woke is running because the work is quiet, not a sign it is moving.
    busy = s.query(Run.id).filter(Run.workspace_id == workspace_id, Run.status == "running",
                                  Run.role != "lead").first() is not None
    # A provider hold stops all work on purpose, and is itself a decision waiting on a person (an overdue alert once
    # it has waited): every outcome going quiet under it isn't news.
    held = workspace_id in held_workspaces(s)
    for o in outcomes.values():
        mine = by_outcome.get(o.id, [])
        if o.status == "attention" and o.id not in asked_outcomes and o.id not in unasked:
            dropped = {t.key for t in mine if t.status in DROPPED}
            waiting = [t.key for t in mine if t.status == "pending" and set(t.depends_on or []) & dropped]
            body = (f"{', '.join(waiting)} wait{'s' if len(waiting) == 1 else ''} on {', '.join(sorted(dropped))}, "
                    f"which {'was' if len(dropped) == 1 else 'were'} dropped." if waiting else o.title)
            add(f"stalled:{o.id}", "stalled", "Nothing is moving and nothing asks a person", body, outcome_id=o.id)
        active = [t for t in mine if t.status in ACTIVE_TASK]
        if o.status == "running" and active and not busy and not held:
            last = _last_activity(s, o.id)
            if last is not None and now - last >= QUIET:
                add(f"quiet:{o.id}", "quiet", f"No activity for over {_span(QUIET)}, though work is marked active",
                    ", ".join(f"{t.key} {t.status.replace('_', ' ')}" for t in active[:6]), last, outcome_id=o.id)

    for reason, seen in _failures(s, workspace_id, now).items():
        if len(seen["tasks"]) >= REPEAT_TASKS or len(seen["outcomes"]) >= REPEAT_OUTCOMES:
            n, m = len(seen["tasks"]), len(seen["outcomes"])
            where = f"{m} outcome{'s' if m != 1 else ''}"
            add("repeated:" + hashlib.sha256(reason.encode()).hexdigest()[:16], "repeated",
                f"The same failure on {f'{n} tasks across {where}' if n else where} in the last day", reason,
                seen["first"], data={"tasks": sorted(seen["tasks"])[:20],
                                                                   "outcomes": sorted(seen["outcomes"])[:20]})
    return found


def handed_off(d: Decision) -> datetime:
    """When a decision reached people: when it opened or, when a judge reviewed it first, when that review finished.
    Time with the judge isn't time a person kept it waiting."""
    finished = d.review.finished_at if d.review is not None else None
    return max(d.created_at, finished) if finished else d.created_at


def _last_activity(s, outcome_id: str) -> datetime | None:
    """When the work on an outcome last moved: an event other than NOT_ACTIVITY, or an agent session (not the
    Lead's) starting or ending."""
    event = s.query(func.max(Event.created_at)).filter(
        Event.outcome_id == outcome_id, Event.kind.notin_(NOT_ACTIVITY),
        or_(Event.kind != "guidance", Event.run_id.is_(None))).scalar()
    run = s.query(func.max(func.coalesce(Run.finished_at, Run.started_at))).filter(
        Run.outcome_id == outcome_id, Run.role != "lead").scalar()
    return max((at for at in (event, run) if at is not None), default=None)


def _failures(s, workspace_id: str, now: datetime) -> dict[str, dict[str, Any]]:
    """Failures in the window by reason: failed agent sessions, and unblock decisions other than a task running out
    of attempts."""
    since = now - REPEAT_WINDOW
    seen: dict[str, dict[str, Any]] = {}

    def add(text: str, task_id: str | None, outcome_id: str | None, at: datetime) -> None:
        reason = reason_of(text)
        if not reason:
            return
        entry = seen.setdefault(reason, {"tasks": set(), "outcomes": set(), "first": at})
        if task_id:
            entry["tasks"].add(task_id)
        if outcome_id:
            entry["outcomes"].add(outcome_id)
        entry["first"] = min(entry["first"], at)

    failed_at = func.coalesce(Run.finished_at, Run.started_at)  # a run fails when it ends; older rows lack an end
    for run in s.query(Run).filter(Run.workspace_id == workspace_id, Run.status == "failed", failed_at > since):
        if not RESTARTED.match(run.error or ""):
            add(run.error, run.task_id, run.outcome_id, run.finished_at or run.started_at)
    for d in s.query(Decision).filter(Decision.workspace_id == workspace_id, Decision.kind == "unblock",
                                      Decision.created_at > since):
        if OWN_FAILURE.search(d.title or "") or {"stranded", "unasked"} & set(d.metadata_json or {}):
            continue  # a task's own failure, or the scheduler asking about work nothing asked about: not a failure
        title = reason_of(d.title)
        add(f"{title}: {_cause(d.body)}" if NO_CAUSE.match(title) else title, d.task_id, d.outcome_id, d.created_at)
    return seen


def _cause(body: str | None) -> str:
    """The cause a decision with a cause-less title gives in its body: what follows "Last error:" (an infrastructure
    problem's body opens with the phase that failed, which differs for one outage), else its first line."""
    text = body or ""
    at = text.rfind("Last error:")
    return reason_of(text[at + len("Last error:"):] if at >= 0 else text)


def reason_of(text: str | None) -> str:
    """A failure's reason without what differs between occurrences: the task key, numbers, hashes and ids."""
    line = next((ln.strip() for ln in (text or "").splitlines() if ln.strip()), "")
    line = re.sub(r"^T\d+\b:?\s*", "", line)
    line = re.sub(r"\b(?=[0-9a-f-]*\d)[0-9a-f-]{7,}\b", "…", line, flags=re.I)
    line = re.sub(r"\d+", "#", line)
    return re.sub(r"\s+", " ", line).strip()[:160]


def _apply(s, workspace_id: str, found: dict[str, dict[str, Any]], lead_on: bool, now: datetime,
           counts: dict[str, int]) -> bool:
    """Raise, show and clear alerts to match what was found. Returns whether what the Inbox shows changed."""
    live = {a.key: a for a in s.query(Alert).filter(Alert.workspace_id == workspace_id, Alert.status.in_(LIVE))}
    changed = False
    for key, spec in found.items():
        alert = live.get(key)
        if alert is None:
            stuck = spec["kind"] in STUCK
            alert = Alert(workspace_id=workspace_id, key=key, kind=spec["kind"], outcome_id=spec["outcome_id"],
                          task_id=spec["task_id"], decision_id=spec["decision_id"], title=spec["title"],
                          body=spec["body"], since=spec["since"], data=spec["data"],
                          status="watching" if stuck else "open", created_at=now)
            try:
                with s.begin_nested():  # the index allows one live alert per condition
                    s.add(alert)
            except IntegrityError:
                continue  # another scheduler raised it since this pass looked; it carries on from there
            counts["raised"] += 1
            if stuck and lead_on and spec["outcome_id"]:
                # The Lead's wake rules read this event: its next run looks into why, and says so to people.
                record(workspace_id, "lead_watch", f"Watch: {spec['title']}", outcome_id=spec["outcome_id"],
                       task_id=spec["task_id"], data={"alert_id": alert.id, "kind": spec["kind"]}, session=s)
                counts["woken"] += 1
        else:
            alert.title, alert.body, alert.data = spec["title"], spec["body"], spec["data"]
        if alert.status == "watching" and now - alert.created_at >= GRACE:
            alert.status = "open"
        if alert.status == "open" and alert.opened_at is None:
            alert.opened_at = now
            counts["shown"] += 1
            changed = True
            record(workspace_id, "alert_raised", alert.title, outcome_id=alert.outcome_id, task_id=alert.task_id,
                   level="warn", data={"alert_id": alert.id, "kind": alert.kind}, session=s)
    for key, alert in live.items():
        if key not in found:
            changed = changed or alert.status == "open"
            alert.status, alert.cleared_at = "cleared", now
            counts["cleared"] += 1
    return changed


def _span(delta: timedelta) -> str:
    minutes = int(delta.total_seconds() // 60)
    if minutes % 60 == 0:
        hours = minutes // 60
        return f"{hours} hour{'s' if hours != 1 else ''}"
    return f"{minutes} minutes"
