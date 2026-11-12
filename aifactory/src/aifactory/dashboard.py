"""Delivery performance over time, for the Dashboard page: daily, weekly or monthly buckets.

Each figure reuses a definition the Runs page's strip (api.stats) or metrics.py already uses, so the pages agree:

- a delivered task is a completed one, dated by when it finished;
- cost is what the period's runs (and their scorers) cost, so cost per delivered task is the period's spend over the
  tasks it delivered, the strip's figure for a shorter window. Spend that includes usage of unknown cost is a lower
  bound (``known: false``), and so is a median of lower bounds;
- the first-attempt rate counts merged tasks whose first conclusive attempt passed (metrics.first_attempt_result);
- autonomy is metrics.autonomy, per completed outcome. Outcomes no person took part in, but where the Director stood
  in for people, are counted apart ("director"), so the unattended share never takes credit for the Director;
- a change failure is a merge regret.regret_free_merge_rate settles as regretted (smoke failure, revert, linked
  critical regression or a recorded merge regret);
- decisions and guidance are split by who acted: a person (directly or through their Mind), the Director, an
  outcome's Lead, a delegated operator, or another part of the Factory.

Buckets are UTC (weeks start on Monday); the last one is the current, partial one and says so. Benchmark work is
left out, as everywhere else. Facts without a date are counted in ``undated`` rather than guessed into a bucket.
"""
from __future__ import annotations

from bisect import bisect_right
from collections import Counter, defaultdict
from datetime import datetime, timedelta
from statistics import median
from typing import Any, Callable

from sqlalchemy import func
from sqlalchemy.orm import Session

from .db import utcnow
from .metrics import AGENT_SOURCES, AUTOMATED_ACTORS, autonomy, benchmark, first_attempt_result
from .models import Decision, Event, Message, Outcome, Plan, Repository, Run, ScorerResult, Task, User
from .regret import regret_free_merge_rate
from .regrets import merge_time, timestamp

# Buckets shown by default, and the most a request may ask for.
BUCKETS: dict[str, tuple[int, int]] = {"day": (30, 120), "week": (12, 104), "month": (6, 36)}
STAGES = ("plan", "build", "verify", "review", "merge_queue", "release")
AUTONOMY = ("unattended", "director", "assisted", "unverified")
DECIDERS = ("person", "director", "lead", "operator", "factory")
TOP = 5

DEFINITIONS = {
    "tasks_delivered": "Tasks completed in the period (the strip's delivered tasks), by when they finished.",
    "prs_opened": "Pull requests the Factory opened in the period, counted once, when first published; "
                  "prs_opened_merged is how many of those have merged since.",
    "merges": "Factory tasks merged in the period, by the recorded merge time. Per day, this is deploy frequency.",
    "cost": "What the period's runs and their scorers cost. known=false: some usage has no known cost, so the "
            "figure is a lower bound.",
    "cost_per_delivered_task": "The period's cost over the tasks it delivered, as in the Runs strip.",
    "outcome_cost": "Median total cost of the outcomes completed in the period (cost per feature).",
    "autonomy": "Completed outcomes by who took part: unattended (no person), director (no person, but the Director "
                "stood in for people), assisted (a person took part) or unverified (the evidence can't tell).",
    "cycle_time": "Median hours from a task starting to its merge, for tasks merged in the period.",
    "stages": "Median hours per stage for tasks merged in the period: plan (outcome created to task created), "
              "build (task start to its last verification before the pull request), verify (to the pull request), "
              "review (CI and review until the merge queue), merge_queue (to the merge), release (merge to done).",
    "lead_time": "Median hours from an outcome being created to it completing.",
    "first_attempt": "Merged tasks whose first conclusive attempt passed its checks and verification.",
    "change_failure": "Merges in the period that later failed: a smoke failure, a revert, a linked critical "
                      "regression or a recorded merge regret. Pending merges are not yet settled.",
    "decisions": "Decisions asked in the period, and the ones resolved in it by who resolved them.",
    "guidance": "Guidance posted to outcomes in the period: a person's (an intervention) or a Factory agent's.",
}


def floor(at: datetime, period: str) -> datetime:
    day = at.replace(hour=0, minute=0, second=0, microsecond=0)
    if period == "week":
        return day - timedelta(days=day.weekday())
    if period == "month":
        return day.replace(day=1)
    return day


def step(start: datetime, period: str, n: int = 1) -> datetime:
    if period == "day":
        return start + timedelta(days=n)
    if period == "week":
        return start + timedelta(weeks=n)
    months = start.year * 12 + start.month - 1 + n
    return start.replace(year=months // 12, month=months % 12 + 1)


def _hours(start: Any, end: Any) -> float | None:
    a, b = timestamp(start), timestamp(end)
    if a is None or b is None or b < a:
        return None
    return (b - a).total_seconds() / 3600


def _iso(at: datetime) -> str:
    return at.isoformat() + "Z"


def _median(values: list[float]) -> float | None:
    return round(median(values), 2) if values else None


def _stage(e: Event, name: str) -> bool:
    return e.kind == "stage" and (e.data or {}).get("stage") == name


def stages(task: Task, outcome: Outcome, journal: list[Event], merged_at: datetime) -> dict[str, float | None]:
    """Hours a merged task spent in each stage; None where the history doesn't date both ends."""
    ev = task.evidence or {}
    published = next((e for e in journal if e.kind == "published"), None)
    published_at = timestamp(published.created_at if published else (ev.get("published") or {}).get("at"))
    # The last verification before the pull request: earlier ones belong to rejected candidates (build retries).
    verify = [e.created_at for e in journal if _stage(e, "verify") and published_at
              and timestamp(e.created_at) <= published_at]
    verify_start = verify[-1] if verify else None
    # The merge queue records when the task first reached the merge stage; older history has the stage event.
    queued = timestamp((ev.get("merge_queue") or {}).get("entered_at"))
    if _hours(published_at, queued) is None or _hours(queued, merged_at) is None:
        queued = next((timestamp(e.created_at) for e in journal if _stage(e, "merge") and published_at
                       and _hours(published_at, e.created_at) is not None
                       and _hours(e.created_at, merged_at) is not None), None)
    return {
        "plan": _hours(outcome.created_at, task.created_at),
        "build": _hours(task.started_at, verify_start),
        "verify": _hours(verify_start, published_at),
        "review": _hours(published_at, queued),
        "merge_queue": _hours(queued, merged_at),
        "release": _hours(merged_at, task.finished_at) if task.status == "completed" else None,
    }


class _Tally:
    """One bucket's (or the whole range's) facts before they become figures."""

    def __init__(self) -> None:
        self.n: Counter[str] = Counter()
        self.samples: defaultdict[str, list[float]] = defaultdict(list)
        self.roles: Counter[str] = Counter()
        self.cost = 0.0
        self.cost_unknown = False
        self.outcome_cost_unknown = False

    def figures(self, days: float) -> dict[str, Any]:
        n, samples, delivered = self.n, self.samples, self.n["delivered"]
        return {
            "days": round(days, 3),
            "tasks_delivered": delivered,
            "outcomes_completed": n["outcomes"],
            "prs_opened": n["prs_opened"],
            "prs_opened_merged": n["prs_opened_merged"],
            "merges": n["merges"],
            "runs": {"total": n["runs"], "by_role": dict(self.roles)},
            "cost": {"usd": round(self.cost, 2), "known": not self.cost_unknown},
            "cost_per_delivered_task": {"usd": round(self.cost / delivered, 2) if delivered else None,
                                        "known": not self.cost_unknown},
            "outcome_cost": {"median_usd": _median(samples["outcome_cost"]), "known": not self.outcome_cost_unknown,
                             "sample_count": len(samples["outcome_cost"])},
            "autonomy": {k: n[f"autonomy_{k}"] for k in AUTONOMY},
            "cycle_time": {"median_hours": _median(samples["cycle"]), "sample_count": len(samples["cycle"]),
                           "unknown_count": n["cycle_unknown"]},
            "stages": {s: {"median_hours": _median(samples[s]), "sample_count": len(samples[s])} for s in STAGES},
            "lead_time": {"median_hours": _median(samples["lead_time"]), "sample_count": len(samples["lead_time"])},
            "first_attempt": {"merged": n["merges"], "passed": n["first_attempt"]},
            "change_failure": {"settled": n["cf_settled"], "failed": n["cf_failed"], "pending": n["cf_pending"]},
            "decisions": {"asked": n["asked"], **{k: n[f"decided_{k}"] for k in DECIDERS}},
            "guidance": {k: n[f"guidance_{k}"] for k in ("person", "director", "lead")},
        }


def series(db: Session, workspace_id: str, period: str = "week", buckets: int | None = None,
           now: datetime | None = None) -> dict[str, Any]:
    if period not in BUCKETS:
        raise ValueError(f"period must be one of {', '.join(BUCKETS)}")
    default, most = BUCKETS[period]
    count = default if buckets is None else buckets
    if not 1 <= count <= most:
        raise ValueError(f"A {period} series has 1 to {most} buckets")
    now = timestamp(now) if now is not None else utcnow()
    bounds = [step(floor(now, period), period, i - count + 1) for i in range(count + 1)]
    start, end = bounds[0], bounds[-1]
    tallies = [_Tally() for _ in range(count)]
    whole = _Tally()
    undated: Counter[str] = Counter()

    def add(at: Any, fn: Callable[[_Tally], None], key: str = "") -> bool:
        """Apply a fact to its bucket and to the range. A fact without a date is counted as undated under key."""
        at = timestamp(at)
        if at is None:
            if key:
                undated[key] += 1
            return False
        if not start <= at < end:
            return False
        fn(tallies[bisect_right(bounds, at) - 1])
        fn(whole)
        return True

    def bump(*keys: str) -> Callable[[_Tally], None]:
        return lambda t: t.n.update(keys)

    def rows(model):
        return db.query(model).filter(model.workspace_id == workspace_id).all()

    outcomes = {o.id: o for o in rows(Outcome) if not benchmark(o)}
    tasks = {t.id: t for t in rows(Task) if t.outcome_id in outcomes and not benchmark(t)}
    runs = [r for r in rows(Run) if r.outcome_id in outcomes and not benchmark(r)
            and (not r.task_id or r.task_id in tasks)]
    events = sorted(rows(Event), key=lambda e: e.id)
    messages, decisions = rows(Message), rows(Decision)
    system = {u.id: u for u in db.query(User).filter(User.is_system.is_(True))}
    plans = {p.id: p for p in db.query(Plan).join(Outcome).filter(Outcome.workspace_id == workspace_id)}
    repos = {r.id: r for r in rows(Repository)}

    # Spend: each run with its scorers' cost, dated by when the run started. Score history is unbounded: aggregate.
    scored = {run_id: (cost, unknown) for run_id, cost, unknown in db.query(
        ScorerResult.run_id, func.coalesce(func.sum(ScorerResult.cost_usd), 0),
        func.count(ScorerResult.id).filter(ScorerResult.cost_known.is_(False)),
    ).filter(ScorerResult.workspace_id == workspace_id).group_by(ScorerResult.run_id)}
    outcome_cost: defaultdict[str, list] = defaultdict(lambda: [0.0, True])
    for r in runs:
        extra, unknown = scored.get(r.id, (0, 0))
        cost, known = r.cost_usd + extra, r.cost_known and not unknown
        outcome_cost[r.outcome_id][0] += cost
        outcome_cost[r.outcome_id][1] &= known

        def spend(t: _Tally, cost=cost, known=known, role=r.role) -> None:
            t.n["runs"] += 1
            t.roles[role] += 1
            t.cost += cost
            t.cost_unknown |= not known

        add(r.started_at, spend)

    journals: defaultdict[str, list[Event]] = defaultdict(list)
    for e in events:
        if e.task_id in tasks:
            journals[e.task_id].append(e)
    for t in tasks.values():
        ev, journal = t.evidence or {}, journals[t.id]
        if t.status == "completed":
            add(t.finished_at, bump("delivered"), "tasks_delivered")
        published = next((e for e in journal if e.kind == "published"), None)
        if t.pr_number or published or ev.get("published"):
            opened = published.created_at if published else (ev.get("published") or {}).get("at")
            keys = ("prs_opened", "prs_opened_merged") if t.merge_sha else ("prs_opened",)
            add(opened, bump(*keys), "prs_opened")
        if not t.merge_sha:
            continue
        merged_at = merge_time(t)
        repo = repos.get(t.repo_id)
        passed = first_attempt_result(t, journal, (repo.config or {}).get("checks", []) if repo else None) is True
        cycle = _hours(t.started_at, merged_at)
        times = stages(t, outcomes[t.outcome_id], journal, merged_at) if merged_at else {}

        def merge(tally: _Tally, passed=passed, cycle=cycle, times=times) -> None:
            tally.n.update(merges=1, first_attempt=passed, cycle_unknown=cycle is None)
            if cycle is not None:
                tally.samples["cycle"].append(cycle)
            for name, hours in times.items():
                if hours is not None:
                    tally.samples[name].append(hours)

        add(merged_at, merge, "merges")

    # Change failure: the canonical regret accounting, over the same window, bucketed by merge time.
    window = regret_free_merge_rate(db, workspace_id, now=end, window_days=(end - start).total_seconds() / 86400)
    for sample in window["samples"]:
        if sample["task_id"] in tasks:
            add(sample["merged_at"], bump("cf_failed", "cf_settled") if sample["regretted"]
                else bump("cf_settled") if sample["settled"] else bump("cf_pending"))

    # Autonomy, per completed outcome. The Director standing in for people is not a person, and not hidden either.
    # Like metrics.autonomy, only what the Director did while the outcome was being delivered counts: a later note
    # on a completed outcome doesn't rewrite how it was delivered.
    directed: set[str] = set()
    for row, at in [(d, d.resolved_at) for d in decisions] + [(m, m.created_at) for m in messages]:
        if (row.attribution or {}).get("source") != "director":
            continue
        for outcome_id in {row.outcome_id, tasks[row.task_id].outcome_id if row.task_id in tasks else None}:
            o = outcomes.get(outcome_id)
            begun, done, when = (timestamp(o.created_at), timestamp(o.completed_at), timestamp(at)) if o else (None,) * 3
            if begun and done and when and begun <= when <= done:
                directed.add(outcome_id)
    classes = autonomy(outcomes, tasks, events, messages, decisions, set(system), plans)
    for outcome_id, kind in classes.items():
        o = outcomes[outcome_id]
        kind = "director" if kind == "unattended" and outcome_id in directed else kind
        cost, known = outcome_cost.get(outcome_id, (0.0, True))
        lead = _hours(o.created_at, o.completed_at)

        def complete(t: _Tally, kind=kind, cost=cost, known=known, lead=lead) -> None:
            t.n.update(("outcomes", f"autonomy_{kind}"))
            t.samples["outcome_cost"].append(cost)
            t.outcome_cost_unknown |= not known
            if lead is not None:
                t.samples["lead_time"].append(lead)

        add(o.completed_at, complete, "outcomes_completed")

    def decider(d: Decision) -> str:
        source = (d.attribution or {}).get("source")
        if source in AGENT_SOURCES:
            return source
        if d.resolved_by in system:
            return "operator" if (system[d.resolved_by].email or "").startswith("operator") else "factory"
        return "factory" if d.resolved_by in AUTOMATED_ACTORS else "person"

    for d in decisions:
        add(d.created_at, bump("asked"), "decisions")
        if d.status in ("resolved", "dismissed") and d.resolved_at is not None:
            add(d.resolved_at, bump(f"decided_{decider(d)}"))
    for m in messages:
        if m.author_kind == "user":
            source = (m.attribution or {}).get("source")
            add(m.created_at, bump(f"guidance_{source if source in AGENT_SOURCES else 'person'}"), "guidance")

    def days(a: datetime, b: datetime) -> float:
        return max(0.0, (min(b, now) - a).total_seconds() / 86400)

    finished = [o for o in outcomes.values() if o.id in classes and timestamp(o.completed_at)
                and start <= timestamp(o.completed_at) < end]
    expensive = sorted(finished, key=lambda o: (-outcome_cost[o.id][0], -o.number))[:TOP]
    return {
        "period": period,
        "generated_at": _iso(now),
        "definitions": DEFINITIONS,
        "buckets": [{"start": _iso(a), "end": _iso(b), "partial": b > now, **tally.figures(days(a, b))}
                    for a, b, tally in zip(bounds, bounds[1:], tallies)],
        "range": {"start": _iso(start), "end": _iso(end), **whole.figures(days(start, end))},
        "undated": dict(undated),
        "expensive_outcomes": [
            {"id": o.id, "number": o.number, "title": o.title, "cost_usd": round(outcome_cost[o.id][0], 2),
             "cost_known": outcome_cost[o.id][1], "completed_at": _iso(timestamp(o.completed_at)),
             "tasks_delivered": sum(t.outcome_id == o.id and t.status == "completed" for t in tasks.values())}
            for o in expensive],
    }
