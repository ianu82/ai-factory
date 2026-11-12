"""Replaceable, local regret-free merge metric for earned autonomy.

The sample window is closed: [now - window_days, now], in UTC, by *merge*
time, not task creation/completion or regret detection time. Each Factory task
is one sample (a repair/revert PR is not a second delivery). Completed merges
are settled; a durable regret settles a merge immediately, even during recovery.
Explicit critical-regression links are regrets regardless of signal source;
append-only link events preserve them after feedback edits. Other merged work
is pending. A rate of None means there are no settled samples.

This API deliberately does not apply the ledger's post-merge observation-window
filter: a later discovered regression must not turn into a successful delivery.
Missing merge identity/time is reported separately, never imputed from status,
finished_at, or the clock. Callers own transactions; reads make no external calls.
"""
from __future__ import annotations

import math
from collections import defaultdict
from datetime import datetime, timedelta
from typing import Any

from sqlalchemy import or_
from sqlalchemy.orm import Session

from .db import utcnow
from .events import record
from .models import Event, MergeRegret, Outcome, Repository, Signal, Task, Workspace
from .regrets import timestamp


def _iso(at: datetime) -> str:
    return at.isoformat() + "Z"


def _merge(task: Task) -> tuple[str, datetime | None]:
    evidence = (task.evidence or {}).get("merge") or {}
    return task.merge_sha or evidence.get("sha", ""), timestamp(evidence.get("at"))


def _critical_link_matches(link: Any, task: Task, sha: str) -> bool:
    """Only structured attestations for this exact merged scope are causal evidence."""
    return (isinstance(link, dict) and link.get("severity") == "critical"
            and link.get("workspace_id") == task.workspace_id
            and link.get("repo_id") == task.repo_id and link.get("task_id") == task.id
            and link.get("merge_sha") == sha)


def _scope(db: Session, workspace_id: str, repo_id: str | None) -> None:
    if db.get(Workspace, workspace_id) is None:
        raise ValueError("Workspace not found")
    if repo_id is not None:
        repo = db.get(Repository, repo_id)
        if repo is None or repo.workspace_id != workspace_id:
            raise ValueError("Repository does not belong to workspace")


def regret_free_merge_rate(
    db: Session, workspace_id: str, repo_id: str | None = None, *,
    now: datetime | None = None, window_days: float = 30,
) -> dict[str, Any]:
    """Return counts, nullable [0, 1] rate, UTC boundaries and traceable samples.

    ``samples`` includes in-window merges, with settlement and evidence paths/IDs;
    ``unknown_samples`` retains merges whose identity or date cannot be established.
    Unmerged and out-of-window tasks are not samples. Signal status and fix links
    never affect accounting. ``now`` fixes the window, not a historical DB snapshot:
    mutable completion status cannot reconstruct past settlement.
    """
    if isinstance(window_days, bool) or not math.isfinite(window_days) or window_days <= 0:
        raise ValueError("window_days must be finite and positive")
    end = utcnow() if now is None else timestamp(now)
    if end is None:
        raise ValueError("now must be a valid datetime")
    start = end - timedelta(days=window_days)
    _scope(db, workspace_id, repo_id)

    tasks = db.query(Task).join(Repository, Repository.id == Task.repo_id).filter(
        Task.workspace_id == workspace_id, Repository.workspace_id == workspace_id)
    signals = db.query(Signal).filter(
        Signal.workspace_id == workspace_id,
        or_(Signal.source == "smoke_failure",
            Signal.data["critical_regression"]["severity"].as_string() == "critical"))
    regrets = db.query(MergeRegret).filter_by(workspace_id=workspace_id)
    if repo_id is not None:
        tasks = tasks.filter(Task.repo_id == repo_id)
        signals = signals.filter(Signal.repo_id == repo_id)
        regrets = regrets.filter(MergeRegret.repo_id == repo_id)

    # Load once, then index. Legacy signals identify the PR via url; newer ones
    # may name data.task_id. An explicit task link takes precedence over the URL.
    by_task: dict[tuple[str, str], list[Signal]] = defaultdict(list)
    by_pr: dict[tuple[str, str], list[Signal]] = defaultdict(list)
    critical: dict[tuple[str, str], list[Signal]] = defaultdict(list)
    for signal in signals:
        data = signal.data or {}
        link = data.get("critical_regression")
        if isinstance(link, dict) and isinstance(link.get("task_id"), str):
            critical[signal.repo_id, link["task_id"]].append(signal)
        if signal.source == "smoke_failure":
            task_id = data.get("task_id")
            if task_id:
                by_task[signal.repo_id, task_id].append(signal)
            elif signal.url:
                by_pr[signal.repo_id, signal.url].append(signal)
    ledger: dict[tuple[str, str], list[MergeRegret]] = defaultdict(list)
    for regret in regrets:
        ledger[regret.repo_id, regret.task_id].append(regret)
    # Events preserve regrets even if the task or signal's latest evidence changes.
    events: dict[str, list[Event]] = defaultdict(list)
    query = db.query(Event).join(Task, Task.id == Event.task_id).filter(
        Event.workspace_id == workspace_id, Task.workspace_id == workspace_id,
        Event.kind.in_(("reverted", "smoke_failed", "critical_regression_linked")))
    if repo_id is not None:
        query = query.filter(Task.repo_id == repo_id)
    for event in query:
        events[event.task_id].append(event)

    samples, unknown = [], []
    for task in tasks.order_by(Task.id):
        ev = task.evidence or {}
        sha, merged_at = _merge(task)
        if not sha and not ev.get("merge"):
            continue
        sample = {"task_id": task.id, "repo_id": task.repo_id, "pr_number": task.pr_number,
                  "pr_url": task.pr_url, "merge_sha": sha, "merged_at": _iso(merged_at) if merged_at else None,
                  "merge_reference": {"task_id": task.id, "path": "evidence.merge"},
                  "settled": False, "regretted": False, "evidence": []}
        if not sha or merged_at is None:
            sample["reason"] = "unknown_merge_identity" if not sha else "unknown_merge_time"
            unknown.append(sample)
            continue
        if not start <= merged_at <= end:
            continue
        refs = sample["evidence"]

        def smoke_failure(value: dict, path: str) -> None:
            # A retry against a different candidate is not evidence against this merge.
            if (value.get("state") in {"failed", "failure"}
                    and (not value.get("ref") or value["ref"] == sha)):
                refs.append({"task_id": task.id, "path": path, "kind": "smoke_failure"})

        smoke_failure(ev.get("smoke") or {}, "evidence.smoke")
        for index, failure in enumerate(ev.get("failure_facts") or []):
            if failure.get("kind") == "smoke":
                smoke_failure(failure, f"evidence.failure_facts.{index}")
        revert = ev.get("revert") or {}
        if revert.get("sha"):
            refs.append({"task_id": task.id, "path": "evidence.revert", "kind": "factory_revert"})
        for signal in by_task[task.repo_id, task.id] + by_pr[task.repo_id, task.pr_url]:
            refs.append({"signal_id": signal.id, "kind": "smoke_failure", "url": signal.url})
        for signal in critical[task.repo_id, task.id]:
            if _critical_link_matches(signal.data["critical_regression"], task, sha):
                refs.append({"signal_id": signal.id, "kind": "critical_regression",
                             "path": "data.critical_regression", "url": signal.url})
        for regret in ledger[task.repo_id, task.id]:
            refs.append({"regret_id": regret.id, "kind": regret.kind, "url": regret.url})
        for event in events[task.id]:
            if (event.kind != "critical_regression_linked"
                    or _critical_link_matches(event.data, task, sha)):
                refs.append({"event_id": event.id, "kind": event.kind})
        sample["regretted"] = bool(refs)
        sample["settled"] = bool(refs) or task.status == "completed"
        sample["reason"] = "regretted" if refs else ("completed" if sample["settled"] else "pending")
        if task.status == "completed":
            sample["settlement_reference"] = {"task_id": task.id, "path": "status", "value": "completed"}
        samples.append(sample)

    settled = sum(sample["settled"] for sample in samples)
    regretted = sum(sample["regretted"] for sample in samples)
    return {"workspace_id": workspace_id, "repo_id": repo_id,
            "window_start": _iso(start), "window_end": _iso(end), "window_days": window_days,
            "settled_count": settled, "regretted_count": regretted, "regret_free_count": settled - regretted,
            "rate": (settled - regretted) / settled if settled else None,
            "pending_count": len(samples) - settled, "unknown_count": len(unknown),
            "samples": samples, "unknown_samples": unknown}


def link_critical_regression(
    db: Session, workspace_id: str, signal_id: str, task_id: str, *,
    repo_id: str | None = None, actor: str = "factory",
) -> dict[str, Any]:
    """Explicitly attest a critical regression's merged cause (never parse text).

    This is a trusted policy/admin service primitive, not an authorization endpoint.
    Validate all scope and existing causal references before writing Signal.data and
    an append-only critical_regression_linked Event atomically. Replays are no-ops;
    links cannot be silently reassigned. Signal.outcome_id is a *fix*, not a cause.
    """
    _scope(db, workspace_id, repo_id)
    signal = db.query(Signal).filter_by(id=signal_id).with_for_update().one_or_none()
    task = db.get(Task, task_id)
    if task is None or task.workspace_id != workspace_id:
        raise ValueError("Task does not belong to workspace")
    _scope(db, workspace_id, task.repo_id)
    outcome = db.get(Outcome, task.outcome_id)
    if outcome is None or outcome.workspace_id != workspace_id:
        raise ValueError("Task outcome does not belong to workspace")
    if repo_id is not None and task.repo_id != repo_id:
        raise ValueError("Task does not belong to repository")
    if signal is None or signal.workspace_id != workspace_id or signal.repo_id != task.repo_id:
        raise ValueError("Signal does not belong to task repository/workspace")
    sha, merged_at = _merge(task)
    if not sha:
        raise ValueError("Critical regression requires a merged task")
    data = signal.data or {}
    if ((data.get("task_id") and data["task_id"] != task.id)
            or (data.get("repo_id") and data["repo_id"] != task.repo_id)
            or (data.get("pr_number") is not None and data["pr_number"] != task.pr_number)
            or (data.get("pr_url") and data["pr_url"] != task.pr_url)
            or (signal.source == "smoke_failure" and signal.url and signal.url != task.pr_url)):
        raise ValueError("Signal references an unrelated task/PR")
    link = {"severity": "critical", "workspace_id": workspace_id, "repo_id": task.repo_id,
            "task_id": task.id, "pr_number": task.pr_number, "pr_url": task.pr_url,
            "merge_sha": sha, "merged_at": _iso(merged_at) if merged_at else None}
    existing = data.get("critical_regression")
    if existing is not None:
        if existing != link:
            raise ValueError("Critical regression is already linked to another merge")
        return existing
    record(workspace_id, "critical_regression_linked", "Critical regression linked to merged task",
           task_id=task.id, outcome_id=task.outcome_id, actor=actor, level="error",
           data={"signal_id": signal.id, **link}, session=db, publish=False)
    signal.data = {**data, "critical_regression": link}
    db.flush()
    return link
