"""The orchestrator: schedules planning and task delivery with bounded concurrency.

- Outcomes in ``planning`` get one planner.
- Tasks become ``queued`` once their dependencies are complete; queued tasks start while the
  workspace (and the server) has free slots. Independent tasks run in parallel.
- Merges into the same repository are serialized; everything else overlaps.
- On restart, work that was running resumes from its last durable stage.
"""

from __future__ import annotations

import asyncio
import logging
import os
from typing import Any

from ..config import get_settings
from ..db import session_scope, utcnow
from ..events import notify, record
from ..models import Outcome, Task, Workspace
from .delivery import TaskRunner
from .planning import plan_outcome
from .runtime import open_decision, workspace_settings

log = logging.getLogger(__name__)

ACTIVE_TASK = {"queued", "running"}
DONE_TASK = {"completed"}
TERMINAL_TASK = {"completed", "abandoned", "cancelled"}
HUMAN_TASK = {"needs_input", "awaiting_approval", "blocked", "failed", "smoke_failed"}


class Orchestrator:
    def __init__(self) -> None:
        self.instance = f"{os.getpid()}"
        self._wake = asyncio.Event()
        self._loop_task: asyncio.Task | None = None
        self._planning: dict[str, tuple[asyncio.Task, asyncio.Event]] = {}
        self._tasks: dict[str, tuple[asyncio.Task, asyncio.Event]] = {}
        self._repo_locks: dict[str, asyncio.Lock] = {}
        self._stopping = False
        self._last_signal_sync = 0.0

    # lifecycle -----------------------------------------------------------------
    def start(self) -> None:
        if self._loop_task is None:
            self._recover()
            self._loop_task = asyncio.create_task(self._run(), name="orchestrator")

    async def stop(self) -> None:
        self._stopping = True
        for job, cancel in list(self._planning.values()) + list(self._tasks.values()):
            cancel.set()
            job.cancel()
        if self._loop_task:
            self._loop_task.cancel()
        await asyncio.sleep(0)

    def wake(self) -> None:
        try:
            self._wake.set()
        except RuntimeError:
            pass

    def repo_lock(self, repo_id: str) -> asyncio.Lock:
        return self._repo_locks.setdefault(repo_id, asyncio.Lock())

    def status(self) -> dict[str, Any]:
        return {"planning": list(self._planning), "running": list(self._tasks),
                "global_limit": get_settings().global_concurrency}

    def _recover(self) -> None:
        """Work that was mid-flight when the server stopped resumes from its durable stage."""
        with session_scope() as s:
            for task in s.query(Task).filter(Task.status == "running"):
                task.status = "queued"
                record(task.workspace_id, "resumed", f"{task.key} resumed after a restart (stage: {task.stage or 'start'})",
                       outcome_id=task.outcome_id, task_id=task.id, session=s)

    # scheduling ----------------------------------------------------------------
    async def _run(self) -> None:
        while not self._stopping:
            try:
                await asyncio.to_thread(self._promote)
                self._schedule()
                await self._maybe_sync_signals()
            except Exception:  # never let the scheduler die
                log.exception("orchestrator tick failed")
            try:
                await asyncio.wait_for(self._wake.wait(), timeout=3)
            except asyncio.TimeoutError:
                pass
            self._wake.clear()

    async def _maybe_sync_signals(self) -> None:
        """Poll GitHub for issues and CI failures every few minutes (intake + feedback)."""
        import time

        interval = int(os.environ.get("AIFACTORY_SIGNAL_INTERVAL_SECONDS", "180"))
        if interval <= 0 or time.monotonic() - self._last_signal_sync < interval:
            return
        self._last_signal_sync = time.monotonic()
        from ..db import SessionLocal
        from ..github import GitHubError, resolve_token
        from ..models import Repository
        from ..signals import sync_github

        with session_scope() as s:
            targets = {r.workspace_id for r in s.query(Repository)}
            paused = {w.id for w in s.query(Workspace).filter(Workspace.paused.is_(True))}
        for workspace_id in targets - paused:
            if (workspace_settings(workspace_id).get("signals") or {}).get("auto_sync") is False:
                continue
            db = SessionLocal()
            try:
                result = await sync_github(db, workspace_id, resolve_token(workspace_id))
                if result.get("accepted"):
                    self.wake()
            except GitHubError as exc:
                log.warning("signal sync skipped for %s: %s", workspace_id, exc)
            except Exception:
                log.exception("signal sync failed for %s", workspace_id)
            finally:
                db.close()

    def _promote(self) -> None:
        """Advance pending tasks whose dependencies completed, and roll up outcome status."""
        with session_scope() as s:
            outcomes = s.query(Outcome).filter(Outcome.status.in_(["running", "attention"])).all()
            for outcome in outcomes:
                tasks = s.query(Task).filter(Task.outcome_id == outcome.id).all()
                by_key = {t.key: t for t in tasks}
                for task in tasks:
                    if task.status == "pending":
                        deps = [by_key.get(k) for k in task.depends_on or []]
                        if all(d is not None and d.status in DONE_TASK for d in deps):
                            task.status = "queued"
                            notify(outcome.workspace_id, "task", task.id, outcome_id=outcome.id)
                self._rollup(outcome, tasks)

    def _rollup(self, outcome: Outcome, tasks: list[Task]) -> None:
        if not tasks:
            return
        statuses = {t.status for t in tasks}
        if statuses <= {"completed"}:
            outcome.status = "completed"
            outcome.completed_at = utcnow()
            outcome.summary = f"All {len(tasks)} task(s) merged and verified."
            record(outcome.workspace_id, "outcome_completed", f"Outcome #{outcome.number} delivered",
                   outcome_id=outcome.id, level="success")
            notify(outcome.workspace_id, "outcome", outcome.id)
            return
        if statuses <= {"cancelled", "abandoned"}:
            outcome.status = "cancelled"
            notify(outcome.workspace_id, "outcome", outcome.id)
            return
        if statuses <= TERMINAL_TASK:
            outcome.status = "partially_completed"
            outcome.completed_at = utcnow()
            outcome.summary = f"{sum(t.status == 'completed' for t in tasks)} of {len(tasks)} task(s) delivered."
            notify(outcome.workspace_id, "outcome", outcome.id)
            return
        # A pending task is stalled when something it depends on is waiting on a person or was dropped.
        by_key = {t.key: t for t in tasks}
        stalled: set[str] = {t.key for t in tasks if t.status in HUMAN_TASK | {"abandoned", "cancelled"}}
        changed = True
        while changed:
            changed = False
            for t in tasks:
                if t.status == "pending" and t.key not in stalled and any(d in stalled for d in t.depends_on or []):
                    stalled.add(t.key)
                    changed = True
        moving = [t for t in tasks if t.status in ACTIVE_TASK or (t.status == "pending" and t.key not in stalled)]
        wanted = "running" if moving else "attention"
        if outcome.status != wanted:
            outcome.status = wanted
            notify(outcome.workspace_id, "outcome", outcome.id)

    def _schedule(self) -> None:
        settings = get_settings()
        with session_scope() as s:
            paused = {w.id for w in s.query(Workspace).filter(Workspace.paused.is_(True))}
            planning = [o.id for o in s.query(Outcome).filter(Outcome.status == "planning")
                        if o.workspace_id not in paused]
            queued = [(t.id, t.workspace_id) for t in
                      s.query(Task).filter(Task.status == "queued").order_by(Task.created_at)
                      if t.workspace_id not in paused]
        for outcome_id in planning:
            if outcome_id not in self._planning:
                self._spawn_plan(outcome_id)
        running_by_ws: dict[str, int] = {}
        with session_scope() as s:
            for task_id in self._tasks:
                task = s.get(Task, task_id)
                if task:
                    running_by_ws[task.workspace_id] = running_by_ws.get(task.workspace_id, 0) + 1
        for task_id, workspace_id in queued:
            if task_id in self._tasks:
                continue
            if len(self._tasks) >= settings.global_concurrency:
                break
            limit = int(workspace_settings(workspace_id).get("concurrency", 3))
            if running_by_ws.get(workspace_id, 0) >= limit:
                continue
            running_by_ws[workspace_id] = running_by_ws.get(workspace_id, 0) + 1
            self._spawn_task(task_id)

    def _spawn_plan(self, outcome_id: str) -> None:
        cancel = asyncio.Event()

        async def job() -> None:
            try:
                await plan_outcome(outcome_id, cancel)
            except asyncio.CancelledError:
                pass
            except Exception as exc:
                log.exception("planning crashed")
                with session_scope() as s:
                    outcome = s.get(Outcome, outcome_id)
                    outcome.status = "blocked"
                    outcome.blocked_reason = f"Planning crashed: {exc}"
                    ws = outcome.workspace_id
                open_decision(ws, "unblock", "Planning crashed", str(exc), outcome_id=outcome_id,
                              options=[{"value": "retry", "label": "Retry planning"}, {"value": "cancel", "label": "Cancel"}])
            finally:
                self._planning.pop(outcome_id, None)
                self.wake()

        self._planning[outcome_id] = (asyncio.create_task(job(), name=f"plan-{outcome_id}"), cancel)

    def _spawn_task(self, task_id: str) -> None:
        cancel = asyncio.Event()
        with session_scope() as s:
            task = s.get(Task, task_id)
            task.status = "running"
            task.lease_owner = self.instance
            ws, outcome_id = task.workspace_id, task.outcome_id
        notify(ws, "task", task_id, outcome_id=outcome_id)

        async def job() -> None:
            try:
                await TaskRunner(task_id, cancel, self.repo_lock).run()
            except asyncio.CancelledError:
                with session_scope() as s:
                    task = s.get(Task, task_id)
                    if task and task.status == "running":
                        task.status = "cancelled" if cancel.is_set() else "queued"
            except Exception as exc:
                log.exception("task crashed")
                with session_scope() as s:
                    task = s.get(Task, task_id)
                    task.status = "blocked"
                    task.blocked_reason = f"{type(exc).__name__}: {exc}"
                open_decision(ws, "unblock", "A task stopped unexpectedly", f"{type(exc).__name__}: {exc}",
                              outcome_id=outcome_id, task_id=task_id,
                              options=[{"value": "retry", "label": "Retry"}, {"value": "abandon", "label": "Abandon"}])
            finally:
                self._tasks.pop(task_id, None)
                with session_scope() as s:
                    task = s.get(Task, task_id)
                    if task and task.status == "running" and not self._stopping:
                        # Never silently loop: a runner that stops without a recorded state needs a look.
                        task.status = "blocked"
                        task.blocked_reason = "The task stopped without recording an outcome."
                notify(ws, "task", task_id, outcome_id=outcome_id)
                self.wake()

        self._tasks[task_id] = (asyncio.create_task(job(), name=f"task-{task_id}"), cancel)

    # control -------------------------------------------------------------------
    def cancel_outcome(self, outcome_id: str) -> None:
        if outcome_id in self._planning:
            job, cancel = self._planning[outcome_id]
            cancel.set()
            job.cancel()
        with session_scope() as s:
            task_ids = [t.id for t in s.query(Task).filter(Task.outcome_id == outcome_id)]
        for task_id in task_ids:
            self.cancel_task(task_id)

    def cancel_task(self, task_id: str) -> None:
        if task_id in self._tasks:
            job, cancel = self._tasks[task_id]
            cancel.set()
            job.cancel()


orchestrator = Orchestrator()
