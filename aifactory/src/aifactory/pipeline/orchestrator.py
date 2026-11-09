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
from ..sandbox import host_capacity
from .delivery import TaskRunner
from .planning import plan_outcome
from .runtime import merged_settings, open_decision, workspace_settings
from .scheduling import Candidate, RequesterTurns

log = logging.getLogger(__name__)

ACTIVE_TASK = {"queued", "running"}
DONE_TASK = {"completed"}
TERMINAL_TASK = {"completed", "abandoned", "cancelled", "reverted", "handed_over"}
HUMAN_TASK = {"needs_input", "awaiting_approval", "blocked", "failed", "smoke_failed"}


class Orchestrator:
    def __init__(self) -> None:
        self.instance = f"{os.getpid()}"
        self._wake = asyncio.Event()
        self._loop_task: asyncio.Task | None = None
        self._planning: dict[str, tuple[asyncio.Task, asyncio.Event]] = {}
        self._tasks: dict[str, tuple[asyncio.Task, asyncio.Event]] = {}
        self._planning_turns = RequesterTurns()
        self._delivery_turns = RequesterTurns()
        self._repo_locks: dict[str, asyncio.Lock] = {}
        self._stopping = False
        self._last_signal_sync = float("-inf")

    # lifecycle -----------------------------------------------------------------
    def start(self) -> None:
        if self._loop_task is None:
            log.info("capacity %s", host_capacity())
            self._recover()
            self._loop_task = asyncio.create_task(self._run(), name="orchestrator")

    async def stop(self) -> None:
        """Shutdown (e.g. a redeploy) interrupts work without cancelling it: tasks resume on restart."""
        self._stopping = True
        for job, _cancel in list(self._planning.values()) + list(self._tasks.values()):
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
                await asyncio.to_thread(self._maybe_analyze)
                await self._maybe_reconcile_costs()
                await self._maybe_reconcile_pulls()
                await self._maybe_inventory()
            except Exception:  # never let the scheduler die
                log.exception("orchestrator tick failed")
            try:
                await asyncio.wait_for(self._wake.wait(), timeout=3)
            except asyncio.TimeoutError:
                pass
            self._wake.clear()

    def log_inventory(self) -> None:
        """Operator visibility in the server log: open work, its state and what it waits on."""
        from ..models import Decision

        with session_scope() as s:
            open_outcomes = s.query(Outcome).filter(Outcome.status.notin_(["completed", "cancelled"])).all()
            for o in open_outcomes:
                log.info("inventory outcome #%s %s %s%s", o.number, o.status, o.title[:80],
                         f" — {o.blocked_reason[:200]}" if o.blocked_reason else "")
                for t in s.query(Task).filter(Task.outcome_id == o.id):
                    log.info("inventory   task %s %s/%s attempt=%s pr=%s %s", t.key, t.status, t.stage or "-", t.attempt,
                             t.pr_number or "-", (t.blocked_reason or "")[:300].replace("\n", " "))
                for d in s.query(Decision).filter(Decision.outcome_id == o.id, Decision.status == "open"):
                    log.info("inventory   decision %s: %s — %s", d.kind, d.title[:100], (d.body or "")[:400].replace("\n", " "))
        from ..models import Membership, Secret

        with session_scope() as s:
            for ws in s.query(Workspace).all():
                creds = ", ".join(f"{c.kind}={c.status}" for c in s.query(Secret).filter(Secret.workspace_id == ws.id)) or "none"
                roles = [m.role for m in s.query(Membership).filter(Membership.workspace_id == ws.id)]
                log.info("inventory workspace %s '%s' members=%s credentials: %s", ws.id[:6], ws.name, roles, creds)
        log.info("inventory orchestrator %s", self.status())

    async def _maybe_reconcile_pulls(self) -> None:
        """PRs merged or closed on GitHub while their task waits on a decision (every 2 minutes)."""
        import time

        if time.monotonic() - getattr(self, "_last_pull_reconcile", float("-inf")) < 120:
            return
        self._last_pull_reconcile = time.monotonic()
        from ..reconcile import reconcile_pull_requests

        try:
            await reconcile_pull_requests()
        except Exception:
            log.exception("pull request reconciliation failed")

    async def _maybe_reconcile_costs(self) -> None:
        """Price finished runs whose cost was not known yet (MindsHub usage can lag behind the run)."""
        import time
        from datetime import timedelta

        if time.monotonic() - getattr(self, "_last_cost_reconcile", float("-inf")) < 600:
            return
        self._last_cost_reconcile = time.monotonic()
        from ..inference import InferenceError, ModelClient, price_for, resolve_provider
        from sqlalchemy import and_, func, or_

        from ..models import Run, ScorerAttempt
        from ..scorers import aggregate

        since = utcnow() - timedelta(days=7)
        with session_scope() as s:
            pending = [(Run, r.id, r.workspace_id) for r in s.query(Run).filter(
                Run.cost_known.is_(False), Run.provider == "mindshub", Run.finished_at.isnot(None),
                Run.finished_at >= since).limit(50)]
            pending += [(ScorerAttempt, a.id, a.workspace_id) for a in s.query(ScorerAttempt).filter(
                ScorerAttempt.cost_known.is_(False), ScorerAttempt.finished_at.isnot(None),
                or_(ScorerAttempt.provider == "mindshub",
                    and_(ScorerAttempt.provider == "openai", ScorerAttempt.usage_known.is_(True))),
                or_(ScorerAttempt.cost_retry_at.is_(None), ScorerAttempt.cost_retry_at <= utcnow())
            ).order_by(func.coalesce(ScorerAttempt.cost_retry_at, ScorerAttempt.finished_at),
                       ScorerAttempt.id).limit(50)]
        priced = 0
        clients: dict[tuple[str, str | None], Any] = {}
        for kind, run_id, workspace_id in pending:
            try:
                # A judge attempt may outlive a workspace's provider selection. Never
                # query a different provider (or silently fall back to another key).
                recorded_provider = None
                if kind is ScorerAttempt:
                    with session_scope() as s:
                        attempt = s.get(ScorerAttempt, run_id)
                        # Advance even on absent credentials, missing pricing or provider errors:
                        # an unpriceable page must not starve later attempts. Keep retrying
                        # (capped at one day); manual billing reconciliation is always available.
                        attempt.cost_retry_count = (attempt.cost_retry_count or 0) + 1
                        attempt.cost_retry_at = utcnow() + timedelta(
                            seconds=min(86400, 600 * 2 ** min(attempt.cost_retry_count - 1, 8)))
                        recorded_provider = attempt.provider
                        if recorded_provider == "openai":
                            # OpenAI exposes no session billing API. Reprice retained usage
                            # after pricing is configured; otherwise retain honest unknown
                            # cost for an admin to reconcile from provider billing.
                            ws = s.get(Workspace, workspace_id)
                            price = price_for(attempt.model, ws.settings or {})
                            if attempt.usage_known and price is not None:
                                attempt.cost_usd = round((attempt.input_tokens * price[0]
                                                          + attempt.output_tokens * price[1]) / 1e6, 6)
                                attempt.cost_known = True
                                aggregate(s, attempt.result_id)
                                priced += 1
                            else:
                                log.info("scorer %s requires manual OpenAI billing reconciliation", run_id)
                            continue
                key = (workspace_id, recorded_provider)
                if key not in clients:
                    if kind is ScorerAttempt:
                        provider = resolve_provider(workspace_id, required_provider=recorded_provider)
                    else:
                        provider = resolve_provider(workspace_id)
                    clients[key] = ModelClient(provider)
                cost = await clients[key].session_cost(run_id)
            except (InferenceError, Exception):  # noqa: BLE001 - best effort
                if kind is ScorerAttempt:
                    log.info("scorer cost reconciliation deferred for %s: recorded provider unavailable", run_id)
                continue
            if cost is None:
                continue
            with session_scope() as s:
                run = s.get(kind, run_id)
                run.cost_usd, run.cost_known = round(cost, 6), True
                if kind is ScorerAttempt:
                    aggregate(s, run.result_id)
                else:
                    notify(run.workspace_id, "outcome", run.outcome_id)
            priced += 1
        if priced:
            log.info("priced %d run(s) from provider usage", priced)

    def _maybe_analyze(self) -> None:
        """Refresh improvement proposals from delivery history (every 30 minutes by default)."""
        import time

        interval = int(os.environ.get("AIFACTORY_INSIGHT_INTERVAL_SECONDS", "1800"))
        if interval <= 0 or time.monotonic() - getattr(self, "_last_analysis", float("-inf")) < interval:
            return
        self._last_analysis = time.monotonic()
        from .. import insights

        with session_scope() as s:
            ids = [w.id for w in s.query(Workspace)]
        for workspace_id in ids:
            try:
                with session_scope() as s:
                    result = insights.analyze(s, workspace_id)
                if result["created"] or result["updated"]:
                    log.info("improvements %s %s", workspace_id[:6], result)
            except Exception:
                log.exception("improvement analysis failed for %s", workspace_id)

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
        from ..linear_intake import sync_linear

        with session_scope() as s:
            targets = {r.workspace_id for r in s.query(Repository)}
            targets |= {w.id for w in s.query(Workspace) if (w.settings or {}).get("linear", {}).get("enabled")}
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
            db = SessionLocal()
            try:
                result = await sync_linear(db, workspace_id)
                if result.get("accepted"):
                    self.wake()
            except Exception:
                log.exception("Linear intake failed for %s", workspace_id)
            finally:
                db.close()

    async def _maybe_inventory(self) -> None:
        import time

        if time.monotonic() - getattr(self, "_last_inventory", float("-inf")) < 300:
            return
        self._last_inventory = time.monotonic()
        await asyncio.to_thread(self.log_inventory)
        await self._provider_readiness()

    async def _provider_readiness(self) -> None:
        """Log whether each workspace's model provider answers and serves the configured models."""
        from ..inference import InferenceError, ModelClient, resolve_provider, workspace_models

        with session_scope() as s:
            workspace_ids = [w.id for w in s.query(Workspace).all()]
        for workspace_id in workspace_ids:
            try:
                provider = resolve_provider(workspace_id)
                names = await ModelClient(provider).list_models()
                models = workspace_models(workspace_settings(workspace_id))
                wanted = {provider.model_override} if provider.model_override else {models.planner, models.builder, models.verifier}
                missing = sorted(m for m in wanted if m and m not in names)
                log.info("readiness %s provider=%s models=%d configured=%s missing=%s", workspace_id[:6],
                         provider.provider, len(names), sorted(wanted), missing or "none")
            except InferenceError as exc:
                log.warning("readiness %s provider unavailable: %s", workspace_id[:6], exc)
            except Exception as exc:  # readiness must never break scheduling
                log.warning("readiness %s check failed: %s", workspace_id[:6], type(exc).__name__)

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
                self._rollup(outcome, tasks, s)

    def _rollup(self, outcome: Outcome, tasks: list[Task], session=None) -> None:
        if not tasks:
            return
        statuses = {t.status for t in tasks}
        if statuses <= {"completed"}:
            outcome.status = "completed"
            outcome.completed_at = utcnow()
            outcome.summary = f"All {len(tasks)} task(s) merged and verified."
            record(outcome.workspace_id, "outcome_completed", f"Outcome #{outcome.number} delivered",
                   outcome_id=outcome.id, level="success", session=session)
            notify(outcome.workspace_id, "outcome", outcome.id)
            return
        if statuses <= {"cancelled", "abandoned"}:
            outcome.status = "cancelled"
            notify(outcome.workspace_id, "outcome", outcome.id)
            return
        if statuses <= TERMINAL_TASK:
            outcome.status = "partially_completed"
            outcome.completed_at = utcnow()
            handed = sum(t.status == "handed_over" for t in tasks)
            outcome.summary = (f"{sum(t.status == 'completed' for t in tasks)} of {len(tasks)} task(s) delivered"
                               + (f"; {handed} handed over to people." if handed else "."))
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
            workspaces = {w.id: w for w in s.query(Workspace)}
            limits = {wid: merged_settings(w.settings) for wid, w in workspaces.items()}
            planning = [Candidate(o.id, o.workspace_id, o.priority, o.created_by, o.created_at)
                        for o in s.query(Outcome).filter(Outcome.status == "planning")]
            queued = [Candidate(t.id, t.workspace_id, o.priority, o.created_by, t.created_at,
                                0 if t.started_at is not None or t.stage or t.attempt > 0 else 1)
                      for t, o in s.query(Task, Outcome).join(Outcome, Task.outcome_id == Outcome.id)
                      .filter(Task.status == "queued")]
            planning_by_ws: dict[str, int] = {}
            for outcome in s.query(Outcome).filter(Outcome.id.in_(list(self._planning))):
                planning_by_ws[outcome.workspace_id] = planning_by_ws.get(outcome.workspace_id, 0) + 1
            running_by_ws: dict[str, int] = {}
            for task in s.query(Task).filter(Task.id.in_(list(self._tasks))):
                running_by_ws[task.workspace_id] = running_by_ws.get(task.workspace_id, 0) + 1

        def dispatch(candidates, active, counts, global_limit, limit_key, turns, spawn):
            while len(active) < global_limit:
                eligible = [c for c in candidates if c.id not in active
                            and c.workspace_id in workspaces and not workspaces[c.workspace_id].paused
                            and counts.get(c.workspace_id, 0) < limits[c.workspace_id][limit_key]]
                if not eligible:
                    break
                candidate = turns.choose(eligible)
                spawn(candidate.id)
                turns.dispatched(candidate)
                counts[candidate.workspace_id] = counts.get(candidate.workspace_id, 0) + 1
                candidates.remove(candidate)

        dispatch(planning, self._planning, planning_by_ws, settings.global_planning_concurrency,
                 "planning_concurrency", self._planning_turns, self._spawn_plan)
        dispatch(queued, self._tasks, running_by_ws, settings.global_concurrency,
                 "concurrency", self._delivery_turns, self._spawn_task)

    def _spawn_plan(self, outcome_id: str) -> None:
        cancel = asyncio.Event()

        async def job() -> None:
            try:
                await plan_outcome(outcome_id, cancel)
            except asyncio.CancelledError:
                pass
            except Exception as exc:
                log.exception("planning crashed")
                try:
                    with session_scope() as s:
                        outcome = s.get(Outcome, outcome_id)
                        outcome.status = "blocked"
                        outcome.blocked_reason = f"Planning crashed: {exc}"[:2000]
                        ws = outcome.workspace_id
                    open_decision(ws, "unblock", "Planning crashed", str(exc)[:2000], outcome_id=outcome_id,
                                  options=[{"value": "retry", "label": "Retry planning"}, {"value": "cancel", "label": "Cancel"}])
                except Exception:
                    log.exception("could not record the planning crash")
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
                try:
                    with session_scope() as s:
                        task = s.get(Task, task_id)
                        task.status = "blocked"
                        task.blocked_reason = f"{type(exc).__name__}: {exc}"[:2000]
                    open_decision(ws, "unblock", "A task stopped unexpectedly", f"{type(exc).__name__}: {exc}"[:2000],
                                  outcome_id=outcome_id, task_id=task_id,
                                  options=[{"value": "retry", "label": "Retry"}, {"value": "abandon", "label": "Abandon"}])
                except Exception:
                    log.exception("could not record the task crash")
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
