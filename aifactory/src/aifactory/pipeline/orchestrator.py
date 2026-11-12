"""The orchestrator: schedules planning and task delivery with bounded concurrency.

- Outcomes in ``planning`` get one planner. A new outcome is first planned only while fewer than the workspace's
  ``limits.open_outcomes`` are open, unless it repairs the line or is urgent.
- Tasks become ``queued`` once their dependencies are complete; queued tasks start while the
  workspace (and the server) has free slots. Independent tasks run in parallel.
- Merges into the same repository are serialized; everything else overlaps.
- On restart, work that was running resumes from its last durable stage.
"""

from __future__ import annotations

import asyncio
import logging
import os
import time
from typing import Any

from .. import merge_manager
from ..config import get_settings
from ..db import session_scope, utcnow
from ..events import notify, record
from ..models import Decision, Outcome, Plan, Run, Signal, Task, User, Workspace
from ..sandbox import disk_report, host_capacity, process_report, purge_quarantine, quarantine_orphaned_jobs
from .check_slots import check_slot_count, check_slot_usage
from .delivery import Stop, TaskRunner
from .holds import held_workspaces
from .lifecycle import ACCEPTING, ENDED, ENDED_AS, accepted_after_merge, complete_accepted, stranded, transition
from .planning import ACTIVE_OUTCOME, plan_outcome
from .runtime import merged_settings, open_decision, publish_decision, workspace_settings
from .scheduling import Candidate, RequesterTurns

log = logging.getLogger(__name__)

ACTIVE_TASK = {"queued", "running", "waiting_ci"}
DONE_TASK = {"completed"}
TERMINAL_TASK = {"completed", "abandoned", "cancelled", "reverted", "handed_over"}
HUMAN_TASK = {"needs_input", "awaiting_approval", "blocked", "failed", "smoke_failed"}
# A task that waits on a person with no open decision for this long gets one (the scheduler's own sweep). The wait
# covers the places that set the status and open the decision in two transactions.
UNASKED_AFTER = 120
WAITS_AS = {"needs_input": "is waiting for an answer", "awaiting_approval": "is waiting for an approval",
            "blocked": "is blocked", "failed": "failed",
            "smoke_failed": "was merged, and its checks after the merge failed"}
RETRY_OR_ABANDON = [{"value": "retry", "label": "Retry"}, {"value": "abandon", "label": "Abandon task"}]
AFTER_SMOKE = [{"value": "revert", "label": "Revert the merge"}, {"value": "fix", "label": "Create a fix outcome"},
               {"value": "acknowledge", "label": "Acknowledge"}]
# A revert that did not go through: accepting cannot complete the task (the merge is still in), so the answers are
# to try the revert again or to end the task.
REVERT_AGAIN = [{"value": "revert", "label": "Try the revert again"}, {"value": "abandon", "label": "Abandon task"}]
STRANDED_OPTIONS = [{"value": "continue", "label": "Start the waiting tasks"},
                    {"value": "drop", "label": "Drop the waiting tasks"}]
# A running task is leased to the instance that runs it. The instance extends the lease every HEARTBEAT_SECONDS;
# a lease that ran out belongs to nobody and any instance may requeue the task.
LEASE_SECONDS = 90
HEARTBEAT_SECONDS = LEASE_SECONDS / 3
# Work that repairs the line is never held by the cap on open outcomes (limits.open_outcomes): outcomes from a red
# main branch, the steward or a failed smoke run. Nor is work more urgent than Normal (priority 2).
REPAIR_SOURCES = merge_manager.REPAIR_SOURCES | {"smoke_failure"}
NORMAL_PRIORITY = 2
WAIT_TAIL = "This one is planned when one finishes."


def open_cap(settings: dict[str, Any]) -> int | None:
    cap = (settings.get("limits") or {}).get("open_outcomes")
    return cap if type(cap) is int and cap > 0 else None


def waiting_reason(count: int, cap: int) -> str:
    are = "outcome is" if count == 1 else "outcomes are"
    return f"{count} {are} open in this workspace (limit {cap}). {WAIT_TAIL}"


class Orchestrator:
    def __init__(self) -> None:
        self._purge_task: asyncio.Task | None = None
        import secrets
        import socket

        # Unique per process start: in a container the process id alone is the same number on every start.
        self.instance = f"{socket.gethostname()[:40]}-{os.getpid()}-{secrets.token_hex(3)}"
        self._wake = asyncio.Event()
        self._loop_task: asyncio.Task | None = None
        self._heartbeat_task: asyncio.Task | None = None
        self._planning: dict[str, tuple[asyncio.Task, asyncio.Event]] = {}
        self._ci_polls: dict[str, asyncio.Task] = {}
        self._tasks: dict[str, tuple[asyncio.Task, asyncio.Event]] = {}
        self._planning_turns = RequesterTurns()
        self._delivery_turns = RequesterTurns()
        self._repo_locks: dict[str, asyncio.Lock] = {}
        self._stopping = False
        self._last_signal_sync = float("-inf")
        self._linear_task: asyncio.Task | None = None
        from ..operator_relay import Relay

        self._relay = Relay(wake=self.wake)  # operator commands from the bootstrap's control issue, live
        self._relay_task: asyncio.Task | None = None
        self._vm_sweep_task: asyncio.Task | None = None
        self._leads: dict[str, asyncio.Task] = {}  # outcome id -> its Lead's run
        self._lead_check = float("-inf")
        self._directors: dict[str, asyncio.Task] = {}  # workspace id -> its Director's run
        self._director_check = float("-inf")
        self._last_watch = time.monotonic()  # the first pass waits a full interval: a restart settles first
        self._unasked: dict[str, float] = {}  # task id -> when it was first seen waiting on a person with no decision

    # lifecycle -----------------------------------------------------------------
    def start(self) -> None:
        if self._loop_task is None:
            log.info("instance %s", self.instance)
            log.info("capacity %s check_slots=%d", host_capacity(), check_slot_count())
            trash = quarantine_orphaned_jobs()
            if trash is not None:
                log.info("moved job directories left by the previous run aside: %s", trash.name)
            self._purge_task = asyncio.create_task(asyncio.to_thread(purge_quarantine), name="purge-jobs")
            self._recover()
            import time

            self._last_reclaim = time.monotonic()  # _recover just looked: the periodic pass starts an interval later
            self._loop_task = asyncio.create_task(self._run(), name="orchestrator")
            self._heartbeat_task = asyncio.create_task(self._beat(), name="lease-heartbeat")
            from ..linear_sync import LinearSync

            self._linear_task = asyncio.create_task(LinearSync(wake=self.wake).run(), name="linear-sync")
            self._relay_task = asyncio.create_task(self._relay.run(), name="operator-relay")

    async def stop(self) -> None:
        """Shutdown (e.g. a redeploy) interrupts work without cancelling it: tasks resume on restart."""
        self._stopping = True
        if self._heartbeat_task:
            self._heartbeat_task.cancel()
        jobs = [job for job, _cancel in list(self._planning.values()) + list(self._tasks.values())]
        for job in jobs:
            job.cancel()
        for job in list(self._ci_polls.values()):
            job.cancel()
        released = asyncio.create_task(self._release(jobs), name="release-leases")  # beside the rest of the shutdown
        for job in list(self._leads.values()) + list(self._directors.values()):
            job.cancel()
        await asyncio.gather(*list(self._leads.values()), *list(self._directors.values()), return_exceptions=True)
        if self._loop_task:
            self._loop_task.cancel()
        await asyncio.gather(*list(self._ci_polls.values()), return_exceptions=True)
        if self._linear_task:
            self._linear_task.cancel()
            await asyncio.gather(self._linear_task, return_exceptions=True)
        if self._relay_task:
            self._relay_task.cancel()
            await asyncio.gather(self._relay_task, return_exceptions=True)
        if self._vm_sweep_task:
            self._vm_sweep_task.cancel()
            await asyncio.gather(self._vm_sweep_task, return_exceptions=True)
        await released
        await asyncio.sleep(0)

    async def _release(self, jobs: list[asyncio.Task]) -> None:
        """The cancelled jobs put their work back in their handlers. Wait for that, bounded (a job stuck in a sandbox
        call must not hold the shutdown), then give up every lease this instance still holds: the next start requeues
        those tasks at once instead of waiting for the lease to run out."""
        if jobs:
            await asyncio.wait(jobs, timeout=10)
        try:
            with session_scope() as s:
                s.query(Task).filter(Task.lease_owner == self.instance, Task.status == "running").update(
                    {Task.lease_owner: "", Task.lease_expires_at: None}, synchronize_session=False)
        except Exception:
            log.exception("could not release this instance's task leases")

    def wake(self) -> None:
        try:
            self._wake.set()
        except RuntimeError:
            pass

    def repo_lock(self, repo_id: str) -> asyncio.Lock:
        return self._repo_locks.setdefault(repo_id, asyncio.Lock())

    def status(self) -> dict[str, Any]:
        with session_scope() as s:
            waiting = [t.id for t in s.query(Task).filter(Task.status == "waiting_ci")]
            running = [t.id for t in s.query(Task).filter(Task.id.in_(list(self._tasks)),
                                                        Task.status == "running")]
        return {"planning": list(self._planning), "running": running, "waiting_ci": waiting,
                "global_limit": get_settings().global_concurrency, "check_slots": check_slot_usage(),
                "leads": list(self._leads), "directors": list(self._directors), "instance": self.instance}

    def _recover(self) -> None:
        """Work that was mid-flight when the server stopped resumes from its durable stage."""
        with session_scope() as s:
            s.query(Task).filter(Task.status == "waiting_ci").update(
                {Task.lease_owner: "", Task.lease_expires_at: None})
        self._reclaim(start=True)
        from .lead import recover as recover_leads

        recover_leads()
        self._complete_accepted()

    def _reclaim(self, start: bool = False) -> list[str]:
        """Requeue the running tasks nobody holds: no owner, no expiry (rows from before leases had one) or a lease
        that ran out. A live lease is another instance's work. A task this instance runs is its own to finish, even
        when its heartbeat fell behind. Returns the ids of the tasks it requeued.

        Their owner died mid-session, so the sessions it left open are closed in the transaction that requeues the
        tasks: once that is committed another server may claim one and start a session of its own, and a later look
        could not tell the dead owner's session from that one. `start`: this server is starting, so the same
        transaction closes every other session nobody is running too (a planning session has no task to requeue)."""
        from sqlalchemy import or_

        now = utcnow()
        reclaimed: list[str] = []
        free = or_(Task.lease_owner == "", Task.lease_expires_at.is_(None), Task.lease_expires_at < now)
        with session_scope() as s:
            for task in s.query(Task).filter(Task.status == "running", free).all():
                if task.id in self._tasks:
                    continue
                owner, expired = task.lease_owner, task.lease_expires_at
                # Conditional: its owner may have renewed the lease, or moved the task on, since the read.
                if not s.query(Task).filter(Task.id == task.id, Task.status == "running", free).update(
                        {Task.status: "queued", Task.lease_owner: "", Task.lease_expires_at: None},
                        synchronize_session=False):
                    continue
                reclaimed.append(task.id)
                if owner and owner != self.instance and expired:
                    log.info("reclaimed %s from instance %s: its lease ran out %.0fs ago", task.key, owner,
                             (now - expired).total_seconds())
                record(task.workspace_id, "resumed",
                       f"{task.key} resumed after a restart (stage: {task.stage or 'start'})",
                       outcome_id=task.outcome_id, task_id=task.id, session=s)
            if start:
                self._close_interrupted_runs(session=s)
            elif reclaimed:
                self._close_interrupted_runs(reclaimed, session=s)
        return reclaimed

    def _maybe_reclaim(self) -> None:
        """A task whose owner died without releasing it is picked up once its lease has run out."""
        import time

        if time.monotonic() - getattr(self, "_last_reclaim", float("-inf")) < HEARTBEAT_SECONDS:
            return
        self._last_reclaim = time.monotonic()
        self._reclaim()  # this also closes the sessions a start left open for an owner that was still alive

    def _lease_end(self):
        from datetime import timedelta

        return utcnow() + timedelta(seconds=LEASE_SECONDS)

    def _heartbeat(self) -> None:
        """Extend the lease of every task this instance is running. A row it holds without a job is left to run out,
        so the periodic pass picks it up."""
        if not self._tasks:
            return
        with session_scope() as s:
            # updated_at is the task's last change; a heartbeat is not one.
            s.query(Task).filter(Task.id.in_(list(self._tasks)), Task.lease_owner == self.instance,
                                 Task.status == "running").update(
                {Task.lease_expires_at: self._lease_end(), Task.updated_at: Task.updated_at}, synchronize_session=False)

    async def _beat(self) -> None:
        """Its own task, not a pass of _run: one scheduler tick can outlast a lease (cost reconciliation and signal
        sync await providers call by call)."""
        while not self._stopping:
            await asyncio.sleep(HEARTBEAT_SECONDS)
            try:
                self._heartbeat()
            except Exception:  # a failed beat is retried at the next one; two may be missed before the lease runs out
                log.exception("lease heartbeat failed")

    def _close_interrupted_runs(self, tasks: list[str] | None = None, session=None) -> None:
        """A killed process never reaches run_agent's finally, so its sessions would stay "running" for good.
        Close them as interrupted, which is not a verdict on the work. Lead sessions are closed by lead.recover().

        At a start (`tasks` is None) that is every open session except those of a task another instance holds with a
        live lease: that session is running now, on the other server. They are closed when their task is reclaimed,
        if its owner dies without closing them (`tasks`: the ids being requeued). `session`: the transaction that
        requeues tasks, at a start as well, so a session another server starts after claiming one is never among
        them and the dead owner's sessions are never taken for that server's."""
        from contextlib import nullcontext

        from sqlalchemy import func, or_, select

        from ..models import Event, Run

        open_runs = (Run.status == "running", Run.role != "lead")
        if tasks is not None:
            open_runs += (Run.task_id.in_(tasks),)
        else:
            held = select(Task.id).where(Task.status == "running", Task.lease_owner.notin_(("", self.instance)),
                                         Task.lease_expires_at > utcnow())
            open_runs += (or_(Run.task_id.is_(None), Run.task_id.notin_(held)),)
        with (session_scope() if session is None else nullcontext(session)) as s:
            # Events are indexed by outcome, not by run; a session's events carry both.
            last_event = dict(s.query(Event.run_id, func.max(Event.created_at)).filter(
                Event.outcome_id.in_(select(Run.outcome_id).where(*open_runs)),
                Event.run_id.in_(select(Run.id).where(*open_runs))).group_by(Event.run_id).all())
            closed: dict[str, int] = {}
            for run in s.query(Run).filter(*open_runs).all():
                run.status, run.error = "interrupted", "Interrupted by a restart"
                # Not now: a session found days later would show a duration it never had.
                run.finished_at = last_event.get(run.id) or run.started_at
                # A request in flight when the process died was charged and never recorded, so what the session
                # recorded is a lower bound. The cost reconciliation asks MindsHub what its sessions really cost; no
                # other provider can say, and those stay unknown.
                run.cost_known = False
                closed[run.workspace_id] = closed.get(run.workspace_id, 0) + 1
            for workspace_id, count in closed.items():
                record(workspace_id, "runs_interrupted",
                       f"Closed {count} agent session{'s' if count != 1 else ''} that a restart interrupted",
                       data={"runs": count}, session=s)

    def _complete_accepted(self) -> None:
        """Repair: acknowledging (or opening a fix for) a failed post-merge check used to leave the merged task
        smoke_failed, so every task depending on it waited forever. Complete each one whose latest unblock decision
        accepted the failure. Idempotent: a completed task no longer matches."""
        with session_scope() as s:
            for task in s.query(Task).filter(Task.status == "smoke_failed").all():
                if not accepted_after_merge(task):
                    continue
                latest = (s.query(Decision).filter(Decision.task_id == task.id, Decision.kind == "unblock")
                          .order_by(Decision.created_at.desc()).first())
                if latest is None or latest.status != "resolved" or latest.resolution not in ACCEPTING:
                    continue
                outcome = s.get(Outcome, task.outcome_id)
                if outcome is None or outcome.status == "cancelled":
                    continue
                person = s.get(User, latest.resolved_by) if latest.resolved_by else None
                complete_accepted(s, task, (person.name or person.email) if person else "a person")
                log.info("completed %s: its failed post-merge check was accepted before this fix", task.id)

    # scheduling ----------------------------------------------------------------
    async def _run(self) -> None:
        while not self._stopping:
            try:
                self._maybe_reclaim()
                await asyncio.to_thread(self._promote)
                self._schedule()
                await self._maybe_sync_signals()
                await asyncio.to_thread(self._maybe_supervise)
                await asyncio.to_thread(self._maybe_analyze)
                await self._maybe_reconcile_costs()
                await self._maybe_reconcile_pulls()
                await self._maybe_inventory()
                await self._maybe_sweep_vms()
                await self._maybe_watch()
                await self._maybe_run_leads()
                await self._maybe_run_directors()
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

        try:
            log.info("inventory disk %s", disk_report())
        except OSError as exc:
            log.warning("inventory disk unavailable: %s", exc)
        try:
            log.info("inventory processes %s", process_report())
        except OSError as exc:
            log.warning("inventory processes unavailable: %s", exc)

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

    async def _maybe_watch(self) -> None:
        """Every few minutes, look for stuck work and repeated failures: wake Leads, raise alerts (watch.py)."""
        from .watch import WATCH_EVERY, run as watch

        if time.monotonic() - self._last_watch < WATCH_EVERY.total_seconds():
            return
        self._last_watch = time.monotonic()
        try:
            counts = await asyncio.to_thread(watch)
        except Exception:  # the watch must never stop scheduling
            log.exception("watch failed")
            return
        if any(counts.values()):
            log.info("watch %s", counts)

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

    def _maybe_supervise(self) -> None:
        """Best-effort, environment and workspace-policy throttled DB-only pass."""
        import time
        from datetime import timedelta
        from ..supervisor import sweep, timestamp
        from ..supervisor_policy import supervisor_policy

        try:
            interval = int(os.environ.get("AIFACTORY_SUPERVISOR_INTERVAL_SECONDS", "180"))
            tick = time.monotonic()
            if interval <= 0 or tick - getattr(self, "_last_supervisor", float("-inf")) < interval:
                return
            self._last_supervisor = tick
            with session_scope() as db:
                targets = [(w.id, w.settings) for w in db.query(Workspace).filter(Workspace.paused.is_(False))]
            now = utcnow()
            for workspace_id, settings in targets:
                try:
                    policy = supervisor_policy(settings if isinstance(settings, dict) else {})
                    state = settings.get("supervisor_state", {}) if isinstance(settings, dict) else {}
                    last = timestamp(state.get("last_sweep_at")) if isinstance(state, dict) else None
                    if last is None and not isinstance(settings, dict):
                        from ..models import Event
                        with session_scope() as db:
                            event = (db.query(Event).filter(Event.workspace_id == workspace_id,
                                Event.kind == "supervisor_sweep").order_by(Event.created_at.desc(), Event.id.desc()).first())
                            last = timestamp(((event.data or {}).get("state") or {}).get("last_sweep_at")) if event else None
                    if policy["mode"] == "off" or (last and timestamp(now) - last < timedelta(minutes=policy["interval_minutes"])):
                        continue
                    sweep(workspace_id, now)
                except Exception:
                    log.exception("supervisor sweep failed for %s", workspace_id)
        except Exception:
            log.exception("supervisor pass failed")

    async def _maybe_sync_signals(self) -> None:
        """Poll GitHub for issues and CI failures every few minutes (intake + feedback)."""
        import time

        interval = int(os.environ.get("AIFACTORY_SIGNAL_INTERVAL_SECONDS", "180"))
        if interval <= 0 or time.monotonic() - self._last_signal_sync < interval:
            return
        self._last_signal_sync = time.monotonic()
        from ..db import SessionLocal
        from ..github import GitHubError, resolve_credential as resolve_token
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

    async def _maybe_run_leads(self) -> None:
        """Start the Lead of each outcome that has something needing judgement (pipeline/lead.py), at most every 10
        seconds and within the server's Lead limit. A Lead runs beside the scheduler, never inside its tick."""
        import time

        from .lead import concurrency, due, run_lead

        free = concurrency() - len(self._leads)
        now = time.monotonic()
        if self._stopping or free <= 0 or now - self._lead_check < 10:
            return
        self._lead_check = now
        for item in (await asyncio.to_thread(due, frozenset(self._leads)))[:free]:
            outcome_id = item["outcome_id"]
            job = asyncio.create_task(run_lead(item["workspace_id"], outcome_id, item["reasons"], item["seen"],
                                               item["decisions"]), name=f"lead-{outcome_id[:8]}")
            self._leads[outcome_id] = job

            def done(finished: asyncio.Task, outcome_id: str = outcome_id) -> None:
                self._leads.pop(outcome_id, None)
                if not finished.cancelled() and finished.exception() is not None:
                    log.error("Lead of %s failed: %r", outcome_id, finished.exception())
                self.wake()

            job.add_done_callback(done)

    async def _maybe_run_directors(self) -> None:
        """Start the Director of each workspace that turned it on and has something new waiting on a person
        (pipeline/director.py), at most every 15 seconds and within the server's Director limit. It runs beside the
        scheduler, never inside its tick."""
        import time

        from .director import concurrency, due, run_director

        free = concurrency() - len(self._directors)
        now = time.monotonic()
        if self._stopping or free <= 0 or now - self._director_check < 15:
            return
        self._director_check = now
        for item in (await asyncio.to_thread(due, frozenset(self._directors)))[:free]:
            workspace_id = item["workspace_id"]
            job = asyncio.create_task(run_director(item), name=f"director-{workspace_id[:8]}")
            self._directors[workspace_id] = job

            def done(finished: asyncio.Task, workspace_id: str = workspace_id) -> None:
                self._directors.pop(workspace_id, None)
                if not finished.cancelled() and finished.exception() is not None:
                    log.error("Director of %s failed: %r", workspace_id, finished.exception())
                self.wake()

            job.add_done_callback(done)

    async def _maybe_sweep_vms(self) -> None:
        """Every 15 minutes, starting 10 minutes after startup (a deployment being replaced may still be closing its
        VMs): destroy sandbox VMs no job holds, and log how full the environment is. It runs beside the scheduler,
        never inside its tick. Orphans once filled Railway's cap and silently moved all work onto this host."""
        import time

        now = time.monotonic()
        first = getattr(self, "_vm_sweep_due", None)
        if first is None:
            self._vm_sweep_due = now + 600
            return
        if now < first or (self._vm_sweep_task is not None and not self._vm_sweep_task.done()):
            return
        self._vm_sweep_due = now + 900
        self._vm_sweep_task = asyncio.create_task(self._sweep_vms(), name="vm-sweep")

    async def _sweep_vms(self) -> None:
        from ..vm import sweep_orphans

        try:
            report = await sweep_orphans()
        except Exception:  # never let a sweep failure reach the scheduler
            log.exception("VM sweep failed")
            return
        if "skipped" not in report:
            log.info("inventory vms %s", report)

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
                models = workspace_models(workspace_settings(workspace_id), provider=provider.provider)
                wanted = ({provider.model_override} if provider.model_override else
                          {models.planner, models.plan_reviewer, models.builder, models.verifier})
                missing = sorted(m for m in wanted if m and m not in names)
                log.info("readiness %s provider=%s models=%d configured=%s missing=%s", workspace_id[:6],
                         provider.provider, len(names), sorted(wanted), missing or "none")
            except InferenceError as exc:
                log.warning("readiness %s provider unavailable: %s", workspace_id[:6], exc)
            except Exception as exc:  # readiness must never break scheduling
                log.warning("readiness %s check failed: %s", workspace_id[:6], type(exc).__name__)

    def _promote(self) -> None:
        """Advance pending tasks whose dependencies completed, roll up outcome status, and see that whatever waits
        on a person has a decision to answer."""
        opened: list[tuple[str, str, str, str, str | None]] = []
        with session_scope() as s:
            outcomes = s.query(Outcome).filter(Outcome.source != "review", Outcome.status.in_(["running", "attention"])).all()
            asked: dict[str, list[Decision]] = {}
            if outcomes:
                for d in s.query(Decision).filter(Decision.status == "open",
                                                  Decision.outcome_id.in_([o.id for o in outcomes])):
                    asked.setdefault(d.outcome_id, []).append(d)
            unasked: set[str] = set()
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
                opened += self._ask(s, outcome, tasks, asked.get(outcome.id, []), unasked)
            for task_id in [k for k in self._unasked if k not in unasked]:
                del self._unasked[task_id]
        for workspace_id, decision_id, title, outcome_id, task_id in opened:  # announced once they are committed
            publish_decision(workspace_id, decision_id, "unblock", title, outcome_id=outcome_id, task_id=task_id)

    @staticmethod
    def _open_decisions(s, outcome: Outcome) -> list[Decision]:
        """The outcome's open decisions, read again under the workspace's lock (the one open_decision takes, held
        until this pass commits): another instance's pass may have opened one since this pass first read them."""
        if s.bind.dialect.name == "sqlite":  # FOR UPDATE is ignored there; a write takes the lock
            s.query(Workspace).filter(Workspace.id == outcome.workspace_id).update(
                {Workspace.outcome_seq: Workspace.outcome_seq}, synchronize_session=False)
        s.query(Workspace).filter(Workspace.id == outcome.workspace_id).with_for_update().one()
        return (s.query(Decision).filter(Decision.status == "open", Decision.outcome_id == outcome.id)
                .populate_existing().all())

    def _ask(self, s, outcome: Outcome, tasks: list[Task], decisions: list[Decision], unasked: set[str],
             locked: bool = False) -> list[tuple[str, str, str, str, str | None]]:
        """Nothing waits on a person without a decision to answer. Two cases no other code asks about: tasks that
        can never start because what they depend on ended undelivered (one decision for the outcome), and a task
        left waiting on a person with no open decision. Returns the decisions opened, for announcing.

        A decision is only opened from what is read under the workspace's lock, so that two instances cannot both
        open it: the first time this pass is about to open one, it starts over with the decisions read there."""
        ws, opened = outcome.workspace_id, []
        whole = [d for d in decisions if not d.task_id]
        current = next((d for d in whole if d.kind == "unblock" and "stranded" in (d.metadata_json or {})), None)
        waiting, ended = stranded(tasks)
        if waiting:
            keys = {"waiting": [t.key for t in waiting], "ended": [t.key for t in ended]}
            names = ", ".join(keys["waiting"])
            title = (f"{names} cannot start: {', '.join(keys['ended'])} ended without being delivered")[:300]
            body = "\n".join([
                "These tasks wait for work that will not arrive by itself:", "",
                *[f"- {t.key} ({t.title}) waits for {', '.join(t.depends_on or [])}." for t in waiting], "",
                *[f"- {t.key} ({t.title}) {ENDED_AS[t.status]}." for t in ended], "",
                "Start the waiting tasks when what they needed is in the repository some other way (a pull request "
                "finished by hand, for example) or they can do without it; anything you write here is passed to "
                "the builders. Drop them otherwise: the outcome then ends with what was delivered.",
            ])
            if current is None:
                if not locked:
                    return self._ask(s, outcome, tasks, self._open_decisions(s, outcome), unasked, locked=True)
                decision = Decision(workspace_id=ws, kind="unblock", title=title, body=body, outcome_id=outcome.id,
                                    options=STRANDED_OPTIONS, metadata_json={"stranded": keys})
                s.add(decision)
                s.flush()
                record(ws, "tasks_stranded", f"{names} cannot start: {', '.join(keys['ended'])} ended without "
                       "being delivered. A decision asks whether to start or drop them", outcome_id=outcome.id,
                       level="warn", data=keys, session=s)
                opened.append((ws, decision.id, title, outcome.id, None))
            elif current.metadata_json.get("stranded") != keys:
                current.title, current.body = title, body
                current.metadata_json = {**current.metadata_json, "stranded": keys}
                notify(ws, "decision", current.id)
        elif current is not None:
            current.status, current.comment = "superseded", "Nothing waits behind an ended task any more."
            notify(ws, "decision", current.id)
        # An open question about the whole outcome (a budget, for example) covers its tasks. A supervisor's finding
        # and the decision above do not: answering them moves no waiting task.
        if any(d is not current and d.kind != "supervisor" for d in whole):
            return opened
        # Nor does a supervisor's finding about one task ask what to do with it.
        with_decision = {d.task_id for d in decisions if d.task_id and d.kind != "supervisor"}
        now = time.monotonic()
        for task in tasks:
            if task.status not in HUMAN_TASK or task.id in with_decision:
                continue
            unasked.add(task.id)
            if now - self._unasked.setdefault(task.id, now) < UNASKED_AFTER:
                continue
            if not locked:
                return self._ask(s, outcome, tasks, self._open_decisions(s, outcome), unasked, locked=True)
            waits, options = WAITS_AS[task.status], RETRY_OR_ABANDON
            if task.status == "smoke_failed" and accepted_after_merge(task):
                options = AFTER_SMOKE
            elif task.status == "smoke_failed":
                waits, options = "was merged, and reverting it did not go through", REVERT_AGAIN
            title = f"{task.key} {waits}, and nothing was asking what to do"[:300]
            body = (f"{task.key} ({task.title}) {waits}, with no open decision: nobody was being "
                    f"asked what to do next.\n\nRecorded reason: {task.blocked_reason or 'none'}")
            decision = Decision(workspace_id=ws, kind="unblock", title=title, body=body, outcome_id=outcome.id,
                                task_id=task.id, metadata_json={"unasked": task.status}, options=options)
            s.add(decision)
            s.flush()
            record(ws, "decision_missing", f"{task.key} {waits} with no open decision; one was opened",
                   outcome_id=outcome.id, task_id=task.id, level="warn", data={"status": task.status}, session=s)
            opened.append((ws, decision.id, title, outcome.id, task.id))
        return opened

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
        # A pending task is stalled when something it depends on is waiting on a person or ended undelivered.
        stalled: set[str] = {t.key for t in tasks if t.status in HUMAN_TASK | set(ENDED)}
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
        from sqlalchemy import exists, or_

        self._schedule_ci()
        settings = get_settings()
        now = utcnow().isoformat()
        with session_scope() as s:
            workspaces = {w.id: w for w in s.query(Workspace)}
            # The model provider refused these workspaces: nothing starts there until a person resumes the hold.
            on_hold = held_workspaces(s)
            limits = {wid: merged_settings(w.settings) for wid, w in workspaces.items()}
            caps = {wid: open_cap(ws_settings) for wid, ws_settings in limits.items()}
            # An outcome is open from its first planning until it ends. One query for every active outcome: whether
            # it was ever planned (a plan, or a planning session) and the signal it came from.
            planned = or_(exists().where(Plan.outcome_id == Outcome.id),
                          exists().where(Run.outcome_id == Outcome.id, Run.task_id.is_(None),
                                         Run.role.in_(("planner", "plan_reviewer")))).label("planned")
            open_by_ws: dict[str, int] = {}
            fresh: dict[str, tuple[str, str]] = {}  # never planned: id -> (workspace, blocked_reason)
            capped: set[str] = set()  # the fresh outcomes the cap applies to
            planning = []
            for o, was_planned, signal_source in (
                    s.query(Outcome, planned, Signal.source).outerjoin(Signal, Signal.id == Outcome.signal_id)
                    .filter(Outcome.source != "review", Outcome.status.in_(ACTIVE_OUTCOME))):
                first = o.status == "planning" and not was_planned and o.id not in self._planning
                if not first:
                    open_by_ws[o.workspace_id] = open_by_ws.get(o.workspace_id, 0) + 1
                if o.status != "planning":
                    continue
                planning.append(Candidate(o.id, o.workspace_id, o.priority, o.created_by, o.created_at))
                if first:
                    fresh[o.id] = (o.workspace_id, o.blocked_reason or "")
                    if o.priority >= NORMAL_PRIORITY and not {o.source, signal_source} & REPAIR_SOURCES:
                        capped.add(o.id)
            queued = [Candidate(t.id, t.workspace_id, o.priority, o.created_by, t.created_at,
                                0 if t.started_at is not None or t.stage or t.attempt > 0 else 1,
                                ci_continuation=bool((t.evidence or {}).get("ci_wait"))
                                and t.stage in {"merge", "ci_repair", "ci_refresh", "revert_merge"},
                                outcome_created_at=o.created_at)
                      for t, o in s.query(Task, Outcome).join(Outcome, Task.outcome_id == Outcome.id)
                      .filter(Outcome.source != "review", Task.status == "queued")
                      # A task requeued after an infrastructure failure waits out its backoff.
                      if ((t.evidence or {}).get("infra_retry_at") or "") <= now]
            # The merge manager hands out one merge turn per repository; the rest wait without a slot.
            held = merge_manager.held_back(s)
            queued = [c for c in queued if c.id not in held]
            planning_by_ws: dict[str, int] = {}
            for outcome in s.query(Outcome).filter(Outcome.id.in_(list(self._planning))):
                planning_by_ws[outcome.workspace_id] = planning_by_ws.get(outcome.workspace_id, 0) + 1
            running_by_ws: dict[str, int] = {}
            for task in s.query(Task).filter(Task.id.in_(list(self._tasks))):
                running_by_ws[task.workspace_id] = running_by_ws.get(task.workspace_id, 0) + 1

        def at_cap(workspace_id: str) -> bool:
            cap = caps.get(workspace_id)
            return cap is not None and open_by_ws.get(workspace_id, 0) >= cap

        def spawn_plan(outcome_id: str) -> None:
            self._spawn_plan(outcome_id)
            if outcome_id in fresh:  # open from now on
                workspace_id = fresh[outcome_id][0]
                open_by_ws[workspace_id] = open_by_ws.get(workspace_id, 0) + 1

        def dispatch(candidates, active, counts, global_limit, limit_key, turns, spawn, hold=lambda c: False):
            while len(active) < global_limit:
                eligible = [c for c in candidates if c.id not in active
                            and c.workspace_id in workspaces and not workspaces[c.workspace_id].paused
                            and c.workspace_id not in on_hold
                            and counts.get(c.workspace_id, 0) < limits[c.workspace_id][limit_key]
                            and not hold(c)]
                if not eligible:
                    break
                candidate = turns.choose(eligible)
                spawn(candidate.id)
                turns.dispatched(candidate)
                counts[candidate.workspace_id] = counts.get(candidate.workspace_id, 0) + 1
                candidates.remove(candidate)

        dispatch(planning, self._planning, planning_by_ws, settings.global_planning_concurrency,
                 "planning_concurrency", self._planning_turns, spawn_plan,
                 hold=lambda c: c.id in capped and at_cap(c.workspace_id))
        waiting = {oid: waiting_reason(open_by_ws[ws], caps[ws]) for oid, (ws, _) in fresh.items()
                   if oid in capped and oid not in self._planning and at_cap(ws)}
        changed = {oid: reason for oid, reason in waiting.items() if reason != fresh[oid][1]}
        cleared = [oid for oid, (_, reason) in fresh.items() if oid not in waiting and reason.endswith(WAIT_TAIL)]
        if changed or cleared:
            self._show_waiting(changed, cleared)
        dispatch(queued, self._tasks, running_by_ws, settings.global_concurrency,
                 "concurrency", self._delivery_turns, self._spawn_task)

    @staticmethod
    def _show_waiting(waiting: dict[str, str], cleared: list[str]) -> None:
        """Say on each outcome the cap holds why it waits (one event per wait), and clear that once it may start."""
        touched: list[tuple[str, str]] = []
        with session_scope() as s:
            for oid, reason in waiting.items():
                outcome = s.get(Outcome, oid)
                if outcome is None or outcome.status != "planning":
                    continue
                if not (outcome.blocked_reason or "").endswith(WAIT_TAIL):
                    record(outcome.workspace_id, "waiting_for_capacity", reason, outcome_id=oid, session=s)
                outcome.blocked_reason = reason
                touched.append((outcome.workspace_id, oid))
            for oid in cleared:
                outcome = s.get(Outcome, oid)
                if outcome is not None and (outcome.blocked_reason or "").endswith(WAIT_TAIL):
                    outcome.blocked_reason = ""
                    touched.append((outcome.workspace_id, oid))
        for workspace_id, oid in touched:
            notify(workspace_id, "outcome", oid)

    def _schedule_ci(self) -> None:
        # A done callback (not only coroutine finally) handles cancellation before
        # the first coroutine step. Prune here too so callback ordering cannot stall us.
        self._ci_polls = {tid: job for tid, job in self._ci_polls.items() if not job.done()}
        if len(self._ci_polls) >= 4:
            return
        now = utcnow().isoformat()
        with session_scope() as s:
            due = [t.id for t in s.query(Task).join(Workspace, Task.workspace_id == Workspace.id)
                   .filter(Task.status == "waiting_ci", Workspace.paused.is_(False))
                   .order_by(Task.updated_at, Task.id)
                   # A head waiting for its review (stage "review") is polled on the review wait's own clock.
                   if ((t.evidence or {}).get("review" if t.stage == "review" else "ci_wait") or {}).get(
                       "next_poll_at", "") <= now
                   and t.id not in self._ci_polls and t.id not in self._tasks][:4 - len(self._ci_polls)]
        for task_id in due:
            async def poll(tid=task_id):
                try:
                    await TaskRunner(tid, asyncio.Event(), self.repo_lock).poll_ci()
                except (Stop, asyncio.CancelledError):
                    pass
                except Exception:
                    log.exception("CI poll failed for %s", tid)
                finally:
                    self.wake()

            job = asyncio.create_task(poll(), name=f"ci-{task_id}")
            self._ci_polls[task_id] = job

            def done(finished, tid=task_id):
                if self._ci_polls.get(tid) is finished:
                    self._ci_polls.pop(tid, None)
                self.wake()

            job.add_done_callback(done)

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
        if self._stopping:  # a tick still in flight during shutdown must not take a lease nobody will release
            return
        cancel = asyncio.Event()
        with session_scope() as s:
            task = s.get(Task, task_id)
            if task.status != "queued" or not transition(s, task, status="running", lease_owner=self.instance,
                                                         lease_expires_at=self._lease_end()):
                return
            ws, outcome_id = task.workspace_id, task.outcome_id
        notify(ws, "task", task_id, outcome_id=outcome_id)

        async def job() -> None:
            try:
                await TaskRunner(task_id, cancel, self.repo_lock, self.instance).run()
            except asyncio.CancelledError:
                with session_scope() as s:
                    task = s.get(Task, task_id)
                    if task and task.status == "running":
                        transition(s, task, owner=self.instance, status="cancelled" if cancel.is_set() else "queued",
                                   lease_owner="")
            except Exception as exc:
                log.exception("task crashed")
                try:
                    with session_scope() as s:
                        task = s.get(Task, task_id)
                        if task.status not in ("queued", "running") or not transition(
                                s, task, owner=self.instance, status="blocked", lease_owner="",
                                blocked_reason=f"{type(exc).__name__}: {exc}"[:2000]):
                            return
                        decision = Decision(workspace_id=ws, kind="unblock", title="A task stopped unexpectedly",
                                            body=f"{type(exc).__name__}: {exc}"[:2000], outcome_id=outcome_id,
                                            task_id=task_id, options=[{"value": "retry", "label": "Retry"},
                                                                    {"value": "abandon", "label": "Abandon"}])
                        s.add(decision)
                        s.flush()
                        decision_id, title = decision.id, decision.title
                    publish_decision(ws, decision_id, "unblock", title, outcome_id=outcome_id, task_id=task_id)
                except Exception:
                    log.exception("could not record the task crash")
            finally:
                self._tasks.pop(task_id, None)
                asked: tuple[str, str] | None = None
                try:
                    with session_scope() as s:
                        task = s.get(Task, task_id)
                        if task and task.status == "running" and not self._stopping:
                            # Never silently loop: a runner that stops without a recorded state needs a look, and the
                            # decision that asks for it is written with the status.
                            if transition(s, task, owner=self.instance, status="blocked", lease_owner="",
                                          blocked_reason="The task stopped without recording an outcome."):
                                decision = Decision(
                                    workspace_id=ws, kind="unblock", outcome_id=outcome_id, task_id=task_id,
                                    title=f"{task.key} stopped without recording an outcome", options=RETRY_OR_ABANDON,
                                    body=f"The run of {task.key} ended at stage '{task.stage or 'start'}' without "
                                         "saying how it ended. Retry continues from that stage.")
                                s.add(decision)
                                s.flush()
                                asked = decision.id, decision.title
                        if task and task.status == "running":
                            # A workspace pause stops continuation at the next gate;
                            # retain the stage for resumption when it is unpaused.
                            s.query(Task).filter(Task.id == task_id, Task.status == "running",
                                                 Task.lease_owner.in_(("", self.instance)),
                                                 Task.stage == task.stage, Task.workspace_id.in_(
                                                     s.query(Workspace.id).filter(Workspace.paused.is_(True)))).update(
                                {Task.status: "queued"}, synchronize_session=False)
                        if task:
                            # Leases are execution-only, including pause/stop/error exits.
                            s.query(Task).filter(Task.id == task_id, Task.lease_owner == self.instance).update(
                                {Task.lease_owner: "", Task.lease_expires_at: None}, synchronize_session=False)
                except Exception:  # the lease runs out and the task is picked up again; nothing was asked
                    log.exception("could not record how the task ended")
                    asked = None
                if asked:
                    publish_decision(ws, asked[0], "unblock", asked[1], outcome_id=outcome_id, task_id=task_id)
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
        poll = self._ci_polls.get(task_id)
        if poll:
            poll.cancel()
        if task_id in self._tasks:
            job, cancel = self._tasks[task_id]
            cancel.set()
            job.cancel()


orchestrator = Orchestrator()
