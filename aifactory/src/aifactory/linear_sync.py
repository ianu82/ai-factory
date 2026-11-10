"""Independent, bounded Linear intake and durable outbound reconciliation."""

from __future__ import annotations

import asyncio
import logging
import os
import time

from . import linear_config
from .db import session_scope
from .events import notify
from .linear import LinearError
from .linear_intake import _admit
from .linear_outbox import Dispatcher, _category, warning
from .models import Workspace

log = logging.getLogger(__name__)

WORKSPACE_TIMEOUT_SECONDS = 60
WORKSPACE_CONCURRENCY = 4
RETRY_SECONDS = 30


def workspace_deadline(workspace_id: str) -> asyncio.Timeout:
    """The deadline for one workspace's sync, so one stalled workspace never holds up the others."""
    return asyncio.timeout(WORKSPACE_TIMEOUT_SECONDS)


class LinearSync:
    def __init__(self, provider=None, wake=None):
        self.provider = provider or linear_config.provider
        self.wake = wake
        self._lock = asyncio.Lock()
        self._last_intake = {}
        self._retry_after = {}

    def _warning(self, workspace_id, exc):
        with session_scope() as db:
            warning(db, workspace_id, _category(exc))

    async def _intake(self, workspace_id):
        with session_scope() as db:
            workspace = db.get(Workspace, workspace_id)
            config = linear_config.saved_config(workspace)
            if (not config.enabled or workspace.paused
                    or (workspace.settings or {}).get("signals", {}).get("auto_sync") is False):
                return
        issues = await self.provider(workspace_id).issues(config.team_id, config.states.queued, config.intake_label)
        accepted = False
        for issue in issues:
            with session_scope() as db:
                workspace = db.get(Workspace, workspace_id)
                if (workspace.paused
                        or (workspace.settings or {}).get("signals", {}).get("auto_sync") is False):
                    break
                _created, admitted = _admit(db, workspace_id, issue, config)
                accepted |= admitted
        if accepted:
            notify(workspace_id, "signal", "sync")
            if self.wake:
                self.wake()

    async def cycle(self):
        if self._lock.locked():
            return
        async with self._lock:
            with session_scope() as db:
                workspace_ids = [workspace.id for workspace in db.query(Workspace)
                                 if (workspace.settings or {}).get("linear", {}).get("enabled")]
            interval = int(os.environ.get("AIFACTORY_SIGNAL_INTERVAL_SECONDS", "180"))
            semaphore = asyncio.Semaphore(WORKSPACE_CONCURRENCY)

            async def sync_workspace(workspace_id):
                async with semaphore:
                    if time.monotonic() < self._retry_after.get(workspace_id, 0):
                        return
                    try:
                        async with workspace_deadline(workspace_id):
                            errors = await Dispatcher(self.provider).dispatch(workspace_id)
                            if (interval <= 0 or "rate_limit" in errors
                                    or time.monotonic() - self._last_intake.get(workspace_id, float("-inf")) < interval):
                                return
                            await self._intake(workspace_id)
                            self._last_intake[workspace_id] = time.monotonic()
                    except TimeoutError:
                        self._retry_after[workspace_id] = time.monotonic() + RETRY_SECONDS
                        self._warning(workspace_id, LinearError("timeout"))
                    except Exception as exc:
                        self._retry_after[workspace_id] = time.monotonic() + RETRY_SECONDS
                        self._warning(workspace_id, exc)

            async with asyncio.TaskGroup() as workers:
                for workspace_id in workspace_ids:
                    workers.create_task(sync_workspace(workspace_id))

    async def run(self):
        while True:
            try:
                await self.cycle()
            except Exception:
                log.warning("Linear background reconciliation will retry")
            await asyncio.sleep(3)
