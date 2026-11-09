"""Process-wide credential admission, shared by direct clients and the gateway.

Only counters/timestamps live in the registry, never credentials or asyncio primitives.
A short threading lock makes admission atomic even across TestClient/event-loop threads.
Waiters own nothing until both limits allow an actual send; cancellation needs no rollback.
"""
from __future__ import annotations

import asyncio
import hashlib
import threading
import time
from collections import deque
from contextlib import asynccontextmanager
from typing import Callable, TYPE_CHECKING

if TYPE_CHECKING:
    from .inference import ProviderConfig


class Permit:
    def __init__(self, guard: AdmissionGuard):
        self.guard = guard
        self.released = False

    def release(self) -> None:
        with self.guard._lock:
            if not self.released:
                self.guard.active -= 1
                self.released = True


class AdmissionGuard:
    def __init__(self, *, clock: Callable[[], float] = time.monotonic):
        self.clock = clock
        self._lock = threading.Lock()
        self._sends: deque[float] = deque()
        self.active = 0

    async def acquire(self, rpm: int, report=None) -> Permit:
        if rpm <= 0:
            raise ValueError("Requests per minute must be positive")
        started = self.clock()
        throttled = False
        while True:
            with self._lock:
                now = self.clock()
                while self._sends and self._sends[0] <= now - 60:
                    self._sends.popleft()
                reason = "concurrency" if self.active >= 16 else "request_rate"
                admitted = self.active < 16 and len(self._sends) < rpm
                if admitted:
                    self.active += 1
                    self._sends.append(now)
                    permit = Permit(self)
                delay = min(0.1, max(0.001, self._sends[0] + 60 - now)) if self._sends else 0.1
            if admitted:
                try:
                    if throttled and report:
                        report("inference_admitted", "Model request admitted after waiting",
                               {"wait_seconds": max(0.0, now - started)})
                except BaseException:
                    permit.release()
                    raise
                return permit
            if not throttled:
                throttled = True
                if report:
                    report("inference_throttled", "Waiting for model request capacity", {"reason": reason})
            await asyncio.sleep(delay)


_registry: dict[tuple[str, str, str], AdmissionGuard] = {}
_registry_lock = threading.Lock()


def provider_guard(provider: ProviderConfig) -> AdmissionGuard:
    identity = (provider.provider, provider.base_url.rstrip("/"),
                hashlib.sha256(provider.api_key.encode()).hexdigest())
    with _registry_lock:
        return _registry.setdefault(identity, AdmissionGuard())


def run_reporter(run_id: str | None):
    def report(kind, message, data):
        if not run_id:
            return
        from .db import session_scope
        from .events import record
        from .models import Run

        with session_scope() as s:
            run = s.get(Run, run_id)
            if run is not None:
                record(run.workspace_id, kind, message, run_id=run.id, outcome_id=run.outcome_id,
                       task_id=run.task_id, data=data, session=s)
    return report


def provider_rpm(provider: ProviderConfig) -> int:
    from .config import get_settings

    settings = get_settings()
    return settings.mindshub_requests_per_minute if provider.provider == "mindshub" else settings.openai_requests_per_minute


async def acquire(provider: ProviderConfig, run_id: str | None = None) -> Permit:
    return await provider_guard(provider).acquire(provider_rpm(provider), run_reporter(run_id))


@asynccontextmanager
async def admission(provider: ProviderConfig, run_id: str | None = None):
    permit = await acquire(provider, run_id)
    try:
        yield
    finally:
        permit.release()
