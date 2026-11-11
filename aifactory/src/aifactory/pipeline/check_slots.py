"""Host-wide slots for running a repository's checks.

Checks run a repository's configured commands, usually its whole test suite, in local sandboxes on the
control-plane host: the candidate gate after a build, revalidation after a rebase, and smoke after a merge. Every
job shares that host's CPUs and process limit. When many builds finish together, their suites starve each other
and all hit the command timeout, which fails the attempt and sends the task back to its builder having learned
nothing. A check holds a slot instead, and the extra checks wait their turn. A command's timeout starts only once
its check holds a slot.

``AIFACTORY_CHECK_CONCURRENCY`` sets the number of slots. By default there is one per ``CPUS_PER_CHECK`` CPUs the
Factory may use (the lower of its cgroup quota and the CPUs it may run on), and at least one.
"""

from __future__ import annotations

import asyncio
import os
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager

from ..sandbox import host_capacity

CPUS_PER_CHECK = 6


def check_slot_count() -> int:
    raw = os.environ.get("AIFACTORY_CHECK_CONCURRENCY", "").strip()
    if raw:
        try:
            return max(1, int(raw))
        except ValueError:
            pass
    capacity = host_capacity()
    limits = [cpus for cpus in (capacity.get("cpu_quota"), _cpu_set() or capacity.get("cpus")) if cpus]
    return max(1, int(min(limits, default=1) // CPUS_PER_CHECK))


def _cpu_set() -> int | None:
    """The CPUs this process may run on (its affinity or cpuset), which can be fewer than the host has."""
    try:
        return len(os.sched_getaffinity(0)) or None
    except (AttributeError, OSError):
        return None


class _Slots:
    def __init__(self, loop: asyncio.AbstractEventLoop, size: int):
        self.loop = loop
        self.size = size
        self.semaphore = asyncio.Semaphore(size)
        self.running = 0
        self.waiting = 0


_slots: _Slots | None = None


def _current() -> _Slots:
    """The running event loop's slots. The server has one loop; tests start a fresh one per asyncio.run."""
    global _slots
    loop = asyncio.get_running_loop()
    if _slots is None or _slots.loop is not loop:
        _slots = _Slots(loop, check_slot_count())
    return _slots


def check_slot_usage() -> dict[str, int]:
    """Slots in use and checks waiting, for status reports. Zero before the first check."""
    slots = _slots
    if slots is None:
        return {"size": check_slot_count(), "running": 0, "waiting": 0}
    return {"size": slots.size, "running": slots.running, "waiting": slots.waiting}


@asynccontextmanager
async def check_slot(on_wait: Callable[[int, int], None] | None = None) -> AsyncIterator[None]:
    """Hold one check slot for the block. ``on_wait(running, waiting)`` is called once if every slot is taken."""
    slots = _current()
    if on_wait is not None and slots.semaphore.locked():
        on_wait(slots.running, slots.waiting + 1)
    slots.waiting += 1
    try:
        await slots.semaphore.acquire()
    finally:
        slots.waiting -= 1
    slots.running += 1
    try:
        yield
    finally:
        slots.running -= 1
        slots.semaphore.release()
