"""Host-wide check slots: checks beyond the slots wait their turn instead of starving each other into timeouts."""

from __future__ import annotations

import asyncio

from aifactory.pipeline import check_slots, delivery
from aifactory.pipeline.check_slots import check_slot, check_slot_count, check_slot_usage


def test_slots_follow_the_cpus_the_host_allows(monkeypatch):
    monkeypatch.delenv("AIFACTORY_CHECK_CONCURRENCY", raising=False)
    monkeypatch.setattr(check_slots, "host_capacity", lambda: {"cpus": 48, "cpu_quota": 24.0})
    assert check_slot_count() == 4  # the cgroup quota, not the CPUs the container can see
    monkeypatch.setattr(check_slots, "host_capacity", lambda: {"cpus": 13})
    assert check_slot_count() == 2
    monkeypatch.setattr(check_slots, "host_capacity", lambda: {"cpus": 4})
    assert check_slot_count() == 1  # never zero
    monkeypatch.setenv("AIFACTORY_CHECK_CONCURRENCY", "3")
    assert check_slot_count() == 3
    monkeypatch.setenv("AIFACTORY_CHECK_CONCURRENCY", "0")
    assert check_slot_count() == 1
    monkeypatch.setenv("AIFACTORY_CHECK_CONCURRENCY", "lots")
    assert check_slot_count() == 1  # unreadable: the host's default


def test_checks_beyond_the_slots_wait_their_turn(monkeypatch):
    monkeypatch.setenv("AIFACTORY_CHECK_CONCURRENCY", "2")
    state = {"now": 0, "peak": 0}
    waits = []

    async def check(n):
        async with check_slot(on_wait=lambda running, waiting: waits.append((n, running, waiting))):
            state["now"] += 1
            state["peak"] = max(state["peak"], state["now"])
            await asyncio.sleep(0.02)
            state["now"] -= 1
        return n

    async def go():
        return await asyncio.gather(*(check(n) for n in range(5)))

    assert asyncio.run(go()) == [0, 1, 2, 3, 4]
    assert state["peak"] == 2
    # The first two start at once; each later one is told how many are running and waiting, including itself.
    assert waits == [(2, 2, 1), (3, 2, 2), (4, 2, 3)]
    assert check_slot_usage() == {"size": 2, "running": 0, "waiting": 0}


def test_a_check_cancelled_while_waiting_gives_up_its_place(monkeypatch):
    monkeypatch.setenv("AIFACTORY_CHECK_CONCURRENCY", "1")

    async def go():
        release = asyncio.Event()

        async def holder():
            async with check_slot():
                await release.wait()

        async def waiter():
            async with check_slot():
                raise AssertionError("a cancelled check must never run")

        first = asyncio.create_task(holder())
        await asyncio.sleep(0)
        second = asyncio.create_task(waiter())
        await asyncio.sleep(0.01)
        assert check_slot_usage() == {"size": 1, "running": 1, "waiting": 1}
        second.cancel()
        await asyncio.gather(second, return_exceptions=True)
        assert check_slot_usage() == {"size": 1, "running": 1, "waiting": 0}
        release.set()
        await first
        async with check_slot():  # the slot came back
            assert check_slot_usage()["running"] == 1

    asyncio.run(go())
    assert check_slot_usage()["running"] == 0


def test_the_task_reports_waiting_for_a_slot(monkeypatch):
    monkeypatch.setenv("AIFACTORY_CHECK_CONCURRENCY", "1")
    events = []

    class Runner:
        def event(self, kind, message, level="info", **data):
            events.append((kind, message, data))

    runner = Runner()

    async def go():
        release = asyncio.Event()

        async def holder():
            async with check_slot():
                await release.wait()

        first = asyncio.create_task(holder())
        await asyncio.sleep(0)
        async with delivery.TaskRunner._check_slot(runner, False):  # no checks configured: nothing to wait for
            pass
        assert events == []

        async def checks():
            async with delivery.TaskRunner._check_slot(runner):
                return check_slot_usage()["running"]

        second = asyncio.create_task(checks())
        await asyncio.sleep(0.01)
        assert [kind for kind, _, _ in events] == ["check_slot_wait"]
        release.set()
        await first
        return await second

    assert asyncio.run(go()) == 1  # the checks ran holding the slot
    assert [kind for kind, _, _ in events] == ["check_slot_wait", "check_slot"]
    kind, message, data = events[0]
    assert message == "Waiting for a check slot: 1 running on this server, 1 waiting"
    assert data == {"running": 1, "waiting": 1}
    assert events[1][2]["waited"] >= 0
