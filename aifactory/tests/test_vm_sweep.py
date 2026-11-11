"""Orphaned sandbox VMs are reclaimed: by the periodic sweep, and when creating a VM hits the environment's cap."""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone

import pytest

from aifactory import vm
from aifactory.sandbox import SandboxError

NOW = datetime(2026, 11, 11, 9, 0, tzinfo=timezone.utc)


def at(minutes_ago: int) -> str:
    return (NOW - timedelta(minutes=minutes_ago)).isoformat().replace("+00:00", "Z")


class Bridge:
    def __init__(self, sandboxes, create_errors=0):
        self.sandboxes = sandboxes
        self.create_errors = create_errors
        self.calls = []

    async def call(self, op, args=None, timeout=600):
        self.calls.append((op, (args or {}).get("id")))
        if op == "sandboxes":
            return {"items": [dict(s) for s in self.sandboxes]}
        if op == "forget":
            self.sandboxes = [s for s in self.sandboxes if s["id"] != args["id"]]
            return {"destroyed": True}
        if op == "create":
            if self.create_errors:
                self.create_errors -= 1
                raise SandboxError("VM bridge: RailwayGraphQLError: Too many running sandboxes in environment.")
            return {"id": "new", "region": "test"}
        if op == "destroy":
            return {"destroyed": True}
        raise AssertionError(op)


@pytest.fixture
def bridge(monkeypatch):
    def install(sandboxes, create_errors=0):
        fake = Bridge(sandboxes, create_errors)
        monkeypatch.setattr(vm.VMBridge, "get", classmethod(lambda cls: fake))
        monkeypatch.setattr(vm, "vm_available", lambda: (True, "test"))
        monkeypatch.setattr(vm, "_LIVE", set())
        return fake
    return install


def test_the_sweep_destroys_only_old_unowned_active_sandboxes(bridge):
    fake = bridge([{"id": "orphan", "status": "RUNNING", "createdAt": at(600)},
                   {"id": "owned", "status": "RUNNING", "createdAt": at(600)},
                   {"id": "starting", "status": "RUNNING", "createdAt": at(2)},  # within the grace period
                   {"id": "gone", "status": "DESTROYING", "createdAt": at(900)}])
    vm._LIVE.add("owned")
    report = asyncio.run(vm.sweep_orphans(NOW))
    assert [c for c in fake.calls if c[0] == "forget"] == [("forget", "orphan")]
    assert report == {"active": 2, "owned": 1, "destroyed": 1, "cap": vm.SANDBOX_CAP}


def test_the_sweep_never_raises_when_the_list_fails(bridge, monkeypatch):
    fake = bridge([])

    async def broken(op, args=None, timeout=600):
        raise SandboxError("VM bridge: Railway GraphQL 403")
    monkeypatch.setattr(fake, "call", broken)
    assert asyncio.run(vm.sweep_orphans(NOW)) == {"skipped": "list failed"}


def test_creating_at_the_cap_sweeps_orphans_and_retries_once(bridge):
    fake = bridge([{"id": "orphan", "status": "RUNNING", "createdAt": at(600)}], create_errors=1)
    box = asyncio.run(vm.RailwayVMSandbox.create())
    assert box.vm_id == "new" and "new" in vm._LIVE
    assert [op for op, _ in fake.calls] == ["create", "sandboxes", "forget", "create"]
    asyncio.run(box.close())
    assert "new" not in vm._LIVE  # a closed VM is no longer held, so a later sweep may reclaim it


def test_creating_at_the_cap_with_nothing_to_reclaim_fails_as_before(bridge):
    fake = bridge([], create_errors=1)
    with pytest.raises(SandboxError, match="Too many running sandboxes"):
        asyncio.run(vm.RailwayVMSandbox.create())
    assert [op for op, _ in fake.calls] == ["create", "sandboxes"]
