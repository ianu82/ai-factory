"""Durable delivery recovery against real local remotes and disposable sandboxes."""
from __future__ import annotations

import asyncio
from copy import deepcopy

import pytest

from aifactory import gitops
from aifactory.db import session_scope
from aifactory.models import Artifact, DeliveryCheckpoint, Outcome, Plan, Repository, Task, Workspace
from aifactory.pipeline import checkpoints, delivery
from aifactory.sandbox import LocalSandbox

from .fakes import FakeGitHub, git
from .test_pipeline import (factory, new_outcome, drive, outcome_status, _stop_once, _runner)


def task_for(factory, stage="build", attempt=3):
    outcome_id = new_outcome(factory)
    base = git("rev-parse", "main", cwd=factory["remotes"] / "acme/demo.git")
    with session_scope() as s:
        s.get(Outcome, outcome_id).status = "running"
        plan = Plan(outcome_id=outcome_id, version=1, status="approved", content={})
        s.add(plan)
        s.flush()
        prior = [{"n": n, "epoch": 7, "result": "builder_failed", "error": "real failure"}
                 for n in range(1, attempt)]
        task = Task(workspace_id=factory["ws"], outcome_id=outcome_id, plan_id=plan.id, repo_id=factory["repo"],
                    key="T1", title="Greeting", status="running", stage=stage, base_sha=base, branch="recovery",
                    attempt=attempt, evidence={"attempts": prior, "budget_epoch": 7, "attempt_base": 1,
                                               "auto_retries": 1, "fix_round": ""})
        s.add(task)
        s.flush()
        return task.id


class DestroyedVM:
    """Backend-neutral I/O, no path access; close permanently destroys the checkout."""
    kind = "vm"

    def __init__(self, local):
        self.local, self.closed = local, False

    async def exec(self, *args, **kwargs):
        assert not self.closed
        return await self.local.exec(*args, **kwargs)

    async def read_bytes_async(self, relative, limit=None, from_end=False):
        assert not self.closed
        with self.local.resolve(relative).open("rb") as handle:
            return handle.read(limit) if limit else handle.read()

    async def close(self):
        self.closed = True
        await self.local.close()


async def prepared_runner(task_id):
    runner = _runner(task_id)
    runner.builder = LocalSandbox("checkpoint-test").create()
    await gitops.checkout(runner.builder, runner.repo.full_name, "github-secret", runner.task.base_sha,
                          runner.task.branch)
    return runner


@pytest.mark.parametrize("stage", ["build", "checks", "verify"])
def test_roundtrip_after_vm_destruction_is_binary_safe_and_private(factory, monkeypatch, stage):
    task_id = task_for(factory, stage)
    # Large enough that a display artifact could not preserve this diff.
    binary = bytes(range(256)) * 500
    text = "line with non-ascii é\n" * 22000

    async def go():
        runner = await prepared_runner(task_id)
        root = runner.builder.work
        runner.builder.write_bytes("new.bin", binary)
        runner.builder.write_text("new.txt", text)
        runner.builder.write_text("greet.sh", "echo 'Hello, Factory!'\n")
        # Sandbox-supplied hooks/config are never copied or executed by control-plane Git.
        runner.builder.write_text(".git/hooks/post-checkout", "#!/bin/sh\ntouch /tmp/must-not-run\n")
        runner.builder_report = {"summary": "Original builder", "tests": ["evidence"], "runtime_pages": ["/changed"]}
        runner.checks = [{"command": "sh test.sh", "ok": True, "tail": "original check", "duration": 1}]
        before = deepcopy(runner.task.evidence)
        runner.patch = await gitops.extract_patch(runner.builder, runner.task.base_sha)
        # Bound policy is deliberately large for this recovery test, independent of publication policy.
        runner.settings["limits"]["max_changed_lines"] = 50000
        runner.builder = DestroyedVM(runner.builder)
        vm = runner.builder
        assert await checkpoints.park(runner)
        assert runner.task.attempt == 2
        assert await checkpoints.park(runner)  # idempotent refund
        with session_scope() as s:
            row = s.get(DeliveryCheckpoint, task_id)
            assert len(row.patch) > 400000
            assert row.data["reserved_attempt"] == 3
            assert row.data["consumed_before"] == 2
            assert s.query(Artifact).filter(Artifact.task_id == task_id).count() == 0
            for key in ("attempts", "budget_epoch", "attempt_base", "auto_retries"):
                assert s.get(Task, task_id).evidence[key] == before[key]
        await vm.close()
        assert not root.exists()
        resumed = _runner(task_id)
        resumed.settings["limits"]["max_changed_lines"] = 50000
        data = await checkpoints.resume(resumed, "github-secret")
        try:
            assert data["stage"] == stage and resumed.task.attempt == 3
            assert resumed.builder is not vm
            assert resumed.builder.read_text("new.txt", limit=1000000) == text
            assert resumed.builder.resolve("new.bin").read_bytes() == binary
            assert (await gitops.extract_patch(resumed.builder, resumed.task.base_sha)).diff == runner.patch.diff
            assert resumed.builder_report == runner.builder_report
            assert resumed.checks == (runner.checks if stage == "verify" else [])
            config = resumed.builder.resolve(".git/config").read_text()
            assert "github-secret" not in config and "extraheader" not in config.lower()
            assert not resumed.builder.resolve(".git/hooks/post-checkout").exists()
            for key in ("attempts", "budget_epoch", "attempt_base", "auto_retries"):
                assert resumed.task.evidence[key] == before[key]
            resumed.evidence("attempts", {"n": 3, "result": "verified"})
            with session_scope() as s:
                assert s.get(DeliveryCheckpoint, task_id) is None
                assert "delivery_checkpoint" not in s.get(Task, task_id).evidence
                assert s.get(Task, task_id).attempt == 3
        finally:
            await resumed.builder.close()

    asyncio.run(go())


@pytest.mark.parametrize("mutation", ["cancelled", "handed_over", "abandoned", "completed", "pause", "epoch",
                                       "attempt", "base", "round", "lease", "approval", "cancel_event"])
@pytest.mark.parametrize("operation", ["park", "resume"])
def test_lifecycle_changes_fence_checkpoint_work(factory, mutation, operation):
    task_id = task_for(factory)

    async def go():
        runner = await prepared_runner(task_id)
        runner.builder.write_text("partial.txt", "saved work")
        if operation == "resume":
            assert await checkpoints.park(runner)
            await runner.builder.close()
            runner.builder = None
        with session_scope() as s:
            task = s.get(Task, task_id)
            if mutation in ("cancelled", "handed_over", "abandoned", "completed"):
                task.status = mutation
            elif mutation == "pause":
                s.get(Workspace, task.workspace_id).paused = True
            elif mutation == "epoch":
                task.evidence = {**task.evidence, "budget_epoch": 8}
            elif mutation == "round":
                task.evidence = {**task.evidence, "fix_round": "revise"}
            elif mutation == "attempt":
                task.attempt = 10
            elif mutation == "base":
                task.base_sha = "0" * 40
            elif mutation == "lease":
                task.lease_owner = "another-instance"
                runner.owner = "original-instance"
            elif mutation == "approval":
                s.get(Plan, task.plan_id).status = "proposed"
            elif mutation == "cancel_event":
                runner.cancelled.set()
        try:
            with pytest.raises(delivery.Stop):
                if operation == "park":
                    await checkpoints.park(runner)
                else:
                    await checkpoints.resume(runner, "token")
            if operation == "resume":
                assert runner.builder is None  # refused before allocating any new sandbox
            with session_scope() as s:
                if operation == "park":
                    assert s.get(DeliveryCheckpoint, task_id) is None
        finally:
            if runner.builder:
                await runner.builder.close()

    asyncio.run(go())


def test_stale_epoch_never_restores_even_in_a_new_runner(factory):
    task_id = task_for(factory)

    async def go():
        runner = await prepared_runner(task_id)
        await checkpoints.park(runner)
        await runner.builder.close()
        with session_scope() as s:
            task = s.get(Task, task_id)
            task.evidence = {**task.evidence, "budget_epoch": 8}
        with pytest.raises(checkpoints.CheckpointError, match="Stale"):
            await checkpoints.resume(_runner(task_id), "token")

    asyncio.run(go())


@pytest.mark.parametrize("stage", ["build", "verify"])
def test_current_protected_path_policy_is_not_bypassed(factory, stage):
    task_id = task_for(factory, stage)

    async def go():
        runner = await prepared_runner(task_id)
        runner.builder.write_text("no-longer-allowed.txt", "saved work")
        await checkpoints.park(runner)
        await runner.builder.close()
        with session_scope() as s:
            repo = s.get(Repository, factory["repo"])
            repo.config = {**repo.config, "protected_paths": ["no-longer-allowed.txt"]}
        with pytest.raises(checkpoints.CheckpointError, match="protected"):
            await checkpoints.resume(_runner(task_id), "token")

    asyncio.run(go())


def test_interrupted_verification_never_rebuilds_and_uses_fresh_verifier(factory, monkeypatch):
    stopped = _stop_once(monkeypatch, "_verifier_sandbox")
    builds, reviews = [], []
    original = delivery.run_agent

    async def agent(spec, cancel):
        if spec.role == "builder":
            builds.append(spec.sandbox)
        if spec.role == "verifier":
            reviews.append(spec)
            assert spec.sandbox not in builds
        return await original(spec, cancel)

    monkeypatch.setattr(delivery, "run_agent", agent)
    outcome_id = new_outcome(factory)
    assert drive(lambda: outcome_status(outcome_id) in ("completed", "attention", "blocked"), timeout=90)
    assert stopped and len(builds) == 1 and len(reviews) == 1
    with session_scope() as s:
        task = s.query(Task).filter(Task.outcome_id == outcome_id).one()
        assert task.status == "completed", task.blocked_reason
        assert task.attempt == 1
        assert [(a["n"], a["result"]) for a in task.evidence["attempts"]] == [(1, "verified")]
        assert s.get(DeliveryCheckpoint, task.id) is None
    assert "Updated the greeting." in reviews[0].prompt
    assert "`sh test.sh` → passed" in reviews[0].prompt
    assert FakeGitHub.merged == [1]


@pytest.mark.parametrize("damage", ["missing", "corrupt", "workspace"])
def test_missing_or_corrupt_private_checkpoint_fails_closed(factory, damage):
    task_id = task_for(factory, "verify")

    async def go():
        runner = await prepared_runner(task_id)
        runner.builder.write_text("partial.txt", "saved work")
        await checkpoints.park(runner)
        await runner.builder.close()
        with session_scope() as s:
            row = s.get(DeliveryCheckpoint, task_id)
            if damage == "missing":
                s.delete(row)
            elif damage == "corrupt":
                row.patch = b"not the saved bytes"
            else:
                other = Workspace(name="Other workspace", slug="other",
                                  org_id=s.get(Workspace, factory["ws"]).org_id)
                s.add(other)
                s.flush()
                row.workspace_id = other.id
        resumed = _runner(task_id)
        with pytest.raises(checkpoints.CheckpointError):
            await checkpoints.resume(resumed, "token")
        assert resumed.builder is None
        with session_scope() as s:
            task = s.get(Task, task_id)
            assert task.attempt == 2 and len(task.evidence["attempts"]) == 2

    asyncio.run(go())


def test_oversized_checkpoint_is_never_truncated_or_refunded(factory, monkeypatch):
    task_id = task_for(factory)
    monkeypatch.setattr(checkpoints, "MAX_PATCH_BYTES", 30)

    async def go():
        runner = await prepared_runner(task_id)
        runner.builder.write_text("partial.txt", "saved work" * 100)
        try:
            with pytest.raises(delivery.SandboxError, match="checkpoint byte limit"):
                await checkpoints.park(runner)
            with session_scope() as s:
                assert s.get(DeliveryCheckpoint, task_id) is None
                assert s.get(Task, task_id).attempt == 3
                assert "delivery_checkpoint" not in s.get(Task, task_id).evidence
        finally:
            await runner.builder.close()

    asyncio.run(go())


def test_second_interruption_replaces_checkpoint_without_double_charging(factory):
    task_id = task_for(factory)

    async def go():
        runner = await prepared_runner(task_id)
        runner.builder.write_text("partial.txt", "first")
        await checkpoints.park(runner)
        await runner.builder.close()
        for content in ("second", "third"):
            runner = _runner(task_id)
            await checkpoints.resume(runner, "token")
            assert runner.task.attempt == 3
            runner.builder.write_text("partial.txt", content)
            await checkpoints.park(runner)
            await runner.builder.close()
            assert runner.task.attempt == 2
        runner = _runner(task_id)
        await checkpoints.resume(runner, "token")
        try:
            assert runner.builder.read_text("partial.txt") == "third"
            assert runner.task.attempt == 3
            with session_scope() as s:
                assert len(s.get(Task, task_id).evidence["attempts"]) == 2
                assert s.query(DeliveryCheckpoint).count() == 1
        finally:
            await runner.builder.close()

    asyncio.run(go())


def test_checks_checkpoint_uses_candidate_not_check_mutations(factory):
    task_id = task_for(factory, "checks")

    async def go():
        runner = await prepared_runner(task_id)
        runner.builder.write_text("greet.sh", "echo 'candidate'\n")
        runner.patch = await gitops.extract_patch(runner.builder, runner.task.base_sha)
        runner.builder.write_text("greet.sh", "echo 'rewritten by checks'\n")
        runner.builder.write_text("generated.txt", "check side effect")
        await checkpoints.park(runner)
        await runner.builder.close()
        resumed = _runner(task_id)
        await checkpoints.resume(resumed, "token")
        try:
            assert resumed.builder.read_text("greet.sh") == "echo 'candidate'\n"
            assert not resumed.builder.resolve("generated.txt").exists()
        finally:
            await resumed.builder.close()

    asyncio.run(go())


def test_moving_main_does_not_change_the_recovered_base(factory):
    from .test_pipeline import _land_on_main

    task_id = task_for(factory, "verify")

    async def go():
        runner = await prepared_runner(task_id)
        original_base = runner.task.base_sha
        runner.builder.write_text("greet.sh", "echo 'saved candidate'\n")
        await checkpoints.park(runner)
        await runner.builder.close()
        _land_on_main(factory, "greet.sh", "echo 'different main'\n")
        resumed = _runner(task_id)
        await checkpoints.resume(resumed, "token")
        try:
            head = await resumed.builder.exec("git rev-parse HEAD")
            assert head.output.strip() == original_base == resumed.task.base_sha
            assert resumed.builder.read_text("greet.sh") == "echo 'saved candidate'\n"
        finally:
            await resumed.builder.close()

    asyncio.run(go())


def test_checkpoint_storage_failure_rolls_back_attempt_refund(factory, monkeypatch):
    from sqlalchemy import event
    from sqlalchemy.orm import Session

    task_id = task_for(factory)

    def fail_insert(session, context, instances):
        if any(isinstance(obj, DeliveryCheckpoint) for obj in session.new):
            raise RuntimeError("database unavailable")

    async def go():
        runner = await prepared_runner(task_id)
        runner.builder.write_text("partial.txt", "saved work")
        event.listen(Session, "before_flush", fail_insert)
        try:
            with pytest.raises(RuntimeError, match="database unavailable"):
                await checkpoints.park(runner)
        finally:
            event.remove(Session, "before_flush", fail_insert)
            await runner.builder.close()
        assert runner.task.attempt == 3
        with session_scope() as s:
            assert s.get(Task, task_id).attempt == 3
            assert "delivery_checkpoint" not in s.get(Task, task_id).evidence
            assert s.get(DeliveryCheckpoint, task_id) is None

    asyncio.run(go())


def test_billing_stop_recovers_partial_builder_work_without_charging_it(factory):
    from aifactory.inference import InferenceError
    from aifactory.models import Decision
    from .fakes import call, step
    from .test_pipeline import ScriptedModel

    def refused(messages):
        if step(messages) == 0:
            return {"content": "", "tool_calls": [
                call("write_file", {"path": "partial.txt", "content": "work before quota stop"})]}
        raise InferenceError("Server returned 402: credits used up", status=402)

    ScriptedModel.scripts["builder"] = refused
    outcome_id = new_outcome(factory)

    def held():
        with session_scope() as s:
            return s.query(Decision).filter(Decision.workspace_id == factory["ws"],
                                             Decision.kind == "provider_hold", Decision.status == "open").count() == 1

    assert drive(held)
    with session_scope() as s:
        task = s.query(Task).filter(Task.outcome_id == outcome_id).one()
        assert task.status == "queued" and task.attempt == 0
        assert s.get(DeliveryCheckpoint, task.id) is not None
        assert [(a["n"], a["result"]) for a in task.evidence["attempts"]] == [(1, "infra")]
        hold = s.query(Decision).filter(Decision.workspace_id == factory["ws"],
                                         Decision.kind == "provider_hold", Decision.status == "open").one()
        hold.status = "resolved"  # simulate scheduler admission after the existing hold is resolved
    seen = []

    # Keep scripted-model step counting simple: fix and report after reading the old work.
    def continued(messages):
        if step(messages) == 0:
            seen.append(messages[1]["content"])
            return {"content": "", "tool_calls": [call("read_file", {"path": "partial.txt"})]}
        if step(messages) == 1:
            assert "work before quota stop" in str(messages)
            return {"content": "", "tool_calls": [
                call("write_file", {"path": "greet.sh", "content": "echo 'Hello, Factory!'\n"})]}
        return {"content": "", "tool_calls": [call("finish", {"status": "done", "summary": "Recovered", "tests": []})]}

    ScriptedModel.scripts["builder"] = continued
    assert drive(lambda: outcome_status(outcome_id) in ("completed", "attention", "blocked"), timeout=90)
    assert seen and "was restored into it from a saved copy" in seen[0]
    with session_scope() as s:
        task = s.query(Task).filter(Task.outcome_id == outcome_id).one()
        assert task.status == "completed", task.blocked_reason
        assert task.attempt == 1
        assert [(a["n"], a["result"]) for a in task.evidence["attempts"]] == [(1, "infra"), (1, "verified")]
        assert s.get(DeliveryCheckpoint, task.id) is None


def test_cancellation_while_restoring_never_reclaims_or_revives_task(factory, monkeypatch):
    task_id = task_for(factory, "verify")
    original = checkpoints.apply_in_sandbox
    allocated = []

    async def cancelled_during_restore(sandbox, patch):
        allocated.append(sandbox)
        await original(sandbox, patch)
        with session_scope() as s:
            s.get(Task, task_id).status = "cancelled"

    async def go():
        runner = await prepared_runner(task_id)
        runner.builder.write_text("partial.txt", "saved work")
        await checkpoints.park(runner)
        await runner.builder.close()
        monkeypatch.setattr(checkpoints, "apply_in_sandbox", cancelled_during_restore)
        resumed = _runner(task_id)
        with pytest.raises(delivery.Stop):
            await checkpoints.resume(resumed, "token")
        assert resumed.builder is None and not allocated[0].work.exists()
        with session_scope() as s:
            task = s.get(Task, task_id)
            assert task.status == "cancelled" and task.attempt == 2

    asyncio.run(go())


def test_fresh_budget_invalidates_private_bytes_without_restoring_old_work(factory):
    from aifactory.decisions import fresh_budget

    task_id = task_for(factory)

    async def go():
        runner = await prepared_runner(task_id)
        runner.builder.write_text("partial.txt", "old epoch")
        await checkpoints.park(runner)
        await runner.builder.close()
        with session_scope() as s:
            task = s.get(Task, task_id)
            task.evidence = fresh_budget(task.evidence)
            task.attempt = 0
        resumed = _runner(task_id)
        assert await checkpoints.resume(resumed, "token") is None
        assert resumed.builder is None and resumed.task.attempt == 0
        with session_scope() as s:
            assert s.get(DeliveryCheckpoint, task_id) is None
            assert s.get(Task, task_id).evidence["budget_epoch"] == 8

    asyncio.run(go())


@pytest.mark.parametrize("stage", ["build", "checks", "verify"])
def test_protected_work_is_never_persisted_or_refunded(factory, stage):
    task_id = task_for(factory, stage)

    async def go():
        runner = await prepared_runner(task_id)
        runner.builder.write_text(".env", "private configuration")
        try:
            with pytest.raises(checkpoints.CheckpointError, match="protected"):
                await checkpoints.park(runner)
            with session_scope() as s:
                assert s.get(DeliveryCheckpoint, task_id) is None
                assert s.get(Task, task_id).attempt == 3
        finally:
            await runner.builder.close()

    asyncio.run(go())


@pytest.mark.parametrize("change", ["episode_id", "checks", "setup", "base"])
def test_steward_rebuild_gate_deletes_private_bytes_before_approval(factory, monkeypatch, change):
    from .test_pipeline import _stop_at_the_checks
    from .test_steward_delivery import land, prepared_revert, run

    task_id, _ = prepared_revert(factory)
    _stop_at_the_checks(monkeypatch)
    with pytest.raises(asyncio.CancelledError):
        run(task_id)
    with session_scope() as s:
        assert s.get(DeliveryCheckpoint, task_id) is not None
        task = s.get(Task, task_id)
        if change == "episode_id":
            task.evidence = {**task.evidence, "steward": {**task.evidence["steward"], "episode_id": "new"}}
        elif change in ("checks", "setup"):
            repo = s.get(Repository, factory["repo"])
            repo.config = {**repo.config, change: [*repo.config[change], "sh -c true"]}
    if change == "base":
        land(factory, "other.txt", "main moved\n")
    run(task_id)
    with session_scope() as s:
        task = s.get(Task, task_id)
        assert task.status == "needs_input" and task.stage == "prepare"
        assert "delivery_checkpoint" not in task.evidence
        assert s.get(DeliveryCheckpoint, task_id) is None
    assert not FakeGitHub.pulls


def test_pause_while_extracting_cannot_write_checkpoint_or_refund(factory, monkeypatch):
    task_id = task_for(factory)
    original = checkpoints.extract_patch

    async def pause_during_read(*args, **kwargs):
        patch = await original(*args, **kwargs)
        with session_scope() as s:
            s.get(Workspace, factory["ws"]).paused = True
        return patch

    async def go():
        runner = await prepared_runner(task_id)
        runner.builder.write_text("partial.txt", "work in progress")
        monkeypatch.setattr(checkpoints, "extract_patch", pause_during_read)
        try:
            with pytest.raises(delivery.Stop):
                await checkpoints.park(runner)
            with session_scope() as s:
                assert s.get(DeliveryCheckpoint, task_id) is None
                assert s.get(Task, task_id).attempt == 3
        finally:
            await runner.builder.close()

    asyncio.run(go())
