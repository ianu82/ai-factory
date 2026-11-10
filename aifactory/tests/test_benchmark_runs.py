"""Benchmark gates run in CI without root; only the isolation admission is faked."""
import asyncio
import json
from datetime import datetime, timedelta

import pytest
from fastapi import HTTPException

from aifactory import gitops
from aifactory.benchmarks import load_suite
from aifactory.db import session_scope
from aifactory.models import BenchmarkTrial, Outcome, Run, Task, User, Workspace
from aifactory.pipeline import delivery, runtime, handover
from aifactory.pipeline.benchmarking import run_suite, summarize, print_summary
from aifactory.pipeline.orchestrator import Orchestrator
from .fakes import FakeGitHub, ScriptedModel, call, git, step
from .test_pipeline import factory, CRITERION, good_builder


@pytest.fixture
def benchmark(factory, tmp_path, monkeypatch):
    attempted = []
    def forbidden(*args, **kwargs):
        attempted.append((args, kwargs))
        raise AssertionError("benchmark attempted publication")
    for name in ("_publish", "ci_then_merge", "merge", "smoke", "release", "revert", "_settle_issues"):
        monkeypatch.setattr(delivery.TaskRunner, name, forbidden)
    monkeypatch.setattr(delivery, "publish", forbidden)
    monkeypatch.setattr(gitops, "publish", forbidden)
    monkeypatch.setattr(handover, "publish", forbidden)
    monkeypatch.setattr(gitops, "revert_commit", forbidden)
    monkeypatch.setattr(delivery, "revert_commit", forbidden)
    for name in ("create_pull", "comment", "merge", "set_status", "request"):
        monkeypatch.setattr(FakeGitHub, name, forbidden)
    # Exercise real local git/checks/harness, but emulate isolated admission, not OS UID changes.
    monkeypatch.setattr(delivery, "isolation_available", lambda: True)
    monkeypatch.setattr(runtime, "take_guidance", forbidden)
    monkeypatch.setattr(delivery, "guidance_history", forbidden)
    monkeypatch.setattr(delivery, "precedents", forbidden)
    base = git("rev-parse", "main", cwd=factory["remotes"] / "acme/demo.git")
    path = tmp_path / "suite.json"
    path.write_text(json.dumps({"schema_version": 1, "name": "greeting", "tasks": [{
        "key": "T1", "repository": "acme/demo", "base_commit": base, "title": "Greeting",
        "specification": "Update greet.sh", "acceptance": [CRITERION]}]}))
    load_suite(factory["ws"], path)
    factory["forbidden_calls"] = attempted
    return factory, base


def execute(ids, n=1):
    return asyncio.run(run_suite(ids["ws"], "greeting", "native", "chosen-model", n))


@pytest.mark.parametrize("failure", [None, "checks", "missing", "verifier", "infra", "blocking"])
def test_trials(benchmark, monkeypatch, failure):
    ids, base = benchmark
    remote = ids["remotes"] / "acme/demo.git"
    before = git("show-ref", cwd=remote)
    with session_scope() as s:
        original = s.get(Workspace, ids["ws"]).settings
    if failure == "checks":
        def bad(messages):
            if step(messages) == 0:
                return {"content": "", "tool_calls": [call("write_file", {"path": "greet.sh", "content": "echo wrong\n"})]}
            return {"content": "", "tool_calls": [call("finish", {"status": "done", "summary": "wrong"})]}
        ScriptedModel.scripts["builder"] = bad
    elif failure:
        def verify(messages):
            if failure == "infra":
                raise RuntimeError("verifier infrastructure offline")
            report = {"verdict": "fail" if failure == "verifier" else "pass", "summary": "review",
                      "criteria": [] if failure == "missing" else [{"criterion": CRITERION, "status": "pass", "evidence": "checked"}],
                      "findings": [{"severity": "major", "description": "broken", "location": "greet.sh"}] if failure == "blocking" else []}
            return {"content": "", "tool_calls": [call("finish", report)]}
        ScriptedModel.scripts["verifier"] = verify
    rows = execute(ids, 2)
    assert len(rows) == 2
    assert all(r.passed == (failure is None) for r in rows), [r.checks for r in rows]
    assert git("show-ref", cwd=remote) == before
    assert not FakeGitHub.pulls and not FakeGitHub.merged
    assert not ids["forbidden_calls"]
    with session_scope() as s:
        assert s.get(Workspace, ids["ws"]).settings == original
        tasks = s.query(Task).all()
        assert len(tasks) == 2 and all(t.attempt == 1 and t.base_sha == base for t in tasks)
        builders = s.query(Run).filter_by(role="builder").all()
        assert len(builders) == 2
        for row in rows:
            runs = s.query(Run).filter_by(benchmark_trial_id=row.id).all()
            assert runs and all(r.model == "chosen-model" and r.config_version for r in runs)
            assert row.config_version == next(r.config_version for r in runs if r.role == "builder")
            assert row.configuration["primary_run_id"]
            if not failure:
                assert {r.role for r in runs} == {"builder", "verifier"}
                assert len({r.id for r in runs}) == 2
                assert row.checks["results"][0]["ok"]
                assert len({r.config_version for r in runs}) == 2


def test_refuses_unisolated(benchmark, monkeypatch):
    ids, _ = benchmark
    monkeypatch.setattr(delivery, "isolation_available", lambda: False)
    rows = execute(ids)
    assert not rows[0].passed
    assert "benchmark requires an isolated sandbox" in rows[0].checks["error"]
    with session_scope() as s:
        assert s.query(Run).count() == 0


def test_base_only_objects(benchmark):
    ids, base = benchmark
    remote = ids["remotes"] / "acme/demo.git"
    # Create a later commit directly in a temporary clone, then push it to main.
    import tempfile
    from pathlib import Path
    with tempfile.TemporaryDirectory() as temp:
        clone = Path(temp) / "clone"
        git("clone", str(remote), str(clone))
        (clone / "future.txt").write_text("reference solution")
        git("add", ".", cwd=clone)
        git("commit", "-m", "later", cwd=clone)
        later = git("rev-parse", "HEAD", cwd=clone)
        git("push", "origin", "HEAD:main", cwd=clone)
    seen = []
    original = delivery.benchmark_checkout
    async def check(*args):
        result = await original(*args)
        sb = args[0]
        assert git("rev-parse", "HEAD", cwd=sb.work) == base
        assert not git("fsck", "--unreachable", cwd=sb.work)
        assert not (await sb.exec(f"git cat-file -e {later}")).ok
        assert not git("remote", "-v", cwd=sb.work)
        assert "acme/demo" not in (sb.work / ".git/config").read_text()
        seen.append(result)
        return result
    from unittest.mock import patch
    with patch.object(delivery, "benchmark_checkout", check):
        rows = execute(ids)
    assert rows[0].passed, (rows[0].checks, rows[0].verifier)
    assert seen == [base, base]


def test_interrupted_never_recovered_or_taken_over(benchmark, monkeypatch):
    ids, _ = benchmark
    row = execute(ids)[0]
    with session_scope() as s:
        t = s.get(Task, row.task_id)
        t.status, t.stage = "running", "publish"
        s.get(Outcome, row.outcome_id).status = "running"
    orch = Orchestrator()
    orch._recover()
    orch._promote()
    spawned = []
    monkeypatch.setattr(orch, "_spawn_task", spawned.append)
    monkeypatch.setattr(orch, "_spawn_plan", spawned.append)
    orch._schedule()
    assert not spawned
    with session_scope() as s:
        assert s.get(Task, row.task_id).status == "running"
        user = s.get(User, ids["user"])
    with pytest.raises(handover.HandoverError, match="Benchmark"):
        handover.mark_handed_over(row.task_id, user)
    with pytest.raises(handover.HandoverError, match="Benchmark"):
        asyncio.run(handover.publish_candidate(row.task_id, {}))
    assert not asyncio.run(handover.close_dropped_pull(row.task_id, "user", "cancel"))


def test_summary(capsys):
    start = datetime(2025, 1, 1)
    rows = [BenchmarkTrial(harness="native", model="m", config_version=version,
                           configuration={"sandbox_kind": "local"}, status="completed", passed=passed,
                           cost_known=cost is not None, cost_usd=cost, started_at=start,
                           finished_at=start + timedelta(seconds=duration))
            for version, passed, cost, duration in [("a", True, 2, 2), ("a", False, 4, 8),
                                                   ("a", False, None, 5), ("b", True, 9, 10)]]
    a, b = summarize(rows)
    assert (a["passed"], a["total"], a["median_cost"], a["median_duration"], a["unknown_costs"]) == (1, 3, 3, 5, 1)
    assert b["total"] == 1
    print_summary(rows)
    output = capsys.readouterr().out
    assert "33.3%" in output and "unknown costs 1" in output and "unrestricted" in output


def test_guidance_refused_and_sessions_fixed(benchmark, monkeypatch):
    ids, _ = benchmark
    from aifactory.api import post_message, MessageBody
    from aifactory.harness.native import NativeHarness
    from aifactory.db import SessionLocal
    original = NativeHarness.run
    seen = []
    async def run(self, session):
        assert session.take_guidance() == []
        assert "acme/demo" not in session.prompt
        seen.append(session.role)
        with SessionLocal() as db:
            task = db.get(Task, next(iter(db.query(Task.id)))[0])
            with pytest.raises(HTTPException) as exc:
                post_message(ids["ws"], task.outcome_id, MessageBody(body="alter the trial"),
                             db.get(User, ids["user"]), db)
            assert exc.value.status_code == 409
            assert "fixed suite inputs" in exc.value.detail
        return await original(self, session)
    monkeypatch.setattr(NativeHarness, "run", run)
    rows = execute(ids)
    assert rows[0].passed, (rows[0].checks, rows[0].verifier)
    assert seen == ["builder", "verifier"]


def test_network_contamination(benchmark):
    ids, _ = benchmark
    def builder(messages):
        if step(messages) == 0:
            return {"content": "", "tool_calls": [call("run", {
                "command": "true " + "x" * 400 + " https://github.com/acme/demo"})]}
        return good_builder(messages[2:])
    ScriptedModel.scripts["builder"] = builder
    rows = execute(ids)
    assert rows[0].status == "contaminated" and not rows[0].passed
    assert summarize(rows) == []


def test_benchmark_does_not_revoke_live_job_traversal(benchmark):
    ids, _ = benchmark
    from aifactory.config import get_settings
    from aifactory.sandbox import LocalSandbox
    import stat
    root = get_settings().data_dir
    before = stat.S_IMODE(root.stat().st_mode)
    async def run():
        sb = LocalSandbox("ordinary-live-job").create()
        try:
            sb.work.mkdir()
            (sb.work / "test.sh").write_text("echo alive")
            assert (await sb.exec('sh "$PWD/test.sh"')).ok
            await run_suite(ids["ws"], "greeting", "native", "chosen-model", 1)
            assert stat.S_IMODE(root.stat().st_mode) == before
            assert (await sb.exec('sh "$PWD/test.sh"')).ok
        finally:
            await sb.close()
    asyncio.run(run())


def test_prepared_vm_never_used(benchmark, monkeypatch):
    ids, _ = benchmark
    from aifactory import vm
    from aifactory.models import Repository
    from aifactory.sandbox import LocalSandbox
    calls = []
    class FakeVM(LocalSandbox):
        kind = "vm"
        prepared = None
        vm_id = "fake"
        region = "test"
        async def read_text_async(self, relative, limit):
            return self.read_text(relative, limit)
        async def write_bytes_async(self, relative, content):
            self.write_bytes(relative, content)
        async def read_bytes_async(self, relative, limit=None, from_end=False):
            data = self.resolve(relative).read_bytes()
            return (data[-limit:] if from_end else data[:limit]) if limit else data
        @classmethod
        async def from_local(cls, local, **kwargs):
            import shutil
            assert kwargs["prepared"] is None
            calls.append(kwargs)
            sb = cls(local.job_id + "-vm", root=local.root.parent).create()
            shutil.copytree(local.work, sb.work)
            return sb
    monkeypatch.setattr(vm, "RailwayVMSandbox", FakeVM)
    monkeypatch.setattr(vm, "vm_available", lambda: (True, ""))
    with session_scope() as s:
        repo = s.get(Repository, ids["repo"])
        repo.config = {**repo.config, "setup": ["echo fresh >> setup.log"],
                       "build": {"isolation": "vm"}, "qa": {"isolation": "vm", "prepared": True}}
    rows = execute(ids)
    assert rows[0].passed, (rows[0].checks, rows[0].verifier)
    assert len(calls) == 2


@pytest.mark.parametrize("mode", ["disabled", "nonroot"])
def test_actual_isolation_admission(benchmark, monkeypatch, mode):
    ids, _ = benchmark
    from aifactory import sandbox
    monkeypatch.setattr(delivery, "isolation_available", sandbox.isolation_available)
    monkeypatch.setenv("AIFACTORY_SANDBOX_ISOLATION", "0" if mode == "disabled" else "1")
    if mode == "nonroot":
        monkeypatch.setattr(sandbox.os, "geteuid", lambda: 1000)
    row = execute(ids)[0]
    assert not row.passed and "benchmark requires an isolated sandbox" in row.checks["error"]
    with session_scope() as s:
        assert not s.query(Run).count()


def test_cancelled_trial_durable(benchmark, monkeypatch):
    ids, _ = benchmark
    from aifactory.harness.native import NativeHarness
    async def cancelled(*args):
        raise asyncio.CancelledError()
    monkeypatch.setattr(NativeHarness, "run", cancelled)
    with pytest.raises(asyncio.CancelledError):
        execute(ids)
    with session_scope() as s:
        row = s.query(BenchmarkTrial).one()
        assert row.status == "cancelled" and not row.passed and row.finished_at
        assert s.query(Run).one().benchmark_trial_id == row.id


def test_cli_private_umask(benchmark, monkeypatch):
    ids, _ = benchmark
    import os
    from aifactory.cli import main
    from aifactory.config import get_settings
    before_mode = get_settings().data_dir.stat().st_mode & 0o777
    previous = os.umask(0o022)
    try:
        main(["benchmarks", "run", "--workspace", ids["ws"], "--suite", "greeting",
              "--harness", "native", "--model", "chosen-model", "--trials", "1"])
        current = os.umask(0o077)
        assert current == 0o077
        assert gitops._mirror_path("acme/demo").stat().st_mode & 0o777 == 0o700
        # Do not revoke traverse access for existing ordinary jobs.
        assert get_settings().data_dir.stat().st_mode & 0o777 == before_mode
        assert not (get_settings().data_dir.stat().st_mode & 0o044)
    finally:
        os.umask(previous)


def test_tasks_times_trials(benchmark, tmp_path):
    ids, base = benchmark
    from aifactory.models import BenchmarkSuite
    with session_scope() as s:
        tasks = s.query(BenchmarkSuite).one().tasks
    path = tmp_path / "multi.json"
    second = {**tasks[0], "key": "T2"}
    path.write_text(json.dumps({"schema_version": 1, "name": "multi", "tasks": [tasks[0], second]}))
    suite = load_suite(ids["ws"], path)
    rows = asyncio.run(run_suite(ids["ws"], "multi", "native", "chosen-model", 2))
    assert len(rows) == 4 and all(r.passed and r.suite_id == suite.id for r in rows)
    assert {(r.task_key, r.trial_number) for r in rows} == {("T1", 1), ("T1", 2), ("T2", 1), ("T2", 2)}
    assert len({r.task_id for r in rows}) == 4
    assert all(r.configuration["network_isolation"] == "unrestricted" for r in rows)


def test_checkout_error_durable(benchmark, monkeypatch):
    ids, _ = benchmark
    async def broken(*args):
        raise RuntimeError("checkout infrastructure failed")
    monkeypatch.setattr(delivery, "benchmark_checkout", broken)
    row = execute(ids)[0]
    assert not row.passed and row.finished_at
    assert "checkout infrastructure failed" in row.checks["error"]
    assert not row.cost_known and row.cost_usd is None
