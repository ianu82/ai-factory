from types import SimpleNamespace
import os
import subprocess
import sys
import threading
import time

import pytest

from auto_mindsdb_factory import product_cancellation as cancellation, railway_runner as runner
from test_railway_runner import Provider


def test_actual_lifecycle_passes_remaining_work_and_finalization_budgets(tmp_path, monkeypatch):
    clock = [0]
    time = SimpleNamespace(monotonic=lambda: clock[0])
    monkeypatch.setattr(cancellation, "time", time)
    monkeypatch.setattr(runner, "time", time)
    provider = Provider()
    after = {"upload": 2, "enter": 2, "run": 5, "exit": 9, "download": 10}
    def transport(request):
        response = provider(request)
        if request["action"] == "request":
            clock[0] = after[request["request"]["action"]]
        return response
    pool = runner.RailwayPool(transport)
    try:
        pool.warm()  # The separately bounded worker bootstrap is unchanged.
        with cancellation.enter_cancellable(pool(tmp_path, "image"), cancelled=lambda: False,
                deadline=10, finalization_deadline=12) as sandbox:
            cancellation.run_cancellable(sandbox, ["true"], cancelled=lambda: False, deadline=6)
        requests = [r for r in provider.calls if r["action"] == "request"]
        assert [r.get("budget_ms") for r in requests] == [10000, 8000, 4000, 7000, 3000]
        assert [r["timeout"] for r in requests] == [120, 150, 345, 150, 150]
        assert not any("budget_ms" in r for r in provider.calls if r["action"] != "request")
    finally:
        pool.close()
    assert not pool.thread.is_alive()


def test_expiration_while_waiting_for_pool_lock_never_dispatches(tmp_path, monkeypatch):
    clock = [0]
    monkeypatch.setattr(runner, "time", SimpleNamespace(monotonic=lambda: clock[0]))
    provider, started, errors = Provider(), threading.Event(), []
    pool = runner.RailwayPool(provider)
    def run():
        started.set()
        try:
            pool.call({"action": "run", "command": ["must-not-run"]}, deadline=5)
        except Exception as error:
            errors.append(error)
    try:
        pool.open(tmp_path)
        with pool.lock:
            worker = threading.Thread(target=run)
            worker.start()
            assert started.wait(2)
            clock[0] = 6
        worker.join(2)
        assert not worker.is_alive() and len(errors) == 1
        assert isinstance(errors[0], runner.RunnerError)
        assert not any(r.get("request", {}).get("action") == "run" for r in provider.calls)
        assert [r["id"] for r in provider.calls if r["action"] == "destroy"] == ["1"]
    finally:
        pool.close()


@pytest.mark.parametrize("deadline", [True, None, "10", float("nan"), float("inf")])
def test_invalid_bound_deadline_never_allocates(tmp_path, deadline):
    provider = Provider()
    pool = runner.RailwayPool(provider)
    try:
        with pytest.raises(runner.RunnerError, match="Invalid"):
            pool(tmp_path, "image").set_deadline(deadline)
        assert provider.calls == []
    finally:
        pool.close()


def test_preexpired_entry_does_not_warm_a_worker(tmp_path, monkeypatch):
    monkeypatch.setattr(runner, "time", SimpleNamespace(monotonic=lambda: 10))
    provider = Provider()
    pool = runner.RailwayPool(provider)
    try:
        with pytest.raises(runner.RunnerError, match="expired"):
            pool.open(tmp_path, deadline=9)
        assert provider.calls == []
    finally:
        pool.close()


def test_retirement_cannot_admit_a_replacement_after_deadline(tmp_path, monkeypatch):
    clock, provider = [0], Provider()
    monkeypatch.setattr(runner, "time", SimpleNamespace(monotonic=lambda: clock[0]))
    def transport(request):
        if request["action"] == "destroy":
            clock[0] = 10
        return provider(request)
    pool = runner.RailwayPool(transport)
    first, second = tmp_path / "first", tmp_path / "second"
    first.mkdir(); second.mkdir()
    try:
        pool.open(first)
        with pytest.raises(runner.RunnerError, match="expired"):
            pool.open(second, deadline=5)
        assert provider.serial == 1 and pool.id is None
    finally:
        pool.close()


@pytest.mark.parametrize("slow_spawn", [False, True])
def test_real_bridge_child_is_killed_and_reaped_at_deadline(monkeypatch, slow_spawn):
    processes = []
    popen = subprocess.Popen
    def spawn(*args, **kwargs):
        process = popen(*args, **kwargs)
        processes.append(process)
        if slow_spawn:
            time.sleep(0.25)
        return process
    monkeypatch.setattr(runner.subprocess, "Popen", spawn)
    started = time.monotonic()
    with pytest.raises(subprocess.TimeoutExpired):
        runner._bounded_bridge([sys.executable, "-c", "import sys,time;sys.stdin.read();time.sleep(5)"],
            input="request", env={"PATH": os.defpath}, timeout=100, deadline=started + 0.2)
    assert time.monotonic() - started < 2
    assert len(processes) == 1 and processes[0].poll() is not None


def test_absolute_deadline_stays_in_python_and_timeout_classification_is_preserved(monkeypatch):
    import json
    monkeypatch.setenv("RAILWAY_API_TOKEN", "test-only")
    monkeypatch.setenv("RAILWAY_ENVIRONMENT_ID", "test-only")
    deadline = time.monotonic() + 2
    def expired(command, **kwargs):
        assert kwargs["deadline"] == deadline and kwargs["timeout"] == 210
        assert "deadline" not in json.loads(kwargs["input"])
        raise subprocess.TimeoutExpired(command, 2)
    monkeypatch.setattr(runner, "_bounded_bridge", expired)
    with pytest.raises(runner.RailwayOperationError) as raised:
        runner.bridge({"action": "request", "request": {"action": "enter"},
            "timeout": 150, "budget_ms": 2000, "deadline": deadline})
    assert runner.railway_failure_facts(raised.value) == ("enter", "TimeoutExpired")


def test_bridge_interruption_preserves_primary_error_and_reaps_child(monkeypatch):
    processes, primary = [], KeyboardInterrupt()
    popen = subprocess.Popen
    def interrupted(*args, **kwargs):
        raise primary
    def spawn(*args, **kwargs):
        process = popen(*args, **kwargs)
        processes.append(process)
        process.communicate = interrupted
        return process
    monkeypatch.setattr(runner.subprocess, "Popen", spawn)
    try:
        with pytest.raises(KeyboardInterrupt) as raised:
            runner._bounded_bridge([sys.executable, "-c", "import time;time.sleep(5)"],
                input="request", env={"PATH": os.defpath}, timeout=100, deadline=time.monotonic() + 2)
        assert raised.value is primary
        assert len(processes) == 1 and processes[0].poll() is not None
    finally:
        for process in processes:
            if process.poll() is None:
                process.kill()
                process.wait()
