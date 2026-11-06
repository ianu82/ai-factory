import threading
import time
from types import SimpleNamespace

import pytest

from auto_mindsdb_factory.product_agent import ProductAgent, HarnessStopped
from auto_mindsdb_factory.product_cancellation import RunCancellation, request_cancellation, run_cancellable
from auto_mindsdb_factory.product_roles import snapshot_roles
from auto_mindsdb_factory.run_journal import RunJournal


def test_public_cancel_is_idempotent_and_never_rewrites_terminal_history(tmp_path, capsys):
    from auto_mindsdb_factory.__main__ import main
    journal = RunJournal(tmp_path, "active")
    journal.create({"task": {"id": "active"}})
    manifest = (journal.root / "manifest.json").read_bytes()
    argv = ["product-cancel", "--store-dir", str(tmp_path), "--run-id", "active"]
    assert main(argv) == main(argv) == 0
    assert RunCancellation(journal)() and len(journal.events()) == 1
    assert (journal.root / "manifest.json").read_bytes() == manifest
    journal.append("run_finished", status="blocked")
    before = {p.name: p.read_bytes() for p in journal.root.iterdir()}
    assert main(argv) == 1
    assert before == {p.name: p.read_bytes() for p in journal.root.iterdir()}
    with pytest.raises(Exception):
        request_cancellation(tmp_path, "absent")
    assert not (tmp_path / "delivery" / "absent").exists()


def test_observed_cancellation_cannot_be_cleared_by_removing_marker(tmp_path):
    journal = RunJournal(tmp_path, "latched")
    journal.create({"task": {"id": "latched"}})
    cancellation = RunCancellation(journal)
    request_cancellation(tmp_path, "latched")
    assert cancellation()
    cancellation.path.unlink()  # Only this disposable test artifact.
    assert cancellation()


@pytest.mark.parametrize("mode", ["cancel", "deadline", "cleanup-failed", "state-unavailable"])
def test_active_command_is_aborted_and_drained_before_policy_stop(monkeypatch, mode):
    from auto_mindsdb_factory import product_cancellation
    clock, cancellation, aborted = [0], threading.Event(), threading.Event()
    monkeypatch.setattr(product_cancellation, "time", SimpleNamespace(monotonic=lambda: clock[0]))
    calls = []
    def cancelled():
        if mode == "state-unavailable" and cancellation.is_set():
            raise OSError("private state unavailable")
        return cancellation.is_set() and mode != "deadline"
    class Sandbox:
        def run(self, command):
            calls.append("run")
            clock[0] = 2 if mode == "deadline" else 0
            cancellation.set()
            assert aborted.wait(2), "active command was not stopped"
            return {"exit_code": 137}
        def abort(self):
            calls.append("abort")
            aborted.set()
            if mode == "cleanup-failed":
                raise RuntimeError("private cleanup failure")
    with pytest.raises(HarnessStopped):
        run_cancellable(Sandbox(), ["sleep", "600"], cancelled=cancelled, deadline=1)
    assert calls == ["run", "abort"]
    assert not any(t.name == "factory-command-cancellation" for t in threading.enumerate())


def test_native_cancel_after_response_retains_usage_and_dispatches_no_tool(tmp_path):
    cancelled, calls = threading.Event(), []
    journal = RunJournal(tmp_path, "native")
    journal.create({"role_snapshots": snapshot_roles()})
    def request(*args, **kwargs):
        calls.append("request")
        cancelled.set()
        return {"id": "r", "model": "pinned", "status": "completed", "usage": {"total_tokens": 10},
                "output": [{"type": "function_call", "name": "run", "call_id": "c", "arguments": '{"command":["true"]}'}]}
    connector = SimpleNamespace(config=SimpleNamespace(model="pinned", max_output_tokens=100,
        reasoning_effort="low", timeout_seconds=1), request_response=request)
    agent = ProductAgent(connector, journal, max_turns=3, max_total_tokens=100,
                         deadline=time.monotonic() + 10, cancelled=cancelled.is_set)
    with pytest.raises(HarnessStopped):
        agent.run("builder", {}, lambda cmd: calls.append("command"))
    assert calls == ["request"] and journal.summary()["total_tokens"] == 10
    assert journal.summary()["usage_complete"]
