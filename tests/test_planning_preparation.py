import json
import threading
import time

import pytest

from auto_mindsdb_factory.docker_runner import RunnerError
from auto_mindsdb_factory.product_agent import HarnessStopped
from auto_mindsdb_factory.product_delivery import ProductDelivery
from auto_mindsdb_factory.run_journal import JournalError, RunJournal


@pytest.fixture
def preparation(tmp_path, monkeypatch):
    import auto_mindsdb_factory.product_delivery as module
    journal = RunJournal(tmp_path / "store", "prepare")
    journal.create({})
    calls, clock = [], [0.0]
    monkeypatch.setattr(time, "monotonic", lambda: clock[0])
    def export(target, sha, workspace):
        workspace.mkdir()
        (workspace / "source.txt").write_text("original")
        clock[0] += 2
    monkeypatch.setattr(module, "export_source", export)
    class Sandbox:
        def __enter__(self):
            calls.append("entry")
            clock[0] += 3
            return self
        def __exit__(self, *exception):
            calls.append("exit")
            clock[0] += 5
            return False
        def abort(self): calls.append("abort")
    sandbox = Sandbox()
    delivery = object.__new__(ProductDelivery)
    delivery.target, delivery.deadline = tmp_path, 1000
    delivery.settings = {"setup": [["private-setup-argument"]], "image": "unused",
                         "setup_network": False, "platform": "linux/amd64"}
    delivery.sandbox_factory = lambda *a, **kw: sandbox
    def command(actual, argv, **kwargs):
        assert actual is sandbox and kwargs["deadline"] == 900
        calls.append("command")
        clock[0] += 7
        return {"exit_code": 0}
    delivery._command = command
    return delivery, journal, tmp_path / "workspace", sandbox, calls


def execute(p, *, planning=True):
    delivery, journal, workspace, _, _ = p
    return delivery._prepare("a" * 40, workspace, journal, deadline=900, planning=planning)


def observations(journal):
    return [e["data"] for e in journal.events() if e["kind"].startswith("planning_preparation_")]


def test_actual_preparation_records_fixed_order_and_operation_durations(preparation):
    assert execute(preparation) == {"source.txt": b"original"}
    _, journal, _, _, calls = preparation
    assert calls == ["entry", "command", "exit"]
    starts = [e["data"]["operation"] for e in journal.events() if e["kind"] == "planning_preparation_started"]
    assert starts == ["source_export", "source_inventory", "setup_entry", "setup_command",
                      "setup_finalization", "setup_validation"]
    finishes = [e["data"] for e in journal.events() if e["kind"] == "planning_preparation_finished"]
    assert [d["duration_seconds"] for d in finishes] == [2, 0, 3, 7, 5, 0]
    assert all(d["role"] == "planner" and d["returned"] is True for d in finishes)
    assert "private-setup-argument" not in json.dumps(observations(journal))
    assert journal.summary()["total_tokens"] == 0 and journal.summary()["usage_complete"]


@pytest.mark.parametrize("operation", ["setup_entry", "setup_command", "setup_finalization"])
def test_actual_failure_retains_only_fixed_operation_and_propagates(preparation, operation):
    delivery, journal, _, sandbox, calls = preparation
    class PrivateError(RunnerError):
        def __str__(self): raise AssertionError("Exception formatting is forbidden")
    error = PrivateError("private-provider-content")
    def fail(*args, **kwargs): raise error
    if operation == "setup_entry": type(sandbox).__enter__ = fail
    elif operation == "setup_finalization": type(sandbox).__exit__ = fail
    else: delivery._command = fail
    with pytest.raises(PrivateError) as caught: execute(preparation)
    assert caught.value is error
    failed = [d for d in observations(journal) if d.get("returned") is False]
    assert failed == [{"role": "planner", "operation": operation, "returned": False, "duration_seconds": 0}]
    assert "private-provider-content" not in json.dumps(journal.events())
    assert not any(d["operation"] == "setup_validation" for d in observations(journal))
    if operation == "setup_finalization": assert calls[-1] == "abort"


@pytest.mark.parametrize("operation", ["setup_entry", "setup_finalization"])
def test_finish_evidence_failure_retires_acquired_resource(preparation, monkeypatch, operation):
    _, journal, _, _, calls = preparation
    append = journal.append
    def record(kind, **data):
        if kind == "planning_preparation_finished" and data["operation"] == operation:
            raise JournalError("private-storage-detail")
        return append(kind, **data)
    monkeypatch.setattr(journal, "append", record)
    with pytest.raises(JournalError): execute(preparation)
    assert calls[-1] == "abort" and calls.count("abort") == 1
    assert ("command" in calls) is (operation == "setup_finalization")
    assert "private-storage-detail" not in json.dumps(journal.events())


def test_cancelled_entry_drains_and_records_failed_operation_before_return(preparation):
    delivery, journal, _, sandbox, calls = preparation
    started, retired, drained = threading.Event(), threading.Event(), threading.Event()
    def enter(self):
        started.set()
        assert retired.wait(2)
        drained.set()
        return self
    def abort(self):
        calls.append("abort")
        retired.set()
    type(sandbox).__enter__, type(sandbox).abort = enter, abort
    delivery.cancelled = started.is_set
    with pytest.raises(HarnessStopped): execute(preparation)
    assert drained.is_set() and calls == ["abort"]
    assert observations(journal)[-1] == {"role": "planner", "operation": "setup_entry",
                                      "returned": False, "duration_seconds": 0}


@pytest.mark.parametrize("operation", ["setup_entry", "setup_finalization"])
def test_slow_observation_retention_does_not_admit_expired_work(preparation, monkeypatch, operation):
    _, journal, _, _, calls = preparation
    append, expired = journal.append, [False]
    clock = time.monotonic
    monkeypatch.setattr(time, "monotonic", lambda: 901 if expired[0] else clock())
    def record(kind, **data):
        result = append(kind, **data)
        if kind == "planning_preparation_finished" and data["operation"] == operation:
            expired[0] = True
        return result
    monkeypatch.setattr(journal, "append", record)
    with pytest.raises(HarnessStopped): execute(preparation)
    assert calls[-1] == "abort" and calls.count("abort") == 1
    assert ("command" in calls) is (operation == "setup_finalization")
    assert not any(d["operation"] == "setup_validation" for d in observations(journal))


def test_other_preparation_paths_keep_existing_journal_surface(preparation):
    execute(preparation, planning=False)
    assert observations(preparation[1]) == []


def test_untrusted_operation_is_rejected_without_formatting(tmp_path):
    from auto_mindsdb_factory.planning_preparation import preparation_operation
    class Unprintable:
        def __str__(self): raise AssertionError("Untrusted formatting")
        def __hash__(self): raise AssertionError("Untrusted lookup")
    journal = RunJournal(tmp_path, "invalid")
    journal.create({})
    with pytest.raises(ValueError, match="Unknown planning preparation operation"):
        with preparation_operation(journal, Unprintable()): pytest.fail("Unknown operation ran")
    assert observations(journal) == []
