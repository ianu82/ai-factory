import threading
import time
from types import SimpleNamespace

import pytest

from auto_mindsdb_factory import product_cancellation as cancellation
from auto_mindsdb_factory.product_agent import HarnessStopped
from auto_mindsdb_factory.product_workspace import git
from auto_mindsdb_factory.railway_runner import RailwayDelivery, RailwayOperationError, RailwayPool
from auto_mindsdb_factory.run_journal import RunJournal
from test_railway_exec_observations import observations


def delivery_for(pool, target):
    delivery = object.__new__(RailwayDelivery)
    delivery._railway_pool = delivery.sandbox_factory = pool
    delivery.target, delivery.deadline = target, time.monotonic() + 30
    delivery.settings = {"setup": [["true"]], "image": "pinned", "setup_network": False,
        "platform": "linux/amd64", "command_timeout_seconds": 10}
    return delivery


@pytest.mark.parametrize("stop", ["cancel", "deadline"])
def test_actual_setup_retains_transport_evidence_before_cancellation_replaces_error(validation_checkout, monkeypatch, stop):
    root, target = validation_checkout
    (target / "source.txt").write_text("unchanged")
    git(target, "add", "source.txt")
    git(target, "-c", "user.name=Test", "-c", "user.email=test@example.test", "commit", "-qm", "base")
    journal = RunJournal(root / "evidence", "setup-failure"); journal.create({})
    requested, retired = threading.Event(), threading.Event()
    calls, clock = [], [time.monotonic()]
    original = RailwayOperationError({"action": "run"}, "ExecNotStartedError", exec_observations=observations())
    def transport(request):
        action = request.get("request", {}).get("action", request["action"])
        calls.append(action)
        if action == "destroy": retired.set()
        if action == "run":
            requested.set(); clock[0] = delivery.deadline + 1
            assert retired.wait(3), "Cancellation could not retire its worker"
            raise original
        return {"id": "owned"} if action == "create" else {}
    pool = RailwayPool(transport)
    delivery = delivery_for(pool, target)
    delivery._cancellation = lambda: stop == "cancel" and requested.is_set()
    monkeypatch.setattr(cancellation, "time", SimpleNamespace(monotonic=lambda: clock[0]))
    try:
        with pytest.raises(HarnessStopped, match="cancelled"):
            delivery._prepare(git(target, "rev-parse", "HEAD"), root / "source", journal)
        events = [e for e in journal.events() if e["kind"] == "runner_transport_failed"]
        assert len(events) == 1
        assert events[0]["data"] == {"operation": "run", "category": "ExecNotStartedError",
            "exec_observations": observations()}
        assert calls == ["create", "upload", "enter", "run", "destroy"]
        assert pool.id is None and pool.stopped.is_set()
    finally:
        pool.close()
    assert not pool.thread.is_alive()
    assert not any(t.name == "factory-command-cancellation" for t in threading.enumerate())


@pytest.mark.parametrize("after_write", [False, True])
@pytest.mark.parametrize("diagnostic_error", [RuntimeError, KeyboardInterrupt])
def test_journal_failure_preserves_primary_error_and_owned_cleanup(tmp_path, after_write, diagnostic_error):
    journal = RunJournal(tmp_path, "write-failure"); journal.create({})
    original_append = journal.append
    def append(kind, **data):
        if kind == "runner_transport_failed":
            if after_write: original_append(kind, **data)
            raise diagnostic_error("private-journal-detail")
        return original_append(kind, **data)
    journal.append = append
    primary = RailwayOperationError({"action": "run"}, "ExecInterruptedError", exec_observations=observations())
    calls = []
    def transport(request):
        calls.append(request["action"])
        if request["action"] == "request": raise primary
        return {"id": "owned"} if request["action"] == "create" else {}
    pool = RailwayPool(transport)
    delivery = delivery_for(pool, tmp_path)
    try:
        delivery.prepare_runner(journal)
        with pytest.raises(RailwayOperationError) as raised:
            pool.call({"action": "run"})
        assert raised.value is primary
        assert calls == ["create", "request", "destroy"] and pool.id is None
        assert "private-journal-detail" not in str(primary) + str(journal.events())
    finally:
        pool.close()


def test_bootstrap_error_is_recorded_before_preparation_can_replace_it(tmp_path):
    journal = RunJournal(tmp_path, "bootstrap-failure"); journal.create({})
    primary = RailwayOperationError({"action": "create"}, "ExecNotStartedError", exec_observations=observations())
    def transport(request):
        raise primary
    pool = RailwayPool(transport)
    delivery = delivery_for(pool, tmp_path)
    try:
        with pytest.raises(RailwayOperationError) as raised:
            delivery.prepare_runner(journal)
        assert raised.value is primary
        event = journal.events()[-1]
        assert event["kind"] == "runner_transport_failed"
        assert event["data"]["operation"] == "create" and event["data"]["exec_observations"] == observations()
    finally:
        pool.close()


@pytest.mark.parametrize("state", ["missing", "mutated", "forged"])
def test_untrusted_failure_metadata_is_explicitly_unknown_in_durable_event(tmp_path, state):
    journal = RunJournal(tmp_path, "unknown-failure"); journal.create({})
    error = RailwayOperationError({"action": "run"}, "ExecNotStartedError", exec_observations=observations())
    if state == "missing":
        error = RuntimeError("private-provider-text")
    elif state == "mutated":
        error.phase, error.error_type, error.args = "exit", "ExecInterruptedError", ("Railway exit failed (ExecInterruptedError)",)
    else:
        error = RailwayOperationError.__new__(RailwayOperationError)
        error.phase, error.error_type, error.exec_observations = "run", "ExecNotStartedError", observations()
        error.args = ("private-provider-text",)
    def transport(request):
        if request["action"] == "request": raise error
        if request["action"] == "destroy":
            assert journal.events()[-1]["kind"] == "runner_transport_failed"
        return {"id": "owned"} if request["action"] == "create" else {}
    pool = RailwayPool(transport)
    try:
        delivery_for(pool, tmp_path).prepare_runner(journal)
        with pytest.raises(type(error)) as raised:
            pool.call({"action": "run"})
        assert raised.value is error
        assert journal.events()[-1]["data"] == {"operation": "unknown", "category": "UnknownError", "exec_observations": None}
    finally:
        pool.close()
