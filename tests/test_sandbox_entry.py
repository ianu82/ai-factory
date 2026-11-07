import threading
from types import SimpleNamespace

import pytest

from auto_mindsdb_factory import product_cancellation as cancellation
from auto_mindsdb_factory.product_agent import HarnessStopped
from auto_mindsdb_factory.railway_runner import RailwayPool


@pytest.mark.parametrize("operation", ["create", "enter"])
@pytest.mark.parametrize("stop", ["cancel", "deadline"])
def test_actual_pool_retires_late_entry_once_before_any_body(tmp_path, monkeypatch, operation, stop):
    clock, requested, retired, drained = [0], threading.Event(), threading.Event(), threading.Event()
    monkeypatch.setattr(cancellation, "time", SimpleNamespace(monotonic=lambda: clock[0]))
    calls = []
    def transport(request):
        action = request.get("request", {}).get("action", request["action"])
        calls.append(action)
        if action == "destroy":
            retired.set()
        if action == operation:
            clock[0] = 2 if stop == "deadline" else 0
            requested.set()
            # Creation has no ID yet; warm must retire its eventual response.
            assert (pool.stopped if operation == "create" else retired).wait(2)
            drained.set()
        return {"id": "owned"} if action == "create" else {}
    pool = RailwayPool(transport)
    try:
        with pytest.raises(HarnessStopped):
            with cancellation.enter_cancellable(pool(tmp_path, "image"),
                    cancelled=lambda: stop == "cancel" and requested.is_set(), deadline=1):
                pytest.fail("Late sandbox entry admitted work")
        assert drained.is_set() and retired.is_set() and pool.id is None
        assert calls.count("destroy") == 1
        assert not set(calls) & {"run", "exit", "download"}
    finally:
        pool.close()
    assert not pool.thread.is_alive()
    assert not any(t.name == "factory-command-cancellation" for t in threading.enumerate())


@pytest.mark.parametrize("failed_cleanup", [False, True])
def test_entry_return_between_polls_still_retires_at_deadline(monkeypatch, failed_cleanup):
    clock, calls = [0], []
    monkeypatch.setattr(cancellation, "time", SimpleNamespace(monotonic=lambda: clock[0]))
    class Manager:
        def __enter__(self):
            clock[0] = 2
            return self
        def abort(self):
            calls.append("abort")
            if failed_cleanup:
                raise RuntimeError("private cleanup detail")
        def __exit__(self, *args):
            pytest.fail("Cancelled entry must not download source")
    with pytest.raises(HarnessStopped) as raised:
        with cancellation.enter_cancellable(Manager(), cancelled=lambda: False, deadline=1):
            pytest.fail("Expired entry admitted work")
    assert calls == ["abort"] and "private" not in str(raised.value)


def test_entry_watcher_finishes_before_command_owner_and_preserves_context_exit():
    calls = []
    class Manager:
        def __enter__(self):
            return "resource"
        def __exit__(self, kind, error, traceback):
            calls.append((kind, str(error)))
            return True
    with cancellation.enter_cancellable(Manager(), cancelled=lambda: False, deadline=float("inf")) as value:
        assert value == "resource"
        assert not any(t.name == "factory-command-cancellation" for t in threading.enumerate())
        raise ValueError("body failure")
    assert calls == [(ValueError, "body failure")]


def test_cancelled_entry_does_not_allocate_a_sandbox():
    class Manager:
        def __enter__(self):
            pytest.fail("Cancelled entry allocated a sandbox")
        def abort(self):
            pytest.fail("No sandbox was allocated")
    with pytest.raises(HarnessStopped):
        with cancellation.enter_cancellable(Manager(), cancelled=lambda: True, deadline=float("inf")):
            pytest.fail("Cancelled entry admitted work")


def test_real_setup_path_clamps_entry_to_planning_deadline(tmp_path, monkeypatch):
    from auto_mindsdb_factory import product_delivery as module
    clock, calls = [0], []
    monkeypatch.setattr(cancellation, "time", SimpleNamespace(monotonic=lambda: clock[0]))
    def export(target, sha, workspace):
        workspace.mkdir()
        (workspace / "source.txt").write_text("retained")
    monkeypatch.setattr(module, "export_source", export)
    class Manager:
        def __enter__(self):
            clock[0] = 2
            return self
        def abort(self):
            calls.append("abort")
        def __exit__(self, *args):
            pytest.fail("Late setup entry downloaded source")
    delivery = object.__new__(module.ProductDelivery)
    delivery.target, delivery.deadline, delivery.cancelled = tmp_path, 100, lambda: False
    delivery.settings = {"setup": [["npm", "ci"]], "image": "image", "setup_network": True, "platform": None}
    delivery.sandbox_factory = lambda *a, **kw: Manager()
    delivery._command = lambda *a, **kw: pytest.fail("Late entry ran setup")
    with pytest.raises(HarnessStopped):
        delivery._prepare("base", tmp_path / "source", None, deadline=1)
    assert calls == ["abort"] and (tmp_path / "source/source.txt").read_text() == "retained"
