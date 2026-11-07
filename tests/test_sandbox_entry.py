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
    assert 1 <= len(calls) <= (2 if failed_cleanup else 1)
    assert set(calls) == {"abort"} and "private" not in str(raised.value)


@pytest.mark.parametrize("stop", ["cancel", "deadline"])
def test_actual_docker_late_creation_is_removed_after_early_abort_failed(tmp_path, monkeypatch, stop):
    import subprocess
    from auto_mindsdb_factory import docker_runner
    clock, requested, early_removal = [0], threading.Event(), threading.Event()
    created, removals = [], []
    monkeypatch.setattr(cancellation, "time", SimpleNamespace(monotonic=lambda: clock[0]))
    monkeypatch.setattr(docker_runner.os, "getuid", lambda: 10001)
    class Docker(docker_runner.DockerSandbox):
        def _docker(self, *args, **kwargs):
            assert args[0] == "run"
            clock[0] = 2 if stop == "deadline" else 0
            requested.set()
            assert early_removal.wait(2)
            created.append(self.name)
        def _remove(self):
            removals.append(bool(created))
            if not created:
                early_removal.set()
                return subprocess.CompletedProcess([], 1)
            created.clear()
            return subprocess.CompletedProcess([], 0)
    with pytest.raises(HarnessStopped):
        with cancellation.enter_cancellable(Docker(tmp_path, "node@sha256:" + "a" * 64),
                cancelled=lambda: stop == "cancel" and requested.is_set(), deadline=1):
            pytest.fail("Cancelled late container admitted work")
    assert removals == [False, True] and not created
    assert not any(t.name == "factory-command-cancellation" for t in threading.enumerate())


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


@pytest.mark.parametrize("operation", ["exit", "download"])
@pytest.mark.parametrize("stop", ["cancel", "deadline"])
def test_actual_pool_finalization_is_aborted_and_drained(tmp_path, monkeypatch, operation, stop):
    clock, requested, retired, drained = [0], threading.Event(), threading.Event(), threading.Event()
    monkeypatch.setattr(cancellation, "time", SimpleNamespace(monotonic=lambda: clock[0]))
    calls = []
    def transport(request):
        action = request.get("request", {}).get("action", request["action"])
        calls.append(action)
        if action == "destroy": retired.set()
        if action == operation:
            clock[0] = 2 if stop == "deadline" else 0
            requested.set()
            assert retired.wait(1), "Finalization had no cancellation owner"
            drained.set()
            raise RuntimeError("private interrupted finalization")
        return {"id": "owned"} if action == "create" else {}
    pool = RailwayPool(transport)
    try:
        with pytest.raises(HarnessStopped):
            with cancellation.enter_cancellable(pool(tmp_path, "image"),
                    cancelled=lambda: stop == "cancel" and requested.is_set(), deadline=1):
                assert not any(t.name == "factory-command-cancellation" for t in threading.enumerate())
        assert drained.is_set() and retired.is_set() and pool.id is None
        assert calls.count("destroy") == 1 and calls.count(operation) == 1
        if operation == "exit": assert "download" not in calls
    finally:
        pool.close()
    assert not pool.thread.is_alive()
    assert not any(t.name == "factory-command-cancellation" for t in threading.enumerate())


@pytest.mark.parametrize("stop", ["cancel", "deadline"])
def test_expired_body_retires_without_starting_source_download(monkeypatch, stop):
    clock, requested, calls = [0], [False], []
    monkeypatch.setattr(cancellation, "time", SimpleNamespace(monotonic=lambda: clock[0]))
    class Manager:
        def __enter__(self): return self
        def __exit__(self, *args): pytest.fail("Expired body started source download")
        def abort(self): calls.append("abort")
    with pytest.raises(HarnessStopped):
        with cancellation.enter_cancellable(Manager(), cancelled=lambda: requested[0], deadline=1):
            if stop == "cancel": requested[0] = True
            else: clock[0] = 2
    assert calls == ["abort"]


@pytest.mark.parametrize("cleanup_failed", [False, True])
def test_finalization_return_between_polls_cannot_admit_work(monkeypatch, cleanup_failed):
    clock, calls = [0], []
    monkeypatch.setattr(cancellation, "time", SimpleNamespace(monotonic=lambda: clock[0]))
    class Manager:
        def __enter__(self): return self
        def __exit__(self, *args): clock[0] = 2
        def abort(self):
            calls.append("abort")
            if cleanup_failed: raise RuntimeError("private cleanup detail")
    with pytest.raises(HarnessStopped) as caught:
        with cancellation.enter_cancellable(Manager(), cancelled=lambda: False, deadline=1): pass
    assert calls == ["abort"] and "private" not in str(caught.value)


def test_failed_finalization_abort_is_not_repeated_or_reported_as_cleaned():
    requested, released, calls = threading.Event(), threading.Event(), []
    class Manager:
        def __enter__(self): return self
        def __exit__(self, *args):
            requested.set()
            assert released.wait(2)
            raise RuntimeError("private transport detail")
        def abort(self):
            calls.append("abort")
            released.set()
            raise RuntimeError("private retirement detail")
    with pytest.raises(HarnessStopped, match="cleanup could not be confirmed") as caught:
        with cancellation.enter_cancellable(Manager(), cancelled=requested.is_set, deadline=float("inf")): pass
    assert calls == ["abort"] and "private" not in str(caught.value)
    assert not any(t.name == "factory-command-cancellation" for t in threading.enumerate())


@pytest.mark.parametrize("stop", ["cancel", "deadline"])
def test_docker_command_retirement_stays_confirmed_during_finalization(tmp_path, monkeypatch, stop):
    import subprocess
    from auto_mindsdb_factory import docker_runner
    clock, requested, removed, calls = [0], threading.Event(), threading.Event(), []
    monkeypatch.setattr(cancellation, "time", SimpleNamespace(monotonic=lambda: clock[0]))
    monkeypatch.setattr(docker_runner.os, "getuid", lambda: 10001)
    def remove(*args, **kwargs):
        calls.append(True)
        removed.set()
        return subprocess.CompletedProcess([], 0 if len(calls) == 1 else 1)
    monkeypatch.setattr(docker_runner.subprocess, "run", remove)
    class Docker(docker_runner.DockerSandbox):
        def _docker(self, *args, **kwargs): pass
        def run(self, command):
            clock[0] = 2 if stop == "deadline" else 0
            requested.set()
            assert removed.wait(2)
            raise RuntimeError("private interrupted command")
    with pytest.raises(HarnessStopped) as caught:
        with cancellation.enter_cancellable(Docker(tmp_path, "node@sha256:" + "a" * 64),
                cancelled=lambda: stop == "cancel" and requested.is_set(), deadline=1) as sandbox:
            cancellation.run_cancellable(sandbox, [],
                cancelled=lambda: stop == "cancel" and requested.is_set(), deadline=1)
    assert calls == [True] and "cleanup could not be confirmed" not in str(caught.value)


def test_docker_failed_removal_is_never_cached_as_confirmed(tmp_path, monkeypatch):
    import subprocess
    from auto_mindsdb_factory import docker_runner
    results, calls = [1, 0], []
    def remove(*args, **kwargs):
        calls.append(True)
        return subprocess.CompletedProcess([], results.pop(0))
    monkeypatch.setattr(docker_runner.subprocess, "run", remove)
    sandbox = docker_runner.DockerSandbox(tmp_path, "node@sha256:" + "a" * 64)
    with pytest.raises(docker_runner.RunnerError): sandbox.__exit__(None, None, None)
    sandbox.abort(); sandbox.abort()
    assert calls == [True, True] and not results


@pytest.mark.parametrize("result", [None, SimpleNamespace(returncode=None), SimpleNamespace(returncode=False)])
def test_unknown_docker_retirement_result_cannot_confirm_exit(tmp_path, monkeypatch, result):
    from auto_mindsdb_factory import docker_runner
    results = [result, SimpleNamespace(returncode=0)]
    monkeypatch.setattr(docker_runner.subprocess, "run", lambda *a, **kw: results.pop(0))
    sandbox = docker_runner.DockerSandbox(tmp_path, "node@sha256:" + "a" * 64)
    with pytest.raises(docker_runner.RunnerError, match="not confirmed"):
        sandbox.__exit__(None, None, None)
    sandbox.abort()
    assert not results


def test_new_docker_entry_requires_new_removal_confirmation(tmp_path, monkeypatch):
    import subprocess
    from auto_mindsdb_factory import docker_runner
    calls = []
    monkeypatch.setattr(docker_runner.os, "getuid", lambda: 10001)
    def remove(*args, **kwargs):
        calls.append(True)
        return subprocess.CompletedProcess([], 0)
    monkeypatch.setattr(docker_runner.subprocess, "run", remove)
    sandbox = docker_runner.DockerSandbox(tmp_path, "node@sha256:" + "a" * 64)
    monkeypatch.setattr(sandbox, "_docker", lambda *a, **kw: None)
    for _ in range(2): sandbox.__enter__(); sandbox.abort()
    assert calls == [True, True]


def test_real_setup_exit_cannot_reuse_source_after_deadline(tmp_path, monkeypatch):
    from auto_mindsdb_factory import product_delivery as module
    clock, calls = [0], []
    monkeypatch.setattr(cancellation, "time", SimpleNamespace(monotonic=lambda: clock[0]))
    def export(target, sha, workspace):
        workspace.mkdir()
        (workspace / "source.txt").write_text("retained")
    monkeypatch.setattr(module, "export_source", export)
    class Manager:
        def __enter__(self): return self
        def __exit__(self, *args): clock[0] = 2
        def abort(self): calls.append("abort")
    delivery = object.__new__(module.ProductDelivery)
    delivery.target, delivery.deadline, delivery.cancelled = tmp_path, 100, lambda: False
    delivery.settings = {"setup": [["npm", "ci"]], "image": "image", "setup_network": True, "platform": None}
    delivery.sandbox_factory = lambda *a, **kw: Manager()
    delivery._command = lambda *a, **kw: {"exit_code": 0}
    with pytest.raises(HarnessStopped):
        delivery._prepare("base", tmp_path / "source", SimpleNamespace(append=lambda *a, **kw: None), deadline=1)
    assert calls == ["abort"] and (tmp_path / "source/source.txt").read_text() == "retained"


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
