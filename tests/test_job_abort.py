import threading
from types import SimpleNamespace

import pytest

from auto_mindsdb_factory.docker_runner import DockerSandbox, RunnerError
from auto_mindsdb_factory.railway_runner import RailwayPool


def test_abort_destroys_owned_vm_while_command_holds_lifecycle_lock(tmp_path):
    entered, released, caught = threading.Event(), threading.Event(), threading.Event()
    calls, errors = [], []
    def transport(request):
        calls.append(request)
        if request["action"] == "create":
            return {"id": "owned-worker"}
        if request["action"] == "destroy":
            assert request["id"] == "owned-worker"
            released.set()
            assert caught.wait(3), "command failure could not drain during cancellation"
        if request["action"] == "request" and request["request"]["action"] == "run":
            entered.set()
            assert released.wait(3), "abort waited behind the command lock"
            raise RunnerError("Remote command terminated")
        return {}
    pool = RailwayPool(transport)
    pool.open(tmp_path)
    def run():
        try:
            pool.call({"action": "run", "command": ["sleep", "600"]})
        except RunnerError as exc:
            errors.append(exc)
            caught.set()
    thread = threading.Thread(target=run)
    thread.start()
    try:
        assert entered.wait(3)
        pool.abort()
        thread.join(timeout=3)
        assert not thread.is_alive() and len(errors) == 1
        with pytest.raises(RunnerError):
            pool.open(tmp_path)
        assert pool.id is None and pool.stopped.is_set()
        assert [r["id"] for r in calls if r["action"] == "destroy"] == ["owned-worker"]
    finally:
        released.set()
        thread.join(timeout=3)
        pool.close()
    assert not pool.thread.is_alive()


def test_abort_racing_creation_retires_late_worker(tmp_path):
    calls = []
    def transport(request):
        calls.append(request)
        if request["action"] == "create":
            pool.abort()
            return {"id": "late-worker"}
        return {}
    pool = RailwayPool(transport)
    try:
        with pytest.raises(RunnerError, match="cancelled"):
            pool.open(tmp_path)
        assert [r["id"] for r in calls if r["action"] == "destroy"] == ["late-worker"]
        assert pool.id is None
    finally:
        pool.close()


@pytest.mark.parametrize("failed", [False, True])
def test_docker_abort_confirms_whole_container_removal(tmp_path, monkeypatch, failed):
    sandbox = DockerSandbox(tmp_path, "python@sha256:" + "a" * 64)
    calls = []
    def remove(*args, **kwargs):
        calls.append((args, kwargs))
        return SimpleNamespace(returncode=int(failed))
    monkeypatch.setattr("auto_mindsdb_factory.docker_runner.subprocess.run", remove)
    if failed:
        with pytest.raises(RunnerError, match="confirmed"):
            sandbox.abort()
    else:
        sandbox.abort()
    assert calls == [((["docker", "rm", "--force", sandbox.name],), {"capture_output": True, "timeout": 30})]
