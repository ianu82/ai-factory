import pytest

from auto_mindsdb_factory.docker_runner import RunnerError
from auto_mindsdb_factory.railway_runner import RailwayPool
from auto_mindsdb_factory.source_transfer import pack_source


class Provider:
    def __init__(self):
        self.calls, self.serial, self.source = [], 0, None

    def __call__(self, request):
        self.calls.append(request)
        if request["action"] == "create":
            self.serial += 1
            return {"id": str(self.serial)}
        if request["action"] == "request":
            operation = request["request"]
            if operation["action"] == "upload":
                self.source = operation["source"]
            if operation["action"] == "download":
                return {"source": self.source}
            if operation["action"] == "run":
                return {"command": operation["command"], "exit_code": 0}
        return {}


def test_setup_is_retained_but_fresh_exports_use_fresh_vms(tmp_path):
    first, second = tmp_path / "builder", tmp_path / "verifier"
    first.mkdir()
    second.mkdir()
    (first / "x").write_text("source")
    (second / "x").write_text("committed")
    provider = Provider()
    pool = RailwayPool(provider)
    try:
        for options in [{"writable": True, "network": False}, {"writable": True}]:
            with pool(first, "pinned", **options) as sandbox:
                assert sandbox.run(["echo", "hello world"])["exit_code"] == 0
        assert provider.serial == 1  # even offline setup must retain its environment
        with pool(second, "pinned"):
            assert provider.serial == 2
        assert [r["id"] for r in provider.calls if r["action"] == "destroy"] == ["1"]
    finally:
        pool.close()
    assert not pool.thread.is_alive()
    assert [r["id"] for r in provider.calls if r["action"] == "destroy"] == ["1", "2"]


def test_candidate_edits_are_imported_after_container_stop(tmp_path):
    provider = Provider()
    (tmp_path / "x").write_text("before")
    pool = RailwayPool(provider)
    try:
        with pool(tmp_path, "pinned", writable=True):
            edited = tmp_path / "changed"
            edited.write_text("after")
            provider.source = pack_source(tmp_path)
            edited.unlink()
        assert edited.read_text() == "after"
        actions = [r["request"]["action"] for r in provider.calls if r["action"] == "request"]
        assert actions[-2:] == ["exit", "download"]
    finally:
        pool.close()


def test_uncertain_transport_fails_closed_and_cleanup_still_attempts(tmp_path):
    provider = Provider()
    pool = RailwayPool(provider)
    try:
        pool.open(tmp_path)
        pool.failure = RunnerError("lost worker")
        with pytest.raises(RunnerError, match="lost worker"):
            pool.call({"action": "run", "command": ["true"]})
    finally:
        pool.close()
    assert provider.calls[-1]["action"] == "destroy"


def test_disconnected_command_retires_vm_and_prohibits_further_execution(tmp_path):
    provider = Provider()
    pool = RailwayPool(provider)
    try:
        pool.open(tmp_path)
        def disconnected(request):
            if request["action"] == "request":
                raise RunnerError("connection lost")
            return provider(request)
        pool.transport = disconnected
        with pytest.raises(RunnerError, match="connection lost"):
            pool.call({"action": "run", "command": ["sleep", "1000"]})
        assert pool.id is None
        assert provider.calls[-1]["action"] == "destroy"
        with pytest.raises(RunnerError, match="uncertain"):
            pool.open(tmp_path)
    finally:
        pool.close()
