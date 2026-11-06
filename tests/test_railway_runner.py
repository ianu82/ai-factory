import threading
from types import SimpleNamespace

import pytest

from auto_mindsdb_factory.docker_runner import RunnerError
from auto_mindsdb_factory.railway_runner import RailwayOperationError, RailwayPool, bridge, railway_failure_facts
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


def test_heartbeat_continues_while_remote_command_is_blocked(tmp_path):
    provider = Provider()
    started, release, alive = threading.Event(), threading.Event(), threading.Event()
    def transport(request):
        if request["action"] == "request" and request["request"]["action"] == "run":
            started.set()
            assert release.wait(5)
        if request["action"] == "heartbeat":
            alive.set()
        return provider(request)
    pool = RailwayPool(transport)
    command = heartbeat = None
    results = []
    try:
        pool.open(tmp_path)
        command = threading.Thread(target=lambda: results.append(
            pool.call({"action": "run", "command": ["sleep", "90"]})))
        command.start()
        assert started.wait(2)
        heartbeat = threading.Thread(target=pool._keep_alive)
        heartbeat.start()
        assert alive.wait(2), "A running command prevented its worker's keepalive"
        assert command.is_alive()
    finally:
        release.set()
        for thread in (command, heartbeat):
            if thread:
                thread.join(5)
        pool.close()
    assert all(not thread.is_alive() for thread in (command, heartbeat))
    assert results[0]["exit_code"] == 0


def test_late_heartbeat_failure_does_not_poison_replacement(tmp_path):
    provider = Provider()
    started, release = threading.Event(), threading.Event()
    def transport(request):
        if request["action"] == "heartbeat":
            started.set()
            assert release.wait(5)
            raise RuntimeError("retired worker")
        return provider(request)
    pool = RailwayPool(transport)
    heartbeat = None
    first, second = tmp_path / "first", tmp_path / "second"
    first.mkdir()
    second.mkdir()
    try:
        pool.open(first)
        heartbeat = threading.Thread(target=pool._keep_alive)
        heartbeat.start()
        assert started.wait(2)
        pool.open(second)
        release.set()
        heartbeat.join(5)
        assert not heartbeat.is_alive()
        assert pool.failure is None
        assert pool.call({"action": "run", "command": ["true"]})["exit_code"] == 0
    finally:
        release.set()
        if heartbeat:
            heartbeat.join(5)
        pool.close()


def test_current_worker_heartbeat_failure_still_fails_closed(tmp_path):
    provider = Provider()
    def transport(request):
        if request["action"] == "heartbeat":
            raise RuntimeError("private provider body")
        return provider(request)
    pool = RailwayPool(transport)
    try:
        pool.open(tmp_path)
        pool._keep_alive()
        with pytest.raises(RunnerError, match=r"^Railway heartbeat failed \(RuntimeError\)$"):
            pool.call({"action": "run", "command": ["true"]})
    finally:
        pool.close()


@pytest.mark.parametrize("payload,expected", [
    ('{"error":"RailwayAuthError","message":"provider-private-body"}', "RailwayAuthError"),
    ('{"error":"provider-private-body"}', "UnknownError"),
    ('provider-private-body', "InvalidJSON"),
    ('[]', "InvalidResponse"),
])
def test_bridge_retains_only_approved_error_classification(monkeypatch, payload, expected):
    import auto_mindsdb_factory.railway_runner as module
    monkeypatch.setenv("RAILWAY_API_TOKEN", "private-token")
    monkeypatch.setenv("RAILWAY_ENVIRONMENT_ID", "environment")
    monkeypatch.setattr(module.subprocess, "run", lambda *a, **k: SimpleNamespace(
        returncode=1, stdout=payload, stderr="provider-private-body"))
    with pytest.raises(RailwayOperationError) as failure:
        bridge({"action": "request", "request": {"action": "run"}})
    assert failure.value.public_reason() == f"Railway run failed ({expected})"
    assert railway_failure_facts(failure.value) == ("run", expected)
    assert "provider-private-body" not in str(failure.value)


def test_original_transport_error_survives_context_and_cleanup_failures(tmp_path, capsys):
    provider = Provider()
    original = RailwayOperationError({"action": "request", "request": {"action": "run"}}, "ExecInterruptedError")
    def transport(request):
        if request["action"] == "request" and request["request"]["action"] == "run":
            raise original
        if request["action"] == "destroy":
            raise RuntimeError("private cleanup response")
        return provider(request)
    pool = RailwayPool(transport)
    try:
        with pytest.raises(RailwayOperationError) as failure:
            with pool(tmp_path, "pinned") as sandbox:
                sandbox.run(["true"])
        assert failure.value is original
        assert pool.failure is not None
    finally:
        pool.close()
    assert "private cleanup response" not in capsys.readouterr().err


@pytest.mark.parametrize("action", ["create", "destroy", "heartbeat", "upload", "download", "enter", "run", "exit"])
@pytest.mark.parametrize("nested", [False, True])
def test_original_runner_facts_bind_each_operation(action, nested):
    operation = {"action": action}
    error = RailwayOperationError({"request": operation} if nested else operation, "OSError")
    assert railway_failure_facts(error) == (action, "OSError")
    assert error.args == (f"Railway {action} failed (OSError)",)
    assert error.public_reason() == error.args[0]


class HostileFact(str):
    def __eq__(self, other):
        raise AssertionError("untrusted equality executed")

    def __str__(self):
        raise AssertionError("untrusted formatting executed")


class HostileMapping(dict):
    def get(self, *args):
        raise AssertionError("untrusted mapping executed")


@pytest.mark.parametrize("payload", [None, [], "https://private-token", {"request": None},
    HostileMapping(action="run"), {"request": HostileMapping(action="run")},
    {"action": []}, {"action": HostileFact("run")}, {"request": {"action": "https://private-token"}}])
def test_malformed_request_does_not_format_or_trust_unbounded_facts(payload):
    error = RailwayOperationError(payload, HostileFact("OSError"))
    assert railway_failure_facts(error) == ("unknown", "UnknownError")
    assert error.public_reason() == "Railway unknown failed (UnknownError)"
    assert "private-token" not in str(error)


@pytest.mark.parametrize("category", [None, [], "https://private-token", HostileFact("OSError")])
def test_untrusted_categories_cannot_escape_constructor_allowlist(category):
    error = RailwayOperationError({"action": "run"}, category)
    assert railway_failure_facts(error) == ("run", "UnknownError")
    assert error.public_reason() == "Railway run failed (UnknownError)"


@pytest.mark.parametrize("changes", [
    {"phase": "exit"}, {"error_type": "OSError"}, {"args": ("private-token",)},
    {"phase": "exit", "error_type": "OSError", "args": ("Railway exit failed (OSError)",)},
    {"phase": "https://private-token"}, {"error_type": HostileFact("ExecInterruptedError")},
    {"phase": None}, {"args": (HostileFact("Railway run failed (ExecInterruptedError)"),)},
    {"args": ()}, {"args": ("one", "two")},
])
def test_mutated_runner_facts_fail_closed_even_when_copies_agree(changes):
    error = RailwayOperationError({"action": "run"}, "ExecInterruptedError")
    for name, value in changes.items():
        object.__setattr__(error, name, value)
    # Adding another alleged original to the error cannot replace the witness.
    error.__dict__["_original_failure_facts"] = (error.phase, error.error_type)
    assert railway_failure_facts(error) == ("unknown", "UnknownError")
    assert error.public_reason() == "Railway unknown failed (UnknownError)"


def test_missing_or_forged_runner_state_is_not_constructor_provenance():
    error = RailwayOperationError.__new__(RailwayOperationError)
    error.phase, error.error_type = "run", "OSError"
    error.args = ("Railway run failed (OSError)",)
    assert railway_failure_facts(error) == ("unknown", "UnknownError")
    class DerivedError(RailwayOperationError):
        pass
    assert railway_failure_facts(DerivedError({"action": "run"}, "OSError")) == ("unknown", "UnknownError")
    issued = RailwayOperationError({"action": "run"}, "OSError")
    issued.__dict__.clear()
    assert railway_failure_facts(issued) == ("unknown", "UnknownError")
    issued.__dict__ = HostileMapping(phase="run", error_type="OSError")
    assert railway_failure_facts(issued) == ("unknown", "UnknownError")


def test_reinitialization_cannot_replace_original_runner_facts():
    error = RailwayOperationError({"action": "run"}, "ExecInterruptedError")
    with pytest.raises(TypeError, match="already initialized"):
        error.__init__({"action": "exit"}, "OSError")
    assert railway_failure_facts(error) == ("run", "ExecInterruptedError")


def test_constructor_witness_does_not_retain_exception_or_traceback():
    import gc
    import weakref
    def raised_error():
        try:
            raise RailwayOperationError({"action": "run"}, "OSError")
        except RailwayOperationError as error:
            assert error.__traceback__ is not None
            assert railway_failure_facts(error) == ("run", "OSError")
            return weakref.ref(error)
    reference = raised_error()
    gc.collect()
    assert reference() is None
