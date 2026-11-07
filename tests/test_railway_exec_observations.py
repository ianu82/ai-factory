import json
from types import SimpleNamespace

import pytest

from auto_mindsdb_factory import railway_runner as runner


def observations():
    return dict.fromkeys(("connections", "opened", "errors", "closed", "http_rejections",
        "last_http_status", "last_close_code", "capabilities_frames", "typed_error_frames",
        "hello_send_attempts", "init_send_attempts", "init_send_returned",
        "unclassified_control_sends"), 0) | {
        "connections": 2, "http_rejections": 2, "last_http_status": 503,
        "counters_saturated": False, "observation_failed": False}


@pytest.mark.parametrize("category", ["ExecNotStartedError", "ExecInterruptedError", "ExecControlUnsupportedError"])
def test_bridge_preserves_bounded_exec_observations_and_original_error(monkeypatch, category):
    facts = observations()
    monkeypatch.setenv("RAILWAY_API_TOKEN", "test-only")
    monkeypatch.setenv("RAILWAY_ENVIRONMENT_ID", "test-only")
    monkeypatch.setattr(runner.subprocess, "run", lambda *a, **k: SimpleNamespace(
        returncode=1, stdout=json.dumps({"error": category, "exec_observations": facts,
            "message": "private-provider-text"}), stderr="private-provider-text"))
    with pytest.raises(runner.RailwayOperationError) as raised:
        runner.bridge({"request": {"action": "run"}})
    error = raised.value
    assert runner.railway_failure_facts(error) == ("run", category)
    assert error.args == (f"Railway run failed ({category})",)
    assert runner.railway_exec_observations(error) == facts
    assert "private-provider-text" not in str(error) + repr(vars(error))


@pytest.mark.parametrize("changes", [
    {"connections": True}, {"connections": -1}, {"connections": 65536}, {"connections": 1.0},
    {"last_http_status": 99}, {"last_http_status": 600}, {"last_close_code": 999},
    {"last_close_code": 5000}, {"observation_failed": 0}, {"counters_saturated": "false"},
    {"private-provider-key": "private-provider-text"},
])
def test_malformed_snapshot_is_unknown_without_changing_primary_failure(changes):
    error = runner.RailwayOperationError({"action": "run"}, "ExecNotStartedError",
        exec_observations=observations() | changes)
    assert runner.railway_exec_observations(error) is None
    assert runner.railway_failure_facts(error) == ("run", "ExecNotStartedError")
    assert "private-provider" not in repr(vars(error))


def test_metadata_witness_is_detached_and_requires_original_failure_provenance():
    facts = observations()
    error = runner.RailwayOperationError({"action": "run"}, "ExecNotStartedError", exec_observations=facts)
    facts["last_http_status"] = 401
    returned = runner.railway_exec_observations(error)
    assert returned["last_http_status"] == 503
    returned["last_http_status"] = 402
    assert runner.railway_exec_observations(error)["last_http_status"] == 503
    error.phase, error.error_type, error.args = "exit", "ExecInterruptedError", ("Railway exit failed (ExecInterruptedError)",)
    assert runner.railway_exec_observations(error) is None
    forged = runner.RailwayOperationError.__new__(runner.RailwayOperationError)
    forged.exec_observations = observations()
    assert runner.railway_exec_observations(forged) is None


@pytest.mark.parametrize("payload", [None, [], {}, {"connections": 0}])
def test_missing_snapshot_stays_unknown(payload):
    error = runner.RailwayOperationError({"action": "run"}, "ExecNotStartedError", exec_observations=payload)
    assert runner.railway_exec_observations(error) is None


def test_unrelated_error_does_not_attest_to_execution_observations():
    error = runner.RailwayOperationError({"action": "run"}, "OSError", exec_observations=observations())
    assert runner.railway_exec_observations(error) is None


def test_unavailable_diagnostic_preserves_original_failure(monkeypatch):
    def unavailable(value):
        raise ValueError("private-diagnostic-text")
    monkeypatch.setattr(runner, "_freeze_exec_observations", unavailable)
    error = runner.RailwayOperationError({"action": "run"}, "ExecNotStartedError", exec_observations=observations())
    assert runner.railway_failure_facts(error) == ("run", "ExecNotStartedError")
    assert runner.railway_exec_observations(error) is None
    assert error.args == ("Railway run failed (ExecNotStartedError)",)


def test_hostile_metadata_and_subclass_cannot_supply_constructor_evidence():
    from test_railway_runner import HostileFact, HostileMapping
    class HostileKey(HostileFact):
        __hash__ = str.__hash__
    class Derived(runner.RailwayOperationError):
        pass
    for payload in [HostileMapping(observations()), observations() | {HostileKey("private-key"): 1},
            observations() | {"connections": HostileFact("2")}]:
        error = runner.RailwayOperationError({"action": "run"}, "ExecNotStartedError", exec_observations=payload)
        assert runner.railway_exec_observations(error) is None
    assert runner.railway_exec_observations(Derived(
        {"action": "run"}, "ExecNotStartedError", exec_observations=observations())) is None


def test_metadata_cannot_be_reinitialized_and_does_not_retain_error_lifetime():
    import gc
    import weakref
    error = runner.RailwayOperationError({"action": "run"}, "ExecNotStartedError", exec_observations=observations())
    with pytest.raises(TypeError, match="already initialized"):
        error.__init__({"action": "exit"}, "ExecInterruptedError", exec_observations=observations() | {"last_http_status": 401})
    assert runner.railway_exec_observations(error) == observations()
    reference = weakref.ref(error)
    del error
    gc.collect()
    assert reference() is None


@pytest.mark.parametrize('change', [
    {'nonce': 'b' * 32}, {'sequence': True}, {'sequence': 0}, {'sequence': 65},
    {'stage': 'private-command'}, {'extra': 'private-text'},
    {'exec_observations': observations() | {'command': 'private-text'}},
])
def test_partial_progress_rejects_foreign_or_unbounded_metadata(change):
    frame = {'nonce': 'a' * 32, 'sequence': 1, 'stage': 'exec', 'exec_observations': observations()}
    stderr = 'FACTORY_BRIDGE_PROGRESS ' + json.dumps(frame | change) + '\n'
    assert runner._bridge_progress(stderr, 'a' * 32) is None


def test_partial_progress_is_bounded_detached_and_does_not_become_final_exec_evidence():
    frame = {'nonce': 'a' * 32, 'sequence': 1, 'stage': 'connect', 'exec_observations': observations()}
    line = 'FACTORY_BRIDGE_PROGRESS ' + json.dumps(frame) + '\n'
    for stderr in [line * 2, line + 'x' * 65536, line[:-1], 'FACTORY_BRIDGE_PROGRESS {bad}\n']:
        assert runner._bridge_progress(stderr, 'a' * 32) is None
    value = runner._bridge_progress('untrusted-private-text\n' + line, 'a' * 32)
    error = runner.RailwayOperationError({'action': 'run'}, 'TimeoutExpired', bridge_progress=value)
    returned = runner.railway_bridge_progress(error)
    assert returned['stage'] == 'connect' and returned['partial'] is True
    returned['exec_observations']['opened'] = 999
    assert runner.railway_bridge_progress(error)['exec_observations']['opened'] == 0
    assert runner.railway_exec_observations(error) is None
    error.phase = 'exit'
    assert runner.railway_bridge_progress(error) is None
    assert runner.railway_bridge_progress(object()) is None
    malformed = runner.RailwayOperationError({'action': 'run'}, 'TimeoutExpired', bridge_progress=(1, 'exec', ('private',)))
    assert runner.railway_bridge_progress(malformed) is None
