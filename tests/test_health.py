from copy import deepcopy
from datetime import datetime, timedelta, timezone
import builtins
import json
import socket
import subprocess

import pytest

from auto_mindsdb_factory.health import evaluate_health


NOW = datetime(2026, 1, 1, tzinfo=timezone.utc)


def stamp(seconds=0):
    return (NOW + timedelta(seconds=seconds)).isoformat()


def snapshot(state="idle", **fields):
    return {"observed_at": stamp(), "state": state, "task_outcome": "none", **fields}


def evaluate(value):
    return evaluate_health(value, now=NOW, max_age_seconds=60)


@pytest.mark.parametrize("state,outcome,reason,readiness", [
    ("idle", "none", "idle", "ready"),
    ("idle", "failed", "task_attention", "ready"),
    ("idle", "blocked", "task_attention", "ready"),
    ("paused", "none", "paused", "not_ready"),
    ("blocked", "running", "blocked", "not_ready"),
])
def test_states_and_task_outcomes_are_independent(state, outcome, reason, readiness):
    result = evaluate(snapshot(state, task_outcome=outcome))
    assert (result["state"], result["task_outcome"], result["liveness"], result["readiness"], result["reason"]) == (
        state, outcome, "alive", readiness, reason)
    assert result["observation_age_seconds"] == 0
    assert (result["action"] is None) == (reason == "idle")


@pytest.mark.parametrize("kind", ["process", "heartbeat"])
def test_busy_live_evidence_and_deadline(kind):
    sample = snapshot("busy", task_outcome="running", live={"kind": kind, "observed_at": stamp(-60)}, deadline=stamp(1))
    result = evaluate(sample)
    assert (result["liveness"], result["readiness"], result["reason"], result["live_evidence_age_seconds"]) == (
        "alive", "not_ready", "busy", 60)
    sample["task_outcome"] = "failed"
    assert evaluate(sample)["reason"] == "task_attention"
    assert evaluate(sample)["liveness"] == "alive" and evaluate(sample)["action"]
    sample["deadline"] = stamp()
    result = evaluate(sample)
    assert (result["liveness"], result["readiness"], result["reason"]) == ("alive", "not_ready", "deadline_exceeded")
    assert result["action"]


@pytest.mark.parametrize("value,reason", [
    (None, "observation_missing"),
    ({}, "observation_invalid"),
    (snapshot(observed_at="bad"), "observation_invalid"),
    (snapshot(observed_at="2026-01-01T00:00:00"), "observation_invalid"),
    (snapshot(observed_at=stamp(1)), "observation_future"),
    (snapshot(observed_at=stamp(-61)), "observation_stale"),
    (snapshot(state=[]), "observation_invalid"),
    (snapshot(task_outcome="secret"), "observation_invalid"),
    (snapshot("busy"), "busy_evidence_missing"),
    (snapshot("busy", live={"kind": "journal", "observed_at": stamp()}, deadline=stamp(5)), "busy_evidence_invalid"),
    (snapshot("busy", live={"kind": "process", "observed_at": "bad"}, deadline=stamp(5)), "busy_evidence_invalid"),
    (snapshot("busy", live={"kind": "process", "observed_at": stamp()}, deadline="bad"), "busy_evidence_invalid"),
    (snapshot("busy", live={"kind": "process", "observed_at": stamp(1)}, deadline=stamp(5)), "busy_evidence_future"),
    (snapshot("busy", live={"kind": "heartbeat", "observed_at": stamp(-61)}, deadline=stamp(5)), "busy_evidence_stale"),
])
def test_unavailable_invalid_future_and_stale_are_never_healthy(value, reason):
    result = evaluate(value)
    assert (result["reason"], result["liveness"], result["readiness"]) == (reason, "unknown", "unknown")
    assert result["action"]


def test_freshness_boundary_and_stale_snapshot_with_fresh_process():
    assert evaluate(snapshot(observed_at=stamp(-60)))["reason"] == "idle"
    value = snapshot("busy", observed_at=stamp(-61), live={"kind": "process", "observed_at": stamp()}, deadline=stamp(5))
    assert evaluate(value)["reason"] == "observation_stale"


def test_activity_is_not_a_heartbeat_and_is_not_required():
    sample = snapshot("busy", activity_at=stamp(), deadline=stamp(10))
    assert evaluate(sample)["reason"] == "busy_evidence_missing"
    sample["live"] = {"kind": "heartbeat", "observed_at": stamp()}
    del sample["activity_at"]
    assert evaluate(sample)["reason"] == "busy"


def test_repeated_pure_evaluation_does_not_touch_evidence_or_call_services(tmp_path, monkeypatch):
    evidence = tmp_path / "event.json"
    evidence.write_bytes(b"original event")
    before_stat = evidence.stat()
    sample = snapshot("busy", live={"kind": "process", "observed_at": stamp()}, deadline=stamp(10),
                      payload={"secret": "credential-or-command"})
    before = deepcopy(sample)
    def forbidden(*args, **kwargs):
        pytest.fail("evaluation attempted external access")
    with monkeypatch.context() as patch:
        patch.setattr(builtins, "open", forbidden)
        patch.setattr(subprocess, "run", forbidden)
        patch.setattr(subprocess, "Popen", forbidden)
        patch.setattr(socket, "socket", forbidden)
        first = evaluate(sample)
        assert evaluate(sample) == first
    assert sample == before and evidence.read_bytes() == b"original event"
    assert evidence.stat().st_mtime_ns == before_stat.st_mtime_ns
    assert "credential-or-command" not in json.dumps(first)


@pytest.mark.parametrize("now,age", [(NOW.replace(tzinfo=None), 60), (NOW, True), (NOW, 0), (NOW, 86401)])
def test_bad_configuration_rejected(now, age):
    with pytest.raises(ValueError, match="Expected an aware clock"):
        evaluate_health(snapshot(), now=now, max_age_seconds=age)
