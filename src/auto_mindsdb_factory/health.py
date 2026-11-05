"""Pure evaluation of a bounded controller snapshot; see docs/factory-health.md."""

from datetime import datetime, timezone


_STATES = {"idle", "paused", "busy", "blocked"}
_OUTCOMES = {"none", "running", "succeeded", "failed", "blocked", "unknown"}
_ACTIONS = {
    "observation_missing": "Provide a current controller observation.",
    "observation_invalid": "Inspect the controller observation producer.",
    "observation_future": "Check the observer and evaluator clocks.",
    "observation_stale": "Check the controller and refresh its observation.",
    "idle": None,
    "paused": "Resume the controller if work should proceed.",
    "busy": None,
    "blocked": "Inspect the controller's progress blocker.",
    "busy_evidence_missing": "Provide current live evidence and a command deadline.",
    "busy_evidence_invalid": "Inspect the live evidence and command deadline producer.",
    "busy_evidence_future": "Check the live evidence and evaluator clocks.",
    "busy_evidence_stale": "Check the running command and refresh its live evidence.",
    "deadline_exceeded": "Inspect the running command and its deadline.",
    "task_attention": "Inspect the failed or blocked task separately.",
}


def _timestamp(value):
    if type(value) is not str or len(value) > 40:
        return None
    try:
        result = datetime.fromisoformat(value.replace("Z", "+00:00"))
        return result.astimezone(timezone.utc) if result.utcoffset() is not None else None
    except (ValueError, OverflowError):
        return None


def evaluate_health(observation, *, now, max_age_seconds):
    """Return a JSON-serializable v1 report, without accessing external state."""
    if (type(now) is not datetime or now.utcoffset() is None
            or type(max_age_seconds) is not int or not 1 <= max_age_seconds <= 86400):
        raise ValueError("Expected an aware clock and max age between 1 and 86400 seconds")
    now = now.astimezone(timezone.utc)
    report = {"version": 1, "state": "unknown", "task_outcome": "unknown",
              "observation_age_seconds": None, "live_evidence_age_seconds": None,
              "liveness": "unknown", "readiness": "unknown"}

    def finish(reason, liveness="unknown", readiness="unknown"):
        return {**report, "liveness": liveness, "readiness": readiness,
                "reason": reason, "action": _ACTIONS[reason]}

    if observation is None:
        return finish("observation_missing")
    if type(observation) is not dict:
        return finish("observation_invalid")
    state, outcome = observation.get("state"), observation.get("task_outcome")
    observed = _timestamp(observation.get("observed_at"))
    if (type(state) is not str or len(state) > 7 or state not in _STATES
            or type(outcome) is not str or len(outcome) > 9 or outcome not in _OUTCOMES or observed is None):
        return finish("observation_invalid")
    report.update(state=state, task_outcome=outcome)
    age = (now - observed).total_seconds()
    if age < 0:
        return finish("observation_future")
    report["observation_age_seconds"] = age
    if age > max_age_seconds:
        return finish("observation_stale")
    if state == "busy":
        live, deadline_value = observation.get("live"), observation.get("deadline")
        if live is None or deadline_value is None:
            return finish("busy_evidence_missing")
        if (type(live) is not dict or type(live.get("kind")) is not str
                or len(live["kind"]) > 9 or live["kind"] not in {"process", "heartbeat"}):
            return finish("busy_evidence_invalid")
        live_at, deadline = _timestamp(live.get("observed_at")), _timestamp(deadline_value)
        if live_at is None or deadline is None:
            return finish("busy_evidence_invalid")
        live_age = (now - live_at).total_seconds()
        if live_age < 0:
            return finish("busy_evidence_future")
        report["live_evidence_age_seconds"] = live_age
        if live_age > max_age_seconds:
            return finish("busy_evidence_stale")
        if now >= deadline:
            return finish("deadline_exceeded", "alive", "not_ready")
        reason = "task_attention" if outcome in {"failed", "blocked"} else "busy"
        return finish(reason, "alive", "not_ready")
    if state == "idle":
        reason = "task_attention" if outcome in {"failed", "blocked"} else "idle"
        return finish(reason, "alive", "ready")
    return finish(state, "alive", "not_ready")
