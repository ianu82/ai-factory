"""Deterministic journal fixtures for downstream metric reducers (no DB/clock needed)."""
from datetime import datetime, timedelta


def event_fixture(*rows, workspace_id="workspace", outcome_id="outcome", task_id="task"):
    """Rows are (seconds since epoch, kind, structured data); returns API event dicts.

    Explicit correlation IDs in data let fixtures model concurrent attempts and
    queue episodes without random UUIDs. Order is stable even at equal timestamps.
    """
    epoch = datetime(2026, 1, 1)
    return [{"id": i, "workspace_id": workspace_id, "outcome_id": outcome_id,
             "task_id": task_id, "run_id": data.get("run_id"), "actor": "factory",
             "level": "info", "message": "", "kind": kind, "data": dict(data),
             "created_at": (epoch + timedelta(seconds=seconds)).isoformat() + "Z"}
            for i, (seconds, kind, data) in enumerate(rows, 1)]
