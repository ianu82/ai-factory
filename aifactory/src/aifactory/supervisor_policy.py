"""Strict Supervisor policy validation and defaults, including legacy settings."""
from __future__ import annotations

from typing import Any


def validate_supervisor(raw: Any) -> dict[str, Any]:
    if not isinstance(raw, dict):
        raise ValueError("supervisor must be an object")
    unknown = raw.keys() - {"mode", "interval_minutes", "stuck_minutes", "unaccepted_issue_hours"}
    if unknown:
        raise ValueError(f"Unsupported supervisor fields: {', '.join(sorted(unknown))}")
    if "mode" in raw and raw["mode"] not in ("off", "observe", "fix"):
        raise ValueError("supervisor.mode must be off, observe or fix")
    for key, minimum in (("interval_minutes", 5), ("stuck_minutes", 1), ("unaccepted_issue_hours", 1)):
        if key in raw and (type(raw[key]) is not int or raw[key] < minimum):
            raise ValueError(f"supervisor.{key} must be an integer >= {minimum}")
    return dict(raw)


def supervisor_policy(settings: Any = None) -> dict[str, Any]:
    raw = settings.get("supervisor", {}) if isinstance(settings, dict) else {}
    return {"mode": "observe", "interval_minutes": 30, "stuck_minutes": 120,
            "unaccepted_issue_hours": 24, **validate_supervisor(raw)}
