"""Repository steward policy defaults and strict validation.

Callers use steward_policy(repo.config) for the effective policy, including for
legacy repositories. Validation never coerces JSON strings, floats or booleans.
"""

from __future__ import annotations

from typing import Any


def validate_steward(raw: Any) -> dict[str, Any]:
    """Validate a partial steward update without mutating it."""
    if not isinstance(raw, dict):
        raise ValueError("steward must be an object")
    unknown = raw.keys() - {"mode", "fix_minutes"}
    if unknown:
        raise ValueError(f"Unsupported steward fields: {', '.join(sorted(unknown))}")
    if "mode" in raw and raw["mode"] not in ("off", "propose", "fix"):
        raise ValueError("steward.mode must be off, propose or fix")
    if "fix_minutes" in raw:
        minutes = raw["fix_minutes"]
        if type(minutes) is not int or minutes <= 0:
            raise ValueError("steward.fix_minutes must be a positive integer")
    return dict(raw)


def steward_policy(config: dict[str, Any] | None = None) -> dict[str, Any]:
    """Return a fresh, normalized effective policy from repository configuration."""
    raw = (config or {}).get("steward", {})
    return {"mode": "propose", "fix_minutes": 60, **validate_steward(raw)}
