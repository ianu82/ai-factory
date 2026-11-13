"""Anton-only capability selection and comparable run metadata (no SDK imports)."""

from __future__ import annotations

from typing import Any

from .tools import tool_specs


DEFAULT_ANTON_OPTIONS = {"own_tools": True, "web_enabled": False, "skills_root": None}


def requested_tools(writable: bool, options: dict[str, Any]) -> list[str]:
    names = [t["function"]["name"] for t in tool_specs(writable)] + ["finish"]
    if options.get("own_tools", True) is True:
        names += ["read_image", "memorize", "recall"]
        if options.get("web_enabled") is True:
            names += ["web_search", "web_fetch"]
    return sorted(names)


def tool_config(writable: bool, options: dict[str, Any], sandbox_kind: str, *,
                effective: list[str] | None = None, fallback_reason: str | None = None) -> dict[str, Any]:
    own = options.get("own_tools", True) is True
    return {
        "requested_tools": requested_tools(writable, options),
        "effective_tools": sorted(effective or []),
        "profile": ("degraded" if fallback_reason else "factory-only" if not own else
                    "web-enabled" if options.get("web_enabled") is True else "enhanced"),
        "sandbox_kind": sandbox_kind,
        "fallback_reason": fallback_reason,
    }
