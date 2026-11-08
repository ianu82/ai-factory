"""Harness registry: native, Codex and Anton share one contract."""

from __future__ import annotations

from .anton import AntonHarness
from .base import Harness, HarnessFailed, HarnessResult, HarnessSession, HarnessUnavailable
from .codex import CodexHarness
from .native import NativeHarness

HARNESSES: dict[str, Harness] = {
    "native": NativeHarness(),
    "codex": CodexHarness(),
    "anton": AntonHarness(),
}


def get_harness(name: str) -> Harness:
    if name not in HARNESSES:
        raise HarnessUnavailable(f"Unknown harness '{name}'")
    return HARNESSES[name]


def capabilities() -> list[dict]:
    rows = []
    for name, harness in HARNESSES.items():
        ok, detail = harness.availability()
        rows.append({"name": name, "label": harness.label, "available": ok, "detail": detail})
    return rows


__all__ = ["HARNESSES", "Harness", "HarnessFailed", "HarnessResult", "HarnessSession", "HarnessUnavailable",
           "capabilities", "get_harness"]
