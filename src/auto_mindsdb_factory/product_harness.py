"""Trusted agent execution boundary; unavailable backends never fall back."""

from typing import Any, Callable, Mapping, Protocol


HARNESS_NAMES = ("native", "anton", "codex")
_ROLE_FIELDS = {"builder": "builder_harness", "verifier": "reviewer_harness"}


class HarnessSelectionError(ValueError):
    pass


class AgentHarness(Protocol):
    def run(self, role: str, context: dict[str, Any],
            command: Callable[[list[str]], dict]) -> dict: ...


def resolve_harness(document: Mapping[str, Any], role: str) -> str:
    """Resolve trusted configuration without modifying its canonical document."""
    if role not in _ROLE_FIELDS:
        raise HarnessSelectionError("Unknown product agent role")
    name = document.get(_ROLE_FIELDS[role], "native")
    if name not in HARNESS_NAMES:
        raise HarnessSelectionError("Unknown product agent harness")
    # Reserve names, but fail before execution until their adapters are ready.
    if name != "native":
        raise HarnessSelectionError(f"Product agent harness is unavailable: {name}")
    return name
