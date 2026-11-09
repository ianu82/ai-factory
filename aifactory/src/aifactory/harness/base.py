"""The shared harness contract.

Every worker — native, Codex or Anton — receives the same HarnessSession and must return
a HarnessResult whose ``report`` validates against the role's report schema. The Factory,
not the harness, owns budgets, sandboxes, credentials, acceptance and publication.
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable, Protocol

from ..inference import ModelClient, UsageLedger
from ..sandbox import LocalSandbox


class HarnessUnavailable(RuntimeError):
    """The harness cannot run in this deployment (missing binary, unsupported provider)."""


class HarnessFailed(RuntimeError):
    pass


@dataclass
class HarnessSession:
    run_id: str
    role: str  # planner|builder|verifier
    system: str
    prompt: str
    sandbox: LocalSandbox
    model: str
    client: ModelClient
    ledger: UsageLedger
    report_schema: dict[str, Any]
    writable: bool
    max_turns: int = 80
    deadline: float = field(default_factory=lambda: time.monotonic() + 3600)
    command_timeout: float = 600
    cancelled: asyncio.Event = field(default_factory=asyncio.Event)
    on_progress: Callable[[str, str, dict[str, Any]], None] = lambda kind, message, data: None
    take_guidance: Callable[[], list[str]] = lambda: []
    gateway_url: str = ""
    gateway_token: str = ""
    reasoning_effort: str | None = None
    # Stored originals: {name: filename, mime_type: detected media type, content: base64 bytes}.
    # Harnesses own provider-specific image delivery/fallback.
    image_inputs: list[dict[str, str]] = field(default_factory=list)

    def remaining(self) -> float:
        return self.deadline - time.monotonic()


@dataclass
class HarnessResult:
    report: dict[str, Any]
    turns: int
    transcript: list[dict[str, Any]] = field(default_factory=list)
    log: str = ""


class Harness(Protocol):
    name: str
    label: str

    def availability(self) -> tuple[bool, str]: ...

    async def run(self, session: HarnessSession) -> HarnessResult: ...


ProgressFn = Callable[[str, str, dict[str, Any]], Awaitable[None] | None]
