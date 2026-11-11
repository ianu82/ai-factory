"""Private Mind integration boundary. No provider objects or credentials cross it."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Protocol

from pydantic import BaseModel, ConfigDict, Field


@dataclass(frozen=True)
class TurnContext:
    user_id: str
    thread_id: str
    billing_workspace_id: str
    workspace_ids: tuple[str, ...]


class ContextBody(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)
    workspace_id: str = Field(min_length=1, max_length=32)
    kind: str = Field(min_length=1, max_length=40)
    id: str | None = Field(default=None, max_length=100)
    outcome_id: str | None = Field(default=None, max_length=32)
    label: str = Field(max_length=300)


class CreateThread(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)
    billing_workspace_id: str = Field(min_length=1, max_length=32)


class SubmitMessage(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)
    text: str = Field(min_length=1, max_length=8000)
    client_message_id: str = Field(min_length=1, max_length=128)
    context: ContextBody | None = None


class Ledger(Protocol):
    session_id: str

    async def before_call(self) -> None: ...
    async def after_call(self, usage: Any) -> None: ...
    async def reconcile(self) -> None: ...


class Runtime(Protocol):
    async def readiness(self, workspace_id: str, *, ledger: Ledger) -> Any:
        """No paid probe without ledger.before_call(); finish with after_call()."""
        ...

    async def run_turn(self, context: TurnContext, history: list[dict],
                       emit: Callable[[dict], Awaitable[None]], tools: Any,
                       ledger: Ledger) -> Any: ...

    async def session_cost(self, workspace_id: str, session_id: str) -> float | None:
        """MindsHub's cumulative session charge, including failed sends."""
        ...
