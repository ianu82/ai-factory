"""T1's runtime contract (mind/contracts.py) over T2's engine and MindsHub provider.

One MindsHub session per turn: the readiness probe (when not cached) and every engine send carry the session id
of T1's ledger, so its durable charge rows reconcile against MindsHub's cumulative cost for exactly that turn.
Every paid send is admitted by T1's ledger first (before_call claims the person's paid-call lease and checks the
daily budget); after_call releases the lease once the send has finished.
"""
from __future__ import annotations

import asyncio
import json
from typing import Any, Awaitable, Callable

from fastapi import HTTPException

from ..db import SessionLocal
from ..models import MindAction, MindActionResult, MindCharge, Workspace
from .api import scrub, secrets_for
from .contracts import TurnContext
from .engine import MindEngine
from .provider import MindUnavailable, Readiness, _ProviderStop
from .store import AccountingError, memberships, owned
from .tools import clean

# MindsHub can report a just-finished send's cost a moment late; wait this long before calling usage unknown.
RECONCILE_TRIES = 4
RECONCILE_WAIT = 1.5


class LedgerBridge:
    """T2's per-send ledger (check before, add after) over T1's async TurnLedger."""

    def __init__(self, ledger: Any, wait: float | None = None) -> None:
        self.ledger, self.wait = ledger, RECONCILE_WAIT if wait is None else wait
        self.error: BaseException | None = None

    def check(self) -> None:
        if self.error is not None:
            raise _ProviderStop("budget_exceeded")

    async def admit(self) -> None:
        for attempt in range(RECONCILE_TRIES):
            try:
                await self.ledger.before_call()
                return
            except AccountingError as exc:
                if exc.code == "accounting_pending" and attempt + 1 < RECONCILE_TRIES:
                    await asyncio.sleep(self.wait)
                    continue
                self.error = exc
            except HTTPException as exc:  # the thread or workspace is no longer the person's
                self.error = exc
            # A BaseException passes ChatSession's own retry loop; the runtime re-raises the original error.
            raise _ProviderStop("budget_exceeded")

    async def add(self, model: str, usage: Any) -> None:
        # MindsHub reports cost cumulatively per session; the next admission or the end of the turn reconciles it.
        await self.ledger.after_call(None)


def _pending(user_id: str) -> bool:
    with SessionLocal() as db:
        return db.query(MindCharge).filter_by(user_id=user_id, settled=False).first() is not None


def _prompt(message: dict[str, Any], results: list[dict], workspaces: list[dict] | None = None) -> str:
    """The person's message, with the workspace Mind may read for them, what they are looking at and the
    confirmed results of earlier actions. All are Factory records, quoted as data: the persona already treats
    object text as untrusted."""
    parts = []
    if workspaces:
        workspace = workspaces[0]
        parts.append("This conversation belongs to the Factory workspace "
                     + json.dumps(workspace["name"], ensure_ascii=False) + " (id " + workspace["id"] + "). "
                     "It is the only workspace you can read or act in: pass this id as workspace_id on every tool call. "
                     "Do not look for or mention other workspaces.")
    context = {k: v for k, v in (message.get("context") or {}).items()
               if k in ("workspace_id", "kind", "id", "outcome_id", "label") and v}
    if context:
        parts.append("The person is looking at: " + json.dumps(context, ensure_ascii=False))
    if results:
        parts.append("Confirmed action results in this conversation, oldest first (Factory records): "
                     + json.dumps(results, ensure_ascii=False, default=str)[:6000])
    parts.append(str(message.get("text") or ""))
    return "\n\n".join(parts)


class EngineRuntime:
    def __init__(self, engine: MindEngine | None = None) -> None:
        self.engine = engine or MindEngine()

    @property
    def provider(self):
        return self.engine.provider

    async def readiness(self, workspace_id: str, *, ledger: Any) -> Readiness:
        return await self.provider.readiness(workspace_id, LedgerBridge(ledger), session_id=ledger.session_id)

    async def session_cost(self, workspace_id: str, session_id: str) -> float | None:
        return await self.provider.session_cost(workspace_id, session_id)

    async def run_turn(self, context: TurnContext, history: list[dict],
                       emit: Callable[[dict], Awaitable[None]], tools: Any, ledger: Any) -> None:
        messages = [m for m in history if m.get("role") in ("user", "assistant")]
        if not messages or messages[-1].get("role") != "user":
            raise ValueError("A Mind turn needs the person's message last")
        with SessionLocal() as db:
            thread = owned(db, context.user_id, context.thread_id)
            if (thread.billing_workspace_id != context.billing_workspace_id
                    or context.billing_workspace_id not in memberships(db, context.user_id)):
                raise HTTPException(404, "Thread not found")
            workspace = db.get(Workspace, thread.billing_workspace_id)
            if workspace is None:
                raise HTTPException(404, "Thread not found")
            # Legacy messages have no reliable workspace provenance: even messages without context or citations
            # may contain another workspace's data. Keep the stored transcript, but omit prior text on every turn
            # of a legacy multi-workspace thread; its unchanged scope remains the marker of that risk.
            prior = []
            if set(thread.workspace_ids) == {thread.billing_workspace_id}:
                prior = [{"role": m["role"], "content": m["text"]} for m in messages[:-1]
                         if (m.get("text") or "").strip()]
            secrets = secrets_for(db, context.user_id, thread.workspace_ids)
            # The provider sees confirmed results and workspace names only through the same workspace-secret scrub
            # as every other egress.
            # Legacy threads can contain confirmed actions in other workspaces. Filter by the action's
            # immutable target before limiting results, so those records never reach the provider.
            rows = db.query(MindActionResult).join(MindAction, MindActionResult.action_id == MindAction.id).filter(
                MindActionResult.thread_id == thread.id, MindActionResult.owner_id == context.user_id,
                MindAction.thread_id == thread.id, MindAction.owner_id == context.user_id,
                MindAction.workspace_id == thread.billing_workspace_id,
            ).order_by(MindActionResult.created_at.desc(), MindActionResult.id.desc()).limit(20).all()
            results = scrub([clean(row.content) for row in reversed(rows)], secrets)
            workspaces = scrub([{"id": workspace.id, "name": workspace.name}], secrets)
        bridge = LedgerBridge(ledger)
        ready = await self.provider.readiness(context.billing_workspace_id, bridge, session_id=ledger.session_id)
        if not ready.available:
            if bridge.error is not None:
                raise bridge.error
            raise MindUnavailable(ready.reason or "provider_failure")

        async def forward(event: dict) -> None:
            # The private tool dispatcher reports finished tools itself, with their citations.
            if event.get("type") != "tool_result":
                await emit(event)

        try:
            await self.engine.run(workspace_id=context.billing_workspace_id, prompt=_prompt(messages[-1], results, workspaces),
                                  history=prior, tools=tools, ledger=bridge, emit=forward, readiness=ready,
                                  session_id=ledger.session_id)
        except MindUnavailable:
            if bridge.error is not None:
                raise bridge.error from None
            raise
        for attempt in range(RECONCILE_TRIES):
            await ledger.reconcile()
            if not _pending(context.user_id):
                return
            if attempt + 1 < RECONCILE_TRIES:
                await asyncio.sleep(bridge.wait)
