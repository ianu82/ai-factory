"""Append-only journal plus an in-process pub/sub used for live UI updates."""

from __future__ import annotations

import asyncio
import json
import logging
import threading
from typing import Any

from sqlalchemy.orm import Session

from .db import session_scope
from .models import Event
from .security import redact


class EventBus:
    def __init__(self) -> None:
        self._subscribers: dict[int, tuple[asyncio.AbstractEventLoop, asyncio.Queue, str | None]] = {}
        self._lock = threading.Lock()
        self._next = 0

    def subscribe(self, workspace_id: str | None) -> tuple[int, asyncio.Queue]:
        queue: asyncio.Queue = asyncio.Queue(maxsize=1000)
        loop = asyncio.get_running_loop()
        with self._lock:
            self._next += 1
            sub_id = self._next
            self._subscribers[sub_id] = (loop, queue, workspace_id)
        return sub_id, queue

    def unsubscribe(self, sub_id: int) -> None:
        with self._lock:
            self._subscribers.pop(sub_id, None)

    def publish(self, payload: dict[str, Any]) -> None:
        with self._lock:
            subscribers = list(self._subscribers.values())
        for loop, queue, workspace_id in subscribers:
            if workspace_id and payload.get("workspace_id") not in (workspace_id, None):
                continue

            def _put(q=queue, p=payload):
                try:
                    q.put_nowait(p)
                except asyncio.QueueFull:
                    pass

            try:
                loop.call_soon_threadsafe(_put)
            except RuntimeError:  # loop closed
                self.unsubscribe_queue(queue)

    def unsubscribe_queue(self, queue: asyncio.Queue) -> None:
        with self._lock:
            for key, (_, q, _) in list(self._subscribers.items()):
                if q is queue:
                    self._subscribers.pop(key, None)


bus = EventBus()
log = logging.getLogger("aifactory.events")


def event_dict(event: Event) -> dict[str, Any]:
    return {
        "id": event.id,
        "workspace_id": event.workspace_id,
        "outcome_id": event.outcome_id,
        "task_id": event.task_id,
        "run_id": event.run_id,
        "actor": event.actor,
        "level": event.level,
        "kind": event.kind,
        "message": event.message,
        "data": event.data,
        "created_at": event.created_at.isoformat() + "Z",
    }


def record(
    workspace_id: str,
    kind: str,
    message: str = "",
    *,
    outcome_id: str | None = None,
    task_id: str | None = None,
    run_id: str | None = None,
    level: str = "info",
    actor: str = "factory",
    data: dict[str, Any] | None = None,
    session: Session | None = None,
) -> dict[str, Any]:
    clean = json.loads(redact(json.dumps(data or {}, default=str)))
    event = Event(
        workspace_id=workspace_id,
        outcome_id=outcome_id,
        task_id=task_id,
        run_id=run_id,
        actor=actor,
        level=level,
        kind=kind,
        message=redact(message)[:4000],
        data=clean,
    )
    if session is not None:
        session.add(event)
        session.flush()
        payload = event_dict(event)
    else:
        with session_scope() as s:
            s.add(event)
            s.flush()
            payload = event_dict(event)
    bus.publish({"type": "event", **payload})
    if kind not in ("agent_thought", "agent_tool"):
        log.info("[%s %s%s] %s: %s", workspace_id[:6], (outcome_id or "-")[:6], f"/{task_id[:6]}" if task_id else "",
                 kind, payload["message"][:300])
    return payload


def notify(workspace_id: str, entity: str, entity_id: str, **extra: Any) -> None:
    """Tell live clients an entity changed so they can refetch it."""
    bus.publish({"type": "changed", "workspace_id": workspace_id, "entity": entity, "id": entity_id, **extra})
