"""Private Mind router, registered by app.py under /api.

All projections (including model history and SSE) pass through scrub() as one
original value. Exact-value matches are unioned before any replacement, so one
redaction cannot destroy evidence for an overlapping match in another projection.
"""
from __future__ import annotations

import asyncio
import inspect
import itertools
import json
import re
from datetime import timedelta
from uuid import uuid4

import anyio
from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import RequestValidationError
from fastapi.routing import APIRoute
from sqlalchemy import func, update
from sqlalchemy.orm import Session
from starlette.responses import JSONResponse, StreamingResponse

from ..auth import current_user, require_member
from ..db import SessionLocal, get_db, utcnow
from ..models import MindAction, MindCharge, MindMessage, MindThread, Secret, User
from ..security import decrypt, _SECRET_PATTERNS
from . import store
from .contracts import CreateThread, SubmitMessage, TurnContext
from .provider import MindUnavailable
from .tools import Context, Tools


class PrivateRoute(APIRoute):
    """Do not echo rejected inputs (or their field names) on validation errors.

    FastAPI's default 422 includes arbitrary input, even extra fields. Keeping
    this policy on the router also covers errors before endpoint dependencies
    run, without relying on application registration or an authenticated actor.
    """

    def get_route_handler(self):
        handler = super().get_route_handler()

        async def private_handler(request: Request):
            try:
                return await handler(request)
            except RequestValidationError:
                # No request-derived data leaves this path. Still use the same
                # egress helper as every other Mind response.
                return JSONResponse(status_code=422, content=scrub({"detail": "Invalid Mind request"}))

        return private_handler


router = APIRouter(prefix="/mind", route_class=PrivateRoute)
MAX_EVENTS = 1000
MAX_OUTPUT = 64000


def secrets_for(db, user_id, scoped=()):
    # Reads can target newly joined workspaces outside the creation snapshot.
    # Include those credentials as well as the original scope on every egress.
    ids = set(store.memberships(db, user_id)) | set(scoped)
    return tuple(filter(None, (decrypt(s.ciphertext) for s in db.query(Secret).filter(Secret.workspace_id.in_(ids)))))


# Field names of the Mind wire format (threads, messages, contexts, citations, cards, stream envelopes and
# payloads, tool results). The server writes them, so they never carry a credential, and they are not part of the
# text a credential can span: a span across fields never rewrites the format. Every other key is content:
# scanned together with values, and renamed when redacted.
STRUCTURAL_KEYS = frozenset({
    "thread", "messages", "id", "title", "billing_workspace_id", "workspace_ids", "model_id", "status",
    "created_at", "updated_at", "cost", "usd", "known", "thread_id", "role", "text", "content", "context",
    "citations", "action_cards", "history", "tools", "workspace_id", "kind", "outcome_id", "task_id", "label",
    "href", "seq", "message_id", "type", "data", "call_id", "name", "summary", "card", "code", "message",
    "thread_cost", "daily_spend", "daily_limit", "revision", "workspace", "object", "fields", "effect", "result",
    "action_id", "ok", "reason", "detail", "items", "truncated", "provenance",
})


def scrub(value, secrets=()):
    """Union exact/pattern spans on original leaves, then replace each leaf span.

    Keys are leaves too. The text projection additionally catches credentials
    split between message texts/tokens despite intervening envelope metadata.
    Nothing is normalized, encoded, guessed or rewritten in the database.
    """
    leaves = []

    def collect(v, path=()):
        if isinstance(v, str):
            leaves.append((path, v))
        elif isinstance(v, dict):
            for i, (k, child) in enumerate(v.items()):
                if k not in STRUCTURAL_KEYS:
                    collect(k, path + (("key", i),))
                collect(child, path + (("value", i),))
        elif isinstance(v, (list, tuple)):
            for i, child in enumerate(v):
                collect(child, path + (i,))
    collect(value)
    masks = {p: bytearray(len(s)) for p, s in leaves}

    def mark(group):
        text = "".join(s for _, s in group)
        spans = []
        for secret in secrets:
            start = 0
            while secret and (pos := text.find(secret, start)) != -1:
                spans.append((pos, pos + len(secret)))
                start = pos + 1
        spans.extend((m.start(), m.end()) for pattern in _SECRET_PATTERNS for m in pattern.finditer(text))
        offset = 0
        for path, s in group:
            for start, end in spans:
                lo, hi = max(0, start - offset), min(len(s), end - offset)
                if lo < hi:
                    masks[path][lo:hi] = b"\1" * (hi - lo)
            offset += len(s)
    def values_only(group):
        return [(p, s) for p, s in group if not any(isinstance(x, tuple) and x[0] == "key" for x in p)]

    # A field that ends inside an unfinished credential (a key block without its END marker, an x-access-token
    # without its @) can match no pattern: redact it from the credential's start to the field's end.
    for path, s in leaves:
        if (start := open_credential(s, final=True)) is not None:
            masks[path][start:] = b"\1" * (len(s) - start)
    mark(leaves)  # content keys and values together: a credential split between a key and its value is found
    mark(values_only(leaves))
    # Transport metadata must not break credentials split across event payloads.
    if isinstance(value, list) and value and isinstance(value[0], dict) and "seq" in value[0]:
        roots = [(i, ("value", list(event).index("data"))) for i, event in enumerate(value)]
        payloads = [(p, s) for p, s in leaves if any(p[:2] == root for root in roots)]
        mark(payloads)
        mark(values_only(payloads))
    # Locate text/content fields without flattening away the original offset map.
    text_paths = set()
    def texts(v, path=()):
        if isinstance(v, dict):
            for i, (key, child) in enumerate(v.items()):
                p = path + (("value", i),)
                if key in ("text", "content") and isinstance(child, str):
                    text_paths.add(p)
                texts(child, p)
        elif isinstance(v, (list, tuple)):
            for i, child in enumerate(v):
                texts(child, path + (i,))
    texts(value)
    mark([(p, s) for p, s in leaves if p in text_paths])

    def rebuild(v, path=()):
        if isinstance(v, str):
            mask = masks.get(path)
            if mask is None:  # a field name of the wire format
                return v
            parts, start = [], 0
            while start < len(v):
                end = start + 1
                while end < len(v) and mask[end] == mask[start]:
                    end += 1
                parts.append("[redacted]" if mask[start] else v[start:end])
                start = end
            return "".join(parts)
        if isinstance(v, dict):
            out = {}
            for i, (k, child) in enumerate(v.items()):
                key = rebuild(k, path + (("key", i),))
                while key in out:  # redacted keys never collapse into one entry
                    key += "*"
                out[key] = rebuild(child, path + (("value", i),))
            return out
        if isinstance(v, (list, tuple)):
            return [rebuild(child, path + (i,)) for i, child in enumerate(v)]
        return v
    return rebuild(value)


def scrub_events(events, secrets=()):
    """SSE envelopes are built by this module; only their payloads carry content. All payloads are scrubbed as
    one value, so a credential split across events is still found, and every envelope keeps its shape."""
    payloads = scrub([event["data"] for event in events], secrets)
    return [{**event, "data": data} for event, data in zip(events, payloads)]


_PEM_START = re.compile(r"-----BEGIN (?:[A-Z0-9]+ )*PRIVATE KEY-----")
_PEM_END = re.compile(r"-----END (?:[A-Z0-9]+ )*PRIVATE KEY-----")


_UNFINISHED_URL_TOKEN = re.compile(r"x-access-token:[^@\s]*$")
_TRAILING_RUN = re.compile(r"\S+$")
_BEARER_PREFIX = re.compile(r"(?i)authorization:\s*(?:bearer\s*)?\S*$")


def open_credential(text: str, *, final: bool = False) -> int | None:
    """Where a credential that may still be arriving (or that ended unfinished) starts in text, or None.

    Every credential pattern except a private key block and the bearer header is a single run of non-space
    characters, and some only match once a later delimiter arrives (x-access-token:…@). So while a stream is
    open, the last run of non-space characters is held until a space ends it, and so are an unclosed key block,
    a bearer header and any match that reaches the end. When text is final, an unclosed key block or an
    x-access-token without its @ is still open: from its start to the end is a credential fragment."""
    starts = [m.start() for m in _PEM_START.finditer(text) if not _PEM_END.search(text, m.end())]
    if unfinished := _UNFINISHED_URL_TOKEN.search(text):
        starts.append(unfinished.start())
    if not final:
        for pattern in (_TRAILING_RUN, _BEARER_PREFIX):
            if held := pattern.search(text):
                starts.append(held.start())
        starts += [m.start() for pattern in _SECRET_PATTERNS for m in pattern.finditer(text) if m.end() == len(text)]
    return min(starts, default=None)


def cut_open_credential(events: list[dict]) -> list[dict]:
    """Redact reply text from the start of a credential that never finished (a key block without END, an
    x-access-token without @) to the end of the reply. Runs on the original text, before scrub(): scrubbing first
    would remove the start marker this needs to find."""
    text = "".join(e["data"].get("text", "") for e in events if e["type"] == "token")
    start = open_credential(text, final=True)
    if start is None:
        return events
    out, offset = [], 0
    for event in events:
        if event["type"] == "token":
            piece = event["data"].get("text", "")
            begin, offset = offset, offset + len(piece)  # offsets in the original reply text
            if offset > start:
                piece = piece[:max(0, start - begin)] + ("[redacted]" if begin <= start else "")
                event = {**event, "data": {**event["data"], "text": piece}}
        out.append(event)
    return out


class UnavailableRuntime:
    async def readiness(self, workspace_id, *, ledger):
        return {"status": "unavailable", "model_id": None, "reason": "Mind runtime is not configured."}

    async def run_turn(self, **kwargs):
        raise RuntimeError("Unavailable")


def get_runtime(request: Request):
    return getattr(request.app.state, "mind_runtime", None) or UnavailableRuntime()


async def readiness(runtime, workspace_id, ledger):
    # Scripted runtimes may expose readiness(workspace_id) when it is free.
    params = inspect.signature(runtime.readiness).parameters
    async with asyncio.timeout(30):
        result = await runtime.readiness(workspace_id, **({"ledger": ledger} if "ledger" in params else {}))
    if isinstance(result, dict):
        ready = result.get("status") == "ready" or result.get("available") is True
        model = result.get("model_id", result.get("model"))
    else:
        ready, model = result.available, result.model
    return {"status": "ready" if ready else "unavailable", "model_id": model if ready else None,
            "reason": None if ready else "Mind is unavailable. Check workspace inference settings."}


def thread_view(db, thread):
    return {key: getattr(thread, key) for key in ("id", "title", "billing_workspace_id", "workspace_ids",
            "model_id", "status", "created_at", "updated_at")} | {
                "cost": store.cost(db.query(MindCharge).filter_by(thread_id=thread.id))}


def message_view(row, history=False):
    keys = ("id", "thread_id", "role", "text", "context", "citations", "action_cards", "status", "created_at")
    return {k: getattr(row, k) for k in keys} | ({"history": row.history} if history else {})


def transcript(db, thread, history=False):
    from .actions import card  # actions imports this module

    rows = db.query(MindMessage).filter_by(thread_id=thread.id).order_by(MindMessage.position).all()
    messages = [message_view(r, history) for r in rows]
    # Cards are saved as streamed; edits, dismissals and confirmations change only the action row. Show each
    # card as it is now, so a reload never offers a stale revision.
    actions = {a.id: a for a in db.query(MindAction).filter_by(thread_id=thread.id, owner_id=thread.owner_id)}
    context = Context(db, thread.owner_id, thread.id)
    for message in messages:
        message["action_cards"] = [card(actions[c["id"]], context) if isinstance(c, dict) and c.get("id") in actions
                                   else c for c in message["action_cards"] or []]
    original = {"thread": thread_view(db, thread), "messages": messages}
    return scrub(jsonable_encoder(original), secrets_for(db, thread.owner_id, thread.workspace_ids))


@router.post("/threads")
def create_thread(body: CreateThread, user: User = Depends(current_user), db: Session = Depends(get_db)):
    require_member(db, user, body.billing_workspace_id)
    row = MindThread(owner_id=user.id, billing_workspace_id=body.billing_workspace_id,
                     workspace_ids=store.memberships(db, user.id))
    db.add(row)
    db.commit()
    return scrub(jsonable_encoder(thread_view(db, row)), secrets_for(db, user.id, row.workspace_ids))


@router.get("/threads")
def list_threads(user: User = Depends(current_user), db: Session = Depends(get_db)):
    scope = set(store.memberships(db, user.id))
    rows = db.query(MindThread).filter_by(owner_id=user.id).order_by(MindThread.updated_at.desc()).all()
    result = []
    for row in rows:
        if set(row.workspace_ids).issubset(scope):
            store.recover(db, row)
            result.append(thread_view(db, row))
    return scrub(jsonable_encoder(result), secrets_for(db, user.id))


@router.get("/threads/{thread_id}")
def get_thread(thread_id: str, user: User = Depends(current_user), db: Session = Depends(get_db)):
    row = store.owned(db, user.id, thread_id)
    store.recover(db, row)
    return transcript(db, row)


@router.get("/status")
async def status(workspace_id: str, user: User = Depends(current_user), db: Session = Depends(get_db),
                 runtime=Depends(get_runtime)):
    require_member(db, user, workspace_id)
    ledger = store.TurnLedger(user.id, workspace_id, runtime=runtime)
    try:
        result = await readiness(runtime, workspace_id, ledger)
        await ledger.reconcile()
    except store.AccountingError as exc:
        result = {"status": "unavailable", "model_id": None, "reason": str(exc)}
    except Exception:
        result = {"status": "unavailable", "model_id": None, "reason": "Mind is unavailable."}
    finally:
        ledger.close()
    return scrub(result, secrets_for(db, user.id))


class PrivateTools:
    def __init__(self, context, emit):
        self.context, self.emit, self.calls = context, emit, 0

    def definitions(self):
        return Tools(self.context).definitions()

    async def call(self, name, arguments):
        self.calls += 1
        if self.calls > 12:
            raise store.AccountingError("turn_limit")
        try:
            return await self._call(name, arguments)
        except Exception:
            # The dock shows the step as finished; the engine gives the model only a fixed error string.
            await self.emit({"type": "tool_finished", "data": {"call_id": str(self.calls), "name": name,
                             "summary": "Factory tool unavailable", "citations": []}})
            raise

    async def _call(self, name, arguments):
        with SessionLocal() as db:
            thread = store.owned(db, self.context.user_id, self.context.thread_id)
            result = await Tools(Context(db, self.context.user_id, thread.id)).call(name, arguments)
            # A read or proposal in a workspace joined after the thread began widens the thread's scope before
            # anything derived from it is kept, so revoking that membership later hides the thread (owned()).
            touched = {arguments.get("workspace_id") if isinstance(arguments, dict) else None}
            touched |= {c.get("workspace_id") for c in result.get("citations") or [] if isinstance(c, dict)}
            touched = {w for w in touched if isinstance(w, str) and w}
            if not touched <= set(thread.workspace_ids):
                thread.workspace_ids = sorted(set(thread.workspace_ids) | touched)
            result = scrub(result, secrets_for(db, self.context.user_id, thread.workspace_ids))
            # Proposal rows are also private persistence: scrub the complete record.
            from ..models import MindAction
            for action in db.query(MindAction).filter_by(thread_id=thread.id, owner_id=self.context.user_id):
                clean = scrub({"fields": action.fields, "snapshot": action.snapshot, "result": action.result},
                              secrets_for(db, self.context.user_id, thread.workspace_ids))
                action.fields, action.snapshot, action.result = clean["fields"], clean["snapshot"], clean["result"]
            db.commit()
        await self.emit({"type": "tool_finished", "data": {"call_id": str(self.calls), "name": name,
                         "summary": "Factory tool completed", "citations": result.get("citations", [])}})
        if name.startswith("propose_"):
            await self.emit({"type": "action_card", "data": {"card": result}})
        return result


@router.post("/threads/{thread_id}/messages")
def submit(thread_id: str, body: SubmitMessage, user: User = Depends(current_user), db: Session = Depends(get_db),
           runtime=Depends(get_runtime)):
    thread = store.owned(db, user.id, thread_id)
    store.recover(db, thread)
    if body.context:
        require_member(db, user, body.context.workspace_id)
        # Context from a workspace joined after the thread began widens its scope in the same transaction that
        # keeps the message, so revoking that membership later hides the thread (owned()).
        if body.context.workspace_id not in thread.workspace_ids:
            thread.workspace_ids = sorted(set(thread.workspace_ids) | {body.context.workspace_id})
    existing = db.query(MindMessage).filter_by(thread_id=thread_id, client_message_id=body.client_message_id).first()
    if existing:
        raise HTTPException(409, "Message already submitted; reload the thread")
    turn_id = uuid4().hex
    if db.execute(update(MindThread).where(MindThread.id == thread_id, MindThread.active_turn.is_(None)).values(
            active_turn=turn_id, lease_until=utcnow() + timedelta(seconds=store.LEASE_SECONDS),
            status="streaming", updated_at=utcnow())).rowcount != 1:
        db.rollback()
        raise HTTPException(409, "A turn is already active")
    # Recheck idempotency after acquiring the database write claim.
    if db.query(MindMessage).filter_by(thread_id=thread_id, client_message_id=body.client_message_id).first():
        db.rollback()
        raise HTTPException(409, "Message already submitted; reload the thread")
    position = db.query(func.max(MindMessage.position)).filter_by(thread_id=thread_id).scalar() or 0
    if position >= 200:
        db.rollback()
        raise HTTPException(409, "Thread turn limit reached; create a new conversation")
    original = {"text": body.text, "context": body.context.model_dump() if body.context else None}
    clean = scrub(original, secrets_for(db, user.id, thread.workspace_ids))
    prompt = MindMessage(thread_id=thread_id, role="user", **clean, turn_id=turn_id,
                         position=position + 1, client_message_id=body.client_message_id)
    assistant = MindMessage(thread_id=thread_id, role="assistant", turn_id=turn_id,
                            position=position + 2, status="streaming")
    db.add_all([prompt, assistant])
    if position == 0:
        thread.title = " ".join(clean["text"].split())[:100]
    db.commit()
    assistant_id, user_id = assistant.id, user.id
    billing, scope = thread.billing_workspace_id, tuple(thread.workspace_ids)

    async def stream():
        queue = asyncio.Queue(maxsize=32)
        events, released, output_size = [], 0, 0
        ledger = store.TurnLedger(user_id, billing, thread_id, runtime)
        terminal = "interrupted"
        flushed = False

        async def emit(event):
            nonlocal output_size
            if len(events) >= MAX_EVENTS:
                raise store.AccountingError("turn_limit")
            typ = event.get("type")
            typ = {"text_delta": "token", "tool_start": "tool_started", "tool_result": "tool_finished"}.get(typ, typ)
            if typ not in {"token", "tool_started", "tool_finished", "action_card"}:
                return  # No reasoning, traces, provider errors or arbitrary event kinds.
            data = event.get("data", {k: v for k, v in event.items() if k != "type"})
            if typ == "token":
                data = {"text": str(data.get("text", ""))}
            elif typ.startswith("tool_"):
                data = {k: data.get(k, "" if k != "citations" else []) for k in ("call_id", "name", "summary", "citations")}
            else:
                data = {"card": data.get("card", {})}
            output_size += len(json.dumps(data))
            if output_size > MAX_OUTPUT:
                raise store.AccountingError("turn_limit")
            await queue.put((typ, data))

        async def run():
            nonlocal terminal
            try:
                with SessionLocal() as session:
                    row = store.owned(session, user_id, thread_id)
                    history = transcript(session, row, history=True)["messages"]
                ready = await readiness(runtime, billing, ledger)
                if ready["status"] != "ready":
                    raise RuntimeError("Unavailable")
                with SessionLocal() as session:
                    row = store.owned(session, user_id, thread_id)
                    row.model_id = ready["model_id"]
                    session.commit()
                context = TurnContext(user_id, thread_id, billing, scope)
                async with asyncio.timeout(120):
                    await runtime.run_turn(context=context, history=history[:-1], emit=emit,
                        tools=PrivateTools(Context(None, user_id, thread_id), emit), ledger=ledger)
                await ledger.reconcile()
                with SessionLocal() as session:
                    if session.query(MindCharge).filter_by(
                            user_id=user_id, settled=False).first():
                        raise store.AccountingError("accounting_pending")
                terminal = "complete"  # the dock's wire value (web/src/mindHttp.ts)
            except store.AccountingError as exc:
                terminal = "failed"
                await queue.put(("error", {"code": exc.code, "message": str(exc)}))
            except MindUnavailable as exc:  # fixed public reasons only, never provider text
                terminal = "failed"
                await queue.put(("error", {"code": exc.reason, "message": str(exc)}))
            except asyncio.CancelledError:
                raise
            except Exception:
                terminal = "failed"
                await queue.put(("error", {"code": "unavailable", "message": "Mind could not complete this turn. Please try again."}))
            finally:
                ledger.close()
                # No blocking queue write on cancellation/disconnect.
                if not asyncio.current_task().cancelling():
                    await queue.put(None)

        def envelope(typ, data):
            return {"seq": len(events) + 1, "thread_id": thread_id, "message_id": assistant_id,
                    "type": typ, "data": data}

        def encoded(event):
            return "data: " + json.dumps(event, ensure_ascii=False) + "\n\n"

        task = None
        try:
            events.append(envelope("message_started", {}))
            task = asyncio.create_task(run())
            while True:
                item = await queue.get()
                if item is None:
                    break
                with SessionLocal() as session:
                    store.owned(session, user_id, thread_id)
                    secrets = secrets_for(session, user_id, scope)
                events.append(envelope(*item))
                # One shared buffer for every event kind. Retain at least the
                # longest credential's worth of later token text AND structured
                # data. Scrub the original complete buffer, never substitutions.
                tail = max([len(s) for s in secrets] + [256])
                safe = scrub_events(events, secrets)
                tokens = [e["data"].get("text", "") if e["type"] == "token" else "" for e in events]
                text = "".join(tokens)
                ends = list(itertools.accumulate(len(t) for t in tokens))
                # Nothing at or after an unfinished credential leaves until it can be recognized whole.
                hold = open_credential(text)
                for i in range(released, len(events)):
                    if len(text) - ends[i] < tail or (hold is not None and ends[i] > hold):
                        break
                    yield encoded(safe[i])
                    released = i + 1
            with SessionLocal() as session:
                store.owned(session, user_id, thread_id)
                events.append(envelope("usage", store.wire_usage(session, user_id, thread_id)))
                events.append(envelope("done", {"status": terminal}))
                safe = scrub_events(cut_open_credential(events), secrets_for(session, user_id, scope))
            for event in safe[released:]:
                yield encoded(event)
            flushed = True
        finally:
            # Starlette cancels the response task in an AnyIO cancel scope on
            # http.disconnect. Shield the await AND persistence: otherwise the
            # gather below is cancelled before partial output reaches the DB.
            with anyio.CancelScope(shield=True):
                if not flushed:
                    terminal = "interrupted"
                if task is not None:
                    if not task.done():
                        task.cancel()
                    await asyncio.gather(task, return_exceptions=True)
                ledger.close()
                while not queue.empty():
                    item = queue.get_nowait()
                    if item is not None:
                        events.append(envelope(*item))
                # Persist even on disconnect. No durable worker or public event bus.
                with SessionLocal() as session:
                    secrets = secrets_for(session, user_id, scope)
                    safe = scrub_events(cut_open_credential(events), secrets)
                    row = session.get(MindMessage, assistant_id)
                    row.text = "".join(e["data"].get("text", "") for e in safe if e["type"] == "token")
                    row.citations = [c for e in safe if e["type"] == "tool_finished" for c in e["data"].get("citations", [])]
                    row.action_cards = [e["data"]["card"] for e in safe if e["type"] == "action_card"]
                    row.history = {"tools": [e["data"] for e in safe if e["type"] == "tool_finished"]}
                    row.status = terminal
                    session.execute(update(MindThread).where(MindThread.id == thread_id,
                        MindThread.active_turn == turn_id).values(active_turn=None, lease_until=None,
                        status=terminal, updated_at=utcnow()))
                    session.commit()

    return StreamingResponse(stream(), media_type="text/event-stream", headers={"Cache-Control": "no-store", "X-Accel-Buffering": "no"})
