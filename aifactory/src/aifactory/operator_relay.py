"""Live operator relays: the bootstrap's delegated actions within a minute, without a restart.

The bootstrap's ``resolve``, ``guidance``, ``retry`` and ``cancel`` items apply only when production starts
(``AIFACTORY_BOOTSTRAP`` is read at startup), and production restarts only on a release, at least two hours
apart, so a relayed review finding, retry or piece of guidance could wait hours. This channel takes the same
items from comments on one GitHub issue that the bootstrap names, and applies them on the next poll (every 30
seconds)::

    "control": {"repository": "owner/name", "issue": 123, "operators": ["github-login"]}

A command is a comment that starts with ``/factory`` and holds one fenced ``json`` block: an object with any of
``resolve``, ``guidance``, ``retry`` and ``cancel``, each a list in the bootstrap's own item format. Items
without a nonce get one from the comment id, so a command applies once. Only commands by a listed operator login
who has write access to that repository (asked of GitHub for each command) apply, and only while the command is
less than a day old when first seen. The Factory answers each command with what it applied or skipped.

Nothing new is trusted: the Factory reads with the GitHub access it already has and adds no credential, and the
items keep the bootstrap's delegation rules (actions are recorded as the operator under ``delegated_by``; only
relays outside the bootstrap workspace; ``head`` guards). A live cancel never discards started work.
"""

from __future__ import annotations

import asyncio
import json
import logging
import re
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Callable

log = logging.getLogger(__name__)

POLL_SECONDS = 30
MAX_AGE = timedelta(hours=24)
COMMAND = "/factory"
REPLY_MARKER = "<!-- ai-factory:operator-relay -->"
EVENT = "operator_relay"
KINDS = ("cancel", "retry", "resolve", "guidance")  # applied in the bootstrap's order
WRITE = {"admin", "maintain", "write", "push"}
_JSON_BLOCK = re.compile(r"```json[ \t]*\r?\n(.*?)\r?\n[ \t]*```", re.DOTALL)


def control() -> dict[str, Any] | None:
    """The configured control issue with the bootstrap workspace and delegator, or None when not configured."""
    from .bootstrap import bootstrap_config
    from .fallbacks import bootstrap_workspace

    cfg = bootstrap_config() or {}
    raw, workspace = cfg.get("control"), bootstrap_workspace()
    if not isinstance(raw, dict) or not workspace:
        return None
    repository, issue = str(raw.get("repository") or "").strip(), raw.get("issue")
    operators = {str(o).strip().casefold() for o in raw.get("operators") or [] if str(o).strip()}
    if repository.count("/") != 1 or type(issue) is not int or issue <= 0 or not operators:
        log.error("Bootstrap control needs a repository (owner/name), an issue number and operator logins")
        return None
    return {"repository": repository, "issue": issue, "operators": operators, "workspace": workspace,
            "delegated_by": str(cfg.get("delegated_by") or "")}


def parse(body: str) -> tuple[dict[str, list] | None, str]:
    """(items by kind, error) from a command's body."""
    blocks = _JSON_BLOCK.findall(body)
    if len(blocks) != 1:
        return None, "a command needs exactly one ```json block"
    try:
        data = json.loads(blocks[0])
    except json.JSONDecodeError as exc:
        return None, f"the json block is not valid JSON ({exc.msg}, line {exc.lineno})"
    if not isinstance(data, dict) or not data or set(data) - set(KINDS):
        return None, f"the json block must be an object with any of {', '.join(KINDS)}"
    for kind, items in data.items():
        if not isinstance(items, list) or not items:
            return None, f"{kind} must be a non-empty list"
        if kind != "cancel" and not all(isinstance(item, dict) for item in items):
            return None, f"every {kind} item must be an object"
    return data, ""


def _number(value: Any) -> int | None:
    if type(value) is int and value > 0:
        return value
    if isinstance(value, str) and value.strip().isdigit() and int(value) > 0:
        return int(value)
    return None


STRING_FIELDS = ("task", "kind", "resolution", "comment", "body", "nonce", "head", "workspace", "key", "scope")
BOOL_FIELDS = ("include_cancelled", "reusable", "discard_started")  # "false" as a string would read as true


def item_error(kind: str, item: Any) -> str | None:
    """Why an item is malformed (None when well formed). Malformed items are answered, never retried; anything
    that fails later is an infrastructure problem, and the command is retried on the next poll."""
    if kind == "cancel":
        if isinstance(item, str):
            return None if item.strip() else "a cancel key must not be empty"
        if isinstance(item, dict) and _number(item.get("outcome")) is not None:
            return _flag_error(item)
        return 'a cancel item is "#N", a declared key, or {"outcome": N}'
    if not isinstance(item, dict):
        return "an item must be an object"
    bad = next((f for f in STRING_FIELDS if f in item and not isinstance(item[f], str)), None)
    if bad:
        return f"{bad} must be a string"
    if _flag_error(item):
        return _flag_error(item)
    if ("outcome" in item or kind != "retry") and _number(item.get("outcome")) is None:
        return "outcome must be an outcome number"
    if kind == "resolve" and not (item.get("kind") and item.get("resolution")):
        return "a resolve item needs kind and resolution"
    if kind == "guidance" and not str(item.get("body") or "").strip():
        return "a guidance item needs a body"
    if kind == "retry" and "outcome" not in item and not str(item.get("key") or "").strip():
        return "a retry item needs an outcome or a declared key"
    return None


async def _permission(gh, repository: str, login: str) -> str | None:
    """The login's current permission on the repository. Only "not a collaborator" (404) is a definite no; a rate
    limit or an outage raises, so the command is retried on the next poll instead of being refused for good."""
    from urllib.parse import quote

    from .github import GitHubError

    try:
        data = await gh.request("GET", f"/repos/{repository}/collaborators/{quote(login, safe='')}/permission")
    except GitHubError as exc:
        if exc.status == 404:
            return None
        raise
    permission = data.get("permission") if isinstance(data, dict) else None
    return permission if isinstance(permission, str) else None


def _flag_error(item: dict) -> str | None:
    bad = next((f for f in BOOL_FIELDS if f in item and type(item[f]) is not bool), None)
    return f"{bad} must be true or false" if bad else None


def _time(value: Any) -> datetime | None:
    try:
        parsed = datetime.fromisoformat(str(value or "").replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def _handled(workspace_id: str, comment_id: int, now: datetime) -> tuple[int, dict] | None:
    """The record of an earlier poll (perhaps before a restart) that handled this command: (event id, data)."""
    from .db import session_scope
    from .models import Event

    cutoff = (now - 2 * MAX_AGE).replace(tzinfo=None)  # older commands are refused by age anyway
    with session_scope() as s:
        for e in s.query(Event).filter(Event.workspace_id == workspace_id, Event.kind == EVENT,
                                       Event.created_at >= cutoff).order_by(Event.id):
            if (e.data or {}).get("comment_id") == comment_id:
                return e.id, dict(e.data or {})
    return None


def _unanswered(workspace_id: str) -> list[tuple[int, dict]]:
    """Relay records whose answer never posted, however old: a restarted process answers them."""
    from .db import session_scope
    from .models import Event

    with session_scope() as s:
        return [(e.id, dict(e.data or {})) for e in s.query(Event).filter(
            Event.workspace_id == workspace_id, Event.kind == EVENT).order_by(Event.id)
            if (e.data or {}).get("acknowledged") is False]


def _mark_acknowledged(event_id: int) -> None:
    from .db import session_scope
    from .models import Event

    with session_scope() as s:
        event = s.get(Event, event_id)
        if event is not None:
            event.data = {**(event.data or {}), "acknowledged": True}


def _apply_items(cfg: dict[str, Any], items: dict[str, list], schedule: Callable,
                 stop_outcome: Callable[[str], None]) -> list[str]:
    """Apply a command's items with the bootstrap's own functions; one result line per item. Runs on the event
    loop, as the app's own decision endpoint does: resolving a decision can stop the orchestrator's planners and
    jobs, which are asyncio tasks of this loop."""
    from . import bootstrap

    workspace, delegated_by = cfg["workspace"], cfg["delegated_by"]
    steps = {
        "cancel": lambda entries: bootstrap._cancel_declared(workspace, entries, live=True,
                                                             on_cancelled=stop_outcome),
        "retry": lambda entries: bootstrap._retry_declared(workspace, entries),
        "resolve": lambda entries: bootstrap._resolve_delegated(workspace, entries, delegated_by, schedule=schedule),
        "guidance": lambda entries: bootstrap._guidance_delegated(workspace, entries, delegated_by),
    }
    lines: list[str] = []
    for kind in KINDS:
        for n, item in enumerate(items.get(kind) or []):
            # One item at a time, so each gets its own line. A malformed item is answered and skipped; any other
            # failure (a locked database, a lost connection) propagates and the whole command is retried on the
            # next poll, where items already applied are skipped by their nonces or state.
            error = item_error(kind, item)
            if error:
                lines.append(f"{kind} item {n + 1}: not applied, {error}")
                continue
            lines += steps[kind]([item])
    return lines


class Relay:
    """Polls the control issue from the orchestrator's loop."""

    def __init__(self, wake: Callable[[], None] = lambda: None) -> None:
        self.wake = wake
        self.last_poll = float("-inf")
        self.since: datetime | None = None  # GitHub's `since` (updated at or after) for the next poll
        self.seen: set[int] = set()  # commands this process handled and answered; the events table covers restarts
        self.unanswered: dict[int, tuple[dict[str, Any], int, list[str]]] = {}  # comment id -> (comment, event, lines)
        self.loaded = False  # whether this process has picked up answers an earlier one never posted

    async def run(self) -> None:
        """Poll on its own schedule: slow maintenance in the scheduler loop must never delay an operator's cancel
        or change request."""
        while True:
            await self.maybe_poll()
            await asyncio.sleep(POLL_SECONDS)

    async def maybe_poll(self) -> None:
        if time.monotonic() - self.last_poll < POLL_SECONDS:
            return
        self.last_poll = time.monotonic()
        cfg = control()
        if cfg is None:
            return
        try:
            await self.poll(cfg)
        except Exception:  # never let a GitHub or database hiccup reach the scheduler
            log.exception("operator relay poll failed")

    async def poll(self, cfg: dict[str, Any], now: datetime | None = None) -> list[int]:
        """Handle new commands; returns the comment ids handled."""
        from .github import GitHub, GitHubError, resolve_credential

        now = now or datetime.now(timezone.utc)
        gh = GitHub(resolve_credential(cfg["workspace"]))
        since = self.since or now - MAX_AGE
        comments: list[dict[str, Any]] = []
        page = 1
        while True:
            batch = await gh.request("GET", f"/repos/{cfg['repository']}/issues/{cfg['issue']}/comments",
                                     params={"since": since.strftime("%Y-%m-%dT%H:%M:%SZ"), "per_page": 100,
                                             "page": page})
            if not isinstance(batch, list):
                raise GitHubError("Malformed GitHub comments response")
            comments += batch
            if len(batch) < 100:
                break
            page += 1
        if not self.loaded:
            for event_id, data in await asyncio.to_thread(_unanswered, cfg["workspace"]):
                comment = {"id": data.get("comment_id"), "html_url": data.get("html_url") or "",
                           "user": {"login": data.get("author") or ""}}
                if comment["id"]:
                    self.unanswered.setdefault(int(comment["id"]), (comment, event_id, list(data.get("results") or [])))
            self.loaded = True
        for comment_id, (comment, event_id, lines) in list(self.unanswered.items()):
            await self._answer(gh, cfg, comment, event_id, lines)  # applied earlier; only the answer failed
        handled = []
        for comment in sorted(comments, key=lambda c: (str(c.get("created_at") or ""), int(c.get("id") or 0))):
            comment_id, body = int(comment.get("id") or 0), str(comment.get("body") or "")
            if not comment_id or comment_id in self.seen or comment_id in self.unanswered or REPLY_MARKER in body:
                continue
            if not body.lstrip().startswith(COMMAND):
                continue
            earlier = await asyncio.to_thread(_handled, cfg["workspace"], comment_id, now)
            if earlier is not None:
                event_id, data = earlier
                if data.get("acknowledged"):
                    self.seen.add(comment_id)
                else:  # handled before a restart, but its answer never posted
                    await self._answer(gh, cfg, comment, event_id, list(data.get("results") or []))
                continue
            await self._handle(gh, cfg, comment, now)
            handled.append(comment_id)
        # Overlap the next window by a minute (clock skew between GitHub and this host); handled ids are skipped.
        self.since = now - timedelta(minutes=1)
        return handled

    async def _handle(self, gh, cfg: dict[str, Any], comment: dict[str, Any], now: datetime) -> None:
        from .events import record

        comment_id = int(comment["id"])
        login = str((comment.get("user") or {}).get("login") or "")
        lines = await self._apply(gh, cfg, comment, login, now)
        applied = any(line.endswith((": applied", ": posted", ": cancelled")) or ": retrying " in line
                      for line in lines)
        # Recorded before the answer: a crash after it never applies a command twice (nonces also guard each item).
        # The record says whether the answer posted, so a failed answer is retried without applying again.
        payload = await asyncio.to_thread(
            record, cfg["workspace"], EVENT, f"Operator relay from {login or 'unknown'}: " + "; ".join(lines)[:300],
            actor="operator", level="info" if applied else "warn",
            data={"comment_id": comment_id, "author": login, "results": lines, "html_url": comment.get("html_url"),
                  "delegated_by": cfg["delegated_by"], "acknowledged": False})
        if applied:
            self.wake()
        await self._answer(gh, cfg, comment, int(payload["id"]), lines)

    async def _answer(self, gh, cfg: dict[str, Any], comment: dict[str, Any], event_id: int,
                      lines: list[str]) -> None:
        """Post the result lines on the control issue; until that succeeds every poll tries again."""
        comment_id = int(comment["id"])
        login = str((comment.get("user") or {}).get("login") or "")
        reply = "\n".join([REPLY_MARKER, f"Factory relay for {comment.get('html_url') or comment_id} "
                                         f"(by {login or 'unknown'}):", *[f"- {line}" for line in lines]])
        try:
            await gh.comment(cfg["repository"], cfg["issue"], reply)
        except Exception as exc:
            log.warning("operator relay could not answer comment %s yet: %s", comment_id, exc)
            self.unanswered[comment_id] = (comment, event_id, lines)
            return
        self.unanswered.pop(comment_id, None)
        self.seen.add(comment_id)
        await asyncio.to_thread(_mark_acknowledged, event_id)

    async def _apply(self, gh, cfg: dict[str, Any], comment: dict[str, Any], login: str,
                     now: datetime) -> list[str]:
        if login.casefold() not in cfg["operators"]:
            return [f"not applied: {login or 'unknown'} is not a listed operator"]
        created = _time(comment.get("created_at"))
        if created is None or now - created > MAX_AGE:
            return ["not applied: first seen more than 24 hours after it was written"]
        permission = await _permission(gh, cfg["repository"], login)
        if permission not in WRITE:
            return [f"not applied: {login} has no write access to {cfg['repository']}"]
        items, error = parse(str(comment.get("body") or ""))
        if items is None:
            return [f"not applied: {error}"]
        comment_id = int(comment["id"])
        for kind in ("retry", "resolve", "guidance"):
            if kind in items:
                items[kind] = [{**item, "nonce": str(item.get("nonce") or f"relay-{comment_id}-{kind}-{n}")}
                               for n, item in enumerate(items[kind])]
        from .pipeline.orchestrator import orchestrator

        # On this loop (no worker thread): resolve() and a live cancel stop planners and jobs, which are this
        # loop's tasks; follow-ups such as publishing a handover are scheduled on it too. As the app's cancel, a
        # live cancel then stops the outcome's active planner, which could otherwise overwrite it and start tasks.
        lines = _apply_items(cfg, items, asyncio.get_running_loop().create_task,
                             lambda outcome_id: orchestrator.cancel_outcome(outcome_id))
        return lines or ["nothing to apply"]
