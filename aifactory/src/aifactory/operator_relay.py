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


def item_error(kind: str, item: Any) -> str | None:
    """Why an item is malformed (None when well formed). Malformed items are answered, never retried; anything
    that fails later is an infrastructure problem, and the command is retried on the next poll."""
    if kind == "cancel":
        if isinstance(item, str):
            return None if item.strip() else "a cancel key must not be empty"
        if isinstance(item, dict) and _number(item.get("outcome")) is not None:
            return None
        return 'a cancel item is "#N", a declared key, or {"outcome": N}'
    if not isinstance(item, dict):
        return "an item must be an object"
    bad = next((f for f in STRING_FIELDS if f in item and not isinstance(item[f], str)), None)
    if bad:
        return f"{bad} must be a string"
    if ("outcome" in item or kind != "retry") and _number(item.get("outcome")) is None:
        return "outcome must be an outcome number"
    if kind == "resolve" and not (item.get("kind") and item.get("resolution")):
        return "a resolve item needs kind and resolution"
    if kind == "guidance" and not str(item.get("body") or "").strip():
        return "a guidance item needs a body"
    if kind == "retry" and "outcome" not in item and not str(item.get("key") or "").strip():
        return "a retry item needs an outcome or a declared key"
    return None


def _time(value: Any) -> datetime | None:
    try:
        parsed = datetime.fromisoformat(str(value or "").replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def _handled(workspace_id: str, comment_id: int, now: datetime) -> bool:
    """Whether an earlier poll (perhaps before a restart) already handled this command."""
    from .db import session_scope
    from .models import Event

    cutoff = (now - 2 * MAX_AGE).replace(tzinfo=None)  # older commands are refused by age anyway
    with session_scope() as s:
        return any((e.data or {}).get("comment_id") == comment_id for e in s.query(Event).filter(
            Event.workspace_id == workspace_id, Event.kind == EVENT, Event.created_at >= cutoff))


def _apply_items(cfg: dict[str, Any], items: dict[str, list], schedule: Callable,
                 stop_outcome: Callable[[str], None]) -> list[str]:
    """Apply a command's items with the bootstrap's own functions; one result line per item. Runs in a worker
    thread: ``schedule`` and ``stop_outcome`` hand work back to the event loop."""
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
        self.seen: set[int] = set()  # commands this process handled; the events table covers restarts

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
        handled = []
        for comment in sorted(comments, key=lambda c: (str(c.get("created_at") or ""), int(c.get("id") or 0))):
            comment_id, body = int(comment.get("id") or 0), str(comment.get("body") or "")
            if not comment_id or comment_id in self.seen or REPLY_MARKER in body:
                continue
            if not body.lstrip().startswith(COMMAND):
                continue
            if await asyncio.to_thread(_handled, cfg["workspace"], comment_id, now):
                self.seen.add(comment_id)
                continue
            await self._handle(gh, cfg, comment, now)
            self.seen.add(comment_id)
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
        # Recorded before the reply: a crash after it never applies a command twice (nonces also guard each item).
        await asyncio.to_thread(
            record, cfg["workspace"], EVENT, f"Operator relay from {login or 'unknown'}: " + "; ".join(lines)[:300],
            actor="operator", level="info" if applied else "warn",
            data={"comment_id": comment_id, "author": login, "results": lines,
                  "delegated_by": cfg["delegated_by"]})
        if applied:
            self.wake()
        reply = "\n".join([REPLY_MARKER, f"Factory relay for {comment.get('html_url') or comment_id} "
                                         f"(by {login or 'unknown'}):", *[f"- {line}" for line in lines]])
        try:
            await gh.comment(cfg["repository"], cfg["issue"], reply)
        except Exception as exc:
            log.warning("operator relay could not answer comment %s: %s", comment_id, exc)

    async def _apply(self, gh, cfg: dict[str, Any], comment: dict[str, Any], login: str,
                     now: datetime) -> list[str]:
        if login.casefold() not in cfg["operators"]:
            return [f"not applied: {login or 'unknown'} is not a listed operator"]
        created = _time(comment.get("created_at"))
        if created is None or now - created > MAX_AGE:
            return ["not applied: first seen more than 24 hours after it was written"]
        permission = await gh.repository_author_permission(cfg["repository"], login)
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
        loop = asyncio.get_running_loop()

        def stop_outcome(outcome_id: str) -> None:
            # As the app's own cancel: stop an active planner (it could otherwise overwrite the cancellation and
            # start tasks) and any task jobs, on the event loop that owns them.
            from .pipeline.orchestrator import orchestrator

            asyncio.run_coroutine_threadsafe(_stop(orchestrator, outcome_id), loop).result(timeout=30)

        lines = await asyncio.to_thread(_apply_items, cfg, items,
                                        lambda coro: asyncio.run_coroutine_threadsafe(coro, loop), stop_outcome)
        return lines or ["nothing to apply"]


async def _stop(orchestrator, outcome_id: str) -> None:
    orchestrator.cancel_outcome(outcome_id)
