"""Live operator relays: the bootstrap's delegated actions within a minute, without a restart.

The bootstrap's ``resolve``, ``guidance``, ``retry`` and ``cancel`` items apply only when production starts
(``AIFACTORY_BOOTSTRAP`` is read at startup), and production restarts only on a release, at least two hours
apart, so a relayed review finding, retry or piece of guidance could wait hours. This channel takes the same
items from comments on one control issue that the bootstrap names, a GitHub issue or a Linear issue, and applies
them on the next poll (every 30 seconds)::

    "control": {"repository": "owner/name", "issue": 123, "operators": ["github-login"]}
    "control": {"linear": {"issue": "ENG-123"}, "operators": ["linear-user-email-or-id"]}

A command is a comment that starts with ``/factory`` and holds one fenced ``json`` block: an object with any of
``resolve``, ``guidance``, ``retry`` and ``cancel``, each a list in the bootstrap's own item format. Items
without a nonce get one from the comment id, so a command applies once. Only commands by a listed operator apply,
and only while the command is less than a day old when first seen. On GitHub the operator is a login with write
access to that repository (asked of GitHub for each command); on Linear, an active member (not a guest or an app)
of the workspace the Factory's Linear key belongs to, named by email or user id. The Factory answers each command
with what it applied or skipped, on the same issue; an answer that fails is retried with a growing delay (at most
one try per comment every ten minutes), so a missing permission cannot spend the API quota.

Nothing new is trusted: the Factory reads with the access it already has (the workspace's GitHub access, or its
Linear API key) and adds no credential, and the items keep the bootstrap's delegation rules (actions are recorded
as the operator under ``delegated_by``; only relays outside the bootstrap workspace; ``head`` guards). A live
cancel never discards started work.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import re
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Callable

log = logging.getLogger(__name__)

POLL_SECONDS = 30
MAX_AGE = timedelta(hours=24)
MAX_BACKOFF = timedelta(minutes=10)  # a failing answer or poll is tried at most this rarely
COMMAND = "/factory"
REPLY_MARKER = "<!-- ai-factory:operator-relay -->"  # GitHub replies; Linear's markdown may drop HTML comments
REPLY_HEADER = "Factory relay for "  # every reply's first line, on both channels
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
    operators = {str(o).strip().casefold() for o in raw.get("operators") or [] if str(o).strip()}
    common = {"operators": operators, "workspace": workspace, "delegated_by": str(cfg.get("delegated_by") or "")}
    if "linear" in raw:
        linear = raw["linear"] if isinstance(raw["linear"], dict) else {}
        issue = linear.get("issue").strip() if isinstance(linear.get("issue"), str) else ""
        if not issue or "repository" in raw or "issue" in raw or not operators:
            log.error("Bootstrap control on Linear needs only linear.issue (such as ENG-123) and operator emails or "
                      "user ids")
            return None
        return {"channel": "linear", "issue": issue, **common}
    repository, issue = str(raw.get("repository") or "").strip(), raw.get("issue")
    if repository.count("/") != 1 or type(issue) is not int or issue <= 0 or not operators:
        log.error("Bootstrap control needs a repository (owner/name), an issue number and operator logins")
        return None
    return {"channel": "github", "repository": repository, "issue": issue, **common}


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


def _handled(workspace_id: str, comment_id: int | str, now: datetime) -> tuple[int, dict] | None:
    """The record of an earlier poll (perhaps before a restart) that handled this command: (event id, data)."""
    from .db import session_scope
    from .models import Event

    cutoff = (now - 2 * MAX_AGE).replace(tzinfo=None)  # older commands are refused by age anyway
    with session_scope() as s:
        for e in s.query(Event).filter(Event.workspace_id == workspace_id, Event.kind == EVENT,
                                       Event.created_at >= cutoff).order_by(Event.id):
            if str((e.data or {}).get("comment_id")) == str(comment_id):  # GitHub ids are numbers, Linear's UUIDs
                return e.id, dict(e.data or {})
    return None


def _unanswered(workspace_id: str, channel: str) -> list[tuple[int, dict]]:
    """This channel's relay records whose answer never posted, however old: a restarted process answers them."""
    from .db import session_scope
    from .models import Event

    def same_channel(data: dict) -> bool:
        # Records from before channels were named came from the GitHub control issue.
        return data.get("channel") == channel if data.get("channel") else channel.startswith("github:")

    with session_scope() as s:
        return [(e.id, dict(e.data or {})) for e in s.query(Event).filter(
            Event.workspace_id == workspace_id, Event.kind == EVENT).order_by(Event.id)
            if (e.data or {}).get("acknowledged") is False and same_channel(e.data or {})]


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


def _is_reply(body: str) -> bool:
    """Whether a comment is one of the Factory's own replies. The Factory writes as an operator (on GitHub as the
    workspace's account, on Linear as the owner of its API key), so its replies are told apart by their marker, never
    by their author; and they never start with the command word, so they are never commands either."""
    return REPLY_MARKER in body or body.lstrip().startswith(REPLY_HEADER)


def _backoff(failures: int) -> timedelta:
    """The wait after the given number of failures in a row: one poll, then doubling, at most ten minutes."""
    return min(MAX_BACKOFF, timedelta(seconds=POLL_SECONDS * 2 ** min(failures - 1, 10)))


class GitHubChannel:
    """Commands on a GitHub issue, read and answered with the workspace's GitHub access."""

    def __init__(self, cfg: dict[str, Any]) -> None:
        from .github import GitHub, resolve_credential

        self.cfg = cfg
        self.gh = GitHub(resolve_credential(cfg["workspace"]))
        self.key = f"github:{cfg['repository']}#{cfg['issue']}"

    async def comments(self, since: datetime) -> list[dict[str, Any]]:
        from .github import GitHubError

        rows: list[dict[str, Any]] = []
        page = 1
        while True:
            batch = await self.gh.request("GET", f"/repos/{self.cfg['repository']}/issues/{self.cfg['issue']}/comments",
                                          params={"since": since.strftime("%Y-%m-%dT%H:%M:%SZ"), "per_page": 100,
                                                  "page": page})
            if not isinstance(batch, list):
                raise GitHubError("Malformed GitHub comments response")
            rows += batch
            if len(batch) < 100:
                break
            page += 1
        return [{"id": int(c.get("id") or 0), "body": str(c.get("body") or ""),
                 "author": str((c.get("user") or {}).get("login") or ""), "created_at": _time(c.get("created_at")),
                 "url": str(c.get("html_url") or "")} for c in rows if isinstance(c, dict)]

    def listed(self, comment: dict[str, Any]) -> bool:
        return comment["author"].casefold() in self.cfg["operators"]

    async def refusal(self, comment: dict[str, Any]) -> str | None:
        login = comment["author"]
        if await _permission(self.gh, self.cfg["repository"], login) not in WRITE:
            return f"{login} has no write access to {self.cfg['repository']}"
        return None

    def reply_id(self, comment_id: int | str) -> str | None:
        return None

    async def posted(self, comment_ids: list[int | str]) -> set[int | str]:
        return set()  # GitHub replies carry no fixed id; a lost response can post a second reply

    async def reply(self, comment_id: int | str, text: str) -> None:
        await self.gh.comment(self.cfg["repository"], self.cfg["issue"], f"{REPLY_MARKER}\n{text}")


class LinearChannel:
    """Commands on a Linear issue, read and answered with the workspace's Linear API key. A comment's author is a
    member of the workspace the key belongs to (integrations and synced external users have none)."""

    def __init__(self, cfg: dict[str, Any], issues: dict[str, str]) -> None:
        from .linear import Linear, resolve_token

        self.cfg = cfg
        self.linear = Linear(resolve_token(cfg["workspace"]))
        self.issues = issues  # configured issue (identifier or UUID) -> UUID, kept by the relay across polls
        self.key = f"linear:{cfg['issue']}"

    async def _issue_id(self) -> str:
        if self.cfg["issue"] not in self.issues:
            self.issues[self.cfg["issue"]] = (await self.linear.issue(self.cfg["issue"]))["id"]
        return self.issues[self.cfg["issue"]]

    async def comments(self, since: datetime) -> list[dict[str, Any]]:
        nodes = await self.linear.comments_since(await self._issue_id(), since.strftime("%Y-%m-%dT%H:%M:%S.000Z"))
        rows = []
        for node in nodes:
            user = node.get("user") if isinstance(node.get("user"), dict) else {}
            rows.append({"id": str(node["id"]), "body": str(node.get("body") or ""), "user": user,
                         "author": str(user.get("name") or user.get("email") or ""),
                         "created_at": _time(node.get("createdAt")), "url": str(node.get("url") or "")})
        return rows

    def listed(self, comment: dict[str, Any]) -> bool:
        user = comment.get("user") or {}
        names = {str(user.get(field) or "").strip().casefold() for field in ("id", "email")} - {""}
        return bool(names & self.cfg["operators"])

    async def refusal(self, comment: dict[str, Any]) -> str | None:
        user = comment.get("user") or {}
        # Read with the comment in this poll, so a deactivated or downgraded operator stops counting at once.
        if user.get("active") is not True or user.get("guest") is not False or user.get("app") is not False:
            return f"{comment['author'] or 'unknown'} is not an active member of the Linear workspace"
        return None

    def reply_id(self, comment_id: int | str) -> str | None:
        """The answer's own comment id, fixed per command: a retried answer whose first try did post (but whose
        response was lost) is recognised in the next listing instead of posting twice."""
        digest = hashlib.sha256(f"ai-factory:operator-relay:{self.key}:{comment_id}".encode()).digest()
        return str(uuid.UUID(bytes=digest[:16], version=4))

    async def posted(self, comment_ids: list[int | str]) -> set[int | str]:
        """The commands among these whose answer an earlier try did post, looked up by the answer's fixed id: the
        try's response may have been lost, and a restart a day later lists no comment that old."""
        if not comment_ids:
            return set()
        replies = {self.reply_id(comment_id): comment_id for comment_id in comment_ids}
        found = await self.linear.comments_by_id(await self._issue_id(), list(replies))
        return {replies[node["id"]] for node in found if node["id"] in replies}

    async def reply(self, comment_id: int | str, text: str) -> None:
        await self.linear.create_comment(await self._issue_id(), text, comment_id=self.reply_id(comment_id))


@dataclass
class Pending:
    """A handled command whose answer has not posted yet."""

    comment: dict[str, Any]
    event_id: int
    lines: list[str]
    failures: int = 0
    next_try: datetime | None = None


class Relay:
    """Polls the control issue from the orchestrator's loop."""

    def __init__(self, wake: Callable[[], None] = lambda: None) -> None:
        self.wake = wake
        self.last_poll = float("-inf")
        self.since: datetime | None = None  # comments created (Linear) or updated (GitHub) from here, next poll
        self.seen: set[int | str] = set()  # commands this process handled and answered; events cover restarts
        self.unanswered: dict[int | str, Pending] = {}  # by comment id
        self.loaded = False  # whether this process has picked up answers an earlier one never posted
        self.linear_issues: dict[str, str] = {}
        self.failures = 0  # polls failed in a row
        self.resume_at = float("-inf")  # monotonic time before which a failing channel is not polled again

    async def run(self) -> None:
        """Poll on its own schedule: slow maintenance in the scheduler loop must never delay an operator's cancel
        or change request."""
        while True:
            await self.maybe_poll()
            await asyncio.sleep(POLL_SECONDS)

    async def maybe_poll(self) -> None:
        clock = time.monotonic()
        if clock - self.last_poll < POLL_SECONDS or clock < self.resume_at:
            return
        self.last_poll = clock
        cfg = control()
        if cfg is None:
            return
        try:
            await self.poll(cfg)
        except Exception as exc:  # never let a GitHub, Linear or database hiccup reach the scheduler
            # Back off a channel that keeps failing (a missing key or permission, an exhausted rate limit), so it
            # costs a call every ten minutes at most; one log line per step, with the traceback on the first.
            self.failures += 1
            wait = _backoff(self.failures)
            self.resume_at = time.monotonic() + wait.total_seconds()
            log.warning("operator relay poll failed (%d in a row); next poll in %ds: %s", self.failures,
                        wait.total_seconds(), exc, exc_info=self.failures == 1)
        else:
            self.failures = 0

    def channel(self, cfg: dict[str, Any]) -> GitHubChannel | LinearChannel:
        return LinearChannel(cfg, self.linear_issues) if cfg.get("channel") == "linear" else GitHubChannel(cfg)

    async def poll(self, cfg: dict[str, Any], now: datetime | None = None) -> list[int | str]:
        """Handle new commands; returns the comment ids handled."""
        now = now or datetime.now(timezone.utc)
        channel = self.channel(cfg)
        comments = await channel.comments(self.since or now - MAX_AGE)
        if not self.loaded:
            for event_id, data in await asyncio.to_thread(_unanswered, cfg["workspace"], channel.key):
                comment = {"id": data.get("comment_id"), "url": data.get("url") or data.get("html_url") or "",
                           "author": data.get("author") or ""}
                if comment["id"]:
                    lines = list(data.get("results") or [])
                    self.unanswered.setdefault(comment["id"], Pending(comment, event_id, lines))
            self.loaded = True
        listed = {comment["id"] for comment in comments}
        # Answers that failed earlier (the commands were applied), due again; an earlier try may have posted one.
        due = {comment_id: pending for comment_id, pending in self.unanswered.items()
               if channel.reply_id(comment_id) in listed or pending.next_try is None or now >= pending.next_try}
        posted = await channel.posted([comment_id for comment_id in due if channel.reply_id(comment_id) not in listed])
        for comment_id, pending in due.items():
            if comment_id in posted or channel.reply_id(comment_id) in listed:
                await self._acknowledged(comment_id, pending.event_id)
            else:
                await self._answer(channel, pending.comment, pending.event_id, pending.lines, now)
        handled: list[int | str] = []
        for comment in sorted(comments, key=lambda c: (c["created_at"] or now, c["id"])):
            comment_id, body = comment["id"], comment["body"]
            if not comment_id or comment_id in self.seen or comment_id in self.unanswered or _is_reply(body):
                continue
            if not body.lstrip().startswith(COMMAND):
                continue
            earlier = await asyncio.to_thread(_handled, cfg["workspace"], comment_id, now)
            if earlier is not None:
                event_id, data = earlier
                if data.get("acknowledged"):
                    self.seen.add(comment_id)
                else:  # handled before a restart, but its answer never posted
                    await self._answer(channel, comment, event_id, list(data.get("results") or []), now)
                continue
            await self._handle(channel, cfg, comment, now)
            handled.append(comment_id)
        # Overlap the next window by a minute (clock skew between the tracker and this host); handled ids are
        # skipped.
        self.since = now - timedelta(minutes=1)
        return handled

    async def _handle(self, channel, cfg: dict[str, Any], comment: dict[str, Any], now: datetime) -> None:
        from .events import record

        author = comment["author"]
        lines = await self._apply(channel, cfg, comment, now)
        applied = any(line.endswith((": applied", ": posted", ": cancelled")) or ": retrying " in line
                      for line in lines)
        # Recorded before the answer: a crash after it never applies a command twice (nonces also guard each item).
        # The record says whether the answer posted, so a failed answer is retried without applying again.
        payload = await asyncio.to_thread(
            record, cfg["workspace"], EVENT, f"Operator relay from {author or 'unknown'}: " + "; ".join(lines)[:300],
            actor="operator", level="info" if applied else "warn",
            data={"comment_id": comment["id"], "author": author, "results": lines, "url": comment["url"],
                  "channel": channel.key, "delegated_by": cfg["delegated_by"], "acknowledged": False})
        if applied:
            self.wake()
        await self._answer(channel, comment, int(payload["id"]), lines, now)

    async def _answer(self, channel, comment: dict[str, Any], event_id: int, lines: list[str],
                      now: datetime) -> None:
        """Post the result lines on the control issue. A failed answer is tried again after a growing delay: once
        per poll at first, at most once every ten minutes, with one log line per try."""
        comment_id = comment["id"]
        text = "\n".join([f"{REPLY_HEADER}{comment.get('url') or comment_id} "
                          f"(by {comment.get('author') or 'unknown'}):", *[f"- {line}" for line in lines]])
        try:
            await channel.reply(comment_id, text)
        except Exception as exc:
            pending = self.unanswered.get(comment_id) or Pending(comment, event_id, lines)
            pending.failures += 1
            wait = _backoff(pending.failures)
            pending.next_try = now + wait
            self.unanswered[comment_id] = pending
            log.warning("operator relay could not answer comment %s (try %d); next try in %ds: %s", comment_id,
                        pending.failures, wait.total_seconds(), exc)
            return
        await self._acknowledged(comment_id, event_id)

    async def _acknowledged(self, comment_id: int | str, event_id: int) -> None:
        self.unanswered.pop(comment_id, None)
        self.seen.add(comment_id)
        await asyncio.to_thread(_mark_acknowledged, event_id)

    async def _apply(self, channel, cfg: dict[str, Any], comment: dict[str, Any], now: datetime) -> list[str]:
        if not channel.listed(comment):
            return [f"not applied: {comment['author'] or 'unknown'} is not a listed operator"]
        created = comment["created_at"]
        if created is None or now - created > MAX_AGE:
            return ["not applied: first seen more than 24 hours after it was written"]
        refusal = await channel.refusal(comment)
        if refusal:
            return [f"not applied: {refusal}"]
        items, error = parse(comment["body"])
        if items is None:
            return [f"not applied: {error}"]
        comment_id = comment["id"]
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
