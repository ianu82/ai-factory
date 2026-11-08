"""Server-side bootstrap: an operator-declared organization, workspace and repositories.

``AIFACTORY_BOOTSTRAP`` is JSON, for example::

    {"organization": "MindsDB", "workspace": "AI Factory",
     "admins": ["ian@mindsdb.com"], "repositories": ["ianu82/calorie-tracker"],
     "settings": {"autonomy": {"merge": "auto"}}}

It is applied idempotently at startup. Admin emails are invitations: the membership is
created when that person signs in with MindsHub (their email is verified by MindsHub), and
the bootstrap organization is then linked to their real MindsHub organization.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os

from .db import session_scope
from .models import Organization, Repository, Workspace
from .pipeline.runtime import DEFAULT_SETTINGS, merged_settings

log = logging.getLogger(__name__)


def bootstrap_config() -> dict | None:
    raw = os.environ.get("AIFACTORY_BOOTSTRAP", "").strip()
    if not raw:
        return None
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        log.error("AIFACTORY_BOOTSTRAP is not valid JSON; ignoring it")
        return None
    return data if isinstance(data, dict) else None


def invited_role(email: str | None, workspace: Workspace) -> str | None:
    if not email:
        return None
    invites = (workspace.settings or {}).get("invites") or {}
    return invites.get(email.lower())


def apply_bootstrap() -> str | None:
    cfg = bootstrap_config()
    if not cfg:
        return None
    with session_scope() as s:
        org = s.query(Organization).filter(Organization.name == cfg.get("organization", "Factory")).first()
        if org is None:
            org = Organization(name=cfg.get("organization", "Factory"))
            s.add(org)
            s.flush()
        ws = s.query(Workspace).filter(Workspace.org_id == org.id, Workspace.name == cfg.get("workspace", "Factory")).first()
        if ws is None:
            ws = Workspace(org_id=org.id, name=cfg.get("workspace", "Factory"), slug="default",
                           settings=json.loads(json.dumps(DEFAULT_SETTINGS)))
            s.add(ws)
            s.flush()
        settings = merged_settings(ws.settings)
        overrides = cfg.get("settings") or {}
        for key, value in overrides.items():
            settings[key] = {**settings.get(key, {}), **value} if isinstance(value, dict) else value
        settings["invites"] = {**(settings.get("invites") or {}),
                               **{e.lower(): "owner" for e in cfg.get("admins", [])}}
        ws.settings = settings
        existing = {r.full_name: r for r in s.query(Repository).filter(Repository.workspace_id == ws.id)}
        pending = []
        for entry in cfg.get("repositories", []):
            name, overrides = (entry, {}) if isinstance(entry, str) else (entry.get("full_name"), entry.get("config") or {})
            if name in existing:
                if overrides:
                    existing[name].config = {**(existing[name].config or {}), **overrides}
            elif name:
                pending.append((name, overrides))
        workspace_id = ws.id
    loop = asyncio.get_running_loop()
    task = loop.create_task(_connect(workspace_id, pending)) if pending else None
    declared = cfg.get("outcomes") or []
    _cancel_declared(workspace_id, cfg.get("cancel") or [])
    _retry_declared(workspace_id, cfg.get("retry") or [])
    if declared:
        loop.create_task(_declare_outcomes(workspace_id, declared, task))
    log.info("Bootstrap workspace ready: %s", workspace_id)
    return workspace_id


def _cancel_declared(workspace_id: str, keys: list[str]) -> None:
    """Operator cancellation of declared outcomes (e.g. an accidental duplicate)."""
    if not keys:
        return
    from .events import record
    from .models import Outcome, Task

    with session_scope() as s:
        for key in keys:
            outcome = s.query(Outcome).filter(Outcome.workspace_id == workspace_id,
                                              Outcome.signal_id == f"operator:{key}").first()
            if outcome is None or outcome.status in ("completed", "cancelled", "partially_completed"):
                continue
            outcome.status = "cancelled"
            for t in s.query(Task).filter(Task.outcome_id == outcome.id, Task.status != "completed"):
                t.status = "cancelled"
            record(workspace_id, "outcome_cancelled", f"Operator cancelled #{outcome.number} (declared key {key})",
                   outcome_id=outcome.id, actor="operator", level="warn", session=s)


def _retry_declared(workspace_id: str, items: list[dict]) -> None:
    """One-shot operator retry of a blocked declared outcome; each nonce applies once."""
    from .db import utcnow
    from .events import record
    from .models import Decision, Event, Outcome

    with session_scope() as s:
        for item in items:
            key, nonce = str(item.get("key", "")), str(item.get("nonce", ""))
            outcome = s.query(Outcome).filter(Outcome.workspace_id == workspace_id,
                                              Outcome.signal_id == f"operator:{key}").first()
            if outcome is None:
                continue
            done = any((e.data or {}).get("nonce") == nonce for e in s.query(Event).filter(
                Event.outcome_id == outcome.id, Event.kind == "operator_retry"))
            if done:
                continue
            from .models import Task

            blocked_tasks = s.query(Task).filter(Task.outcome_id == outcome.id, Task.status == "blocked").all()
            if outcome.status == "blocked" and not blocked_tasks:
                outcome.status, outcome.blocked_reason = "planning", ""
                what = "planning"
            elif blocked_tasks:
                for t in blocked_tasks:
                    t.status, t.blocked_reason = "queued", ""  # resumes at its durable stage
                outcome.status = "running"
                what = ", ".join(t.key for t in blocked_tasks)
            else:
                continue
            for d in s.query(Decision).filter(Decision.outcome_id == outcome.id, Decision.status == "open",
                                              Decision.kind == "unblock"):
                d.status, d.resolution, d.comment, d.resolved_at = "resolved", "retry", "Operator retried", utcnow()
            record(workspace_id, "operator_retry", f"Operator retried {what} for #{outcome.number}",
                   outcome_id=outcome.id, actor="operator", session=s, data={"key": key, "nonce": nonce})


async def _declare_outcomes(workspace_id: str, items: list[dict], connecting) -> None:
    """Operator-declared work (idempotent by key): visible in the app with source "operator"."""
    if connecting is not None:
        await connecting
    from .db import SessionLocal
    from .events import record
    from .models import Outcome
    from .signals import outcome_from_text

    db = SessionLocal()
    try:
        for item in items:
            key = str(item.get("key") or item.get("title") or "")[:200]
            if not key or not item.get("title"):
                continue
            marker = f"operator:{key}"
            if db.query(Outcome).filter(Outcome.workspace_id == workspace_id, Outcome.signal_id == marker).first():
                continue
            repos = db.query(Repository).filter(Repository.workspace_id == workspace_id,
                                                Repository.full_name.in_(item.get("repositories") or [])).all()
            if not repos:
                log.error("Declared outcome %s names no connected repository", key)
                continue
            outcome = outcome_from_text(db, workspace_id, None, item["title"], item.get("brief", ""), [r.id for r in repos],
                                        acceptance=item.get("acceptance") or [], harness=item.get("harness"),
                                        source="operator", signal_id=marker)
            record(workspace_id, "outcome_declared", f"Operator-declared outcome #{outcome.number} admitted",
                   outcome_id=outcome.id, session=db, data={"key": key})
        db.commit()
    finally:
        db.close()


async def _connect(workspace_id: str, names: list[tuple[str, dict]]) -> None:
    from .github import GitHub, GitHubError, resolve_token
    from .repo_detect import detect

    for name, overrides in names:
        try:
            gh = GitHub(resolve_token(workspace_id))
            info = await gh.repo(name)
            config = {**await detect(gh, name, info.get("default_branch") or "main"), **overrides}
        except GitHubError as exc:
            log.error("Bootstrap could not connect %s: %s", name, exc)
            continue
        with session_scope() as s:
            if not s.query(Repository).filter(Repository.workspace_id == workspace_id,
                                              Repository.full_name == info["full_name"]).first():
                s.add(Repository(workspace_id=workspace_id, full_name=info["full_name"],
                                 default_branch=info.get("default_branch") or "main",
                                 description=info.get("description") or "", config=config))
        log.info("Bootstrap connected %s", name)
