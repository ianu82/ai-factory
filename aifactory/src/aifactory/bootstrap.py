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
    if declared:
        loop.create_task(_declare_outcomes(workspace_id, declared, task))
    log.info("Bootstrap workspace ready: %s", workspace_id)
    return workspace_id


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
