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


def _changed(applied: dict[str, str], key: str, value) -> bool:
    """Bootstrap values are operator intent at the time they change: apply a key when its declared value
    differs from the one last applied, and leave later edits made in the app alone otherwise."""
    import hashlib

    digest = hashlib.sha256(json.dumps(value, sort_keys=True, default=str).encode()).hexdigest()[:16]
    if applied.get(key) == digest:
        return False
    applied[key] = digest
    return True


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
        applied = dict(settings.get("_bootstrap") or {})
        for key, value in overrides.items():
            if _changed(applied, key, value):  # otherwise keep what people set in the app since
                settings[key] = {**settings.get(key, {}), **value} if isinstance(value, dict) else value
        settings["_bootstrap"] = applied
        settings["invites"] = {**(settings.get("invites") or {}),
                               **{e.lower(): "owner" for e in cfg.get("admins", [])}}
        ws.settings = settings
        existing = {r.full_name: r for r in s.query(Repository).filter(Repository.workspace_id == ws.id)}
        pending = []
        for entry in cfg.get("repositories", []):
            name, overrides = (entry, {}) if isinstance(entry, str) else (entry.get("full_name"), entry.get("config") or {})
            branch = None if isinstance(entry, str) else entry.get("default_branch")
            if name in existing:
                if overrides:
                    current = dict(existing[name].config or {})
                    applied = dict(current.get("_bootstrap") or {})
                    for key, value in overrides.items():
                        if _changed(applied, key, value):
                            current[key] = value
                    current["_bootstrap"] = applied
                    existing[name].config = current
                if branch:
                    existing[name].default_branch = branch
            elif name:
                pending.append((name, {**overrides, **({"_base_branch": branch} if branch else {})}))
        workspace_id = ws.id
    loop = asyncio.get_running_loop()
    task = loop.create_task(_connect(workspace_id, pending)) if pending else None
    declared = cfg.get("outcomes") or []
    _cancel_declared(workspace_id, cfg.get("cancel") or [])
    _retry_declared(workspace_id, cfg.get("retry") or [])
    _resolve_delegated(workspace_id, cfg.get("resolve") or [], cfg.get("delegated_by", ""))
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
            if item.get("outcome"):
                outcome = s.query(Outcome).filter(Outcome.workspace_id == workspace_id,
                                                  Outcome.number == int(item["outcome"])).first()
                key = f"#{item['outcome']}"
            else:
                outcome = s.query(Outcome).filter(Outcome.workspace_id == workspace_id,
                                                  Outcome.signal_id == f"operator:{key}").first()
            if outcome is None:
                continue
            done = any((e.data or {}).get("nonce") == nonce for e in s.query(Event).filter(
                Event.outcome_id == outcome.id, Event.kind == "operator_retry"))
            if done:
                continue
            from .models import Task

            retry_statuses = ["blocked", "failed"] + (["cancelled"] if item.get("include_cancelled") else [])
            blocked_tasks = s.query(Task).filter(Task.outcome_id == outcome.id,
                                                 Task.status.in_(retry_statuses)).all()
            if outcome.status == "blocked" and not blocked_tasks:
                outcome.status, outcome.blocked_reason = "planning", ""
                what = "planning"
            elif blocked_tasks:
                outcome.completed_at = None
                for t in blocked_tasks:
                    if t.status == "failed":  # attempts exhausted: start fresh attempts
                        t.attempt = 0
                        t.stage = "revise" if t.pr_number else ""
                    t.status, t.blocked_reason = "queued", ""  # otherwise resumes at its durable stage
                outcome.status = "running"
                what = ", ".join(t.key for t in blocked_tasks)
            else:
                continue
            for d in s.query(Decision).filter(Decision.outcome_id == outcome.id, Decision.status == "open",
                                              Decision.kind == "unblock"):
                d.status, d.resolution, d.comment, d.resolved_at = "resolved", "retry", "Operator retried", utcnow()
            record(workspace_id, "operator_retry", f"Operator retried {what} for #{outcome.number}",
                   outcome_id=outcome.id, actor="operator", session=s, data={"key": key, "nonce": nonce})


def _resolve_delegated(workspace_id: str, items: list[dict], delegated_by: str) -> None:
    """Resolve open decisions as an operator acting under a person's explicit delegation.

    Each item: {"outcome": N, "kind": "approve_merge"|"question"|..., "resolution": "...", "comment": "...",
    "reusable": bool, "nonce": "..."}. The decision records the operator identity and who delegated it.
    """
    if not items:
        return
    from .db import SessionLocal
    from .decisions import DecisionError, resolve
    from .events import record
    from .models import Decision, Event, Outcome, User

    db = SessionLocal()
    try:
        name = f"Operator (delegated by {delegated_by})" if delegated_by else "Operator"
        operator = db.query(User).filter(User.is_system.is_(True), User.email == "operator@factory.local").first()
        if operator is None:
            operator = User(email="operator@factory.local", name=name, is_system=True)
            db.add(operator)
            db.commit()
        elif operator.name != name:
            operator.name = name
            db.commit()
        for item in items:
            outcome = db.query(Outcome).filter(Outcome.workspace_id == workspace_id,
                                               Outcome.number == int(item.get("outcome", 0))).first()
            if outcome is None:
                continue
            nonce = str(item.get("nonce", ""))
            if any((e.data or {}).get("nonce") == nonce for e in db.query(Event).filter(
                    Event.outcome_id == outcome.id, Event.kind == "operator_decision")):
                continue
            decision = (db.query(Decision).filter(Decision.outcome_id == outcome.id, Decision.status == "open",
                                                  Decision.kind == item.get("kind")).order_by(Decision.created_at.desc()).first())
            if decision is None:
                continue
            try:
                resolve(db, decision, operator, item["resolution"], item.get("comment", ""),
                        bool(item.get("reusable")), item.get("scope", "workspace" if item.get("reusable") else "item"))
            except DecisionError as exc:
                log.error("Delegated decision for #%s not applied: %s", outcome.number, exc)
                continue
            record(workspace_id, "operator_decision",
                   f"{name} resolved '{decision.title}': {item['resolution']}", outcome_id=outcome.id,
                   actor="operator", data={"nonce": nonce, "decision_id": decision.id, "delegated_by": delegated_by})
    finally:
        db.close()


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
            branch = overrides.pop("_base_branch", None) or info.get("default_branch") or "main"
            applied: dict[str, str] = {}
            for key, value in overrides.items():
                _changed(applied, key, value)
            config = {**await detect(gh, name, branch), **overrides, "_bootstrap": applied}
        except GitHubError as exc:
            log.error("Bootstrap could not connect %s: %s", name, exc)
            continue
        with session_scope() as s:
            if not s.query(Repository).filter(Repository.workspace_id == workspace_id,
                                              Repository.full_name == info["full_name"]).first():
                s.add(Repository(workspace_id=workspace_id, full_name=info["full_name"], default_branch=branch,
                                 description=info.get("description") or "", config=config))
        log.info("Bootstrap connected %s", name)
