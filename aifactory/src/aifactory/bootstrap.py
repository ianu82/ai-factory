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

from . import definitions
from .db import session_scope
from .models import Organization, Repository, Workspace
from .pipeline.runtime import DEFAULT_SETTINGS, merged_settings
from .steward_policy import validate_steward

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


def _repo_overrides(name: str, overrides: dict) -> dict:
    """Skip invalid operator policy without discarding unrelated configuration."""
    overrides = dict(overrides)
    if "steward" in overrides:
        try:
            overrides["steward"] = validate_steward(overrides["steward"])
        except ValueError as exc:
            log.error("Bootstrap ignoring invalid steward policy for %s: %s", name, exc)
            overrides.pop("steward")
    return overrides


BOOTSTRAP_MARKER = "_bootstrap_workspace"


def apply_bootstrap() -> str | None:
    from .fallbacks import set_bootstrap_workspace

    set_bootstrap_workspace(None)
    cfg = bootstrap_config()
    if not cfg:
        return None
    with session_scope() as s:
        # The managed workspace carries a marker in its settings (the API strips "_" keys), found first and in
        # any organization: renaming the workspace in the app, or its organization through MindsHub sign-in,
        # keeps it. Without one, the first startup adopts the oldest workspace of the configured name in the
        # configured organization; a later namesake never takes over its configuration or credentials.
        ws = next((w for w in s.query(Workspace).order_by(Workspace.created_at, Workspace.id)
                   if (w.settings or {}).get(BOOTSTRAP_MARKER) is True), None)
        if ws is None:
            org = s.query(Organization).filter(Organization.name == cfg.get("organization", "Factory")).first()
            if org is None:
                org = Organization(name=cfg.get("organization", "Factory"))
                s.add(org)
                s.flush()
            in_org = (s.query(Workspace).filter(Workspace.org_id == org.id)
                      .order_by(Workspace.created_at, Workspace.id).all())
            ws = next((w for w in in_org if w.name == cfg.get("workspace", "Factory")), None)
            if ws is None:
                taken = {w.slug for w in in_org}  # sign-in may already have made this org's "default" workspace
                slug = next(c for c in ("default", "factory", *(f"factory-{n}" for n in range(2, 1000)))
                            if c not in taken)
                ws = Workspace(org_id=org.id, name=cfg.get("workspace", "Factory"), slug=slug,
                               settings=json.loads(json.dumps(DEFAULT_SETTINGS)))
                s.add(ws)
                s.flush()
        from .onboarding_policy import lock_workspace

        ws = lock_workspace(s, ws.id)
        set_bootstrap_workspace(ws.id)
        settings = merged_settings(ws.settings)
        overrides = cfg.get("settings") or {}
        applied = dict(settings.get("_bootstrap") or {})
        for key, value in overrides.items():
            if _changed(applied, key, value):  # otherwise keep what people set in the app since
                settings[key] = {**settings.get(key, {}), **value} if isinstance(value, dict) else value
        settings["_bootstrap"] = applied
        settings[BOOTSTRAP_MARKER] = True
        settings["invites"] = {**(settings.get("invites") or {}),
                               **{e.lower(): "owner" for e in cfg.get("admins", [])}}
        ws.settings = settings
        definitions.lock_workspace(s, ws.id)
        existing = {r.full_name: r for r in definitions.lock_repositories(s, ws.id)}
        pending = []
        for entry in cfg.get("repositories", []):
            name, overrides = (entry, {}) if isinstance(entry, str) else (entry.get("full_name"), entry.get("config") or {})
            overrides = _repo_overrides(name, overrides)
            branch = None if isinstance(entry, str) else entry.get("default_branch")
            if name in existing:
                if overrides:
                    current = dict(existing[name].config or {})
                    applied = dict(current.get("_bootstrap") or {})
                    for key, value in overrides.items():
                        if _changed(applied, key, value):
                            current[key] = value
                    current["_bootstrap"] = applied
                    try:
                        # Roll back both context and its applied-value marker on rejection,
                        # so the operator's intent can be retried after quota is freed.
                        with s.begin_nested():
                            existing[name].config = current
                            definitions.validate_repository_change(s, ws.id)
                    except definitions.DefinitionError as exc:
                        log.error("Bootstrap skipped configuration for %s: %s", name, exc)
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
    _guidance_delegated(workspace_id, cfg.get("guidance") or [], cfg.get("delegated_by", ""))
    if declared:
        loop.create_task(_declare_outcomes(workspace_id, declared, task))
    log.info("Bootstrap workspace ready: %s", workspace_id)
    return workspace_id


def _cancel_declared(workspace_id: str, keys: list[str], *, live: bool = False) -> list[str]:
    """Operator cancellation of outcomes: a declared key (e.g. an accidental duplicate), or "#N" for outcome
    number N from any source (e.g. an intake outcome replaced by a re-scoped one). "#N" applies only while no
    task has ever started (no attempt, stage, start or PR; an interrupted task is persisted as queued with
    those markers), so it never discards work in flight; otherwise it is skipped and logged.

    {"outcome": N, "discard_started": true} is the explicit form that also discards started work. It runs
    before the orchestrator starts, so no task is live in this process; outcomes with a pull request are
    still refused (closing PRs belongs to the app's own cancel).

    ``live`` (an operator relay while the orchestrator runs, see operator_relay) never discards started work, by
    key or by number: a task may be running in this process. Returns one line per item saying what happened."""
    results: list[str] = []
    if not keys:
        return results
    from .events import record
    from .models import Decision, Outcome, Task

    with session_scope() as s:
        for key in keys:
            discard = isinstance(key, dict) and key.get("discard_started") is True and not live
            if isinstance(key, dict):
                key = f"#{key.get('outcome')}"
            key = str(key)
            by_number = key.startswith("#") and key[1:].isdigit()
            query = s.query(Outcome).filter(Outcome.workspace_id == workspace_id)
            outcome = (query.filter(Outcome.number == int(key[1:])) if by_number
                       else query.filter(Outcome.signal_id == f"operator:{key}")).first()
            if outcome is None or outcome.status in ("completed", "cancelled", "partially_completed"):
                results.append(f"cancel {key}: no open outcome")
                continue
            tasks = s.query(Task).filter(Task.outcome_id == outcome.id).all()
            started = [t.key for t in tasks if t.status not in ("pending", "queued", "cancelled")
                       or t.attempt or t.stage or t.started_at is not None or t.pr_number]
            with_prs = [t.key for t in tasks if t.pr_number and t.status != "completed"]
            if (by_number or live) and (with_prs or (started and not discard)):
                why = (f"open pull requests on {', '.join(with_prs)}" if with_prs
                       else f"work has started on {', '.join(started)}")
                log.error("Not cancelling #%s: %s", outcome.number, why)
                results.append(f"cancel #{outcome.number}: not cancelled, {why}")
                continue
            outcome.status = "cancelled"
            for t in tasks:
                if t.status != "completed":
                    t.status = "cancelled"
            for d in s.query(Decision).filter(Decision.outcome_id == outcome.id, Decision.status == "open"):
                d.status = "superseded"  # as the app's own cancel does: a stale answer must not restart work
            how = "" if by_number else f" (declared key {key})"
            record(workspace_id, "outcome_cancelled", f"Operator cancelled #{outcome.number}{how}",
                   outcome_id=outcome.id, actor="operator", level="warn", session=s)
            results.append(f"cancel #{outcome.number}: cancelled")
    return results


def _retry_declared(workspace_id: str, items: list[dict]) -> list[str]:
    """One-shot operator retry of a blocked declared outcome; each nonce applies once. Returns one line per item."""
    results: list[str] = []
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
                results.append(f"retry {key}: no such outcome")
                continue
            done = any((e.data or {}).get("nonce") == nonce for e in s.query(Event).filter(
                Event.outcome_id == outcome.id, Event.kind == "operator_retry"))
            if done:
                results.append(f"retry {key}: already applied (nonce {nonce})")
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
                        from .decisions import fresh_budget

                        t.evidence = fresh_budget(t.evidence)
                        t.stage = "revise" if t.pr_number else ""
                    t.status, t.blocked_reason = "queued", ""  # otherwise resumes at its durable stage
                outcome.status = "running"
                what = ", ".join(t.key for t in blocked_tasks)
            else:
                results.append(f"retry {key}: nothing blocked or failed to retry")
                continue
            for d in s.query(Decision).filter(Decision.outcome_id == outcome.id, Decision.status == "open",
                                              Decision.kind == "unblock"):
                d.status, d.resolution, d.comment, d.resolved_at = "resolved", "retry", "Operator retried", utcnow()
            record(workspace_id, "operator_retry", f"Operator retried {what} for #{outcome.number}",
                   outcome_id=outcome.id, actor="operator", session=s, data={"key": key, "nonce": nonce})
            results.append(f"retry {key}: retrying {what}")
    return results


def _operator_user(db, delegated_by: str):
    """The system identity operator actions are recorded under: one per delegator, never renamed, so what was
    delegated earlier keeps naming who delegated it. (Records before this used one shared identity, which
    keeps the name it last had.)"""
    import hashlib

    from .models import User

    key = hashlib.sha256(delegated_by.strip().lower().encode()).hexdigest()[:12] if delegated_by else "self"
    email = f"operator+{key}@factory.local"
    operator = db.query(User).filter(User.is_system.is_(True), User.email == email).first()
    if operator is None:
        name = f"Operator (delegated by {delegated_by})" if delegated_by else "Operator"
        operator = User(email=email, name=name, is_system=True)
        db.add(operator)
        db.commit()
    return operator


# Outside the bootstrap workspace the operator may only relay; anything that approves, spends, stops or merges
# stays with that workspace's people.
RELAY_ONLY = {("approve_merge", "reject"), ("question", "answer"), ("unblock", "retry"), ("unblock", "acknowledge")}
# Resuming a task at one of these stages would merge, release or revert: never a relay.
MERGING_STAGES = {"merge", "release", "revert"}


def _delegation_target(db, bootstrap_workspace_id: str, item: dict, delegated_by: str, *, kind: str = "",
                       resolution: str | None = None) -> str | None:
    """The workspace a delegated item acts in: the bootstrap workspace by default, or another workspace (by id)
    where the delegating person is a member, so the operator never does what they could not do themselves.
    Outside the bootstrap workspace only RELAY_ONLY resolutions apply (guidance has resolution None)."""
    target = str(item.get("workspace") or bootstrap_workspace_id)
    if target == bootstrap_workspace_id:
        return target
    if resolution is not None and (kind, resolution) not in RELAY_ONLY:
        log.error("Delegated %s '%s' refused in workspace %s: the operator only relays there", kind, resolution,
                  target)
        return None
    from sqlalchemy import func

    from .auth import ROLE_RANK
    from .models import Membership, User

    user = (db.query(User).filter(func.lower(User.email) == delegated_by.strip().lower(), User.is_system.is_(False))
            .first() if delegated_by else None)
    member = user and db.query(Membership).filter(Membership.workspace_id == target,
                                                  Membership.user_id == user.id).first()
    if not member or ROLE_RANK.get(member.role, 0) < ROLE_RANK["member"]:
        log.error("Delegated item refused in workspace %s: %s is not a member there", target, delegated_by or "nobody")
        return None
    return target


def _guidance_delegated(workspace_id: str, items: list[dict], delegated_by: str) -> list[str]:
    """Post guidance to an outcome (or one task) as the operator under a person's delegation, like the app's
    guidance box: outcome-level guidance reaches every task of the outcome, including tasks not started yet.

    Each item: {"outcome": N, "task": "T2" (optional), "body": "...", "nonce": "...", "workspace": id (optional,
    see _delegation_target)}; each nonce posts once per workspace. Returns one line per item.
    """
    results: list[str] = []
    if not items:
        return results
    from .db import SessionLocal
    from .events import record
    from .models import Event, Message, Outcome, Task

    db = SessionLocal()
    try:
        operator = _operator_user(db, delegated_by)
        for item in items:
            body, nonce = str(item.get("body") or "").strip(), str(item.get("nonce") or "")
            label = f"guidance #{item.get('outcome')}" + (f" {item['task']}" if item.get("task") else "")
            target = _delegation_target(db, workspace_id, item, delegated_by)
            if target is None:
                results.append(f"{label}: refused by the delegation rules")
                continue
            outcome = db.query(Outcome).filter(Outcome.workspace_id == target,
                                               Outcome.number == int(item.get("outcome", 0))).first()
            if outcome is None or not body or not nonce:
                results.append(f"{label}: no such outcome, or no body or nonce")
                continue
            if any((e.data or {}).get("nonce") == nonce for e in db.query(Event).filter(  # once per workspace
                    Event.workspace_id == target, Event.kind == "guidance")):
                results.append(f"{label}: already posted (nonce {nonce})")
                continue
            task_id = None
            if item.get("task"):
                task = db.query(Task).filter(Task.outcome_id == outcome.id, Task.key == str(item["task"])).first()
                if task is None:
                    results.append(f"{label}: no such task")
                    continue
                task_id = task.id
            # The message and its nonce-bearing event commit together, so a crash can never post it twice.
            db.add(Message(workspace_id=target, outcome_id=outcome.id, task_id=task_id, author_id=operator.id,
                           author_kind="user", body=body))
            record(target, "guidance", f"{operator.name}: {body[:200]}", outcome_id=outcome.id, task_id=task_id,
                   actor="operator", data={"intervention": True, "nonce": nonce, "delegated_by": delegated_by},
                   session=db)
            db.commit()
            results.append(f"{label}: posted")
    finally:
        db.close()
    return results


def _resolve_delegated(workspace_id: str, items: list[dict], delegated_by: str,
                       schedule=None) -> list[str]:
    """Resolve open decisions as an operator acting under a person's explicit delegation.

    Each item: {"outcome": N, "task": "T1" (optional), "kind": "approve_merge"|"question"|..., "resolution": "...",
    "comment": "...", "reusable": bool, "nonce": "...", "workspace": id (optional, see _delegation_target),
    "head": sha (optional, at least 7 characters: apply only while the task's head is still that commit)}. The
    decision records the operator identity and who delegated it. Returns one line per item.

    ``schedule`` runs follow-up coroutines (publishing a handover, closing a dropped pull request); by default on
    the running event loop, as at startup.
    """
    results: list[str] = []
    if not items:
        return results
    if schedule is None:
        schedule = asyncio.get_running_loop().create_task
    from .db import SessionLocal
    from .decisions import DecisionError, resolve
    from .events import record
    from .models import Decision, Event, Outcome

    db = SessionLocal()
    try:
        operator = _operator_user(db, delegated_by)
        name = operator.name
        for item in items:
            label = (f"resolve #{item.get('outcome')}" + (f" {item['task']}" if item.get("task") else "")
                     + f" {item.get('kind')} {item.get('resolution')}")
            target = _delegation_target(db, workspace_id, item, delegated_by, kind=str(item.get("kind", "")),
                                        resolution=str(item.get("resolution", "")))
            if target is None:
                results.append(f"{label}: refused by the delegation rules")
                continue
            outcome = db.query(Outcome).filter(Outcome.workspace_id == target,
                                               Outcome.number == int(item.get("outcome", 0))).first()
            if outcome is None:
                results.append(f"{label}: no such outcome")
                continue
            nonce = str(item.get("nonce", ""))
            if any((e.data or {}).get("nonce") == nonce for e in db.query(Event).filter(
                    Event.outcome_id == outcome.id, Event.kind == "operator_decision")):
                results.append(f"{label}: already applied (nonce {nonce})")
                continue
            query = db.query(Decision).filter(Decision.outcome_id == outcome.id, Decision.status == "open",
                                              Decision.kind == item.get("kind"))
            if item.get("task"):  # one task's decision when several tasks of an outcome wait at once
                from .models import Task

                task = db.query(Task).filter(Task.outcome_id == outcome.id, Task.key == str(item["task"])).first()
                if task is None:
                    results.append(f"{label}: no such task")
                    continue
                query = query.filter(Decision.task_id == task.id)
            decision = query.order_by(Decision.created_at.desc()).first()
            if decision is None:
                results.append(f"{label}: no open decision of that kind")
                continue
            expected = str(item.get("head") or "").strip().lower()
            if expected and decision.task_id:
                # A decision judged on one commit must not apply to a newer one: an approval staged for a reviewed
                # head would otherwise merge an unreviewed revision pushed before the next restart.
                from .models import Task

                current = (db.get(Task, decision.task_id).head_sha or "").lower()
                if len(expected) < 7 or not current.startswith(expected):
                    log.error("Delegated %s for #%s skipped: it was judged on %s but the task's head is %s",
                              item["resolution"], outcome.number, expected[:12], current[:12] or "unknown")
                    results.append(f"{label}: skipped, judged on {expected[:12]} but the head is "
                                   f"{current[:12] or 'unknown'}")
                    continue
            if target != workspace_id and item["resolution"] != "reject" and decision.task_id:
                from .models import Task

                stuck = db.get(Task, decision.task_id)
                if stuck is not None and stuck.stage in MERGING_STAGES:
                    log.error("Delegated %s refused for #%s %s: resuming its %s stage would merge, release or revert",
                              item["resolution"], outcome.number, stuck.key, stuck.stage)
                    results.append(f"{label}: refused, resuming its {stuck.stage} stage would merge, release or revert")
                    continue
            try:
                # Only the bootstrap workspace takes operator decisions as reusable precedents.
                reusable = bool(item.get("reusable")) and target == workspace_id
                result = resolve(db, decision, operator, item["resolution"], item.get("comment", ""),
                                 reusable, item.get("scope", "workspace") if reusable else "item")
                if result.get("handover"):
                    from .pipeline.handover import publish_candidate

                    schedule(publish_candidate(decision.task_id, result["handover"]))
                if result.get("dropped"):  # as the app's own resolve: an abandoned candidate's PR is closed
                    from .pipeline.handover import close_dropped_pull

                    schedule(close_dropped_pull(result["dropped"], name, "Abandoned"))
            except DecisionError as exc:
                log.error("Delegated decision for #%s not applied: %s", outcome.number, exc)
                results.append(f"{label}: not applied, {exc}")
                continue
            record(target, "operator_decision",
                   f"{name} resolved '{decision.title}': {item['resolution']}", outcome_id=outcome.id,
                   actor="operator", data={"nonce": nonce, "decision_id": decision.id, "delegated_by": delegated_by})
            results.append(f"{label}: applied")
    finally:
        db.close()
    return results


def _priority(value) -> int:
    """0 urgent .. 3 low, as in the app; anything else is the default 2."""
    return value if type(value) is int and 0 <= value <= 3 else 2


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
                                        source="operator", signal_id=marker, priority=_priority(item.get("priority")))
            record(workspace_id, "outcome_declared", f"Operator-declared outcome #{outcome.number} admitted",
                   outcome_id=outcome.id, session=db, data={"key": key})
        db.commit()
    finally:
        db.close()


async def _connect(workspace_id: str, names: list[tuple[str, dict]]) -> None:
    from .github import GitHub, GitHubError, resolve_credential as resolve_token
    from .repo_detect import detect

    for name, overrides in names:
        overrides = _repo_overrides(name, overrides)
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
        try:
            with session_scope() as s:
                definitions.lock_workspace(s, workspace_id)
                repositories = definitions.lock_repositories(s, workspace_id)
                if not any(r.full_name == info["full_name"] for r in repositories):
                    s.add(Repository(workspace_id=workspace_id, full_name=info["full_name"], default_branch=branch,
                                     description=info.get("description") or "", config=config))
                    definitions.validate_repository_change(s, workspace_id)
        except definitions.DefinitionError as exc:
            log.error("Bootstrap could not connect %s: %s", name, exc)
            continue
        log.info("Bootstrap connected %s", name)
