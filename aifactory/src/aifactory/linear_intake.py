"""Transactional, restart-safe admission of configured Linear work."""

from __future__ import annotations

import re
from typing import Any

from pydantic import ValidationError
from sqlalchemy import update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from . import linear_config
from .events import notify, record
from .linear import LinearError, LinearProvider
from .models import Event, Outcome, Repository, Signal, Workspace
from .signals import fingerprint, outcome_from_text, upsert_signal


def acceptance_from_description(description: str) -> list[str]:
    criteria: list[str] = []
    active = False
    lines = description.splitlines()
    for index, line in enumerate(lines):
        heading = re.match(r"^\s{0,3}#{1,6}\s+(.+?)\s*#*\s*$", line)
        setext = index + 1 < len(lines) and re.fullmatch(r"\s*(?:=+|-+)\s*", lines[index + 1])
        text = heading.group(1) if heading else line.strip()
        normalized = text.strip("*_").rstrip(":").strip().casefold()
        if heading or setext or normalized == "acceptance criteria":
            if active:
                break
            active = normalized == "acceptance criteria"
            continue
        if active:
            item = re.match(r"^\s*(?:[-+*]|\d+[.)])\s+(?:\[[ xX]\]\s*)?(.+)$", line)
            if item:
                criteria.append(item.group(1).strip())
    return criteria


def _problem(db: Session, workspace_id: str, category: str) -> None:
    if not db.query(Event).filter_by(workspace_id=workspace_id, kind="linear_intake_warning").filter(
            Event.data["category"].as_string() == category).first():
        message = ("Linear intake skipped: no configured repositories remain connected. Update Linear settings."
                   if category == "repositories" else f"Linear intake skipped ({category})")
        record(workspace_id, "linear_intake_warning", message, level="warn",
               data={"category": category}, session=db)


def _admit(db: Session, workspace_id: str, issue: dict[str, Any], fetched_config: linear_config.LinearConfig) -> tuple[bool, bool]:
    db.execute(update(Workspace).where(Workspace.id == workspace_id).values(
        outcome_seq=Workspace.outcome_seq).execution_options(synchronize_session=False))
    workspace = db.query(Workspace).filter_by(id=workspace_id).populate_existing().one()
    config = linear_config.saved_config(workspace)
    if not config.enabled or config != fetched_config:
        return False, False
    available = {repo.id for repo in db.query(Repository).filter_by(workspace_id=workspace_id)}
    repo_ids = list(dict.fromkeys(repo_id for repo_id in config.repo_ids if repo_id in available))
    if not repo_ids:
        _problem(db, workspace_id, "repositories")
        return False, False
    existing = db.query(Signal).filter_by(workspace_id=workspace_id,
        fingerprint=fingerprint("linear_issue", issue["id"])).populate_existing().one_or_none()
    if existing is not None and existing.outcome_id:
        return False, False
    description = issue.get("description") or ""
    signal, created = upsert_signal(db, workspace_id, source="linear_issue", external_id=issue["id"],
        title=issue["title"], body=description, url=issue["url"], repo_id=None)
    signal.data = {"identifier": issue["identifier"], "team_id": config.team_id}
    priority = {1: 0, 2: 1, 3: 2, 4: 3}.get(issue.get("priority"), 2)
    outcome = outcome_from_text(db, workspace_id, None, issue["title"], description, repo_ids,
        acceptance=acceptance_from_description(description), priority=priority, source="signal", signal_id=signal.id)
    outcome.title, outcome.brief = issue["title"], description
    signal.status, signal.outcome_id = "accepted", outcome.id
    record(workspace_id, "signal_accepted", f"Accepted Linear {issue['identifier']}",
           outcome_id=outcome.id, data={"url": issue["url"]}, session=db)
    return created, True


async def sync_linear(db: Session, workspace_id: str, provider: LinearProvider | None = None) -> dict[str, int]:
    result = {"created": 0, "accepted": 0}
    try:
        config = linear_config.saved_config(db.get(Workspace, workspace_id))
        db.commit()
        if not config.enabled:
            return result
        issues = await (provider or linear_config.provider(workspace_id)).issues(
            config.team_id, config.states.queued, config.intake_label)
        for issue in issues:
            for attempt in range(2):
                try:
                    created, accepted = _admit(db, workspace_id, issue, config)
                    db.commit()
                    result["created"] += int(created)
                    result["accepted"] += int(accepted)
                    break
                except IntegrityError:
                    db.rollback()
                    if attempt:
                        raise
                except Exception:
                    db.rollback()
                    raise
    except (LinearError, ValidationError) as exc:
        db.rollback()
        _problem(db, workspace_id, exc.category if isinstance(exc, LinearError) else "configuration")
        db.commit()
    if result["accepted"]:
        notify(workspace_id, "signal", "sync")
    return result
