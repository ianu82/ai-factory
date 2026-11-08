"""Product signals → proposed work.

Signals (GitHub issues labelled for the Factory, CI failures on the default branch, failed
post-merge smoke/release checks) are deduplicated by fingerprint. A signal never starts work
by itself: it becomes a *proposed* outcome that a person accepts, unless the workspace has
explicitly allowed automatic acceptance for that source.
"""

from __future__ import annotations

import hashlib
from typing import Any

from sqlalchemy.orm import Session

from .db import utcnow
from .events import notify, record
from .github import GitHub
from .models import Outcome, Repository, Signal, User, Workspace


def outcome_from_text(db: Session, workspace_id: str, user: User | None, title: str, brief: str, repo_ids: list[str],
                      *, acceptance: list[str] | None = None, harness: str | None = None, source: str = "user",
                      signal_id: str | None = None, priority: int = 2, status: str = "planning") -> Outcome:
    ws = db.get(Workspace, workspace_id)
    ws.outcome_seq = (ws.outcome_seq or 0) + 1
    outcome = Outcome(workspace_id=workspace_id, number=ws.outcome_seq, title=title.strip()[:300], brief=brief.strip(),
                      acceptance=[a.strip() for a in (acceptance or []) if a.strip()], repo_ids=repo_ids,
                      harness=harness or None, source=source, signal_id=signal_id, priority=priority,
                      created_by=user.id if user else None, status=status)
    db.add(outcome)
    db.flush()
    record(workspace_id, "outcome_created", f"#{outcome.number} {outcome.title}", outcome_id=outcome.id,
           actor=user.id if user else "factory", data={"source": source}, session=db)
    notify(workspace_id, "outcome", outcome.id)
    return outcome


def fingerprint(*parts: str) -> str:
    return hashlib.sha256("\x1f".join(parts).encode()).hexdigest()[:40]


def upsert_signal(db: Session, workspace_id: str, *, source: str, title: str, body: str, url: str,
                  external_id: str, repo_id: str | None) -> tuple[Signal, bool]:
    fp = fingerprint(source, external_id or title)
    row = db.query(Signal).filter(Signal.workspace_id == workspace_id, Signal.fingerprint == fp).one_or_none()
    if row:
        row.count += 1
        row.last_seen_at = utcnow()
        row.body = body[:8000] or row.body
        return row, False
    row = Signal(workspace_id=workspace_id, repo_id=repo_id, source=source, external_id=external_id, fingerprint=fp,
                 title=title[:300], body=body[:8000], url=url)
    db.add(row)
    db.flush()
    return row, True


async def sync_github(db: Session, workspace_id: str, token: str) -> dict[str, Any]:
    """Pull open issues labelled `factory` and recent failing default-branch CI runs as signals."""
    gh = GitHub(token)
    created = updated = 0
    for repo in db.query(Repository).filter(Repository.workspace_id == workspace_id):
        label = (repo.config or {}).get("signal_label", "factory")
        for issue in await gh.issues(repo.full_name, labels=label):
            _, new = upsert_signal(db, workspace_id, source="github_issue", title=issue["title"],
                                   body=issue.get("body") or "", url=issue["html_url"],
                                   external_id=f"{repo.full_name}#{issue['number']}", repo_id=repo.id)
            created += new
            updated += not new
        try:
            runs = await gh.request("GET", f"/repos/{repo.full_name}/actions/runs",
                                    params={"branch": repo.default_branch, "status": "failure", "per_page": 5})
        except Exception:
            runs = {}
        for run in (runs or {}).get("workflow_runs", [])[:3]:
            _, new = upsert_signal(db, workspace_id, source="ci_failure",
                                   title=f"{run.get('name', 'CI')} failing on {repo.default_branch} in {repo.full_name}",
                                   body=f"Run {run.get('html_url')} failed at {run.get('head_sha', '')[:8]}.",
                                   url=run.get("html_url", ""), external_id=f"{repo.full_name}:ci:{run.get('name')}",
                                   repo_id=repo.id)
            created += new
            updated += not new
    db.commit()
    notify(workspace_id, "signal", "sync")
    return {"created": created, "updated": updated}
