"""Product signals → proposed work.

Signals (GitHub issues labelled for the Factory, CI failures on the default branch, failed
post-merge smoke/release checks) are deduplicated by fingerprint. A signal never starts work
by itself: it becomes a *proposed* outcome that a person accepts, unless the workspace has
explicitly allowed automatic acceptance for that source.
"""

from __future__ import annotations

import hashlib
import logging
from typing import Any

from sqlalchemy import update
from sqlalchemy.orm import Session

from .db import utcnow
from .events import notify, record
from .github import GitHub
from .models import Outcome, Repository, Signal, User, Workspace

log = logging.getLogger(__name__)


def outcome_from_text(db: Session, workspace_id: str, user: User | None, title: str, brief: str, repo_ids: list[str],
                      *, acceptance: list[str] | None = None, harness: str | None = None, source: str = "user",
                      signal_id: str | None = None, priority: int = 2, status: str = "planning",
                      guidance: dict | None = None) -> Outcome:
    number = db.execute(update(Workspace).where(Workspace.id == workspace_id).values(
        outcome_seq=Workspace.outcome_seq + 1).returning(Workspace.outcome_seq)).scalar_one()
    outcome = Outcome(workspace_id=workspace_id, number=number, title=title.strip()[:300], brief=brief.strip(),
                      acceptance=[a.strip() for a in (acceptance or []) if a.strip()], repo_ids=repo_ids,
                      harness=harness or None, source=source, signal_id=signal_id, priority=priority,
                      created_by=user.id if user else None, status=status)
    db.add(outcome)
    db.flush()
    record(workspace_id, "outcome_created", f"#{outcome.number} {outcome.title}", outcome_id=outcome.id,
           actor=user.id if user else "factory", data={"source": source, **({"guidance": guidance} if guidance else {})}, session=db)
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
    """Pull GitHub issues and failing default-branch CI runs as signals.

    Issues labelled ``factory`` become proposed signals. Issues labelled ``factory:auto``
    (pre-authorized work) become outcomes immediately and are acknowledged on the issue.
    """
    from .config import get_settings

    gh = GitHub(token)
    created = updated = accepted = 0
    for repo in db.query(Repository).filter(Repository.workspace_id == workspace_id):
        cfg = repo.config or {}
        label = cfg.get("signal_label", "factory")
        auto_label = cfg.get("auto_label", "factory:auto")
        seen: set[int] = set()
        for wanted, auto in ((auto_label, True), (label, False)):
            if not wanted:
                continue
            found = await gh.issues(repo.full_name, labels=wanted)
            log.info("signal sync %s: %d open issue(s) labelled %s", repo.full_name, len(found), wanted)
            for issue in found:
                if issue["number"] in seen:
                    continue
                seen.add(issue["number"])
                sig, new = upsert_signal(db, workspace_id, source="github_issue", title=issue["title"],
                                         body=issue.get("body") or "", url=issue["html_url"],
                                         external_id=f"{repo.full_name}#{issue['number']}", repo_id=repo.id)
                created += new
                updated += not new
                db.commit()  # never hold the database write lock across network calls
                if auto and sig.status == "new":
                    existing = (db.query(Outcome)
                                .filter(Outcome.workspace_id == workspace_id, Outcome.status != "cancelled",
                                        Outcome.brief.contains(issue["html_url"])).first())
                    if existing is not None:
                        # Repeated signals attach to the work already addressing them.
                        sig.status, sig.outcome_id = "accepted", existing.id
                        db.commit()
                        continue
                    author = (issue.get("user") or {}).get("login", "")
                    names = {(lbl.get("name") if isinstance(lbl, dict) else str(lbl)) for lbl in issue.get("labels") or []}
                    worker = next((n.split(":", 1)[1] for n in names if n in ("worker:native", "worker:codex", "worker:anton")), None)
                    outcome = outcome_from_text(
                        db, workspace_id, None, issue["title"],
                        f"{issue.get('body') or ''}\n\nSource: {issue['html_url']} (opened by {author}, labelled {auto_label})",
                        [repo.id], source="signal", signal_id=sig.id, harness=worker)
                    sig.status, sig.outcome_id = "accepted", outcome.id
                    accepted += 1
                    record(workspace_id, "signal_accepted",
                           f"Accepted {repo.full_name}#{issue['number']} automatically (label {auto_label})",
                           outcome_id=outcome.id, session=db, data={"url": issue["html_url"]})
                    db.commit()
                    base = get_settings().public_url
                    link = f"{base}/w/{workspace_id}/outcomes/{outcome.id}" if base.startswith("https") else ""
                    try:
                        await gh.comment(repo.full_name, issue["number"],
                                         f"AI Factory picked this up as outcome #{outcome.number}."
                                         + (f" Follow progress: {link}" if link else ""))
                    except Exception:
                        pass
        try:
            runs = await gh.request("GET", f"/repos/{repo.full_name}/actions/runs",
                                    params={"branch": repo.default_branch, "status": "failure", "per_page": 5})
        except Exception:
            runs = {}
        latest_ok = None
        try:
            ok_runs = await gh.request("GET", f"/repos/{repo.full_name}/actions/runs",
                                       params={"branch": repo.default_branch, "status": "success", "per_page": 1})
            latest_ok = (ok_runs.get("workflow_runs") or [{}])[0].get("created_at")
        except Exception:
            pass
        for run in (runs or {}).get("workflow_runs", [])[:3]:
            if latest_ok and run.get("created_at", "") < latest_ok:
                continue  # already fixed by a later green run
            _, new = upsert_signal(db, workspace_id, source="ci_failure",
                                   title=f"{run.get('name', 'CI')} failing on {repo.default_branch} in {repo.full_name}",
                                   body=f"Run {run.get('html_url')} failed at {run.get('head_sha', '')[:8]}.",
                                   url=run.get("html_url", ""), external_id=f"{repo.full_name}:ci:{run.get('name')}",
                                   repo_id=repo.id)
            created += new
            updated += not new
    db.commit()
    notify(workspace_id, "signal", "sync")
    return {"created": created, "updated": updated, "accepted": accepted}
