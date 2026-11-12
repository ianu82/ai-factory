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
from .models import Event, Outcome, Repository, Signal, User, Workspace

log = logging.getLogger(__name__)


def outcome_from_text(db: Session, workspace_id: str, user: User | None, title: str, brief: str, repo_ids: list[str],
                      *, acceptance: list[str] | None = None, harness: str | None = None, source: str = "user",
                      signal_id: str | None = None, priority: int = 2, status: str = "planning",
                      guidance: dict | None = None, action_id: str | None = None,
                      pending_events: list[dict] | None = None) -> Outcome:
    """With pending_events (a confirmed Mind card), the creation event waits there until the caller has committed;
    with action_id it is attributed "<person> via Mind"."""
    number = db.execute(update(Workspace).where(Workspace.id == workspace_id).values(
        outcome_seq=Workspace.outcome_seq + 1).returning(Workspace.outcome_seq)).scalar_one()
    outcome = Outcome(workspace_id=workspace_id, number=number, title=title.strip()[:300], brief=brief.strip(),
                      acceptance=[a.strip() for a in (acceptance or []) if a.strip()], repo_ids=repo_ids,
                      harness=harness or None, source=source, signal_id=signal_id, priority=priority,
                      created_by=user.id if user else None, status=status)
    db.add(outcome)
    db.flush()
    message = f"#{outcome.number} {outcome.title}"
    if action_id and user:
        message = f"{user.name or user.email} via Mind created {message}"
    payload = record(workspace_id, "outcome_created", message, outcome_id=outcome.id,
                     actor=user.id if user else "factory",
                     data={"source": source, **({"guidance": guidance} if guidance else {}),
                           **({"source": "mind", "action_id": action_id} if action_id else {})},
                     session=db, publish=pending_events is None)
    if pending_events is None:
        notify(workspace_id, "outcome", outcome.id)
    else:
        pending_events.append(payload)
    return outcome




TRIAGE = ("accept", "ignore")


def triage(db: Session, sig: Signal, user: User, action: str, *, title: str | None = None,
           brief: str | None = None) -> Signal:
    """Accept a signal as work or ignore it, as the Signals page does: accepting creates one outcome, never a second.
    Linear issues (Linear intake owns them) and improvement proposals (they change settings) have their own paths.
    The caller commits, then wakes the orchestrator when an outcome was created."""
    if action not in TRIAGE:
        raise ValueError("Unknown action")
    sig.data = {key: value for key, value in (sig.data or {}).items() if key != "acceptance_reason"}
    if action == "ignore":
        sig.status = "ignored"
        return sig
    if sig.outcome_id:
        return sig  # accepting again must not create duplicate work
    from .connectors.core import signal_priority

    worker = (sig.data or {}).get("worker") if sig.source == "github_issue" else None
    if worker not in ("native", "codex", "anton"):
        worker = None
    repo_ids = [sig.repo_id] if sig.repo_id else [r.id for r in db.query(Repository).filter(
        Repository.workspace_id == sig.workspace_id).limit(1)]
    outcome = outcome_from_text(db, sig.workspace_id, user, title or sig.title,
                                (brief or sig.body or "") + (f"\n\nSource: {sig.url}" if sig.url else ""),
                                repo_ids, source="signal", signal_id=sig.id, harness=worker,
                                priority=signal_priority(sig))
    sig.status, sig.outcome_id = "accepted", outcome.id
    return sig
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
    (pre-authorized work) become outcomes when the author has current write/admin access,
    unless the repository explicitly opts out of that trust rule.
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
                    if (sig.data or {}).get("acceptance_reason"):
                        db.commit()
                        continue  # a held issue always needs a person, even after permissions change
                    author = (issue.get("user") or {}).get("login", "")
                    names = {(lbl.get("name") if isinstance(lbl, dict) else str(lbl)) for lbl in issue.get("labels") or []}
                    worker = next((n.split(":", 1)[1] for n in names if n in ("worker:native", "worker:codex", "worker:anton")), None)
                    # Persist the worker before holding a proposal so human acceptance
                    # uses the same worker as automatic acceptance would have used.
                    sig.data = {**(sig.data or {}), "worker": worker}
                    if cfg.get("require_trusted_issue_author") is not False:
                        reason = ""
                        if not author:
                            reason = "Acceptance needed: the issue author is missing."
                        else:
                            try:
                                permission = await gh.repository_author_permission(repo.full_name, author)
                                if permission not in ("write", "admin", "push"):
                                    reason = "Acceptance needed: the issue author does not have verified write or admin access."
                            except Exception:
                                # Never persist provider messages: they can contain credentials.
                                reason = "Acceptance needed: the issue author's repository permission could not be verified."
                        if reason:
                            sig.data = {**sig.data, "acceptance_reason": reason}
                            db.commit()
                            continue
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
                                         f"MindsHub AI Factory picked this up as outcome #{outcome.number}."
                                         + (f" Follow progress: {link}" if link else ""))
                    except Exception:
                        pass
        try:
            runs = await gh.request("GET", f"/repos/{repo.full_name}/actions/runs",
                                    params={"branch": repo.default_branch, "status": "failure", "per_page": 5})
        except Exception:
            runs = {}
        green_runs = []
        try:
            ok_runs = await gh.request("GET", f"/repos/{repo.full_name}/actions/runs",
                                       params={"branch": repo.default_branch, "status": "success", "per_page": 100})
            green_runs = (ok_runs.get("workflow_runs") or [])[:100]
        except Exception:
            pass
        for green in green_runs:
            # Match the workflow, not merely another workflow's successful run.
            # Legacy signals use the workflow name as their external identity.
            if not green.get("name") or not green.get("created_at"):
                continue
            from .supervisor import timestamp

            green_at = timestamp(green["created_at"])
            if green_at is None:
                continue
            observed_at = green_at.replace(tzinfo=None)
            known = db.query(Event).filter(Event.workspace_id == workspace_id,
                Event.kind == "ci_status", Event.task_id.is_(None), Event.created_at == observed_at)
            if not any((e.data or {}).get("repo_id") == repo.id
                       and (e.data or {}).get("workflow") == green["name"] for e in known):
                payload = record(workspace_id, "ci_status", f"{green['name']} green on {repo.default_branch}",
                    data={"repo_id": repo.id, "branch": repo.default_branch,
                          "workflow": green["name"], "state": "success"}, session=db, publish=False)
                db.get(Event, payload["id"]).created_at = observed_at
            for signal in db.query(Signal).filter(Signal.workspace_id == workspace_id,
                    Signal.repo_id == repo.id, Signal.source == "ci_failure",
                    Signal.external_id == f"{repo.full_name}:ci:{green['name']}",
                    Signal.status.in_(["new", "proposed", "accepted"])):
                # Compare against the newest known failure, including every
                # fetched run: this workflow may be beyond the upsert limit
                # below. Persist that evidence so Supervisor also sees the red
                # run rather than mistaking this success for recovery.
                failure_times = [timestamp((signal.data or {}).get("failure_at"))]
                failure_times += [timestamp(r.get("created_at")) for r in (runs or {}).get("workflow_runs", [])
                                  if r.get("name") == green["name"]]
                failure_times = [at for at in failure_times if at is not None]
                failed_at = max(failure_times) if failure_times else timestamp(signal.last_seen_at)
                if failure_times:
                    signal.data = {**(signal.data or {}), "failure_at": failed_at.isoformat()}
                if failed_at is not None and green_at > failed_at:
                    signal.status = "resolved"
        seen_workflows = set()
        for run in (runs or {}).get("workflow_runs", [])[:3]:
            # GitHub returns newest first. An older failure in the same poll
            # must not overwrite the incident time used to establish recovery.
            workflow = run.get("name")
            if workflow in seen_workflows:
                continue
            seen_workflows.add(workflow)
            if any(g.get("name") == run.get("name") and g.get("created_at", "") > run.get("created_at", "")
                   for g in green_runs):
                continue
            signal, new = upsert_signal(db, workspace_id, source="ci_failure",
                                   title=f"{run.get('name', 'CI')} failing on {repo.default_branch} in {repo.full_name}",
                                   body=f"Run {run.get('html_url')} failed at {run.get('head_sha', '')[:8]}.",
                                   url=run.get("html_url", ""), external_id=f"{repo.full_name}:ci:{run.get('name')}",
                                   repo_id=repo.id)
            signal.data = {**(signal.data or {}), "branch": repo.default_branch,
                           "workflow": run.get("name"), "failure_at": run.get("created_at")}
            if signal.status == "resolved":
                signal.status = "new"
                signal.first_seen_at = utcnow()
            created += new
            updated += not new
    db.commit()
    notify(workspace_id, "signal", "sync")
    return {"created": created, "updated": updated, "accepted": accepted}
