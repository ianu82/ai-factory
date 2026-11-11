"""Shared merge-policy and verified requester contracts for delivery and presentation.

``github`` is a prohibition on Factory merges, not an approval variant. Callers
performing a merge must read under the workspace/settings lock and retain that
serialization through the bounded external call.
"""
from __future__ import annotations

from sqlalchemy.orm import Session

from .models import Decision, Outcome, Repository, Signal, Task, User, Workspace

MERGE_POLICIES = ("auto", "approval", "github")


def effective_merge_policy(settings: dict | None, config: dict | None = None) -> str:
    """Repository mode overrides workspace autonomy; empty/missing inherits."""
    mode = ((config or {}).get("merge") or {}).get("mode")
    mode = mode or ((settings or {}).get("autonomy") or {}).get("merge", "auto")
    # Unknown stored policy must never accidentally authorize a merge.
    return mode if mode in MERGE_POLICIES else "approval"


def review_requester(db: Session, outcome: Outcome) -> dict | None:
    """Identity is provider-verified, never inferred from brief prose or a bot token.

    An accepted issue still belongs to its issue author, not the accepting admin.
    Signal.data.github_requester and User.github_id/github_login share this shape.
    """
    if outcome.signal_id:
        signal = db.get(Signal, outcome.signal_id)
        if signal and signal.source == "github_issue":
            return github_identity((signal.data or {}).get("github_requester"))
    user = db.get(User, outcome.created_by) if outcome.created_by else None
    return github_identity({"id": user.github_id, "login": user.github_login}) if user else None


def github_identity(raw: dict | None) -> dict | None:
    import re

    if not isinstance(raw, dict):
        return None
    ident, login = raw.get("id"), raw.get("login")
    if (not isinstance(ident, (str, int)) or isinstance(ident, bool)
            or not str(ident).isdigit() or int(ident) <= 0
            or not isinstance(login, str) or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9-]{0,38}(?:\[bot\])?", login)):
        return None
    return {"id": str(ident), "login": login}


def reroute_policy_waits(db: Session, workspace_id: str, repo_id: str | None = None) -> int:
    """Supersede obsolete policy gates and resume at their durable merge stage.

    Used by all settings writers (including learning presets) and reconciliation.
    Approval grants are not synthesized: the runner rechecks live policy itself.
    """
    from .pipeline.runtime import merged_settings

    ws = db.get(Workspace, workspace_id)
    settings = merged_settings(ws.settings)
    count = 0
    query = db.query(Decision, Task).join(Task, Decision.task_id == Task.id).filter(
        Task.workspace_id == workspace_id, Decision.status == "open",
        Decision.kind.in_(("merge_on_github", "approve_merge", "approve_revert")))
    if repo_id:
        query = query.filter(Task.repo_id == repo_id)
    for decision, task in query:
        if task.status not in ("awaiting_approval", "smoke_failed"):
            continue
        repo = db.get(Repository, task.repo_id)
        policy = effective_merge_policy(settings, repo.config)
        is_revert = decision.kind == "approve_revert"
        # A release approval/learning gate remains meaningful in automatic mode.
        from .onboarding_policy import active
        needs_approval = (active(settings) or policy == "approval" or
                          (((repo.config or {}).get("release") or {}).get("mode") == "on_merge"
                           and settings["autonomy"].get("release") == "approval"))
        obsolete = (policy != "github" if decision.kind == "merge_on_github" else
                    policy == "github" if not is_revert else False)
        if policy == "auto" and not needs_approval:
            obsolete = True
        # Revert gates also change from human-only to Factory approval or auto.
        if is_revert and (("revert_merged" in {o.get("value") for o in decision.options or []}) != (policy == "github")):
            obsolete = True
        if not obsolete:
            continue
        decision.status, decision.comment = "superseded", "Merge policy changed; applying the current policy."
        task.status, task.stage, task.blocked_reason = "queued", "revert_merge" if is_revert else "merge", ""
        count += 1
    return count
