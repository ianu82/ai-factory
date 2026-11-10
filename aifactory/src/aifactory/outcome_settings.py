"""Current outcome policy and its determining sources.

Agent sessions own immutable snapshots; approval gates must resolve again at the
point of action. The scheduler deliberately only batch-loads priorities instead.
"""
from __future__ import annotations

from copy import deepcopy
from typing import Any

from sqlalchemy.orm import Session

from .models import Outcome, Repository, Workspace, Workstream
from .pipeline.runtime import merged_settings


def resolve_settings(db: Session, outcome: Outcome, *, repo_id: str | None = None) -> dict[str, Any]:
    """Resolve planning across selected repos, and merging for one repo when supplied.

    Only same-workspace references apply, even if legacy/corrupt JSON references
    another workspace. Sources list every tied determining approval, not just the
    first veto. Absent overrides inherit; zero priority is a real override.
    """
    workspace = db.get(Workspace, outcome.workspace_id)
    raw = (workspace.settings if workspace else None) or {}
    effective = deepcopy(merged_settings(raw))
    stream = (db.query(Workstream).filter_by(id=outcome.workstream_id, workspace_id=outcome.workspace_id).first()
              if outcome.workstream_id else None)
    selected = set(outcome.repo_ids or [])
    repo_ids = selected | ({repo_id} if repo_id else set())
    repos = db.query(Repository).filter(Repository.workspace_id == outcome.workspace_id,
                                       Repository.id.in_(repo_ids)).order_by(Repository.id).all() if repo_ids else []

    def source(scope, id, field, value):
        return {"scope": scope, "id": id, "field": field, "value": value}

    def workspace_source(field, value, explicit):
        return source("workspace" if explicit else "default", outcome.workspace_id if explicit else None, field, value)

    sources: dict[str, Any] = {"harness": {}, "budgets": {}, "autonomy": {}}
    for role in ("planner", "builder", "verifier"):
        value = effective["harness"][role]
        origin = workspace_source(f"harness.{role}", value, role in (raw.get("harness") or {}))
        if stream and stream.worker_override:
            value = stream.worker_override
            origin = source("workstream", stream.id, "worker_override", value)
        if outcome.harness:
            value = outcome.harness
            origin = source("outcome", outcome.id, "harness", value)
        effective["harness"][role] = value
        sources["harness"][role] = [origin]

    priority = outcome.priority if outcome.priority is not None else 2
    origin = source("outcome", outcome.id, "priority", priority)
    if stream and stream.priority_override is not None:
        priority = stream.priority_override
        origin = source("workstream", stream.id, "priority_override", priority)
    effective["priority"] = priority
    sources["priority"] = [origin]
    budget = effective["budgets"]["outcome_usd"]
    origin = workspace_source("budgets.outcome_usd", budget, "outcome_usd" in (raw.get("budgets") or {}))
    if stream and stream.outcome_budget_override is not None:
        budget = stream.outcome_budget_override
        origin = source("workstream", stream.id, "outcome_budget_override", budget)
    effective["budgets"]["outcome_usd"] = budget
    sources["budgets"]["outcome_usd"] = [origin]

    for kind in ("plan", "merge"):
        value = effective["autonomy"][kind]
        policies = [workspace_source(f"autonomy.{kind}", value, kind in (raw.get("autonomy") or {}))]
        override = getattr(stream, f"{kind}_approval_override", None)
        if override is not None:
            policies.append(source("workstream", stream.id, f"{kind}_approval_override", override))
        applicable = [r for r in repos if r.id in selected] if kind == "plan" or repo_id is None else [
            r for r in repos if r.id == repo_id]
        for repo in applicable:
            config = repo.config or {}
            mode = (config.get(kind) or {}).get("mode")
            if mode:
                policies.append(source("repository", repo.id, f"{kind}.mode", mode))
            if (kind == "merge" and (config.get("release") or {}).get("mode") == "on_merge"
                    and effective["autonomy"]["release"] == "approval"):
                # Both halves determine this existing gate: workspace release policy and repository deployment.
                policies.append(source("workspace", outcome.workspace_id, "autonomy.release", "approval"))
                policies.append(source("repository", repo.id, "release.mode", "on_merge"))
        approvals = [p for p in policies if p["value"] != "auto"]
        effective["autonomy"][kind] = "approval" if approvals else "auto"
        sources["autonomy"][kind] = approvals or policies
    return {"effective": effective, "sources": sources}
