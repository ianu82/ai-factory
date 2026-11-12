"""Outcome policy resolution. Overrides replace values; approval requirements accumulate.

Callers performing an automatic approval must resolve again under the workspace
policy lock. This module never changes outcomes, tasks, or active sessions.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from sqlalchemy.orm import Session

from .models import Outcome, Repository, Workspace, Workstream
from .pipeline.runtime import merged_settings


def effective_priority(outcome: Outcome, stream: Workstream | None) -> int:
    return (stream.priority_override if stream is not None and stream.priority_override is not None
            else outcome.priority if outcome.priority is not None else 2)


def approval_policy(sources: list[dict[str, Any]]) -> tuple[str, list[dict[str, Any]]]:
    """Unknown persisted policies fail closed, too. Keep every tied determining source."""
    requiring = [source for source in sources if source["value"] != "auto"]
    return ("approval", requiring) if requiring else ("auto", sources)


@dataclass
class OutcomeSettings:
    settings: dict[str, Any]
    effective: dict[str, Any]
    sources: dict[str, list[dict[str, Any]]]

    def as_dict(self) -> dict[str, Any]:
        # Do not expose unrelated workspace configuration through outcome detail.
        return {"effective": self.effective, "sources": self.sources}


def resolve_settings(db: Session, outcome: Outcome, *, repo_id: str | None = None) -> OutcomeSettings:
    """Plan policy covers all selected repositories; task merge policy covers its own.

    References are always workspace scoped, even for legacy or corrupt rows.
    populate_existing makes this safe after acquiring the policy lock when the
    caller's session previously read these rows.
    """
    workspace = db.query(Workspace).filter_by(id=outcome.workspace_id).populate_existing().one()
    settings = merged_settings(workspace.settings)
    stream = (db.query(Workstream).filter_by(id=outcome.workstream_id, workspace_id=outcome.workspace_id)
              .populate_existing().one_or_none()) if outcome.workstream_id else None
    ids = set(outcome.repo_ids or []) | ({repo_id} if repo_id else set())
    repos = (db.query(Repository).filter(Repository.workspace_id == outcome.workspace_id, Repository.id.in_(ids))
             .populate_existing().all()) if ids else []

    def source(kind, identity, field, value):
        return {"kind": kind, "id": identity, "field": field, "value": value}

    effective: dict[str, Any] = {}
    sources: dict[str, list[dict[str, Any]]] = {}
    for role in ("planner", "builder", "verifier"):
        value = settings["harness"].get(role) or "native"
        origin = source("workspace", workspace.id, f"harness.{role}", value)
        if stream is not None and stream.worker_override:
            value = stream.worker_override
            origin = source("workstream", stream.id, "worker_override", value)
        if outcome.harness:
            value = outcome.harness
            origin = source("outcome", outcome.id, "harness", value)
        effective[role] = value
        sources[role] = [origin]
    settings["harness"] = {**settings["harness"],
                           **{role: effective[role] for role in ("planner", "builder", "verifier")}}
    effective["priority"] = effective_priority(outcome, stream)
    sources["priority"] = [source("workstream", stream.id, "priority_override", effective["priority"])
                           if stream is not None and stream.priority_override is not None else
                           source("outcome", outcome.id, "priority", effective["priority"])]
    budget = settings["budgets"].get("outcome_usd")
    origin = source("workspace", workspace.id, "budgets.outcome_usd", budget)
    if stream is not None and stream.outcome_budget_override is not None:
        budget = stream.outcome_budget_override
        origin = source("workstream", stream.id, "outcome_budget_override", budget)
    effective["outcome_usd"], sources["outcome_usd"] = budget, [origin]
    settings["budgets"] = {**settings["budgets"], "outcome_usd": budget}

    for policy in ("plan", "merge"):
        candidates = [source("workspace", workspace.id, f"autonomy.{policy}", settings["autonomy"].get(policy))]
        override = getattr(stream, f"{policy}_approval_override", None)
        if override is not None:
            candidates.append(source("workstream", stream.id, f"{policy}_approval_override", override))
        applicable = ([r for r in repos if r.id in (outcome.repo_ids or [])]
                      if policy == "plan" or repo_id is None else [r for r in repos if r.id == repo_id])
        for repo in sorted(applicable, key=lambda r: r.id):
            cfg = repo.config or {}
            mode = (cfg.get(policy) or {}).get("mode")
            if mode is not None:
                candidates.append(source("repository", repo.id, f"{policy}.mode", mode))
            if (policy == "merge" and (cfg.get("release") or {}).get("mode") == "on_merge"
                    and settings["autonomy"].get("release") != "auto"):
                candidates.append(source("repository", repo.id, "release.mode", "approval")
                                  | {"gate": "release_on_merge", "release_policy": settings["autonomy"].get("release"),
                                     "release_policy_source": workspace.id})
        value, determining = approval_policy(candidates)
        effective[policy], sources[policy] = value, determining
        # Keep all applicable policy inputs as well as the determining ties.
        sources[f"{policy}_inputs"] = candidates
        settings["autonomy"][policy] = value
    return OutcomeSettings(settings, effective, sources)


def current_settings(outcome_id: str, *, repo_id: str | None = None) -> OutcomeSettings:
    from .db import session_scope

    with session_scope() as db:
        return resolve_settings(db, db.get(Outcome, outcome_id), repo_id=repo_id)
