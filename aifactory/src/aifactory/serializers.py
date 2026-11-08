"""JSON views of the model. Secrets are never serialized."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from .models import Artifact, Decision, Message, Outcome, Plan, Repository, Run, Signal, Task, User, Workspace


def ts(value: datetime | None) -> str | None:
    return value.isoformat() + "Z" if value else None


def user(u: User) -> dict[str, Any]:
    return {"id": u.id, "email": u.email, "name": u.name}


def workspace(w: Workspace, role: str | None = None, org_name: str = "") -> dict[str, Any]:
    from .pipeline.runtime import merged_settings

    return {"id": w.id, "name": w.name, "slug": w.slug, "description": w.description, "org_id": w.org_id,
            "org_name": org_name, "role": role, "paused": w.paused, "settings": merged_settings(w.settings),
            "mindshub_workspace_id": w.mindshub_workspace_id, "created_at": ts(w.created_at)}


def repository(r: Repository) -> dict[str, Any]:
    return {"id": r.id, "full_name": r.full_name, "default_branch": r.default_branch, "description": r.description,
            "config": r.config or {}, "created_at": ts(r.created_at)}


def run(r: Run) -> dict[str, Any]:
    return {"id": r.id, "task_id": r.task_id, "outcome_id": r.outcome_id, "role": r.role, "harness": r.harness,
            "provider": r.provider, "model": r.model, "attempt": r.attempt, "status": r.status, "turns": r.turns,
            "input_tokens": r.input_tokens, "output_tokens": r.output_tokens, "cost_usd": r.cost_usd,
            "cost_known": r.cost_known, "error": r.error, "result": r.result, "started_at": ts(r.started_at),
            "finished_at": ts(r.finished_at)}


def task(t: Task, runs: list[Run] | None = None, repo_name: str = "") -> dict[str, Any]:
    runs = runs or []
    return {"id": t.id, "outcome_id": t.outcome_id, "key": t.key, "title": t.title, "spec": t.spec,
            "acceptance": t.acceptance, "repo_id": t.repo_id, "repo": repo_name, "depends_on": t.depends_on,
            "harness": t.harness, "verifier_harness": t.verifier_harness, "status": t.status, "stage": t.stage,
            "attempt": t.attempt, "branch": t.branch, "base_sha": t.base_sha, "head_sha": t.head_sha,
            "pr_number": t.pr_number, "pr_url": t.pr_url, "merge_sha": t.merge_sha, "summary": t.summary,
            "blocked_reason": t.blocked_reason, "evidence": t.evidence or {},
            "cost_usd": round(sum(r.cost_usd for r in runs), 4), "cost_known": all(r.cost_known for r in runs),
            "tokens": sum(r.input_tokens + r.output_tokens for r in runs),
            "created_at": ts(t.created_at), "started_at": ts(t.started_at), "finished_at": ts(t.finished_at),
            "updated_at": ts(t.updated_at)}


def plan(p: Plan) -> dict[str, Any]:
    return {"id": p.id, "version": p.version, "status": p.status, "summary": p.summary, "content": p.content,
            "approved_by": p.approved_by, "approved_at": ts(p.approved_at), "created_at": ts(p.created_at)}


def outcome(o: Outcome, tasks: list[Task] | None = None, runs: list[Run] | None = None,
            open_decisions: int = 0) -> dict[str, Any]:
    tasks = tasks if tasks is not None else []
    runs = runs or []
    done = sum(1 for t in tasks if t.status == "completed")
    return {"id": o.id, "number": o.number, "title": o.title, "brief": o.brief, "acceptance": o.acceptance,
            "repo_ids": o.repo_ids, "harness": o.harness, "status": o.status, "priority": o.priority,
            "source": o.source, "summary": o.summary, "blocked_reason": o.blocked_reason,
            "created_by": o.created_by, "created_at": ts(o.created_at), "updated_at": ts(o.updated_at),
            "completed_at": ts(o.completed_at), "progress": {"done": done, "total": len(tasks)},
            "cost_usd": round(sum(r.cost_usd for r in runs), 4), "cost_known": all(r.cost_known for r in runs),
            "open_decisions": open_decisions,
            "prs": [{"number": t.pr_number, "url": t.pr_url, "task": t.key} for t in tasks if t.pr_url],
            "track": [{"key": t.key, "status": t.status, "stage": t.stage,
                       "evidence": {k: {"state": (t.evidence or {}).get(k, {}).get("state")}
                                    for k in ("ci", "smoke") if (t.evidence or {}).get(k)}}
                      for t in sorted(tasks, key=lambda t: t.key)]}


def decision(d: Decision, context: dict[str, Any] | None = None) -> dict[str, Any]:
    return {"id": d.id, "kind": d.kind, "title": d.title, "body": d.body, "options": d.options, "status": d.status,
            "resolution": d.resolution, "comment": d.comment, "resolved_by": d.resolved_by,
            "resolved_at": ts(d.resolved_at), "reusable": d.reusable, "scope": d.scope,
            "outcome_id": d.outcome_id, "task_id": d.task_id, "created_at": ts(d.created_at), **(context or {})}


def message(m: Message, author: str = "") -> dict[str, Any]:
    return {"id": m.id, "outcome_id": m.outcome_id, "task_id": m.task_id, "author_kind": m.author_kind,
            "author": author, "body": m.body, "delivered": m.delivered, "created_at": ts(m.created_at)}


def artifact(a: Artifact, content: bool = False) -> dict[str, Any]:
    row = {"id": a.id, "kind": a.kind, "name": a.name, "task_id": a.task_id, "run_id": a.run_id, "data": a.data,
           "size": len(a.content or ""), "created_at": ts(a.created_at)}
    if content:
        row["content"] = a.content
    return row


def signal(s: Signal) -> dict[str, Any]:
    return {"id": s.id, "source": s.source, "title": s.title, "body": s.body, "url": s.url, "count": s.count,
            "status": s.status, "outcome_id": s.outcome_id, "repo_id": s.repo_id, "data": s.data or {},
            "first_seen_at": ts(s.first_seen_at), "last_seen_at": ts(s.last_seen_at)}
