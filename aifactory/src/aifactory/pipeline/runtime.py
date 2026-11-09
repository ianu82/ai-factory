"""Shared helpers for running agent sessions inside the pipeline."""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from typing import Any

from ..config import get_settings
from ..db import session_scope, utcnow
from ..events import notify, record
from ..gateway import GatewayGrant, issue, revoke
from ..harness import HarnessResult, HarnessSession, get_harness
from ..harness.schemas import SCHEMAS
from ..inference import ModelClient, UsageLedger, resolve_provider, workspace_models
from ..models import Decision, Message, Outcome, Run, ScorerResult, Task, Workspace
from ..sandbox import LocalSandbox
from .attachments import prepare_attachments

DEFAULT_SETTINGS: dict[str, Any] = {
    "provider": "mindshub",
    "models": {"default": "gpt-terra"},
    "harness": {"builder": "native", "verifier": "native", "planner": "native"},
    "concurrency": 3,
    "max_attempts": 3,
    # When attempts run out on concrete feedback (verifier findings, failed checks, policy), try once more with a
    # fresh budget before asking a person: relaying findings into a retry is not a decision.
    "auto_retry_rounds": 1,
    "autonomy": {"plan": "auto", "merge": "auto", "release": "approval"},
    "budgets": {"run_usd": 8.0, "outcome_usd": 40.0, "run_tokens": 6_000_000},
    "limits": {"builder_turns": 120, "verifier_turns": 60, "planner_turns": 60, "run_minutes": 60,
               "ci_minutes": 45, "max_changed_lines": 1500},
    "reasoning_effort": "medium",
}


def merged_settings(raw: dict[str, Any] | None) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for key, default in DEFAULT_SETTINGS.items():
        value = (raw or {}).get(key)
        if isinstance(default, dict):
            out[key] = {**default, **(value or {})}
        else:
            out[key] = value if value is not None else default
    for key, value in (raw or {}).items():
        out.setdefault(key, value)
    return out


def workspace_settings(workspace_id: str) -> dict[str, Any]:
    with session_scope() as s:
        ws = s.get(Workspace, workspace_id)
        return merged_settings(ws.settings if ws else {})


def guidance_history(outcome_id: str, task_id: str | None = None) -> list[str]:
    """All team guidance so far for an outcome (and this task), for inclusion in new prompts."""
    with session_scope() as s:
        rows = (s.query(Message).filter(Message.outcome_id == outcome_id, Message.author_kind == "user")
                .order_by(Message.created_at).all())
        return [m.body for m in rows if m.task_id in (None, task_id)]


def take_guidance(outcome_id: str, task_id: str | None = None, since=None):
    """Yield guidance posted while a session runs, once per session (every task's agent hears it)."""
    started = since or utcnow()
    seen: set[str] = set()

    def take() -> list[str]:
        with session_scope() as s:
            rows = [m for m in s.query(Message).filter(Message.outcome_id == outcome_id, Message.author_kind == "user",
                                                        Message.created_at >= started).order_by(Message.created_at)
                    if m.task_id in (None, task_id) and m.id not in seen]
            for m in rows:
                seen.add(m.id)
                m.delivered = True
            return [m.body for m in rows]

    return take


def precedents(workspace_id: str, repo_id: str | None = None, limit: int = 12) -> list[Decision]:
    """Reusable, attributed human decisions that apply to this workspace (and repository)."""
    with session_scope() as s:
        query = (s.query(Decision)
                 .filter(Decision.workspace_id == workspace_id, Decision.status == "resolved",
                         Decision.reusable.is_(True), Decision.scope.in_(("workspace", "repository")))
                 .order_by(Decision.resolved_at.desc(), Decision.id))

        def applicable(rows: list[Decision]) -> list[Decision]:
            """A repository-scoped decision applies to the repository it was made about (its task's, else its
            outcome's). Repositories are looked up once per page, not once per decision."""
            if repo_id is None:
                return rows
            scoped = [d for d in rows if d.scope == "repository"]
            task_ids = {d.task_id for d in scoped if d.task_id}
            outcome_ids = {d.outcome_id for d in scoped if not d.task_id and d.outcome_id}
            task_repo = dict(s.query(Task.id, Task.repo_id).filter(Task.id.in_(task_ids)).all()) if task_ids else {}
            outcome_repos = ({oid: set(repos or []) for oid, repos in
                              s.query(Outcome.id, Outcome.repo_ids).filter(Outcome.id.in_(outcome_ids)).all()}
                             if outcome_ids else {})
            return [d for d in rows if d.scope == "workspace"
                    or (d.task_id and task_repo.get(d.task_id) == repo_id)
                    or (not d.task_id and repo_id in outcome_repos.get(d.outcome_id, set()))]

        # Without a repository (planning across an outcome's repositories) every decision applies. Page until
        # enough apply: other repositories' decisions must not crowd out older applicable ones.
        found: list[Decision] = []
        offset, page = 0, max(limit * 4, 20)
        while len(found) < limit:
            rows = query.offset(offset).limit(page).all()
            found += applicable(rows)
            if len(rows) < page:
                break
            offset += page
        return found[:limit]


@dataclass
class SessionSpec:
    workspace_id: str
    outcome_id: str
    task_id: str | None
    role: str
    harness: str
    prompt: str
    system: str
    sandbox: LocalSandbox
    attempt: int = 1
    writable: bool = False


async def run_agent(spec: SessionSpec, cancelled: asyncio.Event) -> tuple[Run, HarnessResult | None, str]:
    """Run one harness session as a recorded Run. Returns (run, result_or_None, error)."""
    settings = workspace_settings(spec.workspace_id)
    models = workspace_models(settings)
    model = {"planner": models.planner, "builder": models.builder, "verifier": models.verifier}[spec.role]
    provider = resolve_provider(spec.workspace_id)
    model = provider.model_override or model
    limits = settings["limits"]
    budgets = settings["budgets"]
    with session_scope() as s:
        run = Run(workspace_id=spec.workspace_id, outcome_id=spec.outcome_id, task_id=spec.task_id, role=spec.role,
                  harness=spec.harness, provider=provider.provider, model=model, attempt=spec.attempt)
        s.add(run)
        s.flush()
        run_id = run.id
    ledger = UsageLedger(run_id=run_id, workspace_settings=settings, max_cost_usd=budgets.get("run_usd"),
                         max_tokens=budgets.get("run_tokens"))
    client = ModelClient(provider, session_id=run_id)
    turns = limits.get(f"{spec.role}_turns", 80)
    harness = get_harness(spec.harness)
    gateway_token = ""
    result: HarnessResult | None = None
    error = ""
    status = "succeeded"
    try:
        if spec.harness == "codex":
            gateway_token = issue(GatewayGrant(run_id=run_id, workspace_id=spec.workspace_id, model=model,
                                               provider=provider, ledger=ledger))
        record(spec.workspace_id, "run_started", f"{harness.label} {spec.role} started on {model}",
               outcome_id=spec.outcome_id, task_id=spec.task_id, run_id=run_id,
               data={"role": spec.role, "harness": spec.harness, "model": model, "provider": provider.provider,
                     "attempt": spec.attempt})
        notify(spec.workspace_id, "run", run_id)

        last_progress = {"t": 0.0}

        def on_progress(kind: str, message: str, data: dict[str, Any]) -> None:
            # Commands, edits and guidance are always journaled; chatter is rate-limited.
            now = time.monotonic()
            if kind in ("thought", "tool") and now - last_progress["t"] < 4:
                return
            last_progress["t"] = now
            record(spec.workspace_id, f"agent_{kind}", message, outcome_id=spec.outcome_id, task_id=spec.task_id,
                   run_id=run_id, data={"role": spec.role, **data})

        session = HarnessSession(
            run_id=run_id, role=spec.role, system=spec.system, prompt=spec.prompt, sandbox=spec.sandbox, model=model,
            client=client, ledger=ledger, report_schema=SCHEMAS[spec.role], writable=spec.writable, max_turns=turns,
            deadline=time.monotonic() + limits.get("run_minutes", 60) * 60, cancelled=cancelled,
            on_progress=on_progress, take_guidance=take_guidance(spec.outcome_id, spec.task_id),
            # A VM reaches the Factory only over its public URL; local sandboxes use the internal one.
            gateway_url=f"{get_settings().public_url if getattr(spec.sandbox, 'kind', 'local') == 'vm' else get_settings().internal_url}/gateway/v1",
            gateway_token=gateway_token,
            reasoning_effort=settings.get("reasoning_effort"),
        )
        context, session.image_inputs = await prepare_attachments(spec.sandbox, spec.workspace_id, spec.outcome_id)
        session.prompt += context
        result = await harness.run(session)
    except asyncio.CancelledError:
        status, error = "cancelled", "Cancelled"
        raise
    except Exception as exc:  # harness failures are recorded, never hidden
        status, error = "failed", str(exc) or type(exc).__name__
    finally:
        if gateway_token:
            revoke(run_id)
        measured = await client.session_cost(run_id) if provider.provider == "mindshub" else None
        with session_scope() as s:
            row = s.get(Run, run_id)
            row.status = status
            row.finished_at = utcnow()
            row.error = error[:4000]
            row.turns = result.turns if result else ledger.calls
            row.input_tokens, row.output_tokens = ledger.input_tokens, ledger.output_tokens
            if measured is not None:
                row.cost_usd, row.cost_known = round(measured, 6), True
            else:
                row.cost_usd, row.cost_known = round(ledger.cost_usd, 6), ledger.cost_known and ledger.calls > 0
            if result:
                row.result = result.report
            # Reserve in the same transaction as completion: a restart cannot lose sampled work.
            try:
                from ..scorers import reserve_automatic
                with s.begin_nested():
                    reserve_automatic(s, row)
            except Exception:
                import logging
                logging.getLogger(__name__).exception("could not reserve automatic scorers")
            s.flush()
            run_snapshot = Run(**{c.name: getattr(row, c.name) for c in Run.__table__.columns})
        level = "success" if status == "succeeded" else ("warn" if status == "cancelled" else "error")
        tokens = ledger.input_tokens + ledger.output_tokens
        spend = (f"${run_snapshot.cost_usd:.2f}" if run_snapshot.cost_known else "cost unknown")
        record(spec.workspace_id, "run_finished",
               f"{harness.label} {spec.role} {status} · {tokens:,} tokens · {spend}" + (f": {error[:300]}" if error else ""),
               outcome_id=spec.outcome_id, task_id=spec.task_id, run_id=run_id, level=level,
               data={"status": status, "tokens": ledger.input_tokens + ledger.output_tokens,
                     "cost_usd": run_snapshot.cost_usd, "cost_known": run_snapshot.cost_known})
        notify(spec.workspace_id, "run", run_id)
    return run_snapshot, result, error


def outcome_cost(outcome_id: str) -> tuple[float, bool]:
    from sqlalchemy import func, select

    with session_scope() as s:
        # Budget checks need only numeric totals, not the unbounded run/score
        # history (including large result, definition and reasoning payloads).
        run_cost, run_unknown = s.query(
            func.coalesce(func.sum(Run.cost_usd), 0),
            func.count(Run.id).filter(Run.cost_known.is_(False)),
        ).filter(Run.outcome_id == outcome_id).one()
        score_cost, score_unknown = s.query(
            func.coalesce(func.sum(ScorerResult.cost_usd), 0),
            func.count(ScorerResult.id).filter(ScorerResult.cost_known.is_(False)),
        ).filter(ScorerResult.run_id.in_(select(Run.id).where(Run.outcome_id == outcome_id))).one()
        return run_cost + score_cost, run_unknown == 0 and score_unknown == 0


def open_decision(workspace_id: str, kind: str, title: str, body: str, *, outcome_id: str | None = None,
                  task_id: str | None = None, options: list[dict[str, Any]] | None = None) -> str:
    with session_scope() as s:
        existing = (s.query(Decision)
                    .filter(Decision.workspace_id == workspace_id, Decision.kind == kind, Decision.status == "open",
                            Decision.outcome_id == outcome_id, Decision.task_id == task_id).first())
        if existing:
            existing.body = body
            existing.title = title
            if options is not None:
                existing.options = options
            decision_id = existing.id
        else:
            decision = Decision(workspace_id=workspace_id, kind=kind, title=title, body=body,
                                outcome_id=outcome_id, task_id=task_id, options=options or [])
            s.add(decision)
            s.flush()
            decision_id = decision.id
    record(workspace_id, "decision_requested", title, outcome_id=outcome_id, task_id=task_id, level="warn",
           data={"decision_id": decision_id, "kind": kind})
    notify(workspace_id, "decision", decision_id)
    return decision_id


def set_outcome(outcome_id: str, **fields: Any) -> None:
    with session_scope() as s:
        outcome = s.get(Outcome, outcome_id)
        for key, value in fields.items():
            setattr(outcome, key, value)
        workspace_id = outcome.workspace_id
    notify(workspace_id, "outcome", outcome_id)
