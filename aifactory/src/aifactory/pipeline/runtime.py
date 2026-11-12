"""Shared helpers for running agent sessions inside the pipeline."""

from __future__ import annotations

import asyncio
import json
import time
from datetime import timedelta
from collections.abc import Mapping
from dataclasses import dataclass
from types import MappingProxyType
from typing import Any

from ..config import concurrency_limit, get_settings
from ..config_versions import canonical_json, configuration_version
from ..db import session_scope, utcnow
from ..events import notify, record
from ..failures import failure_cause, failure_status
from ..gateway import GatewayGrant, issue, revoke
from ..harness import HarnessFailed, HarnessResult, HarnessSession, get_harness
from ..harness.schemas import SCHEMAS
from ..inference import InferenceError, ModelClient, UsageLedger, resolve_provider, workspace_models
from ..models import Decision, DecisionReview, Message, Outcome, Run, ScorerResult, Task, Workspace
from ..sandbox import LocalSandbox
from ..review import DEFAULT_REVIEW
from .attachments import prepare_attachments

DEFAULT_SETTINGS: dict[str, Any] = {
    "review": DEFAULT_REVIEW,
    "supervisor": {"mode": "observe", "interval_minutes": 30, "stuck_minutes": 120, "unaccepted_issue_hours": 24},
    "provider": "mindshub",
    "decision_judges": "decide",
    # One agent per outcome, responsible for delivering it (pipeline/lead.py).
    "lead": {"enabled": True, "run_usd": 2.0, "turns": 30},
    # The Director stands in for the people who run the workspace (pipeline/director.py). Off unless a workspace turns
    # it on: run_usd caps a run, daily_usd its runs in a day, max_outcome_usd how far it lets an outcome spend past
    # its budget, and after_minutes how long a decision waits for its judge and Lead first.
    "director": {"mode": "off", "run_usd": 4.0, "daily_usd": 40.0, "max_outcome_usd": 120.0, "after_minutes": 5},
    # Alerts show in the app's Inbox. Slack and email are delivery options that aren't available yet: both stay off.
    "alerts": {"after_minutes": 120, "slack": False, "email": False},
    "models": {"default": "gpt-terra"},
    "harness": {"builder": "native", "verifier": "native", "planner": "native"},
    "concurrency": 3,
    "planning_concurrency": 2,
    "max_attempts": 3,
    # When attempts run out on concrete feedback (verifier findings, failed checks, policy), try once more with a
    # fresh budget before asking a person: relaying findings into a retry is not a decision.
    "auto_retry_rounds": 1,
    "autonomy": {"plan": "auto", "merge": "auto", "release": "approval"},
    # "patch": a merge approval covers the same change when it is published again on a newer base. "head": every new
    # head is approved again. A repository's merge.approval overrides it (TaskRunner._approved).
    "merge": {"approval": "patch"},
    "budgets": {"run_usd": 8.0, "outcome_usd": 40.0, "run_tokens": 6_000_000,
                "task_usd": 15.0},  # a task's builder and verifier runs over its whole life; 0 or None: no limit
    "limits": {"builder_turns": 120, "verifier_turns": 60, "planner_turns": 60, "plan_reviewer_turns": 40,
               "task_attempts_total": 12,  # charged attempts over a task's whole life; 0 or None: no limit
               "run_minutes": 60,
               "ci_minutes": 45, "max_changed_lines": 1500},
    "reasoning_effort": "medium",
    # Per person per UTC day across all their workspaces, the lowest of their workspaces' limits (mind/store.py).
    "mind": {"daily_budget_usd": 10.0},
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
    for key in ("concurrency", "planning_concurrency"):
        out[key] = concurrency_limit(out[key], DEFAULT_SETTINGS[key])
    from ..onboarding_policy import effective

    return effective(out)


# A workspace is created with a copy of DEFAULT_SETTINGS and every save writes the merged settings back, so a default
# that changes here never reaches a workspace that already exists. List each changed default: (revision, section,
# key, the value it had). A stored value that still equals the former default is that copy, not a choice, and moves
# to the current default once; "_defaults" in the settings (a key the API never sets) is the revision a workspace
# is at. A workspace made after a change runs through this at the next start with nothing to move.
SETTINGS_REVISION = 1
FORMER_DEFAULTS = ((1, "review", "wait_seconds", 120), (1, "review", "max_rounds", 2))


def new_settings() -> dict[str, Any]:
    """A new workspace's own copy of the defaults, marked current: a restart has nothing to refresh in it."""
    return {**json.loads(json.dumps(DEFAULT_SETTINGS)), "_defaults": SETTINGS_REVISION}


def refreshed_defaults(raw: dict[str, Any] | None) -> dict[str, Any] | None:
    """The settings with copies of former defaults moved to the current ones; None when they are up to date."""
    raw = raw or {}
    at = raw.get("_defaults") if type(raw.get("_defaults")) is int else 0
    if at >= SETTINGS_REVISION:
        return None
    out = json.loads(json.dumps(raw))
    for revision, section, key, former in FORMER_DEFAULTS:
        stored = out.get(section)
        if (revision > at and isinstance(stored, dict) and type(stored.get(key)) is type(former)
                and stored[key] == former):
            stored[key] = DEFAULT_SETTINGS[section][key]
    out["_defaults"] = SETTINGS_REVISION
    return out


def refresh_copied_defaults() -> int:
    """At startup: bring every workspace's copy of the defaults up to date. Returns how many were written."""
    from ..onboarding_policy import lock_workspace

    with session_scope() as s:
        stale = [workspace_id for workspace_id, settings in s.query(Workspace.id, Workspace.settings)
                 .order_by(Workspace.created_at, Workspace.id) if refreshed_defaults(settings) is not None]
        for workspace_id in stale:
            workspace = lock_workspace(s, workspace_id)  # a settings save may be running; read again under the lock
            fresh = refreshed_defaults(workspace.settings)
            if fresh is not None:
                workspace.settings = fresh
        return len(stale)


def workspace_settings(workspace_id: str) -> dict[str, Any]:
    with session_scope() as s:
        ws = s.get(Workspace, workspace_id)
        return merged_settings(ws.settings if ws else {})


def budget_extension(settings: dict[str, Any]) -> float:
    """Dollars that continuing a task past the outcome budget adds to what is spent by then: budgets.continue_usd
    when that is a positive number, else a quarter of the budget. The budget decision's button states this amount
    and resolving it grants the same, so both read it here."""
    budgets = settings.get("budgets") or {}

    def positive(value: Any) -> float:
        number = isinstance(value, (int, float)) and not isinstance(value, bool)
        return float(value) if number and 0 < value < float("inf") else 0.0

    return max(0.01, round(positive(budgets.get("continue_usd")) or positive(budgets.get("outcome_usd")) / 4, 2))


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


def precedents(workspace_id: str, repo_id: str | None = None, limit: int = 12,
                repo_ids: list[str] | None = None) -> list[Decision]:
    """Reusable, attributed human decisions that apply to this workspace (and repository)."""
    with session_scope() as s:
        query = (s.query(Decision)
                 .filter(Decision.workspace_id == workspace_id, Decision.status == "resolved",
                         Decision.reusable.is_(True),
                         ~Decision.review.has(DecisionReview.correction_decision_id.is_not(None)),
                         Decision.scope.in_(("workspace", "repository")))
                 .order_by(Decision.id.in_(s.query(DecisionReview.correction_decision_id).filter(
                     DecisionReview.workspace_id == workspace_id,
                     DecisionReview.correction_decision_id.is_not(None))).desc(),
                           Decision.resolved_at.desc(), Decision.id))

        def applicable_to(d: Decision, repo: str) -> bool:
            if d.task_id:
                t = s.get(Task, d.task_id)
                return bool(t and t.repo_id == repo)
            o = s.get(Outcome, d.outcome_id) if d.outcome_id else None
            return bool(o and repo in (o.repo_ids or []))

        def applicable(rows: list[Decision]) -> list[Decision]:
            """A repository-scoped decision applies to the repository it was made about (its task's, else its
            outcome's). Repositories are looked up once per page, not once per decision."""
            if repo_id is None:
                return rows
            scoped = [d for d in rows if d.scope == "repository"]
            task_ids = {d.task_id for d in scoped if d.task_id}
            outcome_ids = {d.outcome_id for d in scoped if not d.task_id and d.outcome_id}
            task_repo = dict(s.query(Task.id, Task.repo_id).filter(Task.workspace_id == workspace_id, Task.id.in_(task_ids)).all()) if task_ids else {}
            outcome_repos = ({oid: set(repos or []) for oid, repos in
                              s.query(Outcome.id, Outcome.repo_ids).filter(Outcome.workspace_id == workspace_id, Outcome.id.in_(outcome_ids)).all()}
                             if outcome_ids else {})
            return [d for d in rows if d.scope == "workspace"
                    or (d.task_id and task_repo.get(d.task_id) == repo_id)
                    or (not d.task_id and repo_id in outcome_repos.get(d.outcome_id, set()))]

        # Without a repository (planning across an outcome's repositories) every decision applies. Page until
        # enough apply: other repositories' decisions must not crowd out older applicable ones.
        found: list[Decision] = []
        offset, page = 0, max(limit * 4, 20)
        while len(found) < limit:
            rows = query.filter(Decision.kind != "ruling").offset(offset).limit(page).all()
            found += applicable(rows)
            if len(rows) < page:
                break
            offset += page
        # Team rulings are rules, not examples: every applicable one is always given, beyond the limit.
        rules = applicable(query.filter(Decision.kind == "ruling").all())
        if repo_id is None and repo_ids is not None:
            # Planning across an outcome's repositories: a repository rule for another repository doesn't apply.
            rules = [d for d in rules if d.scope == "workspace" or any(applicable_to(d, r) for r in repo_ids)]
        return rules + found[:limit]


@dataclass(frozen=True)
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
    effective_settings: Mapping[str, Any] | None = None
    model_override: str | None = None
    repositories: Mapping[str, Any] | None = None
    expected_criteria: list[str] | None = None  # the task's acceptance criteria, for a verifier's report
    expected_findings: list[str] | None = None  # ids of the review findings a builder's report must answer

    def __post_init__(self) -> None:
        # Own a recursively immutable snapshot, not a reference to mutable workspace/repo JSON.
        def freeze(value):
            if isinstance(value, Mapping):
                return MappingProxyType({k: freeze(v) for k, v in value.items()})
            if isinstance(value, (list, tuple)):
                return tuple(freeze(v) for v in value)
            return value
        for key in ("effective_settings", "repositories"):
            value = getattr(self, key)
            if value is not None:
                object.__setattr__(self, key, freeze(value))


def carried_cause(run: Any) -> str | None:
    """Why a failed run failed, as run_agent recorded it from the exception: "provider", "transient" or "" (see
    failures.failure_cause). None when the failure did not say (a sandbox error, a run recorded before causes were
    kept): the caller then reads the error's wording."""
    failure = (getattr(run, "config_snapshot", None) or {}).get("failure")
    return failure.get("cause") if isinstance(failure, dict) else None


async def run_agent(spec: SessionSpec, cancelled: asyncio.Event) -> tuple[Run, HarnessResult | None, str]:
    """Run one harness session as a recorded Run. Returns (run, result_or_None, error). A failed run keeps what
    failed it under ``config_snapshot["failure"]`` (cause and HTTP status) when the exception says: carried_cause."""
    settings = (merged_settings(json.loads(canonical_json(spec.effective_settings)))
                if spec.effective_settings is not None else workspace_settings(spec.workspace_id))
    provider = (resolve_provider(spec.workspace_id, required_provider=settings["provider"])
                if spec.effective_settings is not None
                and settings["provider"] != workspace_settings(spec.workspace_id)["provider"]
                else resolve_provider(spec.workspace_id))
    models = workspace_models(settings, provider=provider.provider)
    model = {"planner": models.planner, "builder": models.builder, "verifier": models.verifier,
             "plan_reviewer": models.plan_reviewer}[spec.role]
    model = spec.model_override or provider.model_override or model
    version, snapshot = configuration_version(
        role=spec.role, system=spec.system, harness=spec.harness, provider=provider, model=model,
        settings=settings, repositories=spec.repositories, sandbox_kind=getattr(spec.sandbox, "kind", "local"))
    limits = settings["limits"]
    budgets = settings["budgets"]
    with session_scope() as s:
        run = Run(workspace_id=spec.workspace_id, outcome_id=spec.outcome_id, task_id=spec.task_id, role=spec.role,
                  harness=spec.harness, provider=provider.provider, model=model, attempt=spec.attempt,
                  config_version=version, config_snapshot=snapshot)
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
    failure: dict[str, Any] | None = None
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
                   run_id=run_id, level="warn" if kind == "warn" else "info", data={"role": spec.role, **data})

        session = HarnessSession(
            run_id=run_id, role=spec.role, system=spec.system, prompt=spec.prompt, sandbox=spec.sandbox, model=model,
            client=client, ledger=ledger, report_schema=SCHEMAS[spec.role], writable=spec.writable, max_turns=turns,
            expected_criteria=spec.expected_criteria, expected_findings=spec.expected_findings,
            deadline=time.monotonic() + limits.get("run_minutes", 60) * 60, cancelled=cancelled,
            on_progress=on_progress, take_guidance=take_guidance(spec.outcome_id, spec.task_id),
            # A VM reaches the Factory only over its public URL; local sandboxes use the internal one.
            gateway_url=f"{get_settings().public_url if getattr(spec.sandbox, 'kind', 'local') == 'vm' else get_settings().internal_url}/gateway/v1",
            gateway_token=gateway_token,
            reasoning_effort=settings.get("reasoning_effort"),
        )
        context, session.image_inputs = await prepare_attachments(spec.sandbox, spec.workspace_id, spec.outcome_id)
        session.prompt += context
        if spec.role == "builder":  # the one role whose unreported work is still checked (TaskRunner.deliver)
            budget = f"{limits.get('run_minutes', 60):g} minutes"
            if spec.harness != "codex":  # the Codex CLI has no turn limit
                budget += f" and {turns} turns (each reply of yours, with its tool calls, is one turn)"
            session.prompt += (
                f"\n\n## Your budget\nThis session has {budget}. If a limit is reached before you finish, the "
                "factory checks whatever is in the working tree as it stands. So work in small complete steps, keep "
                "the tree in a working state, and finish with an honest report before the limit.")
        result = await harness.run(session)
    except asyncio.CancelledError:
        status, error = "cancelled", "Cancelled"
        raise
    except Exception as exc:  # harness failures are recorded, never hidden
        status, error = "failed", str(exc) or type(exc).__name__
        cause = failure_cause(exc)
        # A harness or model-client failure says what it was, even when that is "not the provider". Anything else
        # (the sandbox, a stray error) carries nothing unless its type is telling, and is read from its wording.
        if cause or isinstance(exc, (HarnessFailed, InferenceError)):
            failure = {"cause": cause, "status": failure_status(exc)}
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
            if failure is not None:
                row.config_snapshot = {**(row.config_snapshot or {}), "failure": failure}
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


def outcome_committed(outcome_id: str) -> tuple[float, bool]:
    """What admitting more paid work must count against the outcome's budget: its recorded cost (outcome_cost), plus
    what Lead runs in progress have reserved and not yet spent (pipeline/lead.py). A run admitted after a Lead can't
    spend what the Lead set aside. A restart marks interrupted Lead runs failed, so no reservation outlives its run."""
    spent, known = outcome_cost(outcome_id)
    # A Lead run lasts at most 15 minutes; one still marked running after 30 was cut off and holds nothing (the
    # same rule as pipeline/lead.STALE), so a crash can't keep an outcome's budget reserved until the next restart.
    alive = utcnow() - timedelta(minutes=30)
    with session_scope() as s:
        held = 0.0
        for run in s.query(Run).filter(Run.outcome_id == outcome_id, Run.role == "lead", Run.status == "running",
                                       Run.started_at > alive):
            reserved = (run.config_snapshot or {}).get("budget_reserved")
            if isinstance(reserved, (int, float)) and not isinstance(reserved, bool) and reserved > 0:
                held += max(0.0, float(reserved) - float(run.cost_usd or 0.0))
    return spent + held, known


def outcome_allowance(outcome_id: str, budget: float | None = None) -> float:
    """What the outcome may spend in all, to compare with outcome_committed(): ``budget``, or the largest amount a
    person allowed one of its tasks when they let it continue past the budget (``budget_allowance_usd``, granted in
    decisions.resolve and enforced by the gate in deliver()), whichever is more. 0.0 with neither. A task still
    carrying the older ``budget_override`` has no amount until it next runs, when the gate gives it one."""
    with session_scope() as s:
        evidence = [ev for (ev,) in s.query(Task.evidence).filter(Task.outcome_id == outcome_id)]
    allowed = [ev.get("budget_allowance_usd") for ev in evidence if isinstance(ev, dict)]
    return max([float(budget or 0.0)] + [float(amount) for amount in allowed
                                         if isinstance(amount, (int, float)) and not isinstance(amount, bool)])


def open_decision(workspace_id: str, kind: str, title: str, body: str, *, outcome_id: str | None = None,
                  task_id: str | None = None, options: list[dict[str, Any]] | None = None,
                  metadata: dict[str, Any] | None = None, dedup_key: str | None = None) -> str:
    with session_scope() as s:
        if dedup_key is not None:
            # Serialize the read/check/insert boundary, including on SQLite
            # where FOR UPDATE is ignored. No external work runs under the lock.
            if s.bind.dialect.name == "sqlite":
                s.query(Workspace).filter(Workspace.id == workspace_id).update(
                    {Workspace.outcome_seq: Workspace.outcome_seq}, synchronize_session=False)
            s.query(Workspace).filter(Workspace.id == workspace_id).with_for_update().one()
            candidates = s.query(Decision).filter(Decision.workspace_id == workspace_id,
                Decision.kind == kind, Decision.status.in_(["open", "dismissed"]))
            existing = next((d for d in candidates
                             if (d.metadata_json or {}).get("fingerprint") == dedup_key), None)
            if existing:
                return existing.id
        else:
            existing = (s.query(Decision)
                        .filter(Decision.workspace_id == workspace_id, Decision.kind == kind, Decision.status == "open",
                                Decision.outcome_id == outcome_id, Decision.task_id == task_id).first())
        if existing:
            existing.body = body
            existing.title = title
            existing.metadata_json = metadata
            if options is not None:
                existing.options = options
            decision_id = existing.id
        else:
            decision = Decision(workspace_id=workspace_id, kind=kind, title=title, body=body,
                                outcome_id=outcome_id, task_id=task_id, options=options or [], metadata_json=metadata)
            s.add(decision)
            s.flush()
            decision_id = decision.id
    publish_decision(workspace_id, decision_id, kind, title, outcome_id=outcome_id, task_id=task_id)
    return decision_id


def publish_decision(workspace_id: str, decision_id: str, kind: str, title: str, *,
                     outcome_id: str | None = None, task_id: str | None = None) -> None:
    """Announce a decision only after its transaction has committed."""
    record(workspace_id, "decision_requested", title, outcome_id=outcome_id, task_id=task_id, level="warn",
           data={"decision_id": decision_id, "kind": kind},
           actor="supervisor" if kind == "supervisor" else "factory")
    notify(workspace_id, "decision", decision_id)


def set_outcome(outcome_id: str, **fields: Any) -> None:
    with session_scope() as s:
        outcome = s.get(Outcome, outcome_id)
        for key, value in fields.items():
            setattr(outcome, key, value)
        workspace_id = outcome.workspace_id
    notify(workspace_id, "outcome", outcome_id)
