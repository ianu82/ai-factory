"""The provider hold: when the model provider refuses a workspace (billing, quota or its credential), every task and
plan of that workspace would fail the same way. One open ``provider_hold`` decision, with no outcome and no task, is
the hold: the scheduler starts nothing in the workspace while it is open, and resolving it lets work start again.
The hold keeps why the provider refused (hold_details), and every screen that shows the hold names it.

While a hold is open the orchestrator tests the provider every ``holds.test_minutes`` (probe_provider): when every
model the workspace works with answers, the Factory lifts the hold itself, as a person's Resume work would. A refusal
for billing, quota or the credential leaves it for a person."""

from __future__ import annotations

import asyncio
import logging
import re
from datetime import datetime, timedelta
from typing import Any

from sqlalchemy.orm import Session

from ..db import SessionLocal, session_scope, utcnow
from ..events import record
from ..failures import _PAYMENT_REQUIRED, failure_cause, quota_exhausted, rate_limited
from ..inference import ModelClient, resolve_provider, workspace_models
from ..models import Decision, Event, User, Workspace
from ..onboarding_policy import lock_workspace
from .runtime import DEFAULT_SETTINGS, merged_settings, publish_decision, workspace_settings

log = logging.getLogger(__name__)

PROVIDER_HOLD = "provider_hold"
TITLE = "Model provider refused: work is on hold"
# Why the provider refused, as people read it wherever the hold shows: what happened, and what lifts it. Overloads
# and rate limits are retried and don't open a hold (failures.failure_cause); they are here for a hold opened before
# that, or by a refusal worded like one.
CAUSES: dict[str, tuple[str, str]] = {
    "credit": ("the model provider account is out of credit",
               "Add credit to the model provider account, then resume work."),
    "credential": ("the model provider refused the Factory's credential",
                   "Fix the model provider credential in Settings, then resume work."),
    "no_provider": ("no model provider is set up for this workspace",
                    "Set up a model provider in Settings, then resume work."),
    "limit": ("the model provider account reached a usage limit",
              "Raise the limit with the provider or wait until it resets, then resume work."),
    "rate_limit": ("the model provider was limiting the rate of requests",
                   "A rate limit lifts on its own within minutes: resume work."),
    "overloaded": ("the model provider was overloaded or unavailable",
                   "An overload usually clears within minutes: resume work."),
    "unknown": ("the model provider refused a request",
                "Check the provider account and the credential in Settings, then resume work."),
}
_AUTH = re.compile(r"refused the credential|bad credentials|invalid api key|unauthori[sz]ed|forbidden|"
                   r"(?:\b(?:returned|error code|status(?: code)?|http)\W{0,3}|\()40[13]\b", re.IGNORECASE)
_SERVER = re.compile(r"(?:\b(?:returned|error(?: code)?|status(?: code)?|http)\W{0,3}|\()5\d\d\b", re.IGNORECASE)
# The text of a hold opened before its cause was kept: what was refused, then the provider's words.
_BODY = re.compile(r"^The model provider refused a request \((?P<source>.*?)\):\n\n(?P<error>.*?)"
                   r"\n\n(?:Cause: |Nothing new)", re.DOTALL)
FACTORY_EMAIL = "factory@factory.local"
TEST_AFTER = timedelta(minutes=2)  # a hold is first tested this long after it opened
TEST_TIMEOUT = 30.0
TEST_TOKENS = 16
# When each workspace's hold was last tested, in memory: after a restart it is simply tested again.
PROBED: dict[str, datetime] = {}


def hold_cause(error: str) -> str:
    """Which of CAUSES a provider's refusal is, from what it said."""
    text = (error or "").lower()
    if "no model provider" in text:
        return "no_provider"
    if _PAYMENT_REQUIRED.search(text) or quota_exhausted(text) or "allowance" in text:
        return "credit"
    if _AUTH.search(text):
        return "credential"
    if "overloaded" in text or "unavailable" in text or _SERVER.search(text):
        return "overloaded"
    if rate_limited(text):
        return "rate_limit"
    if "limit" in text or "quota" in text:
        return "limit"
    return "rate_limit" if "429" in text else "unknown"


def hold_details(decision: Decision) -> dict[str, Any]:
    """A hold's cause, what was refused and the provider's words, for every screen that shows the hold. A hold
    opened before causes were kept is read from its text."""
    meta = decision.metadata_json if isinstance(decision.metadata_json, dict) else {}
    found = _BODY.match(decision.body or "")
    error = str(meta.get("error") or (found["error"] if found else "") or "")
    cause = meta.get("cause") if meta.get("cause") in CAUSES else hold_cause(error)
    label, advice = CAUSES[cause]
    return {"cause": cause, "label": label, "advice": advice,
            "source": meta.get("source") or (found["source"] if found else None), "error": error[:500]}


def open_provider_hold(workspace_id: str, error: str, source: str) -> str:
    """Hold the workspace unless it is held already; returns the hold's decision. ``source`` says what was refused
    first ("T2 of outcome #7, the builder session"). Work that fails while the hold is open finds that decision."""
    cause = hold_cause(error)
    label, advice = CAUSES[cause]
    with session_scope() as s:
        # First statement of the transaction, so two failures in the same moment take turns: the second one's
        # query below runs after the first one's commit and finds its decision.
        lock_workspace(s, workspace_id)
        existing = s.query(Decision.id).filter(Decision.workspace_id == workspace_id, Decision.kind == PROVIDER_HOLD,
                                               Decision.status == "open").first()
        if existing:
            return existing[0]
        decision = Decision(
            workspace_id=workspace_id, kind=PROVIDER_HOLD, title=TITLE,
            body=f"The model provider refused a request ({source}):\n\n{error[:1500]}\n\n"
                 f"Cause: {label}.\n\n"
                 "Nothing new starts in this workspace, neither planning nor tasks, until someone chooses Resume "
                 "work. Work that is refused the same way waits in the queue; no task is charged an attempt for "
                 f"it.\n\nWhat to do: {advice} If the provider still refuses, this hold opens again.",
            options=[{"value": "retry", "label": "Resume work"}],
            metadata_json={"cause": cause, "source": source[:300], "error": error[:1500]})
        s.add(decision)
        s.flush()
        decision_id = decision.id
    publish_decision(workspace_id, decision_id, PROVIDER_HOLD, TITLE)
    return decision_id


def held_workspaces(session: Session) -> set[str]:
    """The workspaces with an open provider hold."""
    return {workspace_id for (workspace_id,) in session.query(Decision.workspace_id).filter(
        Decision.kind == PROVIDER_HOLD, Decision.status == "open")}


def every_minutes(settings: dict[str, Any]) -> int:
    """How often a held workspace's provider is tested, in minutes; 0: never."""
    raw = settings.get("holds") if isinstance(settings.get("holds"), dict) else {}
    value = raw.get("test_minutes")
    return value if type(value) is int and 0 <= value <= 60 else DEFAULT_SETTINGS["holds"]["test_minutes"]


def probe_status(workspace_id: str, settings: dict[str, Any]) -> dict[str, Any]:
    """For the screens that show a hold: how often the Factory tests the provider (0: never), and when it last did."""
    last = PROBED.get(workspace_id)
    return {"tests_every_minutes": every_minutes(settings), "last_tested_at": last.isoformat() + "Z" if last else None}


def due_probes(tested: dict[str, datetime], running: frozenset[str] = frozenset(),
              now: datetime | None = None) -> list[dict[str, str]]:
    """The holds to test now: open for TEST_AFTER, in a workspace that isn't paused, isn't being tested and wasn't
    tested (``tested``: when each workspace last was) in its last ``holds.test_minutes``."""
    now = now or utcnow()
    due: dict[str, dict[str, str]] = {}
    with session_scope() as s:
        rows = (s.query(Decision.id, Decision.workspace_id, Decision.created_at, Workspace.settings)
                .join(Workspace, Workspace.id == Decision.workspace_id)
                .filter(Decision.kind == PROVIDER_HOLD, Decision.status == "open", Workspace.paused.isnot(True))
                .order_by(Decision.created_at).all())
    for decision_id, workspace_id, opened, settings in rows:
        every = every_minutes(merged_settings(settings))
        last = tested.get(workspace_id)
        if (not every or workspace_id in running or workspace_id in due or now - opened < TEST_AFTER
                or (last is not None and now - last < timedelta(minutes=every))):
            continue
        due[workspace_id] = {"workspace_id": workspace_id, "decision_id": decision_id}
    return list(due.values())


def factory_user(db) -> User:
    """The system identity the Factory acts as when it lifts a hold itself; the resolution carries
    ``{"source": "factory"}``."""
    user = db.query(User).filter(User.is_system.is_(True), User.email == FACTORY_EMAIL).first()
    if user is None:
        user = User(email=FACTORY_EMAIL, name="Factory", is_system=True)
        db.add(user)
        db.flush()
    return user


async def probe_provider(workspace_id: str, decision_id: str) -> str:
    """Test a held workspace's provider: one minimal request (no tools, no retries) to each distinct model its
    planner, builder and verifier use. Every model answered: the hold is lifted ("lifted"), or a person resumed it
    meanwhile ("resolved"). A refusal for billing, quota or the credential leaves the hold to a person and says why
    once per reason ("refused"). Any other failure is tried again at the next interval, silently ("failed")."""
    models: list[str] = []
    try:
        settings = workspace_settings(workspace_id)
        provider = resolve_provider(workspace_id)
        chosen = workspace_models(settings, provider=provider.provider)
        models = list(dict.fromkeys(provider.model_override or model
                                    for model in (chosen.planner, chosen.builder, chosen.verifier)))
        client = ModelClient(provider, timeout=TEST_TIMEOUT, max_retries=0, log_errors=False)
        for model in models:
            await asyncio.wait_for(client._post("/responses", {
                "model": model, "input": "Reply with OK.", "store": False, "max_output_tokens": TEST_TOKENS,
            }), timeout=TEST_TIMEOUT)
    except Exception as exc:
        if failure_cause(exc) != "provider":
            log.info("provider hold test for %s failed: %s", workspace_id[:6], type(exc).__name__)
            return "failed"
        try:
            await asyncio.to_thread(_still_refused, workspace_id, decision_id, str(exc))
        except Exception:
            log.exception("could not record the provider hold test for %s", workspace_id[:6])
        return "refused"
    try:
        # On the loop, as the Lead's resolutions are: resolve() wakes the orchestrator.
        return _lift(workspace_id, decision_id, models)
    except Exception:
        log.exception("could not lift the provider hold of %s", workspace_id[:6])
        return "failed"


def _still_refused(workspace_id: str, decision_id: str, error: str) -> None:
    """Say why the provider still refuses, once per hold and reason."""
    cause = hold_cause(error)
    with session_scope() as s:
        last = (s.query(Event.data).filter(Event.workspace_id == workspace_id, Event.kind == "provider_hold_tested")
                .order_by(Event.id.desc()).first())
    said = last[0] if last and isinstance(last[0], dict) else {}
    if said.get("decision_id") == decision_id and said.get("cause") == cause:
        return
    label = CAUSES[cause][0]
    record(workspace_id, "provider_hold_tested",
           f"The Factory tested the model provider: {label}. The hold stays until a person resumes work.",
           level="warn", data={"decision_id": decision_id, "cause": cause, "reason": label, "error": error[:500]})


def _lift(workspace_id: str, decision_id: str, models: list[str]) -> str:
    """Lift the hold through the same path as a person's Resume work, attributed to the Factory."""
    from ..decisions import DecisionError, resolve

    names = ", ".join(models)
    db = SessionLocal()
    try:
        decision = db.query(Decision).filter(Decision.id == decision_id, Decision.workspace_id == workspace_id,
                                             Decision.kind == PROVIDER_HOLD).first()
        if decision is None or decision.status != "open":
            return "resolved"
        try:
            resolve(db, decision, factory_user(db), "retry",
                    f"The provider answered a test request ({names}). Resumed by the Factory.",
                    attribution={"source": "factory"})
        except DecisionError:  # a person resumed it while the test ran: theirs stands
            db.rollback()
            return "resolved"
    finally:
        db.close()
    record(workspace_id, "provider_hold_lifted",
           f"The model provider answered a test request ({names}): the hold is lifted and work resumes.",
           level="success", data={"decision_id": decision_id, "models": models, "source": "factory"})
    return "lifted"
