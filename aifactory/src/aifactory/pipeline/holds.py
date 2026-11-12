"""The provider hold: when the model provider refuses a workspace (billing, quota or its credential), every task and
plan of that workspace would fail the same way. One open ``provider_hold`` decision, with no outcome and no task, is
the hold: the scheduler starts nothing in the workspace while it is open, and resolving it lets work start again.
The hold keeps why the provider refused (hold_details), and every screen that shows the hold names it."""

from __future__ import annotations

import re
from typing import Any

from sqlalchemy.orm import Session

from ..db import session_scope
from ..failures import _PAYMENT_REQUIRED, quota_exhausted, rate_limited
from ..models import Decision
from ..onboarding_policy import lock_workspace
from .runtime import publish_decision

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
