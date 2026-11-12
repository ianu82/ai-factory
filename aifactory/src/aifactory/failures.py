"""Why a model session failed, decided where the failure happens.

The model client and the harnesses know what a failure was (an HTTP status, a refusal marked not retryable, a dropped
connection); the pipeline used to guess it back from the error's wording. ``failure_cause`` classifies the exception
itself. ``text_cause`` is the wording match, kept for failures that arrive only as text: the Codex CLI, sandbox and
GitHub errors, and runs recorded before causes were carried. This module imports nothing from the Factory, so the
model client, the harnesses and the pipeline can all use it.
"""

from __future__ import annotations

import re
from typing import Any

import httpx

# Billing, quota and access problems: a person has to act (top up credits, fix a key), so the task blocks and no
# attempt is charged. Each harness words them differently: the native client ("MindsHub: no credit remaining"), Codex
# through the Factory gateway ("wallet_empty", "Provider returned 402: …") and Anton through the OpenAI SDK
# ("Error code: 402 - …", "insufficient_quota").
PROVIDER_PROBLEMS = ("refused the credential", "no credit", "No model provider", "limit reached", "wallet",
                     "No GitHub access", "Bad credentials", "credits are used up", "wallet_empty", "insufficient_quota",
                     "quota exceeded", "billing")
# HTTP 402 as a status code, never as digits inside a number such as a token count ("1,402,000").
_PAYMENT_REQUIRED = re.compile(r"(?:\b(?:returned|error code|status(?: code)?|http)\W{0,3}|\()402\b|\b402 payment",
                               re.IGNORECASE)
# An attempt's own caps (turns, time, tokens, cost) are not infrastructure: they stay charged.
RUN_CAPS = ("budget of", "budget_exhausted", "time budget", "Stopped after")
# The provider or the sandbox was unavailable after the client's own retries: the same attempt is redone later.
TRANSIENT_PROBLEMS = ("Model request failed", "unreachable", "timed out", "timeout", "connection error",
                      "connection reset", "connection refused", "stream disconnected", "server disconnected",
                      "bad gateway", "service unavailable", "gateway timeout", "overloaded", "upstream_error",
                      "VM bridge", "No sandbox identities", "Sandbox setup failed")
_SERVER_ERROR = re.compile(r"\b(?:returned|error(?: code)?|status(?: code)?|http)\W{0,3}(?:5\d\d|429)\b",
                           re.IGNORECASE)
# A 429 that is a spent quota, not a rate limit: waiting does not help. Narrower than PROVIDER_PROBLEMS, whose
# "limit reached" also matches an ordinary rate limit ("Rate limit reached for …"). MindsHub's gateway sends its
# allowance and daily spend fuse codes on a 429 too.
_EXHAUSTED = ("insufficient_quota", "quota exceeded", "exceeded your current quota", "credits are used up",
              "no credit", "wallet", "billing", "allowance_exhausted", "spend_fuse")
# A rate limit: the provider asks for requests to slow down, and waiting lifts it.
_RATE_LIMITS = ("rate limit", "rate_limit", "ratelimit", "too many requests")
# Wording of an answer whose trouble passes by itself. A provider that adds "do not send this again" to such an
# answer (it already retried on its side) has given up for the moment, not set a limit.
_MOMENTARY = ("overloaded", "momentarily", "temporarily")
# The Anton SDK's own failures, by class name: its retries ended on an overload or outage, or it stopped on billing
# or on the credential.
_ANTON_TRANSIENT = ("ProviderOverloadedError", "TransientProviderError")
_ANTON_PROVIDER = ("MindsHubBillingStop", "ProviderAuthError")
CAUSES = ("provider", "transient", "")


def rate_limited(text: str) -> bool:
    """Whether a refusal is a rate limit, which lifts on its own, rather than a spent quota or credit."""
    text = (text or "").lower()
    return any(p in text for p in _RATE_LIMITS) and not quota_exhausted(text)


def text_cause(error: str) -> str:
    """A failure's cause read from its wording: "provider" (billing, quota or access; a person acts), "transient"
    (the provider or the sandbox was unavailable, or limited its rate; the work is redone later), or "" (the
    session's own failure, including its turn, time, token and cost caps)."""
    text = (error or "").lower()
    if (any(p.lower() in text for p in PROVIDER_PROBLEMS) and not rate_limited(text)) or _PAYMENT_REQUIRED.search(text):
        return "provider"
    if any(c.lower() in text for c in RUN_CAPS):
        return ""
    if any(p.lower() in text for p in TRANSIENT_PROBLEMS) or _SERVER_ERROR.search(text) or rate_limited(text):
        return "transient"
    return ""


def quota_exhausted(text: str) -> bool:
    """Whether a rate-limit response (429) says the account's quota or credit is spent."""
    text = (text or "").lower()
    return any(p in text for p in _EXHAUSTED)


def momentary(text: str) -> bool:
    """Whether a provider's answer says its trouble passes by itself (an overload, a brief outage)."""
    text = (text or "").lower()
    return any(p in text for p in _MOMENTARY)


def _status(exc: BaseException | None) -> int | None:
    for name in ("status", "status_code"):
        value = getattr(exc, name, None)
        if isinstance(value, int) and not isinstance(value, bool):
            return value
    return None


def failure_status(exc: BaseException) -> int | None:
    """The HTTP status a failure carries (ours as ``status``, an SDK's as ``status_code``), or the one of the
    exception it was raised from."""
    return _status(exc) if _status(exc) is not None else _status(exc.__cause__)


def _never_retry(exc: BaseException) -> bool:
    """The provider marked its answer as not to be sent again (``x-should-retry: false``). On a limit that isn't a
    rate limit, that is a limit a person lifts (a monthly spend limit). On an overload, a rate limit or a server error
    it isn't: a gateway sends it once its own retries are spent, and the provider recovers on its own."""
    headers: Any = getattr(getattr(exc, "response", None), "headers", None)
    try:
        return str(headers.get("x-should-retry", "")).lower() == "false"
    except Exception:
        return False


def failure_cause(exc: BaseException) -> str:
    """Why a model request or session failed, from the exception itself: "provider" (billing, quota or access: a
    person acts), "transient" (the provider was unavailable: worth trying again) or "" (anything else: a rejected
    request, the session's own failure or cap, a cancellation).

    Our own exceptions (InferenceError, HarnessFailed) carry the answer as ``cause``, set where they are raised. A
    provider SDK's exceptions are read by shape (``status_code``, class name), so no SDK is imported here."""
    cause = getattr(exc, "cause", None)
    if isinstance(cause, str) and cause in CAUSES:
        # A refusal whose own words say it passes by itself is not one a person has to lift: on 12 November a session
        # ended with "The model provider is momentarily overloaded", carried as a refusal, and held the workspace.
        return "transient" if cause == "provider" and momentary(str(exc)) else cause
    names = {c.__name__ for c in type(exc).__mro__}
    if names & set(_ANTON_PROVIDER):
        return "provider"
    if names & set(_ANTON_TRANSIENT):
        return "transient"
    status = getattr(exc, "status_code", None)
    if isinstance(status, int) and not isinstance(status, bool):
        if status in (401, 402, 403):
            return "provider"
        if status == 429 and quota_exhausted(str(exc)):
            return "provider"
        # "Do not send this again" on a limit is a limit a person lifts. On a server error, or on an answer that says
        # its trouble is momentary (whatever its status), the provider only gave up for now: tried again, by nobody.
        if _never_retry(exc) and momentary(str(exc)):
            return "transient"
        if status not in (408, 409, 429) and status < 500:
            return ""
        if _never_retry(exc) and status < 500 and not rate_limited(str(exc)):
            return "provider"
        return "transient"  # an overload, a server error or a rate limit: the provider recovers on its own
    if isinstance(exc, (httpx.TransportError, httpx.TimeoutException)):
        return "transient"
    if any(c.__name__ in ("APIConnectionError", "APITimeoutError") for c in type(exc).__mro__):
        return "transient"
    return ""
