"""Controller-owned admission, accounting and cancellation for Codex inference."""

import asyncio
import hashlib
import json
import math
import re
import time

from .codex_actions import CodexActionError, METADATA_REJECTION_DETAILS, REJECTION_REASONS
from .codex_request import CodexRequestError, codex_request
from .codex_stream import codex_response
from .product_costs import estimate_cost


class CodexStopped(BaseException):
    """A policy stop must also stop SDK compatibility/recovery attempts."""


_REASONS = {"cancelled_or_deadline", "evidence_unavailable", "usage_unknown_or_exhausted",
            "overlapping_requests_or_limit", "request_policy", "request_cancelled", "response_policy",
            "unconsumed_action", "unapproved_callback", "rpc_policy"}


class CodexInference:
    def __init__(self, config, journal, *, role, max_requests, max_total_tokens, deadline,
                 pricing=None, cancelled=lambda: False):
        if (config.fallback_model or any(type(n) is not int or n < 1 for n in
                (max_requests, max_total_tokens, config.max_output_tokens))
                or any(type(n) not in (int, float) or not math.isfinite(n) or n <= 0
                       for n in (deadline, config.timeout_seconds))):
            raise ValueError("Codex inference requires bounded configuration without fallback")
        self.config, self.journal, self.role = config, journal, role
        self.max_requests, self.max_total_tokens, self.deadline = max_requests, max_total_tokens, deadline
        self.pricing, self.cancelled = pricing or {}, cancelled
        self.calls, self._busy, self._stopped = 0, False, False
        self._pending_action, self._tool_active = None, False

    @property
    def stopped(self):
        return self._stopped

    def record(self, kind, **data):
        try:
            return self.journal.append(kind, role=self.role, harness="codex", **data)
        except Exception:
            self._stopped = True
            raise CodexStopped("Codex execution evidence could not be retained") from None

    def stop(self, reason, *, response_rejection=None, response_rejection_detail=None):
        self._stopped = True
        data = {}
        if type(reason) is str and reason == "response_policy":
            data["response_rejection"] = (response_rejection if type(response_rejection) is str
                and response_rejection in REJECTION_REASONS else "unknown")
            if (data["response_rejection"] == "action_metadata" and type(response_rejection_detail) is str
                    and response_rejection_detail in METADATA_REJECTION_DETAILS):
                data["response_rejection_detail"] = response_rejection_detail
        self.record("harness_policy_stopped", reason=reason if type(reason) is str and reason in _REASONS else "unknown", **data)
        raise CodexStopped("Codex execution stopped by Factory policy")

    def check_stop(self):
        """Observe owned work; this does not admit another request or action."""
        if self._stopped:
            raise CodexStopped("Codex session is stopped")
        if self.cancelled() or time.monotonic() >= self.deadline:
            self.stop("cancelled_or_deadline")

    def admit(self):
        self.check_stop()
        try:
            usage = self.journal.summary()
            valid = (usage["usage_complete"] is True and type(usage["total_tokens"]) is int
                     and 0 <= usage["total_tokens"] < self.max_total_tokens)
        except Exception:
            self.stop("evidence_unavailable")
        if not valid:
            self.stop("usage_unknown_or_exhausted")
        return usage

    def take_action(self, name, arguments):
        """Consume the exact approved action once; return detached trusted JSON.

        The session separately binds the callback to its owned thread and turn.
        Provider and app-server call IDs need not be the same identifier.
        """
        self.admit()
        try:
            supplied = json.dumps([name, arguments], sort_keys=True, allow_nan=False)
        except (ValueError, TypeError, RecursionError):
            self.stop("unapproved_callback")
        if self._pending_action is None or self._tool_active or supplied != self._pending_action:
            self.stop("unapproved_callback")
        approved = json.loads(self._pending_action)
        self._pending_action, self._tool_active = None, True
        return approved[0], approved[1]

    def action_finished(self):
        """Allow another inference only after the admitted tool has completed."""
        self.admit()
        if not self._tool_active:
            self.stop("unapproved_callback")
        self._tool_active = False

    async def request(self, client, supplied):
        """Account one non-streaming provider call, then return validated SSE bytes.

        The owning session creates/closes the controller-held client. It supplies
        a credential-free app-server with these bytes only after this returns.
        No provider response or exception text is retained in the journal.
        """
        usage = self.admit()
        if self._pending_action is not None or self._tool_active:
            self.stop("unconsumed_action")
        if self._busy or self.calls >= self.max_requests:
            self.stop("overlapping_requests_or_limit")
        try:
            body = codex_request(supplied, model=self.config.model, effort=self.config.reasoning_effort,
                max_output_tokens=self.config.max_output_tokens,
                remaining_tokens=self.max_total_tokens - usage["total_tokens"])
        except CodexRequestError:
            self.stop("request_policy")
        self.admit()  # Validation time cannot extend the global deadline.
        # The controller buffers the response. App-server still receives SSE,
        # produced only after full usage and action validation below.
        body["stream"] = False
        self.calls += 1
        self.record("model_requested", turn=self.calls, model=self.config.model,
            reasoning_effort=self.config.reasoning_effort, max_output_tokens=body["max_output_tokens"],
            input_sha=hashlib.sha256(json.dumps(body, sort_keys=True).encode()).hexdigest())
        started, self._busy = time.monotonic(), True
        try:
            if self.cancelled() or time.monotonic() >= self.deadline:
                self.stop("cancelled_or_deadline")
            timeout = min(self.config.timeout_seconds, self.deadline - time.monotonic())
            async with asyncio.timeout(max(0, timeout)):
                request = asyncio.create_task(client.with_options(max_retries=0, timeout=max(0, timeout)).post(
                    "/responses", body=body, cast_to=dict))
                try:
                    while not request.done():
                        await asyncio.wait({request}, timeout=0.05)
                        if self._stopped or self.cancelled():
                            self.stop("request_cancelled")
                    response = await request
                finally:
                    if not request.done():
                        request.cancel()
                        await asyncio.gather(request, return_exceptions=True)
        except BaseException:
            self._stopped = True
            self.record("model_failed", turn=self.calls, model=self.config.model)
            raise CodexStopped("Codex inference ended without a usable response") from None
        finally:
            self._busy = False
        raw = response.get("usage") if type(response) is dict else None
        recorded = None
        if (type(raw) is dict and all(type(raw.get(k)) is int and raw[k] >= 0
                for k in ("input_tokens", "output_tokens", "total_tokens"))
                and raw["total_tokens"] == raw["input_tokens"] + raw["output_tokens"]):
            recorded = {k: raw[k] for k in ("input_tokens", "output_tokens", "total_tokens")}
            details = raw.get("input_tokens_details")
            if type(details) is dict and type(details.get("cached_tokens")) is int and 0 <= details["cached_tokens"] <= raw["input_tokens"]:
                recorded["input_tokens_details"] = {"cached_tokens": details["cached_tokens"]}
        identity = response.get("id") if type(response) is dict else None
        safe_id = identity if type(identity) is str and re.fullmatch(r"[A-Za-z0-9_-]{1,200}", identity) else None
        model = self.config.model if type(response) is dict and response.get("model") == self.config.model else None
        rate = self.pricing.get(model)
        if type(raw) is dict and "input_tokens_details" in raw and recorded is not None and "input_tokens_details" not in recorded:
            rate = None  # Invalid cache coverage cannot become a known cost.
        self.record("model_response", turn=self.calls, model=model, response_id=safe_id,
            usage=recorded, duration_seconds=round(time.monotonic() - started, 3),
            cost_estimate_usd=estimate_cost(recorded, rate), pricing=rate)
        self.admit()
        try:
            wire, (name, arguments, _) = codex_response(response, model=self.config.model)
            self._pending_action = json.dumps([name, arguments], sort_keys=True, allow_nan=False)
            return wire
        except CodexActionError as exc:
            self.stop("response_policy", response_rejection=exc.reason, response_rejection_detail=exc.detail)
