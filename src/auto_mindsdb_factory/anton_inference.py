"""Single-request Anton transport admission and accounting, owned by the controller."""

import asyncio
import hashlib
import json
import time

from jsonschema import ValidationError, validate

from .product_agent import COMMAND_SCHEMA, REPORT_SCHEMA
from .product_costs import estimate_cost


class AntonStopped(BaseException):
    """Stop SDK recovery too: a Factory policy stop cannot authorize another call."""


class AntonInference:
    def __init__(self, config, journal, *, role, max_total_tokens, deadline, pricing=None, cancelled=lambda: False):
        self.config, self.journal, self.role = config, journal, role
        self.max_total_tokens, self.deadline = max_total_tokens, deadline
        self.pricing, self.cancelled = pricing or {}, cancelled
        self.calls = 0

    def admit(self):
        if self.cancelled() or time.monotonic() >= self.deadline:
            raise AntonStopped("Anton execution cancelled or timed out")
        try:
            usage = self.journal.summary()
        except Exception:
            raise AntonStopped("Anton execution evidence is unavailable") from None
        if not usage["usage_complete"] or usage["total_tokens"] >= self.max_total_tokens:
            raise AntonStopped("Anton usage is unknown or its shared budget is exhausted")
        return usage

    def record(self, kind, **data):
        try:
            return self.journal.append(kind, **data)
        except Exception:
            raise AntonStopped("Anton execution evidence could not be retained") from None

    async def request(self, client, supplied):
        usage = self.admit()
        if supplied.get("model") != self.config.model:
            raise AntonStopped("Anton attempted an unapproved model")
        reasoning = supplied.get("reasoning") or {}
        if reasoning.get("effort", self.config.reasoning_effort) != self.config.reasoning_effort:
            raise AntonStopped("Anton attempted an unapproved reasoning setting")
        if supplied.get("stream") or any(tool.get("type") != "function" or tool.get("name") not in {"run", "finish"}
                                         for tool in supplied.get("tools", [])):
            raise AntonStopped("Anton attempted an unsupported transport or tool")
        remaining = self.deadline - time.monotonic()
        body = {**supplied, "store": False, "parallel_tool_calls": False,
                "max_output_tokens": min(self.config.max_output_tokens, self.max_total_tokens - usage["total_tokens"]),
                "reasoning": {**reasoning, "effort": self.config.reasoning_effort},
                "include": sorted(set(supplied.get("include", [])) | {"reasoning.encrypted_content"})}
        self.calls += 1
        self.record("model_requested", role=self.role, turn=self.calls, harness="anton",
            model=self.config.model, reasoning_effort=self.config.reasoning_effort,
            max_output_tokens=body["max_output_tokens"],
            input_sha=hashlib.sha256(json.dumps(body, sort_keys=True).encode()).hexdigest())
        started = time.monotonic()
        try:
            async with asyncio.timeout(min(self.config.timeout_seconds, remaining)):
                # No SDK retries or Anton compatibility retries can hide extra requests.
                request = asyncio.create_task(client.with_options(max_retries=0, timeout=min(
                    self.config.timeout_seconds, remaining)).responses.create(**body))
                try:
                    while not request.done():
                        await asyncio.wait({request}, timeout=0.05)
                        if self.cancelled():
                            raise AntonStopped("Anton execution cancelled")
                    response = await request
                finally:
                    if not request.done():
                        request.cancel()
                        await asyncio.gather(request, return_exceptions=True)
        except BaseException:
            self.record("model_failed", role=self.role, turn=self.calls, harness="anton", model=self.config.model)
            raise
        raw_usage = getattr(response, "usage", None)
        recorded = raw_usage.model_dump() if hasattr(raw_usage, "model_dump") else None
        if not isinstance(recorded, dict) or any(type(recorded.get(k)) is not int or recorded[k] < 0
                for k in ("input_tokens", "output_tokens", "total_tokens")):
            recorded = None
        elif recorded["total_tokens"] != recorded["input_tokens"] + recorded["output_tokens"]:
            recorded = None
        model = getattr(response, "model", None)
        rate = self.pricing.get(model)
        self.record("model_response", role=self.role, turn=self.calls, harness="anton",
            response_id=getattr(response, "id", None), model=model, usage=recorded,
            status=getattr(response, "status", None), duration_seconds=round(time.monotonic() - started, 3),
            cost_estimate_usd=estimate_cost(recorded, rate), pricing=rate)
        self.admit()
        if model != self.config.model or getattr(response, "status", None) != "completed":
            raise AntonStopped("Anton model response did not complete")
        output = getattr(response, "output", None)
        if not isinstance(output, list) or any(getattr(item, "type", None) not in
                {"message", "reasoning", "function_call"} for item in output):
            raise AntonStopped("Anton returned unsupported response items")
        calls = [item for item in output if item.type == "function_call"]
        if len(calls) > 1 or any(getattr(call, "name", None) not in {"run", "finish"} for call in calls):
            raise AntonStopped("Anton returned unsupported tool calls")
        for call in calls:
            try:
                validate(json.loads(call.arguments), REPORT_SCHEMA if call.name == "finish" else COMMAND_SCHEMA)
            except (ValueError, TypeError, ValidationError):
                raise AntonStopped("Anton returned malformed tool arguments") from None
        return response
