"""Single-request Anton transport admission and accounting, owned by the controller."""

import asyncio
import hashlib
import json
import time

from jsonschema import ValidationError, validate

from .product_agent import COMMAND_SCHEMA, REPORT_SCHEMA, TOOLS
from .product_costs import estimate_cost


class AntonStopped(BaseException):
    """Stop SDK recovery too: a Factory policy stop cannot authorize another call."""


STOP_REASONS = frozenset({"cancelled_or_deadline", "evidence_unavailable", "usage_unknown_or_exhausted",
    "unapproved_model", "unapproved_reasoning", "unsupported_request_tools", "unapproved_tool_schema",
    "request_cancelled", "gateway_policy_mismatch", "model_or_status_mismatch", "unsupported_response_items", "unsupported_tool_calls",
    "malformed_tool_arguments", "request_limit", "scratchpad_blocked", "cleanup_unconfirmed"})


class AntonInference:
    def __init__(self, config, journal, *, role, max_total_tokens, deadline, pricing=None, cancelled=lambda: False):
        self.config, self.journal, self.role = config, journal, role
        self.max_total_tokens, self.deadline = max_total_tokens, deadline
        self.pricing, self.cancelled = pricing or {}, cancelled
        self.calls = 0

    def stop(self, reason):
        # Record the established decision here, never by inspecting mutable exception state.
        safe = reason if type(reason) is str and reason in STOP_REASONS else "unknown"
        self.record("harness_policy_stopped", role=self.role, harness="anton", reason=safe)
        raise AntonStopped("Anton execution stopped by Factory policy")

    def admit(self):
        if self.cancelled() or time.monotonic() >= self.deadline:
            self.stop("cancelled_or_deadline")
        try:
            usage = self.journal.summary()
        except Exception:
            self.stop("evidence_unavailable")
        if not usage["usage_complete"] or usage["total_tokens"] >= self.max_total_tokens:
            self.stop("usage_unknown_or_exhausted")
        return usage

    def record(self, kind, **data):
        try:
            return self.journal.append(kind, **data)
        except Exception:
            raise AntonStopped("Anton execution evidence could not be retained") from None

    async def request(self, client, supplied):
        usage = self.admit()
        if supplied.get("model") != self.config.model:
            self.stop("unapproved_model")
        reasoning = supplied.get("reasoning") or {}
        if type(reasoning) is not dict or reasoning.get("effort", self.config.reasoning_effort) != self.config.reasoning_effort:
            self.stop("unapproved_reasoning")
        tools = supplied.get("tools")
        expected = {tool["name"]: tool["parameters"] for tool in TOOLS}
        if (supplied.get("stream") or type(tools) is not list or len(tools) != 2
                or any(type(tool) is not dict or tool.get("type") != "function"
                       or type(tool.get("name")) is not str or tool["name"] not in expected for tool in tools)
                or {tool["name"] for tool in tools} != set(expected)):
            self.stop("unsupported_request_tools")
        if any(tool.get("parameters") != expected[tool["name"]] for tool in tools):
            self.stop("unapproved_tool_schema")
        remaining = self.deadline - time.monotonic()
        body = {**supplied, "store": False, "parallel_tool_calls": False,
                "tools": json.loads(json.dumps(TOOLS)), "tool_choice": "required",
                "max_output_tokens": min(self.config.max_output_tokens, self.max_total_tokens - usage["total_tokens"]),
                "reasoning": {**reasoning, "effort": self.config.reasoning_effort},
                "include": sorted(set(supplied.get("include", [])) | {"reasoning.encrypted_content"})}
        self.calls += 1
        self.record("model_requested", role=self.role, turn=self.calls, harness="anton",
            model=self.config.model, reasoning_effort=self.config.reasoning_effort,
            max_output_tokens=body["max_output_tokens"],
            input_sha=hashlib.sha256(json.dumps(body, sort_keys=True).encode()).hexdigest())
        started = time.monotonic()
        gateway, connector, raw = None, None, None
        async def create():
            transport = client.with_options(max_retries=0, timeout=min(self.config.timeout_seconds, remaining))
            return await transport.responses.create(**body)
        try:
            if getattr(self.config, "inference_binding_sha256", None) is not None:
                from .mindshub_native import MindsHubNativeConnector
                connector = MindsHubNativeConnector(self.config, cancelled=self.cancelled, deadline=self.deadline)
                # The owned process polls cancellation and retires within this same deadline.
                raw = connector.request_response(body, timeout_seconds=min(self.config.timeout_seconds, remaining))
                gateway = raw.gateway
            else:
                async with asyncio.timeout(min(self.config.timeout_seconds, remaining)):
                    # No SDK retries or Anton compatibility retries can hide extra requests.
                    request = asyncio.create_task(create())
                    try:
                        while not request.done():
                            await asyncio.wait({request}, timeout=0.05)
                            if self.cancelled():
                                self.stop("request_cancelled")
                        response = await request
                    finally:
                        if not request.done():
                            request.cancel()
                            await asyncio.gather(request, return_exceptions=True)
        except BaseException:
            if connector is not None and connector.worker_facts is not None:
                self.record("inference_process", role=self.role, turn=self.calls, **connector.worker_facts)
            self.record("model_failed", role=self.role, turn=self.calls, harness="anton", model=self.config.model)
            raise
        raw_usage = raw.get("usage") if raw is not None else getattr(response, "usage", None)
        recorded = raw_usage if raw is not None else (raw_usage.model_dump() if hasattr(raw_usage, "model_dump") else None)
        if not isinstance(recorded, dict) or any(type(recorded.get(k)) is not int or recorded[k] < 0
                for k in ("input_tokens", "output_tokens", "total_tokens")):
            recorded = None
        elif recorded["total_tokens"] != recorded["input_tokens"] + recorded["output_tokens"]:
            recorded = None
        model = raw.get("model") if raw is not None else getattr(response, "model", None)
        rate = self.pricing.get(model)
        self.record("model_response", role=self.role, turn=self.calls, harness="anton",
            response_id=raw.get("id") if raw is not None else getattr(response, "id", None), model=model, usage=recorded,
            status=raw.get("status") if raw is not None else getattr(response, "status", None), duration_seconds=round(time.monotonic() - started, 3),
            cost_estimate_usd=estimate_cost(recorded, rate), pricing=rate)
        if raw is not None:
            self.record("inference_process", role=self.role, turn=self.calls, **connector.worker_facts)
            if not raw.transport_complete:
                self.record("inference_usage_unknown", role=self.role, turn=self.calls, harness="anton",
                    reason="transport_incomplete")
        if gateway is not None:
            self.record("inference_gateway", role=self.role, turn=self.calls, harness="anton", **gateway)
            if not gateway["single_attempt"] or not gateway["requested_model"]:
                self.record("inference_usage_unknown", role=self.role, turn=self.calls, harness="anton",
                    reason="gateway_attempt_coverage")
            if raw is not None and not raw.transport_complete:
                self.stop("cleanup_unconfirmed")
            if (not gateway["single_attempt"] or not gateway["requested_model"]
                    or any(gateway[k] for k in ("dropped_params", "dropped_content", "clamped_params", "chain_truncated"))):
                self.stop("gateway_policy_mismatch")
        if raw is not None:
            # Match the pinned SDK's recursive loose parsing, only after raw usage is durable.
            from openai.types.responses import Response
            response = Response.model_construct(**raw)
        self.admit()
        if model != self.config.model or getattr(response, "status", None) != "completed":
            self.stop("model_or_status_mismatch")
        output = getattr(response, "output", None)
        if not isinstance(output, list) or any(type(getattr(item, "type", None)) is not str
                or item.type not in {"message", "reasoning", "function_call"} for item in output):
            self.stop("unsupported_response_items")
        calls = [item for item in output if item.type == "function_call"]
        if len(calls) != 1 or any(type(getattr(call, "name", None)) is not str
                                or call.name not in {"run", "finish"} for call in calls):
            self.stop("unsupported_tool_calls")
        for call in calls:
            try:
                validate(json.loads(call.arguments), REPORT_SCHEMA if call.name == "finish" else COMMAND_SCHEMA)
            except (ValueError, TypeError, RecursionError, ValidationError):
                self.stop("malformed_tool_arguments")
        return response
