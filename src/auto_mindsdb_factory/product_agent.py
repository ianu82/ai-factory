"""Bounded tool sessions; inference credentials remain in the controller."""

from __future__ import annotations

import hashlib
import json
import time
from typing import Any, Callable

from jsonschema import validate, ValidationError

from .connectors import OpenAIResponsesAgentConnector
from .run_journal import RunJournal
from .product_costs import estimate_cost
from .product_roles import RoleDefinitionError, stored_role_snapshots


class AgentSessionError(RuntimeError):
    pass


class HarnessStopped(AgentSessionError):
    """An interruption or evidence fault must not start another implementation attempt."""


class RoleSnapshotError(AgentSessionError):
    """Execution cannot proceed with missing or untrusted historical roles."""


def execution_role(journal: RunJournal, role: str) -> dict:
    if role not in ("builder", "verifier"):
        raise AgentSessionError("Unknown product agent role")
    try:
        manifest = json.loads((journal.root / "manifest.json").read_text(encoding="utf-8"))
        return stored_role_snapshots(manifest)[role]
    except RoleDefinitionError as exc:
        raise RoleSnapshotError(f"Invalid execution role snapshot: {exc}") from exc
    except (OSError, ValueError) as exc:
        raise RoleSnapshotError("Missing or unreadable execution manifest") from exc


STATUS = {"type": "string", "enum": ["passed", "failed", "unverified"]}
REPORT_SCHEMA = {
    "type": "object", "additionalProperties": False,
    "required": ["status", "summary", "criteria", "findings"],
    "properties": {
        "status": STATUS, "summary": {"type": "string"},
        "findings": {"type": "array", "description": "Unresolved material blocking defects only. Return an empty array when none remain; put observations in summary or criterion evidence.", "items": {"type": "string"}},
        "criteria": {"type": "array", "items": {
            "type": "object", "additionalProperties": False,
            "required": ["id", "status", "evidence"],
            "properties": {"id": {"type": "string"}, "status": STATUS, "evidence": {"type": "string"}},
        }},
    },
}
COMMAND_SCHEMA = {
    "type": "object", "additionalProperties": False, "required": ["command"],
    "properties": {"command": {"type": "array", "minItems": 1, "items": {"type": "string"}}},
}
TOOLS = [
    {"type": "function", "name": "run", "description": "Run command arguments in /workspace. Use sh -c for shell operations. Output is bounded; narrow large reads.", "parameters": COMMAND_SCHEMA, "strict": True},
    {"type": "function", "name": "finish", "description": "Finish with evidence for every supplied acceptance criterion. Report anything not established as unverified.", "parameters": REPORT_SCHEMA, "strict": True},
]


class ProductAgent:
    report_schema = REPORT_SCHEMA
    tools = TOOLS
    report_event = "agent_report"

    def __init__(self, connector: OpenAIResponsesAgentConnector, journal: RunJournal,
                 *, max_turns: int, max_total_tokens: int, deadline: float, pricing: dict | None = None,
                 cancelled: Callable[[], bool] = lambda: False) -> None:
        self.connector = connector
        self.journal = journal
        self.max_turns = max_turns
        self.max_total_tokens = max_total_tokens
        self.deadline = deadline
        self.pricing = pricing or {}
        self.cancelled = cancelled

    def _instructions(self, role: str) -> str:
        return execution_role(self.journal, role)["instructions"]

    def _turn_policy(self, turn: int, investigated: bool) -> tuple[list, dict | None]:
        return self.tools, None

    def _accept_report(self, arguments: dict, context: dict, ran_command: bool) -> None:
        expected = {criterion["id"] for criterion in context["acceptance_criteria"]}
        actual = [criterion["id"] for criterion in arguments["criteria"]]
        if set(actual) != expected or len(actual) != len(expected) or not ran_command:
            raise AgentSessionError("Agent must investigate and report every acceptance criterion exactly once")
        if arguments["status"] == "passed" and (
            arguments["findings"] or any(c["status"] != "passed" or not c["evidence"].strip() for c in arguments["criteria"])
        ):
            raise AgentSessionError("Passed report contains unresolved findings or missing evidence")

    def run(self, role: str, context: dict[str, Any], command: Callable[[list[str]], dict]) -> dict:
        # Only executable roles and trusted historical manifest content can supply instructions.
        instructions = self._instructions(role)
        items: list[dict] = [{"role": "user", "content": json.dumps(context)}]
        ran_command = False
        for turn in range(self.max_turns):
            if self.cancelled():
                raise HarnessStopped("Agent execution was cancelled")
            if time.monotonic() >= self.deadline:
                raise AgentSessionError("Run time budget exhausted")
            usage = self.journal.summary()
            if not usage["usage_complete"]:
                raise AgentSessionError("Prior model usage is unknown; cannot continue within a token budget")
            if usage["total_tokens"] >= self.max_total_tokens:
                raise AgentSessionError("Run token budget exhausted")
            tools, guidance = self._turn_policy(turn, ran_command)
            body = {
                "model": self.connector.config.model, "instructions": instructions,
                "input": items if guidance is None else [*items, {"role": "developer", "content": json.dumps(guidance)}],
                "tools": tools, "parallel_tool_calls": False, "tool_choice": "required",
                "max_output_tokens": min(self.connector.config.max_output_tokens, self.max_total_tokens - usage["total_tokens"]),
                "reasoning": {"effort": self.connector.config.reasoning_effort},
                "store": False, "include": ["reasoning.encrypted_content"],
            }
            self.journal.append("model_requested", role=role, turn=turn + 1,
                model=body["model"], reasoning_effort=self.connector.config.reasoning_effort,
                max_output_tokens=body["max_output_tokens"],
                input_sha=hashlib.sha256(json.dumps(body, sort_keys=True).encode()).hexdigest())
            started = time.monotonic()
            try:
                payload = self.connector.request_response(body, timeout_seconds=min(
                    self.connector.config.timeout_seconds, max(1, self.deadline - time.monotonic())))
            except Exception:
                self.journal.append("model_failed", role=role, turn=turn + 1, model=body["model"])
                raise
            rate = self.pricing.get(payload.get("model"))
            self.journal.append("model_response", role=role, turn=turn + 1,
                response_id=payload.get("id"), model=payload.get("model"), usage=payload.get("usage"),
                status=payload.get("status"), incomplete_details=payload.get("incomplete_details"),
                duration_seconds=round(time.monotonic() - started, 3),
                cost_estimate_usd=estimate_cost(payload.get("usage"), rate), pricing=rate)
            if getattr(self.connector.config, "inference_binding_sha256", None) is not None:
                from .mindshub_native import MindsHubPayload
                from .mindshub_transport import gateway_evidence
                complete = type(payload) is MindsHubPayload and payload.transport_complete
                gateway = payload.gateway if type(payload) is MindsHubPayload else gateway_evidence({})
                self.journal.append("inference_gateway", role=role, turn=turn + 1, harness="native", transport_complete=complete, **gateway)
                if not gateway["single_attempt"] or not gateway["requested_model"]:
                    self.journal.append("inference_usage_unknown", role=role, turn=turn + 1,
                        harness="native", reason="gateway_attempt_coverage")
                if (not complete or not gateway["single_attempt"] or not gateway["requested_model"]
                        or any(gateway[k] for k in ("dropped_params", "dropped_content", "clamped_params", "chain_truncated"))
                        or payload.get("model") != self.connector.config.model):
                    raise HarnessStopped("MindsHub gateway policy mismatch")
            if self.cancelled():
                raise HarnessStopped("Agent execution was cancelled after an observed response")
            if payload.get("status") != "completed":
                raise AgentSessionError("Model response did not complete")
            if not self.journal.summary()["usage_complete"]:
                raise AgentSessionError("Model usage unavailable; cannot enforce run token budget")
            if self.journal.summary()["total_tokens"] > self.max_total_tokens:
                raise AgentSessionError("Run token budget exhausted after response")
            output = payload.get("output")
            if not isinstance(output, list):
                raise AgentSessionError("Model returned invalid output")
            # Preserve reasoning items as required for stateless Responses tool continuations.
            items.extend(output)
            calls = [item for item in output if item.get("type") == "function_call"]
            if len(calls) != 1:
                raise AgentSessionError("Expected exactly one tool call")
            call = calls[0]
            self.journal.append("agent_tool_requested", role=role, tool=call.get("name"), arguments=call.get("arguments"))
            if call.get("name") not in {tool["name"] for tool in tools}:
                raise AgentSessionError("Agent requested a tool unavailable for this turn")
            try:
                arguments = json.loads(call["arguments"])
                validate(arguments, self.report_schema if call["name"] == "finish" else COMMAND_SCHEMA)
            except (KeyError, ValueError, ValidationError) as exc:
                raise AgentSessionError("Invalid tool arguments") from exc
            if call["name"] == "finish":
                self._accept_report(arguments, context, ran_command)
                self.journal.append(self.report_event, role=role, report=arguments)
                return arguments
            if call["name"] != "run":
                raise AgentSessionError("Unknown agent tool")
            result = command(arguments["command"])
            ran_command = True
            self.journal.append("agent_command", role=role, **result)
            items.append({"type": "function_call_output", "call_id": call["call_id"], "output": json.dumps(result)})
        raise AgentSessionError("Agent turn budget exhausted")
