"""Native harness: the Factory's own tool-using agent loop.

Designed to be forgiving: tool errors are returned to the model instead of aborting, any
number of tool calls per turn is accepted, outputs are clipped head+tail so errors stay
visible, and an invalid final report gets another chance with the exact problem stated.
"""

from __future__ import annotations

import json
from typing import Any

from ..inference import BudgetExceeded, InferenceError
from .base import HarnessFailed, HarnessResult, HarnessSession
from .schemas import validate_report
from .tools import ToolBox, clip, finish_tool, tool_specs

CONTEXT_CHAR_BUDGET = 500_000


class NativeHarness:
    name = "native"
    label = "Native"

    def availability(self) -> tuple[bool, str]:
        return True, "Built-in Factory agent"

    async def run(self, session: HarnessSession) -> HarnessResult:
        toolbox = ToolBox(session)
        finish = finish_tool(session.report_schema)
        tools = tool_specs(session.writable) + [finish]
        messages: list[dict[str, Any]] = [
            {"role": "system", "content": session.system},
            {"role": "user", "content": session.prompt},
        ]
        turns = nudges = report_attempts = 0
        while True:
            if session.cancelled.is_set():
                raise HarnessFailed("Cancelled")
            for note in session.take_guidance():
                messages.append({"role": "user", "content": f"Guidance from your team (takes priority): {note}"})
                session.on_progress("guidance", "Delivered team guidance to the agent", {"text": note[:500]})
            if turns >= session.max_turns + 3:
                raise HarnessFailed(f"Stopped after {turns} turns without a valid final report")
            if session.remaining() <= 0:
                raise HarnessFailed("Time budget exhausted before a final report")
            final_turn = turns >= session.max_turns or session.remaining() < 90
            if final_turn:
                messages.append({"role": "user", "content": (
                    "You are out of turns or time. Call finish now with an honest report of what is done and what is not.")})
            self._trim(messages)
            try:
                message = await session.client.chat(
                    model=session.model,
                    messages=messages,
                    tools=[finish] if final_turn else tools,
                    tool_choice="required" if final_turn else None,
                    ledger=session.ledger,
                    reasoning_effort=session.reasoning_effort,
                )
            except BudgetExceeded as exc:
                raise HarnessFailed(str(exc)) from exc
            except InferenceError as exc:
                raise HarnessFailed(f"Model request failed: {exc}") from exc
            turns += 1
            calls = message.get("tool_calls") or []
            assistant: dict[str, Any] = {"role": "assistant", "content": message.get("content") or ""}
            if calls:
                assistant["tool_calls"] = calls
            messages.append(assistant)
            if message.get("content"):
                session.on_progress("thought", clip(message["content"], 600), {})
            if not calls:
                nudges += 1
                if nudges > 4:
                    raise HarnessFailed("The model stopped calling tools without submitting a report")
                messages.append({"role": "user", "content": "Continue using the tools. When finished, call finish with your report."})
                continue
            accepted: dict[str, Any] | None = None
            for call in calls:
                fn = call.get("function") or {}
                name = fn.get("name", "")
                raw = fn.get("arguments") or "{}"
                try:
                    args = json.loads(raw) if isinstance(raw, str) else dict(raw)
                except (json.JSONDecodeError, TypeError, ValueError):
                    args = None
                if name == "finish" and accepted is None:
                    report_attempts += 1
                    report, error = validate_report(session.role, args)
                    if report is not None:
                        accepted = report
                        output = "Report accepted."
                    elif report_attempts > 3:
                        raise HarnessFailed(f"Final report rejected repeatedly: {error}")
                    else:
                        output = f"Report not accepted: {error} Fix it and call finish again."
                elif name == "finish":
                    output = "Report already accepted."
                else:
                    output = await toolbox.call(name, args)
                messages.append({"role": "tool", "tool_call_id": call.get("id", ""), "content": output})
            if accepted is not None:
                return HarnessResult(report=accepted, turns=turns, transcript=messages[-6:], log="\n".join(toolbox.log))

    def _trim(self, messages: list[dict[str, Any]]) -> None:
        """Keep the conversation within budget by eliding the oldest tool outputs."""
        total = sum(len(str(m.get("content") or "")) for m in messages)
        if total <= CONTEXT_CHAR_BUDGET:
            return
        for message in messages[2:-8]:
            if message.get("role") == "tool" and len(message.get("content") or "") > 400:
                total -= len(message["content"]) - 60
                message["content"] = "[earlier tool output omitted to save context]"
                if total <= CONTEXT_CHAR_BUDGET * 0.7:
                    break
