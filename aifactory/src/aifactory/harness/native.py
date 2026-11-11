"""Native harness: the Factory's own tool-using agent loop.

Designed to be forgiving: tool errors are returned to the model instead of aborting, any
number of tool calls per turn is accepted, outputs are clipped head+tail so errors stay
visible, and an invalid final report gets another chance with the exact problem stated.
"""

from __future__ import annotations

import json
from typing import Any

from ..attachments import IMAGE_EXTENSIONS, decode_content
from ..image_input import ImagePayloadRedactor, rejects_image_input
from ..inference import BudgetExceeded, InferenceError
from ..pipeline.image_validation import validate_image
from .base import HarnessFailed, HarnessResult, HarnessSession
from .schemas import validate_report
from .tools import ToolBox, clip, finish_tool, tool_specs

CONTEXT_CHAR_BUDGET = 500_000


def budget_warning(session: HarnessSession, turns: int) -> str:
    """What to tell the agent once under 10 minutes or 10 turns are left of its session; empty before that."""
    seconds, left = session.remaining(), session.max_turns - turns
    if seconds >= 600 and left >= 10:
        return ""
    return (f"Budget: about {max(int(seconds // 60), 0)} min and {max(left, 0)} turns are left in this session. "
            "Wrap up what you are doing and call finish with an honest report before either runs out.")


class NativeHarness:
    name = "native"
    label = "Native"

    def availability(self) -> tuple[bool, str]:
        return True, "Built-in Factory agent"

    async def run(self, session: HarnessSession) -> HarnessResult:
        redactor = ImagePayloadRedactor(session.image_inputs)
        session.client.image_redactor = redactor
        original_progress = session.on_progress

        def safe_progress(kind, message, data):
            original_progress(kind, redactor.text(message), redactor.value(data))

        session.on_progress = safe_progress
        toolbox = ToolBox(session, redactor=redactor)
        content: Any = session.prompt
        if session.image_inputs:
            content = [{"type": "text", "text": session.prompt}]
            for image in session.image_inputs:
                try:
                    mime = image["mime_type"]
                    if mime not in IMAGE_EXTENSIONS.values():
                        raise ValueError("Unsupported image type")
                    validate_image(decode_content(image["content"]), mime)
                except (ValueError, KeyError):
                    raise HarnessFailed("Invalid session image input") from None
                content.append({"type": "image_url", "image_url": {
                    "url": f"data:{mime};base64,{image['content']}"}})
        files_only = False
        finish = finish_tool(session.report_schema)
        tools = tool_specs(session.writable) + [finish]
        messages: list[dict[str, Any]] = [
            {"role": "system", "content": session.system},
            {"role": "user", "content": content},
        ]
        turns = nudges = report_attempts = 0
        while True:
            if session.cancelled.is_set():
                raise HarnessFailed("Cancelled")
            for note in session.take_guidance():
                messages.append({"role": "user", "content": f"Guidance from your team (takes priority): {note}"})
                session.on_progress("guidance", "Delivered team guidance to the agent", {"text": redactor.text(note)[:500]})
            if turns >= session.max_turns + 3:
                raise HarnessFailed(f"Stopped after {turns} turns without a valid final report")
            if session.remaining() <= 0:
                raise HarnessFailed("Time budget exhausted before a final report")
            final_turn = turns >= session.max_turns or session.remaining() < 90
            if final_turn:
                messages.append({"role": "user", "content": (
                    "You are out of turns or time. Call finish now with an honest report of what is done and what is not.")})
            elif warning := budget_warning(session, turns):
                messages.append({"role": "user", "content": warning})
            self._trim(messages)
            while True:
                try:
                    message = await session.client.chat(
                        model=session.model, messages=messages,
                        tools=[finish] if final_turn else tools,
                        tool_choice="required" if final_turn else None,
                        ledger=session.ledger, reasoning_effort=session.reasoning_effort,
                    )
                    break
                except BudgetExceeded as exc:
                    raise HarnessFailed(redactor.text(str(exc))) from None
                except InferenceError as exc:
                    if session.image_inputs and not files_only and rejects_image_input(exc):
                        files_only = True
                        for entry in messages:
                            if isinstance(entry.get("content"), list):
                                entry["content"] = [p for p in entry["content"] if p.get("type") != "image_url"]
                        session.on_progress("warning", "The provider does not support image input; continuing with attachment files and text only.", {})
                        continue
                    raise HarnessFailed(f"Model request failed: {redactor.text(str(exc))}") from None
            turns += 1
            calls = message.get("tool_calls") or []
            assistant: dict[str, Any] = {"role": "assistant", "content": message.get("content") or ""}
            if calls:
                assistant["tool_calls"] = calls
            if message.get("_raw"):
                assistant["_raw"] = message["_raw"]  # Responses API: replay reasoning items verbatim
            messages.append(assistant)
            if message.get("content"):
                session.on_progress("thought", clip(redactor.text(message["content"]), 600), {})
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
                    report, error = validate_report(session.role, redactor.value(args))
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
                return HarnessResult(report=accepted, turns=turns, transcript=redactor.value(messages[-6:]),
                                     log=redactor.text("\n".join(toolbox.log)))

    def _trim(self, messages: list[dict[str, Any]]) -> None:
        """Keep the conversation within budget by eliding the oldest tool outputs."""
        def text_length(content):
            if isinstance(content, list):
                return sum(len(p.get("text", "")) for p in content if p.get("type") == "text")
            return len(content or "")

        total = sum(text_length(m.get("content")) for m in messages)
        if total <= CONTEXT_CHAR_BUDGET:
            return
        for message in messages[2:-8]:
            if message.get("role") == "tool" and len(message.get("content") or "") > 400:
                total -= len(message["content"]) - 60
                message["content"] = "[earlier tool output omitted to save context]"
                if total <= CONTEXT_CHAR_BUDGET * 0.7:
                    break
