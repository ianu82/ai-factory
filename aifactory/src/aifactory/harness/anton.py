"""Anton harness: MindsDB's Anton agent session driving the shared Factory tool set.

Anton's ChatSession runs in the control plane; its provider is pointed at the workspace's
model provider with per-run accounting. Factory tools and a protected image reader
access the job sandbox. Web tools are explicit policy opt-ins and execute only in
the sandbox; SDK/native web, code-execution runtime, memory and skills stay disabled.
"""

from __future__ import annotations

import asyncio
import tempfile
import time
from dataclasses import replace
from pathlib import Path
from typing import Any
from uuid import uuid4

from ..inference import BudgetExceeded
from ..inference_limits import admission
from .anton_tools import replace_session_tools
from .anton_web import sandbox_web_tools
from .base import HarnessFailed, HarnessResult, HarnessSession, HarnessUnavailable
from .native import budget_warning
from .schemas import validate_report
from .tools import ToolBox, finish_tool, tool_specs


class _ReportReady(BaseException):
    pass


class _Stop(BaseException):
    pass


def _usage(response: Any) -> tuple[int, int] | None:
    usage = getattr(response, "usage", None)
    if usage is None and isinstance(response, dict):
        usage = response.get("usage")
    if usage is None:
        return None
    get = (lambda k: usage.get(k)) if isinstance(usage, dict) else (lambda k: getattr(usage, k, None))
    inp = get("input_tokens") if get("input_tokens") is not None else get("prompt_tokens")
    out = get("output_tokens") if get("output_tokens") is not None else get("completion_tokens")
    if isinstance(inp, int) and isinstance(out, int):
        return inp, out
    return None


class _AdmittedResponses:
    """Keep SDK compatibility retries inside the hook individually admitted."""

    def __init__(self, responses, config, run_id):
        self.responses = responses
        self.config = config
        self.run_id = run_id

    def __getattr__(self, name):
        return getattr(self.responses, name)

    async def create(self, **body):
        async with admission(self.config, self.run_id):
            return await self.responses.create(**body)


async def _create_admitted_response(config, run_id, create, provider, body):
    # The SDK hook can send again directly when an optional parameter is refused.
    # Wrap its resource, not the whole hook, so every actual send is admitted and
    # recursive hook retries never hold a permit while waiting for another one.
    if not isinstance(provider._client.responses, _AdmittedResponses):
        provider._client.responses = _AdmittedResponses(provider._client.responses, config, run_id)
    return await create(provider, body)


class AntonHarness:
    name = "anton"
    label = "Anton"

    def availability(self) -> tuple[bool, str]:
        try:
            import anton.core.session  # noqa: F401
        except Exception:
            return False, "Anton SDK is not installed on this deployment"
        return True, "MindsDB Anton agent with Factory tools"

    async def run(self, session: HarnessSession) -> HarnessResult:
        ok, why = self.availability()
        if not ok:
            raise HarnessUnavailable(why)
        from anton.core.llm.client import LLMClient
        from anton.core.llm.openai import OpenAIProvider
        from anton.core.llm.provider import LLMProvider, ProviderConnectionInfo
        from anton.core.llm.prompt_builder import SystemPromptContext
        from anton.core.session import ChatSession, ChatSessionConfig
        from anton.core.settings import CoreSettings
        from anton.core.tools.tool_defs import ToolDef

        toolbox = ToolBox(session)
        provider_cfg = session.client.provider
        state: dict[str, Any] = {"report": None, "calls": 0, "error": ""}

        class Provider(OpenAIProvider):
            async def _create_response(provider, body):  # noqa: N805 - SDK hook signature
                if session.cancelled.is_set():
                    raise _Stop()
                if state["calls"] >= session.max_turns + 3 or session.remaining() <= 0:
                    state["error"] = "Anton exhausted its turn or time budget"
                    raise _Stop()
                try:
                    session.ledger.check()
                except BudgetExceeded as exc:
                    state["error"] = str(exc)
                    raise _Stop() from exc
                state["calls"] += 1
                response = await _create_admitted_response(
                    provider_cfg, session.run_id, OpenAIProvider._create_response, provider, body)
                session.ledger.add(session.model, _usage(response))
                return response

            async def stream(provider, **kwargs):  # noqa: N805
                async for event in LLMProvider.stream(provider, **kwargs):
                    yield event

            def export_connection_info(provider):  # noqa: N805
                return ProviderConnectionInfo(provider="factory-managed")

            async def current_api_key(provider):  # noqa: N805
                return None

            def native_web_tools(provider):  # noqa: N805
                return set()

        def make_handler(name: str):
            async def handler(_session, arguments):
                for note in session.take_guidance():
                    session.on_progress("guidance", "Delivered team guidance to the agent", {"text": note[:500]})
                    return f"Guidance from your team (takes priority, then retry): {note}"
                return await toolbox.call(name, arguments)
            return handler

        async def finish_handler(_session, arguments):
            report, error = validate_report(session.role, arguments)
            if report is None:
                return f"Report not accepted: {error} Fix it and call finish again."
            state["report"] = report
            raise _ReportReady()

        def budgeted(tool):
            """Anton takes no message mid-turn, so tool results carry the budget: a warning near the limits (as the
            native harness sends) and, past them, a refusal: only finish still runs."""
            run = tool.handler

            async def handler(chat, arguments):
                if state["calls"] > session.max_turns or session.remaining() < 90:
                    return ("You are out of turns or time, so this tool did not run. Call finish now with an honest "
                            "report of what is done and what is not.")
                output = await run(chat, arguments)
                warning = budget_warning(session, state["calls"])
                return f"{output}\n\n{warning}" if warning and isinstance(output, str) else output
            return replace(tool, handler=handler)

        def no_runtime(**_kwargs):
            raise _Stop()

        specs = tool_specs(session.writable) + [finish_tool(session.report_schema)]
        tools = [ToolDef(t["function"]["name"], t["function"]["description"], t["function"]["parameters"],
                         finish_handler if t["function"]["name"] == "finish" else make_handler(t["function"]["name"]))
                 for t in specs]
        tools = replace_session_tools(tools, session.sandbox) + sandbox_web_tools(session)
        tools = [t if t.name == "finish" else budgeted(t) for t in tools]

        class SandboxChatSession(ChatSession):
            def _build_core_tools(chat):
                super()._build_core_tools()
                # The pinned SDK registers built-ins before config.tools and
                # deduplicates by name. Override only this session's registry,
                # including on lazy rebuilds; never patch SDK ToolDefs globally.
                chat.tool_registry.unregister_tool("read_image")
                chat.tool_registry.register_tool(next(t for t in tools if t.name == "read_image"))

        provider = Provider(api_key=provider_cfg.api_key, base_url=provider_cfg.base_url,
                            flavor=OpenAIProvider.FLAVOR_OPENAI, reasoning_effort=session.reasoning_effort)
        try:
            provider._client._client.follow_redirects = False
            provider._client = provider._client.with_options(max_retries=0, default_headers=provider_cfg.headers(session.run_id) | {
                "Authorization": f"Bearer {provider_cfg.api_key}"})
        except AttributeError:
            pass
        provider._emit_trace_headers = False
        client = LLMClient(planning_provider=provider, planning_model=session.model, coding_provider=provider,
                           coding_model=session.model, router_provider=provider, router_model=session.model,
                           max_tokens=16000)
        started = time.monotonic()
        with tempfile.TemporaryDirectory(prefix="factory-anton-") as temporary:
            # Anton's per-turn token ceiling (max_turn_tokens, 1.25M with cache reads at full weight) protects an
            # interactive user's allowance: it pauses to ask them whether to go on. A Factory run has nobody to
            # ask, so it only cost a hand-back call and interrupted the agent mid-task until we said "Continue".
            # The run's ledger already bounds its tokens, cost, calls and time.
            settings = CoreSettings.model_construct(max_tool_rounds=session.max_turns, max_continuations=2,
                                                    verify_min_tool_rounds=session.max_turns + 1, verifier_jev="off",
                                                    max_turn_tokens=0)
            settings = settings.model_copy(update={
                "analytics_enabled": False, "skills_root": Path(temporary) / "skills", "skills_extra_roots": [],
                "skills_artifact_roots": [], "skill_drafts_root": None})
            chat = SandboxChatSession(ChatSessionConfig(
                llm_client=client, settings=settings, runtime_factory=no_runtime, session_id=uuid4().hex,
                harness="anton", tools=tools, tool_allowlist=frozenset(t.name for t in tools),
                background_memory=False, web_search_enabled=False, web_fetch_enabled=False,
                connector_usage_notes={}, system_prompt_context=SystemPromptContext(suffix=session.system),
                output_dir=temporary))
            try:
                async with asyncio.timeout(max(30.0, session.remaining())):
                    prompt = session.prompt
                    for _ in range(4):  # allow continuation if Anton stops without finishing
                        try:
                            events = chat.turn_stream(prompt, enable_interaction=True)
                            try:
                                async for event in events:
                                    text = getattr(event, "text", None) or (event.get("text") if isinstance(event, dict) else None)
                                    if text and len(text) > 40:
                                        session.on_progress("thought", str(text)[:600], {})
                            finally:
                                await events.aclose()
                        except _ReportReady:
                            break
                        if state["report"] is not None:
                            break
                        prompt = "Continue. When the work is complete, call the finish tool with your report."
            except _ReportReady:
                pass
            except _Stop:
                pass
            except TimeoutError:
                state["error"] = "Anton exceeded its time budget"
            finally:
                try:
                    await chat.close()
                except BaseException:
                    pass
                await client.aclose()
        if state["report"] is None:
            raise HarnessFailed(state["error"] or "Anton ended without a valid report")
        session.on_progress("harness", f"Anton finished in {time.monotonic() - started:.0f}s", {})
        return HarnessResult(report=state["report"], turns=state["calls"], log="\n".join(toolbox.log))
