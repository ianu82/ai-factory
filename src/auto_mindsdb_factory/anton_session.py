"""Pinned embedded Anton sessions with only controller-owned job tools."""

import asyncio
import json
import os
import tempfile
import time
from importlib.metadata import distribution
from pathlib import Path
from uuid import uuid4

from jsonschema import validate

from .anton_inference import AntonInference, AntonStopped
from .product_agent import AgentSessionError, ProductAgent, REPORT_SCHEMA, COMMAND_SCHEMA, TOOLS, execution_role


ANTON_COMMIT = "537cc1fb11849e9d152870738af22ca55c1d59d8"


def validate_anton_installation():
    if any(key.startswith("ANTON_") for key in os.environ):
        raise AgentSessionError("Anton environment overrides are unsupported in Factory sessions")
    try:
        metadata = json.loads(distribution("anton-agent").read_text("direct_url.json") or "{}")
        if (type(metadata) is not dict
                or metadata.get("url", "").removesuffix(".git") != "https://github.com/mindsdb/anton"
                or metadata.get("vcs_info", {}).get("commit_id") != ANTON_COMMIT):
            raise ValueError("unreviewed installation")
    except (ValueError, TypeError, OSError, LookupError, ImportError, AttributeError):
        raise AgentSessionError("Anton requires the reviewed pinned SDK installation") from None


class _ReportReady(BaseException):
    pass


class AntonAgent:
    def __init__(self, config, journal, *, max_turns, max_total_tokens, deadline, pricing=None, cancelled=lambda: False):
        if config.fallback_model:
            raise AgentSessionError("Anton does not permit fallback models")
        self.config, self.journal = config, journal
        self.max_turns, self.max_total_tokens, self.deadline = max_turns, max_total_tokens, deadline
        self.pricing, self.cancelled = pricing or {}, cancelled

    def run(self, role, context, command):
        instructions = execution_role(self.journal, role)["instructions"]
        validate_anton_installation()
        try:
            return asyncio.run(self._run(role, context, command, instructions))
        except AntonStopped:
            raise AgentSessionError("Anton execution stopped by Factory policy") from None

    async def _run(self, role, context, command, instructions):
        from anton.core.session import ChatSession, ChatSessionConfig
        from anton.core.settings import CoreSettings
        from anton.core.llm.client import LLMClient
        from anton.core.llm.openai import OpenAIProvider
        from anton.core.llm.provider import LLMProvider, ProviderConnectionInfo
        from anton.core.llm.prompt_builder import SystemPromptContext
        from anton.core.tools.tool_defs import ToolDef

        gate = AntonInference(self.config, self.journal, role=role, max_total_tokens=self.max_total_tokens,
                              deadline=self.deadline, pricing=self.pricing, cancelled=self.cancelled)
        gate.admit()
        report, investigated = None, False

        class Provider(OpenAIProvider):
            async def _create_response(provider, body):
                if gate.calls >= self.max_turns:
                    raise AntonStopped("Anton request-count budget exhausted")
                return await gate.request(provider._client, body)

            async def stream(provider, **kwargs):
                # Use the SDK's non-streaming adapter: no partial tool arguments can execute.
                async for event in LLMProvider.stream(provider, **kwargs):
                    yield event

            def export_connection_info(provider):
                return ProviderConnectionInfo(provider="factory-managed")

            async def current_api_key(provider):
                return None

            def native_web_tools(provider):
                return set()

        async def run_tool(session, arguments):
            nonlocal investigated
            gate.admit()
            validate(arguments, COMMAND_SCHEMA)
            result = command(arguments["command"])
            investigated = True
            self.journal.append("agent_command", role=role, **result)
            return json.dumps(result)

        async def finish_tool(session, arguments):
            nonlocal report
            gate.admit()
            validate(arguments, REPORT_SCHEMA)
            # Use the same acceptance rules as the native backend.
            ProductAgent._accept_report(self, arguments, context, investigated)
            report = json.loads(json.dumps(arguments))
            raise _ReportReady()

        def no_scratchpad(**kwargs):
            raise AntonStopped("Native scratchpad execution is disabled; use the isolated job tool")

        provider = Provider(api_key=self.config.api_key,
            base_url=self.config.base_url.removesuffix("/responses"), flavor=OpenAIProvider.FLAVOR_OPENAI,
            reasoning_effort=self.config.reasoning_effort)
        provider._emit_trace_headers = False
        provider._client = provider._client.with_options(organization=self.config.organization, project=self.config.project)
        client = LLMClient(planning_provider=provider, planning_model=self.config.model,
            coding_provider=provider, coding_model=self.config.model, router_provider=provider,
            router_model=self.config.model, max_tokens=self.config.max_output_tokens)
        with tempfile.TemporaryDirectory(prefix="factory-anton-session-") as temporary:
            # model_construct does not discover ANTON_* env, a home config or candidate .env.
            settings = CoreSettings.model_construct(max_tool_rounds=self.max_turns, max_continuations=0,
                verify_min_tool_rounds=self.max_turns + 1, verifier_jev="off", max_turn_tokens=self.max_total_tokens)
            settings = settings.model_copy(update={"analytics_enabled": False, "skills_root": Path(temporary) / "skills",
                "skills_extra_roots": [], "skills_artifact_roots": [], "skill_drafts_root": None})
            try:
                session = ChatSession(ChatSessionConfig(llm_client=client, settings=settings,
                    runtime_factory=no_scratchpad, session_id=uuid4().hex, harness="anton",
                    tools=[ToolDef(t["name"], t["description"], t["parameters"],
                        run_tool if t["name"] == "run" else finish_tool) for t in TOOLS],
                    tool_allowlist=frozenset({"run", "finish"}), background_memory=False,
                    web_search_enabled=False, web_fetch_enabled=False, connector_usage_notes={},
                    system_prompt_context=SystemPromptContext(suffix=instructions), output_dir=temporary))
            except BaseException:
                await client.aclose()
                raise
            try:
                self.journal.append("harness_started", role=role, harness="anton", version=ANTON_COMMIT,
                    session_id=session._session_id, model=self.config.model, resume_supported=False)
                async with asyncio.timeout(max(0, self.deadline - time.monotonic())):
                    try:
                        # Generic tools require the supported event-stream emitter, even headless.
                        events = session.turn_stream(json.dumps(context), enable_interaction=True)
                        try:
                            async for event in events:
                                pass
                        finally:
                            await events.aclose()
                    except _ReportReady:
                        pass
            finally:
                await session.close()
            if report is None:
                raise AgentSessionError("Anton ended without a validated criterion report")
            self.journal.append("agent_report", role=role, report=report)
            return report
