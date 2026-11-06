"""Factory-owned Codex sessions; selection remains disabled until qualification."""

import asyncio
from contextlib import ExitStack
from importlib.metadata import distribution
from pathlib import Path
import time
from uuid import uuid4

from .codex_inference import CodexInference, CodexStopped
from .codex_process import open_codex_process
from .codex_proxy import open_codex_proxy
from .codex_rpc import codex_turn
from .product_agent import AgentSessionError, HarnessStopped, ProductAgent, execution_role


def validate_codex_dependencies():
    """Reuse the image's reviewed, hash-pinned client dependency set."""
    try:
        for line in Path(__file__).with_name("anton_requirements.txt").read_text().splitlines():
            if line and not line.startswith("#"):
                name, version = line.split()[0].split("==")
                if distribution(name).version != version:
                    raise ValueError("changed dependency")
    except (ValueError, TypeError, OSError, LookupError, ImportError, AttributeError):
        raise AgentSessionError("Codex requires the reviewed pinned client dependencies") from None


async def _drain(awaitable):
    """Complete owned cleanup even if the outer task is cancelled again."""
    task, interrupted = asyncio.ensure_future(awaitable), False
    while not task.done():
        try:
            await asyncio.shield(task)
        except asyncio.CancelledError:
            interrupted = True
    result = task.result()
    if interrupted:
        raise asyncio.CancelledError()
    return result


class CodexAgent:
    def __init__(self, config, journal, *, max_turns, max_total_tokens, deadline, pricing=None, cancelled=lambda: False):
        if config.fallback_model:
            raise AgentSessionError("Codex does not permit fallback models")
        self.config, self.journal = config, journal
        self.max_turns, self.max_total_tokens, self.deadline = max_turns, max_total_tokens, deadline
        self.pricing, self.cancelled = pricing or {}, cancelled

    def run(self, role, context, command):
        instructions = execution_role(self.journal, role)["instructions"]
        if not callable(getattr(command, "abort", None)):
            raise AgentSessionError("Codex requires an abortable owned job command")
        self.config.validated()
        validate_codex_dependencies()
        try:
            return asyncio.run(self._run(role, context, command, instructions))
        except AgentSessionError:
            raise
        except (CodexStopped, asyncio.CancelledError, Exception):
            raise HarnessStopped("Codex session or cleanup stopped without acceptance") from None

    async def _run(self, role, context, command, instructions):
        import openai

        gate = CodexInference(self.config, self.journal, role=role, max_requests=self.max_turns,
            max_total_tokens=self.max_total_tokens, deadline=self.deadline, pricing=self.pricing, cancelled=self.cancelled)
        gate.admit()
        investigated, identity = False, uuid4().hex

        async def run_command(arguments):
            nonlocal investigated
            gate.admit()
            gate.record("agent_tool_requested", tool="run")
            task = asyncio.create_task(asyncio.to_thread(command, arguments))
            try:
                while not task.done():
                    gate.admit()
                    await asyncio.wait({task}, timeout=0.05)
                result = task.result()
            except BaseException:
                if not task.done():
                    try:
                        await _drain(asyncio.to_thread(command.abort))
                    finally:
                        await _drain(asyncio.gather(task, return_exceptions=True))
                raise
            investigated = True
            gate.record("agent_command", **result)
            gate.admit()
            return result

        ownership = ExitStack()
        client = openai.AsyncOpenAI(api_key=self.config.api_key, base_url=self.config.base_url.removesuffix("/responses"),
            organization=self.config.organization, project=self.config.project, max_retries=0)
        try:
            # Exit the proxy (draining provider work) BEFORE waiting for the process.
            async with open_codex_proxy(gate, client) as proxy:
                owner = ownership.enter_context(open_codex_process(proxy.base_url, proxy_token=proxy.token, deadline=gate.deadline))
                gate.record("harness_started", version="0.167.0", session_id=identity,
                    model=self.config.model, resume_supported=False)
                async with asyncio.timeout(max(0, gate.deadline - time.monotonic())):
                    report = await codex_turn(owner, gate, instructions=instructions, context=context, command=run_command)
                    gate.record("agent_tool_requested", tool="finish")
        finally:
            try:
                await _drain(asyncio.to_thread(ownership.close))
            finally:
                await _drain(client.close())
            gate.record("harness_closed", session_id=identity)
        gate.admit()
        ProductAgent._accept_report(self, report, context, investigated)
        gate.record("agent_report", report=report)
        return report
