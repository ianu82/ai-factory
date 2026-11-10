"""Control-plane-only MindsHub access for Mind. No bootstrap or model fallback.

The SDK is imported lazily: a deployment without it is safely unavailable. Every
paid send (including interrupted streams and readiness probes) belongs to the
caller's ledger. No SDK compatibility retries are used on this path.
"""
from __future__ import annotations

import asyncio
import hashlib
import inspect
import json
import re
import time
from contextlib import aclosing
from dataclasses import dataclass, field
from typing import Protocol
from uuid import uuid4

from ..config import get_settings
from ..db import session_scope
from ..harness.anton import _usage
from ..inference import BudgetExceeded, ModelClient, ProviderConfig
from ..inference_limits import admission
from ..models import Secret
from ..security import decrypt, redact


class Ledger(Protocol):
    """check() before each paid send; add() after it. A ledger may also define admit(), called once per send
    right before the request goes out (inside provider admission), instead of a second check(). admit() and
    add() may be coroutines, so a durable ledger can claim and release its lease around each send."""
    def check(self) -> None: ...
    def add(self, model: str, usage: tuple[int, int] | None) -> None: ...


async def _ledger_step(result) -> None:
    if inspect.isawaitable(result):
        await result


REASONS = {
    "missing_key": "Add a saved MindsHub Inference key in workspace Settings.",
    "missing_sdk": "The Anton SDK is not installed on this deployment.",
    "model_absent": "Claude Opus 5.5 is not available in the authenticated model catalog.",
    "ambiguous_catalog": "The model catalog has multiple Claude Opus 5.5 entries.",
    "unsupported_tools": "Claude Opus 5.5 did not complete the required tool-calling probe.",
    "model_substitution": "The provider did not verify the selected Claude Opus 5.5 identity.",
    "provider_failure": "MindsHub Inference is unavailable or refused the request.",
    "budget_exceeded": "Mind reached its request, token, cost or time budget.",
}


class MindUnavailable(RuntimeError):
    """Only fixed public reasons cross the provider boundary; never response bodies."""

    def __init__(self, reason: str = "provider_failure"):
        self.reason = reason if reason in REASONS else "provider_failure"
        super().__init__(REASONS[self.reason])


class _ProviderStop(BaseException):
    """Bypass ChatSession's general repair/retry loop; converted at host boundary."""

    def __init__(self, reason):
        self.reason = reason


@dataclass(frozen=True)
class Credential:
    config: ProviderConfig = field(repr=False)
    version: str


def saved_credential(workspace_id: str) -> Credential:
    """Do not use resolve_provider: even required_provider allows bootstrap keys."""
    try:
        with session_scope() as db:
            secret = db.query(Secret).filter_by(
                workspace_id=workspace_id, kind="mindshub_api_key").one_or_none()
            if secret is None or secret.status == "invalid":
                raise MindUnavailable("missing_key")
            version = hashlib.sha256(
                f"{secret.id}:{secret.updated_at}:{secret.ciphertext}".encode()).hexdigest()
            key = decrypt(secret.ciphertext)
        if not key:
            raise MindUnavailable("missing_key")
        return Credential(ProviderConfig("mindshub", get_settings().mindshub_api_base.rstrip("/"),
                                         key, "MindsHub Inference"), version)
    except MindUnavailable:
        raise
    except Exception:
        raise MindUnavailable() from None


def select_model(names: list[str]) -> str:
    # Match a complete generation, not "5", "5.6", "5.50", or a display-name
    # claim attached to an unrelated id. Return the actual catalog id unchanged.
    pattern = r"(?:anthropic/)?claude[-_ ]opus[-_ ]5[._-]5(?:[-_]\d{8})?"
    matches = {name for name in names if re.fullmatch(pattern, name, re.IGNORECASE)}
    if not matches:
        raise MindUnavailable("model_absent")
    if len(matches) != 1:
        raise MindUnavailable("ambiguous_catalog")
    return matches.pop()


class _Responses:
    """Wrap the native SDK send and stream, not LLMProvider's buffering stream."""

    def __init__(self, resource, config, model, session_id, ledger, max_requests):
        self.resource, self.config, self.model = resource, config, model
        self.session_id, self.ledger = session_id, ledger
        self.max_requests, self.calls = max_requests, 0

    async def create(self, **body):
        if body.get("model") != self.model or not body.get("stream"):
            raise MindUnavailable()
        if self.calls >= self.max_requests:
            raise MindUnavailable("budget_exceeded")
        self.ledger.check()
        body["extra_headers"] = self.config.headers(self.session_id)
        # Retain admission until the stream closes, including cancellation.
        async def events():
            usage = None
            stream = None
            sent = False
            terminal = False
            try:
                async with admission(self.config, self.session_id):
                    await _ledger_step(getattr(self.ledger, "admit", self.ledger.check)())
                    self.calls += 1
                    sent = True
                    try:
                        stream = await self.resource.create(**body)
                    except Exception as exc:
                        if getattr(exc, "status_code", None) in (400, 422) and body.get("tools"):
                            raise MindUnavailable("unsupported_tools") from None
                        raise MindUnavailable() from None
                    async for event in stream:
                        kind = getattr(event, "type", "")
                        response = getattr(event, "response", None)
                        if response is not None:
                            usage = _usage(response) or usage
                            identity = getattr(response, "model", None)
                            if identity and identity != self.model:
                                raise MindUnavailable("model_substitution")
                        if kind in ("response.completed", "response.incomplete"):
                            terminal = True
                            if getattr(response, "model", None) != self.model:
                                raise MindUnavailable("model_substitution")
                        if kind in ("response.failed", "error"):
                            raise MindUnavailable()
                        # Never forward the provider's reasoning, even internally.
                        if "reasoning" not in kind:
                            yield event
                    if not terminal:
                        raise MindUnavailable()
            except (MindUnavailable, BudgetExceeded):
                raise
            except Exception:
                raise MindUnavailable() from None
            finally:
                try:
                    if stream is not None:
                        await stream.close()
                finally:
                    if sent:
                        await _ledger_step(self.ledger.add(self.model, usage))
        return events()


def make_provider(credential: Credential, model: str, session_id: str, ledger: Ledger,
                  *, max_requests: int = 8, forced_tool: str | None = None):
    """Build a private SDK provider; credentials cannot be exported to runtimes."""
    try:
        from anton.core.llm.openai import OpenAIProvider
        from anton.core.llm.provider import ProviderConnectionInfo
    except ImportError:
        raise MindUnavailable("missing_sdk") from None

    class Provider(OpenAIProvider):
        def export_connection_info(self):
            return ProviderConnectionInfo(provider="factory-managed")

        async def current_api_key(self):
            return None

        def native_web_tools(self):
            return set()

        def trace_headers(self):
            return {}

        def log_usage(self, **_kwargs):
            # Accounting is performed at the raw send boundary, not trace sinks.
            pass

        def _build_responses_kwargs(self, **kwargs):
            body = super()._build_responses_kwargs(**kwargs)
            body["store"] = False
            if self._probe_tool:
                body["tool_choice"] = {"type": "function", "name": self._probe_tool}
            elif forced_tool:
                body["tool_choice"] = "none"
            return body

        def sanitize(self, text):
            return redact(text, (config.api_key,))

        async def stream(self, **kwargs):
            from anton.core.llm.provider import StreamComplete, StreamTextDelta, StreamReasoningDelta
            pending = ""
            try:
                async with aclosing(super().stream(**kwargs)) as events:
                    async for event in events:
                        if isinstance(event, StreamReasoningDelta):
                            continue
                        if isinstance(event, StreamTextDelta):
                            pending += event.text
                            pending = pending.replace(config.api_key, "[redacted]")
                            # Hold only a possible split-key prefix, not whole deltas.
                            keep = max((n for n in range(1, min(len(config.api_key), len(pending)) + 1)
                                        if pending.endswith(config.api_key[:n])), default=0)
                            visible = pending[:-keep] if keep else pending
                            pending = pending[-keep:] if keep else ""
                            if visible:
                                yield StreamTextDelta(text=self.sanitize(visible))
                        else:
                            if isinstance(event, StreamComplete):
                                allowed = {tool["name"] for tool in kwargs.get("tools") or []}
                                if any(call.name not in allowed for call in event.response.tool_calls):
                                    raise MindUnavailable("unsupported_tools")
                                event.response.content = self.sanitize(event.response.content)
                                for call in event.response.tool_calls:
                                    call.input = json.loads(self.sanitize(json.dumps(call.input)))
                            if pending and type(event).__name__ == "StreamComplete":
                                yield StreamTextDelta(text="[redacted]")
                                pending = ""
                            yield event
            except MindUnavailable as exc:
                raise _ProviderStop(exc.reason) from None
            except BudgetExceeded:
                raise _ProviderStop("budget_exceeded") from None
            except Exception:
                raise _ProviderStop("provider_failure") from None

        async def _create_response(self, body):
            # Deliberately bypass SDK's optional-parameter compatibility retry.
            return await self._client.responses.create(**body)

    config = credential.config
    provider = Provider(api_key=config.api_key, base_url=config.base_url,
                        flavor=OpenAIProvider.FLAVOR_OPENAI, supports_vision=False)
    provider._probe_tool = forced_tool
    provider._emit_trace_headers = False
    provider._reasoning_summary = False
    provider._client._client.follow_redirects = False
    provider._client = provider._client.with_options(max_retries=0, default_headers=config.headers(session_id))
    provider._client.responses = _Responses(provider._client.responses, config, model, session_id,
                                            ledger, max_requests)
    return provider


@dataclass(frozen=True)
class Readiness:
    available: bool
    reason: str | None = None
    model: str | None = None
    endpoint: str = "responses"
    session_id: str | None = None
    cost_usd: float | None = None


class MindProvider:
    """Brief successful-readiness cache. Always re-read credential version first."""

    def __init__(self, *, ttl: float = 60, timeout: float = 25):
        self.ttl, self.timeout = min(ttl, 60), min(timeout, 30)
        self._ready: dict[tuple, tuple[float, Readiness]] = {}

    async def readiness(self, workspace_id: str, ledger: Ledger, session_id: str | None = None) -> Readiness:
        session_id = session_id or "mind-ready-" + uuid4().hex
        credential = None
        model = None
        cache_hit = False
        result = Readiness(False, "provider_failure", session_id=session_id)
        try:
            credential = saved_credential(workspace_id)
            # A rotation/revocation must not reuse a prior successful probe.
            for key in list(self._ready):
                if self._ready[key][0] <= time.monotonic() or (
                        key[0] == workspace_id and key[1] != credential.version):
                    del self._ready[key]
            async with asyncio.timeout(self.timeout):
                try:
                    import anton.core.session  # noqa: F401
                except ImportError:
                    raise MindUnavailable("missing_sdk") from None
                client = ModelClient(credential.config, max_retries=0, session_id=session_id, log_errors=False)
                model = select_model(await client.list_models())
                key = (workspace_id, credential.version, model, credential.config.base_url, "responses")
                cached = self._ready.get(key)
                if cached and cached[0] > time.monotonic():
                    cache_hit = True
                    return cached[1]
                await self._probe(credential, model, session_id, ledger)
                result = Readiness(True, model=model, session_id=session_id)
        except _ProviderStop as exc:
            result = Readiness(False, exc.reason, model=model, session_id=session_id)
        except BudgetExceeded:
            result = Readiness(False, "budget_exceeded", model=model, session_id=session_id)
        except MindUnavailable as exc:
            result = Readiness(False, exc.reason, model=model, session_id=session_id)
        except Exception:
            result = Readiness(False, "provider_failure", model=model, session_id=session_id)
        finally:
            if credential is not None and not cache_hit:
                # Includes paid failed probes. Unknown price remains unknown.
                cost = None
                try:
                    async with asyncio.timeout(5):
                        cost = await ModelClient(credential.config, max_retries=0,
                                                 log_errors=False).session_cost(session_id)
                except Exception:
                    pass
                result = Readiness(result.available, result.reason, result.model,
                                   session_id=session_id, cost_usd=cost)
        if result.available:
            self._ready[key] = (time.monotonic() + self.ttl, result)
        else:
            self.invalidate(workspace_id)
        return result

    async def _probe(self, credential, model, session_id, ledger):
        from anton.core.llm.provider import StreamComplete

        name = "mind_readiness_ping"
        provider = make_provider(credential, model, session_id, ledger, max_requests=2, forced_tool=name)
        messages = [{"role": "user", "content": "Call mind_readiness_ping once, then say ready after its result."}]
        tools = [{"name": name, "description": "Harmless readiness test; no side effects.",
                  "input_schema": {"type": "object", "properties": {}, "additionalProperties": False}}]

        async def complete():
            response = None
            async with aclosing(provider.stream(model=model, system="Test tool calling only.", messages=messages,
                                                 tools=tools, max_tokens=128)) as events:
                async for event in events:
                    if isinstance(event, StreamComplete):
                        response = event.response
            if response is None or response.model != model:
                raise MindUnavailable("model_substitution")
            return response

        try:
            first = await complete()
            if len(first.tool_calls) != 1 or first.tool_calls[0].name != name or first.tool_calls[0].input != {}:
                raise MindUnavailable("unsupported_tools")
            call = first.tool_calls[0]
            messages.extend([
                {"role": "assistant", "content": [{"type": "tool_use", "id": call.id, "name": name, "input": {}}]},
                {"role": "user", "content": [{"type": "tool_result", "tool_use_id": call.id, "content": "ready"}]},
            ])
            # Same native stream and endpoint, without forcing another function.
            provider._probe_tool = None
            second = await complete()
            if second.tool_calls or not second.content.strip():
                raise MindUnavailable("unsupported_tools")
        finally:
            await provider.aclose()

    def invalidate(self, workspace_id: str) -> None:
        self._ready = {key: value for key, value in self._ready.items() if key[0] != workspace_id}

    async def open(self, workspace_id: str, readiness: Readiness, session_id: str, ledger: Ledger):
        """Recheck saved credentials at execution time; never retain a ready bearer."""
        credential = saved_credential(workspace_id)
        key = (workspace_id, credential.version, readiness.model, credential.config.base_url, "responses")
        cached = self._ready.get(key)
        if not readiness.available or not cached or cached[0] <= time.monotonic() or cached[1] != readiness:
            raise MindUnavailable()
        return make_provider(credential, readiness.model, session_id, ledger)

    async def session_cost(self, workspace_id: str, session_id: str) -> float | None:
        try:
            return await ModelClient(saved_credential(workspace_id).config, max_retries=0,
                                     log_errors=False).session_cost(session_id)
        except Exception:
            return None
