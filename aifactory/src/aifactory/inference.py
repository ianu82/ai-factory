"""Model access: provider resolution, an OpenAI-compatible client, usage and cost accounting.

Every model call made for a run — by the native harness directly, or by Codex/Anton through
the gateway — is attributed to that run. Provider keys come from the workspace's encrypted
secrets (MindsHub Inference first) and never enter a sandbox.
"""

from __future__ import annotations

import asyncio
import json
import logging
import random
from dataclasses import dataclass, field
from typing import Any

import httpx

from .config import get_settings
from .inference_limits import admission
from .db import session_scope
from .models import Run, Secret, Workspace
from .security import decrypt

log = logging.getLogger(__name__)

PROVIDERS = {
    "mindshub": {"secret": "mindshub_api_key", "label": "MindsHub Inference"},
    "openai": {"secret": "openai_api_key", "label": "OpenAI"},
}

# USD per million tokens (input, output). Workspaces can override in settings["pricing"].
DEFAULT_PRICING: dict[str, tuple[float, float]] = {}

class InferenceError(RuntimeError):
    def __init__(self, message: str, *, retryable: bool = False, status: int | None = None):
        super().__init__(message)
        self.retryable = retryable
        self.status = status


class BudgetExceeded(InferenceError):
    pass


@dataclass
class ProviderConfig:
    provider: str
    base_url: str
    api_key: str
    label: str
    # Set when a server bootstrap credential for a different provider is used, so roles
    # get a model that provider actually serves (recorded on every run).
    model_override: str | None = None

    def headers(self, session_id: str | None = None) -> dict[str, str]:
        headers = {"Authorization": f"Bearer {self.api_key}", "Content-Type": "application/json"}
        if self.provider == "mindshub":
            # Honest model identity: never let the gateway silently substitute another model.
            headers["X-MindsHub-Allow-Failover"] = "false"
            if session_id:
                # Groups requests so per-run cost can be read back from MindsHub usage.
                headers["Langfuse-Session-Id"] = session_id
        return headers


@dataclass
class ModelSelection:
    planner: str
    builder: str
    verifier: str


def workspace_models(settings: dict[str, Any]) -> ModelSelection:
    models = settings.get("models") or {}
    default = models.get("default") or "gpt-terra"
    return ModelSelection(
        planner=models.get("planner") or default,
        builder=models.get("builder") or default,
        verifier=models.get("verifier") or default,
    )


def resolve_provider(workspace_id: str) -> ProviderConfig:
    """Pick the workspace's configured provider and decrypt its key (control plane only)."""
    settings = get_settings()
    with session_scope() as s:
        ws = s.get(Workspace, workspace_id)
        if ws is None:
            raise InferenceError("Workspace not found")
        preferred = (ws.settings or {}).get("provider") or "mindshub"
        secrets = {sec.kind: sec for sec in s.query(Secret).filter(Secret.workspace_id == workspace_id)}
        order = [preferred] + [p for p in PROVIDERS if p != preferred]
        for provider in order:
            sec = secrets.get(PROVIDERS[provider]["secret"])
            if sec is not None and sec.status != "invalid":
                key = decrypt(sec.ciphertext)
                break
        else:
            key, provider = "", preferred
    override = None
    if not key:
        # Server-level bootstrap keys (explicitly configured by the operator) are a fallback.
        if preferred == "mindshub" and settings.bootstrap_mindshub_key:
            key, provider = settings.bootstrap_mindshub_key, "mindshub"
        elif settings.bootstrap_openai_key:
            key, provider = settings.bootstrap_openai_key, "openai"
        elif settings.bootstrap_mindshub_key:
            key, provider = settings.bootstrap_mindshub_key, "mindshub"
        if key and provider != preferred:
            import os

            override = os.environ.get(f"AIFACTORY_BOOTSTRAP_{provider.upper()}_MODEL") or (
                "gpt-6.1-terra" if provider == "openai" else "gpt-terra")
    if not key:
        raise InferenceError(
            "No model provider is configured for this workspace. Add a MindsHub Inference key in Settings."
        )
    base = settings.mindshub_api_base if provider == "mindshub" else settings.openai_api_base
    return ProviderConfig(provider=provider, base_url=base, api_key=key, label=PROVIDERS[provider]["label"],
                          model_override=override)


def price_for(model: str, workspace_settings: dict[str, Any] | None = None) -> tuple[float, float] | None:
    overrides = (workspace_settings or {}).get("pricing") or {}
    if model in overrides:
        value = overrides[model]
        return float(value.get("input", 0)), float(value.get("output", 0))
    if model in DEFAULT_PRICING:
        return DEFAULT_PRICING[model]
    base = model.split("/")[-1]
    return DEFAULT_PRICING.get(base)


def extract_usage(payload: dict[str, Any]) -> tuple[int, int] | None:
    usage = payload.get("usage") if isinstance(payload, dict) else None
    if not isinstance(usage, dict):
        response = payload.get("response") if isinstance(payload, dict) else None
        usage = response.get("usage") if isinstance(response, dict) else None
    if not isinstance(usage, dict):
        return None
    inp = usage.get("prompt_tokens", usage.get("input_tokens"))
    out = usage.get("completion_tokens", usage.get("output_tokens"))
    if isinstance(inp, int) and isinstance(out, int):
        return inp, out
    return None


@dataclass
class UsageLedger:
    """Accumulates usage for a run and enforces its token/cost budget."""

    run_id: str
    workspace_settings: dict[str, Any]
    max_cost_usd: float | None = None
    max_tokens: int | None = None
    input_tokens: int = 0
    output_tokens: int = 0
    cost_usd: float = 0.0
    cost_known: bool = True
    calls: int = 0
    _lock: asyncio.Lock = field(default_factory=asyncio.Lock)

    def check(self) -> None:
        if self.max_tokens is not None and self.input_tokens + self.output_tokens >= self.max_tokens:
            raise BudgetExceeded(f"Token budget of {self.max_tokens:,} reached")
        if self.max_cost_usd is not None and self.cost_known and self.cost_usd >= self.max_cost_usd:
            raise BudgetExceeded(f"Cost budget of ${self.max_cost_usd:.2f} reached")

    def add(self, model: str, usage: tuple[int, int] | None) -> None:
        self.calls += 1
        if usage is None:
            self.cost_known = False
        else:
            inp, out = usage
            self.input_tokens += inp
            self.output_tokens += out
            price = price_for(model, self.workspace_settings)
            if price is None:
                self.cost_known = False
            else:
                self.cost_usd += inp / 1e6 * price[0] + out / 1e6 * price[1]
        self.persist()

    def persist(self) -> None:
        with session_scope() as s:
            run = s.get(Run, self.run_id)
            if run is not None:
                run.input_tokens = self.input_tokens
                run.output_tokens = self.output_tokens
                run.cost_usd = round(self.cost_usd, 6)
                run.cost_known = self.cost_known
                run.turns = self.calls


def to_responses_input(messages: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Translate a chat-style transcript into stateless Responses API input items."""
    items: list[dict[str, Any]] = []
    for m in messages:
        role = m.get("role")
        if role == "assistant" and m.get("_raw"):
            items.extend(m["_raw"])  # reasoning, messages and calls exactly as returned
        elif role == "assistant":
            if m.get("content"):
                items.append({"role": "assistant", "content": m["content"]})
            for call in m.get("tool_calls") or []:
                fn = call.get("function") or {}
                items.append({"type": "function_call", "call_id": call.get("id", ""), "name": fn.get("name", ""),
                              "arguments": fn.get("arguments") or "{}"})
        elif role == "tool":
            items.append({"type": "function_call_output", "call_id": m.get("tool_call_id", ""),
                          "output": str(m.get("content") or "")})
        elif role == "system":
            items.append({"role": "developer", "content": m.get("content") or ""})
        else:
            items.append({"role": role or "user", "content": m.get("content") or ""})
    return items


def from_responses_output(data: dict[str, Any]) -> dict[str, Any]:
    output = data.get("output") or []
    text: list[str] = []
    calls: list[dict[str, Any]] = []
    raw: list[dict[str, Any]] = []
    for item in output:
        kind = item.get("type")
        if kind == "message":
            for part in item.get("content") or []:
                if part.get("type") in ("output_text", "text") and part.get("text"):
                    text.append(part["text"])
            raw.append({k: v for k, v in item.items() if k in ("type", "role", "content", "id", "status")})
        elif kind == "function_call":
            calls.append({"id": item.get("call_id") or item.get("id", ""), "type": "function",
                          "function": {"name": item.get("name", ""), "arguments": item.get("arguments") or "{}"}})
            raw.append({k: v for k, v in item.items() if k in ("type", "call_id", "name", "arguments", "id", "status")})
        elif kind == "reasoning":
            raw.append({k: v for k, v in item.items() if k in ("type", "id", "summary", "encrypted_content")})
    message: dict[str, Any] = {"role": "assistant", "content": "\n".join(text)}
    if calls:
        message["tool_calls"] = calls
    message["_raw"] = raw
    return message


class ModelClient:
    """OpenAI-compatible model client (Chat Completions or Responses) with bounded, honest retries."""

    def __init__(self, provider: ProviderConfig, *, timeout: float = 300.0, max_retries: int = 4,
                 session_id: str | None = None, wire: str | None = None, log_errors: bool = True):
        self.provider = provider
        self.timeout = timeout
        self.max_retries = max_retries
        self.session_id = session_id
        self.log_errors = log_errors
        # The Responses API is the common denominator: OpenAI's newest models only support function tools
        # with reasoning there, and MindsHub serves it for every model family (Anton and Codex use it too).
        self.wire = wire or "responses"

    async def chat(
        self,
        *,
        model: str,
        messages: list[dict[str, Any]],
        tools: list[dict[str, Any]] | None = None,
        max_tokens: int = 16000,
        ledger: UsageLedger | None = None,
        tool_choice: str | None = None,
        reasoning_effort: str | None = None,
    ) -> dict[str, Any]:
        if ledger:
            ledger.check()
        if self.wire == "responses":
            body: dict[str, Any] = {"model": model, "input": to_responses_input(messages), "store": False,
                                    "max_output_tokens": max_tokens, "include": ["reasoning.encrypted_content"]}
            if tools:
                body["tools"] = [{"type": "function", **t["function"]} for t in tools]
                body["parallel_tool_calls"] = True
                if tool_choice:
                    body["tool_choice"] = tool_choice
            if reasoning_effort:
                body["reasoning"] = {"effort": reasoning_effort}
            data = await self._post("/responses", body)
            if ledger:
                ledger.add(model, extract_usage(data))
            if data.get("status") == "incomplete" and not (data.get("output") or []):
                raise InferenceError(f"Model response incomplete: {data.get('incomplete_details')}")
            return from_responses_output(data)
        clean = [{k: v for k, v in m.items() if not k.startswith("_")} for m in messages]
        body = {"model": model, "messages": clean, "max_completion_tokens": max_tokens}
        if tools:
            body["tools"] = tools
            body["parallel_tool_calls"] = True
            if tool_choice:
                body["tool_choice"] = tool_choice
        if reasoning_effort:
            body["reasoning_effort"] = reasoning_effort
        try:
            data = await self._post("/chat/completions", body)
        except InferenceError as exc:
            if exc.status in (400, 404) and "responses" in str(exc).lower():
                self.wire = "responses"
                return await self.chat(model=model, messages=messages, tools=tools, max_tokens=max_tokens,
                                       ledger=ledger, tool_choice=tool_choice, reasoning_effort=reasoning_effort)
            raise
        if ledger:
            ledger.add(model, extract_usage(data))
        choices = data.get("choices") or []
        if not choices:
            raise InferenceError("Model returned no choices", retryable=False)
        return choices[0].get("message") or {}

    async def _post(self, path: str, body: dict[str, Any]) -> dict[str, Any]:
        url = self.provider.base_url + path
        last: Exception | None = None
        for attempt in range(self.max_retries + 1):
            try:
                async with httpx.AsyncClient(timeout=httpx.Timeout(self.timeout, connect=20.0)) as client:
                    async with admission(self.provider, self.session_id):
                        response = await client.post(url, headers=self.provider.headers(self.session_id),
                                                     content=json.dumps(body))
                if response.status_code == 200:
                    return response.json()
                detail = _error_detail(response)
                if response.status_code == 402:
                    raise InferenceError(f"{self.provider.label}: no credit remaining ({detail})", status=402)
                if response.headers.get("x-should-retry", "").lower() == "false":
                    raise InferenceError(f"{self.provider.label} limit reached ({response.status_code}): {detail}",
                                         status=response.status_code)
                if response.status_code in (401, 403):
                    raise InferenceError(f"{self.provider.label} refused the credential ({response.status_code}): {detail}", status=response.status_code)
                if response.status_code in (400, 404, 422):
                    last = InferenceError(
                        f"{self.provider.label} rejected the request ({response.status_code}): {detail}",
                        status=response.status_code)
                    if self.log_errors:
                        log.warning("%s %s rejected (%s): %s", self.provider.label, path, response.status_code, detail[:400])
                    if attempt == self.max_retries:
                        raise last
                    if "max_completion_tokens" in detail and "max_completion_tokens" in body:
                        body = {**body, "max_tokens": body.pop("max_completion_tokens")}
                        continue
                    if "parallel_tool_calls" in detail and "parallel_tool_calls" in body:
                        body = {k: v for k, v in body.items() if k != "parallel_tool_calls"}
                        continue
                    if "reasoning_effort" in detail and "reasoning_effort" in body:
                        body = {k: v for k, v in body.items() if k != "reasoning_effort"}
                        continue
                    if "reasoning" in detail and "reasoning" in body:
                        body = {k: v for k, v in body.items() if k != "reasoning"}
                        continue
                    if "include" in detail and "include" in body:
                        body = {k: v for k, v in body.items() if k != "include"}
                        continue
                    raise InferenceError(f"{self.provider.label} rejected the request ({response.status_code}): {detail}", status=response.status_code)
                last = InferenceError(f"{self.provider.label} error {response.status_code}: {detail}", retryable=True, status=response.status_code)
                retry_after = response.headers.get("retry-after")
                delay = float(retry_after) if retry_after and retry_after.replace(".", "").isdigit() else None
            except (httpx.TimeoutException, httpx.TransportError) as exc:
                last = InferenceError(f"{self.provider.label} unreachable: {type(exc).__name__}", retryable=True)
                delay = None
            if attempt < self.max_retries:
                await asyncio.sleep(min(60.0, delay or (2 ** attempt + random.random())))
        assert last is not None
        raise last

    async def session_cost(self, session_id: str) -> float | None:
        """MindsHub's own list-price estimate for every request tagged with this session."""
        if self.provider.provider != "mindshub":
            return None
        try:
            async with httpx.AsyncClient(timeout=20.0) as client:
                response = await client.get(f"{self.provider.base_url}/usage/sessions/{session_id}",
                                            headers=self.provider.headers())
            if response.status_code != 200:
                return None
            data = response.json()
            for key in ("estimated_cost_usd", "total_estimated_cost_usd"):
                if isinstance(data.get(key), (int, float)):
                    return float(data[key])
            totals = data.get("totals") or data.get("total") or {}
            if isinstance(totals, dict) and isinstance(totals.get("estimated_cost_usd"), (int, float)):
                return float(totals["estimated_cost_usd"])
        except (httpx.HTTPError, ValueError):
            return None
        return None

    async def list_models(self) -> list[str]:
        try:
            async with httpx.AsyncClient(timeout=min(self.timeout, 20.0)) as client:
                response = await client.get(self.provider.base_url + "/models", headers=self.provider.headers())
        except httpx.HTTPError:
            raise InferenceError("Model catalog is unreachable", retryable=True) from None
        if response.status_code != 200:
            raise InferenceError("Model catalog request failed", status=response.status_code,
                                 retryable=response.status_code == 429 or response.status_code >= 500)
        try:
            data = response.json()
        except ValueError:
            raise InferenceError("Invalid model catalog response", status=502, retryable=True) from None
        rows = data if isinstance(data, list) else data.get("data") if isinstance(data, dict) else None
        if not isinstance(rows, list):
            raise InferenceError("Invalid model catalog response", status=502, retryable=True)
        names = []
        for row in rows:
            if isinstance(row, dict) and row.get("id"):
                if row.get("enabled") is False or row.get("status") in ("disabled", "unavailable"):
                    continue
                if row.get("embedding") or row.get("kind") == "embedding":
                    continue
                names.append(str(row["id"]))
        return sorted(set(names))


def _error_detail(response: httpx.Response) -> str:
    try:
        data = response.json()
        err = data.get("error", data)
        if isinstance(err, dict):
            return str(err.get("message") or err.get("detail") or err)[:500]
        return str(err)[:500]
    except Exception:
        return response.text[:300]
