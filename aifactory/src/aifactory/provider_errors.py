"""Provider account stops, shared by HTTP and SDK adapters without import cycles.

Supported contracts (not fuzzy message matching):
* MindsHub: HTTP 402, including the exhausted-credit response recorded in
  https://github.com/ianu82/ai-factory/issues/344. No undocumented MindsHub
  string codes are inferred from words such as 'quota' or 'balance'.
* OpenAI: HTTP 402 and `insufficient_quota` (code or type), as shown in the
  official example at https://cookbook.openai.com/examples/how_to_handle_rate_limits
  and the 429 quota entry at https://platform.openai.com/docs/guides/error-codes/api-errors.
  A normal 429 / rate_limit_exceeded is NOT an account stop.

Only allowlisted provider/code identifiers enter diagnostics. Credential identity
belongs to trusted host configuration, never to this exception or its public data.
"""
from __future__ import annotations

import json
from typing import Any, Callable, Protocol

import httpx

from .security import redact

BILLING_CODES = {"mindshub": frozenset(), "openai": frozenset({"insufficient_quota"})}
MAX_ERROR_BODY = 64 * 1024


class ProviderStop(RuntimeError):
    """Non-retryable account billing/quota stop, not a task or Factory budget failure.

    Deliberately separate from InferenceError: compatibility-repair and ordinary
    failure handlers must not swallow it. Direct-call consumers account for the
    rejected request, then re-raise this same stop for host recovery.
    """

    retryable = False
    reason = "billing_quota"
    cause = "provider"

    def __init__(self, provider: str, status: int, code: str | None, message: str):
        self.provider = provider
        self.status = status
        self.code = code
        self.message = message
        super().__init__(f"{provider} billing/quota stop ({status}{': ' + code if code else ''}): {message}")

    def diagnostics(self) -> dict[str, Any]:
        return {"provider": self.provider, "status": self.status, "code": self.code,
                "reason": self.reason, "message": self.message}


# Synchronous, host-only hook: persistence can happen at detection, before sandbox
# cleanup or SDK error handling. The owner already has the credential configuration;
# neither keys nor credential fingerprints need to be copied into the stop.
StopHandler = Callable[[ProviderStop, str | None], None]


def workspace_stop_handler(workspace_id: str) -> StopHandler:
    """Install the host persistence boundary for every resolved workspace provider.

    Reuse the existing idempotent workspace hold until credential-scoped recovery
    replaces it. Import lazily: holds depends on inference and its runtime helpers.
    The callback captures only workspace identity, never the provider credential.
    """
    def persist(stop: ProviderStop, run_id: str | None) -> None:
        from .pipeline.holds import open_provider_hold

        open_provider_hold(workspace_id, str(stop), "model provider request")

    return persist


class _Provider(Protocol):
    provider: str
    api_key: str
    on_provider_stop: StopHandler | None


def _error(body: Any) -> Any:
    if isinstance(body, (str, bytes)):
        try:
            body = json.loads(body)
        except (ValueError, UnicodeDecodeError, RecursionError):
            return body.decode(errors="replace") if isinstance(body, bytes) else body
    return body.get("error", body) if isinstance(body, dict) else body


def safe_error_detail(body: Any, api_key: str, *, limit: int = 500) -> str:
    """Scrub the COMPLETE message before clipping, even for nonstandard/short keys."""
    error = _error(body)
    if isinstance(error, dict):
        # Do not serialize arbitrary fields (request headers, credential hashes, etc.).
        error = error.get("message") or error.get("detail") or "Provider request failed"
    if not isinstance(error, str):
        error = "Provider request failed"
    return redact(error, (api_key,))[:limit]


def classify_provider_error(provider: _Provider, status: int | None, body: Any) -> ProviderStop | None:
    """Pure classification; authentication, transport and local budgets stay distinct."""
    name = provider.provider if provider.provider in BILLING_CODES else "unknown"
    error = _error(body)
    codes = BILLING_CODES.get(name, ())
    code = None
    if isinstance(error, dict):
        code = next((error.get(k) for k in ("code", "type")
                     if isinstance(error.get(k), str) and error[k] in codes), None)
    if status != 402 and not (status in (400, 429) and code is not None):
        return None
    return ProviderStop(name, status, code, safe_error_detail(body, provider.api_key))


def detect_provider_stop(provider: _Provider, status: int | None, body: Any,
                         run_id: str | None = None) -> ProviderStop | None:
    """Classify and notify the host once per detected response, before any retry."""
    stop = classify_provider_error(provider, status, body)
    if stop is not None and provider.on_provider_stop is not None:
        provider.on_provider_stop(stop, run_id)
    return stop


async def read_error_body(response: httpx.Response) -> bytes:
    """Bound streamed error reads. Never diagnose a partially read credential."""
    body = bytearray()
    async for chunk in response.aiter_bytes(chunk_size=MAX_ERROR_BODY + 1):
        if len(body) + len(chunk) > MAX_ERROR_BODY:
            # Discard the entire oversized diagnostic, rather than truncating an
            # echoed key before it can be scrubbed. HTTP 402 still classifies.
            return b'Provider error body exceeded the diagnostic limit'
        body.extend(chunk)
    return bytes(body)
