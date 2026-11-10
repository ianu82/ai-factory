"""Inference gateway for harnesses that run inside a sandbox (Codex).

The sandbox only ever holds a short-lived, run-scoped token. The gateway authenticates it,
pins the run's model, enforces the run's budget, injects the workspace's real provider key,
records usage against the run, and normalizes provider errors so the harness neither leaks
credentials nor retries forever.
"""

from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass, field
from datetime import timedelta

import anyio
import httpx
from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse, StreamingResponse

from .db import session_scope, utcnow
from .images import ImageRedactor, collect_image_payloads
from .inference import BudgetExceeded, ProviderConfig, UsageLedger, extract_usage
from .inference_limits import acquire
from .models import RunToken
from .security import new_token, token_hash

router = APIRouter()

ALLOWED_PATHS = {"models", "responses", "responses/compact", "chat/completions"}
MAX_BODY = 16 * 1024 * 1024


@dataclass
class GatewayGrant:
    run_id: str
    workspace_id: str
    model: str
    provider: ProviderConfig
    ledger: UsageLedger
    image_redactor: ImageRedactor = field(default_factory=ImageRedactor, repr=False)


_grants: dict[str, GatewayGrant] = {}


def issue(grant: GatewayGrant, ttl_seconds: int = 4 * 3600) -> str:
    token = new_token("fgw_")
    with session_scope() as s:
        s.add(RunToken(token_hash=token_hash(token), run_id=grant.run_id, workspace_id=grant.workspace_id,
                       expires_at=utcnow() + timedelta(seconds=ttl_seconds)))
    _grants[grant.run_id] = grant
    return token


def revoke(run_id: str) -> None:
    _grants.pop(run_id, None)
    with session_scope() as s:
        for row in s.query(RunToken).filter(RunToken.run_id == run_id):
            row.revoked = True


def _error(status: int, code: str, message: str) -> JSONResponse:
    # Codex retries every status except 400, so terminal problems are reported as 400.
    return JSONResponse({"error": {"message": message, "type": code, "code": code}}, status_code=status)


def _authenticate(request: Request) -> GatewayGrant | None:
    auth = request.headers.get("authorization", "")
    token = auth[7:].strip() if auth.lower().startswith("bearer ") else ""
    if not token:
        return None
    with session_scope() as s:
        row = s.get(RunToken, token_hash(token))
        if row is None or row.revoked or row.expires_at < utcnow():
            return None
        run_id = row.run_id
    return _grants.get(run_id)


@router.api_route("/gateway/v1/{path:path}", methods=["GET", "POST"])
async def proxy(path: str, request: Request):
    grant = _authenticate(request)
    if grant is None:
        return JSONResponse({"error": {"message": "Invalid or expired run token", "type": "unauthorized"}}, status_code=401)
    path = path.strip("/")
    if path not in ALLOWED_PATHS:
        return _error(400, "unsupported_endpoint", f"/{path} is not available to Factory runs")
    url = f"{grant.provider.base_url}/{path}"
    headers = grant.provider.headers(grant.run_id)
    if request.method == "GET":
        async with httpx.AsyncClient(timeout=30.0) as client:
            upstream = await client.get(url, headers=headers)
        return JSONResponse(upstream.json() if upstream.headers.get("content-type", "").startswith("application/json")
                            else {"data": []}, status_code=upstream.status_code if upstream.status_code < 400 else 400)

    raw = await request.body()
    if len(raw) > MAX_BODY:
        return _error(400, "request_too_large", "Request body too large")
    try:
        body = json.loads(raw)
    except json.JSONDecodeError:
        return _error(400, "invalid_json", "Request body must be JSON")
    collect_image_payloads(body, grant.image_redactor)
    body.pop("client_metadata", None)
    requested = body.get("model")
    if requested and requested != grant.model:
        return _error(400, "model_not_allowed", f"This run is pinned to {grant.model}")
    body["model"] = grant.model
    try:
        grant.ledger.check()
    except BudgetExceeded as exc:
        return _error(400, "budget_exhausted", str(exc))

    stream = bool(body.get("stream"))
    payload = json.dumps(body).encode()
    for attempt in range(3):
        client = httpx.AsyncClient(timeout=httpx.Timeout(600.0, connect=20.0))
        upstream = None
        permit = None
        cleaned = False

        async def cleanup():
            nonlocal cleaned
            if cleaned:
                return
            cleaned = True
            # Starlette cancels the relay's AnyIO scope on disconnect. Shield resource
            # cleanup, and release even if either upstream close itself raises.
            with anyio.CancelScope(shield=True):
                try:
                    if upstream is not None:
                        await upstream.aclose()
                finally:
                    try:
                        await client.aclose()
                    finally:
                        if permit is not None:
                            permit.release()

        try:
            upstream_request = client.build_request("POST", url, headers=headers, content=payload)
            permit = await acquire(grant.provider, grant.run_id)
            upstream = await client.send(upstream_request, stream=True)
            retry = (upstream.status_code == 429
                     and upstream.headers.get("x-should-retry", "").lower() != "false" and attempt < 2)
            if retry:
                retry_after = upstream.headers.get("retry-after", "5")
            elif upstream.status_code >= 400:
                detail = grant.image_redactor((await upstream.aread()).decode(errors="replace"))[:800]
                status = upstream.status_code
                code = {401: "credential_refused", 402: "wallet_empty", 403: "permission_denied", 404: "model_not_found",
                        429: "rate_limited"}.get(status, "upstream_error")
                return _error(400 if status in (401, 402, 403, 404, 429) else 502, code,
                              f"Provider returned {status}: {detail}")
            elif not stream:
                data = await upstream.aread()
                try:
                    parsed = json.loads(data)
                except json.JSONDecodeError:
                    parsed = {}
                grant.ledger.add(grant.model, extract_usage(parsed))
                return JSONResponse(parsed, status_code=200)
            else:
                break  # Transfer the permit and upstream ownership to the response.
        except BaseException:
            await cleanup()
            raise
        finally:
            if upstream is not None and (upstream.status_code >= 400 or not stream):
                await cleanup()
        # No concurrency permit is held during upstream retry backoff.
        await asyncio.sleep(min(20.0, float(retry_after) if retry_after.replace(".", "").isdigit() else 5.0))

    async def relay():
        usage = None
        buffer = b""
        try:
            async for chunk in upstream.aiter_raw():
                yield chunk
                buffer += chunk
                while b"\n" in buffer:
                    line, buffer = buffer.split(b"\n", 1)
                    if line.startswith(b"data:") and b"usage" in line:
                        try:
                            found = extract_usage(json.loads(line[5:].strip()))
                            if found:
                                usage = found
                        except json.JSONDecodeError:
                            pass
        finally:
            try:
                await cleanup()
            finally:
                grant.ledger.add(grant.model, usage)

    class GatewayStream(StreamingResponse):
        async def __call__(self, scope, receive, send):
            try:
                await super().__call__(scope, receive, send)
            finally:
                # Close a suspended iterator on downstream send failure as well.
                # Also covers disconnect before the iterator is ever started.
                with anyio.CancelScope(shield=True):
                    try:
                        await self.body_iterator.aclose()
                    finally:
                        await cleanup()

    return GatewayStream(relay(), media_type=upstream.headers.get("content-type", "text/event-stream"))
