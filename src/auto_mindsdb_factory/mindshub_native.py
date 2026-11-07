"""Bounded MindsHub transport for the mandatory native planner."""

import asyncio
import json
import math
import time

from .connectors import FactoryConnectorError
from .product_agent import HarnessStopped
from .product_inference_auth import _trusted_http_client, _object, _constant
from .mindshub_transport import gateway_evidence


class MindsHubPayload(dict):
    """Gateway evidence is a controller attribute, never a response-body key."""
    def __init__(self, payload, headers):
        super().__init__(payload)
        usage = self.get("usage")
        if (type(usage) is not dict or any(type(usage.get(k)) is not int or usage[k] < 0
                for k in ("input_tokens", "output_tokens", "total_tokens"))
                or usage["total_tokens"] != usage["input_tokens"] + usage["output_tokens"]):
            self["usage"] = None
        self.gateway = gateway_evidence(headers)
        self.transport_complete = True


class MindsHubNativeConnector:
    def __init__(self, config, *, cancelled=lambda: False, deadline=float("inf")):
        from .product_inference_runtime import MindsHubQualificationConfig
        if type(config) is not MindsHubQualificationConfig:
            raise HarnessStopped("MindsHub transport requires a trusted bound configuration")
        self.config = config.validated()
        self.cancelled, self.deadline = cancelled, deadline

    def request_response(self, body, *, timeout_seconds=None):
        if (body.get("model") != self.config.model or type(body.get("reasoning")) is not dict
                or body["reasoning"].get("effort") != self.config.reasoning_effort):
            raise HarnessStopped("MindsHub request configuration mismatch")
        timeout = self.config.timeout_seconds if timeout_seconds is None else timeout_seconds
        if type(timeout) not in (int, float) or not math.isfinite(timeout) or timeout <= 0:
            raise HarnessStopped("Invalid MindsHub request deadline")
        try:
            return asyncio.run(self._request_response(body, min(timeout, self.config.timeout_seconds)))
        except HarnessStopped:
            raise
        except Exception:
            raise FactoryConnectorError("MindsHub request failed; inspect retained safe evidence") from None

    async def _request_response(self, body, timeout):
        deadline = min(self.deadline, time.monotonic() + timeout)
        from .anton_session import validate_anton_installation
        validate_anton_installation()
        http = _trusted_http_client()
        if self.cancelled() or time.monotonic() >= deadline:
            raise HarnessStopped("MindsHub request cancelled or expired before admission")
        # A single absolute deadline covers polling, body reads and client cleanup.
        remaining = deadline - time.monotonic()
        work_deadline = deadline - min(1, max(0, remaining) / 10)
        observed = None
        try:
            async with asyncio.timeout_at(asyncio.get_running_loop().time() + max(0, deadline - time.monotonic())):
                async with http.AsyncClient(timeout=timeout, follow_redirects=False) as client:
                    async def exchange():
                        nonlocal observed
                        async with client.stream("POST", self.config.base_url, json=body,
                                headers={"Authorization": "Bearer " + self.config.api_key,
                                         "Accept": "application/json"}) as response:
                            if response.status_code != 200:
                                raise FactoryConnectorError("MindsHub request refused")
                            data = bytearray()
                            async for chunk in response.aiter_bytes(8192):
                                if len(data) + len(chunk) > 2 * 1024 * 1024:
                                    raise FactoryConnectorError("MindsHub response exceeds controller limit")
                                data.extend(chunk)
                            payload = json.loads(data, object_pairs_hook=_object, parse_constant=_constant)
                            if type(payload) is not dict:
                                raise FactoryConnectorError("Invalid MindsHub response object")
                            observed = MindsHubPayload(payload, response.headers)
                            observed.transport_complete = False
                            return observed
                    request = asyncio.create_task(exchange())
                    try:
                        while not request.done():
                            if self.cancelled() or time.monotonic() >= work_deadline:
                                raise HarnessStopped("MindsHub request cancelled or deadline reached")
                            await asyncio.wait({request}, timeout=min(0.05, max(0, work_deadline - time.monotonic())))
                        await request
                    finally:
                        if not request.done():
                            request.cancel()
                        await asyncio.gather(request, return_exceptions=True)
        except Exception:
            if observed is None:
                raise
            # Account for a fully parsed response even if retirement failed.
            return observed
        observed.transport_complete = time.monotonic() < deadline and not self.cancelled()
        return observed
