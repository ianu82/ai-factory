"""Bounded MindsHub transport for the mandatory native planner."""

import math
import time

from .connectors import FactoryConnectorError
from .product_agent import HarnessStopped
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
        self.worker_facts = None

    def request_response(self, body, *, timeout_seconds=None):
        self.worker_facts = None
        if (body.get("model") != self.config.model or type(body.get("reasoning")) is not dict
                or body["reasoning"].get("effort") != self.config.reasoning_effort):
            raise HarnessStopped("MindsHub request configuration mismatch")
        timeout = self.config.timeout_seconds if timeout_seconds is None else timeout_seconds
        if type(timeout) not in (int, float) or not math.isfinite(timeout) or timeout <= 0:
            raise HarnessStopped("Invalid MindsHub request deadline")
        deadline = min(self.deadline, time.monotonic() + min(timeout, self.config.timeout_seconds))
        try:
            from .mindshub_native_process import exchange
            observed, payload, complete, self.worker_facts = exchange({"api_key": self.config.api_key,
                "body": body, "deadline": deadline}, deadline=deadline, cancelled=self.cancelled)
            if type(observed) is not dict or type(observed.get("summary")) is not dict:
                raise ValueError()
            value = MindsHubPayload({**(payload if type(payload) is dict else {}), **observed["summary"]}, {})
            expected = gateway_evidence({})
            gateway = observed.get("gateway")
            if (type(gateway) is not dict or set(gateway) != set(expected)
                    or any(type(gateway[k]) is not bool for k in expected if type(expected[k]) is bool)
                    or (gateway["attempts"] is not None and type(gateway["attempts"]) is not int)
                    or (gateway["failover"] is not None and type(gateway["failover"]) is not bool)):
                complete, gateway = False, expected
            value.gateway, value.transport_complete = gateway, complete
            return value
        except Exception:
            raise FactoryConnectorError("MindsHub request failed; inspect retained safe evidence") from None
