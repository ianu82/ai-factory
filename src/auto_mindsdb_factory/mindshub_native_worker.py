"""Single trusted HTTP operation; no tools or candidate execution."""

from .controller_imports import require_controller_import_path
require_controller_import_path()

import asyncio
import json
import math
import sys
import time

from .anton_session import validate_anton_installation
from .mindshub_transport import gateway_evidence
from .product_inference_auth import _trusted_http_client, _object, _constant

MAX_BYTES = 2 * 1024 * 1024


def emit(message):
    sys.stdout.write(json.dumps(message, allow_nan=False, ensure_ascii=False) + "\n")
    sys.stdout.flush()


async def perform(packet, http):
    deadline = packet["deadline"]
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise ValueError()
    async with asyncio.timeout(remaining):
        async with http.AsyncClient(timeout=remaining, follow_redirects=False) as client:
            async with client.stream("POST", "https://api.mindshub.ai/v1/responses", json=packet["body"],
                    headers={"Authorization": "Bearer " + packet["api_key"], "Accept": "application/json",
                             "X-MindsHub-Allow-Failover": "false"}) as response:
                if response.status_code != 200:
                    raise ValueError()
                data = bytearray()
                async for chunk in response.aiter_bytes(8192):
                    if len(data) + len(chunk) > MAX_BYTES:
                        raise ValueError()
                    data.extend(chunk)
                payload = json.loads(data, object_pairs_hook=_object, parse_constant=_constant)
                if type(payload) is not dict:
                    raise ValueError()
                summary = {name: payload.get(name) for name in ("id", "model", "status", "usage")}
                for name in ("id", "model", "status"):
                    if type(summary[name]) is not str or len(summary[name]) > 256:
                        summary[name] = None
                if len(json.dumps(summary).encode()) > 65536:
                    summary["usage"] = None
                emit({"kind": "observed", "summary": summary, "gateway": gateway_evidence(response.headers)})
                emit({"kind": "payload", "payload": payload})
    emit({"kind": "retired"})


def main():
    try:
        validate_anton_installation()
        http = _trusted_http_client()
        emit({"kind": "ready"})  # No secret read before imports/SDK admission.
        data = sys.stdin.buffer.read(MAX_BYTES + 1)
        if len(data) > MAX_BYTES:
            raise ValueError()
        packet = json.loads(data, object_pairs_hook=_object, parse_constant=_constant)
        if (type(packet) is not dict or set(packet) != {"api_key", "body", "deadline"}
                or type(packet["api_key"]) is not str or not 1 <= len(packet["api_key"]) <= 4096
                or type(packet["body"]) is not dict or type(packet["deadline"]) not in (int, float)
                or not math.isfinite(packet["deadline"])):
            raise ValueError()
        asyncio.run(perform(packet, http))
        return 0
    except BaseException:
        return 1  # Never emit provider errors, headers, input or tracebacks.


if __name__ == "__main__":
    raise SystemExit(main())
