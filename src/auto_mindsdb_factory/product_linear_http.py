"""Private HTTP subprocess; its parent enforces a wall-clock request deadline."""
from __future__ import annotations

import json
import sys
from urllib import request as urllib_request

from .linear_trigger import LinearGraphQLClient, LinearTriggerConfig
from .product_linear_client import LinearProductClient


class _NoRedirect(urllib_request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def main():
    try:
        packet = json.load(sys.stdin)
        urllib_request.install_opener(urllib_request.build_opener(_NoRedirect()))
        client = LinearProductClient(LinearTriggerConfig(**packet["config"]))
        # Invoke the shared transport directly; retain the product envelope hook.
        data = LinearGraphQLClient._execute(client, packet["query"], packet["variables"],
            timeout_seconds=packet["timeout_seconds"], max_response_bytes=4 * 1024 * 1024)
    except Exception as exc:
        json.dump({"error": type(exc).__name__}, sys.stdout)
        return 1
    json.dump({"data": data}, sys.stdout)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
