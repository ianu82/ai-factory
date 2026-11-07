"""Preserve MindsHub gateway policy evidence without retaining arbitrary headers."""


def gateway_evidence(headers):
    """Fail closed on absent/malformed routing evidence or changed semantics."""
    attempts = headers.get("x-mindshub-attempts")
    failover = headers.get("x-mindshub-failover")
    flags = {name: "x-mindshub-" + name.replace("_", "-") in headers
             for name in ("dropped_params", "dropped_content", "clamped_params", "chain_truncated")}
    return {**flags, "attempts": int(attempts) if attempts and attempts.isascii() and attempts.isdecimal()
            and len(attempts) <= 6 else None,
            "failover": {"true": True, "false": False}.get(failover),
            "single_attempt": attempts == "1", "requested_model": failover == "false"}


async def create_response(client, body):
    raw = await client.responses.with_raw_response.create(**body,
        extra_headers={"X-MindsHub-Allow-Failover": "false"})
    # The reviewed OpenAI 3.46.0 legacy raw-response API parses synchronously.
    return raw.parse(), gateway_evidence(raw.headers)
