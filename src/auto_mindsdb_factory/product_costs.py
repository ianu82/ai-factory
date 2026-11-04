"""Explicit, versioned inference estimates; never infer prices from model names."""

from __future__ import annotations

import math


def estimate_cost(usage: dict | None, rate: dict | None) -> float | None:
    if not usage or not rate:
        return None
    incoming, outgoing = usage.get("input_tokens"), usage.get("output_tokens")
    cached = (usage.get("input_tokens_details") or {}).get("cached_tokens", 0)
    if any(type(n) is not int or n < 0 for n in [incoming, outgoing, cached]):
        return None
    if cached > incoming or incoming > rate["max_input_tokens"]:
        return None
    value = ((incoming - cached) * rate["input"] + cached * rate["cached_input"] + outgoing * rate["output"]) / 1000000
    return round(value, 8) if math.isfinite(value) else None
