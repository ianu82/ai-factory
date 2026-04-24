from __future__ import annotations

from typing import Any


MERGE_GATE_TIERS = frozenset({"pr_smoke", "pre_merge"})


def merge_gate_tiers(eval_manifest: dict[str, Any]) -> list[str]:
    return [
        tier["name"]
        for tier in eval_manifest["tiers"]
        if tier["name"] in MERGE_GATE_TIERS
    ]


def deferred_tiers(eval_manifest: dict[str, Any]) -> list[str]:
    return [
        tier["name"]
        for tier in eval_manifest["tiers"]
        if tier["name"] not in MERGE_GATE_TIERS
    ]


def pending_merge_gate_tiers(
    eval_manifest: dict[str, Any],
    *,
    completed_tiers: set[str] | None = None,
) -> list[str]:
    completed = completed_tiers or set()
    return [
        tier_name
        for tier_name in merge_gate_tiers(eval_manifest)
        if tier_name not in completed
    ]
