from __future__ import annotations

from auto_mindsdb_factory.eval_common import (
    deferred_tiers,
    merge_gate_tiers,
    pending_merge_gate_tiers,
)


def test_merge_gate_and_deferred_tiers_are_split() -> None:
    eval_manifest = {
        "tiers": [
            {"name": "pr_smoke", "checks": [{"id": "smoke"}]},
            {"name": "pre_merge", "checks": [{"id": "merge"}]},
            {"name": "nightly", "checks": [{"id": "nightly"}]},
            {"name": "post_deploy", "checks": [{"id": "deploy"}]},
        ]
    }

    assert merge_gate_tiers(eval_manifest) == ["pr_smoke", "pre_merge"]
    assert deferred_tiers(eval_manifest) == ["nightly", "post_deploy"]
    assert pending_merge_gate_tiers(
        eval_manifest,
        completed_tiers={"pr_smoke"},
    ) == ["pre_merge"]
