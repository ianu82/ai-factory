from __future__ import annotations

from pathlib import Path

from auto_mindsdb_factory.policy import PolicyEngine


def test_guarded_hard_override_wins() -> None:
    engine = PolicyEngine(Path(__file__).resolve().parents[1])
    decision = engine.evaluate_change(
        spec_packet_id="spec-guarded-001",
        decision="active_build_candidate",
        flags=["user_facing_llm_output_change"],
        reasoning=["User-facing LLM output should never go through the fast lane."],
        artifact_id="policy-guarded-001",
        timestamp="2026-04-22T12:00:00Z",
    )

    assert decision["lane_assignment"]["lane"] == "guarded"
    assert decision["lane_assignment"]["hard_override_reason"] == "user_facing_llm_output_change"
    assert decision["required_approvals"] == []
    assert decision["deployment_policy"]["strategy"] == "canary_then_soak"


def test_restricted_override_requires_security_merge_and_release() -> None:
    engine = PolicyEngine(Path(__file__).resolve().parents[1])
    decision = engine.evaluate_change(
        spec_packet_id="spec-restricted-001",
        decision="active_build_candidate",
        flags=["auth_or_permissions"],
        reasoning=["Permission changes must take the restricted lane."],
        artifact_id="policy-restricted-001",
        timestamp="2026-04-22T12:00:00Z",
    )

    assert decision["lane_assignment"]["lane"] == "restricted"
    assert decision["required_approvals"] == ["security", "merge", "release"]
    assert decision["artifact"]["approval_requirements"] == ["security", "merge", "release"]
