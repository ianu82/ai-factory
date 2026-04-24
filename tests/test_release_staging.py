from __future__ import annotations

from copy import deepcopy
import json
from pathlib import Path

import pytest

from auto_mindsdb_factory.contracts import load_validators, validation_errors_for
from auto_mindsdb_factory.controller import ControllerState, WorkItem
from auto_mindsdb_factory.merge_orchestration import StageMergePipeline
from auto_mindsdb_factory.release_staging import (
    ReleaseStagingConsistencyError,
    ReleaseStagingEligibilityError,
    Stage7ReleaseStagingPipeline,
)
from auto_mindsdb_factory.security_review import Stage6SecurityReviewPipeline


def _load_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def load_stage6_fixture(root: Path, scenario_name: str) -> dict[str, object]:
    base = root / "fixtures" / "scenarios" / scenario_name
    return {
        "spec_packet": _load_json(base / "spec-packet.json"),
        "policy_decision": _load_json(base / "policy-decision.json"),
        "ticket_bundle": _load_json(base / "ticket-bundle.json"),
        "eval_manifest": _load_json(base / "eval-manifest.json"),
        "pr_packet": _load_json(base / "pr-packet.json"),
        "prompt_contract": _load_json(base / "prompt-contract.json"),
        "tool_schema": _load_json(base / "tool-schema.json"),
        "golden_dataset": _load_json(base / "golden-dataset.json"),
        "latency_baseline": _load_json(base / "latency-baseline.json"),
        "eval_report": _load_json(base / "eval-report.json"),
        "security_review": _load_json(base / "security-review.json"),
        "work_item": WorkItem.from_document(_load_json(base / "work-item.json")),
    }


def restricted_stage6_security_approved_result(root: Path):
    pending = load_stage6_fixture(root, "stage6_security_pending_feature")
    return Stage6SecurityReviewPipeline(root).process(
        pending["spec_packet"],
        pending["policy_decision"],
        pending["ticket_bundle"],
        pending["eval_manifest"],
        pending["pr_packet"],
        pending["prompt_contract"],
        pending["tool_schema"],
        pending["golden_dataset"],
        pending["latency_baseline"],
        pending["eval_report"],
        pending["work_item"],
        approved_security_reviewers=["security-oncall"],
    )


def restricted_stage6_merged_result(root: Path):
    security_approved = restricted_stage6_security_approved_result(root)
    return StageMergePipeline(root).process(
        security_approved.spec_packet,
        security_approved.policy_decision,
        security_approved.ticket_bundle,
        security_approved.eval_manifest,
        security_approved.pr_packet,
        security_approved.prompt_contract,
        security_approved.tool_schema,
        security_approved.golden_dataset,
        security_approved.latency_baseline,
        security_approved.eval_report,
        security_approved.security_review,
        security_approved.work_item,
        approved_merge_reviewers=["merge-oncall"],
    )


def test_stage7_release_staging_auto_promotes_guarded_candidate() -> None:
    root = Path(__file__).resolve().parents[1]
    validators = load_validators(root)
    stage6_result = load_stage6_fixture(root, "stage6_security_approved_feature")

    result = Stage7ReleaseStagingPipeline(root).process(
        stage6_result["spec_packet"],
        stage6_result["policy_decision"],
        stage6_result["ticket_bundle"],
        stage6_result["eval_manifest"],
        stage6_result["pr_packet"],
        stage6_result["prompt_contract"],
        stage6_result["tool_schema"],
        stage6_result["golden_dataset"],
        stage6_result["latency_baseline"],
        stage6_result["eval_report"],
        stage6_result["security_review"],
        stage6_result["work_item"],
    )

    assert validation_errors_for(validators["promotion-decision"], result.promotion_decision) == []
    assert validation_errors_for(validators["pr-packet"], result.pr_packet) == []
    assert validation_errors_for(validators["work-item"], result.work_item.to_document()) == []
    assert result.promotion_decision["promotion_decision"]["status"] == "promoted"
    assert result.promotion_decision["promotion_decision"]["mode"] == "auto"
    assert result.pr_packet["artifact"]["next_stage"] == "production_monitoring"
    assert result.pr_packet["merge_readiness"]["mergeable"] is True
    assert result.work_item.state is ControllerState.PRODUCTION_MONITORING
    assert result.work_item.current_artifact_id == result.promotion_decision["artifact"]["id"]


def test_stage7_release_staging_accepts_merged_candidate() -> None:
    root = Path(__file__).resolve().parents[1]
    validators = load_validators(root)
    stage6_result = load_stage6_fixture(root, "stage6_security_approved_feature")
    merge_result = StageMergePipeline(root).process(
        stage6_result["spec_packet"],
        stage6_result["policy_decision"],
        stage6_result["ticket_bundle"],
        stage6_result["eval_manifest"],
        stage6_result["pr_packet"],
        stage6_result["prompt_contract"],
        stage6_result["tool_schema"],
        stage6_result["golden_dataset"],
        stage6_result["latency_baseline"],
        stage6_result["eval_report"],
        stage6_result["security_review"],
        stage6_result["work_item"],
    )

    result = Stage7ReleaseStagingPipeline(root).process(
        merge_result.spec_packet,
        merge_result.policy_decision,
        merge_result.ticket_bundle,
        merge_result.eval_manifest,
        merge_result.pr_packet,
        merge_result.prompt_contract,
        merge_result.tool_schema,
        merge_result.golden_dataset,
        merge_result.latency_baseline,
        merge_result.eval_report,
        merge_result.security_review,
        merge_result.work_item,
        merge_decision=merge_result.merge_decision,
    )

    assert validation_errors_for(validators["promotion-decision"], result.promotion_decision) == []
    assert result.merge_decision is not None
    assert result.merge_decision["merge_decision"]["status"] == "merged"
    assert result.pr_packet["merge_execution"]["status"] == "merged"
    assert result.work_item.state is ControllerState.PRODUCTION_MONITORING


def test_stage7_release_staging_can_wait_for_release_approval_and_resume() -> None:
    root = Path(__file__).resolve().parents[1]
    validators = load_validators(root)
    merge_result = restricted_stage6_merged_result(root)
    pipeline = Stage7ReleaseStagingPipeline(root)

    pending = pipeline.process(
        merge_result.spec_packet,
        merge_result.policy_decision,
        merge_result.ticket_bundle,
        merge_result.eval_manifest,
        merge_result.pr_packet,
        merge_result.prompt_contract,
        merge_result.tool_schema,
        merge_result.golden_dataset,
        merge_result.latency_baseline,
        merge_result.eval_report,
        merge_result.security_review,
        merge_result.work_item,
        merge_decision=merge_result.merge_decision,
    )

    assert validation_errors_for(validators["promotion-decision"], pending.promotion_decision) == []
    assert pending.promotion_decision["promotion_decision"]["status"] == "pending_human"
    assert pending.promotion_decision["promotion_decision"]["mode"] == "human_required"
    assert pending.promotion_decision["promotion_decision"]["pending_approvals"] == ["release"]
    assert pending.pr_packet["merge_readiness"]["reviewable"] is True
    assert pending.pr_packet["merge_readiness"]["mergeable"] is False
    assert pending.work_item.state is ControllerState.STAGING_SOAK

    resumed = pipeline.process(
        pending.spec_packet,
        pending.policy_decision,
        pending.ticket_bundle,
        pending.eval_manifest,
        pending.pr_packet,
        pending.prompt_contract,
        pending.tool_schema,
        pending.golden_dataset,
        pending.latency_baseline,
        pending.eval_report,
        pending.security_review,
        pending.work_item,
        merge_decision=pending.merge_decision,
        approved_release_reviewers=["release-manager"],
    )

    assert validation_errors_for(validators["promotion-decision"], resumed.promotion_decision) == []
    assert resumed.promotion_decision["promotion_decision"]["status"] == "promoted"
    assert resumed.promotion_decision["promotion_decision"]["mode"] == "human_approved"
    assert resumed.promotion_decision["promotion_decision"]["approvals_granted"] == ["release"]
    assert resumed.pr_packet["merge_readiness"]["mergeable"] is True
    assert resumed.work_item.state is ControllerState.PRODUCTION_MONITORING


def test_stage7_release_staging_refreshes_pending_artifact_on_repeat_review() -> None:
    root = Path(__file__).resolve().parents[1]
    merge_result = restricted_stage6_merged_result(root)
    pipeline = Stage7ReleaseStagingPipeline(root)

    pending = pipeline.process(
        merge_result.spec_packet,
        merge_result.policy_decision,
        merge_result.ticket_bundle,
        merge_result.eval_manifest,
        merge_result.pr_packet,
        merge_result.prompt_contract,
        merge_result.tool_schema,
        merge_result.golden_dataset,
        merge_result.latency_baseline,
        merge_result.eval_report,
        merge_result.security_review,
        merge_result.work_item,
        merge_decision=merge_result.merge_decision,
    )

    repeated = pipeline.process(
        pending.spec_packet,
        pending.policy_decision,
        pending.ticket_bundle,
        pending.eval_manifest,
        pending.pr_packet,
        pending.prompt_contract,
        pending.tool_schema,
        pending.golden_dataset,
        pending.latency_baseline,
        pending.eval_report,
        pending.security_review,
        pending.work_item,
        merge_decision=pending.merge_decision,
    )

    assert repeated.promotion_decision["artifact"]["id"] != pending.promotion_decision["artifact"]["id"]
    assert repeated.work_item.state is ControllerState.STAGING_SOAK
    assert repeated.work_item.current_artifact_id == repeated.promotion_decision["artifact"]["id"]
    assert repeated.work_item.updated_at == repeated.promotion_decision["artifact"]["updated_at"]


def test_stage7_release_staging_respects_zero_soak_and_sample_overrides() -> None:
    root = Path(__file__).resolve().parents[1]
    stage6_result = load_stage6_fixture(root, "stage6_security_approved_feature")

    result = Stage7ReleaseStagingPipeline(root).process(
        stage6_result["spec_packet"],
        stage6_result["policy_decision"],
        stage6_result["ticket_bundle"],
        stage6_result["eval_manifest"],
        stage6_result["pr_packet"],
        stage6_result["prompt_contract"],
        stage6_result["tool_schema"],
        stage6_result["golden_dataset"],
        stage6_result["latency_baseline"],
        stage6_result["eval_report"],
        stage6_result["security_review"],
        stage6_result["work_item"],
        observed_soak_minutes=0,
        observed_request_samples=0,
    )

    assert result.promotion_decision["promotion_decision"]["status"] == "blocked"
    assert "Minimum soak window was not met in staging." in result.promotion_decision["summary"]["threshold_breaches"]
    assert "Minimum request sample threshold was not met in staging." in result.promotion_decision["summary"]["threshold_breaches"]
    assert result.promotion_decision["staging_report"]["soak_minutes_observed"] == 0
    assert result.promotion_decision["staging_report"]["request_samples_observed"] == 0


def test_stage7_release_staging_can_return_to_pr_revision() -> None:
    root = Path(__file__).resolve().parents[1]
    stage6_result = load_stage6_fixture(root, "stage6_security_approved_feature")

    result = Stage7ReleaseStagingPipeline(root).process(
        stage6_result["spec_packet"],
        stage6_result["policy_decision"],
        stage6_result["ticket_bundle"],
        stage6_result["eval_manifest"],
        stage6_result["pr_packet"],
        stage6_result["prompt_contract"],
        stage6_result["tool_schema"],
        stage6_result["golden_dataset"],
        stage6_result["latency_baseline"],
        stage6_result["eval_report"],
        stage6_result["security_review"],
        stage6_result["work_item"],
        observed_soak_minutes=30,
        observed_request_samples=120,
    )

    assert result.promotion_decision["promotion_decision"]["status"] == "blocked"
    assert "Minimum soak window was not met in staging." in result.promotion_decision["summary"]["threshold_breaches"]
    assert result.pr_packet["merge_readiness"]["mergeable"] is False
    assert result.pr_packet["reviewer_report"]["approved"] is False
    assert result.work_item.state is ControllerState.PR_REVISION


def test_stage7_release_staging_rejects_mismatched_security_review() -> None:
    root = Path(__file__).resolve().parents[1]
    stage6_result = load_stage6_fixture(root, "stage6_security_approved_feature")
    security_review = deepcopy(stage6_result["security_review"])
    security_review["pr_packet_id"] = "pr-wrong-001"

    with pytest.raises(ReleaseStagingConsistencyError):
        Stage7ReleaseStagingPipeline(root).process(
            stage6_result["spec_packet"],
            stage6_result["policy_decision"],
            stage6_result["ticket_bundle"],
            stage6_result["eval_manifest"],
            stage6_result["pr_packet"],
            stage6_result["prompt_contract"],
            stage6_result["tool_schema"],
            stage6_result["golden_dataset"],
            stage6_result["latency_baseline"],
            stage6_result["eval_report"],
            security_review,
            stage6_result["work_item"],
        )


def test_stage7_release_staging_rejects_restricted_lane_without_merge_stage() -> None:
    root = Path(__file__).resolve().parents[1]
    stage6_result = restricted_stage6_security_approved_result(root)

    with pytest.raises(ReleaseStagingEligibilityError):
        Stage7ReleaseStagingPipeline(root).process(
            stage6_result.spec_packet,
            stage6_result.policy_decision,
            stage6_result.ticket_bundle,
            stage6_result.eval_manifest,
            stage6_result.pr_packet,
            stage6_result.prompt_contract,
            stage6_result.tool_schema,
            stage6_result.golden_dataset,
            stage6_result.latency_baseline,
            stage6_result.eval_report,
            stage6_result.security_review,
            stage6_result.work_item,
        )
