from __future__ import annotations

from copy import deepcopy
import json
from pathlib import Path

import pytest

from auto_mindsdb_factory.contracts import load_validators, validation_errors_for
from auto_mindsdb_factory.controller import ControllerState, WorkItem
from auto_mindsdb_factory.merge_orchestration import StageMergePipeline
from auto_mindsdb_factory.production_monitoring import (
    ProductionMonitoringConsistencyError,
    Stage8ProductionMonitoringPipeline,
)
from auto_mindsdb_factory.release_staging import Stage7ReleaseStagingPipeline
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


def load_stage7_fixture(root: Path, scenario_name: str) -> dict[str, object]:
    base = root / "fixtures" / "scenarios" / scenario_name
    result = {
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
        "promotion_decision": _load_json(base / "promotion-decision.json"),
        "work_item": WorkItem.from_document(_load_json(base / "work-item.json")),
    }
    merge_path = base / "merge-decision.json"
    if merge_path.exists():
        result["merge_decision"] = _load_json(merge_path)
    return result


def restricted_stage7_pending_result(root: Path):
    pending = load_stage6_fixture(root, "stage6_security_pending_feature")
    security_approved = Stage6SecurityReviewPipeline(root).process(
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
    merge_result = StageMergePipeline(root).process(
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
    return Stage7ReleaseStagingPipeline(root).process(
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


def restricted_stage7_promoted_result(root: Path):
    pending = restricted_stage7_pending_result(root)
    return Stage7ReleaseStagingPipeline(root).process(
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


def test_stage8_production_monitoring_records_healthy_window() -> None:
    root = Path(__file__).resolve().parents[1]
    validators = load_validators(root)
    stage7_result = load_stage7_fixture(root, "stage7_production_monitoring_feature")

    result = Stage8ProductionMonitoringPipeline(root).process(
        stage7_result["spec_packet"],
        stage7_result["policy_decision"],
        stage7_result["ticket_bundle"],
        stage7_result["eval_manifest"],
        stage7_result["pr_packet"],
        stage7_result["prompt_contract"],
        stage7_result["tool_schema"],
        stage7_result["golden_dataset"],
        stage7_result["latency_baseline"],
        stage7_result["eval_report"],
        stage7_result["security_review"],
        stage7_result["promotion_decision"],
        stage7_result["work_item"],
        merge_decision=stage7_result.get("merge_decision"),
    )

    assert validation_errors_for(validators["monitoring-report"], result.monitoring_report) == []
    assert validation_errors_for(validators["pr-packet"], result.pr_packet) == []
    assert validation_errors_for(validators["work-item"], result.work_item.to_document()) == []
    assert result.monitoring_report["monitoring_decision"]["status"] == "healthy"
    assert result.pr_packet["artifact"]["next_stage"] == "production_monitoring"
    assert result.pr_packet["merge_readiness"]["mergeable"] is True
    assert result.work_item.state is ControllerState.PRODUCTION_MONITORING
    assert result.work_item.current_artifact_id == result.monitoring_report["artifact"]["id"]


def test_stage8_production_monitoring_can_auto_mitigate_guarded_incident() -> None:
    root = Path(__file__).resolve().parents[1]
    stage7_result = load_stage7_fixture(root, "stage7_production_monitoring_feature")

    result = Stage8ProductionMonitoringPipeline(root).process(
        stage7_result["spec_packet"],
        stage7_result["policy_decision"],
        stage7_result["ticket_bundle"],
        stage7_result["eval_manifest"],
        stage7_result["pr_packet"],
        stage7_result["prompt_contract"],
        stage7_result["tool_schema"],
        stage7_result["golden_dataset"],
        stage7_result["latency_baseline"],
        stage7_result["eval_report"],
        stage7_result["security_review"],
        stage7_result["promotion_decision"],
        stage7_result["work_item"],
        merge_decision=stage7_result.get("merge_decision"),
        metric_overrides={"error_rate_pct": 1.1},
    )

    assert result.monitoring_report["monitoring_decision"]["status"] == "auto_mitigated"
    assert result.monitoring_report["mitigation"]["action"] == "feature_flag_disable"
    assert result.monitoring_report["mitigation"]["executed"] is True
    assert result.monitoring_report["incident"]["status"] == "mitigated"
    assert result.pr_packet["artifact"]["next_stage"] == "feedback_synthesis"
    assert result.pr_packet["merge_readiness"]["mergeable"] is False
    assert result.pr_packet["reviewer_report"]["approved"] is False
    assert result.work_item.state is ControllerState.PRODUCTION_MONITORING


def test_stage8_production_monitoring_can_escalate_restricted_incident() -> None:
    root = Path(__file__).resolve().parents[1]
    stage7_result = restricted_stage7_promoted_result(root)

    result = Stage8ProductionMonitoringPipeline(root).process(
        stage7_result.spec_packet,
        stage7_result.policy_decision,
        stage7_result.ticket_bundle,
        stage7_result.eval_manifest,
        stage7_result.pr_packet,
        stage7_result.prompt_contract,
        stage7_result.tool_schema,
        stage7_result.golden_dataset,
        stage7_result.latency_baseline,
        stage7_result.eval_report,
        stage7_result.security_review,
        stage7_result.promotion_decision,
        stage7_result.work_item,
        merge_decision=stage7_result.merge_decision,
        metric_overrides={"error_rate_pct": 0.8},
    )

    assert result.monitoring_report["monitoring_decision"]["status"] == "human_escalated"
    assert result.monitoring_report["mitigation"]["action"] == "feature_flag_disable"
    assert result.monitoring_report["mitigation"]["executed"] is False
    assert result.monitoring_report["mitigation"]["result"] == "blocked_by_policy"
    assert result.monitoring_report["incident"]["page_targets"] == ["lead_engineer", "core_builder"]
    assert result.pr_packet["artifact"]["next_stage"] == "human_incident_response"
    assert result.pr_packet["merge_readiness"]["mergeable"] is False


def test_stage8_production_monitoring_does_not_reopen_pr_after_prior_incident() -> None:
    root = Path(__file__).resolve().parents[1]
    stage7_result = load_stage7_fixture(root, "stage7_production_monitoring_feature")
    pipeline = Stage8ProductionMonitoringPipeline(root)

    first_result = pipeline.process(
        stage7_result["spec_packet"],
        stage7_result["policy_decision"],
        stage7_result["ticket_bundle"],
        stage7_result["eval_manifest"],
        stage7_result["pr_packet"],
        stage7_result["prompt_contract"],
        stage7_result["tool_schema"],
        stage7_result["golden_dataset"],
        stage7_result["latency_baseline"],
        stage7_result["eval_report"],
        stage7_result["security_review"],
        stage7_result["promotion_decision"],
        stage7_result["work_item"],
        merge_decision=stage7_result.get("merge_decision"),
        metric_overrides={"error_rate_pct": 1.1},
    )

    second_result = pipeline.process(
        first_result.spec_packet,
        first_result.policy_decision,
        first_result.ticket_bundle,
        first_result.eval_manifest,
        first_result.pr_packet,
        first_result.prompt_contract,
        first_result.tool_schema,
        first_result.golden_dataset,
        first_result.latency_baseline,
        first_result.eval_report,
        first_result.security_review,
        first_result.promotion_decision,
        first_result.work_item,
        merge_decision=first_result.merge_decision,
    )

    assert second_result.monitoring_report["monitoring_decision"]["status"] == "healthy"
    assert second_result.pr_packet["artifact"]["status"] == "blocked"
    assert second_result.pr_packet["artifact"]["next_stage"] == "feedback_synthesis"
    assert second_result.pr_packet["merge_readiness"]["mergeable"] is False
    assert second_result.pr_packet["reviewer_report"]["approved"] is False
    assert (
        "Error rate exceeded the allowed production threshold."
        in second_result.pr_packet["reviewer_report"]["blocking_findings"]
    )


def test_stage8_production_monitoring_preserves_human_escalation_on_follow_up_windows() -> None:
    root = Path(__file__).resolve().parents[1]
    stage7_result = restricted_stage7_promoted_result(root)
    pipeline = Stage8ProductionMonitoringPipeline(root)

    first_result = pipeline.process(
        stage7_result.spec_packet,
        stage7_result.policy_decision,
        stage7_result.ticket_bundle,
        stage7_result.eval_manifest,
        stage7_result.pr_packet,
        stage7_result.prompt_contract,
        stage7_result.tool_schema,
        stage7_result.golden_dataset,
        stage7_result.latency_baseline,
        stage7_result.eval_report,
        stage7_result.security_review,
        stage7_result.promotion_decision,
        stage7_result.work_item,
        merge_decision=stage7_result.merge_decision,
        metric_overrides={"error_rate_pct": 0.8},
    )

    second_result = pipeline.process(
        first_result.spec_packet,
        first_result.policy_decision,
        first_result.ticket_bundle,
        first_result.eval_manifest,
        first_result.pr_packet,
        first_result.prompt_contract,
        first_result.tool_schema,
        first_result.golden_dataset,
        first_result.latency_baseline,
        first_result.eval_report,
        first_result.security_review,
        first_result.promotion_decision,
        first_result.work_item,
        merge_decision=first_result.merge_decision,
    )

    assert second_result.monitoring_report["monitoring_decision"]["status"] == "healthy"
    assert second_result.pr_packet["artifact"]["next_stage"] == "human_incident_response"
    assert second_result.pr_packet["merge_readiness"]["mergeable"] is False


def test_stage8_production_monitoring_rejects_forged_repeated_run_inputs() -> None:
    root = Path(__file__).resolve().parents[1]
    stage7_result = load_stage7_fixture(root, "stage7_production_monitoring_feature")
    pr_packet = deepcopy(stage7_result["pr_packet"])
    pr_packet["artifact"]["version"] = (
        stage7_result["promotion_decision"]["resulting_pr_artifact_version"] + 1
    )
    work_item = deepcopy(stage7_result["work_item"])
    work_item.current_artifact_id = "monitoring-report-fake-001"

    with pytest.raises(ProductionMonitoringConsistencyError):
        Stage8ProductionMonitoringPipeline(root).process(
            stage7_result["spec_packet"],
            stage7_result["policy_decision"],
            stage7_result["ticket_bundle"],
            stage7_result["eval_manifest"],
            pr_packet,
            stage7_result["prompt_contract"],
            stage7_result["tool_schema"],
            stage7_result["golden_dataset"],
            stage7_result["latency_baseline"],
            stage7_result["eval_report"],
            stage7_result["security_review"],
            stage7_result["promotion_decision"],
            work_item,
            merge_decision=stage7_result.get("merge_decision"),
        )


def test_stage8_production_monitoring_rejects_mismatched_promotion_decision() -> None:
    root = Path(__file__).resolve().parents[1]
    stage7_result = load_stage7_fixture(root, "stage7_production_monitoring_feature")
    promotion_decision = deepcopy(stage7_result["promotion_decision"])
    promotion_decision["pr_packet_id"] = "pr-wrong-001"

    with pytest.raises(ProductionMonitoringConsistencyError):
        Stage8ProductionMonitoringPipeline(root).process(
            stage7_result["spec_packet"],
            stage7_result["policy_decision"],
            stage7_result["ticket_bundle"],
            stage7_result["eval_manifest"],
            stage7_result["pr_packet"],
            stage7_result["prompt_contract"],
            stage7_result["tool_schema"],
            stage7_result["golden_dataset"],
            stage7_result["latency_baseline"],
            stage7_result["eval_report"],
            stage7_result["security_review"],
            promotion_decision,
            stage7_result["work_item"],
            merge_decision=stage7_result.get("merge_decision"),
        )


def test_stage8_production_monitoring_rejects_merge_gated_lane_without_merge_decision() -> None:
    root = Path(__file__).resolve().parents[1]
    stage7_result = restricted_stage7_promoted_result(root)

    with pytest.raises(ProductionMonitoringConsistencyError):
        Stage8ProductionMonitoringPipeline(root).process(
            stage7_result.spec_packet,
            stage7_result.policy_decision,
            stage7_result.ticket_bundle,
            stage7_result.eval_manifest,
            stage7_result.pr_packet,
            stage7_result.prompt_contract,
            stage7_result.tool_schema,
            stage7_result.golden_dataset,
            stage7_result.latency_baseline,
            stage7_result.eval_report,
            stage7_result.security_review,
            stage7_result.promotion_decision,
            stage7_result.work_item,
        )
