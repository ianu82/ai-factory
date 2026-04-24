from __future__ import annotations

from copy import deepcopy
import json
from pathlib import Path

import pytest

from auto_mindsdb_factory.contracts import load_validators, validation_errors_for
from auto_mindsdb_factory.controller import ControllerState, WorkItem
from auto_mindsdb_factory.feedback_synthesis import (
    FeedbackSynthesisConsistencyError,
    Stage9FeedbackSynthesisPipeline,
)


def _load_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def load_stage8_fixture(root: Path, scenario_name: str) -> dict[str, object]:
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
        "monitoring_report": _load_json(base / "monitoring-report.json"),
        "work_item": WorkItem.from_document(_load_json(base / "work-item.json")),
    }
    merge_path = base / "merge-decision.json"
    if merge_path.exists():
        result["merge_decision"] = _load_json(merge_path)
    return result


def test_stage9_feedback_synthesis_builds_weekly_rollup() -> None:
    root = Path(__file__).resolve().parents[1]
    validators = load_validators(root)
    stage8_result = load_stage8_fixture(root, "stage8_healthy_feature")

    result = Stage9FeedbackSynthesisPipeline(root).process(
        stage8_result["spec_packet"],
        stage8_result["policy_decision"],
        stage8_result["ticket_bundle"],
        stage8_result["eval_manifest"],
        stage8_result["pr_packet"],
        stage8_result["prompt_contract"],
        stage8_result["tool_schema"],
        stage8_result["golden_dataset"],
        stage8_result["latency_baseline"],
        stage8_result["eval_report"],
        stage8_result["security_review"],
        stage8_result["promotion_decision"],
        stage8_result["monitoring_report"],
        stage8_result["work_item"],
        positive_surprises=["Users adopted the feature with no support escalations."],
    )

    assert validation_errors_for(validators["feedback-report"], result.feedback_report) == []
    assert validation_errors_for(validators["work-item"], result.work_item.to_document()) == []
    assert result.feedback_report["feedback_window"]["mode"] == "weekly_rollup"
    assert result.feedback_report["incident_learning_packets"] == []
    assert result.feedback_report["summary"]["incident_count"] == 0
    assert result.work_item.state is ControllerState.PRODUCTION_MONITORING
    assert result.work_item.current_artifact_id == result.feedback_report["artifact"]["id"]


def test_stage9_feedback_synthesis_creates_incident_learning_packet() -> None:
    root = Path(__file__).resolve().parents[1]
    stage8_result = load_stage8_fixture(root, "stage8_auto_mitigated_feature")

    result = Stage9FeedbackSynthesisPipeline(root).process(
        stage8_result["spec_packet"],
        stage8_result["policy_decision"],
        stage8_result["ticket_bundle"],
        stage8_result["eval_manifest"],
        stage8_result["pr_packet"],
        stage8_result["prompt_contract"],
        stage8_result["tool_schema"],
        stage8_result["golden_dataset"],
        stage8_result["latency_baseline"],
        stage8_result["eval_report"],
        stage8_result["security_review"],
        stage8_result["promotion_decision"],
        stage8_result["monitoring_report"],
        stage8_result["work_item"],
    )

    assert result.feedback_report["feedback_window"]["mode"] == "incident_follow_up"
    assert len(result.feedback_report["incident_learning_packets"]) == 1
    assert result.feedback_report["summary"]["incident_count"] == 1
    assert any(
        item["category"] == "eval_gap"
        for item in result.feedback_report["backlog_candidates"]
    )
    assert result.work_item.state is ControllerState.PRODUCTION_MONITORING


def test_stage9_feedback_synthesis_keeps_followup_healthy_windows_linked_to_open_incidents() -> None:
    root = Path(__file__).resolve().parents[1]
    stage8_result = load_stage8_fixture(root, "stage8_followup_healthy_after_incident_feature")

    result = Stage9FeedbackSynthesisPipeline(root).process(
        stage8_result["spec_packet"],
        stage8_result["policy_decision"],
        stage8_result["ticket_bundle"],
        stage8_result["eval_manifest"],
        stage8_result["pr_packet"],
        stage8_result["prompt_contract"],
        stage8_result["tool_schema"],
        stage8_result["golden_dataset"],
        stage8_result["latency_baseline"],
        stage8_result["eval_report"],
        stage8_result["security_review"],
        stage8_result["promotion_decision"],
        stage8_result["monitoring_report"],
        stage8_result["work_item"],
    )

    assert result.monitoring_report["monitoring_decision"]["status"] == "healthy"
    assert result.feedback_report["feedback_window"]["mode"] == "incident_follow_up"
    assert result.feedback_report["incident_learning_packets"][0]["summary"].startswith(
        "A prior production incident remains open"
    )


def test_stage9_feedback_synthesis_allows_recorded_repeat_runs() -> None:
    root = Path(__file__).resolve().parents[1]
    stage8_result = load_stage8_fixture(root, "stage8_healthy_feature")
    pipeline = Stage9FeedbackSynthesisPipeline(root)

    first_result = pipeline.process(
        stage8_result["spec_packet"],
        stage8_result["policy_decision"],
        stage8_result["ticket_bundle"],
        stage8_result["eval_manifest"],
        stage8_result["pr_packet"],
        stage8_result["prompt_contract"],
        stage8_result["tool_schema"],
        stage8_result["golden_dataset"],
        stage8_result["latency_baseline"],
        stage8_result["eval_report"],
        stage8_result["security_review"],
        stage8_result["promotion_decision"],
        stage8_result["monitoring_report"],
        stage8_result["work_item"],
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
        first_result.monitoring_report,
        first_result.work_item,
        positive_surprises=["Production stayed steady through the second review window."],
    )

    assert second_result.feedback_report["artifact"]["id"] == first_result.feedback_report["artifact"]["id"]
    assert second_result.feedback_report["signals"]["positive_surprises"] == [
        "Production stayed steady through the second review window."
    ]
    assert second_result.work_item.history[-1].event == "feedback_synthesized"


def test_stage9_feedback_synthesis_rejects_forged_prior_feedback_snapshot() -> None:
    root = Path(__file__).resolve().parents[1]
    stage8_result = load_stage8_fixture(root, "stage8_healthy_feature")
    work_item = deepcopy(stage8_result["work_item"])
    work_item.current_artifact_id = "feedback-report-fake-001"

    with pytest.raises(FeedbackSynthesisConsistencyError):
        Stage9FeedbackSynthesisPipeline(root).process(
            stage8_result["spec_packet"],
            stage8_result["policy_decision"],
            stage8_result["ticket_bundle"],
            stage8_result["eval_manifest"],
            stage8_result["pr_packet"],
            stage8_result["prompt_contract"],
            stage8_result["tool_schema"],
            stage8_result["golden_dataset"],
            stage8_result["latency_baseline"],
            stage8_result["eval_report"],
            stage8_result["security_review"],
            stage8_result["promotion_decision"],
            stage8_result["monitoring_report"],
            work_item,
        )


def test_stage9_feedback_synthesis_rejects_mismatched_monitoring_report() -> None:
    root = Path(__file__).resolve().parents[1]
    stage8_result = load_stage8_fixture(root, "stage8_healthy_feature")
    monitoring_report = deepcopy(stage8_result["monitoring_report"])
    monitoring_report["pr_packet_id"] = "pr-wrong-001"

    with pytest.raises(FeedbackSynthesisConsistencyError):
        Stage9FeedbackSynthesisPipeline(root).process(
            stage8_result["spec_packet"],
            stage8_result["policy_decision"],
            stage8_result["ticket_bundle"],
            stage8_result["eval_manifest"],
            stage8_result["pr_packet"],
            stage8_result["prompt_contract"],
            stage8_result["tool_schema"],
            stage8_result["golden_dataset"],
            stage8_result["latency_baseline"],
            stage8_result["eval_report"],
            stage8_result["security_review"],
            stage8_result["promotion_decision"],
            monitoring_report,
            stage8_result["work_item"],
        )


def test_stage9_feedback_synthesis_rejects_merge_gated_lane_without_merge_decision() -> None:
    root = Path(__file__).resolve().parents[1]
    stage8_result = load_stage8_fixture(root, "stage8_human_escalated_feature")

    with pytest.raises(FeedbackSynthesisConsistencyError):
        Stage9FeedbackSynthesisPipeline(root).process(
            stage8_result["spec_packet"],
            stage8_result["policy_decision"],
            stage8_result["ticket_bundle"],
            stage8_result["eval_manifest"],
            stage8_result["pr_packet"],
            stage8_result["prompt_contract"],
            stage8_result["tool_schema"],
            stage8_result["golden_dataset"],
            stage8_result["latency_baseline"],
            stage8_result["eval_report"],
            stage8_result["security_review"],
            stage8_result["promotion_decision"],
            stage8_result["monitoring_report"],
            stage8_result["work_item"],
        )
