from __future__ import annotations

import json
from pathlib import Path

import pytest

from auto_mindsdb_factory.controller import (
    ControllerEvent,
    ControllerState,
    FactoryController,
    InvalidTransitionError,
)


def test_invalid_transition_is_rejected() -> None:
    controller = FactoryController()
    work_item = controller.create_work_item(
        source_provider="anthropic",
        source_external_id="demo-invalid-transition",
        title="Demo invalid transition",
        work_item_id="wi-invalid-001",
    )

    with pytest.raises(InvalidTransitionError):
        controller.apply_event(work_item, event=ControllerEvent.REVIEWABLE_TRUE)


def test_replay_fast_lane_scenario() -> None:
    controller = FactoryController()
    scenario = Path(__file__).resolve().parents[1] / "fixtures" / "scenarios" / "fast_lane_feature"
    replayed = controller.replay_scenario(scenario)

    assert replayed.state is ControllerState.PR_MERGEABLE
    assert replayed.execution_lane == "fast"
    assert replayed.policy_decision_id == "policy-fast-001"
    assert replayed.current_artifact_id == "pr-fast-001"
    assert replayed.attempt_count == 1


def test_replay_watchlist_scenario() -> None:
    controller = FactoryController()
    scenario = Path(__file__).resolve().parents[1] / "fixtures" / "scenarios" / "watchlist_feature"
    replayed = controller.replay_scenario(scenario)

    assert replayed.state is ControllerState.WATCHLISTED
    assert replayed.current_artifact_id == "policy-watch-001"
    assert replayed.policy_decision_id == "policy-watch-001"
    assert replayed.risk_score == 35


def test_retry_budget_exhausted_can_dead_letter_from_active_state() -> None:
    controller = FactoryController()
    work_item = controller.create_work_item(
        source_provider="anthropic",
        source_external_id="demo-retry-budget",
        title="Demo retry budget exhaustion",
        work_item_id="wi-retry-001",
    )
    controller.apply_event(work_item, event=ControllerEvent.CHANGELOG_ITEM_RECORDED)
    controller.apply_event(
        work_item,
        event=ControllerEvent.RETRY_BUDGET_EXHAUSTED,
        dead_letter_reason="builder retry budget exhausted",
    )

    assert work_item.state is ControllerState.DEAD_LETTER
    assert work_item.dead_letter_reason == "builder retry budget exhausted"


def test_replay_backlog_candidate_scenario_stops_at_policy_assignment() -> None:
    controller = FactoryController()
    scenario = Path(__file__).resolve().parents[1] / "fixtures" / "scenarios" / "backlog_candidate"
    replayed = controller.replay_scenario(scenario)

    assert replayed.state is ControllerState.POLICY_ASSIGNED
    assert replayed.execution_lane == "guarded"
    assert replayed.policy_decision_id == "policy-backlog-001"
    assert replayed.current_artifact_id == "policy-backlog-001"


def test_replay_stage5_mergeable_scenario() -> None:
    controller = FactoryController()
    scenario = (
        Path(__file__).resolve().parents[1]
        / "fixtures"
        / "scenarios"
        / "stage5_mergeable_feature"
    )
    replayed = controller.replay_scenario(scenario)
    expected_eval_report_id = json.loads((scenario / "eval-report.json").read_text())["artifact"]["id"]

    assert replayed.state is ControllerState.PR_MERGEABLE
    assert replayed.current_artifact_id == expected_eval_report_id


def test_replay_stage5_revision_scenario() -> None:
    controller = FactoryController()
    scenario = (
        Path(__file__).resolve().parents[1]
        / "fixtures"
        / "scenarios"
        / "stage5_revision_feature"
    )
    replayed = controller.replay_scenario(scenario)
    expected_eval_report_id = json.loads((scenario / "eval-report.json").read_text())["artifact"]["id"]

    assert replayed.state is ControllerState.PR_REVISION
    assert replayed.current_artifact_id == expected_eval_report_id


def test_replay_stage6_security_approved_scenario() -> None:
    controller = FactoryController()
    scenario = (
        Path(__file__).resolve().parents[1]
        / "fixtures"
        / "scenarios"
        / "stage6_security_approved_feature"
    )
    replayed = controller.replay_scenario(scenario)
    expected_security_review_id = json.loads(
        (scenario / "security-review.json").read_text()
    )["artifact"]["id"]

    assert replayed.state is ControllerState.SECURITY_APPROVED
    assert replayed.current_artifact_id == expected_security_review_id


def test_replay_merge_merged_scenario() -> None:
    controller = FactoryController()
    scenario = (
        Path(__file__).resolve().parents[1]
        / "fixtures"
        / "scenarios"
        / "merge_merged_feature"
    )
    replayed = controller.replay_scenario(scenario)
    expected_merge_decision_id = json.loads(
        (scenario / "merge-decision.json").read_text()
    )["artifact"]["id"]

    assert replayed.state is ControllerState.MERGED
    assert replayed.current_artifact_id == expected_merge_decision_id


def test_replay_stage6_security_pending_scenario() -> None:
    controller = FactoryController()
    scenario = (
        Path(__file__).resolve().parents[1]
        / "fixtures"
        / "scenarios"
        / "stage6_security_pending_feature"
    )
    replayed = controller.replay_scenario(scenario)
    expected_security_review_id = json.loads(
        (scenario / "security-review.json").read_text()
    )["artifact"]["id"]

    assert replayed.state is ControllerState.SECURITY_REVIEWING
    assert replayed.current_artifact_id == expected_security_review_id


def test_replay_stage6_security_revision_scenario() -> None:
    controller = FactoryController()
    scenario = (
        Path(__file__).resolve().parents[1]
        / "fixtures"
        / "scenarios"
        / "stage6_security_revision_feature"
    )
    replayed = controller.replay_scenario(scenario)
    expected_security_review_id = json.loads(
        (scenario / "security-review.json").read_text()
    )["artifact"]["id"]

    assert replayed.state is ControllerState.PR_REVISION
    assert replayed.current_artifact_id == expected_security_review_id


def test_replay_stage7_production_monitoring_scenario() -> None:
    controller = FactoryController()
    scenario = (
        Path(__file__).resolve().parents[1]
        / "fixtures"
        / "scenarios"
        / "stage7_production_monitoring_feature"
    )
    replayed = controller.replay_scenario(scenario)
    expected_promotion_decision_id = json.loads(
        (scenario / "promotion-decision.json").read_text()
    )["artifact"]["id"]

    assert replayed.state is ControllerState.PRODUCTION_MONITORING
    assert replayed.current_artifact_id == expected_promotion_decision_id


def test_replay_stage7_staging_pending_scenario() -> None:
    controller = FactoryController()
    scenario = (
        Path(__file__).resolve().parents[1]
        / "fixtures"
        / "scenarios"
        / "stage7_staging_pending_feature"
    )
    replayed = controller.replay_scenario(scenario)
    expected_promotion_decision_id = json.loads(
        (scenario / "promotion-decision.json").read_text()
    )["artifact"]["id"]

    assert replayed.state is ControllerState.STAGING_SOAK
    assert replayed.current_artifact_id == expected_promotion_decision_id


def test_replay_stage7_staging_revision_scenario() -> None:
    controller = FactoryController()
    scenario = (
        Path(__file__).resolve().parents[1]
        / "fixtures"
        / "scenarios"
        / "stage7_staging_revision_feature"
    )
    replayed = controller.replay_scenario(scenario)
    expected_promotion_decision_id = json.loads(
        (scenario / "promotion-decision.json").read_text()
    )["artifact"]["id"]

    assert replayed.state is ControllerState.PR_REVISION
    assert replayed.current_artifact_id == expected_promotion_decision_id


def test_replay_stage8_healthy_scenario() -> None:
    controller = FactoryController()
    scenario = (
        Path(__file__).resolve().parents[1]
        / "fixtures"
        / "scenarios"
        / "stage8_healthy_feature"
    )
    replayed = controller.replay_scenario(scenario)
    expected_monitoring_report_id = json.loads(
        (scenario / "monitoring-report.json").read_text()
    )["artifact"]["id"]

    assert replayed.state is ControllerState.PRODUCTION_MONITORING
    assert replayed.current_artifact_id == expected_monitoring_report_id


def test_replay_stage8_auto_mitigated_scenario() -> None:
    controller = FactoryController()
    scenario = (
        Path(__file__).resolve().parents[1]
        / "fixtures"
        / "scenarios"
        / "stage8_auto_mitigated_feature"
    )
    replayed = controller.replay_scenario(scenario)
    expected_monitoring_report_id = json.loads(
        (scenario / "monitoring-report.json").read_text()
    )["artifact"]["id"]

    assert replayed.state is ControllerState.PRODUCTION_MONITORING
    assert replayed.current_artifact_id == expected_monitoring_report_id


def test_replay_stage8_human_escalated_scenario() -> None:
    controller = FactoryController()
    scenario = (
        Path(__file__).resolve().parents[1]
        / "fixtures"
        / "scenarios"
        / "stage8_human_escalated_feature"
    )
    replayed = controller.replay_scenario(scenario)
    expected_monitoring_report_id = json.loads(
        (scenario / "monitoring-report.json").read_text()
    )["artifact"]["id"]

    assert replayed.state is ControllerState.PRODUCTION_MONITORING
    assert replayed.current_artifact_id == expected_monitoring_report_id


def test_replay_stage8_followup_healthy_after_incident_scenario() -> None:
    controller = FactoryController()
    scenario = (
        Path(__file__).resolve().parents[1]
        / "fixtures"
        / "scenarios"
        / "stage8_followup_healthy_after_incident_feature"
    )
    replayed = controller.replay_scenario(scenario)
    expected_monitoring_report_id = json.loads(
        (scenario / "monitoring-report.json").read_text()
    )["artifact"]["id"]

    assert replayed.state is ControllerState.PRODUCTION_MONITORING
    assert replayed.current_artifact_id == expected_monitoring_report_id


def test_replay_stage9_feedback_healthy_scenario() -> None:
    controller = FactoryController()
    scenario = (
        Path(__file__).resolve().parents[1]
        / "fixtures"
        / "scenarios"
        / "stage9_feedback_healthy_feature"
    )
    replayed = controller.replay_scenario(scenario)
    expected_feedback_report_id = json.loads(
        (scenario / "feedback-report.json").read_text()
    )["artifact"]["id"]

    assert replayed.state is ControllerState.PRODUCTION_MONITORING
    assert replayed.current_artifact_id == expected_feedback_report_id


def test_replay_stage9_feedback_followup_incident_scenario() -> None:
    controller = FactoryController()
    scenario = (
        Path(__file__).resolve().parents[1]
        / "fixtures"
        / "scenarios"
        / "stage9_feedback_followup_incident_feature"
    )
    replayed = controller.replay_scenario(scenario)
    expected_feedback_report_id = json.loads(
        (scenario / "feedback-report.json").read_text()
    )["artifact"]["id"]

    assert replayed.state is ControllerState.PRODUCTION_MONITORING
    assert replayed.current_artifact_id == expected_feedback_report_id
