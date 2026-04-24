from __future__ import annotations

from copy import deepcopy
from pathlib import Path

import pytest

from auto_mindsdb_factory.build_review import (
    BuildReviewConsistencyError,
    Stage3BuildReviewPipeline,
)
from auto_mindsdb_factory.contracts import load_validators, validation_errors_for
from auto_mindsdb_factory.controller import ControllerState
from auto_mindsdb_factory.intake import AnthropicScout, Stage1IntakePipeline
from auto_mindsdb_factory.ticketing import Stage2TicketingPipeline


def fixture_html() -> str:
    root = Path(__file__).resolve().parents[1]
    fixture_path = root / "fixtures" / "intake" / "anthropic-release-notes-sample.html"
    return fixture_path.read_text(encoding="utf-8")


def stage2_active_result(root: Path):
    item = AnthropicScout().list_items(
        html=fixture_html(),
        detected_at="2026-04-22T12:00:00Z",
    )[0]
    stage1_result = Stage1IntakePipeline(root).process_item(item)
    return Stage2TicketingPipeline(root).process(
        stage1_result.spec_packet,
        stage1_result.policy_decision,
        stage1_result.work_item,
    )


def test_stage3_build_review_emits_valid_pr_packet() -> None:
    root = Path(__file__).resolve().parents[1]
    validators = load_validators(root)
    stage2_result = stage2_active_result(root)
    pipeline = Stage3BuildReviewPipeline(root)

    result = pipeline.process(
        stage2_result.spec_packet,
        stage2_result.policy_decision,
        stage2_result.ticket_bundle,
        stage2_result.eval_manifest,
        stage2_result.work_item,
    )

    check_statuses = {check["name"]: check["status"] for check in result.pr_packet["checks"]}

    assert validation_errors_for(validators["pr-packet"], result.pr_packet) == []
    assert validation_errors_for(validators["work-item"], result.work_item.to_document()) == []
    assert result.work_item.state is ControllerState.PR_REVIEWABLE
    assert result.work_item.current_artifact_id == result.pr_packet["artifact"]["id"]
    assert result.work_item.attempt_count == 1
    assert result.pr_packet["artifact"]["owner_agent"] == "Reviewer"
    assert result.pr_packet["artifact"]["model_fingerprint"] == "reviewer.v1"
    assert pipeline.builder.PROMPT_CONTRACT_ID != pipeline.reviewer.PROMPT_CONTRACT_ID
    assert result.pr_packet["reviewer_report"]["approved"] is True
    assert result.pr_packet["merge_readiness"]["reviewable"] is True
    assert result.pr_packet["merge_readiness"]["mergeable"] is False
    assert check_statuses["Lint"] == "passed"
    assert check_statuses["Integration tests"] == "pending"
    assert any(
        blocker.startswith("Pending eval tiers before merge:")
        for blocker in result.pr_packet["merge_readiness"]["blockers"]
    )


def test_stage3_build_review_can_return_to_pr_revision() -> None:
    root = Path(__file__).resolve().parents[1]
    stage2_result = stage2_active_result(root)

    result = Stage3BuildReviewPipeline(root).process(
        stage2_result.spec_packet,
        stage2_result.policy_decision,
        stage2_result.ticket_bundle,
        stage2_result.eval_manifest,
        stage2_result.work_item,
        blocking_findings=["Compatibility fallbacks are still missing for legacy callers."],
    )

    assert result.work_item.state is ControllerState.PR_REVISION
    assert result.pr_packet["reviewer_report"]["approved"] is False
    assert result.pr_packet["reviewer_report"]["blocking_findings"] == [
        "Compatibility fallbacks are still missing for legacy callers."
    ]
    assert result.pr_packet["merge_readiness"]["reviewable"] is False
    assert result.pr_packet["merge_readiness"]["mergeable"] is False


def test_stage3_build_review_can_resume_from_pr_revision() -> None:
    root = Path(__file__).resolve().parents[1]
    initial_source = stage2_active_result(root)
    first_pass = Stage3BuildReviewPipeline(root).process(
        initial_source.spec_packet,
        initial_source.policy_decision,
        initial_source.ticket_bundle,
        initial_source.eval_manifest,
        initial_source.work_item,
        blocking_findings=["Compatibility fallbacks are still missing for legacy callers."],
    )

    retry_source = stage2_active_result(root)
    result = Stage3BuildReviewPipeline(root).process(
        retry_source.spec_packet,
        retry_source.policy_decision,
        retry_source.ticket_bundle,
        retry_source.eval_manifest,
        first_pass.work_item,
    )

    assert result.work_item.state is ControllerState.PR_REVIEWABLE
    assert result.work_item.attempt_count == 2
    assert result.pr_packet["reviewer_report"]["approved"] is True


def test_stage3_build_review_rejects_mismatched_eval_manifest() -> None:
    root = Path(__file__).resolve().parents[1]
    stage2_result = stage2_active_result(root)
    mismatched_eval_manifest = deepcopy(stage2_result.eval_manifest)
    mismatched_eval_manifest["target_id"] = "wrong-target"

    with pytest.raises(BuildReviewConsistencyError):
        Stage3BuildReviewPipeline(root).process(
            stage2_result.spec_packet,
            stage2_result.policy_decision,
            stage2_result.ticket_bundle,
            mismatched_eval_manifest,
            stage2_result.work_item,
        )


def test_stage3_build_review_rejects_missing_required_eval_tier() -> None:
    root = Path(__file__).resolve().parents[1]
    stage2_result = stage2_active_result(root)
    mismatched_eval_manifest = deepcopy(stage2_result.eval_manifest)
    mismatched_eval_manifest["tiers"] = mismatched_eval_manifest["tiers"][:1]

    with pytest.raises(BuildReviewConsistencyError):
        Stage3BuildReviewPipeline(root).process(
            stage2_result.spec_packet,
            stage2_result.policy_decision,
            stage2_result.ticket_bundle,
            mismatched_eval_manifest,
            stage2_result.work_item,
        )


def test_stage3_build_review_rejects_mismatched_work_item_identity() -> None:
    root = Path(__file__).resolve().parents[1]
    stage2_result = stage2_active_result(root)
    mismatched_work_item = deepcopy(stage2_result.work_item)
    mismatched_work_item.source_external_id = "wrong-stage3-item"

    with pytest.raises(BuildReviewConsistencyError):
        Stage3BuildReviewPipeline(root).process(
            stage2_result.spec_packet,
            stage2_result.policy_decision,
            stage2_result.ticket_bundle,
            stage2_result.eval_manifest,
            mismatched_work_item,
        )
