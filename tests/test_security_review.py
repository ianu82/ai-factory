from __future__ import annotations

from copy import deepcopy
from pathlib import Path

import pytest

from auto_mindsdb_factory.build_review import Stage3BuildReviewPipeline
from auto_mindsdb_factory.contracts import load_validators, validation_errors_for
from auto_mindsdb_factory.controller import ControllerState
from auto_mindsdb_factory.eval_execution import Stage5EvalPipeline
from auto_mindsdb_factory.integration import Stage4IntegrationPipeline
from auto_mindsdb_factory.intake import AnthropicScout, Stage1IntakePipeline
from auto_mindsdb_factory.security_review import (
    SecurityReviewConsistencyError,
    Stage6SecurityReviewPipeline,
)
from auto_mindsdb_factory.ticketing import Stage2TicketingPipeline


def fixture_html() -> str:
    root = Path(__file__).resolve().parents[1]
    fixture_path = root / "fixtures" / "intake" / "anthropic-release-notes-sample.html"
    return fixture_path.read_text(encoding="utf-8")


def stage5_active_result(root: Path):
    item = AnthropicScout().list_items(
        html=fixture_html(),
        detected_at="2026-04-22T12:00:00Z",
    )[0]
    stage1_result = Stage1IntakePipeline(root).process_item(item)
    stage2_result = Stage2TicketingPipeline(root).process(
        stage1_result.spec_packet,
        stage1_result.policy_decision,
        stage1_result.work_item,
    )
    stage3_result = Stage3BuildReviewPipeline(root).process(
        stage2_result.spec_packet,
        stage2_result.policy_decision,
        stage2_result.ticket_bundle,
        stage2_result.eval_manifest,
        stage2_result.work_item,
    )
    stage4_result = Stage4IntegrationPipeline(root).process(
        stage3_result.spec_packet,
        stage3_result.policy_decision,
        stage3_result.ticket_bundle,
        stage3_result.eval_manifest,
        stage3_result.pr_packet,
        stage3_result.work_item,
    )
    return Stage5EvalPipeline(root).process(
        stage4_result.spec_packet,
        stage4_result.policy_decision,
        stage4_result.ticket_bundle,
        stage4_result.eval_manifest,
        stage4_result.pr_packet,
        stage4_result.prompt_contract,
        stage4_result.tool_schema,
        stage4_result.golden_dataset,
        stage4_result.latency_baseline,
        stage4_result.work_item,
    )


def require_security_signoff(stage5_result) -> None:
    approvals = ["security", "release"]
    stage5_result.policy_decision["required_approvals"] = approvals
    for artifact_owner in (
        stage5_result.policy_decision,
        stage5_result.ticket_bundle,
        stage5_result.eval_manifest,
        stage5_result.pr_packet,
        stage5_result.prompt_contract,
        stage5_result.tool_schema,
        stage5_result.golden_dataset,
        stage5_result.latency_baseline,
        stage5_result.eval_report,
    ):
        artifact_owner["artifact"]["approval_requirements"] = approvals


def test_stage6_security_review_auto_approves_guarded_pr() -> None:
    root = Path(__file__).resolve().parents[1]
    validators = load_validators(root)
    stage5_result = stage5_active_result(root)

    result = Stage6SecurityReviewPipeline(root).process(
        stage5_result.spec_packet,
        stage5_result.policy_decision,
        stage5_result.ticket_bundle,
        stage5_result.eval_manifest,
        stage5_result.pr_packet,
        stage5_result.prompt_contract,
        stage5_result.tool_schema,
        stage5_result.golden_dataset,
        stage5_result.latency_baseline,
        stage5_result.eval_report,
        stage5_result.work_item,
    )

    assert validation_errors_for(validators["security-review"], result.security_review) == []
    assert validation_errors_for(validators["pr-packet"], result.pr_packet) == []
    assert validation_errors_for(validators["work-item"], result.work_item.to_document()) == []
    assert result.security_review["signoff"]["status"] == "approved"
    assert result.security_review["signoff"]["mode"] == "auto"
    assert result.security_review["evaluated_pr_artifact_version"] == 2
    assert result.security_review["resulting_pr_artifact_version"] == result.pr_packet["artifact"]["version"]
    assert result.pr_packet["merge_readiness"]["mergeable"] is True
    assert result.work_item.state is ControllerState.SECURITY_APPROVED
    assert result.work_item.current_artifact_id == result.security_review["artifact"]["id"]


def test_stage6_security_review_can_hold_for_human_signoff_and_resume() -> None:
    root = Path(__file__).resolve().parents[1]
    stage5_result = stage5_active_result(root)
    require_security_signoff(stage5_result)
    pipeline = Stage6SecurityReviewPipeline(root)

    pending = pipeline.process(
        stage5_result.spec_packet,
        stage5_result.policy_decision,
        stage5_result.ticket_bundle,
        stage5_result.eval_manifest,
        stage5_result.pr_packet,
        stage5_result.prompt_contract,
        stage5_result.tool_schema,
        stage5_result.golden_dataset,
        stage5_result.latency_baseline,
        stage5_result.eval_report,
        stage5_result.work_item,
    )

    assert pending.security_review["signoff"]["status"] == "pending_human"
    assert pending.pr_packet["merge_readiness"]["reviewable"] is True
    assert pending.pr_packet["merge_readiness"]["mergeable"] is False
    assert pending.work_item.state is ControllerState.SECURITY_REVIEWING

    approved = pipeline.process(
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
        pending.work_item,
        approved_security_reviewers=["security-oncall"],
    )

    assert approved.security_review["signoff"]["status"] == "approved"
    assert approved.security_review["signoff"]["mode"] == "human_approved"
    assert approved.security_review["signoff"]["approvals_granted"] == ["security"]
    assert approved.security_review["signoff"]["downstream_approvals"] == ["release"]
    assert approved.pr_packet["merge_readiness"]["mergeable"] is True
    assert approved.work_item.state is ControllerState.SECURITY_APPROVED


def test_stage6_security_review_refreshes_pending_artifact_on_repeat_review() -> None:
    root = Path(__file__).resolve().parents[1]
    stage5_result = stage5_active_result(root)
    require_security_signoff(stage5_result)
    pipeline = Stage6SecurityReviewPipeline(root)

    pending = pipeline.process(
        stage5_result.spec_packet,
        stage5_result.policy_decision,
        stage5_result.ticket_bundle,
        stage5_result.eval_manifest,
        stage5_result.pr_packet,
        stage5_result.prompt_contract,
        stage5_result.tool_schema,
        stage5_result.golden_dataset,
        stage5_result.latency_baseline,
        stage5_result.eval_report,
        stage5_result.work_item,
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
        pending.work_item,
    )

    assert repeated.security_review["artifact"]["id"] != pending.security_review["artifact"]["id"]
    assert repeated.work_item.state is ControllerState.SECURITY_REVIEWING
    assert repeated.work_item.current_artifact_id == repeated.security_review["artifact"]["id"]
    assert repeated.work_item.updated_at == repeated.security_review["artifact"]["updated_at"]


def test_stage6_security_review_can_return_to_pr_revision() -> None:
    root = Path(__file__).resolve().parents[1]
    stage5_result = stage5_active_result(root)

    result = Stage6SecurityReviewPipeline(root).process(
        stage5_result.spec_packet,
        stage5_result.policy_decision,
        stage5_result.ticket_bundle,
        stage5_result.eval_manifest,
        stage5_result.pr_packet,
        stage5_result.prompt_contract,
        stage5_result.tool_schema,
        stage5_result.golden_dataset,
        stage5_result.latency_baseline,
        stage5_result.eval_report,
        stage5_result.work_item,
        blocking_findings=["Prompt injection controls do not cover a newly added tool response path."],
    )

    assert result.security_review["signoff"]["status"] == "blocked"
    assert result.pr_packet["merge_readiness"]["reviewable"] is False
    assert result.pr_packet["merge_readiness"]["mergeable"] is False
    assert result.pr_packet["reviewer_report"]["blocking_findings"]
    assert result.work_item.state is ControllerState.PR_REVISION


def test_stage6_security_review_rejects_mismatched_eval_report() -> None:
    root = Path(__file__).resolve().parents[1]
    stage5_result = stage5_active_result(root)
    eval_report = deepcopy(stage5_result.eval_report)
    eval_report["prompt_contract_id"] = "wrong-prompt-contract"

    with pytest.raises(SecurityReviewConsistencyError):
        Stage6SecurityReviewPipeline(root).process(
            stage5_result.spec_packet,
            stage5_result.policy_decision,
            stage5_result.ticket_bundle,
            stage5_result.eval_manifest,
            stage5_result.pr_packet,
            stage5_result.prompt_contract,
            stage5_result.tool_schema,
            stage5_result.golden_dataset,
            stage5_result.latency_baseline,
            eval_report,
            stage5_result.work_item,
        )
