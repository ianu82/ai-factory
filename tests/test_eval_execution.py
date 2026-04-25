from __future__ import annotations

import sys
from copy import deepcopy
from pathlib import Path

import pytest

from auto_mindsdb_factory.build_review import Stage3BuildReviewPipeline
from auto_mindsdb_factory.contracts import load_validators, validation_errors_for
from auto_mindsdb_factory.controller import ControllerState
from auto_mindsdb_factory.eval_common import deferred_tiers, merge_gate_tiers
from auto_mindsdb_factory.eval_execution import (
    CommandGateRunner,
    EvalExecutionConsistencyError,
    Stage5EvalPipeline,
)
from auto_mindsdb_factory.integration import Stage4IntegrationPipeline
from auto_mindsdb_factory.intake import AnthropicScout, Stage1IntakePipeline
from auto_mindsdb_factory.ticketing import Stage2TicketingPipeline


def fixture_html() -> str:
    root = Path(__file__).resolve().parents[1]
    fixture_path = root / "fixtures" / "intake" / "anthropic-release-notes-sample.html"
    return fixture_path.read_text(encoding="utf-8")


def stage4_active_result(root: Path):
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
    return Stage4IntegrationPipeline(root).process(
        stage3_result.spec_packet,
        stage3_result.policy_decision,
        stage3_result.ticket_bundle,
        stage3_result.eval_manifest,
        stage3_result.pr_packet,
        stage3_result.work_item,
    )


def test_stage5_eval_execution_emits_valid_eval_report_and_mergeable_pr() -> None:
    root = Path(__file__).resolve().parents[1]
    validators = load_validators(root)
    stage4_result = stage4_active_result(root)

    result = Stage5EvalPipeline(root).process(
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

    check_statuses = {check["name"]: check["status"] for check in result.pr_packet["checks"]}
    expected_merge_gate_tiers = merge_gate_tiers(stage4_result.eval_manifest)
    expected_deferred_tiers = deferred_tiers(stage4_result.eval_manifest)

    assert validation_errors_for(validators["eval-report"], result.eval_report) == []
    assert validation_errors_for(validators["pr-packet"], result.pr_packet) == []
    assert validation_errors_for(validators["work-item"], result.work_item.to_document()) == []
    assert result.eval_report["build_attempt"] == 1
    assert result.eval_report["evaluated_pr_artifact_version"] == 1
    assert result.eval_report["resulting_pr_artifact_version"] == result.pr_packet["artifact"]["version"]
    assert "attempt-1-prv-1" in result.eval_report["artifact"]["id"]
    assert result.eval_report["summary"]["merge_gate_passed"] is True
    assert result.eval_report["summary"]["merge_gate_tiers"] == expected_merge_gate_tiers
    assert result.eval_report["summary"]["deferred_tiers"] == expected_deferred_tiers
    assert result.pr_packet["merge_readiness"]["reviewable"] is True
    assert result.pr_packet["merge_readiness"]["mergeable"] is True
    assert result.pr_packet["reviewer_report"]["approved"] is True
    assert result.work_item.state is ControllerState.PR_MERGEABLE
    assert result.work_item.current_artifact_id == result.eval_report["artifact"]["id"]
    assert "failed" not in set(check_statuses.values())
    if expected_deferred_tiers:
        assert result.eval_report["summary"]["pending_check_ids"]
    else:
        assert result.eval_report["summary"]["pending_check_ids"] == []
    if expected_deferred_tiers:
        assert any(
            finding.startswith("Deferred eval tiers after merge:")
            for finding in result.pr_packet["reviewer_report"]["non_blocking_findings"]
        )


def test_stage5_eval_execution_can_return_to_pr_revision_on_eval_failure() -> None:
    root = Path(__file__).resolve().parents[1]
    stage4_result = stage4_active_result(root)
    pre_merge_tier = next(
        tier for tier in stage4_result.eval_manifest["tiers"] if tier["name"] == "pre_merge"
    )
    failing_check_id = pre_merge_tier["checks"][0]["id"]

    result = Stage5EvalPipeline(root).process(
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
        failing_check_ids=[failing_check_id],
    )

    assert result.eval_report["summary"]["merge_gate_passed"] is False
    assert result.eval_report["summary"]["failing_merge_gate_tiers"] == ["pre_merge"]
    assert result.eval_report["build_attempt"] == 1
    assert result.eval_report["evaluated_pr_artifact_version"] == 1
    assert result.eval_report["resulting_pr_artifact_version"] == result.pr_packet["artifact"]["version"]
    assert result.pr_packet["reviewer_report"]["approved"] is False
    assert result.pr_packet["merge_readiness"]["reviewable"] is False
    assert result.pr_packet["merge_readiness"]["mergeable"] is False
    assert result.work_item.state is ControllerState.PR_REVISION
    assert result.work_item.current_artifact_id == result.eval_report["artifact"]["id"]
    assert result.pr_packet["reviewer_report"]["blocking_findings"]
    assert any(
        finding.startswith("Deferred eval tiers remain queued once merge gates pass:")
        for finding in result.pr_packet["reviewer_report"]["non_blocking_findings"]
    )


def test_stage5_eval_execution_surfaces_optional_eval_warnings_without_blocking_merge() -> None:
    root = Path(__file__).resolve().parents[1]
    stage4_result = stage4_active_result(root)
    pre_merge_tier = next(
        tier for tier in stage4_result.eval_manifest["tiers"] if tier["name"] == "pre_merge"
    )
    optional_check_id = next(
        check["id"] for check in pre_merge_tier["checks"] if not check["required"]
    )

    result = Stage5EvalPipeline(root).process(
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
        failing_check_ids=[optional_check_id],
    )

    assert result.eval_report["summary"]["merge_gate_passed"] is True
    assert result.eval_report["summary"]["warning_count"] == 1
    assert result.pr_packet["merge_readiness"]["mergeable"] is True
    assert any(
        finding.startswith("Optional eval warnings in pre_merge:")
        for finding in result.pr_packet["reviewer_report"]["non_blocking_findings"]
    )


def test_stage5_command_gates_run_real_commands_and_defer_unconfigured_checks(tmp_path) -> None:
    root = Path(__file__).resolve().parents[1]
    stage4_result = stage4_active_result(root)
    runner = CommandGateRunner(
        root,
        commands_by_kind={
            "unit": [sys.executable, "-c", "raise SystemExit(0)"],
            "contract": [sys.executable, "-c", "raise SystemExit(0)"],
        },
        required_kinds={"unit", "contract"},
    )

    result = Stage5EvalPipeline(root, gate_runner=runner).process(
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

    summary = result.eval_report["summary"]
    statuses = {
        check["kind"]: check["status"]
        for tier in result.eval_report["tiers"]
        for check in tier["checks"]
    }
    assert summary["merge_gate_passed"] is True
    assert statuses["unit"] == "passed"
    assert statuses["contract"] == "passed"
    assert "not_configured" in {
        check["status"]
        for tier in result.eval_report["tiers"]
        for check in tier["checks"]
        if check["kind"] in {"latency", "cost", "llm_quality", "integration"}
    }
    assert summary["not_configured_check_ids"]


def test_stage5_command_gate_failure_returns_to_revision() -> None:
    root = Path(__file__).resolve().parents[1]
    stage4_result = stage4_active_result(root)
    runner = CommandGateRunner(
        root,
        commands_by_kind={
            "unit": [sys.executable, "-c", "raise SystemExit(7)"],
            "contract": [sys.executable, "-c", "raise SystemExit(0)"],
        },
        required_kinds={"unit", "contract"},
    )

    result = Stage5EvalPipeline(root, gate_runner=runner).process(
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

    assert result.eval_report["summary"]["merge_gate_passed"] is False
    assert result.work_item.state is ControllerState.PR_REVISION
    assert any(
        finding.startswith("Eval gate failed")
        for finding in result.pr_packet["reviewer_report"]["blocking_findings"]
    )


def test_stage5_eval_execution_rejects_mismatched_latency_baseline() -> None:
    root = Path(__file__).resolve().parents[1]
    stage4_result = stage4_active_result(root)
    latency_baseline = deepcopy(stage4_result.latency_baseline)
    latency_baseline["pr_packet_id"] = "wrong-pr-packet"

    with pytest.raises(EvalExecutionConsistencyError):
        Stage5EvalPipeline(root).process(
            stage4_result.spec_packet,
            stage4_result.policy_decision,
            stage4_result.ticket_bundle,
            stage4_result.eval_manifest,
            stage4_result.pr_packet,
            stage4_result.prompt_contract,
            stage4_result.tool_schema,
            stage4_result.golden_dataset,
            latency_baseline,
            stage4_result.work_item,
        )
