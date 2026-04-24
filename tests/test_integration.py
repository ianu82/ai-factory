from __future__ import annotations

from copy import deepcopy
from pathlib import Path

import pytest

from auto_mindsdb_factory.build_review import Stage3BuildReviewPipeline
from auto_mindsdb_factory.contracts import load_validators, validation_errors_for
from auto_mindsdb_factory.integration import (
    IntegrationConsistencyError,
    IntegrationEligibilityError,
    Stage4IntegrationPipeline,
)
from auto_mindsdb_factory.controller import ControllerState
from auto_mindsdb_factory.intake import AnthropicScout, Stage1IntakePipeline
from auto_mindsdb_factory.ticketing import Stage2TicketingPipeline


def fixture_html() -> str:
    root = Path(__file__).resolve().parents[1]
    fixture_path = root / "fixtures" / "intake" / "anthropic-release-notes-sample.html"
    return fixture_path.read_text(encoding="utf-8")


def stage3_active_result(root: Path):
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
    return Stage3BuildReviewPipeline(root).process(
        stage2_result.spec_packet,
        stage2_result.policy_decision,
        stage2_result.ticket_bundle,
        stage2_result.eval_manifest,
        stage2_result.work_item,
    )


def test_stage4_integration_emits_valid_artifacts() -> None:
    root = Path(__file__).resolve().parents[1]
    validators = load_validators(root)
    stage3_result = stage3_active_result(root)

    result = Stage4IntegrationPipeline(root).process(
        stage3_result.spec_packet,
        stage3_result.policy_decision,
        stage3_result.ticket_bundle,
        stage3_result.eval_manifest,
        stage3_result.pr_packet,
        stage3_result.work_item,
    )

    tool_ids = [tool["id"] for tool in result.tool_schema["tools"]]

    assert validation_errors_for(validators["prompt-contract"], result.prompt_contract) == []
    assert validation_errors_for(validators["tool-schema"], result.tool_schema) == []
    assert validation_errors_for(validators["golden-dataset"], result.golden_dataset) == []
    assert validation_errors_for(validators["latency-baseline"], result.latency_baseline) == []
    assert result.work_item.state is ControllerState.PR_REVIEWABLE
    assert result.work_item.current_artifact_id == stage3_result.pr_packet["artifact"]["id"]
    assert result.prompt_contract["tool_schema_id"] == result.tool_schema["artifact"]["id"]
    assert result.prompt_contract["golden_dataset_id"] == result.golden_dataset["artifact"]["id"]
    assert result.latency_baseline["prompt_contract_id"] == result.prompt_contract["artifact"]["id"]
    assert result.latency_baseline["pr_packet_id"] == stage3_result.pr_packet["artifact"]["id"]
    assert result.prompt_contract["tool_choice_policy"]["allowed_tool_ids"] == tool_ids
    assert "response-format-adapter" in tool_ids
    assert "tool-result-normalizer" in tool_ids
    assert result.golden_dataset["coverage_summary"]["failure_injection_count"] == len(
        result.golden_dataset["failure_injection_cases"]
    )
    assert result.golden_dataset["coverage_summary"]["failure_injection_count"] >= 3
    assert result.latency_baseline["reference_check_ids"]
    assert len(result.to_document()["history"]) == len(result.work_item.history)


def test_stage4_integration_supports_integration_only_surface() -> None:
    root = Path(__file__).resolve().parents[1]
    stage3_result = stage3_active_result(root)
    spec_packet = deepcopy(stage3_result.spec_packet)
    spec_packet["summary"]["affected_surfaces"] = ["anthropic_integration"]
    spec_packet["risk_profile"]["factors"] = [
        factor
        for factor in spec_packet["risk_profile"]["factors"]
        if factor["name"] != "new_tool_permission"
    ]
    ticket_bundle = deepcopy(stage3_result.ticket_bundle)
    ticket_bundle["tickets"] = [deepcopy(ticket_bundle["tickets"][1])]
    ticket_bundle["tickets"][0]["kind"] = "llm_integration"
    ticket_bundle["tickets"][0]["dependencies"] = []
    ticket_bundle["dependency_graph"] = []
    pr_packet = deepcopy(stage3_result.pr_packet)
    pr_packet["ticket_ids"] = [ticket_bundle["tickets"][0]["id"]]

    result = Stage4IntegrationPipeline(root).process(
        spec_packet,
        stage3_result.policy_decision,
        ticket_bundle,
        stage3_result.eval_manifest,
        pr_packet,
        stage3_result.work_item,
    )

    tool_ids = [tool["id"] for tool in result.tool_schema["tools"]]
    entry_tags = [set(entry["tags"]) for entry in result.golden_dataset["entries"]]
    failure_tags = [set(case["tags"]) for case in result.golden_dataset["failure_injection_cases"]]
    context_kinds = {source["kind"] for source in result.prompt_contract["context_assembly"]["sources"]}
    assert tool_ids == ["anthropic-response-guard"]
    assert result.prompt_contract["tool_choice_policy"]["allowed_tool_ids"] == tool_ids
    assert any("anthropic_integration" in tags for tags in entry_tags)
    assert all("api_contract" not in tags for tags in entry_tags)
    assert all("tool_schema" not in tags for tags in entry_tags)
    assert any("anthropic_integration" in tags for tags in failure_tags)
    assert result.golden_dataset["coverage_summary"]["failure_injection_count"] >= 3
    assert "tool_result" not in context_kinds


def test_stage4_integration_rejects_non_model_touching_changes() -> None:
    root = Path(__file__).resolve().parents[1]
    stage3_result = stage3_active_result(root)
    spec_packet = deepcopy(stage3_result.spec_packet)
    spec_packet["summary"]["affected_surfaces"] = ["api_contract"]
    spec_packet["risk_profile"]["factors"] = [
        factor
        for factor in spec_packet["risk_profile"]["factors"]
        if factor["name"] != "new_tool_permission"
    ]
    ticket_bundle = deepcopy(stage3_result.ticket_bundle)
    ticket_bundle["tickets"] = [deepcopy(ticket_bundle["tickets"][0])]
    ticket_bundle["dependency_graph"] = []
    pr_packet = deepcopy(stage3_result.pr_packet)
    pr_packet["ticket_ids"] = [ticket_bundle["tickets"][0]["id"]]

    with pytest.raises(IntegrationEligibilityError):
        Stage4IntegrationPipeline(root).process(
            spec_packet,
            stage3_result.policy_decision,
            ticket_bundle,
            stage3_result.eval_manifest,
            pr_packet,
            stage3_result.work_item,
        )


def test_stage4_integration_rejects_mismatched_pr_packet_ticket_ids() -> None:
    root = Path(__file__).resolve().parents[1]
    stage3_result = stage3_active_result(root)
    pr_packet = deepcopy(stage3_result.pr_packet)
    pr_packet["ticket_ids"] = ["wrong-ticket-id"]

    with pytest.raises(IntegrationConsistencyError):
        Stage4IntegrationPipeline(root).process(
            stage3_result.spec_packet,
            stage3_result.policy_decision,
            stage3_result.ticket_bundle,
            stage3_result.eval_manifest,
            pr_packet,
            stage3_result.work_item,
        )
