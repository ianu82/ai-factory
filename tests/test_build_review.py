from __future__ import annotations

from copy import deepcopy
from pathlib import Path

import pytest

from auto_mindsdb_factory.build_review import (
    BuildReviewConsistencyError,
    Stage3BuildReviewPipeline,
)
from auto_mindsdb_factory.connectors import AgentResult
from auto_mindsdb_factory.contracts import load_validators, validation_errors_for
from auto_mindsdb_factory.controller import ControllerState
from auto_mindsdb_factory.intake import AnthropicScout, Stage1IntakePipeline, build_manual_intake_item
from auto_mindsdb_factory.ticketing import Stage2TicketingPipeline


class ScriptedAgentConnector:
    def __init__(self, outputs: dict[str, dict]) -> None:
        self.outputs = outputs

    def run_task(self, task):
        return AgentResult(
            name=task.name,
            output_document=self.outputs[task.name],
            model_fingerprint="openai.responses:gpt-5.4",
            provider="openai",
            model="gpt-5.4",
            response_id="resp_test",
        )


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


def stage2_manual_issue_result(root: Path):
    item = build_manual_intake_item(
        provider="github",
        external_id="github-issue-2",
        title="Factory cockpit should surface GitHub check conclusions and eval status",
        url="https://github.com/ianu82/ai-factory/issues/2",
        detected_at="2026-04-24T12:00:00Z",
        published_at="2026-04-24T11:30:00Z",
        body=(
            "The operator cockpit should surface GitHub pull request check conclusions, local eval "
            "status, and a clear health summary for each work item. This is a control-plane API and "
            "JSON schema change for the cockpit command, not a model-runtime change. Operators should "
            "not need to cross-check multiple artifacts to decide whether a run is healthy. Acceptance "
            "criteria: - update the factory cockpit tool output to include the latest GitHub check "
            "conclusions for each run - include the latest local eval status summary from vertical-slice "
            "or automation artifacts - add a single health field that resolves to ready, blocked, or "
            "warning based on PR checks, eval status, and monitoring alerts - cover the new output with "
            "CLI tests and contract-safe validation"
        ),
    )
    stage1_result = Stage1IntakePipeline(root).process_item(item)
    return Stage2TicketingPipeline(root).process(
        stage1_result.spec_packet,
        stage1_result.policy_decision,
        stage1_result.work_item,
    )


def stage2_linear_issue_result(root: Path):
    item = build_manual_intake_item(
        provider="linear",
        external_id="linear-issue-2",
        title="Factory API should surface Linear intake status in the cockpit",
        url="https://linear.app/example/issue/ENG-123/factory-intake",
        detected_at="2026-04-24T12:00:00Z",
        published_at="2026-04-24T11:30:00Z",
        body=(
            "The operator cockpit API should surface Linear-triggered factory runs and their status. "
            "This is a control-plane API and response format change for the cockpit command, not a "
            "model-runtime change. Acceptance criteria: - include the latest Linear-triggered run "
            "status in the cockpit JSON output - show whether Stage 1 accepted or rejected the issue "
            "in the response format - keep the response schema compatibility-safe for existing callers "
            "- cover the output with CLI tests"
        ),
    )
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


def test_stage3_build_review_uses_factory_paths_for_manual_github_issues() -> None:
    root = Path(__file__).resolve().parents[1]
    stage2_result = stage2_manual_issue_result(root)

    result = Stage3BuildReviewPipeline(root).process(
        stage2_result.spec_packet,
        stage2_result.policy_decision,
        stage2_result.ticket_bundle,
        stage2_result.eval_manifest,
        stage2_result.work_item,
        repository="ianu82/ai-factory",
    )

    assert "src/auto_mindsdb_factory/__main__.py" in result.pr_packet["changed_paths"]
    assert "tests/test_cli.py" in result.pr_packet["changed_paths"]
    assert "src/auto_mindsdb_factory/vertical_slice.py" in result.pr_packet["changed_paths"]
    assert "tests/test_vertical_slice.py" in result.pr_packet["changed_paths"]
    assert not any(
        path.startswith("integrations/anthropic/")
        for path in result.pr_packet["changed_paths"]
    )


def test_stage3_build_review_uses_factory_paths_for_manual_linear_issues() -> None:
    root = Path(__file__).resolve().parents[1]
    stage2_result = stage2_linear_issue_result(root)

    result = Stage3BuildReviewPipeline(root).process(
        stage2_result.spec_packet,
        stage2_result.policy_decision,
        stage2_result.ticket_bundle,
        stage2_result.eval_manifest,
        stage2_result.work_item,
        repository="ianu82/ai-factory",
    )

    assert "src/auto_mindsdb_factory/__main__.py" in result.pr_packet["changed_paths"]
    assert "tests/test_cli.py" in result.pr_packet["changed_paths"]
    assert "src/auto_mindsdb_factory/vertical_slice.py" in result.pr_packet["changed_paths"]
    assert "tests/test_vertical_slice.py" in result.pr_packet["changed_paths"]
    assert not any(
        path.startswith("integrations/anthropic/")
        for path in result.pr_packet["changed_paths"]
    )


def test_stage3_build_review_can_use_agent_drafts() -> None:
    root = Path(__file__).resolve().parents[1]
    stage2_result = stage2_manual_issue_result(root)
    agent_connector = ScriptedAgentConnector(
        {
            "stage3_pr_draft": {
                "what_changed": [
                    "Add cockpit health synthesis to the JSON output.",
                    "Surface GitHub check conclusions and local eval state together.",
                ],
                "key_risks": [
                    "Health synthesis must stay compatibility-safe for older callers."
                ],
                "changed_paths": [
                    "src/auto_mindsdb_factory/__main__.py",
                    "tests/test_cli.py",
                ],
            },
            "stage3_pr_review": {
                "blocking_findings": [],
                "non_blocking_findings": [
                    "Confirm that absent GitHub check data downgrades health instead of crashing the cockpit."
                ],
            },
        }
    )

    result = Stage3BuildReviewPipeline(root, agent_connector=agent_connector).process(
        stage2_result.spec_packet,
        stage2_result.policy_decision,
        stage2_result.ticket_bundle,
        stage2_result.eval_manifest,
        stage2_result.work_item,
        repository="ianu82/ai-factory",
    )

    assert result.pr_packet["artifact"]["model_fingerprint"] == "openai.responses:gpt-5.4"
    assert result.pr_packet["summary"]["what_changed"][0] == (
        "Add cockpit health synthesis to the JSON output."
    )
    assert result.pr_packet["summary"]["key_risks"] == [
        "Health synthesis must stay compatibility-safe for older callers."
    ]
    assert "src/auto_mindsdb_factory/__main__.py" in result.pr_packet["changed_paths"]
    assert (
        "Confirm that absent GitHub check data downgrades health instead of crashing the cockpit."
        in result.pr_packet["reviewer_report"]["non_blocking_findings"]
    )


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
