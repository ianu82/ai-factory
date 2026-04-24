from __future__ import annotations

from pathlib import Path

import pytest

from auto_mindsdb_factory.connectors import AgentResult
from auto_mindsdb_factory.contracts import load_validators, validation_errors_for
from auto_mindsdb_factory.controller import ControllerState
from auto_mindsdb_factory.intake import (
    AnthropicScout,
    ReleaseNoteItem,
    Stage1IntakePipeline,
    build_manual_intake_item,
)
from auto_mindsdb_factory.ticketing import (
    Stage2TicketingPipeline,
    TicketingConsistencyError,
    TicketingEligibilityError,
)


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


def stage1_active_result(root: Path):
    item = AnthropicScout().list_items(
        html=fixture_html(),
        detected_at="2026-04-22T12:00:00Z",
    )[0]
    return Stage1IntakePipeline(root).process_item(item)


def stage1_backlog_result(root: Path):
    item = ReleaseNoteItem(
        provider="anthropic",
        kind="release_note",
        external_id="manual-backlog-stage2-001",
        title="Experimental API orchestration preview",
        url="https://example.com/release-note",
        detected_at="2026-04-22T12:00:00Z",
        published_at="2026-04-20",
        body="Experimental API orchestration preview for multi-repo workflows.",
        date_label="April 20, 2026",
        anchor="april-20-2026",
    )
    return Stage1IntakePipeline(root).process_item(item)


def stage1_manual_issue_result(root: Path):
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
    return Stage1IntakePipeline(root).process_item(item)


def test_stage2_ticketing_emits_valid_ticket_bundle_and_eval_manifest() -> None:
    root = Path(__file__).resolve().parents[1]
    validators = load_validators(root)
    stage1_result = stage1_active_result(root)

    result = Stage2TicketingPipeline(root).process(
        stage1_result.spec_packet,
        stage1_result.policy_decision,
        stage1_result.work_item,
    )

    assert validation_errors_for(validators["ticket-bundle"], result.ticket_bundle) == []
    assert validation_errors_for(validators["eval-manifest"], result.eval_manifest) == []
    assert validation_errors_for(validators["work-item"], result.work_item.to_document()) == []
    assert result.work_item.state is ControllerState.TICKETED
    assert result.work_item.current_artifact_id == result.ticket_bundle["artifact"]["id"]
    assert result.ticket_bundle["eval_manifest_id"] == result.eval_manifest["artifact"]["id"]
    assert result.eval_manifest["target_id"] == result.ticket_bundle["artifact"]["id"]
    assert len(result.ticket_bundle["tickets"]) == 2
    assert [ticket["kind"] for ticket in result.ticket_bundle["tickets"]] == [
        "backend",
        "llm_integration",
    ]
    assert result.ticket_bundle["tickets"][1]["dependencies"] == [
        result.ticket_bundle["tickets"][0]["id"]
    ]
    assert result.ticket_bundle["dependency_graph"] == [
        {
            "from": result.ticket_bundle["tickets"][0]["id"],
            "to": result.ticket_bundle["tickets"][1]["id"],
            "type": "blocks",
        }
    ]
    assert [tier["name"] for tier in result.eval_manifest["tiers"]] == stage1_result.policy_decision[
        "required_eval_tiers"
    ]
    check_ids = [
        check["id"]
        for tier in result.eval_manifest["tiers"]
        for check in tier["checks"]
    ]
    assert len(check_ids) == len(set(check_ids))
    assert all(ticket["execution_lane"] == "guarded" for ticket in result.ticket_bundle["tickets"])


def test_stage2_ticketing_rejects_backlog_candidates() -> None:
    root = Path(__file__).resolve().parents[1]
    stage1_result = stage1_backlog_result(root)

    with pytest.raises(TicketingEligibilityError):
        Stage2TicketingPipeline(root).process(
            stage1_result.spec_packet,
            stage1_result.policy_decision,
            stage1_result.work_item,
        )


def test_stage2_ticketing_shapes_manual_issue_into_contract_and_control_plane_work() -> None:
    root = Path(__file__).resolve().parents[1]
    stage1_result = stage1_manual_issue_result(root)

    result = Stage2TicketingPipeline(root).process(
        stage1_result.spec_packet,
        stage1_result.policy_decision,
        stage1_result.work_item,
    )

    assert [ticket["kind"] for ticket in result.ticket_bundle["tickets"]] == [
        "backend",
        "frontend",
    ]
    assert [ticket["title"] for ticket in result.ticket_bundle["tickets"]] == [
        "Implement Factory cockpit should surface GitHub check conclusions and eval status: contract compatibility",
        "Implement Factory cockpit should surface GitHub check conclusions and eval status: operator control-plane updates",
    ]
    assert result.ticket_bundle["tickets"][1]["dependencies"] == [
        result.ticket_bundle["tickets"][0]["id"]
    ]


def test_stage2_ticketing_can_use_agent_drafts_for_ticket_content() -> None:
    root = Path(__file__).resolve().parents[1]
    stage1_result = stage1_manual_issue_result(root)
    agent_connector = ScriptedAgentConnector(
        {
            "stage2_ticket_drafting": {
                "tickets": [
                    {
                        "slug": "contract",
                        "summary": "Update the cockpit schema and compatibility layer.",
                        "scope": [
                            "Expose the health field in the cockpit JSON contract.",
                            "Keep older callers compatible until they adopt the new field.",
                        ],
                        "definition_of_done": [
                            "Contract consumers can parse the new health field safely."
                        ],
                        "known_edge_cases": [
                            "Older callers should still parse the payload without crashing."
                        ],
                    },
                    {
                        "slug": "control-plane",
                        "summary": "Update operator-facing cockpit rendering for the new health signal.",
                        "scope": [
                            "Show GitHub checks, eval state, and one consolidated health status.",
                        ],
                        "definition_of_done": [
                            "Operators can judge run health from one cockpit view."
                        ],
                        "known_edge_cases": [
                            "Missing GitHub check data should degrade to a warning state."
                        ],
                    },
                ]
            }
        }
    )

    result = Stage2TicketingPipeline(root, agent_connector=agent_connector).process(
        stage1_result.spec_packet,
        stage1_result.policy_decision,
        stage1_result.work_item,
    )

    assert result.ticket_bundle["artifact"]["model_fingerprint"] == "openai.responses:gpt-5.4"
    assert (
        result.ticket_bundle["tickets"][0]["summary"]
        == "Update the cockpit schema and compatibility layer."
    )
    assert result.ticket_bundle["tickets"][0]["scope"][0] == (
        "Expose the health field in the cockpit JSON contract."
    )
    assert result.ticket_bundle["tickets"][1]["known_edge_cases"] == [
        "Missing GitHub check data should degrade to a warning state."
    ]


def test_stage2_ticketing_rejects_mismatched_work_item() -> None:
    root = Path(__file__).resolve().parents[1]
    html = fixture_html()
    items = AnthropicScout().list_items(
        html=html,
        detected_at="2026-04-22T12:00:00Z",
    )
    stage1_active = Stage1IntakePipeline(root).process_item(items[0])
    stage1_watchlist = Stage1IntakePipeline(root).process_item(items[1])
    mismatched_work_item = stage1_watchlist.work_item
    mismatched_work_item.state = stage1_active.work_item.state
    mismatched_work_item.policy_decision_id = stage1_active.work_item.policy_decision_id
    mismatched_work_item.execution_lane = stage1_active.work_item.execution_lane
    mismatched_work_item.current_artifact_id = stage1_active.work_item.current_artifact_id

    with pytest.raises(TicketingConsistencyError):
        Stage2TicketingPipeline(root).process(
            stage1_active.spec_packet,
            stage1_active.policy_decision,
            mismatched_work_item,
        )
