from __future__ import annotations

from pathlib import Path

from auto_mindsdb_factory.contracts import load_validators, validation_errors_for
from auto_mindsdb_factory.controller import ControllerState
from auto_mindsdb_factory.intake import AnthropicScout, ReleaseNoteItem, Stage1IntakePipeline


def fixture_html() -> str:
    root = Path(__file__).resolve().parents[1]
    fixture_path = root / "fixtures" / "intake" / "anthropic-release-notes-sample.html"
    return fixture_path.read_text(encoding="utf-8")


def test_anthropic_scout_parses_release_notes_fixture() -> None:
    scout = AnthropicScout()
    items = scout.list_items(
        html=fixture_html(),
        detected_at="2026-04-22T12:00:00Z",
    )

    assert len(items) == 3
    assert items[0].published_at == "2026-04-20"
    assert items[0].title == "Support response format tool mode for tool results"
    assert items[0].url.endswith("#april-20-2026")
    assert items[1].external_id != items[0].external_id
    assert items[2].published_at == "2026-04-16"


def test_anthropic_scout_external_ids_are_stable_when_bullets_reorder() -> None:
    scout = AnthropicScout()
    html_a = """
    <article>
      <h3><div id="april-20-2026">April 20, 2026</div></h3>
      <ul>
        <li>Alpha API feature.</li>
        <li>Beta API feature.</li>
      </ul>
    </article>
    """
    html_b = """
    <article>
      <h3><div id="april-20-2026">April 20, 2026</div></h3>
      <ul>
        <li>Beta API feature.</li>
        <li>Alpha API feature.</li>
      </ul>
    </article>
    """

    ids_a = {
        item.external_id
        for item in scout.list_items(html=html_a, detected_at="2026-04-22T12:00:00Z")
    }
    ids_b = {
        item.external_id
        for item in scout.list_items(html=html_b, detected_at="2026-04-22T12:00:00Z")
    }

    assert ids_a == ids_b


def test_stage1_intake_emits_valid_active_build_bundle() -> None:
    root = Path(__file__).resolve().parents[1]
    validators = load_validators(root)
    item = AnthropicScout().list_items(
        html=fixture_html(),
        detected_at="2026-04-22T12:00:00Z",
    )[0]

    result = Stage1IntakePipeline(root).process_item(item)

    assert validation_errors_for(validators["spec-packet"], result.spec_packet) == []
    assert validation_errors_for(validators["policy-decision"], result.policy_decision) == []
    assert validation_errors_for(validators["work-item"], result.work_item.to_document()) == []
    assert result.spec_packet["relevance"]["decision"] == "active_build_candidate"
    assert result.policy_decision["decision"] == "active_build_candidate"
    assert result.work_item.state is ControllerState.POLICY_ASSIGNED
    assert result.work_item.execution_lane == "guarded"


def test_stage1_intake_watchlist_path_stops_in_watchlisted_state() -> None:
    root = Path(__file__).resolve().parents[1]
    validators = load_validators(root)
    item = AnthropicScout().list_items(
        html=fixture_html(),
        detected_at="2026-04-22T12:00:00Z",
    )[1]

    result = Stage1IntakePipeline(root).process_item(item)

    assert validation_errors_for(validators["spec-packet"], result.spec_packet) == []
    assert validation_errors_for(validators["policy-decision"], result.policy_decision) == []
    assert validation_errors_for(validators["work-item"], result.work_item.to_document()) == []
    assert result.spec_packet["relevance"]["decision"] == "watchlist"
    assert result.policy_decision["decision"] == "watchlist"
    assert result.work_item.state is ControllerState.WATCHLISTED
    assert result.work_item.current_artifact_id == result.policy_decision["artifact"]["id"]


def test_stage1_intake_backlog_candidate_path_stops_at_policy_assignment() -> None:
    root = Path(__file__).resolve().parents[1]
    validators = load_validators(root)
    item = ReleaseNoteItem(
        provider="anthropic",
        kind="release_note",
        external_id="manual-backlog-001",
        title="Experimental API orchestration preview",
        url="https://example.com/release-note",
        detected_at="2026-04-22T12:00:00Z",
        published_at="2026-04-20",
        body="Experimental API orchestration preview for multi-repo workflows.",
        date_label="April 20, 2026",
        anchor="april-20-2026",
    )

    result = Stage1IntakePipeline(root).process_item(item)

    assert validation_errors_for(validators["spec-packet"], result.spec_packet) == []
    assert validation_errors_for(validators["policy-decision"], result.policy_decision) == []
    assert validation_errors_for(validators["work-item"], result.work_item.to_document()) == []
    assert result.spec_packet["relevance"]["decision"] == "backlog_candidate"
    assert result.policy_decision["decision"] == "backlog_candidate"
    assert result.work_item.state is ControllerState.POLICY_ASSIGNED
    assert result.work_item.execution_lane == "guarded"


def test_stage1_intake_ignore_path_ends_rejected() -> None:
    root = Path(__file__).resolve().parents[1]
    validators = load_validators(root)
    item = ReleaseNoteItem(
        provider="anthropic",
        kind="release_note",
        external_id="manual-ignore-001",
        title="Support article for billing portal changes",
        url="https://example.com/release-note",
        detected_at="2026-04-22T12:00:00Z",
        published_at="2026-04-20",
        body="Support article for billing portal changes and account settings.",
        date_label="April 20, 2026",
        anchor="april-20-2026",
    )

    result = Stage1IntakePipeline(root).process_item(item)

    assert validation_errors_for(validators["spec-packet"], result.spec_packet) == []
    assert validation_errors_for(validators["policy-decision"], result.policy_decision) == []
    assert validation_errors_for(validators["work-item"], result.work_item.to_document()) == []
    assert result.spec_packet["relevance"]["decision"] == "ignore"
    assert result.policy_decision["decision"] == "ignore"
    assert result.work_item.state is ControllerState.REJECTED
    assert result.work_item.current_artifact_id == result.policy_decision["artifact"]["id"]
