from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from auto_mindsdb_factory.intake import (
    AnthropicScout,
    ArtifactValidationError,
    Clarifier,
    ReleaseNoteItem,
    Stage1IntakePipeline,
    UpstreamFetchError,
    UpstreamShapeError,
)


def load_cases() -> list[dict]:
    root = Path(__file__).resolve().parents[1]
    fixture_path = root / "fixtures" / "intake" / "clarifier-decisions.yaml"
    with fixture_path.open("r", encoding="utf-8") as handle:
        document = yaml.safe_load(handle)
    return list(document["cases"])


def test_anthropic_scout_invalid_html_raises_shape_error() -> None:
    scout = AnthropicScout()
    with pytest.raises(UpstreamShapeError):
        scout.list_items(html="<html><body><main>No release note structure</main></body></html>")


def test_anthropic_scout_fetch_error_is_wrapped(monkeypatch: pytest.MonkeyPatch) -> None:
    def raise_fetch_error(*args, **kwargs):  # type: ignore[no-untyped-def]
        raise OSError("simulated network failure")

    monkeypatch.setattr("auto_mindsdb_factory.intake.urlopen", raise_fetch_error)
    scout = AnthropicScout()
    with pytest.raises(UpstreamFetchError):
        scout.fetch_release_notes_html()


def test_clarifier_decision_fixture_suite() -> None:
    root = Path(__file__).resolve().parents[1]
    clarifier = Clarifier(root)
    for case in load_cases():
        item = ReleaseNoteItem(
            provider="anthropic",
            kind="release_note",
            external_id=case["id"],
            title=case["item"]["title"],
            url="https://example.com/release-note",
            detected_at="2026-04-22T12:00:00Z",
            published_at="2026-04-20",
            body=case["item"]["body"],
            date_label="April 20, 2026",
            anchor="april-20-2026",
        )
        clarification = clarifier.clarify(item)
        expected = case["expected"]

        assert clarification.decision == expected["decision"]
        assert clarification.expected_roi == expected["expected_roi"]
        assert set(expected["required_flags"]).issubset(set(clarification.flags))
        assert set(expected["required_surfaces"]).issubset(set(clarification.affected_surfaces))


def test_stage1_pipeline_wraps_artifact_validation_failures() -> None:
    root = Path(__file__).resolve().parents[1]
    pipeline = Stage1IntakePipeline(root)
    invalid_item = ReleaseNoteItem(
        provider="anthropic",
        kind="release_note",
        external_id="invalid-stage1-item",
        title="",
        url="https://example.com/release-note",
        detected_at="2026-04-22T12:00:00Z",
        published_at="2026-04-20",
        body="Tool API feature",
        date_label="April 20, 2026",
        anchor="april-20-2026",
    )

    with pytest.raises(ArtifactValidationError):
        pipeline.process_item(invalid_item)
