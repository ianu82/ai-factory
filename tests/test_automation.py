from __future__ import annotations

import json
import time
from pathlib import Path

import pytest

from auto_mindsdb_factory.automation import (
    ArtifactStoreError,
    AutomationState,
    AutomationStateConflictError,
    FactoryAutomationCoordinator,
    FactoryRunStore,
    ImmediateHandoffError,
    ImmediateHandoffResult,
    RunLeaseBusyError,
    StateLeaseBusyError,
)
from auto_mindsdb_factory.build_review import Stage3BuildReviewPipeline
from auto_mindsdb_factory.contracts import load_validators, validation_errors_for
from auto_mindsdb_factory.controller import FactoryController
from auto_mindsdb_factory.intake import Stage1IntakePipeline, build_manual_intake_item
from auto_mindsdb_factory.ticketing import Stage2TicketingPipeline


@pytest.fixture(autouse=True)
def _disable_linear_workflow_sync(monkeypatch) -> None:
    monkeypatch.setenv("LINEAR_FACTORY_SYNC_DISABLED", "1")
    monkeypatch.delenv("LINEAR_API_KEY", raising=False)
    monkeypatch.delenv("LINEAR_TARGET_TEAM_ID", raising=False)
    monkeypatch.delenv("LINEAR_TARGET_STATE_ID", raising=False)


def _load_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _history_document(work_item) -> list[dict[str, str | None]]:
    return [
        {
            "event": record.event,
            "from_state": record.from_state,
            "to_state": record.to_state,
            "artifact_id": record.artifact_id,
            "occurred_at": record.occurred_at,
        }
        for record in work_item.history
    ]


def load_stage8_result_document(root: Path, scenario_name: str) -> dict:
    scenario = root / "fixtures" / "scenarios" / scenario_name
    replayed = FactoryController().replay_scenario(scenario)
    return {
        "spec_packet": _load_json(scenario / "spec-packet.json"),
        "policy_decision": _load_json(scenario / "policy-decision.json"),
        "ticket_bundle": _load_json(scenario / "ticket-bundle.json"),
        "eval_manifest": _load_json(scenario / "eval-manifest.json"),
        "pr_packet": _load_json(scenario / "pr-packet.json"),
        "prompt_contract": _load_json(scenario / "prompt-contract.json"),
        "tool_schema": _load_json(scenario / "tool-schema.json"),
        "golden_dataset": _load_json(scenario / "golden-dataset.json"),
        "latency_baseline": _load_json(scenario / "latency-baseline.json"),
        "eval_report": _load_json(scenario / "eval-report.json"),
        "security_review": _load_json(scenario / "security-review.json"),
        "promotion_decision": _load_json(scenario / "promotion-decision.json"),
        "monitoring_report": _load_json(scenario / "monitoring-report.json"),
        "work_item": replayed.to_document(),
        "history": _history_document(replayed),
    }


def load_stage4_result_document(root: Path, scenario_name: str) -> dict:
    scenario = root / "fixtures" / "scenarios" / scenario_name
    return {
        "spec_packet": _load_json(scenario / "spec-packet.json"),
        "policy_decision": _load_json(scenario / "policy-decision.json"),
        "ticket_bundle": _load_json(scenario / "ticket-bundle.json"),
        "eval_manifest": _load_json(scenario / "eval-manifest.json"),
        "pr_packet": _load_json(scenario / "pr-packet.json"),
        "prompt_contract": _load_json(scenario / "prompt-contract.json"),
        "tool_schema": _load_json(scenario / "tool-schema.json"),
        "golden_dataset": _load_json(scenario / "golden-dataset.json"),
        "latency_baseline": _load_json(scenario / "latency-baseline.json"),
        "work_item": _load_json(scenario / "work-item.json"),
    }


class FakeLinearWorkflowSync:
    def __init__(self) -> None:
        self.calls: list[dict[str, object]] = []

    def sync_stage_result(
        self,
        stage_name: str,
        document: dict,
        *,
        stall_reason: str | None = None,
    ) -> dict[str, object]:
        self.calls.append(
            {
                "stage_name": stage_name,
                "work_item_id": document["work_item"]["work_item_id"],
                "stall_reason": stall_reason,
            }
        )
        return {"status": "synced"}


def test_automation_stage1_cycle_persists_new_items(tmp_path) -> None:
    root = Path(__file__).resolve().parents[1]
    html = (root / "fixtures" / "intake" / "anthropic-release-notes-sample.html").read_text(
        encoding="utf-8"
    )
    coordinator = FactoryAutomationCoordinator(
        tmp_path / "automation-store",
        repo_root_override=root,
    )
    validators = load_validators(root)

    first_result = coordinator.run_stage1_cycle(html=html)

    assert first_result.detected_count >= 1
    assert len(first_result.created_results) == first_result.detected_count
    assert validation_errors_for(
        validators["automation-state"],
        first_result.state.to_document(),
    ) == []
    for created in first_result.created_results:
        assert Path(created["stored_path"]).exists()

    second_result = coordinator.run_stage1_cycle(html=html)

    assert second_result.created_results == []
    assert len(second_result.skipped_known_external_ids) == first_result.detected_count


def test_automation_stage1_cycle_can_advance_immediately(tmp_path) -> None:
    root = Path(__file__).resolve().parents[1]
    html = (root / "fixtures" / "intake" / "anthropic-release-notes-sample.html").read_text(
        encoding="utf-8"
    )
    store_dir = tmp_path / "automation-store"
    coordinator = FactoryAutomationCoordinator(
        store_dir,
        repo_root_override=root,
    )

    result = coordinator.run_stage1_cycle(
        html=html,
        max_new_items=1,
        advance_immediately=True,
    )

    assert result.advance_immediately is True
    assert len(result.handoff_results) == 1
    handoff = result.handoff_results[0]
    assert handoff["status"] == "progressed"
    assert handoff["source_stage"] == "stage1"
    assert handoff["final_stage"] == "stage8"
    assert handoff["final_state"] == "PRODUCTION_MONITORING"
    assert Path(handoff["stored_paths"]["stage8"]).exists()


def test_automation_register_bundle_syncs_linear_stage_result(tmp_path) -> None:
    root = Path(__file__).resolve().parents[1]
    item = build_manual_intake_item(
        provider="github",
        external_id="github-issue-2",
        title="Factory cockpit should surface GitHub check conclusions and eval status",
        url="https://github.com/ianu82/ai-factory/issues/2",
        detected_at="2026-04-24T12:00:00Z",
        published_at="2026-04-24T11:30:00Z",
        body=(
            "The operator cockpit should surface GitHub pull request check conclusions, local eval "
            "status, and a clear health summary for each work item. Acceptance criteria: - keep the "
            "output compact - add a clear health field - cover it with tests"
        ),
    )
    stage1_result = Stage1IntakePipeline(root).process_item(item)
    fake_sync = FakeLinearWorkflowSync()
    coordinator = FactoryAutomationCoordinator(
        tmp_path / "automation-store",
        repo_root_override=root,
        linear_workflow_sync=fake_sync,
    )

    coordinator.register_bundle("stage1", stage1_result.to_document())

    assert fake_sync.calls == [
        {
            "stage_name": "stage1",
            "work_item_id": stage1_result.work_item.work_item_id,
            "stall_reason": None,
        }
    ]


def test_automation_progression_skip_syncs_linear_stall_reason(tmp_path) -> None:
    root = Path(__file__).resolve().parents[1]
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
            "JSON schema change for the cockpit command, not a model-runtime change. Acceptance criteria: "
            "- include check conclusions - keep the schema compatibility-safe - cover it with tests"
        ),
    )
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
    fake_sync = FakeLinearWorkflowSync()
    coordinator = FactoryAutomationCoordinator(
        tmp_path / "automation-store",
        repo_root_override=root,
        linear_workflow_sync=fake_sync,
    )
    coordinator.register_bundle("stage3", stage3_result.to_document())

    result = coordinator.run_progression_cycle()

    assert result.processed_runs == []
    assert result.skipped_runs == [
        {
            "work_item_id": stage3_result.work_item.work_item_id,
            "stage_name": "stage3",
            "reason": "non_model_touching_progression_not_supported",
        }
    ]
    assert fake_sync.calls[-1] == {
        "stage_name": "stage3",
        "work_item_id": stage3_result.work_item.work_item_id,
        "stall_reason": "non_model_touching_progression_not_supported",
    }


def test_automation_progression_cycle_advances_active_build_run_to_stage8(tmp_path) -> None:
    root = Path(__file__).resolve().parents[1]
    html = (root / "fixtures" / "intake" / "anthropic-release-notes-sample.html").read_text(
        encoding="utf-8"
    )
    store_dir = tmp_path / "automation-store"
    coordinator = FactoryAutomationCoordinator(
        store_dir,
        repo_root_override=root,
    )

    coordinator.run_stage1_cycle(html=html, max_new_items=1)
    result = coordinator.run_progression_cycle()

    assert len(result.processed_runs) == 1
    processed = result.processed_runs[0]
    assert processed.stages_completed == [
        "stage2",
        "stage3",
        "stage4",
        "stage5",
        "stage6",
        "merge",
        "stage7",
        "stage8",
    ]
    stage8_document = _load_json(Path(processed.stored_paths["stage8"]))
    assert stage8_document["work_item"]["state"] == "PRODUCTION_MONITORING"
    assert stage8_document["monitoring_report"]["monitoring_decision"]["status"] == "healthy"


def test_automation_supervisor_cycle_runs_stage1_progression_and_weekly_feedback(tmp_path) -> None:
    root = Path(__file__).resolve().parents[1]
    html = (root / "fixtures" / "intake" / "anthropic-release-notes-sample.html").read_text(
        encoding="utf-8"
    )
    store_dir = tmp_path / "automation-store"
    coordinator = FactoryAutomationCoordinator(
        store_dir,
        repo_root_override=root,
    )

    result = coordinator.run_supervisor_cycle(
        html=html,
        max_new_items=1,
        advance_immediately=False,
        run_weekly_feedback=True,
        window_label="2026-W17",
    )

    assert result.stage1_result.created_results
    assert len(result.progression_result.processed_runs) == 1
    assert result.progression_result.processed_runs[0].final_stage == "stage8"
    assert result.weekly_feedback_result is not None
    assert len(result.weekly_feedback_result.processed_results) == 1
    assert result.post_progression_handoff_results == []


def test_automation_supervisor_cycle_runs_post_progression_handoff_when_weekly_feedback_disabled(
    tmp_path,
    monkeypatch,
) -> None:
    root = Path(__file__).resolve().parents[1]
    html = (root / "fixtures" / "intake" / "anthropic-release-notes-sample.html").read_text(
        encoding="utf-8"
    )
    store_dir = tmp_path / "automation-store"
    coordinator = FactoryAutomationCoordinator(
        store_dir,
        repo_root_override=root,
    )
    handoff_calls: list[str] = []
    original_run_immediate_handoff = FactoryAutomationCoordinator.run_immediate_handoff

    def _wrapped_run_immediate_handoff(self, work_item_id: str, **kwargs):
        handoff_calls.append(work_item_id)
        return original_run_immediate_handoff(self, work_item_id, **kwargs)

    monkeypatch.setattr(
        FactoryAutomationCoordinator,
        "run_immediate_handoff",
        _wrapped_run_immediate_handoff,
    )

    result = coordinator.run_supervisor_cycle(
        html=html,
        max_new_items=1,
        advance_immediately=False,
        run_weekly_feedback=False,
    )

    assert len(result.progression_result.processed_runs) == 1
    assert handoff_calls == [result.progression_result.processed_runs[0].work_item_id]
    assert len(result.post_progression_handoff_results) == 1
    assert result.post_progression_handoff_results[0]["source_stage"] == "stage8"


def test_automation_progression_cycle_skips_non_actionable_runs(tmp_path) -> None:
    root = Path(__file__).resolve().parents[1]
    html = (root / "fixtures" / "intake" / "anthropic-release-notes-sample.html").read_text(
        encoding="utf-8"
    )
    store_dir = tmp_path / "automation-store"
    coordinator = FactoryAutomationCoordinator(
        store_dir,
        repo_root_override=root,
    )

    stage1_result = coordinator.run_stage1_cycle(html=html, max_new_items=2)
    result = coordinator.run_progression_cycle()
    watchlisted = next(
        created
        for created in stage1_result.created_results
        if created["state"] == "WATCHLISTED"
    )

    assert len(result.processed_runs) == 1
    assert result.skipped_runs == [
        {
            "work_item_id": watchlisted["work_item_id"],
            "stage_name": "stage1",
            "reason": "non_actionable_state",
        }
    ]


def test_immediate_handoff_skips_watchlisted_stage1_item(tmp_path) -> None:
    root = Path(__file__).resolve().parents[1]
    html = (root / "fixtures" / "intake" / "anthropic-release-notes-sample.html").read_text(
        encoding="utf-8"
    )
    coordinator = FactoryAutomationCoordinator(
        tmp_path / "automation-store",
        repo_root_override=root,
    )

    result = coordinator.run_stage1_cycle(
        html=html,
        max_new_items=2,
        advance_immediately=True,
    )
    watchlisted = next(
        handoff for handoff in result.handoff_results if handoff["status"] == "skipped"
    )

    assert watchlisted["source_stage"] == "stage1"
    assert watchlisted["source_state"] == "WATCHLISTED"
    assert watchlisted["reason"] == "non_actionable_state"


def test_immediate_handoff_raises_for_failed_library_call(tmp_path) -> None:
    root = Path(__file__).resolve().parents[1]
    store_dir = tmp_path / "automation-store"
    store = FactoryRunStore(store_dir, repo_root_override=root)
    stage4_document = load_stage4_result_document(root, "stage4_reviewable_feature")
    stage4_document["work_item"]["state"] = "PR_MERGEABLE"
    store.save_stage_result("stage4", stage4_document)
    coordinator = FactoryAutomationCoordinator(store_dir, repo_root_override=root)

    with pytest.raises(ImmediateHandoffError) as exc_info:
        coordinator.run_immediate_handoff(stage4_document["work_item"]["work_item_id"])

    assert exc_info.value.result is not None
    assert "missing required object fields: eval_report" in str(exc_info.value)


def test_automation_stage1_cycle_raises_after_failed_immediate_handoff(tmp_path, monkeypatch) -> None:
    root = Path(__file__).resolve().parents[1]
    html = (root / "fixtures" / "intake" / "anthropic-release-notes-sample.html").read_text(
        encoding="utf-8"
    )
    coordinator = FactoryAutomationCoordinator(
        tmp_path / "automation-store",
        repo_root_override=root,
    )

    def _fake_handoff(self, work_item_id: str, **kwargs) -> ImmediateHandoffResult:
        return ImmediateHandoffResult(
            work_item_id=work_item_id,
            source_stage="stage1",
            source_state="POLICY_ASSIGNED",
            status="failed",
            reason="synthetic handoff failure",
        )

    monkeypatch.setattr(
        FactoryAutomationCoordinator,
        "run_immediate_handoff",
        _fake_handoff,
    )

    with pytest.raises(ImmediateHandoffError) as exc_info:
        coordinator.run_stage1_cycle(
            html=html,
            max_new_items=1,
            advance_immediately=True,
        )

    assert exc_info.value.cycle_result is not None
    cycle_result = exc_info.value.cycle_result
    assert cycle_result is not None
    assert cycle_result.failed_handoffs()[0]["reason"] == "synthetic handoff failure"
    assert Path(cycle_result.created_results[0]["stored_path"]).exists()


def test_immediate_handoff_skips_locked_run(tmp_path) -> None:
    root = Path(__file__).resolve().parents[1]
    html = (root / "fixtures" / "intake" / "anthropic-release-notes-sample.html").read_text(
        encoding="utf-8"
    )
    coordinator = FactoryAutomationCoordinator(
        tmp_path / "automation-store",
        repo_root_override=root,
    )
    cycle_result = coordinator.run_stage1_cycle(html=html, max_new_items=1)
    work_item_id = cycle_result.created_results[0]["work_item_id"]

    with coordinator.store.run_lease(work_item_id):
        handoff = coordinator.run_immediate_handoff(
            work_item_id,
            raise_on_failure=False,
        )

    assert handoff.status == "skipped"
    assert handoff.reason == "run_locked"


def test_run_lease_heartbeat_keeps_lock_alive_past_ttl(tmp_path) -> None:
    root = Path(__file__).resolve().parents[1]
    store = FactoryRunStore(tmp_path / "automation-store", repo_root_override=root)

    with store.run_lease(
        "wi-heartbeat-001",
        ttl_seconds=0.2,
        renew_interval_seconds=0.05,
    ):
        time.sleep(0.35)
        with pytest.raises(RunLeaseBusyError):
            with store.run_lease("wi-heartbeat-001", ttl_seconds=0.2, renew_interval_seconds=0.05):
                pass


def test_state_transaction_prevents_concurrent_updates(tmp_path) -> None:
    root = Path(__file__).resolve().parents[1]
    store = FactoryRunStore(tmp_path / "automation-store", repo_root_override=root)
    store.save_state(AutomationState(updated_at="2026-04-23T00:00:00Z"))

    with store.state_transaction(ttl_seconds=0.2, renew_interval_seconds=0.05) as state:
        state.last_stage1_cycle_at = "2026-04-23T00:05:00Z"
        with pytest.raises(StateLeaseBusyError):
            with store.state_transaction(ttl_seconds=0.2, renew_interval_seconds=0.05):
                pass


def test_save_state_rejects_stale_compare_and_swap_update(tmp_path) -> None:
    root = Path(__file__).resolve().parents[1]
    store = FactoryRunStore(tmp_path / "automation-store", repo_root_override=root)
    baseline = AutomationState(updated_at="2026-04-23T00:00:00Z")
    store.save_state(baseline)

    fresh = store.load_state()
    fresh.last_stage1_cycle_at = "2026-04-23T00:05:00Z"
    fresh.updated_at = "2026-04-23T00:05:00Z"
    store.save_state(
        fresh,
        expected_previous_updated_at="2026-04-23T00:00:00Z",
    )

    stale = store.load_state()
    stale.last_stage9_cycle_at = "2026-04-23T00:10:00Z"
    stale.updated_at = "2026-04-23T00:10:00Z"

    with pytest.raises(AutomationStateConflictError):
        store.save_state(
            stale,
            expected_previous_updated_at="2026-04-23T00:00:00Z",
        )


def test_automation_progression_cycle_skips_corrupted_run_and_keeps_processing(tmp_path) -> None:
    root = Path(__file__).resolve().parents[1]
    html = (root / "fixtures" / "intake" / "anthropic-release-notes-sample.html").read_text(
        encoding="utf-8"
    )
    store_dir = tmp_path / "automation-store"
    coordinator = FactoryAutomationCoordinator(
        store_dir,
        repo_root_override=root,
    )

    coordinator.run_stage1_cycle(html=html, max_new_items=1)
    corrupted_run_dir = store_dir / "runs" / "wi-corrupted-run"
    corrupted_run_dir.mkdir(parents=True, exist_ok=True)
    (corrupted_run_dir / "stage3-result.json").write_text("{not valid json", encoding="utf-8")

    result = coordinator.run_progression_cycle()

    assert len(result.processed_runs) == 1
    assert result.skipped_runs == [
        {
            "work_item_id": "wi-corrupted-run",
            "stage_name": "store_scan",
            "reason": (
                "Could not read stored stage3 result at "
                f"{corrupted_run_dir / 'stage3-result.json'}: "
                "Expecting property name enclosed in double quotes: line 1 column 2 (char 1)"
            ),
        }
    ]


def test_automation_progression_cycle_rejects_state_stage_mismatch(tmp_path) -> None:
    root = Path(__file__).resolve().parents[1]
    store_dir = tmp_path / "automation-store"
    store = FactoryRunStore(store_dir, repo_root_override=root)
    stage4_document = load_stage4_result_document(root, "stage4_reviewable_feature")
    stage4_document["work_item"]["state"] = "PR_MERGEABLE"
    store.save_stage_result("stage4", stage4_document)

    coordinator = FactoryAutomationCoordinator(store_dir, repo_root_override=root)
    result = coordinator.run_progression_cycle()

    assert result.processed_runs == []
    assert result.skipped_runs == [
        {
            "work_item_id": stage4_document["work_item"]["work_item_id"],
            "stage_name": "stage4",
            "reason": (
                "Stored stage result for PR_MERGEABLE is missing required object fields: "
                "eval_report."
            ),
        }
    ]


def test_automation_progression_cycle_rejects_run_directory_identity_mismatch(tmp_path) -> None:
    root = Path(__file__).resolve().parents[1]
    store_dir = tmp_path / "automation-store"
    run_dir = store_dir / "runs" / "wi-directory-001"
    run_dir.mkdir(parents=True, exist_ok=True)
    stage4_document = load_stage4_result_document(root, "stage4_reviewable_feature")
    stage4_document["work_item"]["work_item_id"] = "wi-payload-001"
    (run_dir / "stage4-result.json").write_text(
        json.dumps(stage4_document),
        encoding="utf-8",
    )

    coordinator = FactoryAutomationCoordinator(store_dir, repo_root_override=root)
    result = coordinator.run_progression_cycle()

    assert result.processed_runs == []
    assert result.skipped_runs == [
        {
            "work_item_id": "wi-directory-001",
            "stage_name": "store_scan",
            "reason": (
                "Stored stage result work_item.work_item_id 'wi-payload-001' "
                "does not match run directory 'wi-directory-001'."
            ),
        }
    ]


def test_automation_progression_cycle_skips_locked_run(tmp_path) -> None:
    root = Path(__file__).resolve().parents[1]
    html = (root / "fixtures" / "intake" / "anthropic-release-notes-sample.html").read_text(
        encoding="utf-8"
    )
    store_dir = tmp_path / "automation-store"
    coordinator = FactoryAutomationCoordinator(
        store_dir,
        repo_root_override=root,
    )
    stage1_result = coordinator.run_stage1_cycle(html=html, max_new_items=1)
    work_item_id = stage1_result.created_results[0]["work_item_id"]

    with coordinator.store.run_lease(work_item_id):
        result = coordinator.run_progression_cycle()

    assert result.processed_runs == []
    assert result.skipped_runs == [
        {
            "work_item_id": work_item_id,
            "stage_name": "store_scan",
            "reason": "run_locked",
        }
    ]


def test_automation_weekly_feedback_cycle_processes_stage8_candidates(tmp_path) -> None:
    root = Path(__file__).resolve().parents[1]
    store_dir = tmp_path / "automation-store"
    store = FactoryRunStore(store_dir, repo_root_override=root)
    validators = load_validators(root)
    stage8_document = load_stage8_result_document(root, "stage8_healthy_feature")

    stored_path = store.save_stage_result("stage8", stage8_document)
    store.save_state(store.load_state())
    coordinator = FactoryAutomationCoordinator(store_dir, repo_root_override=root)

    result = coordinator.run_weekly_feedback_cycle(window_label="2026-W17")

    assert stored_path.exists()
    assert len(result.processed_results) == 1
    stage9_path = Path(result.processed_results[0]["stored_path"])
    stage9_document = _load_json(stage9_path)
    assert validation_errors_for(
        validators["feedback-report"],
        stage9_document["feedback_report"],
    ) == []
    assert result.state.weekly_feedback_windows[
        result.processed_results[0]["work_item_id"]
    ] == "2026-W17"


def test_immediate_handoff_advances_stage8_incident_into_stage9(tmp_path) -> None:
    root = Path(__file__).resolve().parents[1]
    store_dir = tmp_path / "automation-store"
    store = FactoryRunStore(store_dir, repo_root_override=root)
    stage8_document = load_stage8_result_document(root, "stage8_auto_mitigated_feature")
    store.save_stage_result("stage8", stage8_document)
    coordinator = FactoryAutomationCoordinator(store_dir, repo_root_override=root)

    result = coordinator.run_immediate_handoff(stage8_document["work_item"]["work_item_id"])

    assert result.status == "progressed"
    assert result.source_stage == "stage8"
    assert result.final_stage == "stage9"
    stage9_document = _load_json(Path(result.stored_paths["stage9"]))
    assert stage9_document["feedback_report"]["feedback_window"]["trigger"] == "incident_signal"
    state = store.load_state()
    store.apply_stage_result_to_state(state, "stage9", stage9_document)
    assert state.weekly_feedback_windows == {}


def test_automation_weekly_feedback_cycle_dedupes_incident_follow_up_within_window(tmp_path) -> None:
    root = Path(__file__).resolve().parents[1]
    store_dir = tmp_path / "automation-store"
    store = FactoryRunStore(store_dir, repo_root_override=root)
    stage8_document = load_stage8_result_document(root, "stage8_auto_mitigated_feature")
    store.save_stage_result("stage8", stage8_document)
    store.save_state(store.load_state())
    coordinator = FactoryAutomationCoordinator(store_dir, repo_root_override=root)

    first_result = coordinator.run_weekly_feedback_cycle(window_label="2026-W17")
    second_result = coordinator.run_weekly_feedback_cycle(window_label="2026-W17")

    assert len(first_result.processed_results) == 1
    assert second_result.processed_results == []
    assert second_result.skipped_runs == [
        {
            "work_item_id": first_result.processed_results[0]["work_item_id"],
            "reason": "already_synthesized_for_window",
            "stage_name": "stage9",
        }
    ]


def test_immediate_handoff_advances_followup_incident_into_stage9(tmp_path) -> None:
    root = Path(__file__).resolve().parents[1]
    store_dir = tmp_path / "automation-store"
    store = FactoryRunStore(store_dir, repo_root_override=root)
    stage8_document = load_stage8_result_document(
        root,
        "stage8_followup_healthy_after_incident_feature",
    )
    store.save_stage_result("stage8", stage8_document)
    coordinator = FactoryAutomationCoordinator(store_dir, repo_root_override=root)

    result = coordinator.run_immediate_handoff(stage8_document["work_item"]["work_item_id"])

    assert result.status == "progressed"
    assert result.final_stage == "stage9"
    stage9_document = _load_json(Path(result.stored_paths["stage9"]))
    assert stage9_document["feedback_report"]["feedback_window"]["mode"] == "incident_follow_up"


def test_immediate_handoff_skips_healthy_stage8_without_feedback_signal(tmp_path) -> None:
    root = Path(__file__).resolve().parents[1]
    store_dir = tmp_path / "automation-store"
    store = FactoryRunStore(store_dir, repo_root_override=root)
    stage8_document = load_stage8_result_document(root, "stage8_healthy_feature")
    store.save_stage_result("stage8", stage8_document)
    coordinator = FactoryAutomationCoordinator(store_dir, repo_root_override=root)

    result = coordinator.run_immediate_handoff(
        stage8_document["work_item"]["work_item_id"],
        raise_on_failure=False,
    )

    assert result.status == "skipped"
    assert result.reason == "no_immediate_feedback_required"


def test_automation_weekly_feedback_cycle_uses_latest_stage9_bundle_across_weeks(tmp_path) -> None:
    root = Path(__file__).resolve().parents[1]
    store_dir = tmp_path / "automation-store"
    store = FactoryRunStore(store_dir, repo_root_override=root)
    stage8_document = load_stage8_result_document(root, "stage8_healthy_feature")
    store.save_stage_result("stage8", stage8_document)
    store.save_state(store.load_state())
    coordinator = FactoryAutomationCoordinator(store_dir, repo_root_override=root)

    first_result = coordinator.run_weekly_feedback_cycle(window_label="2026-W17")
    second_result = coordinator.run_weekly_feedback_cycle(window_label="2026-W18")
    third_result = coordinator.run_weekly_feedback_cycle(window_label="2026-W18")

    assert len(first_result.processed_results) == 1
    assert len(second_result.processed_results) == 1
    assert (
        first_result.processed_results[0]["feedback_report_id"]
        != second_result.processed_results[0]["feedback_report_id"]
    )
    assert third_result.processed_results == []
    assert third_result.skipped_runs == [
        {
            "work_item_id": second_result.processed_results[0]["work_item_id"],
            "reason": "already_synthesized_for_window",
            "stage_name": "stage9",
        }
    ]


def test_automation_weekly_feedback_cycle_prefers_newer_stage8_result_over_older_stage9_result(
    tmp_path,
) -> None:
    root = Path(__file__).resolve().parents[1]
    store_dir = tmp_path / "automation-store"
    store = FactoryRunStore(store_dir, repo_root_override=root)
    stage8_document = load_stage8_result_document(root, "stage8_healthy_feature")
    store.save_stage_result("stage8", stage8_document)
    store.save_state(store.load_state())
    coordinator = FactoryAutomationCoordinator(store_dir, repo_root_override=root)

    first_result = coordinator.run_weekly_feedback_cycle(window_label="2026-W17")
    assert first_result.processed_results[0]["source_stage"] == "stage8"

    stage8_document["work_item"]["updated_at"] = "2026-04-30T00:00:00Z"
    stage8_document["monitoring_report"]["artifact"]["updated_at"] = "2026-04-30T00:00:00Z"
    store.save_stage_result("stage8", stage8_document)

    second_result = coordinator.run_weekly_feedback_cycle(window_label="2026-W18")

    assert second_result.processed_results[0]["source_stage"] == "stage8"


def test_automation_weekly_feedback_cycle_skips_corrupted_run_and_keeps_processing(tmp_path) -> None:
    root = Path(__file__).resolve().parents[1]
    store_dir = tmp_path / "automation-store"
    store = FactoryRunStore(store_dir, repo_root_override=root)
    stage8_document = load_stage8_result_document(root, "stage8_healthy_feature")
    store.save_stage_result("stage8", stage8_document)
    corrupted_run_dir = store_dir / "runs" / "wi-corrupted-run"
    corrupted_run_dir.mkdir(parents=True, exist_ok=True)
    (corrupted_run_dir / "stage8-result.json").write_text("{not valid json", encoding="utf-8")

    coordinator = FactoryAutomationCoordinator(store_dir, repo_root_override=root)
    result = coordinator.run_weekly_feedback_cycle(window_label="2026-W17")

    assert len(result.processed_results) == 1
    assert result.skipped_runs == [
        {
            "work_item_id": "wi-corrupted-run",
            "stage_name": "store_scan",
            "reason": (
                "Could not read stored stage8 result at "
                f"{corrupted_run_dir / 'stage8-result.json'}: "
                "Expecting property name enclosed in double quotes: line 1 column 2 (char 1)"
            ),
        }
    ]


def test_automation_weekly_feedback_cycle_skips_locked_run(tmp_path) -> None:
    root = Path(__file__).resolve().parents[1]
    store_dir = tmp_path / "automation-store"
    store = FactoryRunStore(store_dir, repo_root_override=root)
    stage8_document = load_stage8_result_document(root, "stage8_healthy_feature")
    store.save_stage_result("stage8", stage8_document)
    coordinator = FactoryAutomationCoordinator(store_dir, repo_root_override=root)
    work_item_id = stage8_document["work_item"]["work_item_id"]

    with coordinator.store.run_lease(work_item_id):
        result = coordinator.run_weekly_feedback_cycle(window_label="2026-W17")

    assert result.processed_results == []
    assert result.skipped_runs == [
        {
            "work_item_id": work_item_id,
            "reason": "run_locked",
            "stage_name": "store_scan",
        }
    ]


def test_automation_store_rejects_malformed_stage8_bundle(tmp_path) -> None:
    root = Path(__file__).resolve().parents[1]
    store = FactoryRunStore(tmp_path / "automation-store", repo_root_override=root)

    with pytest.raises(ArtifactStoreError):
        store.save_stage_result(
            "stage8",
            {
                "work_item": {
                    "work_item_id": "wi-test-001",
                    "updated_at": "2026-04-22T12:30:00Z",
                }
            },
        )
