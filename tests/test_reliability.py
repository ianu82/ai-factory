from __future__ import annotations

import json
from pathlib import Path

import pytest

from auto_mindsdb_factory.reliability import (
    OperationReaper,
    OperationTracker,
    RecoveryManager,
    ReliabilityError,
    classify_queue,
    operation_summary,
    operation_path,
    recovery_state_path,
    write_json_atomic,
)


def _read(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def test_operation_tracker_writes_active_heartbeat_and_completion(tmp_path) -> None:
    with OperationTracker(
        tmp_path,
        work_item_id="wi-1",
        stage="stage3",
        operation="stage3_code_worker",
        worker_id="worker-a",
        heartbeat_interval_seconds=60,
        stale_seconds=120,
    ) as tracker:
        active = _read(operation_path(tmp_path, "wi-1"))
        assert active["status"] == "active"
        assert active["worker_id"] == "worker-a"
        tracker.heartbeat(message="worker is running")
        updated = _read(operation_path(tmp_path, "wi-1"))
        assert updated["message"] == "worker is running"

    completed = _read(operation_path(tmp_path, "wi-1"))
    assert completed["status"] == "completed"
    assert completed["completed_at"]


def test_operation_reaper_marks_stale_operation_as_stuck(tmp_path) -> None:
    run_dir = tmp_path / "runs" / "wi-stale"
    run_dir.mkdir(parents=True)
    write_json_atomic(
        run_dir / "operation.json",
        {
            "version": 1,
            "work_item_id": "wi-stale",
            "stage": "stage3",
            "operation": "stage3_code_worker",
            "worker_id": "worker-a",
            "pid": 999999,
            "started_at": "2026-04-26T00:00:00Z",
            "updated_at": "2026-04-26T00:00:00Z",
            "status": "active",
            "message": "silent worker",
            "heartbeat_interval_seconds": 15,
            "stale_after_seconds": 1,
            "subprocess": {},
        },
    )

    result = OperationReaper(tmp_path, stale_seconds=1).run()

    assert result.marked_stuck[0]["work_item_id"] == "wi-stale"
    recovery = _read(recovery_state_path(tmp_path, "wi-stale"))
    assert recovery["status"] == "stuck"
    assert recovery["reason"] == "stale_operation_heartbeat"
    assert recovery["operation_snapshot"]["operation"] == "stage3_code_worker"


def test_operation_reaper_syncs_stuck_reason_to_linear_when_configured(tmp_path) -> None:
    run_dir = tmp_path / "runs" / "wi-stale"
    run_dir.mkdir(parents=True)
    stage3_document = {
        "work_item": {
            "work_item_id": "wi-stale",
            "source_provider": "manual",
            "source_external_id": "manual-1",
            "title": "Stale run",
            "state": "PR_REVISION",
            "attempt_count": 1,
            "created_at": "2026-04-26T00:00:00Z",
            "updated_at": "2026-04-26T00:00:00Z",
        },
        "spec_packet": {},
        "policy_decision": {},
        "ticket_bundle": {},
        "eval_manifest": {},
        "pr_packet": {},
    }
    write_json_atomic(run_dir / "stage3-result.json", stage3_document)
    write_json_atomic(
        run_dir / "operation.json",
        {
            "version": 1,
            "work_item_id": "wi-stale",
            "stage": "stage3",
            "operation": "stage3_revision",
            "worker_id": "worker-a",
            "pid": 999999,
            "started_at": "2026-04-26T00:00:00Z",
            "updated_at": "2026-04-26T00:00:00Z",
            "status": "active",
            "message": "silent worker",
            "heartbeat_interval_seconds": 15,
            "stale_after_seconds": 1,
            "subprocess": {},
        },
    )

    class FakeLinearSync:
        repo_root = Path(__file__).resolve().parents[1]

        def __init__(self) -> None:
            self.calls: list[dict[str, object]] = []

        def sync_stage_result(self, stage_name, document, *, stall_reason=None):
            self.calls.append(
                {
                    "stage_name": stage_name,
                    "work_item_id": document["work_item"]["work_item_id"],
                    "stall_reason": stall_reason,
                }
            )
            return {"status": "synced"}

    linear_sync = FakeLinearSync()

    result = OperationReaper(tmp_path, stale_seconds=1, linear_sync=linear_sync).run()

    assert result.linear_sync_results == [{"status": "synced"}]
    assert linear_sync.calls == [
        {
            "stage_name": "stage3",
            "work_item_id": "wi-stale",
            "stall_reason": "stale_operation_heartbeat",
        }
    ]


def test_operation_reaper_marks_expired_run_lease_as_stuck(tmp_path) -> None:
    run_dir = tmp_path / "runs" / "wi-expired"
    run_dir.mkdir(parents=True)
    write_json_atomic(
        run_dir / ".automation.lock",
        {
            "scope": "run",
            "resource_id": "wi-expired",
            "lease_id": "lease-1",
            "acquired_at": "2026-04-26T00:00:00Z",
            "refreshed_at": "2026-04-26T00:00:00Z",
            "expires_at": "2026-04-26T00:00:01Z",
            "pid": 999999,
        },
    )

    result = OperationReaper(tmp_path, stale_seconds=1).run()

    assert result.marked_stuck[0]["reason"] == "expired_run_lease"
    recovery = _read(recovery_state_path(tmp_path, "wi-expired"))
    assert recovery["status"] == "stuck"
    assert recovery["lease_snapshot"]["lease_id"] == "lease-1"


def test_operation_reaper_treats_malformed_run_lease_as_stuck(tmp_path) -> None:
    run_dir = tmp_path / "runs" / "wi-malformed-lease"
    run_dir.mkdir(parents=True)
    (run_dir / ".automation.lock").write_text("not json", encoding="utf-8")

    result = OperationReaper(tmp_path, stale_seconds=1).run()

    assert result.marked_stuck[0]["reason"] == "expired_run_lease"
    recovery = _read(recovery_state_path(tmp_path, "wi-malformed-lease"))
    assert recovery["status"] == "stuck"
    assert "artifact_error" in recovery["lease_snapshot"]


def test_operation_reaper_treats_malformed_operation_as_stuck(tmp_path) -> None:
    run_dir = tmp_path / "runs" / "wi-malformed-operation"
    run_dir.mkdir(parents=True)
    (run_dir / "operation.json").write_text("not json", encoding="utf-8")

    result = OperationReaper(tmp_path, stale_seconds=1).run()

    assert result.marked_stuck[0]["reason"] == "malformed_operation_artifact"
    recovery = _read(recovery_state_path(tmp_path, "wi-malformed-operation"))
    assert recovery["status"] == "stuck"
    assert recovery["operation_snapshot"]["status"] == "artifact_error"


def test_operation_reaper_can_repair_malformed_recovery_for_stale_run(tmp_path) -> None:
    run_dir = tmp_path / "runs" / "wi-malformed-recovery"
    run_dir.mkdir(parents=True)
    (run_dir / "recovery-state.json").write_text("not json", encoding="utf-8")
    write_json_atomic(
        run_dir / "operation.json",
        {
            "version": 1,
            "work_item_id": "wi-malformed-recovery",
            "stage": "stage3",
            "operation": "stage3_code_worker",
            "worker_id": "worker-a",
            "pid": 999999,
            "started_at": "2026-04-26T00:00:00Z",
            "updated_at": "2026-04-26T00:00:00Z",
            "status": "active",
            "message": "silent worker",
            "heartbeat_interval_seconds": 15,
            "stale_after_seconds": 1,
            "subprocess": {},
        },
    )

    result = OperationReaper(tmp_path, stale_seconds=1).run()

    assert result.marked_stuck[0]["reason"] == "stale_operation_heartbeat"
    recovery = _read(recovery_state_path(tmp_path, "wi-malformed-recovery"))
    assert recovery["status"] == "stuck"


def test_recovery_manager_records_retry_unblock_and_dead_letter(tmp_path) -> None:
    write_json_atomic(
        operation_path(tmp_path, "wi-1"),
        {
            "version": 1,
            "work_item_id": "wi-1",
            "stage": "stage3",
            "operation": "stage3_code_worker",
            "worker_id": "worker-a",
            "pid": 999999,
            "started_at": "2026-04-26T00:00:00Z",
            "updated_at": "2026-04-26T00:00:00Z",
            "status": "active",
            "message": "stale",
            "heartbeat_interval_seconds": 15,
            "stale_after_seconds": 1,
            "subprocess": {},
        },
    )
    manager = RecoveryManager(tmp_path)

    retry = manager.retry("wi-1", reason="worker restarted")
    assert retry["status"] == "retry_pending"
    retry_state = _read(recovery_state_path(tmp_path, "wi-1"))
    assert retry_state["actions"][0]["action"] == "retry"
    assert retry_state["reason"] is None
    assert _read(operation_path(tmp_path, "wi-1"))["status"] == "cleared_by_operator"

    unblock = manager.unblock("wi-1", reason="operator cleared")
    assert unblock["status"] == "cleared"
    unblock_state = _read(recovery_state_path(tmp_path, "wi-1"))
    assert unblock_state["actions"][-1]["action"] == "unblock"
    assert unblock_state["reason"] is None

    dead = manager.dead_letter("wi-1", reason="invalid ticket")
    assert dead["status"] == "dead_letter"
    state = _read(recovery_state_path(tmp_path, "wi-1"))
    assert state["status"] == "dead_letter"
    assert state["reason"] == "invalid ticket"
    assert state["last_action_reason"] == "invalid ticket"


def test_recovery_manager_rejects_unknown_work_item(tmp_path) -> None:
    manager = RecoveryManager(tmp_path)

    with pytest.raises(ReliabilityError, match="No persisted run found"):
        manager.retry("wi-missing", reason="typo")

    assert not recovery_state_path(tmp_path, "wi-missing").exists()


def test_operation_reaper_does_not_overwrite_dead_letter_recovery(tmp_path) -> None:
    run_dir = tmp_path / "runs" / "wi-dead"
    run_dir.mkdir(parents=True)
    write_json_atomic(
        run_dir / "operation.json",
        {
            "version": 1,
            "work_item_id": "wi-dead",
            "stage": "stage3",
            "operation": "stage3_code_worker",
            "worker_id": "worker-a",
            "pid": 999999,
            "started_at": "2026-04-26T00:00:00Z",
            "updated_at": "2026-04-26T00:00:00Z",
            "status": "active",
            "message": "stale",
            "heartbeat_interval_seconds": 15,
            "stale_after_seconds": 1,
            "subprocess": {},
        },
    )
    RecoveryManager(tmp_path).dead_letter("wi-dead", reason="operator closed")

    result = OperationReaper(tmp_path, stale_seconds=1).run()

    assert result.marked_stuck == []
    state = _read(recovery_state_path(tmp_path, "wi-dead"))
    assert state["status"] == "dead_letter"
    assert state["reason"] == "operator closed"


def test_operation_reaper_refreshes_detected_at_after_recovery_cleared(tmp_path) -> None:
    run_dir = tmp_path / "runs" / "wi-restuck"
    run_dir.mkdir(parents=True)
    write_json_atomic(
        run_dir / "operation.json",
        {
            "version": 1,
            "work_item_id": "wi-restuck",
            "stage": "stage3",
            "operation": "stage3_code_worker",
            "worker_id": "worker-a",
            "pid": 999999,
            "started_at": "2026-04-26T00:00:00Z",
            "updated_at": "2026-04-26T00:00:00Z",
            "status": "active",
            "message": "stale",
            "heartbeat_interval_seconds": 15,
            "stale_after_seconds": 1,
            "subprocess": {},
        },
    )
    write_json_atomic(
        recovery_state_path(tmp_path, "wi-restuck"),
        {
            "version": 1,
            "work_item_id": "wi-restuck",
            "status": "cleared",
            "reason": None,
            "detected_at": "2026-04-26T00:00:00Z",
            "updated_at": "2026-04-26T00:00:01Z",
            "actions": [],
        },
    )

    OperationReaper(tmp_path, stale_seconds=1).run()

    state = _read(recovery_state_path(tmp_path, "wi-restuck"))
    assert state["status"] == "stuck"
    assert state["detected_at"] != "2026-04-26T00:00:00Z"


def test_operation_summary_tolerates_malformed_stale_threshold(tmp_path) -> None:
    write_json_atomic(
        operation_path(tmp_path, "wi-malformed"),
        {
            "version": 1,
            "work_item_id": "wi-malformed",
            "stage": "stage3",
            "operation": "stage3_code_worker",
            "worker_id": "worker-a",
            "pid": 999999,
            "started_at": "2026-04-26T00:00:00Z",
            "updated_at": "2026-04-26T00:00:00Z",
            "status": "active",
            "message": "stale",
            "heartbeat_interval_seconds": 15,
            "stale_after_seconds": "not-a-number",
            "subprocess": {},
        },
    )

    summary = operation_summary(tmp_path, "wi-malformed")

    assert summary is not None
    assert summary["heartbeat_age_seconds"] is not None
    assert summary["stale"] is True


def test_queue_classification_treats_human_wait_states_as_blocked() -> None:
    assert classify_queue(state="SECURITY_REVIEWING", stage_name="stage6") == "blocked"
    assert classify_queue(state="MERGE_REVIEWING", stage_name="merge") == "blocked"
    assert classify_queue(state="STAGING_SOAK", stage_name="stage7") == "blocked"
