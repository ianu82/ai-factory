from __future__ import annotations

import json
import os
import threading
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from uuid import uuid4

from .controller import ControllerState
from .intake import utc_now


OPERATION_FILENAME = "operation.json"
RECOVERY_STATE_FILENAME = "recovery-state.json"
SCHEDULER_STATE_FILENAME = "scheduler-state.json"
DEFAULT_OPERATION_HEARTBEAT_SECONDS = 15.0
DEFAULT_OPERATION_STALE_SECONDS = 120.0


class ReliabilityError(RuntimeError):
    """Raised when operation, scheduler, or recovery state cannot be managed."""


def parse_utc_timestamp(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)


def seconds_since(timestamp: str | None, *, now: datetime | None = None) -> float | None:
    if not timestamp:
        return None
    try:
        moment = parse_utc_timestamp(timestamp)
    except (TypeError, ValueError):
        return None
    current = now or datetime.now(timezone.utc)
    return max(0.0, (current - moment).total_seconds())


def operation_heartbeat_seconds_from_env() -> float:
    return _float_env(
        "AI_FACTORY_OPERATION_HEARTBEAT_SECONDS",
        default=DEFAULT_OPERATION_HEARTBEAT_SECONDS,
        minimum=0.1,
    )


def operation_stale_seconds_from_env() -> float:
    return _float_env(
        "AI_FACTORY_OPERATION_STALE_SECONDS",
        default=DEFAULT_OPERATION_STALE_SECONDS,
        minimum=1.0,
    )


def worker_id_from_env() -> str:
    return os.environ.get("AI_FACTORY_WORKER_ID", "").strip() or f"worker-{os.getpid()}"


def max_active_runs_from_env() -> int:
    return _int_env("AI_FACTORY_MAX_ACTIVE_RUNS", default=1, minimum=1)


def _float_env(name: str, *, default: float, minimum: float) -> float:
    raw = os.environ.get(name)
    if raw is None or not raw.strip():
        return default
    try:
        value = float(raw)
    except ValueError as exc:
        raise ReliabilityError(f"{name} must be a number.") from exc
    if value < minimum:
        raise ReliabilityError(f"{name} must be >= {minimum}.")
    return value


def _int_env(name: str, *, default: int, minimum: int) -> int:
    raw = os.environ.get(name)
    if raw is None or not raw.strip():
        return default
    try:
        value = int(raw)
    except ValueError as exc:
        raise ReliabilityError(f"{name} must be an integer.") from exc
    if value < minimum:
        raise ReliabilityError(f"{name} must be >= {minimum}.")
    return value


def run_dir(store_dir: Path, work_item_id: str) -> Path:
    return store_dir.resolve() / "runs" / work_item_id


def operation_path(store_dir: Path, work_item_id: str) -> Path:
    return run_dir(store_dir, work_item_id) / OPERATION_FILENAME


def recovery_state_path(store_dir: Path, work_item_id: str) -> Path:
    return run_dir(store_dir, work_item_id) / RECOVERY_STATE_FILENAME


def scheduler_state_path(store_dir: Path, work_item_id: str) -> Path:
    return run_dir(store_dir, work_item_id) / SCHEDULER_STATE_FILENAME


def load_json_object(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    try:
        document = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ReliabilityError(f"Could not read JSON artifact at {path}: {exc}") from exc
    if not isinstance(document, dict):
        raise ReliabilityError(f"JSON artifact at {path} must be an object.")
    return document


def write_json_atomic(path: Path, document: dict[str, Any]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_name(f".{path.name}.{uuid4().hex}.tmp")
    try:
        tmp_path.write_text(f"{json.dumps(document, indent=2)}\n", encoding="utf-8")
        os.replace(tmp_path, path)
    finally:
        tmp_path.unlink(missing_ok=True)
    return path


@dataclass(slots=True)
class OperationTracker:
    store_dir: Path
    work_item_id: str
    stage: str
    operation: str
    worker_id: str | None = None
    message: str = "Operation started."
    heartbeat_interval_seconds: float = DEFAULT_OPERATION_HEARTBEAT_SECONDS
    stale_seconds: float = DEFAULT_OPERATION_STALE_SECONDS
    subprocess: dict[str, Any] | None = None
    _lock: threading.Lock = field(default_factory=threading.Lock, init=False, repr=False)
    _stop_event: threading.Event = field(default_factory=threading.Event, init=False, repr=False)
    _thread: threading.Thread | None = field(default=None, init=False, repr=False)
    _document: dict[str, Any] = field(default_factory=dict, init=False, repr=False)

    def __enter__(self) -> "OperationTracker":
        if self.heartbeat_interval_seconds <= 0:
            raise ReliabilityError("operation heartbeat interval must be > 0.")
        if self.stale_seconds <= 0:
            raise ReliabilityError("operation stale threshold must be > 0.")
        timestamp = utc_now()
        self._document = {
            "version": 1,
            "work_item_id": self.work_item_id,
            "stage": self.stage,
            "operation": self.operation,
            "worker_id": self.worker_id or worker_id_from_env(),
            "pid": os.getpid(),
            "started_at": timestamp,
            "updated_at": timestamp,
            "status": "active",
            "message": self.message,
            "heartbeat_interval_seconds": self.heartbeat_interval_seconds,
            "stale_after_seconds": self.stale_seconds,
            "subprocess": dict(self.subprocess or {}),
        }
        self._write_locked()
        self._thread = threading.Thread(
            target=self._heartbeat_loop,
            name=f"factory-operation-{self.work_item_id}",
            daemon=True,
        )
        self._thread.start()
        return self

    def __exit__(self, exc_type, exc, traceback) -> bool:
        self._stop_event.set()
        if self._thread is not None:
            self._thread.join(timeout=max(1.0, self.heartbeat_interval_seconds * 2))
        if exc is None:
            self.complete("Operation completed.")
        else:
            self.fail(str(exc) or exc_type.__name__)
        return False

    def heartbeat(
        self,
        *,
        message: str | None = None,
        subprocess: dict[str, Any] | None = None,
    ) -> None:
        self.update(message=message, subprocess=subprocess)

    def update(
        self,
        *,
        message: str | None = None,
        status: str | None = None,
        subprocess: dict[str, Any] | None = None,
        extra: dict[str, Any] | None = None,
    ) -> None:
        with self._lock:
            if not self._document:
                raise ReliabilityError("operation tracker has not been started.")
            if message is not None:
                self._document["message"] = message
            if status is not None:
                self._document["status"] = status
            if subprocess is not None:
                self._document["subprocess"] = dict(subprocess)
            if extra:
                self._document.update(extra)
            self._document["updated_at"] = utc_now()
            self._write_locked()

    def complete(self, message: str = "Operation completed.") -> None:
        self.update(
            message=message,
            status="completed",
            extra={"completed_at": utc_now()},
        )

    def fail(self, message: str) -> None:
        self.update(
            message=message,
            status="failed",
            extra={"completed_at": utc_now(), "error": message},
        )

    def _heartbeat_loop(self) -> None:
        while not self._stop_event.wait(self.heartbeat_interval_seconds):
            try:
                self.heartbeat()
            except ReliabilityError:
                self._stop_event.set()
                return

    def _write_locked(self) -> None:
        write_json_atomic(operation_path(self.store_dir, self.work_item_id), self._document)


def operation_summary(
    store_dir: Path,
    work_item_id: str,
    *,
    stale_seconds: float | None = None,
) -> dict[str, Any] | None:
    document = load_json_object(operation_path(store_dir, work_item_id))
    if document is None:
        return None
    heartbeat_age = seconds_since(str(document.get("updated_at") or ""))
    threshold = stale_seconds
    if threshold is None:
        try:
            threshold = float(document.get("stale_after_seconds") or DEFAULT_OPERATION_STALE_SECONDS)
        except (TypeError, ValueError):
            threshold = DEFAULT_OPERATION_STALE_SECONDS
    status = str(document.get("status") or "unknown")
    stale = status == "active" and heartbeat_age is not None and heartbeat_age > threshold
    return {
        **document,
        "heartbeat_age_seconds": heartbeat_age,
        "stale": stale,
    }


def recovery_state(store_dir: Path, work_item_id: str) -> dict[str, Any] | None:
    return load_json_object(recovery_state_path(store_dir, work_item_id))


def scheduler_state(store_dir: Path, work_item_id: str) -> dict[str, Any] | None:
    return load_json_object(scheduler_state_path(store_dir, work_item_id))


def save_scheduler_state(
    store_dir: Path,
    work_item_id: str,
    *,
    queue_status: str,
    last_skip_reason: str | None = None,
    worker_id: str | None = None,
) -> Path:
    document = {
        "version": 1,
        "work_item_id": work_item_id,
        "queue_status": queue_status,
        "last_skip_reason": last_skip_reason,
        "worker_id": worker_id or worker_id_from_env(),
        "updated_at": utc_now(),
    }
    return write_json_atomic(scheduler_state_path(store_dir, work_item_id), document)


def classify_queue(
    *,
    state: str,
    stage_name: str,
    recovery: dict[str, Any] | None = None,
) -> str:
    recovery_status = str((recovery or {}).get("status") or "")
    if recovery_status == "dead_letter" or state == ControllerState.DEAD_LETTER.value:
        return "dead_letter"
    if recovery_status == "stuck":
        return "blocked"
    if state in {
        ControllerState.WATCHLISTED.value,
        ControllerState.REJECTED.value,
        ControllerState.PRODUCTION_MONITORING.value,
    }:
        return "complete"
    if state in {
        ControllerState.POLICY_ASSIGNED.value,
        ControllerState.TICKETED.value,
        ControllerState.BUILD_READY.value,
    }:
        return "new_build"
    if state == ControllerState.PR_REVISION.value:
        return "revision"
    if state in {
        ControllerState.SECURITY_REVIEWING.value,
        ControllerState.MERGE_REVIEWING.value,
        ControllerState.STAGING_SOAK.value,
    }:
        return "blocked"
    if state in {
        ControllerState.PR_REVIEWABLE.value,
        ControllerState.PR_MERGEABLE.value,
        ControllerState.SECURITY_APPROVED.value,
        ControllerState.MERGED.value,
    }:
        return "eval"
    if stage_name == "stage1":
        return "new_build"
    return "blocked"


QUEUE_PRIORITY = {
    "new_build": 0,
    "eval": 1,
    "revision": 2,
    "blocked": 3,
    "dead_letter": 4,
    "complete": 5,
}


def queue_priority(queue_status: str) -> int:
    return QUEUE_PRIORITY.get(queue_status, 99)


def recommended_action_for_reason(reason: str, work_item_id: str) -> str:
    if reason == "stale_operation_heartbeat":
        return (
            "Inspect the worker process and recent logs, then run "
            f"`factory-retry --work-item-id {work_item_id}` if it is safe to resume."
        )
    if reason == "expired_run_lease":
        return f"Run `factory-retry --work-item-id {work_item_id}` to reacquire the run lease."
    return f"Review the run artifacts, then retry, unblock, or dead-letter `{work_item_id}`."


def process_running(pid: int | None) -> bool | None:
    if pid is None or pid <= 0:
        return None
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except OSError:
        return None
    return True


@dataclass(slots=True)
class OperationReaperResult:
    checked_runs: int
    marked_stuck: list[dict[str, Any]]
    skipped_runs: list[dict[str, Any]]
    linear_sync_results: list[dict[str, Any]] = field(default_factory=list)

    def to_document(self) -> dict[str, Any]:
        return {
            "cycle": "factory-reap-stale-operations",
            "checked_runs": self.checked_runs,
            "marked_stuck": list(self.marked_stuck),
            "skipped_runs": list(self.skipped_runs),
            "linear_sync_results": list(self.linear_sync_results),
            "completed_at": utc_now(),
        }


class OperationReaper:
    def __init__(
        self,
        store_dir: Path,
        *,
        stale_seconds: float | None = None,
        linear_sync: Any | None = None,
    ) -> None:
        self.store_dir = store_dir.resolve()
        self.runs_dir = self.store_dir / "runs"
        self.stale_seconds = stale_seconds or operation_stale_seconds_from_env()
        self.linear_sync = linear_sync

    def run(self) -> OperationReaperResult:
        checked = 0
        marked: list[dict[str, Any]] = []
        skipped: list[dict[str, Any]] = []
        linear_results: list[dict[str, Any]] = []
        if not self.runs_dir.exists():
            return OperationReaperResult(0, [], [])

        for path in sorted(item for item in self.runs_dir.iterdir() if item.is_dir()):
            checked += 1
            work_item_id = path.name
            try:
                result = self._inspect_run(path)
            except ReliabilityError as exc:
                skipped.append({"work_item_id": work_item_id, "reason": str(exc)})
                continue
            if result is None:
                skipped.append({"work_item_id": work_item_id, "reason": "healthy_or_no_operation"})
                continue
            marked.append(result)
            linear_result = self._sync_linear_stuck(work_item_id, result["reason"])
            if linear_result is not None:
                linear_results.append(linear_result)

        return OperationReaperResult(
            checked_runs=checked,
            marked_stuck=marked,
            skipped_runs=skipped,
            linear_sync_results=linear_results,
        )

    def _inspect_run(self, path: Path) -> dict[str, Any] | None:
        work_item_id = path.name
        try:
            current_recovery = recovery_state(self.store_dir, work_item_id)
        except ReliabilityError:
            current_recovery = None
        if isinstance(current_recovery, dict) and current_recovery.get("status") == "dead_letter":
            return None
        malformed_operation_reason = None
        try:
            operation = operation_summary(
                self.store_dir,
                work_item_id,
                stale_seconds=self.stale_seconds,
            )
        except ReliabilityError as exc:
            malformed_operation_reason = str(exc)
            operation = {
                "status": "artifact_error",
                "reason": malformed_operation_reason,
                "message": malformed_operation_reason,
            }
        malformed_lease_reason = None
        try:
            lease = load_json_object(path / ".automation.lock")
        except ReliabilityError as exc:
            malformed_lease_reason = str(exc)
            lease = {"artifact_error": malformed_lease_reason}
        stale_operation = (
            malformed_operation_reason is not None
            or (operation is not None and operation.get("stale") is True)
        )
        expired_lease = malformed_lease_reason is not None or self._lease_expired(lease)
        if not stale_operation and not expired_lease:
            return None

        if malformed_operation_reason is not None:
            reason = "malformed_operation_artifact"
        elif stale_operation:
            reason = "stale_operation_heartbeat"
        else:
            reason = "expired_run_lease"
        pid_status = None
        if operation is not None:
            raw_pid = operation.get("pid")
            pid_status = process_running(raw_pid if isinstance(raw_pid, int) else None)
        recovery = self._mark_stuck(
            work_item_id=work_item_id,
            reason=reason,
            operation=operation,
            lease=lease,
            pid_running=pid_status,
        )
        return {
            "work_item_id": work_item_id,
            "reason": reason,
            "pid_running": pid_status,
            "recovery_state_path": str(recovery),
        }

    @staticmethod
    def _lease_expired(lease: dict[str, Any] | None) -> bool:
        if lease is None:
            return False
        expires_at = lease.get("expires_at")
        if not isinstance(expires_at, str) or not expires_at:
            return True
        try:
            return parse_utc_timestamp(expires_at) <= datetime.now(timezone.utc)
        except ValueError:
            return True

    def _mark_stuck(
        self,
        *,
        work_item_id: str,
        reason: str,
        operation: dict[str, Any] | None,
        lease: dict[str, Any] | None,
        pid_running: bool | None,
    ) -> Path:
        path = recovery_state_path(self.store_dir, work_item_id)
        try:
            current = load_json_object(path) or {}
        except ReliabilityError:
            current = {}
        attempts = [
            dict(item)
            for item in current.get("actions", [])
            if isinstance(item, dict)
        ]
        detected_at = (
            current.get("detected_at")
            if current.get("status") == "stuck" and current.get("reason") == reason
            else utc_now()
        )
        document = {
            "version": 1,
            "work_item_id": work_item_id,
            "status": "stuck",
            "reason": reason,
            "detected_at": detected_at,
            "updated_at": utc_now(),
            "recommended_action": recommended_action_for_reason(reason, work_item_id),
            "operation_snapshot": operation,
            "lease_snapshot": lease,
            "pid_running": pid_running,
            "actions": attempts,
        }
        return write_json_atomic(path, document)

    def _sync_linear_stuck(self, work_item_id: str, reason: str) -> dict[str, Any] | None:
        if self.linear_sync is None:
            return None
        try:
            from .automation import FactoryRunStore, PROGRESSION_SCAN_STAGES
            from .linear_workflow import LinearWorkflowError

            store = FactoryRunStore(self.store_dir, repo_root_override=self.linear_sync.repo_root)
            candidate = store.load_latest_candidate(
                self.store_dir / "runs" / work_item_id,
                PROGRESSION_SCAN_STAGES,
            )
            if candidate is None:
                return {
                    "work_item_id": work_item_id,
                    "status": "skipped",
                    "reason": "no_persisted_run_found",
                }
            try:
                result = self.linear_sync.sync_stage_result(
                    candidate.stage_name,
                    candidate.document,
                    stall_reason=reason,
                )
            except LinearWorkflowError as exc:
                return {
                    "work_item_id": work_item_id,
                    "status": "failed",
                    "reason": str(exc),
                }
            return result
        except Exception as exc:  # pragma: no cover - defensive boundary for ops safety.
            return {
                "work_item_id": work_item_id,
                "status": "failed",
                "reason": str(exc),
            }


class RecoveryManager:
    def __init__(self, store_dir: Path, *, linear_sync: Any | None = None) -> None:
        self.store_dir = store_dir.resolve()
        self.linear_sync = linear_sync

    def retry(self, work_item_id: str, *, reason: str | None = None) -> dict[str, Any]:
        return self._record_action(
            work_item_id,
            status="retry_pending",
            action="retry",
            reason=reason or "operator_retry_requested",
            clear_blocked=True,
        )

    def unblock(self, work_item_id: str, *, reason: str | None = None) -> dict[str, Any]:
        return self._record_action(
            work_item_id,
            status="cleared",
            action="unblock",
            reason=reason or "operator_unblocked_run",
            clear_blocked=True,
        )

    def dead_letter(self, work_item_id: str, *, reason: str) -> dict[str, Any]:
        if not reason.strip():
            raise ReliabilityError("dead-letter reason is required.")
        return self._record_action(
            work_item_id,
            status="dead_letter",
            action="dead_letter",
            reason=reason,
            clear_blocked=False,
        )

    def _record_action(
        self,
        work_item_id: str,
        *,
        status: str,
        action: str,
        reason: str,
        clear_blocked: bool,
    ) -> dict[str, Any]:
        path = recovery_state_path(self.store_dir, work_item_id)
        if not run_dir(self.store_dir, work_item_id).exists():
            raise ReliabilityError(f"No persisted run found for work item `{work_item_id}`.")
        current = load_json_object(path) or {
            "version": 1,
            "work_item_id": work_item_id,
            "detected_at": None,
            "reason": None,
        }
        actions = [
            dict(item)
            for item in current.get("actions", [])
            if isinstance(item, dict)
        ]
        actions.append(
            {
                "action": action,
                "reason": reason,
                "at": utc_now(),
                "operator": os.environ.get("USER") or os.environ.get("LOGNAME") or "unknown",
            }
        )
        document = {
            **current,
            "version": 1,
            "work_item_id": work_item_id,
            "status": status,
            "reason": None if clear_blocked else reason,
            "updated_at": utc_now(),
            "last_action": action,
            "last_action_reason": reason,
            "actions": actions,
        }
        write_json_atomic(path, document)
        if clear_blocked:
            self._clear_stale_local_blockers(work_item_id, action=action)
        linear_result = self._sync_linear(work_item_id, stall_reason=None if clear_blocked else reason)
        return {
            "work_item_id": work_item_id,
            "status": status,
            "action": action,
            "reason": reason,
            "recovery_state_path": str(path),
            "linear_sync": linear_result,
        }

    def _clear_stale_local_blockers(self, work_item_id: str, *, action: str) -> None:
        operation = load_json_object(operation_path(self.store_dir, work_item_id))
        if operation is not None and operation.get("status") == "active":
            operation["status"] = "cleared_by_operator"
            operation["message"] = f"Operation cleared by `{action}` recovery command."
            operation["updated_at"] = utc_now()
            operation["completed_at"] = utc_now()
            write_json_atomic(operation_path(self.store_dir, work_item_id), operation)

        lock_path = run_dir(self.store_dir, work_item_id) / ".automation.lock"
        lease = load_json_object(lock_path)
        if OperationReaper._lease_expired(lease):
            lock_path.unlink(missing_ok=True)

    def _sync_linear(self, work_item_id: str, *, stall_reason: str | None) -> dict[str, Any] | None:
        if self.linear_sync is None:
            return None
        try:
            from .automation import FactoryRunStore, PROGRESSION_SCAN_STAGES
            from .linear_workflow import LinearWorkflowError

            store = FactoryRunStore(self.store_dir, repo_root_override=self.linear_sync.repo_root)
            candidate = store.load_latest_candidate(
                self.store_dir / "runs" / work_item_id,
                PROGRESSION_SCAN_STAGES,
            )
            if candidate is None:
                return {
                    "status": "skipped",
                    "reason": "no_persisted_run_found",
                    "work_item_id": work_item_id,
                }
            try:
                return self.linear_sync.sync_stage_result(
                    candidate.stage_name,
                    candidate.document,
                    stall_reason=stall_reason,
                )
            except LinearWorkflowError as exc:
                return {
                    "status": "failed",
                    "reason": str(exc),
                    "work_item_id": work_item_id,
                }
        except Exception as exc:  # pragma: no cover - defensive boundary for ops safety.
            return {
                "status": "failed",
                "reason": str(exc),
                "work_item_id": work_item_id,
            }
