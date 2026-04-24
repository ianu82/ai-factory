from __future__ import annotations

import json
import os
import re
import threading
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterator
from uuid import uuid4

from .build_review import BuildReviewError, Stage3BuildReviewPipeline
from .contracts import load_validators, validation_errors_for
from .controller import ControllerState, WorkItem
from .eval_execution import EvalExecutionError, Stage5EvalPipeline
from .feedback_synthesis import FeedbackSynthesisError, Stage9FeedbackSynthesisPipeline
from .integration import IntegrationError, Stage4IntegrationPipeline
from .intake import AnthropicScout, Stage1IntakePipeline, build_identifier, repo_root, utc_now
from .merge_orchestration import MergeError, StageMergePipeline
from .production_monitoring import (
    ProductionMonitoringError,
    Stage8ProductionMonitoringPipeline,
)
from .release_staging import ReleaseStagingError, Stage7ReleaseStagingPipeline
from .security_review import SecurityReviewError, Stage6SecurityReviewPipeline
from .ticketing import Stage2TicketingPipeline, TicketingError


class AutomationError(RuntimeError):
    """Base class for automation orchestration failures."""


class AutomationStateError(AutomationError):
    """Raised when automation state cannot be loaded or validated."""


class AutomationStateConflictError(AutomationStateError):
    """Raised when a stale automation-state save would clobber a newer update."""


class ArtifactStoreError(AutomationError):
    """Raised when a stage result cannot be persisted or reloaded."""


class LeaseBusyError(AutomationError):
    """Raised when another automation worker already owns a renewable lease."""


class RunLeaseBusyError(LeaseBusyError):
    """Raised when another automation worker already owns the run lease."""


class StateLeaseBusyError(LeaseBusyError):
    """Raised when another automation worker already owns the automation-state lease."""


class ImmediateHandoffError(AutomationError):
    """Raised when an immediate handoff fails for a direct library caller."""

    def __init__(
        self,
        message: str,
        *,
        result: ImmediateHandoffResult | None = None,
        cycle_result: Stage1AutomationCycleResult | None = None,
    ) -> None:
        super().__init__(message)
        self.result = result
        self.cycle_result = cycle_result


SUPPORTED_AUTOMATION_STAGES = (
    "stage1",
    "stage2",
    "stage3",
    "stage4",
    "stage5",
    "stage6",
    "merge",
    "stage7",
    "stage8",
    "stage9",
)
PROGRESSION_SCAN_STAGES = SUPPORTED_AUTOMATION_STAGES
STAGE_ORDER_RANK = {
    stage_name: index
    for index, stage_name in enumerate(SUPPORTED_AUTOMATION_STAGES)
}
WEEK_WINDOW_PATTERN = re.compile(r"^\d{4}-W\d{2}$")
STAGE_RESULT_REQUIRED_OBJECTS: dict[str, tuple[str, ...]] = {
    "stage1": ("source_item", "spec_packet", "policy_decision", "work_item"),
    "stage2": (
        "spec_packet",
        "policy_decision",
        "ticket_bundle",
        "eval_manifest",
        "work_item",
    ),
    "stage3": (
        "spec_packet",
        "policy_decision",
        "ticket_bundle",
        "eval_manifest",
        "pr_packet",
        "work_item",
    ),
    "stage4": (
        "spec_packet",
        "policy_decision",
        "ticket_bundle",
        "eval_manifest",
        "pr_packet",
        "prompt_contract",
        "tool_schema",
        "golden_dataset",
        "latency_baseline",
        "work_item",
    ),
    "stage5": (
        "spec_packet",
        "policy_decision",
        "ticket_bundle",
        "eval_manifest",
        "pr_packet",
        "prompt_contract",
        "tool_schema",
        "golden_dataset",
        "latency_baseline",
        "eval_report",
        "work_item",
    ),
    "stage6": (
        "spec_packet",
        "policy_decision",
        "ticket_bundle",
        "eval_manifest",
        "pr_packet",
        "prompt_contract",
        "tool_schema",
        "golden_dataset",
        "latency_baseline",
        "eval_report",
        "security_review",
        "work_item",
    ),
    "merge": (
        "spec_packet",
        "policy_decision",
        "ticket_bundle",
        "eval_manifest",
        "pr_packet",
        "prompt_contract",
        "tool_schema",
        "golden_dataset",
        "latency_baseline",
        "eval_report",
        "security_review",
        "merge_decision",
        "work_item",
    ),
    "stage7": (
        "spec_packet",
        "policy_decision",
        "ticket_bundle",
        "eval_manifest",
        "pr_packet",
        "prompt_contract",
        "tool_schema",
        "golden_dataset",
        "latency_baseline",
        "eval_report",
        "security_review",
        "promotion_decision",
        "work_item",
    ),
    "stage8": (
        "spec_packet",
        "policy_decision",
        "ticket_bundle",
        "eval_manifest",
        "pr_packet",
        "prompt_contract",
        "tool_schema",
        "golden_dataset",
        "latency_baseline",
        "eval_report",
        "security_review",
        "promotion_decision",
        "monitoring_report",
        "work_item",
    ),
    "stage9": (
        "spec_packet",
        "policy_decision",
        "ticket_bundle",
        "eval_manifest",
        "pr_packet",
        "prompt_contract",
        "tool_schema",
        "golden_dataset",
        "latency_baseline",
        "eval_report",
        "security_review",
        "promotion_decision",
        "monitoring_report",
        "feedback_report",
        "work_item",
    ),
}


def parse_utc_timestamp(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)


def weekly_window_label(timestamp: str | None = None) -> str:
    moment = parse_utc_timestamp(timestamp) if timestamp is not None else parse_utc_timestamp(utc_now())
    iso_year, iso_week, _ = moment.isocalendar()
    return f"{iso_year}-W{iso_week:02d}"


@dataclass(slots=True)
class AutomationState:
    version: int = 1
    seen_source_external_ids: list[str] = field(default_factory=list)
    weekly_feedback_windows: dict[str, str] = field(default_factory=dict)
    last_stage1_cycle_at: str | None = None
    last_stage9_cycle_at: str | None = None
    updated_at: str = field(default_factory=utc_now)

    @classmethod
    def from_document(cls, document: dict[str, Any]) -> "AutomationState":
        return cls(
            version=int(document["version"]),
            seen_source_external_ids=list(document["seen_source_external_ids"]),
            weekly_feedback_windows=dict(document["weekly_feedback_windows"]),
            last_stage1_cycle_at=document.get("last_stage1_cycle_at"),
            last_stage9_cycle_at=document.get("last_stage9_cycle_at"),
            updated_at=document["updated_at"],
        )

    def to_document(self) -> dict[str, Any]:
        return {
            "version": self.version,
            "seen_source_external_ids": list(self.seen_source_external_ids),
            "weekly_feedback_windows": dict(self.weekly_feedback_windows),
            "last_stage1_cycle_at": self.last_stage1_cycle_at,
            "last_stage9_cycle_at": self.last_stage9_cycle_at,
            "updated_at": self.updated_at,
        }


@dataclass(slots=True)
class StoredRunCandidate:
    work_item_id: str
    stage_name: str
    path: Path
    document: dict[str, Any]


@dataclass(slots=True)
class Stage1AutomationCycleResult:
    detected_count: int
    created_results: list[dict[str, str]]
    skipped_known_external_ids: list[str]
    deferred_external_ids: list[str]
    state: AutomationState
    advance_immediately: bool = False
    handoff_results: list[dict[str, Any]] = field(default_factory=list)

    def failed_handoffs(self) -> list[dict[str, Any]]:
        return [
            result
            for result in self.handoff_results
            if str(result.get("status", "")).lower() == "failed"
        ]

    def to_document(self) -> dict[str, Any]:
        return {
            "cycle": "stage1",
            "detected_count": self.detected_count,
            "created_results": list(self.created_results),
            "skipped_known_external_ids": list(self.skipped_known_external_ids),
            "deferred_external_ids": list(self.deferred_external_ids),
            "advance_immediately": self.advance_immediately,
            "handoff_results": list(self.handoff_results),
            "automation_state": self.state.to_document(),
        }


@dataclass(slots=True)
class WeeklyFeedbackAutomationCycleResult:
    window_label: str
    processed_results: list[dict[str, str]]
    skipped_runs: list[dict[str, str]]
    state: AutomationState

    def to_document(self) -> dict[str, Any]:
        return {
            "cycle": "stage9-weekly-feedback",
            "window_label": self.window_label,
            "processed_results": list(self.processed_results),
            "skipped_runs": list(self.skipped_runs),
            "automation_state": self.state.to_document(),
        }


@dataclass(slots=True)
class ProgressionRunResult:
    work_item_id: str
    starting_stage: str
    final_stage: str
    final_state: str
    stages_completed: list[str]
    stored_paths: dict[str, str]

    def to_document(self) -> dict[str, Any]:
        return {
            "work_item_id": self.work_item_id,
            "starting_stage": self.starting_stage,
            "final_stage": self.final_stage,
            "final_state": self.final_state,
            "stages_completed": list(self.stages_completed),
            "stored_paths": dict(self.stored_paths),
        }


@dataclass(slots=True)
class ProgressionCycleResult:
    processed_runs: list[ProgressionRunResult]
    skipped_runs: list[dict[str, str]]

    def to_document(self) -> dict[str, Any]:
        return {
            "cycle": "stage2-through-stage8-progression",
            "processed_runs": [run.to_document() for run in self.processed_runs],
            "skipped_runs": list(self.skipped_runs),
        }


@dataclass(slots=True)
class ImmediateHandoffResult:
    work_item_id: str
    source_stage: str
    source_state: str | None
    status: str
    final_stage: str | None = None
    final_state: str | None = None
    stages_completed: list[str] = field(default_factory=list)
    stored_paths: dict[str, str] = field(default_factory=dict)
    reason: str | None = None

    def to_document(self) -> dict[str, Any]:
        return {
            "work_item_id": self.work_item_id,
            "source_stage": self.source_stage,
            "source_state": self.source_state,
            "status": self.status,
            "final_stage": self.final_stage,
            "final_state": self.final_state,
            "stages_completed": list(self.stages_completed),
            "stored_paths": dict(self.stored_paths),
            "reason": self.reason,
        }

    def raise_for_failure(self) -> None:
        if self.status != "failed":
            return
        message = self.reason or "Immediate handoff failed."
        raise ImmediateHandoffError(message, result=self)


@dataclass(slots=True)
class AutomationSupervisorCycleResult:
    stage1_result: Stage1AutomationCycleResult
    progression_result: ProgressionCycleResult
    weekly_feedback_result: WeeklyFeedbackAutomationCycleResult | None = None
    post_progression_handoff_results: list[dict[str, Any]] = field(default_factory=list)

    def to_document(self) -> dict[str, Any]:
        return {
            "cycle": "automation-supervisor-cycle",
            "stage1_result": self.stage1_result.to_document(),
            "progression_result": self.progression_result.to_document(),
            "weekly_feedback_result": (
                None
                if self.weekly_feedback_result is None
                else self.weekly_feedback_result.to_document()
            ),
            "post_progression_handoff_results": list(self.post_progression_handoff_results),
        }


@dataclass(slots=True)
class FileLease:
    store: "FactoryRunStore"
    lock_path: Path
    lease_id: str
    resource_id: str
    scope: str
    ttl_seconds: float
    renew_interval_seconds: float
    _stop_event: threading.Event = field(
        default_factory=threading.Event,
        init=False,
        repr=False,
    )
    _thread: threading.Thread | None = field(default=None, init=False, repr=False)
    _renewal_error: AutomationError | None = field(default=None, init=False, repr=False)

    def start(self) -> None:
        if self._thread is not None:
            return
        self._thread = threading.Thread(
            target=self._heartbeat_loop,
            name=f"{self.scope}-lease-{self.resource_id}",
            daemon=True,
        )
        self._thread.start()

    def heartbeat(self) -> None:
        self._raise_if_renewal_failed()
        self.store._refresh_lease(
            self.lock_path,
            lease_id=self.lease_id,
            resource_id=self.resource_id,
            scope=self.scope,
            ttl_seconds=self.ttl_seconds,
        )

    def close(self) -> None:
        self._stop_event.set()
        if self._thread is not None:
            self._thread.join(timeout=max(1.0, self.renew_interval_seconds * 2))
        renewal_error = self._renewal_error
        self.store._release_lease(self.lock_path, lease_id=self.lease_id)
        if renewal_error is not None:
            raise renewal_error

    def _heartbeat_loop(self) -> None:
        while not self._stop_event.wait(self.renew_interval_seconds):
            try:
                self.store._refresh_lease(
                    self.lock_path,
                    lease_id=self.lease_id,
                    resource_id=self.resource_id,
                    scope=self.scope,
                    ttl_seconds=self.ttl_seconds,
                )
            except AutomationError as exc:
                self._renewal_error = exc
                self._stop_event.set()
                return

    def _raise_if_renewal_failed(self) -> None:
        if self._renewal_error is not None:
            raise self._renewal_error


class FactoryRunStore:
    """Persist stage results so recurring automation can operate on active runs."""

    def __init__(
        self,
        root: Path,
        *,
        repo_root_override: Path | None = None,
    ) -> None:
        self.root = root.resolve()
        self.repo_root = repo_root(repo_root_override)
        self.runs_dir = self.root / "runs"
        self.state_dir = self.root / "state"
        self.state_path = self.state_dir / "automation-state.json"
        validators = load_validators(self.repo_root)
        self.state_validator = validators["automation-state"]

    def load_state(self) -> AutomationState:
        document = self._load_state_document()
        if document is None:
            return AutomationState()
        return AutomationState.from_document(document)

    def save_state(
        self,
        state: AutomationState,
        *,
        expected_previous_updated_at: str | None = None,
    ) -> Path:
        self.runs_dir.mkdir(parents=True, exist_ok=True)
        self.state_dir.mkdir(parents=True, exist_ok=True)
        document = state.to_document()
        self._validate_state_document(document)
        if expected_previous_updated_at is not None:
            current_document = self._load_state_document()
            current_updated_at = (
                None if current_document is None else current_document.get("updated_at")
            )
            if current_updated_at != expected_previous_updated_at:
                raise AutomationStateConflictError(
                    "automation-state changed during update: expected previous updated_at "
                    f"'{expected_previous_updated_at}', found '{current_updated_at}'."
                )
        self._write_json_atomic(self.state_path, document)
        return self.state_path

    def save_stage_result(self, stage_name: str, document: dict[str, Any]) -> Path:
        self._validate_stage_name(stage_name)
        self._validate_stage_result_document(stage_name, document)
        work_item_id = self.extract_work_item_id(document)
        run_dir = self.runs_dir / work_item_id
        run_dir.mkdir(parents=True, exist_ok=True)
        path = run_dir / f"{stage_name}-result.json"
        self._write_json_atomic(path, document)
        return path

    @contextmanager
    def state_transaction(
        self,
        *,
        ttl_seconds: float = 300.0,
        renew_interval_seconds: float | None = None,
    ) -> Iterator[AutomationState]:
        with self.state_lease(
            ttl_seconds=ttl_seconds,
            renew_interval_seconds=renew_interval_seconds,
        ):
            state_previously_existed = self.state_path.exists()
            state = self.load_state()
            expected_previous_updated_at = state.updated_at if state_previously_existed else None
            yield state
            self.save_state(
                state,
                expected_previous_updated_at=expected_previous_updated_at,
            )

    def apply_stage_result_to_state(
        self,
        state: AutomationState,
        stage_name: str,
        document: dict[str, Any],
        *,
        window_label_override: str | None = None,
    ) -> None:
        self._validate_stage_name(stage_name)
        changed = False

        if stage_name == "stage1":
            source_item = document.get("source_item")
            if not isinstance(source_item, dict) or not source_item.get("external_id"):
                raise ArtifactStoreError(
                    "stage1 result is missing source_item.external_id for automation dedupe."
                )
            external_id = str(source_item["external_id"])
            if external_id not in state.seen_source_external_ids:
                state.seen_source_external_ids.append(external_id)
                state.seen_source_external_ids.sort()
                changed = True

        if stage_name == "stage9":
            feedback_report = document.get("feedback_report")
            work_item_id = self.extract_work_item_id(document)
            if not isinstance(feedback_report, dict):
                raise ArtifactStoreError(
                    "stage9 result is missing feedback_report for weekly automation state."
                )
            feedback_window = feedback_report.get("feedback_window")
            if not isinstance(feedback_window, dict):
                raise ArtifactStoreError(
                    "stage9 result is missing feedback_report.feedback_window for automation state."
                )
            if window_label_override is not None:
                window = window_label_override
                if state.weekly_feedback_windows.get(work_item_id) != window:
                    state.weekly_feedback_windows[work_item_id] = window
                    changed = True
            elif feedback_window.get("trigger") == "scheduled_rollup":
                feedback_timestamp = feedback_report["artifact"]["updated_at"]
                window = window_label_override or weekly_window_label(feedback_timestamp)
                if state.weekly_feedback_windows.get(work_item_id) != window:
                    state.weekly_feedback_windows[work_item_id] = window
                    changed = True

        if changed:
            state.updated_at = utc_now()

    def list_production_monitoring_candidates(self) -> list[StoredRunCandidate]:
        return [
            candidate
            for run_dir in self.iter_run_directories()
            if (
                candidate := self.load_latest_candidate(
                    run_dir,
                    ("stage8", "stage9"),
                    require_production_monitoring=True,
                )
            )
            is not None
        ]

    def list_progression_candidates(self) -> list[StoredRunCandidate]:
        return [
            candidate
            for run_dir in self.iter_run_directories()
            if (candidate := self.load_latest_candidate(run_dir, PROGRESSION_SCAN_STAGES))
            is not None
        ]

    def iter_run_directories(self) -> list[Path]:
        if not self.runs_dir.exists():
            return []
        return sorted(path for path in self.runs_dir.iterdir() if path.is_dir())

    def load_latest_candidate(
        self,
        run_dir: Path,
        stage_names: tuple[str, ...],
        *,
        require_production_monitoring: bool = False,
    ) -> StoredRunCandidate | None:
        selected_candidate = self._select_latest_candidate(
            run_dir=run_dir,
            stage_names=stage_names,
        )
        if selected_candidate is None:
            return None
        stage_name, selected_path, document = selected_candidate
        work_item_document = self.extract_work_item_document(document)
        if work_item_document["work_item_id"] != run_dir.name:
            raise ArtifactStoreError(
                "Stored stage result work_item.work_item_id "
                f"'{work_item_document['work_item_id']}' does not match run directory "
                f"'{run_dir.name}'."
            )
        if (
            require_production_monitoring
            and work_item_document["state"] != ControllerState.PRODUCTION_MONITORING.value
        ):
            return None
        return StoredRunCandidate(
            work_item_id=work_item_document["work_item_id"],
            stage_name=stage_name,
            path=selected_path,
            document=document,
        )

    @staticmethod
    def extract_work_item_document(document: dict[str, Any]) -> dict[str, Any]:
        work_item = document.get("work_item")
        if not isinstance(work_item, dict):
            raise ArtifactStoreError("Stored stage result is missing a work_item object.")
        return work_item

    @classmethod
    def extract_work_item_id(cls, document: dict[str, Any]) -> str:
        work_item_document = cls.extract_work_item_document(document)
        work_item_id = work_item_document.get("work_item_id")
        if not isinstance(work_item_id, str) or not work_item_id:
            raise ArtifactStoreError("Stored stage result is missing work_item.work_item_id.")
        return work_item_id

    @staticmethod
    def hydrate_work_item(document: dict[str, Any]) -> WorkItem:
        work_item_document = dict(FactoryRunStore.extract_work_item_document(document))
        history = document.get("history")
        if isinstance(history, list) and "history" not in work_item_document:
            work_item_document["history"] = history
        try:
            return WorkItem.from_document(work_item_document)
        except (KeyError, TypeError, ValueError) as exc:
            raise ArtifactStoreError(f"Stored work item could not be hydrated: {exc}") from exc

    @contextmanager
    def state_lease(
        self,
        *,
        ttl_seconds: float = 300.0,
        renew_interval_seconds: float | None = None,
    ) -> Iterator[FileLease]:
        self.state_dir.mkdir(parents=True, exist_ok=True)
        lock_path = self.state_dir / ".automation-state.lock"
        with self._lease(
            lock_path=lock_path,
            resource_id="automation-state",
            scope="state",
            ttl_seconds=ttl_seconds,
            renew_interval_seconds=renew_interval_seconds,
            busy_error_cls=StateLeaseBusyError,
            busy_message="automation-state is currently locked by another automation worker.",
        ) as lease:
            yield lease

    @contextmanager
    def run_lease(
        self,
        work_item_id: str,
        *,
        ttl_seconds: float = 1800.0,
        renew_interval_seconds: float | None = None,
    ) -> Iterator[FileLease]:
        run_dir = self.runs_dir / work_item_id
        run_dir.mkdir(parents=True, exist_ok=True)
        lock_path = run_dir / ".automation.lock"
        with self._lease(
            lock_path=lock_path,
            resource_id=work_item_id,
            scope="run",
            ttl_seconds=ttl_seconds,
            renew_interval_seconds=renew_interval_seconds,
            busy_error_cls=RunLeaseBusyError,
            busy_message=f"Run '{work_item_id}' is currently locked by another automation worker.",
        ) as lease:
            yield lease

    @contextmanager
    def _lease(
        self,
        *,
        lock_path: Path,
        resource_id: str,
        scope: str,
        ttl_seconds: float,
        renew_interval_seconds: float | None,
        busy_error_cls: type[LeaseBusyError],
        busy_message: str,
    ) -> Iterator[FileLease]:
        if ttl_seconds <= 0:
            raise AutomationError(f"{scope} lease ttl_seconds must be > 0.")
        interval = renew_interval_seconds
        if interval is None:
            interval = max(0.1, ttl_seconds / 3.0)
        if interval <= 0 or interval >= ttl_seconds:
            raise AutomationError(
                f"{scope} lease renew_interval_seconds must be > 0 and < ttl_seconds."
            )

        lock_path.parent.mkdir(parents=True, exist_ok=True)
        lease_id = uuid4().hex
        now = parse_utc_timestamp(utc_now())
        self._clear_expired_lease(lock_path, now=now)
        document = self._lease_document(
            resource_id=resource_id,
            scope=scope,
            lease_id=lease_id,
            acquired_at=now,
            ttl_seconds=ttl_seconds,
            refreshed_at=now,
        )
        try:
            fd = os.open(lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o644)
        except FileExistsError as exc:
            raise busy_error_cls(busy_message) from exc

        lease = FileLease(
            store=self,
            lock_path=lock_path,
            lease_id=lease_id,
            resource_id=resource_id,
            scope=scope,
            ttl_seconds=ttl_seconds,
            renew_interval_seconds=interval,
        )
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as handle:
                json.dump(document, handle, indent=2)
                handle.write("\n")
            lease.start()
            yield lease
        finally:
            lease.close()

    @staticmethod
    def _lease_document(
        *,
        resource_id: str,
        scope: str,
        lease_id: str,
        acquired_at: datetime,
        ttl_seconds: float,
        refreshed_at: datetime,
    ) -> dict[str, Any]:
        expires_at = refreshed_at + timedelta(seconds=ttl_seconds)
        return {
            "scope": scope,
            "resource_id": resource_id,
            "lease_id": lease_id,
            "acquired_at": acquired_at.isoformat().replace("+00:00", "Z"),
            "refreshed_at": refreshed_at.isoformat().replace("+00:00", "Z"),
            "expires_at": expires_at.isoformat().replace("+00:00", "Z"),
            "pid": os.getpid(),
        }

    def _refresh_lease(
        self,
        lock_path: Path,
        *,
        lease_id: str,
        resource_id: str,
        scope: str,
        ttl_seconds: float,
    ) -> None:
        try:
            document = json.loads(lock_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise AutomationError(
                f"Could not refresh {scope} lease for '{resource_id}': {exc}"
            ) from exc

        if document.get("lease_id") != lease_id:
            raise AutomationError(
                f"{scope.capitalize()} lease for '{resource_id}' is no longer owned by this worker."
            )
        acquired_at_raw = document.get("acquired_at")
        if not isinstance(acquired_at_raw, str):
            raise AutomationError(
                f"{scope.capitalize()} lease for '{resource_id}' is missing acquired_at."
            )
        refreshed_at = parse_utc_timestamp(utc_now())
        updated_document = self._lease_document(
            resource_id=resource_id,
            scope=scope,
            lease_id=lease_id,
            acquired_at=parse_utc_timestamp(acquired_at_raw),
            ttl_seconds=ttl_seconds,
            refreshed_at=refreshed_at,
        )
        self._write_json_atomic(lock_path, updated_document)

    @staticmethod
    def _clear_expired_lease(lock_path: Path, *, now: datetime) -> None:
        if not lock_path.exists():
            return
        try:
            document = json.loads(lock_path.read_text(encoding="utf-8"))
            expires_at = document.get("expires_at")
            if not isinstance(expires_at, str):
                lock_path.unlink(missing_ok=True)
                return
            if parse_utc_timestamp(expires_at) <= now:
                lock_path.unlink(missing_ok=True)
        except (OSError, json.JSONDecodeError, ValueError):
            lock_path.unlink(missing_ok=True)

    @staticmethod
    def _release_lease(lock_path: Path, *, lease_id: str) -> None:
        try:
            document = json.loads(lock_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            lock_path.unlink(missing_ok=True)
            return

        if document.get("lease_id") != lease_id:
            return
        lock_path.unlink(missing_ok=True)

    def _load_state_document(self) -> dict[str, Any] | None:
        if not self.state_path.exists():
            return None
        try:
            document = json.loads(self.state_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise AutomationStateError(
                f"Could not load automation state at {self.state_path}: {exc}"
            ) from exc
        self._validate_state_document(document)
        return document

    @staticmethod
    def _write_json_atomic(path: Path, document: dict[str, Any]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = path.with_name(f".{path.name}.{uuid4().hex}.tmp")
        try:
            tmp_path.write_text(f"{json.dumps(document, indent=2)}\n", encoding="utf-8")
            os.replace(tmp_path, path)
        finally:
            tmp_path.unlink(missing_ok=True)

    def _validate_state_document(self, document: dict[str, Any]) -> None:
        errors = validation_errors_for(self.state_validator, document)
        if errors:
            raise AutomationStateError(
                f"automation-state failed validation: {'; '.join(errors)}"
            )

    def _select_latest_candidate(
        self,
        *,
        run_dir: Path,
        stage_names: tuple[str, ...],
    ) -> tuple[str, Path, dict[str, Any]] | None:
        loaded_candidates: list[tuple[str, Path, dict[str, Any]]] = []
        for stage_name in stage_names:
            path = run_dir / f"{stage_name}-result.json"
            document = self._load_stage_result_document(stage_name, path)
            if document is not None:
                loaded_candidates.append((stage_name, path, document))

        if not loaded_candidates:
            return None
        return max(
            loaded_candidates,
            key=lambda candidate: (
                self._result_updated_at(candidate[2]),
                STAGE_ORDER_RANK[candidate[0]],
            ),
        )

    def _load_stage_result_document(
        self,
        stage_name: str,
        path: Path,
    ) -> dict[str, Any] | None:
        if not path.exists():
            return None
        try:
            document = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise ArtifactStoreError(
                f"Could not read stored {stage_name} result at {path}: {exc}"
            ) from exc
        self._validate_stage_result_document(stage_name, document)
        return document

    @staticmethod
    def _validate_stage_name(stage_name: str) -> None:
        if stage_name not in SUPPORTED_AUTOMATION_STAGES:
            raise ArtifactStoreError(
                f"Unsupported automation stage '{stage_name}'. "
                f"Expected one of: {', '.join(SUPPORTED_AUTOMATION_STAGES)}."
            )

    @staticmethod
    def _result_updated_at(document: dict[str, Any]) -> datetime:
        work_item_document = FactoryRunStore.extract_work_item_document(document)
        updated_at = work_item_document.get("updated_at")
        if not isinstance(updated_at, str) or not updated_at:
            raise ArtifactStoreError(
                "Stored stage result is missing work_item.updated_at for freshness comparison."
            )
        try:
            return parse_utc_timestamp(updated_at)
        except ValueError as exc:
            raise ArtifactStoreError(
                f"Stored stage result has invalid work_item.updated_at '{updated_at}'."
            ) from exc

    @staticmethod
    def _validate_stage_result_document(stage_name: str, document: dict[str, Any]) -> None:
        required_objects = STAGE_RESULT_REQUIRED_OBJECTS[stage_name]
        missing = [
            field_name
            for field_name in required_objects
            if not isinstance(document.get(field_name), dict)
        ]
        if missing:
            joined = ", ".join(missing)
            raise ArtifactStoreError(
                f"{stage_name} result is missing required object fields: {joined}."
            )


class FactoryAutomationCoordinator:
    """Drive recurring Stage 1 and Stage 9 cycles against the persisted run store."""

    def __init__(
        self,
        store_dir: Path,
        *,
        repo_root_override: Path | None = None,
        stage1_pipeline: Stage1IntakePipeline | None = None,
        stage2_pipeline: Stage2TicketingPipeline | None = None,
        stage3_pipeline: Stage3BuildReviewPipeline | None = None,
        stage4_pipeline: Stage4IntegrationPipeline | None = None,
        stage5_pipeline: Stage5EvalPipeline | None = None,
        stage6_pipeline: Stage6SecurityReviewPipeline | None = None,
        merge_pipeline: StageMergePipeline | None = None,
        stage7_pipeline: Stage7ReleaseStagingPipeline | None = None,
        stage8_pipeline: Stage8ProductionMonitoringPipeline | None = None,
        stage9_pipeline: Stage9FeedbackSynthesisPipeline | None = None,
    ) -> None:
        self.repo_root = repo_root(repo_root_override)
        self.store = FactoryRunStore(store_dir, repo_root_override=self.repo_root)
        self.stage1_pipeline = stage1_pipeline or Stage1IntakePipeline(self.repo_root)
        self.stage2_pipeline = stage2_pipeline or Stage2TicketingPipeline(self.repo_root)
        self.stage3_pipeline = stage3_pipeline or Stage3BuildReviewPipeline(self.repo_root)
        self.stage4_pipeline = stage4_pipeline or Stage4IntegrationPipeline(self.repo_root)
        self.stage5_pipeline = stage5_pipeline or Stage5EvalPipeline(self.repo_root)
        self.stage6_pipeline = stage6_pipeline or Stage6SecurityReviewPipeline(self.repo_root)
        self.merge_pipeline = merge_pipeline or StageMergePipeline(self.repo_root)
        self.stage7_pipeline = stage7_pipeline or Stage7ReleaseStagingPipeline(self.repo_root)
        self.stage8_pipeline = stage8_pipeline or Stage8ProductionMonitoringPipeline(self.repo_root)
        self.stage9_pipeline = stage9_pipeline or Stage9FeedbackSynthesisPipeline(self.repo_root)

    def register_bundle(
        self,
        stage_name: str,
        document: dict[str, Any],
    ) -> tuple[Path, AutomationState]:
        with self.store.state_transaction() as state:
            stored_path = self.store.save_stage_result(stage_name, document)
            self.store.apply_stage_result_to_state(state, stage_name, document)
        return stored_path, state

    def run_supervisor_cycle(
        self,
        *,
        html: str | None = None,
        source_url: str | None = None,
        detected_at: str | None = None,
        max_new_items: int | None = None,
        advance_immediately: bool = True,
        raise_on_failed_handoff: bool = True,
        repository: str = "mindsdb/platform",
        run_weekly_feedback: bool = False,
        window_label: str | None = None,
        feedback_window_days: int = 7,
    ) -> AutomationSupervisorCycleResult:
        stage1_result = self.run_stage1_cycle(
            html=html,
            source_url=source_url,
            detected_at=detected_at,
            max_new_items=max_new_items,
            advance_immediately=advance_immediately,
            raise_on_failed_handoff=raise_on_failed_handoff,
            repository=repository,
        )
        progression_result = self.run_progression_cycle(repository=repository)
        post_progression_handoff_results: list[dict[str, Any]] = []
        if not run_weekly_feedback:
            for run in progression_result.processed_runs:
                if run.final_stage != "stage8":
                    continue
                handoff = self.run_immediate_handoff(
                    run.work_item_id,
                    raise_on_failure=False,
                    repository=repository,
                )
                post_progression_handoff_results.append(handoff.to_document())
                if raise_on_failed_handoff and handoff.status == "failed":
                    message = handoff.reason or "Immediate handoff failed."
                    raise ImmediateHandoffError(message, result=handoff)
        weekly_feedback_result = None
        if run_weekly_feedback:
            weekly_feedback_result = self.run_weekly_feedback_cycle(
                window_label=window_label,
                feedback_window_days=feedback_window_days,
            )
        return AutomationSupervisorCycleResult(
            stage1_result=stage1_result,
            progression_result=progression_result,
            weekly_feedback_result=weekly_feedback_result,
            post_progression_handoff_results=post_progression_handoff_results,
        )

    def run_stage1_cycle(
        self,
        *,
        html: str | None = None,
        source_url: str | None = None,
        detected_at: str | None = None,
        max_new_items: int | None = None,
        advance_immediately: bool = False,
        raise_on_failed_handoff: bool = True,
        repository: str = "mindsdb/platform",
    ) -> Stage1AutomationCycleResult:
        if max_new_items is not None and max_new_items < 1:
            raise AutomationError("max_new_items must be >= 1 when provided.")

        scout = AnthropicScout(source_url=source_url)
        items = scout.list_items(html=html, detected_at=detected_at)
        created_results: list[dict[str, str]] = []
        handoff_results: list[dict[str, Any]] = []
        with self.store.state_transaction() as state:
            known_external_ids = set(state.seen_source_external_ids)
            skipped_known = sorted(
                item.external_id for item in items if item.external_id in known_external_ids
            )
            unseen_items = [item for item in items if item.external_id not in known_external_ids]
            selected_items = unseen_items[:max_new_items] if max_new_items is not None else unseen_items
            deferred_ids = [item.external_id for item in unseen_items[len(selected_items) :]]

            for item in selected_items:
                result = self.stage1_pipeline.process_item(item)
                document = result.to_document()
                stored_path = self.store.save_stage_result("stage1", document)
                self.store.apply_stage_result_to_state(state, "stage1", document)
                created_results.append(
                    {
                        "work_item_id": result.work_item.work_item_id,
                        "source_external_id": item.external_id,
                        "state": result.work_item.state.value,
                        "stored_path": str(stored_path),
                    }
                )

            state.last_stage1_cycle_at = detected_at or utc_now()
            state.updated_at = utc_now()

        if advance_immediately:
            for created in created_results:
                handoff_results.append(
                    self.run_immediate_handoff(
                        created["work_item_id"],
                        raise_on_failure=False,
                        repository=repository,
                    ).to_document()
                )

        cycle_result = Stage1AutomationCycleResult(
            detected_count=len(items),
            created_results=created_results,
            skipped_known_external_ids=skipped_known,
            deferred_external_ids=deferred_ids,
            state=state,
            advance_immediately=advance_immediately,
            handoff_results=handoff_results,
        )
        failed_handoffs = cycle_result.failed_handoffs()
        if raise_on_failed_handoff and failed_handoffs:
            message = str(failed_handoffs[0].get("reason") or "Immediate handoff failed.")
            raise ImmediateHandoffError(message, cycle_result=cycle_result)
        return cycle_result

    def run_immediate_handoff(
        self,
        work_item_id: str,
        *,
        feedback_window_days: int = 1,
        raise_on_failure: bool = True,
        repository: str = "mindsdb/platform",
    ) -> ImmediateHandoffResult:
        if feedback_window_days < 1:
            raise AutomationError("feedback_window_days must be >= 1.")
        run_dir = self.store.runs_dir / work_item_id
        if not run_dir.exists():
            return ImmediateHandoffResult(
                work_item_id=work_item_id,
                source_stage="store_scan",
                source_state=None,
                status="skipped",
                reason="no_persisted_run_found",
            )
        try:
            with self.store.run_lease(work_item_id):
                try:
                    candidate = self.store.load_latest_candidate(run_dir, PROGRESSION_SCAN_STAGES)
                except ArtifactStoreError as exc:
                    result = ImmediateHandoffResult(
                        work_item_id=work_item_id,
                        source_stage="store_scan",
                        source_state=None,
                        status="failed",
                        reason=str(exc),
                    )
                else:
                    if candidate is None:
                        result = ImmediateHandoffResult(
                            work_item_id=work_item_id,
                            source_stage="store_scan",
                            source_state=None,
                            status="skipped",
                            reason="no_persisted_run_found",
                        )
                    else:
                        result = self._run_immediate_handoff_candidate(
                            candidate,
                            repository=repository,
                            feedback_window_days=feedback_window_days,
                        )
        except RunLeaseBusyError:
            result = ImmediateHandoffResult(
                work_item_id=work_item_id,
                source_stage="store_scan",
                source_state=None,
                status="skipped",
                reason="run_locked",
            )

        if raise_on_failure:
            result.raise_for_failure()
        return result

    def run_progression_cycle(
        self,
        *,
        repository: str = "mindsdb/platform",
    ) -> ProgressionCycleResult:
        processed_runs: list[ProgressionRunResult] = []
        skipped_runs: list[dict[str, str]] = []

        for run_dir in self.store.iter_run_directories():
            try:
                with self.store.run_lease(run_dir.name):
                    try:
                        candidate = self.store.load_latest_candidate(run_dir, PROGRESSION_SCAN_STAGES)
                    except ArtifactStoreError as exc:
                        skipped_runs.append(
                            {
                                "work_item_id": run_dir.name,
                                "stage_name": "store_scan",
                                "reason": str(exc),
                            }
                        )
                        continue

                    if candidate is None:
                        continue

                    try:
                        run_result = self._advance_candidate(
                            candidate,
                            repository=repository,
                        )
                    except (
                        ArtifactStoreError,
                        TicketingError,
                        BuildReviewError,
                        IntegrationError,
                        EvalExecutionError,
                        SecurityReviewError,
                        MergeError,
                        ReleaseStagingError,
                        ProductionMonitoringError,
                    ) as exc:
                        skipped_runs.append(
                            {
                                "work_item_id": candidate.work_item_id,
                                "stage_name": candidate.stage_name,
                                "reason": str(exc),
                            }
                        )
                        continue

                    if run_result is None:
                        skipped_runs.append(
                            {
                                "work_item_id": candidate.work_item_id,
                                "stage_name": candidate.stage_name,
                                "reason": self._skip_reason(candidate),
                            }
                        )
                        continue

                    processed_runs.append(run_result)
            except RunLeaseBusyError:
                skipped_runs.append(
                    {
                        "work_item_id": run_dir.name,
                        "stage_name": "store_scan",
                        "reason": "run_locked",
                    }
                )

        return ProgressionCycleResult(
            processed_runs=processed_runs,
            skipped_runs=skipped_runs,
        )

    def _run_immediate_handoff_candidate(
        self,
        candidate: StoredRunCandidate,
        *,
        repository: str,
        feedback_window_days: int,
    ) -> ImmediateHandoffResult:
        source_work_item = self.store.extract_work_item_document(candidate.document)
        try:
            run_result = self._advance_candidate(
                candidate,
                repository=repository,
                continue_into_feedback=True,
                feedback_window_days=feedback_window_days,
            )
        except (
            ArtifactStoreError,
            TicketingError,
            BuildReviewError,
            IntegrationError,
            EvalExecutionError,
            SecurityReviewError,
            MergeError,
            ReleaseStagingError,
            ProductionMonitoringError,
            FeedbackSynthesisError,
        ) as exc:
            return ImmediateHandoffResult(
                work_item_id=candidate.work_item_id,
                source_stage=candidate.stage_name,
                source_state=source_work_item["state"],
                status="failed",
                reason=str(exc),
            )

        if run_result is None:
            return ImmediateHandoffResult(
                work_item_id=candidate.work_item_id,
                source_stage=candidate.stage_name,
                source_state=source_work_item["state"],
                status="skipped",
                reason=self._immediate_handoff_skip_reason(candidate),
            )

        return ImmediateHandoffResult(
            work_item_id=run_result.work_item_id,
            source_stage=run_result.starting_stage,
            source_state=source_work_item["state"],
            status="progressed",
            final_stage=run_result.final_stage,
            final_state=run_result.final_state,
            stages_completed=run_result.stages_completed,
            stored_paths=run_result.stored_paths,
        )

    def _advance_candidate(
        self,
        candidate: StoredRunCandidate,
        *,
        repository: str,
        continue_into_feedback: bool = False,
        feedback_window_days: int = 7,
    ) -> ProgressionRunResult | None:
        current_stage = candidate.stage_name
        current_document = candidate.document
        current_work_item = self.store.extract_work_item_document(current_document)
        current_state = ControllerState(current_work_item["state"])
        self._assert_progression_artifacts(
            current_stage=current_stage,
            state=current_state,
            document=current_document,
        )

        if (
            self._skip_reason_for_state(
                current_state,
                current_stage,
                current_document,
                continue_into_feedback=continue_into_feedback,
            )
            is not None
        ):
            return None

        stages_completed: list[str] = []
        stored_paths: dict[str, str] = {}
        while True:
            next_step = self._advance_once(
                current_stage=current_stage,
                current_document=current_document,
                repository=repository,
                continue_into_feedback=continue_into_feedback,
                feedback_window_days=feedback_window_days,
            )
            if next_step is None:
                break

            next_stage, next_document, continue_allowed = next_step
            stored_path = self.store.save_stage_result(next_stage, next_document)
            stages_completed.append(next_stage)
            stored_paths[next_stage] = str(stored_path)
            current_stage = next_stage
            current_document = next_document
            if not continue_allowed:
                break

        if not stages_completed:
            return None

        final_work_item = self.store.extract_work_item_document(current_document)
        return ProgressionRunResult(
            work_item_id=final_work_item["work_item_id"],
            starting_stage=candidate.stage_name,
            final_stage=current_stage,
            final_state=final_work_item["state"],
            stages_completed=stages_completed,
            stored_paths=stored_paths,
        )

    def _advance_once(
        self,
        *,
        current_stage: str,
        current_document: dict[str, Any],
        repository: str,
        continue_into_feedback: bool,
        feedback_window_days: int,
    ) -> tuple[str, dict[str, Any], bool] | None:
        current_work_item = self.store.extract_work_item_document(current_document)
        state = ControllerState(current_work_item["state"])
        work_item = self.store.hydrate_work_item(current_document)

        if state is ControllerState.POLICY_ASSIGNED:
            result = self.stage2_pipeline.process(
                current_document["spec_packet"],
                current_document["policy_decision"],
                work_item,
            )
            return ("stage2", result.to_document(), True)

        if state is ControllerState.TICKETED:
            result = self.stage3_pipeline.process(
                current_document["spec_packet"],
                current_document["policy_decision"],
                current_document["ticket_bundle"],
                current_document["eval_manifest"],
                work_item,
                repository=repository,
            )
            return (
                "stage3",
                result.to_document(),
                result.work_item.state is ControllerState.PR_REVIEWABLE,
            )

        if state is ControllerState.PR_REVISION:
            if current_stage != "stage3":
                return None
            result = self.stage3_pipeline.process(
                current_document["spec_packet"],
                current_document["policy_decision"],
                current_document["ticket_bundle"],
                current_document["eval_manifest"],
                work_item,
                repository=repository,
            )
            return (
                "stage3",
                result.to_document(),
                result.work_item.state is ControllerState.PR_REVIEWABLE,
            )

        if state is ControllerState.PR_REVIEWABLE:
            if self._has_stage4_artifacts(current_document):
                result = self.stage5_pipeline.process(
                    current_document["spec_packet"],
                    current_document["policy_decision"],
                    current_document["ticket_bundle"],
                    current_document["eval_manifest"],
                    current_document["pr_packet"],
                    current_document["prompt_contract"],
                    current_document["tool_schema"],
                    current_document["golden_dataset"],
                    current_document["latency_baseline"],
                    work_item,
                )
                return (
                    "stage5",
                    result.to_document(),
                    result.work_item.state is ControllerState.PR_MERGEABLE,
                )

            if not self.stage4_pipeline.integration_engineer.requires_integration(
                current_document["spec_packet"],
                current_document["ticket_bundle"],
            ):
                return None

            result = self.stage4_pipeline.process(
                current_document["spec_packet"],
                current_document["policy_decision"],
                current_document["ticket_bundle"],
                current_document["eval_manifest"],
                current_document["pr_packet"],
                work_item,
            )
            return ("stage4", result.to_document(), True)

        if state is ControllerState.PR_MERGEABLE:
            result = self.stage6_pipeline.process(
                current_document["spec_packet"],
                current_document["policy_decision"],
                current_document["ticket_bundle"],
                current_document["eval_manifest"],
                current_document["pr_packet"],
                current_document["prompt_contract"],
                current_document["tool_schema"],
                current_document["golden_dataset"],
                current_document["latency_baseline"],
                current_document["eval_report"],
                work_item,
            )
            return (
                "stage6",
                result.to_document(),
                result.work_item.state is ControllerState.SECURITY_APPROVED,
            )

        if state is ControllerState.SECURITY_APPROVED:
            result = self.merge_pipeline.process(
                current_document["spec_packet"],
                current_document["policy_decision"],
                current_document["ticket_bundle"],
                current_document["eval_manifest"],
                current_document["pr_packet"],
                current_document["prompt_contract"],
                current_document["tool_schema"],
                current_document["golden_dataset"],
                current_document["latency_baseline"],
                current_document["eval_report"],
                current_document["security_review"],
                work_item,
            )
            return (
                "merge",
                result.to_document(),
                result.work_item.state is ControllerState.MERGED,
            )

        if state is ControllerState.MERGED:
            result = self.stage7_pipeline.process(
                current_document["spec_packet"],
                current_document["policy_decision"],
                current_document["ticket_bundle"],
                current_document["eval_manifest"],
                current_document["pr_packet"],
                current_document["prompt_contract"],
                current_document["tool_schema"],
                current_document["golden_dataset"],
                current_document["latency_baseline"],
                current_document["eval_report"],
                current_document["security_review"],
                work_item,
                merge_decision=current_document.get("merge_decision"),
            )
            return (
                "stage7",
                result.to_document(),
                result.work_item.state is ControllerState.PRODUCTION_MONITORING,
            )

        if (
            state is ControllerState.PRODUCTION_MONITORING
            and not self._has_stage8_artifacts(current_document)
        ):
            result = self.stage8_pipeline.process(
                current_document["spec_packet"],
                current_document["policy_decision"],
                current_document["ticket_bundle"],
                current_document["eval_manifest"],
                current_document["pr_packet"],
                current_document["prompt_contract"],
                current_document["tool_schema"],
                current_document["golden_dataset"],
                current_document["latency_baseline"],
                current_document["eval_report"],
                current_document["security_review"],
                current_document["promotion_decision"],
                work_item,
                merge_decision=current_document.get("merge_decision"),
            )
            return ("stage8", result.to_document(), continue_into_feedback)

        if (
            state is ControllerState.PRODUCTION_MONITORING
            and self._has_stage8_artifacts(current_document)
            and current_stage == "stage8"
            and continue_into_feedback
            and self._requires_immediate_feedback(current_document)
        ):
            result = self._feedback_handoff_step(
                current_document,
                work_item,
                feedback_window_days=feedback_window_days,
            )
            return ("stage9", result.to_document(), False)

        return None

    @staticmethod
    def _has_stage4_artifacts(document: dict[str, Any]) -> bool:
        return all(
            isinstance(document.get(field_name), dict)
            for field_name in (
                "prompt_contract",
                "tool_schema",
                "golden_dataset",
                "latency_baseline",
            )
        )

    @staticmethod
    def _has_stage8_artifacts(document: dict[str, Any]) -> bool:
        return isinstance(document.get("monitoring_report"), dict)

    @staticmethod
    def _has_open_incident(pr_packet: dict[str, Any]) -> bool:
        artifact = pr_packet["artifact"]
        return (
            artifact["owner_agent"] == "SRE Sentinel"
            and artifact["status"] == "blocked"
            and artifact["next_stage"] in {"feedback_synthesis", "human_incident_response"}
        )

    def _requires_immediate_feedback(self, document: dict[str, Any]) -> bool:
        monitoring_report = document.get("monitoring_report")
        pr_packet = document.get("pr_packet")
        if not isinstance(monitoring_report, dict) or not isinstance(pr_packet, dict):
            return False
        monitoring_status = monitoring_report["monitoring_decision"]["status"]
        if monitoring_status != "healthy":
            return True
        return self._has_open_incident(pr_packet)

    def _feedback_handoff_step(
        self,
        current_document: dict[str, Any],
        work_item: WorkItem,
        *,
        feedback_window_days: int,
    ):
        monitoring_report = current_document["monitoring_report"]
        feedback_report_id = build_identifier(
            "feedback-report",
            f"incident-{monitoring_report['artifact']['id']}",
            max_length=72,
        )
        return self.stage9_pipeline.process(
            current_document["spec_packet"],
            current_document["policy_decision"],
            current_document["ticket_bundle"],
            current_document["eval_manifest"],
            current_document["pr_packet"],
            current_document["prompt_contract"],
            current_document["tool_schema"],
            current_document["golden_dataset"],
            current_document["latency_baseline"],
            current_document["eval_report"],
            current_document["security_review"],
            current_document["promotion_decision"],
            monitoring_report,
            work_item,
            merge_decision=current_document.get("merge_decision"),
            feedback_report_id=feedback_report_id,
            feedback_window_days=feedback_window_days,
        )

    def _skip_reason(self, candidate: StoredRunCandidate) -> str:
        work_item_document = self.store.extract_work_item_document(candidate.document)
        state = ControllerState(work_item_document["state"])
        reason = self._skip_reason_for_state(
            state,
            candidate.stage_name,
            candidate.document,
            continue_into_feedback=False,
        )
        if reason is not None:
            return reason
        return "no_autonomous_progress_available"

    def _immediate_handoff_skip_reason(self, candidate: StoredRunCandidate) -> str:
        work_item_document = self.store.extract_work_item_document(candidate.document)
        state = ControllerState(work_item_document["state"])
        if (
            state is ControllerState.PRODUCTION_MONITORING
            and candidate.stage_name == "stage8"
            and self._has_stage8_artifacts(candidate.document)
            and not self._requires_immediate_feedback(candidate.document)
        ):
            return "no_immediate_feedback_required"
        reason = self._skip_reason_for_state(
            state,
            candidate.stage_name,
            candidate.document,
            continue_into_feedback=True,
        )
        if reason is not None:
            return reason
        return "no_autonomous_progress_available"

    def _skip_reason_for_state(
        self,
        state: ControllerState,
        current_stage: str,
        current_document: dict[str, Any],
        *,
        continue_into_feedback: bool,
    ) -> str | None:
        if state in {ControllerState.WATCHLISTED, ControllerState.REJECTED}:
            return "non_actionable_state"
        if state is ControllerState.DEAD_LETTER:
            return "dead_letter_state"
        if state is ControllerState.SECURITY_REVIEWING:
            return "awaiting_security_signoff"
        if state is ControllerState.MERGE_REVIEWING:
            return "awaiting_merge_signoff"
        if state is ControllerState.STAGING_SOAK:
            return "awaiting_release_signoff"
        if state is ControllerState.PRODUCTION_MONITORING and self._has_stage8_artifacts(current_document):
            if (
                continue_into_feedback
                and current_stage == "stage8"
                and self._requires_immediate_feedback(current_document)
            ):
                return None
            return "already_in_production_monitoring"
        if state is ControllerState.PR_REVIEWABLE and not self._has_stage4_artifacts(current_document):
            if not self.stage4_pipeline.integration_engineer.requires_integration(
                current_document["spec_packet"],
                current_document["ticket_bundle"],
            ):
                return "non_model_touching_progression_not_supported"
        if state is ControllerState.PR_REVISION and current_stage != "stage3":
            return "awaiting_builder_follow_up"
        return None

    @staticmethod
    def _require_document_objects(
        document: dict[str, Any],
        field_names: tuple[str, ...],
        *,
        state: ControllerState,
    ) -> None:
        missing = [
            field_name for field_name in field_names if not isinstance(document.get(field_name), dict)
        ]
        if missing:
            raise ArtifactStoreError(
                "Stored stage result for "
                f"{state.value} is missing required object fields: {', '.join(missing)}."
            )

    def _assert_progression_artifacts(
        self,
        *,
        current_stage: str,
        state: ControllerState,
        document: dict[str, Any],
    ) -> None:
        if state is ControllerState.POLICY_ASSIGNED:
            self._require_document_objects(
                document,
                ("spec_packet", "policy_decision"),
                state=state,
            )
            return

        if state in {ControllerState.TICKETED, ControllerState.PR_REVISION}:
            if state is ControllerState.PR_REVISION and current_stage != "stage3":
                return
            self._require_document_objects(
                document,
                ("spec_packet", "policy_decision", "ticket_bundle", "eval_manifest"),
                state=state,
            )
            return

        if state is ControllerState.PR_REVIEWABLE:
            self._require_document_objects(
                document,
                ("spec_packet", "policy_decision", "ticket_bundle", "eval_manifest", "pr_packet"),
                state=state,
            )
            if self._has_stage4_artifacts(document):
                self._require_document_objects(
                    document,
                    ("prompt_contract", "tool_schema", "golden_dataset", "latency_baseline"),
                    state=state,
                )
            return

        if state is ControllerState.PR_MERGEABLE:
            self._require_document_objects(
                document,
                (
                    "spec_packet",
                    "policy_decision",
                    "ticket_bundle",
                    "eval_manifest",
                    "pr_packet",
                    "prompt_contract",
                    "tool_schema",
                    "golden_dataset",
                    "latency_baseline",
                    "eval_report",
                ),
                state=state,
            )
            return

        if state is ControllerState.SECURITY_APPROVED:
            self._require_document_objects(
                document,
                (
                    "spec_packet",
                    "policy_decision",
                    "ticket_bundle",
                    "eval_manifest",
                    "pr_packet",
                    "prompt_contract",
                    "tool_schema",
                    "golden_dataset",
                    "latency_baseline",
                    "eval_report",
                    "security_review",
                ),
                state=state,
            )
            return

        if state is ControllerState.MERGED:
            self._require_document_objects(
                document,
                (
                    "spec_packet",
                    "policy_decision",
                    "ticket_bundle",
                    "eval_manifest",
                    "pr_packet",
                    "prompt_contract",
                    "tool_schema",
                    "golden_dataset",
                    "latency_baseline",
                    "eval_report",
                    "security_review",
                    "merge_decision",
                ),
                state=state,
            )
            return

        if state is ControllerState.PRODUCTION_MONITORING:
            self._require_document_objects(
                document,
                (
                    "spec_packet",
                    "policy_decision",
                    "ticket_bundle",
                    "eval_manifest",
                    "pr_packet",
                    "prompt_contract",
                    "tool_schema",
                    "golden_dataset",
                    "latency_baseline",
                    "eval_report",
                    "security_review",
                    "promotion_decision",
                ),
                state=state,
            )
            if self._has_stage8_artifacts(document):
                self._require_document_objects(document, ("monitoring_report",), state=state)

    def run_weekly_feedback_cycle(
        self,
        *,
        window_label: str | None = None,
        feedback_window_days: int = 7,
    ) -> WeeklyFeedbackAutomationCycleResult:
        if feedback_window_days < 1:
            raise AutomationError("feedback_window_days must be >= 1.")
        window = window_label or weekly_window_label()
        if not WEEK_WINDOW_PATTERN.match(window):
            raise AutomationError(
                f"window_label must match YYYY-Www; received '{window}'."
            )

        processed_results: list[dict[str, str]] = []
        skipped_runs: list[dict[str, str]] = []
        with self.store.state_transaction() as state:
            for run_dir in self.store.iter_run_directories():
                try:
                    with self.store.run_lease(run_dir.name):
                        try:
                            candidate = self.store.load_latest_candidate(
                                run_dir,
                                ("stage8", "stage9"),
                                require_production_monitoring=True,
                            )
                        except ArtifactStoreError as exc:
                            skipped_runs.append(
                                {
                                    "work_item_id": run_dir.name,
                                    "reason": str(exc),
                                    "stage_name": "store_scan",
                                }
                            )
                            continue

                        if candidate is None:
                            continue

                        if state.weekly_feedback_windows.get(candidate.work_item_id) == window:
                            skipped_runs.append(
                                {
                                    "work_item_id": candidate.work_item_id,
                                    "reason": "already_synthesized_for_window",
                                    "stage_name": candidate.stage_name,
                                }
                            )
                            continue

                        try:
                            work_item = self.store.hydrate_work_item(candidate.document)
                            feedback_report_id = build_identifier(
                                "feedback-report",
                                (
                                    f"{window}-attempt-{work_item.attempt_count}-"
                                    f"prv-{candidate.document['pr_packet']['artifact']['version']}-"
                                    f"{candidate.document['spec_packet']['artifact']['id']}"
                                ),
                                max_length=72,
                            )
                            result = self.stage9_pipeline.process(
                                candidate.document["spec_packet"],
                                candidate.document["policy_decision"],
                                candidate.document["ticket_bundle"],
                                candidate.document["eval_manifest"],
                                candidate.document["pr_packet"],
                                candidate.document["prompt_contract"],
                                candidate.document["tool_schema"],
                                candidate.document["golden_dataset"],
                                candidate.document["latency_baseline"],
                                candidate.document["eval_report"],
                                candidate.document["security_review"],
                                candidate.document["promotion_decision"],
                                candidate.document["monitoring_report"],
                                work_item,
                                merge_decision=candidate.document.get("merge_decision"),
                                feedback_report_id=feedback_report_id,
                                feedback_window_days=feedback_window_days,
                            )
                        except (ArtifactStoreError, FeedbackSynthesisError) as exc:
                            skipped_runs.append(
                                {
                                    "work_item_id": candidate.work_item_id,
                                    "reason": str(exc),
                                    "stage_name": candidate.stage_name,
                                }
                            )
                            continue

                        result_document = result.to_document()
                        stored_path = self.store.save_stage_result("stage9", result_document)
                        self.store.apply_stage_result_to_state(
                            state,
                            "stage9",
                            result_document,
                            window_label_override=window,
                        )
                        processed_results.append(
                            {
                                "work_item_id": result.work_item.work_item_id,
                                "source_stage": candidate.stage_name,
                                "feedback_report_id": result.feedback_report["artifact"]["id"],
                                "stored_path": str(stored_path),
                            }
                        )
                except RunLeaseBusyError:
                    skipped_runs.append(
                        {
                            "work_item_id": run_dir.name,
                            "reason": "run_locked",
                            "stage_name": "store_scan",
                        }
                    )

            state.last_stage9_cycle_at = utc_now()
            state.updated_at = utc_now()

        return WeeklyFeedbackAutomationCycleResult(
            window_label=window,
            processed_results=processed_results,
            skipped_runs=skipped_runs,
            state=state,
        )
