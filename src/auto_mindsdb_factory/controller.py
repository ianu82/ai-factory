from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import StrEnum
from pathlib import Path
from typing import Any


def utc_now() -> str:
    return (
        datetime.now(timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )


class ControllerState(StrEnum):
    DETECTED = "DETECTED"
    SCREENING = "SCREENING"
    WATCHLISTED = "WATCHLISTED"
    REJECTED = "REJECTED"
    SPEC_DRAFTING = "SPEC_DRAFTING"
    SPEC_READY = "SPEC_READY"
    POLICY_ASSIGNED = "POLICY_ASSIGNED"
    TICKETING = "TICKETING"
    TICKETED = "TICKETED"
    BUILD_READY = "BUILD_READY"
    BUILDING = "BUILDING"
    PR_OPEN = "PR_OPEN"
    REVIEWING = "REVIEWING"
    PR_REVISION = "PR_REVISION"
    PR_REVIEWABLE = "PR_REVIEWABLE"
    PR_MERGEABLE = "PR_MERGEABLE"
    SECURITY_REVIEWING = "SECURITY_REVIEWING"
    SECURITY_APPROVED = "SECURITY_APPROVED"
    MERGE_REVIEWING = "MERGE_REVIEWING"
    MERGED = "MERGED"
    STAGING_SOAK = "STAGING_SOAK"
    PRODUCTION_MONITORING = "PRODUCTION_MONITORING"
    DEAD_LETTER = "DEAD_LETTER"


class ControllerEvent(StrEnum):
    CHANGELOG_ITEM_RECORDED = "changelog_item_recorded"
    RELEVANCE_WATCHLIST = "relevance_watchlist"
    RELEVANCE_REJECTED = "relevance_rejected"
    RELEVANCE_ACCEPTED = "relevance_accepted"
    SPEC_PACKET_VALID = "spec_packet_valid"
    POLICY_DECISION_WRITTEN = "policy_decision_written"
    TICKET_GENERATION_STARTED = "ticket_generation_started"
    TICKET_BUNDLE_VALID = "ticket_bundle_valid"
    BUILD_SLOT_RESERVED = "build_slot_reserved"
    BUILDER_STARTED = "builder_started"
    PR_CREATED = "pr_created"
    REVIEWER_STARTED = "reviewer_started"
    BLOCKING_FINDINGS_PRESENT = "blocking_findings_present"
    BUILDER_RETRY_STARTED = "builder_retry_started"
    REVIEWABLE_TRUE = "reviewable_true"
    REQUIRED_EVAL_TIER_PASSED = "required_eval_tier_passed"
    REQUIRED_EVAL_TIER_FAILED = "required_eval_tier_failed"
    SECURITY_REVIEW_STARTED = "security_review_started"
    SECURITY_FINDINGS_PRESENT = "security_findings_present"
    SECURITY_SIGNOFF_GRANTED = "security_signoff_granted"
    MERGE_STARTED = "merge_started"
    MERGE_BLOCKED = "merge_blocked"
    PR_MERGED = "pr_merged"
    STAGING_SOAK_STARTED = "staging_soak_started"
    STAGING_SOAK_FAILED = "staging_soak_failed"
    PRODUCTION_PROMOTED = "production_promoted"
    PRODUCTION_HEALTH_CHECK_RECORDED = "production_health_check_recorded"
    PRODUCTION_INCIDENT_RECORDED = "production_incident_recorded"
    FEEDBACK_SYNTHESIZED = "feedback_synthesized"
    RETRY_BUDGET_EXHAUSTED = "retry_budget_exhausted"


TERMINAL_STATES = {
    ControllerState.WATCHLISTED,
    ControllerState.REJECTED,
    ControllerState.DEAD_LETTER,
}


TRANSITIONS: dict[ControllerState, dict[ControllerEvent, ControllerState]] = {
    ControllerState.DETECTED: {
        ControllerEvent.CHANGELOG_ITEM_RECORDED: ControllerState.SCREENING,
    },
    ControllerState.SCREENING: {
        ControllerEvent.RELEVANCE_WATCHLIST: ControllerState.WATCHLISTED,
        ControllerEvent.RELEVANCE_REJECTED: ControllerState.REJECTED,
        ControllerEvent.RELEVANCE_ACCEPTED: ControllerState.SPEC_DRAFTING,
    },
    ControllerState.SPEC_DRAFTING: {
        ControllerEvent.SPEC_PACKET_VALID: ControllerState.SPEC_READY,
    },
    ControllerState.SPEC_READY: {
        ControllerEvent.POLICY_DECISION_WRITTEN: ControllerState.POLICY_ASSIGNED,
    },
    ControllerState.POLICY_ASSIGNED: {
        ControllerEvent.TICKET_GENERATION_STARTED: ControllerState.TICKETING,
    },
    ControllerState.TICKETING: {
        ControllerEvent.TICKET_BUNDLE_VALID: ControllerState.TICKETED,
    },
    ControllerState.TICKETED: {
        ControllerEvent.BUILD_SLOT_RESERVED: ControllerState.BUILD_READY,
    },
    ControllerState.BUILD_READY: {
        ControllerEvent.BUILDER_STARTED: ControllerState.BUILDING,
    },
    ControllerState.BUILDING: {
        ControllerEvent.PR_CREATED: ControllerState.PR_OPEN,
    },
    ControllerState.PR_OPEN: {
        ControllerEvent.REVIEWER_STARTED: ControllerState.REVIEWING,
    },
    ControllerState.REVIEWING: {
        ControllerEvent.BLOCKING_FINDINGS_PRESENT: ControllerState.PR_REVISION,
        ControllerEvent.REVIEWABLE_TRUE: ControllerState.PR_REVIEWABLE,
    },
    ControllerState.PR_REVISION: {
        ControllerEvent.BUILDER_RETRY_STARTED: ControllerState.BUILDING,
    },
    ControllerState.PR_REVIEWABLE: {
        ControllerEvent.REQUIRED_EVAL_TIER_PASSED: ControllerState.PR_MERGEABLE,
        ControllerEvent.REQUIRED_EVAL_TIER_FAILED: ControllerState.PR_REVISION,
    },
    ControllerState.PR_MERGEABLE: {
        ControllerEvent.SECURITY_REVIEW_STARTED: ControllerState.SECURITY_REVIEWING,
    },
    ControllerState.SECURITY_REVIEWING: {
        ControllerEvent.SECURITY_FINDINGS_PRESENT: ControllerState.PR_REVISION,
        ControllerEvent.SECURITY_SIGNOFF_GRANTED: ControllerState.SECURITY_APPROVED,
    },
    ControllerState.SECURITY_APPROVED: {
        ControllerEvent.MERGE_STARTED: ControllerState.MERGE_REVIEWING,
        ControllerEvent.STAGING_SOAK_STARTED: ControllerState.STAGING_SOAK,
    },
    ControllerState.MERGE_REVIEWING: {
        ControllerEvent.MERGE_BLOCKED: ControllerState.PR_REVISION,
        ControllerEvent.PR_MERGED: ControllerState.MERGED,
    },
    ControllerState.MERGED: {
        ControllerEvent.STAGING_SOAK_STARTED: ControllerState.STAGING_SOAK,
    },
    ControllerState.STAGING_SOAK: {
        ControllerEvent.STAGING_SOAK_FAILED: ControllerState.PR_REVISION,
        ControllerEvent.PRODUCTION_PROMOTED: ControllerState.PRODUCTION_MONITORING,
    },
    ControllerState.PRODUCTION_MONITORING: {
        ControllerEvent.PRODUCTION_HEALTH_CHECK_RECORDED: ControllerState.PRODUCTION_MONITORING,
        ControllerEvent.PRODUCTION_INCIDENT_RECORDED: ControllerState.PRODUCTION_MONITORING,
        ControllerEvent.FEEDBACK_SYNTHESIZED: ControllerState.PRODUCTION_MONITORING,
    },
}


class InvalidTransitionError(ValueError):
    """Raised when the controller receives an invalid event for the current state."""


@dataclass(slots=True)
class TransitionRecord:
    event: str
    from_state: str
    to_state: str
    artifact_id: str | None
    occurred_at: str


@dataclass(slots=True)
class WorkItem:
    work_item_id: str
    source_provider: str
    source_external_id: str
    title: str
    state: ControllerState
    risk_score: int | None = None
    execution_lane: str | None = None
    policy_decision_id: str | None = None
    current_artifact_id: str | None = None
    attempt_count: int = 0
    dead_letter_reason: str | None = None
    created_at: str = field(default_factory=utc_now)
    updated_at: str = field(default_factory=utc_now)
    history: list[TransitionRecord] = field(default_factory=list)

    @classmethod
    def from_document(cls, document: dict[str, Any]) -> "WorkItem":
        history_records: list[TransitionRecord] = []
        for raw_record in document.get("history", []):
            history_records.append(
                TransitionRecord(
                    event=raw_record["event"],
                    from_state=raw_record["from_state"],
                    to_state=raw_record["to_state"],
                    artifact_id=raw_record.get("artifact_id"),
                    occurred_at=raw_record["occurred_at"],
                )
            )
        return cls(
            work_item_id=document["work_item_id"],
            source_provider=document["source_provider"],
            source_external_id=document["source_external_id"],
            title=document["title"],
            state=ControllerState(document["state"]),
            risk_score=document.get("risk_score"),
            execution_lane=document.get("execution_lane"),
            policy_decision_id=document.get("policy_decision_id"),
            current_artifact_id=document.get("current_artifact_id"),
            attempt_count=document["attempt_count"],
            dead_letter_reason=document.get("dead_letter_reason"),
            created_at=document["created_at"],
            updated_at=document["updated_at"],
            history=history_records,
        )

    def to_document(self) -> dict[str, Any]:
        return {
            "work_item_id": self.work_item_id,
            "source_provider": self.source_provider,
            "source_external_id": self.source_external_id,
            "title": self.title,
            "state": self.state.value,
            "risk_score": self.risk_score,
            "execution_lane": self.execution_lane,
            "policy_decision_id": self.policy_decision_id,
            "current_artifact_id": self.current_artifact_id,
            "attempt_count": self.attempt_count,
            "dead_letter_reason": self.dead_letter_reason,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
        }


class FactoryController:
    """Small runtime scaffold for the first controller state machine."""

    def create_work_item(
        self,
        *,
        source_provider: str,
        source_external_id: str,
        title: str,
        work_item_id: str,
        created_at: str | None = None,
    ) -> WorkItem:
        timestamp = created_at or utc_now()
        return WorkItem(
            work_item_id=work_item_id,
            source_provider=source_provider,
            source_external_id=source_external_id,
            title=title,
            state=ControllerState.DETECTED,
            created_at=timestamp,
            updated_at=timestamp,
        )

    def apply_event(
        self,
        work_item: WorkItem,
        *,
        event: ControllerEvent,
        artifact_id: str | None = None,
        occurred_at: str | None = None,
        risk_score: int | None = None,
        execution_lane: str | None = None,
        policy_decision_id: str | None = None,
        dead_letter_reason: str | None = None,
    ) -> WorkItem:
        next_state = TRANSITIONS.get(work_item.state, {}).get(event)
        if next_state is None and event is ControllerEvent.RETRY_BUDGET_EXHAUSTED:
            if work_item.state not in TERMINAL_STATES:
                next_state = ControllerState.DEAD_LETTER
        if next_state is None:
            raise InvalidTransitionError(
                f"Cannot apply {event.value} while in {work_item.state.value}"
            )

        timestamp = occurred_at or utc_now()
        from_state = work_item.state
        work_item.state = next_state
        work_item.updated_at = timestamp

        if event in {ControllerEvent.BUILDER_STARTED, ControllerEvent.BUILDER_RETRY_STARTED}:
            work_item.attempt_count += 1

        if artifact_id is not None:
            work_item.current_artifact_id = artifact_id
        if risk_score is not None:
            work_item.risk_score = risk_score
        if execution_lane is not None:
            work_item.execution_lane = execution_lane
        if policy_decision_id is not None:
            work_item.policy_decision_id = policy_decision_id
        if next_state is ControllerState.DEAD_LETTER:
            work_item.dead_letter_reason = dead_letter_reason or event.value

        work_item.history.append(
            TransitionRecord(
                event=event.value,
                from_state=from_state.value,
                to_state=next_state.value,
                artifact_id=artifact_id,
                occurred_at=timestamp,
            )
        )
        return work_item

    @staticmethod
    def _load_json(path: Path) -> dict[str, Any]:
        with path.open("r", encoding="utf-8") as handle:
            return json.load(handle)

    @staticmethod
    def _load_optional_json(path: Path) -> dict[str, Any] | None:
        if not path.exists():
            return None
        with path.open("r", encoding="utf-8") as handle:
            return json.load(handle)

    def replay_scenario(self, scenario_dir: Path) -> WorkItem:
        base = scenario_dir.resolve()
        work_item_document = self._load_json(base / "work-item.json")
        work_item = self.create_work_item(
            source_provider=work_item_document["source_provider"],
            source_external_id=work_item_document["source_external_id"],
            title=work_item_document["title"],
            work_item_id=work_item_document["work_item_id"],
            created_at=work_item_document["created_at"],
        )
        work_item.updated_at = work_item_document["created_at"]

        self.apply_event(
            work_item,
            event=ControllerEvent.CHANGELOG_ITEM_RECORDED,
            occurred_at=work_item_document["created_at"],
        )

        policy_decision = self._load_json(base / "policy-decision.json")
        decision = policy_decision["decision"]
        policy_artifact_id = policy_decision["artifact"]["id"]

        if decision == "watchlist":
            self.apply_event(
                work_item,
                event=ControllerEvent.RELEVANCE_WATCHLIST,
                artifact_id=policy_artifact_id,
                occurred_at=policy_decision["artifact"]["updated_at"],
                risk_score=policy_decision["risk_score"],
                policy_decision_id=policy_artifact_id,
            )
            return work_item

        if decision == "ignore":
            self.apply_event(
                work_item,
                event=ControllerEvent.RELEVANCE_REJECTED,
                artifact_id=policy_artifact_id,
                occurred_at=policy_decision["artifact"]["updated_at"],
                risk_score=policy_decision["risk_score"],
                policy_decision_id=policy_artifact_id,
            )
            return work_item

        spec_packet = self._load_json(base / "spec-packet.json")
        ticket_bundle = self._load_optional_json(base / "ticket-bundle.json")
        pr_packet = self._load_optional_json(base / "pr-packet.json")
        eval_report = self._load_optional_json(base / "eval-report.json")
        security_review = self._load_optional_json(base / "security-review.json")
        merge_decision = self._load_optional_json(base / "merge-decision.json")
        promotion_decision = self._load_optional_json(base / "promotion-decision.json")
        monitoring_report = self._load_optional_json(base / "monitoring-report.json")
        feedback_report = self._load_optional_json(base / "feedback-report.json")

        self.apply_event(
            work_item,
            event=ControllerEvent.RELEVANCE_ACCEPTED,
            occurred_at=spec_packet["artifact"]["created_at"],
            risk_score=policy_decision["risk_score"],
        )
        self.apply_event(
            work_item,
            event=ControllerEvent.SPEC_PACKET_VALID,
            artifact_id=spec_packet["artifact"]["id"],
            occurred_at=spec_packet["artifact"]["updated_at"],
        )
        self.apply_event(
            work_item,
            event=ControllerEvent.POLICY_DECISION_WRITTEN,
            artifact_id=policy_artifact_id,
            occurred_at=policy_decision["artifact"]["updated_at"],
            policy_decision_id=policy_artifact_id,
            execution_lane=policy_decision["lane_assignment"]["lane"],
        )
        if ticket_bundle is None:
            return work_item

        self.apply_event(
            work_item,
            event=ControllerEvent.TICKET_GENERATION_STARTED,
            occurred_at=ticket_bundle["artifact"]["created_at"],
        )
        self.apply_event(
            work_item,
            event=ControllerEvent.TICKET_BUNDLE_VALID,
            artifact_id=ticket_bundle["artifact"]["id"],
            occurred_at=ticket_bundle["artifact"]["updated_at"],
        )
        if pr_packet is None:
            return work_item

        self.apply_event(
            work_item,
            event=ControllerEvent.BUILD_SLOT_RESERVED,
            occurred_at=pr_packet["artifact"]["created_at"],
        )
        self.apply_event(
            work_item,
            event=ControllerEvent.BUILDER_STARTED,
            occurred_at=pr_packet["artifact"]["created_at"],
        )
        self.apply_event(
            work_item,
            event=ControllerEvent.PR_CREATED,
            artifact_id=pr_packet["artifact"]["id"],
            occurred_at=pr_packet["artifact"]["updated_at"],
        )
        review_timestamp = (
            pr_packet["artifact"]["created_at"]
            if eval_report is not None
            else pr_packet["artifact"]["updated_at"]
        )
        self.apply_event(
            work_item,
            event=ControllerEvent.REVIEWER_STARTED,
            occurred_at=review_timestamp,
        )

        if eval_report is None and pr_packet["reviewer_report"]["blocking_findings"]:
            self.apply_event(
                work_item,
                event=ControllerEvent.BLOCKING_FINDINGS_PRESENT,
                artifact_id=pr_packet["artifact"]["id"],
                occurred_at=review_timestamp,
            )
            return work_item

        self.apply_event(
            work_item,
            event=ControllerEvent.REVIEWABLE_TRUE,
            artifact_id=pr_packet["artifact"]["id"],
            occurred_at=review_timestamp,
        )

        if eval_report is not None:
            event = (
                ControllerEvent.REQUIRED_EVAL_TIER_PASSED
                if eval_report["summary"]["merge_gate_passed"]
                else ControllerEvent.REQUIRED_EVAL_TIER_FAILED
            )
            self.apply_event(
                work_item,
                event=event,
                artifact_id=eval_report["artifact"]["id"],
                occurred_at=eval_report["artifact"]["updated_at"],
            )
            if security_review is None:
                return work_item

            self.apply_event(
                work_item,
                event=ControllerEvent.SECURITY_REVIEW_STARTED,
                artifact_id=security_review["artifact"]["id"],
                occurred_at=security_review["artifact"]["created_at"],
            )
            signoff_status = security_review["signoff"]["status"]
            if signoff_status == "blocked":
                self.apply_event(
                    work_item,
                    event=ControllerEvent.SECURITY_FINDINGS_PRESENT,
                    artifact_id=security_review["artifact"]["id"],
                    occurred_at=security_review["artifact"]["updated_at"],
                )
            elif signoff_status == "approved":
                self.apply_event(
                    work_item,
                    event=ControllerEvent.SECURITY_SIGNOFF_GRANTED,
                    artifact_id=security_review["artifact"]["id"],
                    occurred_at=security_review["artifact"]["updated_at"],
                )
            if merge_decision is not None:
                self.apply_event(
                    work_item,
                    event=ControllerEvent.MERGE_STARTED,
                    artifact_id=merge_decision["artifact"]["id"],
                    occurred_at=merge_decision["artifact"]["created_at"],
                )
                merge_status = merge_decision["merge_decision"]["status"]
                if merge_status == "blocked":
                    self.apply_event(
                        work_item,
                        event=ControllerEvent.MERGE_BLOCKED,
                        artifact_id=merge_decision["artifact"]["id"],
                        occurred_at=merge_decision["artifact"]["updated_at"],
                    )
                elif merge_status == "merged":
                    self.apply_event(
                        work_item,
                        event=ControllerEvent.PR_MERGED,
                        artifact_id=merge_decision["artifact"]["id"],
                        occurred_at=merge_decision["artifact"]["updated_at"],
                    )
            if promotion_decision is None:
                return work_item

            self.apply_event(
                work_item,
                event=ControllerEvent.STAGING_SOAK_STARTED,
                artifact_id=promotion_decision["artifact"]["id"],
                occurred_at=promotion_decision["artifact"]["created_at"],
            )
            decision_status = promotion_decision["promotion_decision"]["status"]
            if decision_status == "blocked":
                self.apply_event(
                    work_item,
                    event=ControllerEvent.STAGING_SOAK_FAILED,
                    artifact_id=promotion_decision["artifact"]["id"],
                    occurred_at=promotion_decision["artifact"]["updated_at"],
                )
            elif decision_status == "promoted":
                self.apply_event(
                    work_item,
                    event=ControllerEvent.PRODUCTION_PROMOTED,
                    artifact_id=promotion_decision["artifact"]["id"],
                    occurred_at=promotion_decision["artifact"]["updated_at"],
                )
            if monitoring_report is None:
                return work_item
            monitoring_event = (
                ControllerEvent.PRODUCTION_HEALTH_CHECK_RECORDED
                if monitoring_report["monitoring_decision"]["status"] == "healthy"
                else ControllerEvent.PRODUCTION_INCIDENT_RECORDED
            )
            self.apply_event(
                work_item,
                event=monitoring_event,
                artifact_id=monitoring_report["artifact"]["id"],
                occurred_at=monitoring_report["artifact"]["updated_at"],
            )
            if feedback_report is not None:
                self.apply_event(
                    work_item,
                    event=ControllerEvent.FEEDBACK_SYNTHESIZED,
                    artifact_id=feedback_report["artifact"]["id"],
                    occurred_at=feedback_report["artifact"]["updated_at"],
                )
            return work_item

        if pr_packet["merge_readiness"]["mergeable"]:
            self.apply_event(
                work_item,
                event=ControllerEvent.REQUIRED_EVAL_TIER_PASSED,
                occurred_at=work_item_document["updated_at"],
            )
        return work_item
