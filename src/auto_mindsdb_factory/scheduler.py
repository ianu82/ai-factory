from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any

from .reliability import queue_priority


RUNNABLE_QUEUES = frozenset({"new_build", "eval", "revision"})
NON_RUNNABLE_QUEUES = frozenset({"blocked", "dead_letter", "complete"})


@dataclass(frozen=True, slots=True)
class SchedulerCandidate:
    work_item_id: str
    stage_name: str
    queue_status: str
    updated_at: datetime
    payload: Any


@dataclass(frozen=True, slots=True)
class SchedulerDecision:
    work_item_id: str
    stage_name: str
    queue_status: str
    action: str
    reason: str | None
    payload: Any

    @property
    def should_run(self) -> bool:
        return self.action == "run"


class FactoryScheduler:
    """Build a deterministic, fair execution plan for persisted factory runs."""

    def __init__(self, *, max_active_runs: int | None = None) -> None:
        if max_active_runs is not None and max_active_runs < 1:
            raise ValueError("max_active_runs must be >= 1 when provided.")
        self.max_active_runs = max_active_runs

    def plan(self, candidates: list[SchedulerCandidate]) -> list[SchedulerDecision]:
        decisions: list[SchedulerDecision] = []
        scheduled_runs = 0
        for candidate in sorted(candidates, key=self._sort_key):
            if candidate.queue_status in NON_RUNNABLE_QUEUES:
                decisions.append(
                    self._decision(
                        candidate,
                        action="skip",
                        reason=None,
                    )
                )
                continue

            if candidate.queue_status not in RUNNABLE_QUEUES:
                decisions.append(
                    self._decision(
                        candidate,
                        action="skip",
                        reason="unknown_queue_status",
                    )
                )
                continue

            if self.max_active_runs is not None and scheduled_runs >= self.max_active_runs:
                decisions.append(
                    self._decision(
                        candidate,
                        action="skip",
                        reason="max_active_runs_reached",
                    )
                )
                continue

            scheduled_runs += 1
            decisions.append(self._decision(candidate, action="run", reason=None))
        return decisions

    @staticmethod
    def _sort_key(candidate: SchedulerCandidate) -> tuple[int, datetime, str]:
        return (
            queue_priority(candidate.queue_status),
            candidate.updated_at,
            candidate.work_item_id,
        )

    @staticmethod
    def _decision(
        candidate: SchedulerCandidate,
        *,
        action: str,
        reason: str | None,
    ) -> SchedulerDecision:
        return SchedulerDecision(
            work_item_id=candidate.work_item_id,
            stage_name=candidate.stage_name,
            queue_status=candidate.queue_status,
            action=action,
            reason=reason,
            payload=candidate.payload,
        )
