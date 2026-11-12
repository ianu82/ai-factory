"""Priority queues with requester turns that survive scheduler ticks (not restarts)."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True)
class Candidate:
    id: str
    workspace_id: str
    priority: int
    requester: str | None
    created_at: datetime
    # Within one priority, resumptions (0) precede new delivery work (1).
    dispatch_class: int = 0
    # CI-only waits have already released their execution slot. Their ready
    # continuations get the next eligible slot, even ahead of urgent fresh work.
    ci_continuation: bool = False
    # A task's outcome: within one priority, class and requester, the oldest outcome's work goes first, so outcomes
    # finish before more start. Planning candidates leave it unset (they are the outcome).
    outcome_created_at: datetime | None = None

    def age(self) -> tuple[datetime, datetime, str]:
        return self.outcome_created_at or self.created_at, self.created_at, self.id


class RequesterTurns:
    def __init__(self) -> None:
        self.last_served: dict[int, tuple[bool, str]] = {}

    @staticmethod
    def _requester(candidate: Candidate) -> tuple[bool, str]:
        # Null is one shared bucket, distinct from every real requester ID.
        return candidate.requester is not None, candidate.requester or ""

    def choose(self, eligible: list[Candidate]) -> Candidate:
        """Choose without consuming a turn; the caller records only actual dispatches."""
        continuations = [c for c in eligible if c.ci_continuation]
        if continuations:
            eligible = continuations
        rank = min((c.priority, c.dispatch_class) for c in eligible)
        heads: dict[tuple[bool, str], Candidate] = {}
        for candidate in eligible:
            if (candidate.priority, candidate.dispatch_class) != rank:
                continue
            requester = self._requester(candidate)
            previous = heads.get(requester)
            if previous is None or candidate.age() < previous.age():
                heads[requester] = candidate
        requesters = sorted(heads)
        last = self.last_served.get(rank[0])
        # A stable requester ring also works when the last requester has no eligible work.
        next_requester = next((r for r in requesters if last is None or r > last), requesters[0])
        return heads[next_requester]

    def dispatched(self, candidate: Candidate) -> None:
        self.last_served[candidate.priority] = self._requester(candidate)
