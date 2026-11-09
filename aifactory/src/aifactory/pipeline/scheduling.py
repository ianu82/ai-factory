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
    # Resumptions precede new delivery work, independently of outcome priority.
    dispatch_class: int = 0


class RequesterTurns:
    def __init__(self) -> None:
        self.last_served: dict[int, tuple[bool, str]] = {}

    @staticmethod
    def _requester(candidate: Candidate) -> tuple[bool, str]:
        # Null is one shared bucket, distinct from every real requester ID.
        return candidate.requester is not None, candidate.requester or ""

    def choose(self, eligible: list[Candidate]) -> Candidate:
        """Choose without consuming a turn; the caller records only actual dispatches."""
        rank = min((c.dispatch_class, c.priority) for c in eligible)
        heads: dict[tuple[bool, str], Candidate] = {}
        for candidate in eligible:
            if (candidate.dispatch_class, candidate.priority) != rank:
                continue
            requester = self._requester(candidate)
            previous = heads.get(requester)
            if previous is None or (candidate.created_at, candidate.id) < (previous.created_at, previous.id):
                heads[requester] = candidate
        requesters = sorted(heads)
        last = self.last_served.get(rank[1])
        # A stable requester ring also works when the last requester has no eligible work.
        next_requester = next((r for r in requesters if last is None or r > last), requesters[0])
        return heads[next_requester]

    def dispatched(self, candidate: Candidate) -> None:
        self.last_served[candidate.priority] = self._requester(candidate)
