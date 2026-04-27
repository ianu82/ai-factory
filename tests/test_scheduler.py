from __future__ import annotations

from datetime import datetime, timezone

import pytest

from auto_mindsdb_factory.scheduler import FactoryScheduler, SchedulerCandidate


def _candidate(work_item_id: str, queue_status: str, minute: int) -> SchedulerCandidate:
    return SchedulerCandidate(
        work_item_id=work_item_id,
        stage_name="stage",
        queue_status=queue_status,
        updated_at=datetime(2026, 4, 26, 12, minute, tzinfo=timezone.utc),
        payload={"id": work_item_id},
    )


def test_scheduler_prioritizes_new_build_eval_then_revision() -> None:
    decisions = FactoryScheduler(max_active_runs=3).plan(
        [
            _candidate("revision", "revision", 0),
            _candidate("eval", "eval", 0),
            _candidate("new", "new_build", 0),
        ]
    )

    assert [decision.work_item_id for decision in decisions] == ["new", "eval", "revision"]
    assert [decision.action for decision in decisions] == ["run", "run", "run"]


def test_scheduler_applies_slot_limit_only_to_runnable_work() -> None:
    decisions = FactoryScheduler(max_active_runs=1).plan(
        [
            _candidate("complete", "complete", 0),
            _candidate("new", "new_build", 1),
            _candidate("revision", "revision", 2),
        ]
    )

    by_id = {decision.work_item_id: decision for decision in decisions}
    assert by_id["new"].should_run is True
    assert by_id["revision"].reason == "max_active_runs_reached"
    assert by_id["complete"].reason is None


def test_scheduler_orders_same_queue_by_age_then_work_item_id() -> None:
    decisions = FactoryScheduler(max_active_runs=None).plan(
        [
            _candidate("b", "new_build", 2),
            _candidate("a", "new_build", 2),
            _candidate("old", "new_build", 1),
        ]
    )

    assert [decision.work_item_id for decision in decisions] == ["old", "a", "b"]


def test_scheduler_rejects_invalid_slot_limit() -> None:
    with pytest.raises(ValueError, match="max_active_runs"):
        FactoryScheduler(max_active_runs=0)
