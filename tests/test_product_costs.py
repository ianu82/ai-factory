import pytest

from auto_mindsdb_factory.product_costs import estimate_cost
from auto_mindsdb_factory.product_outcomes import outcome_metrics
from auto_mindsdb_factory.run_journal import RunJournal


RATE = {"input": 2.5, "cached_input": 0.25, "output": 15, "max_input_tokens": 272000}


def test_estimates_separate_cached_tokens_and_decline_unknown_regimes():
    usage = {"input_tokens": 1000, "output_tokens": 100, "input_tokens_details": {"cached_tokens": 500}}
    assert estimate_cost(usage, RATE) == 0.002875
    assert estimate_cost({**usage, "input_tokens": 300000}, RATE) is None
    assert estimate_cost(usage, None) is None
    assert estimate_cost({**usage, "input_tokens": -1}, RATE) is None


def test_outcomes_include_failed_cost_and_exclude_replay(tmp_path):
    for name, purpose, status, cost, duration in [
        ("accepted", "delivery", "complete", 0.2, 20),
        ("failed", "delivery", "blocked", 0.3, 10),
        ("replay", "replay", "verified", 5.0, 100),
    ]:
        j = RunJournal(tmp_path, name)
        j.create({"purpose": purpose})
        j.append("model_response", usage={"total_tokens": 100}, cost_estimate_usd=cost)
        j.append("run_finished", status=status, duration_seconds=duration)
    metrics = outcome_metrics(tmp_path)
    assert metrics["accepted_work_items"] == 1
    assert metrics["work_items"] == 2
    assert metrics["inference_cost_per_accepted_item_usd"] == 0.5
    assert metrics["completion_latency_p90_seconds"] == 20
    RunJournal(tmp_path, "failed").append("model_failed")
    metrics = outcome_metrics(tmp_path)
    assert metrics["unpriced_work_items"] == 1
    assert metrics["inference_cost_per_accepted_item_usd"] is None


def test_inflight_requests_do_not_appear_free(tmp_path):
    journal = RunJournal(tmp_path, "inflight")
    journal.create({})
    journal.append("model_requested")
    assert journal.summary()["usage_complete"] is False
    assert journal.summary()["cost_usd"] is None
