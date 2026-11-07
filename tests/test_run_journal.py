from concurrent.futures import ThreadPoolExecutor

import pytest

from auto_mindsdb_factory.run_journal import JournalError, RunJournal


def test_journal_preserves_failed_attempts_and_usage(tmp_path):
    journal = RunJournal(tmp_path, "task-1")
    journal.create({"task": "fix", "base_sha": "a" * 40, "profile_sha": "b" * 64})
    for status in ["failed", "passed"]:
        journal.append("attempt_started")
        journal.append("model_response", response_id=status, usage={"total_tokens": 50})
        journal.append("attempt_finished", status=status)
    journal.append("run_finished", status="complete")
    summary = RunJournal(tmp_path, "task-1").summary()
    assert summary["attempts"] == 2
    assert summary["total_tokens"] == 100
    assert summary["cost_usd"] is None
    assert summary["status"] == "complete"
    with pytest.raises(JournalError, match="already exists"):
        journal.create({"task": "replacement"})


def test_append_serializes_independent_writers(tmp_path):
    journal = RunJournal(tmp_path, "task-2")
    journal.create({})
    with ThreadPoolExecutor(max_workers=4) as pool:
        list(pool.map(lambda n: RunJournal(tmp_path, "task-2").append("tool", number=n), range(16)))
    assert len(journal.events()) == 16
    assert {event["data"]["number"] for event in journal.events()} == set(range(16))


def test_corrupt_or_missing_events_fail_closed(tmp_path):
    journal = RunJournal(tmp_path, "task-3")
    journal.create({})
    journal.append("first")
    journal.append("second")
    (journal.root / "event-000001.json").unlink()
    with pytest.raises(JournalError):
        journal.append("must-not-hide-gap")


def test_missing_usage_is_explicit(tmp_path):
    journal = RunJournal(tmp_path, "task-4")
    journal.append("model_response", usage=None)
    assert not journal.summary()["usage_complete"]
    assert journal.summary()["status"] == "incomplete"


@pytest.mark.parametrize("run_id", ["..", "a/b", "/absolute", "", "x" * 100])
def test_run_ids_cannot_escape_store(tmp_path, run_id):
    with pytest.raises(JournalError):
        RunJournal(tmp_path, run_id)


def test_read_only_open_and_mutators_never_create_evidence(tmp_path):
    store = tmp_path / "store"
    with pytest.raises(JournalError, match="does not exist"):
        RunJournal(store, "task", read_only=True)
    assert not store.exists()
    writable = RunJournal(store, "task")
    readonly = RunJournal(store, "task", read_only=True)
    assert readonly.summary() == writable.summary()
    with pytest.raises(JournalError, match="read-only"):
        readonly.create({})
    with pytest.raises(JournalError, match="read-only"):
        readonly.append("event")
    assert list(writable.root.iterdir()) == []
    writable.append("event")
    assert readonly.events() == writable.events()


def test_gateway_unknown_coverage_preserves_observed_tokens_and_cost(tmp_path):
    journal = RunJournal(tmp_path, "gateway")
    journal.append("model_requested", role="builder")
    journal.append("model_response", role="builder", usage={"total_tokens": 50}, cost_estimate_usd=0.01)
    assert journal.summary()["usage_complete"] and journal.summary()["cost_complete"]
    journal.append("inference_usage_unknown", role="builder", reason="gateway_attempt_coverage")
    summary = journal.summary()
    assert summary["total_tokens"] == 50 and summary["known_inference_cost_usd"] == 0.01
    assert not summary["usage_complete"] and not summary["cost_complete"]
    assert summary["cost_usd"] is None
