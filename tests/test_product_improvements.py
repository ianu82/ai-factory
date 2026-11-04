from datetime import datetime, timedelta, timezone

from auto_mindsdb_factory.product_improvements import propose_improvements
from auto_mindsdb_factory.run_journal import RunJournal


def failed_run(store, name, purpose="delivery"):
    j = RunJournal(store, name)
    j.create({"purpose": purpose, "profile_sha": "profile-1", "profile": {"repository": "owner/product"}, "task": {"id": name}})
    for _ in range(2):
        j.append("verification_check", name="unit", command=["npm", "test"], exit_code=1)
    j.append("run_finished", status="blocked")


def test_proposals_count_distinct_work_items_and_exclude_recursive_work(tmp_path):
    now = datetime(2026, 11, 4, tzinfo=timezone.utc)
    failed_run(tmp_path, "one")
    for purpose in ["replay", "improvement"]:
        for n in range(4):
            failed_run(tmp_path, f"{purpose}-{n}", purpose)
    assert propose_improvements(tmp_path, now=now) == []
    failed_run(tmp_path, "two")
    failed_run(tmp_path, "three")
    proposals = propose_improvements(tmp_path, now=now)
    assert len(proposals) == 1
    assert proposals[0]["source_runs"] == ["one", "three", "two"]
    assert proposals[0]["status"] == "proposed"
    assert propose_improvements(tmp_path, now=now + timedelta(days=2)) == []
    failed_run(tmp_path, "four")
    assert propose_improvements(tmp_path, now=now + timedelta(hours=1)) == []
    assert len(propose_improvements(tmp_path, now=now + timedelta(days=2))) == 1


def test_infrastructure_failures_are_not_grouped_as_product_defects(tmp_path):
    for n in range(3):
        failed_run(tmp_path, f"task-{n}")
        j = RunJournal(tmp_path, f"task-{n}")
        j.append("verification_check", name="timeout", command=["npm", "test"], exit_code=124)
    proposals = propose_improvements(tmp_path)
    assert len(proposals) == 1
    assert proposals[0]["failure"]["name"] == "unit"
