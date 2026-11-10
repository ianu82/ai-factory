"""Job directories left by a restart are moved aside at startup and deleted in the background."""
from __future__ import annotations

from aifactory.sandbox import TRASH_PREFIX, LocalSandbox, disk_report, purge_quarantine, quarantine_orphaned_jobs


def test_orphaned_job_directories_are_quarantined_then_purged(tmp_path):
    jobs = tmp_path / "jobs"
    for name in ("task-abc-b1", "plan-def-2"):
        (jobs / name / "work" / "node_modules").mkdir(parents=True)
        (jobs / name / "work" / "node_modules" / "big.js").write_text("x" * 1000)
    trash = quarantine_orphaned_jobs(jobs)
    assert trash is not None and trash.name.startswith(TRASH_PREFIX)
    assert sorted(p.name for p in trash.iterdir()) == ["plan-def-2", "task-abc-b1"]
    assert [p.name for p in jobs.iterdir()] == [trash.name]
    (jobs / "task-abc-b1").mkdir()  # a resumed attempt starts in a fresh, empty directory
    assert not any((jobs / "task-abc-b1").iterdir())
    assert purge_quarantine(jobs) == 1
    assert [p.name for p in jobs.iterdir()] == ["task-abc-b1"]  # live jobs are never purged


def test_nothing_to_quarantine(tmp_path):
    assert quarantine_orphaned_jobs(tmp_path / "missing") is None
    (tmp_path / "jobs").mkdir()
    assert quarantine_orphaned_jobs(tmp_path / "jobs") is None
    assert purge_quarantine(tmp_path / "jobs") == 0


def test_disk_report_is_cheap_and_complete(tmp_path):
    report = disk_report(tmp_path)
    assert set(report) == {"used_gb", "total_gb", "used_percent", "job_dirs"}
    assert 0 <= report["used_percent"] <= 100


def test_open_sandboxes_are_never_quarantined(tmp_path):
    jobs = tmp_path / "jobs"
    live = LocalSandbox("task-live-b1", root=jobs).create()
    (jobs / "task-dead-b1").mkdir()
    try:
        trash = quarantine_orphaned_jobs(jobs)
        assert [p.name for p in trash.iterdir()] == ["task-dead-b1"]
        assert live.root.exists()
    finally:
        import asyncio
        asyncio.run(live.close())
    assert not live.root.exists()
