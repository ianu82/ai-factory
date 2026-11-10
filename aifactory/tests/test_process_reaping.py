"""Orphaned job processes are reaped, and the inventory shows zombies if they ever pile up again."""

from __future__ import annotations

import re
from pathlib import Path

from aifactory.sandbox import process_report

ROOT = Path(__file__).resolve().parents[1]


def test_image_runs_an_init_that_reaps_orphans_as_pid_1():
    # Without it the server is PID 1, never reaps what sandbox cleanup kills, and zombies fill the task limit.
    dockerfile = (ROOT / "Dockerfile").read_text()
    assert re.search(r"apt-get install[^\n]*\btini\b", dockerfile)
    assert 'ENTRYPOINT ["/usr/bin/tini", "--"]' in dockerfile
    assert dockerfile.rstrip().endswith('CMD ["aifactory", "serve"]')


def test_process_report_counts_zombies(tmp_path):
    for pid, stat in {"1": "1 (tini) S 0 1", "7": "7 (aifactory serve) S 1 7", "40": "40 (node) Z 7 40",
                      "41": "41 (py) test (x)) Z 7 41", "42": "42 (git) R 7 42"}.items():
        (tmp_path / pid).mkdir()
        (tmp_path / pid / "stat").write_text(stat)
    (tmp_path / "43").mkdir()
    (tmp_path / "43" / "stat").write_bytes(b"43 (\xff\xfe job) Z 7 43")  # a job may name its process anything
    (tmp_path / "self").mkdir()  # not a process directory
    (tmp_path / "99").mkdir()  # exited between listing and reading
    report = process_report(tmp_path)
    assert report["zombies"] == 3
    assert {"pids_current", "pids_max"} <= set(report)
