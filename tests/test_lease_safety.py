import json
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path

import auto_mindsdb_factory.automation as automation
from auto_mindsdb_factory.automation import FactoryRunStore, RunLeaseBusyError


def test_lease_uses_full_clock_precision(tmp_path, monkeypatch):
    now = datetime(2026, 11, 4, 12, 0, 0, 850000, tzinfo=timezone.utc)
    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return now
    monkeypatch.setattr(automation, "datetime", Clock)
    monkeypatch.setattr(automation, "utc_now", lambda: "2026-11-04T12:00:00Z")
    store = FactoryRunStore(tmp_path, repo_root_override=Path(__file__).resolve().parents[1])
    with store.run_lease("precision", ttl_seconds=0.2, renew_interval_seconds=0.05) as lease:
        document = json.loads(lease.lock_path.read_text())
        assert (datetime.fromisoformat(document["expires_at"].replace("Z", "+00:00")) - now).total_seconds() == 0.2


def test_contender_cannot_steal_a_lock_while_json_is_being_written(tmp_path, monkeypatch):
    store = FactoryRunStore(tmp_path, repo_root_override=Path(__file__).resolve().parents[1])
    writing = threading.Event()
    real_dump = json.dump
    def slow_dump(document, handle, **kwargs):
        writing.set()
        time.sleep(0.05)
        real_dump(document, handle, **kwargs)
    monkeypatch.setattr(automation.json, "dump", slow_dump)
    def acquire():
        try:
            with store.run_lease("contended", ttl_seconds=5):
                time.sleep(0.2)
                return "acquired"
        except RunLeaseBusyError:
            return "busy"
    with ThreadPoolExecutor(max_workers=2) as pool:
        first = pool.submit(acquire)
        assert writing.wait(timeout=5)
        second = pool.submit(acquire)
        assert [first.result(), second.result()] == ["acquired", "busy"]
