"""Fail-fast contention, connection-free telemetry and loop-safe scorer recovery."""
from __future__ import annotations

import asyncio
import sqlite3
import threading
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FutureTimeoutError
from contextlib import ExitStack
from time import perf_counter
from unittest.mock import Mock

import pytest
from fastapi import Depends
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, event, text
from sqlalchemy.exc import OperationalError, TimeoutError as PoolTimeoutError
from sqlalchemy.pool import NullPool, QueuePool

from aifactory import config, db, scorers
from aifactory.app import create_app


def configure_pool(monkeypatch, size=1, overflow=0):
    monkeypatch.setenv("AIFACTORY_DB_POOL_SIZE", str(size))
    monkeypatch.setenv("AIFACTORY_DB_MAX_OVERFLOW", str(overflow))
    config.reset_settings_cache()
    return db.init_engine()


def test_exhaustion_is_immediate_and_session_is_cleaned_up(monkeypatch):
    engine = configure_pool(monkeypatch, size=1, overflow=1)
    assert isinstance(engine.pool, QueuePool)
    assert engine.pool.timeout() == 0
    baseline = db.pool_snapshot()
    assert baseline == {"capacity": 2, "in_use": 0, "overflow": 0, "waiters": 0,
                        "saturated": False, "checkout_failures": 0}
    session = db.SessionLocal()
    rollback, close = Mock(wraps=session.rollback), Mock(wraps=session.close)
    monkeypatch.setattr(session, "rollback", rollback)
    monkeypatch.setattr(session, "close", close)
    monkeypatch.setattr(db, "SessionLocal", lambda: session)
    with ExitStack() as stack:
        for _ in range(2):
            stack.enter_context(engine.connect())
        assert db.pool_snapshot() == baseline | {"in_use": 2, "overflow": 1, "saturated": True}
        start = perf_counter()
        with pytest.raises(PoolTimeoutError):
            with db.session_scope() as s:
                s.execute(text("SELECT 1"))
        # Explicit regression bound for the former 30-second wait, not a scheduling deadline.
        assert perf_counter() - start < .1
        rollback.assert_called_once()
        close.assert_called_once()
        assert not session.in_transaction()
        assert db.pool_snapshot() == baseline | {
            "in_use": 2, "overflow": 1, "saturated": True, "checkout_failures": 1}
    assert db.pool_snapshot() == baseline | {"checkout_failures": 1}


def test_budget_covers_seventeen_tasks_and_background_workers(monkeypatch):
    monkeypatch.setenv("AIFACTORY_GLOBAL_CONCURRENCY", "17")
    monkeypatch.setenv("AIFACTORY_GLOBAL_PLANNING_CONCURRENCY", "4")
    monkeypatch.delenv("AIFACTORY_DB_POOL_SIZE", raising=False)
    monkeypatch.delenv("AIFACTORY_DB_MAX_OVERFLOW", raising=False)
    config.reset_settings_cache()
    engine = db.init_engine()
    assert db.pool_snapshot()["capacity"] == 37  # 17 execution + 4 planning + 4 each CI/scorer/background/HTTP
    with ExitStack() as stack:
        for _ in range(37):
            stack.enter_context(engine.connect())
        assert db.pool_snapshot()["in_use"] == 37
        with pytest.raises(PoolTimeoutError):
            engine.connect()
    assert db.pool_snapshot()["in_use"] == 0


@pytest.mark.parametrize("name,value", [
    ("AIFACTORY_DB_POOL_SIZE", "0"), ("AIFACTORY_DB_POOL_SIZE", "129"),
    ("AIFACTORY_DB_POOL_SIZE", "not-an-int"), ("AIFACTORY_DB_MAX_OVERFLOW", "-1"),
    ("AIFACTORY_DB_MAX_OVERFLOW", "33"), ("AIFACTORY_DB_MAX_OVERFLOW", "1.5"),
    ("AIFACTORY_SQLITE_BUSY_TIMEOUT_MS", "-1"), ("AIFACTORY_SQLITE_BUSY_TIMEOUT_MS", "5001"),
    ("AIFACTORY_SQLITE_BUSY_TIMEOUT_MS", "1.5"),
])
def test_pool_overrides_are_bounded(monkeypatch, name, value):
    monkeypatch.setenv(name, value)
    config.reset_settings_cache()
    with pytest.raises(ValueError, match=name):
        config.get_settings()


@pytest.mark.parametrize("size,overflow", [(1, 0), (128, 32)])
def test_valid_pool_overrides(monkeypatch, size, overflow):
    configure_pool(monkeypatch, size, overflow)
    assert db.pool_snapshot()["capacity"] == size + overflow


@pytest.mark.parametrize("fail", [False, True])
def test_waiters_measure_concurrent_acquisitions_and_cleanup(monkeypatch, fail):
    engine = configure_pool(monkeypatch, size=2)
    # Hold both acquisitions inside the creator, before a checkout event can fire.
    engine.dispose()
    entered = threading.Barrier(3)
    release = threading.Event()
    creator = engine.pool._creator

    def blocked_creator():
        entered.wait(timeout=5)
        assert release.wait(timeout=5)
        if fail:
            raise sqlite3.OperationalError("synthetic connect failure")
        return creator()

    monkeypatch.setattr(engine.pool, "_creator", blocked_creator)

    def acquire():
        with engine.connect():
            pass

    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = [executor.submit(acquire) for _ in range(2)]
        try:
            entered.wait(timeout=5)
            assert db.pool_snapshot()["waiters"] == 2
            assert db.pool_snapshot()["checkout_failures"] == 0
            with pytest.raises(PoolTimeoutError):
                engine.connect()
            assert db.pool_snapshot()["waiters"] == 2  # unsuccessful third acquisition is gone
        finally:
            release.set()
        for future in futures:
            if fail:
                with pytest.raises(OperationalError):
                    future.result(timeout=5)
            else:
                future.result(timeout=5)
    assert db.pool_snapshot()["waiters"] == 0
    assert db.pool_snapshot()["in_use"] == 0
    assert db.pool_snapshot()["checkout_failures"] == (3 if fail else 1)
    assert db.pool_snapshot()["overflow"] == 0


def test_snapshot_is_connection_free_and_resets_on_engine_replacement(monkeypatch):
    first = configure_pool(monkeypatch)
    with first.connect():
        with pytest.raises(PoolTimeoutError):
            first.connect()
        second = db.init_engine()
        assert db.pool_snapshot()["in_use"] == db.pool_snapshot()["checkout_failures"] == 0
    # Returning an old connection must not change the new pool's counters.
    assert db.pool_snapshot()["in_use"] == 0
    monkeypatch.setattr(second, "connect", Mock(side_effect=AssertionError("must not connect")))
    assert db.pool_snapshot()["capacity"] == 1
    monkeypatch.setattr(db, "_engine", None)
    monkeypatch.setattr(db, "init_engine", Mock(side_effect=AssertionError("must not initialize")))
    assert db.pool_snapshot()["capacity"] is None


@pytest.mark.parametrize("url", ["sqlite://", "sqlite:///:memory:"])
def test_in_memory_sqlite_remains_compatible(url):
    engine = db.init_engine(url)
    assert not isinstance(engine.pool, QueuePool)
    with db.session_scope() as s:
        assert s.execute(text("SELECT 1")).scalar_one() == 1
    assert db.pool_snapshot() == {"capacity": None, "in_use": None, "overflow": 0,
                                  "waiters": 0, "saturated": False, "checkout_failures": 0}


def test_non_queue_pool_snapshot(monkeypatch):
    engine = create_engine("sqlite://", poolclass=NullPool)
    try:
        monkeypatch.setattr(db, "_engine", engine)
        assert db.pool_snapshot()["capacity"] is None
        assert db.pool_snapshot()["waiters"] == 0
    finally:
        engine.dispose()


@pytest.mark.parametrize("busy_timeout_ms", [0, 100])
async def test_sqlite_writer_contention_is_bounded_and_loop_keeps_ticking(monkeypatch, tmp_path, busy_timeout_ms):
    # Always use an independent SQLite database, including in the Postgres CI job.
    monkeypatch.setenv("AIFACTORY_SQLITE_BUSY_TIMEOUT_MS", str(busy_timeout_ms))
    config.reset_settings_cache()
    engine = db.init_engine(f"sqlite:///{tmp_path / 'locked.db'}")
    with engine.begin() as conn:
        conn.exec_driver_sql("CREATE TABLE contention_probe (value INTEGER)")
        conn.exec_driver_sql("INSERT INTO contention_probe VALUES (0)")
    independent = sqlite3.connect(str(tmp_path / "locked.db"), timeout=0)
    independent.execute("UPDATE contention_probe SET value=1")
    ticks = 0
    finished = asyncio.Event()
    started = threading.Event()
    ticking = threading.Event()

    async def heartbeat():
        nonlocal ticks
        while not finished.is_set():
            if started.is_set():
                ticks += 1
                ticking.set()
            await asyncio.sleep(0)

    def competing_write():
        with pytest.raises(OperationalError) as error:
            with db.session_scope() as s:
                assert s.execute(text("PRAGMA busy_timeout")).scalar_one() == busy_timeout_ms
                started.set()
                assert ticking.wait(timeout=5)
                start = perf_counter()
                s.execute(text("UPDATE contention_probe SET value=2"))
        assert db.is_database_contention(error.value)
        elapsed = perf_counter() - start
        assert busy_timeout_ms / 1000 <= elapsed < busy_timeout_ms / 1000 + .5

    heartbeat_task = asyncio.create_task(heartbeat())
    try:
        await asyncio.to_thread(competing_write)
        assert ticks > 0
    finally:
        finished.set()
        await heartbeat_task
        independent.rollback()
        independent.close()
    assert db.pool_snapshot()["in_use"] == 0
    with engine.begin() as conn:
        conn.exec_driver_sql("UPDATE contention_probe SET value=3")


def test_sqlite_short_writer_overlap_succeeds(tmp_path):
    engine = db.init_engine(f"sqlite:///{tmp_path / 'short-write.db'}")
    assert config.get_settings().sqlite_busy_timeout_ms == 2000
    with engine.begin() as conn:
        conn.exec_driver_sql("CREATE TABLE contention_probe (value INTEGER)")
        conn.exec_driver_sql("INSERT INTO contention_probe VALUES (0)")
    independent = sqlite3.connect(str(tmp_path / "short-write.db"), timeout=0)
    independent.execute("UPDATE contention_probe SET value=1")
    started = threading.Event()

    def observe(conn, cursor, statement, parameters, context, executemany):
        if statement.startswith("UPDATE contention_probe"):
            started.set()

    def write():
        with db.session_scope() as s:
            assert s.execute(text("PRAGMA busy_timeout")).scalar_one() == 2000
            s.execute(text("UPDATE contention_probe SET value=2"))

    event.listen(engine, "before_cursor_execute", observe)
    try:
        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(write)
            try:
                assert started.wait(timeout=5)
                # The writer is genuinely waiting, not rejected at the first overlap.
                with pytest.raises(FutureTimeoutError):
                    future.result(timeout=.05)
            finally:
                independent.commit()
            future.result(timeout=5)
    finally:
        independent.close()
        event.remove(engine, "before_cursor_execute", observe)
    with engine.connect() as conn:
        assert conn.exec_driver_sql("SELECT value FROM contention_probe").scalar_one() == 2
    assert db.pool_snapshot()["in_use"] == 0


def test_api_pool_exhaustion_is_sanitized_after_dependency_cleanup(monkeypatch):
    async def parked_worker():
        await asyncio.Event().wait()

    monkeypatch.setattr(scorers, "worker", parked_worker)
    configure_pool(monkeypatch)
    app = create_app(start_orchestrator=False)

    def dependency():
        yield from db.get_db()

    @app.get("/contention-test")
    def query(session=Depends(dependency)):
        session.execute(text("SELECT 'sensitive-sql'"))

    # Routes added after create_app must precede its optional SPA catch-all.
    app.router.routes.insert(0, app.router.routes.pop())

    # Observe rollback/close even when lazy checkout fails inside the dependency's session.
    original = db.SessionLocal
    sessions = []

    def session_factory():
        s = original()
        s.rollback = Mock(wraps=s.rollback)
        s.close = Mock(wraps=s.close)
        sessions.append(s)
        return s

    monkeypatch.setattr(db, "SessionLocal", session_factory)
    with TestClient(app) as client:
        with db.engine().connect():
            response = client.get("/contention-test")
        assert response.status_code == 503
        assert response.headers["Retry-After"] == "1"
        assert response.json() == {"detail": "Database temporarily unavailable; please retry"}
        assert sessions[-1].rollback.call_count == sessions[-1].close.call_count == 1
        assert client.get("/contention-test").status_code == 200
    assert db.pool_snapshot()["in_use"] == 0


@pytest.mark.parametrize("message,status", [("database is locked", 503), ("database table is locked", 503),
                                            ("no such table: SECRET", 500)])
def test_api_only_maps_sqlite_contention(monkeypatch, message, status):
    app = create_app(start_orchestrator=False)

    @app.get("/database-error-test")
    def query():
        raise OperationalError("SELECT 'SECRET'", {"password": "SECRET"}, sqlite3.OperationalError(message))

    app.router.routes.insert(0, app.router.routes.pop())

    with TestClient(app, raise_server_exceptions=False) as client:
        response = client.get("/database-error-test")
        assert response.status_code == status
        assert "SECRET" not in response.text
        assert ("Retry-After" in response.headers) == (status == 503)


def test_sqlite_error_codes_do_not_mask_other_operational_errors():
    for code in (sqlite3.SQLITE_BUSY, sqlite3.SQLITE_LOCKED, sqlite3.SQLITE_BUSY | (2 << 8)):
        orig = sqlite3.OperationalError("driver-specific locked text")
        orig.sqlite_errorcode = code
        assert db.is_database_contention(OperationalError("", {}, orig))
    orig = sqlite3.OperationalError("database is locked")
    orig.sqlite_errorcode = sqlite3.SQLITE_ERROR
    assert not db.is_database_contention(OperationalError("", {}, orig))
    assert not db.is_database_contention(OperationalError("", {}, RuntimeError("database is locked")))


async def test_scorer_selection_is_offloaded_and_dispatch_stays_on_loop(monkeypatch):
    loop = asyncio.get_running_loop()
    loop_thread = threading.get_ident()
    selected, dispatched = threading.Event(), asyncio.Event()
    shutdown = asyncio.Event()
    calls = []

    def select():
        assert threading.get_ident() != loop_thread
        selected.set()
        return ["queued-result"]

    def dispatch(ids):
        assert asyncio.get_running_loop() is loop
        assert ids == ["queued-result"]
        calls.append(ids)
        dispatched.set()

    async def pause(_):
        await shutdown.wait()

    monkeypatch.setattr(scorers, "select_recovery", select)
    monkeypatch.setattr(scorers, "dispatch", dispatch)
    class WorkerAsyncio:
        sleep = staticmethod(pause)

        def __getattr__(self, name):
            return getattr(asyncio, name)

    monkeypatch.setattr(scorers, "asyncio", WorkerAsyncio())
    worker = asyncio.create_task(scorers.worker())
    try:
        await asyncio.wait_for(dispatched.wait(), 5)
        assert selected.is_set() and len(calls) == 1
    finally:
        worker.cancel()
        await asyncio.gather(worker, return_exceptions=True)
