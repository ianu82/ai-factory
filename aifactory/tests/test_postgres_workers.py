"""Disposable Postgres ownership and routing, also tested in the SQLite job with fake admin connections."""
from contextlib import contextmanager
from types import SimpleNamespace
from uuid import uuid4

import pytest
from sqlalchemy import create_engine, inspect
from sqlalchemy.dialects.postgresql import dialect
from sqlalchemy.engine import make_url

from aifactory import db
from .conftest import postgres_worker_database, test_database_url as database_url_fixture, worker_database_name


@pytest.mark.parametrize("run,worker", [("run-a", "gw0"), ('run/雪;"', 'gw0"; DROP DATABASE factory_test;--')])
def test_worker_names_are_bounded_and_distinct(run, worker):
    name = worker_database_name(run, worker)
    assert name.isascii() and len(name.encode()) <= 63
    assert name.replace("_", "").isalnum()
    assert name == worker_database_name(run, worker)
    assert name != worker_database_name(run + "other", worker)
    assert name != worker_database_name(run, worker + "other")
    assert worker_database_name("ab", "c") != worker_database_name("a", "bc")
    assert name != "factory_test"


@pytest.mark.parametrize("run,worker", [("", "gw0"), ("run", "")])
def test_missing_identity_is_rejected(run, worker):
    with pytest.raises(ValueError, match="identifier"):
        worker_database_name(run, worker)


@pytest.fixture
def admin(monkeypatch):
    calls = []
    engine = SimpleNamespace(dialect=dialect(), dispose=lambda: calls.append("dispose"))

    @contextmanager
    def connect():
        yield SimpleNamespace(exec_driver_sql=lambda sql: calls.append(sql))

    engine.connect = connect

    def create(url, **kwargs):
        calls.append((url, kwargs))
        return engine

    monkeypatch.setattr("sqlalchemy.create_engine", create)
    return engine, calls


@pytest.mark.parametrize("scheme", ["postgres", "postgresql", "postgresql+psycopg"])
def test_urls_preserve_credentials_and_options_and_cleanup_is_owned(admin, scheme):
    engine, calls = admin
    shared = f"{scheme}://us%40er:p%3Ass%2Fword@localhost:5433/factory_test?sslmode=require&application_name=tests"
    with postgres_worker_database(shared, "run", 'gw0"') as worker_url:
        source = make_url(db.normalize_url(shared))
        target = make_url(worker_url)
        name = worker_database_name("run", 'gw0"')
        assert target == source.set(database=name)
        assert calls[0][0] == source
        assert calls[0][1]["isolation_level"] == "AUTOCOMMIT"
        assert calls[1:] == [f'CREATE DATABASE "{name}"']
    assert calls[2:] == [f'DROP DATABASE "{name}" WITH (FORCE)', "dispose"]
    assert all("factory_test" not in sql for sql in calls[1:])


def test_failed_create_never_drops_an_existing_database(admin):
    engine, calls = admin

    @contextmanager
    def connect():
        def execute(sql):
            calls.append(sql)
            raise RuntimeError("already exists")
        yield SimpleNamespace(exec_driver_sql=execute)

    engine.connect = connect
    with pytest.raises(RuntimeError, match="already exists"):
        with postgres_worker_database("postgresql://localhost/factory_test", "run", "gw0"):
            pytest.fail("must not adopt an existing database")
    assert not any(isinstance(c, str) and c.startswith("DROP") for c in calls)
    assert calls[-1] == "dispose"


def test_body_failure_still_cleans_up(admin):
    _, calls = admin
    with pytest.raises(RuntimeError, match="test failed"):
        with postgres_worker_database("postgresql://localhost/factory_test", "run", "gw0"):
            raise RuntimeError("test failed")
    assert calls[-2].startswith("DROP DATABASE ") and calls[-1] == "dispose"


@pytest.mark.parametrize("source", ["sqlite:///test.db", "postgresql://localhost/" + worker_database_name("run", "gw0")])
def test_unsafe_source_is_rejected_before_connecting(admin, source):
    _, calls = admin
    with pytest.raises(ValueError, match="unsafe"):
        with postgres_worker_database(source, "run", "gw0"):
            pytest.fail("must not provision")
    assert calls == []


@pytest.mark.parametrize("shared,worker", [
    (None, None), (None, {"workerid": "gw0", "testrunuid": "run"}),
    ("postgresql://localhost/factory_test", None),
    ("sqlite:///explicit.db", {"workerid": "gw0", "testrunuid": "run"}),
])
def test_serial_and_sqlite_do_not_provision(monkeypatch, shared, worker):
    if shared is None:
        monkeypatch.delenv("AIFACTORY_TEST_DATABASE_URL", raising=False)
    else:
        monkeypatch.setenv("AIFACTORY_TEST_DATABASE_URL", shared)
    monkeypatch.setattr("sqlalchemy.create_engine", lambda *a, **kw: pytest.fail("unexpected provisioning"))
    config = SimpleNamespace(**({"workerinput": worker} if worker is not None else {}))
    fixture = database_url_fixture.__wrapped__(SimpleNamespace(config=config))
    assert next(fixture) == shared
    with pytest.raises(StopIteration):
        next(fixture)


def test_session_disposes_app_engine_before_dropping(monkeypatch):
    events = []

    @contextmanager
    def provision(shared, run, worker):
        assert (shared, run, worker) == ("postgresql://localhost/factory_test", "run", "gw1")
        events.append("create")
        yield "postgresql+psycopg://localhost/owned"
        events.append("drop")

    monkeypatch.setenv("AIFACTORY_TEST_DATABASE_URL", "postgresql://localhost/factory_test")
    monkeypatch.setattr("tests.conftest.postgres_worker_database", provision)
    monkeypatch.setattr(db, "_engine", SimpleNamespace(dispose=lambda: events.append("dispose")))
    request = SimpleNamespace(config=SimpleNamespace(workerinput={"testrunuid": "run", "workerid": "gw1"}))
    fixture = database_url_fixture.__wrapped__(request)
    assert next(fixture) == "postgresql+psycopg://localhost/owned"
    with pytest.raises(StopIteration):
        next(fixture)
    assert events == ["create", "dispose", "drop"]


def test_env_and_reinitialization_use_the_selected_database(client, isolated_env, test_database_url, request):
    import os

    expected = test_database_url or f"sqlite:///{isolated_env / 'factory.db'}"
    assert os.environ["DATABASE_URL"] == expected
    for _ in range(2):
        assert db.init_engine().url == make_url(db.normalize_url(expected))
    # client has already run create_app; creating another app must keep using this worker's URL.
    from aifactory.app import create_app

    create_app(start_orchestrator=False)
    assert db.engine().url == make_url(db.normalize_url(expected))
    if test_database_url and hasattr(request.config, "workerinput"):
        worker = request.config.workerinput
        assert db.engine().url.database == worker_database_name(worker["testrunuid"], worker["workerid"])


@pytest.mark.parametrize("case", range(3))
def test_per_test_reset_removes_committed_rows(case):
    from aifactory.models import Organization

    with db.session_scope() as session:
        assert session.get(Organization, "worker-reset-probe") is None
        session.add(Organization(id="worker-reset-probe", name="committed by the previous test"))


def test_real_postgres_committed_writes_and_ddl_are_isolated(test_database_url):
    # No service in the SQLite job; the same test exercises real DDL in the Postgres job, serial or xdist.
    if not test_database_url or make_url(db.normalize_url(test_database_url)).get_backend_name() != "postgresql":
        return
    run = uuid4().hex
    names = [worker_database_name(run, worker) for worker in ("gw0", "gw1")]
    admin = create_engine(db.normalize_url(test_database_url))
    try:
        with (
            postgres_worker_database(test_database_url, run, "gw0") as first_url,
            postgres_worker_database(test_database_url, run + "other", "gw0") as other_run_url,
        ):
            with postgres_worker_database(test_database_url, run, "gw1") as second_url:
                first, second = create_engine(first_url), create_engine(second_url)
                try:
                    for engine in (first, second):
                        with engine.begin() as conn:
                            conn.exec_driver_sql("CREATE TABLE worker_probe (value INTEGER)")
                    with first.begin() as conn:
                        conn.exec_driver_sql("INSERT INTO worker_probe VALUES (42)")
                        conn.exec_driver_sql("ALTER TABLE worker_probe ADD COLUMN extra TEXT")
                    # Fresh connections observe committed changes, not rollback-only test transactions.
                    with first.connect() as conn:
                        assert conn.exec_driver_sql("SELECT value FROM worker_probe").scalar_one() == 42
                    with second.connect() as conn:
                        assert conn.exec_driver_sql("SELECT count(*) FROM worker_probe").scalar_one() == 0
                    assert "extra" in {c["name"] for c in inspect(first).get_columns("worker_probe")}
                    assert "extra" not in {c["name"] for c in inspect(second).get_columns("worker_probe")}
                    with first.begin() as conn:
                        conn.exec_driver_sql("DROP TABLE worker_probe")
                    assert inspect(second).has_table("worker_probe")
                finally:
                    first.dispose()
                    second.dispose()
            # Cleaning up one worker cannot delete another worker's (or the supplied) database.
            with admin.connect() as conn:
                databases = set(conn.exec_driver_sql("SELECT datname FROM pg_database").scalars())
                assert names[0] in databases and names[1] not in databases
                assert make_url(other_run_url).database in databases
                assert admin.url.database in databases
        with admin.connect() as conn:
            databases = set(conn.exec_driver_sql("SELECT datname FROM pg_database").scalars())
            assert not set(names) & databases
            assert make_url(other_run_url).database not in databases
            assert admin.url.database in databases
    finally:
        admin.dispose()
