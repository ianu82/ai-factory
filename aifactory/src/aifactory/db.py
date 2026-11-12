"""Database engine and session helpers."""

from __future__ import annotations

import logging
from contextlib import contextmanager
from datetime import UTC, datetime
from typing import Iterator

from sqlalchemy import create_engine, event
from sqlalchemy.engine import Engine
from sqlalchemy.orm import DeclarativeBase, Session, sessionmaker

from .config import get_settings

log = logging.getLogger(__name__)


class Base(DeclarativeBase):
    pass


@event.listens_for(Session, "before_flush")
def _fit_strings(session: Session, _context, _instances) -> None:
    """Clip text to its column's declared length. SQLite ignores VARCHAR limits; Postgres rejects the row,
    and a long command or title must never fail a delivery step."""
    from sqlalchemy import String, inspect as sa_inspect

    for obj in list(session.new) + list(session.dirty):
        mapper = sa_inspect(obj).mapper
        for column in mapper.columns:
            if isinstance(column.type, String) and column.type.length:
                value = getattr(obj, column.key, None)
                if isinstance(value, str) and len(value) > column.type.length:
                    setattr(obj, column.key, value[: column.type.length - 1] + "…")


def utcnow() -> datetime:
    return datetime.now(UTC).replace(tzinfo=None)


_engine: Engine | None = None
_session_factory: sessionmaker[Session] | None = None


def _configure_sqlite(engine: Engine) -> None:
    @event.listens_for(engine, "connect")
    def _pragmas(dbapi_connection, _record):  # pragma: no cover - driver hook
        cursor = dbapi_connection.cursor()
        cursor.execute("PRAGMA journal_mode=WAL")
        cursor.execute("PRAGMA synchronous=NORMAL")
        cursor.execute("PRAGMA foreign_keys=ON")
        cursor.execute("PRAGMA busy_timeout=60000")
        cursor.close()


def normalize_url(url: str) -> str:
    """Accept the URLs hosting providers hand out (postgres://, postgresql://) and use the psycopg 3 driver."""
    for prefix in ("postgres://", "postgresql://"):
        if url.startswith(prefix):
            return "postgresql+psycopg://" + url[len(prefix):]
    return url


def init_engine(url: str | None = None) -> Engine:
    global _engine, _session_factory
    settings = get_settings()
    url = normalize_url(url or settings.database_url)
    if url.startswith("sqlite:///"):
        path = url.removeprefix("sqlite:///")
        if path and path != ":memory:":
            from pathlib import Path

            Path(path).parent.mkdir(parents=True, exist_ok=True)
    kwargs: dict = {"future": True}
    if url.startswith("sqlite"):
        kwargs["connect_args"] = {"check_same_thread": False, "timeout": 60}
    else:
        kwargs.update(pool_pre_ping=True, pool_size=10, max_overflow=20, pool_recycle=1800)
    if _engine is not None:
        # Re-initialisation (every test, each create_app) must not leave the old pool's connections open: on
        # PostgreSQL they count against max_connections until garbage collection ("too many clients").
        _engine.dispose()
    _engine = create_engine(url, **kwargs)
    if url.startswith("sqlite"):
        _configure_sqlite(_engine)
    _session_factory = sessionmaker(bind=_engine, expire_on_commit=False, future=True)
    from . import models, telemetry  # noqa: F401  (register tables and lifecycle listeners)

    Base.metadata.create_all(_engine)
    _add_missing_columns(_engine)
    _migrate_benchmark_trial_link(_engine)
    _migrate_workstream_link(_engine)
    _migrate_mind_action_scope(_engine)
    harden_data_dir()
    return _engine


def _add_missing_columns(engine: Engine) -> None:
    """Additive schema evolution: new nullable model columns are added to existing tables."""
    from sqlalchemy import inspect, text

    tables = Base.metadata.sorted_tables
    # One reflection for every table (two catalog queries per table on PostgreSQL otherwise, at each start).
    reflected = inspect(engine).get_multi_columns(filter_names=[t.name for t in tables])
    existing = {name: {c["name"] for c in columns} for (_schema, name), columns in reflected.items()}
    with engine.begin() as conn:
        for table in tables:
            if table.name not in existing:
                continue
            present = existing[table.name]
            for column in table.columns:
                if column.name in present or not column.nullable:
                    continue
                kind = column.type.compile(dialect=engine.dialect)
                conn.execute(text(f'ALTER TABLE "{table.name}" ADD COLUMN "{column.name}" {kind}'))


def _migrate_benchmark_trial_link(engine: Engine) -> None:
    """create_all does not add indexes/constraints to an existing runs table.

    SQLite cannot add a foreign key without rebuilding the table; PostgreSQL can.
    New SQLite databases get the constraint from the model as usual.
    """
    from sqlalchemy import inspect, text

    from .models import Run

    with engine.begin() as conn:
        for index in Run.__table__.indexes:
            if index.name == "ix_runs_benchmark_trial_id":
                index.create(conn, checkfirst=True)
        if engine.dialect.name == "postgresql":
            keys = inspect(conn).get_foreign_keys("runs")
            if not any(k["constrained_columns"] == ["benchmark_trial_id"] for k in keys):
                conn.execute(text(
                    "ALTER TABLE runs ADD CONSTRAINT fk_runs_benchmark_trial_id "
                    "FOREIGN KEY (benchmark_trial_id) REFERENCES benchmark_trials (id)"
                ))


def _migrate_workstream_link(engine: Engine) -> None:
    """Existing tables need their new index (and, on Postgres, foreign key) explicitly."""
    from sqlalchemy import inspect, text

    from .models import Outcome

    with engine.begin() as conn:
        for index in Outcome.__table__.indexes:
            if index.name == "ix_outcomes_workstream_id":
                index.create(conn, checkfirst=True)
        if engine.dialect.name == "postgresql":
            keys = inspect(conn).get_foreign_keys("outcomes")
            if not any(k["constrained_columns"] == ["workstream_id"] for k in keys):
                conn.execute(text(
                    "ALTER TABLE outcomes ADD CONSTRAINT fk_outcomes_workstream_id "
                    "FOREIGN KEY (workstream_id) REFERENCES workstreams (id)"
                ))


def _migrate_mind_action_scope(engine: Engine) -> None:
    """A Mind card for workspace-level work (a new outcome) has no outcome yet: mind_actions.outcome_id became
    nullable. PostgreSQL drops the constraint in place. SQLite cannot alter a column, so it rebuilds the table as
    sqlite.org/lang_altertable.html describes: in one transaction with foreign keys off, a copy of the stored
    definition without that NOT NULL, every row copied, the old table dropped, the copy renamed and its indexes
    recreated. Idempotent: a nullable column (any new database) is left alone."""
    import re

    from sqlalchemy import inspect, text

    inspector = inspect(engine)
    if not inspector.has_table("mind_actions"):
        return
    column = next((c for c in inspector.get_columns("mind_actions") if c["name"] == "outcome_id"), None)
    if column is None or column["nullable"]:
        return
    if engine.dialect.name == "postgresql":
        with engine.begin() as conn:
            conn.execute(text("ALTER TABLE mind_actions ALTER COLUMN outcome_id DROP NOT NULL"))
        return
    if engine.dialect.name != "sqlite":
        return
    with engine.connect() as conn:
        table = conn.exec_driver_sql(
            "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'mind_actions'").scalar_one()
        indexes = [row[0] for row in conn.exec_driver_sql(
            "SELECT sql FROM sqlite_master WHERE type = 'index' AND tbl_name = 'mind_actions' AND sql IS NOT NULL")]
        rebuilt, named = re.subn(r'^\s*CREATE TABLE\s+"?mind_actions"?(?=[\s(])', "CREATE TABLE mind_actions_rebuild",
                                 table, count=1)
        rebuilt, relaxed = re.subn(r"(\boutcome_id\s+VARCHAR(?:\(\d+\))?)\s+NOT NULL", r"\1", rebuilt, count=1)
        if not (named and relaxed):
            # Never stop the app over this: only Mind cards for new outcomes need the nullable column.
            log.error("mind_actions has an unexpected definition; outcome_id stays NOT NULL")
            return
        conn.commit()  # end the reads: the pragma below is ignored inside a transaction
        conn.exec_driver_sql("PRAGMA foreign_keys=OFF")
        try:
            conn.exec_driver_sql("BEGIN")
            conn.exec_driver_sql("DROP TABLE IF EXISTS mind_actions_rebuild")
            conn.exec_driver_sql(rebuilt)
            conn.exec_driver_sql("INSERT INTO mind_actions_rebuild SELECT * FROM mind_actions")
            conn.exec_driver_sql("DROP TABLE mind_actions")
            conn.exec_driver_sql("ALTER TABLE mind_actions_rebuild RENAME TO mind_actions")
            for statement in indexes:
                conn.exec_driver_sql(statement)
            conn.commit()
        except Exception:
            conn.rollback()  # the original table is untouched
            log.exception("Could not make mind_actions.outcome_id nullable; Mind cannot propose new outcomes")
        finally:
            try:
                conn.exec_driver_sql("PRAGMA foreign_keys=ON")
                conn.commit()
            finally:
                conn.invalidate()  # this connection never returns to the pool with altered pragmas


def harden_data_dir() -> None:
    """Job sandboxes run as other UIDs: they may traverse the data dir but not read it."""
    import os

    settings = get_settings()
    root = settings.data_dir
    try:
        os.chmod(root, 0o711)
        for entry in root.iterdir():
            if entry.name == "jobs":
                os.chmod(entry, 0o711)
            elif entry.is_dir():
                os.chmod(entry, 0o700)
            else:
                os.chmod(entry, 0o600)
    except (FileNotFoundError, PermissionError):
        pass


def engine() -> Engine:
    if _engine is None:
        init_engine()
    assert _engine is not None
    return _engine


def SessionLocal() -> Session:  # noqa: N802 - factory naming
    if _session_factory is None:
        init_engine()
    assert _session_factory is not None
    return _session_factory()


@contextmanager
def session_scope() -> Iterator[Session]:
    session = SessionLocal()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def get_db() -> Iterator[Session]:
    """FastAPI dependency."""
    session = SessionLocal()
    try:
        yield session
    finally:
        session.close()
