"""Database engine and session helpers."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import UTC, datetime
from typing import Iterator

from sqlalchemy import create_engine, event
from sqlalchemy.engine import Engine
from sqlalchemy.orm import DeclarativeBase, Session, sessionmaker

from .config import get_settings


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
    _engine = create_engine(url, **kwargs)
    if url.startswith("sqlite"):
        _configure_sqlite(_engine)
    _session_factory = sessionmaker(bind=_engine, expire_on_commit=False, future=True)
    from . import models  # noqa: F401  (register tables)

    Base.metadata.create_all(_engine)
    _add_missing_columns(_engine)
    harden_data_dir()
    return _engine


def _add_missing_columns(engine: Engine) -> None:
    """Additive schema evolution: new nullable model columns are added to existing tables."""
    from sqlalchemy import inspect, text

    inspector = inspect(engine)
    with engine.begin() as conn:
        for table in Base.metadata.sorted_tables:
            if not inspector.has_table(table.name):
                continue
            present = {c["name"] for c in inspector.get_columns(table.name)}
            for column in table.columns:
                if column.name in present or not column.nullable:
                    continue
                kind = column.type.compile(dialect=engine.dialect)
                conn.execute(text(f'ALTER TABLE "{table.name}" ADD COLUMN "{column.name}" {kind}'))


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
