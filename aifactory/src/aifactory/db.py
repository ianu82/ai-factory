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


def init_engine(url: str | None = None) -> Engine:
    global _engine, _session_factory
    settings = get_settings()
    url = url or settings.database_url
    if url.startswith("sqlite:///"):
        path = url.removeprefix("sqlite:///")
        if path and path != ":memory:":
            from pathlib import Path

            Path(path).parent.mkdir(parents=True, exist_ok=True)
    kwargs: dict = {"future": True}
    if url.startswith("sqlite"):
        kwargs["connect_args"] = {"check_same_thread": False, "timeout": 60}
    else:
        kwargs["pool_pre_ping"] = True
    _engine = create_engine(url, **kwargs)
    if url.startswith("sqlite"):
        _configure_sqlite(_engine)
    _session_factory = sessionmaker(bind=_engine, expire_on_commit=False, future=True)
    from . import models  # noqa: F401  (register tables)

    Base.metadata.create_all(_engine)
    harden_data_dir()
    return _engine


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
