"""The database engine is replaced cleanly: an old pool never keeps connections open."""
from aifactory import db


def test_reinitialising_disposes_the_previous_engine(monkeypatch):
    first = db.init_engine()
    disposed = []
    monkeypatch.setattr(first, "dispose", lambda *args, **kwargs: disposed.append(True))
    second = db.init_engine()
    assert second is not first and disposed == [True]
