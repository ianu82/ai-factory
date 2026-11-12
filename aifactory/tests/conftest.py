from __future__ import annotations

import os
import sys
import zlib
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))


def shard_of(nodeid: str, total: int) -> int:
    """Which of `total` shards (1-based) runs this test: a stable hash of its file and function.

    The parameters of a parametrised test are left out of the key, so all its cases run in one shard and an id
    that differs between runs can never put a case in two shards or in none.
    """
    return zlib.crc32(nodeid.split("[", 1)[0].encode()) % total + 1


def pytest_collection_modifyitems(config, items):
    """CI runs the suite on several runners at once: AIFACTORY_TEST_SHARD=2/3 keeps the second third of the tests.

    Unset (a developer's machine, the Factory's own check commands) every test runs, as before. Every test sets
    up its own data directory and database (`isolated_env`), so which tests share a process does not matter.
    """
    spec = os.environ.get("AIFACTORY_TEST_SHARD", "").strip()
    if not spec:
        return
    index, _, total = spec.partition("/")
    if not (index.isdigit() and total.isdigit() and 1 <= int(index) <= int(total)):
        raise pytest.UsageError(f"AIFACTORY_TEST_SHARD must look like 2/3, got {spec!r}")
    keep = [item for item in items if shard_of(item.nodeid, int(total)) == int(index)]
    dropped = [item for item in items if shard_of(item.nodeid, int(total)) != int(index)]
    if dropped:
        config.hook.pytest_deselected(items=dropped)
        items[:] = keep


@pytest.fixture(autouse=True)
def isolated_env(tmp_path, monkeypatch):
    data = tmp_path / "data"
    monkeypatch.setenv("AIFACTORY_DATA_DIR", str(data))
    shared = os.environ.get("AIFACTORY_TEST_DATABASE_URL")  # e.g. a throwaway Postgres; reset per test
    monkeypatch.setenv("DATABASE_URL", shared or f"sqlite:///{data / 'factory.db'}")
    monkeypatch.setenv("AIFACTORY_SECRET_KEY", "test-secret-key-for-unit-tests-only")
    monkeypatch.setenv("AIFACTORY_ALLOW_DEV_SIGN_IN", "1")
    monkeypatch.setenv("AIFACTORY_ORCHESTRATOR", "0")
    monkeypatch.setenv("AIFACTORY_SANDBOX_ISOLATION", "0")
    monkeypatch.setenv("AIFACTORY_VM_WORK", "/fake-vm-work")  # the fake VM's checkout; never a real path
    for key in ("AIFACTORY_BOOTSTRAP_OPENAI_API_KEY", "AIFACTORY_BOOTSTRAP_MINDSHUB_API_KEY",
                "AIFACTORY_BOOTSTRAP_GITHUB_TOKEN", "GH_TOKEN", "GITHUB_TOKEN",
                "AIFACTORY_GITHUB_APP_ID", "AIFACTORY_GITHUB_APP_PRIVATE_KEY",
                "AIFACTORY_GITHUB_APP_CLIENT_ID", "AIFACTORY_GITHUB_APP_CLIENT_SECRET",
                "AIFACTORY_GITHUB_APP_WEBHOOK_SECRET"):
        monkeypatch.delenv(key, raising=False)
    from aifactory import config, db, fallbacks, security

    config.reset_settings_cache()
    fallbacks.set_bootstrap_workspace(None)  # server credentials serve only a bootstrap workspace a test declares
    from aifactory import inference_limits

    # Admission guards are process-wide and keyed by provider config, which fakes share across tests: a send
    # still inside a previous test's rate window would otherwise leak into the next test's counts.
    with inference_limits._registry_lock:
        inference_limits._registry.clear()
    security.reset_crypto_cache()
    if shared:
        reset_shared_database(db.init_engine())
    else:
        db.init_engine()
    yield data
    config.reset_settings_cache()
    security.reset_crypto_cache()


# The schema each shared database had right after it was last rebuilt.
_PRISTINE: dict[str, str] = {}

# One query over the tables the models define: their columns, indexes and constraints.
_SCHEMA = """
SELECT md5(coalesce(string_agg(part, '|' ORDER BY part), '')) FROM (
    SELECT 'c ' || c.relname || '.' || a.attname || ' ' || format_type(a.atttypid, a.atttypmod)
           || ' ' || a.attnotnull || ' ' || coalesce(pg_get_expr(d.adbin, d.adrelid), '') AS part
    FROM pg_attribute a JOIN pg_class c ON c.oid = a.attrelid JOIN pg_namespace n ON n.oid = c.relnamespace
    LEFT JOIN pg_attrdef d ON d.adrelid = a.attrelid AND d.adnum = a.attnum
    WHERE n.nspname = current_schema() AND c.relname = ANY(%(names)s) AND a.attnum > 0 AND NOT a.attisdropped
    UNION ALL
    SELECT 'i ' || indexdef FROM pg_indexes WHERE schemaname = current_schema() AND tablename = ANY(%(names)s)
    UNION ALL
    SELECT 'k ' || c.relname || ' ' || k.conname || ' ' || pg_get_constraintdef(k.oid)
    FROM pg_constraint k JOIN pg_class c ON c.oid = k.conrelid JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname = current_schema() AND c.relname = ANY(%(names)s)
) parts
"""


def reset_shared_database(engine):
    """Empty the shared test database before each test.

    Dropping and recreating every table cost ~0.4 s per test on Postgres. Truncating is enough while the schema is
    the one the models create; a test that changed it (the migration tests do) gets a full rebuild instead.
    """
    from aifactory.models import Base

    names = [t.name for t in Base.metadata.sorted_tables]
    tables = ", ".join(f'"{name}"' for name in names)

    def fingerprint():
        with engine.connect() as conn:
            return conn.exec_driver_sql(_SCHEMA, {"names": names}).scalar_one()

    key = engine.url.render_as_string(hide_password=True)
    if _PRISTINE.get(key) == fingerprint():
        with engine.begin() as conn:
            conn.exec_driver_sql(f"TRUNCATE TABLE {tables} RESTART IDENTITY CASCADE")
        return
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    _PRISTINE[key] = fingerprint()


@pytest.fixture
def client():
    from fastapi.testclient import TestClient

    from aifactory.app import create_app

    with TestClient(create_app(start_orchestrator=False)) as c:
        yield c


@pytest.fixture
def signed_in(client):
    r = client.post("/auth/dev", json={"email": "ian@example.com", "name": "Ian", "organization": "MindsDB"})
    assert r.status_code == 200, r.text
    me = client.get("/api/me").json()
    return me


def git(*args, cwd=None):
    import subprocess

    env = {**os.environ, "GIT_AUTHOR_NAME": "t", "GIT_AUTHOR_EMAIL": "t@t", "GIT_COMMITTER_NAME": "t",
           "GIT_COMMITTER_EMAIL": "t@t"}
    return subprocess.run(["git", *args], cwd=cwd, check=True, capture_output=True, text=True, env=env).stdout
