from __future__ import annotations

import os
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))


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
                "AIFACTORY_BOOTSTRAP_GITHUB_TOKEN", "GH_TOKEN", "GITHUB_TOKEN"):
        monkeypatch.delenv(key, raising=False)
    from aifactory import config, db, security

    config.reset_settings_cache()
    security.reset_crypto_cache()
    if shared:
        from aifactory.models import Base

        engine = db.init_engine()
        Base.metadata.drop_all(engine)
        Base.metadata.create_all(engine)
    else:
        db.init_engine()
    yield data
    config.reset_settings_cache()
    security.reset_crypto_cache()


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
