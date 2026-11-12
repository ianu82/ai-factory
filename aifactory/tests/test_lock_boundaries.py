"""Executable lock audit: database mutation locks last until transaction end.

The async repository merge queue lock is intentionally NOT a SQL lock: bounded
GitHub operations are allowed there, but never under either workspace SQL lock
or the repository row locks. Instrument actual callables (including imported
aliases) rather than relying on lexical `with` blocks or AST heuristics.
"""
from __future__ import annotations

import asyncio
import functools
import inspect
import json
import sys
import threading

import httpx
import pytest
from sqlalchemy import event
from sqlalchemy.orm import Session

from aifactory import gitops, onboarding_policy
from aifactory.db import session_scope
from aifactory.definitions import store, validation
from aifactory.github import GitHub
from aifactory.inference import ModelClient
from aifactory.models import Repository, Workspace
from . import fakes, test_definitions_api, test_insights, test_linear_config, test_onboarding, test_pipeline
from .test_pipeline import factory  # noqa: F401 - fixture used by audit scenarios
from .test_linear_config import configured  # noqa: F401


@pytest.fixture
def lock_guard(monkeypatch):
    held = {}  # Session -> owning thread; never clear on helper return/savepoint exit
    visited = set()
    exercised = set()

    def transaction_end(session, transaction):
        if transaction.parent is None:
            held.pop(session, None)

    event.listen(Session, "after_transaction_end", transaction_end)

    def unlocked(name):
        assert threading.get_ident() not in held.values(), f"{name} ran under a SQL mutation lock"
        exercised.add(name)

    def replace_aliases(original, wrapped):
        for module in list(sys.modules.values()):
            if module and getattr(module, "__name__", "").startswith(("aifactory", "tests")):
                for name, value in list(vars(module).items()):
                    if value is original:
                        monkeypatch.setattr(module, name, wrapped)

    for original in (onboarding_policy.lock_workspace, store.lock_workspace, store.lock_repositories):
        name = f"{original.__module__}.{original.__name__}"

        @functools.wraps(original)
        def lock(db, *args, _original=original, _name=name, **kwargs):
            held[db] = threading.get_ident()
            visited.add(_name)
            return _original(db, *args, **kwargs)

        replace_aliases(original, lock)

    def boundary(original):
        name = f"{original.__module__}.{original.__qualname__}"
        if inspect.iscoroutinefunction(original):
            @functools.wraps(original)
            async def wrapped(*args, **kwargs):
                unlocked(name)
                return await original(*args, **kwargs)
        else:
            @functools.wraps(original)
            def wrapped(*args, **kwargs):
                unlocked(name)
                return original(*args, **kwargs)
        return wrapped

    # Low-level HTTP plus model/git/ingestion boundaries, including the fakes
    # used by pipeline tests (which intentionally bypass the real HTTP client).
    for cls, names in ((httpx.AsyncClient, ("send",)), (httpx.Client, ("send",)),
                       (GitHub, ("request", "merge", "pull")), (ModelClient, ("chat",)),
                       (fakes.ScriptedModel, ("chat",)),
                       (fakes.FakeGitHub, ("merge", "pull", "branch_sha", "create_pull"))):
        for name in names:
            monkeypatch.setattr(cls, name, boundary(getattr(cls, name)))
    for original in (gitops.git, validation.read_skill_folder):
        replace_aliases(original, boundary(original))
    try:
        yield unlocked, visited, exercised
        assert not held, "A locked transaction leaked out of the scenario"
    finally:
        event.remove(Session, "after_transaction_end", transaction_end)


@pytest.mark.parametrize("lock", [onboarding_policy.lock_workspace, store.lock_workspace, store.lock_repositories])
@pytest.mark.parametrize("end", ["commit", "rollback", "close"])
def test_guard_tracks_locks_through_transaction_end(client, signed_in, lock_guard, lock, end):
    # Resolve the patched alias after parametrization: the guard must outlive the
    # helper return and nested transactions, and stop only at root transaction end.
    lock = getattr(sys.modules[lock.__module__], lock.__name__)
    ws = signed_in["workspaces"][0]["id"]
    unlocked, visited, _ = lock_guard
    from aifactory.db import SessionLocal
    db = SessionLocal()
    try:
        lock(db, ws)
        with db.begin_nested():
            pass
        with pytest.raises(AssertionError, match="SQL mutation lock"):
            unlocked("expensive preparation")
        with pytest.raises(AssertionError, match="SQL mutation lock"):
            asyncio.run(GitHub("token").merge("acme/demo", 1, sha="head", method="squash", title="Merge"))
        with pytest.raises(AssertionError, match="SQL mutation lock"):
            asyncio.run(gitops.git("--version"))
        with pytest.raises(AssertionError, match="SQL mutation lock"):
            validation.read_skill_folder(None)
        with pytest.raises(AssertionError, match="SQL mutation lock"):
            asyncio.run(fakes.ScriptedModel(None).chat(model="test", messages=[]))
        with httpx.Client() as http:
            with pytest.raises(AssertionError, match="SQL mutation lock"):
                http.get("https://must-not-be-contacted.invalid")
        getattr(db, end)()
        unlocked("after transaction end")
        assert visited
    finally:
        db.close()


def test_merge_offender_and_pipeline_are_guarded(factory, lock_guard):
    test_pipeline.test_outcome_is_planned_built_verified_merged_and_smoke_tested(factory)
    _, visited, exercised = lock_guard
    assert "aifactory.onboarding_policy.lock_workspace" in visited
    assert "tests.fakes.FakeGitHub.merge" in exercised
    assert "tests.fakes.ScriptedModel.chat" in exercised
    assert "aifactory.gitops.git" in exercised


def test_api_definitions_onboarding_and_insights_lock_audit(client, signed_in, lock_guard):
    test_definitions_api.test_save_history_restore_and_conflicts(client, signed_in)
    test_onboarding.test_start_onboarding_enforces_learning_before_any_step_and_is_idempotent(client, signed_in)
    test_insights.test_consistent_merge_approvals_propose_auto_merge_and_apply_attributed(client, signed_in)
    _, visited, _ = lock_guard
    assert {"aifactory.onboarding_policy.lock_workspace", "aifactory.definitions.store.lock_workspace",
            "aifactory.definitions.store.lock_repositories"} <= visited


def test_linear_discovery_precedes_mutation_lock(client, signed_in, configured, lock_guard):
    test_linear_config.test_save_reload_defaults_custom_label_and_audit(client, signed_in, configured, "owner")
    assert "httpx._client.AsyncClient.send" in lock_guard[2]


@pytest.mark.parametrize("lock", [onboarding_policy.lock_workspace, store.lock_workspace, store.lock_repositories])
def test_all_mutation_lock_entry_points_fence_reservations(client, signed_in, lock):
    ws = signed_in["workspaces"][0]["id"]
    with session_scope() as db:
        db.get(Workspace, ws).merge_reservation = {"token": "active-owner"}
    with session_scope() as db:
        with pytest.raises(onboarding_policy.MergeReservationConflict):
            lock(db, ws)
    for method, path, body in (
        ("PATCH", "", {"settings": {"autonomy": {"merge": "auto"}}}),
        ("POST", "/onboarding/start", None),
        ("POST", "/onboarding/policy/apply", None),
        ("PUT", "/definitions", {"bundle": {}, "expected_revision": 0}),
        ("PUT", "/linear", {"enabled": False}),
    ):
        response = client.request(method, f"/api/workspaces/{ws}" + path, json=body)
        assert response.status_code == 409, response.text
        assert response.headers["Retry-After"] == "1"
    with session_scope() as db:
        assert db.get(Workspace, ws).merge_reservation == {"token": "active-owner"}


def test_linear_save_revalidates_repository_ownership(client, signed_in):
    from aifactory import linear_config
    ws = signed_in["workspaces"][0]["id"]
    with session_scope() as db:
        repo = Repository(workspace_id=ws, full_name="acme/disconnected")
        db.add(repo)
        db.flush()
        config = linear_config.LinearConfig(repo_ids=[repo.id])
    with session_scope() as db:
        db.delete(db.get(Repository, config.repo_ids[0]))
    with session_scope() as db:
        with pytest.raises(linear_config.LinearConfigError, match="belong"):
            linear_config.save_config(db, db.get(Workspace, ws), config, signed_in["user"]["id"])


def test_insights_settings_lock_audit(client, signed_in, lock_guard):
    test_insights.test_settings_proposal_applies_to_workspace_settings(client, signed_in)
    assert "aifactory.onboarding_policy.lock_workspace" in lock_guard[1]


def test_bootstrap_prepares_detection_before_locks(client, signed_in, monkeypatch, lock_guard):
    from aifactory import bootstrap, github, repo_detect
    ws = signed_in["workspaces"][0]["id"]
    unlocked, visited, _ = lock_guard

    class DetectedGitHub:
        def __init__(self, token):
            pass

        async def repo(self, name):
            unlocked("bootstrap GitHub detection")
            return {"full_name": name, "default_branch": "main"}

    async def detect(*args):
        unlocked("bootstrap file ingestion")
        return {"context": "Detected notes"}

    monkeypatch.setattr(github, "GitHub", DetectedGitHub)
    monkeypatch.setattr(github, "resolve_credential", lambda _: "fake")
    monkeypatch.setattr(repo_detect, "detect", detect)
    asyncio.run(bootstrap._connect(ws, [("acme/boot", {})]))
    # Adopt the already connected workspace by its actual name.
    with session_scope() as db:
        name = db.get(Workspace, ws).name
    monkeypatch.setenv("AIFACTORY_BOOTSTRAP", json.dumps({
        "organization": "MindsDB", "workspace": name,
        "repositories": [{"full_name": "acme/boot", "config": {"context": "Updated notes"}}],
    }))

    async def boot():
        assert bootstrap.apply_bootstrap() == ws
    asyncio.run(boot())
    assert "aifactory.definitions.store.lock_repositories" in visited
    with session_scope() as db:
        assert db.query(Repository).filter_by(workspace_id=ws).one().config["context"] == "Updated notes"


def test_file_ingestion_precedes_definition_save(client, signed_in, tmp_path, lock_guard):
    from aifactory.definitions import Bundle
    ws = signed_in["workspaces"][0]["id"]
    folder = tmp_path / "skill"
    folder.mkdir()
    (folder / "SKILL.md").write_text('---\nname: example\ndescription: Example skill\n---\nDo the work.\n')
    skill = validation.read_skill_folder(folder)
    with session_scope() as db:
        store.save_bundle(db, ws, Bundle(skills=[skill]), expected_revision=0, author=signed_in["user"]["id"])
    assert "aifactory.definitions.validation.read_skill_folder" in lock_guard[2]


def test_merge_reservation_column_is_additive_and_durable(tmp_path):
    from sqlalchemy import create_engine, inspect, text
    from aifactory import db

    url = f"sqlite:///{tmp_path / 'legacy.db'}"
    old = create_engine(url)
    with old.begin() as conn:
        conn.execute(text("CREATE TABLE workspaces (id VARCHAR(32) PRIMARY KEY, settings JSON NOT NULL)"))
        conn.execute(text("INSERT INTO workspaces VALUES ('old', '{}')"))
    old.dispose()
    engine = db.init_engine(url)
    column = next(c for c in inspect(engine).get_columns("workspaces") if c["name"] == "merge_reservation")
    assert column["nullable"] and column["default"] is None
    with engine.begin() as conn:
        assert conn.execute(text("SELECT merge_reservation FROM workspaces WHERE id='old'")).scalar() is None
        conn.execute(text("UPDATE workspaces SET merge_reservation = :intent WHERE id='old'"),
                     {"intent": json.dumps({"token": "survives-restart"})})
    engine = db.init_engine(url)
    with engine.connect() as conn:
        assert json.loads(conn.execute(text("SELECT merge_reservation FROM workspaces WHERE id='old'"))
                          .scalar_one()) == {"token": "survives-restart"}
