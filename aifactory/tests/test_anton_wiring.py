"""Workspace configuration, trusted scope resolution and durable run tool metadata."""

import asyncio
from dataclasses import replace

import pytest
from sqlalchemy import create_engine, inspect, text

from aifactory import db
from aifactory.db import session_scope
from aifactory.harness import HarnessFailed, HarnessResult
from aifactory.harness.anton_policy import DEFAULT_ANTON_OPTIONS, tool_config
from aifactory.models import Outcome, Run, Task
from aifactory.pipeline import runtime
from aifactory.pipeline.runtime import SessionSpec, anton_repository_ids, merged_settings, new_settings
from aifactory.sandbox import LocalSandbox
from aifactory.serializers import run as run_dict

from .test_memory import seed


def test_anton_defaults_merge_without_sharing_mutable_settings():
    assert merged_settings(None)["anton"] == DEFAULT_ANTON_OPTIONS
    assert merged_settings({"anton": {"web_enabled": True}})["anton"] == {
        "own_tools": True, "web_enabled": True, "skills_root": None}
    first = new_settings()
    first["anton"]["own_tools"] = False
    assert new_settings()["anton"]["own_tools"] is True
    assert merged_settings(None)["anton"]["own_tools"] is True
    assert merged_settings({"anton": None})["anton"] == DEFAULT_ANTON_OPTIONS


@pytest.mark.parametrize("value,detail", [
    (None, "anton must be an object"), ([], "anton must be an object"),
    ({"own_tools": "false"}, "anton.own_tools must be a boolean"),
    ({"web_enabled": 1}, "anton.web_enabled must be a boolean"),
    ({"skills_root": ""}, "anton.skills_root must be a non-empty path or null"),
    ({"skills_root": 4}, "anton.skills_root must be a non-empty path or null"),
    ({"skills_root": "bad\x00path"}, "anton.skills_root must be a non-empty path or null"),
])
def test_anton_settings_validation(client, signed_in, value, detail):
    ws = signed_in["workspaces"][0]["id"]
    response = client.patch(f"/api/workspaces/{ws}", json={"settings": {"anton": value}})
    assert response.status_code == 422
    assert response.json()["detail"] == detail


def test_anton_settings_partial_updates(client, signed_in):
    ws = signed_in["workspaces"][0]["id"]
    path = f"/api/workspaces/{ws}"
    first = client.patch(path, json={"settings": {"anton": {"web_enabled": True, "skills_root": "/configured/skills"}}})
    assert first.status_code == 200
    assert first.json()["settings"]["anton"] == {
        "own_tools": True, "web_enabled": True, "skills_root": "/configured/skills"}
    second = client.patch(path, json={"settings": {"anton": {"own_tools": False}}})
    assert second.status_code == 200
    assert second.json()["settings"]["anton"] == {
        "own_tools": False, "web_enabled": True, "skills_root": "/configured/skills"}
    third = client.patch(path, json={"settings": {"concurrency": 4}})
    assert third.status_code == 200
    assert third.json()["settings"]["anton"] == second.json()["settings"]["anton"]


def test_repository_scope_is_resolved_from_task_or_outcome_not_prompt_or_snapshot(tmp_path):
    ws, repos, _, outcome, task = seed()
    _, other_repos, _, foreign_outcome, foreign_task = seed()
    spec = SessionSpec(ws, outcome, task, "builder", "anton", "Use a different repo", "System",
                       LocalSandbox("test", root=tmp_path), repositories={other_repos[0]: {"name": "forged"}})
    assert anton_repository_ids(spec) == (repos[0],)
    assert anton_repository_ids(replace(spec, task_id=None, role="planner")) == tuple(repos)
    for invalid in (replace(spec, task_id=foreign_task), replace(spec, outcome_id=foreign_outcome),
                    replace(spec, workspace_id="unknown"), replace(spec, task_id="unknown")):
        with pytest.raises(HarnessFailed, match="scope not found"):
            anton_repository_ids(invalid)
    with session_scope() as s:
        s.get(Task, task).repo_id = other_repos[0]
    with pytest.raises(HarnessFailed, match="scope not found"):
        anton_repository_ids(spec)
    with session_scope() as s:
        s.get(Outcome, outcome).repo_ids = [other_repos[0]]
    with pytest.raises(HarnessFailed, match="scope not found"):
        anton_repository_ids(replace(spec, task_id=None))


@pytest.mark.parametrize("harness_name", ["native", "codex", "anton"])
@pytest.mark.parametrize("exit_kind", ["success", "failed", "cancelled"])
async def test_run_metadata_and_context_survive_every_exit(tmp_path, monkeypatch, harness_name, exit_kind):
    from aifactory.inference import ProviderConfig

    ws, repos, _, outcome, task = seed()
    sandbox = LocalSandbox("metadata", root=tmp_path / "sandbox").create()
    sandbox.work.mkdir()
    options = {"own_tools": True, "web_enabled": True, "skills_root": None}
    effective = ["read_file", "finish"]
    config = tool_config(True, options, sandbox.kind, effective=effective, fallback_reason="memory_startup_failed")
    seen = []

    class FakeHarness:
        label = "Fake"

        async def run(self, session):
            seen.append(session)
            if harness_name == "anton":
                assert session.workspace_id == ws
                assert session.repository_ids == (repos[0],)
                assert session.anton_options == options
                assert session.tool_policy == {"web": True}
                session.on_tool_config(config)
            else:
                assert session.anton_options == {} and session.tool_policy == {}
                assert session.repository_ids == ()
            if exit_kind == "failed":
                raise HarnessFailed("No report")
            if exit_kind == "cancelled":
                raise asyncio.CancelledError
            return HarnessResult(report={"status": "done"}, turns=1)

    monkeypatch.setattr(runtime, "get_harness", lambda _: FakeHarness())
    monkeypatch.setattr(runtime, "resolve_provider", lambda *a, **kw: ProviderConfig("openai", "http://unused/v1", "sk-test", "Fake"))
    monkeypatch.setattr(runtime, "issue", lambda grant: "grant")
    monkeypatch.setattr(runtime, "revoke", lambda _: None)
    spec = SessionSpec(ws, outcome, task, "builder", harness_name, "Build", "System", sandbox,
                       writable=True, effective_settings={"anton": options, "provider": "openai"})
    try:
        if exit_kind == "cancelled":
            with pytest.raises(asyncio.CancelledError):
                await runtime.run_agent(spec, asyncio.Event())
        else:
            run, result, error = await runtime.run_agent(spec, asyncio.Event())
            assert run.status == ("succeeded" if exit_kind == "success" else "failed")
            assert bool(result) == (exit_kind == "success")
        assert len(seen) == 1
        with session_scope() as s:
            saved = s.get(Run, seen[0].run_id)
            assert saved.tool_config == (config if harness_name == "anton" else None)
            assert run_dict(saved)["tool_config"] == saved.tool_config
            assert saved.status == {"success": "succeeded", "failed": "failed", "cancelled": "cancelled"}[exit_kind]
            assert saved.result == ({"status": "done"} if exit_kind == "success" else {})
    finally:
        await sandbox.close()


def test_tool_config_additive_migration_and_legacy_null(tmp_path):
    engine = create_engine(f"sqlite:///{tmp_path / 'legacy.db'}")
    with engine.begin() as conn:
        conn.execute(text("CREATE TABLE runs (id VARCHAR PRIMARY KEY)"))
        conn.execute(text("INSERT INTO runs (id) VALUES ('old')"))
    try:
        db._add_missing_columns(engine)
        db._add_missing_columns(engine)
        assert "tool_config" in {c["name"] for c in inspect(engine).get_columns("runs")}
        with engine.connect() as conn:
            assert conn.execute(text("SELECT tool_config FROM runs WHERE id='old'")).scalar() is None
    finally:
        engine.dispose()
