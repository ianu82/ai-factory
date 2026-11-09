from __future__ import annotations

import asyncio
import json
from dataclasses import FrozenInstanceError
from importlib import metadata

import pytest
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.schema import CreateTable

from aifactory import config_versions as versions, db, prompts, serializers
from aifactory.inference import ProviderConfig
from aifactory.models import Outcome, Repository, Run, Workspace
from aifactory.pipeline import runtime


def fingerprint(**kwargs):
    inputs = dict(role="builder", system=prompts.BUILDER, harness="native",
                  provider=ProviderConfig("openai", "https://model.test/v1", "secret", "Test"),
                  model="model-a", settings={})
    inputs.update(kwargs)
    return versions.configuration_version(**inputs)


def test_canonical_defaults_and_numeric_equivalence():
    assert fingerprint()[0] == fingerprint(settings=runtime.merged_settings({}))[0]
    assert fingerprint(settings={"budgets": {"run_usd": 8}})[0] == fingerprint()[0]
    assert fingerprint(settings={"extra": {"a": 8, "b": [1, 2]}})[0] == fingerprint(
        settings={"extra": {"b": [1.0, 2.0], "a": 8.0}})[0]


@pytest.mark.parametrize("build", [None, {}, {"isolation": "local"}])
def test_local_builder_defaults(build):
    cfg = {} if build is None else {"build": build}
    assert fingerprint(repositories={"a/b": cfg}) == fingerprint(repositories={"a/b": {}})


@pytest.mark.parametrize("build", [
    {"isolation": "vm"}, {"isolation": "vm", "cpu": 4, "memory_gb": 8},
    {"isolation": "vm", "cpu": "4", "memory_gb": "8.0"},
])
def test_vm_builder_resource_defaults(build):
    assert fingerprint(repositories={"a/b": {"build": build}}) == fingerprint(
        repositories={"a/b": {"build": {"isolation": "vm"}}})
    assert fingerprint(repositories={"a/b": {"build": {**build, "cpu": 5}}})[0] != fingerprint(
        repositories={"a/b": {"build": build}})[0]


@pytest.mark.parametrize("qa", [None, {}, {"isolation": "local"}])
def test_qa_sandbox_defaults(qa):
    assert fingerprint(settings={} if qa is None else {"qa": qa}) == fingerprint()
    assert fingerprint(repositories={"a/b": {} if qa is None else {"qa": qa}}) == fingerprint(
        repositories={"a/b": {}})


def test_app_defaults():
    minimal = {"start": "npm start"}
    explicit = {**minimal, "env": {"NODE_ENV": "production"}, "ready_path": "/", "ready_timeout": 300,
                "login": {}, "pages": [], "probes": [], "required": False}
    assert fingerprint(repositories={"a/b": {"qa": minimal}}) == fingerprint(
        repositories={"a/b": {"qa": explicit}})


@pytest.mark.parametrize("change", [
    {"system": prompts.BUILDER + "\nNew instructions"}, {"model": "model-b"}, {"harness": "anton"},
    {"settings": {"reasoning_effort": "high"}}, {"settings": {"limits": {"builder_turns": 1}}},
    {"settings": {"limits": {"run_minutes": 1}}}, {"settings": {"budgets": {"run_tokens": 1}}},
    {"settings": {"budgets": {"run_usd": 1}}}, {"settings": {"custom_policy": True}},
])
def test_relevant_inputs(change):
    assert fingerprint()[0] != fingerprint(**change)[0]


@pytest.mark.parametrize("settings", [
    {"models": {"verifier": "other", "planner": "other"}},
    {"limits": {"verifier_turns": 1, "planner_turns": 1}},
    {"pricing": {"unused": {"input": 9}}}, {"theme": "dark"},
    {"api_key": "new-secret"}, {"harness": {"verifier": "anton"}},
])
def test_irrelevant_inputs(settings):
    assert fingerprint()[0] == fingerprint(settings=settings)[0]


@pytest.mark.parametrize("role", ["builder", "verifier", "planner"])
@pytest.mark.parametrize("change", [{"context": "team notes"}, {"checks": ["pytest"]},
                                    {"qa": {"start": "npm start"}}])
def test_repository_prompt_inputs(role, change):
    assert fingerprint(role=role, repositories={"a/b": {}})[0] != fingerprint(
        role=role, repositories={"a/b": change})[0]


def test_repository_associations_and_order():
    first = {"a/b": {"context": "a"}, "a/c": {"context": "b"}}
    assert fingerprint(role="planner", repositories=first) == fingerprint(
        role="planner", repositories=dict(reversed(list(first.items()))))
    assert fingerprint(role="planner", repositories=first)[0] != fingerprint(
        role="planner", repositories={"a/b": first["a/c"], "a/c": first["a/b"]})[0]


def test_verifier_template_not_task_base():
    assert fingerprint(role="verifier", system=prompts.verifier_system("a" * 40)) == fingerprint(
        role="verifier", system=prompts.verifier_system("b" * 40))
    assert fingerprint(role="verifier", system=prompts.verifier_system("a" * 40))[0] != fingerprint(
        role="verifier", system=prompts.verifier_system("a" * 40) + "new instructions")[0]


@pytest.mark.parametrize("location", ["login", "probe", "env"])
@pytest.mark.parametrize("key", ["password", "token"])
def test_behavior_credentials_are_keyed_not_dropped(location, key, monkeypatch):
    def trial(value):
        qa = {"start": "npm start"}
        if location == "env":
            qa["env"] = {key.upper(): value}
        elif location == "login":
            qa["login"] = {"path": "/auth", "json": {key: value}}
        else:
            qa["probes"] = [{"path": "/auth", "json": {key: value}}]
        return fingerprint(repositories={"a/b": {"qa": qa}})
    before = trial("hidden-value-one")
    assert before == trial("hidden-value-one")
    assert before[0] != trial("hidden-value-two")[0]
    assert "hidden-value-one" not in json.dumps(before[1])
    assert "hmac:" in json.dumps(before[1])
    monkeypatch.setattr(versions, "_master_key", lambda: b"another-key")
    assert before[0] != trial("hidden-value-one")[0]


@pytest.mark.parametrize("key", ["max_tokens", "sort_key"])
def test_noncredential_keys_preserved(key):
    def trial(value):
        return fingerprint(repositories={"a/b": {"qa": {"start": "start", "probes": [
            {"path": "/", "json": {key: value}}]}}})
    assert trial(100)[0] != trial(1000)[0]
    assert trial(100)[1]["repositories"]["a/b"]["qa"]["probes"][0]["json"][key] == 100


def test_pricing_only_for_selected_model_when_budget_applies():
    assert fingerprint(settings={"pricing": {"model-a": {"input": 3, "output": 4}}})[0] != fingerprint()[0]
    base = {"budgets": {"run_usd": None}}
    assert fingerprint(settings=base) == fingerprint(settings={**base, "pricing": {"model-a": {"input": 3}}})


def test_whole_package_and_bridge_implementation(tmp_path, monkeypatch):
    from aifactory import vm
    package = tmp_path / "aifactory"
    package.mkdir()
    attachment = package / "attachments.py"
    attachment.write_text("first")
    bridge = tmp_path / "vmbridge"
    bridge.mkdir()
    sidecar = bridge / "sidecar.mjs"
    sidecar.write_text("first")
    (bridge / "package.json").write_text("{}")
    monkeypatch.setattr(versions, "PACKAGE_ROOT", package)
    monkeypatch.setattr(vm, "SIDECAR", sidecar)
    before = fingerprint()[0]
    attachment.write_text("second")
    after = fingerprint()[0]
    assert after != before
    sidecar.write_text("second")
    assert fingerprint()[0] != after
    after = fingerprint()[0]
    (bridge / "package.json").write_text('{"version": "2"}')
    assert fingerprint()[0] != after


def test_anton_deployed_distribution_metadata(tmp_path, monkeypatch):
    # Real importlib metadata, including direct VCS pins which may share a package version.
    dist = tmp_path / "anton_agent-0.1.dist-info"
    dist.mkdir()
    (dist / "METADATA").write_text("Name: anton-agent\nVersion: 0.1\n")
    direct = dist / "direct_url.json"
    direct.write_text(json.dumps({"vcs_info": {"commit_id": "first"}}))
    monkeypatch.syspath_prepend(str(tmp_path))
    assert metadata.distribution("anton-agent").version == "0.1"
    first = fingerprint(harness="anton")[0]
    native = fingerprint()[0]
    direct.write_text(json.dumps({"vcs_info": {"commit_id": "second"}}))
    assert fingerprint(harness="anton")[0] != first
    assert fingerprint()[0] == native
    second = fingerprint(harness="anton")[0]
    (dist / "METADATA").write_text("Name: anton-agent\nVersion: 0.2\n")
    assert fingerprint(harness="anton")[0] != second
    assert versions.runtime_version("anton")["version"] == "0.2"


def test_codex_vm_pin_not_control_plane(monkeypatch):
    from aifactory import vm
    from types import SimpleNamespace
    monkeypatch.setattr(versions.subprocess, "run", lambda *a, **kw: SimpleNamespace(stdout="local-1"))
    first_vm = fingerprint(harness="codex", sandbox_kind="vm")[0]
    first_local = fingerprint(harness="codex")[0]
    monkeypatch.setattr(versions.subprocess, "run", lambda *a, **kw: SimpleNamespace(stdout="local-2"))
    assert fingerprint(harness="codex", sandbox_kind="vm")[0] == first_vm
    assert fingerprint(harness="codex")[0] != first_local
    native = fingerprint()[0]
    monkeypatch.setattr(vm, "CODEX_VM_VERSION", "new-pin")
    assert fingerprint(harness="codex", sandbox_kind="vm")[0] != first_vm
    assert fingerprint()[0] == native


@pytest.mark.parametrize("provider,env", [("mindshub", "MINDSHUB_API_BASE"), ("openai", "OPENAI_API_BASE")])
def test_selected_endpoint_digest(provider, env, monkeypatch, client, signed_in):
    from aifactory.config import reset_settings_cache
    from aifactory.inference import resolve_provider
    from aifactory.models import Secret
    from aifactory.security import encrypt
    ws = signed_in["workspaces"][0]["id"]
    with db.session_scope() as s:
        s.add(Secret(workspace_id=ws, kind=f"{provider}_api_key", ciphertext=encrypt("key")))
    def trial(url):
        monkeypatch.setenv(env, url)
        reset_settings_cache()
        return fingerprint(provider=resolve_provider(ws, required_provider=provider))
    first = trial("https://user:password@first.test/v1?key=hidden")
    assert first[0] != trial("https://second.test/v1")[0]
    assert "https://" not in json.dumps(first[1]) and "hidden" not in json.dumps(first[1])


@pytest.mark.parametrize("role", ["planner", "builder", "verifier"])
async def test_run_persistence_override_and_immutable_snapshot(role, client, signed_in, monkeypatch):
    from aifactory.harness import HarnessResult
    from types import SimpleNamespace
    ws = signed_in["workspaces"][0]["id"]
    with db.session_scope() as s:
        o = Outcome(workspace_id=ws, number=1, title="first task")
        s.add(o)
        s.flush()
        oid = o.id
    provider = ProviderConfig("openai", "http://model.test", "key", "Test", model_override="provider-model")
    monkeypatch.setattr(runtime, "resolve_provider", lambda *a, **kw: provider)
    async def prepare(*args):
        return "", []
    monkeypatch.setattr(runtime, "prepare_attachments", prepare)
    seen = []
    class Harness:
        label = "Scripted"
        async def run(self, session):
            seen.append(session)
            return HarnessResult(report={}, turns=1)
    monkeypatch.setattr(runtime, "get_harness", lambda _: Harness())
    settings = {"models": {role: "workspace-model"}, "limits": {f"{role}_turns": 7}}
    repo_cfg = {"context": "loaded notes"}
    spec = runtime.SessionSpec(ws, oid, None, role, "native", "task one", "system", SimpleNamespace(kind="local"),
                               effective_settings=settings, repositories={"a/b": repo_cfg})
    settings["limits"][f"{role}_turns"] = 99
    repo_cfg["context"] = "edited notes"
    with pytest.raises(TypeError):
        spec.effective_settings["limits"][f"{role}_turns"] = 1
    with pytest.raises(FrozenInstanceError):
        spec.model_override = "other"
    run, _, error = await runtime.run_agent(spec, asyncio.Event())
    assert not error and run.config_version and len(run.config_version) == 16
    assert run.model == seen[-1].model == "provider-model"
    assert seen[-1].max_turns == 7
    assert run.config_snapshot["repositories"]["a/b"]["context"] == "loaded notes"
    assert serializers.run(run)["config_version"] == run.config_version
    with db.session_scope() as s:
        assert s.get(Run, run.id).config_version == run.config_version
        assert s.get(Workspace, ws).settings.get("models", {}).get(role) != "workspace-model"
    spec2 = runtime.SessionSpec(ws, oid, None, role, "native", "different task", "system", spec.sandbox,
                                effective_settings=spec.effective_settings, repositories=spec.repositories)
    second, _, _ = await runtime.run_agent(spec2, asyncio.Event())
    assert second.config_version == run.config_version
    explicit = runtime.SessionSpec(ws, oid, None, role, "native", "task", "system", spec.sandbox,
                                   effective_settings=spec.effective_settings, model_override="benchmark-model")
    third, _, _ = await runtime.run_agent(explicit, asyncio.Event())
    assert third.model == seen[-1].model == "benchmark-model" and third.config_version != run.config_version


def test_legacy_runs_table_upgrade(tmp_path):
    # Independent SQLite engine: never copy Postgres-adapted dict bindings into sqlite3.
    url = f"sqlite:///{tmp_path / 'legacy.db'}"
    engine = create_engine(url)
    ddl = str(CreateTable(Run.__table__).compile(engine))
    ddl = "\n".join(line for line in ddl.splitlines() if not any(
        name in line for name in ("config_version", "config_snapshot")))
    with engine.begin() as conn:
        conn.execute(text(ddl))
        conn.execute(text("INSERT INTO runs (id, workspace_id, outcome_id, role, harness, provider, model, attempt, "
                          "status, turns, input_tokens, output_tokens, cost_usd, cost_known, result, error, started_at) "
                          "VALUES ('old', 'ws', 'outcome', 'builder', 'native', 'openai', 'model', 1, 'succeeded', "
                          "0, 0, 0, 0, 1, :result, '', '2025-01-01 00:00:00')"), {"result": json.dumps({})})
    engine.dispose()
    upgraded = db.init_engine(url)
    assert {"config_version", "config_snapshot"} <= {c["name"] for c in inspect(upgraded).get_columns("runs")}
    with db.session_scope() as s:
        old = s.get(Run, "old")
        assert old.config_version is None and old.config_snapshot is None
        assert serializers.run(old)["config_version"] is None


def test_new_database_columns():
    with db.session_scope() as s:
        assert {"config_version", "config_snapshot"} <= {c["name"] for c in inspect(s.bind).get_columns("runs")}


async def test_runner_uses_loaded_repository_snapshot(tmp_path, monkeypatch):
    from aifactory.models import Task
    from aifactory.pipeline import planning
    from aifactory.pipeline.delivery import TaskRunner
    from .test_pipeline import factory, new_outcome

    # Use the existing scripted model and local git remote without altering pipeline helpers.
    ids = factory.__wrapped__(tmp_path, monkeypatch)
    oid = new_outcome(ids)
    await planning.plan_outcome(oid, asyncio.Event())
    with db.session_scope() as s:
        task_id = s.query(Task).filter_by(outcome_id=oid).one().id
    runner = TaskRunner(task_id, asyncio.Event(), lambda _: asyncio.Lock())
    loaded = json.loads(json.dumps(runner.cfg))
    with db.session_scope() as s:
        s.get(Repository, ids["repo"]).config = {**loaded, "context": "new notes after load", "checks": ["false"]}
    async def no_publish(*args):
        pass
    monkeypatch.setattr(runner, "_publish", no_publish)
    monkeypatch.setattr(runner, "ci_then_merge", no_publish)
    await runner.run()
    with db.session_scope() as s:
        runs = s.query(Run).filter_by(outcome_id=oid).all()
        assert {r.role for r in runs} == {"planner", "builder", "verifier"}
        for run in runs:
            assert run.config_version and serializers.run(run)["config_version"] == run.config_version
            assert run.config_snapshot["repositories"]["acme/demo"]["checks"] == loaded["checks"]
            if run.role != "planner":
                expected, _ = versions.configuration_version(
                    role=run.role, system=prompts.BUILDER if run.role == "builder" else prompts.VERIFIER,
                    harness=run.harness, provider=ProviderConfig("openai", "http://model.test", "k", "Test"),
                    model=run.model, settings=runner.settings, repositories={"acme/demo": loaded})
                assert run.config_version == expected
