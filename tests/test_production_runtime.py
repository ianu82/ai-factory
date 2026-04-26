from __future__ import annotations

from auto_mindsdb_factory.production_runtime import FactoryDoctor
from auto_mindsdb_factory.production_runtime import FactoryWorker
from auto_mindsdb_factory.production_runtime import ProductionRuntimeConfig


def test_factory_doctor_env_check_rejects_malformed_linear_ids(monkeypatch) -> None:
    monkeypatch.setenv(
        "LINEAR_TARGET_STATE_ID",
        "LINEAR_TARGET_STATE_ID=17fa210f-6809-46cf-8e51-6bfa69a25cfe",
    )

    check = FactoryDoctor._env_check("LINEAR_TARGET_STATE_ID")

    assert check == {
        "name": "env:LINEAR_TARGET_STATE_ID",
        "status": "failed",
        "summary": "must be a UUID",
    }


def test_factory_doctor_env_check_accepts_linear_uuid(monkeypatch) -> None:
    monkeypatch.setenv("LINEAR_TARGET_STATE_ID", "17fa210f-6809-46cf-8e51-6bfa69a25cfe")

    check = FactoryDoctor._env_check("LINEAR_TARGET_STATE_ID")

    assert check == {
        "name": "env:LINEAR_TARGET_STATE_ID",
        "status": "passed",
        "summary": "set",
    }


def test_factory_doctor_requires_code_worker_os_user(monkeypatch, tmp_path) -> None:
    monkeypatch.delenv("AI_FACTORY_CODE_WORKER_RUN_AS_USER", raising=False)
    doctor = FactoryDoctor(
        ProductionRuntimeConfig(
            store_dir=tmp_path / "store",
            repo_root=tmp_path,
            repository="ianu82/ai-factory",
        )
    )

    check = doctor._code_worker_command_check()

    assert check == {
        "name": "command:codex-worker-user",
        "status": "failed",
        "summary": "AI_FACTORY_CODE_WORKER_RUN_AS_USER is required in production",
    }


def test_factory_doctor_checks_code_worker_under_target_user(monkeypatch, tmp_path) -> None:
    captured: dict[str, object] = {}
    monkeypatch.setenv("AI_FACTORY_CODE_WORKER_RUN_AS_USER", "ai-factory-worker")
    monkeypatch.setenv("AI_FACTORY_CODE_WORKER_CODEX_BIN", "/usr/bin/codex")
    doctor = FactoryDoctor(
        ProductionRuntimeConfig(
            store_dir=tmp_path / "store",
            repo_root=tmp_path,
            repository="ianu82/ai-factory",
        )
    )

    def fake_command_check(name: str, command: list[str]) -> dict[str, str]:
        captured["name"] = name
        captured["command"] = command
        return {"name": f"command:{name}", "status": "passed", "summary": "ok"}

    monkeypatch.setattr(doctor, "_command_check", fake_command_check)

    check = doctor._code_worker_command_check()

    assert check["status"] == "passed"
    assert captured["name"] == "codex-worker-user"
    assert captured["command"] == [
        "sudo",
        "-H",
        "-u",
        "ai-factory-worker",
        "--",
        "/usr/bin/codex",
        "--version",
    ]


def test_factory_worker_uses_production_coordinator_for_linear_handoff(
    monkeypatch,
    tmp_path,
) -> None:
    created: dict[str, object] = {}

    class _FakeStage3:
        def __init__(self, root, *, code_worker_connector=None, repo_connector=None) -> None:
            self.root = root
            self.code_worker_connector = code_worker_connector
            self.repo_connector = repo_connector

    class _FakeStage5:
        def __init__(self, root, *, gate_runner=None) -> None:
            self.root = root
            self.gate_runner = gate_runner

    class _FakeCodeWorkerConfig:
        @classmethod
        def from_env(cls):
            return cls()

    class _FakeCodeWorker:
        def __init__(self, config) -> None:
            self.config = config

    class _FakeRepoConnector:
        def __init__(self, root, *, repository: str, base_branch: str) -> None:
            self.root = root
            self.repository = repository
            self.base_branch = base_branch

    class _FakeGateRunner:
        @classmethod
        def from_env(cls, root):
            return cls()

    class _FakeCoordinator:
        def __init__(
            self,
            store_dir,
            *,
            repo_root_override=None,
            stage3_pipeline=None,
            stage5_pipeline=None,
            autonomy_mode=None,
        ) -> None:
            self.stage3_pipeline = stage3_pipeline
            self.stage5_pipeline = stage5_pipeline
            created["coordinator"] = self

        def run_progression_cycle(self, *, repository: str):
            class _Result:
                def to_document(self):
                    return {"cycle": "progression"}

            return _Result()

    class _FakeTriggerWorker:
        def __init__(self, store_dir, *, repo_root_override=None, coordinator=None) -> None:
            created["trigger_coordinator"] = coordinator

        def run_cycle(self, *, repository: str, max_events=None):
            class _Result:
                def to_document(self):
                    return {"cycle": "linear-trigger"}

            return _Result()

    monkeypatch.setenv("AI_FACTORY_CODE_WORKER_PROVIDER", "codex_cli")
    monkeypatch.setattr("auto_mindsdb_factory.production_runtime.Stage3BuildReviewPipeline", _FakeStage3)
    monkeypatch.setattr("auto_mindsdb_factory.production_runtime.Stage5EvalPipeline", _FakeStage5)
    monkeypatch.setattr("auto_mindsdb_factory.production_runtime.CodexCLICodeWorkerConfig", _FakeCodeWorkerConfig)
    monkeypatch.setattr("auto_mindsdb_factory.production_runtime.CodexCLICodeWorkerConnector", _FakeCodeWorker)
    monkeypatch.setattr("auto_mindsdb_factory.production_runtime.GitHubCLIRepoConnector", _FakeRepoConnector)
    monkeypatch.setattr("auto_mindsdb_factory.production_runtime.CommandGateRunner", _FakeGateRunner)
    monkeypatch.setattr("auto_mindsdb_factory.production_runtime.FactoryAutomationCoordinator", _FakeCoordinator)
    monkeypatch.setattr("auto_mindsdb_factory.production_runtime.LinearTriggerWorker", _FakeTriggerWorker)

    config = ProductionRuntimeConfig(
        store_dir=tmp_path / "store",
        repo_root=tmp_path,
        repository="ianu82/ai-factory",
    )

    result = FactoryWorker(config).run_cycle()

    assert result["trigger_result"] == {"cycle": "linear-trigger"}
    assert created["trigger_coordinator"] is created["coordinator"]
    coordinator = created["coordinator"]
    assert coordinator.stage3_pipeline.code_worker_connector is not None
    assert coordinator.stage3_pipeline.repo_connector.repository == "ianu82/ai-factory"
    assert coordinator.stage5_pipeline.gate_runner is not None
