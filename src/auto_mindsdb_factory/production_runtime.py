from __future__ import annotations

import os
import shutil
import subprocess
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from uuid import UUID

from .automation import AutomationError, AutonomyMode, FactoryAutomationCoordinator
from .build_review import Stage3BuildReviewPipeline
from .connectors import (
    CodexCLICodeWorkerConfig,
    CodexCLICodeWorkerConnector,
    GitHubAPIRepoConnector,
    GitHubCLIRepoConnector,
)
from .eval_execution import CommandGateRunner, Stage5EvalPipeline
from .intake import utc_now
from .linear_trigger import LinearTriggerWorker
from .reliability import (
    OperationReaper,
    max_active_runs_from_env,
    operation_heartbeat_seconds_from_env,
    operation_stale_seconds_from_env,
    worker_id_from_env,
)


def env_flag(name: str, *, default: bool = False) -> bool:
    value = os.environ.get(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def intake_paused() -> bool:
    return env_flag("AI_FACTORY_INTAKE_PAUSED", default=False)


def simulation_runtime_allowed() -> bool:
    return env_flag("AI_FACTORY_ALLOW_SIMULATION_RUNTIME", default=False)


def repo_connector_provider() -> str:
    return os.environ.get("AI_FACTORY_REPO_CONNECTOR_PROVIDER", "github_cli").strip()


@dataclass(slots=True)
class ProductionRuntimeConfig:
    store_dir: Path
    repo_root: Path
    repository: str
    base_branch: str = "main"
    autonomy_mode: AutonomyMode = AutonomyMode.PR_READY
    interval_seconds: float = 30.0
    max_events_per_cycle: int | None = None
    worker_id: str = ""
    operation_heartbeat_seconds: float = 15.0
    operation_stale_seconds: float = 120.0
    max_active_runs: int = 1

    @classmethod
    def from_env(
        cls,
        *,
        store_dir: Path,
        repo_root: Path,
        repository: str,
        base_branch: str = "main",
        interval_seconds: float = 30.0,
        max_events_per_cycle: int | None = None,
        autonomy_mode: str | None = None,
    ) -> "ProductionRuntimeConfig":
        return cls(
            store_dir=store_dir,
            repo_root=repo_root.resolve(),
            repository=repository,
            base_branch=base_branch,
            interval_seconds=interval_seconds,
            max_events_per_cycle=max_events_per_cycle,
            autonomy_mode=AutonomyMode.from_value(
                autonomy_mode
                or os.environ.get("AI_FACTORY_AUTONOMY_MODE")
                or AutonomyMode.PR_READY.value
            ),
            worker_id=worker_id_from_env(),
            operation_heartbeat_seconds=operation_heartbeat_seconds_from_env(),
            operation_stale_seconds=operation_stale_seconds_from_env(),
            max_active_runs=max_active_runs_from_env(),
        )


class FactoryDoctor:
    REQUIRED_ENV = (
        "OPENAI_API_KEY",
        "LINEAR_API_KEY",
        "LINEAR_TARGET_TEAM_ID",
        "LINEAR_TARGET_STATE_ID",
        "LINEAR_WEBHOOK_SECRET",
        "AI_FACTORY_PUBLIC_BASE_URL",
        "AI_FACTORY_CODE_WORKER_RUN_AS_USER",
    )
    UUID_ENV = frozenset({"LINEAR_TARGET_TEAM_ID", "LINEAR_TARGET_STATE_ID"})

    def __init__(self, config: ProductionRuntimeConfig) -> None:
        self.config = config

    def run(self) -> dict[str, Any]:
        checks = [
            self._env_check(name) for name in self.REQUIRED_ENV
        ]
        checks.extend(
            [
                self._runtime_boundary_check(),
                self._command_check("git", ["git", "rev-parse", "--is-inside-work-tree"]),
                self._repo_connector_check(),
                self._code_worker_command_check(),
                self._store_check(),
                self._repo_remote_check(),
            ]
        )
        status = "passed" if all(check["status"] == "passed" for check in checks) else "failed"
        return {
            "cycle": "factory-doctor",
            "status": status,
            "autonomy_mode": self.config.autonomy_mode.value,
            "repository": self.config.repository,
            "store_dir": str(self.config.store_dir),
            "checked_at": utc_now(),
            "checks": checks,
        }

    @staticmethod
    def _env_check(name: str) -> dict[str, str]:
        value = os.environ.get(name, "").strip()
        if not value:
            return {"name": f"env:{name}", "status": "failed", "summary": "missing"}
        if name in FactoryDoctor.UUID_ENV:
            try:
                UUID(value)
            except ValueError:
                return {
                    "name": f"env:{name}",
                    "status": "failed",
                    "summary": "must be a UUID",
                }
        return {
            "name": f"env:{name}",
            "status": "passed",
            "summary": "set",
        }

    def _runtime_boundary_check(self) -> dict[str, str]:
        if self.config.autonomy_mode is AutonomyMode.PR_READY:
            return {
                "name": "runtime:simulation-boundary",
                "status": "passed",
                "summary": "production worker stops at PR-ready human merge/deploy boundary",
            }
        if simulation_runtime_allowed():
            return {
                "name": "runtime:simulation-boundary",
                "status": "passed",
                "summary": "simulation_full explicitly allowed by AI_FACTORY_ALLOW_SIMULATION_RUNTIME",
            }
        return {
            "name": "runtime:simulation-boundary",
            "status": "failed",
            "summary": (
                "factory-worker refuses simulation_full unless "
                "AI_FACTORY_ALLOW_SIMULATION_RUNTIME=true"
            ),
        }

    def _command_check(self, name: str, command: list[str]) -> dict[str, str]:
        if shutil.which(command[0]) is None:
            return {"name": f"command:{name}", "status": "failed", "summary": "not found on PATH"}
        try:
            completed = subprocess.run(
                command,
                cwd=self.config.repo_root,
                check=False,
                capture_output=True,
                text=True,
                timeout=30,
            )
        except (OSError, subprocess.TimeoutExpired) as exc:
            return {"name": f"command:{name}", "status": "failed", "summary": str(exc)}
        status = "passed" if completed.returncode == 0 else "failed"
        detail = completed.stderr.strip() or completed.stdout.strip() or f"exit {completed.returncode}"
        return {"name": f"command:{name}", "status": status, "summary": detail[:500]}

    def _code_worker_command_check(self) -> dict[str, str]:
        run_as_user = os.environ.get("AI_FACTORY_CODE_WORKER_RUN_AS_USER", "").strip()
        if not run_as_user:
            return {
                "name": "command:codex-worker-user",
                "status": "failed",
                "summary": "AI_FACTORY_CODE_WORKER_RUN_AS_USER is required in production",
            }
        sudo_bin = os.environ.get("AI_FACTORY_CODE_WORKER_SUDO_BIN", "sudo")
        codex_bin = os.environ.get("AI_FACTORY_CODE_WORKER_CODEX_BIN", "codex")
        return self._command_check(
            "codex-worker-user",
            [sudo_bin, "-H", "-u", run_as_user, "--", codex_bin, "--version"],
        )

    def _store_check(self) -> dict[str, str]:
        try:
            self.config.store_dir.mkdir(parents=True, exist_ok=True)
            probe = self.config.store_dir / ".doctor-write-probe"
            probe.write_text("ok\n", encoding="utf-8")
            probe.unlink(missing_ok=True)
        except OSError as exc:
            return {"name": "store:writable", "status": "failed", "summary": str(exc)}
        return {"name": "store:writable", "status": "passed", "summary": "run store is writable"}

    def _repo_remote_check(self) -> dict[str, str]:
        try:
            completed = subprocess.run(
                ["git", "remote", "get-url", "origin"],
                cwd=self.config.repo_root,
                check=False,
                capture_output=True,
                text=True,
                timeout=30,
            )
        except (OSError, subprocess.TimeoutExpired) as exc:
            return {"name": "git:origin", "status": "failed", "summary": str(exc)}
        summary = completed.stdout.strip() or completed.stderr.strip()
        return {
            "name": "git:origin",
            "status": "passed" if completed.returncode == 0 else "failed",
            "summary": summary,
        }

    def _repo_connector_check(self) -> dict[str, str]:
        provider = repo_connector_provider()
        if provider == "github_cli":
            return self._command_check("gh", ["gh", "auth", "status"])
        if provider == "github_api":
            token = os.environ.get("GITHUB_TOKEN") or os.environ.get("GH_TOKEN")
            if not token:
                return {
                    "name": "repo-connector:github-api",
                    "status": "failed",
                    "summary": "GITHUB_TOKEN or GH_TOKEN is required",
                }
            return {
                "name": "repo-connector:github-api",
                "status": "passed",
                "summary": "token configured",
            }
        return {
            "name": "repo-connector",
            "status": "failed",
            "summary": "AI_FACTORY_REPO_CONNECTOR_PROVIDER must be github_cli or github_api",
        }


class FactoryWorker:
    def __init__(self, config: ProductionRuntimeConfig) -> None:
        self.config = config

    def run(self, *, once: bool = False, max_cycles: int | None = None) -> dict[str, Any]:
        cycles: list[dict[str, Any]] = []
        cycle_count = 0
        while True:
            cycle_count += 1
            cycles.append(self.run_cycle())
            if once or (max_cycles is not None and cycle_count >= max_cycles):
                break
            time.sleep(self.config.interval_seconds)
        return {
            "cycle": "factory-worker",
            "status": "completed" if once or max_cycles is not None else "running",
            "cycles": cycles,
            "completed_at": utc_now(),
        }

    def run_cycle(self) -> dict[str, Any]:
        provider = os.environ.get("AI_FACTORY_CODE_WORKER_PROVIDER", "codex_cli").strip()
        if provider != "codex_cli":
            raise AutomationError("AI_FACTORY_CODE_WORKER_PROVIDER must be 'codex_cli' for production v1.")
        if (
            self.config.autonomy_mode is AutonomyMode.SIMULATION_FULL
            and not simulation_runtime_allowed()
        ):
            raise AutomationError(
                "factory-worker is a production-facing runtime and refuses "
                "AI_FACTORY_AUTONOMY_MODE=simulation_full unless "
                "AI_FACTORY_ALLOW_SIMULATION_RUNTIME=true is set explicitly."
            )

        stage3 = Stage3BuildReviewPipeline(
            self.config.repo_root,
            code_worker_connector=CodexCLICodeWorkerConnector(CodexCLICodeWorkerConfig.from_env()),
            repo_connector=self._repo_connector(),
        )
        stage5 = Stage5EvalPipeline(
            self.config.repo_root,
            gate_runner=CommandGateRunner.from_env(self.config.repo_root),
        )
        coordinator = FactoryAutomationCoordinator(
            self.config.store_dir,
            repo_root_override=self.config.repo_root,
            stage3_pipeline=stage3,
            stage5_pipeline=stage5,
            autonomy_mode=self.config.autonomy_mode,
            worker_id=self.config.worker_id,
            operation_heartbeat_seconds=self.config.operation_heartbeat_seconds,
            operation_stale_seconds=self.config.operation_stale_seconds,
            max_active_runs=self.config.max_active_runs,
        )
        reaper_result = OperationReaper(
            self.config.store_dir,
            stale_seconds=self.config.operation_stale_seconds,
            linear_sync=coordinator.linear_workflow_sync,
        ).run()
        trigger_result = None
        if not intake_paused():
            trigger_result = LinearTriggerWorker(
                self.config.store_dir,
                repo_root_override=self.config.repo_root,
                coordinator=coordinator,
            ).run_cycle(
                repository=self.config.repository,
                max_events=self.config.max_events_per_cycle,
            ).to_document()
        progression_result = coordinator.run_progression_cycle(repository=self.config.repository)
        return {
            "cycle": "factory-worker-cycle",
            "started_at": utc_now(),
            "intake_paused": intake_paused(),
            "worker_id": self.config.worker_id,
            "max_active_runs": self.config.max_active_runs,
            "reaper_result": reaper_result.to_document(),
            "trigger_result": trigger_result,
            "progression_result": progression_result.to_document(),
        }

    def _repo_connector(self):
        provider = repo_connector_provider()
        if provider == "github_cli":
            return GitHubCLIRepoConnector(
                self.config.repo_root,
                repository=self.config.repository,
                base_branch=self.config.base_branch,
            )
        if provider == "github_api":
            return GitHubAPIRepoConnector(
                self.config.repo_root,
                repository=self.config.repository,
                base_branch=self.config.base_branch,
            )
        raise AutomationError(
            "AI_FACTORY_REPO_CONNECTOR_PROVIDER must be 'github_cli' or 'github_api'."
        )
