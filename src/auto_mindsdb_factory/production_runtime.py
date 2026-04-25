from __future__ import annotations

import os
import shutil
import subprocess
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .automation import AutonomyMode, FactoryAutomationCoordinator
from .build_review import Stage3BuildReviewPipeline
from .connectors import (
    CodexCLICodeWorkerConfig,
    CodexCLICodeWorkerConnector,
    GitHubCLIRepoConnector,
)
from .eval_execution import CommandGateRunner, Stage5EvalPipeline
from .intake import utc_now
from .linear_trigger import LinearTriggerWorker


def env_flag(name: str, *, default: bool = False) -> bool:
    value = os.environ.get(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def intake_paused() -> bool:
    return env_flag("AI_FACTORY_INTAKE_PAUSED", default=False)


@dataclass(slots=True)
class ProductionRuntimeConfig:
    store_dir: Path
    repo_root: Path
    repository: str
    base_branch: str = "main"
    autonomy_mode: AutonomyMode = AutonomyMode.PR_READY
    interval_seconds: float = 30.0
    max_events_per_cycle: int | None = None

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
        )


class FactoryDoctor:
    REQUIRED_ENV = (
        "OPENAI_API_KEY",
        "LINEAR_API_KEY",
        "LINEAR_TARGET_TEAM_ID",
        "LINEAR_TARGET_STATE_ID",
        "LINEAR_WEBHOOK_SECRET",
        "AI_FACTORY_PUBLIC_BASE_URL",
    )

    def __init__(self, config: ProductionRuntimeConfig) -> None:
        self.config = config

    def run(self) -> dict[str, Any]:
        checks = [
            self._env_check(name) for name in self.REQUIRED_ENV
        ]
        checks.extend(
            [
                self._command_check("git", ["git", "rev-parse", "--is-inside-work-tree"]),
                self._command_check("gh", ["gh", "auth", "status"]),
                self._command_check("codex", ["codex", "--version"]),
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
        return {
            "name": f"env:{name}",
            "status": "passed" if os.environ.get(name, "").strip() else "failed",
            "summary": "set" if os.environ.get(name, "").strip() else "missing",
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
            from .automation import AutomationError

            raise AutomationError("AI_FACTORY_CODE_WORKER_PROVIDER must be 'codex_cli' for production v1.")
        trigger_result = None
        if not intake_paused():
            trigger_result = LinearTriggerWorker(
                self.config.store_dir,
                repo_root_override=self.config.repo_root,
            ).run_cycle(
                repository=self.config.repository,
                max_events=self.config.max_events_per_cycle,
            ).to_document()

        stage3 = Stage3BuildReviewPipeline(
            self.config.repo_root,
            code_worker_connector=CodexCLICodeWorkerConnector(CodexCLICodeWorkerConfig.from_env()),
            repo_connector=GitHubCLIRepoConnector(
                self.config.repo_root,
                repository=self.config.repository,
                base_branch=self.config.base_branch,
            ),
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
        )
        progression_result = coordinator.run_progression_cycle(repository=self.config.repository)
        return {
            "cycle": "factory-worker-cycle",
            "started_at": utc_now(),
            "intake_paused": intake_paused(),
            "trigger_result": trigger_result,
            "progression_result": progression_result.to_document(),
        }
