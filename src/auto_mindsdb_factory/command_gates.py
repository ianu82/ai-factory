from __future__ import annotations

import os
import pwd
import re
import shlex
import subprocess
import tempfile
import time
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterator

from .execution_account import ExecutionAccount, ExecutionAccountError

class CommandGateError(RuntimeError):
    """A command gate could not safely prepare its evaluation target."""


@dataclass(slots=True)
class GateResult:
    check_kind: str
    status: str
    command: list[str] | None
    exit_code: int | None
    stdout: str
    stderr: str
    duration_seconds: int
    summary: str

    def to_document(self) -> dict[str, Any]:
        return {
            "check_kind": self.check_kind,
            "status": self.status,
            "command": None if self.command is None else list(self.command),
            "exit_code": self.exit_code,
            "stdout": self.stdout,
            "stderr": self.stderr,
            "duration_seconds": self.duration_seconds,
            "summary": self.summary,
        }


class CommandGateRunner:
    """Run the subset of eval checks that are backed by real local commands."""

    MAX_CAPTURED_OUTPUT_CHARS = 4000
    DEFAULT_COMMANDS = {
        "unit": ["uv", "run", "--locked", "--extra", "dev", "python", "-m", "pytest", "-q"],
        "contract": ["uv", "run", "--locked", "--extra", "dev", "python", "scripts/validate_contracts.py"],
    }
    ENV_COMMANDS = {
        "lint": "AI_FACTORY_GATE_LINT_COMMAND",
        "typecheck": "AI_FACTORY_GATE_TYPECHECK_COMMAND",
        "unit": "AI_FACTORY_GATE_UNIT_COMMAND",
        "contract": "AI_FACTORY_GATE_CONTRACT_COMMAND",
        "integration": "AI_FACTORY_GATE_INTEGRATION_COMMAND",
        "migration_safety": "AI_FACTORY_GATE_MIGRATION_SAFETY_COMMAND",
        "latency": "AI_FACTORY_GATE_LATENCY_COMMAND",
        "cost": "AI_FACTORY_GATE_COST_COMMAND",
        "llm_quality": "AI_FACTORY_GATE_LLM_QUALITY_COMMAND",
        "adversarial": "AI_FACTORY_GATE_ADVERSARIAL_COMMAND",
    }

    def __init__(
        self,
        repo_root: Path,
        *,
        commands_by_kind: dict[str, list[str]] | None = None,
        required_kinds: set[str] | None = None,
        timeout_seconds: int = 900,
        require_candidate: bool = False,
        execution_account: ExecutionAccount | None = None,
    ) -> None:
        if timeout_seconds < 1:
            raise CommandGateError("Gate timeout_seconds must be positive.")
        self.repo_root = repo_root.resolve()
        self.commands_by_kind = dict(self.DEFAULT_COMMANDS if commands_by_kind is None else commands_by_kind)
        self.required_kinds = {"unit", "contract"} if required_kinds is None else set(required_kinds)
        self.timeout_seconds = timeout_seconds
        self.require_candidate = require_candidate
        self.commit_sha: str | None = None
        self.execution_account = execution_account or ExecutionAccount(None)

    @classmethod
    def from_env(cls, repo_root: Path, *, require_candidate: bool = False) -> "CommandGateRunner":
        commands = dict(cls.DEFAULT_COMMANDS)
        for kind, env_name in cls.ENV_COMMANDS.items():
            raw = os.environ.get(env_name, "").strip()
            if raw:
                commands[kind] = shlex.split(raw)
        required_raw = os.environ.get("AI_FACTORY_REQUIRED_GATE_KINDS", "unit,contract")
        required = {item.strip() for item in required_raw.split(",") if item.strip()}
        timeout = int(os.environ.get("AI_FACTORY_GATE_TIMEOUT_SECONDS", "900"))
        user = os.environ.get("AI_FACTORY_GATE_RUN_AS_USER") or os.environ.get("AI_FACTORY_CODE_WORKER_RUN_AS_USER")
        if require_candidate and not user:
            raise CommandGateError("Production gates require AI_FACTORY_GATE_RUN_AS_USER or AI_FACTORY_CODE_WORKER_RUN_AS_USER.")
        if require_candidate:
            try:
                uid = int(user.lstrip("#")) if user.lstrip("#").isdigit() else pwd.getpwnam(user).pw_uid
            except KeyError as exc:
                raise CommandGateError(f"Gate execution account does not exist: {user}") from exc
            if uid in {0, os.getuid()}:
                raise CommandGateError("Production gates must use a non-root account distinct from the orchestrator.")
        return cls(
            repo_root, commands_by_kind=commands, required_kinds=required,
            timeout_seconds=timeout, require_candidate=require_candidate,
            execution_account=ExecutionAccount(user, sudo_bin=os.environ.get("AI_FACTORY_GATE_SUDO_BIN", "sudo")),
        )

    @contextmanager
    def for_pr(self, pr_packet: dict[str, Any]) -> Iterator["CommandGateRunner"]:
        evidence = pr_packet.get("delivery_evidence")
        if not evidence:
            if self.require_candidate:
                raise CommandGateError("Production evals require candidate delivery evidence.")
            yield self
            return
        commit_sha = evidence.get("commit_sha", "")
        if not isinstance(commit_sha, str) or not re.fullmatch(r"[0-9a-f]{40}", commit_sha):
            raise CommandGateError("Candidate evals require a full 40-character commit SHA.")
        with candidate_workspace(self.repo_root, commit_sha) as workspace:
            runner = CommandGateRunner(
                workspace, commands_by_kind=self.commands_by_kind,
                required_kinds=self.required_kinds, timeout_seconds=self.timeout_seconds,
                require_candidate=True,
                execution_account=self.execution_account,
            )
            runner.commit_sha = commit_sha
            try:
                prepared = self.execution_account.prepare_directory(workspace.parent)
                try:
                    yield runner
                finally:
                    if prepared:
                        self.execution_account.restore_directory(workspace.parent)
            except ExecutionAccountError as exc:
                raise CommandGateError(str(exc)) from exc

    def is_blocking_kind(self, kind: str) -> bool:
        return kind in self.required_kinds

    def run_check(self, check: dict[str, Any]) -> GateResult:
        kind = str(check["kind"])
        command = self.commands_by_kind.get(kind)
        if not command:
            return GateResult(
                check_kind=kind,
                status="not_configured",
                command=None,
                exit_code=None,
                stdout="",
                stderr="",
                duration_seconds=0,
                summary=f"No command-backed gate is configured for {kind}.",
            )
        try:
            started = time.monotonic()
            env = self._command_env()
            execution_command = command
            if self.execution_account.user:
                # sudo resets the environment; pass only the candidate import path
                # after the identity change, never the service environment.
                execution_command = self.execution_account.command([
                    "env", f"PYTHONPATH={env['PYTHONPATH']}", *command,
                ])
            completed = subprocess.run(
                execution_command,
                cwd=self.repo_root,
                check=False,
                capture_output=True,
                text=True,
                timeout=min(self.timeout_seconds, int(check["timeout_minutes"]) * 60),
                env=self._command_env(),
            )
            duration = max(0, int(round(time.monotonic() - started)))
            status = "passed" if completed.returncode == 0 else "failed"
            return GateResult(
                check_kind=kind,
                status=status,
                command=list(command),
                exit_code=completed.returncode,
                stdout=self._captured_output(completed.stdout),
                stderr=self._captured_output(completed.stderr),
                duration_seconds=duration,
                summary=(
                    f"Command-backed gate passed: {' '.join(command)}."
                    if status == "passed"
                    else f"Command-backed gate failed: {' '.join(command)}."
                ),
            )
        except subprocess.TimeoutExpired as exc:
            return GateResult(
                check_kind=kind,
                status="failed",
                command=list(command),
                exit_code=124,
                stdout=self._captured_output(exc.stdout if isinstance(exc.stdout, str) else ""),
                stderr=self._captured_output(exc.stderr if isinstance(exc.stderr, str) else str(exc)),
                duration_seconds=min(self.timeout_seconds, int(check["timeout_minutes"]) * 60),
                summary=f"Command-backed gate timed out: {' '.join(command)}.",
            )
        except OSError as exc:
            return GateResult(
                check_kind=kind,
                status="failed",
                command=list(command),
                exit_code=127,
                stdout="",
                stderr=str(exc),
                duration_seconds=0,
                summary=f"Command-backed gate could not start: {' '.join(command)}.",
            )

    @classmethod
    def _captured_output(cls, output: str) -> str:
        if len(output) <= cls.MAX_CAPTURED_OUTPUT_CHARS:
            return output
        return output[-cls.MAX_CAPTURED_OUTPUT_CHARS :]

    def _command_env(self) -> dict[str, str]:
        # Gates execute candidate-authored code. Do not pass service credentials
        # or import paths from the orchestrator's editable installation.
        env = {
            name: value for name, value in os.environ.items()
            if not re.search(r"(?:API_KEY|TOKEN|SECRET|PASSWORD|ACCESS_KEY(?:_ID)?|SESSION_TOKEN)$", name)
            and name not in {
                "PYTHONHOME", "OPENAI_ORGANIZATION", "OPENAI_PROJECT", "VIRTUAL_ENV",
                "UV_PROJECT", "UV_PROJECT_ENVIRONMENT", "UV_WORKING_DIRECTORY",
            }
        }
        env["PYTHONPATH"] = os.pathsep.join(
            str(path) for path in (self.repo_root / "src", self.repo_root) if path.is_dir()
        )
        return env


def _git(repo_root: Path, *args: str) -> str:
    try:
        result = subprocess.run(
            ["git", "-C", str(repo_root), *args], check=True,
            capture_output=True, text=True, timeout=120,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        detail = exc.stderr.strip() if isinstance(exc, subprocess.CalledProcessError) else str(exc)
        raise CommandGateError(f"Could not prepare candidate eval workspace: {detail}") from exc
    return result.stdout.strip()


@contextmanager
def candidate_workspace(repo_root: Path, commit_sha: str) -> Iterator[Path]:
    """Evaluate a pinned commit without switching or dirtying the service checkout."""
    try:
        _git(repo_root, "cat-file", "-e", f"{commit_sha}^{{commit}}")
    except CommandGateError:
        _git(repo_root, "fetch", "--no-tags", "origin", commit_sha)
    with tempfile.TemporaryDirectory(prefix="ai-factory-eval-") as temporary_dir:
        workspace = Path(temporary_dir) / "repo"
        _git(repo_root, "worktree", "add", "--detach", str(workspace), commit_sha)
        try:
            if _git(workspace, "rev-parse", "HEAD") != commit_sha:
                raise CommandGateError("Candidate eval workspace does not match the requested commit.")
            yield workspace
            if _git(workspace, "rev-parse", "HEAD") != commit_sha:
                raise CommandGateError("Candidate HEAD changed while gates were running.")
        finally:
            _git(repo_root, "worktree", "remove", "--force", str(workspace))


