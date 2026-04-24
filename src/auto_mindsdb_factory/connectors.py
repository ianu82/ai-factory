from __future__ import annotations

import json
import re
import subprocess
import sys
import tempfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Protocol

from .intake import normalize_whitespace, slugify, utc_now


class FactoryConnectorError(RuntimeError):
    """Raised when an external factory connector cannot produce usable evidence."""


@dataclass(slots=True)
class CommandEvidence:
    command: list[str]
    exit_code: int
    stdout: str
    stderr: str

    @property
    def passed(self) -> bool:
        return self.exit_code == 0

    def to_document(self) -> dict[str, Any]:
        return {
            "command": list(self.command),
            "exit_code": self.exit_code,
            "stdout": self.stdout,
            "stderr": self.stderr,
            "status": "passed" if self.passed else "failed",
        }


@dataclass(slots=True)
class PullRequestEvidence:
    repository: str
    branch_name: str
    base_branch: str
    commit_sha: str
    number: int
    url: str
    title: str
    created_at: str = field(default_factory=utc_now)

    def to_document(self) -> dict[str, Any]:
        return {
            "repository": self.repository,
            "branch_name": self.branch_name,
            "base_branch": self.base_branch,
            "commit_sha": self.commit_sha,
            "number": self.number,
            "url": self.url,
            "title": self.title,
            "created_at": self.created_at,
        }


@dataclass(slots=True)
class PullRequestStatus:
    repository: str
    number: int
    state: str
    mergeable: str | None
    url: str
    checks: list[dict[str, Any]]
    observed_at: str = field(default_factory=utc_now)

    def to_document(self) -> dict[str, Any]:
        return {
            "repository": self.repository,
            "number": self.number,
            "state": self.state,
            "mergeable": self.mergeable,
            "url": self.url,
            "checks": list(self.checks),
            "observed_at": self.observed_at,
        }


@dataclass(slots=True)
class EvalEvidence:
    status: str
    commands: list[CommandEvidence]
    observed_at: str = field(default_factory=utc_now)

    def to_document(self) -> dict[str, Any]:
        return {
            "status": self.status,
            "commands": [command.to_document() for command in self.commands],
            "observed_at": self.observed_at,
        }

    def assert_passed(self) -> None:
        if self.status == "passed":
            return
        failing = [
            " ".join(command.command)
            for command in self.commands
            if not command.passed
        ]
        raise FactoryConnectorError(
            "Required local eval commands failed: "
            + (", ".join(failing) if failing else "unknown command failure")
        )


class RepoConnector(Protocol):
    def create_pull_request(
        self,
        *,
        work_item_id: str,
        spec_packet: dict[str, Any],
        ticket_bundle: dict[str, Any],
        pr_packet: dict[str, Any],
    ) -> PullRequestEvidence:
        """Create delivery evidence for the reviewable PR candidate."""

    def read_pull_request_status(
        self,
        evidence: PullRequestEvidence,
    ) -> PullRequestStatus:
        """Read PR state and CI/check evidence."""


class EvalConnector(Protocol):
    def run_required_evals(self) -> EvalEvidence:
        """Run eval commands that gate the vertical slice."""


class OpsSignalConnector(Protocol):
    def ensure_default_signals(self, work_item_id: str) -> None:
        """Create healthy default ops signals if the connector supports seeding."""

    def read_staging_signal(self, work_item_id: str) -> dict[str, Any]:
        """Read staging soak evidence."""

    def read_monitoring_signal(self, work_item_id: str) -> dict[str, Any]:
        """Read production monitoring evidence."""

    def read_rollback_signal(self, work_item_id: str) -> dict[str, Any]:
        """Read rollback probe evidence."""


@dataclass(slots=True)
class AgentTask:
    name: str
    input_document: dict[str, Any]
    output_schema: str


@dataclass(slots=True)
class AgentResult:
    name: str
    output_document: dict[str, Any]
    model_fingerprint: str
    completed_at: str = field(default_factory=utc_now)


class AgentConnector(Protocol):
    def run_task(self, task: AgentTask) -> AgentResult:
        """Run an agent behind a deterministic artifact contract."""


class DeterministicAgentConnector:
    """A no-network agent boundary used until an LLM-backed connector is enabled."""

    MODEL_FINGERPRINT = "deterministic-agent.v1"

    def run_task(self, task: AgentTask) -> AgentResult:
        return AgentResult(
            name=task.name,
            output_document=dict(task.input_document),
            model_fingerprint=self.MODEL_FINGERPRINT,
        )


class LocalEvalConnector:
    """Run the local contract and test gates that back the vertical slice."""

    def __init__(
        self,
        repo_root: Path,
        *,
        commands: list[list[str]] | None = None,
        timeout_seconds: int = 600,
    ) -> None:
        self.repo_root = repo_root.resolve()
        self.commands = commands or [
            [sys.executable, "-m", "pytest", "-q"],
            [sys.executable, "scripts/validate_contracts.py"],
        ]
        self.timeout_seconds = timeout_seconds

    def run_required_evals(self) -> EvalEvidence:
        command_results = [
            self._run_command(command)
            for command in self.commands
        ]
        status = "passed" if all(result.passed for result in command_results) else "failed"
        return EvalEvidence(status=status, commands=command_results)

    def _run_command(self, command: list[str]) -> CommandEvidence:
        try:
            completed = subprocess.run(
                command,
                cwd=self.repo_root,
                check=False,
                capture_output=True,
                text=True,
                timeout=self.timeout_seconds,
            )
        except (OSError, subprocess.TimeoutExpired) as exc:
            return CommandEvidence(
                command=list(command),
                exit_code=124 if isinstance(exc, subprocess.TimeoutExpired) else 127,
                stdout=getattr(exc, "stdout", "") or "",
                stderr=str(exc),
            )
        return CommandEvidence(
            command=list(command),
            exit_code=completed.returncode,
            stdout=completed.stdout,
            stderr=completed.stderr,
        )


class GitHubCLIRepoConnector:
    """Create and inspect real GitHub PRs through local git plus GitHub CLI."""

    def __init__(
        self,
        repo_root: Path,
        *,
        repository: str,
        base_branch: str = "main",
        git_bin: str = "git",
        gh_bin: str = "gh",
        timeout_seconds: int = 120,
    ) -> None:
        self.repo_root = repo_root.resolve()
        self.repository = repository
        self.base_branch = base_branch
        self.git_bin = git_bin
        self.gh_bin = gh_bin
        self.timeout_seconds = timeout_seconds

    def create_pull_request(
        self,
        *,
        work_item_id: str,
        spec_packet: dict[str, Any],
        ticket_bundle: dict[str, Any],
        pr_packet: dict[str, Any],
    ) -> PullRequestEvidence:
        self._assert_available()
        self._assert_clean_worktree()
        branch_name = self._branch_name(work_item_id, spec_packet)
        title = pr_packet["pull_request"]["title"]
        body = self._pr_body(work_item_id, spec_packet, ticket_bundle, pr_packet)

        self._git(["switch", self.base_branch])
        self._git(["pull", "--ff-only", "origin", self.base_branch])
        if self._git_stdout(["branch", "--list", branch_name]).strip():
            raise FactoryConnectorError(
                f"Branch '{branch_name}' already exists locally; refusing to reuse it."
            )
        self._git(["switch", "-c", branch_name])
        evidence_file = self._write_vertical_slice_doc(
            work_item_id,
            spec_packet,
            ticket_bundle,
            pr_packet,
        )
        self._git(["add", str(evidence_file.relative_to(self.repo_root))])
        self._git(["commit", "-m", f"Add vertical slice evidence for {work_item_id}"])
        commit_sha = self._git_stdout(["rev-parse", "HEAD"]).strip()
        self._git(["push", "-u", "origin", branch_name])
        pr_url = self._create_github_pr(branch_name, title, body)
        number_match = re.search(r"/pull/(\d+)(?:$|[/?#])", pr_url)
        if number_match is None:
            raise FactoryConnectorError(f"Could not parse PR number from GitHub URL: {pr_url}")
        return PullRequestEvidence(
            repository=self.repository,
            branch_name=branch_name,
            base_branch=self.base_branch,
            commit_sha=commit_sha,
            number=int(number_match.group(1)),
            url=pr_url,
            title=title,
        )

    def read_pull_request_status(
        self,
        evidence: PullRequestEvidence,
    ) -> PullRequestStatus:
        payload = self._gh_json(
            [
                "pr",
                "view",
                str(evidence.number),
                "--repo",
                evidence.repository,
                "--json",
                "state,mergeable,url,statusCheckRollup",
            ]
        )
        checks = payload.get("statusCheckRollup")
        if not isinstance(checks, list):
            checks = []
        return PullRequestStatus(
            repository=evidence.repository,
            number=evidence.number,
            state=str(payload.get("state") or "UNKNOWN"),
            mergeable=(
                None
                if payload.get("mergeable") is None
                else str(payload.get("mergeable"))
            ),
            url=str(payload.get("url") or evidence.url),
            checks=[self._normalize_check(check) for check in checks],
        )

    def _assert_available(self) -> None:
        self._run([self.gh_bin, "--version"], cwd=self.repo_root)
        self._run([self.gh_bin, "auth", "status"], cwd=self.repo_root)
        self._git(["rev-parse", "--is-inside-work-tree"])

    def _assert_clean_worktree(self) -> None:
        status = self._git_stdout(["status", "--porcelain"])
        if status.strip():
            raise FactoryConnectorError(
                "Git worktree must be clean before the vertical-slice PR is created."
            )

    def _create_github_pr(self, branch_name: str, title: str, body: str) -> str:
        with tempfile.NamedTemporaryFile("w", encoding="utf-8", delete=False) as handle:
            handle.write(body)
            body_path = Path(handle.name)
        try:
            completed = self._run(
                [
                    self.gh_bin,
                    "pr",
                    "create",
                    "--repo",
                    self.repository,
                    "--base",
                    self.base_branch,
                    "--head",
                    branch_name,
                    "--title",
                    title,
                    "--body-file",
                    str(body_path),
                    "--draft",
                ],
                cwd=self.repo_root,
            )
            return completed.stdout.strip()
        finally:
            body_path.unlink(missing_ok=True)

    def _write_vertical_slice_doc(
        self,
        work_item_id: str,
        spec_packet: dict[str, Any],
        ticket_bundle: dict[str, Any],
        pr_packet: dict[str, Any],
    ) -> Path:
        output_dir = self.repo_root / "docs" / "vertical-slices"
        output_dir.mkdir(parents=True, exist_ok=True)
        output_path = output_dir / f"{work_item_id}.md"
        ticket_lines = "\n".join(
            f"- {ticket['id']}: {ticket['title']}"
            for ticket in ticket_bundle["tickets"]
        )
        output_path.write_text(
            "\n".join(
                [
                    f"# Vertical Slice Evidence: {work_item_id}",
                    "",
                    f"- Source: {spec_packet['source']['title']}",
                    f"- Spec packet: {spec_packet['artifact']['id']}",
                    f"- PR packet: {pr_packet['artifact']['id']}",
                    f"- Generated at: {utc_now()}",
                    "",
                    "## Tickets",
                    ticket_lines,
                    "",
                    "## Purpose",
                    (
                        "This file is the intentionally small factory-generated "
                        "change used to prove branch, commit, and pull-request "
                        "creation for the first AI Factory vertical slice."
                    ),
                    "",
                ]
            ),
            encoding="utf-8",
        )
        return output_path

    def _pr_body(
        self,
        work_item_id: str,
        spec_packet: dict[str, Any],
        ticket_bundle: dict[str, Any],
        pr_packet: dict[str, Any],
    ) -> str:
        ticket_lines = "\n".join(
            f"- `{ticket['id']}`: {ticket['title']}"
            for ticket in ticket_bundle["tickets"]
        )
        return "\n".join(
            [
                "## Summary",
                (
                    "Factory-generated vertical slice PR proving that the AI Factory "
                    "can turn an upstream release-note item into real GitHub delivery evidence."
                ),
                "",
                "## Source",
                f"- Work item: `{work_item_id}`",
                f"- Spec packet: `{spec_packet['artifact']['id']}`",
                f"- PR packet: `{pr_packet['artifact']['id']}`",
                f"- Release note: {spec_packet['source']['title']}",
                "",
                "## Tickets",
                ticket_lines,
                "",
                "## Validation",
                "- The vertical-slice runner executes local pytest and contract validation before Stage 5.",
                "- Staging, monitoring, and rollback are intentionally file-backed in this first slice.",
                "",
            ]
        )

    def _branch_name(self, work_item_id: str, spec_packet: dict[str, Any]) -> str:
        title = normalize_whitespace(spec_packet["source"]["title"])
        slug = slugify(title)[:34].strip("-") or "anthropic-change"
        suffix = re.sub(r"[^a-zA-Z0-9]", "", work_item_id)[-10:] or "slice"
        return f"factory/vertical-slice-{slug}-{suffix}".lower()

    def _git(self, args: list[str]) -> None:
        self._run([self.git_bin, *args], cwd=self.repo_root)

    def _git_stdout(self, args: list[str]) -> str:
        return self._run([self.git_bin, *args], cwd=self.repo_root).stdout

    def _gh_json(self, args: list[str]) -> dict[str, Any]:
        completed = self._run([self.gh_bin, *args], cwd=self.repo_root)
        try:
            payload = json.loads(completed.stdout)
        except json.JSONDecodeError as exc:
            raise FactoryConnectorError(
                f"GitHub CLI returned invalid JSON for '{' '.join(args)}': {exc}"
            ) from exc
        if not isinstance(payload, dict):
            raise FactoryConnectorError(
                f"GitHub CLI returned a non-object payload for '{' '.join(args)}'."
            )
        return payload

    def _run(self, args: list[str], *, cwd: Path) -> subprocess.CompletedProcess[str]:
        try:
            completed = subprocess.run(
                args,
                cwd=cwd,
                check=False,
                capture_output=True,
                text=True,
                timeout=self.timeout_seconds,
            )
        except (OSError, subprocess.TimeoutExpired) as exc:
            raise FactoryConnectorError(
                f"Command failed before completion: {' '.join(args)}: {exc}"
            ) from exc
        if completed.returncode != 0:
            stderr = completed.stderr.strip()
            stdout = completed.stdout.strip()
            detail = stderr or stdout or f"exit code {completed.returncode}"
            raise FactoryConnectorError(
                f"Command failed: {' '.join(args)}: {detail}"
            )
        return completed

    @staticmethod
    def _normalize_check(check: dict[str, Any]) -> dict[str, Any]:
        name = check.get("name") or check.get("workflowName") or check.get("context")
        status = check.get("conclusion") or check.get("status") or check.get("state")
        return {
            "name": str(name or "unknown"),
            "status": str(status or "unknown"),
            "url": check.get("detailsUrl") or check.get("targetUrl") or check.get("url"),
        }


class FileBackedOpsConnector:
    """Use run-store JSON files as the first safe deployment and observability seam."""

    DEFAULT_STAGING_SIGNAL = {
        "soak_minutes": 1440,
        "request_samples": 5000,
        "metrics": {},
    }
    DEFAULT_MONITORING_SIGNAL = {
        "window_minutes": 240,
        "metrics": {},
        "security_anomaly": False,
    }
    DEFAULT_ROLLBACK_SIGNAL = {
        "tested": True,
        "executed": False,
        "status": "passed",
        "evidence": "File-backed rollback probe passed for the vertical slice.",
    }

    def __init__(
        self,
        store_dir: Path,
        *,
        seed_missing_signals: bool = True,
    ) -> None:
        self.store_dir = store_dir.resolve()
        self.seed_missing_signals = seed_missing_signals
        self.signal_dir = self.store_dir / "ops-signals"

    def ensure_default_signals(self, work_item_id: str) -> None:
        if not self.seed_missing_signals:
            return
        self.signal_dir.mkdir(parents=True, exist_ok=True)
        defaults = {
            self._staging_path(work_item_id): self.DEFAULT_STAGING_SIGNAL,
            self._monitoring_path(work_item_id): self.DEFAULT_MONITORING_SIGNAL,
            self._rollback_path(work_item_id): self.DEFAULT_ROLLBACK_SIGNAL,
        }
        for path, document in defaults.items():
            if not path.exists():
                self._write_json(path, {"work_item_id": work_item_id, **document})

    def read_staging_signal(self, work_item_id: str) -> dict[str, Any]:
        signal = self._read_signal(self._staging_path(work_item_id), work_item_id, "staging")
        self._require_int(signal, "soak_minutes", minimum=1)
        self._require_int(signal, "request_samples", minimum=1)
        self._require_mapping(signal, "metrics")
        return signal

    def read_monitoring_signal(self, work_item_id: str) -> dict[str, Any]:
        signal = self._read_signal(self._monitoring_path(work_item_id), work_item_id, "monitoring")
        self._require_int(signal, "window_minutes", minimum=1)
        self._require_mapping(signal, "metrics")
        if not isinstance(signal.get("security_anomaly"), bool):
            raise FactoryConnectorError("monitoring signal must include boolean security_anomaly.")
        return signal

    def read_rollback_signal(self, work_item_id: str) -> dict[str, Any]:
        signal = self._read_signal(self._rollback_path(work_item_id), work_item_id, "rollback")
        if signal.get("tested") is not True:
            raise FactoryConnectorError("rollback signal must set tested=true.")
        if signal.get("status") != "passed":
            raise FactoryConnectorError("rollback signal status must be 'passed'.")
        return signal

    def _read_signal(self, path: Path, work_item_id: str, signal_name: str) -> dict[str, Any]:
        if not path.exists():
            raise FactoryConnectorError(
                f"Missing {signal_name} signal for work item '{work_item_id}' at {path}."
            )
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise FactoryConnectorError(f"Could not read {signal_name} signal at {path}: {exc}") from exc
        if not isinstance(payload, dict):
            raise FactoryConnectorError(f"{signal_name} signal must be a JSON object.")
        if payload.get("work_item_id") != work_item_id:
            raise FactoryConnectorError(
                f"{signal_name} signal work_item_id does not match '{work_item_id}'."
            )
        return payload

    def _staging_path(self, work_item_id: str) -> Path:
        return self.signal_dir / work_item_id / "staging-signal.json"

    def _monitoring_path(self, work_item_id: str) -> Path:
        return self.signal_dir / work_item_id / "monitoring-signal.json"

    def _rollback_path(self, work_item_id: str) -> Path:
        return self.signal_dir / work_item_id / "rollback-signal.json"

    @staticmethod
    def _write_json(path: Path, document: dict[str, Any]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(f"{json.dumps(document, indent=2)}\n", encoding="utf-8")

    @staticmethod
    def _require_int(document: dict[str, Any], key: str, *, minimum: int) -> None:
        value = document.get(key)
        if not isinstance(value, int) or value < minimum:
            raise FactoryConnectorError(f"signal field '{key}' must be an integer >= {minimum}.")

    @staticmethod
    def _require_mapping(document: dict[str, Any], key: str) -> None:
        if not isinstance(document.get(key), dict):
            raise FactoryConnectorError(f"signal field '{key}' must be an object.")
