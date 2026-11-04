"""Short product workflow: implement, independently verify, then promote."""

from __future__ import annotations

import fnmatch
import json
import tempfile
import time
from pathlib import Path
from typing import Callable

from .automation import FactoryRunStore
from .connectors import OpenAIResponsesAgentConfig, OpenAIResponsesAgentConnector
from .docker_runner import DockerSandbox
from .intake import repo_root
from .product_agent import ProductAgent, AgentSessionError
from .product_profile import ProductProfile
from .product_task import ProductTask
from .product_workspace import controller_worktree, executable_paths, export_source, git, is_git_metadata_path, source_files, source_digest, WorkspaceError
from .reliability import OperationTracker
from .run_journal import RunJournal


class DeliveryError(RuntimeError):
    pass


class VerificationFailed(DeliveryError):
    """Evidence establishes a defect, rather than an infrastructure failure."""


def _report_feedback(report: dict) -> str:
    details = {"summary": report["summary"], "findings": report["findings"],
        "criteria": [criterion for criterion in report["criteria"] if criterion["status"] != "passed"]}
    return json.dumps(details)[:8000]


class ProductDelivery:
    def __init__(self, profile: ProductProfile, target_root: Path, store: Path, *,
                 agent_factory: Callable | None = None, sandbox_factory: Callable = DockerSandbox) -> None:
        self.profile = profile
        self.settings = profile.document
        self.target = target_root.resolve()
        self.store = store.resolve()
        if self.store.is_relative_to(self.target):
            raise DeliveryError("Delivery evidence must be outside candidate source")
        profile.validate_repository(self.target)
        self.agent_factory = agent_factory or self._agent
        self.sandbox_factory = sandbox_factory
        self.deadline = 0.0

    def _agent(self, role: str, journal: RunJournal) -> ProductAgent:
        model_key = {"builder": "builder_model", "verifier": "reviewer_model"}.get(role)
        if model_key is None:
            raise DeliveryError("Unknown delivery agent role")
        config = OpenAIResponsesAgentConfig.from_env(model=self.settings[model_key],
            max_output_tokens=self.settings["agent_max_output_tokens"])
        if config.fallback_model:
            raise DeliveryError("Product delivery requires pinned role models; fallback routing is not enabled")
        return ProductAgent(OpenAIResponsesAgentConnector(config), journal,
            max_turns=self.settings["max_agent_turns"], max_total_tokens=self.settings["max_total_tokens"],
            deadline=self.deadline, pricing=self.settings["pricing"])

    def _command(self, sandbox, command: list[str]) -> dict:
        remaining = self.deadline - time.monotonic()
        if remaining < 1:
            raise DeliveryError("Run time budget exhausted")
        sandbox.timeout = min(self.settings["command_timeout_seconds"], int(remaining))
        return sandbox.run(command)

    def _prepare(self, sha: str, workspace: Path, journal: RunJournal) -> dict[str, bytes]:
        export_source(self.target, sha, workspace)
        original = source_files(workspace)
        original_executables = executable_paths(workspace, original)
        if self.settings["setup"]:
            with self.sandbox_factory(workspace, self.settings["image"], writable=True,
                                      network=self.settings["setup_network"], platform=self.settings["platform"]) as sandbox:
                for command in self.settings["setup"]:
                    result = self._command(sandbox, command)
                    journal.append("setup_command", commit_sha=sha, **result)
                    if result["exit_code"]:
                        raise DeliveryError("Candidate environment setup failed")
            if source_files(workspace) != original or executable_paths(workspace, original) != original_executables:
                raise DeliveryError("Environment setup changed source files")
        return original

    def _commit(self, task: ProductTask, base: str, original: dict, modified: dict, attempt: int,
                original_executables: set[str], modified_executables: set[str]) -> tuple[str, list[str]]:
        paths = sorted(path for path in set(original) | set(modified) if original.get(path) != modified.get(path)
                       or (path in original_executables) != (path in modified_executables))
        if not paths:
            raise DeliveryError("Builder produced no source changes")
        for path in paths:
            if is_git_metadata_path(path):
                raise DeliveryError("Candidate cannot change Git metadata or its portability aliases")
            if (not any(fnmatch.fnmatchcase(path, pattern) for pattern in self.settings["allowed_paths"])
                    or any(fnmatch.fnmatchcase(path, pattern) for pattern in self.settings["protected_paths"])):
                raise DeliveryError(f"Candidate changed a path outside its permitted scope: {path}")
        with controller_worktree(self.target, base) as checkout:
            for path in paths:
                target = checkout / path
                if path not in modified:
                    target.unlink()
                else:
                    target.parent.mkdir(parents=True, exist_ok=True)
                    target.write_bytes(modified[path])
                    target.chmod(0o755 if path in modified_executables else 0o644)
            git(checkout, "add", "--", *paths)
            stats = git(checkout, "diff", "--cached", "--numstat", "--no-renames")
            lines = 0
            for row in stats.splitlines():
                added, removed, _ = row.split("\t", 2)
                if not added.isdigit() or not removed.isdigit():
                    raise DeliveryError("Binary changes need a separate delivery policy")
                lines += int(added) + int(removed)
            if lines > self.settings["max_changed_lines"]:
                raise DeliveryError("Candidate exceeds the small-PR line budget")
            git(checkout, "-c", "user.name=AI Factory", "-c", "user.email=ai-factory@users.noreply.github.com",
                "-c", "commit.gpgsign=false", "commit", "-m", task.document["title"])
            sha = git(checkout, "rev-parse", "HEAD")
            git(self.target, "update-ref", f"refs/factory/{task.document['id']}/attempt-{attempt}", sha)
            return sha, paths

    def verify(self, task: ProductTask, sha: str, base: str, journal: RunJournal, *, prior_feedback: str = "") -> dict:
        with tempfile.TemporaryDirectory(prefix="ai-factory-verify-") as temporary:
            workspace = Path(temporary) / "source"
            original = self._prepare(sha, workspace, journal)
            original_executables = executable_paths(workspace, original)
            checks = []
            with self.sandbox_factory(workspace, self.settings["image"], writable=False, platform=self.settings["platform"]) as sandbox:
                commands = [(name, command) for name, command in self.settings["checks"].items()]
                commands += [("acceptance:" + item["id"], item["command"]) for item in task.document["acceptance_criteria"]]
                for name, command in commands:
                    result = self._command(sandbox, command)
                    journal.append("verification_check", name=name, commit_sha=sha, **result)
                    checks.append({"name": name, **result})
                if any(check["exit_code"] in {124, 125, 126, 127, 137} for check in checks):
                    raise DeliveryError("Verification execution was interrupted or unavailable")
                if any(check["exit_code"] for check in checks):
                    raise VerificationFailed("Independent executable verification failed: " + ", ".join(c["name"] for c in checks if c["exit_code"]))
                diff = git(self.target, "diff", "--no-ext-diff", "--no-textconv", "--no-renames", base, sha, "--")
                if not diff or len(diff) > 60000:
                    raise DeliveryError("Candidate diff is empty or too large to review")
                context = {**task.public_context, "base_sha": base, "candidate_sha": sha, "diff": diff,
                           "required_checks": self.settings["checks"], "prior_attempt_feedback": prior_feedback,
                           "controller_checks": [{"name": c["name"], "exit_code": c["exit_code"]} for c in checks]}
                report = self.agent_factory("verifier", journal).run("verifier", context, lambda cmd: self._command(sandbox, cmd))
            if (source_files(workspace) != original or executable_paths(workspace, original) != original_executables):
                raise DeliveryError("Verifier changed candidate source")
            if report["status"] == "failed":
                raise VerificationFailed("Independent verifier found blocking defects: " + _report_feedback(report))
            if report["status"] != "passed":
                raise DeliveryError("Independent verifier left the candidate unverified: " + _report_feedback(report))
            return {"commit_sha": sha, "source_sha": source_digest(original, original_executables), "checks": checks, "report": report}

    def run(self, task: ProductTask, *, promote: Callable | None = None, base_sha: str | None = None,
            replay: dict | None = None) -> dict:
        task_id = task.document["id"]
        journal = RunJournal(self.store, task_id)
        started = time.monotonic()
        self.deadline = started + self.settings["max_run_seconds"]
        locks = FactoryRunStore(self.store, repo_root_override=repo_root())
        with locks.run_lease(task_id, ttl_seconds=60, renew_interval_seconds=15) as lease:
            journal.create({"purpose": "replay" if replay is not None else (
                                "improvement" if task.document.get("origin") == "improvement" else "delivery"), "replay": replay,
                            "task": task.document, "base_sha": base_sha, "profile": self.settings,
                            "profile_sha": self.profile.digest, "task_sha": task.digest,
                            "runtime_sha": git(repo_root(), "rev-parse", "HEAD")})
            with OperationTracker(self.store, task_id, "delivery", "product-delivery"):
                feedback = ""
                try:
                    if base_sha is None:
                        git(self.target, "fetch", "--no-tags", "origin", self.settings["base_branch"])
                        base_sha = git(self.target, "rev-parse", f"origin/{self.settings['base_branch']}")
                    journal.append("base_resolved", commit_sha=base_sha)
                    for attempt in range(1, self.settings["max_attempts"] + 1):
                        journal.append("attempt_started", attempt=attempt, base_sha=base_sha)
                        try:
                            with tempfile.TemporaryDirectory(prefix="ai-factory-build-") as temporary:
                                workspace = Path(temporary) / "source"
                                original = self._prepare(base_sha, workspace, journal)
                                original_executables = executable_paths(workspace, original)
                                context = {**task.public_context, "required_checks": self.settings["checks"],
                                           "allowed_paths": self.settings["allowed_paths"], "revision_feedback": feedback}
                                with self.sandbox_factory(workspace, self.settings["image"], writable=True, platform=self.settings["platform"]) as sandbox:
                                    report = self.agent_factory("builder", journal).run("builder", context, lambda cmd: self._command(sandbox, cmd))
                                if report["status"] not in {"passed", "unverified"} or report["findings"]:
                                    raise DeliveryError("Builder did not complete the task: " + _report_feedback(report))
                                if report["status"] == "unverified":
                                    journal.append("builder_unverified", attempt=attempt)
                                modified = source_files(workspace)
                                sha, paths = self._commit(task, base_sha, original, modified, attempt,
                                    original_executables, executable_paths(workspace, modified))
                            journal.append("candidate_created", attempt=attempt, commit_sha=sha, changed_paths=paths)
                            verification = self.verify(task, sha, base_sha, journal, prior_feedback=feedback)
                            journal.append("candidate_verified", attempt=attempt, **verification)
                        except Exception as exc:
                            # No service secrets are sent to candidate commands; avoid storing provider error bodies.
                            feedback = str(exc) if isinstance(exc, (DeliveryError, WorkspaceError, AgentSessionError)) else type(exc).__name__
                            journal.append("attempt_finished", attempt=attempt, status="failed", reason=feedback)
                            continue
                        journal.append("attempt_finished", attempt=attempt, status="verified")
                        candidate = {"run_id": task_id, "base_sha": base_sha, "candidate_sha": sha,
                                     "profile_sha": self.profile.digest, "task_sha": task.digest,
                                     "changed_paths": paths, "verification": verification}
                        lease.heartbeat()
                        result = promote(candidate, journal) if promote else {"status": "verified", **candidate}
                        journal.append("run_finished", status=result["status"], duration_seconds=round(time.monotonic() - started, 3))
                        return {**result, "evidence_dir": str(journal.root), "usage": journal.summary()}
                    raise DeliveryError("No candidate passed independent verification within the attempt budget")
                except Exception as exc:
                    journal.append("run_finished", status="blocked", reason=type(exc).__name__, duration_seconds=round(time.monotonic() - started, 3))
                    raise

    def smoke(self, sha: str, journal: RunJournal) -> None:
        with tempfile.TemporaryDirectory(prefix="ai-factory-smoke-") as temporary:
            workspace = Path(temporary) / "source"
            original = self._prepare(sha, workspace, journal)
            with self.sandbox_factory(workspace, self.settings["image"], writable=False, platform=self.settings["platform"]) as sandbox:
                for index, command in enumerate(self.settings["smoke"], start=1):
                    result = self._command(sandbox, command)
                    journal.append("post_merge_smoke", commit_sha=sha, index=index, **result)
                    if result["exit_code"]:
                        raise DeliveryError("Post-merge smoke failed; task remains blocked")
            if source_files(workspace) != original:
                raise DeliveryError("Post-merge smoke changed source")
