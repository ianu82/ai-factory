from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

from auto_mindsdb_factory.command_gates import CommandGateError, CommandGateRunner
from auto_mindsdb_factory.controller import ControllerState, WorkItem
from auto_mindsdb_factory.connectors import FactoryConnectorError, GitHubCLIRepoConnector
from auto_mindsdb_factory.eval_execution import EvalExecutionError, Stage5EvalPipeline
from auto_mindsdb_factory.execution_account import ExecutionAccount


def git(repo: Path, *args: str) -> str:
    return subprocess.run(
        ["git", "-C", str(repo), *args], check=True, capture_output=True, text=True,
    ).stdout.strip()


@pytest.fixture
def candidate_repo(tmp_path):
    repo = tmp_path / "checkout"
    repo.mkdir()
    git(repo, "init", "-b", "main")
    git(repo, "config", "user.name", "Factory test")
    git(repo, "config", "user.email", "factory@example.invalid")
    (repo / "src").mkdir()
    probe = repo / "src" / "candidate_probe.py"
    probe.write_text("VALUE = 'base'\n")
    git(repo, "add", ".")
    git(repo, "commit", "-m", "Passing base")
    base_sha = git(repo, "rev-parse", "HEAD")
    probe.write_text("VALUE = 'candidate'\n")
    git(repo, "commit", "-am", "Candidate changes behavior")
    candidate_sha = git(repo, "rev-parse", "HEAD")
    git(repo, "reset", "--hard", base_sha)
    return repo, base_sha, candidate_sha


def test_stage5_checks_candidate_commit_instead_of_passing_base(candidate_repo, monkeypatch):
    repo, base_sha, candidate_sha = candidate_repo
    root = Path(__file__).resolve().parents[1]
    fixture = root / "fixtures/scenarios/stage4_reviewable_feature"
    documents = {
        path.stem.replace("-", "_"): json.loads(path.read_text())
        for path in fixture.glob("*.json")
    }
    packet = documents["pr_packet"]
    packet["delivery_evidence"] = {
        "mode": "code_worker_pr", "repository": "owner/repo",
        "branch_name": "candidate", "base_branch": "main", "commit_sha": candidate_sha,
        "pull_request_number": 1, "pull_request_url": "https://github.com/owner/repo/pull/1",
        "created_at": "2026-04-22T14:17:08Z",
        "code_worker": {
            "status": "succeeded", "provider": "codex_cli", "model": "test",
            "command": ["codex", "exec"], "changed_paths": ["src/candidate_probe.py"],
            "diff_stat": "1 file changed", "stdout": "", "stderr": "",
            "started_at": "2026-04-22T14:17:08Z", "completed_at": "2026-04-22T14:17:08Z",
            "exit_code": 0,
        },
    }
    # A host import path and credentials must not bleed into candidate tests.
    monkeypatch.setenv("PYTHONPATH", str(repo / "src"))
    monkeypatch.setenv("LINEAR_API_KEY", "must-not-reach-candidate")
    check = [sys.executable, "-c", (
        "import os; from candidate_probe import VALUE; "
        "assert 'LINEAR_API_KEY' not in os.environ; assert VALUE == 'base', VALUE"
    )]
    runner = CommandGateRunner(
        repo, commands_by_kind={"unit": check, "contract": [sys.executable, "-c", "pass"]},
        require_candidate=True,
    )
    result = Stage5EvalPipeline(root, gate_runner=runner).process(
        **{key: value for key, value in documents.items() if key != "work_item"},
        work_item=WorkItem.from_document(documents["work_item"]),
    )

    assert result.work_item.state is ControllerState.PR_REVISION
    assert result.eval_report["execution"] == {"mode": "candidate_commit", "commit_sha": candidate_sha}
    unit = next(check for tier in result.eval_report["tiers"] for check in tier["checks"] if check["kind"] == "unit")
    assert unit["status"] == "failed"
    assert "candidate" in unit["stderr"]
    assert git(repo, "rev-parse", "HEAD") == base_sha
    assert git(repo, "status", "--porcelain") == ""
    assert len(git(repo, "worktree", "list", "--porcelain").split("worktree ")) == 2


@pytest.mark.parametrize("sha", ["short", "f" * 40])
def test_invalid_or_missing_candidate_never_falls_back_to_base(candidate_repo, sha):
    repo, _, _ = candidate_repo
    runner = CommandGateRunner(repo, require_candidate=True)
    with pytest.raises(CommandGateError):
        with runner.for_pr({"delivery_evidence": {"commit_sha": sha}}):
            pytest.fail("An unavailable candidate must not run gates.")


def test_production_gates_require_delivery_evidence(candidate_repo):
    runner = CommandGateRunner(candidate_repo[0], require_candidate=True)
    with pytest.raises(CommandGateError, match="require candidate delivery"):
        with runner.for_pr({}):
            pytest.fail("Production must not fall back to the service checkout.")


def test_candidate_workspace_is_removed_after_exception(candidate_repo):
    repo, _, sha = candidate_repo
    workspace = None
    with pytest.raises(RuntimeError, match="interrupted"):
        with CommandGateRunner(repo).for_pr({"delivery_evidence": {"commit_sha": sha}}) as runner:
            workspace = runner.repo_root
            raise RuntimeError("interrupted")
    assert workspace is not None and not workspace.exists()
    assert len(git(repo, "worktree", "list", "--porcelain").split("worktree ")) == 2


def test_delivered_pr_cannot_use_simulated_evals():
    from auto_mindsdb_factory.eval_execution import EvalRunner

    with pytest.raises(EvalExecutionError, match="command-backed"):
        with EvalRunner().for_pr({"delivery_evidence": {"commit_sha": "a" * 40}}):
            pytest.fail("Simulation cannot approve a delivered PR.")


def test_code_review_reads_the_pinned_candidate_diff(candidate_repo):
    repo, base_sha, candidate_sha = candidate_repo
    git(repo, "update-ref", "refs/remotes/origin/main", base_sha)
    connector = GitHubCLIRepoConnector(repo, repository="owner/repo")
    diff = connector.read_candidate_diff(candidate_sha)
    assert "+VALUE = 'candidate'" in diff
    assert "-VALUE = 'base'" in diff


def test_code_review_refuses_partial_evidence(candidate_repo, monkeypatch):
    connector = GitHubCLIRepoConnector(candidate_repo[0], repository="owner/repo")
    monkeypatch.setattr(connector, "_git_stdout", lambda args: "x" * 60001)
    with pytest.raises(FactoryConnectorError, match="split the work"):
        connector.read_candidate_diff(candidate_repo[2])


def test_production_gates_use_restricted_account_and_restore_ownership(candidate_repo, monkeypatch):
    repo, _, sha = candidate_repo
    ownership = []
    executed = []
    original_run = subprocess.run

    def chown(command, **kwargs):
        ownership.append(command)
        return subprocess.CompletedProcess(command, 0, "", "")

    def run(command, **kwargs):
        if command[0] == "sudo":
            executed.append((command, kwargs))
            return subprocess.CompletedProcess(command, 0, "", "")
        return original_run(command, **kwargs)

    monkeypatch.setenv("OPENAI_API_KEY", "service-secret")
    monkeypatch.setattr("auto_mindsdb_factory.command_gates.subprocess.run", run)
    account = ExecutionAccount("factory-gates", ownership_run=chown)
    runner = CommandGateRunner(repo, execution_account=account)
    with runner.for_pr({"delivery_evidence": {"commit_sha": sha}}) as scoped:
        result = scoped.run_check({"kind": "unit", "timeout_minutes": 1})
        workspace = scoped.repo_root
    assert result.status == "passed"
    command, kwargs = executed[0]
    assert command[:6] == ["sudo", "-H", "-u", "factory-gates", "--", "env"]
    assert command[6].startswith(f"PYTHONPATH={workspace}/src")
    assert "OPENAI_API_KEY" not in kwargs["env"]
    assert ownership[0][3] == "factory-gates"
    assert ownership[-1][3] == f"{os.getuid()}:{os.getgid()}"
    assert not workspace.exists()


def test_production_gate_account_is_required_and_explicit_override_is_respected(candidate_repo, monkeypatch):
    monkeypatch.setattr("auto_mindsdb_factory.command_gates.pwd.getpwnam", lambda name: SimpleNamespace(pw_uid=os.getuid() + 1))
    monkeypatch.delenv("AI_FACTORY_GATE_RUN_AS_USER", raising=False)
    monkeypatch.delenv("AI_FACTORY_CODE_WORKER_RUN_AS_USER", raising=False)
    with pytest.raises(CommandGateError, match="Production gates require"):
        CommandGateRunner.from_env(candidate_repo[0], require_candidate=True)
    monkeypatch.setenv("AI_FACTORY_CODE_WORKER_RUN_AS_USER", "code-worker")
    assert CommandGateRunner.from_env(candidate_repo[0], require_candidate=True).execution_account.user == "code-worker"
    monkeypatch.setenv("AI_FACTORY_GATE_RUN_AS_USER", "dedicated-gates")
    assert CommandGateRunner.from_env(candidate_repo[0], require_candidate=True).execution_account.user == "dedicated-gates"
    monkeypatch.setenv("AI_FACTORY_GATE_RUN_AS_USER", str(os.getuid()))
    with pytest.raises(CommandGateError, match="distinct from the orchestrator"):
        CommandGateRunner.from_env(candidate_repo[0], require_candidate=True)


@pytest.mark.parametrize("user", ["#0", "0", f"#{os.getuid()}"])
def test_numeric_gate_account_cannot_select_root_or_service_identity(candidate_repo, monkeypatch, user):
    monkeypatch.setenv("AI_FACTORY_GATE_RUN_AS_USER", user)
    with pytest.raises(CommandGateError, match="distinct from the orchestrator"):
        CommandGateRunner.from_env(candidate_repo[0], require_candidate=True)


@pytest.mark.skipif(shutil.which("uv") is None, reason="locked-environment integration requires uv")
def test_default_gate_rejects_candidate_with_invalid_dependency_lock(candidate_repo):
    repo, _, _ = candidate_repo
    (repo / "pyproject.toml").write_text(
        '[project]\nname = "gate-fixture"\nversion = "0.0.0"\nrequires-python = ">=3.11"\n'
        '[project.optional-dependencies]\ndev = []\n'
    )
    (repo / "scripts").mkdir()
    (repo / "scripts/validate_contracts.py").write_text("print('base environment passed')\n")
    subprocess.run(["uv", "lock", "--offline"], cwd=repo, check=True, capture_output=True)
    git(repo, "add", ".")
    git(repo, "commit", "-m", "Valid locked environment")
    passing_base = git(repo, "rev-parse", "HEAD")
    (repo / "uv.lock").write_text("invalid lockfile [\n")
    git(repo, "commit", "-am", "Break candidate dependency lock")
    broken_sha = git(repo, "rev-parse", "HEAD")
    git(repo, "reset", "--hard", passing_base)

    with CommandGateRunner(repo).for_pr({"delivery_evidence": {"commit_sha": broken_sha}}) as runner:
        result = runner.run_check({"kind": "contract", "timeout_minutes": 1})
    assert result.status == "failed"
    assert "uv.lock" in result.stderr
    assert "base environment passed" not in result.stdout
    assert git(repo, "rev-parse", "HEAD") == passing_base
