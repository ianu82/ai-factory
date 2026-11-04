from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pytest

from auto_mindsdb_factory.command_gates import CommandGateError, CommandGateRunner
from auto_mindsdb_factory.controller import ControllerState, WorkItem
from auto_mindsdb_factory.eval_execution import EvalExecutionError, Stage5EvalPipeline


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
