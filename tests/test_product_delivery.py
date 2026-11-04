import os
import subprocess

import pytest

from auto_mindsdb_factory.product_delivery import ProductDelivery, DeliveryError
from auto_mindsdb_factory.product_profile import ProductProfile
from auto_mindsdb_factory.product_task import ProductTask
from auto_mindsdb_factory.product_workspace import git
from auto_mindsdb_factory.run_journal import RunJournal


def test_commit_boundary_rejects_metadata_alias_before_git_operations(tmp_path):
    delivery = object.__new__(ProductDelivery)
    delivery.settings = {"allowed_paths": ["*"], "protected_paths": []}
    delivery.target = tmp_path / "not-a-repository"
    with pytest.raises(DeliveryError, match="Git metadata"):
        delivery._commit(None, "a" * 40, {}, {".GiT": b"gitdir: /outside"}, 1, set(), set())


def test_real_agent_factory_respects_builder_and_reviewer_configuration(tmp_path, monkeypatch):
    monkeypatch.setenv("OPENAI_API_KEY", "test-only-no-request")
    monkeypatch.delenv("AI_FACTORY_OPENAI_FALLBACK_MODEL", raising=False)
    delivery = object.__new__(ProductDelivery)
    delivery.settings = ProductProfile.from_document({"version": 1, "name": "p", "repository": "owner/product",
        "image": "node@sha256:" + "a" * 64, "checks": {"unit": ["true"]}, "smoke": [["true"]],
        "builder_model": "builder-snapshot", "reviewer_model": "reviewer-snapshot", "agent_max_output_tokens": 9000}).document
    delivery.deadline = 999999999
    journal = RunJournal(tmp_path, "factory-config")
    assert delivery._agent("builder", journal).connector.config.model == "builder-snapshot"
    verifier = delivery._agent("verifier", journal)
    assert verifier.connector.config.model == "reviewer-snapshot"
    assert verifier.connector.config.max_output_tokens == 9000


@pytest.mark.parametrize("verdict", ["failed", "unverified"])
def test_verifier_reproduction_details_are_retained_for_revision(tmp_path, monkeypatch, verdict):
    import auto_mindsdb_factory.product_delivery as module
    delivery = object.__new__(ProductDelivery)
    delivery.settings = {"checks": {"unit": ["true"]}, "image": "unused", "platform": "linux/amd64"}
    delivery.target = tmp_path
    def prepare(sha, workspace, journal):
        workspace.mkdir()
        (workspace / "app.txt").write_text("source")
        return {"app.txt": b"source"}
    delivery._prepare = prepare
    class Sandbox:
        def __init__(self, *args, **kwargs): pass
        def __enter__(self): return self
        def __exit__(self, *args): pass
    delivery.sandbox_factory = Sandbox
    delivery._command = lambda sandbox, command: {"command": command, "exit_code": 0}
    class Verifier:
        def run(self, *args):
            assert args[1]["prior_attempt_feedback"] == "earlier unresolved finding"
            return {"status": verdict, "summary": "Rounding needs clarification", "findings": ["Reproduce 99.99 times 0.5"],
                "criteria": [{"id": "rounding", "status": verdict, "evidence": "Observed 50; task requested exact legacy output"}]}
    delivery.agent_factory = lambda *args: Verifier()
    monkeypatch.setattr(module, "git", lambda *args: "candidate diff")
    task = ProductTask.from_document({"id": "task", "title": "Fix", "goal": "fix", "acceptance_criteria": [
        {"id": "rounding", "description": "round consistently", "command": ["true"]}]})
    with pytest.raises(module.DeliveryError) as error:
        delivery.verify(task, "a" * 40, "b" * 40, RunJournal(tmp_path, "revision"), prior_feedback="earlier unresolved finding")
    assert "99.99 times 0.5" in str(error.value)
    assert "rounding" in str(error.value)
    assert "Observed 50" in str(error.value)


def test_failed_base_fetch_is_preserved_before_attempts(tmp_path, monkeypatch):
    import auto_mindsdb_factory.product_delivery as module
    target = tmp_path / "target"
    target.mkdir()
    subprocess.run(["git", "init", "-q", str(target)], check=True)
    git(target, "remote", "add", "origin", "https://github.com/owner/product.git")
    profile = ProductProfile.from_document({"version": 1, "name": "p", "repository": "owner/product",
        "image": "node@sha256:" + "a" * 64, "checks": {"unit": ["true"]}, "smoke": [["true"]]})
    task = ProductTask.from_document({"id": "fetch-failed", "title": "Fix", "goal": "fix",
        "acceptance_criteria": [{"id": "a", "description": "works", "command": ["true"]}]})
    real_git = module.git
    def failed_fetch(root, *args):
        if args[0] == "fetch":
            raise module.WorkspaceError("Fetch failed")
        return real_git(root, *args)
    monkeypatch.setattr(module, "git", failed_fetch)
    with pytest.raises(module.WorkspaceError):
        ProductDelivery(profile, target, tmp_path / "store").run(task)
    journal = RunJournal(tmp_path / "store", "fetch-failed")
    assert (journal.root / "manifest.json").exists()
    assert journal.summary()["status"] == "blocked"


def test_task_rejects_duplicate_criteria_and_internal_executable_requests(tmp_path):
    criterion = {"id": "a", "description": "works", "command": ["true"]}
    with pytest.raises(ValueError, match="unique"):
        ProductTask.from_document({"id": "task", "title": "Fix", "goal": "fix", "acceptance_criteria": [criterion, criterion]})
    with pytest.raises(ValueError, match="outside"):
        ProductTask.load(tmp_path / "task.json", target_root=tmp_path)


@pytest.mark.skipif(not os.environ.get("AI_FACTORY_TEST_IMAGE"), reason="requires a pinned Docker image")
@pytest.mark.parametrize("variant", ["correct", "broken", "out-of-scope"])
def test_actual_delivery_verifies_a_separate_repository(tmp_path, variant):
    target = tmp_path / "product"
    target.mkdir()
    subprocess.run(["git", "init", "-q", str(target)], check=True)
    git(target, "config", "user.name", "test")
    git(target, "config", "user.email", "test@example.com")
    git(target, "remote", "add", "origin", "https://github.com/owner/product.git")
    (target / "app.js").write_text("module.exports=(a,b)=>a-b;\n")
    git(target, "add", ".")
    git(target, "commit", "-qm", "base")
    base = git(target, "rev-parse", "HEAD")
    (target / "uncommitted.txt").write_text("preserve user work")
    profile = ProductProfile.from_document({"version": 1, "name": "product", "repository": "owner/product",
        "image": os.environ["AI_FACTORY_TEST_IMAGE"], "platform": os.environ.get("AI_FACTORY_TEST_PLATFORM", "linux/amd64"),
        "checks": {"syntax": ["node", "--check", "app.js"]},
        "smoke": [["node", "--check", "app.js"]], "allowed_paths": ["app.js"], "max_attempts": 1})
    task = ProductTask.from_document({"id": "add", "title": "Add numbers", "goal": "Add two integers",
        "acceptance_criteria": [{"id": "addition", "description": "1 plus 1 is 2", "command": ["node", "-e", "process.exit(require('./app')(1,1)===2?0:1)"]}]})
    seen = []
    class Agent:
        def run(self, role, context, command):
            seen.append((role, context))
            assert "command" not in context["acceptance_criteria"][0]
            if role == "builder":
                content = "module.exports=(a,b)=>a+b;" if variant == "correct" else "module.exports=(a,b)=>a*b;"
                result = command(["node", "-e", f"require('fs').writeFileSync('app.js',{content!r})"])
                if variant == "correct":
                    assert command(["chmod", "+x", "app.js"])["exit_code"] == 0
                if variant == "out-of-scope":
                    command(["node", "-e", "require('fs').writeFileSync('unexpected.txt','bad')"])
            else:
                assert "revision_feedback" not in context
                assert command(["sh", "-c", "echo tamper > app.js"])["exit_code"] != 0
                result = command(["node", "-e", "process.exit(require('./app')(2,3)===5?0:1)"])
            assert result["exit_code"] == 0, result
            return {"status": "passed", "summary": "checked", "criteria": [], "findings": []}
    delivery = ProductDelivery(profile, target, tmp_path / "store", agent_factory=lambda *args: Agent())
    if variant == "correct":
        result = delivery.run(task, base_sha=base)
        assert result["status"] == "verified"
        assert result["verification"]["commit_sha"] == result["candidate_sha"] != base
        assert result["changed_paths"] == ["app.js"]
        assert git(target, "ls-tree", result["candidate_sha"], "app.js").startswith("100755")
        assert [role for role, _ in seen] == ["builder", "verifier"]
    else:
        with pytest.raises(DeliveryError):
            delivery.run(task, base_sha=base)
        assert RunJournal(tmp_path / "store", "add").summary()["status"] == "blocked"
        assert [role for role, _ in seen] == ["builder"]
    assert git(target, "rev-parse", "HEAD") == base
    assert (target / "uncommitted.txt").read_text() == "preserve user work"
