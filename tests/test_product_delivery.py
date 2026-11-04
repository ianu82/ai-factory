import os
import subprocess

import pytest

from auto_mindsdb_factory.product_delivery import ProductDelivery, DeliveryError
from auto_mindsdb_factory.product_profile import ProductProfile
from auto_mindsdb_factory.product_task import ProductTask
from auto_mindsdb_factory.product_workspace import git
from auto_mindsdb_factory.run_journal import RunJournal


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
        assert [role for role, _ in seen] == ["builder", "verifier"]
    else:
        with pytest.raises(DeliveryError):
            delivery.run(task, base_sha=base)
        assert RunJournal(tmp_path / "store", "add").summary()["status"] == "blocked"
        assert [role for role, _ in seen] == ["builder"]
    assert git(target, "rev-parse", "HEAD") == base
    assert (target / "uncommitted.txt").read_text() == "preserve user work"
