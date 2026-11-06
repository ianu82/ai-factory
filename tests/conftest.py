"""Explicit planning doubles for delivery component tests; no provider requests."""

import subprocess
import tempfile
from pathlib import Path
from types import SimpleNamespace

import pytest


@pytest.fixture
def investigating_planner():
    def factory(task, base, journal, deadline):
        def run(role, context, command):
            assert role == "planner" and context["base_sha"] == base
            assert "expected" not in context and "case" not in context
            command(["true"])
            return {"status": "ready", "reason": "Component-test planning double",
                "goal": task.document["goal"], "constraints": ["Keep task and API"],
                "source_paths": ["."], "changes": ["Implement original task"],
                "risks": ["Preserve existing behavior"], "questions": [],
                "validation": [{"criterion_id": c["id"], "evidence": c["description"]}
                               for c in context["acceptance_criteria"]]}
        return SimpleNamespace(run=run)
    return factory


@pytest.fixture
def validation_checkout():
    # Keep Git metadata out of the exported source tree, even with --basetemp.
    with tempfile.TemporaryDirectory(prefix="product-validate-", dir="/tmp") as directory:
        root = Path(directory)
        target = root / "target"
        subprocess.run(["git", "init", "-q", str(target)], check=True)
        subprocess.run(["git", "-C", str(target), "remote", "add", "origin",
                        "git@github.com:owner/product.git"], check=True)
        yield root, target
