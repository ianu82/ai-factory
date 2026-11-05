"""Stage callbacks follow real delivery boundaries and fail closed."""
from contextlib import contextmanager
from pathlib import Path
from shutil import copytree
import subprocess

import pytest

from auto_mindsdb_factory.product_delivery import ProductDelivery, DeliveryStageError, VerificationFailed
from auto_mindsdb_factory.product_profile import ProductProfile
from auto_mindsdb_factory.product_task import ProductTask
from auto_mindsdb_factory.run_journal import RunJournal


@pytest.fixture
def delivery(tmp_path, monkeypatch):
    # Source exports intentionally omit the controller checkout's Git metadata.
    controller = tmp_path / "controller"
    subprocess.run(["git", "init", "-q", str(controller)], check=True)
    copytree(Path(__file__).resolve().parents[1] / "schemas", controller / "schemas")
    subprocess.run(["git", "-C", str(controller), "add", "schemas"], check=True)
    subprocess.run(["git", "-C", str(controller), "-c", "user.name=Test",
                    "-c", "user.email=test@example.invalid", "-c", "commit.gpgsign=false",
                    "commit", "-qm", "controller fixture"], check=True)
    monkeypatch.setattr("auto_mindsdb_factory.product_delivery.repo_root", lambda: controller)
    target = tmp_path / "target"
    subprocess.run(["git", "init", "-q", str(target)], check=True)
    subprocess.run(["git", "-C", str(target), "remote", "add", "origin",
                    "https://github.com/owner/product.git"], check=True)
    profile = ProductProfile.from_document({"version": 1, "name": "stages", "repository": "owner/product",
        "image": "python@sha256:" + "a" * 64, "checks": {"test": ["true"]}, "smoke": [["true"]]})
    task = ProductTask.from_document({"id": "stages", "title": "Fix", "goal": "fix",
        "acceptance_criteria": [{"id": "works", "description": "works", "command": ["true"]}]})
    calls = []
    class Agent:
        def run(self, *args):
            calls.append("builder")
            return {"status": "passed", "findings": []}
    @contextmanager
    def sandbox(*args, **kwargs):
        yield None
    instance = ProductDelivery(profile, target, tmp_path / "store",
                               agent_factory=lambda *args: Agent(), sandbox_factory=sandbox)
    def prepare(sha, workspace, journal):
        workspace.mkdir()
        return {}
    monkeypatch.setattr(instance, "_prepare", prepare)
    monkeypatch.setattr(instance, "_commit", lambda *args: ("b" * 40, ["app.txt"]))
    def verify(*args, **kwargs):
        calls.append("verifier")
        return {}
    monkeypatch.setattr(instance, "verify", verify)
    return instance, task, calls


def test_boundaries_and_retry_return_to_building(delivery, monkeypatch):
    instance, task, calls = delivery
    def verify(*args, **kwargs):
        calls.append("verifier")
        if calls.count("verifier") == 1:
            raise VerificationFailed("reproduce defect")
        return {}
    monkeypatch.setattr(instance, "verify", verify)
    def promote(candidate, journal):
        calls.append("promotion")
        return {"status": "complete"}
    instance.run(task, base_sha="a" * 40, promote=promote, on_stage=lambda s: calls.append("stage:" + s))
    assert calls == ["stage:building", "builder", "stage:verifying", "verifier",
                     "stage:building", "builder", "stage:verifying", "verifier",
                     "stage:merging", "promotion"]
    journal = RunJournal(instance.store, task.document["id"])
    assert [e["data"]["stage"] for e in journal.events() if e["kind"] == "delivery_stage_started"] == [
        "building", "verifying", "building", "verifying", "merging"]


@pytest.mark.parametrize("failed_stage", ["building", "verifying", "merging"])
def test_sync_failure_aborts_without_code_retry_or_publication(delivery, failed_stage):
    instance, task, calls = delivery
    def observe(stage):
        if stage == failed_stage:
            raise RuntimeError("private provider response")
    with pytest.raises(DeliveryStageError):
        instance.run(task, base_sha="a" * 40,
                     promote=lambda *args: calls.append("promotion"), on_stage=observe)
    assert calls.count("builder") == (failed_stage != "building")
    assert "promotion" not in calls
    journal = RunJournal(instance.store, task.document["id"])
    assert journal.summary()["status"] == "blocked"
    assert "private provider response" not in str(journal.events())


def test_legacy_delivery_has_no_observer_side_effects(delivery):
    instance, task, calls = delivery
    assert instance.run(task, base_sha="a" * 40)["status"] == "verified"
    assert calls == ["builder", "verifier"]
    assert not any(e["kind"] == "delivery_stage_started"
                   for e in RunJournal(instance.store, task.document["id"]).events())
