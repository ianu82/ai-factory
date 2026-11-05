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


def test_real_delivery_manifest_freezes_roles_and_keeps_provenance(delivery, monkeypatch):
    import json
    from auto_mindsdb_factory import product_roles
    instance, task, _ = delivery
    calls = []
    select = product_roles.select_role
    def counted(*args):
        calls.append(args)
        return select(*args)
    monkeypatch.setattr(product_roles, "select_role", counted)
    instance.run(task, base_sha="a" * 40)
    path = instance.store / "delivery" / task.document["id"] / "manifest.json"
    original = path.read_bytes()
    manifest = json.loads(original)
    assert [call for call in calls if len(call) == 1] == [("builder",), ("verifier",)]
    assert ("builder", "1") in calls and ("verifier", "1") in calls
    assert manifest["purpose"] == "delivery" and manifest["replay"] is None
    assert manifest["base_sha"] == "a" * 40 and len(manifest["runtime_sha"]) == 40
    assert manifest["task"] == task.document and manifest["task_sha"] == task.digest
    assert manifest["profile"] == instance.profile.document and manifest["profile_sha"] == instance.profile.digest
    assert product_roles.stored_role_snapshots(manifest) == {r: select(r, "1") for r in ("builder", "verifier")}
    for role in ("builder", "verifier"):
        updated = select(role, "1")
        updated["version"] = "2"
        updated["digest"] = product_roles._digest({k: v for k, v in updated.items() if k != "digest"})
        monkeypatch.setitem(product_roles._DEFINITIONS, (role, "2"), updated)
        monkeypatch.setitem(product_roles._DEFAULT_VERSIONS, role, "2")
    assert product_roles.stored_role_snapshots(manifest)["builder"]["version"] == "1"
    assert path.read_bytes() == original


def test_real_build_and_verification_replay_manifests(delivery, tmp_path, monkeypatch):
    import json
    from auto_mindsdb_factory.product_replay import run_suite
    from auto_mindsdb_factory import product_roles
    from auto_mindsdb_factory.product_roles import select_role, stored_role_snapshots
    instance, task, _ = delivery
    suite = {"version": 1, "cases": [
        {"name": "build", "mode": "build", "base_sha": "a" * 40, "task": task.document, "expected": "verified"},
        {"name": "verify", "mode": "verify", "base_sha": "a" * 40, "candidate_sha": "b" * 40,
         "task": task.document, "expected": "verified"},
    ]}
    path = tmp_path / "suite.json"
    path.write_text(json.dumps(suite))
    result = run_suite(instance.profile, instance.target, instance.store, path,
                       delivery_factory=lambda *args: instance)
    assert result["correct"] == result["total"] == 2
    paths = [instance.store / "delivery" / score["run_id"] / "manifest.json" for score in result["results"]]
    originals = [path.read_bytes() for path in paths]
    for index, (score, original) in enumerate(zip(result["results"], originals)):
        manifest = json.loads(original)
        assert manifest["purpose"] == "replay" and manifest["base_sha"] == "a" * 40
        assert manifest["profile"] == instance.profile.document and manifest["profile_sha"] == instance.profile.digest
        assert manifest["task"] == {**task.document, "id": score["run_id"]}
        assert manifest["replay"] == {k: score[k] for k in ("suite_sha", "case", "repetition", "original_task_sha")}
        if index == 0:
            assert manifest["task_sha"] and len(manifest["runtime_sha"]) == 40
        else:
            assert manifest["candidate_sha"] == "b" * 40
        assert stored_role_snapshots(manifest) == {r: select_role(r, "1") for r in ("builder", "verifier")}
    for role in ("builder", "verifier"):
        updated = select_role(role, "1")
        updated["version"] = "2"
        updated["digest"] = product_roles._digest({k: v for k, v in updated.items() if k != "digest"})
        monkeypatch.setitem(product_roles._DEFINITIONS, (role, "2"), updated)
        monkeypatch.setitem(product_roles._DEFAULT_VERSIONS, role, "2")
    for path, original in zip(paths, originals):
        assert stored_role_snapshots(json.loads(path.read_bytes()))["verifier"]["version"] == "1"
        assert path.read_bytes() == original
