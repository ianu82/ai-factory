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


def test_harness_policy_stop_cannot_retry_or_promote(delivery):
    from auto_mindsdb_factory.product_agent import HarnessStopped
    instance, task, calls = delivery
    def stop(*args):
        raise HarnessStopped("cancelled")
    instance.agent_factory = stop
    with pytest.raises(HarnessStopped):
        instance.run(task, base_sha="a" * 40, promote=lambda *args: pytest.fail("promoted"))
    events = RunJournal(instance.store, task.document["id"]).events()
    assert sum(e["kind"] == "attempt_started" for e in events) == 1
    assert not calls and not any(e["kind"] == "candidate_created" for e in events)


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


@pytest.mark.parametrize("failed_stage", ["building", "verifying", "merging"])
@pytest.mark.parametrize("write_before_failure", [False, True])
def test_ack_write_failure_aborts_without_retry_or_publication(delivery, monkeypatch, failed_stage, write_before_failure):
    instance, task, calls = delivery
    stages = ["building", "verifying", "merging"]
    observed = []
    prepare = instance._prepare
    def record_prepare(*args):
        calls.append("prepare")
        return prepare(*args)
    monkeypatch.setattr(instance, "_prepare", record_prepare)
    append = RunJournal.append
    def failing_append(journal, kind, **data):
        if kind == "delivery_stage_started" and data["stage"] == failed_stage:
            assert observed[-1] == failed_stage  # observer returned successfully first
            if write_before_failure:
                append(journal, kind, **data)  # outcome of a failed durable write can be uncertain
            raise OSError("private journal failure")
        return append(journal, kind, **data)
    monkeypatch.setattr(RunJournal, "append", failing_append)
    def observe(stage):
        observed.append(stage)
    def promote(*args):
        calls.append("promotion")
        return {"status": "complete"}
    with pytest.raises(DeliveryStageError, match="Controller stage synchronization failed"):
        instance.run(task, base_sha="a" * 40, promote=promote, on_stage=observe)
    assert observed == stages[:stages.index(failed_stage) + 1]
    assert calls.count("prepare") == calls.count("builder") == (failed_stage != "building")
    assert calls.count("verifier") == (failed_stage == "merging")
    assert "promotion" not in calls
    journal = RunJournal(instance.store, task.document["id"])
    events = journal.events()
    assert journal.summary()["status"] == "blocked"
    assert journal.summary()["attempts"] == 1
    assert events[-1]["kind"] == "run_finished"
    assert events[-1]["data"]["reason"] == "DeliveryStageError"
    assert not any(e["kind"] == "attempt_finished" and e["data"]["status"] == "failed" for e in events)
    assert [e["data"]["stage"] for e in events if e["kind"] == "delivery_stage_started"] == (
        stages[:stages.index(failed_stage) + int(write_before_failure)])
    assert "private journal failure" not in str(events)


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


@pytest.mark.parametrize("variant", ["known", "generic", "missing", "wrong_type", "phase", "category",
                                     "args", "coordinated", "forged", "subclass", "secret", "unavailable"])
def test_terminal_smoke_runner_facts_fail_closed(delivery, monkeypatch, variant):
    from auto_mindsdb_factory.docker_runner import RunnerError
    from auto_mindsdb_factory.railway_runner import RailwayOperationError
    import auto_mindsdb_factory.railway_runner as railway

    instance, task, calls = delivery
    instance.settings["smoke"] = [["first"], ["second"], ["third"]]
    error = RailwayOperationError({"action": "run"}, "ExecInterruptedError")
    if variant == "generic":
        error = RunnerError("secret-provider-body")
    elif variant == "missing":
        del error.phase
    elif variant == "wrong_type":
        error.phase = ["run"]
    elif variant == "phase":
        error.phase = "secret-provider-body"
    elif variant == "category":
        error.error_type = "OSError"
    elif variant == "args":
        error.args = ("secret-provider-body",)
    elif variant == "coordinated":
        error.phase, error.error_type = "exit", "OSError"
        error.args = ("Railway exit failed (OSError)",)
    elif variant == "forged":
        error = RailwayOperationError.__new__(RailwayOperationError)
        error.phase, error.error_type, error.args = "run", "OSError", ("Railway run failed (OSError)",)
    elif variant == "subclass":
        class DerivedError(RailwayOperationError):
            pass
        error = DerivedError({"action": "run"}, "OSError")
    elif variant == "secret":
        error = RailwayOperationError({"action": "secret-provider-body"}, "secret-provider-body")
    elif variant == "unavailable":
        def unavailable(*args):
            raise RuntimeError("secret-provider-body")
        monkeypatch.setattr(railway, "railway_failure_facts", unavailable)
    prepare = instance._prepare
    def counted_prepare(*args):
        calls.append("prepare")
        return prepare(*args)
    monkeypatch.setattr(instance, "_prepare", counted_prepare)
    def command(sandbox, cmd):
        calls.append(cmd[0])
        if cmd == ["third"]:
            raise error
        return {"command": cmd, "exit_code": 0}
    monkeypatch.setattr(instance, "_command", command)
    def promote(candidate, journal):
        calls.append("promotion")
        journal.append("merge_confirmed", commit_sha=candidate["candidate_sha"])
        instance.smoke(candidate["candidate_sha"], journal)
        pytest.fail("failed smoke must not return")
    with pytest.raises(RunnerError) as raised:
        instance.run(task, base_sha="a" * 40, promote=promote, on_stage=lambda s: None)
    assert raised.value is error
    assert calls == ["prepare", "builder", "verifier", "promotion", "prepare", "first", "second", "third"]
    journal = RunJournal(instance.store, task.document["id"])
    events = journal.events()
    assert journal.summary()["status"] == "blocked" and journal.summary()["attempts"] == 1
    assert [e["data"]["index"] for e in events if e["kind"] == "post_merge_smoke"] == [1, 2]
    data = events[-1]["data"]
    assert events[-1]["kind"] == "run_finished" and data["status"] == "blocked"
    assert data["reason"] == type(error).__name__
    assert (data["runner_operation"], data["runner_category"]) == (
        ("run", "ExecInterruptedError") if variant == "known" else ("unknown", "UnknownError"))
    assert not any(e["kind"] == "run_finished" and e["data"]["status"] == "complete" for e in events)
    assert "secret-provider-body" not in str(events)


@pytest.mark.parametrize("failure", ["construction", "write_before", "write_after"])
def test_terminal_diagnostic_failure_preserves_primary(delivery, monkeypatch, failure):
    import auto_mindsdb_factory.product_delivery as module
    from auto_mindsdb_factory.railway_runner import RailwayOperationError
    instance, task, calls = delivery
    error = RailwayOperationError({"action": "run"}, "OSError")
    def promote(*args):
        calls.append("promotion")
        raise error
    def broken(*args, **kwargs):
        raise OSError("secret-diagnostic-failure")
    if failure == "construction":
        monkeypatch.setattr(module, "_terminal_runner_facts", broken)
    else:
        append = RunJournal.append
        def failing_append(journal, kind, **data):
            if kind == "run_finished":
                if failure == "write_after":
                    append(journal, kind, **data)
                broken()
            return append(journal, kind, **data)
        monkeypatch.setattr(RunJournal, "append", failing_append)
    with pytest.raises(RailwayOperationError) as raised:
        instance.run(task, base_sha="a" * 40, promote=promote)
    assert raised.value is error
    assert calls == ["builder", "verifier", "promotion"]
    journal = RunJournal(instance.store, task.document["id"])
    assert journal.summary()["status"] == ("blocked" if failure == "write_after" else "incomplete")
    assert sum(e["kind"] == "run_finished" for e in journal.events()) == (failure == "write_after")
    assert "secret-diagnostic-failure" not in str(journal.events())
