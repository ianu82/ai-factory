import json

import pytest

from auto_mindsdb_factory.product_delivery import DeliveryError, VerificationFailed
from auto_mindsdb_factory.product_profile import ProductProfile
from auto_mindsdb_factory.product_replay import load_suite, run_suite
from auto_mindsdb_factory.run_journal import RunJournal


def suite_document():
    task = {"id": "original", "title": "Fix", "goal": "fix the behaviour", "acceptance_criteria": [
        {"id": "works", "description": "works", "command": ["private-acceptance-check"]}]}
    return {"version": 1, "cases": [
        {"name": "good", "mode": "build", "base_sha": "a" * 40, "task": task, "expected": "verified"},
        {"name": "bad", "mode": "verify", "base_sha": "a" * 40, "candidate_sha": "b" * 40, "task": task, "expected": "rejected"},
        {"name": "environment", "mode": "verify", "base_sha": "a" * 40, "candidate_sha": "c" * 40, "task": task, "expected": "rejected"},
    ]}


def test_replays_pin_inputs_preserve_scores_and_keep_failures_honest(tmp_path, monkeypatch):
    monkeypatch.setattr("auto_mindsdb_factory.product_replay.git", lambda *args: "d" * 40)
    path = tmp_path / "suite.json"
    path.write_text(json.dumps(suite_document()))
    profile = ProductProfile.from_document({"version": 1, "name": "product", "repository": "owner/product",
        "image": "node@sha256:" + "a" * 64, "checks": {"unit": ["true"]}, "smoke": [["true"]]})
    seen = []
    class Delivery:
        def __init__(self, profile, target, store):
            self.store = store
        def run(self, task, **kwargs):
            assert set(kwargs) == {"base_sha", "replay"}
            assert kwargs["base_sha"] == "a" * 40
            assert "expected" not in task.public_context
            assert "command" not in task.public_context["acceptance_criteria"][0]
            seen.append(task.document["id"])
            j = RunJournal(self.store, task.document["id"])
            j.create({"purpose": "replay", "replay": kwargs["replay"]})
            j.append("run_finished", status="verified")
            return {"status": "verified"}
        def plan(self, task, base, journal):
            pytest.fail("Verification-only replay must not plan implementation")
        def verify_fixture(self, task, candidate, base, journal):
            manifest = json.loads((journal.root / "manifest.json").read_text())
            assert manifest["verification_only"] is True and manifest["replay"]["mode"] == "verify"
            assert manifest["task_sha"] == task.digest
            assert "expected" not in task.public_context and "case" not in task.public_context
            if candidate == "b" * 40:
                raise VerificationFailed("assertion failed")
            raise DeliveryError("environment unavailable")
    result = run_suite(profile, tmp_path / "target", tmp_path / "store", path, repetitions=2, delivery_factory=Delivery)
    assert result["correct"] == 4
    assert result["total"] == 6
    assert [r["actual"] for r in result["results"]] == ["verified", "rejected", "unverified"] * 2
    assert len(set(seen)) == 2
    assert len(list((tmp_path / "store" / "delivery").glob("*/manifest.json"))) == 6


@pytest.mark.parametrize("mutation", ["branch", "missing-candidate", "unknown-field"])
def test_suite_rejects_unfrozen_or_ambiguous_inputs(tmp_path, mutation):
    document = suite_document()
    if mutation == "branch":
        document["cases"][0]["base_sha"] = "main"
    elif mutation == "missing-candidate":
        del document["cases"][1]["candidate_sha"]
    else:
        document["silent_override"] = True
    path = tmp_path / "suite.json"
    path.write_text(json.dumps(document))
    with pytest.raises(ValueError):
        load_suite(path, tmp_path / "target")
