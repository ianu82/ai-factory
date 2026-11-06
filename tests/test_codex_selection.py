"""Manual certification uses validated profiles and the real delivery dispatcher."""

import json
import os
from pathlib import Path
import time

import pytest

from auto_mindsdb_factory.__main__ import main
from auto_mindsdb_factory import codex_process, codex_session
from auto_mindsdb_factory.product_agent import ProductAgent, HarnessStopped
from auto_mindsdb_factory.product_delivery import ProductDelivery
from auto_mindsdb_factory.product_execution import execution_manifest
from auto_mindsdb_factory.product_profile import ProductProfile, ProfileError
from auto_mindsdb_factory.run_journal import RunJournal


def document(**changes):
    return {"version": 1, "name": "qualification", "repository": "owner/product",
        "image": "python@sha256:" + "a" * 64, "checks": {"unit": ["true"]}, "smoke": [["true"]],
        "execution_lane": "codex-qualification", "builder_harness": "codex", **changes}


@pytest.mark.parametrize("change", [{}, {"reviewer_harness": "codex"}, {"builder_harness": "native", "reviewer_harness": "codex"}])
def test_lane_requires_explicit_manual_opt_in_and_is_retained(change):
    value = document(**change)
    with pytest.raises(ProfileError): ProductProfile.from_document(value)
    profile = ProductProfile.from_document(value, codex_qualification=True)
    assert profile.document["execution_lane"] == "codex-qualification"
    assert ProductProfile.from_document(profile.document, codex_qualification=True).digest == profile.digest
    with pytest.raises(ProfileError): ProductProfile.from_document(profile.document)


@pytest.mark.parametrize("change", [{"execution_lane": None}, {"execution_lane": "delivery"},
    {"builder_harness": "native"}, {"builder_harness": "typo"}, {"execution_lane": True}])
def test_opt_in_cannot_relax_profile_validation(change):
    with pytest.raises(ProfileError): ProductProfile.from_document(document(**change), codex_qualification=True)
    ordinary = document(); ordinary.pop("execution_lane")
    with pytest.raises(ProfileError): ProductProfile.from_document(ordinary, codex_qualification=True)


def test_public_validation_requires_both_lane_and_option(validation_checkout, monkeypatch, capsys):
    root, target = validation_checkout
    profile = root / "codex.json"; profile.write_text(json.dumps(document()))
    calls = []
    monkeypatch.setattr(codex_session, "validate_codex_installation", lambda: calls.append("installation"))
    args = ["product-validate", "--profile", str(profile), "--target-root", str(target)]
    assert main(args) == 1 and not calls
    assert not capsys.readouterr().out
    assert main(args + ["--codex-qualification"]) == 0
    output = json.loads(capsys.readouterr().out)
    assert calls == ["installation"]
    assert (output["builder_harness"], output["reviewer_harness"], output["execution_lane"]) == (
        "codex", "native", "codex-qualification")
    # Automatic intake has neither the opt-in nor authority to load this profile.
    assert main(["product-linear-worker", "--profile", str(profile), "--target-root", str(target),
        "--route", str(root / "absent-route.json"), "--store-dir", str(root / "absent-store"), "--once"]) == 1
    assert not (root / "absent-store").exists()
    assert not capsys.readouterr().out and calls == ["installation"]


def actual_installation(tmp_path, monkeypatch):
    binary = Path(os.environ.get("AI_FACTORY_CODEX_TEST_BINARY") or
        "src/auto_mindsdb_factory/codex_bridge/node_modules/.bin/codex").resolve()
    if not binary.is_file():
        pytest.skip("Pinned CLI unavailable; dedicated pinned-client CI requires it")
    installed = tmp_path / "codex_bridge/node_modules/.bin/codex"
    installed.parent.mkdir(parents=True); installed.symlink_to(binary)
    monkeypatch.setattr(codex_process, "__file__", str(tmp_path / "codex_process.py"))


@pytest.mark.parametrize("role,field", [("builder", "builder_harness"), ("verifier", "reviewer_harness")])
def test_actual_installation_and_truthful_dispatch_preserve_native_planning(tmp_path, monkeypatch, role, field):
    pytest.importorskip("openai")
    actual_installation(tmp_path, monkeypatch)
    monkeypatch.setenv("OPENAI_API_KEY", "controller-selection-only")
    monkeypatch.setenv("AI_FACTORY_OPENAI_REASONING_EFFORT", "high")
    profile = ProductProfile.from_document(document(builder_harness="native", **{field: "codex"})
        if field != "builder_harness" else document(), codex_qualification=True)
    journal = RunJournal(tmp_path / "evidence", "selection")
    frozen = execution_manifest(profile.document); journal.create(frozen)
    delivery = object.__new__(ProductDelivery)
    delivery.settings, delivery.deadline = profile.document, time.monotonic() + 60
    selected = delivery._agent(role, journal)
    other = delivery._agent("verifier" if role == "builder" else "builder", journal)
    assert type(selected) is codex_session.CodexAgent and type(other) is ProductAgent
    retained = frozen["execution_snapshots"]
    assert retained[role]["harness"] == "codex" and retained[role]["version"] == "0.167.0"
    assert retained["planner"]["harness"] == "native" and retained[role]["reasoning_effort"] == "high"
    assert selected.max_total_tokens == profile.document["max_total_tokens"]
    assert selected.max_turns == profile.document["max_agent_turns"] and selected.deadline == delivery.deadline
    assert "controller-selection-only" not in json.dumps(frozen) and not journal.events()
    monkeypatch.setenv("AI_FACTORY_OPENAI_REASONING_EFFORT", "low")
    with pytest.raises(HarnessStopped): delivery._agent(role, journal)
    assert not journal.events()


def test_missing_codex_installation_never_falls_back(tmp_path, monkeypatch):
    def missing(): raise HarnessStopped("Missing pinned Codex installation")
    monkeypatch.setattr(codex_session, "validate_codex_installation", missing)
    profile = ProductProfile.from_document(document(), codex_qualification=True)
    with pytest.raises(HarnessStopped, match="Missing pinned Codex"):
        execution_manifest(profile.document)
