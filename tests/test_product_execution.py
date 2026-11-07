import json
import time
from types import SimpleNamespace

import pytest

from auto_mindsdb_factory.anton_session import ANTON_COMMIT, AntonAgent
from auto_mindsdb_factory.product_agent import HarnessStopped, ProductAgent
from auto_mindsdb_factory.product_delivery import ProductDelivery
from auto_mindsdb_factory.product_execution import execution_manifest, execution_config
from auto_mindsdb_factory.product_profile import ProductProfile
from auto_mindsdb_factory.run_journal import RunJournal


def test_execution_snapshot_distinguishes_explicit_credential_binding():
    from auto_mindsdb_factory.connectors import OpenAIResponsesAgentConfig
    from auto_mindsdb_factory.product_execution import codex_execution_snapshot
    from dataclasses import asdict
    fields = asdict(OpenAIResponsesAgentConfig(api_key="synthetic-only"))
    first = SimpleNamespace(**fields, inference_binding_sha256="a" * 64)
    second = SimpleNamespace(**fields, inference_binding_sha256="b" * 64)
    assert codex_execution_snapshot(first) != codex_execution_snapshot(second)


def settings(**overrides):
    return ProductProfile.from_document({"version": 1, "name": "selection", "repository": "owner/product",
        "image": "python@sha256:" + "a" * 64, "checks": {"unit": ["true"]}, "smoke": [["true"]],
        "builder_model": "builder-pinned", "reviewer_model": "reviewer-pinned", **overrides}).document


@pytest.mark.parametrize("role,field", [("builder", "builder_harness"), ("verifier", "reviewer_harness")])
def test_explicit_anton_selection_freezes_runtime_and_preserves_other_role(tmp_path, monkeypatch, role, field):
    pytest.importorskip("anton.core.session")
    monkeypatch.setenv("OPENAI_API_KEY", "controller-selection-test")
    monkeypatch.setenv("AI_FACTORY_OPENAI_REASONING_EFFORT", "low")
    document = settings(**{field: "anton"})
    journal = RunJournal(tmp_path, "selection")
    manifest = execution_manifest(document)
    journal.create(manifest)
    delivery = object.__new__(ProductDelivery)
    delivery.settings, delivery.deadline = document, time.monotonic() + 60
    selected = delivery._agent(role, journal)
    other = delivery._agent("verifier" if role == "builder" else "builder", journal)
    assert isinstance(selected, AntonAgent) and isinstance(other, ProductAgent)
    retained = manifest["execution_snapshots"]
    assert retained[role]["version"] == ANTON_COMMIT and len(retained[role]["dependencies_sha256"]) == 64
    assert retained["planner"]["harness"] == "native" and retained["planner"]["model"] == "builder-pinned"
    assert selected.config.model == document["reviewer_model" if role == "verifier" else "builder_model"]
    assert all(item["reasoning_effort"] == "low" for item in retained.values())
    assert "controller-selection-test" not in json.dumps(manifest)


@pytest.mark.parametrize("change", ["reasoning", "endpoint", "model", "missing", "tampered"])
def test_configuration_changes_cannot_start_a_session(tmp_path, monkeypatch, change):
    monkeypatch.setenv("AI_FACTORY_OPENAI_REASONING_EFFORT", "medium")
    document = settings()
    journal = RunJournal(tmp_path, "drift")
    manifest = execution_manifest(document)
    if change == "missing":
        manifest = {}
    elif change == "tampered":
        manifest["execution_snapshots"]["builder"]["version"] = "unreviewed"
    journal.create(manifest)
    if change == "reasoning":
        monkeypatch.setenv("AI_FACTORY_OPENAI_REASONING_EFFORT", "high")
    elif change == "endpoint":
        monkeypatch.setenv("AI_FACTORY_OPENAI_BASE_URL", "https://different-provider.invalid/v1/responses")
    elif change == "model":
        document["builder_model"] = "different-model"
    before = (journal.root / "manifest.json").read_bytes()
    with pytest.raises(HarnessStopped):
        execution_config(document, "builder", journal)
    assert (journal.root / "manifest.json").read_bytes() == before and not journal.events()


def test_missing_sdk_does_not_fall_back_to_native(monkeypatch):
    from auto_mindsdb_factory import anton_session
    def unavailable():
        raise HarnessStopped("Pinned SDK unavailable")
    monkeypatch.setattr(anton_session, "validate_anton_installation", unavailable)
    with pytest.raises(HarnessStopped, match="unavailable"):
        execution_manifest(settings(builder_harness="anton"))


def test_public_validation_checks_selected_anton_installation(tmp_path, capsys):
    pytest.importorskip("anton.core.session")
    from auto_mindsdb_factory.__main__ import main
    from auto_mindsdb_factory.product_workspace import git
    import subprocess
    target = tmp_path / "target"
    target.mkdir()
    subprocess.run(["git", "init", "-q", str(target)], check=True)
    git(target, "remote", "add", "origin", "https://github.com/owner/product.git")
    profile = tmp_path / "profile.json"
    profile.write_text(json.dumps(settings(builder_harness="anton")))
    assert main(["product-validate", "--profile", str(profile), "--target-root", str(target)]) == 0
    result = json.loads(capsys.readouterr().out)
    assert result["builder_harness"] == "anton" and result["reviewer_harness"] == "native"
