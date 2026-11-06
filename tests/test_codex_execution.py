"""Codex receipts bind the real selection and exact configuration before work."""

from dataclasses import replace
import json
from pathlib import Path

import pytest

from auto_mindsdb_factory import product_execution
from auto_mindsdb_factory.codex_session import CodexAgent
from auto_mindsdb_factory.connectors import OpenAIResponsesAgentConfig
from auto_mindsdb_factory.product_agent import HarnessStopped
from auto_mindsdb_factory.product_execution import codex_execution_snapshot, require_codex_execution
from auto_mindsdb_factory.product_harness import HarnessSelectionError, resolve_harness
from auto_mindsdb_factory.product_roles import snapshot_roles
from auto_mindsdb_factory.run_journal import RunJournal


def config():
    return OpenAIResponsesAgentConfig(api_key="not-recorded", reasoning_effort="high", timeout_seconds=1)


def manifest(value):
    return {"role_snapshots": snapshot_roles(), "execution_snapshots": {"builder": codex_execution_snapshot(value)}}


def test_truthful_snapshot_does_not_enable_selection_or_record_credentials(tmp_path):
    value = config(); document = manifest(value)
    receipt = document["execution_snapshots"]["builder"]
    assert receipt["harness"] == "codex" and receipt["version"] == "0.167.0"
    assert len(receipt["dependencies_sha256"]) == 64 and "not-recorded" not in json.dumps(document)
    journal = RunJournal(tmp_path, "receipt"); journal.create(document)
    require_codex_execution(value, "builder", journal)
    with pytest.raises(HarnessSelectionError): resolve_harness({"builder_harness": "codex"}, "builder")
    assert not journal.events()


@pytest.mark.parametrize("change", ["model", "reasoning_effort", "max_output_tokens", "timeout_seconds",
    "base_url", "organization", "project", "fallback_model", "harness", "version", "dependencies", "bool", "missing"])
def test_drift_fails_before_session_resources_or_evidence(tmp_path, change):
    value = config(); document = manifest(value)
    if change in {"model", "reasoning_effort", "base_url", "organization", "project", "fallback_model"}:
        value = replace(value, **{change: "changed"})
    elif change in {"max_output_tokens", "timeout_seconds"}: value = replace(value, **{change: 2})
    elif change == "missing": document.pop("execution_snapshots")
    else:
        field = {"dependencies": "dependencies_sha256", "bool": "timeout_seconds"}.get(change, change)
        document["execution_snapshots"]["builder"][field] = True if change == "bool" else "changed"
    journal = RunJournal(tmp_path, "changed"); journal.create(document)
    before = (journal.root / "manifest.json").read_bytes()
    agent = CodexAgent(value, journal, max_turns=2, max_total_tokens=100, deadline=1) if change != "fallback_model" else None
    with pytest.raises(HarnessStopped, match="frozen Codex"):
        if agent is not None: agent.run("builder", {}, None)
        else: require_codex_execution(value, "builder", journal)
    assert (journal.root / "manifest.json").read_bytes() == before and not journal.events()


@pytest.mark.parametrize("changed", ["anton_requirements.txt", "codex_bridge/package-lock.json"])
def test_client_and_cli_lock_changes_invalidate_receipt(tmp_path, monkeypatch, changed):
    value = config()
    source = Path(product_execution.__file__).parent
    for name in ("anton_requirements.txt", "codex_bridge/package-lock.json"):
        destination = tmp_path / name; destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes((source / name).read_bytes())
    document = manifest(value)
    monkeypatch.setattr(product_execution, "Path", lambda _: tmp_path / "product_execution.py")
    journal = RunJournal(tmp_path / "evidence", "locks"); journal.create(document)
    require_codex_execution(value, "builder", journal)
    path = tmp_path / changed; path.write_bytes(path.read_bytes() + b"\n")
    with pytest.raises(HarnessStopped): require_codex_execution(value, "builder", journal)
    assert not journal.events()


@pytest.mark.parametrize("corruption", ["duplicate", "nonfinite"])
def test_ambiguous_receipt_json_is_rejected(tmp_path, corruption):
    value = config(); document = manifest(value)
    journal = RunJournal(tmp_path, "json"); journal.create(document)
    encoded = json.dumps(document)
    if corruption == "duplicate":
        encoded = encoded.replace('"harness": "codex"', '"harness":"native","harness":"codex"')
    else:
        document["extra"] = float("nan"); encoded = json.dumps(document)
    (journal.root / "manifest.json").write_text(encoded)
    with pytest.raises(HarnessStopped): require_codex_execution(value, "builder", journal)
    assert not journal.events()
