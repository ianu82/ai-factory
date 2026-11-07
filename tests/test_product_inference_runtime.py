from dataclasses import FrozenInstanceError, asdict, replace
import hashlib
import json
import os

import pytest

from auto_mindsdb_factory import product_inference_runtime as runtime
from auto_mindsdb_factory.connectors import OpenAIResponsesAgentConfig
from auto_mindsdb_factory.product_execution import codex_execution_snapshot, role_config
from auto_mindsdb_factory.product_inference import InferencePolicy, InferencePolicyError
from auto_mindsdb_factory.product_inference_auth import _receipt


KEY = "mdb_synthetic.private-only"
UUID = "00000000-0000-4000-8000-000000000001"


@pytest.fixture
def inputs(tmp_path, monkeypatch):
    monkeypatch.setattr(runtime, "require_controller_import_path", lambda target: None)
    root = tmp_path.resolve()
    root.chmod(0o700)
    settings = {"builder_model": "gpt-6.1-terra", "reviewer_model": "pinned-reviewer",
        "agent_max_output_tokens": 10000, "inference": {"version": 1, "scope": "controller",
        "provider": "mindshub", "protocol": "responses", "endpoint": runtime.MindsHubQualificationConfig.base_url,
        "credential_ref": {"id": UUID, "revision": 1}}}
    key = root / "key.env"
    key.write_text("AI_FACTORY_MINDSHUB_API_KEY=" + KEY + "\n")
    key.chmod(0o600)
    receipt = _receipt(InferencePolicy.from_document(settings["inference"]), KEY, None,
        {"valid": True, "auth_method": "api_key", "key_type": "user", "principal_kind": "user",
        "credential_kind": "api_key", "user_id": UUID, "organization_id": UUID,
        "workspace_id": UUID, "key_prefix": "mdb_synthetic"})
    path = root / "receipt.json"
    path.write_text(json.dumps(receipt))
    return settings, dict(reasoning_effort="high", timeout_seconds=180, receipt_path=path,
        receipt_sha256=hashlib.sha256(path.read_bytes()).hexdigest(), credential_file=key,
        credential_version=runtime._version(key.stat()), target_root=root / "candidate")


def test_resolved_config_is_frozen_secret_safe_and_ignores_global_provider(inputs, monkeypatch):
    settings, kwargs = inputs
    monkeypatch.setenv("OPENAI_API_KEY", "wrong-global-key")
    monkeypatch.setenv("AI_FACTORY_OPENAI_BASE_URL", "https://wrong.invalid/responses")
    monkeypatch.setenv("AI_FACTORY_OPENAI_FALLBACK_MODEL", "unapproved")
    original_hash = runtime.hashlib.sha256
    def safe_hash(data):
        assert KEY.encode() not in data
        return original_hash(data)
    monkeypatch.setattr(runtime.hashlib, "sha256", safe_hash)
    config = runtime.resolve_qualification_config(settings, "builder", **kwargs)
    assert config.api_key == KEY and config.fallback_model is None
    assert (config.model, config.reasoning_effort, config.max_output_tokens, config.timeout_seconds) == (
        "gpt-6.1-terra", "high", 10000, 180)
    assert replace(config).api_key == KEY  # Actual Codex ownership-copy contract.
    assert KEY not in repr(config) + json.dumps(config.inference_binding) + json.dumps(codex_execution_snapshot(config))
    with pytest.raises(FrozenInstanceError):
        config.api_key = "changed"
    with pytest.raises(TypeError):
        replace(config, api_key="changed")
    with pytest.raises(InferencePolicyError, match="not execution-ready"):
        role_config(settings, "builder")
    assert runtime.resolve_qualification_config(settings, "verifier", **kwargs).model == "pinned-reviewer"


@pytest.mark.parametrize("change", ["policy", "digest", "version", "workspace", "candidate", "key-mode",
    "parent-mode", "symlink-key", "symlink-receipt", "fifo", "key-format", "oversized", "receipt-extra"])
def test_untrusted_or_stale_input_cannot_resolve(inputs, change):
    settings, kwargs = inputs
    key, receipt = kwargs["credential_file"], kwargs["receipt_path"]
    if change == "policy": settings["inference"]["credential_ref"]["revision"] = 2
    elif change == "digest": kwargs["receipt_sha256"] = "a" * 64
    elif change == "version": kwargs["credential_version"] = (0,) * 6
    elif change == "workspace": kwargs["auth_workspace_id"] = "00000000-0000-4000-8000-000000000002"
    elif change == "candidate": kwargs["target_root"] = key.parent
    elif change == "key-mode": key.chmod(0o644)
    elif change == "parent-mode": key.parent.chmod(0o755)
    elif change.startswith("symlink"):
        path = key if change == "symlink-key" else receipt
        path.rename(path.with_suffix(".original"))
        path.symlink_to(path.with_suffix(".original"))
    elif change == "fifo":
        key.unlink()
        os.mkfifo(key)
    elif change in ("key-format", "oversized"):
        key.write_text(KEY if change == "key-format" else "x" * 5000)
        kwargs["credential_version"] = runtime._version(key.stat())
    elif change == "receipt-extra":
        data = json.loads(receipt.read_text()); data["unknown"] = True
        receipt.write_text(json.dumps(data))
        kwargs["receipt_sha256"] = hashlib.sha256(receipt.read_bytes()).hexdigest()
    with pytest.raises(InferencePolicyError) as error:
        runtime.resolve_qualification_config(settings, "builder", **kwargs)
    assert KEY not in str(error.value)


def test_import_guard_and_invalid_settings_precede_file_read(inputs, monkeypatch):
    settings, kwargs = inputs
    def refused(*args, **options): raise AssertionError("must not read any file")
    monkeypatch.setattr(runtime, "_read_owned", refused)
    def import_guard(target): raise InferencePolicyError("untrusted imports")
    monkeypatch.setattr(runtime, "require_controller_import_path", import_guard)
    with pytest.raises(InferencePolicyError, match="untrusted imports"):
        runtime.resolve_qualification_config(settings, "builder", **kwargs)
    monkeypatch.setattr(runtime, "require_controller_import_path", lambda target: None)
    kwargs["timeout_seconds"] = True
    with pytest.raises(InferencePolicyError):
        runtime.resolve_qualification_config(settings, "builder", **kwargs)


def test_legacy_provider_fingerprint_is_unchanged():
    config = OpenAIResponsesAgentConfig(api_key="synthetic-only")
    provider = {k: asdict(config)[k] for k in ("base_url", "organization", "project")}
    assert codex_execution_snapshot(config)["provider_sha256"] == hashlib.sha256(
        json.dumps(provider, sort_keys=True).encode()).hexdigest()


def test_key_rotation_requires_new_frozen_execution_receipt(inputs, tmp_path):
    from auto_mindsdb_factory.product_execution import require_codex_execution
    from auto_mindsdb_factory.product_agent import HarnessStopped
    from auto_mindsdb_factory.run_journal import RunJournal
    settings, kwargs = inputs
    config = runtime.resolve_qualification_config(settings, "builder", **kwargs)
    journal = RunJournal(tmp_path, "bound")
    journal.create({"execution_snapshots": {"builder": codex_execution_snapshot(config)}})
    key = kwargs["credential_file"]
    key.write_text("AI_FACTORY_MINDSHUB_API_KEY=mdb_synthetic.rotated\n")
    with pytest.raises(InferencePolicyError):
        runtime.resolve_qualification_config(settings, "builder", **kwargs)
    kwargs["credential_version"] = runtime._version(key.stat())
    changed = runtime.resolve_qualification_config(settings, "builder", **kwargs)
    with pytest.raises(HarnessStopped, match="frozen Codex"):
        require_codex_execution(changed, "builder", journal)
    assert not journal.events()


@pytest.mark.parametrize("change", ["write", "short-read"])
def test_credential_change_during_read_is_rejected(inputs, monkeypatch, change):
    settings, kwargs = inputs
    read = runtime.os.read
    def raced(descriptor, count):
        data = read(descriptor, count)
        if data.startswith(b"AI_FACTORY_MINDSHUB_API_KEY="):
            if change == "short-read":
                return data[:-1]
            kwargs["credential_file"].write_text("AI_FACTORY_MINDSHUB_API_KEY=mdb_synthetic.changed\n")
        return data
    monkeypatch.setattr(runtime.os, "read", raced)
    with pytest.raises(InferencePolicyError):
        runtime.resolve_qualification_config(settings, "builder", **kwargs)
