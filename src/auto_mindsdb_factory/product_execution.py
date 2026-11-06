"""Freeze backend/model/runtime configuration without serializing credentials."""

import hashlib
import json
from pathlib import Path

from .connectors import OpenAIResponsesAgentConfig
from .product_agent import HarnessStopped
from .product_harness import resolve_harness
from .codex_process import CODEX_VERSION


def role_config(settings, role):
    return OpenAIResponsesAgentConfig.from_env(model=settings[
        "reviewer_model" if role == "verifier" else "builder_model"],
        max_output_tokens=settings["agent_max_output_tokens"])


def snapshot(settings, role, config):
    name = "native" if role == "planner" else resolve_harness(settings, role)
    if config.fallback_model:
        raise HarnessStopped("Delivery requires pinned models without fallback")
    version, dependencies = "1", None
    if name == "anton":
        from .anton_session import ANTON_COMMIT, validate_anton_installation
        validate_anton_installation()
        version = ANTON_COMMIT
        dependencies = hashlib.sha256(Path(__file__).with_name("anton_requirements.txt").read_bytes()).hexdigest()
    return _configuration(name, version, dependencies, config)


def _configuration(name, version, dependencies, config):
    provider = json.dumps({"base_url": config.base_url, "organization": config.organization,
                           "project": config.project}, sort_keys=True).encode()
    return {"harness": name, "version": version, "dependencies_sha256": dependencies,
        "model": config.model, "reasoning_effort": config.reasoning_effort,
        "max_output_tokens": config.max_output_tokens, "timeout_seconds": config.timeout_seconds,
        "provider_sha256": hashlib.sha256(provider).hexdigest()}


def codex_execution_snapshot(config):
    """Truthful preparation/qualification binding; does not enable selection."""
    if config.fallback_model:
        raise HarnessStopped("Codex requires pinned models without fallback")
    root = Path(__file__).parent
    locks = {name: hashlib.sha256((root / name).read_bytes()).hexdigest()
             for name in ("anton_requirements.txt", "codex_bridge/package-lock.json")}
    dependencies = hashlib.sha256(json.dumps(locks, sort_keys=True).encode()).hexdigest()
    return _configuration("codex", CODEX_VERSION, dependencies, config)


def _receipt_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("Duplicate execution receipt key")
        result[key] = value
    return result


def _receipt_constant(_):
    raise ValueError("Non-JSON execution receipt value")


def require_codex_execution(config, role, journal):
    try:
        if role not in ("builder", "verifier"):
            raise ValueError("unsupported role")
        manifest = json.loads((journal.root / "manifest.json").read_text(),
            object_pairs_hook=_receipt_object, parse_constant=_receipt_constant)
        # JSON comparison also rejects bool/int aliases in a corrupted receipt.
        actual = json.dumps(manifest["execution_snapshots"][role], sort_keys=True, allow_nan=False)
        expected = json.dumps(codex_execution_snapshot(config), sort_keys=True, allow_nan=False)
        if actual != expected:
            raise ValueError("changed Codex configuration")
    except Exception:
        raise HarnessStopped("Missing or changed frozen Codex execution configuration") from None


def execution_manifest(settings):
    return {"execution_snapshots": {role: snapshot(settings, role, role_config(settings, role))
                                    for role in ("planner", "builder", "verifier")}}


def execution_config(settings, role, journal):
    config = role_config(settings, role)
    try:
        manifest = json.loads((journal.root / "manifest.json").read_text())
        if manifest["execution_snapshots"][role] != snapshot(settings, role, config):
            raise ValueError("changed execution configuration")
    except Exception:
        raise HarnessStopped("Missing or changed frozen execution configuration") from None
    return config
