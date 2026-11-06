"""Freeze backend/model/runtime configuration without serializing credentials."""

import hashlib
import json
from pathlib import Path

from .connectors import OpenAIResponsesAgentConfig
from .product_agent import HarnessStopped
from .product_harness import resolve_harness


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
    provider = json.dumps({"base_url": config.base_url, "organization": config.organization,
                           "project": config.project}, sort_keys=True).encode()
    return {"harness": name, "version": version, "dependencies_sha256": dependencies,
        "model": config.model, "reasoning_effort": config.reasoning_effort,
        "max_output_tokens": config.max_output_tokens, "timeout_seconds": config.timeout_seconds,
        "provider_sha256": hashlib.sha256(provider).hexdigest()}


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
