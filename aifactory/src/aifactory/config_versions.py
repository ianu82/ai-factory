"""Stable, secret-safe evidence of the configuration actually consumed by a session.

Task content is deliberately absent. Implementation groups are intentionally conservative:
any deployed Python or VM bridge change starts a new experiment group.
"""
from __future__ import annotations

import hashlib
import hmac
import json
import re
import subprocess
from collections.abc import Mapping
from importlib import metadata
from pathlib import Path
from typing import Any

from .inference import ProviderConfig, price_for
from .security import _master_key

PACKAGE_ROOT = Path(__file__).resolve().parent
# These are credential fields, not suffix rules: max_tokens and sort_key are inputs.
CREDENTIAL_FIELDS = frozenset({
    "api_key", "apikey", "token", "access_token", "password", "secret", "authorization", "cookie",
    "github_token", "linear_api_key", "mindshub_api_key", "openai_api_key",
})
QA_SECRET_ENV = frozenset({
    "API_KEY", "OPENAI_API_KEY", "MINDSHUB_API_KEY", "GITHUB_TOKEN", "GH_TOKEN", "LINEAR_API_KEY",
    "PASSWORD", "TOKEN", "ACCESS_TOKEN", "SECRET", "AUTHORIZATION", "COOKIE", "AIFACTORY_SECRET_KEY",
})


def canonical_json(value: Any) -> str:
    """JSON with sorted keys and value-equivalent integer/float representations."""
    def normalize(item):
        if isinstance(item, Mapping):
            return {k: normalize(v) for k, v in item.items()}
        if isinstance(item, (list, tuple)):
            return [normalize(v) for v in item]
        if isinstance(item, float) and item.is_integer():
            return int(item)
        return item
    return json.dumps(normalize(value), sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False)


def private_digest(value: Any) -> str:
    return "hmac:" + hmac.new(_master_key(), canonical_json(value).encode(), hashlib.sha256).hexdigest()[:16]


def sanitize(value: Any, *, env: bool = False) -> Any:
    if isinstance(value, Mapping):
        return {k: private_digest(v) if k.lower() in CREDENTIAL_FIELDS or (env and k in QA_SECRET_ENV)
                else sanitize(v, env=k == "env") for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [sanitize(v) for v in value]
    return value


def implementation_fingerprint() -> str:
    from .vm import SIDECAR, SIDECAR_ALT

    files = [(f"aifactory/{p.relative_to(PACKAGE_ROOT).as_posix()}", p)
             for p in PACKAGE_ROOT.rglob("*.py")]
    bridge = SIDECAR.parent if SIDECAR.is_file() else SIDECAR_ALT.parent
    if bridge.is_dir():
        files += [(f"vmbridge/{p.relative_to(bridge).as_posix()}", p) for p in bridge.rglob("*")
                  if p.is_file() and "node_modules" not in p.relative_to(bridge).parts]
    digest = hashlib.sha256()
    for name, path in sorted(files):
        digest.update(name.encode() + b"\0" + hashlib.sha256(path.read_bytes()).digest())
    return digest.hexdigest()


def runtime_version(harness: str, sandbox_kind: str = "local") -> Any:
    if harness == "codex":
        if sandbox_kind == "vm":
            from .vm import CODEX_VM_VERSION
            return CODEX_VM_VERSION
        from .harness.codex import CODEX_BIN
        try:
            return subprocess.run([CODEX_BIN, "--version"], capture_output=True, text=True,
                                  check=True, timeout=5).stdout.strip()
        except (OSError, subprocess.SubprocessError):
            return "unavailable"
    if harness == "anton":
        try:
            # The import is `anton`, but the deployed distribution is `anton-agent`.
            dist = metadata.distribution("anton-agent")
            source = dist.read_text("direct_url.json")
            return {"version": dist.version, "source": private_digest(source) if source else None}
        except metadata.PackageNotFoundError:
            return "unavailable"
    return None


def effective_build(raw: Mapping | None) -> dict:
    cfg = dict(raw or {})
    cfg["isolation"] = "vm" if cfg.get("isolation") == "vm" else "local"
    if cfg["isolation"] == "vm":
        cfg["cpu"] = float(cfg.get("cpu", 4))
        cfg["memory_gb"] = float(cfg.get("memory_gb", 8))
    else:
        cfg.pop("cpu", None)
        cfg.pop("memory_gb", None)
    return cfg


def effective_qa(raw: Mapping | None, workspace_qa: Mapping | None = None) -> dict:
    cfg = dict(raw or {})
    cfg["isolation"] = "vm" if (cfg.get("isolation") or (workspace_qa or {}).get("isolation")) == "vm" else "local"
    if cfg["isolation"] == "vm":
        cfg.update(cpu=float(cfg.get("cpu", 4)), memory_gb=float(cfg.get("memory_gb", 8)),
                   prepared=cfg.get("prepared", True))
    else:
        for key in ("cpu", "memory_gb", "prepared"):
            cfg.pop(key, None)
    if cfg.get("start"):
        cfg["env"] = {k: str(v) for k, v in (cfg.get("env") or {}).items()}
        cfg["env"].setdefault("NODE_ENV", "production")
        cfg["login"] = cfg.get("login") or None
        if cfg["login"]:
            cfg["login"] = {**cfg["login"], "json": cfg["login"].get("json") or {}}
        cfg["pages"] = cfg.get("pages") or []
        cfg["probes"] = [{**p, "method": p.get("method", "GET"), "json": p.get("json"),
                          "expect_status": p.get("expect_status") or None} for p in cfg.get("probes") or []]
        cfg["ready_path"] = cfg.get("ready_path", "/")
        cfg["ready_timeout"] = float(cfg.get("ready_timeout", 300))
        cfg["required"] = bool(cfg.get("required"))
    return cfg


def configuration_version(*, role: str, system: str, harness: str, provider: ProviderConfig, model: str,
                          settings: Mapping, repositories: Mapping | None = None,
                          sandbox_kind: str = "local") -> tuple[str, dict]:
    from .pipeline.runtime import merged_settings
    from . import prompts

    effective = merged_settings(settings)
    # Resolve selections first: other roles' choices and unused pricing are not behaviour inputs.
    selected = {k: v for k, v in effective.items() if k not in {
        "models", "harness", "provider", "pricing", "theme", "presentation", "mind",
    } and k.lower() not in CREDENTIAL_FIELDS}
    selected["limits"] = {k: v for k, v in effective["limits"].items()
                          if not k.endswith("_turns") or k == f"{role}_turns"}
    selected["qa"] = effective_qa(effective.get("qa"))
    if effective["budgets"].get("run_usd") is not None:
        selected["pricing"] = price_for(model, effective)
    repos = {}
    for name, raw in (repositories or {}).items():
        cfg = dict(raw or {})
        cfg["build"] = effective_build(cfg.get("build"))
        cfg["qa"] = effective_qa(cfg.get("qa"), effective.get("qa"))
        repos[name] = cfg
    # verifier_system interpolates a task-specific base SHA; the instructions do not vary.
    if role == "verifier":
        # Match the complete template, preserving any actual instruction edits.
        pattern = re.escape(prompts.VERIFIER).replace(re.escape("{base}"), r"[^\s`]+")
        if re.fullmatch(pattern, system):
            system = prompts.VERIFIER
    snapshot = sanitize({"role": role, "system": system, "harness": harness,
                         "implementation": implementation_fingerprint(),
                         "runtime": runtime_version(harness, sandbox_kind), "sandbox": sandbox_kind,
                         "provider": provider.provider, "endpoint": private_digest(provider.base_url),
                         "model": model, "settings": selected, "repositories": repos})
    snapshot = json.loads(canonical_json(snapshot))
    return hashlib.sha256(canonical_json(snapshot).encode()).hexdigest()[:16], snapshot
