"""Trusted, versioned product configuration, independent of candidate source."""

from __future__ import annotations

import hashlib
import json
import re
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from jsonschema import Draft202012Validator

from .product_harness import HARNESS_NAMES, HarnessSelectionError, resolve_harness


class ProfileError(ValueError):
    pass


COMMAND = {"type": "array", "minItems": 1, "items": {"type": "string", "minLength": 1}}
COMMANDS = {"type": "array", "items": COMMAND}
RATE = {
    "type": "object", "additionalProperties": False,
    "required": ["input", "cached_input", "output", "max_input_tokens", "source", "as_of"],
    "properties": {**{name: {"type": "number", "minimum": 0} for name in ["input", "cached_input", "output"]},
        "max_input_tokens": {"type": "integer", "minimum": 1},
        "source": {"type": "string", "pattern": "^https://"},
        "as_of": {"type": "string", "pattern": "^[0-9]{4}-[0-9]{2}-[0-9]{2}$"}},
}
SCHEMA = {
    "type": "object", "additionalProperties": False,
    "required": ["version", "name", "repository", "image", "checks", "smoke"],
    "properties": {
        "version": {"const": 1},
        "name": {"type": "string", "pattern": "^[a-z][a-z0-9-]{0,62}$"},
        "repository": {"type": "string", "pattern": "^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$"},
        "base_branch": {"type": "string", "pattern": "^[A-Za-z0-9][A-Za-z0-9_./-]*$"},
        "image": {"type": "string", "pattern": "^[a-zA-Z0-9./:_-]+@sha256:[0-9a-f]{64}$"},
        "platform": {"enum": ["linux/amd64", "linux/arm64"]},
        "pricing": {"type": "object", "additionalProperties": RATE},
        "setup": COMMANDS,
        "setup_network": {"type": "boolean"},
        "checks": {"type": "object", "minProperties": 1, "additionalProperties": COMMAND},
        "smoke": {**COMMANDS, "minItems": 1},
        "allowed_paths": {"type": "array", "minItems": 1, "items": {"type": "string", "minLength": 1}},
        "protected_paths": {"type": "array", "items": {"type": "string", "minLength": 1}},
        "auto_merge": {"type": "boolean"},
        "required_status_checks": {"type": "array", "uniqueItems": True, "items": {"type": "string", "minLength": 1}},
        "builder_model": {"type": "string", "minLength": 1},
        "reviewer_model": {"type": "string", "minLength": 1},
        "builder_harness": {"enum": list(HARNESS_NAMES)},
        "reviewer_harness": {"enum": list(HARNESS_NAMES)},
        "max_attempts": {"type": "integer", "minimum": 1, "maximum": 5},
        "max_agent_turns": {"type": "integer", "minimum": 1, "maximum": 100},
        "agent_max_output_tokens": {"type": "integer", "minimum": 256, "maximum": 32000},
        "max_total_tokens": {"type": "integer", "minimum": 1000, "maximum": 1000000},
        "command_timeout_seconds": {"type": "integer", "minimum": 1, "maximum": 1800},
        "max_run_seconds": {"type": "integer", "minimum": 30, "maximum": 14400},
        "max_changed_lines": {"type": "integer", "minimum": 1, "maximum": 2000},
    },
}
# Implicit native harness selection must not alter already activated profile hashes.
DEFAULTS = {
    "base_branch": "main", "setup": [], "setup_network": False,
    "platform": "linux/amd64", "pricing": {},
    "allowed_paths": ["*"],
    "protected_paths": [".git", ".git/**", ".github/**", ".env", ".env.*"],
    "auto_merge": False, "required_status_checks": [],
    "builder_model": "gpt-6.1-terra", "reviewer_model": "gpt-6.1-terra",
    "max_attempts": 2, "max_agent_turns": 25, "max_total_tokens": 200000, "agent_max_output_tokens": 8000,
    "command_timeout_seconds": 300, "max_run_seconds": 1800, "max_changed_lines": 500,
}


@dataclass(frozen=True)
class ProductProfile:
    # Canonical JSON is immutable and can be retained verbatim with each run.
    canonical_json: str

    @classmethod
    def from_document(cls, document: dict[str, Any]) -> "ProductProfile":
        errors = sorted(Draft202012Validator(SCHEMA).iter_errors(document), key=lambda e: str(e.path))
        if errors:
            raise ProfileError(f"Invalid product profile at {list(errors[0].path)}: {errors[0].message}")
        try:
            for role in ("builder", "verifier"):
                resolve_harness(document, role)
        except HarnessSelectionError as exc:
            raise ProfileError(str(exc)) from None
        resolved = {**DEFAULTS, **document}
        branch = resolved["base_branch"]
        if subprocess.run(["git", "check-ref-format", "--branch", branch], capture_output=True).returncode:
            raise ProfileError("Invalid base branch")
        for pattern in resolved["allowed_paths"] + resolved["protected_paths"]:
            if pattern.startswith(("/", "-")) or ".." in pattern.split("/") or "\x00" in pattern:
                raise ProfileError("Path patterns must be relative and cannot traverse directories")
        return cls(json.dumps(resolved, sort_keys=True, separators=(",", ":"), allow_nan=False))

    @classmethod
    def load(cls, path: Path, *, target_root: Path | None = None) -> "ProductProfile":
        if target_root and path.resolve().is_relative_to(target_root.resolve()):
            raise ProfileError("Product profile must be outside the candidate repository")
        try:
            return cls.from_document(json.loads(path.read_text(encoding="utf-8")))
        except (OSError, json.JSONDecodeError) as exc:
            raise ProfileError("Could not read product profile") from exc

    @property
    def document(self) -> dict[str, Any]:
        return json.loads(self.canonical_json)

    @property
    def digest(self) -> str:
        return hashlib.sha256(self.canonical_json.encode()).hexdigest()

    def validate_repository(self, target_root: Path) -> None:
        result = subprocess.run(
            ["git", "-C", str(target_root), "remote", "get-url", "origin"],
            capture_output=True, text=True, timeout=15,
        )
        remote = result.stdout.strip()
        match = re.fullmatch(r"(?:https://github\.com/|git@github\.com:|ssh://git@github\.com/)([^/]+/[^/]+?)(?:\.git)?", remote)
        if result.returncode or not match or match[1].lower() != self.document["repository"].lower():
            # A remote URL may contain credentials. Never include it in the error.
            raise ProfileError("Checkout origin does not match the configured GitHub repository")
