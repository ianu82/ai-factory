"""Bounded repository evidence for drafting agents; never read local env files."""

from __future__ import annotations

import subprocess
from pathlib import Path
from typing import Any

from .connectors import sanitize_factory_document


def repository_context(root: Path) -> dict[str, Any]:
    try:
        files = subprocess.run(
            ["git", "-C", str(root), "ls-files", "-z"], check=True,
            capture_output=True, text=True, timeout=15,
        ).stdout.split("\0")
    except (OSError, subprocess.SubprocessError):
        return {"available": False, "reason": "tracked file inventory unavailable"}
    paths = sorted(path for path in files if path)
    manifests = {}
    for name in ("pyproject.toml", "package.json"):
        path = root / name
        if name in paths and not path.is_symlink():
            manifests[name] = path.read_text(encoding="utf-8")[:6000]
    return sanitize_factory_document({
        "available": True, "tracked_paths": paths[:1000],
        "inventory_truncated": len(paths) > 1000, "project_manifests": manifests,
    })
