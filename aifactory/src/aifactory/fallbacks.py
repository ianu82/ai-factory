"""Server-level fallback credentials belong to the operator's bootstrap workspace only.

The operator can configure a GitHub token and model keys on the server (``AIFACTORY_BOOTSTRAP_*``, and
``GH_TOKEN``/``GITHUB_TOKEN`` for GitHub). They apply only to the workspace the current bootstrap manages,
recorded by its exact id at startup (before the orchestrator starts), never matched by name. Every other
workspace must add its own credentials, so it can never reach the operator's repositories or bill the
operator's model keys, and its setup shows those credentials as missing.
"""

from __future__ import annotations

import os

from .config import get_settings

_bootstrap_workspace_id: str | None = None


def set_bootstrap_workspace(workspace_id: str | None) -> None:
    global _bootstrap_workspace_id
    _bootstrap_workspace_id = workspace_id or None


def bootstrap_workspace() -> str | None:
    """The workspace the current bootstrap manages (None before startup applied it, or without a bootstrap)."""
    return _bootstrap_workspace_id


def allowed(workspace_id: str | None) -> bool:
    return bool(workspace_id) and workspace_id == _bootstrap_workspace_id


def github_token(workspace_id: str | None) -> str:
    if not allowed(workspace_id):
        return ""
    return get_settings().bootstrap_github_token or os.environ.get("GH_TOKEN") or os.environ.get("GITHUB_TOKEN", "")


def model_keys(workspace_id: str | None) -> tuple[str, str]:
    """(MindsHub key, OpenAI key) this workspace may fall back to; empty for every non-bootstrap workspace."""
    if not allowed(workspace_id):
        return "", ""
    settings = get_settings()
    return settings.bootstrap_mindshub_key, settings.bootstrap_openai_key


def available(workspace_id: str | None) -> dict[str, bool]:
    mindshub, openai = model_keys(workspace_id)
    return {"github_token": bool(github_token(workspace_id)), "openai_api_key": bool(openai),
            "mindshub_api_key": bool(mindshub)}
