"""Bounded, non-generating credential checks and allowlisted public diagnostics."""

from __future__ import annotations

from typing import Any

from .config import get_settings
from .inference import InferenceError, ModelClient, ProviderConfig
from .security import redact

# Messages are local constants, never upstream exception text.
MESSAGES = {
    "identity_invalid": "MindsHub did not accept this key. Create a new API key and try again.",
    "inference_denied": "This key cannot access inference. Enable inference access and check your credits.",
    "models_empty": "This key has no available models. Enable models for your account and try again.",
    "models_available": "Model access verified. Available models are listed below.",
    "network_error": "The service could not be reached. Check your connection and try again.",
    "rate_limited": "The service is rate limiting requests. Wait a minute and try again.",
    "service_error": "The service could not complete the check. Try again shortly.",
    "github_invalid": "GitHub did not accept this token. Create a new token and try again.",
    "github_verified": "GitHub account and required repository permissions verified.",
    "github_permissions_missing": "This token lacks required permissions. Create a token with the permissions listed below.",
    "github_permissions_unverified": "GitHub account verified. Some repository permissions still need verification.",
    "github_repository_unverified": "Repository access could not be verified. Check the repository name, token selection and organization policies.",
}


def result(code: str, status: str, **metadata: Any) -> tuple[str, str, dict[str, Any]]:
    return status, MESSAGES[code], {"diagnostic_code": code, "test_status": status, **metadata}


def safe_metadata(metadata: dict[str, Any], value: str) -> dict[str, Any]:
    """Only diagnostic fields may cross the credential API boundary."""
    strings = {"organization_id", "organization_name", "workspace_id", "login", "name", "repository"}
    safe: dict[str, Any] = {}
    for key in strings:
        if isinstance(metadata.get(key), (str, int)):
            # Redact the full upstream value first: truncation can turn an echoed
            # credential into a prefix that exact-value redaction cannot detect.
            safe[key] = redact(str(metadata[key]), (value,))[:200]
    if metadata.get("diagnostic_code") in MESSAGES:
        safe["diagnostic_code"] = metadata["diagnostic_code"]
    if metadata.get("test_status") in ("valid", "invalid", "untested"):
        safe["test_status"] = metadata["test_status"]
    if isinstance(metadata.get("models"), list):
        safe["models"] = [redact(m, (value,))[:200] for m in metadata["models"][:80] if isinstance(m, str)]
    if isinstance(metadata.get("permissions"), list):
        from .github import REQUIRED_PERMISSIONS, PERMISSION_EVIDENCE, PERMISSION_INSTRUCTION

        safe["permissions"] = []
        for p in metadata["permissions"]:
            if not isinstance(p, dict) or p.get("permission") not in REQUIRED_PERMISSIONS:
                continue
            state = p.get("state") if p.get("state") in ("present", "missing", "unverified") else "unverified"
            evidence = p.get("evidence") if p.get("evidence") in PERMISSION_EVIDENCE else "not_verified"
            safe["permissions"].append({"permission": p["permission"], "required": REQUIRED_PERMISSIONS[p["permission"]],
                                        "state": state, "evidence": evidence,
                                        "instruction": "" if state == "present" else PERMISSION_INSTRUCTION})
    return safe


async def test_models(kind: str, value: str, identity: dict[str, Any] | None = None) -> tuple[str, str, dict[str, Any]]:
    settings = get_settings()
    provider = "mindshub" if kind == "mindshub_api_key" else "openai"
    if provider == "mindshub" and identity is None:
        from .auth import AuthError, mindshub_identity

        try:
            identity = await mindshub_identity(value)
        except AuthError as exc:
            if exc.status in (401, 403):
                return result("identity_invalid", "invalid")
            return result("rate_limited" if exc.status == 429 else "network_error" if exc.status is None else "service_error",
                          "untested")
    metadata = {k: (identity or {}).get(k) for k in ("organization_id", "organization_name", "workspace_id")}
    base = settings.mindshub_api_base if provider == "mindshub" else settings.openai_api_base
    try:
        # Catalog only: no paid generation, retries or unbounded polling.
        models = await ModelClient(ProviderConfig(provider, base, value, provider), timeout=10, max_retries=0).list_models()
    except InferenceError as exc:
        if exc.status in (401, 402, 403):
            return result("inference_denied", "invalid", **metadata)
        code = "rate_limited" if exc.status == 429 else "network_error" if exc.status is None else "service_error"
        return result(code, "untested", **metadata)
    if not models:
        return result("models_empty", "invalid", **metadata)
    return result("models_available", "valid", models=models[:80], **metadata)
