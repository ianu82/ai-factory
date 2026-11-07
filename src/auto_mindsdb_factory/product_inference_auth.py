"""Read-only MindsHub credential identity checks; never admits inference."""

import asyncio
import hashlib
import json
import re
import sys
from importlib import import_module
from importlib.metadata import distribution
from importlib.util import find_spec
from pathlib import Path

from .product_inference import InferencePolicyError, _identifier


AUTH_URL = "https://auth.mindshub.ai/v1/authenticate/"
AUTH_TIMEOUT_SECONDS = 5.0
MAX_RESPONSE_BYTES = 65536
IMPORT_ERROR = "MindsHub credential checks require trusted controller imports"


def require_controller_import_path(target_root=None):
    """Require the trusted controller bootstrap, without changing global import state."""
    roots = (Path(__file__).resolve().parents[2], Path(sys.prefix).resolve(), Path(sys.base_prefix).resolve())
    target = Path(target_root).resolve() if target_root is not None else None
    if target is not None and Path.cwd().resolve().is_relative_to(target):
        raise InferencePolicyError(IMPORT_ERROR)
    for entry in sys.path:
        if type(entry) is not str:
            raise InferencePolicyError(IMPORT_ERROR)
        path = Path(entry or Path.cwd()).resolve()
        if ((target is not None and path.is_relative_to(target))
                or not any(path.is_relative_to(root) for root in roots)):
            raise InferencePolicyError(IMPORT_ERROR)


def _trusted_http_client():
    require_controller_import_path()
    expected = Path(distribution("httpx2").locate_file("httpx2/__init__.py")).resolve()
    spec = find_spec("httpx2")
    if (not expected.is_relative_to(Path(sys.prefix).resolve()) or spec is None
            or type(spec.origin) is not str or Path(spec.origin).resolve() != expected):
        raise InferencePolicyError(IMPORT_ERROR)
    module = import_module("httpx2")
    if type(module.__file__) is not str or Path(module.__file__).resolve() != expected:
        raise InferencePolicyError(IMPORT_ERROR)
    return module


def _object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("Duplicate authentication field")
        result[key] = value
    return result


def _constant(_):
    raise ValueError("Non-JSON authentication value")


async def _read_identity(key):
    from .anton_session import validate_anton_installation
    validate_anton_installation()
    httpx2 = _trusted_http_client()

    async with httpx2.AsyncClient(timeout=AUTH_TIMEOUT_SECONDS, follow_redirects=False) as client:
        async with client.stream("GET", AUTH_URL,
                headers={"Authorization": "Bearer " + key, "Accept": "application/json"}) as response:
            if response.status_code != 200:
                reason = {401: "credential refused", 403: "credential refused", 429: "rate limited"}.get(
                    response.status_code, "authentication unavailable")
                raise InferencePolicyError("MindsHub " + reason)
            data = bytearray()
            async for chunk in response.aiter_bytes(8192):
                if len(data) + len(chunk) > MAX_RESPONSE_BYTES:
                    raise InferencePolicyError("MindsHub authentication response too large")
                data.extend(chunk)
            return json.loads(data, object_pairs_hook=_object, parse_constant=_constant)


def _validate_workspace(policy, auth_workspace_id):
    if auth_workspace_id is None and policy.document.get("scope") == "controller":
        return
    if not _identifier(auth_workspace_id):
        raise InferencePolicyError("A canonical MindsHub Auth workspace UUID is required")


def _receipt(policy, key, auth_workspace_id, payload):
    _validate_workspace(policy, auth_workspace_id)
    if (type(payload) is not dict or payload.get("valid") is not True
            or payload.get("auth_method") != "api_key"
            or (payload.get("key_type"), payload.get("principal_kind"), payload.get("credential_kind"))
                not in (("user", "user", "api_key"), ("service", "service_account", "service_account"))
            or any(not _identifier(payload.get(k)) for k in ("user_id", "organization_id", "workspace_id"))
            or type(payload.get("key_prefix")) is not str
            or not re.fullmatch(r"mdb_[A-Za-z0-9_-]{1,64}", payload["key_prefix"])
            or payload["key_prefix"] != key.partition(".")[0]):
        raise InferencePolicyError("MindsHub credential identity is invalid")
    if auth_workspace_id is not None and payload["workspace_id"] != auth_workspace_id:
        raise InferencePolicyError("MindsHub credential does not match the configured Auth workspace")
    identity = {"issuer": AUTH_URL, **{k: payload[k] for k in (
        "user_id", "organization_id", "workspace_id", "principal_kind", "key_type", "credential_kind", "key_prefix")}}
    safe = {**policy.inspect(), "status": "credential_identity_verified", "mindshub_identity": identity}
    binding = {"policy_sha256": safe["policy_sha256"], "mindshub_identity": identity}
    safe["identity_sha256"] = hashlib.sha256(json.dumps(binding, sort_keys=True).encode()).hexdigest()
    return safe


async def check_credential(policy, key, auth_workspace_id=None):
    require_controller_import_path()
    _validate_workspace(policy, auth_workspace_id)
    if not key:
        raise InferencePolicyError("MindsHub API key is not configured")
    if (type(key) is not str or not key.startswith("mdb_") or "." not in key or len(key) > 4096
            or any(not 33 <= ord(c) <= 126 for c in key)):
        raise InferencePolicyError("Invalid MindsHub API key format")
    try:
        payload = await asyncio.wait_for(_read_identity(key), AUTH_TIMEOUT_SECONDS)
        return _receipt(policy, key, auth_workspace_id, payload)
    except InferencePolicyError:
        raise
    except TimeoutError:
        raise InferencePolicyError("MindsHub authentication timed out") from None
    except Exception:
        raise InferencePolicyError("MindsHub authentication unavailable") from None
