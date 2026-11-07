"""Trusted inference configuration; public execution still requires admission.

The receipt hash and credential-file version must come from approved controller
evidence. A receipt is not reauthentication or proof of model compatibility.
"""

from dataclasses import dataclass, field, replace
import hashlib
import json
import os
from pathlib import Path
import re
import stat

from .connectors import OpenAIResponsesAgentConfig
from .product_inference import InferencePolicy, InferencePolicyError
from .product_inference_auth import _constant, _object, _receipt, require_auth_workspace, require_controller_import_path


def _version(info):
    return (info.st_dev, info.st_ino, info.st_mtime_ns, info.st_ctime_ns, info.st_uid, stat.S_IMODE(info.st_mode))


def _read_owned(path, target, limit, *, credential=False, version=None):
    path = Path(path).absolute()
    parent = path.parent.stat()
    if (path.resolve() != path or path.is_relative_to(target)
            or parent.st_uid != os.geteuid() or stat.S_IMODE(parent.st_mode) != 0o700):
        raise ValueError()
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    try:
        before = os.fstat(descriptor)
        mode = stat.S_IMODE(before.st_mode)
        if (not stat.S_ISREG(before.st_mode) or before.st_uid != os.geteuid()
                or mode & 0o022 or (credential and (mode != 0o600 or _version(before) != version))):
            raise ValueError()
        data = os.read(descriptor, limit + 1)
        if len(data) > limit or len(data) != before.st_size or _version(os.fstat(descriptor)) != _version(before):
            raise ValueError()
        return data
    finally:
        os.close(descriptor)


@dataclass(frozen=True)
class MindsHubQualificationConfig:
    # Keep the key and its binding together; dataclasses.replace used by Codex
    # preserves this immutable pair. The controller is trusted, not sandboxed.
    _credential: tuple[str, str] = field(repr=False)
    model: str
    reasoning_effort: str
    max_output_tokens: int
    timeout_seconds: int
    base_url = "https://api.mindshub.ai/v1/responses"
    fallback_model = organization = project = None
    REASONING_EFFORTS = OpenAIResponsesAgentConfig.REASONING_EFFORTS

    @property
    def api_key(self):
        return self._credential[0]

    @property
    def inference_binding(self):
        return json.loads(self._credential[1])

    @property
    def inference_binding_sha256(self):
        return hashlib.sha256(self._credential[1].encode()).hexdigest()

    def validated(self):
        OpenAIResponsesAgentConfig.validated(self)
        return self


def resolve_qualification_config(settings, role, *, reasoning_effort, timeout_seconds,
        receipt_path, receipt_sha256, credential_file, credential_version, target_root,
        auth_workspace_id=None):
    """Resolve an explicit private file without environment fallback or networking."""
    require_controller_import_path(target_root)
    policy = InferencePolicy.from_document(settings.get("inference"))
    require_auth_workspace(policy, auth_workspace_id)
    try:
        model = settings["reviewer_model" if role == "verifier" else "builder_model"]
        output = settings["agent_max_output_tokens"]
        if (role not in ("planner", "builder", "verifier") or type(model) is not str or not model
                or reasoning_effort not in OpenAIResponsesAgentConfig.REASONING_EFFORTS
                or any(type(n) is not int or n < 1 for n in (output, timeout_seconds))
                or type(receipt_sha256) is not str or not re.fullmatch("[0-9a-f]{64}", receipt_sha256)
                or len(credential_version) != 6 or any(type(n) is not int for n in credential_version)):
            raise ValueError()
        target = Path(target_root).resolve()
        data = _read_owned(receipt_path, target, 65536)
        if hashlib.sha256(data).hexdigest() != receipt_sha256:
            raise ValueError()
        receipt = json.loads(data, object_pairs_hook=_object, parse_constant=_constant)
        identity = receipt["mindshub_identity"]
        # Revalidate the safe schema/policy before reading the secret. Trust in
        # the earlier Auth outcome comes from the separately approved hash.
        expected = _receipt(policy, identity["key_prefix"] + ".", auth_workspace_id,
            {**identity, "valid": True, "auth_method": "api_key"})
        if json.dumps(receipt, sort_keys=True) != json.dumps(expected, sort_keys=True):
            raise ValueError()
        raw = _read_owned(credential_file, target, 4200, credential=True, version=tuple(credential_version))
        text = raw.decode("ascii")
        prefix = "AI_FACTORY_MINDSHUB_API_KEY="
        if not text.startswith(prefix) or not text.endswith("\n") or text.count("\n") != 1:
            raise ValueError()
        key = text[len(prefix):-1]
        if (not 1 <= len(key) <= 4096 or any(not 33 <= ord(c) <= 126 for c in key)
                or not key.partition(".")[2] or key.partition(".")[0] != identity["key_prefix"]):
            raise ValueError()
        binding = {"policy": policy.document, "identity_sha256": receipt["identity_sha256"],
            "receipt_sha256": receipt_sha256, "credential_version": list(credential_version)}
        return MindsHubQualificationConfig((key, json.dumps(binding, sort_keys=True)),
            model, reasoning_effort, output, timeout_seconds).validated()
    except InferencePolicyError:
        raise
    except (OSError, ValueError, KeyError, TypeError):
        raise InferencePolicyError("Could not bind MindsHub qualification configuration") from None


def _canonical_settings(settings):
    return json.dumps(settings, sort_keys=True, separators=(",", ":"), allow_nan=False)


@dataclass(frozen=True)
class MindsHubExecutionContext:
    """One trusted bootstrap's private binding, reusable after dropping privileges."""
    _settings: str = field(repr=False)
    _config: MindsHubQualificationConfig = field(repr=False)

    def role_config(self, settings, role):
        try:
            if role not in ("planner", "builder", "verifier") or _canonical_settings(settings) != self._settings:
                raise ValueError()
            policy = InferencePolicy.from_document(settings.get("inference"))
            if (policy.document != self._config.inference_binding["policy"]
                    or self._config.model != settings["builder_model"]
                    or self._config.max_output_tokens != settings["agent_max_output_tokens"]):
                raise ValueError()
            model = settings["reviewer_model" if role == "verifier" else "builder_model"]
            if type(model) is not str or not model:
                raise ValueError()
            return replace(self._config, model=model).validated()
        except (ValueError, TypeError, KeyError):
            raise InferencePolicyError("Missing or changed MindsHub execution context") from None

    def inspect(self):
        safe = {"profile_sha256": hashlib.sha256(self._settings.encode()).hexdigest(),
            "inference_binding_sha256": self._config.inference_binding_sha256}
        contract = {**safe, "reasoning_effort": self._config.reasoning_effort,
            "timeout_seconds": self._config.timeout_seconds,
            "max_output_tokens": self._config.max_output_tokens}
        return {**safe, "contract_sha256": hashlib.sha256(_canonical_settings(contract).encode()).hexdigest()}


def resolve_execution_context(settings, *, reasoning_effort, timeout_seconds,
        receipt_path, receipt_sha256, credential_file, credential_version, target_root,
        auth_workspace_id=None):
    """Read the private credential once before the controller drops privileges."""
    if any(type(settings.get(field)) is not str or not settings[field]
            for field in ("builder_model", "reviewer_model")):
        raise InferencePolicyError("Invalid MindsHub execution role models")
    canonical = _canonical_settings(settings)
    config = resolve_qualification_config(settings, "builder", reasoning_effort=reasoning_effort,
        timeout_seconds=timeout_seconds, receipt_path=receipt_path, receipt_sha256=receipt_sha256,
        credential_file=credential_file, credential_version=credential_version,
        target_root=target_root, auth_workspace_id=auth_workspace_id)
    context = MindsHubExecutionContext(canonical, config)
    # Validate both configured model roles before admitting any phase.
    for role in ("planner", "builder", "verifier"):
        context.role_config(settings, role)
    return context
