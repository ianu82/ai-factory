"""Non-secret provider configuration; credential resolution is not enabled yet."""

from dataclasses import dataclass
import hashlib
import json
from uuid import UUID


class InferencePolicyError(ValueError):
    pass


def _identifier(value):
    try:
        return type(value) is str and str(UUID(value)) == value
    except ValueError:
        return False


@dataclass(frozen=True)
class InferencePolicy:
    canonical_json: str

    @classmethod
    def from_document(cls, document):
        if type(document) is not dict:
            raise InferencePolicyError("Invalid inference policy")
        controller = document.get("scope") == "controller"
        fields = {"version", "provider", "protocol", "endpoint", "credential_ref"}
        fields |= {"scope"} if controller else {"workspace_id"}
        fixed = {"provider": "mindshub", "protocol": "responses",
                 "endpoint": "https://api.mindshub.ai/v1/responses"}
        if controller:
            fixed["scope"] = "controller"
        if (set(document) != fields
                or type(document["version"]) is not int or document["version"] != 1
                or any(type(document[k]) is not str or document[k] != v for k, v in fixed.items())
                or (not controller and not _identifier(document["workspace_id"]))):
            raise InferencePolicyError("Invalid inference policy")
        ref = document["credential_ref"]
        ref_fields = {"id", "revision"} | (set() if controller else {"workspace_id"})
        if (type(ref) is not dict or set(ref) != ref_fields
                or (not controller and (not _identifier(ref["workspace_id"])
                    or ref["workspace_id"] != document["workspace_id"]))
                or not _identifier(ref["id"])
                or type(ref["revision"]) is not int or not 1 <= ref["revision"] <= 2**31 - 1):
            raise InferencePolicyError("Invalid inference policy")
        return cls(json.dumps(document, sort_keys=True, separators=(",", ":"), allow_nan=False))

    @property
    def document(self):
        return json.loads(self.canonical_json)

    def inspect(self):
        return {**self.document, "policy_sha256": hashlib.sha256(self.canonical_json.encode()).hexdigest(),
                "status": "configured", "execution_ready": False}


def require_inference_execution(settings):
    if "inference" in settings:
        raise InferencePolicyError("Configured inference is not execution-ready")
