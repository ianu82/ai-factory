import copy
import json

import pytest

from auto_mindsdb_factory.__main__ import main
from auto_mindsdb_factory.connectors import OpenAIResponsesAgentConfig
from auto_mindsdb_factory.product_delivery import ProductDelivery
from auto_mindsdb_factory.product_execution import execution_manifest, role_config
from auto_mindsdb_factory.product_linear_worker import LinearProductWorker
from auto_mindsdb_factory.product_profile import ProductProfile, ProfileError


WORKSPACE = "00000000-0000-4000-8000-000000000001"
REFERENCE = "00000000-0000-4000-8000-000000000002"
OTHER = "00000000-0000-4000-8000-000000000003"
PRIVATE = "private-input-must-not-be-echoed"


def policy():
    return {"version": 1, "provider": "mindshub", "protocol": "responses",
        "endpoint": "https://api.mindshub.ai/v1/responses", "workspace_id": WORKSPACE,
        "credential_ref": {"workspace_id": WORKSPACE, "id": REFERENCE, "revision": 1}}


def controller_policy():
    result = policy()
    result.pop("workspace_id")
    result["credential_ref"].pop("workspace_id")
    return {**result, "scope": "controller"}


def test_controller_scope_is_explicit_and_preserves_legacy_workspace_identity():
    profile = ProductProfile.from_document(document(inference=controller_policy()))
    assert profile.document["inference"] == controller_policy()
    assert profile.digest != ProductProfile.from_document(document(inference=policy())).digest
    changed = controller_policy()
    changed["credential_ref"]["revision"] = 2
    assert ProductProfile.from_document(document(inference=changed)).digest != profile.digest


@pytest.mark.parametrize("value", [
    {**controller_policy(), "scope": "global"}, {**controller_policy(), "scope": True},
    {**controller_policy(), "workspace_id": WORKSPACE},
    {**controller_policy(), "credential_ref": policy()["credential_ref"]},
    {**policy(), "scope": "controller"}, {**policy(), "scope": "workspace"},
])
def test_ambiguous_controller_and_workspace_scope_is_rejected(value):
    with pytest.raises(ProfileError, match="Invalid inference policy"):
        ProductProfile.from_document(document(inference=value))


def document(**extra):
    return {"version": 1, "name": "sample", "repository": "owner/product",
        "image": "python@sha256:" + "a" * 64, "checks": {"unit": ["python", "-m", "unittest"]},
        "smoke": [["python", "--version"]], **extra}


def test_provider_metadata_is_detached_frozen_and_distinct_from_legacy():
    original = document(inference=policy())
    profile = ProductProfile.from_document(original)
    identity = profile.digest
    original["inference"]["credential_ref"]["revision"] = 2
    view = profile.document
    view["inference"]["workspace_id"] = OTHER
    assert profile.document["inference"] == policy() and profile.digest == identity
    assert ProductProfile.from_document(original).digest != identity
    changed = policy()
    changed["workspace_id"] = changed["credential_ref"]["workspace_id"] = OTHER
    assert ProductProfile.from_document(document(inference=changed)).digest != identity
    legacy = ProductProfile.from_document(document())
    assert "inference" not in legacy.document
    assert legacy.digest == "f64a16bebc844dcd3b58e7912c8a83be243372e6d9e7340446ae6d36781f5353"


@pytest.mark.parametrize("value", [None, PRIVATE, [],
    {**policy(), "api_key": PRIVATE}, {**policy(), "provider": PRIVATE},
    {**policy(), "protocol": "chat"}, {**policy(), "version": True},
    {k: v for k, v in policy().items() if k != "endpoint"},
    {**policy(), "endpoint": "https://user:" + PRIVATE + "@api.mindshub.ai/v1/responses"},
    {**policy(), "endpoint": "https://api.mindshub.ai/v1/responses?key=" + PRIVATE},
    {**policy(), "endpoint": "http://api.mindshub.ai/v1/responses"},
    {**policy(), "workspace_id": PRIVATE},
    {**policy(), "credential_ref": None},
    {**policy(), "credential_ref": {**policy()["credential_ref"], "id": PRIVATE}},
    {**policy(), "credential_ref": {**policy()["credential_ref"], "workspace_id": OTHER}},
    {**policy(), "credential_ref": {**policy()["credential_ref"], "revision": True}},
    {**policy(), "credential_ref": {**policy()["credential_ref"], "revision": 0}},
    {**policy(), "credential_ref": {**policy()["credential_ref"], "revision": 2**31}},
    {**policy(), "credential_ref": {**policy()["credential_ref"], "api_key": PRIVATE}},
])
def test_invalid_or_secret_bearing_policy_is_rejected_without_echo(value):
    before = copy.deepcopy(value)
    with pytest.raises(ProfileError) as error:
        ProductProfile.from_document(document(inference=value))
    assert str(error.value) == "Invalid inference policy"
    assert PRIVATE not in str(error.value) and value == before


def forbidden(*args, **kwargs):
    raise AssertionError("No credentials, admission or resources permitted")


@pytest.mark.parametrize("role", ["planner", "builder", "verifier"])
@pytest.mark.parametrize("inference", [policy(), controller_policy()])
def test_unready_policy_never_resolves_legacy_credentials(role, inference, monkeypatch):
    monkeypatch.setattr(OpenAIResponsesAgentConfig, "from_env", forbidden)
    settings = ProductProfile.from_document(document(inference=inference)).document
    with pytest.raises(ValueError, match="not execution-ready"):
        role_config(settings, role)
    with pytest.raises(ValueError, match="not execution-ready"):
        execution_manifest(settings)


def test_unready_delivery_and_worker_stop_before_resources_or_claims(tmp_path, monkeypatch):
    from auto_mindsdb_factory import product_linear_worker
    monkeypatch.setattr(ProductProfile, "validate_repository", forbidden)
    monkeypatch.setattr(product_linear_worker, "LinearDeliveryState", forbidden)
    profile = ProductProfile.from_document(document(inference=policy(), auto_merge=True,
        required_status_checks=["validate"]))
    with pytest.raises(ValueError, match="not execution-ready"):
        ProductDelivery(profile, tmp_path / "target", tmp_path / "store")
    with pytest.raises(ValueError, match="not execution-ready"):
        LinearProductWorker(profile, tmp_path / "target", tmp_path / "store", None, None)


def test_public_validation_reports_configuration_without_activation(validation_checkout, monkeypatch, capsys):
    root, target = validation_checkout
    monkeypatch.setattr(OpenAIResponsesAgentConfig, "from_env", forbidden)
    path = root / "profile.json"
    path.write_text(json.dumps(document(inference=policy())))
    assert main(["product-validate", "--profile", str(path), "--target-root", str(target)]) == 0
    result = json.loads(capsys.readouterr().out)
    assert result["status"] == "valid" and result["builder_model"] == "gpt-6.1-terra"
    assert result["inference"]["provider"] == "mindshub"
    assert result["inference"]["credential_ref"] == policy()["credential_ref"]
    assert result["inference"]["status"] == "configured"
    assert result["inference"]["execution_ready"] is False
    path.write_text(json.dumps(document(inference={**policy(), "api_key": PRIVATE})))
    assert main(["product-validate", "--profile", str(path), "--target-root", str(target)]) == 1
    captured = capsys.readouterr()
    assert PRIVATE not in captured.err and captured.out == ""
