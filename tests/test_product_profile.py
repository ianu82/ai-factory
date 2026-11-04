import copy
import subprocess

import pytest

from auto_mindsdb_factory.product_profile import ProductProfile, ProfileError


@pytest.fixture
def profile_document():
    return {"version": 1, "name": "sample", "repository": "owner/product", "image": "python@sha256:" + "a" * 64,
            "checks": {"unit": ["python", "-m", "unittest"]}, "smoke": [["python", "--version"]]}


def test_profile_is_resolved_immutable_and_hashes_configuration(profile_document):
    profile = ProductProfile.from_document(profile_document)
    original = profile.digest
    profile_document["checks"]["unit"].append("changed")
    view = profile.document
    view["checks"].clear()
    assert profile.digest == original
    assert profile.document["checks"]
    assert profile.document["auto_merge"] is False
    assert profile.digest != ProductProfile.from_document(profile_document).digest


def test_profile_model_defaults_and_overrides(profile_document):
    defaults = ProductProfile.from_document(profile_document).document
    assert defaults["builder_model"] == "gpt-6.1-terra"
    assert defaults["reviewer_model"] == "gpt-6.1-terra"

    for role in ("builder_model", "reviewer_model"):
        resolved = ProductProfile.from_document({**profile_document, role: "custom-model"}).document
        assert resolved[role] == "custom-model"
        other_role = "reviewer_model" if role == "builder_model" else "builder_model"
        assert resolved[other_role] == "gpt-6.1-terra"


@pytest.mark.parametrize("change", [{"typo": True}, {"image": "python:latest"}, {"checks": {}},
    {"max_attempts": 0}, {"max_attempts": True}, {"base_branch": "../main"},
    {"allowed_paths": ["../secret"]}, {"smoke": []}, {"setup": [[""]]}])
def test_invalid_profiles_fail_closed(profile_document, change):
    with pytest.raises(ProfileError):
        ProductProfile.from_document({**profile_document, **change})


def test_origin_must_match_and_profile_is_external(tmp_path, profile_document):
    subprocess.run(["git", "init", "-q", str(tmp_path)], check=True)
    subprocess.run(["git", "-C", str(tmp_path), "remote", "add", "origin", "git@github.com:owner/product.git"], check=True)
    profile = ProductProfile.from_document(profile_document)
    profile.validate_repository(tmp_path)
    wrong = copy.deepcopy(profile_document)
    wrong["repository"] = "owner/other"
    with pytest.raises(ProfileError, match="does not match"):
        ProductProfile.from_document(wrong).validate_repository(tmp_path)
    with pytest.raises(ProfileError, match="outside"):
        ProductProfile.load(tmp_path / "profile.json", target_root=tmp_path)
