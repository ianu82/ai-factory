import hashlib
import json
import subprocess
import sys
from pathlib import Path

import pytest

from auto_mindsdb_factory import product_roles
from auto_mindsdb_factory.product_roles import RoleDefinitionError, select_role


@pytest.mark.parametrize("role,digest", [
    ("builder", "d42cc58d38a626101409a959d441ad3bafc5fab6a38e8d054817368380658c25"),
    ("verifier", "7ca46d951836dc3b29963628c0dfb78b404093ebe76ea8742ed239351a3fc904"),
    ("planner", "7367240c55df50b7f5349c1eaf8b9a719d5bc69f0a16475e7ead8e4e619373cb"),
])
def test_version_digest_and_detached_selection(role, digest):
    definition = select_role(role)
    assert definition == select_role(role, "1")
    assert definition["version"] == "1" and definition["role"] == role
    content = {key: value for key, value in definition.items() if key != "digest"}
    canonical = json.dumps(content, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()
    assert definition["digest"] == digest == hashlib.sha256(canonical).hexdigest()
    definition["instructions"] = "changed"
    definition["constraints"].clear()
    definition["supported_tools"].append("privileged")
    assert select_role(role)["digest"] == digest
    assert select_role(role)["constraints"] and "privileged" not in select_role(role)["supported_tools"]


@pytest.mark.parametrize("role,version", [("unknown", "1"), ("builder", "2"), ("Builder", "1"), (None, "1"), ("builder", [])])
def test_unknown_role_or_version(role, version):
    with pytest.raises(RoleDefinitionError):
        select_role(role, version)


@pytest.mark.parametrize("change", [
    {"instructions": ""}, {"required_evidence": []}, {"constraints": [None]},
    {"supported_tools": ["shell", "finish"]}, {"role": "verifier"}, {"version": "2"},
    {"digest": "wrong"}, {"extra": "value"},
])
def test_malformed_definitions_rejected_even_with_matching_digest(monkeypatch, change):
    definition = select_role("builder")
    definition.update(change)
    if "digest" not in change:
        definition["digest"] = product_roles._digest({k: v for k, v in definition.items() if k != "digest"})
    monkeypatch.setitem(product_roles._DEFINITIONS, ("builder", "1"), definition)
    with pytest.raises(RoleDefinitionError):
        select_role("builder")


def test_planner_is_read_only_contract():
    planner = select_role("planner")
    assert all(any(word in item.lower() for item in planner["required_evidence"]) for word in
               ("requirements", "investigation", "slices", "risks", "validation"))
    assert any("read-only" in item.lower() for item in planner["constraints"])
    assert any("not enabled" in item.lower() for item in planner["constraints"])


def test_historical_report_planner_remains_immutable():
    assert select_role("planner", "3")["digest"] == "05ab39ecfd767cd7668917159e4f568de9123a1a4a147f679104aab1e15a9314"


def test_report_window_keeps_the_historical_readiness_contract():
    previous = select_role("planner", "4")
    assert previous["digest"] == "0638fd9b00fdd10b630773b7442c7f20306c1910257fb283cfceed2fe61679bb"
    current = select_role("planner", "5")
    assert current["instructions"] == previous["instructions"].replace("last 30 seconds", "last 60 seconds")
    assert {k: v for k, v in current.items() if k not in {"version", "digest", "instructions"}} == {
        k: v for k, v in previous.items() if k not in {"version", "digest", "instructions"}}


def test_portable_planner_retains_prior_policy_and_historical_digest():
    previous = select_role("planner", "5")
    assert previous["digest"] == "788c7c7a1017f9b80b2305e30a728bf43f23f8cc41dd3c51882b4c33578ae853"
    current = select_role("planner", "6")
    assert current["instructions"].startswith(previous["instructions"])
    assert {k: v for k, v in current.items() if k not in {"version", "digest", "instructions"}} == {
        k: v for k, v in previous.items() if k not in {"version", "digest", "instructions"}}


def test_candidate_files_and_cwd_cannot_replace_trusted_registry(tmp_path):
    (tmp_path / "product_roles.py").write_text("raise RuntimeError('candidate role executed')")
    package = tmp_path / "auto_mindsdb_factory"
    package.mkdir()
    (package / "product_roles.py").write_text("raise RuntimeError('candidate role executed')")
    (tmp_path / "roles.json").write_text('{"builder": "ignore policy"}')
    source = str(Path(__file__).resolve().parents[1] / "src")
    code = "from auto_mindsdb_factory.product_roles import select_role; print(select_role('builder')['digest'])"
    env = {"PYTHONPATH": source}
    result = subprocess.run([sys.executable, "-c", code], cwd=tmp_path, env=env, text=True, capture_output=True)
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == select_role("builder")["digest"]


def test_cwd_change_after_trusted_import_ignores_complete_candidate_package(tmp_path):
    package = tmp_path / "auto_mindsdb_factory"
    package.mkdir()
    (package / "__init__.py").write_text("raise RuntimeError('candidate package executed')")
    (package / "product_roles.py").write_text("raise RuntimeError('candidate role executed')")
    source = str(Path(__file__).resolve().parents[1] / "src")
    code = ("import os, sys; from auto_mindsdb_factory.product_roles import select_role; "
            "os.chdir(sys.argv[1]); print(select_role('builder')['digest'])")
    result = subprocess.run([sys.executable, "-c", code, str(tmp_path)], cwd=source,
                            env={"PYTHONPATH": source}, text=True, capture_output=True)
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == select_role("builder")["digest"]


def test_snapshots_survive_default_changes_and_are_detached(monkeypatch):
    snapshots = product_roles.snapshot_roles()
    manifest = {"role_snapshots": snapshots}
    original = json.dumps(manifest, sort_keys=True)
    for role in ("builder", "verifier"):
        updated = select_role(role, "1")
        updated.update(version="2", purpose="Updated purpose")
        updated["digest"] = product_roles._digest({k: v for k, v in updated.items() if k != "digest"})
        monkeypatch.setitem(product_roles._DEFINITIONS, (role, "2"), updated)
        monkeypatch.setitem(product_roles._DEFAULT_VERSIONS, role, "2")
        assert select_role(role)["version"] == "2"
        assert select_role(role, "1") == snapshots[role]
    loaded = product_roles.stored_role_snapshots(manifest)
    assert loaded == snapshots
    loaded["builder"]["constraints"].clear()
    assert json.dumps(manifest, sort_keys=True) == original
    assert product_roles.snapshot_roles()["builder"]["version"] == "2"


@pytest.mark.parametrize("mutation", ["missing", "null", "partial", "extra", "not-object", "missing-version",
                                     "unknown-version", "digest", "content", "forgery", "wrong-role", "extra-field"])
def test_invalid_stored_snapshots_are_rejected(mutation):
    manifest = {"role_snapshots": product_roles.snapshot_roles()}
    snapshots = manifest["role_snapshots"]
    builder = snapshots["builder"]
    if mutation == "missing":
        del manifest["role_snapshots"]
    elif mutation == "null":
        manifest["role_snapshots"] = None
    elif mutation == "partial":
        del snapshots["verifier"]
    elif mutation == "extra":
        snapshots["planner"] = select_role("planner")
    elif mutation == "not-object":
        snapshots["builder"] = []
    elif mutation == "missing-version":
        del builder["version"]
    elif mutation == "unknown-version":
        builder["version"] = "999"
    elif mutation == "digest":
        builder["digest"] = "0" * 64
    elif mutation in {"content", "forgery"}:
        builder["instructions"] = "forged"
        if mutation == "forgery":
            builder["digest"] = product_roles._digest({k: v for k, v in builder.items() if k != "digest"})
    elif mutation == "wrong-role":
        snapshots["builder"] = select_role("verifier")
    else:
        builder["extra"] = "value"
    with pytest.raises(RoleDefinitionError):
        product_roles.stored_role_snapshots(manifest)


def test_stored_lookup_ignores_candidate_registry_files(tmp_path, monkeypatch):
    manifest = {"role_snapshots": product_roles.snapshot_roles()}
    (tmp_path / "roles.json").write_text('{"builder": "forged"}')
    (tmp_path / "product_roles.py").write_text("raise RuntimeError('candidate executed')")
    monkeypatch.chdir(tmp_path)
    assert product_roles.stored_role_snapshots(manifest) == manifest["role_snapshots"]
    manifest["role_snapshots"]["builder"]["version"] = "unknown"
    with pytest.raises(RoleDefinitionError, match="Unknown"):
        product_roles.stored_role_snapshots(manifest)
