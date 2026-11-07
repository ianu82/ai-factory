"""No application or dependency import may precede trusted-path admission."""

import os
from pathlib import Path
import subprocess
import sys

import pytest

from auto_mindsdb_factory import controller_imports as guard


def test_stdlib_guard_accepts_qualified_source_and_changes_no_paths():
    before = list(sys.path)
    guard.require_controller_import_path()
    assert sys.path == before


@pytest.mark.parametrize("entry", [None, "/unapproved", ""])
def test_guard_rejects_unapproved_or_candidate_search_paths(tmp_path, monkeypatch, entry):
    if entry == "": monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(guard.sys, "path", [entry])
    with pytest.raises(ValueError, match="trusted controller imports"):
        guard.require_controller_import_path(tmp_path)


def test_guard_runs_before_synthetic_dependency_shadow(tmp_path):
    marker = tmp_path / "marker"
    (tmp_path / "jsonschema.py").write_text("from pathlib import Path\nPath(" + repr(str(marker)) + ").touch()\n")
    source = Path(__file__).resolve().parents[1] / "src"
    program = "from auto_mindsdb_factory.controller_imports import require_controller_import_path\nrequire_controller_import_path()\nimport jsonschema"
    result = subprocess.run([sys.executable, "-c", program], cwd=tmp_path,
        env={"PATH": os.defpath, "PYTHONPATH": str(source)}, capture_output=True, text=True, timeout=3)
    assert result.returncode != 0 and "trusted controller imports" in result.stderr
    assert not marker.exists()


@pytest.mark.parametrize("with_target", [False, True])
def test_guard_rejects_mutable_symlink_alias_into_trusted_source(tmp_path, monkeypatch, with_target):
    source = Path(guard.__file__).resolve().parents[1]
    candidate = tmp_path / "candidate"
    candidate.mkdir()
    alias = candidate / "imports"
    alias.symlink_to(source, target_is_directory=True)
    monkeypatch.setattr(guard.sys, "path", [str(alias)])
    with pytest.raises(ValueError, match="trusted controller imports"):
        guard.require_controller_import_path(candidate if with_target else None)
    assert guard.sys.path == [str(alias)]
