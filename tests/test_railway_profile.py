"""The trusted job command must establish its required tool before testing."""
import json
import os
import subprocess
import sys
from pathlib import Path

import pytest


@pytest.mark.parametrize("optimize", ["", "1"])
@pytest.mark.parametrize("version, expected", [
    (None, False), ("uv 0.12.4", False), ("uv 0.12.50", False),
    ("uv 0.12.5", True), ("uv 0.12.5 (x86_64-unknown-linux-gnu)", True),
])
def test_regressions_require_pinned_uv_before_running_pytest(tmp_path, version, expected, optimize):
    profile = json.loads((Path(__file__).parents[1] / "deploy/railway/profile.json").read_text())
    command = profile["checks"]["regressions"]
    assert command == profile["smoke"][0]
    binaries = tmp_path / ".venv/bin"
    binaries.mkdir(parents=True)
    (binaries / "python").symlink_to(sys.executable)
    if version:
        uv = binaries / "uv"
        uv.write_text(f"#!/bin/sh\nprintf '%s\\n' '{version}'\n")
        uv.chmod(0o755)
    # This marker stands in for pytest, so a failure cannot accidentally run it.
    marker = tmp_path / "regressions-ran"
    (tmp_path / "pytest.py").write_text(
        "import shutil\nfrom pathlib import Path\n"
        "assert Path(shutil.which('uv')) == Path('.venv/bin/uv').absolute()\n"
        "Path('regressions-ran').write_text('started')\n"
    )
    result = subprocess.run(command, cwd=tmp_path, capture_output=True, text=True,
                            env={**os.environ, "PYTHONPATH": str(tmp_path), "PYTHONOPTIMIZE": optimize})
    assert marker.exists() == expected
    assert (result.returncode == 0) == expected
