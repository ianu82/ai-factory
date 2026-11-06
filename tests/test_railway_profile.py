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
        # The real uv executable is covered by the invalid-lock integration.
        command = command[:-1] + [
            "import subprocess\nsubprocess.check_output = lambda *args, **kwargs: "
            + repr(version) + "\n" + command[-1]
        ]
    # This marker stands in for pytest, so a failure cannot accidentally run it.
    marker = tmp_path / "regressions-ran"
    (tmp_path / "pytest.py").write_text(
        "import os\nfrom pathlib import Path\n"
        "if os.environ['PATH'].split(os.pathsep)[0] != str(Path('.venv/bin').absolute()):\n"
        "    raise RuntimeError('venv binaries must lead PATH')\n"
        "Path('regressions-ran').write_text('started')\n"
    )
    result = subprocess.run(command, cwd=tmp_path, capture_output=True, text=True,
                            env={**os.environ, "PYTHONPATH": str(tmp_path), "PYTHONOPTIMIZE": optimize})
    assert marker.exists() == expected, result.stderr
    assert (result.returncode == 0) == expected, result.stderr
