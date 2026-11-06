"""Install the reviewed controller-only SDK without resolving floating dependencies."""

import os
import subprocess
import sys
from pathlib import Path


root = Path(__file__).resolve().parents[1]
if len(sys.argv) != 2:
    raise SystemExit("Supply the controller virtual-environment interpreter")
python = Path(sys.argv[1]).absolute()
if not (python.parent.parent / "pyvenv.cfg").is_file():
    raise SystemExit("Anton installation requires an explicit virtual-environment interpreter")
python = str(python)
requirements = root / "src/auto_mindsdb_factory/anton_requirements.txt"
subprocess.run(["uv", "pip", "install", "--python", python, "--no-deps", "--require-hashes",
                "--only-binary=:all:", "-r", str(requirements)], check=True)
subprocess.run(["uv", "pip", "install", "--python", python, "--no-deps", "--no-build-isolation",
                "anton-agent @ git+https://github.com/mindsdb/anton@537cc1fb11849e9d152870738af22ca55c1d59d8"], check=True)
subprocess.run(["uv", "pip", "check", "--python", python], check=True)
subprocess.run([python, "-c", "from anton.core.session import ChatSession; "
                "from auto_mindsdb_factory.anton_session import validate_anton_installation; "
                "validate_anton_installation(); print('Pinned Anton runtime validated')"],
               check=True, env={**os.environ, "PYTHONPATH": str(root / "src")})
