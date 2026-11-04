import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

from auto_mindsdb_factory import railway_remote
from auto_mindsdb_factory.docker_runner import RunnerError
from auto_mindsdb_factory.source_transfer import pack_source


def test_remote_protocol_roundtrip_uses_only_disposable_workspace(tmp_path, monkeypatch):
    source = tmp_path / "source"
    source.mkdir()
    (source / "hello").write_text("hello")
    destination = tmp_path / "worker"
    monkeypatch.setattr(railway_remote, "WORKSPACE", destination)
    railway_remote.dispatch({"action": "upload", "source": pack_source(source)})
    assert railway_remote.dispatch({"action": "download"}) == {"source": pack_source(source)}


def test_remote_errors_do_not_return_exception_text_or_credentials():
    result = subprocess.run([sys.executable, "-m", "auto_mindsdb_factory.railway_remote"],
        input=json.dumps({"action": "run", "sandbox": {"name": "secret-marker"}}),
        text=True, capture_output=True, env={**os.environ, "PYTHONPATH": str(Path(railway_remote.__file__).parents[1])}, timeout=10)
    assert json.loads(result.stdout) == {"error": "RunnerError"}
    assert "secret-marker" not in result.stdout + result.stderr


def test_remote_rejects_shell_in_container_identity():
    with pytest.raises(RunnerError, match="identity"):
        railway_remote.dispatch({"action": "exit", "sandbox": {"name": "container; touch /tmp/escape"}})


def test_unconfirmed_container_removal_cannot_authorize_source_import(monkeypatch):
    class Unstopped:
        def __init__(self, *args, **kwargs):
            pass
        def __exit__(self):
            pass  # simulates a daemon refusing rm without the old wrapper raising
        def _docker(self, *args):
            return subprocess.CompletedProcess(args, 0, "still-running\n", "")
    monkeypatch.setattr(railway_remote, "DockerSandbox", Unstopped)
    with pytest.raises(RunnerError, match="not confirmed"):
        railway_remote.dispatch({"action": "exit", "sandbox": {
            "name": "ai-factory-" + "a" * 32, "image": "unused", "writable": False,
            "network": False, "timeout": 5, "platform": None}})
