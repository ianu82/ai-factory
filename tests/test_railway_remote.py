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


def test_remote_uses_existing_docker_policy(tmp_path, monkeypatch):
    image = os.environ.get("AI_FACTORY_TEST_IMAGE")
    if not image:
        pytest.skip("Set AI_FACTORY_TEST_IMAGE for real Docker execution")
    monkeypatch.setattr(railway_remote, "WORKSPACE", tmp_path)
    (tmp_path / "source.txt").write_text("immutable")
    options = {"name": "ai-factory-" + "a" * 32, "image": image,
               "writable": False, "network": False, "timeout": 5, "platform": None}
    request = {"sandbox": options}
    try:
        railway_remote.dispatch({**request, "action": "enter"})
        result = railway_remote.dispatch({**request, "action": "run", "command": ["sh", "-c",
            "test $(id -u) != 0 && test ! -S /var/run/docker.sock && ! touch /workspace/source.txt && ! touch /etc/probe"]})
        assert result["exit_code"] == 0, result
        assert (tmp_path / "source.txt").read_text() == "immutable"
    finally:
        railway_remote.dispatch({**request, "action": "exit"})
