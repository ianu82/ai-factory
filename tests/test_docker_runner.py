import os

import pytest

from auto_mindsdb_factory.docker_runner import DockerSandbox, RunnerError


def test_mutable_image_tags_are_rejected(tmp_path):
    with pytest.raises(RunnerError):
        DockerSandbox(tmp_path, "node:latest")


@pytest.mark.skipif(not os.environ.get("AI_FACTORY_TEST_IMAGE"), reason="requires a digest-pinned Docker test image")
def test_real_sandbox_has_no_credentials_network_or_source_writes(tmp_path, monkeypatch):
    monkeypatch.setenv("OPENAI_API_KEY", "must-not-enter-container")
    (tmp_path / "app.js").write_text("original")
    with DockerSandbox(tmp_path, os.environ["AI_FACTORY_TEST_IMAGE"]) as sandbox:
        result = sandbox.run(["sh", "-c", "test -z \"$OPENAI_API_KEY\" && test ! -e /var/run/docker.sock && test $(id -u) -ne 0 && ! touch /workspace/new-file && ! touch /root-file"])
        assert result["exit_code"] == 0, result
        result = sandbox.run(["node", "-e", "const os=require('os'); if(Object.values(os.networkInterfaces()).flat().some(a=>!a.internal)) process.exit(1); const s=require('net').connect(443,'1.1.1.1'); s.on('connect',()=>process.exit(1)); s.on('error',()=>process.exit(0)); setTimeout(()=>process.exit(1),1000);"])
        assert result["exit_code"] == 0, result
    assert not (tmp_path / "new-file").exists()


@pytest.mark.skipif(not os.environ.get("AI_FACTORY_TEST_IMAGE"), reason="requires Docker")
def test_timeout_destroys_container_and_output_is_bounded(tmp_path):
    import subprocess
    with DockerSandbox(tmp_path, os.environ["AI_FACTORY_TEST_IMAGE"], timeout=1) as sandbox:
        result = sandbox.run(["node", "-e", "process.stdout.write('prefix'+'x'.repeat(99990)+'tail')"])
        assert result["exit_code"] == 0
        assert result["stdout"] == 'x' * (DockerSandbox.MAX_CAPTURED_OUTPUT_BYTES - 4) + 'tail'
        assert result["stdout_bytes"] == 100000
        assert result["stdout_truncated"] is True
        assert result["stderr_truncated"] is False
        result = sandbox.run(["node", "-e", "setTimeout(()=>{},20000)"])
        assert result["exit_code"] == 124
        assert subprocess.run(["docker", "inspect", sandbox.name], capture_output=True).returncode != 0


@pytest.mark.skipif(not os.environ.get("AI_FACTORY_TEST_IMAGE"), reason="requires Docker")
def test_abort_terminates_an_active_command_and_its_container(tmp_path):
    import subprocess
    import threading
    import time
    results = []
    with DockerSandbox(tmp_path, os.environ["AI_FACTORY_TEST_IMAGE"], writable=True) as sandbox:
        thread = threading.Thread(target=lambda: results.append(sandbox.run(
            ["sh", "-c", "touch /workspace/started; exec sleep 600"])))
        thread.start()
        try:
            deadline = time.monotonic() + 10
            while not (tmp_path / "started").exists() and time.monotonic() < deadline:
                time.sleep(0.02)
            assert (tmp_path / "started").exists(), "candidate command never started"
            sandbox.abort()
        finally:
            sandbox._remove()
            thread.join(timeout=10)
        assert not thread.is_alive() and results and results[0]["exit_code"] != 0
        assert subprocess.run(["docker", "inspect", sandbox.name], capture_output=True).returncode != 0


@pytest.mark.skipif(not os.environ.get("AI_FACTORY_TEST_IMAGE"), reason="requires Docker")
def test_real_railway_driver_confirms_isolation_and_container_removal(tmp_path, monkeypatch):
    from uuid import uuid4
    from auto_mindsdb_factory import railway_remote
    monkeypatch.setattr(railway_remote, "WORKSPACE", tmp_path)
    (tmp_path / "source.txt").write_text("immutable")
    options = {"name": "ai-factory-" + uuid4().hex, "image": os.environ["AI_FACTORY_TEST_IMAGE"],
               "writable": False, "network": False, "timeout": 5, "platform": None}
    request = {"sandbox": options}
    try:
        railway_remote.dispatch({**request, "action": "enter"})
        result = railway_remote.dispatch({**request, "action": "run", "command": ["sh", "-c",
            "test $(id -u) != 0 && test ! -S /var/run/docker.sock && ! touch /workspace/source.txt && ! touch /etc/probe"]})
        assert result["exit_code"] == 0, result
        assert (tmp_path / "source.txt").read_text() == "immutable"
        with monkeypatch.context() as failed_removal:
            failed_removal.setattr(DockerSandbox, "_remove", lambda _: None)
            with pytest.raises(RunnerError, match="not confirmed"):
                railway_remote.dispatch({**request, "action": "exit"})
    finally:
        railway_remote.dispatch({**request, "action": "exit"})
