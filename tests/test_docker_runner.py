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
        result = sandbox.run(["node", "-e", "process.stdout.write('x'.repeat(100000))"])
        assert result["exit_code"] == 0
        assert len(result["stdout"]) <= 12000
        assert result["stdout_bytes"] == 100000
        assert result["stdout_truncated"] is True
        assert result["stderr_truncated"] is False
        result = sandbox.run(["node", "-e", "setTimeout(()=>{},20000)"])
        assert result["exit_code"] == 124
        assert subprocess.run(["docker", "inspect", sandbox.name], capture_output=True).returncode != 0
