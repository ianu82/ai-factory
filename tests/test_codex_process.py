import json
import os
from pathlib import Path
import queue
import shutil
import threading
import time

import pytest

from auto_mindsdb_factory.codex_process import CodexProcessError, open_codex_process


@pytest.mark.parametrize("url", ["https://127.0.0.1:80/v1", "http://localhost:80/v1", "http://127.0.0.1/v1",
    "http://127.0.0.1:80/v1?token=x", "http://key@127.0.0.1:80/v1", "http://127.0.0.1:80/v1\n", None])
def test_unknown_remote_or_noncanonical_proxy_is_rejected_before_start(url, monkeypatch):
    monkeypatch.setattr("auto_mindsdb_factory.codex_process.subprocess.Popen", lambda *a, **k: pytest.fail("spawned"))
    with pytest.raises(CodexProcessError):
        with open_codex_process(url, deadline=time.monotonic() + 10): pass


@pytest.mark.parametrize("deadline", [0, -1, float("nan"), float("inf"), True, None])
def test_invalid_or_elapsed_deadline_starts_nothing(deadline, monkeypatch):
    monkeypatch.setattr("auto_mindsdb_factory.codex_process.subprocess.Popen", lambda *a, **k: pytest.fail("spawned"))
    with pytest.raises(CodexProcessError):
        with open_codex_process("http://127.0.0.1:1/v1", deadline=deadline): pass


def test_unknown_binary_cannot_be_used_as_a_compatible_fallback():
    with pytest.raises(CodexProcessError, match="pinned version"):
        with open_codex_process("http://127.0.0.1:1/v1", deadline=time.monotonic() + 10,
                               binary=shutil.which("true")):
            pytest.fail("App-server started")


def test_explicit_empty_binary_cannot_silently_select_the_default():
    with pytest.raises(CodexProcessError, match="unavailable"):
        with open_codex_process("http://127.0.0.1:1/v1", deadline=time.monotonic() + 10, binary=""):
            pytest.fail("App-server started")


@pytest.mark.parametrize("token", ["", "x" * 44, "controller-test-only", "t" * 42 + "\n"])
def test_invalid_proxy_capability_never_reaches_a_child(token, monkeypatch):
    monkeypatch.setattr("auto_mindsdb_factory.codex_process.subprocess.Popen", lambda *a, **k: pytest.fail("spawned"))
    with pytest.raises(CodexProcessError, match="Invalid local proxy capability"):
        with open_codex_process("http://127.0.0.1:1/v1", deadline=time.monotonic() + 10, proxy_token=token):
            pytest.fail("App-server started")


@pytest.mark.parametrize("interrupt", [False, True])
def test_real_pinned_process_uses_owned_empty_directories_and_retires_its_group(monkeypatch, interrupt):
    configured = os.environ.get("AI_FACTORY_CODEX_TEST_BINARY")
    binary = Path(configured or "src/auto_mindsdb_factory/codex_bridge/node_modules/.bin/codex").resolve()
    if not configured and not binary.is_file():
        pytest.skip("Pinned Codex CLI unavailable; dedicated Codex CI requires it")
    assert binary.is_file()
    for key in ["OPENAI_API_KEY", "GH_TOKEN", "LINEAR_API_KEY", "RAILWAY_API_TOKEN", "CODEX_HOME", "ANTON_MODEL"]:
        monkeypatch.setenv(key, "untrusted-parent-fixture-value")
    import subprocess
    original, launches = subprocess.Popen, []
    def launch(command, **kwargs):
        launches.append((command, kwargs))
        return original(command, **kwargs)
    monkeypatch.setattr("auto_mindsdb_factory.codex_process.subprocess.Popen", launch)
    owner, reader = None, None
    try:
        with open_codex_process("http://127.0.0.1:1/v1", deadline=time.monotonic() + 10, binary=binary) as owner:
            assert not list(owner.work.iterdir()) and owner.home.is_dir()
            messages = queue.Queue()
            reader = threading.Thread(target=lambda: messages.put(json.loads(owner.process.stdout.readline())))
            reader.start()
            owner.process.stdin.write(json.dumps({"id": 1, "method": "initialize", "params": {
                "clientInfo": {"name": "factory_lifecycle_fixture", "version": "1"},
                "capabilities": {"experimentalApi": True}}}) + "\n")
            owner.process.stdin.flush()
            try:
                response = messages.get(timeout=5)
                assert response["id"] == 1 and "error" not in response
            finally:
                reader.join(timeout=1)
            assert not reader.is_alive()
            if interrupt: raise RuntimeError("owned interruption")
    except RuntimeError as error:
        assert interrupt and str(error) == "owned interruption"
    finally:
        if reader is not None: reader.join(timeout=2)
    assert reader is not None and not reader.is_alive()
    assert owner is not None and owner.process.poll() is not None
    assert owner.process.stdin.closed and owner.process.stdout.closed and not owner.home.parent.exists()
    with pytest.raises(ProcessLookupError): os.killpg(owner.process.pid, 0)
    assert len(launches) == 2 and launches[0][0][-1] == "--version"
    for _, options in launches:
        assert set(options["env"]) == {"HOME", "PATH"} and options["start_new_session"] is True
        assert options["cwd"] == owner.work
    command = launches[1][0]
    assert "features.daemon_auto_start=false" in command and "features.shell_tool=false" in command
    assert "requires_openai_auth=false" in command[-2] and "request_max_retries=0" in command[-2]
