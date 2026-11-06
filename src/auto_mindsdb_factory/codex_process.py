"""Owned, credential-free app-server process; session/proxy wiring is separate."""

import json
import math
import os
import shutil
import signal
import subprocess
import tempfile
import time
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlsplit


class CodexProcessError(RuntimeError):
    pass


_FEATURES = ("shell_tool", "apps", "plugins", "memories", "goals", "multi_agent", "hooks", "remote_plugin",
             "view_image", "sleep_tool", "browser_use", "computer_use", "daemon_auto_start")


@dataclass(frozen=True)
class CodexProcess:
    process: subprocess.Popen
    home: Path
    work: Path


@contextmanager
def _owned_process(command, *, work, env):
    process = subprocess.Popen(command, cwd=work, env=env, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                               stderr=subprocess.DEVNULL, text=True, start_new_session=True)
    try:
        yield process
    finally:
        try:
            # Let the wrapper reap its child before escalating to group signals.
            try:
                process.stdin.close()
            except BrokenPipeError:
                pass
            try:
                process.wait(timeout=1)
            except subprocess.TimeoutExpired:
                pass
            try:
                os.killpg(process.pid, signal.SIGTERM)
            except ProcessLookupError:
                pass
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                pass
            # Retire owned descendants too, even if the group leader exited.
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            process.wait(timeout=5)
        except (OSError, subprocess.TimeoutExpired):
            raise CodexProcessError("Codex process cleanup could not be confirmed") from None
        finally:
            for pipe in (process.stdin, process.stdout):
                pipe.close()


@contextmanager
def open_codex_process(provider_url, *, deadline, binary=None):
    """Launch only the pinned CLI, with a new empty home/work and no inherited keys.

    provider_url must identify the owning controller's loopback proxy. The caller
    owns JSON-RPC reading, thread policy, inference admission and the global run
    lifetime. No request or tool is dispatched here. stderr is discarded, so raw
    SDK/provider diagnostics cannot become evidence or block on an unread pipe.
    """
    try:
        url = urlsplit(provider_url)
        valid = (type(provider_url) is str and url.scheme == "http" and url.hostname == "127.0.0.1"
                 and url.username is None and url.password is None and url.path == "/v1"
                 and not url.query and not url.fragment and url.port is not None and 1 <= url.port <= 65535
                 and provider_url == f"http://127.0.0.1:{url.port}/v1")
    except (ValueError, TypeError, AttributeError):
        valid = False
    if not valid:
        raise CodexProcessError("Codex requires its controller-owned loopback proxy")
    if type(deadline) not in (int, float) or not math.isfinite(deadline) or time.monotonic() >= deadline:
        raise CodexProcessError("Codex startup deadline exhausted")
    executable = Path(binary or Path(__file__).with_name("codex_bridge") / "node_modules/.bin/codex").resolve()
    node = shutil.which("node")
    if not executable.is_file() or node is None:
        raise CodexProcessError("The pinned Codex installation is unavailable")
    with tempfile.TemporaryDirectory(prefix="factory-codex-session-") as temporary:
        home, work = Path(temporary) / "home", Path(temporary) / "work"
        home.mkdir(); work.mkdir()
        env = {"HOME": str(home), "PATH": str(Path(node).resolve().parent) + os.pathsep + os.defpath}
        if time.monotonic() >= deadline:
            raise CodexProcessError("Codex startup deadline exhausted")
        with _owned_process([str(executable), "--version"], work=work, env=env) as version:
            try:
                output, _ = version.communicate(timeout=max(0, min(10, deadline - time.monotonic())))
            except subprocess.TimeoutExpired:
                raise CodexProcessError("Codex version check timed out") from None
            if version.returncode != 0 or output.strip() != "codex-cli 0.167.0":
                raise CodexProcessError("Codex installation differs from the pinned version")
        config = {"model_provider": "factory_guarded", "web_search": "disabled", "agents.enabled": False,
                  "skills.bundled.enabled": False, "analytics.enabled": False, "feedback.enabled": False,
                  **{"features." + feature: False for feature in _FEATURES}}
        command = [str(executable)]
        for key, value in config.items():
            command.extend(["-c", key + "=" + json.dumps(value)])
        provider = {"name": "Factory guarded inference", "base_url": provider_url, "wire_api": "responses",
                    "requires_openai_auth": False, "supports_websockets": False,
                    "request_max_retries": 0, "stream_max_retries": 0}
        command.extend(["-c", "model_providers.factory_guarded={" + ",".join(
            key + "=" + json.dumps(value) for key, value in provider.items()) + "}", "app-server"])
        if time.monotonic() >= deadline:
            raise CodexProcessError("Codex startup deadline exhausted")
        with _owned_process(command, work=work, env=env) as process:
            yield CodexProcess(process, home, work)
