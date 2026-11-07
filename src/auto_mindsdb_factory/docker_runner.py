"""Single-host disposable execution; no controller credentials enter the task."""

from __future__ import annotations

import os
import re
import subprocess
import threading
import time
from pathlib import Path
from uuid import uuid4


class RunnerError(RuntimeError):
    def public_reason(self) -> str:
        # Docker stderr and provider messages may contain private data.
        return type(self).__name__


class DockerSandbox:
    MAX_CAPTURED_OUTPUT_BYTES = 12000

    def __init__(self, workspace: Path, image: str, *, writable: bool = False,
                 network: bool = False, timeout: int = 300, platform: str | None = None) -> None:
        if not re.fullmatch(r"[a-zA-Z0-9./:_-]+@sha256:[0-9a-f]{64}", image):
            raise RunnerError("Runner image must be pinned by digest")
        self.workspace = workspace.resolve()
        self.image = image
        self.writable = writable
        self.network = network
        self.timeout = timeout
        if platform not in {None, "linux/amd64", "linux/arm64"}:
            raise RunnerError("Unsupported runner platform")
        self.platform = platform
        self.name = "ai-factory-" + uuid4().hex
        self._removal_lock, self._removed = threading.Lock(), False

    def _docker(self, *args: str, timeout: int = 60) -> subprocess.CompletedProcess:
        try:
            result = subprocess.run(["docker", *args], capture_output=True, text=True, timeout=timeout)
        except (OSError, subprocess.TimeoutExpired) as exc:
            raise RunnerError(f"Docker operation could not finish: {args[0]}") from exc
        if result.returncode:
            raise RunnerError(f"Docker operation failed: {args[0]}: {result.stderr[-2000:]}")
        return result

    def __enter__(self) -> "DockerSandbox":
        if not self.workspace.is_dir() or "," in str(self.workspace):
            raise RunnerError("Invalid sandbox workspace")
        uid = os.getuid()
        if uid == 0:
            raise RunnerError("Run the product controller as a non-root user")
        with self._removal_lock:
            self._removed = False
        mount = f"type=bind,source={self.workspace},target=/workspace"
        if not self.writable:
            mount += ",readonly"
        try:
            self._docker(
                "run", "--detach", "--name", self.name, "--init", "--read-only",
                "--cap-drop=ALL", "--security-opt=no-new-privileges", "--pids-limit=128",
                "--memory=2g", "--cpus=1", "--ulimit", "fsize=268435456:268435456",
                "--user", f"{uid}:{os.getgid()}", "--network", "bridge" if self.network else "none",
                *(["--platform", self.platform] if self.platform else []),
                "--mount", mount, "--tmpfs", "/tmp:rw,nosuid,size=512m,mode=1777",
                "--env", "HOME=/tmp", "--env", "npm_config_cache=/tmp/npm-cache",
                "--workdir", "/workspace", "--entrypoint", "/bin/sh", self.image,
                "-c", "exec sleep 14400", timeout=120,
            )
            return self
        except BaseException:
            self._remove()
            raise

    def run(self, command: list[str], *, stdin: str | None = None) -> dict:
        if not command or any(not isinstance(arg, str) or "\0" in arg for arg in command):
            raise RunnerError("Command must contain valid arguments")
        started = time.monotonic()
        try:
            process = subprocess.Popen(["docker", "exec", "-i", self.name, *command],
                stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        except OSError as exc:
            raise RunnerError("Could not start sandbox command") from exc
        captured = {"stdout": b"", "stderr": b""}
        sizes = {"stdout": 0, "stderr": 0}
        def drain(name, stream):
            while chunk := stream.read(4096):
                sizes[name] += len(chunk)
                captured[name] = (captured[name] + chunk)[-self.MAX_CAPTURED_OUTPUT_BYTES:]
            stream.close()
        def feed():
            try:
                process.stdin.write((stdin or "").encode())
                process.stdin.close()
            except (BrokenPipeError, OSError):
                pass
        threads = [threading.Thread(target=drain, args=(name, stream), daemon=True)
                   for name, stream in [("stdout", process.stdout), ("stderr", process.stderr)]]
        threads.append(threading.Thread(target=feed, daemon=True))
        for thread in threads:
            thread.start()
        try:
            exit_code = process.wait(timeout=self.timeout)
        except subprocess.TimeoutExpired:
            # Killing the local docker client alone leaves candidate processes alive.
            self._remove()
            process.kill()
            process.wait(timeout=10)
            exit_code = 124
        for thread in threads:
            thread.join(timeout=10)
        return {"command": command, "exit_code": exit_code,
                **{name: value.decode("utf-8", errors="replace") for name, value in captured.items()},
                **{name + "_bytes": size for name, size in sizes.items()},
                **{name + "_truncated": sizes[name] > len(captured[name]) for name in captured},
                "duration_seconds": round(time.monotonic() - started, 3)}

    def _remove(self) -> subprocess.CompletedProcess:
        command = ["docker", "rm", "--force", self.name]
        with self._removal_lock:
            if self._removed:
                return subprocess.CompletedProcess(command, 0)
            result = subprocess.run(command, capture_output=True, timeout=30)
            self._removed = result.returncode == 0
            return result

    def abort(self) -> None:
        """Stop the owned container, including processes behind a blocked exec client."""
        if self._remove().returncode:
            raise RunnerError("Sandbox cancellation cleanup could not be confirmed")

    def __exit__(self, *exc) -> None:
        self.abort()
