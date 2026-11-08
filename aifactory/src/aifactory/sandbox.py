"""Job sandboxes.

A sandbox is a private working directory plus a way to run commands in it. When the
control plane runs as root, every sandbox gets its own unprivileged UID, so jobs cannot
read the database, the server key, other jobs, or each other's processes, and cleanup
can reliably stop everything a job started. Commands get a minimal environment that
never includes provider or GitHub credentials.
"""

from __future__ import annotations

import asyncio
import os
import shutil
import signal
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

from .config import get_settings

HEAD_BYTES = 6_000
TAIL_BYTES = 24_000
DEFAULT_PATH = "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"
SCRATCH = ".factory"


class SandboxError(RuntimeError):
    pass


@dataclass
class ExecResult:
    command: str
    exit_code: int
    output: str
    duration: float
    timed_out: bool = False
    truncated: bool = False
    total_bytes: int = 0

    @property
    def ok(self) -> bool:
        return self.exit_code == 0 and not self.timed_out

    def for_model(self) -> str:
        status = "timed out" if self.timed_out else f"exit code {self.exit_code}"
        note = f" (output truncated; {self.total_bytes:,} bytes total)" if self.truncated else ""
        return f"$ {self.command}\n[{status}, {self.duration:.1f}s{note}]\n{self.output}"


class _UidPool:
    def __init__(self, base: int, size: int = 200):
        self.base, self.size = base, size
        self._used: set[int] = set()
        self._lock = threading.Lock()

    def acquire(self) -> int:
        with self._lock:
            for uid in range(self.base, self.base + self.size):
                if uid not in self._used:
                    self._used.add(uid)
                    return uid
        raise SandboxError("No sandbox identities available")

    def release(self, uid: int) -> None:
        with self._lock:
            self._used.discard(uid)


_uid_pool = _UidPool(int(os.environ.get("AIFACTORY_SANDBOX_UID_BASE", "20000")))


def playwright_browsers_path() -> str:
    explicit = os.environ.get("PLAYWRIGHT_BROWSERS_PATH")
    if explicit:
        return explicit
    home = Path(os.environ.get("HOME", "~")).expanduser()
    for candidate in (home / "Library/Caches/ms-playwright", home / ".cache/ms-playwright"):
        if candidate.is_dir():
            return str(candidate)
    return ""


def isolation_available() -> bool:
    return hasattr(os, "geteuid") and os.geteuid() == 0 and os.environ.get("AIFACTORY_SANDBOX_ISOLATION", "1") != "0"


def _clip(data: bytes) -> tuple[str, bool]:
    if len(data) <= HEAD_BYTES + TAIL_BYTES:
        return data.decode("utf-8", "replace"), False
    head = data[:HEAD_BYTES].decode("utf-8", "replace")
    tail = data[-TAIL_BYTES:].decode("utf-8", "replace")
    return f"{head}\n\n… [{len(data) - HEAD_BYTES - TAIL_BYTES:,} bytes omitted] …\n\n{tail}", True


class LocalSandbox:
    """A job directory with optional per-job UID isolation."""

    kind = "local"

    def __init__(self, job_id: str, root: Path | None = None):
        settings = get_settings()
        self.job_id = job_id
        self.root = (root or settings.jobs_dir) / job_id
        self.work = self.root / "work"
        self.home = self.root / "home"
        self.tmp = self.root / "tmp"
        self.uid: int | None = None
        self._groups: set[int] = set()
        self._closed = False

    # lifecycle -----------------------------------------------------------------
    def create(self) -> "LocalSandbox":
        for path in (self.root, self.home, self.tmp):
            path.mkdir(parents=True, exist_ok=True)
        os.chmod(self.root, 0o711)
        if isolation_available():
            self.uid = _uid_pool.acquire()
        return self

    def adopt_work(self) -> None:
        """Hand the prepared checkout to the sandbox identity."""
        if self.uid is None:
            return
        for path in (self.work, self.home, self.tmp):
            if path.exists():
                os.chown(path, self.uid, self.uid)
                for dirpath, dirnames, filenames in os.walk(path):
                    for name in dirnames + filenames:
                        try:
                            os.lchown(os.path.join(dirpath, name), self.uid, self.uid)
                        except FileNotFoundError:
                            pass
                os.chmod(path, 0o700)

    async def close(self, *, keep_files: bool = False) -> None:
        if self._closed:
            return
        self._closed = True
        await self.kill_all()
        if self.uid is not None:
            _uid_pool.release(self.uid)
        if not keep_files:
            shutil.rmtree(self.root, ignore_errors=True)

    async def kill_all(self) -> None:
        for pgid in list(self._groups):
            try:
                os.killpg(pgid, signal.SIGKILL)
            except (ProcessLookupError, PermissionError):
                pass
        self._groups.clear()
        if self.uid is not None:
            # Anything the job left running under its private identity.
            proc = await asyncio.create_subprocess_exec(
                "pkill", "-KILL", "-U", str(self.uid),
                stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.DEVNULL,
            )
            await proc.wait()

    # commands ------------------------------------------------------------------
    def env(self, extra: dict[str, str] | None = None) -> dict[str, str]:
        node_bin = os.environ.get("AIFACTORY_SANDBOX_PATH_PREFIX", "")
        path = (node_bin + ":" if node_bin else "") + os.environ.get("AIFACTORY_SANDBOX_PATH", DEFAULT_PATH)
        # Local development (no isolation) keeps the user's PATH so toolchains resolve.
        if self.uid is None:
            path = os.environ.get("PATH", path)
        env = {
            "PATH": path,
            "HOME": str(self.home),
            "TMPDIR": str(self.tmp),
            "LANG": "C.UTF-8",
            "LC_ALL": "C.UTF-8",
            "CI": "1",
            "TERM": "dumb",
            "NO_COLOR": "1",
            "FORCE_COLOR": "0",
            "GIT_TERMINAL_PROMPT": "0",
            "npm_config_update_notifier": "false",
            "PIP_DISABLE_PIP_VERSION_CHECK": "1",
        }
        browsers = playwright_browsers_path()
        if browsers:
            env["PLAYWRIGHT_BROWSERS_PATH"] = browsers  # shared, read-only browser install for runtime QA
        if extra:
            env.update(extra)
        return env

    def _preexec(self):
        uid = self.uid

        def run() -> None:
            os.setsid()
            if uid is not None:
                os.setgroups([])
                os.setgid(uid)
                os.setuid(uid)
            os.umask(0o022)

        return run

    async def exec(self, command: str, *, timeout: float = 600, cwd: str | None = None,
                   env: dict[str, str] | None = None, stdin: bytes | None = None,
                   on_output: Callable[[bytes], None] | None = None) -> ExecResult:
        if self._closed:
            raise SandboxError("Sandbox is closed")
        workdir = self.resolve(cwd) if cwd else self.work
        started = time.monotonic()
        proc = await asyncio.create_subprocess_exec(
            "/bin/bash", "-lc", command,
            cwd=str(workdir),
            env=self.env(env),
            stdin=asyncio.subprocess.PIPE if stdin is not None else asyncio.subprocess.DEVNULL,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
            preexec_fn=self._preexec(),
        )
        self._groups.add(proc.pid)
        chunks: list[bytes] = []
        total = 0
        timed_out = False

        async def pump() -> None:
            nonlocal total
            assert proc.stdout is not None
            while True:
                block = await proc.stdout.read(65536)
                if not block:
                    break
                total += len(block)
                chunks.append(block)
                if on_output is not None:
                    try:
                        on_output(block)
                    except Exception:  # progress reporting must never break a command
                        pass
                # Keep memory bounded: retain head and a rolling tail.
                if total > 4 * (HEAD_BYTES + TAIL_BYTES):
                    joined = b"".join(chunks)
                    chunks[:] = [joined[:HEAD_BYTES], joined[-(TAIL_BYTES * 2):]]

        try:
            if stdin is not None and proc.stdin is not None:
                proc.stdin.write(stdin)
                await proc.stdin.drain()
                proc.stdin.close()
            await asyncio.wait_for(asyncio.gather(pump(), proc.wait()), timeout=timeout)
        except asyncio.TimeoutError:
            timed_out = True
        except asyncio.CancelledError:
            self._kill_group(proc.pid)
            raise
        finally:
            if proc.returncode is None:
                self._kill_group(proc.pid)
                try:
                    await asyncio.wait_for(proc.wait(), timeout=10)
                except asyncio.TimeoutError:
                    pass
            self._groups.discard(proc.pid)
        data = b"".join(chunks)
        output, truncated = _clip(data)
        return ExecResult(
            command=command,
            exit_code=proc.returncode if proc.returncode is not None and not timed_out else 124,
            output=output,
            duration=time.monotonic() - started,
            timed_out=timed_out,
            truncated=truncated or total > len(data),
            total_bytes=total,
        )

    async def spawn(self, command: str, *, env: dict[str, str] | None = None, log: str = ".factory/app.log",
                    cwd: str | None = None) -> asyncio.subprocess.Process:
        """Start a long-running process (e.g. the app under test); it is stopped with the sandbox."""
        if self._closed:
            raise SandboxError("Sandbox is closed")
        log_path = self.resolve(log)
        log_path.parent.mkdir(parents=True, exist_ok=True)
        if self.uid is not None:
            os.chown(log_path.parent, self.uid, self.uid)
        handle = open(log_path, "ab")
        try:
            proc = await asyncio.create_subprocess_exec(
                "/bin/bash", "-lc", command, cwd=str(self.resolve(cwd) if cwd else self.work), env=self.env(env),
                stdin=asyncio.subprocess.DEVNULL, stdout=handle, stderr=asyncio.subprocess.STDOUT,
                preexec_fn=self._preexec())
        finally:
            handle.close()
        if self.uid is not None:
            os.chown(log_path, self.uid, self.uid)
        self._groups.add(proc.pid)
        return proc

    def stop(self, proc: asyncio.subprocess.Process) -> None:
        self._kill_group(proc.pid)
        self._groups.discard(proc.pid)

    def _kill_group(self, pgid: int) -> None:
        try:
            os.killpg(pgid, signal.SIGKILL)
        except (ProcessLookupError, PermissionError):
            pass

    # files ---------------------------------------------------------------------
    def resolve(self, relative: str | None) -> Path:
        """Resolve a path inside the work tree, refusing anything that escapes it."""
        rel = (relative or ".").strip()
        if rel.startswith("/"):
            # Allow absolute paths that already point inside the work tree.
            candidate = Path(rel)
        else:
            candidate = self.work / rel
        real_root = Path(os.path.realpath(self.work))
        real = Path(os.path.realpath(candidate))
        if real != real_root and real_root not in real.parents:
            raise SandboxError(f"Path escapes the repository: {relative}")
        return real

    def read_text(self, relative: str, limit: int = 400_000) -> str:
        path = self.resolve(relative)
        if not path.is_file():
            raise SandboxError(f"Not a file: {relative}")
        fd = os.open(path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
        with os.fdopen(fd, "rb") as handle:
            data = handle.read(limit + 1)
        if b"\x00" in data[:8000]:
            raise SandboxError(f"Binary file: {relative}")
        text = data[:limit].decode("utf-8", "replace")
        if len(data) > limit:
            text += f"\n… [file truncated at {limit:,} bytes]"
        return text

    def write_text(self, relative: str, content: str) -> None:
        self.write_bytes(relative, content.encode("utf-8"))

    def write_bytes(self, relative: str, content: bytes) -> None:
        path = self.resolve(relative)
        if path.is_dir():
            raise SandboxError(f"Is a directory: {relative}")
        parent = self.resolve(str(path.parent))
        created: list[Path] = []
        probe = parent
        while not probe.exists():
            created.append(probe)
            probe = probe.parent
        parent.mkdir(parents=True, exist_ok=True)
        flags = os.O_WRONLY | os.O_CREAT | os.O_TRUNC | getattr(os, "O_NOFOLLOW", 0)
        fd = os.open(path, flags, 0o644)
        with os.fdopen(fd, "wb") as handle:
            handle.write(content)
        if self.uid is not None:
            for item in [path, *created]:
                try:
                    os.lchown(item, self.uid, self.uid)
                except FileNotFoundError:
                    pass
