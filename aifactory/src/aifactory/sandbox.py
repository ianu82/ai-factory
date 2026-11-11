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
from typing import Any, Callable

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


TRASH_PREFIX = ".trash-"
_live_roots: set[Path] = set()  # job directories of sandboxes open in this process


def quarantine_orphaned_jobs(jobs_dir: Path | None = None) -> Path | None:
    """Move every job directory present at startup aside, in O(1) per directory.

    Sandboxes never survive a restart: close() removes a job's directory, and an interrupted attempt is redone
    in a fresh sandbox. So each restart used to leave every running job's checkout, virtualenv and node_modules
    on the volume, and a resumed job reused its stale directory. Renaming keeps startup fast however much was
    left; purge_quarantine() deletes the moved directories in the background.
    """
    root = jobs_dir or get_settings().jobs_dir
    if not root.is_dir():
        return None
    live = {p.resolve() for p in _live_roots}
    orphans = [p for p in root.iterdir() if not p.name.startswith(TRASH_PREFIX) and p.resolve() not in live]
    if not orphans:
        return None
    trash = root / f"{TRASH_PREFIX}{time.time_ns()}"
    trash.mkdir()
    os.chmod(trash, 0o700)
    for path in orphans:
        try:
            path.rename(trash / path.name)
        except OSError:
            shutil.rmtree(path, ignore_errors=True)
    return trash


def purge_quarantine(jobs_dir: Path | None = None) -> int:
    """Delete quarantined job directories (slow for large checkouts; run it off the event loop)."""
    root = jobs_dir or get_settings().jobs_dir
    removed = 0
    if root.is_dir():
        for trash in root.glob(f"{TRASH_PREFIX}*"):
            shutil.rmtree(trash, ignore_errors=True)
            removed += 1
    return removed


def disk_report(data_dir: Path | None = None) -> dict[str, Any]:
    """Volume usage for the inventory log, cheap enough to run every few minutes (no directory walks)."""
    settings = get_settings()
    usage = shutil.disk_usage(data_dir or settings.data_dir)
    jobs = settings.jobs_dir
    count = sum(1 for p in jobs.iterdir() if not p.name.startswith(TRASH_PREFIX)) if jobs.is_dir() else 0
    return {"used_gb": round(usage.used / 1e9, 1), "total_gb": round(usage.total / 1e9, 1),
            "used_percent": round(100 * usage.used / usage.total, 1) if usage.total else 0, "job_dirs": count}


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
        _live_roots.add(self.root)
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
        _live_roots.discard(self.root)
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
        cpus = job_cpus(uid)

        def run() -> None:
            os.setsid()
            if cpus:
                # Toolchains size worker pools from the CPUs they can see; on a big shared host that exhausts
                # the container's process limit. A small, per-job CPU set keeps pools proportionate.
                os.sched_setaffinity(0, cpus)
            if uid is not None:
                os.setgroups([])
                os.setgid(uid)
                os.setuid(uid)
            os.umask(0o022)

        return run

    async def exec(self, command: str, *, timeout: float = 600, cwd: str | None = None,
                   env: dict[str, str] | None = None, stdin: bytes | None = None,
                   on_output: Callable[[bytes], None] | None = None,
                   output_redactor=None) -> ExecResult:
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

        import codecs

        decoder = codecs.getincrementaldecoder("utf-8")("replace")

        def append_output(block: bytes) -> None:
            nonlocal total
            if not block:
                return
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

        async def pump() -> None:
            assert proc.stdout is not None
            while True:
                block = await proc.stdout.read(65536)
                eof = not block
                if output_redactor is not None:
                    block = output_redactor.feed(decoder.decode(block, final=eof), final=eof).encode()
                if not block:
                    if eof:
                        break
                    continue
                append_output(block)
                if eof:
                    break

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
            if timed_out and output_redactor is not None:
                # wait_for cancelled the pump before EOF. Preserve its withheld
                # diagnostic tail, through the same redaction/callback/buffer path.
                append_output(output_redactor.feed(decoder.decode(b"", final=True), final=True).encode())
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
        from .safe_files import safe_write

        # Keep support for absolute paths inside the checkout, but do not resolve symlinks.
        path = Path(os.path.abspath(self.work / relative))
        try:
            rel = path.relative_to(self.work.absolute()).as_posix()
            safe_write(str(self.work), rel, content, self.uid)
        except (ValueError, OSError) as exc:
            raise SandboxError(f"Unsafe sandbox write: {relative}: {exc}") from exc


# Backend-neutral file helpers: local sandboxes are synchronous, VM sandboxes asynchronous.
def _is_vm(sandbox) -> bool:
    return getattr(sandbox, "kind", "local") == "vm"


async def read_text(sandbox, relative: str, limit: int = 400_000) -> str:
    if _is_vm(sandbox):
        return await sandbox.read_text_async(relative, limit)
    return sandbox.read_text(relative, limit)


async def write_text(sandbox, relative: str, content: str) -> None:
    if _is_vm(sandbox):
        await sandbox.write_bytes_async(relative, content.encode("utf-8"))
    else:
        sandbox.write_text(relative, content)


async def write_bytes(sandbox, relative: str, content: bytes) -> None:
    if _is_vm(sandbox):
        await sandbox.write_bytes_async(relative, content)
    else:
        sandbox.write_bytes(relative, content)


async def read_bytes(sandbox, relative: str, limit: int | None = None, from_end: bool = False) -> bytes:
    if _is_vm(sandbox):
        return await sandbox.read_bytes_async(relative, limit, from_end)
    data = sandbox.resolve(relative).read_bytes()
    if limit:
        return data[-limit:] if from_end else data[:limit]
    return data


async def read_image_bytes(sandbox, relative: str, limit: int) -> bytes:
    """Read model-selected image paths with a hard byte cap on either backend."""
    if _is_vm(sandbox):
        return await sandbox.read_image_bytes_async(relative, limit)
    from .safe_files import safe_read

    try:
        return safe_read(str(sandbox.work), relative, limit)
    except (OSError, ValueError) as exc:
        raise SandboxError(f"Cannot read sandbox image: {exc}") from exc


async def list_files(sandbox, relative: str, pattern: str = "*") -> list[str]:
    if _is_vm(sandbox):
        return await sandbox.list_files(relative, pattern)
    try:
        return sorted(p.name for p in sandbox.resolve(relative).glob(pattern))
    except OSError:
        return []


def job_cpus(uid: int | None) -> set[int] | None:
    """The CPU set one job's processes may use (AIFACTORY_SANDBOX_CPUS, default 4), spread across jobs by UID."""
    if not hasattr(os, "sched_getaffinity"):
        return None
    try:
        width = int(os.environ.get("AIFACTORY_SANDBOX_CPUS", "4"))
        allowed = sorted(os.sched_getaffinity(0))
    except (ValueError, OSError):
        return None
    if width <= 0 or len(allowed) <= width:
        return None
    slots = len(allowed) // width
    start = ((uid or 0) % slots) * width
    return set(allowed[start:start + width])


def process_report(proc: Path = Path("/proc")) -> dict[str, Any]:
    """Tasks in use against the container's ceiling, and how many are zombies (exited, never reaped). Zombies
    growing between inventories mean orphans are not being reaped; the image runs tini as PID 1 for that."""
    info = {key: value for key, value in host_capacity().items() if key in ("pids_current", "pids_max")}
    zombies = 0
    for stat in proc.glob("[0-9]*/stat"):
        try:
            # Bytes: a job may name its process anything, not necessarily UTF-8. The command name in parentheses
            # may contain spaces and parentheses, so the state is the first field after the last ")".
            if stat.read_bytes().rsplit(b")", 1)[1].split()[0] == b"Z":
                zombies += 1
        except (OSError, IndexError):
            continue
    info["zombies"] = zombies
    return info


CGROUP_ROOT = Path("/sys/fs/cgroup")


def host_capacity() -> dict[str, Any]:
    """What a job can use on this host: CPUs, process and memory ceilings (cgroup v2 / v1 or rlimit)."""
    root = CGROUP_ROOT

    def first(*paths: Path) -> str | None:
        for path in paths:
            try:
                return path.read_text().strip()
            except OSError:
                continue
        return None

    info: dict[str, Any] = {"cpus": os.cpu_count()}
    try:
        import resource

        soft, _hard = resource.getrlimit(resource.RLIMIT_NPROC)
        info["nproc_rlimit"] = None if soft == resource.RLIM_INFINITY else soft
    except (ImportError, AttributeError, ValueError):
        pass
    info["pids_max"] = first(root / "pids.max", root / "pids" / "pids.max")
    info["pids_current"] = first(root / "pids.current", root / "pids" / "pids.current")
    memory = first(root / "memory.max", root / "memory" / "memory.limit_in_bytes")
    if memory and memory.isdigit():
        memory = f"{int(memory) / 2**30:.1f}GiB"
    info["memory_max"] = memory
    cpu = first(root / "cpu.max")
    if cpu is not None:  # v2: "<quota> <period>", or "max <period>" for none
        quota, period = (cpu.split() + ["", ""])[:2]
    else:  # v1: the cpu controller's CFS quota (-1 for none), mounted alone or with cpuacct
        controllers = [root / name for name in ("cpu", "cpu,cpuacct", "cpuacct,cpu")]
        quota = first(*(c / "cpu.cfs_quota_us" for c in controllers)) or ""
        period = first(*(c / "cpu.cfs_period_us" for c in controllers)) or ""
    if quota.isdigit() and period.isdigit() and int(quota) > 0 and int(period) > 0:
        info["cpu_quota"] = round(int(quota) / int(period), 1)
    return info
