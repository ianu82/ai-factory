"""Railway sandbox VMs for verification and runtime QA.

One long-lived Node sidecar (``vmbridge/sidecar.mjs``) holds the Railway SDK connections and
multiplexes requests over JSON lines; the Railway token never leaves the control plane and VMs
start with an empty environment. ``RailwayVMSandbox`` implements the same interface as
``LocalSandbox`` (exec, background processes, file reads/writes), so verifiers, the shared
tools and runtime QA run unchanged inside the VM.
"""

from __future__ import annotations

import asyncio
import base64
import hashlib
import io
import json
import logging
import os
import posixpath
import shlex
import shutil
import tarfile
import time
from pathlib import Path, PurePosixPath
from typing import Any, Callable

from .sandbox import SCRATCH, ExecResult, LocalSandbox, SandboxError, _clip

log = logging.getLogger(__name__)
SIDECAR = Path(__file__).resolve().parents[2] / "vmbridge" / "sidecar.mjs"
SIDECAR_ALT = Path("/app/vmbridge/sidecar.mjs")

# Debian packages and build steps for the QA image; Railway builds it once and caches it.
QA_TEMPLATE: dict[str, Any] = {
    "packages": ["git", "curl", "ca-certificates", "xz-utils", "python3", "python3-pip", "python3-venv",
                 "build-essential", "procps"],
    "run": [
        "curl -fsSL https://nodejs.org/dist/v22.20.0/node-v22.20.0-linux-x64.tar.xz"
        " | tar -xJ -C /usr/local --strip-components=1",
        "python3 -m pip install --break-system-packages --quiet playwright==1.64.1",
        "python3 -m playwright install --with-deps chromium",
        "mkdir -p /work",
    ],
    "workdir": "/work",
}


def sidecar_path() -> Path | None:
    for path in (SIDECAR, SIDECAR_ALT):
        if path.is_file() and (path.parent / "node_modules" / "railway").is_dir():
            return path
    return None


DEPENDENCY_DIRS = ("node_modules", ".venv")
LOCKFILES = ("package-lock.json", "npm-shrinkwrap.json", "pnpm-lock.yaml", "yarn.lock", "bun.lockb", "uv.lock",
             "poetry.lock", "Pipfile.lock", "requirements.txt", "requirements-dev.txt", "pyproject.toml", "setup.py",
             "setup.cfg", "Gemfile.lock", "go.sum", "Cargo.lock", ".nvmrc", ".python-version")


def environment_key(work: Path, repo: str, setup: list[str]) -> str:
    """Name of the prepared environment for this repository's dependencies: same lockfiles + setup → same VM."""
    digest = hashlib.sha256()
    digest.update(json.dumps({"repo": repo, "setup": setup, "template": QA_TEMPLATE}, sort_keys=True).encode())
    for root, dirs, files in os.walk(work):
        rel_root = Path(root).relative_to(work)
        dirs[:] = sorted(d for d in dirs if d not in DEPENDENCY_DIRS and d != ".git" and len(rel_root.parts) < 4)
        for name in sorted(files):
            if name in LOCKFILES:
                digest.update(str(rel_root / name).encode() + b"\0" + (Path(root) / name).read_bytes() + b"\0")
    return "aif-env-" + digest.hexdigest()[:24]


def vm_available() -> tuple[bool, str]:
    if not shutil.which("node"):
        return False, "Node.js is not installed"
    if sidecar_path() is None:
        return False, "VM bridge is not installed"
    if not (os.environ.get("RAILWAY_API_TOKEN") and os.environ.get("RAILWAY_ENVIRONMENT_ID")):
        return False, "Railway sandbox credentials are not configured"
    return True, "Railway sandbox VMs"


class VMBridge:
    _instance: "VMBridge | None" = None

    def __init__(self) -> None:
        self.proc: asyncio.subprocess.Process | None = None
        self.pending: dict[int, asyncio.Future] = {}
        self.exits: dict[str, asyncio.Future] = {}
        self.next_id = 0
        self.lock = asyncio.Lock()
        self.reader: asyncio.Task | None = None

    @classmethod
    def get(cls) -> "VMBridge":
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    async def _ensure(self) -> None:
        async with self.lock:
            if self.proc is not None and self.proc.returncode is None:
                return
            path = sidecar_path()
            if path is None:
                raise SandboxError("VM bridge is not installed")
            env = {k: os.environ[k] for k in ("PATH", "HOME", "RAILWAY_API_TOKEN", "RAILWAY_ENVIRONMENT_ID",
                                              "RAILWAY_TOKEN_TYPE") if k in os.environ}
            self.proc = await asyncio.create_subprocess_exec(
                "node", str(path), cwd=str(path.parent), env=env, stdin=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.DEVNULL, limit=64 * 1024 * 1024)
            self.reader = asyncio.create_task(self._read())

    async def _read(self) -> None:
        assert self.proc and self.proc.stdout
        while True:
            line = await self.proc.stdout.readline()
            if not line:
                break
            try:
                message = json.loads(line)
            except json.JSONDecodeError:
                continue
            if "event" in message:
                if message["event"] == "exit" and message.get("proc") in self.exits:
                    fut = self.exits.pop(message["proc"])
                    if not fut.done():
                        fut.set_result(message)
                continue
            fut = self.pending.pop(message.get("id"), None)
            if fut and not fut.done():
                if message.get("ok"):
                    fut.set_result(message.get("result"))
                else:
                    fut.set_exception(SandboxError(f"VM bridge: {message.get('error')}"))
        for fut in list(self.pending.values()):
            if not fut.done():
                fut.set_exception(SandboxError("VM bridge stopped"))
        self.pending.clear()

    async def call(self, op: str, args: dict[str, Any] | None = None, timeout: float = 600) -> Any:
        await self._ensure()
        assert self.proc and self.proc.stdin
        self.next_id += 1
        rid = self.next_id
        fut = asyncio.get_running_loop().create_future()
        self.pending[rid] = fut
        self.proc.stdin.write((json.dumps({"id": rid, "op": op, "args": args or {}}) + "\n").encode())
        await self.proc.stdin.drain()
        try:
            return await asyncio.wait_for(fut, timeout=timeout)
        finally:
            self.pending.pop(rid, None)

    def watch_exit(self, proc: str) -> asyncio.Future:
        fut = asyncio.get_running_loop().create_future()
        self.exits[proc] = fut
        return fut


class _VMProcess:
    """Stand-in for asyncio.subprocess.Process used by runtime QA."""

    def __init__(self, proc_id: str, exit_future: asyncio.Future):
        self.proc_id = proc_id
        self._exit = exit_future

    @property
    def returncode(self) -> int | None:
        if not self._exit.done():
            return None
        result = self._exit.result()
        return result.get("exitCode") if result.get("exitCode") is not None else -1


class RailwayVMSandbox:
    kind = "vm"

    def __init__(self, vm_id: str, bridge: VMBridge, region: str | None = None):
        self.vm_id = vm_id
        self.bridge = bridge
        self.region = region
        self.work = PurePosixPath("/work")
        self.home = PurePosixPath("/root")
        self.uid = None
        self.prepared: str | None = None  # checkpoint this VM booted from, when it did
        self.environment_key: str | None = None  # checkpoint to save after a clean setup
        self._closed = False
        self._procs: list[str] = []

    @classmethod
    async def create(cls, *, cpu: float = 4, memory_gb: float = 8, template: dict[str, Any] | None = None,
                     checkpoint: str | None = None, timeout: float = 900) -> "RailwayVMSandbox":
        bridge = VMBridge.get()
        args: dict[str, Any] = {"resources": {"cpu": cpu, "memoryGB": memory_gb}}
        if checkpoint:
            args["checkpoint"] = checkpoint
        else:
            args["template"] = template or QA_TEMPLATE
        info = await bridge.call("create", args, timeout=timeout)
        return cls(info["id"], bridge, info.get("region"))

    async def checkpoint(self, name: str, timeout: float = 600) -> None:
        """Save this VM's filesystem as a named checkpoint (replacing one with the same name)."""
        await self.bridge.call("checkpoint", {"id": self.vm_id, "name": name}, timeout=timeout)

    @classmethod
    async def from_local(cls, local: LocalSandbox, *, prepared: str | None = None,
                         keep: tuple[str, ...] = DEPENDENCY_DIRS, **kwargs: Any) -> "RailwayVMSandbox":
        """Create a VM and upload a prepared local checkout (including .git) to /work.

        With ``prepared`` (a checkpoint name), boot from that checkpoint when it exists: its dependency
        directories are kept, everything else in /work is replaced by this checkout, and ``vm.prepared``
        is set so callers can skip setup.
        """
        started = time.monotonic()
        vm = None
        if prepared:
            try:
                vm = await cls.create(checkpoint=prepared, **kwargs)
                prune = " -o ".join(f"-name {shlex.quote(name)}" for name in keep)
                result = await vm.exec(
                    f"mkdir -p /work && cd /work && find . -mindepth 1 \\( {prune} \\) -prune -o \\( -type f -o -type l \\)"
                    " -print0 | xargs -0 -r rm -f", timeout=300, cwd="/")
                if not result.ok:
                    raise SandboxError(f"Could not reset the prepared checkout: {result.output[-300:]}")
                vm.prepared = prepared
            except Exception as exc:  # no checkpoint yet (or it expired): build from the template
                log.info("prepared environment %s unavailable: %s", prepared, str(exc)[:200])
                if vm is not None:
                    await vm.close()
                vm = None
        if vm is None:
            vm = await cls.create(**kwargs)
        try:
            buffer = io.BytesIO()
            with tarfile.open(fileobj=buffer, mode="w:gz") as tar:
                skip = set(keep)
                for entry in sorted(local.work.iterdir()):
                    if entry.name in skip:
                        continue
                    tar.add(entry, arcname=entry.name, filter=lambda info: None if PurePosixPath(info.name).name in skip else info)
            data = buffer.getvalue()
            if len(data) > 200_000_000:
                raise SandboxError("Checkout is too large to upload to a VM")
            await vm.bridge.call("write", {"id": vm.vm_id, "path": "/tmp/work.tgz", "base64": base64.b64encode(data).decode()},
                                 timeout=600)
            result = await vm.exec("mkdir -p /work && tar xzf /tmp/work.tgz -C /work && rm /tmp/work.tgz "
                                   "&& git config --global --add safe.directory /work", timeout=300, cwd="/")
            if not result.ok:
                raise SandboxError(f"Could not unpack the checkout in the VM: {result.output[-500:]}")
            log.info("VM %s ready with checkout in %.0fs", vm.vm_id, time.monotonic() - started)
            return vm
        except BaseException:
            await vm.close()
            raise

    async def exec(self, command: str, *, timeout: float = 600, cwd: str | None = None,
                   env: dict[str, str] | None = None, stdin: bytes | None = None,
                   on_output: Callable[[bytes], None] | None = None) -> ExecResult:
        if self._closed:
            raise SandboxError("Sandbox is closed")
        if stdin is not None:
            name = f"/tmp/stdin-{time.monotonic_ns()}"
            await self.bridge.call("write", {"id": self.vm_id, "path": name, "base64": base64.b64encode(stdin).decode()})
            command = f"({command}) < {name}"
        started = time.monotonic()
        workdir = str(self.resolve(cwd)) if cwd and cwd != "/" else ("/" if cwd == "/" else str(self.work))
        base_env = {"CI": "1", "TERM": "dumb", "NO_COLOR": "1", "HOME": str(self.home), "LANG": "C.UTF-8"}
        try:
            # Login shell for the image's PATH, then pin the directory: profiles may change it.
            result = await self.bridge.call("exec", {"id": self.vm_id,
                                                     "command": f"bash -lc {_quote(f'cd {_quote(workdir)} && {command}')}",
                                                     "timeoutSec": int(max(5, timeout)), "cwd": workdir,
                                                     "env": {**base_env, **(env or {})}}, timeout=timeout + 120)
        except asyncio.TimeoutError:
            return ExecResult(command=command, exit_code=124, output="(VM command did not return)", duration=timeout,
                              timed_out=True)
        raw = (result.get("stdout") or "") + (result.get("stderr") or "")
        if on_output and raw:
            try:
                on_output(raw.encode())
            except Exception:
                pass
        output, truncated = _clip(raw.encode())
        timed_out = bool(result.get("timedOut"))
        exit_code = result.get("exitCode")
        return ExecResult(command=command, exit_code=124 if timed_out else (exit_code if exit_code is not None else 1),
                          output=output, duration=time.monotonic() - started, timed_out=timed_out,
                          truncated=truncated or bool(result.get("truncated")), total_bytes=len(raw))

    async def spawn(self, command: str, *, env: dict[str, str] | None = None, log: str = ".factory/app.log",
                    cwd: str | None = None) -> _VMProcess:
        log_path = self.resolve(log)
        workdir = str(self.resolve(cwd)) if cwd else str(self.work)
        wrapped = (f"cd {_quote(workdir)} && mkdir -p {_quote(str(log_path.parent))} && "
                   f"exec bash -c {_quote(command)} >> {_quote(str(log_path))} 2>&1")
        info = await self.bridge.call("spawn", {"id": self.vm_id, "command": f"bash -lc {_quote(wrapped)}",
                                                "cwd": workdir, "env": {"HOME": str(self.home), **(env or {})}})
        exit_future = self.bridge.watch_exit(info["proc"])
        self._procs.append(info["proc"])
        return _VMProcess(info["proc"], exit_future)

    def stop(self, proc: _VMProcess) -> None:
        asyncio.get_running_loop().create_task(self.bridge.call("kill", {"proc": proc.proc_id}))

    def resolve(self, relative: str | None) -> PurePosixPath:
        rel = (relative or ".").strip()
        path = PurePosixPath(posixpath.normpath(rel if rel.startswith("/") else str(self.work / rel)))
        if path != self.work and self.work not in path.parents:
            raise SandboxError(f"Path escapes the repository: {relative}")
        return path

    async def read_bytes_async(self, relative: str, limit: int | None = None, from_end: bool = False) -> bytes:
        args: dict[str, Any] = {"id": self.vm_id, "path": str(self.resolve(relative))}
        if limit:
            args.update(length=limit, fromEnd=from_end)
        try:
            result = await self.bridge.call("read", args, timeout=120)
        except SandboxError as exc:
            raise SandboxError(f"Not a file: {relative}") from exc
        return base64.b64decode(result["base64"])

    def read_text(self, relative: str, limit: int = 400_000) -> str:  # pragma: no cover - sync shim
        raise SandboxError("Use read_text_async for VM sandboxes")

    async def read_text_async(self, relative: str, limit: int = 400_000) -> str:
        data = await self.read_bytes_async(relative)
        if b"\x00" in data[:8000]:
            raise SandboxError(f"Binary file: {relative}")
        text = data[:limit].decode("utf-8", "replace")
        return text + (f"\n… [file truncated at {limit:,} bytes]" if len(data) > limit else "")

    async def write_bytes_async(self, relative: str, content: bytes) -> None:
        path = self.resolve(relative)
        await self.exec(f"mkdir -p {_quote(str(path.parent))}", timeout=60, cwd="/")
        await self.bridge.call("write", {"id": self.vm_id, "path": str(path), "base64": base64.b64encode(content).decode()})

    async def list_files(self, relative: str, pattern: str = "*") -> list[str]:
        result = await self.exec(f"ls -1 {_quote(str(self.resolve(relative)))}/{pattern} 2>/dev/null || true", timeout=60)
        return [line.rsplit("/", 1)[-1] for line in result.output.splitlines() if line.strip()]

    async def close(self, *, keep_files: bool = False) -> None:
        if self._closed:
            return
        self._closed = True
        try:
            await self.bridge.call("destroy", {"id": self.vm_id}, timeout=120)
        except Exception as exc:  # recorded; Railway's idle timeout is the backstop
            log.warning("VM %s destroy failed: %s", self.vm_id, exc)

    async def kill_all(self) -> None:
        for proc in self._procs:
            try:
                await self.bridge.call("kill", {"proc": proc})
            except Exception:
                pass


def _quote(value: str) -> str:
    import shlex

    return shlex.quote(value)
