"""Anton SDK scratchpad transport; no agent Python ever executes in this process.

One process/namespace per pad. A terminal failure loses the *whole* namespace;
there is no pickle snapshot, replay, host environment overlay or coding client.
"""
from __future__ import annotations

import asyncio
import json
from pathlib import Path
import shlex
from uuid import uuid4

from anton.core.backends.base import Cell, ScratchpadRuntime

from ..sandbox import SandboxError, _clip, read_bytes, terminate_identity, write_text
from .anton_worker import RPC_LIMIT  # Constant only; the worker never runs here.
from .base import HarnessSession

REQUEST_LIMIT = 1_000_000


class AntonScratchpadRuntime(ScratchpadRuntime):
    def __init__(self, session: HarnessSession, *, name: str, cells=None, cell_sandbox=None, **_ignored):
        if cell_sandbox is not None:
            raise ValueError("CellSandbox policy is unsupported; the sandbox UID/VM is the isolation boundary")
        # Deliberately discard all SDK credentials, suppliers and environment kwargs.
        super().__init__(name, coding_provider="", coding_model="", coding_api_key="", coding_base_url="", cells=cells)
        self.session = session
        self.sandbox = session.sandbox
        self.directory = f".factory/anton/{uuid4().hex}/{uuid4().hex}"
        self._vm = self.sandbox.kind == "vm"
        self._user = "anton_" + uuid4().hex[:16]
        self._uid = None
        self._process = None
        self._pending_cleanup = False
        self._closed = False
        self._stopping = False
        self._generation = 0
        self._serial = asyncio.Lock()
        self._lifecycle = asyncio.Lock()
        # Only runtime-owned tasks belong here. Never await an entire caller:
        # it may catch CancelledError and continue unrelated work indefinitely.
        self._operations: set[asyncio.Task] = set()

    @property
    def process_running(self):
        return self._process is not None and self._process.returncode is None

    def _timeout(self):
        return max(0, min(1800, self.session.command_timeout, self.session.remaining()))

    async def _command(self, command):
        result = await self.sandbox.exec(command, timeout=self._timeout())
        if not result.ok:
            raise SandboxError(result.for_model())
        return result.output

    async def _json(self, filename):
        raw = await read_bytes(self.sandbox, f"{self.directory}/{filename}", RPC_LIMIT + 1)
        if len(raw) > RPC_LIMIT:
            raise SandboxError("Oversized scratchpad RPC response")
        value = json.loads(raw)
        if not isinstance(value, dict):
            raise SandboxError("Invalid scratchpad RPC response")
        return value

    async def _wait(self, filename):
        path = shlex.quote(f"{self.directory}/{filename}")
        # Poll via exec: works on both transports without an unbounded file read.
        await self._command(f"while [ ! -f {path} ]; do sleep 0.02; done")
        return await self._json(filename)

    async def _start(self):
        if self._closed:
            raise SandboxError("Scratchpad is closed")
        if self.process_running:
            return
        if self._pending_cleanup:
            raise SandboxError("Scratchpad process died; namespace lost")
        if not self._vm and self.sandbox.uid is None:
            raise SandboxError("Scratchpad requires sandbox isolation with a private UID")
        self._pending_cleanup = True
        if self._vm:
            # Reserve an OS identity before starting any untrusted process.
            await self._command(f"/usr/sbin/useradd -M -U {self._user}")
            self._uid = int((await self._command(f"id -u {self._user}")).strip())
        else:
            self._uid = self.sandbox.reserve_identity()
            # Run permission changes as the job owner, never in the controller.
            # The pad shares file access through the job group, not its UID.
            await self._command("chmod -R g+rwX -- .")
        source = Path(__file__).with_name("anton_worker.py").read_text()
        await write_text(self.sandbox, f"{self.directory}/worker.py", source)
        args = ["python3", f"{self.directory}/worker.py", "launch", str(self._uid), self.directory, str(int(self._vm))]
        # A cancelled bridge acknowledgement must not discard a process handle.
        spawn = asyncio.create_task(self.sandbox.spawn(shlex.join(args), log=f"{self.directory}/worker.log",
                                                       **({"identity": self._uid} if not self._vm else {})))
        try:
            self._process = await asyncio.shield(spawn)
        except asyncio.CancelledError:
            # Repeated cancellation must not cancel the acknowledgement task.
            while not spawn.done():
                try:
                    await asyncio.shield(spawn)
                except asyncio.CancelledError:
                    continue
            self._process = spawn.result()
            raise
        await self._wait("boot.json")

    async def _shutdown(self):
        if not self._pending_cleanup:
            return
        if self._uid is not None:
            await terminate_identity(self.sandbox, self._uid)
        elif self._vm:
            # Cancellation can arrive during useradd/id; lookup is idempotent.
            result = await self.sandbox.exec(f"id -u {self._user}", timeout=10)
            if result.ok:
                await terminate_identity(self.sandbox, int(result.output.strip()))
        if self._process is not None:
            self.sandbox.stop(self._process)
            if not self._vm:
                await self._process.wait()
        private = self.directory.rsplit("/", 1)[0]
        if self._vm:
            result = await self.sandbox.exec("rm -rf -- " + shlex.quote(private), timeout=10)
            if not result.ok:
                raise SandboxError("Could not remove scratchpad private files")
        else:
            await self.sandbox.remove_private_files(private)
        if self._vm:
            result = await self.sandbox.exec(f"if id {self._user} >/dev/null 2>&1; then /usr/sbin/userdel {self._user}; fi", timeout=10)
            if not result.ok:
                raise SandboxError("Could not release scratchpad UID")
        if not self._vm and self._uid is not None:
            self.sandbox.release_identity(self._uid)
        self._process = None
        self._uid = None
        self._pending_cleanup = False

    async def _bounded(self, function):
        if self._stopping or self._closed:
            raise asyncio.CancelledError
        generation = self._generation

        async def operation():
            async with self._serial:
                if generation != self._generation or self._closed or self._stopping:
                    raise asyncio.CancelledError
                return await function()

        task = asyncio.create_task(operation())
        self._operations.add(task)
        cancelled = asyncio.create_task(self.session.cancelled.wait())
        try:
            done, _ = await asyncio.wait({task, cancelled}, timeout=self._timeout(), return_when=asyncio.FIRST_COMPLETED)
            if cancelled in done:
                raise asyncio.CancelledError
            if task not in done:
                raise TimeoutError("Scratchpad operation timed out; namespace lost")
            return await task
        except BaseException:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
            async with self._lifecycle:
                if generation == self._generation:
                    await self._stop_operations()
            raise
        finally:
            cancelled.cancel()
            await asyncio.gather(cancelled, return_exceptions=True)
            self._operations.discard(task)

    async def start(self):
        await self._bounded(self._start)

    async def _stop_operations(self):
        self._generation += 1
        self._stopping = True
        try:
            # Drain transport operations only. Their callers do not belong to
            # this runtime and may continue indefinitely after cancellation.
            tasks = tuple(self._operations)
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            await self._shutdown()
        finally:
            self._stopping = False

    async def close(self):
        async with self._lifecycle:
            self._closed = True
            await self._stop_operations()

    async def cleanup(self):
        await self.close()

    async def cancel(self):
        async with self._lifecycle:
            await self._stop_operations()
        # Restart lazily so a cancelled/deadline-expired session cannot respawn.

    async def reset(self):
        await self.cancel()
        self.cells.clear()
        self.compacted_cells = ()
        self._installed_packages.clear()
        await self.start()

    async def _rpc(self, request):
        await self._start()
        await self._command("rm -f -- " + shlex.quote(f"{self.directory}/result.json"))
        raw = json.dumps(request)
        if len(raw.encode()) > REQUEST_LIMIT:
            raise SandboxError("Oversized scratchpad request")
        # Atomic publication: the worker must not see a partial write.
        await write_text(self.sandbox, f"{self.directory}/request.tmp", raw)
        await self._command(f"mv {shlex.quote(self.directory + '/request.tmp')} {shlex.quote(self.directory + '/request.json')}")
        result = await self._wait("result.json")
        for field in ("stdout", "stderr"):
            if not isinstance(result.get(field), str):
                raise SandboxError("Invalid scratchpad output")
            result[field] = _clip(result[field].encode())[0]
        error = result.get("error")
        if error is not None and not isinstance(error, str):
            raise SandboxError("Invalid scratchpad error")
        result["error"] = _clip(error.encode())[0] if error else None
        stopped = result.get("stopped_at")
        if stopped is not None and (type(stopped) is not int or stopped < 1):
            raise SandboxError("Invalid scratchpad stopped_at")
        return result

    async def install_packages(self, packages):
        if not all(isinstance(p, str) and p and not p.startswith("-") for p in packages):
            raise ValueError("Expected package names or paths, not pip options")
        result = await self._bounded(lambda: self._rpc({"action": "install", "packages": packages}))
        if not result.get("error"):
            self._installed_packages.update(packages)
        return result["stdout"] + result["stderr"] + (result.get("error") or "")

    async def execute_streaming(self, code, *, description="", estimated_time="", estimated_seconds=0):
        try:
            result = await self._bounded(lambda: self._rpc({"action": "execute", "code": code}))
            cell = Cell(code, result["stdout"], result["stderr"], result.get("error"),
                        stopped_at=result.get("stopped_at"))
        except BaseException as exc:
            cell = Cell(code, "", "", f"{type(exc).__name__}: {exc}; namespace lost", stopped_at=1, ended_process=True)
            cell.description, cell.estimated_time = description, estimated_time
            cell = self._record_cell(cell)
            if isinstance(exc, (asyncio.CancelledError, KeyboardInterrupt, SystemExit)):
                raise
            yield cell
            return
        cell.description, cell.estimated_time = description, estimated_time
        yield self._record_cell(cell)


def runtime_factory(session: HarnessSession):
    """SDK factory bound to a Factory-owned sandbox, not SDK connection details."""
    def create(**kwargs):
        return AntonScratchpadRuntime(session, **kwargs)
    return create
