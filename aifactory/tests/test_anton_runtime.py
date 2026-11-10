"""Real child-process tests on both transports; VM bridge explicitly scrubs env."""
from __future__ import annotations

import asyncio
import base64
import os
from pathlib import Path
import signal
from types import SimpleNamespace

import pytest

pytest.importorskip("anton.core.backends.base")
from aifactory.harness.anton_runtime import AntonScratchpadRuntime, RPC_LIMIT, runtime_factory
from aifactory.sandbox import LocalSandbox, isolation_available
from aifactory.vm import RailwayVMSandbox


class ScrubbedBridge:
    """No inherited host env, no FakeBridge inheritance, real subprocesses."""
    def __init__(self):
        self.procs = {}
        self.read_lengths = []
        self.commands = []

    async def call(self, op, args, timeout=600):
        if op == "write":
            path = Path(args["path"])
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(base64.b64decode(args["base64"]))
            return {}
        if op == "read":
            limit = args.get("length")
            with open(args["path"], "rb") as handle:
                data = handle.read(limit)
            self.read_lengths.append(len(data))
            return {"base64": base64.b64encode(data).decode()}
        if op in {"exec", "spawn"}:
            self.commands.append(args["command"])
            env = {"PATH": "/usr/local/bin:/usr/bin:/bin", "LANG": "C.UTF-8", **args.get("env", {})}
            proc = await asyncio.create_subprocess_exec(
                "/bin/bash", "-c", args["command"], cwd=args.get("cwd", "/"), env=env,
                start_new_session=True, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
            if op == "spawn":
                key = str(proc.pid)
                self.procs[key] = proc
                return {"proc": key}
            try:
                out, err = await asyncio.wait_for(proc.communicate(), args.get("timeoutSec", timeout))
            except BaseException:
                os.killpg(proc.pid, signal.SIGKILL)
                await proc.communicate()
                raise
            return {"exitCode": proc.returncode, "stdout": out.decode(), "stderr": err.decode()}
        if op == "kill":
            proc = self.procs[args["proc"]]
            if proc.returncode is None:
                os.killpg(proc.pid, signal.SIGKILL)
            await proc.wait()
            return {}
        raise AssertionError(op)

    def watch_exit(self, key):
        async def wait():
            return {"exitCode": await self.procs[key].wait()}
        return asyncio.create_task(wait())


@pytest.fixture(params=["local", "vm"])
async def runtime(request, tmp_path, monkeypatch):
    monkeypatch.setenv("AIFACTORY_SANDBOX_ISOLATION", "1")
    if not isolation_available():
        pytest.skip("Private UID process tests require root isolation")
    # pytest's private parent directories must be traversable by sandbox UIDs.
    for parent in (tmp_path, *tmp_path.parents):
        if parent == Path("/tmp"):
            break
        parent.chmod(0o755)
    for key in ("FACTORY_SECRET", "GITHUB_TOKEN", "OPENAI_API_KEY", "DS_TEST__PASSWORD"):
        monkeypatch.setenv(key, "injected-host-secret")
    if request.param == "local":
        sandbox = LocalSandbox("pad", root=tmp_path).create()
        sandbox.work.mkdir()
        sandbox.adopt_work()
    else:
        monkeypatch.setenv("AIFACTORY_VM_WORK", str(tmp_path / "work"))
        (tmp_path / "work").mkdir()
        sandbox = RailwayVMSandbox("test", ScrubbedBridge())
    session = SimpleNamespace(sandbox=sandbox, command_timeout=30, remaining=lambda: 120,
                              cancelled=asyncio.Event())
    pad = runtime_factory(session)(name="test", cells=None, coding_api_key="factory-credential",
                                   workspace_env_overlay={"BAD": "factory-credential"},
                                   scratchpad_ds_env={"BAD": "factory-credential"},
                                   coding_api_key_supplier=lambda: pytest.fail("must not call supplier"))
    try:
        yield pad
    finally:
        await pad.cleanup()
        if sandbox.kind == "local":
            await sandbox.close()
        else:
            await sandbox.kill_all()


def live(pid):
    try:
        return Path(f"/proc/{pid}/stat").read_text().rsplit(")", 1)[1].split()[0] != "Z"
    except FileNotFoundError:
        return False


async def test_persistent_process_identity_and_secrets(runtime):
    pad = runtime
    first = await pad.execute("import os, math, sys\nx = 41\nprint(os.getpid(), os.getuid())")
    assert first.error is None
    pid, uid = map(int, first.stdout.split())
    assert pid != os.getpid() and uid != 0
    if pad.sandbox.kind == "local":
        assert uid == pad._uid and uid != pad.sandbox.uid
    second = await pad.execute("print(x + 1, math.sqrt(4)); print(dict(os.environ)); print(sys.executable)")
    assert "42 2.0" in second.stdout
    assert "injected-host-secret" not in second.stdout
    assert "factory-credential" not in second.stdout
    assert "/venv/bin/python" in second.stdout
    assert pad.cells == [first, second]
    path = Path(str(pad.sandbox.work)) / pad.directory
    assert "factory-credential" not in (path / "worker.py").read_text()
    assert "factory-credential" not in Path(f"/proc/{pid}/cmdline").read_text()


async def test_reset_truncation_and_traceback(runtime):
    pad = runtime
    await pad.execute("x = 1")
    cell = await pad.execute("print('A' * 2000000 + 'TAIL')")
    assert cell.error is None and len(cell.stdout) < 31000
    assert "omitted" in cell.stdout and cell.stdout.endswith("TAIL\n")
    root = Path(str(pad.sandbox.work)) / pad.directory
    assert (root / "result.json").stat().st_size < RPC_LIMIT
    assert (root / "worker.log").stat().st_size < 4096
    cell = await pad.execute("def fail():\n    raise ValueError('oops')\nx = 2\nfail()")
    assert cell.stopped_at == 4
    assert 'File "<scratchpad>", line 4' in cell.error
    assert 'File "<scratchpad>", line 2' in cell.error
    await pad.reset()
    assert not pad.cells
    assert "NameError" in (await pad.execute("print(x)")).error


async def test_installer_private_interpreter_and_environment(runtime):
    pad = runtime
    await pad.start()
    # Deterministic, offline pip stand-in inside the venv: executable evidence,
    # not a mocked controller command or host interpreter installation.
    code = """import pathlib, sys
p = pathlib.Path(sys.prefix) / 'lib' / ('python' + str(sys.version_info.major) + '.' + str(sys.version_info.minor)) / 'site-packages' / 'pip'
(p / '__main__.py').write_text("import os,sys; print('INSTALLER',sys.executable,sys.argv,dict(os.environ))")
"""
    assert (await pad.execute(code)).error is None
    output = await pad.install_packages(["example-package"])
    assert "INSTALLER" in output and "/venv/bin/python" in output and "example-package" in output
    assert "injected-host-secret" not in output and "factory-credential" not in output


# A detached, envless double fork escapes ancestry and marker based cleanup.
DETACH = """import os, subprocess, sys
child = 'import os,time; os.setsid(); p=os.fork(); (os._exit(0) if p else None); open(__import__("sys").argv[1],"w").write(str(os.getpid())); time.sleep(120)'
p = subprocess.Popen([sys.executable, '-c', child, str(__import__('pathlib').Path(sys.prefix).parent / 'detached.pid')], env={}, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
p.wait()
"""


async def detached(pad):
    assert (await pad.execute(DETACH)).error is None
    path = Path(str(pad.sandbox.work)) / pad.directory / "detached.pid"
    for _ in range(100):
        if path.exists() and path.read_text():
            return int(path.read_text())
        await asyncio.sleep(.02)
    pytest.fail("detached child did not start")


async def test_dead_supervisor_detached_child_close(runtime):
    pad = runtime
    pid = await detached(pad)
    worker = (await pad._json("boot.json"))["pid"]
    os.kill(worker, signal.SIGKILL)
    assert live(pid)
    await asyncio.wait_for(pad.close(), 10)
    assert not live(pid) and not live(worker)
    assert not (Path(str(pad.sandbox.work)) / pad.directory).exists()
    await pad.close()
    await pad.cleanup()


@pytest.mark.parametrize("stop", ["timeout", "cancel", "event"])
async def test_cell_terminal_cleanup(runtime, stop):
    pad = runtime
    pid = await detached(pad)
    if stop == "timeout":
        pad.session.command_timeout = .4
    task = asyncio.create_task(pad.execute("import time; time.sleep(120)"))
    await asyncio.sleep(.1)
    if stop == "cancel":
        await pad.cancel()
    elif stop == "event":
        pad.session.cancelled.set()
    result = await asyncio.wait_for(asyncio.gather(task, return_exceptions=True), 10)
    if stop == "timeout":
        assert result[0].ended_process and "namespace lost" in result[0].error
    else:
        assert isinstance(result[0], asyncio.CancelledError)
    assert not live(pid)
    pad.session.command_timeout = 30
    pad.session.cancelled.clear()
    assert (await pad.execute("print(42)")).stdout == "42\n"


@pytest.mark.parametrize("stop", ["cancel", "close"])
async def test_cancel_does_not_await_owner_continuation(runtime, stop):
    pad = runtime
    descendant = await detached(pad)
    worker = (await pad._json("boot.json"))["pid"]
    continuing = asyncio.Event()
    release = asyncio.Event()

    async def owner():
        try:
            await pad.execute("import time; time.sleep(120)")
        except asyncio.CancelledError:
            continuing.set()
            await release.wait()  # valid unrelated caller work, never await it

    # A result publication barrier avoids timing assumptions about whether the
    # first cell or the queued operation has started when stop is called.
    started = asyncio.Event()
    original_wait = pad._wait

    async def waiting(filename):
        if filename == "result.json":
            started.set()
        return await original_wait(filename)

    pad._wait = waiting
    owner_task = asyncio.create_task(owner())
    await asyncio.wait_for(started.wait(), 5)
    second = asyncio.create_task(pad.execute("print('queued')"))
    try:
        await asyncio.wait_for(getattr(pad, stop)(), 5)
        await asyncio.wait_for(continuing.wait(), 5)
        assert not live(worker) and not live(descendant) and not owner_task.done()
        assert isinstance((await asyncio.gather(second, return_exceptions=True))[0], asyncio.CancelledError)
        if stop == "cancel":
            assert (await pad.execute("print('new')")).stdout == "new\n"
    finally:
        release.set()
        await owner_task


async def test_startup_acknowledgement_cancellation(runtime, monkeypatch):
    pad = runtime
    original = pad.sandbox.spawn
    spawned, release = asyncio.Event(), asyncio.Event()
    processes = []

    async def delayed(*args, **kwargs):
        process = await original(*args, **kwargs)
        processes.append(process)
        spawned.set()
        await release.wait()
        return process

    monkeypatch.setattr(pad.sandbox, "spawn", delayed)
    start = asyncio.create_task(pad.start())
    await asyncio.wait_for(spawned.wait(), 20)
    start.cancel()
    closing = asyncio.create_task(pad.cancel())
    await asyncio.sleep(.1)
    release.set()
    await asyncio.wait_for(asyncio.gather(start, closing, return_exceptions=True), 10)
    assert not pad._pending_cleanup
    for process in processes:
        if pad.sandbox.kind == "local":
            assert not live(process.pid)
        else:
            assert not live(int(process.proc_id))


async def test_rpc_bound(runtime):
    pad = runtime
    await pad.start()
    root = Path(str(pad.sandbox.work)) / pad.directory
    (root / "result.json").write_bytes(b"x" * 5_000_000)
    with pytest.raises(Exception, match="Oversized"):
        await pad._json("result.json")
    if pad.sandbox.kind == "vm":
        assert pad.sandbox.bridge.read_lengths[-1] == RPC_LIMIT + 1


async def test_refuses_unenforced_policy_and_nonisolated_sandbox(runtime):
    pad = runtime
    with pytest.raises(ValueError, match="CellSandbox policy is unsupported"):
        AntonScratchpadRuntime(pad.session, name="bad", cell_sandbox=object())
    assert pad.cell_sandbox is None
    if pad.sandbox.kind == "local":
        uid = pad.sandbox.uid
        pad.sandbox.uid = None
        try:
            with pytest.raises(Exception, match="private UID"):
                await pad.start()
        finally:
            pad.sandbox.uid = uid


@pytest.mark.parametrize("stop", ["timeout", "cancel"])
async def test_installer_terminal_cleanup(runtime, stop):
    pad = runtime
    pid = await detached(pad)
    code = """import pathlib, sys
p = pathlib.Path(sys.prefix) / 'lib' / ('python' + str(sys.version_info.major) + '.' + str(sys.version_info.minor)) / 'site-packages' / 'pip'
(p / '__main__.py').write_text('import time; time.sleep(120)')
"""
    assert (await pad.execute(code)).error is None
    if stop == "timeout":
        pad.session.command_timeout = .4
    install = asyncio.create_task(pad.install_packages(["slow-package"]))
    await asyncio.sleep(.1)
    if stop == "cancel":
        await pad.cancel()
    result = (await asyncio.wait_for(asyncio.gather(install, return_exceptions=True), 10))[0]
    assert isinstance(result, (TimeoutError, asyncio.CancelledError))
    assert not live(pid)
    assert not pad.process_running


async def test_stale_caller_does_not_stop_new_generation(runtime):
    pad = runtime
    await pad.start()
    pad.session.command_timeout = .5
    first = asyncio.create_task(pad.execute("import time; time.sleep(120)"))
    second = asyncio.create_task(pad.execute("print('queued')"))
    await asyncio.sleep(.1)
    await pad.cancel()
    pad.session.command_timeout = 30
    assert (await pad.execute("new_value = 123")).error is None
    await asyncio.gather(first, second, return_exceptions=True)
    await asyncio.sleep(.6)
    assert (await pad.execute("print(new_value)")).stdout == "123\n"


async def test_dead_process_reports_namespace_loss(runtime):
    pad = runtime
    await pad.start()
    worker = (await pad._json("boot.json"))["pid"]
    os.kill(worker, signal.SIGKILL)
    for _ in range(100):
        if not pad.process_running:
            break
        await asyncio.sleep(.01)
    cell = await pad.execute("print('should not silently restart')")
    assert cell.ended_process and "namespace lost" in cell.error
    assert (await pad.execute("print('fresh')")).stdout == "fresh\n"


async def test_cleanup_failure_is_retryable(runtime, monkeypatch):
    pad = runtime
    await pad.start()
    from aifactory.harness import anton_runtime
    original = anton_runtime.terminate_identity

    async def fail(*args):
        raise RuntimeError("cleanup unavailable")

    monkeypatch.setattr(anton_runtime, "terminate_identity", fail)
    with pytest.raises(RuntimeError, match="cleanup unavailable"):
        await pad.close()
    assert pad._pending_cleanup
    monkeypatch.setattr(anton_runtime, "terminate_identity", original)
    await pad.close()
    await pad.cleanup()
    assert not pad._pending_cleanup


async def test_session_and_tool_time_caps(runtime):
    pad = runtime
    pad.session.command_timeout = 9999
    pad.session.remaining = lambda: 9000
    assert pad._timeout() == 1800
    pad.session.remaining = lambda: 3
    assert pad._timeout() == 3
    pad.session.command_timeout = 1
    assert pad._timeout() == 1


BIG_RESPONSE_CELL = (
    "import sys\n"
    "sys.stdout.write('\\x01' * 30000)\n"
    "sys.stderr.write('\\x02' * 30000)\n"
    "raise ValueError('\\U0001F600' * 30000)"
)


def test_encoded_response_is_bounded_by_bytes():
    from aifactory.harness import anton_worker
    assert anton_worker.RPC_LIMIT == RPC_LIMIT
    response = {"stdout": "\x01" * 30000, "stderr": "\x02" * 30000,
                "error": "\U0001F600" * 30000, "stopped_at": 4}
    raw = anton_worker.encode_response(response)
    assert len(raw) <= RPC_LIMIT
    # Escaped per-field bytes exceed the limit when measured with ensure_ascii.
    import json
    assert len(json.dumps(response).encode()) > RPC_LIMIT
    assert json.loads(raw) == response  # Fits once encoded as UTF-8: nothing clipped.
    huge = {"stdout": "\x01" * 200_000, "stderr": "\x02" * 200_000,
            "error": "\U0001F600" * 200_000 + "\ud800END", "stopped_at": None}
    raw = anton_worker.encode_response(huge)
    assert len(raw) <= RPC_LIMIT
    decoded = json.loads(raw)
    assert "omitted" in decoded["stdout"] and "omitted" in decoded["error"]
    assert decoded["error"].endswith("?END") and decoded["stopped_at"] is None


async def test_large_encoded_response_keeps_namespace_real_worker(tmp_path):
    """A real worker process (no root needed) publishes a bounded response."""
    import json
    import sys
    worker = Path(__file__).parents[1] / "src/aifactory/harness/anton_worker.py"
    proc = await asyncio.create_subprocess_exec(
        sys.executable, str(worker), "serve", "0", str(tmp_path), "0",
        env={"PATH": "/usr/bin:/bin"}, stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.DEVNULL)

    async def wait_for(name):
        path = tmp_path / name
        for _ in range(2000):
            if path.exists():
                return path
            if proc.returncode is not None:
                pytest.fail("worker exited: namespace destroyed")
            await asyncio.sleep(.005)
        pytest.fail(f"worker did not publish {name}")

    async def run(code):
        (tmp_path / "result.json").unlink(missing_ok=True)
        (tmp_path / "request.tmp").write_text(json.dumps({"action": "execute", "code": code}))
        (tmp_path / "request.tmp").replace(tmp_path / "request.json")
        raw = (await wait_for("result.json")).read_bytes()
        assert len(raw) <= RPC_LIMIT
        return json.loads(raw)

    try:
        await wait_for("boot.json")
        assert (await run("kept = 'namespace'"))["error"] is None
        result = await run(BIG_RESPONSE_CELL)
        assert result["stopped_at"] == 4 and "ValueError" in result["error"]
        assert result["stdout"].count("\x01") > 0 and result["stderr"].count("\x02") > 0
        assert proc.returncode is None
        assert (await run("print(kept)"))["stdout"] == "namespace\n"
    finally:
        if proc.returncode is None:
            proc.kill()
        await proc.wait()


async def test_large_encoded_response_keeps_namespace(runtime):
    pad = runtime
    await pad.execute("kept = 'namespace'")
    cell = await pad.execute(BIG_RESPONSE_CELL)
    root = Path(str(pad.sandbox.work)) / pad.directory
    assert (root / "result.json").stat().st_size <= RPC_LIMIT
    assert not cell.ended_process and cell.stopped_at == 4 and "ValueError" in cell.error
    assert pad.process_running
    assert (await pad.execute("print(kept)")).stdout == "namespace\n"


async def test_closing_pad_preserves_unrelated_job_process(runtime):
    pad = runtime
    process = await pad.sandbox.spawn("exec python3 -c 'import time; time.sleep(120)'", log=".factory/unrelated.log")
    pid = process.pid if pad.sandbox.kind == "local" else int(process.proc_id)
    try:
        await pad.start()
        old_uid = pad._uid
        await pad.reset()
        assert live(pid)
        if pad.sandbox.kind == "local":
            assert old_uid != pad.sandbox.uid and pad._uid != pad.sandbox.uid
        await pad.close()
        assert live(pid)
        if pad.sandbox.kind == "local":
            assert not pad.sandbox._identities
    finally:
        pad.sandbox.stop(process)
        if pad.sandbox.kind == "local":
            await process.wait()


@pytest.mark.parametrize("transport", ["local", "vm"])
@pytest.mark.parametrize("stop", ["cancel", "close"])
async def test_operation_drain_never_waits_for_caller(tmp_path, monkeypatch, transport, stop):
    """Unprivileged transport probe for the review's owner-continuation deadlock.

    This deliberately bypasses identity provisioning, not as isolation evidence:
    only the trusted worker protocol and runtime task ownership are under test.
    The production start path still refuses nonisolated execution.
    """
    from aifactory.harness.anton_worker import __file__ as worker_source
    from aifactory.sandbox import write_text
    import shlex

    monkeypatch.setenv("AIFACTORY_SANDBOX_ISOLATION", "0")
    if transport == "local":
        sandbox = LocalSandbox("drain", root=tmp_path).create()
        sandbox.work.mkdir()
    else:
        monkeypatch.setenv("AIFACTORY_VM_WORK", str(tmp_path / "work"))
        (tmp_path / "work").mkdir()
        sandbox = RailwayVMSandbox("drain", ScrubbedBridge())
    session = SimpleNamespace(sandbox=sandbox, command_timeout=30, remaining=lambda: 120,
                              cancelled=asyncio.Event())
    pad = AntonScratchpadRuntime(session, name="drain")
    process = None

    async def start_protocol():
        nonlocal process
        if pad.process_running:
            return
        await write_text(sandbox, f"{pad.directory}/worker.py", Path(worker_source).read_text())
        process = await sandbox.spawn(shlex.join([
            "python3", f"{pad.directory}/worker.py", "serve", "0", pad.directory, "0"]),
            log=f"{pad.directory}/worker.log")
        pad._process = process
        await pad._wait("boot.json")

    async def stop_protocol():
        if pad._process is not None:
            sandbox.stop(pad._process)
            if transport == "local":
                await pad._process.wait()
            else:
                await sandbox.bridge.procs[pad._process.proc_id].wait()
            pad._process = None
            import shutil
            shutil.rmtree(Path(str(sandbox.work)) / pad.directory, ignore_errors=True)

    monkeypatch.setattr(pad, "_start", start_protocol)
    monkeypatch.setattr(pad, "_shutdown", stop_protocol)
    entered, continued, release = asyncio.Event(), asyncio.Event(), asyncio.Event()
    original_wait = pad._wait

    async def waiting(filename):
        if filename == "result.json":
            entered.set()
        return await original_wait(filename)

    monkeypatch.setattr(pad, "_wait", waiting)

    async def owner():
        try:
            await pad.execute("import time; time.sleep(120)")
        except asyncio.CancelledError:
            continued.set()
            await release.wait()

    owner_task = second = None
    try:
        await pad.start()
        worker = (await pad._json("boot.json"))["pid"]
        owner_task = asyncio.create_task(owner())
        await asyncio.wait_for(entered.wait(), 5)
        second = asyncio.create_task(pad.execute("print('queued')"))
        await asyncio.wait_for(getattr(pad, stop)(), 5)
        await asyncio.wait_for(continued.wait(), 5)
        assert not live(worker) and not owner_task.done()
        assert isinstance((await asyncio.gather(second, return_exceptions=True))[0], asyncio.CancelledError)
        if stop == "cancel":
            assert (await pad.execute("print('new namespace')")).stdout == "new namespace\n"
    finally:
        release.set()
        for task in (owner_task, second):
            if task is not None:
                task.cancel()
        await asyncio.gather(*(t for t in (owner_task, second) if t is not None), return_exceptions=True)
        await pad.close()
        if transport == "local":
            await sandbox.close()
        else:
            await sandbox.kill_all()
