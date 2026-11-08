"""RailwayVMSandbox semantics against a fake bridge that maps the VM filesystem onto a temp dir."""

from __future__ import annotations

import asyncio
import base64
import subprocess
import sys

import pytest

from aifactory.sandbox import LocalSandbox, playwright_browsers_path, read_text, write_text

BROWSERS = playwright_browsers_path()
from aifactory.vm import RailwayVMSandbox


class FakeBridge:
    def __init__(self, root):
        self.root = root
        self.calls = []
        self.procs = {}
        self.exits = {}

    def _map(self, text: str) -> str:
        return text.replace("/work", str(self.root / "work")).replace("/tmp/", str(self.root / "tmp") + "/")

    async def call(self, op, args=None, timeout=600):
        args = args or {}
        self.calls.append(op)
        if op == "create":
            (self.root / "work").mkdir(parents=True, exist_ok=True)
            (self.root / "tmp").mkdir(exist_ok=True)
            return {"id": "sbx_test", "region": "test"}
        if op == "write":
            path = self.root.joinpath(self._map(args["path"]).lstrip("/")) if not self._map(args["path"]).startswith(str(self.root)) else None
            target = path or __import__("pathlib").Path(self._map(args["path"]))
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(base64.b64decode(args["base64"]))
            return {}
        if op == "read":
            data = __import__("pathlib").Path(self._map(args["path"])).read_bytes()
            if args.get("length"):
                data = data[-args["length"]:] if args.get("fromEnd") else data[:args["length"]]
            return {"base64": base64.b64encode(data).decode()}
        if op == "exec":
            cwd = self._map(args.get("cwd") or "/work")
            if cwd == "/":
                cwd = str(self.root)
            import os

            proc = await asyncio.create_subprocess_shell(self._map(args["command"]), cwd=cwd, env={**os.environ, **(args.get("env") or {}), "HOME": str(self.root), "PLAYWRIGHT_BROWSERS_PATH": BROWSERS},
                                                         stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
            out, err = await asyncio.wait_for(proc.communicate(), timeout=args.get("timeoutSec", 60))
            return {"exitCode": proc.returncode, "stdout": out.decode(), "stderr": err.decode(), "timedOut": False,
                    "truncated": False}
        if op == "spawn":
            import os

            proc = await asyncio.create_subprocess_shell(self._map(args["command"]), cwd=self._map(args.get("cwd") or "/work"),
                                                         start_new_session=True, env={**os.environ, **(args.get("env") or {}), "HOME": str(self.root), "PLAYWRIGHT_BROWSERS_PATH": BROWSERS})
            pid = f"p{len(self.procs)}"
            self.procs[pid] = proc
            return {"proc": pid}
        if op == "kill":
            proc = self.procs.get(args["proc"])
            if proc and proc.returncode is None:
                import os
                import signal

                os.killpg(proc.pid, signal.SIGKILL)
            return {}
        if op == "destroy":
            return {"destroyed": True}
        raise AssertionError(op)

    def watch_exit(self, proc):
        return asyncio.get_running_loop().create_future()


def test_vm_sandbox_uploads_checkout_runs_commands_and_files(tmp_path, monkeypatch):
    local = LocalSandbox("src", root=tmp_path / "local").create()
    local.work.mkdir()
    subprocess.run(["git", "init", "-q"], cwd=local.work, check=True)
    (local.work / "app.txt").write_text("hello\n")
    (local.work / "node_modules").mkdir()
    (local.work / "node_modules" / "big.bin").write_text("skip me")
    bridge = FakeBridge(tmp_path / "vm")
    monkeypatch.setattr("aifactory.vm.VMBridge.get", classmethod(lambda cls: bridge))

    async def go():
        vm = await RailwayVMSandbox.from_local(local)
        assert (await vm.exec("cat app.txt")).output.strip() == "hello"
        assert not (tmp_path / "vm" / "work" / "node_modules").exists()
        await write_text(vm, "dir/new.txt", "written")
        assert await read_text(vm, "dir/new.txt") == "written"
        with pytest.raises(Exception):
            vm.resolve("../etc/passwd")
        result = await vm.exec("exit 3")
        assert result.exit_code == 3 and not result.ok
        await vm.close()
        assert bridge.calls[-1] == "destroy"

    asyncio.run(go())


def test_runtime_qa_runs_inside_the_vm_sandbox(tmp_path, monkeypatch):
    pytest.importorskip("playwright")
    from aifactory.pipeline import qa

    bridge = FakeBridge(tmp_path / "vm")
    monkeypatch.setattr("aifactory.vm.VMBridge.get", classmethod(lambda cls: bridge))
    # In the fake "VM" the QA interpreter is this test's Python (which has Playwright).
    monkeypatch.setattr(qa, "shot_command", lambda sandbox=None: f"{sys.executable} .factory/qa/shot.py")

    async def go():
        vm = await RailwayVMSandbox.create()
        (tmp_path / "vm" / "work" / "index.html").write_text("<title>VM app</title><h1>ok</h1>")
        run = qa.AppRun(vm, {"start": "exec python3 -m http.server $PORT --bind 127.0.0.1", "ready_path": "/",
                             "ready_timeout": 30, "pages": ["/"], "probes": [{"path": "/", "expect_status": 200}]})
        ready = await run.start()
        assert ready, await run.log_tail()
        pages = await run.screenshots()
        probes = await run.probes()
        images = await run.collect_images()
        run.stop()
        await asyncio.sleep(0.2)
        assert pages[0].get("status") == 200 and pages[0].get("title") == "VM app", pages
        assert probes[0]["ok"] and images and images[0][1].startswith("iVBOR")
        assert run.evidence["isolation"].startswith("railway-vm sbx_test")

    asyncio.run(go())
