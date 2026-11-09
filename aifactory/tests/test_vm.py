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
        # Only VM-absolute paths (at a word start), never "/work" inside a host path like /home/runner/work/...
        import re

        # One pass, so a mapped path (which may itself live under /tmp on Linux) is never mapped again.
        targets = {"/fake-vm-work": str(self.root / "work"), "/tmp/": str(self.root / "tmp") + "/",
                   "/root": str(self.root / "home")}
        return re.sub(r"(^|[\s\"'=:;(])(/fake-vm-work|/tmp/|/root)", lambda m: m.group(1) + targets[m.group(2)], text)

    async def call(self, op, args=None, timeout=600):
        args = args or {}
        self.calls.append(op)
        if op == "create":
            import shutil

            if args.get("checkpoint"):
                saved = self.root.parent / "checkpoints" / args["checkpoint"]
                if not saved.exists():
                    raise RuntimeError("checkpoint not found")
                shutil.rmtree(self.root / "work", ignore_errors=True)
                shutil.copytree(saved, self.root / "work", symlinks=True)
            (self.root / "work").mkdir(parents=True, exist_ok=True)
            (self.root / "tmp").mkdir(exist_ok=True)
            # Like some real images: a login profile that changes directory. Commands must not depend on it.
            (self.root / ".bash_profile").write_text('cd "$HOME"\n')
            return {"id": "sbx_test", "region": "test"}
        if op == "checkpoints":
            base = self.root.parent / "checkpoints"
            items = sorted(base.iterdir(), key=lambda p: p.stat().st_mtime_ns) if base.exists() else []
            return {"items": [{"id": p.name, "key": p.name, "createdAt": f"{p.stat().st_mtime_ns:020d}"} for p in items]}
        if op == "deleteCheckpoint":
            import shutil

            shutil.rmtree(self.root.parent / "checkpoints" / args["id"])
            return {"deleted": True}
        if op == "checkpoint":
            import shutil

            target = self.root.parent / "checkpoints" / args["name"]
            shutil.rmtree(target, ignore_errors=True)
            shutil.copytree(self.root / "work", target, symlinks=True)
            return {"name": args["name"]}
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
            cwd = self._map(args.get("cwd") or "/fake-vm-work")
            if cwd == "/":
                cwd = str(self.root)
            import os

            env = {k: self._map(v) for k, v in (args.get("env") or {}).items()}
            proc = await asyncio.create_subprocess_shell(self._map(args["command"]), cwd=cwd, env={**os.environ, **env, "HOME": str(self.root), "PLAYWRIGHT_BROWSERS_PATH": BROWSERS},
                                                         stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
            out, err = await asyncio.wait_for(proc.communicate(), timeout=args.get("timeoutSec", 60))
            return {"exitCode": proc.returncode, "stdout": out.decode(), "stderr": err.decode(), "timedOut": False,
                    "truncated": False}
        if op == "spawn":
            import os

            proc = await asyncio.create_subprocess_shell(self._map(args["command"]), cwd=self._map(args.get("cwd") or "/fake-vm-work"),
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

    class Unreachable:  # the app lives on the VM's loopback; the control plane must never call it directly
        def __init__(self, *a, **k):
            raise AssertionError("control plane tried to reach the VM app over its own network")

    monkeypatch.setattr(qa.httpx, "AsyncClient", Unreachable)

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


def test_prepared_environment_keeps_dependencies_and_replaces_the_checkout(tmp_path, monkeypatch):
    from aifactory.vm import environment_key

    bridge = FakeBridge(tmp_path / "vm")
    monkeypatch.setattr("aifactory.vm.VMBridge.get", classmethod(lambda cls: bridge))

    def checkout(name, files):
        local = LocalSandbox(name, root=tmp_path / "local").create()
        local.work.mkdir()
        for rel, text in files.items():
            (local.work / rel).parent.mkdir(parents=True, exist_ok=True)
            (local.work / rel).write_text(text)
        return local

    async def go():
        first = checkout("a", {"package-lock.json": "{lock-1}", "old.txt": "old", "src/app.js": "v1"})
        key = environment_key(first.work, "acme/app", ["npm ci"])
        vm = await RailwayVMSandbox.from_local(first, prepared=key)
        assert vm.prepared is None  # nothing saved yet: built from the template
        await vm.exec("mkdir -p node_modules/dep && echo installed > node_modules/dep/index.js")
        await vm.checkpoint(key)
        await vm.close()

        second = checkout("b", {"package-lock.json": "{lock-1}", "src/app.js": "v2", "new.txt": "new"})
        assert environment_key(second.work, "acme/app", ["npm ci"]) == key
        third = checkout("c", {"package-lock.json": "{lock-2}"})
        assert environment_key(third.work, "acme/app", ["npm ci"]) != key  # lockfile changed

        vm = await RailwayVMSandbox.from_local(second, prepared=key)
        assert vm.prepared == key
        assert (await vm.exec("cat node_modules/dep/index.js; cat src/app.js; echo; cat new.txt")).output.split() == ["installed", "v2", "new"]
        assert not (await vm.exec("test -e old.txt")).ok  # files from the earlier checkout are gone
        await vm.close()

    asyncio.run(go())


def test_prepared_environments_are_pruned_per_repository(tmp_path, monkeypatch):
    import time as _time

    from aifactory.vm import prune_environments

    bridge = FakeBridge(tmp_path / "vm")
    monkeypatch.setattr("aifactory.vm.VMBridge.get", classmethod(lambda cls: bridge))
    base = tmp_path / "checkpoints"
    for name in ["aif-env-aaaaaaaa-0000000000000001", "aif-env-bbbbbbbb-0000000000000009",
                 "aif-env-aaaaaaaa-0000000000000002", "aif-env-0123456789abcdef01234567",
                 "aif-env-aaaaaaaa-0000000000000003", "unrelated"]:
        (base / name).mkdir(parents=True)
        _time.sleep(0.01)
    deleted = asyncio.run(prune_environments("aif-env-aaaaaaaa-0000000000000003"))
    assert sorted(deleted) == ["aif-env-0123456789abcdef01234567", "aif-env-aaaaaaaa-0000000000000001"]
    assert sorted(p.name for p in base.iterdir()) == ["aif-env-aaaaaaaa-0000000000000002",
                                                       "aif-env-aaaaaaaa-0000000000000003",
                                                       "aif-env-bbbbbbbb-0000000000000009", "unrelated"]


def test_vm_attachment_originals_and_safe_writes(tmp_path, monkeypatch):
    import stat

    from aifactory.pipeline.attachments import prepare_attachments
    from aifactory.sandbox import SandboxError, write_bytes

    from .test_attachments import seed_references

    rows = seed_references()
    bridge = FakeBridge(tmp_path / "attachment-vm")
    monkeypatch.setattr("aifactory.vm.VMBridge.get", classmethod(lambda cls: bridge))

    async def go():
        vm = await RailwayVMSandbox.create()
        context, images = await prepare_attachments(vm, "ws", "out")
        assert images and "Outcome attachments" in context
        for aid, name, raw, mime in rows:
            path = f".factory/attachments/{aid}-{name}"
            assert await vm.read_bytes_async(path) == raw
            assert (await vm.exec(f"base64 -w0 {path}")).output.strip() == base64.b64encode(raw).decode()
            assert f"MIME: {mime}; decoded size: {len(raw)} bytes; path: {path}" in context
            await write_bytes(vm, path, b"changed")
        assert (await prepare_attachments(vm, "ws", "out"))[0] == context
        file = bridge.root / "work/run.sh"
        file.write_text("#!/bin/sh\necho old\n")
        file.chmod(0o755)
        await write_text(vm, "run.sh", "#!/bin/sh\necho new\n")
        assert stat.S_IMODE(file.stat().st_mode) == 0o755
        assert (await vm.exec("./run.sh")).output.strip() == "new"
        # Both intermediate and leaf symlinks are refused inside the VM.
        outside = bridge.root / "outside"
        outside.mkdir()
        for target in ("escape", "escape-dir"):
            (bridge.root / "work" / target).symlink_to(outside, target_is_directory=True)
        for path in ("escape", "escape-dir/leak.txt", "../outside/leak.txt"):
            with pytest.raises(SandboxError):
                await write_bytes(vm, path, b"secret")
        assert list(outside.iterdir()) == []
        assert "write" in bridge.calls and "exec" in bridge.calls
        await vm.close()

    asyncio.run(go())
