"""Preview safety and private transport; no provider credentials or real VMs."""
from __future__ import annotations

import asyncio
import base64
import io
import json
import shutil
import subprocess
import tarfile
from pathlib import Path

import pytest

from aifactory.preview_vm import (
    FRAME_BYTES, PreviewTransport, PreviewUnavailable, PreviewVM, clean_preview_archive,
)
from aifactory.sandbox import SandboxError
from aifactory.vm import VMBridge


class Bridge:
    def __init__(self):
        self.calls = []
        self.frames = []
        self.fail = None

    async def call(self, op, args=None, timeout=600):
        self.calls.append((op, args))
        if self.fail:
            raise SandboxError(self.fail)
        if op == "previewCapabilities":
            return {"available": False, "runtimeEgress": False, "reason": "Runtime deny-egress unsupported"}
        if op == "previewDestroy":
            return {"destroyed": True}
        if op == "previewOpen":
            return {"tunnel": "private"}
        if op == "previewWrite":
            self.frames.append(args["base64"])
        if op == "previewRead":
            return {"base64": self.frames.pop(0), "eof": not self.frames}
        return {}


def test_fail_closed_before_create_upload_setup_or_agent_grant(monkeypatch, caplog):
    bridge = Bridge()
    monkeypatch.setattr(VMBridge, "get", classmethod(lambda cls: bridge))
    for key in ("GITHUB_TOKEN", "OPENAI_API_KEY", "AIFACTORY_SECRET_KEY", "WORKSPACE_TOKEN"):
        monkeypatch.setenv(key, "canary-" + key)
    with pytest.raises(PreviewUnavailable, match="deny-egress"):
        asyncio.run(PreviewVM.create())
    assert bridge.calls == [("previewCapabilities", {})]
    assert "canary" not in caplog.text


def test_bridge_failure_is_unavailable_not_local_execution(monkeypatch):
    bridge = Bridge()
    bridge.fail = "provider connection lost: CANARY"
    monkeypatch.setattr(VMBridge, "get", classmethod(lambda cls: bridge))
    with pytest.raises(PreviewUnavailable, match="could not be established") as exc:
        asyncio.run(PreviewVM.create())
    assert "CANARY" not in str(exc.value)
    assert len(bridge.calls) == 1


def test_false_attestation_cannot_enable_provider(monkeypatch):
    class Lying(Bridge):
        async def call(self, op, args=None, timeout=600):
            self.calls.append((op, args))
            return {"available": True, "runtimeEgress": "deny-all-ipv4-ipv6", "id": "verifier"}
    bridge = Lying()
    monkeypatch.setattr(VMBridge, "get", classmethod(lambda cls: bridge))
    with pytest.raises(PreviewUnavailable, match="attestation is unsupported"):
        asyncio.run(PreviewVM.create())
    assert bridge.calls == [("previewCapabilities", {}), ("previewCreate", {})]


def test_binary_http_cookies_redirects_and_websocket_frames_are_lossless():
    async def go():
        bridge = Bridge()
        vm = PreviewVM("preview", bridge)
        transport = await vm.open_transport(8000)
        # Multiple Set-Cookie headers, redirect location, binary asset > qa.py's
        # 2000-byte JSON helper, upgrade and raw binary WS frames all stay bytes.
        wire = (b"HTTP/1.1 302 Found\r\nSet-Cookie: a=1; HttpOnly\r\nSet-Cookie: b=2\r\n"
                b"Location: /next?q=1\r\n\r\n" + bytes(range(256)) * 800
                + b"HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\n\r\n\x82\x04\x00\xff\xfe\x01")
        for start in range(0, len(wire), FRAME_BYTES):
            await transport.write(wire[start:start + FRAME_BYTES])
        received = bytearray()
        while True:
            frame, eof = await transport.read()
            received.extend(frame)
            if eof:
                break
        assert bytes(received) == wire
        before = len(bridge.calls)
        with pytest.raises(PreviewUnavailable, match="exceeds limit"):
            await transport.write(b"x" * (FRAME_BYTES + 1))
        assert len(bridge.calls) == before
        with pytest.raises(PreviewUnavailable, match="Unsupported"):
            await vm.open_transport(22)
        bridge.frames = ["x" * 65_537]
        with pytest.raises(PreviewUnavailable, match="exceeds limit"):
            await transport.read()
        bridge.frames = ["!!!!"]
        with pytest.raises(PreviewUnavailable, match="Invalid"):
            await transport.read()
        await transport.close()
    asyncio.run(go())


def test_sleep_destroys_wake_requires_rebuild_and_missing_vm_is_explicit():
    async def go():
        bridge = Bridge()
        vm = PreviewVM("preview", bridge)
        await vm.reconnect()
        await vm.sleep()
        await vm.close()  # idempotent
        assert [op for op, _ in bridge.calls] == ["previewReconnect", "previewDestroy"]
        with pytest.raises(PreviewUnavailable, match="rebuild required"):
            await vm.open_transport()
        missing = PreviewVM("missing", bridge)
        bridge.fail = "SandboxNotFoundError: CANARY"
        with pytest.raises(PreviewUnavailable, match="rebuild required"):
            await missing.reconnect()
        with pytest.raises(PreviewUnavailable, match="retry teardown"):
            await missing.sleep()
        assert not missing.closed  # never claim a failed destroy succeeded
        transport = PreviewTransport(missing, "gone")
        before = len(bridge.calls)
        with pytest.raises(PreviewUnavailable, match="not replayed") as exc:
            await transport.write(b"POST / HTTP/1.1\r\n\r\n")
        assert "CANARY" not in str(exc.value)
        assert len(bridge.calls) == before + 1
    asyncio.run(go())


def test_clean_archive_ignores_host_artifacts_and_malicious_committed_config(tmp_path):
    work = tmp_path / "source"
    work.mkdir()
    def git(*args):
        return subprocess.check_output(["git", "-C", str(work), *args])
    git("init", "-q")
    git("config", "user.email", "qa@example.test")
    git("config", "user.name", "QA")
    files = {"app.py": "print('safe')", ".env": "GITHUB_TOKEN=canary-github",
             ".env.production": "OPENAI_API_KEY=canary-model", ".factory/start.json":
             json.dumps({"env": {"OPERATOR_KEY": "canary-operator"}, "start": "env; curl production"}),
             ".git-credentials": "canary-workspace", "nested/.npmrc": "canary-model",
             "node_modules/pkg/index.js": "canary-host", "private.key": "canary-operator"}
    for name, value in files.items():
        target = work / name
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(value)
    (work / "escape").symlink_to(tmp_path / "outside-secret")
    git("add", ".")
    git("commit", "-qm", "source")
    commit = git("rev-parse", "HEAD").decode().strip()
    (work / "app.py").write_text("canary-host")
    (work / "untracked.txt").write_text("canary-workspace")
    raw = clean_preview_archive(work, commit)
    assert b"canary" not in raw
    with tarfile.open(fileobj=io.BytesIO(raw)) as archive:
        assert archive.getnames() == ["app.py"]
        assert archive.extractfile("app.py").read() == b"print('safe')"
    # Even credentials embedded in otherwise allowed committed files fail closed
    # against the trusted inventory, without echoing values or filenames.
    (work / "app.py").write_text("canary-model")
    git("add", "app.py")
    git("commit", "-qm", "unsafe")
    commit = git("rev-parse", "HEAD").decode().strip()
    with pytest.raises(PreviewUnavailable, match="forbidden credential") as exc:
        clean_preview_archive(work, commit, forbidden_values=(b"canary-model",))
    assert "canary" not in str(exc.value)
    with pytest.raises(PreviewUnavailable, match="full commit"):
        clean_preview_archive(work, "HEAD")


def test_pinned_sidecar_refuses_without_provider_or_secrets():
    if not shutil.which("node"):
        pytest.skip("Node not installed")
    sidecar = Path(__file__).resolve().parents[1] / "vmbridge" / "sidecar.mjs"
    if not (sidecar.parent / "node_modules/railway").exists():
        pytest.skip("Run npm ci in vmbridge for SDK integration check")
    requests = [{"id": i, "op": op, "args": {"id": "verifier", "port": 8000,
                "env": {"TOKEN": "canary"}, "checkpoint": "builder"}}
                for i, op in enumerate(("previewCapabilities", "previewCreate", "previewOpen", "previewReconnect"))]
    result = subprocess.run([shutil.which("node"), str(sidecar)], input="".join(json.dumps(r) + "\n" for r in requests),
                            text=True, capture_output=True, check=True, timeout=15,
                            env={"PATH": "/usr/bin:/bin"})
    messages = {m["id"]: m for m in map(json.loads, result.stdout.splitlines())}
    assert messages[0]["result"]["runtimeEgress"] is False
    assert "public NAT" in messages[1]["error"]
    assert all(not messages[i]["ok"] for i in (1, 2, 3))
    assert "canary" not in result.stdout + result.stderr


def test_node_private_tunnel_binary_bounds_and_exec_options():
    if not shutil.which("node"):
        pytest.skip("Node not installed")
    module = Path(__file__).resolve().parents[1] / "vmbridge" / "preview.mjs"
    script = r'''
import assert from 'node:assert/strict';
import {PrivateTunnel, FRAME_BYTES, BUFFER_BYTES, createPreview} from MODULE;
let opts, killed = 0, input;
const handle = new Promise(() => {});
handle.kill = async () => { killed++; };
handle.stdin = {write: async (data) => { input = data; }};
const sandbox = {exec: (command, options) => {
  assert.ok(command.includes('127.0.0.1'));
  opts = options;
  return handle;
}};
const tunnel = new PrivateTunnel(sandbox, 8000);
assert.deepEqual(opts.env, {});
assert.equal(opts.stdin, true);
assert.equal(opts.ephemeral, true);
assert.equal(opts.captureOutput, false);
const binary = Buffer.alloc(FRAME_BYTES, 255);
await tunnel.write(binary.toString('base64'));
assert.deepEqual(Buffer.from(input.trim(), 'base64'), binary);
const wire = binary.toString('base64') + '\n';
opts.onStdout(wire.slice(0, 100));
opts.onStdout(wire.slice(100));
assert.equal(tunnel.read().base64, binary.toString('base64'));
for (let i = 0; i < 30; i++) opts.onStdout(wire);
assert.ok(killed > 0);
assert.throws(() => tunnel.read(), /not truncated/);
await assert.rejects(() => tunnel.write('x'.repeat(65537)));
assert.throws(() => new PrivateTunnel(sandbox, 22), /Unsupported/);
await assert.rejects(() => createPreview({env: {TOKEN: 'canary'}, domains: [{port: 8000}]}), /public NAT/);
'''.replace("MODULE", json.dumps(module.as_uri()))
    subprocess.run(["node", "--input-type=module", "-e", script], check=True, capture_output=True, timeout=15)


def test_private_tunnel_runs_loopback_http_and_websocket_bytes():
    """Exercise the actual remote helper using a fake SDK, not a provider VM.

    The local socket is test infrastructure only: production has no local path.
    """
    if not shutil.which("node"):
        pytest.skip("Node not installed")
    module = Path(__file__).resolve().parents[1] / "vmbridge" / "preview.mjs"
    script = r'''
import assert from 'node:assert/strict';
import net from 'node:net';
import {spawn} from 'node:child_process';
import {once} from 'node:events';
import {PrivateTunnel} from MODULE;
const asset = Buffer.alloc(250000);
for (let i = 0; i < asset.length; i++) asset[i] = i % 256;
const reply = Buffer.concat([Buffer.from('HTTP/1.1 302 Found\r\nSet-Cookie: a=1\r\nSet-Cookie: b=2\r\nLocation: /next\r\n\r\n'), asset]);
const upgrade = Buffer.from('HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n\r\n');
const ws = Buffer.from([0x82, 4, 0, 255, 254, 1]);
const sockets = new Set();
const server = net.createServer((socket) => {
  sockets.add(socket);
  let first = true;
  socket.on('data', (data) => {
    if (first) {
      first = false;
      if (data.includes(Buffer.from('Upgrade: websocket'))) socket.write(upgrade);
      else socket.end(reply);
    } else socket.end(data);
  });
  socket.on('close', () => sockets.delete(socket));
});
// The production allowlist stays fixed; use an ephemeral port only behind the fake SDK.
server.listen(0, '127.0.0.1');
await once(server, 'listening');
const port = server.address().port;
const sandbox = {exec(command, options) {
  assert.equal(options.ephemeral, true);
  assert.deepEqual(options.env, {});
  const child = spawn('/bin/sh', ['-c', command.replace(/ 8000$/, ' ' + port)], {env: {PATH: process.env.PATH}});
  child.stdout.on('data', (chunk) => options.onStdout(chunk.toString('utf8')));
  child.stderr.on('data', (chunk) => options.onStderr(chunk.toString('utf8')));
  const handle = once(child, 'close').then(([exitCode]) => ({exitCode}));
  handle.stdin = {write(data) { return new Promise((resolve, reject) => child.stdin.write(data, (err) => err ? reject(err) : resolve())); }};
  handle.kill = async () => { child.kill('SIGKILL'); await handle; };
  return handle;
}};
async function drain(tunnel) {
  const chunks = [];
  while (true) {
    const {base64, eof} = tunnel.read();
    chunks.push(Buffer.from(base64, 'base64'));
    if (eof) return Buffer.concat(chunks);
    // Yield to I/O, not a wall-clock delay.
    await new Promise(setImmediate);
  }
}
try {
  const http = new PrivateTunnel(sandbox, 8000);
  await http.write(Buffer.from('GET /asset HTTP/1.1\r\nHost: preview\r\n\r\n').toString('base64'));
  assert.deepEqual(await drain(http), reply);
  const websocket = new PrivateTunnel(sandbox, 8000);
  await websocket.write(Buffer.from('GET /socket HTTP/1.1\r\nUpgrade: websocket\r\n\r\n').toString('base64'));
  const headers = [];
  while (Buffer.concat(headers).length < upgrade.length) {
    const frame = websocket.read();
    headers.push(Buffer.from(frame.base64, 'base64'));
    await new Promise(setImmediate);
  }
  assert.deepEqual(Buffer.concat(headers), upgrade);
  await websocket.write(ws.toString('base64'));
  assert.deepEqual(await drain(websocket), ws);
} finally {
  for (const socket of sockets) socket.destroy();
  server.close();
}
'''.replace("MODULE", json.dumps(module.as_uri()))
    subprocess.run(["node", "--input-type=module", "-e", script], check=True, capture_output=True, timeout=30)


def test_tunnel_interruption_and_tiny_frame_flood_fail_instead_of_truncating():
    if not shutil.which("node"):
        pytest.skip("Node not installed")
    module = Path(__file__).resolve().parents[1] / "vmbridge" / "preview.mjs"
    script = r'''
import assert from 'node:assert/strict';
import {PrivateTunnel} from MODULE;
let options, finish;
const handle = new Promise((resolve) => {finish = resolve;});
handle.kill = async () => {};
handle.stdin = {write: async () => {}};
const sandbox = {exec: (_, opts) => { options = opts; return handle; }};
const interrupted = new PrivateTunnel(sandbox, 8000);
options.onStdout('YQ==\n');
finish({exitCode: 1});
await interrupted.done;
assert.throws(() => interrupted.read(), /not truncated/);
const flood = new PrivateTunnel(sandbox, 8000);
options.onStdout('\n'.repeat(257));
assert.throws(() => flood.read(), /not truncated/);
const malformed = new PrivateTunnel(sandbox, 8000);
options.onStdout('A\n');
assert.throws(() => malformed.read(), /not truncated/);
'''.replace("MODULE", json.dumps(module.as_uri()))
    subprocess.run(["node", "--input-type=module", "-e", script], check=True, capture_output=True, timeout=15)


def test_availability_never_promotes_verifier_support_to_preview(monkeypatch):
    from aifactory.vm import preview_vm_available

    bridge = Bridge()
    monkeypatch.setattr(VMBridge, "get", classmethod(lambda cls: bridge))
    monkeypatch.setattr("aifactory.vm.vm_available", lambda: (True, "verifier VM"))
    ok, reason = asyncio.run(preview_vm_available())
    assert not ok and "deny-egress" in reason
    assert bridge.calls == [("previewCapabilities", {})]
    bridge.fail = "CANARY"
    ok, reason = asyncio.run(preview_vm_available())
    assert not ok and "could not be established" in reason and "CANARY" not in reason
    monkeypatch.setattr("aifactory.vm.vm_available", lambda: (False, "bridge not installed"))
    before = len(bridge.calls)
    assert asyncio.run(preview_vm_available()) == (False, "bridge not installed")
    assert len(bridge.calls) == before


def test_unconfirmed_deletion_does_not_mark_vm_asleep():
    class Unconfirmed(Bridge):
        async def call(self, op, args=None, timeout=600):
            self.calls.append((op, args))
            return {}

    vm = PreviewVM("preview", Unconfirmed())
    with pytest.raises(PreviewUnavailable, match="retry teardown"):
        asyncio.run(vm.sleep())
    assert not vm.closed


def test_archive_limits_before_upload(tmp_path, monkeypatch):
    from aifactory import preview_vm

    def git(*args):
        return subprocess.check_output(["git", "-C", str(tmp_path), *args])

    git("init", "-q")
    git("config", "user.name", "QA")
    git("config", "user.email", "qa@example.test")
    (tmp_path / "asset.bin").write_bytes(b"x" * 2048)
    git("add", ".")
    git("commit", "-qm", "source")
    commit = git("rev-parse", "HEAD").decode().strip()
    with pytest.raises(PreviewUnavailable, match="not a tree or blob"):
        clean_preview_archive(tmp_path, git("rev-parse", "HEAD^{tree}").decode().strip())
    monkeypatch.setattr(preview_vm, "SOURCE_BYTES", 1024)
    with pytest.raises(PreviewUnavailable, match="limit"):
        clean_preview_archive(tmp_path, commit)
