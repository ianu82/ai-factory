"""Preview-specific VM boundary; never uses a verifier sandbox or local fallback.

Railway 3.16.0 cannot enforce runtime deny-egress. Creation is deliberately
unavailable until an adapter can establish that guarantee, before executing even
preparation commands from an untrusted checkout. See docs/PREVIEW_VM.md.
"""
from __future__ import annotations

import base64
import io
import subprocess
import tarfile
from pathlib import Path, PurePosixPath
from typing import Any

from .sandbox import SandboxError
from .vm import VMBridge

FRAME_BYTES = 49_152
APP_PORTS = (3000, 8000, 8080)
SOURCE_BYTES = 32 * 1024 * 1024
SOURCE_FILES = 10_000


class PreviewUnavailable(SandboxError):
    """No safe preview is possible; callers must show this rather than fall back."""


def _git(work: Path, *args: str, limit: int) -> bytes:
    # Read immutable Git objects, never host working-tree files or credential
    # helpers. No checkout filters, hooks, submodule fetches or network access.
    with subprocess.Popen(["git", "--no-replace-objects", "-C", str(work), *args],
                          stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
                          env={"PATH": "/usr/bin:/bin", "GIT_CONFIG_NOSYSTEM": "1",
                               "GIT_CONFIG_GLOBAL": "/dev/null", "GIT_NO_LAZY_FETCH": "1"}) as proc:
        assert proc.stdout
        data = proc.stdout.read(limit + 1)
        if len(data) > limit:
            proc.kill()
            raise PreviewUnavailable("Preview source exceeds the upload limit")
        if proc.wait() != 0:
            raise PreviewUnavailable("Preview requires a fresh committed Git source tree")
        return data


def clean_preview_archive(work: Path, commit: str, *, forbidden_values: tuple[bytes, ...] = ()) -> bytes:
    """Archive a pinned commit, not a builder/verifier workspace.

    Excludes dot paths (including committed env/start configs), dependency/build
    output, credential files, symlinks and submodules. Trusted QA configuration
    and fixtures are supplied separately, never discovered in this archive.
    ``forbidden_values`` comes from trusted credential inventory, not PR config.
    """
    if len(commit) != 40 or any(c not in "0123456789abcdef" for c in commit):
        raise PreviewUnavailable("Preview source must be pinned to a full commit SHA")
    if _git(work, "cat-file", "-t", commit, limit=16).strip() != b"commit":
        raise PreviewUnavailable("Preview source must be a commit, not a tree or blob")
    entries = _git(work, "ls-tree", "-rz", "--full-tree", commit, limit=2_000_000).split(b"\0")
    if len(entries) > SOURCE_FILES + 1:
        raise PreviewUnavailable("Preview source has too many files")
    out = io.BytesIO()
    total = 0
    skipped = {"node_modules", "venv", "dist", "build", "__pycache__", "coverage",
               "credentials", "credential-helper", "id_rsa", "id_ed25519", "tmp", "logs",
               "artifacts", "htmlcov", "site-packages", "target"}
    with tarfile.open(fileobj=out, mode="w") as archive:
        for entry in entries:
            if not entry:
                continue
            meta, name_bytes = entry.split(b"\t", 1)
            mode, kind, oid = meta.split()
            try:
                name = name_bytes.decode("utf-8", "strict")
            except UnicodeDecodeError:
                raise PreviewUnavailable("Preview source has an unsupported filename") from None
            if any(value and value in name_bytes for value in forbidden_values):
                raise PreviewUnavailable("Preview source contains a forbidden credential")
            path = PurePosixPath(name)
            if (path.is_absolute() or ".." in path.parts or len(name_bytes) > 1024
                    or any(p.startswith(".") or p.lower() in skipped for p in path.parts)
                    or path.name.lower().startswith(("env.", "credentials.", "credential-helper."))
                    or path.suffix.lower() in {".env", ".pem", ".key", ".p12", ".pfx", ".log"}):
                continue
            if kind != b"blob" or mode not in (b"100644", b"100755"):
                continue
            data = _git(work, "cat-file", "blob", oid.decode("ascii"), limit=SOURCE_BYTES - total)
            if any(value and value in data for value in forbidden_values):
                # Do not echo paths, matched content, config or command output.
                raise PreviewUnavailable("Preview source contains a forbidden credential")
            # Include tar padding/headers before serializing into the buffer.
            projected = out.tell() + 2048 + ((len(data) + 511) // 512) * 512 + 10_240
            if projected > SOURCE_BYTES:
                raise PreviewUnavailable("Preview archive exceeds the upload limit")
            total += len(data)
            info = tarfile.TarInfo(name)
            info.size = len(data)
            info.mode = 0o755 if mode == b"100755" else 0o644
            archive.addfile(info, io.BytesIO(data))
    if out.tell() > SOURCE_BYTES:
        raise PreviewUnavailable("Preview archive exceeds the upload limit")
    return out.getvalue()


class PreviewVM:
    """Only preview operations are permitted; no exec, agent grant or checkpoint API.

    Sleep is destroy; wake must call create with clean source again. An unknown
    VM/bridge restart is an error, not an implicit create or request replay.
    """

    def __init__(self, vm_id: str, bridge: VMBridge):
        self.vm_id = vm_id
        self.bridge = bridge
        self.closed = False

    @classmethod
    async def create(cls) -> "PreviewVM":
        bridge = VMBridge.get()
        try:
            caps = await bridge.call("previewCapabilities", {}, timeout=30)
        except (SandboxError, TimeoutError):
            raise PreviewUnavailable("Preview unavailable: private VM policy could not be established") from None
        if not isinstance(caps, dict):
            raise PreviewUnavailable("Preview unavailable: invalid provider capability response")
        if not caps.get("available") or caps.get("runtimeEgress") != "deny-all-ipv4-ipv6":
            raise PreviewUnavailable(caps.get("reason") or "Preview unavailable: runtime deny-egress is unsupported")
        # This pinned provider always refuses. Do not accept fake capability bits
        # as authority to upload source, execute PR setup, or mint an agent grant.
        try:
            await bridge.call("previewCreate", {}, timeout=900)
        except (SandboxError, TimeoutError):
            raise PreviewUnavailable("Preview unavailable: Railway cannot enforce runtime deny-egress; "
                                     "use a provider with a deny-all runtime policy") from None
        raise PreviewUnavailable("Preview unavailable: provider isolation attestation is unsupported")

    async def reconnect(self) -> None:
        self._open()
        try:
            await self.bridge.call("previewReconnect", {"id": self.vm_id}, timeout=30)
        except (SandboxError, TimeoutError):
            raise PreviewUnavailable("Preview VM missing or isolation unproven; rebuild required") from None

    def _open(self) -> None:
        if self.closed:
            raise PreviewUnavailable("Preview VM is stopped; rebuild required")

    async def open_transport(self, port: int = 8000) -> "PreviewTransport":
        self._open()
        if type(port) is not int or port not in APP_PORTS:
            raise PreviewUnavailable("Unsupported preview app port")
        try:
            result = await self.bridge.call("previewOpen", {"id": self.vm_id, "port": port}, timeout=30)
        except (SandboxError, TimeoutError):
            raise PreviewUnavailable("Preview private transport unavailable; rebuild required") from None
        if not isinstance(result, dict) or not isinstance(result.get("tunnel"), str) or not result["tunnel"]:
            raise PreviewUnavailable("Preview private transport unavailable; invalid bridge response")
        return PreviewTransport(self, result["tunnel"])

    async def close(self) -> None:
        if not self.closed:
            # Retain the id on failure so teardown can be retried, never report
            # asleep/stopped after an unconfirmed provider deletion.
            try:
                result = await self.bridge.call("previewDestroy", {"id": self.vm_id}, timeout=120)
            except (SandboxError, TimeoutError):
                raise PreviewUnavailable("Preview deletion unconfirmed; retry teardown") from None
            if not isinstance(result, dict) or result.get("destroyed") is not True:
                raise PreviewUnavailable("Preview deletion unconfirmed; retry teardown")
            self.closed = True

    async def sleep(self) -> None:
        await self.close()


class PreviewTransport:
    """Bounded raw duplex bytes, preserving HTTP and WebSocket framing verbatim.

    Consumers pull frames (empty/non-EOF means try again) and await writes for
    backpressure. No redirect following, cookie parsing, UTF-8 conversion,
    automatic retry or response clipping. Overflow fails the entire connection.
    """

    def __init__(self, vm: PreviewVM, token: str):
        self.vm = vm
        self.token = token

    async def _call(self, op: str, **args: Any) -> Any:
        self.vm._open()
        try:
            return await self.vm.bridge.call(op, {"id": self.vm.vm_id, "tunnel": self.token, **args}, timeout=60)
        except (SandboxError, TimeoutError):
            raise PreviewUnavailable("Preview transport lost; request was not replayed") from None

    async def write(self, data: bytes) -> None:
        if len(data) > FRAME_BYTES:
            raise PreviewUnavailable("Preview transport frame exceeds limit")
        await self._call("previewWrite", base64=base64.b64encode(data).decode("ascii"))

    async def read(self) -> tuple[bytes, bool]:
        result = await self._call("previewRead")
        if not isinstance(result, dict) or type(result.get("eof")) is not bool:
            raise PreviewUnavailable("Invalid preview transport frame")
        encoded = result.get("base64")
        if not isinstance(encoded, str) or len(encoded) > 65_536:
            raise PreviewUnavailable("Preview transport frame exceeds limit")
        try:
            data = base64.b64decode(encoded, validate=True)
        except ValueError:
            raise PreviewUnavailable("Invalid preview transport frame") from None
        if len(data) > FRAME_BYTES:
            raise PreviewUnavailable("Preview transport frame exceeds limit")
        return data, result["eof"]

    async def close(self) -> None:
        await self._call("previewClose")
