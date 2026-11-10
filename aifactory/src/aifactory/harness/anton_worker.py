"""Private scratchpad process. Stdlib only; never imported to execute cells by the host.

The RPC is deliberately JSON, not a Python object/namespace serialization protocol.
All code, package installation and namespace state stay under the sandbox identity.
"""
from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
import sys
import threading
import time
import traceback
import uuid

HEAD = 6_000
TAIL = 24_000
MAX_REQUEST = 1_000_000
# Encoded byte limit of every published response; the controller refuses more.
RPC_LIMIT = 600_000


class Capture:
    """Drain an OS pipe continuously, retaining only a head and rolling tail.

    Captures native writes and child output too, without unbounded spool files.
    A fence makes completed-cell output deterministic without waiting for children
    that inherited the pipe to exit.
    """

    def __init__(self, fd):
        read, self.write = os.pipe()
        os.dup2(self.write, fd)
        self.lock = threading.Lock()
        self.head = b""
        self.tail = b""
        self.total = 0
        self.marker = b"\x00" + uuid.uuid4().hex.encode() + b"\x00"
        self.done = threading.Event()
        threading.Thread(target=self.drain, args=(read,), daemon=True).start()

    def append(self, data):
        with self.lock:
            self.total += len(data)
            left = HEAD - len(self.head)
            self.head += data[:left]
            self.tail = (self.tail + data[left:])[-TAIL:]

    def drain(self, read):
        pending = b""
        while True:
            block = os.read(read, 65536)
            if not block:
                return
            pending += block
            if self.marker in pending:
                before, pending = pending.split(self.marker, 1)
                self.append(before)
                self.done.set()
            # Keep only a possible partial fence across reads.
            keep = len(self.marker) - 1
            if len(pending) > keep:
                self.append(pending[:-keep])
                pending = pending[-keep:]

    def finish(self):
        os.write(self.write, self.marker)
        if not self.done.wait(5):
            raise RuntimeError("Output capture failed")
        self.done.clear()
        with self.lock:
            omitted = self.total - len(self.head) - len(self.tail)
            note = f"\n… [{omitted:,} bytes omitted] …\n".encode() if omitted > 0 else b""
            data = self.head + note + self.tail
            self.head = self.tail = b""
            self.total = 0
        return data.decode("utf-8", "replace")


def _utf8(value):
    # Lone surrogates (e.g. from exception text) cannot be UTF-8 encoded.
    return value.encode("utf-8", "replace").decode("utf-8")


def _size(value):
    return len(json.dumps(value, ensure_ascii=False).encode("utf-8"))


def _clip_encoded(value, budget):
    """Keep a head and tail of ``value`` whose *encoded* JSON fits ``budget`` bytes."""
    if _size(value) <= budget:
        return value
    low, high = 0, len(value)
    best = "… [output omitted] …"
    while low <= high:
        keep = (low + high) // 2
        head = keep // 5
        candidate = (value[:head] + f"\n… [{len(value) - keep:,} characters omitted] …\n"
                     + value[len(value) - (keep - head):])
        if _size(candidate) <= budget:
            best, low = candidate, keep + 1
        else:
            high = keep - 1
    return best


def encode_response(data, limit=RPC_LIMIT):
    """Serialize a response bounded by encoded bytes, never by per-field length.

    Clipping the streams/error keeps a healthy namespace alive: an oversized
    response would otherwise be refused by the controller as terminal.
    """
    data = {key: _utf8(value) if isinstance(value, str) else value for key, value in data.items()}
    raw = json.dumps(data, ensure_ascii=False).encode("utf-8")
    if len(raw) <= limit:
        return raw
    fields = [key for key in ("stdout", "stderr", "error") if isinstance(data.get(key), str)]
    overhead = _size({key: ("" if key in fields else value) for key, value in data.items()})
    budget = max(0, (limit - overhead - 256) // max(1, len(fields)))
    for key in fields:
        data[key] = _clip_encoded(data[key], budget)
    raw = json.dumps(data, ensure_ascii=False).encode("utf-8")
    if len(raw) > limit:  # Defensive: unreachable with the budgets above.
        raise RuntimeError("Scratchpad response could not be bounded")
    return raw


def publish(path, data):
    temporary = path.with_suffix(".tmp")
    temporary.write_bytes(encode_response(data))
    temporary.replace(path)


def serve(root):
    stdout, stderr = Capture(1), Capture(2)
    namespace = {"__name__": "__main__"}
    publish(root / "boot.json", {"pid": os.getpid(), "uid": os.getuid()})
    while True:
        request = root / "request.json"
        if not request.exists():
            time.sleep(0.02)
            continue
        with request.open("rb") as handle:
            raw = handle.read(MAX_REQUEST + 1)
        request.unlink()
        if len(raw) > MAX_REQUEST:
            raise RuntimeError("Oversized RPC request")
        body = json.loads(raw)
        error, stopped_at = None, None
        if body["action"] == "install":
            result = subprocess.run([sys.executable, "-m", "pip", "install", "--", *body["packages"]])
            if result.returncode:
                error = f"pip exited with status {result.returncode}"
        else:
            compiled = None
            try:
                compiled = compile(body["code"], "<scratchpad>", "exec")
                exec(compiled, namespace, namespace)
            except BaseException as exc:
                tb = exc.__traceback__
                while tb:
                    if tb.tb_frame.f_code is compiled:
                        stopped_at = tb.tb_lineno
                        break
                    tb = tb.tb_next
                error = "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))
        sys.stdout.flush()
        sys.stderr.flush()
        publish(root / "result.json", {"stdout": stdout.finish(), "stderr": stderr.finish(),
                                       "error": error, "stopped_at": stopped_at})


def main():
    mode, uid, directory, vm = sys.argv[1:]
    root = Path(directory).absolute()
    if mode == "launch":
        if vm == "1":
            # VM exec/spawn run as root. No agent code runs before dropping all
            # privileges. Numeric UIDs are private reservations, not login users.
            for path in (root, *root.iterdir()):
                os.chown(path, int(uid), int(uid))
            os.setgroups([])
            os.setgid(int(uid))
            os.setuid(int(uid))
            home = root / "home"
            home.mkdir(exist_ok=True)
            os.environ["HOME"] = str(home)
        if os.getuid() != int(uid) or os.getuid() == 0:
            raise RuntimeError("Scratchpad requires a private unprivileged UID")
        subprocess.run([sys.executable, "-m", "venv", str(root / "venv")], check=True,
                       stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        python = str(root / "venv/bin/python")
        os.execv(python, [python, str(root / "worker.py"), "serve", uid, str(root), vm])
    serve(root)


if __name__ == "__main__":
    main()
