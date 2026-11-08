"""One owned inference process; secrets use only its anonymous input pipe."""

from .controller_imports import require_controller_import_path
require_controller_import_path()

import json
import os
from pathlib import Path
import selectors
import signal
import subprocess
import sys
import time

MAX_BYTES = 2 * 1024 * 1024


def _command():
    return [sys.executable, "-P", "-m", "auto_mindsdb_factory.mindshub_native_worker"]


def exchange(packet, *, deadline, cancelled):
    require_controller_import_path()
    root = Path(__file__).resolve().parents[1]
    names = ("HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "NO_PROXY", "SSL_CERT_FILE", "SSL_CERT_DIR",
             "http_proxy", "https_proxy", "all_proxy", "no_proxy")
    env = {name: os.environ[name] for name in names if name in os.environ}
    env.update(PATH=os.defpath, PYTHONPATH=str(root), PYTHONDONTWRITEBYTECODE="1")
    raw = json.dumps(packet, allow_nan=False).encode()
    if len(raw) > MAX_BYTES or cancelled() or time.monotonic() >= deadline:
        raise RuntimeError("Inference process admission refused")
    process = subprocess.Popen(_command(), stdin=subprocess.PIPE, stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL, cwd=root.parent, env=env, start_new_session=True, bufsize=0)
    spawn_late = time.monotonic() >= deadline
    state = {"ready": False, "observed": None, "payload": None, "retired": False}
    pending, buffer, total = memoryview(raw), bytearray(), 0
    sent, eof, failed, forced = False, False, False, False
    work_end = deadline - min(0.1, max(0, deadline - time.monotonic()) / 10)

    def read():
        nonlocal buffer, total, eof
        chunk = os.read(process.stdout.fileno(), 65536)
        if not chunk:
            eof = True
            return
        total += len(chunk)
        if total > MAX_BYTES * 2:
            raise ValueError("Inference output exceeds limit")
        buffer.extend(chunk)
        while b"\n" in buffer:
            line, _, rest = buffer.partition(b"\n")
            buffer = bytearray(rest)
            message = json.loads(line)
            if type(message) is not dict:
                raise ValueError("Invalid inference output")
            kind = message.get("kind")
            if kind == "ready" and not state["ready"]:
                state["ready"] = True
            elif kind == "observed" and state["ready"] and state["observed"] is None:
                state["observed"] = message
            elif kind == "payload" and state["observed"] is not None and state["payload"] is None:
                state["payload"] = message.get("payload")
            elif kind == "retired" and state["payload"] is not None:
                state["retired"] = True
            else:
                raise ValueError("Invalid inference sequence")
    try:
        os.set_blocking(process.stdin.fileno(), False)
        os.set_blocking(process.stdout.fileno(), False)
        with selectors.DefaultSelector() as selector:
            selector.register(process.stdout, selectors.EVENT_READ)
            while time.monotonic() < work_end and not cancelled():
                if state["ready"] and pending and not sent:
                    selector.register(process.stdin, selectors.EVENT_WRITE)
                    sent = True
                for key, event in selector.select(min(0.02, max(0, work_end - time.monotonic()))):
                    if time.monotonic() >= work_end or cancelled():
                        break
                    if key.fileobj is process.stdin:
                        pending = pending[os.write(process.stdin.fileno(), pending[:65536]):]
                        if not pending:
                            selector.unregister(process.stdin)
                            process.stdin.close()
                    else:
                        read()
                        if eof: selector.unregister(process.stdout)
                if eof and process.poll() is not None:
                    break
    except Exception:
        failed = True
    finally:
        retirement_started = time.monotonic()
        if process.poll() is None:
            forced = True
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            except OSError:
                failed = True
        try:
            # Retirement cannot extend inference's original absolute deadline.
            # If the OS cannot reap in budget, retain ownership as unconfirmed.
            process.wait(timeout=max(0, deadline - time.monotonic()))
        except (OSError, subprocess.TimeoutExpired):
            failed = True
        try:
            while not eof and time.monotonic() < deadline:
                try: read()
                except BlockingIOError: break
        except Exception:
            failed = True
        process.stdin.close()
        process.stdout.close()
    complete = (not failed and not forced and state["retired"] and process.returncode == 0
                and time.monotonic() < deadline and not cancelled())
    facts = {"pid": process.pid, "process_group": process.pid,
             "retirement_unconfirmed": process.returncode is None, "reaped": process.returncode is not None, "forced": forced,
             "packet_sent": sent and not pending, "spawn_late": spawn_late,
             "deadline_exceeded": time.monotonic() >= deadline,
             "retirement_ms": round((time.monotonic() - retirement_started) * 1000),
             "deadline_overrun_ms": max(0, round((time.monotonic() - deadline) * 1000))}
    return state["observed"], state["payload"], complete, facts
