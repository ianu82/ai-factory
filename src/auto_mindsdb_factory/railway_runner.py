"""One ephemeral VM per source export; credentials stay in the controller.

Setup and execution share their generated environment, while builder, verifier
and smoke exports use fresh VMs. Only one VM is held at a time. Lost controller
processes stop heartbeating and Railway expires their workers after five minutes.
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
import threading
from pathlib import Path
from uuid import uuid4

from .docker_runner import RunnerError
from .product_delivery import ProductDelivery
from .source_transfer import pack_source, write_source


def bridge(request):
    environment = {name: os.environ[name] for name in
        ("PATH", "RAILWAY_API_TOKEN", "RAILWAY_ENVIRONMENT_ID") if name in os.environ}
    if not all(environment.get(name) for name in ("RAILWAY_API_TOKEN", "RAILWAY_ENVIRONMENT_ID")):
        raise RunnerError("Railway project token and environment ID are required")
    timeout = request.get("timeout", 150) + 60
    try:
        result = subprocess.run(["node", str(Path(__file__).with_name("railway_bridge") / "bridge.mjs")],
            input=json.dumps(request), capture_output=True, text=True, env=environment, timeout=timeout)
        response = json.loads(result.stdout)
        if result.returncode or not isinstance(response, dict) or "error" in response:
            raise RunnerError("Railway operation failed")
        return response
    except (OSError, ValueError, subprocess.TimeoutExpired):
        raise RunnerError("Railway result unavailable") from None


class RailwayPool:
    def __init__(self, transport=bridge):
        self.transport = transport
        self.workspace = self.id = None
        self.lock = threading.RLock()
        self.stopped = threading.Event()
        self.failure = None
        self.thread = threading.Thread(target=self._heartbeat, daemon=True)
        self.thread.start()

    def _heartbeat(self):
        while not self.stopped.wait(60):
            with self.lock:
                if self.id:
                    try:
                        self.transport({"action": "heartbeat", "id": self.id, "timeout": 30})
                    except Exception:
                        self.failure = RunnerError("Railway worker heartbeat failed")

    def _discard(self):
        if self.id:
            self.transport({"action": "destroy", "id": self.id, "timeout": 30})
            self.id = self.workspace = None
            self.failure = None

    def open(self, workspace):
        with self.lock:
            if self.failure:
                raise self.failure
            if self.workspace != workspace:
                self._discard()
                self.id = self.transport({"action": "create", "timeout": 300})["id"]
                self.workspace = workspace
                self.call({"action": "upload", "source": pack_source(workspace)}, timeout=120)

    def call(self, request, *, timeout=120):
        with self.lock:
            if self.failure:
                raise self.failure
            if not self.id:
                raise RunnerError("Railway worker is unavailable")
            try:
                response = self.transport({"action": "request", "id": self.id,
                                           "request": request, "timeout": timeout})
                if "error" in response:
                    raise RunnerError("Railway Docker operation failed")
                return response
            except BaseException:
                # A lost docker-exec transport cannot prove the candidate stopped.
                # Retire its whole VM and prevent further use of this source export.
                try:
                    self._discard()
                finally:
                    self.failure = RunnerError("Railway execution outcome is uncertain")
                raise

    def __call__(self, workspace, image, *, writable=False, network=False, timeout=300, platform=None):
        return RailwaySandbox(self, workspace, image, writable=writable,
                              network=network, timeout=timeout, platform=platform)

    def close(self):
        self.stopped.set()
        # Do not join while holding the lock a heartbeat may be waiting for.
        self.thread.join(timeout=95)
        with self.lock:
            try:
                self._discard()
            except Exception:
                # Never rewrite a verified merge as failure due to cleanup. With
                # heartbeats stopped, the provider's finite idle TTL remains active.
                print("Railway worker cleanup failed; five-minute idle expiry remains active", file=sys.stderr)


class RailwaySandbox:
    def __init__(self, pool, workspace, image, **options):
        self.pool, self.workspace = pool, Path(workspace).resolve()
        self.options = {"name": "ai-factory-" + uuid4().hex, "image": image, **options}
        self.timeout = options["timeout"]

    def _call(self, action, **fields):
        request = {"action": action, "sandbox": {**self.options, "timeout": self.timeout}, **fields}
        return self.pool.call(request, timeout=self.timeout + 45 if action == "run" else 150)

    def __enter__(self):
        self.pool.open(self.workspace)
        self._call("enter")
        return self

    def run(self, command, *, stdin=None):
        return self._call("run", command=command, stdin=stdin)

    def __exit__(self, *exc):
        # Stop candidate processes before reading a stable source snapshot.
        self._call("exit")
        if exc[0] is None:
            response = self._call("download")
            write_source(self.workspace, response["source"])


class RailwayDelivery(ProductDelivery):
    def run(self, *args, **kwargs):
        pool = RailwayPool()
        self.sandbox_factory = pool
        try:
            return super().run(*args, **kwargs)
        finally:
            pool.close()
