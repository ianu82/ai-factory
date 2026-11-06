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
from weakref import WeakKeyDictionary

from .docker_runner import RunnerError
from .product_delivery import ProductDelivery
from .source_transfer import pack_source, write_source


# Keep constructor facts outside mutable exception state without retaining its
# traceback after callers release it. This is controller-local provenance.
_original_failure_facts = WeakKeyDictionary()


class RailwayOperationError(RunnerError):
    def __init__(self, request, error_type):
        if type(self) is RailwayOperationError and self in _original_failure_facts:
            raise TypeError("Railway failure facts are already initialized")
        phase = None
        if type(request) is dict and all(type(key) is str for key in request):
            nested = request.get("request", {})
            if type(nested) is dict and all(type(key) is str for key in nested):
                phase = nested.get("action", request.get("action"))
        self.phase = phase if type(phase) is str and phase in {"create", "destroy", "heartbeat", "upload", "download", "enter", "run", "exit"} else "unknown"
        known = {"RailwayError", "RailwayAuthError", "RailwayGraphQLError", "RailwayConnectionError",
            "ExecNotStartedError", "ExecControlUnsupportedError", "ExecInterruptedError", "StaleEnvironmentError",
            "SandboxNotFoundError", "SandboxFailedError", "SandboxTemplateBuildError", "SandboxFilesError",
            "SandboxFileNotFoundError", "SandboxTimeoutError", "RunnerError", "WorkspaceError", "Error",
            "ValueError", "KeyError", "TypeError", "RuntimeError", "OSError", "PermissionError", "FileNotFoundError",
            "TimeoutExpired", "InvalidJSON", "InvalidResponse", "BridgeProcessError"}
        self.error_type = error_type if type(error_type) is str and error_type in known else "UnknownError"
        reason = f"Railway {self.phase} failed ({self.error_type})"
        super().__init__(reason)
        if type(self) is RailwayOperationError:
            _original_failure_facts[self] = (self.phase, self.error_type, reason)

    def public_reason(self):
        phase, category = railway_failure_facts(self)
        return f"Railway {phase} failed ({category})"


def railway_failure_facts(error):
    """Return original bounded facts only for an intact constructor-issued error.

    Mutable copies, args and public formatting cannot attest to each other.
    This does not provide a boundary against code executing in the controller.
    """
    unknown = ("unknown", "UnknownError")
    if type(error) is not RailwayOperationError:
        return unknown
    original = _original_failure_facts.get(error)
    if original is None:
        return unknown
    attrs, args = vars(error), error.args
    if type(attrs) is not dict or any(type(key) is not str for key in attrs):
        return unknown
    values = (attrs.get("phase"), attrs.get("error_type"))
    if (any(type(value) is not str for value in values) or type(args) is not tuple
            or len(args) != 1 or type(args[0]) is not str):
        return unknown
    if values != original[:2] or args != (original[2],):
        return unknown
    return original[:2]


def bridge(request):
    environment = {name: os.environ[name] for name in
        ("PATH", "RAILWAY_API_TOKEN", "RAILWAY_ENVIRONMENT_ID") if name in os.environ}
    if not all(environment.get(name) for name in ("RAILWAY_API_TOKEN", "RAILWAY_ENVIRONMENT_ID")):
        raise RunnerError("Railway project token and environment ID are required")
    timeout = request.get("timeout", 150) + 60
    try:
        result = subprocess.run(["node", str(Path(__file__).with_name("railway_bridge") / "bridge.mjs")],
            input=json.dumps(request), capture_output=True, text=True, env=environment, timeout=timeout)
    except (OSError, subprocess.TimeoutExpired) as error:
        raise RailwayOperationError(request, type(error).__name__) from None
    try:
        response = json.loads(result.stdout)
    except ValueError:
        raise RailwayOperationError(request, "InvalidJSON") from None
    if not isinstance(response, dict):
        raise RailwayOperationError(request, "InvalidResponse")
    if result.returncode or "error" in response:
        raise RailwayOperationError(request, response.get("error", "BridgeProcessError"))
    return response


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
            self._keep_alive()

    def _keep_alive(self):
        # Commands hold the lifecycle lock while awaiting remote execution.
        # Keepalives must run independently, including during a silent command.
        identity = self.id
        if identity:
            try:
                self.transport({"action": "heartbeat", "id": identity, "timeout": 30})
            except Exception as error:
                # Retirement can race an in-flight heartbeat. A failure for a
                # retired worker must never poison its replacement.
                with self.lock:
                    if self.id == identity:
                        self.failure = error if isinstance(error, RailwayOperationError) else RailwayOperationError(
                            {"action": "heartbeat"}, type(error).__name__)

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
                    raise RailwayOperationError({"request": request}, response["error"])
                return response
            except BaseException:
                # A lost docker-exec transport cannot prove the candidate stopped.
                # Retire its whole VM and prevent further use of this source export.
                try:
                    self._discard()
                except Exception:
                    print("Railway worker cleanup failed; five-minute idle expiry remains active", file=sys.stderr)
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
        if exc[0] is not None and self.pool.failure:
            # The failed pool owns VM cleanup. Preserve the original failure;
            # another request would only replace it with the uncertainty guard.
            return False
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
