"""One ephemeral VM per source export; credentials stay in the controller.

Setup and execution share their generated environment, while builder, verifier
and smoke exports use fresh VMs. Only one VM is held at a time. Lost controller
processes stop heartbeating and Railway expires their workers after five minutes.
"""
from __future__ import annotations

import json
import math
import os
import secrets
import subprocess
import sys
import threading
import time
from pathlib import Path
from uuid import uuid4
from weakref import WeakKeyDictionary

from .docker_runner import RunnerError
from .product_delivery import ProductDelivery
from .source_transfer import pack_source, write_source
from .product_cancellation import run_cancellable


# Keep constructor facts outside mutable exception state without retaining its
# traceback after callers release it. This is controller-local provenance.
_original_failure_facts = WeakKeyDictionary()
_exec_categories = {"ExecNotStartedError", "ExecInterruptedError", "ExecControlUnsupportedError"}
_exec_counters = ("connections", "opened", "errors", "closed", "http_rejections",
    "capabilities_frames", "typed_error_frames", "hello_send_attempts", "init_send_attempts",
    "init_send_returned", "unclassified_control_sends")
_exec_fields = _exec_counters + ("last_http_status", "last_close_code", "counters_saturated", "observation_failed")
_progress_stages = {"dispatch", "connect", "connected", "exec", "returned", "failed",
    "create_allocating", "create_files", "create_bootstrap", "create_ready",
    *(f"create_{phase}_{state}" for phase in ("files", "bootstrap", "ready")
      for state in ("destroying", "destroy_returned"))}


def _freeze_exec_observations(value):
    if (type(value) is not dict or any(type(key) is not str for key in value)
            or set(value) != set(_exec_fields)):
        return None
    if any(type(value[key]) is not int or not 0 <= value[key] <= 65535 for key in _exec_counters):
        return None
    for key, low, high in (("last_http_status", 100, 599), ("last_close_code", 1000, 4999)):
        if type(value[key]) is not int or not (value[key] == 0 or low <= value[key] <= high):
            return None
    if any(type(value[key]) is not bool for key in ("counters_saturated", "observation_failed")):
        return None
    return tuple(value[key] for key in _exec_fields)


def _bridge_progress(stderr, nonce):
    # Never retain raw stderr, the nonce, or an incomplete/foreign frame.
    if type(stderr) is bytes and len(stderr) <= 65536:
        try:
            stderr = stderr.decode('utf-8')
        except UnicodeDecodeError:
            return None
    if type(stderr) is not str or len(stderr) > 65536 or type(nonce) is not str:
        return None
    latest = None
    try:
        for line in stderr.splitlines(keepends=True):
            if not line.startswith('FACTORY_BRIDGE_PROGRESS ') or not line.endswith('\n'):
                continue
            frame = json.loads(line[len('FACTORY_BRIDGE_PROGRESS '):])
            if (type(frame) is not dict or set(frame) != {'nonce', 'sequence', 'stage', 'exec_observations'}
                    or frame['nonce'] != nonce or type(frame['sequence']) is not int
                    or not 1 <= frame['sequence'] <= 64 or (latest and frame['sequence'] <= latest[0])
                    or type(frame['stage']) is not str
                    or frame['stage'] not in _progress_stages):
                return None
            observations = _freeze_exec_observations(frame['exec_observations'])
            if observations is None:
                return None
            latest = (frame['sequence'], frame['stage'], observations)
    except Exception:
        return None
    return latest


def _freeze_bridge_progress(value):
    if (type(value) is not tuple or len(value) != 3 or type(value[0]) is not int
            or not 1 <= value[0] <= 64 or type(value[1]) is not str
            or value[1] not in _progress_stages
            or type(value[2]) is not tuple or len(value[2]) != len(_exec_fields)):
        return None
    observations = _freeze_exec_observations(dict(zip(_exec_fields, value[2])))
    return (value[0], value[1], observations) if observations is not None else None


class RailwayOperationError(RunnerError):
    def __init__(self, request, error_type, *, exec_observations=None, bridge_progress=None):
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
            observations = progress = None
            try:
                if self.error_type in _exec_categories:
                    observations = _freeze_exec_observations(exec_observations)
                progress = _freeze_bridge_progress(bridge_progress)
            except Exception:
                pass  # Diagnostic failure must preserve the original runner error.
            _original_failure_facts[self] = (self.phase, self.error_type, reason, observations, progress)

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


def railway_exec_observations(error):
    """Detached metadata from an intact constructor witness; None is unknown."""
    operation, category = railway_failure_facts(error)
    if operation == "unknown" or category not in _exec_categories:
        return None
    values = _original_failure_facts[error][3]
    return dict(zip(_exec_fields, values)) if values is not None else None


def railway_bridge_progress(error):
    if railway_failure_facts(error)[0] == 'unknown':
        return None
    value = _original_failure_facts[error][4]
    if value is None:
        return None
    return {'sequence': value[0], 'stage': value[1], 'partial': True,
        'exec_observations': dict(zip(_exec_fields, value[2]))}


def _bounded_bridge(command, *, input, env, timeout, deadline):
    # Node waits for the complete stdin request before dispatch. Recalculate
    # after spawn so a slow process launch cannot admit an expired operation.
    with subprocess.Popen(command, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
            stderr=subprocess.PIPE, text=True, env=env) as process:
        remaining = deadline - time.monotonic()
        try:
            if remaining <= 0:
                raise subprocess.TimeoutExpired(command, 0)
            stdout, stderr = process.communicate(input, timeout=min(timeout, remaining))
        except subprocess.TimeoutExpired as error:
            process.kill()
            _, error.stderr = process.communicate()
            raise
        except BaseException:
            process.kill()
            process.wait()
            raise  # Preserve the primary interruption after reaping the child.
        return subprocess.CompletedProcess(command, process.returncode, stdout, stderr)


def bridge(request):
    environment = {name: os.environ[name] for name in
        ("PATH", "RAILWAY_API_TOKEN", "RAILWAY_ENVIRONMENT_ID") if name in os.environ}
    if not all(environment.get(name) for name in ("RAILWAY_API_TOKEN", "RAILWAY_ENVIRONMENT_ID")):
        raise RunnerError("Railway project token and environment ID are required")
    timeout = request.get("timeout", 150) + 60
    request = dict(request)
    deadline = request.pop("deadline", None)
    nonce = None
    try:
        nonce = request['progress_nonce'] = secrets.token_hex(16)
        command = ["node", str(Path(__file__).with_name("railway_bridge") / "bridge.mjs")]
        if deadline is None:
            result = subprocess.run(command, input=json.dumps(request), capture_output=True,
                text=True, env=environment, timeout=timeout)
        else:
            _execution_budget(deadline, timeout)  # Validate before allocating a bridge process.
            result = _bounded_bridge(command, input=json.dumps(request), env=environment,
                timeout=timeout, deadline=deadline)
    except (OSError, subprocess.TimeoutExpired) as error:
        raise RailwayOperationError(request, type(error).__name__,
            bridge_progress=_bridge_progress(getattr(error, 'stderr', None), nonce)) from None
    try:
        response = json.loads(result.stdout)
    except ValueError:
        raise RailwayOperationError(request, "InvalidJSON") from None
    if not isinstance(response, dict):
        raise RailwayOperationError(request, "InvalidResponse")
    if result.returncode or "error" in response:
        raise RailwayOperationError(request, response.get("error", "BridgeProcessError"),
            exec_observations=response.get("exec_observations"),
            bridge_progress=_bridge_progress(getattr(result, 'stderr', None), nonce))
    return response


def _execution_budget(deadline, timeout):
    if deadline is None:
        return {}
    if type(deadline) not in (int, float) or not math.isfinite(deadline):
        raise RunnerError("Invalid Railway execution deadline")
    milliseconds = int(min(deadline - time.monotonic(), timeout + 30) * 1000)
    if milliseconds < 1:
        raise RunnerError("Railway execution deadline expired")
    return {"budget_ms": milliseconds, "deadline": deadline}


class RailwayPool:
    def __init__(self, transport=bridge):
        self.transport = transport
        self.workspace = self.id = None
        self.lock = threading.RLock()
        self.stopped = threading.Event()
        self.failure = None
        self._aborting = None
        self._journal = None
        self.thread = threading.Thread(target=self._heartbeat, daemon=True)
        self.thread.start()

    def _record_failure(self, error):
        if self._journal is None:
            return
        try:
            operation, category = railway_failure_facts(error)
            progress = railway_bridge_progress(error)
            self._journal.append("runner_transport_failed", operation=operation, category=category,
                exec_observations=railway_exec_observations(error),
                **({'bridge_progress': progress} if progress is not None else {}))
        except BaseException:
            # A primary transport failure is already in flight. Even an evidence
            # write interruption cannot replace it or prevent owned retirement.
            pass

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
                        failure = error if isinstance(error, RailwayOperationError) else RailwayOperationError(
                            {"action": "heartbeat"}, type(error).__name__)
                        self.failure = failure
                        self._record_failure(failure)

    def _discard(self):
        if self.id:
            if self.id == self._aborting:
                return  # An independent cancellation already owns this retirement.
            self.transport({"action": "destroy", "id": self.id, "timeout": 30})
            self.id = self.workspace = None
            self.failure = None

    def warm(self, *, timeout=300):
        if type(timeout) is not int or not 1 <= timeout <= 300:
            raise RunnerError("Invalid worker preparation timeout")
        with self.lock:
            if self.stopped.is_set():
                raise RunnerError("Railway worker pool is closed")
            if self.failure:
                raise self.failure
            if self.id is None:
                try:
                    self.id = self.transport({"action": "create", "timeout": timeout})["id"]
                except BaseException as error:
                    self._record_failure(error)
                    raise
                if self.stopped.is_set():
                    self._discard()
                    raise RunnerError("Railway worker creation was cancelled")
            return self.id

    def open(self, workspace, *, deadline=None):
        with self.lock:
            if self.stopped.is_set():
                raise RunnerError("Railway worker pool is closed")
            if self.failure:
                raise self.failure
            _execution_budget(deadline, 120)  # No late allocation after waiting for the lock.
            if self.workspace != workspace:
                if self.workspace is not None:
                    self._discard()
                _execution_budget(deadline, 120)  # Retirement may have consumed the remaining budget.
                self.warm()
                self.workspace = workspace
                self.call({"action": "upload", "source": pack_source(workspace)}, timeout=120, deadline=deadline)

    def call(self, request, *, timeout=120, deadline=None):
        with self.lock:
            if self.stopped.is_set():
                raise RunnerError("Railway worker pool is closed")
            if self.failure:
                raise self.failure
            if not self.id:
                raise RunnerError("Railway worker is unavailable")
            try:
                response = self.transport({"action": "request", "id": self.id,
                    "request": request, "timeout": timeout, **_execution_budget(deadline, timeout)})
                if "error" in response:
                    raise RailwayOperationError({"request": request}, response["error"])
                return response
            except BaseException as error:
                # Retain bounded facts before cleanup/cancellation replaces the
                # runner error. An observation is never execution acknowledgement.
                self._record_failure(error)
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

    def abort(self):
        """Retire our VM without waiting for a command's lifecycle lock."""
        self.stopped.set()
        identity = self.id
        self._aborting = identity
        try:
            if identity:
                response = self.transport({"action": "destroy", "id": identity, "timeout": 30})
                if "error" in response:
                    raise RailwayOperationError({"action": "destroy"}, response["error"])
                if self.id == identity:
                    self.id = self.workspace = None
        finally:
            self._aborting = None
            self.failure = RunnerError("Railway execution was cancelled")


class RailwaySandbox:
    def __init__(self, pool, workspace, image, **options):
        self.pool, self.workspace = pool, Path(workspace).resolve()
        self.options = {"name": "ai-factory-" + uuid4().hex, "image": image, **options}
        self.timeout = options["timeout"]
        self.deadline = None

    def set_deadline(self, deadline):
        if type(deadline) not in (int, float) or not math.isfinite(deadline):
            raise RunnerError("Invalid Railway execution deadline")
        self.deadline = deadline

    def _call(self, action, **fields):
        request = {"action": action, "sandbox": {**self.options, "timeout": self.timeout}, **fields}
        limits = {} if self.deadline is None else {"deadline": self.deadline}
        return self.pool.call(request, timeout=self.timeout + 45 if action == "run" else 150, **limits)

    def __enter__(self):
        limits = {} if self.deadline is None else {"deadline": self.deadline}
        self.pool.open(self.workspace, **limits)
        self._call("enter")
        return self

    def run(self, command, *, stdin=None):
        return self._call("run", command=command, stdin=stdin)

    def abort(self):
        self.pool.abort()

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


class _WorkerPreparation:
    def __init__(self, pool, timeout):
        self.pool, self.timeout = pool, timeout

    def run(self, command):
        return self.pool.warm(timeout=self.timeout)

    def abort(self):
        self.pool.abort()


class RailwayDelivery(ProductDelivery):
    def _prepare(self, sha, workspace, journal, **kwargs):
        self._railway_pool._journal = journal
        return super()._prepare(sha, workspace, journal, **kwargs)

    def prepare_runner(self, journal):
        self.check_cancelled()
        remaining = self.deadline - time.monotonic()
        if remaining < 1:
            raise RunnerError("Run time budget exhausted before worker preparation")
        pool = self._railway_pool
        pool._journal = journal
        if pool.id is not None:
            return
        journal.append("runner_preparation_started", budget="shared_run_deadline")
        started = time.monotonic()
        identity = run_cancellable(_WorkerPreparation(pool, min(300, int(remaining))), [],
            cancelled=self.cancelled, deadline=self.deadline)
        journal.append("runner_preparation_finished", worker_id=identity,
                       duration_seconds=round(time.monotonic() - started, 3))

    def _in_pool(self, operation, *args, **kwargs):
        if getattr(self, "_railway_pool", None) is not None:
            return operation(*args, **kwargs)
        pool = RailwayPool()
        previous = self.sandbox_factory
        self._railway_pool = pool
        self.sandbox_factory = pool
        try:
            return operation(*args, **kwargs)
        finally:
            try:
                pool.close()
            finally:
                self.sandbox_factory = previous
                self._railway_pool = None

    def run(self, *args, **kwargs):
        return self._in_pool(super().run, *args, **kwargs)

    def plan(self, *args, **kwargs):
        return self._in_pool(super().plan, *args, **kwargs)

    def verify(self, *args, **kwargs):
        return self._in_pool(super().verify, *args, **kwargs)

    def verify_fixture(self, *args, **kwargs):
        return self._in_pool(super().verify_fixture, *args, **kwargs)
