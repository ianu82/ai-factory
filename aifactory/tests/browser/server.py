"""Private spawned server and pipe control. No test routes enter the product app."""
from __future__ import annotations

import asyncio
import multiprocessing
import os
from pathlib import Path
import socket
import threading
import time
import traceback

import httpx


def isolated_environment(data: Path, url: str) -> dict[str, str]:
    # Start from a small OS/runtime allowlist, not an ever-growing credential denylist.
    allowed = {"PATH", "HOME", "USERPROFILE", "SYSTEMROOT", "WINDIR", "TEMP", "TMP", "TMPDIR", "LANG", "LC_ALL"}
    env = {k: v for k, v in os.environ.items() if k in allowed or k.startswith("LC_")}
    env.update(AIFACTORY_DATA_DIR=str(data), DATABASE_URL=f"sqlite:///{data / 'factory.db'}",
               AIFACTORY_JOBS_DIR=str(data / "jobs"), AIFACTORY_PUBLIC_URL=url, AIFACTORY_INTERNAL_URL=url,
               AIFACTORY_SECRET_KEY="browser-only-dummy-secret", AIFACTORY_ALLOW_DEV_SIGN_IN="1",
               AIFACTORY_ALLOW_API_KEY_SIGN_IN="0", AIFACTORY_ORCHESTRATOR="0", AIFACTORY_SIGNAL_INTERVAL_SECONDS="0")
    return env


class StreamControl:
    """Track actual ASGI stream tasks; interruption exercises native EventSource recovery."""
    def __init__(self, app):
        self.app = app
        self.streams = {}
        self.connections = {}
        self.changed = asyncio.Event()

    async def __call__(self, scope, receive, send):
        path = scope.get("path", "")
        ws = path.split("/")[3] if path.startswith("/api/workspaces/") and path.endswith("/stream") else None
        task = asyncio.current_task()
        if ws:
            self.streams[task] = ws
            self.connections[ws] = self.connections.get(ws, 0) + 1
            self.changed.set()
        try:
            await self.app(scope, receive, send)
        finally:
            self.streams.pop(task, None)
            self.changed.set()


async def serve(sock, pipe, build, env):
    os.environ.clear()
    os.environ.update(env)
    import uvicorn
    from aifactory import app, events, pulls, api
    from aifactory.inference import ModelClient

    async def list_models(self):
        return ["gpt-test"]

    async def deny_network(self, request, *args, **kwargs):
        raise RuntimeError("External HTTP is forbidden in the browser test server")

    # Model catalogue is deterministic; any unhandled integration fails closed.
    ModelClient.list_models = list_models
    httpx.AsyncClient.send = deny_network
    from aifactory.db import session_scope
    from aifactory.models import Outcome, Workspace
    from tests.browser.seed import create_scenario
    from tests.test_pulls import FakeGitHub

    # Reuse the established PR fake without mutating helpers used by API tests.
    class BrowserGitHub(FakeGitHub):
        pass

    FakeGitHub.calls = []
    pulls.GitHub = BrowserGitHub
    api.GitHub = BrowserGitHub
    pulls.resolve_token = lambda ws: "browser-dummy"
    for cache in (pulls._cache, pulls._details, pulls._checks):
        cache.clear()
    app.WEB_DIST = Path(build)
    wrapped = StreamControl(app.create_app(start_orchestrator=False))
    workspaces = set()

    async def command(body):
        operation = body["op"]
        if operation == "create_workspace":
            result = await create_scenario()
            workspaces.add(result["workspace_id"])
            return result
        ws = body["workspace_id"]
        if ws not in workspaces:
            raise ValueError("Unknown browser workspace")
        if operation == "wait_stream":
            minimum = body.get("connections", 1)
            if not isinstance(minimum, int) or not 1 <= minimum <= 20:
                raise ValueError("connections must be 1..20")
            async with asyncio.timeout(10):
                while not (ws in wrapped.streams.values() and wrapped.connections.get(ws, 0) >= minimum):
                    wrapped.changed.clear()
                    await wrapped.changed.wait()
            return {"connections": wrapped.connections[ws]}
        if operation == "stream_state":
            return {"active": sum(w == ws for w in wrapped.streams.values()),
                    "connections": wrapped.connections.get(ws, 0)}
        if operation == "interrupt_stream":
            tasks = [t for t, w in wrapped.streams.items() if w == ws]
            for task in tasks:
                task.cancel()
            return {"interrupted": len(tasks)}
        if operation == "emit":
            # A bounded, real persisted mutation + normal server event, not an API mock.
            title = body.get("title", "Live browser outcome")
            if not isinstance(title, str) or not 1 <= len(title) <= 100:
                raise ValueError("Title must contain 1..100 characters")
            with session_scope() as s:
                workspace = s.get(Workspace, ws)
                workspace.outcome_seq += 1
                outcome = Outcome(workspace_id=ws, number=workspace.outcome_seq, title=title, status="planning")
                s.add(outcome)
                s.flush()
                outcome_id = outcome.id
            payload = events.record(ws, "outcome_created", title, outcome_id=outcome_id)
            return {"outcome_id": outcome_id, "event_id": payload["id"]}
        raise ValueError("Unsupported browser control operation")

    loop = asyncio.get_running_loop()

    def read_commands():
        try:
            while True:
                body = pipe.recv()
                try:
                    future = asyncio.run_coroutine_threadsafe(command(body), loop)
                    result = future.result(timeout=15)
                    pipe.send({"result": result})
                except Exception:
                    future.cancel()
                    pipe.send({"error": traceback.format_exc()})
        except (EOFError, OSError):
            pass

    threading.Thread(target=read_commands, daemon=True).start()
    server = uvicorn.Server(uvicorn.Config(wrapped, host="127.0.0.1", log_level="info", timeout_graceful_shutdown=2))
    await server.serve(sockets=[sock])


def server_main(sock, pipe, build, env, log_path):
    # Redirect Python and native logs, never inherited credentials (env is allowlisted).
    with open(log_path, "a", buffering=1) as log:
        os.dup2(log.fileno(), 1)
        os.dup2(log.fileno(), 2)
        try:
            asyncio.run(serve(sock, pipe, build, env))
        except BaseException:
            traceback.print_exc()
            raise


class BrowserServer:
    def __init__(self, build: Path, data: Path, log_path: Path, *, startup_timeout=30):
        self.log_path = log_path
        self.process = None
        self.pipe = None
        sock = socket.socket()
        sock.bind(("127.0.0.1", 0))
        sock.listen(128)
        self.url = f"http://127.0.0.1:{sock.getsockname()[1]}"
        context = multiprocessing.get_context("spawn")
        parent, child = context.Pipe()
        self.pipe = parent
        try:
            self.process = context.Process(target=server_main, args=(sock, child, str(build),
                                           isolated_environment(data, self.url), str(log_path)), daemon=True)
            self.process.start()
            child.close()
            sock.close()
            deadline = time.monotonic() + startup_timeout
            last_error = "Server has not responded"
            with httpx.Client(base_url=self.url, timeout=0.5, trust_env=False) as client:
                while time.monotonic() < deadline:
                    if not self.process.is_alive():
                        break
                    try:
                        response = client.get("/api/health")
                        if response.status_code == 200 and response.json().get("ok") is True:
                            spa = client.get("/")
                            if spa.status_code == 200 and "<html" in spa.text.lower():
                                return
                        last_error = f"Health HTTP {response.status_code}: {response.text[:300]}"
                    except (httpx.HTTPError, ValueError) as exc:
                        last_error = str(exc)
                    threading.Event().wait(0.05)  # bounded health polling, never browser readiness sleeps
            raise RuntimeError(f"Browser server did not become healthy: {last_error}")
        except BaseException as exc:
            self.close()
            logs = log_path.read_text()[-16000:] if log_path.exists() else "No startup log written"
            raise RuntimeError(f"{exc}\nStartup log: {log_path}\n{logs}") from exc
        finally:
            sock.close()
            child.close()

    def control(self, op: str, **payload):
        self.pipe.send({"op": op, **payload})
        if not self.pipe.poll(20):
            raise TimeoutError(f"Browser control {op} timed out; server log: {self.log_path}")
        response = self.pipe.recv()
        if "error" in response:
            raise RuntimeError(response["error"])
        return response["result"]

    def close(self):
        if self.pipe:
            self.pipe.close()
        if self.process and self.process.pid:
            self.process.terminate()
            self.process.join(timeout=5)
            if self.process.is_alive():
                self.process.kill()
                self.process.join(timeout=5)
            if self.process.is_alive():
                raise RuntimeError("Browser server could not be stopped")
            self.process.close()
            self.process = None
