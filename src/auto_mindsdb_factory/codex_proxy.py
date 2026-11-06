"""Scoped loopback transport; all provider work stays in the controller gate."""

import asyncio
import secrets
import time
from contextlib import asynccontextmanager
from dataclasses import dataclass, field

from .codex_http import CodexHTTPError, read_codex_http
from .codex_inference import CodexStopped


_ERROR = b'{"error":{"message":"Factory inference request stopped","type":"invalid_request_error"}}'


@dataclass(frozen=True)
class CodexProxy:
    base_url: str
    token: str = field(repr=False)


@asynccontextmanager
async def open_codex_proxy(gate, client):
    """Own the listener/tasks; the owning session creates and closes the client.

    The local capability never forwards to the provider. A fully accounted and
    validated response is buffered before any SSE bytes reach app-server. No
    backend selection, app-server startup or tool dispatch occurs here.
    """
    gate.admit()
    token, tasks, closing = secrets.token_urlsafe(32), set(), False

    async def exchange(reader, writer):
        inference, disconnect = None, None
        try:
            if closing: return
            body = await read_codex_http(reader, token=token, host=host,
                timeout=max(0, min(10, gate.deadline - time.monotonic())))
            inference = asyncio.create_task(gate.request(client, body))
            disconnect = asyncio.create_task(reader.read(1))
            done, _ = await asyncio.wait({inference, disconnect}, return_when=asyncio.FIRST_COMPLETED)
            if disconnect in done:
                gate.stop("request_policy" if disconnect.result() else "request_cancelled")
            payload = await inference
            gate.admit()
            status, content_type = "200 OK", "text/event-stream"
        except (CodexHTTPError, CodexStopped):
            payload, status, content_type = _ERROR, "400 Bad Request", "application/json"
        except OSError:
            return
        finally:
            owned = [task for task in (inference, disconnect) if task is not None]
            for task in owned:
                if not task.done() and not task.cancelling(): task.cancel()
            if owned:
                cleanup = asyncio.gather(*owned, return_exceptions=True)
                try:
                    await asyncio.shield(cleanup)
                except asyncio.CancelledError:
                    await cleanup
                    raise
        try:
            writer.write((f"HTTP/1.1 {status}\r\nContent-Type: {content_type}\r\n"
                          f"Content-Length: {len(payload)}\r\nConnection: close\r\n\r\n").encode() + payload)
            await writer.drain()
        except OSError:
            pass

    async def handle(reader, writer):
        try:
            await exchange(reader, writer)
        except Exception:
            try:
                gate.stop("unknown")
            except CodexStopped:
                pass
        finally:
            writer.close()
            try:
                await asyncio.wait_for(writer.wait_closed(), timeout=1)
            except (OSError, TimeoutError):
                pass

    def connected(reader, writer):
        if closing or len(tasks) >= 4:
            writer.close()
            return
        task = asyncio.create_task(handle(reader, writer))
        tasks.add(task)
        task.add_done_callback(tasks.discard)

    server = await asyncio.start_server(connected, "127.0.0.1", 0, limit=16384, backlog=4)
    host = "127.0.0.1:" + str(server.sockets[0].getsockname()[1])

    async def watch():
        while True:
            try:
                interrupted = gate.cancelled() or time.monotonic() >= gate.deadline
                reason = "cancelled_or_deadline"
            except Exception:
                interrupted = True
                reason = "evidence_unavailable"
            if gate.stopped or interrupted:
                if not gate.stopped:
                    try:
                        gate.stop(reason)
                    except CodexStopped:
                        pass
                server.close()
                for task in tuple(tasks):
                    if not task.cancelling(): task.cancel()
                return
            await asyncio.sleep(0.05)

    watcher = asyncio.create_task(watch())
    try:
        yield CodexProxy("http://" + host + "/v1", token)
    finally:
        closing = True
        server.close()
        watcher.cancel()
        await asyncio.gather(watcher, return_exceptions=True)
        pending = tuple(tasks)
        for task in pending:
            if not task.cancelling(): task.cancel()
        if pending: await asyncio.gather(*pending, return_exceptions=True)
        await server.wait_closed()
