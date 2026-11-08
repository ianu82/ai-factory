"""Runtime QA: boot the candidate app in the verifier's sandbox and collect evidence that it works.

Configured per repository as ``config["qa"]``::

    {"start": "npm run build && npx next start -p $PORT",   # long-running; must listen on $PORT
     "env": {"NEXT_PUBLIC_FLAG": "1"},                       # non-secret environment for the app
     "ready_path": "/login", "ready_timeout": 300,
     "login": {"path": "/auth/dev", "json": {"email": "qa@example.com"}},   # optional session for screenshots
     "pages": ["/login", "/signup"],                         # screenshotted by the Factory itself
     "probes": [{"method": "GET", "path": "/api/health", "expect_status": 200}]}

Screenshots run inside the sandbox as the job identity (headless Chromium), never in the control
plane. The verifier gets the app URL and a helper to capture its own screenshots; every image and
probe result is stored as evidence.
"""

from __future__ import annotations

import asyncio
import base64
import json
import shlex
import socket
import sys
import time
from dataclasses import dataclass, field
from typing import Any

import httpx

from ..sandbox import SCRATCH, LocalSandbox, list_files, read_bytes, write_text

SHOT_SCRIPT = r'''
import asyncio, json, sys
from playwright.async_api import async_playwright

cfg = json.load(open(".factory/qa/config.json"))
if len(sys.argv) >= 3:
    pages = [{"path": sys.argv[1], "name": sys.argv[2]}]
    by = "verifier"
else:
    pages = json.loads(sys.argv[1]) if len(sys.argv) == 2 else cfg.get("pages", [])
    by = "factory"

async def main():
    results = []
    async with async_playwright() as p:
        browser = await p.chromium.launch(args=["--no-sandbox", "--disable-dev-shm-usage"])
        ctx = await browser.new_context(viewport={"width": 1280, "height": 800})
        login = cfg.get("login")
        if login:
            try:
                await ctx.request.post(cfg["base"] + login["path"], data=login.get("json") or {})
            except Exception as exc:
                results.append({"login_error": str(exc)[:300]})
        for page in pages:
            pg = await ctx.new_page()
            errors = []
            pg.on("console", lambda m: errors.append(m.text[:300]) if m.type == "error" else None)
            pg.on("pageerror", lambda e: errors.append(str(e)[:300]))
            row = {"path": page["path"], "name": page["name"], "by": by}
            try:
                resp = await pg.goto(cfg["base"] + page["path"], wait_until="networkidle", timeout=45000)
                await pg.wait_for_timeout(600)
                await pg.screenshot(path=f".factory/shots/{page['name']}.png")
                row.update(status=resp.status if resp else None, title=await pg.title(), url=pg.url)
            except Exception as exc:
                row["error"] = str(exc)[:400]
            row["console_errors"] = errors[:10]
            results.append(row)
            await pg.close()
        await browser.close()
    print(json.dumps(results))

asyncio.run(main())
'''


# Requests from inside the sandbox: in a VM the app is only reachable on the VM's own loopback.
HTTP_SCRIPT = r'''
import json, sys, urllib.error, urllib.request

req = json.loads(sys.argv[1])
data = json.dumps(req["json"]).encode() if req.get("json") is not None else None


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *args, **kwargs):
        return None


request = urllib.request.Request(req["url"], data=data, method=req.get("method", "GET"),
                                 headers={"Content-Type": "application/json"} if data else {})
try:
    response = urllib.request.build_opener(NoRedirect).open(request, timeout=req.get("timeout", 10))
    status, body = response.status, response.read(2000)
except urllib.error.HTTPError as exc:
    status, body = exc.code, exc.read(2000)
except Exception as exc:
    print(json.dumps({"error": type(exc).__name__ + ": " + str(exc)[:200]}))
    sys.exit(0)
print(json.dumps({"status": status, "body": body.decode("utf-8", "replace")}))
'''


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@dataclass
class AppRun:
    sandbox: LocalSandbox
    cfg: dict[str, Any]
    port: int = 0
    proc: Any = None
    started_at: float = 0.0
    evidence: dict[str, Any] = field(default_factory=dict)

    @property
    def base(self) -> str:
        return f"http://127.0.0.1:{self.port}"

    async def start(self) -> bool:
        self.port = _free_port()
        env = {k: str(v) for k, v in (self.cfg.get("env") or {}).items()}
        env.update({"PORT": str(self.port), "HOST": "127.0.0.1", "NODE_ENV": env.get("NODE_ENV", "production")})
        await write_text(self.sandbox, f"{SCRATCH}/qa/shot.py", SHOT_SCRIPT)
        await write_text(self.sandbox, f"{SCRATCH}/qa/qa_request.py", HTTP_SCRIPT)
        await write_text(self.sandbox, f"{SCRATCH}/qa/config.json", json.dumps({
            "base": self.base, "login": self.cfg.get("login"),
            "pages": [{"path": p, "name": _name(p)} for p in self.cfg.get("pages") or []]}))
        await self.sandbox.exec(f"mkdir -p {SCRATCH}/shots", timeout=30)
        self.started_at = time.monotonic()
        self.proc = await self.sandbox.spawn(self.cfg["start"], env=env, log=f"{SCRATCH}/app.log")
        ready = await self._wait_ready()
        self.evidence = {"started": ready, "port": self.port, "start": self.cfg["start"],
                         "ready_seconds": round(time.monotonic() - self.started_at, 1)}
        if getattr(self.sandbox, "kind", "local") == "vm":
            self.evidence["isolation"] = f"railway-vm {self.sandbox.vm_id}" + (f" ({self.sandbox.region})" if self.sandbox.region else "")
        else:
            self.evidence["isolation"] = "sandbox (separate identity in the Factory container)"
        if not ready:
            self.evidence["log_tail"] = await self.log_tail()
        return ready

    @property
    def _remote(self) -> bool:
        return getattr(self.sandbox, "kind", "local") == "vm"

    async def request(self, method: str, path: str, json_body: Any = None, timeout: float = 10.0) -> dict[str, Any]:
        """One HTTP request to the app, from wherever the app is reachable. Returns {status, body} or {error}."""
        if self._remote:
            payload = {"url": self.base + path, "method": method, "json": json_body, "timeout": timeout}
            result = await self.sandbox.exec(f"python3 {SCRATCH}/qa/qa_request.py {_shell_json(payload)}", timeout=timeout + 20)
            try:
                return json.loads(result.output.strip().splitlines()[-1])
            except (json.JSONDecodeError, IndexError):
                return {"error": f"request helper failed: {result.output[-200:]}"}
        try:
            async with httpx.AsyncClient(timeout=timeout, follow_redirects=False) as client:
                response = await client.request(method, self.base + path, json=json_body)
                return {"status": response.status_code, "body": response.text[:2000]}
        except httpx.HTTPError as exc:
            return {"error": type(exc).__name__}

    async def _wait_ready(self) -> bool:
        deadline = time.monotonic() + float(self.cfg.get("ready_timeout", 300))
        path = self.cfg.get("ready_path", "/")
        while time.monotonic() < deadline:
            if self.proc.returncode is not None:
                return False
            status = (await self.request("GET", path)).get("status")
            if status is not None and status < 500:
                return True
            await asyncio.sleep(2)
        return False

    async def log_tail(self, limit: int = 4000) -> str:
        try:
            return (await read_bytes(self.sandbox, f"{SCRATCH}/app.log", limit, True)).decode("utf-8", "replace")
        except Exception:
            return ""

    async def probes(self) -> list[dict[str, Any]]:
        results = []
        for probe in self.cfg.get("probes") or []:
            row = {"method": probe.get("method", "GET"), "path": probe["path"]}
            answer = await self.request(row["method"], probe["path"], probe.get("json"), timeout=20.0)
            if "status" in answer:
                row.update(status=answer["status"], body=answer.get("body", "")[:500])
                expected = probe.get("expect_status")
                row["ok"] = answer["status"] == expected if expected else answer["status"] < 500
            else:
                row.update(ok=False, error=answer.get("error", "no response"))
            results.append(row)
        return results

    async def screenshots(self, pages: list[str] | None = None) -> list[dict[str, Any]]:
        arg = "" if pages is None else " " + _shell_json([{"path": p, "name": _name(p)} for p in pages])
        result = await self.sandbox.exec(f"{shot_command(self.sandbox)}{arg}", timeout=240)
        try:
            return json.loads(result.output.strip().splitlines()[-1])
        except (json.JSONDecodeError, IndexError):
            return [{"path": "(screenshot helper)", "error": f"screenshot helper failed: {result.output[-600:]}"}]

    async def collect_images(self) -> list[tuple[str, str]]:
        """All PNGs saved under .factory/shots (Factory's and the verifier's), base64-encoded."""
        images = []
        for name in (await list_files(self.sandbox, f"{SCRATCH}/shots", "*.png"))[:24]:
            try:
                data = await read_bytes(self.sandbox, f"{SCRATCH}/shots/{name}")
            except Exception:
                continue
            if len(data) <= 3_000_000:
                images.append((name.removesuffix(".png"), base64.b64encode(data).decode()))
        return images

    def stop(self) -> None:
        if self.proc is not None:
            self.sandbox.stop(self.proc)


def shot_command(sandbox=None) -> str:
    """The helper runs as the sandbox identity with an interpreter that has Playwright."""
    if getattr(sandbox, "kind", "local") == "vm":
        return f"python3 {SCRATCH}/qa/shot.py"
    return f"{shlex.quote(sys.executable)} {SCRATCH}/qa/shot.py"


def _name(path: str) -> str:
    cleaned = "".join(c if c.isalnum() else "-" for c in path.strip("/")) or "home"
    return cleaned[:60]


def _shell_json(value: Any) -> str:
    return shlex.quote(json.dumps(value))


def verifier_brief(run: AppRun) -> str:
    if not run.evidence.get("started"):
        return ("\n\n## Runtime QA\nThe Factory tried to start the app (`" + run.cfg["start"] + "`) but it did not become "
                "ready. Last log lines:\n```\n" + run.evidence.get("log_tail", "")[-2000:] + "\n```\nTreat this as "
                "evidence about the candidate.")
    where = "an isolated VM" if getattr(run.sandbox, "kind", "local") == "vm" else "your sandbox"
    return (f"\n\n## The app is running for you\nThe candidate is running in {where} at {run.base} (started with "
            f"`{run.cfg['start']}`, ready after {run.evidence['ready_seconds']}s). Exercise the changed behaviour "
            f"against the running app: use `curl` for HTTP/API calls, and `{shot_command(run.sandbox)} <path> <name>` "
            f"to save a screenshot of a page as evidence. Cite what you observed in your criterion evidence. The app "
            f"log is at {SCRATCH}/app.log.")
