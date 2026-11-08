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

from ..sandbox import SCRATCH, LocalSandbox

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
        self.sandbox.write_text(f"{SCRATCH}/qa/shot.py", SHOT_SCRIPT)
        self.sandbox.write_text(f"{SCRATCH}/qa/config.json", json.dumps({
            "base": self.base, "login": self.cfg.get("login"),
            "pages": [{"path": p, "name": _name(p)} for p in self.cfg.get("pages") or []]}))
        await self.sandbox.exec(f"mkdir -p {SCRATCH}/shots", timeout=30)
        self.started_at = time.monotonic()
        self.proc = await self.sandbox.spawn(self.cfg["start"], env=env, log=f"{SCRATCH}/app.log")
        ready = await self._wait_ready()
        self.evidence = {"started": ready, "port": self.port, "start": self.cfg["start"],
                         "ready_seconds": round(time.monotonic() - self.started_at, 1)}
        if not ready:
            self.evidence["log_tail"] = self.log_tail()
        return ready

    async def _wait_ready(self) -> bool:
        deadline = time.monotonic() + float(self.cfg.get("ready_timeout", 300))
        path = self.cfg.get("ready_path", "/")
        async with httpx.AsyncClient(timeout=10.0, follow_redirects=False) as client:
            while time.monotonic() < deadline:
                if self.proc.returncode is not None:
                    return False
                try:
                    response = await client.get(self.base + path)
                    if response.status_code < 500:
                        return True
                except httpx.HTTPError:
                    pass
                await asyncio.sleep(2)
        return False

    def log_tail(self, limit: int = 4000) -> str:
        try:
            return self.sandbox.resolve(f"{SCRATCH}/app.log").read_bytes()[-limit:].decode("utf-8", "replace")
        except OSError:
            return ""

    async def probes(self) -> list[dict[str, Any]]:
        results = []
        async with httpx.AsyncClient(timeout=20.0, follow_redirects=False) as client:
            for probe in self.cfg.get("probes") or []:
                row = {"method": probe.get("method", "GET"), "path": probe["path"]}
                try:
                    response = await client.request(row["method"], self.base + probe["path"], json=probe.get("json"))
                    row.update(status=response.status_code, body=response.text[:500])
                    expected = probe.get("expect_status")
                    row["ok"] = response.status_code == expected if expected else response.status_code < 500
                except httpx.HTTPError as exc:
                    row.update(ok=False, error=type(exc).__name__)
                results.append(row)
        return results

    async def screenshots(self, pages: list[str] | None = None) -> list[dict[str, Any]]:
        arg = "" if pages is None else " " + _shell_json([{"path": p, "name": _name(p)} for p in pages])
        result = await self.sandbox.exec(f"{shot_command()}{arg}", timeout=240)
        try:
            return json.loads(result.output.strip().splitlines()[-1])
        except (json.JSONDecodeError, IndexError):
            return [{"path": "(screenshot helper)", "error": f"screenshot helper failed: {result.output[-600:]}"}]

    def collect_images(self) -> list[tuple[str, str]]:
        """All PNGs saved under .factory/shots (Factory's and the verifier's), base64-encoded."""
        images = []
        try:
            folder = self.sandbox.resolve(f"{SCRATCH}/shots")
            for path in sorted(folder.glob("*.png"))[:24]:
                data = path.read_bytes()
                if len(data) <= 3_000_000:
                    images.append((path.stem, base64.b64encode(data).decode()))
        except OSError:
            pass
        return images

    def stop(self) -> None:
        if self.proc is not None:
            self.sandbox.stop(self.proc)


def shot_command() -> str:
    """The helper runs with the Factory's interpreter (which has Playwright), as the sandbox identity."""
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
    return (f"\n\n## The app is running for you\nThe candidate is running at {run.base} (started with "
            f"`{run.cfg['start']}`, ready after {run.evidence['ready_seconds']}s). Exercise the changed behaviour "
            f"against the running app: use `curl` for HTTP/API calls, and `{shot_command()} <path> <name>` "
            f"to save a screenshot of a page as evidence. Cite what you observed in your criterion evidence. The app "
            f"log is at {SCRATCH}/app.log.")
