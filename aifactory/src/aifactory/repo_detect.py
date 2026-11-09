"""Propose setup/check commands for a newly connected repository from its manifest files."""

from __future__ import annotations

import base64
import json
import re
from typing import Any

from .github import GitHub, GitHubError


async def _file(gh: GitHub, full_name: str, path: str, ref: str) -> str | None:
    try:
        data = await gh.request("GET", f"/repos/{full_name}/contents/{path}", params={"ref": ref}, retries=1)
    except GitHubError:
        return None
    if isinstance(data, dict) and data.get("encoding") == "base64":
        return base64.b64decode(data.get("content", "")).decode("utf-8", "replace")
    return None


async def detect(gh: GitHub, full_name: str, ref: str) -> dict[str, Any]:
    config: dict[str, Any] = {"setup": [], "checks": [], "smoke": [], "protected_paths": [], "context": ""}
    notes: list[str] = []
    config["start_detection"] = {"available": False, "reason": "No recognized web start script; enter a command that listens on $PORT."}
    package = await _file(gh, full_name, "package.json", ref)
    if package:
        try:
            pkg = json.loads(package)
        except json.JSONDecodeError:
            pkg = {}
        if not isinstance(pkg, dict):
            pkg = {}
        raw_scripts = pkg.get("scripts")
        scripts = {k: v for k, v in raw_scripts.items() if isinstance(v, str)} if isinstance(raw_scripts, dict) else {}
        manager = "npm"
        if await _file(gh, full_name, "pnpm-lock.yaml", ref):
            manager = "pnpm"
            config["setup"].append("corepack enable && pnpm install --frozen-lockfile")
        elif await _file(gh, full_name, "yarn.lock", ref):
            manager = "yarn"
            config["setup"].append("corepack enable && yarn install --frozen-lockfile")
        elif await _file(gh, full_name, "package-lock.json", ref):
            config["setup"].append("npm ci --no-audit --no-fund")
        else:
            config["setup"].append("npm install --no-audit --no-fund")
        run = {"npm": "npm run", "pnpm": "pnpm", "yarn": "yarn"}[manager]
        for name in ("typecheck", "type-check", "lint", "test"):
            # `next lint` was removed in Next.js 16; skip scripts that cannot run unattended.
            if name in scripts and "watch" not in scripts[name] and scripts[name].strip() != "next lint":
                config["checks"].append(f"{run} {name}" if name != "test" else f"{run} test")
        if "build" in scripts:
            config["smoke"].append(f"{run} build")
        if "test" in scripts:
            config["smoke"].append(f"{run} test")
        # Only suggest known web CLIs with documented host/port flags. A generic
        # `node`, library, or arbitrary script is not evidence of a runnable app.
        for name in ("dev", "start"):
            script = scripts.get(name, "").strip()
            if re.fullmatch(r"(?:vite|vite dev|vite preview|next dev|next start|astro dev|astro preview|nuxt dev|nuxt start)", script):
                flags = '-H 0.0.0.0 -p "$PORT"' if script.startswith("next ") else '--host 0.0.0.0 --port "$PORT"'
                separator = " --" if manager == "npm" else ""
                start = f"{run} {name}{separator} {flags}"
                # Runtime QA executes setup then qa.start, not smoke. Preview
                # and production servers need build output in a fresh checkout.
                if script in ("vite preview", "next start", "astro preview", "nuxt start"):
                    if not scripts.get("build", "").strip():
                        config["start_detection"]["reason"] = f"{script} requires a build script before it can serve a fresh checkout."
                        continue
                    start = f"{run} build && {start}"
                config["qa"] = {"start": start}
                config["start_detection"] = {"available": True, "requires_confirmation": True,
                                             "reason": "Review before saving; must listen on $PORT."}
                break
        notes.append(f"Node project ({manager})")
    pyproject = await _file(gh, full_name, "pyproject.toml", ref)
    if pyproject is not None:
        if await _file(gh, full_name, "uv.lock", ref):
            config["setup"].append("pip install uv >/dev/null 2>&1 || true; uv sync --all-extras || uv sync")
            config["checks"].append("uv run pytest -q")
        else:
            config["setup"].append("python3 -m venv .venv && . .venv/bin/activate && pip install -e '.[dev]' || pip install -e .")
            config["checks"].append(". .venv/bin/activate && python -m pytest -q")
        notes.append("Python project")
    elif await _file(gh, full_name, "requirements.txt", ref):
        config["setup"].append("python3 -m venv .venv && . .venv/bin/activate && pip install -r requirements.txt")
        config["checks"].append(". .venv/bin/activate && python -m pytest -q")
        notes.append("Python project (requirements.txt)")
    if await _file(gh, full_name, "go.mod", ref):
        config["checks"].append("go test ./...")
        notes.append("Go module")
    if await _file(gh, full_name, "Cargo.toml", ref):
        config["checks"].append("cargo test")
        notes.append("Rust crate")
    if not config["smoke"]:
        config["smoke"] = list(config["checks"])
    agents = await _file(gh, full_name, "AGENTS.md", ref) or await _file(gh, full_name, "CLAUDE.md", ref)
    if agents:
        config["context"] = agents[:4000]
        notes.append("Found agent instructions")
    config["detected"] = notes
    return config
