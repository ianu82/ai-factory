"""Codex harness: the official Codex CLI running inside the job sandbox.

Codex talks only to the Factory inference gateway with a run-scoped token (never the real
provider key), runs with the sandbox's private identity, and must return the same role
report as every other harness via ``--output-schema``.
"""

from __future__ import annotations

import json
import os
import shlex
import shutil

from ..sandbox import SCRATCH
from .base import HarnessFailed, HarnessResult, HarnessSession, HarnessUnavailable
from .schemas import validate_report

CODEX_BIN = os.environ.get("AIFACTORY_CODEX_BIN", "codex")


def _strict(schema: dict) -> dict:
    """Codex structured output requires every property to be required and no extras."""
    if not isinstance(schema, dict):
        return schema
    out = dict(schema)
    if out.get("type") == "object" and "properties" in out:
        out["properties"] = {k: _strict(v) for k, v in out["properties"].items()}
        out["required"] = list(out["properties"].keys())
        out["additionalProperties"] = False
    if out.get("type") == "array" and "items" in out:
        out["items"] = _strict(out["items"])
    return out


class CodexHarness:
    name = "codex"
    label = "Codex"

    def availability(self) -> tuple[bool, str]:
        path = shutil.which(CODEX_BIN)
        if not path:
            return False, "Codex CLI is not installed on this deployment"
        return True, "Official Codex CLI via the Factory inference gateway"

    async def run(self, session: HarnessSession) -> HarnessResult:
        ok, why = self.availability()
        if not ok:
            raise HarnessUnavailable(why)
        if not session.gateway_url or not session.gateway_token:
            raise HarnessUnavailable("Codex requires the Factory inference gateway")
        if getattr(session.sandbox, "kind", "local") == "vm":
            raise HarnessUnavailable("Codex sessions run in a local sandbox, not a VM")
        sandbox = session.sandbox
        scratch = f"{SCRATCH}/codex"
        sandbox.write_text(f"{scratch}/schema.json", json.dumps(_strict(session.report_schema)))
        codex_home = sandbox.home / ".codex"
        config = "\n".join([
            f'model = "{session.model}"',
            'model_provider = "factory"',
            'approval_policy = "never"',
            'sandbox_mode = "danger-full-access"',
            "[model_providers.factory]",
            'name = "AI Factory gateway"',
            f'base_url = "{session.gateway_url}"',
            'env_key = "FACTORY_GATEWAY_TOKEN"',
            'wire_api = "responses"',
            "request_max_retries = 2",
            "stream_max_retries = 2",
            "",
        ])
        sandbox.write_text(f"{scratch}/config.toml", config)
        prompt = (
            f"{session.system}\n\n---\n\n{session.prompt}\n\n---\n\n"
            + ("You may modify files in the repository." if session.writable else
               "Do NOT modify any tracked files; you are reviewing only.")
            + " When you are done, reply ONLY with the final JSON report matching the provided schema."
        )
        sandbox.write_text(f"{scratch}/prompt.md", prompt)
        work = str(sandbox.work)
        command = (
            f"mkdir -p {shlex.quote(str(codex_home))} && cp {scratch}/config.toml {shlex.quote(str(codex_home))}/config.toml && "
            f"{shlex.quote(CODEX_BIN)} exec --json --skip-git-repo-check --ephemeral "
            f"--dangerously-bypass-approvals-and-sandbox -C {shlex.quote(work)} -m {shlex.quote(session.model)} "
            f"--output-schema {scratch}/schema.json -o {scratch}/last.json - < {scratch}/prompt.md"
        )
        events: list[dict] = []
        pending = bytearray()

        def on_output(block: bytes) -> None:
            pending.extend(block)
            while b"\n" in pending:
                line, _, rest = bytes(pending).partition(b"\n")
                pending[:] = rest
                try:
                    event = json.loads(line)
                except json.JSONDecodeError:
                    continue
                events.append(event)
                item = event.get("item") or {}
                kind = item.get("type") or item.get("item_type")
                if event.get("type") == "item.completed" and kind == "command_execution":
                    session.on_progress("command", str(item.get("command", ""))[:300],
                                        {"exit_code": item.get("exit_code")})
                elif event.get("type") == "item.completed" and kind == "file_change":
                    session.on_progress("edit", "Codex changed files", {"changes": item.get("changes", [])[:20]})
                elif event.get("type") == "item.completed" and kind in ("agent_message", "reasoning"):
                    text = str(item.get("text", ""))[:600]
                    if text:
                        session.on_progress("thought", text, {})

        timeout = max(60.0, session.remaining() - 30)
        result = await sandbox.exec(
            command,
            timeout=timeout,
            env={"FACTORY_GATEWAY_TOKEN": session.gateway_token, "CODEX_HOME": str(codex_home)},
            on_output=on_output,
        )
        turns = sum(1 for e in events if e.get("type") == "turn.completed") or session.ledger.calls
        if result.timed_out:
            raise HarnessFailed("Codex exceeded its time budget")
        try:
            raw = sandbox.read_text(f"{scratch}/last.json")
        except Exception:
            raw = ""
        report = _parse_report(raw)
        if report is None:
            failure = next((e for e in reversed(events) if e.get("type") in ("turn.failed", "error")), None)
            detail = json.dumps(failure)[:600] if failure else result.output[-800:]
            raise HarnessFailed(f"Codex did not return a report (exit {result.exit_code}): {detail}")
        report, error = validate_report(session.role, report)
        if report is None:
            raise HarnessFailed(f"Codex report was invalid: {error}")
        return HarnessResult(report=report, turns=turns, log=result.output[-4000:])


def _parse_report(raw: str):
    raw = (raw or "").strip()
    if not raw:
        return None
    if raw.startswith("```"):
        raw = raw.strip("`")
        raw = raw.split("\n", 1)[1] if "\n" in raw else raw
    start, end = raw.find("{"), raw.rfind("}")
    if start < 0 or end < start:
        return None
    try:
        return json.loads(raw[start:end + 1])
    except json.JSONDecodeError:
        return None
