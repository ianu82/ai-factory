"""The Factory tool set shared by every harness that lets the Factory execute tools
(native and Anton). Tools run inside the job sandbox; errors are returned as text so the
agent can recover instead of the run aborting."""

from __future__ import annotations

import json
import shlex
import time
from typing import Any

from ..definitions.validation import safe_path
from ..sandbox import COMMAND_TIMEOUT, SandboxError
from ..sandbox import read_text as sb_read_text
from ..sandbox import write_text as sb_write_text
from .base import HarnessSession

MAX_TOOL_OUTPUT = 30_000
# Two maximum-size bodies per model turn, leaving headroom in Native's context
# for other tool results. Never spend this budget by partially returning a body.
SKILL_BODY_TURN_BUDGET = 128 * 1024
RUN_DEFAULT_TIMEOUT = 300
RUN_TIMEOUT_HELP = (f"Seconds before the command is stopped. Default {RUN_DEFAULT_TIMEOUT}, max {COMMAND_TIMEOUT} "
                    f"({COMMAND_TIMEOUT // 60} minutes, the limit for one of the repository's checks); less when your "
                    "session is about to run out of time.")


def _fn(name: str, description: str, properties: dict[str, Any], required: list[str]) -> dict[str, Any]:
    return {"type": "function", "function": {"name": name, "description": description,
            "parameters": {"type": "object", "properties": properties, "required": required}}}


def clip(text: str, limit: int = MAX_TOOL_OUTPUT) -> str:
    if len(text) <= limit:
        return text
    head = limit // 4
    return text[:head] + f"\n… [{len(text) - limit:,} characters omitted] …\n" + text[-(limit - head):]


READ_TOOLS = [
    _fn("list_files", "List tracked and new files under a directory of the repository.",
        {"path": {"type": "string", "description": "Directory relative to the repo root (default '.')."},
         "pattern": {"type": "string", "description": "Optional glob such as '*.ts' or 'src/**/*.py'."}}, []),
    _fn("read_file", "Read a text file with line numbers. Use start_line/end_line for large files.",
        {"path": {"type": "string"}, "start_line": {"type": "integer"}, "end_line": {"type": "integer"}}, ["path"]),
    _fn("search", "Search file contents with a regular expression (ripgrep syntax). Returns file:line matches.",
        {"pattern": {"type": "string"}, "path": {"type": "string"}, "glob": {"type": "string"}}, ["pattern"]),
    _fn("run", "Run a shell command in the repository root (bash). Use for builds, tests, git inspection.",
        {"command": {"type": "string"}, "timeout_seconds": {"type": "integer", "description": RUN_TIMEOUT_HELP}},
        ["command"]),
]

WRITE_TOOLS = [
    _fn("write_file", "Create or overwrite a file with the given full content.",
        {"path": {"type": "string"}, "content": {"type": "string"}}, ["path", "content"]),
    _fn("edit_file", "Replace an exact snippet in a file. old_text must match exactly once unless replace_all is true.",
        {"path": {"type": "string"}, "old_text": {"type": "string"}, "new_text": {"type": "string"},
         "replace_all": {"type": "boolean"}}, ["path", "old_text", "new_text"]),
]



def finish_tool(schema: dict[str, Any]) -> dict[str, Any]:
    return {"type": "function", "function": {
        "name": "finish",
        "description": "Submit your final report. Call this exactly once when your work is complete.",
        "parameters": schema}}


DEFINITION_TOOLS = [
    _fn("read_skill", "Load a full skill body by qualified skill_id, or read a paginated supporting file. "
        "Body loads share a per-turn output budget; retry deferred loads in the next turn.",
        {"skill_id": {"type": "string"}, "path": {"type": "string"},
         "offset": {"type": "integer", "description": "Supporting-file character offset (default 0)."},
         "limit": {"type": "integer", "description": "Supporting-file characters, maximum 16000."}}, ["skill_id"]),
    _fn("search_knowledge", "Search scoped knowledge metadata/body, or read a page by qualified page_id. Paginated.",
        {"query": {"type": "string"}, "page_id": {"type": "string"},
         "offset": {"type": "integer"}, "limit": {"type": "integer"}}, []),
]


def _page(text: str, args: dict) -> dict:
    offset = max(0, int(args.get("offset", 0)))
    limit = max(1, min(16000, int(args.get("limit", 16000))))
    end = min(len(text), offset + limit)
    return {"body": text[offset:end], "offset": offset, "total": len(text),
            "next_offset": end if end < len(text) else None}


def _source_metadata(source: dict) -> dict:
    """Tool-facing provenance is a bounded preview; the run retains the original."""
    return {key: (value[:256] + "… [truncated]" if isinstance(value, str) and len(value) > 256 else value)
            for key in ("kind", "repository_id", "path", "commit")
            if (value := source.get(key)) is not None}


def tool_specs(writable: bool) -> list[dict[str, Any]]:
    return list(READ_TOOLS) + list(DEFINITION_TOOLS) + (WRITE_TOOLS if writable else [])


class ToolBox:
    def __init__(self, session: HarnessSession):
        self.session = session
        self.log: list[str] = []
        self.definitions = json.loads(session.definition_context)
        self.begin_turn()

    def begin_turn(self) -> None:
        """Called by the harness for each model response, never by an agent tool."""
        self.skill_body_bytes = 0

    async def call(self, name: str, args: Any) -> str:
        output = await self._tool(self.session, name, args)
        self.log.append(f"{name} {json.dumps(args)[:300] if isinstance(args, dict) else str(args)[:300]}")
        return output

    async def _tool(self, session: HarnessSession, name: str, args: Any) -> str:
        if not isinstance(args, dict):
            return "Error: tool arguments were not valid JSON. Retry with a JSON object."
        sandbox = session.sandbox
        try:
            if name in {"read_skill", "search_knowledge"}:
                # No agent-selected workspace/revision and no fallback name lookup.
                allowed = ({"skill_id", "path", "offset", "limit"} if name == "read_skill" else
                           {"query", "page_id", "offset", "limit"})
                if args.keys() - allowed:
                    return "Error: unsupported definition tool arguments"
                if name == "read_skill":
                    skill = self.definitions.get("skills", {}).get(args["skill_id"])
                    if skill is None:
                        return "Error: skill not found in this session scope"
                    if "path" in args:
                        safe_path(args["path"])
                        text = skill.get("files", {}).get(args["path"])
                        if text is None:
                            return "Error: supporting file not found in this skill"
                        return json.dumps(_page(text, args), ensure_ascii=False)
                    body = skill["body"]
                    size = len(body.encode("utf-8"))
                    if self.skill_body_bytes + size > SKILL_BODY_TURN_BUDGET:
                        return ("Skill not loaded: output budget for this turn reached, "
                                "call read_skill again in the next turn.")
                    self.skill_body_bytes += size
                    metadata = {k: skill[k] for k in ("id", "name", "source")}
                    metadata["revision"] = session.definition_revision
                    session.on_progress("skill_loaded", f"Loaded skill {skill['name']}", metadata)
                    if not any(item["id"] == skill["id"] for item in session.loaded_skills):
                        session.loaded_skills.append(metadata)
                    # Intentional bypass of clip: validated bodies may be 64 KiB.
                    return body
                pages = self.definitions.get("knowledge", {})
                if args.get("page_id"):
                    page = pages.get(args["page_id"])
                    if page is None:
                        return "Error: knowledge page not found in this session scope"
                    return json.dumps({"id": page["id"], "name": page["name"],
                                       "source": _source_metadata(page["source"]),
                                       **_page(page["body"], args)}, ensure_ascii=False)
                query = str(args.get("query", "")).casefold()
                matches = [{k: page[k] for k in ("id", "name", "description", "source")}
                           for page in pages.values()
                           if query in "\n".join(page[k] for k in ("name", "description", "body")).casefold()]
                offset = max(0, int(args.get("offset", 0)))
                limit = max(1, min(50, int(args.get("limit", 20))))
                # Bound search output as well: descriptions have a workspace-wide,
                # not per-entry, quota. One oversized hit must not flood the model.
                hits = []
                size = 0
                for hit in matches[offset:offset + limit]:
                    hit = {**hit, "description": hit["description"][:1000],
                           "source": _source_metadata(hit["source"])}
                    width = len(json.dumps(hit, ensure_ascii=False))
                    if size + width > 16000:
                        break
                    hits.append(hit)
                    size += width
                end = offset + len(hits)
                return json.dumps({"pages": hits, "total": len(matches),
                                   "next_offset": end if end < len(matches) else None}, ensure_ascii=False)
            if name == "list_files":
                path = args.get("path") or "."
                sandbox.resolve(path)
                pattern = args.get("pattern")
                cmd = (f"(git ls-files --cached --others --exclude-standard -- {shlex.quote(path)} 2>/dev/null "
                       f"|| find {shlex.quote(path)} -type f -not -path '*/.git/*' -not -path '*/node_modules/*')")
                if pattern:
                    cmd += f" | grep -E {shlex.quote(_glob_to_regex(pattern))}"
                result = await sandbox.exec(cmd + " | head -n 3000", timeout=60)
                session.on_progress("tool", f"Listed files in {path}", {"tool": name})
                return clip(result.output or "(no files)")
            if name == "read_file":
                text = await sb_read_text(sandbox, args["path"])
                lines = text.splitlines()
                start = max(1, int(args.get("start_line") or 1))
                end = int(args.get("end_line") or min(len(lines), start + 1499))
                end = min(end, len(lines), start + 2999)
                body = "\n".join(f"{i:>5}\t{lines[i - 1]}" for i in range(start, end + 1)) if lines else "(empty file)"
                more = f"\n… file has {len(lines)} lines; showing {start}-{end}" if end < len(lines) or start > 1 else ""
                session.on_progress("tool", f"Read {args['path']}", {"tool": name, "path": args["path"]})
                return clip(body + more)
            if name == "search":
                path = args.get("path") or "."
                sandbox.resolve(path)
                glob = f" --glob {shlex.quote(args['glob'])}" if args.get("glob") else ""
                cmd = (f"if command -v rg >/dev/null; then rg -n --no-heading --max-columns 300 --max-count 50{glob} "
                       f"-e {shlex.quote(args['pattern'])} {shlex.quote(path)}; else grep -rnE --exclude-dir=.git "
                       f"--exclude-dir=node_modules {shlex.quote(args['pattern'])} {shlex.quote(path)}; fi | head -n 400")
                result = await sandbox.exec(cmd, timeout=60)
                session.on_progress("tool", f"Searched for {args['pattern'][:80]}", {"tool": name})
                return clip(result.output or "(no matches)")
            if name == "run":
                timeout = max(10, min(int(args.get("timeout_seconds") or RUN_DEFAULT_TIMEOUT), COMMAND_TIMEOUT,
                                      int(session.command_timeout), int(max(30, session.remaining() - 60))))
                command = str(args["command"])
                session.on_progress("command", command[:300], {"tool": name})
                started = time.monotonic()
                result = await sandbox.exec(command, timeout=timeout)
                took = f"{time.monotonic() - started:.0f}s"
                session.on_progress("command_result", f"timed out after {took} (limit {timeout}s)" if result.timed_out
                                    else f"exit {result.exit_code} in {took}",
                                    {"command": command[:300], "exit_code": result.exit_code, "timed_out": result.timed_out})
                return clip(result.for_model())
            if name == "write_file" and session.writable:
                await sb_write_text(sandbox, args["path"], args.get("content", ""))
                session.on_progress("edit", f"Wrote {args['path']}", {"tool": name, "path": args["path"]})
                return f"Wrote {args['path']} ({len(args.get('content', ''))} characters)."
            if name == "edit_file" and session.writable:
                text = await sb_read_text(sandbox, args["path"], limit=5_000_000)
                old, new = args.get("old_text", ""), args.get("new_text", "")
                count = text.count(old) if old else 0
                if count == 0:
                    return "Error: old_text was not found. Read the file again and copy the exact text (including whitespace)."
                if count > 1 and not args.get("replace_all"):
                    return f"Error: old_text matches {count} places. Add surrounding context or set replace_all."
                updated = text.replace(old, new) if args.get("replace_all") else text.replace(old, new, 1)
                await sb_write_text(sandbox, args["path"], updated)
                session.on_progress("edit", f"Edited {args['path']}", {"tool": name, "path": args["path"]})
                return f"Edited {args['path']} ({count if args.get('replace_all') else 1} replacement)."
            return f"Error: unknown or unavailable tool '{name}'."
        except SandboxError as exc:
            return f"Error: {exc}"
        except KeyError as exc:
            return f"Error: missing argument {exc}"
        except (OSError, ValueError, TypeError) as exc:
            return f"Error: {type(exc).__name__}: {exc}"


def _glob_to_regex(glob: str) -> str:
    out = ""
    i = 0
    while i < len(glob):
        c = glob[i]
        if c == "*":
            if glob[i:i + 2] == "**":
                out += ".*"
                i += 2
                if i < len(glob) and glob[i] == "/":
                    i += 1
                continue
            out += "[^/]*"
        elif c == "?":
            out += "[^/]"
        elif c in ".+()|^$[]{}\\":
            out += "\\" + c
        else:
            out += c
        i += 1
    return "(^|/)" + out + "$"
