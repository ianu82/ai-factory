"""The Factory tool set shared by every harness that lets the Factory execute tools
(native and Anton). Tools run inside the job sandbox; errors are returned as text so the
agent can recover instead of the run aborting."""

from __future__ import annotations

import json
import shlex
import time
from typing import Any

from ..sandbox import SandboxError
from ..sandbox import read_text as sb_read_text
from ..sandbox import write_text as sb_write_text
from .base import HarnessSession

MAX_TOOL_OUTPUT = 30_000


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
        {"command": {"type": "string"}, "timeout_seconds": {"type": "integer", "description": "Default 300, max 1800."}},
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


def tool_specs(writable: bool, *, role: str = "") -> list[dict[str, Any]]:
    if role == "risk_judge":
        tools = [json.loads(json.dumps(t)) for t in READ_TOOLS[:3]]
        tools[0]["function"]["description"] = "List safe checkout files (no history or session files)."
        tools[2]["function"]["description"] = "Search safe checkout files for literal text; returns file:line matches."
        return tools
    return list(READ_TOOLS) + (WRITE_TOOLS if writable else [])


class ToolBox:
    def __init__(self, session: HarnessSession):
        self.session = session
        self.log: list[str] = []

    async def call(self, name: str, args: Any) -> str:
        output = await self._tool(self.session, name, args)
        self.log.append(f"{name} {json.dumps(args)[:300] if isinstance(args, dict) else str(args)[:300]}")
        return output

    async def _tool(self, session: HarnessSession, name: str, args: Any) -> str:
        if not isinstance(args, dict):
            return "Error: tool arguments were not valid JSON. Retry with a JSON object."
        sandbox = session.sandbox
        try:
            if session.role == "risk_judge":
                return await _judge_tool(session, name, args)
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
                timeout = max(10, min(int(args.get("timeout_seconds") or 300), 1800, int(session.command_timeout),
                                      int(max(30, session.remaining() - 60))))
                command = str(args["command"])
                session.on_progress("command", command[:300], {"tool": name})
                started = time.monotonic()
                result = await sandbox.exec(command, timeout=timeout)
                session.on_progress("command_result", f"exit {result.exit_code} in {time.monotonic() - started:.0f}s",
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
        except (OSError, ValueError) as exc:
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


# Runs in the checkout's sandbox (also on VMs), not on the control-plane filesystem.
# No agent-supplied shell, executable code, git commands or environment are accepted.
# Descriptor-relative NOFOLLOW opens prevent traversal and symlink replacement races.
_JUDGE_INSPECT = r"""
import fnmatch, json, os, stat, sys
LIMIT = 30000
MAX_FILES = 3000
MAX_BYTES = 400000
DENIED = {'.git', '.factory', '.anton', '.codex', '.claude', '.sessions',
          'home', 'sessions', 'session', 'node_modules', '__pycache__'}
def allowed(name):
    lower = name.lower()
    return (lower not in DENIED and not lower.startswith('.')
            and not any(word in lower for word in ('report', 'transcript', 'session'))
            and not lower.endswith(('.log', '.jsonl')))
def open_at(parent, name, directory=False):
    flags = os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK
    if directory:
        flags |= os.O_DIRECTORY
    fd = os.open(name, flags, dir_fd=parent)
    mode = os.fstat(fd).st_mode
    if not (stat.S_ISDIR(mode) if directory else stat.S_ISREG(mode)):
        os.close(fd)
        raise ValueError()
    return fd
try:
    root, tool, args = sys.argv[1], sys.argv[2], json.loads(sys.argv[3])
    path = args.get('path') or '.'
    if not isinstance(path, str) or path.startswith('/') or '\\' in path:
        raise ValueError()
    parts = path.split('/')
    if any(p == '..' or (p not in ('', '.') and not allowed(p)) for p in parts):
        raise ValueError()
    parts = [p for p in parts if p not in ('', '.')]
    fd = os.open(root, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    for part in parts[:-1] if tool == 'read_file' else parts:
        child = open_at(fd, part, True)
        os.close(fd)
        fd = child
    def read(parent, name):
        child = open_at(parent, name)
        try:
            data = os.read(child, MAX_BYTES + 1)
            if len(data) > MAX_BYTES:
                return '[file exceeds inspection byte limit]'
            return data.decode('utf-8', 'replace')
        finally:
            os.close(child)
    output = []
    size = count = visited = 0
    def emit(text):
        global size
        remaining = LIMIT - size
        if remaining > 0:
            output.append(text[:remaining])
            size += min(len(text), remaining) + 1
    if tool == 'read_file':
        if not parts:
            raise ValueError()
        lines = read(fd, parts[-1]).splitlines()
        start = max(1, int(args.get('start_line') or 1))
        end = min(len(lines), start + 2999, int(args.get('end_line') or start + 1499))
        for i in range(start, end + 1):
            emit(str(i) + '\t' + lines[i-1])
    else:
        # Search uses a bounded literal pattern: never executes regex code or an
        # unbounded backtracking regex supplied by the model.
        pattern = args.get('pattern') or ''
        if not isinstance(pattern, str) or len(pattern) > 1000:
            raise ValueError()
        glob = args.get('glob') if tool == 'search' else args.get('pattern')
        def walk(parent, prefix='', depth=0):
            global count, visited
            if depth > 40:
                return
            with os.scandir(parent) as entries:
                for entry in entries:
                    visited += 1
                    if visited > 20000 or count >= MAX_FILES or size >= LIMIT:
                        return
                    if not allowed(entry.name) or entry.is_symlink():
                        continue
                    name = prefix + entry.name
                    try:
                        if entry.is_dir(follow_symlinks=False):
                            child = open_at(parent, entry.name, True)
                            try:
                                walk(child, name + '/', depth + 1)
                            finally:
                                os.close(child)
                        elif entry.is_file(follow_symlinks=False):
                            count += 1
                            if glob and not fnmatch.fnmatch(name, glob):
                                continue
                            if tool == 'list_files':
                                emit(name)
                            else:
                                for i, line in enumerate(read(parent, entry.name).splitlines(), 1):
                                    if pattern in line:
                                        emit(name + ':' + str(i) + ':' + line[:300])
                    except OSError:
                        continue
        walk(fd, ('/'.join(parts) + '/') if parts else '')
    os.close(fd)
    print('\n'.join(output) or '(no results)')
except Exception:
    print('Error: checkout inspection refused or unavailable.')
"""


async def _judge_tool(session: HarnessSession, name: str, args: dict[str, Any]) -> str:
    if name not in {"list_files", "read_file", "search"}:
        return "Error: tool unavailable in blind judge profile."
    if len(json.dumps(args)) > 16000:
        return "Error: checkout inspection arguments exceed limit."
    command = "python3 -I -c " + shlex.quote(_JUDGE_INSPECT) + " " + " ".join(
        shlex.quote(value) for value in (str(session.sandbox.work), name, json.dumps(args)))
    result = await session.sandbox.exec(command, timeout=30)
    if not result.ok or result.truncated:
        return "Error: checkout inspection refused or unavailable."
    return clip(result.output)
