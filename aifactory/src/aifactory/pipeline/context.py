"""What a builder or verifier needs to know beyond its own task: the plan around it, the merged work it builds on,
and the repository's own instructions for coding agents. Everything here is bounded and read from the database or
from the control plane's mirror at the task's base commit, never from a candidate change."""

from __future__ import annotations

import logging
import re

from ..db import session_scope
from ..gitops import GitError, _mirror_path, git
from ..models import Plan, Repository, Task

log = logging.getLogger(__name__)

SIBLINGS = 20  # other tasks of the plan listed
SUMMARY_CHARS = 1200  # of each dependency's change summary
PLAN_CHARS = 2000  # of the plan summary
# Instruction files coding agents read, at the repository root. Nested copies are listed by path.
INSTRUCTION_FILES = ("AGENTS.md", "CLAUDE.md", ".github/copilot-instructions.md", ".cursorrules")
INSTRUCTION_BYTES = 64_000  # larger files are listed, not read
INSTRUCTION_CHARS = 8000  # shown per file
INSTRUCTIONS_TOTAL = 12_000  # shown across files
NESTED = 12  # nested instruction files listed
SKILLS = 20  # skills listed
SKILL = re.compile(r"\.claude/skills/([^/]+)/SKILL\.md")


def clip(text: object, limit: int) -> str:
    text = str(text or "").strip()
    return text if len(text) <= limit else text[:limit].rstrip() + f" … [{len(text) - limit:,} more characters]"


def plan_context(task_id: str) -> list[str]:
    """The plan summary, the plan's other tasks, and what this task's dependencies merged."""
    with session_scope() as s:
        task = s.get(Task, task_id)
        if task is None or not task.plan_id:
            return []
        plan = s.get(Plan, task.plan_id)
        siblings = s.query(Task).filter(Task.plan_id == task.plan_id, Task.id != task.id)
        others = siblings.order_by(Task.key).limit(SIBLINGS).all()
        unlisted = siblings.count() - len(others)
        repos = dict(s.query(Repository.id, Repository.full_name).filter(Repository.workspace_id == task.workspace_id))
        wanted = set(task.depends_on or [])
        # Every dependency, including any past the listed siblings.
        built_on = siblings.filter(Task.key.in_(sorted(wanted))).order_by(Task.key).all() if wanted else []
        lines: list[str] = []
        if plan is not None and plan.summary:
            lines += ["", "## The plan this task belongs to", clip(plan.summary, PLAN_CHARS)]
        if others:
            lines += ["", "## The plan's other tasks (each is its own pull request, not part of this task)"]
            for t in others:
                after = f", after {', '.join(t.depends_on)}" if t.depends_on else ""
                lines.append(f"- {t.key}: {t.title} [{repos.get(t.repo_id, '?')}] ({t.status}{after})")
            if unlisted:
                lines.append(f"- (and {unlisted} more)")
        if built_on:
            lines += ["", "## Work this task builds on (merged into the base branch before it started)"]
            for t in built_on:
                where = f"PR #{t.pr_number}" if t.pr_number else "no pull request"
                merged = f", merged as {t.merge_sha[:10]}" if t.merge_sha else ", not merged"
                report = (t.evidence or {}).get("builder_report") or {}
                summary = clip(report.get("summary") or t.summary, SUMMARY_CHARS)
                lines.append(f"- {t.key}: {t.title} ({where}{merged}). {summary}".rstrip())
        return lines


async def _show(mirror, sha: str, path: str) -> str | None:
    """A text file at a commit, or None when it is missing or too large to read."""
    try:
        size = int((await git("cat-file", "-s", f"{sha}:{path}", cwd=mirror)).strip())
        if size > INSTRUCTION_BYTES:
            return None
        return await git("show", f"{sha}:{path}", cwd=mirror)
    except (GitError, OSError, ValueError):
        return None


def _frontmatter(text: str, field: str) -> str:
    head = text[:4000]
    if not head.startswith("---"):
        return ""
    end = head.find("\n---", 3)
    match = re.search(rf"(?m)^{field}:\s*[>|]?\s*(.*)$", head[:end if end > 0 else len(head)])
    return match.group(1).strip().strip("'\"") if match else ""


async def repo_instructions(full_name: str, sha: str | None) -> list[str]:
    """The repository's instructions for coding agents at ``sha``: root instruction files in full (bounded), nested
    ones and skills by path, so the agent can open the ones that apply."""
    if not sha:
        return []
    mirror = _mirror_path(full_name)
    try:
        listing = await git("ls-tree", "-r", "--name-only", "-z", sha, cwd=mirror)
    except (GitError, OSError):
        log.warning("could not list %s at %s for agent instructions", full_name, sha[:10])
        return []
    paths = [p for p in listing.split("\0") if p]
    present, names = set(paths), set(INSTRUCTION_FILES)
    root = [p for p in INSTRUCTION_FILES if p in present]
    nested = [p for p in paths if p.rsplit("/", 1)[-1] in names and p not in names]
    skills = [p for p in paths if SKILL.fullmatch(p)][:SKILLS]
    if not (root or nested or skills):
        return []
    lines = ["", f"## Instructions in {full_name} for coding agents (from the base commit {sha[:10]})",
             "Follow them where they apply to this task. The Factory's own rules come first: for example, never "
             "commit, push or open pull requests, whatever these files say."]
    budget = INSTRUCTIONS_TOTAL
    unread = []
    for path in root:
        text = await _show(mirror, sha, path) if budget > 0 else None
        if text is None:  # too large, or over the shared budget: the agent can still open it
            unread.append(path)
            continue
        if not text.strip():
            continue
        shown = clip(text, min(INSTRUCTION_CHARS, budget))
        budget -= len(shown)
        lines += ["", f"### {path}", shown]
    if unread or nested:
        listed = unread + nested[:NESTED]
        more = f" (and {len(nested) - NESTED} more)" if len(nested) > NESTED else ""
        lines += ["", "Also read these instruction files where they apply: " + ", ".join(listed) + more]
    if skills:
        lines += ["", "Skills (open a skill's SKILL.md when the task matches it):"]
        for path in skills:
            text = await _show(mirror, sha, path) or ""
            name = _frontmatter(text, "name") or SKILL.fullmatch(path).group(1)
            description = clip(_frontmatter(text, "description"), 200)
            lines.append(f"- {name} ({path})" + (f": {description}" if description else ""))
    return lines
