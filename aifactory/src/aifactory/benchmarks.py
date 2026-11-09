"""Versioned benchmark snapshots and database-only delivered-task export.

Schema v1 is deliberately self-contained: no branch resolution, credentials, or
provider calls are needed to parse, load, or export a suite.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import tempfile
from collections import Counter
from pathlib import Path
from typing import Any

from sqlalchemy import select, text

from .db import session_scope
from .models import BenchmarkSuite, Repository, Task, Workspace


class SuiteError(ValueError):
    """Invalid suite content or workspace scope."""


def default_output_root() -> Path:
    """Use the current source checkout, never a path relative to site-packages.

    Packaged deployments without a checkout must choose a durable location with
    --output-root instead of silently writing into the Python installation.
    """
    for root in (Path.cwd(), *Path.cwd().parents):
        product = root / "aifactory"
        if (product / "src/aifactory/cli.py").is_file():
            return product / "benchmarks"
        if (root / "src/aifactory/cli.py").is_file():
            return root / "benchmarks"
    raise SuiteError("No source checkout found; specify --output-root with a writable export directory")


def _safe_name(value: Any, field: str, limit: int = 80) -> str:
    if not isinstance(value, str) or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]*", value) or len(value) > limit:
        raise SuiteError(f"{field} must be a safe name (letters, digits, underscore, hyphen; max {limit})")
    return value


def _string(value: Any, field: str, limit: int | None = None) -> None:
    if not isinstance(value, str) or not value.strip() or (limit is not None and len(value) > limit):
        raise SuiteError(f"{field} must be a nonempty string" + (f" of at most {limit} characters" if limit else ""))


def _commit(value: Any, field: str) -> None:
    if not isinstance(value, str) or not re.fullmatch(r"(?:[0-9a-fA-F]{40}|[0-9a-fA-F]{64})", value):
        raise SuiteError(f"{field} must be a full 40- or 64-character hexadecimal commit identifier")


def validate_suite(value: Any) -> dict[str, Any]:
    if not isinstance(value, dict) or set(value) != {"schema_version", "name", "tasks"}:
        raise SuiteError("Suite must contain schema_version, name and tasks only")
    if type(value["schema_version"]) is not int or value["schema_version"] != 1:
        raise SuiteError("Unsupported schema_version (expected 1)")
    _safe_name(value["name"], "Suite name")
    if not isinstance(value["tasks"], list) or not value["tasks"]:
        raise SuiteError("Suite tasks must be a nonempty list")
    keys = set()
    required = {"key", "repository", "base_commit", "title", "specification", "acceptance"}
    for task in value["tasks"]:
        if not isinstance(task, dict) or not required <= set(task) or set(task) - required - {"reference_change"}:
            raise SuiteError("Task must contain key, repository, base_commit, title, specification and acceptance")
        key = _safe_name(task["key"], "Task key", 100)
        if key in keys:
            raise SuiteError(f"Duplicate task key: {key}")
        keys.add(key)
        repo = task["repository"]
        if not isinstance(repo, str) or len(repo) > 200 or not re.fullmatch(r"[A-Za-z0-9_-]+/[A-Za-z0-9_.-]+", repo) or repo.split("/")[1] in {".", ".."}:
            raise SuiteError(f"Invalid repository for {key}: expected owner/name")
        _commit(task["base_commit"], f"{key}.base_commit")
        _string(task["title"], f"{key}.title", 300)
        _string(task["specification"], f"{key}.specification")
        if not isinstance(task["acceptance"], list) or not task["acceptance"]:
            raise SuiteError(f"{key}.acceptance must be a nonempty list")
        for criterion in task["acceptance"]:
            _string(criterion, f"{key}.acceptance item")
        if "reference_change" in task:
            ref = task["reference_change"]
            if not isinstance(ref, dict) or set(ref) != {"pr_number", "pr_url", "merge_sha"}:
                raise SuiteError(f"{key}.reference_change must contain pr_number, pr_url and merge_sha")
            if type(ref["pr_number"]) is not int or ref["pr_number"] <= 0:
                raise SuiteError(f"{key}.pr_number must be a positive integer")
            if ref["pr_url"] != f"https://github.com/{repo}/pull/{ref['pr_number']}":
                raise SuiteError(f"{key}.pr_url must match repository and PR number")
            _commit(ref["merge_sha"], f"{key}.merge_sha")
    # Detach caller-owned mutable objects from the stored snapshot.
    return json.loads(json.dumps(value, ensure_ascii=False))


def _object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise SuiteError(f"Duplicate JSON field: {key}")
        result[key] = value
    return result


def parse_suite(path: str | Path) -> dict[str, Any]:
    path = Path(path)
    if path.is_dir():
        path = path / "suite.json"
    try:
        return validate_suite(json.loads(path.read_text(encoding="utf-8"), object_pairs_hook=_object))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise SuiteError(f"Cannot read suite JSON: {exc}") from exc


def load_suite(workspace_id: str, path: str | Path) -> BenchmarkSuite:
    suite = parse_suite(path)  # Validate the entire document before any writes.
    digest = hashlib.sha256(json.dumps(suite, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()).hexdigest()
    with session_scope() as session:
        # FOR UPDATE is ignored on SQLite. Acquire the writer lock *before* any
        # reads so competing CLI processes see the committed revision/hash.
        if session.get_bind().dialect.name == "sqlite":
            session.execute(text("BEGIN IMMEDIATE"))
        workspace = session.scalar(select(Workspace).where(Workspace.id == workspace_id).with_for_update())
        if workspace is None:
            raise SuiteError("Workspace not found")
        repos = set(session.scalars(select(Repository.full_name).where(Repository.workspace_id == workspace_id)))
        foreign = {t["repository"] for t in suite["tasks"]} - repos
        if foreign:
            raise SuiteError(f"Repositories not in workspace: {', '.join(sorted(foreign))}")
        revisions = list(session.scalars(select(BenchmarkSuite).where(
            BenchmarkSuite.workspace_id == workspace_id, BenchmarkSuite.name == suite["name"],
        )))
        for existing in revisions:
            if existing.content_hash == digest:
                return existing
        snapshot = BenchmarkSuite(
            workspace_id=workspace_id, name=suite["name"], schema_version=1,
            revision=max((r.revision for r in revisions), default=0) + 1,
            content_hash=digest, tasks=suite["tasks"],
        )
        session.add(snapshot)
        session.flush()
        return snapshot


def export_history(workspace_id: str, repository: str, name: str,
                   output_root: str | Path | None = None, report=print) -> Path:
    """Export only stored evidence; exclusions are reported even if export is empty."""
    _safe_name(name, "Suite name")
    root = Path(output_root) if output_root is not None else default_output_root()
    tasks = []
    excluded: Counter[str] = Counter()
    with session_scope() as session:
        if session.get(Workspace, workspace_id) is None:
            raise SuiteError("Workspace not found")
        repo = session.scalar(select(Repository).where(
            Repository.workspace_id == workspace_id, Repository.full_name == repository,
        ))
        if repo is None:
            raise SuiteError("Repository not in workspace")
        records = session.scalars(select(Task).where(
            Task.workspace_id == workspace_id, Task.repo_id == repo.id,
        ).order_by(Task.id))
        for task in records:
            if task.status != "completed":
                excluded["not completed"] += 1
                continue
            if not task.base_sha:
                excluded["missing base_sha"] += 1
                continue
            if not task.merge_sha or not task.pr_number or not task.pr_url:
                excluded["missing merge/PR evidence"] += 1
                continue
            candidate = {
                "key": f"task-{task.id}", "repository": repo.full_name,
                "base_commit": task.base_sha, "title": task.title,
                "specification": task.spec, "acceptance": task.acceptance,
                "reference_change": {"pr_number": task.pr_number, "pr_url": task.pr_url, "merge_sha": task.merge_sha},
            }
            try:
                validate_suite({"schema_version": 1, "name": name, "tasks": [candidate]})
            except SuiteError as exc:
                excluded[f"invalid stored record: {exc}"] += 1
                continue
            tasks.append(candidate)
    report(f"Included {len(tasks)} task(s); excluded {sum(excluded.values())} task(s)")
    for reason, count in sorted(excluded.items()):
        report(f"  {count}: {reason}")
    if not tasks:
        raise SuiteError("No complete delivered tasks to export")
    suite = validate_suite({"schema_version": 1, "name": name, "tasks": tasks})
    directory = root / name
    directory.mkdir(parents=True, exist_ok=True)
    destination = directory / "suite.json"
    # Write-then-rename prevents a failed export from leaving partial JSON.
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", dir=directory, delete=False) as stream:
            temporary = stream.name
            stream.write(json.dumps(suite, indent=2, ensure_ascii=False) + "\n")
        os.replace(temporary, destination)
    finally:
        if temporary and os.path.exists(temporary):
            os.unlink(temporary)
    return destination
