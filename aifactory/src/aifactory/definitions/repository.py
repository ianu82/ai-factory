"""Read-only repository definition ingestion; never checkout or execute repository files.

The loader consumes Git objects at one immutable commit. The sync service does
network/object reads outside transactions, then rechecks intent, ownership and
revision under the workspace lock before atomically publishing a whole bundle.
"""
from __future__ import annotations

import asyncio
import hashlib
import re
from pathlib import Path
from urllib.parse import quote

from .. import gitops
from ..db import session_scope, utcnow
from ..github import GitHub, resolve_credential
from ..models import DefinitionState, Repository
from ..security import redact
from .schema import Bundle, KnowledgePage, RoleInstruction, Skill, Source
from .sources import configuration
from .store import RevisionConflict, get_snapshot, lock_repositories, lock_workspace, save_bundle
from .validation import (
    BUNDLE_LIMIT, MAX_FOLDER_ENTRIES, DefinitionError, parse_frontmatter, safe_path, validate_bundle,
)

_SHA = re.compile(r"[0-9a-f]{40}(?:[0-9a-f]{24})?\Z")
_REPOSITORY = re.compile(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+\Z")
_ROLES = {"planner", "builder", "verifier"}
_SYNC_TIMEOUT_SECONDS = 120
_POLL_CONCURRENCY = 4


def _id(path: str) -> str:
    return "git-" + hashlib.sha256(path.encode()).hexdigest()[:32]


def bundle_from_files(files: dict[str, str], *, repository_id: str, commit: str,
                      folder: str, repositories: dict[str, str]) -> Bundle:
    """Parse the documented layout, using the same scalar parser and validator as managed saves.

    SKILL.md requires name/description; pages and instructions are plain Markdown
    or use the same optional frontmatter. Only repository adds a scope; roles
    always come from paths. Repository names resolve solely against owned rows.
    """
    bundle = Bundle()
    consumed: set[str] = set()
    for path in sorted(files):
        safe_path(path)
        parts = path.split("/")
        role = None
        skill_root = None
        if len(parts) >= 3 and parts[0] == "skills" and parts[2] == "SKILL.md" and len(parts) == 3:
            skill_root = "/".join(parts[:2])
        elif parts[0] == "agents":
            if len(parts) < 3 or parts[1] not in _ROLES:
                raise DefinitionError("Unknown agent role or layout", path=path, field="role")
            role = parts[1]
            if len(parts) == 5 and parts[2] == "skills" and parts[4] == "SKILL.md":
                skill_root = "/".join(parts[:4])
        is_instruction = len(parts) == 3 and parts[0] == "agents" and parts[2] == "instructions.md"
        is_page = len(parts) >= 2 and parts[0] == "knowledge" and path.endswith(".md")
        if not (skill_root or is_instruction or is_page):
            continue
        text = files[path]
        try:
            fields, body = (parse_frontmatter(text, allow_repository=True, require_metadata=bool(skill_root))
                            if skill_root or text.splitlines()[:1] == ["---"] else ({}, text))
            scoped_name = fields.pop("repository", None)
            scope = None
            if scoped_name is not None:
                if not _REPOSITORY.fullmatch(scoped_name) or scoped_name.casefold() not in repositories:
                    raise DefinitionError("Repository scope must name a connected repository in this workspace",
                                          field="repository")
                scope = repositories[scoped_name.casefold()]
            source = Source(kind="repository", repository_id=repository_id,
                            path=f"{folder}/{path}", commit=commit)
            common = dict(id=_id(path), role=role, repository_id=scope, source=source)
            if skill_root:
                adjacent = {name[len(skill_root) + 1:]: content for name, content in files.items()
                            if name.startswith(skill_root + "/") and name != path}
                bundle.skills.append(Skill(**fields, body=body, files=adjacent, **common))
                consumed.update(skill_root + "/" + name for name in adjacent)
            elif is_instruction:
                bundle.instructions.append(RoleInstruction(
                    name=fields.get("name", f"{role}-instructions"),
                    description=fields.get("description", f"Instructions for {role}"), body=body, **common))
            else:
                # A path hash avoids collisions between equal basenames in nested folders.
                bundle.knowledge.append(KnowledgePage(
                    name=fields.get("name", "page-" + _id(path)[4:]),
                    description=fields.get("description", path), body=body, **common))
            consumed.add(path)
        except DefinitionError as exc:
            raise DefinitionError(str(exc), path=path, field=exc.field) from exc
    # Unknown files inside the documented definition trees are likely mistakes,
    # not silent deletions. README etc. beside these trees may be ignored.
    for path in sorted(files.keys() - consumed):
        if path.split("/")[0] in {"skills", "agents", "knowledge"}:
            raise DefinitionError("Unsupported definition path or missing SKILL.md", path=path, field="path")
    return validate_bundle(bundle, set(repositories.values()))


async def read_repository(mirror: Path, commit: str, folder: str) -> dict[str, str]:
    """Bound tree listings and cumulative blob sizes *before* reading blob data.

    Traverse trees explicitly (including folder ancestors) to catch symlinks and
    submodules, without following them or materializing any repository paths.
    Only regular blobs below the configured folder are read. No attributes,
    filters, hooks, submodule operations or working trees are involved.
    """
    if not _SHA.fullmatch(commit):
        raise DefinitionError("Expected an immutable commit SHA")
    safe_path(folder)
    count = 0
    metadata_bytes = 0

    async def tree(oid, name=None):
        nonlocal count, metadata_bytes
        data = await gitops.read_object("ls-tree", "-z", oid, *(["--", name] if name else []), cwd=mirror,
                                        limit=BUNDLE_LIMIT - metadata_bytes)
        metadata_bytes += len(data)
        entries = []
        for record in data.split(b"\0"):
            if not record:
                continue
            count += 1
            if count > MAX_FOLDER_ENTRIES:
                raise DefinitionError(f"Definition folder exceeds {MAX_FOLDER_ENTRIES} tree entries")
            header, raw_name = record.split(b"\t", 1)
            mode, kind, object_id = header.decode("ascii").split()
            try:
                name = raw_name.decode("utf-8")
            except UnicodeError as exc:
                raise DefinitionError("Definition paths must be UTF-8") from exc
            safe_path(name)
            if "/" in name:
                raise DefinitionError("Unsafe Git tree entry")
            entries.append((name, mode, kind, object_id))
        return entries

    oid = commit
    for part in folder.split("/"):
        selected = next((e for e in await tree(oid, part) if e[0] == part), None)
        if selected is None:
            # Removing the folder is a valid empty snapshot.
            return {}
        _, mode, kind, oid = selected
        if mode != "040000" or kind != "tree":
            raise DefinitionError("Definition folder ancestor is not a regular Git tree", path=folder)
    files: dict[str, str] = {}
    used = 0
    pending = [(oid, "")]
    while pending:
        oid, prefix = pending.pop()
        for name, mode, kind, child in await tree(oid):
            path = prefix + name
            safe_path(path)
            used += len(path.encode("utf-8"))
            if len(path) > 1000 or used > BUNDLE_LIMIT:
                raise DefinitionError("Definition folder paths exceed size limit", path=path[:1000])
            if kind == "tree" and mode == "040000":
                pending.append((child, path + "/"))
            elif kind == "blob" and mode in {"100644", "100755"}:
                size = int((await gitops.read_object("cat-file", "-s", child, cwd=mirror, limit=100)).strip())
                if size > BUNDLE_LIMIT - used:
                    raise DefinitionError(f"Definition folder exceeds {BUNDLE_LIMIT} bytes", path=path)
                data = await gitops.read_object("cat-file", "blob", child, cwd=mirror, limit=size)
                used += len(data)
                try:
                    text = data.decode("utf-8")
                except UnicodeError as exc:
                    raise DefinitionError("Definitions must be UTF-8 text", path=path) from exc
                if "\x00" in text:
                    raise DefinitionError("Definitions must not contain binary content", path=path)
                files[path] = text
            else:
                raise DefinitionError("Symlinks, submodules and special Git entries are forbidden", path=path)
    return files


async def sync_definitions(workspace_id: str) -> dict:
    """Common admin/poller service. Invalid attempts commit diagnostics, never content.

    This owns short transactions, not a caller's session. Source/revision changes
    during I/O discard the candidate, including stale diagnostics.
    """
    with session_scope() as db:
        config = configuration(db, workspace_id)
        desired = config["desired_source"]
        if desired["kind"] != "repository":
            raise RevisionConflict("Select a repository source before syncing")
        revision = get_snapshot(db, workspace_id).revision
        repo = db.query(Repository).filter_by(id=desired.get("repository_id"), workspace_id=workspace_id).first()
        # Ownership is checked BEFORE credentials, network or shared mirror access.
        if repo is None:
            raise DefinitionError("Repository not found in this workspace", field="repository_id")
        full_name, branch, repository_id = repo.full_name, repo.default_branch, repo.id
        repositories = {r.full_name.casefold(): r.id for r in db.query(Repository).filter_by(workspace_id=workspace_id)}
    sha = None
    author = None
    candidate = None
    errors = []
    attempt_at = utcnow().isoformat()

    def checked_state(db):
        lock_workspace(db, workspace_id)
        owned = lock_repositories(db, workspace_id)
        state = db.get(DefinitionState, workspace_id, populate_existing=True)
        if (state is None or state.revision != revision or state.desired_source != desired
                or {r.full_name.casefold(): r.id for r in owned} != repositories):
            raise RevisionConflict("Definitions or connected repositories changed during sync; retry")
        return state

    def attempt_diagnostics(previous, status):
        # Only last-valid/success metadata survives between attempts. An early
        # failure must never attribute this SHA to the previous commit's author.
        return {**{key: previous[key] for key in (
                    "last_valid_sha", "last_valid_revision", "last_valid_git_author", "last_success_at")
                   if key in previous},
                "status": status, "last_attempt_at": attempt_at,
                "attempted_sha": sha, "commit": sha, "errors": errors,
                "git_author": author, "attempted_git_author": author,
                "source_url": (f"https://github.com/{full_name}/tree/{sha}/"
                               f"{quote(desired['folder'], safe='/')}" if sha else None)}

    try:
        # Bound each workspace's I/O independently, for admin and background sync
        # alike. External cancellation is not caught, so shutdown stays prompt.
        async with asyncio.timeout(_SYNC_TIMEOUT_SECONDS):
            if not _REPOSITORY.fullmatch(full_name):
                raise DefinitionError("Unsafe repository name")
            safe_path(desired["folder"])
            credential = resolve_credential(workspace_id)
            github = GitHub(credential)
            # Re-prove this workspace's remote access even when another workspace has
            # already cached the same private repository in the shared mirror.
            remote = await github.repo(full_name)
            if remote.get("full_name", "").casefold() != full_name.casefold():
                raise DefinitionError("Repository identity changed; reconnect before syncing")
            branch = remote.get("default_branch") or branch
            sha = await github.branch_sha(full_name, branch)
            if not _SHA.fullmatch(sha):
                raise DefinitionError("Expected an immutable default-branch commit SHA")
            with session_scope() as db:
                state = checked_state(db)
                previous = state.sync_diagnostics or {}
                state.sync_diagnostics = attempt_diagnostics(previous, "syncing")
                active = state.active_source or {}
                unchanged = (active == {**desired, "commit": sha})
                if unchanged:
                    # A force-push may return to the last-valid commit after a bad
                    # attempt. Recover attribution from the immutable active snapshot,
                    # not diagnostics (which may describe that failed attempt).
                    author = (previous.get("last_valid_git_author")
                              if previous.get("last_valid_sha") == sha else None) or get_snapshot(db, workspace_id).author
            if not unchanged:
                mirror = await gitops.mirror(full_name, credential)
                # Cooperate with mirror fetch/clone operations. Nothing is copied to a sandbox.
                async with gitops._mirror_lock(mirror):
                    author = redact((await gitops.read_object("show", "-s", "--format=%an <%ae>", sha,
                                                              cwd=mirror, limit=8192)).decode("utf-8").strip())
                    files = await read_repository(mirror, sha, desired["folder"])
                candidate = bundle_from_files(files, repository_id=repository_id, commit=sha,
                                              folder=desired["folder"], repositories=repositories)
    except TimeoutError:
        errors = [{"path": "source", "field": "source",
                   "message": "Repository definition sync timed out; retry"}]
    except (DefinitionError, gitops.GitError, UnicodeError, ValueError) as exc:
        errors = [exc.detail() if isinstance(exc, DefinitionError) else
                  {"path": "source", "field": "source", "message": "Unable to read repository definitions"}]
        errors = [{key: redact(value)[:2000] for key, value in error.items()} for error in errors]
    except Exception:
        # Transport/credential failures can contain headers or response bodies.
        # Never persist those; the actionable source-level diagnostic is enough.
        errors = [{"path": "source", "field": "source",
                   "message": "Repository access failed; check workspace GitHub access and retry"}]
    with session_scope() as db:
        state = checked_state(db)
        previous = state.sync_diagnostics or {}
        diagnostics = attempt_diagnostics(previous, "invalid" if errors else "synced")
        if not errors:
            if candidate is not None:
                # save_bundle runs T1's exact validator again under repository locks,
                # including the live team-note projection and its workspace quota.
                try:
                    with db.begin_nested():
                        snapshot = save_bundle(db, workspace_id, candidate, expected_revision=revision,
                                               author=author or "Git author unavailable",
                                               active_source={**desired, "commit": sha})
                    diagnostics.update(last_valid_sha=sha, last_valid_revision=snapshot.revision,
                                       last_valid_git_author=author)
                except DefinitionError as exc:
                    diagnostics.update(status="invalid", errors=[{
                        key: redact(value)[:2000] for key, value in exc.detail().items()}])
            if diagnostics["status"] == "synced":
                diagnostics["last_success_at"] = utcnow().isoformat()
        state.sync_diagnostics = diagnostics
        db.flush()
        return configuration(db, workspace_id)


async def poll_definitions() -> None:
    """No signal auto_sync or paused-workspace gates: these are read-only updates."""
    import logging

    with session_scope() as db:
        targets = [state.workspace_id for state in db.query(DefinitionState)
                   if (state.desired_source or {}).get("kind") == "repository"]
    semaphore = asyncio.Semaphore(_POLL_CONCURRENCY)

    async def sync_target(workspace_id):
        async with semaphore:
            try:
                await sync_definitions(workspace_id)
            except Exception:
                logging.getLogger(__name__).warning("Definition sync deferred for workspace %s", workspace_id)

    await asyncio.gather(*(sync_target(workspace_id) for workspace_id in targets))
