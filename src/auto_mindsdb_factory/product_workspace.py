"""Export source without exposing Git credentials or the controller's checkout."""

from __future__ import annotations

import hashlib
import os
import re
import stat
import subprocess
import tempfile
from contextlib import contextmanager
from pathlib import Path, PurePosixPath


class WorkspaceError(RuntimeError):
    pass


def git(root: Path, *args: str) -> str:
    result = subprocess.run(
        ["git", "-c", "core.hooksPath=/dev/null", "-c", "core.fsmonitor=false", "-C", str(root), *args],
        capture_output=True, text=True, timeout=120,
    )
    if result.returncode:
        raise WorkspaceError(f"Git operation failed: {args[0]}")
    return result.stdout.rstrip("\n")


@contextmanager
def controller_worktree(repository: Path, sha: str):
    """Only trusted controller code may edit Git metadata or create commits."""
    with tempfile.TemporaryDirectory(prefix="ai-factory-commit-") as temporary:
        workspace = Path(temporary) / "repo"
        git(repository, "worktree", "add", "--detach", str(workspace), sha)
        try:
            yield workspace
        finally:
            git(repository, "worktree", "remove", "--force", str(workspace))


def export_source(repository: Path, commit: str, destination: Path) -> None:
    if not re.fullmatch(r"[0-9a-f]{40}", commit):
        raise WorkspaceError("Source export requires a full commit SHA")
    destination.mkdir(parents=True, exist_ok=True)
    if any(destination.iterdir()):
        raise WorkspaceError("Source export requires an empty destination")
    result = subprocess.run(["git", "-C", str(repository), "ls-tree", "-rzl", "--full-tree", commit], capture_output=True, timeout=120)
    if result.returncode:
        raise WorkspaceError("Could not export the pinned source")
    entries, total = [], 0
    for row in result.stdout.split(b"\0"):
        if not row:
            continue
        header, _, name = row.partition(b"\t")
        mode, kind, oid, size_raw = header.split()
        path = PurePosixPath(name.decode("utf-8"))
        if (mode not in {b"100644", b"100755"} or kind != b"blob" or path.is_absolute()
                or ".." in path.parts or ".git" in path.parts
                or any(part in GENERATED_DIRECTORIES for part in path.parts)):
            raise WorkspaceError("Source contains an unsupported path, tracked cache, link or submodule")
        size = int(size_raw)
        total += size
        if size > 4 * 1024 * 1024 or total > 64 * 1024 * 1024 or len(entries) >= 10000:
            raise WorkspaceError("Repository exceeds source size limits")
        entries.append((path, oid, size, mode))
    if not entries:
        raise WorkspaceError("Repository source is empty")
    # Read raw Git objects: archive's export-ignore/export-subst attributes must
    # not let a candidate hide or transform committed source during verification.
    result = subprocess.run(["git", "-C", str(repository), "cat-file", "--batch"],
        input=b"\n".join(oid for _, oid, _, _ in entries) + b"\n", capture_output=True, timeout=120)
    if result.returncode:
        raise WorkspaceError("Could not read pinned source objects")
    cursor = 0
    for path, oid, size, mode in entries:
        end = result.stdout.find(b"\n", cursor)
        if end == -1 or result.stdout[cursor:end].split() != [oid, b"blob", str(size).encode()]:
            raise WorkspaceError("Pinned object evidence is incomplete")
        cursor = end + 1
        content = result.stdout[cursor:cursor + size]
        if len(content) != size or result.stdout[cursor + size:cursor + size + 1] != b"\n":
            raise WorkspaceError("Pinned source object is truncated")
        target = destination / str(path)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(content)
        target.chmod(0o755 if mode == b"100755" else 0o644)
        cursor += size + 1


GENERATED_DIRECTORIES = {"node_modules", ".venv", "__pycache__", ".pytest_cache", ".next"}


def source_files(root: Path) -> dict[str, bytes]:
    """Never follow candidate links when importing edits into a trusted checkout."""
    files = {}
    total = 0
    for directory, dirs, names in os.walk(root, followlinks=False):
        for name in list(dirs):
            path = Path(directory) / name
            if name in GENERATED_DIRECTORIES:
                dirs.remove(name)
                continue
            if path.is_symlink() or name == ".git":
                raise WorkspaceError("Candidate contains a link or Git metadata")
        for name in names:
            path = Path(directory) / name
            mode = path.lstat().st_mode
            if not stat.S_ISREG(mode) or name == ".git":
                raise WorkspaceError("Candidate contains a special file or Git metadata")
            size = path.stat().st_size
            total += size
            if size > 4 * 1024 * 1024 or total > 64 * 1024 * 1024 or len(files) >= 10000:
                raise WorkspaceError("Candidate exceeds source size limits")
            files[path.relative_to(root).as_posix()] = path.read_bytes()
    return files


def executable_paths(root: Path, files: dict[str, bytes]) -> set[str]:
    return {path for path in files if (root / path).stat().st_mode & 0o111}


def source_digest(files: dict[str, bytes], executables: set[str] | None = None) -> str:
    digest = hashlib.sha256()
    for name, content in sorted(files.items()):
        digest.update(name.encode() + b"\0" + hashlib.sha256(content).digest())
        digest.update(b"x" if name in (executables or set()) else b"-")
    return digest.hexdigest()
