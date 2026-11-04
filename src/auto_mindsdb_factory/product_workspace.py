"""Export source without exposing Git credentials or the controller's checkout."""

from __future__ import annotations

import hashlib
import io
import os
import re
import stat
import subprocess
import tarfile
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
    return result.stdout.strip()


def export_source(repository: Path, commit: str, destination: Path) -> None:
    if not re.fullmatch(r"[0-9a-f]{40}", commit):
        raise WorkspaceError("Source export requires a full commit SHA")
    destination.mkdir(parents=True, exist_ok=True)
    if any(destination.iterdir()):
        raise WorkspaceError("Source export requires an empty destination")
    result = subprocess.run(["git", "-C", str(repository), "archive", commit], capture_output=True, timeout=120)
    if result.returncode:
        raise WorkspaceError("Could not export the pinned source")
    with tarfile.open(fileobj=io.BytesIO(result.stdout)) as archive:
        for member in archive:
            path = PurePosixPath(member.name)
            if path.is_absolute() or ".." in path.parts or ".git" in path.parts or not (member.isfile() or member.isdir()):
                raise WorkspaceError("Source contains an unsupported path, link or submodule")
            target = destination / member.name
            if member.isdir():
                target.mkdir(parents=True, exist_ok=True)
            else:
                target.parent.mkdir(parents=True, exist_ok=True)
                stream = archive.extractfile(member)
                assert stream is not None
                target.write_bytes(stream.read())
                target.chmod(0o755 if member.mode & 0o111 else 0o644)


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


def source_digest(files: dict[str, bytes]) -> str:
    digest = hashlib.sha256()
    for name, content in sorted(files.items()):
        digest.update(name.encode() + b"\0" + hashlib.sha256(content).digest())
    return digest.hexdigest()
