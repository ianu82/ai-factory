"""Bounded, regular-file-only transport across the execution boundary.

Generated environments stay on the worker. They are never deserialized onto the
credential-bearing controller, and executable source modes remain part of review.
"""
from __future__ import annotations

import base64
import binascii
import shutil
from pathlib import Path, PurePosixPath

from .product_workspace import (GENERATED_DIRECTORIES, WorkspaceError,
                                executable_paths, is_git_metadata_path, source_files)


def pack_source(root: Path) -> list[list]:
    files = source_files(root)
    executable = executable_paths(root, files)
    return [[name, base64.b64encode(content).decode("ascii"), name in executable]
            for name, content in sorted(files.items())]


def unpack_source(document: object) -> tuple[dict[str, bytes], set[str]]:
    """Validate the entire message before touching a local filesystem."""
    if not isinstance(document, list) or len(document) > 10000:
        raise WorkspaceError("Invalid source transport")
    files, executable, total = {}, set(), 0
    for row in document:
        if (not isinstance(row, list) or len(row) != 3 or not isinstance(row[0], str)
                or not isinstance(row[1], str) or type(row[2]) is not bool):
            raise WorkspaceError("Invalid source entry")
        name, encoded, mode = row
        path = PurePosixPath(name)
        if (not path.parts or str(path) != name or path.is_absolute() or ".." in path.parts
                or "\\" in name or "\0" in name or name in files
                or is_git_metadata_path(name) or any(p in GENERATED_DIRECTORIES for p in path.parts)
                or len(encoded) > 4 * ((4 * 1024 * 1024 + 2) // 3)):
            raise WorkspaceError("Unsupported source path or size")
        try:
            content = base64.b64decode(encoded, validate=True)
        except (ValueError, binascii.Error):
            raise WorkspaceError("Invalid source encoding") from None
        total += len(content)
        if len(content) > 4 * 1024 * 1024 or total > 64 * 1024 * 1024:
            raise WorkspaceError("Source transport exceeds size limits")
        files[name] = content
        if mode:
            executable.add(name)
    for name in files:
        if any(str(parent) in files for parent in PurePosixPath(name).parents):
            raise WorkspaceError("Source contains a file/directory collision")
    return files, executable


def write_source(root: Path, document: object) -> None:
    """Replace a disposable source export; never use for a controller checkout."""
    files, executable = unpack_source(document)
    if root.is_symlink():
        raise WorkspaceError("Source root must not be a link")
    # The caller owns this temporary directory. No candidate runs on this host.
    # Clearing it also handles deleted files and file-to-directory transitions.
    if root.exists():
        source_files(root)  # refuse Git metadata, links or special files
        shutil.rmtree(root)
    root.mkdir(parents=True)
    for name, content in files.items():
        target = root / name
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(content)
        target.chmod(0o755 if name in executable else 0o644)
