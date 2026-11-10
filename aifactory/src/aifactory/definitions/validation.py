"""Restricted frontmatter and bounded, non-following UTF-8 file ingestion."""
from __future__ import annotations

import json
import os
import re
import stat
from pathlib import Path, PurePosixPath

from .schema import Bundle, Skill

SKILL_BODY_LIMIT = 65_536
BUNDLE_LIMIT = 1_048_576
MAX_FOLDER_ENTRIES = 2_000
_NAME = re.compile(r"[a-zA-Z0-9][a-zA-Z0-9._-]{0,99}\Z")
# Supporting files are text, never executable/binary/special filesystem objects.
_SUFFIXES = {".md", ".txt", ".json", ".csv", ".toml", ".py", ".sh", ".sql", ".js", ".ts", ".html", ".css"}


class DefinitionError(ValueError):
    pass


def utf8_size(text: str) -> int:
    try:
        return len(text.encode("utf-8"))
    except UnicodeError as exc:
        raise DefinitionError("Definitions must be valid UTF-8") from exc


def safe_path(path: str) -> None:
    if (not path or "\\" in path or "\x00" in path or any(ord(c) < 32 for c in path)
            or PurePosixPath(path).is_absolute() or ":" in path
            or any(p in {"", ".", ".."} for p in path.split("/"))):
        raise DefinitionError(f"Unsafe relative path: {path!r}")


def parse_frontmatter(text: str) -> tuple[dict[str, str], str]:
    """Only name/description scalar strings, no YAML collections, tags or aliases."""
    utf8_size(text)
    lines = text.splitlines(keepends=True)
    if not lines or lines[0].rstrip("\r\n") != "---":
        raise DefinitionError("SKILL.md must start with --- frontmatter")
    fields: dict[str, str] = {}
    for index, line in enumerate(lines[1:], 1):
        line = line.rstrip("\r\n")
        if line == "---":
            for key in ("name", "description"):
                if not fields.get(key, "").strip():
                    raise DefinitionError(f"Frontmatter requires nonempty {key}")
            if not _NAME.fullmatch(fields["name"]):
                raise DefinitionError("Skill name must be a safe 1-100 character name")
            body = "".join(lines[index + 1:])
            if utf8_size(body) > SKILL_BODY_LIMIT:
                raise DefinitionError(f"Skill body exceeds {SKILL_BODY_LIMIT} UTF-8 bytes")
            return fields, body
        match = re.fullmatch(r"([a-z_]+):[ \t]*(.*)", line)
        if not match:
            raise DefinitionError(f"Malformed frontmatter line {index + 1}: expected key: value")
        key, value = match.groups()
        if key not in {"name", "description"}:
            raise DefinitionError(f"Unsupported frontmatter field: {key}")
        if key in fields:
            raise DefinitionError(f"Duplicate frontmatter field: {key}")
        value = value.strip()
        if value.startswith('"'):
            try:
                value = json.loads(value)
            except ValueError as exc:
                raise DefinitionError(f"Invalid quoted scalar for {key}") from exc
            if not isinstance(value, str):
                raise DefinitionError(f"Expected string for {key}")
        elif value.startswith("'"):
            if not re.fullmatch(r"'(?:[^']|'')*'", value):
                raise DefinitionError(f"Invalid quoted scalar for {key}")
            value = value[1:-1].replace("''", "'")
        elif (not value or any(c in value for c in "[]{}&*!|>#'\"")
              or re.search(r":\s", value) or value.startswith(("-", "?", "%", "@", "`"))
              or value.lower() in {"null", "true", "false", "~"}):
            raise DefinitionError(f"Unsupported YAML scalar for {key}; quote a simple string")
        fields[key] = value
    raise DefinitionError("Unterminated SKILL.md frontmatter (missing ---)")


def render_skill(skill: Skill) -> str:
    return (f"---\nname: {json.dumps(skill.name, ensure_ascii=False)}\n"
            f"description: {json.dumps(skill.description, ensure_ascii=False)}\n---\n{skill.body}")


def bundle_size(bundle: Bundle) -> int:
    """Quota is all UTF-8 string values and support-file paths, including metadata.

    JSON punctuation/schema keys do not count. IDs and source metadata do; nothing
    user-controlled can hide unbounded content outside the active quota.
    """
    def size(value):
        if isinstance(value, str):
            return utf8_size(value)
        if isinstance(value, list):
            return sum(size(v) for v in value)
        if isinstance(value, dict):
            return sum(size(v) + (sum(utf8_size(k) for k in v) if k == "files" else 0)
                       for k, v in value.items())
        return 0
    # Count rendered frontmatter punctuation as well, so exported skill files fit.
    return size(bundle.model_dump()) + sum(
        utf8_size(render_skill(s)) - utf8_size(s.body) - utf8_size(s.name) - utf8_size(s.description)
        for s in bundle.skills
    )


def validate_bundle(bundle: Bundle, repository_ids: set[str]) -> Bundle:
    ids: set[str] = set()
    names: set[tuple] = set()
    for kind, entries in (("skill", bundle.skills), ("instruction", bundle.instructions),
                          ("knowledge", bundle.knowledge)):
        for entry in entries:
            if not _NAME.fullmatch(entry.id) or entry.id in ids:
                raise DefinitionError(f"Unsafe or duplicate definition ID: {entry.id!r}")
            ids.add(entry.id)
            if not _NAME.fullmatch(entry.name) or not entry.description.strip():
                raise DefinitionError(f"{entry.id}: require safe name and nonempty description")
            key = (kind, entry.name, entry.role, entry.repository_id)
            if key in names:
                raise DefinitionError(f"Duplicate {kind} name {entry.name!r} in the same role/repository scope")
            names.add(key)
            for repo_id in (entry.repository_id, entry.source.repository_id):
                if repo_id is not None and repo_id not in repository_ids:
                    raise DefinitionError(f"{entry.name}: repository {repo_id} does not belong to this workspace")
            if entry.source.kind != "managed" and not entry.source.repository_id:
                raise DefinitionError(f"{entry.name}: repository source requires repository_id")
            if entry.source.path is not None:
                safe_path(entry.source.path)
            if entry.source.kind == "repository_context":
                if (kind != "knowledge" or entry.repository_id != entry.source.repository_id
                        or entry.role is not None):
                    raise DefinitionError("repository_context must be a knowledge page scoped only to its source repository")
            if isinstance(entry, Skill):
                if utf8_size(entry.body) > SKILL_BODY_LIMIT:
                    raise DefinitionError(f"{entry.name}: skill body exceeds {SKILL_BODY_LIMIT} UTF-8 bytes")
                for path, content in entry.files.items():
                    if "\x00" in content:
                        raise DefinitionError(f"Unsupported binary file: {path}")
                    safe_path(path)
                    if path == "SKILL.md" or PurePosixPath(path).suffix.lower() not in _SUFFIXES:
                        raise DefinitionError(f"Unsupported supporting file: {path}")
    if bundle_size(bundle) > BUNDLE_LIMIT:
        raise DefinitionError(f"Active workspace bundle exceeds {BUNDLE_LIMIT} UTF-8 bytes")
    return bundle


def read_skill_folder(folder: Path, **scope) -> Skill:
    """Bound cumulative raw reads *before* decoding, retaining or opening more files.

    Directory descriptors plus O_NOFOLLOW prevent symlink swaps as well as static
    symlinks. Special files are rejected without blocking on FIFOs.
    """
    files: dict[str, str] = {}
    used = 0
    entry_count = 0

    def walk(fd: int, prefix: str = ""):
        nonlocal used, entry_count
        # Enumerate lazily and retain only a bounded set before sorting. Charge
        # directories too: deeply nested/empty trees must not bypass the limits.
        names = []
        with os.scandir(fd) as entries:
            for entry in entries:
                entry_count += 1
                if entry_count > MAX_FOLDER_ENTRIES:
                    raise DefinitionError(f"Skill folder exceeds {MAX_FOLDER_ENTRIES} directory entries")
                path = prefix + entry.name
                safe_path(path)
                used += utf8_size(path)
                if used > BUNDLE_LIMIT:
                    raise DefinitionError(f"Skill folder exceeds {BUNDLE_LIMIT} UTF-8 bytes")
                names.append(entry.name)
        for name in sorted(names):
            path = prefix + name
            info = os.stat(name, dir_fd=fd, follow_symlinks=False)
            if stat.S_ISDIR(info.st_mode):
                child = os.open(name, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=fd)
                try:
                    walk(child, path + "/")
                finally:
                    os.close(child)
            elif stat.S_ISREG(info.st_mode):
                if PurePosixPath(path).suffix.lower() not in _SUFFIXES:
                    raise DefinitionError(f"Unsupported supporting file: {path}")
                child = os.open(name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=fd)
                with os.fdopen(child, "rb") as stream:
                    if not stat.S_ISREG(os.fstat(stream.fileno()).st_mode):
                        raise DefinitionError(f"Unsupported file: {path}")
                    data = stream.read(BUNDLE_LIMIT - used + 1)
                used += len(data)
                if used > BUNDLE_LIMIT:
                    raise DefinitionError(f"Skill folder exceeds {BUNDLE_LIMIT} UTF-8 bytes")
                try:
                    files[path] = data.decode("utf-8")
                except UnicodeError as exc:
                    raise DefinitionError(f"{path}: supporting files must be UTF-8 text") from exc
                if "\x00" in files[path]:
                    raise DefinitionError(f"Unsupported binary file: {path}")
            else:
                raise DefinitionError(f"Symlink or unsupported file: {path}")

    try:
        # Reject symlinks anywhere in the supplied root path, not just its leaf.
        # Ancestors need only search permission: O_PATH opens them without read
        # access (an isolated sandbox user may traverse but not list them). Only
        # the skill folder itself is opened for reading.
        root = Path(folder).absolute()
        parts = root.parts[1:]
        search = getattr(os, "O_PATH", os.O_RDONLY)
        fd = os.open(root.anchor, (search if parts else os.O_RDONLY) | os.O_DIRECTORY)
        try:
            for index, part in enumerate(parts):
                mode = os.O_RDONLY if index == len(parts) - 1 else search
                child = os.open(part, mode | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=fd)
                os.close(fd)
                fd = child
                # O_PATH | O_NOFOLLOW can return a descriptor for a symlink itself.
                if not stat.S_ISDIR(os.fstat(fd).st_mode):
                    raise DefinitionError(f"Symlink or unsupported path in skill folder: {part}")
            walk(fd)
        finally:
            os.close(fd)
    except OSError as exc:
        raise DefinitionError(f"Unsafe or unreadable skill folder: {exc}") from exc
    if "SKILL.md" not in files:
        raise DefinitionError("Skill folder requires SKILL.md")
    fields, body = parse_frontmatter(files.pop("SKILL.md"))
    skill = Skill(**fields, body=body, files=files, **scope)
    validate_bundle(Bundle(skills=[skill]), {r for r in (skill.repository_id, skill.source.repository_id) if r})
    return skill
