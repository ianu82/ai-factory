"""Descriptor-relative, symlink-free sandbox writes (also executed inside VMs)."""


def safe_write(root: str, relative: str, content: bytes, owner: int | None = None) -> None:
    import os
    import secrets
    import stat

    # Never resolve links and then open by name: an agent may still be running.
    parts = relative.split("/")
    if not parts or any(p in ("", ".", "..") for p in parts) or "\x00" in relative:
        raise ValueError("Unsafe sandbox write path")
    directory = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW
    fd = os.open(root, directory)
    temporary = ""
    try:
        for part in parts[:-1]:
            try:
                os.mkdir(part, 0o755, dir_fd=fd)
                if owner is not None:
                    os.chown(part, owner, owner, dir_fd=fd, follow_symlinks=False)
            except FileExistsError:
                pass
            child = os.open(part, directory, dir_fd=fd)
            os.close(fd)
            fd = child
        mode = 0o644
        try:
            existing = os.stat(parts[-1], dir_fd=fd, follow_symlinks=False)
            if not stat.S_ISREG(existing.st_mode):
                raise ValueError("Sandbox write target is not a regular file")
            # Ordinary edits must preserve executable bits, without copying privileged bits.
            mode = stat.S_IMODE(existing.st_mode) & 0o777
        except FileNotFoundError:
            pass
        temporary = ".factory-write-" + secrets.token_hex(16)
        out = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600, dir_fd=fd)
        with os.fdopen(out, "wb") as handle:
            handle.write(content)
            if owner is not None:
                os.fchown(handle.fileno(), owner, owner)
            os.fchmod(handle.fileno(), mode)
        os.replace(temporary, parts[-1], src_dir_fd=fd, dst_dir_fd=fd)
        temporary = ""
    finally:
        if temporary:
            os.unlink(temporary, dir_fd=fd)
        os.close(fd)


def safe_read(root: str, relative: str, limit: int) -> bytes:
    """Bounded regular-file read without following any checkout symlink.

    Self-contained so the exact same check-and-open runs inside a VM. Directory
    descriptors keep path replacement races from redirecting subsequent opens.
    """
    import os
    import stat

    if limit <= 0:
        raise ValueError("Invalid read limit")
    if not isinstance(relative, str) or not relative or "\x00" in relative:
        raise ValueError("Invalid sandbox read path")
    root = os.path.abspath(root)
    # Accept checkout-absolute paths, but reject traversal before normalization.
    if ".." in relative.split("/"):
        raise ValueError("Path escapes the repository")
    if os.path.isabs(relative):
        relative = os.path.relpath(relative, root)
    parts = [p for p in relative.split("/") if p not in ("", ".")]
    if not parts or ".." in parts:
        raise ValueError("Path escapes the repository")
    directory = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW
    fd = os.open(root, directory)
    try:
        for part in parts[:-1]:
            child = os.open(part, directory, dir_fd=fd)
            os.close(fd)
            fd = child
        out = os.open(parts[-1], os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=fd)
        with os.fdopen(out, "rb") as handle:
            info = os.fstat(handle.fileno())
            if not stat.S_ISREG(info.st_mode):
                raise ValueError("Not a regular file")
            if info.st_size > limit:
                raise ValueError("Image is too large; resize it and try again")
            data = handle.read(limit + 1)
            if len(data) > limit:
                raise ValueError("Image is too large; resize it and try again")
            return data
    finally:
        os.close(fd)
