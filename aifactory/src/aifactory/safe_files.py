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
