"""Run candidate-authored code under a restricted local execution account."""

from __future__ import annotations

import os
import subprocess
from pathlib import Path


class ExecutionAccountError(RuntimeError):
    pass


class ExecutionAccount:
    def __init__(self, user: str | None, *, sudo_bin: str = "sudo", ownership_run=subprocess.run):
        self.user = user
        self.sudo_bin = sudo_bin
        self.ownership_run = ownership_run

    def command(self, command: list[str]) -> list[str]:
        if not self.user:
            return command
        return [self.sudo_bin, "-H", "-u", self.user, "--", *command]

    def prepare_directory(self, path: Path) -> bool:
        if not self.user:
            return False
        self._set_owner(path, self.user)
        return True

    def restore_directory(self, path: Path) -> None:
        self._set_owner(path, f"{os.getuid()}:{os.getgid()}")

    def _set_owner(self, path: Path, owner: str) -> None:
        try:
            result = self.ownership_run(
                [self.sudo_bin, "chown", "-R", owner, str(path)],
                check=False, capture_output=True, text=True, timeout=60,
            )
        except (OSError, subprocess.TimeoutExpired) as exc:
            raise ExecutionAccountError(f"Could not set execution workspace ownership: {exc}") from exc
        if result.returncode:
            detail = result.stderr.strip() or result.stdout.strip() or f"exit {result.returncode}"
            raise ExecutionAccountError(f"Could not set execution workspace ownership: {detail}")
