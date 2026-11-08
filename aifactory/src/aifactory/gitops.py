"""Git plumbing owned by the control plane.

Flow for a task:
  1. ``mirror()`` keeps a bare cache of each repository (fetched with the workspace token).
  2. ``checkout()`` makes a credential-free clone at an exact base commit inside a sandbox.
  3. ``extract_patch()`` reads the job's changes (run as the sandbox identity).
  4. ``publish()`` applies that patch to a pristine control-plane clone, enforces path and
     size policy, commits and pushes. Git hooks/config written by a job can never run here.
"""

from __future__ import annotations

import asyncio
import fnmatch
import os
import shutil
from dataclasses import dataclass
from pathlib import Path

from .config import get_settings
from .github import git_auth_env
from .sandbox import SCRATCH, LocalSandbox, SandboxError

SAFE_GIT = ["-c", "core.hooksPath=/dev/null", "-c", "core.fsmonitor=false", "-c", "protocol.file.allow=always",
            "-c", "advice.detachedHead=false", "-c", "init.defaultBranch=main"]
AUTHOR = {"GIT_AUTHOR_NAME": "AI Factory", "GIT_AUTHOR_EMAIL": "factory@mindshub.ai",
          "GIT_COMMITTER_NAME": "AI Factory", "GIT_COMMITTER_EMAIL": "factory@mindshub.ai"}
DEFAULT_PROTECTED = [".github/workflows/**", ".env", ".env.*", "**/*.pem", "**/*.key"]
# Overridable for tests and GitHub Enterprise; credentials are never embedded in it.
GIT_BASE = os.environ.get("AIFACTORY_GIT_BASE", "https://github.com").rstrip("/")


def remote_url(full_name: str) -> str:
    return f"{GIT_BASE}/{full_name}.git"


class GitError(RuntimeError):
    pass


async def git(*args: str, cwd: Path | None = None, env: dict[str, str] | None = None, timeout: float = 600,
              input_bytes: bytes | None = None) -> str:
    full_env = {"PATH": os.environ.get("PATH", "/usr/bin:/bin"), "HOME": str(get_settings().data_dir),
                "GIT_TERMINAL_PROMPT": "0", "LANG": "C.UTF-8", **(env or {})}
    proc = await asyncio.create_subprocess_exec(
        "git", *SAFE_GIT, *args, cwd=str(cwd) if cwd else None, env=full_env,
        stdin=asyncio.subprocess.PIPE if input_bytes is not None else asyncio.subprocess.DEVNULL,
        stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
    )
    try:
        out, err = await asyncio.wait_for(proc.communicate(input_bytes), timeout=timeout)
    except asyncio.TimeoutError:
        proc.kill()
        raise GitError(f"git {args[0]} timed out")
    if proc.returncode != 0:
        from .security import redact

        raise GitError(f"git {' '.join(a for a in args[:3])} failed: {redact(err.decode(errors='replace'))[-1500:]}")
    return out.decode(errors="replace")


def _mirror_path(full_name: str) -> Path:
    return get_settings().data_dir / "repos" / (full_name.replace("/", "__") + ".git")


_mirror_locks: dict[str, tuple[asyncio.AbstractEventLoop, asyncio.Lock]] = {}


def _mirror_lock(path: Path) -> asyncio.Lock:
    """One lock per mirror: fetches into it and job clones out of it never overlap."""
    loop = asyncio.get_running_loop()
    entry = _mirror_locks.get(str(path))
    if entry is None or entry[0] is not loop:  # a lock belongs to one event loop
        entry = _mirror_locks[str(path)] = (loop, asyncio.Lock())
    return entry[1]


async def mirror(full_name: str, token: str) -> Path:
    path = _mirror_path(full_name)
    async with _mirror_lock(path):
        url = remote_url(full_name)
        env = git_auth_env(token)
        if not (path / "HEAD").exists():
            path.parent.mkdir(parents=True, exist_ok=True)
            tmp = path.with_suffix(".tmp")
            shutil.rmtree(tmp, ignore_errors=True)
            await git("clone", "--bare", url, str(tmp), env=env, timeout=900)
            tmp.rename(path)
        # Jobs copy the mirror while other jobs fetch into it: no background maintenance may touch its files.
        for key, value in (("gc.auto", "0"), ("maintenance.auto", "false"), ("fetch.writeCommitGraph", "false")):
            await git("config", key, value, cwd=path)
        await git("-c", "maintenance.auto=false", "-c", "gc.auto=0", "fetch", "--prune", "origin",
                  "+refs/heads/*:refs/heads/*", cwd=path, env=env, timeout=900)
    return path


async def clone_mirror(mirror_path: Path, dest: Path, env: dict[str, str]) -> None:
    """Copy the mirror into a job directory while no fetch is writing to it (retry once as a last resort)."""
    for tries in range(2):
        try:
            async with _mirror_lock(mirror_path):
                await git("clone", "--no-hardlinks", "--quiet", str(mirror_path), str(dest), env=env, timeout=900)
            return
        except GitError:
            if tries:
                raise
            shutil.rmtree(dest, ignore_errors=True)
            await asyncio.sleep(2)


async def checkout(sandbox: LocalSandbox, full_name: str, token: str, base_ref: str, branch: str,
                   subdir: str | None = None, adopt: bool = True) -> str:
    """Clone ``base_ref`` into the sandbox work tree (or a subdirectory); returns the exact base commit."""
    mirror_path = await mirror(full_name, token)
    base_sha = (await git("rev-parse", base_ref, cwd=mirror_path)).strip()
    dest = sandbox.work / subdir if subdir else sandbox.work
    if dest.exists():
        shutil.rmtree(dest)
    dest.parent.mkdir(parents=True, exist_ok=True)
    await clone_mirror(mirror_path, dest, git_auth_env(token))
    await git("checkout", "-q", "-B", branch, base_sha, cwd=dest)
    await git("remote", "set-url", "origin", remote_url(full_name), cwd=dest)
    exclude = dest / ".git" / "info" / "exclude"
    exclude.parent.mkdir(parents=True, exist_ok=True)
    with exclude.open("a") as handle:
        handle.write(f"\n{SCRATCH}/\nnode_modules/\n.venv/\n__pycache__/\n")
    if adopt:
        sandbox.adopt_work()
    return base_sha


async def apply_in_sandbox(sandbox: LocalSandbox, patch: "Patch") -> None:
    """Apply a candidate patch inside a sandbox checkout (as the sandbox identity)."""
    if patch.empty:
        return
    from .sandbox import write_bytes

    await write_bytes(sandbox, f"{SCRATCH}/candidate.patch", patch.diff)
    # --index stages the candidate so new files are tracked: reviewers see them in `git diff <base>`.
    # Re-owning the checkout to the job identity invalidates git's cached stat data; refresh it first.
    result = await sandbox.exec(f"git update-index -q --refresh >/dev/null 2>&1; "
                                f"git apply --index --binary --whitespace=nowarn {SCRATCH}/candidate.patch", timeout=120)
    if not result.ok:
        raise SandboxError(f"Candidate patch does not apply: {result.output[-600:]}")


@dataclass
class Patch:
    diff: bytes
    files: list[str]
    additions: int
    deletions: int

    @property
    def empty(self) -> bool:
        return not self.diff.strip()

    @property
    def changed_lines(self) -> int:
        return self.additions + self.deletions


async def extract_patch(sandbox: LocalSandbox, base_sha: str) -> Patch:
    """Everything the job changed relative to the base commit (committed or not)."""
    out_file = f"{SCRATCH}/candidate.patch"
    # The patch goes to a file (exec output is clipped); numstat comes back on stdout.
    script = (
        f"set -e; mkdir -p {SCRATCH}; git add -A >/dev/null; "
        f"git -c core.quotepath=off diff --cached --binary --no-color {base_sha} > {out_file}; "
        f"git -c core.quotepath=off diff --cached --numstat {base_sha}"
    )
    result = await sandbox.exec(script, timeout=180)
    if not result.ok:
        raise SandboxError(f"Could not read the candidate changes: {result.output[-800:]}")
    from .sandbox import read_bytes

    diff = await read_bytes(sandbox, out_file)
    files, adds, dels = [], 0, 0
    for line in result.output.splitlines():
        parts = line.split("\t")
        if len(parts) == 3:
            files.append(parts[2])
            adds += int(parts[0]) if parts[0].isdigit() else 0
            dels += int(parts[1]) if parts[1].isdigit() else 0
    return Patch(diff=diff, files=files, additions=adds, deletions=dels)


def protected_violations(files: list[str], patterns: list[str]) -> list[str]:
    bad = []
    for name in files:
        for pattern in patterns:
            if fnmatch.fnmatch(name, pattern) or fnmatch.fnmatch(name, pattern.removesuffix("/**") + "/*"):
                bad.append(name)
                break
    return bad


async def publish(full_name: str, token: str, *, base_sha: str, branch: str, patch: Patch, message: str,
                  task_id: str) -> str:
    """Apply the patch onto a pristine clone at ``base_sha``, commit and force-push ``branch``."""
    mirror_path = await mirror(full_name, token)
    staging = get_settings().data_dir / "staging" / task_id
    shutil.rmtree(staging, ignore_errors=True)
    staging.parent.mkdir(parents=True, exist_ok=True)
    env = git_auth_env(token)
    await clone_mirror(mirror_path, staging, env)
    try:
        await git("checkout", "-q", "-B", branch, base_sha, cwd=staging)
        await git("apply", "--index", "--binary", "--whitespace=nowarn", "-", cwd=staging, input_bytes=patch.diff)
        await git("commit", "-q", "-m", message, cwd=staging, env=AUTHOR)
        head = (await git("rev-parse", "HEAD", cwd=staging)).strip()
        await git("remote", "set-url", "origin", remote_url(full_name), cwd=staging)
        await git("push", "--force", "origin", f"HEAD:refs/heads/{branch}", cwd=staging, env=env, timeout=600)
        return head
    finally:
        shutil.rmtree(staging, ignore_errors=True)


async def rebase_branch(full_name: str, token: str, *, branch: str, onto: str, task_id: str) -> tuple[bool, str]:
    """Rebase a published branch onto the latest base. Returns (clean, new_head_or_conflict_detail)."""
    mirror_path = await mirror(full_name, token)
    staging = get_settings().data_dir / "staging" / (task_id + "-rebase")
    shutil.rmtree(staging, ignore_errors=True)
    env = git_auth_env(token)
    await clone_mirror(mirror_path, staging, env)
    try:
        await git("fetch", "-q", "origin", f"+refs/heads/{branch}:refs/remotes/origin/{branch}", cwd=staging, env=env)
        await git("checkout", "-q", "-B", branch, f"origin/{branch}", cwd=staging)
        try:
            await git("rebase", "-q", f"origin/{onto}", cwd=staging, env=AUTHOR)
        except GitError as exc:
            await git("rebase", "--abort", cwd=staging)
            return False, str(exc)
        head = (await git("rev-parse", "HEAD", cwd=staging)).strip()
        await git("remote", "set-url", "origin", remote_url(full_name), cwd=staging)
        await git("push", "--force-with-lease", "origin", f"HEAD:refs/heads/{branch}", cwd=staging, env=env)
        return True, head
    finally:
        shutil.rmtree(staging, ignore_errors=True)


async def revert_commit(full_name: str, token: str, *, base: str, sha: str, branch: str, task_id: str) -> str:
    """Create ``branch`` from the latest ``base`` with ``sha`` reverted, push it, return the new head."""
    mirror_path = await mirror(full_name, token)
    staging = get_settings().data_dir / "staging" / (task_id + "-revert")
    shutil.rmtree(staging, ignore_errors=True)
    env = git_auth_env(token)
    await clone_mirror(mirror_path, staging, env)
    try:
        await git("checkout", "-q", "-B", branch, f"origin/{base}", cwd=staging)
        parents = (await git("rev-list", "--parents", "-n", "1", sha, cwd=staging)).split()
        mainline = ["-m", "1"] if len(parents) > 2 else []
        await git("revert", "--no-edit", *mainline, sha, cwd=staging, env=AUTHOR)
        head = (await git("rev-parse", "HEAD", cwd=staging)).strip()
        await git("remote", "set-url", "origin", remote_url(full_name), cwd=staging)
        await git("push", "--force", "origin", f"HEAD:refs/heads/{branch}", cwd=staging, env=env)
        return head
    finally:
        shutil.rmtree(staging, ignore_errors=True)
