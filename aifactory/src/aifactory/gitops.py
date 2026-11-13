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
import base64
import fnmatch
import os
import re
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

from .config import get_settings
from .github import fresh_git_auth_env
from .github_app import InstallationCredential
from .sandbox import SCRATCH, LocalSandbox, SandboxError

SAFE_GIT = ["-c", "core.hooksPath=/dev/null", "-c", "core.fsmonitor=false", "-c", "protocol.file.allow=always",
            "-c", "advice.detachedHead=false", "-c", "init.defaultBranch=main"]
AUTHOR = {"GIT_AUTHOR_NAME": "MindsHub AI Factory", "GIT_AUTHOR_EMAIL": "factory@mindshub.ai",
          "GIT_COMMITTER_NAME": "MindsHub AI Factory", "GIT_COMMITTER_EMAIL": "factory@mindshub.ai"}
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

        # Git/remote diagnostics must not echo an ephemeral header or its token.
        auth = (env or {}).get("GIT_CONFIG_VALUE_0", "")
        sensitive = [auth]
        if auth.startswith("AUTHORIZATION: basic "):
            decoded = base64.b64decode(auth.split()[-1]).decode()
            sensitive.extend((auth.split()[-1], decoded, decoded.removeprefix("x-access-token:")))
        raise GitError(f"git {' '.join(a for a in args[:3])} failed: {redact(err.decode(errors='replace'), extra=tuple(sensitive))[-1500:]}")
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


async def mirror(full_name: str, token: str | InstallationCredential) -> Path:
    path = _mirror_path(full_name)
    async with _mirror_lock(path):
        url = remote_url(full_name)
        if not (path / "HEAD").exists():
            path.parent.mkdir(parents=True, exist_ok=True)
            tmp = path.with_suffix(".tmp")
            shutil.rmtree(tmp, ignore_errors=True)
            await git("clone", "--bare", url, str(tmp), env=await fresh_git_auth_env(token), timeout=900)
            tmp.rename(path)
        # Jobs copy the mirror while other jobs fetch into it: no background maintenance may touch its files.
        for key, value in (("gc.auto", "0"), ("maintenance.auto", "false"), ("fetch.writeCommitGraph", "false")):
            await git("config", key, value, cwd=path)
        await git("-c", "maintenance.auto=false", "-c", "gc.auto=0", "fetch", "--prune", "origin",
                  "+refs/heads/*:refs/heads/*", cwd=path, env=await fresh_git_auth_env(token), timeout=900)
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


async def checkout(sandbox: LocalSandbox, full_name: str, token: str | InstallationCredential, base_ref: str, branch: str,
                   subdir: str | None = None, adopt: bool = True) -> str:
    """Clone ``base_ref`` into the sandbox work tree (or a subdirectory); returns the exact base commit."""
    mirror_path = await mirror(full_name, token)
    base_sha = (await git("rev-parse", base_ref, cwd=mirror_path)).strip()
    dest = sandbox.work / subdir if subdir else sandbox.work
    if dest.exists():
        shutil.rmtree(dest)
    dest.parent.mkdir(parents=True, exist_ok=True)
    await clone_mirror(mirror_path, dest, {})
    await git("checkout", "-q", "-B", branch, base_sha, cwd=dest)
    await git("remote", "set-url", "origin", remote_url(full_name), cwd=dest)
    exclude = dest / ".git" / "info" / "exclude"
    exclude.parent.mkdir(parents=True, exist_ok=True)
    with exclude.open("a") as handle:
        handle.write(f"\n{SCRATCH}/\nnode_modules/\n.venv/\n__pycache__/\n")
    if adopt:
        sandbox.adopt_work()
    return base_sha


@dataclass(frozen=True)
class PullCandidate:
    head_sha: str
    base_sha: str
    merge_base_sha: str


async def checkout_pull(sandbox: LocalSandbox, full_name: str, token: str | InstallationCredential, *,
                        number: int, head_sha: str, base_ref: str, adopt: bool = True) -> PullCandidate:
    """Prepare exactly the captured PR head, using only the base repository's PR ref.

    No repository commands, hooks, filters or setup run in the control plane.
    Authentication is scoped to fetch; neither the clone nor its config receives it.
    Fetch, verification, resolution and copying share the mirror lock, so a concurrent
    review cannot replace the ref between verification and cloning.
    """
    if type(number) is not int or number < 1 or not re.fullmatch(r"[0-9a-fA-F]{40}", head_sha):
        raise GitError("Invalid pull request number or captured SHA")
    head_sha = head_sha.lower()
    # Ignore host/system Git configuration (including filters and templates) when
    # materializing untrusted trees. The freshly cloned config is Factory-owned.
    clean_env = {"GIT_CONFIG_NOSYSTEM": "1", "GIT_CONFIG_GLOBAL": "/dev/null"}
    base = base_ref if re.fullmatch(r"[0-9a-fA-F]{40}", base_ref) else f"refs/heads/{base_ref}"
    if base.startswith("refs/heads/"):
        await git("check-ref-format", base, env=clean_env)
    path = await mirror(full_name, token)
    dest = sandbox.work
    async with _mirror_lock(path):
        ref = f"refs/pull/{number}/head"
        await git("fetch", "--no-tags", remote_url(full_name), f"+{ref}:{ref}", cwd=path,
                  env={**clean_env, **(await fresh_git_auth_env(token))}, timeout=900)
        actual = (await git("rev-parse", "--verify", f"{ref}^{{commit}}", cwd=path, env=clean_env)).strip()
        if actual != head_sha:
            raise BranchMoved("Pull request head moved since it was captured")
        base_sha = (await git("rev-parse", "--verify", "--end-of-options", f"{base}^{{commit}}",
                              cwd=path, env=clean_env)).strip()
        merge_base = (await git("merge-base", base_sha, head_sha, cwd=path, env=clean_env)).strip()
        if dest.exists():
            shutil.rmtree(dest)
        dest.parent.mkdir(parents=True, exist_ok=True)
        await git("-c", "init.templateDir=", "clone", "--no-checkout", "--no-hardlinks", "--quiet",
                  str(path), str(dest), env=clean_env, timeout=900)
    await git("checkout", "--detach", head_sha, cwd=dest, env=clean_env)
    await git("remote", "set-url", "origin", remote_url(full_name), cwd=dest, env=clean_env)
    if adopt:
        sandbox.adopt_work()
    return PullCandidate(head_sha, base_sha, merge_base)


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


async def changed_paths(full_name: str, token: str | InstallationCredential, base: str, head: str) -> list[str]:
    """NUL-delimited, rename-free diff: include deleted paths and both sides of a rename."""
    path = await mirror(full_name, token)
    names = await git("diff", "--name-only", "--no-renames", "-z", base, head, "--", cwd=path)
    return sorted(set(names.split("\0")) - {""})


def path_overlap(candidate: list[str], main: list[str]) -> dict:
    """Linear-size set operations, never a Cartesian product (even within one package).

    Two changes overlap when they share a file; a shared directory is recorded as information only."""
    candidates, intervening = set(candidate), set(main)
    shared = candidates & intervening
    candidate_dirs = {name.rpartition("/")[0] for name in candidates}
    main_dirs = {name.rpartition("/")[0] for name in intervening}
    directories = candidate_dirs & main_dirs
    # One representative pair per shared directory, at most 20 pairs in evidence.
    sample_dirs = sorted(directories)[:20]
    left, right = {}, {}
    for paths, representatives in ((sorted(candidates), left), (sorted(intervening), right)):
        for name in paths:
            directory = name.rpartition("/")[0]
            if directory in sample_dirs:
                representatives.setdefault(directory, name)
    return {"overlap": bool(shared), "shared_files_count": len(shared),
            "shared_directories_count": len(directories), "shared_files": sorted(shared)[:20],
            "matching_pairs": [[left[d], right[d]] for d in sample_dirs]}


def _diff_commands(base_sha: str, stem: str) -> str:
    """Shell commands writing what the index holds beyond ``base_sha`` to three scratch files named ``stem``: the
    diff, its numstat and its paths."""
    # Keep publication rename-free so excluded scratch paths cannot leak as
    # rename sources. Policy size statistics still recognize ordinary renames.
    # File data goes through backend-neutral reads (exec output is clipped).
    paths = "-- . ':(top,exclude).factory' ':(top,exclude).factory/**'"
    out, diff = f"{SCRATCH}/{stem}", "git -c core.quotepath=off diff --cached --binary --no-renames --no-color"
    return (f"{diff} {base_sha} {paths} > {out}.patch; "
            f"git diff --cached --numstat -z --find-renames {base_sha} {paths} > {out}.numstat; "
            f"git diff --cached --name-only -z --no-renames {base_sha} {paths} > {out}.paths")


async def _read_patch(sandbox: LocalSandbox, stem: str, *, limit: int | None = None) -> Patch:
    """The patch that ``_diff_commands`` wrote to the scratch files named ``stem``."""
    from .sandbox import read_bytes

    diff = await read_bytes(sandbox, f"{SCRATCH}/{stem}.patch", limit=limit + 1 if limit is not None else None)
    if limit is not None and len(diff) > limit:
        raise SandboxError("Candidate patch exceeds the checkpoint byte limit")
    names = await read_bytes(sandbox, f"{SCRATCH}/{stem}.paths", limit=limit + 1 if limit is not None else None)
    stats_raw = await read_bytes(sandbox, f"{SCRATCH}/{stem}.numstat", limit=limit + 1 if limit is not None else None)
    if limit is not None and (len(names) > limit or len(stats_raw) > limit):
        raise SandboxError("Candidate metadata exceeds the checkpoint byte limit")
    files = [name.decode("utf-8", "surrogateescape") for name in names.split(b"\0") if name]
    adds, dels = 0, 0
    stats = iter(stats_raw.split(b"\0"))
    for record in stats:
        if not record:
            continue
        added, deleted, name = record.split(b"\t", 2)
        if not name:  # -z rename records carry two additional NUL-delimited names.
            next(stats)
            next(stats)
        adds += int(added) if added.isdigit() else 0
        dels += int(deleted) if deleted.isdigit() else 0
    return Patch(diff=diff, files=files, additions=adds, deletions=dels)


async def extract_patch(sandbox: LocalSandbox, base_sha: str, *, limit: int | None = None) -> Patch:
    """Everything the job changed relative to the base commit (committed or not)."""
    script = f"set -e; mkdir -p {SCRATCH}; git add -A >/dev/null; {_diff_commands(base_sha, 'candidate')}"
    result = await sandbox.exec(script, timeout=180)
    if not result.ok:
        raise SandboxError(f"Could not read the candidate changes: {result.output[-800:]}")
    return await _read_patch(sandbox, "candidate", limit=limit)


async def snapshot_patch(sandbox: LocalSandbox, base_sha: str, timeout: float = 60) -> Patch:
    """What ``extract_patch`` would read at this moment, while the job is still working: the files are added to a
    copy of the index, so the job's own index and working tree are left as they are, and the result is written to
    scratch files of its own, never the candidate's."""
    # The copy lives in the git directory, not the scratch directory: that is in the working tree, where the copy's
    # lock file would come and go as an untracked file under the job's own git commands. No pathspec for `git add`:
    # where the checkout ignores the scratch directory, one that names it fails (see tree_id). The copy keeps the
    # index's time (cp -p): git compares a file's content when it changed in the second its index was written, and
    # a newer index would take such a file as unchanged.
    # Every snapshot has names of its own. One that was cancelled is killed in a local sandbox but runs on in a VM,
    # beside the next one: it then writes only its own files. What earlier snapshots left is removed first, and a
    # snapshot whose index was removed under it fails (an index that is gone reads as "every file deleted").
    stem = f"wip-{os.urandom(4).hex()}"
    script = (f'set -e; mkdir -p {SCRATCH}; dir="$(git rev-parse --absolute-git-dir)"; index="$dir/factory-{stem}"; '
              f'rm -f "$dir"/factory-wip-* {SCRATCH}/wip-*; '
              'cp -p "$(git rev-parse --git-path index)" "$index" 2>/dev/null || true; '
              f'export GIT_INDEX_FILE="$index"; git add -A >/dev/null; {_diff_commands(base_sha, stem)}; '
              'test -s "$index"; rm -f "$index"')
    result = await sandbox.exec(script, timeout=timeout)
    if not result.ok:
        raise SandboxError(f"Could not read the changes in progress: {result.output[-800:]}")
    return await _read_patch(sandbox, stem)


_TREE_ID = re.compile(r"[0-9a-f]{40,64}")


async def tree_id(sandbox: LocalSandbox) -> str | None:
    """The id of a git tree holding every file of the checkout as it is now, committed or not, or None when git
    could not say. Two equal ids mean the same files with the same content. Ignored files (caches, a local
    database) and the scratch directory are not part of it. The files are added to a copy of the index, so the
    checkout's own index is left as it was."""
    # The scratch directory is dropped from the copy after adding, not kept out with an exclude pathspec: where the
    # checkout ignores it (checkout() lists it in .git/info/exclude), `git add` fails for a pathspec that names it.
    script = ('index="$(mktemp -d)/index"; cp "$(git rev-parse --git-path index)" "$index" 2>/dev/null; '
              'export GIT_INDEX_FILE="$index"; git add -A >/dev/null 2>&1 && '
              f"git rm -r -q --cached --ignore-unmatch -- ':(top){SCRATCH}' >/dev/null 2>&1 && git write-tree; "
              'rm -rf "$(dirname "$index")"')
    result = await sandbox.exec(script, timeout=120)
    last = result.output.strip().rpartition("\n")[2].strip()
    return last if _TREE_ID.fullmatch(last) else None


async def tree_changes(sandbox: LocalSandbox, before: str, after: str, limit: int = 5) -> list[str]:
    """The first ``limit`` paths that differ between two trees from ``tree_id``, and "…" when there are more."""
    result = await sandbox.exec(f'names="$(git -c core.quotepath=off diff-tree -r --name-only --no-renames {before} '
                                f'{after})" && printf \'%s\\n\' "$names" | head -n {limit + 1}', timeout=60)
    names = [line.strip()[:200] for line in result.output.splitlines() if line.strip()] if result.ok else []
    return names[:limit] + ["…"] * (len(names) > limit)


async def patch_id(sandbox: LocalSandbox, base_sha: str, head_sha: str) -> str:
    """Identity of what ``head_sha`` changes since its merge base with ``base_sha``: the same change rebased onto a
    newer base keeps its id. "" when there is no change or it cannot be read (never raises)."""
    if not all(re.fullmatch(r"[0-9a-f]{7,64}", sha or "") for sha in (base_sha, head_sha)):
        return ""
    # The candidate diff's flags (extract_patch). The id ignores where in a file each hunk lands but not whitespace:
    # --verbatim implies --stable and hashes each line as it is (a git without the option fails: no id).
    script = (f"set -e -o pipefail; base=$(git merge-base {base_sha} {head_sha}); "
              f"git -c core.quotepath=off diff --binary --no-renames --no-color \"$base\" {head_sha} 2>/dev/null "
              "| git patch-id --verbatim 2>/dev/null")
    try:
        result = await sandbox.exec(script, timeout=60)
    except (SandboxError, OSError):
        return ""
    # Exactly one line, "<patch id> <commit id>" (zeros for a bare diff); no line at all for an empty diff.
    found = re.fullmatch(r"([0-9a-f]{40,64}) [0-9a-f]{40,64}", result.output.strip()) if result.ok else None
    return found.group(1) if found else ""


_CONFLICT_MARKER = re.compile(rb"^\+(<{7}|>{7})( |$)")


def conflict_marker_files(patch: Patch) -> list[str]:
    """Files whose added lines still contain merge conflict markers (``<<<<<<<`` / ``>>>>>>>``)."""
    files: list[str] = []
    current = ""
    for line in patch.diff.splitlines():
        if line.startswith(b"+++ "):
            current = line[6:].decode("utf-8", "replace") if line.startswith(b"+++ b/") else ""
        elif current and current not in files and _CONFLICT_MARKER.match(line):
            files.append(current)
    return files


def protected_violations(files: list[str], patterns: list[str]) -> list[str]:
    bad = []
    for name in files:
        for pattern in patterns:
            if fnmatch.fnmatch(name, pattern) or fnmatch.fnmatch(name, pattern.removesuffix("/**") + "/*"):
                bad.append(name)
                break
    return bad


async def publish(full_name: str, token: str | InstallationCredential, *, base_sha: str, branch: str, patch: Patch, message: str,
                  task_id: str) -> str:
    """Apply the patch onto a pristine clone at ``base_sha``, commit and force-push ``branch``."""
    mirror_path = await mirror(full_name, token)
    staging = get_settings().data_dir / "staging" / task_id
    shutil.rmtree(staging, ignore_errors=True)
    staging.parent.mkdir(parents=True, exist_ok=True)
    await clone_mirror(mirror_path, staging, {})
    try:
        await git("checkout", "-q", "-B", branch, base_sha, cwd=staging)
        await git("apply", "--index", "--binary", "--whitespace=nowarn", "-", cwd=staging, input_bytes=patch.diff)
        await git("commit", "-q", "-m", message, cwd=staging, env=AUTHOR)
        head = (await git("rev-parse", "HEAD", cwd=staging)).strip()
        await git("remote", "set-url", "origin", remote_url(full_name), cwd=staging)
        await git("push", "--force", "origin", f"HEAD:refs/heads/{branch}", cwd=staging, env=await fresh_git_auth_env(token), timeout=600)
        return head
    finally:
        shutil.rmtree(staging, ignore_errors=True)


class BranchMoved(GitError):
    """The published branch no longer points at the commit the Factory verified."""


async def rebase_branch(full_name: str, token: str | InstallationCredential, *, branch: str, onto: str, task_id: str,
                        base: str = "", head: str = "", expected_head: str = "",
                        before_push: Callable[[str], None] | None = None) -> tuple[bool, str]:
    """Rebase a published branch onto the latest base. Returns (clean, new_head_or_conflict_detail).

    With ``base`` (the commit the branch was built on) only the branch's own commits are replayed, so commits
    removed from a rewritten base branch cannot come back. With ``head`` (the verified commit) the rebase refuses
    a branch someone else has pushed to (BranchMoved), and the push only replaces that exact commit."""
    mirror_path = await mirror(full_name, token)
    staging = get_settings().data_dir / "staging" / (task_id + "-rebase")
    shutil.rmtree(staging, ignore_errors=True)
    await clone_mirror(mirror_path, staging, {})
    try:
        await git("fetch", "-q", "origin", f"+refs/heads/{branch}:refs/remotes/origin/{branch}", cwd=staging, env=await fresh_git_auth_env(token))
        if head:
            current = (await git("rev-parse", f"origin/{branch}", cwd=staging)).strip()
            if expected_head and current == expected_head:
                return True, current  # push completed before the caller recorded its result
            if current != head:
                raise BranchMoved(f"{branch} is at {current[:10]}, not the verified {head[:10]}")
        await git("checkout", "-q", "-B", branch, f"origin/{branch}", cwd=staging)
        try:
            target = onto if re.fullmatch(r"[0-9a-f]{40,64}", onto) else f"origin/{onto}"
            await git("rebase", "-q", *(["--onto", target, base] if base else [target]),
                      cwd=staging, env=AUTHOR)
        except GitError as exc:
            await git("rebase", "--abort", cwd=staging)
            return False, str(exc)
        await git("remote", "set-url", "origin", remote_url(full_name), cwd=staging)
        lease = f"--force-with-lease=refs/heads/{branch}:{head}" if head else "--force-with-lease"
        new_head = (await git("rev-parse", "HEAD", cwd=staging)).strip()
        if before_push:
            before_push(new_head)  # durable intent before the external side effect
        await git("push", lease, "origin", f"HEAD:refs/heads/{branch}", cwd=staging, env=await fresh_git_auth_env(token))
        return True, new_head
    finally:
        shutil.rmtree(staging, ignore_errors=True)


async def revert_commit(full_name: str, token: str | InstallationCredential, *, base: str, sha: str, branch: str, task_id: str) -> str:
    """Create ``branch`` from the latest ``base`` with ``sha`` reverted, push it, return the new head."""
    mirror_path = await mirror(full_name, token)
    staging = get_settings().data_dir / "staging" / (task_id + "-revert")
    shutil.rmtree(staging, ignore_errors=True)
    await clone_mirror(mirror_path, staging, {})
    try:
        await git("checkout", "-q", "-B", branch, f"origin/{base}", cwd=staging)
        parents = (await git("rev-list", "--parents", "-n", "1", sha, cwd=staging)).split()
        mainline = ["-m", "1"] if len(parents) > 2 else []
        await git("revert", "--no-edit", *mainline, sha, cwd=staging, env=AUTHOR)
        head = (await git("rev-parse", "HEAD", cwd=staging)).strip()
        await git("remote", "set-url", "origin", remote_url(full_name), cwd=staging)
        await git("push", "--force", "origin", f"HEAD:refs/heads/{branch}", cwd=staging, env=await fresh_git_auth_env(token))
        return head
    finally:
        shutil.rmtree(staging, ignore_errors=True)
