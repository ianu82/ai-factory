"""Bounded, read-only fallback for ``steward_ci.diagnose`` candidate intervals.

``resolve_interval(repo, token, diagnosis['candidate_interval'], setup=cfg['setup'],
checks=cfg['checks'])`` returns JSON-safe evidence. No persistence or repair side
effects. A confirmed boundary is relative to the configured checks, assuming one
good-to-bad transition (as in git bisect), not proof of causality for a CI job.
"""

from __future__ import annotations

import asyncio
import math
import re
import shlex
import time
from typing import Any, Sequence
from uuid import uuid4

from . import gitops
from .github import GitHubError
from .github_app import InstallationCredential
from .sandbox import COMMAND_TIMEOUT, LocalSandbox, SandboxError
from .security import redact

_SHA = re.compile(r"[0-9a-f]{40}")
_UNTESTABLE = ("command not found", "no such file or directory", "can't open file",
               "file or directory not found", "no tests ran", "no tests collected", "collected 0 items")


class _Unresolved(Exception):
    pass


async def resolve_interval(repository: Any, token: str | InstallationCredential,
                           interval: dict | None, *, setup: Sequence[str] = (),
                           checks: Sequence[str] = (), max_probes: int = 24,
                           repeats: int = 2, max_commits: int = 200,
                           command_timeout: float = 300, budget: float = 1800,
                           max_excerpt_chars: int = 4000) -> dict[str, Any]:
    """Validate endpoints, then binary-search a default-branch first-parent window.

    ``max_probes`` counts fresh setup/check attempts, including repeats and both
    endpoints. One additional short-lived sandbox inspects ancestry *before* any
    repository commands run. The bounded window is measured from the captured
    default-branch tip; intervals outside it remain unresolved. Input ``commits``
    is deliberately ignored: local Git must independently prove the ancestry.
    No config env/secrets are injected. Credentials are used only by checkout.
    Cancellation propagates, after closing the current sandbox. Timeouts and
    unavailable evidence return an unresolved (possibly narrowed) interval.
    """
    if not (type(max_probes) is int and 1 <= max_probes <= 100
            and type(repeats) is int and 2 <= repeats <= 5
            and type(max_commits) is int and 1 <= max_commits <= 200
            and type(max_excerpt_chars) is int and 1 <= max_excerpt_chars <= 16000
            and math.isfinite(command_timeout) and 0 < command_timeout <= COMMAND_TIMEOUT
            and math.isfinite(budget) and 0 < budget <= 7200):
        raise ValueError("Bisection bounds exceed supported limits")
    result: dict[str, Any] = dict(state="unresolved", first_bad_sha=None,
        candidate_interval=None, classification="unknown", probes=[], evidence=[], commands=[])
    good = interval.get("good") if isinstance(interval, dict) else None
    bad = interval.get("bad") if isinstance(interval, dict) else None
    if not all(isinstance(sha, str) and _SHA.fullmatch(sha) for sha in (good, bad)) or good == bad:
        result["evidence"].append("No valid exact-SHA good/bad interval available")
        return result
    result["candidate_interval"] = dict(good=good, bad=bad)
    extra: tuple[str, ...] = ()

    async def command(sandbox: LocalSandbox, sha: str, text: str, phase: str) -> Any:
        started = time.monotonic()
        entry = dict(commit=sha, command=redact(text, extra), phase=phase,
                     exit_code=None, excerpt="", duration=0.0, status="interrupted")
        result["commands"].append(entry)
        try:
            execution = await asyncio.wait_for(sandbox.exec(text, timeout=command_timeout),
                                               timeout=command_timeout)
            # LocalSandbox bounds output before returning it. A clipped token
            # cannot safely be scrubbed after that boundary; never export such
            # output or use an incomplete log to rule out an untestable check.
            entry.update(exit_code=execution.exit_code,
                         excerpt="[truncated output omitted]" if execution.truncated else
                         redact(execution.output, extra)[-max_excerpt_chars:],
                         status="timeout" if execution.timed_out else "completed")
            if execution.truncated:
                raise _Unresolved("Sandbox output truncated; complete check evidence unavailable")
            if execution.timed_out:
                raise _Unresolved("Sandbox command timed out")
            return execution
        except TimeoutError:
            entry["status"] = "timeout"
            raise _Unresolved("Sandbox command timed out") from None
        finally:
            entry["duration"] = time.monotonic() - started

    async def close(sandbox: LocalSandbox) -> None:
        # A deadline can expire *during* cleanup, not just inside a command.
        # Finish releasing the identity/killing descendants before propagating
        # cancellation, including when an external cancel races the budget.
        task = asyncio.create_task(sandbox.close())
        cancelled = False
        while not task.done():
            try:
                await asyncio.shield(task)
            except asyncio.CancelledError:
                cancelled = True
        task.result()
        if cancelled:
            raise asyncio.CancelledError

    async def prepare(sandbox: LocalSandbox, sha: str) -> None:
        sandbox.create()
        actual = await gitops.checkout(sandbox, name, token, sha, "steward-probe")
        if actual != sha:
            raise _Unresolved("Checkout did not match the requested exact SHA")

    async def probe(sha: str) -> bool:
        outcomes: list[tuple[bool, ...]] = []
        for _ in range(repeats):
            if len(result["probes"]) >= max_probes:
                raise _Unresolved("Probe count budget exhausted")
            record = dict(commit=sha, attempt=len(outcomes) + 1, state="incomplete")
            result["probes"].append(record)
            sandbox = LocalSandbox("steward-bisect-" + uuid4().hex)
            try:
                await prepare(sandbox, sha)
                for text in setup:
                    execution = await command(sandbox, sha, text, "setup")
                    if not execution.ok:
                        raise _Unresolved("Setup failed; check results cannot establish a boundary")
                statuses = []
                for text in checks:
                    execution = await command(sandbox, sha, text, "check")
                    lower = execution.output.lower()
                    if execution.exit_code in (126, 127) or execution.exit_code < 0 or execution.exit_code >= 128 \
                            or any(signature in lower for signature in _UNTESTABLE):
                        raise _Unresolved("Configured check is unavailable or untestable")
                    statuses.append(execution.ok)
                outcome = tuple(statuses)
                record["state"] = "good" if all(outcome) else "bad"
                if outcomes and outcome != outcomes[0]:
                    # Repeated identical commands at an identical SHA, each with a
                    # fresh home/tmp/work tree: report flakiness, never a culprit.
                    result["classification"] = "flaky"
                    raise _Unresolved("Contradictory repeated checks at the same SHA; flaky check evidence")
                outcomes.append(outcome)
            finally:
                await close(sandbox)
        return all(outcomes[0])

    try:
        async with asyncio.timeout(budget):
            name = repository["full_name"] if isinstance(repository, dict) else repository.full_name
            branch = repository["default_branch"] if isinstance(repository, dict) else repository.default_branch
            if not isinstance(name, str) or not re.fullmatch(r"[\w.-]+/[\w.-]+", name):
                raise _Unresolved("Invalid repository identity")
            if not isinstance(branch, str) or not branch or len(branch) > 255:
                raise _Unresolved("Default branch unavailable")
            secret = token if isinstance(token, str) else await asyncio.to_thread(token.resolve)
            if not isinstance(secret, str) or not secret:
                raise _Unresolved("Workspace credential unavailable; bisection refused")
            extra = (secret,)
            if redact(branch, extra) != branch:
                raise _Unresolved("Credential-bearing default branch refused")
            # Snapshot caller-owned configuration once so repeated probes always
            # execute identical commands, even if the caller updates its policy.
            for commands in (setup, checks):
                if isinstance(commands, (str, bytes)) or len(commands) > 20 or any(
                        not isinstance(c, str) or not c.strip() or len(c) > 16000 for c in commands):
                    raise _Unresolved("Invalid or oversized configured command list")
                if any(redact(c, extra) != c for c in commands):
                    raise _Unresolved("Credential-bearing configured commands refused")
            setup, checks = tuple(setup), tuple(checks)
            if not checks:
                raise _Unresolved("No configured checks; interval is untestable")
            sandbox = LocalSandbox("steward-ancestry-" + uuid4().hex)
            try:
                await prepare(sandbox, bad)
                # Quote the complete ref, never interpret branch text as shell or
                # revision options. No repository scripts run in this sandbox.
                ref = shlex.quote("refs/remotes/origin/" + branch)
                execution = await command(sandbox, bad,
                    f"git --no-replace-objects rev-list --first-parent --max-count={max_commits + 2} {ref} --",
                    "ancestry")
                chain = execution.output.splitlines()
                if not execution.ok or execution.truncated or len(chain) > max_commits + 2 \
                        or any(not _SHA.fullmatch(s) for s in chain) or len(set(chain)) != len(chain) \
                        or good not in chain or bad not in chain or chain.index(bad) >= chain.index(good):
                    raise _Unresolved("Good/bad endpoints are not in the bounded default-branch first-parent window")
                candidates = list(reversed(chain[chain.index(bad):chain.index(good) + 1]))
                if len(candidates) - 1 > max_commits:
                    raise _Unresolved("First-parent interval exceeds the commit budget")
            finally:
                await close(sandbox)
            result["candidate_interval"] = dict(good=good, bad=bad, commits=candidates[1:])
            if not await probe(good):
                raise _Unresolved("Good endpoint fails configured checks")
            if await probe(bad):
                raise _Unresolved("Bad endpoint does not reproduce the failure")
            lo, hi = 0, len(candidates) - 1
            while hi - lo > 1:
                mid = (lo + hi) // 2
                if await probe(candidates[mid]):
                    lo = mid
                else:
                    hi = mid
                result["candidate_interval"] = dict(good=candidates[lo], bad=candidates[hi],
                                                     commits=candidates[lo + 1:hi + 1])
            result.update(state="confirmed", first_bad_sha=candidates[hi])
            result["evidence"].append("Repeated checks confirm adjacent first-parent good/bad revisions; "
                                      "assumes a single good-to-bad transition for the configured checks")
    except _Unresolved as exc:
        result["evidence"].append(str(exc))
    except TimeoutError:
        result["evidence"].append("Overall bisection time budget exhausted")
    except (gitops.GitError, GitHubError, SandboxError, OSError, ValueError):
        # Exception messages may contain credentials, command output or remote
        # URLs. Detailed command evidence has its own redacted egress above.
        result["evidence"].append("Sandbox, credential or checkout evidence unavailable; bisection refused")
    return result
