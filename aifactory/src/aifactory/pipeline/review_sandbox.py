"""Review checkout and isolation boundary; callers run *all* tools after entry.

Only Git data preparation runs locally before VM upload. In particular, do not
apply patches via local.exec, install dependencies, or invoke a verifier here.
"""
from __future__ import annotations

from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Any

from ..github import GitHub, GitHubError, InstallationCredential
from ..gitops import checkout_pull
from ..sandbox import LocalSandbox, SandboxError
from ..vm import RailwayVMSandbox, environment_key, vm_available


@dataclass(frozen=True)
class ReviewTrust:
    is_fork: bool
    author_permission: str | None

    @property
    def trusted(self) -> bool:
        return not self.is_fork and self.author_permission in ("write", "maintain", "admin", "push")


async def review_trust(github: GitHub, full_name: str, pull: dict[str, Any]) -> ReviewTrust:
    """Association is not authorization, and a trusted author cannot bless a fork."""
    # Cross-repository means the head lives in a different repository from the
    # base. Whether the base repository is itself a GitHub fork of some other
    # project (``head.repo.fork``) is irrelevant. A deleted or malformed head
    # repository cannot be shown to be the base, so it fails closed.
    head = pull.get("head") if isinstance(pull.get("head"), dict) else {}
    head_repo = head.get("repo") if isinstance(head.get("repo"), dict) else {}
    name = head_repo.get("full_name")
    is_fork = not isinstance(name, str) or not name or name.casefold() != full_name.casefold()
    author = (pull.get("user") or {}).get("login")
    permission = None
    if isinstance(author, str) and author:
        try:
            permission = await github.repository_author_permission(full_name, author)
        except GitHubError:
            # An unavailable permission service is not proof of write access.
            pass
    return ReviewTrust(is_fork, permission)


class UntrustedReviewVM(RailwayVMSandbox):
    async def checkpoint(self, name: str, timeout: float = 600) -> None:
        raise SandboxError("Untrusted reviews cannot publish prepared environments")


async def isolate_review(local: LocalSandbox, *, trust: ReviewTrust, full_name: str,
                         policy: dict[str, Any], isolation: dict[str, Any] | None = None):
    """Select once, before setup/checks/QA/verifiers; never downgrade a mandatory VM.

    ``policy`` is the repository's review policy. ``isolation`` is the normal
    build/QA isolation configuration for this stage. Local execution uses the
    existing LocalSandbox policy (per-job identity when enabled).
    """
    cfg = isolation or {}
    mandatory = not trust.trusted
    if not mandatory and cfg.get("isolation") != "vm":
        return local
    available, _ = vm_available()
    if not available:
        if mandatory and policy.get("allow_untrusted_local") is not True:
            raise SandboxError("Untrusted review requires a VM or explicit allow_untrusted_local=true")
        return local
    key = None
    if not mandatory and cfg.get("prepared", True) and policy.get("setup"):
        key = environment_key(local.work, full_name, list(policy["setup"]))
    vm_class = UntrustedReviewVM if mandatory else RailwayVMSandbox
    try:
        vm = await vm_class.from_local(local, prepared=key, cpu=float(cfg.get("cpu", 4)),
                                       memory_gb=float(cfg.get("memory_gb", 8)))
    except Exception:
        if mandatory:
            # Even an explicit local allowance applies only to unavailable VMs,
            # not to a failed mandatory startup/upload.
            raise SandboxError("Mandatory review VM could not start") from None
        return local
    vm.environment_key = key
    await local.close()
    return vm


@asynccontextmanager
async def review_sandbox(job_id: str, full_name: str, token: str | InstallationCredential, *,
                         number: int, head_sha: str, base_ref: str, trust: ReviewTrust,
                         policy: dict[str, Any], isolation: dict[str, Any] | None = None):
    """Yield (sandbox, exact candidate) only after the isolation boundary is ready.

    Credentials go only to control-plane Git fetches, never to sandbox commands,
    config, generated scripts, or the VM bridge.
    """
    local = LocalSandbox(job_id).create()
    sandbox = local
    try:
        candidate = await checkout_pull(local, full_name, token, number=number, head_sha=head_sha,
                                        base_ref=base_ref, adopt=False)
        sandbox = await isolate_review(local, trust=trust, full_name=full_name,
                                       policy=policy, isolation=isolation)
        if sandbox is local:
            local.adopt_work()
        yield sandbox, candidate
    finally:
        if sandbox is not local:
            await sandbox.close()
        await local.close()
