from __future__ import annotations

import asyncio
import base64
import io
import json
import tarfile

import pytest

from aifactory import gitops
from aifactory.github import GitHub, GitHubError
from aifactory.pipeline import review_sandbox as review
from aifactory.sandbox import LocalSandbox, SandboxError
from .conftest import git
from .test_vm import FakeBridge


@pytest.fixture
def pull_repo(tmp_path, monkeypatch):
    source = tmp_path / "source"
    source.mkdir()
    git("init", "-q", "-b", "main", cwd=source)
    (source / "app.txt").write_text("base")
    git("add", ".", cwd=source)
    git("commit", "-qm", "base", cwd=source)
    base = git("rev-parse", "HEAD", cwd=source).strip()
    (source / "app.txt").write_text("fork candidate")
    # Would execute if a checkout hook or a setup command ran on the host.
    (source / "setup.sh").write_text("touch HOST_SETUP_MUST_NOT_RUN\n")
    git("add", ".", cwd=source)
    git("commit", "-qm", "fork-only", cwd=source)
    head = git("rev-parse", "HEAD", cwd=source).strip()
    remote = tmp_path / "remotes" / "owner" / "repo.git"
    remote.parent.mkdir(parents=True)
    git("clone", "--bare", str(source), str(remote))
    git("update-ref", "refs/pull/7/head", head, cwd=remote)
    git("update-ref", "refs/heads/main", base, cwd=remote)
    monkeypatch.setattr(gitops, "GIT_BASE", str(tmp_path / "remotes"))
    return remote, base, head


def test_exact_fork_head_and_moving_ref(pull_repo, tmp_path):
    remote, base, head = pull_repo
    assert head not in git("show-ref", "--heads", cwd=remote)

    async def go():
        local = LocalSandbox("exact", root=tmp_path).create()
        candidate = await gitops.checkout_pull(local, "owner/repo", "sentinel-github", number=7,
                                               head_sha=head, base_ref="main")
        assert candidate == gitops.PullCandidate(head, base, base)
        assert git("rev-parse", "HEAD", cwd=local.work).strip() == head
        assert (local.work / "app.txt").read_text() == "fork candidate"
        assert "sentinel-github" not in (local.work / ".git/config").read_text()
        assert not (local.work / "HOST_SETUP_MUST_NOT_RUN").exists()
        git("update-ref", "refs/pull/7/head", base, cwd=remote)
        other = LocalSandbox("moved", root=tmp_path).create()
        with pytest.raises(gitops.BranchMoved):
            await gitops.checkout_pull(other, "owner/repo", "sentinel-github", number=7,
                                       head_sha=head, base_ref=base)
        assert not other.work.exists()
        await other.close()
        await local.close()
    asyncio.run(go())


@pytest.mark.parametrize("permission,expected", [
    ("write", "write"), ("maintain", "maintain"), ("admin", "admin"), ("read", "read"),
    (None, None), ("unknown", None), (403, None), (404, None),
])
def test_permission_lookup_fails_closed(permission, expected):
    gh = GitHub("sentinel")

    async def request(method, path):
        assert method == "GET" and path == "/repos/owner/repo/collaborators/a%2Fb/permission"
        if isinstance(permission, int):
            raise GitHubError("denied", permission)
        return {"permission": permission, "user": {"permissions": {"push": True}}}
    gh.request = request
    assert asyncio.run(gh.repository_author_permission("owner/repo", "a/b")) == expected


@pytest.mark.parametrize("fork,permission,trusted", [
    (False, "write", True), (False, "maintain", True), (False, "admin", True),
    (True, "admin", False), (False, "read", False), (False, None, False),
])
def test_trust_ignores_association(fork, permission, trusted):
    gh = GitHub("sentinel")

    async def lookup(*args):
        return permission
    gh.repository_author_permission = lookup
    pull = {"head": {"repo": {"full_name": "fork/repo" if fork else "owner/repo"}},
            "user": {"login": "author"}, "author_association": "OWNER"}
    trust = asyncio.run(review.review_trust(gh, "owner/repo", pull))
    assert trust.trusted is trusted


def test_same_repository_pr_in_a_base_that_is_itself_a_fork_is_trusted(tmp_path, monkeypatch):
    """GitHub reports head.repo.fork=True when the base repository is a fork of
    another project; a same-repository PR from a writer is still not cross-repository."""
    gh = GitHub("sentinel")

    async def lookup(*args):
        return "write"
    gh.repository_author_permission = lookup
    repo = {"full_name": "Owner/Repo", "fork": True, "parent": {"full_name": "upstream/repo"}}
    pull = {"head": {"repo": repo, "ref": "feature"}, "base": {"repo": repo, "ref": "main"},
            "user": {"login": "author"}}
    trust = asyncio.run(review.review_trust(gh, "owner/repo", pull))
    assert trust.is_fork is False and trust.trusted is True

    def no_vm():
        raise AssertionError("trusted same-repository review must not require a VM")
    monkeypatch.setattr(review, "vm_available", no_vm)

    async def go():
        local = LocalSandbox("local", root=tmp_path).create()
        try:
            assert await review.isolate_review(local, trust=trust, full_name="owner/repo", policy={}) is local
        finally:
            await local.close()
    asyncio.run(go())


@pytest.mark.parametrize("head", [{}, {"repo": None}, {"repo": {"full_name": ""}}, None])
def test_missing_head_repository_is_cross_repository(head):
    gh = GitHub("sentinel")

    async def lookup(*args):
        return "admin"
    gh.repository_author_permission = lookup
    trust = asyncio.run(review.review_trust(gh, "owner/repo", {"head": head, "user": {"login": "a"}}))
    assert trust.is_fork is True and trust.trusted is False


@pytest.mark.parametrize("permission", ["read", None])
@pytest.mark.parametrize("allow", [False, True, "true"])
def test_unavailable_vm_requires_explicit_boolean(tmp_path, monkeypatch, permission, allow):
    monkeypatch.setattr(review, "vm_available", lambda: (False, "unavailable"))

    async def go():
        local = LocalSandbox("local", root=tmp_path).create()
        try:
            call = review.isolate_review(local, trust=review.ReviewTrust(False, permission),
                                         full_name="owner/repo", policy={"allow_untrusted_local": allow})
            if allow is True:
                assert await call is local
            else:
                with pytest.raises(SandboxError, match="requires a VM"):
                    await call
        finally:
            await local.close()
    asyncio.run(go())


def test_failed_mandatory_vm_never_enters_tools(pull_repo, monkeypatch):
    _, _, head = pull_repo
    monkeypatch.setattr(review, "vm_available", lambda: (True, ""))

    async def fail(*args, **kwargs):
        raise RuntimeError("sentinel-vm-secret")

    async def no_local_tools(*args, **kwargs):
        pytest.fail("local setup/check/QA/verifier executed")
    monkeypatch.setattr(review.UntrustedReviewVM, "from_local", fail)
    monkeypatch.setattr(LocalSandbox, "exec", no_local_tools)

    async def go():
        with pytest.raises(SandboxError, match="Mandatory review VM could not start") as exc:
            async with review.review_sandbox("failed", "owner/repo", "sentinel-github", number=7,
                                             head_sha=head, base_ref="main", trust=review.ReviewTrust(True, "admin"),
                                             policy={"allow_untrusted_local": True}):
                pytest.fail("review tools became reachable")
        assert "sentinel-vm-secret" not in str(exc.value)
    asyncio.run(go())


@pytest.mark.parametrize("trust", [review.ReviewTrust(True, "admin"), review.ReviewTrust(False, "read"),
                                    review.ReviewTrust(False, None)])
def test_untrusted_vm_no_credentials_or_checkpoints(pull_repo, tmp_path, monkeypatch, trust):
    _, _, head = pull_repo
    secrets = ["sentinel-github", "sentinel-provider", "sentinel-vm"]
    for name, value in zip(["GITHUB_TOKEN", "OPENAI_API_KEY", "RAILWAY_API_TOKEN"], secrets):
        monkeypatch.setenv(name, value)

    class RecordingBridge(FakeBridge):
        def __init__(self, root):
            super().__init__(root)
            self.payloads = []

        async def call(self, op, args=None, timeout=600):
            self.payloads.append((op, args))
            return await super().call(op, args, timeout)

    bridge = RecordingBridge(tmp_path / "vm")
    monkeypatch.setattr("aifactory.vm.VMBridge.get", classmethod(lambda cls: bridge))
    monkeypatch.setattr(review, "vm_available", lambda: (True, ""))

    async def no_local_tools(*args, **kwargs):
        pytest.fail("control plane ran repository code")
    monkeypatch.setattr(LocalSandbox, "exec", no_local_tools)

    async def go():
        async with review.review_sandbox("isolated", "owner/repo", secrets[0], number=7, head_sha=head,
                                         base_ref="main", trust=trust,
                                         policy={"setup": ["sh setup.sh"]}, isolation={"isolation": "local", "prepared": True}) as (vm, candidate):
            assert vm.kind == "vm" and vm.prepared is None and vm.environment_key is None
            assert candidate.head_sha == head
            assert (await vm.exec("cat app.txt")).output == "fork candidate"
            with pytest.raises(SandboxError, match="cannot publish"):
                await vm.checkpoint("poison")
            assert "checkpoint" not in bridge.calls and "checkpoints" not in bridge.calls
            assert all("checkpoint" not in (args or {}) for op, args in bridge.payloads if op == "create")
            for op, args in bridge.payloads:
                serialized = json.dumps(args)
                assert not any(secret in serialized for secret in secrets)
                if op == "write":
                    payload = base64.b64decode(args["base64"])
                    with tarfile.open(fileobj=io.BytesIO(payload)) as archive:
                        for member in archive.getmembers():
                            if member.isfile():
                                data = archive.extractfile(member).read()
                                assert not any(secret.encode() in data for secret in secrets)
            for path in bridge.root.rglob("*"):
                if path.is_file():
                    assert not any(secret.encode() in path.read_bytes() for secret in secrets)
        assert bridge.calls[-1] == "destroy"
    asyncio.run(go())


def test_trusted_retains_normal_local_selection(tmp_path, monkeypatch):
    def no_vm_probe():
        pytest.fail("normal local selection probed VM")
    monkeypatch.setattr(review, "vm_available", no_vm_probe)

    async def go():
        local = LocalSandbox("trusted", root=tmp_path).create()
        assert await review.isolate_review(local, trust=review.ReviewTrust(False, "write"),
                                           full_name="owner/repo", policy={}) is local
        await local.close()
    asyncio.run(go())


def test_control_plane_checkout_ignores_host_filters_and_hooks(pull_repo, tmp_path, monkeypatch):
    remote, base, _ = pull_repo
    source = tmp_path / "source"
    sentinel = tmp_path / "CONTROL_PLANE_EXECUTED"
    (source / ".gitattributes").write_text("app.txt filter=host\n")
    git("add", ".gitattributes", cwd=source)
    git("commit", "-qm", "attributes", cwd=source)
    head = git("rev-parse", "HEAD", cwd=source).strip()
    git("push", str(remote), "HEAD:refs/pull/7/head", cwd=source)
    from aifactory.config import get_settings

    home = get_settings().data_dir
    home.mkdir(parents=True, exist_ok=True)
    hooks = tmp_path / "hooks"
    hooks.mkdir()
    hook = hooks / "post-checkout"
    hook.write_text(f"#!/bin/sh\ntouch {sentinel}\n")
    hook.chmod(0o755)
    (home / ".gitconfig").write_text(
        f'[filter "host"]\n smudge = touch {sentinel}\n required = true\n'
        f'[core]\n hooksPath = {hooks}\n')

    async def go():
        local = LocalSandbox("safe", root=tmp_path).create()
        await gitops.checkout_pull(local, "owner/repo", "sentinel-github", number=7,
                                   head_sha=head, base_ref=base)
        assert not sentinel.exists()
        assert (local.work / "app.txt").read_text() == "fork candidate"
        await local.close()
    asyncio.run(go())


def test_trusted_vm_retains_prepared_selection_and_normal_fallback(tmp_path, monkeypatch):
    local = LocalSandbox("trusted-vm", root=tmp_path).create()
    local.work.mkdir()
    (local.work / "requirements.txt").write_text("example==1")
    monkeypatch.setattr(review, "vm_available", lambda: (True, ""))
    bridge = FakeBridge(tmp_path / "vm")
    monkeypatch.setattr("aifactory.vm.VMBridge.get", classmethod(lambda cls: bridge))

    async def go():
        vm = await review.isolate_review(local, trust=review.ReviewTrust(False, "maintain"),
                                         full_name="owner/repo", policy={"setup": ["pip install -r requirements.txt"]},
                                         isolation={"isolation": "vm"})
        assert vm.kind == "vm" and vm.environment_key
        await vm.checkpoint(vm.environment_key)
        assert "checkpoint" in bridge.calls
        await vm.close()
        other = LocalSandbox("fallback", root=tmp_path).create()

        async def fail(*args, **kwargs):
            raise SandboxError("failed")
        monkeypatch.setattr(review.RailwayVMSandbox, "from_local", fail)
        assert await review.isolate_review(other, trust=review.ReviewTrust(False, "write"),
                                           full_name="owner/repo", policy={},
                                           isolation={"isolation": "vm"}) is other
        await other.close()
    asyncio.run(go())
