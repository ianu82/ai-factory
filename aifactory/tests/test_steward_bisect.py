"""Local Git integration plus deterministic lifecycle/budget fault injection."""

import asyncio
import json
from collections import Counter

import pytest

from aifactory import gitops, steward_bisect
from aifactory.config import get_settings
from aifactory.sandbox import ExecResult, LocalSandbox, SandboxError
from aifactory.steward_bisect import resolve_interval
from tests.fakes import git, make_remote

REPO = {"full_name": "org/repo", "default_branch": "main"}
TOKEN = "workspace-secret-with-no-generic-prefix"


@pytest.fixture
def history(tmp_path, monkeypatch):
    remotes = tmp_path / "remotes"
    bare = make_remote(remotes, "org/repo", {"value": "good\n"})
    work = remotes / "seed/org/repo"
    chain = [git("rev-parse", "HEAD", cwd=work)]
    for i in range(1, 9):
        (work / "value").write_text("bad\n" if i >= 5 else "good\n")
        (work / "revision").write_text(str(i))
        git("add", "-A", cwd=work)
        git("commit", "-qm", f"commit-{i}", cwd=work)
        chain.append(git("rev-parse", "HEAD", cwd=work))
    git("push", "-q", str(bare), "main", cwd=work)
    monkeypatch.setattr(gitops, "GIT_BASE", str(remotes))
    return chain, bare, work


@pytest.mark.asyncio
async def test_local_untested_regression_exact_sha_and_clean_attempts(history, monkeypatch):
    chain, _, _ = history
    seen = []
    checked_out = []
    checkout = gitops.checkout
    host_git = gitops.git
    sandbox_exec = LocalSandbox.exec
    sandboxes = []
    create = LocalSandbox.create

    def track_create(self):
        sandboxes.append(self)
        return create(self)

    async def control_git(*args, **kwargs):
        # Only existing gitops checkout plumbing; no script, setup, check,
        # bisect runner, staging/publish/apply or authenticated command execution.
        assert args[0] in ("clone", "config", "-c", "fetch", "rev-parse", "checkout", "remote")
        assert TOKEN not in str(args)
        return await host_git(*args, **kwargs)

    async def exact_checkout(sandbox, name, token, sha, branch):
        assert sha in chain
        actual = await checkout(sandbox, name, token, sha, branch)
        assert git("rev-parse", "HEAD", cwd=sandbox.work) == sha == actual
        assert TOKEN not in (sandbox.work / ".git/config").read_text()
        checked_out.append(sha)
        return actual

    async def exec_check(self, text, **kwargs):
        assert TOKEN not in text
        assert not kwargs.get("env")
        seen.append(text)
        return await sandbox_exec(self, text, **kwargs)

    monkeypatch.setattr(LocalSandbox, "create", track_create)
    monkeypatch.setattr(LocalSandbox, "exec", exec_check)
    monkeypatch.setattr(gitops, "git", control_git)
    monkeypatch.setattr(gitops, "checkout", exact_checkout)
    monkeypatch.setenv("GITHUB_TOKEN", TOKEN)
    monkeypatch.setenv("OTHER_PROVIDER_SECRET", TOKEN)
    # Each repeat must start clean in work, home AND tmp. Checks leave files in
    # all three and even rewrite tracked source after evaluating the revision.
    setup = ['set -e; test ! -e generated && test ! -e "$HOME/leftover" && test ! -e "$TMPDIR/leftover"; '
             'test -z "$GITHUB_TOKEN$OTHER_PROVIDER_SECRET" && touch generated "$HOME/leftover" "$TMPDIR/leftover"']
    check = 'value=$(cat value); echo good > value; test "$value" = good'
    result = await resolve_interval(REPO, TOKEN, dict(good=chain[0], bad=chain[-1], commits=["untrusted"]),
                                    setup=setup, checks=[check], max_probes=12)
    assert result["state"] == "confirmed"
    assert result["first_bad_sha"] == chain[5]
    assert result["candidate_interval"] == dict(good=chain[4], bad=chain[5], commits=[chain[5]])
    assert len(result["probes"]) <= 12
    assert all(n == 2 for n in Counter(p["commit"] for p in result["probes"]).values())
    assert all(s._closed and not s.root.exists() for s in sandboxes)
    assert len(sandboxes) == len(result["probes"]) + 1
    assert checked_out == [chain[-1]] + [p["commit"] for p in result["probes"]]
    for sandbox, probe in zip(sandboxes[1:], result["probes"]):
        assert sandbox.job_id.startswith("steward-bisect-")
        assert probe["commit"] in chain
    assert all(c["duration"] >= 0 and c["exit_code"] is not None for c in result["commands"])
    assert seen.count(check) == len(result["probes"])
    assert TOKEN not in json.dumps(result)
    assert not (get_settings().data_dir / "staging").exists()


@pytest.mark.asyncio
@pytest.mark.parametrize("setup,checks,reason", [
    ([], [], "No configured checks"),
    (["exit 1"], ["true"], "Setup failed"),
    ([], ["true"], "does not reproduce"),
    ([], ["false"], "Good endpoint fails"),
    ([], ["factory-nonexistent-command"], "untestable"),
    ([], ["python /nonexistent/check.py"], "untestable"),
    ([], ["echo 'no tests ran'; exit 5"], "untestable"),
])
async def test_unresolved_local_histories(history, setup, checks, reason):
    chain, _, _ = history
    result = await resolve_interval(REPO, TOKEN, dict(good=chain[0], bad=chain[-1]), setup=setup, checks=checks)
    assert result["state"] == "unresolved"
    assert result["first_bad_sha"] is None
    assert any(reason in e for e in result["evidence"])
    assert not list(get_settings().jobs_dir.glob("steward-*"))


@pytest.mark.asyncio
async def test_first_parent_not_side_branch_and_default_branch_membership(history):
    chain, bare, work = history
    git("checkout", "-qb", "side", chain[0], cwd=work)
    (work / "side").write_text("side")
    git("add", ".", cwd=work)
    git("commit", "-qm", "side", cwd=work)
    side = git("rev-parse", "HEAD", cwd=work)
    git("checkout", "-q", "main", cwd=work)
    git("merge", "-q", "--no-ff", "side", "-m", "merge side", cwd=work)
    git("push", "-q", str(bare), "main", "side", cwd=work)
    result = await resolve_interval(REPO, TOKEN, dict(good=side, bad=chain[-1]), checks=["true"])
    assert result["state"] == "unresolved"
    assert result["probes"] == []
    assert "first-parent" in result["evidence"][0]


class Double:
    instances = []
    chain = [f"{n:040x}" for n in range(9)]

    def __init__(self, job_id):
        self.job_id = job_id
        self.closed = False
        self.calls = []
        self.__class__.instances.append(self)

    def create(self):
        return self

    async def close(self):
        self.closed = True

    async def exec(self, text, *, timeout):
        self.calls.append((text, timeout))
        if text.startswith("git "):
            return ExecResult(text, 0, "\n".join(reversed(self.chain)), 0)
        return ExecResult(text, 0 if self.sha < self.chain[5] else 1, "AssertionError: regression", 0)


@pytest.fixture
def doubles(monkeypatch):
    Double.instances = []
    monkeypatch.setattr(steward_bisect, "LocalSandbox", Double)

    async def checkout(sandbox, name, token, sha, branch):
        assert name == REPO["full_name"] and token == TOKEN
        assert sha in Double.chain  # no branch, moving head or guessed revision
        assert branch == "steward-probe"
        sandbox.sha = sha
        return sha

    monkeypatch.setattr(gitops, "checkout", checkout)
    return dict(good=Double.chain[0], bad=Double.chain[-1])


@pytest.mark.asyncio
async def test_probe_count_is_hard_limit_and_preserves_interval(doubles):
    result = await resolve_interval(REPO, TOKEN, doubles, checks=["test"], max_probes=5)
    assert result["state"] == "unresolved"
    assert result["first_bad_sha"] is None
    assert len(result["probes"]) == 5
    assert "Probe count" in result["evidence"][-1]
    assert all(s.closed for s in Double.instances)
    assert len(Double.instances) == 6


@pytest.mark.asyncio
async def test_contradictory_repeats_are_flaky_not_culprit(doubles, monkeypatch):
    original = Double.exec
    attempts = Counter()

    async def alternating(self, text, **kwargs):
        if text == "test":
            attempts[self.sha] += 1
            return ExecResult(text, attempts[self.sha] % 2, f"AssertionError: {TOKEN}", 0)
        return await original(self, text, **kwargs)

    monkeypatch.setattr(Double, "exec", alternating)
    result = await resolve_interval(REPO, TOKEN, doubles, checks=["test"])
    assert result["state"] == "unresolved"
    assert result["classification"] == "flaky"
    assert result["first_bad_sha"] is None
    assert len(result["probes"]) == 2
    assert "Contradictory" in result["evidence"][-1]
    assert TOKEN not in json.dumps(result)
    assert all(s.closed for s in Double.instances)


@pytest.mark.asyncio
@pytest.mark.parametrize("phase", ["create", "checkout", "ancestry", "setup", "check"])
async def test_exceptional_cleanup(doubles, monkeypatch, phase):
    original = Double.exec

    async def execute(self, text, **kwargs):
        if phase == "ancestry" and text.startswith("git ") or text == phase:
            raise SandboxError(TOKEN)
        return await original(self, text, **kwargs)

    def create(self):
        raise SandboxError(TOKEN)

    async def checkout(*args, **kwargs):
        raise gitops.GitError(TOKEN)

    monkeypatch.setattr(Double, "exec", execute)
    if phase == "create":
        monkeypatch.setattr(Double, "create", create)
    if phase == "checkout":
        monkeypatch.setattr(gitops, "checkout", checkout)
    result = await resolve_interval(REPO, TOKEN, doubles, setup=["setup"], checks=["check"])
    assert result["state"] == "unresolved"
    assert TOKEN not in json.dumps(result)
    assert all(s.closed for s in Double.instances)


@pytest.mark.asyncio
@pytest.mark.parametrize("phase", ["checkout", "ancestry", "setup", "check"])
async def test_cancellation_propagates_and_closes(doubles, monkeypatch, phase):
    entered = asyncio.Event()
    original = Double.exec

    async def block():
        entered.set()
        await asyncio.Future()

    async def execute(self, text, **kwargs):
        if phase == "ancestry" and text.startswith("git ") or text == phase:
            await block()
        return await original(self, text, **kwargs)

    async def checkout(*args, **kwargs):
        await block()

    monkeypatch.setattr(Double, "exec", execute)
    if phase == "checkout":
        monkeypatch.setattr(gitops, "checkout", checkout)
    task = asyncio.create_task(resolve_interval(REPO, TOKEN, doubles, setup=["setup"], checks=["check"]))
    await entered.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert all(s.closed for s in Double.instances)


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["returned", "raised", "overall"])
async def test_timeouts_are_unresolved_and_close_without_sleep(doubles, monkeypatch, mode):
    original = Double.exec
    timeouts = []

    async def execute(self, text, *, timeout):
        timeouts.append(timeout)
        if not text.startswith("git "):
            if mode == "returned":
                return ExecResult(text, 124, TOKEN, 0, timed_out=True)
            if mode == "raised":
                raise TimeoutError(TOKEN)
            # Expire the active overall timeout deterministically on the next
            # loop tick, without a wall-clock sleep/deadline race.
            handle.reschedule(asyncio.get_running_loop().time())
            await asyncio.Future()
        return await original(self, text, timeout=timeout)

    original_timeout = asyncio.timeout
    handle = None

    def timeout_context(budget):
        nonlocal handle
        handle = original_timeout(budget)
        return handle

    monkeypatch.setattr(steward_bisect.asyncio, "timeout", timeout_context)
    monkeypatch.setattr(Double, "exec", execute)
    result = await resolve_interval(REPO, TOKEN, doubles, checks=["test"], command_timeout=7, budget=90)
    assert result["state"] == "unresolved"
    assert result["first_bad_sha"] is None
    assert "budget exhausted" in result["evidence"][-1] if mode == "overall" else "timed out" in result["evidence"][-1]
    assert all(t == 7 for t in timeouts)
    assert TOKEN not in json.dumps(result)
    assert all(s.closed for s in Double.instances)


@pytest.mark.asyncio
async def test_bounds_before_work_and_missing_interval(doubles):
    for kwargs in (dict(max_probes=0), dict(repeats=1), dict(max_commits=201),
                   dict(command_timeout=float("nan")), dict(budget=float("inf")), dict(max_excerpt_chars=0)):
        with pytest.raises(ValueError):
            await resolve_interval(REPO, TOKEN, doubles, checks=["test"], **kwargs)
    for interval in (None, {}, dict(good="main", bad="HEAD")):
        result = await resolve_interval(REPO, TOKEN, interval, checks=["test"])
        assert result["state"] == "unresolved"
    assert Double.instances == []


@pytest.mark.asyncio
async def test_credential_commands_refused_before_sandbox_creation(doubles):
    for commands in ([f"echo {TOKEN}"], ["echo ghp_" + "x" * 30]):
        result = await resolve_interval(REPO, TOKEN, doubles, checks=commands)
        assert "Credential-bearing" in result["evidence"][-1]
        assert TOKEN not in json.dumps(result)
    assert Double.instances == []


@pytest.mark.asyncio
async def test_excerpt_redacts_before_truncation_and_commands_have_evidence(doubles, monkeypatch):
    original = Double.exec

    async def output(self, text, **kwargs):
        execution = await original(self, text, **kwargs)
        if text == "test":
            execution.output = "prefix " + TOKEN
        return execution

    monkeypatch.setattr(Double, "exec", output)
    result = await resolve_interval(REPO, TOKEN, doubles, checks=["test"], max_excerpt_chars=20)
    assert result["state"] == "confirmed"
    assert TOKEN not in json.dumps(result)
    assert TOKEN[-20:] not in json.dumps(result)
    assert all(len(c["excerpt"]) <= 20 for c in result["commands"])
    assert all({"commit", "command", "exit_code", "excerpt", "duration"} <= c.keys() for c in result["commands"])


@pytest.mark.asyncio
async def test_cancellation_during_cleanup_finishes_before_propagating(doubles, monkeypatch):
    closing = asyncio.Event()
    release = asyncio.Event()

    async def close(self):
        closing.set()
        await release.wait()
        self.closed = True

    monkeypatch.setattr(Double, "close", close)
    task = asyncio.create_task(resolve_interval(REPO, TOKEN, doubles, checks=["test"]))
    await closing.wait()
    task.cancel()
    # A second cancellation must not cancel the shielded cleanup either.
    task.cancel()
    release.set()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert len(Double.instances) == 1
    assert all(s.closed for s in Double.instances)


@pytest.mark.asyncio
async def test_clipped_output_is_not_exported_or_used_as_proof(doubles, monkeypatch):
    original = Double.exec

    async def output(self, text, **kwargs):
        execution = await original(self, text, **kwargs)
        if text == "test":
            execution.output = TOKEN[-10:]
            execution.truncated = True
        return execution

    monkeypatch.setattr(Double, "exec", output)
    result = await resolve_interval(REPO, TOKEN, doubles, checks=["test"])
    assert result["state"] == "unresolved"
    assert "truncated" in result["evidence"][-1]
    assert TOKEN[-10:] not in json.dumps(result)
    assert all(s.closed for s in Double.instances)


@pytest.mark.asyncio
async def test_commit_budget_does_not_probe_or_guess(doubles):
    result = await resolve_interval(REPO, TOKEN, doubles, checks=["test"], max_commits=3)
    assert result["state"] == "unresolved"
    assert result["probes"] == []
    assert all(s.closed for s in Double.instances)


@pytest.mark.asyncio
async def test_checkout_sha_mismatch_refuses_to_run_commands(doubles, monkeypatch):
    async def checkout(*args, **kwargs):
        return "f" * 40

    monkeypatch.setattr(gitops, "checkout", checkout)
    result = await resolve_interval(REPO, TOKEN, doubles, checks=["test"])
    assert result["state"] == "unresolved"
    assert result["probes"] == []
    assert "exact SHA" in result["evidence"][-1]
    assert all(s.closed and not s.calls for s in Double.instances)


@pytest.mark.asyncio
async def test_installation_credential_failure_is_unresolved(doubles, monkeypatch):
    from aifactory.github import GitHubError
    from aifactory.github_app import InstallationCredential

    def resolve(self):
        raise GitHubError(TOKEN)

    monkeypatch.setattr(InstallationCredential, "resolve", resolve)
    result = await resolve_interval(REPO, InstallationCredential("workspace", "1"), doubles, checks=["test"])
    assert result["state"] == "unresolved"
    assert TOKEN not in json.dumps(result)
    assert Double.instances == []


@pytest.mark.asyncio
async def test_consumes_ci_diagnosis_with_missing_intermediate_coverage(history):
    from aifactory.steward_ci import diagnose
    from tests.test_steward_ci import History, run

    chain, _, _ = history
    github = History([run(chain[0], identity=1), run(chain[-1], identity=2, conclusion="failure")],
                     chain=tuple(reversed(chain)))
    diagnosis = await diagnose(github, REPO)
    assert diagnosis["needs_bisection"]
    assert diagnosis["first_bad_sha"] is None
    assert diagnosis["candidate_interval"]["good"] == chain[0]
    result = await resolve_interval(REPO, TOKEN, diagnosis["candidate_interval"],
                                    checks=['test "$(cat value)" = good'])
    assert result["state"] == "confirmed"
    assert result["first_bad_sha"] == chain[5]


@pytest.mark.asyncio
async def test_branch_credential_never_enters_sandbox_commands(doubles):
    result = await resolve_interval({**REPO, "default_branch": TOKEN}, TOKEN, doubles, checks=["test"])
    assert result["state"] == "unresolved"
    assert TOKEN not in json.dumps(result)
    assert Double.instances == []
