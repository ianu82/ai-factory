"""Repository sync contract against real local bare Git and FakeGitHub patterns."""
from __future__ import annotations

import pytest

from aifactory import gitops
from aifactory.db import session_scope
from aifactory.definitions import Bundle, KnowledgePage, get_snapshot, history, resolve_definitions, save_bundle
from aifactory.definitions.repository import bundle_from_files, poll_definitions, sync_definitions
from aifactory.definitions.sources import SourceConfig, configuration, select_source
from aifactory.definitions.validation import BUNDLE_LIMIT, DefinitionError
from aifactory.github import GitHubError
from aifactory.models import DefinitionState, Organization, Repository, Workspace
from .fakes import FakeGitHub, git, make_remote

SKILL = "---\nname: testing\ndescription: When testing\n---\nRun the tests.\n"
FILES = {
    ".factory/skills/testing/SKILL.md": SKILL,
    ".factory/skills/testing/docs/example.txt": "example",
    ".factory/agents/builder/instructions.md": "Keep changes focused.",
    ".factory/agents/verifier/skills/check/SKILL.md": SKILL,
    ".factory/knowledge/nested/architecture.md": "The architecture.",
    "outside.txt": "Not a definition",
}


class DefinitionGitHub(FakeGitHub):
    access = True
    branch = "main"
    calls: list[str] = []

    async def repo(self, full_name):
        self.calls.append(full_name)
        if not self.access:
            raise GitHubError("private repository unavailable", 404)
        return {"full_name": full_name, "default_branch": self.branch}


@pytest.fixture
def source(tmp_path, monkeypatch):
    from aifactory.definitions import repository

    remotes = tmp_path / "remotes"
    bare = make_remote(remotes, "org/source", FILES)
    monkeypatch.setattr(gitops, "GIT_BASE", str(remotes))
    monkeypatch.setattr(DefinitionGitHub, "remotes", remotes, raising=False)
    monkeypatch.setattr(DefinitionGitHub, "calls", [])
    monkeypatch.setattr(repository, "GitHub", DefinitionGitHub)
    monkeypatch.setattr(repository, "resolve_credential", lambda ws: "fake-token")
    with session_scope() as db:
        org = Organization(name="Definitions")
        db.add(org)
        db.flush()
        ws = Workspace(org_id=org.id, name="one", slug="one", paused=True)
        other = Workspace(org_id=org.id, name="two", slug="two")
        db.add_all([ws, other])
        db.flush()
        repo = Repository(workspace_id=ws.id, full_name="org/source", default_branch="main")
        scope = Repository(workspace_id=ws.id, full_name="org/target")
        foreign = Repository(workspace_id=other.id, full_name="private/other")
        db.add_all([repo, scope, foreign])
        db.flush()
        save_bundle(db, ws.id, Bundle(knowledge=[KnowledgePage(
            id="managed", name="managed", description="d", body="Managed fallback")]),
            expected_revision=0, author="admin")
        select_source(db, ws.id, SourceConfig(kind="repository", repository_id=repo.id),
                      expected_revision=1, author="admin")
        return {"ws": ws.id, "other": other.id, "repo": repo.id, "scope": scope.id,
                "foreign": foreign.id, "bare": bare, "work": remotes / "seed/org/source"}


def commit(source, changes):
    for name, content in changes.items():
        target = source["work"] / name
        if content is None:
            target.unlink()
        else:
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text(content)
    git("add", "-A", cwd=source["work"])
    git("commit", "-qm", "definitions change", cwd=source["work"])
    git("push", "-q", str(source["bare"]), "main", cwd=source["work"])
    return git("rev-parse", "HEAD", cwd=source["work"])


def active(source):
    with session_scope() as db:
        return get_snapshot(db, source["ws"])


@pytest.mark.asyncio
async def test_initial_update_attribution_scopes_and_unchanged(source):
    first_sha = git("rev-parse", "HEAD", cwd=source["bare"])
    result = await sync_definitions(source["ws"])
    assert result["sync_diagnostics"]["status"] == "synced"
    first = active(source)
    assert first.revision == 3 and first.author == "t <t@t>"
    assert next(s for s in first.bundle.skills if s.role is None).files == {"docs/example.txt": "example"}
    assert {s.role for s in first.bundle.skills} == {None, "verifier"}
    assert first.bundle.instructions[0].role == "builder"
    assert first.bundle.knowledge[0].body == "The architecture."
    assert all(e.source.commit == first_sha for e in first.bundle.entries())
    assert all(e.source.path.startswith(".factory/") for e in first.bundle.entries())
    assert all(e.source.repository_id == source["repo"] for e in first.bundle.entries())
    await sync_definitions(source["ws"])
    assert active(source).revision == 3
    sha = commit(source, {
        ".factory/skills/testing/SKILL.md": SKILL.replace("description:", "repository: org/target\ndescription:"),
        ".factory/knowledge/nested/architecture.md": "Updated architecture",
    })
    result = await sync_definitions(source["ws"])
    updated = active(source)
    assert updated.revision == 4
    assert updated.bundle.knowledge[0].body == "Updated architecture"
    assert {e.id for e in first.bundle.entries()} == {e.id for e in updated.bundle.entries()}
    assert result["sync_diagnostics"]["last_valid_sha"] == sha
    assert result["sync_diagnostics"]["last_valid_revision"] == 4
    with session_scope() as db:
        assert len(resolve_definitions(db, source["ws"], "builder", task_repository_id=source["repo"]).skills) == 0
        assert len(resolve_definitions(db, source["ws"], "builder", task_repository_id=source["scope"]).skills) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("invalid", ["bad frontmatter", "---\nname: testing\ndescription: d\n---\n" + "x" * 65_537,
                                    "x" * (BUNDLE_LIMIT + 1)])
async def test_invalid_keeps_last_valid_then_recovers(source, invalid):
    await sync_definitions(source["ws"])
    before = active(source)
    sha = commit(source, {".factory/skills/testing/SKILL.md": invalid})
    failed = (await sync_definitions(source["ws"]))["sync_diagnostics"]
    assert failed["status"] == "invalid" and failed["attempted_sha"] == sha
    assert failed["last_valid_sha"] != sha
    assert failed["last_valid_revision"] == before.revision
    assert failed["errors"]
    assert active(source) == before
    with session_scope() as db:
        assert resolve_definitions(db, source["ws"], "builder", task_repository_id=source["repo"]).lookup("testing").body == "Run the tests.\n"
    good = commit(source, {".factory/skills/testing/SKILL.md": SKILL + "Now valid"})
    result = (await sync_definitions(source["ws"]))["sync_diagnostics"]
    assert result["errors"] == [] and result["last_valid_sha"] == good
    assert active(source).revision == before.revision + 1


@pytest.mark.asyncio
async def test_return_to_last_valid_sha_clears_errors_without_revision_or_wrong_author(source):
    await sync_definitions(source["ws"])
    before = active(source)
    valid_sha = git("rev-parse", "HEAD", cwd=source["work"])
    commit(source, {".factory/skills/testing/SKILL.md": "invalid"})
    # Distinct author on the failed attempt must not leak into a later successful
    # no-op sync when the branch is rolled back to the last-valid SHA.
    git("commit", "--amend", "--author=Invalid Author <t@t>", "--no-edit", "-q", cwd=source["work"])
    git("push", "-q", "--force", str(source["bare"]), "main", cwd=source["work"])
    failed = (await sync_definitions(source["ws"]))["sync_diagnostics"]
    assert failed["attempted_git_author"] == "Invalid Author <t@t>"
    git("reset", "--hard", valid_sha, cwd=source["work"])
    git("push", "-q", "--force", str(source["bare"]), "main", cwd=source["work"])
    diagnostics = (await sync_definitions(source["ws"]))["sync_diagnostics"]
    assert diagnostics["status"] == "synced" and diagnostics["errors"] == []
    assert diagnostics["attempted_sha"] == valid_sha
    assert diagnostics["git_author"] == diagnostics["attempted_git_author"] == "t <t@t>"
    assert diagnostics["last_valid_sha"] == valid_sha
    assert diagnostics["last_valid_revision"] == before.revision
    assert active(source) == before


@pytest.mark.asyncio
async def test_mirror_failure_resets_attempt_attribution_but_retains_last_valid(source, monkeypatch):
    await sync_definitions(source["ws"])
    before = active(source)
    valid_sha = git("rev-parse", "HEAD", cwd=source["bare"])
    sha = commit(source, {".factory/knowledge/new.md": "New commit"})

    async def unavailable(*args):
        raise gitops.GitError("mirror unavailable")

    monkeypatch.setattr(gitops, "mirror", unavailable)
    diagnostics = (await sync_definitions(source["ws"]))["sync_diagnostics"]
    assert diagnostics["status"] == "invalid" and diagnostics["errors"]
    assert diagnostics["attempted_sha"] == sha
    assert diagnostics["attempted_git_author"] is None
    assert diagnostics["git_author"] is None
    assert diagnostics["source_url"] == f"https://github.com/org/source/tree/{sha}/.factory"
    assert diagnostics["last_valid_sha"] == valid_sha
    assert diagnostics["last_valid_revision"] == before.revision
    assert diagnostics["last_valid_git_author"] == before.author
    assert active(source) == before

    # Fail even earlier on the next attempt: neither SHA nor link is inherited.
    monkeypatch.setattr(DefinitionGitHub, "access", False)
    diagnostics = (await sync_definitions(source["ws"]))["sync_diagnostics"]
    assert diagnostics["attempted_sha"] is None
    assert diagnostics["attempted_git_author"] is None
    assert diagnostics["source_url"] is None
    assert diagnostics["last_valid_sha"] == valid_sha
    assert diagnostics["last_valid_git_author"] == before.author


@pytest.mark.asyncio
async def test_initial_failure_managed_fallback_deletions_and_switch_back(source):
    bad = commit(source, {".factory/skills/testing/SKILL.md": "invalid"})
    await sync_definitions(source["ws"])
    assert active(source).bundle.knowledge[0].body == "Managed fallback"
    assert active(source).revision == 2
    commit(source, {".factory/skills/testing/SKILL.md": SKILL})
    await sync_definitions(source["ws"])
    commit(source, {".factory/knowledge/nested/architecture.md": None})
    await sync_definitions(source["ws"])
    assert active(source).bundle.knowledge == []
    with session_scope() as db:
        select_source(db, source["ws"], SourceConfig(), expected_revision=4, author="switching admin")
    restored = active(source)
    assert restored.revision == 5 and restored.bundle.knowledge[0].body == "Managed fallback"
    assert restored.author == "switching admin"
    with session_scope() as db:
        assert configuration(db, source["ws"])["active_source"] == {"kind": "managed"}
        assert len(history(db, source["ws"])) == 5
        assert get_snapshot(db, source["ws"], 3).bundle.skills
        assert configuration(db, source["ws"])["sync_diagnostics"]["attempted_sha"] != bad


@pytest.mark.asyncio
async def test_folder_removal_is_empty_valid_bundle(source):
    await sync_definitions(source["ws"])
    commit(source, {p: None for p in FILES if p.startswith(".factory/")})
    await sync_definitions(source["ws"])
    assert active(source).bundle == Bundle()
    assert active(source).revision == 4


@pytest.mark.asyncio
@pytest.mark.parametrize("unsafe", ["symlink", "ancestor", "submodule"])
async def test_unsafe_git_entries_never_published(source, unsafe):
    await sync_definitions(source["ws"])
    before = active(source)
    work = source["work"]
    if unsafe == "ancestor":
        git("rm", "-qr", ".factory", cwd=work)
        (work / ".factory").symlink_to("/etc")
        git("add", ".factory", cwd=work)
    elif unsafe == "symlink":
        (work / ".factory/skills/testing/link.md").symlink_to("/etc/passwd")
        git("add", "-A", cwd=work)
    else:
        sha = git("rev-parse", "HEAD", cwd=work)
        git("update-index", "--add", "--cacheinfo", f"160000,{sha},.factory/knowledge/submodule", cwd=work)
    git("commit", "-qm", "unsafe entry", cwd=work)
    git("push", "-q", str(source["bare"]), "main", cwd=work)
    result = await sync_definitions(source["ws"])
    assert result["sync_diagnostics"]["status"] == "invalid"
    assert active(source) == before


@pytest.mark.asyncio
async def test_foreign_source_rejected_before_credentials_or_shared_mirror(source, monkeypatch):
    from aifactory.definitions import repository

    with session_scope() as db:
        state = db.get(DefinitionState, source["ws"])
        state.desired_source = {"kind": "repository", "repository_id": source["foreign"], "folder": ".factory"}
    def forbidden(*args):
        pytest.fail("must not resolve credentials or access a mirror for a foreign repository")
    monkeypatch.setattr(repository, "resolve_credential", forbidden)
    monkeypatch.setattr(gitops, "mirror", forbidden)
    with pytest.raises(DefinitionError, match="this workspace"):
        await sync_definitions(source["ws"])


@pytest.mark.asyncio
async def test_cached_private_content_requires_current_workspace_remote_access(source, monkeypatch):
    await sync_definitions(source["ws"])
    before = active(source)
    monkeypatch.setattr(DefinitionGitHub, "access", False)
    def forbidden(*args):
        pytest.fail("must not access shared cache without remote permission")
    monkeypatch.setattr(gitops, "mirror", forbidden)
    result = await sync_definitions(source["ws"])
    assert result["sync_diagnostics"]["status"] == "invalid"
    assert active(source) == before


@pytest.mark.asyncio
async def test_foreign_scope_rejected_and_whole_candidate_kept(source):
    await sync_definitions(source["ws"])
    before = active(source)
    commit(source, {".factory/skills/testing/SKILL.md": SKILL.replace("description:", "repository: private/other\ndescription:"),
                    ".factory/knowledge/nested/architecture.md": "Not partially published"})
    result = await sync_definitions(source["ws"])
    assert result["sync_diagnostics"]["status"] == "invalid"
    assert result["sync_diagnostics"]["errors"][0]["field"] == "repository"
    assert active(source) == before


@pytest.mark.asyncio
async def test_oversized_blob_not_loaded(source, monkeypatch):
    commit(source, {".factory/knowledge/large.md": "x" * (BUNDLE_LIMIT + 1)})
    original = gitops.read_object
    sizes = set()
    async def tracked(*args, **kwargs):
        if args[:2] == ("cat-file", "-s"):
            result = await original(*args, **kwargs)
            if int(result.strip()) > BUNDLE_LIMIT:
                sizes.add(args[2])
            return result
        if args[:2] == ("cat-file", "blob"):
            assert args[2] not in sizes, "oversized object read before rejecting its size"
        return await original(*args, **kwargs)
    monkeypatch.setattr(gitops, "read_object", tracked)
    result = await sync_definitions(source["ws"])
    assert result["sync_diagnostics"]["status"] == "invalid" and sizes
    assert active(source).revision == 2


@pytest.mark.asyncio
async def test_network_io_does_not_hold_write_lock_and_source_race_discards_candidate(source, monkeypatch):
    from aifactory.definitions import repository
    from aifactory.definitions import RevisionConflict

    original = repository.read_repository
    async def racing(*args, **kwargs):
        # A separate transaction can commit while object/network I/O is in flight.
        with session_scope() as db:
            select_source(db, source["ws"], SourceConfig(), expected_revision=2, author="another admin")
        return await original(*args, **kwargs)
    monkeypatch.setattr(repository, "read_repository", racing)
    with pytest.raises(RevisionConflict, match="changed during sync"):
        await sync_definitions(source["ws"])
    snapshot = active(source)
    assert snapshot.revision == 3 and snapshot.bundle.knowledge[0].body == "Managed fallback"
    with session_scope() as db:
        assert configuration(db, source["ws"])["active_source"] == {"kind": "managed"}


@pytest.mark.asyncio
async def test_poll_ignores_signal_settings_and_workspace_pause(source, monkeypatch):
    monkeypatch.setenv("AIFACTORY_SIGNAL_INTERVAL_SECONDS", "0")
    with session_scope() as db:
        ws = db.get(Workspace, source["ws"])
        ws.settings = {"signals": {"auto_sync": False}}
    await poll_definitions()
    assert active(source).revision == 3


@pytest.mark.asyncio
async def test_poll_isolates_hanging_mirror_and_records_its_timeout(source, monkeypatch):
    import asyncio
    from contextlib import asynccontextmanager
    from aifactory.definitions import repository

    await sync_definitions(source["ws"])
    before = active(source)
    sha = commit(source, {".factory/knowledge/new.md": "New commit"})
    with session_scope() as db:
        # A second owned repository uses the same local bare repo, without any
        # shared database state or credentials across the two workspaces.
        repo = Repository(workspace_id=source["other"], full_name="org/source", default_branch="main")
        db.add(repo)
        db.flush()
        select_source(db, source["other"], SourceConfig(kind="repository", repository_id=repo.id),
                      expected_revision=0, author="admin")

    started = asyncio.Event()
    other_finished = asyncio.Event()
    timeouts = {}
    real_timeout = asyncio.timeout
    real_mirror = gitops.mirror

    @asynccontextmanager
    async def controlled_timeout(seconds):
        assert seconds == repository._SYNC_TIMEOUT_SECONDS
        # Exercise real asyncio timeout cancellation, driven by events rather
        # than wall-clock delays. Both workspaces get independent deadlines.
        async with real_timeout(None) as timeout:
            timeouts[asyncio.current_task()] = timeout
            yield timeout

    async def mirror(*args):
        if not started.is_set():
            started.set()
            hanging_task.append(asyncio.current_task())
            await asyncio.Event().wait()
        return await real_mirror(*args)

    async def tracked_sync(workspace_id):
        # Deterministically make the fixture's first workspace the slow one,
        # regardless of database query ordering.
        if workspace_id == source["other"]:
            await started.wait()
        result = await sync_definitions(workspace_id)
        if workspace_id == source["other"]:
            other_finished.set()
        return result

    hanging_task = []
    monkeypatch.setattr(repository.asyncio, "timeout", controlled_timeout)
    monkeypatch.setattr(gitops, "mirror", mirror)
    monkeypatch.setattr(repository, "sync_definitions", tracked_sync)
    poll = asyncio.create_task(poll_definitions())
    try:
        # Guard the test itself against a serial-poll regression hanging forever.
        async with real_timeout(10):
            await other_finished.wait()
        assert not poll.done()
        with session_scope() as db:
            assert get_snapshot(db, source["other"]).revision == 2
            diagnostics = configuration(db, source["ws"])["sync_diagnostics"]
            assert diagnostics["status"] == "syncing"
            assert diagnostics["attempted_sha"] == sha
            assert diagnostics["attempted_git_author"] is None
        timeouts[hanging_task[0]].reschedule(asyncio.get_running_loop().time())
        await poll
    finally:
        poll.cancel()
        await asyncio.gather(poll, return_exceptions=True)

    with session_scope() as db:
        diagnostics = configuration(db, source["ws"])["sync_diagnostics"]
    assert diagnostics["status"] == "invalid"
    assert "timed out" in diagnostics["errors"][0]["message"]
    assert diagnostics["attempted_sha"] == sha
    assert diagnostics["attempted_git_author"] is None
    assert diagnostics["last_valid_revision"] == before.revision
    assert diagnostics["last_valid_git_author"] == before.author
    assert active(source) == before


@pytest.mark.parametrize("path", ["../knowledge/a.md", "/knowledge/a.md", "skills/../SKILL.md",
                                  "knowledge/a\\b.md", "agents/director/instructions.md"])
def test_unsafe_paths_and_unknown_roles(path):
    with pytest.raises(DefinitionError):
        bundle_from_files({path: SKILL}, repository_id="repo", commit="a" * 40,
                          folder=".factory", repositories={"org/source": "repo"})


@pytest.mark.asyncio
async def test_canonical_quota_includes_live_repository_notes(source):
    with session_scope() as db:
        repo = db.get(Repository, source["repo"])
        repo.config = {"context": "x" * (BUNDLE_LIMIT - 100)}
    result = await sync_definitions(source["ws"])
    assert result["sync_diagnostics"]["status"] == "invalid"
    assert result["sync_diagnostics"]["errors"]
    assert active(source).revision == 2


def test_sync_api_uses_service_and_returns_provenance(client, signed_in, source, monkeypatch):
    from aifactory.definitions import repository
    from aifactory.models import Membership

    ws = source["ws"]
    with session_scope() as db:
        db.add(Membership(workspace_id=ws, user_id=signed_in["user"]["id"], role="admin"))
    root = f"/api/workspaces/{ws}/definitions"
    response = client.post(root + "/sync")
    assert response.status_code == 200, response.text
    payload = response.json()
    assert payload["revision"] == 3
    assert payload["sync_diagnostics"]["git_author"] == "t <t@t>"
    sha = payload["sync_diagnostics"]["last_valid_sha"]
    assert any(link["url"] == f"https://github.com/org/source/blob/{sha}/.factory/skills/testing/SKILL.md"
               for link in payload["source_links"])
    assert client.post(root + "/sync").json()["revision"] == 3
    commit(source, {".factory/skills/testing/SKILL.md": "Invalid"})
    invalid = client.post(root + "/sync").json()
    assert invalid["sync_diagnostics"]["status"] == "invalid"
    assert invalid["bundle"] == payload["bundle"] and invalid["revision"] == 3
    assert invalid["sync_diagnostics"]["attempted_git_author"] == "t <t@t>"
    with session_scope() as db:
        member = db.query(Membership).filter_by(workspace_id=ws, user_id=signed_in["user"]["id"]).one()
        member.role = "viewer"
    async def forbidden(*args):
        pytest.fail("viewer must not call sync service")
    monkeypatch.setattr(repository, "sync_definitions", forbidden)
    assert client.post(root + "/sync").status_code == 403


def test_sync_api_managed_mode_conflict_and_auth(client, signed_in):
    from fastapi.testclient import TestClient

    ws = signed_in["workspaces"][0]["id"]
    path = f"/api/workspaces/{ws}/definitions/sync"
    assert client.post(path).status_code == 409
    assert TestClient(client.app).post(path).status_code == 401
    assert client.post("/api/workspaces/foreign/definitions/sync").status_code == 404


@pytest.mark.asyncio
async def test_default_branch_resolved_remotely_and_configured_folder_only(source, monkeypatch):
    # A huge binary file outside the configured folder must not be inspected.
    work = source["work"]
    (work / "unrelated.bin").write_bytes(b"\x00" * (BUNDLE_LIMIT + 1))
    git("add", "unrelated.bin", cwd=work)
    git("commit", "-qm", "unrelated blob", cwd=work)
    git("checkout", "-qb", "new-default", cwd=work)
    (work / ".factory/knowledge/nested/architecture.md").write_text("New default branch")
    git("commit", "-qam", "new branch definition", cwd=work)
    git("push", "-q", str(source["bare"]), "new-default", cwd=work)
    monkeypatch.setattr(DefinitionGitHub, "branch", "new-default")
    result = await sync_definitions(source["ws"])
    assert result["sync_diagnostics"]["status"] == "synced"
    assert active(source).bundle.knowledge[0].body == "New default branch"


@pytest.mark.asyncio
async def test_switch_between_repository_folders_keeps_previous_until_valid(source):
    await sync_definitions(source["ws"])
    commit(source, {"config/knowledge/new.md": "Replacement", "config/skills/bad/SKILL.md": "invalid"})
    with session_scope() as db:
        select_source(db, source["ws"], SourceConfig(kind="repository", repository_id=source["repo"], folder="config"),
                      expected_revision=3, author="admin")
    before = active(source)
    invalid = await sync_definitions(source["ws"])
    assert invalid["sync_diagnostics"]["status"] == "invalid"
    assert active(source) == before
    commit(source, {"config/skills/bad/SKILL.md": None})
    await sync_definitions(source["ws"])
    assert active(source).revision == 5
    assert active(source).bundle.skills == []
    assert active(source).bundle.knowledge[0].body == "Replacement"
    with session_scope() as db:
        select_source(db, source["ws"], SourceConfig(), expected_revision=5, author="admin")
    assert active(source).bundle.knowledge[0].body == "Managed fallback"


@pytest.mark.asyncio
async def test_tree_count_limit_before_unbounded_walk(source, monkeypatch):
    from aifactory.definitions import repository

    monkeypatch.setattr(repository, "MAX_FOLDER_ENTRIES", 2)
    result = await sync_definitions(source["ws"])
    assert result["sync_diagnostics"]["status"] == "invalid"
    assert "tree entries" in result["sync_diagnostics"]["errors"][0]["message"]


@pytest.mark.asyncio
async def test_orchestrator_definition_clock_is_separate_and_cancellable(monkeypatch):
    import asyncio
    from aifactory.definitions import repository
    from aifactory.pipeline.orchestrator import Orchestrator

    calls = []
    async def poll():
        calls.append("poll")
    async def sleep(interval):
        assert interval == 180
        raise asyncio.CancelledError()
    monkeypatch.setattr(repository, "poll_definitions", poll)
    monkeypatch.setattr(asyncio, "sleep", sleep)
    monkeypatch.setenv("AIFACTORY_SIGNAL_INTERVAL_SECONDS", "0")
    orchestrator = Orchestrator()
    with pytest.raises(asyncio.CancelledError):
        await orchestrator._poll_definitions()
    assert calls == ["poll"]


@pytest.mark.asyncio
async def test_sync_cancellation_reaps_git_before_releasing_mirror(source, monkeypatch):
    import asyncio

    started = asyncio.Event()
    cleaned = []

    class HangingGit:
        returncode = None

        async def communicate(self, data):
            started.set()
            await asyncio.Event().wait()

        def kill(self):
            cleaned.append("killed")
            self.returncode = -9

        async def wait(self):
            cleaned.append("reaped")
            return self.returncode

    async def spawn(*args, **kwargs):
        return HangingGit()

    monkeypatch.setattr(asyncio, "create_subprocess_exec", spawn)
    sync = asyncio.create_task(sync_definitions(source["ws"]))
    await started.wait()
    sync.cancel()
    with pytest.raises(asyncio.CancelledError):
        await sync
    assert cleaned == ["killed", "reaped"]
    # Cleanup completed before cancellation unwound the mirror's context.
    lock = gitops._mirror_lock(gitops._mirror_path("org/source"))
    assert not lock.locked()
    assert active(source).revision == 2
