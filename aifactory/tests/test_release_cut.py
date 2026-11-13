"""Release cuts exercise GitHub REST semantics against real local Git objects."""
from __future__ import annotations

import base64
from copy import deepcopy
from datetime import datetime
import json
from pathlib import Path
import subprocess

import pytest
from sqlalchemy import select

from aifactory import db
from aifactory.github import GitHubError
from aifactory.models import (Outcome, Plan, Release, ReleaseStep, ReleaseTrainCursor,
                              Repository, Workspace)
from aifactory.release_config import VersionFile
from aifactory.release_cut import (CutConflict, cut_release, discover_candidate, edit_version,
                                   render_notes)
from tests.fakes import FakeGitHub, git, make_remote


class ReleaseGitHub(FakeGitHub):
    """The release REST surface, backed by the same bare git convention as FakeGitHub."""

    def __init__(self, remote, full_name):
        super().__init__("fake")
        self.remote, self.full_name = remote, full_name
        self.remotes = remote.parents[1]
        self.pulls = {}
        self.writes = []
        self.reads = []
        self.lose = None
        self.hide_once = None
        self.reject = None
        self.reverse = False

    def _bare(self, full_name):
        assert full_name == self.full_name
        return self.remote

    def obj(self, sha):
        kind = git("cat-file", "-t", sha, cwd=self.remote)
        if kind == "commit":
            return {"sha": sha, "tree": {"sha": git("rev-parse", f"{sha}^{{tree}}", cwd=self.remote)},
                    "parents": [{"sha": p} for p in git("show", "-s", "--format=%P", sha, cwd=self.remote).split()],
                    "commit": {"message": git("show", "-s", "--format=%B", sha, cwd=self.remote)}}
        if kind == "tree":
            entries = []
            for line in git("ls-tree", sha, cwd=self.remote).splitlines():
                metadata, path = line.split("\t")
                mode, entry_kind, child = metadata.split()
                entries.append({"mode": mode, "type": entry_kind, "sha": child, "path": path})
            return {"sha": sha, "tree": entries, "truncated": False}
        raw = subprocess.run(["git", "cat-file", "blob", sha], cwd=self.remote, check=True, capture_output=True).stdout
        return {"sha": sha, "encoding": "base64", "size": len(raw), "content": base64.b64encode(raw).decode()}

    def write_object(self, kind, raw):
        return subprocess.run(["git", "hash-object", "-w", "-t", kind, "--stdin"], input=raw,
                              cwd=self.remote, check=True, capture_output=True).stdout.decode().strip()

    async def request(self, method, path, *, json=None, params=None, **kwargs):
        prefix = f"/repos/{self.full_name}/"
        assert path.startswith(prefix)
        path = path[len(prefix):]
        params = params or {}
        if method == "POST":
            assert kwargs.get("retries") == 0, "release writes must not use default HTTP retries"
            with db.session_scope() as s:
                intents = s.scalars(select(ReleaseStep).where(ReleaseStep.state == "dispatching")).all()
                assert any(i.external_handle.get("path") == prefix + path and (
                    i.external_handle.get("payload") == json or "object_sha" in i.external_handle) for i in intents)
            self.writes.append(path)
            if self.reject == path:
                raise GitHubError("branch protection", 403)
        else:
            self.reads.append((path, deepcopy(params)))
        if self.hide_once == path and method == "GET":
            self.hide_once = None
            if path == "pulls":
                return []
            raise GitHubError("not visible yet", 404)
        if path.startswith("git/ref/") and method == "GET":
            try:
                return {"object": {"sha": git("rev-parse", "--verify", "refs/" + path[8:], cwd=self.remote)}}
            except subprocess.CalledProcessError:
                raise GitHubError("not found", 404)
        if path == "git/refs" and method == "POST":
            ref, sha = json["ref"], json["sha"]
            git("update-ref", ref, sha, "0" * 40, cwd=self.remote)
            response = {"ref": ref, "object": {"sha": sha}}
        elif path == "tags":
            response = [{"name": name} for name in git("tag", cwd=self.remote).splitlines()]
        elif path.startswith("compare/"):
            base, head = path[8:].split("...")
            ancestor = git("merge-base", base, head, cwd=self.remote)
            shas = git("rev-list", "--reverse", f"{base}..{head}", cwd=self.remote).splitlines()
            response = {"merge_base_commit": {"sha": ancestor}, "total_commits": len(shas),
                        "status": "identical" if base == head else "ahead" if ancestor == base else "diverged",
                        "commits": [self.obj(sha) for sha in shas]}
            response["commits"] = self.paginate(response["commits"], params)
        elif path == "commits":
            shas = git("rev-list", params["sha"], cwd=self.remote).splitlines()
            response = [self.obj(sha) for sha in shas]
        elif path.startswith("commits/") and path.endswith("/pulls"):
            sha = path.split("/")[1]
            response = [deepcopy(p) for p in self.pulls.values() if p.get("merge_commit_sha") == sha]
        elif path == "pulls" and method == "GET":
            branch = params["head"].split(":", 1)[1]
            response = [deepcopy(p) for p in self.pulls.values() if p["head"]["ref"] == branch]
        elif path.startswith("pulls/") and method == "GET":
            return deepcopy(self.pulls[int(path.split("/")[1])])
        elif path == "pulls" and method == "POST":
            number = max(self.pulls, default=0) + 1
            response = {"number": number, "html_url": f"https://github.com/{self.full_name}/pull/{number}",
                        "state": "open", "merged": False, "merged_at": None, **json,
                        "head": {"ref": json["head"], "sha": await self.branch_sha(self.full_name, json["head"]),
                                 "repo": {"full_name": self.full_name}},
                        "base": {"ref": json["base"], "repo": {"full_name": self.full_name}}}
            self.pulls[number] = deepcopy(response)
        elif path.startswith("git/"):
            _, kind, *ident = path.split("/")
            if method == "GET":
                try:
                    return self.obj(ident[0])
                except subprocess.CalledProcessError:
                    raise GitHubError("not found", 404)
            if kind == "blobs":
                sha = self.write_object("blob", base64.b64decode(json["content"]))
            elif kind == "trees":
                entries = sorted(json["tree"], key=lambda e: (e["path"] + ("/" if e["type"] == "tree" else "")).encode())
                raw = b"".join(e["mode"].lstrip("0").encode() + b" " + e["path"].encode() + b"\0" + bytes.fromhex(e["sha"]) for e in entries)
                sha = self.write_object("tree", raw)
            elif kind == "commits":
                def who(person):
                    timestamp = int(datetime.fromisoformat(person["date"].replace("Z", "+00:00")).timestamp())
                    return f"{person['name']} <{person['email']}> {timestamp} +0000"
                raw = (f"tree {json['tree']}\n" + "".join(f"parent {p}\n" for p in json["parents"])
                       + f"author {who(json['author'])}\ncommitter {who(json['committer'])}\n\n{json['message']}").encode()
                sha = self.write_object("commit", raw)
            else:
                raise AssertionError(path)
            response = {"sha": sha}
        else:
            raise AssertionError((method, path))
        if method == "GET" and isinstance(response, list):
            if self.reverse:
                response.reverse()
            response = self.paginate(response, params)
        if self.lose == path and method == "POST":
            self.lose = None
            # The recovery must work in a separate invocation too, not just inline.
            if path == "git/refs":
                self.hide_once = "git/ref/" + json["ref"].removeprefix("refs/")
            elif path == "pulls":
                self.hide_once = "pulls"
            else:
                self.hide_once = path + "/" + response["sha"]
            raise GitHubError("lost response", 503)
        return response

    @staticmethod
    def paginate(items, params):
        start = (params.get("page", 1) - 1) * params.get("per_page", 100)
        return items[start:start + params.get("per_page", 100)]

    async def pull(self, full_name, number):
        return deepcopy(self.pulls[number])

    async def merge_release(self, number):
        # The Factory never calls merge; stand in for a human/protected merge queue.
        result = await super().merge(self.full_name, number, sha=self.pulls[number]["head"]["sha"],
                                     method="squash", title=self.pulls[number]["title"])
        self.pulls[number]["merged_at"] = "2026-11-13T10:00:00Z"
        return result["sha"]


def commit_file(remote: Path, path="change.txt", content="new", message="external change"):
    work = remote.parent / "editor"
    if not work.exists():
        git("clone", "-q", str(remote), str(work))
    git("fetch", "-q", "origin", cwd=work)
    git("checkout", "-q", "-B", "main", "origin/main", cwd=work)
    file = work / path
    file.parent.mkdir(parents=True, exist_ok=True)
    file.write_text(content)
    git("add", "-A", cwd=work)
    git("commit", "-qm", message, cwd=work)
    git("push", "-q", "origin", "main", cwd=work)
    return git("rev-parse", "HEAD", cwd=work)


@pytest.fixture
def cut_repo(tmp_path, signed_in):
    full_name = "acme/releases"
    remote = make_remote(tmp_path / "remotes", full_name,
                         {"package.json": '{\n  "name": "demo", "version": "1.2.3", "nested": {"version": "untouched"}\n}\n',
                          "VERSION": "1.2.3\n", "nested/VERSION": "1.2.3\n"})
    gh = ReleaseGitHub(remote, full_name)
    baseline = git("rev-parse", "main", cwd=remote)
    head = commit_file(remote)
    ws = signed_in["workspaces"][0]["id"]
    with db.session_scope() as s:
        repo = Repository(workspace_id=ws, full_name=full_name)
        s.add(repo)
        s.flush()
        cursor = ReleaseTrainCursor(repo_id=repo.id, workspace_id=ws, baseline_sha=baseline, last_version="1.2.3")
        s.add(cursor)
        r = Release(workspace_id=ws, repo_id=repo.id, scheduled_slot="2026-11-13T09:00[UTC]",
                    scheduled_at=db.utcnow(), baseline_sha=baseline,
                    config_snapshot={"cut": {"mode": "tag"}})
        s.add(r)
        s.flush()
        rid = r.id
    return gh, rid, baseline, head


def version_mode(rid, path="package.json", format="json", key="version"):
    with db.session_scope() as s:
        s.get(Release, rid).config_snapshot = {"cut": {"mode": "version_pr", "version_file": {
            "path": path, "format": format, **({"key": key} if key else {})}}}


def record(rid):
    with db.session_scope() as s:
        return s.get(Release, rid)


def assert_cursor_unchanged(rid, baseline):
    with db.session_scope() as s:
        cursor = s.get(ReleaseTrainCursor, record(rid).repo_id)
        assert cursor.baseline_sha == baseline
        assert cursor.last_version == "1.2.3"


async def test_tag_cut_is_immutable_and_recovers_lost_response(cut_repo):
    gh, rid, baseline, head = cut_repo
    gh.lose = "git/refs"
    first = await cut_release(gh, rid)
    assert first.status == "unknown"
    commit_file(gh.remote, content="later", message="NOT IN RELEASE")
    result = await cut_release(gh, rid)
    assert result.deployable
    assert result.candidate_sha == head
    assert result.artifact["tag"] == "v1.2.4"
    assert gh.writes == ["git/refs"]
    assert "NOT IN RELEASE" not in record(rid).notes
    assert (await cut_release(gh, rid)) == result
    assert_cursor_unchanged(rid, baseline)


@pytest.mark.parametrize("lost", ["git/blobs", "git/trees", "git/commits", "git/refs", "pulls"])
async def test_version_pr_recovers_every_lost_write_and_waits_for_merge(cut_repo, lost):
    gh, rid, baseline, head = cut_repo
    version_mode(rid)
    gh.lose = lost
    assert (await cut_release(gh, rid)).status == "unknown"
    waiting = await cut_release(gh, rid)
    assert waiting.status == "waiting"
    assert not waiting.deployable
    assert len(gh.pulls) == 1
    assert gh.writes.count(lost) == 1
    number = waiting.artifact["pr_number"]
    branch = waiting.artifact["branch"]
    branch_sha = await gh.branch_sha(gh.full_name, branch)
    assert git("diff", "--name-only", head, branch_sha, cwd=gh.remote) == "package.json"
    content = git("show", f"{branch_sha}:package.json", cwd=gh.remote)
    assert '"version": "1.2.4"' in content
    assert '"nested": {"version": "untouched"}' in content
    later = commit_file(gh.remote, content="intervening", message="Intervening main change")
    merged = await gh.merge_release(number)
    result = await cut_release(gh, rid)
    assert result.deployable and result.candidate_sha == merged
    r = record(rid)
    assert "Intervening main change" in r.notes
    assert later in r.notes
    assert "chore: release" not in r.notes
    assert "Release 1.2.4" in r.notes
    assert not any(i.get("number") == number and i["kind"] == "pr" for i in r.included_items)
    assert (await cut_release(gh, rid)).candidate_sha == merged
    assert len(gh.pulls) == 1
    assert_cursor_unchanged(rid, baseline)


async def test_closed_pr_never_deploys(cut_repo):
    gh, rid, baseline, _ = cut_repo
    version_mode(rid)
    result = await cut_release(gh, rid)
    gh.pulls[result.artifact["pr_number"]]["state"] = "closed"
    assert (await cut_release(gh, rid)).status == "conflict"
    assert len(gh.pulls) == 1
    assert_cursor_unchanged(rid, baseline)


async def test_protection_failure_is_not_retried_or_approved(cut_repo):
    gh, rid, baseline, _ = cut_repo
    version_mode(rid)
    gh.reject = "pulls"
    assert (await cut_release(gh, rid)).status == "unknown"
    assert (await cut_release(gh, rid)).status == "unknown"
    assert gh.writes.count("pulls") == 1
    assert not gh.pulls
    assert_cursor_unchanged(rid, baseline)


async def test_tag_conflict_stops_without_writing(cut_repo):
    gh, rid, baseline, head = cut_repo
    # Reserve version/head, then simulate another actor creating the chosen name.
    from aifactory.release_cut import _prepare
    await _prepare(gh, rid)
    git("update-ref", "refs/tags/v1.2.4", baseline, cwd=gh.remote)
    result = await cut_release(gh, rid)
    assert result.status == "conflict"
    assert gh.writes == []
    assert record(rid).candidate_sha == head
    assert_cursor_unchanged(rid, baseline)


async def test_branch_divergence_stops_without_force_push(cut_repo):
    gh, rid, baseline, _ = cut_repo
    version_mode(rid)
    git("update-ref", f"refs/heads/factory/release/{rid}", baseline, cwd=gh.remote)
    assert (await cut_release(gh, rid)).status == "conflict"
    assert gh.writes == []
    assert_cursor_unchanged(rid, baseline)


async def test_non_ancestor_baseline_fails_closed(cut_repo):
    gh, rid, baseline, _ = cut_repo
    # A side-branch commit with the same root but not reachable from main.
    tree = git("rev-parse", f"{baseline}^{{tree}}", cwd=gh.remote)
    side = git("commit-tree", tree, "-p", baseline, "-m", "side branch", cwd=gh.remote)
    with db.session_scope() as s:
        s.get(Release, rid).baseline_sha = side
    result = await cut_release(gh, rid)
    assert result.status == "conflict"
    assert record(rid).candidate_sha is None
    assert gh.writes == []
    assert_cursor_unchanged(rid, baseline)


def add_pr(gh, number, sha, title="External contribution", base="main"):
    gh.pulls[number] = {"number": number, "title": title, "merged_at": "2026-11-13T10:00:00Z",
                        "merge_commit_sha": sha, "base": {"ref": base, "repo": {"full_name": gh.full_name}},
                        "head": {"ref": f"feature-{number}"}}


async def test_notes_scoped_deduplicated_and_history_based(cut_repo):
    from aifactory.models import Task
    gh, rid, baseline, head = cut_repo
    add_pr(gh, 10, baseline, "Previously released")
    add_pr(gh, 11, head, "Factory PR [untrusted]")
    external = commit_file(gh.remote, content="external", message="External merge")
    add_pr(gh, 12, external)
    direct = commit_file(gh.remote, content="direct", message="Direct commit")
    with db.session_scope() as s:
        r = s.get(Release, rid)
        outcome = Outcome(workspace_id=r.workspace_id, number=32, title="Merged outcome", completed_at=None)
        old = Outcome(workspace_id=r.workspace_id, number=31, title="Previously released outcome", completed_at=db.utcnow())
        s.add_all([outcome, old])
        s.flush()
        plan = Plan(outcome_id=outcome.id)
        old_plan = Plan(outcome_id=old.id)
        s.add_all([plan, old_plan])
        s.flush()
        for index in range(2):
            s.add(Task(workspace_id=r.workspace_id, outcome_id=outcome.id, plan_id=plan.id,
                       repo_id=r.repo_id, key=f"T{index}", title="same outcome", merge_sha=head, pr_number=11))
        s.add(Task(workspace_id=r.workspace_id, outcome_id=old.id, plan_id=old_plan.id,
                   repo_id=r.repo_id, key="old", title="old", merge_sha=baseline, pr_number=10))
        foreign_repo = Repository(workspace_id=r.workspace_id, full_name="other/repo")
        s.add(foreign_repo)
        s.flush()
        s.add(Task(workspace_id=r.workspace_id, outcome_id=old.id, plan_id=old_plan.id,
                   repo_id=foreign_repo.id, key="wrong-repo", title="foreign", merge_sha=head, pr_number=11))
    head1, items = await discover_candidate(gh, rid)
    assert head1 == direct
    notes = render_notes("1.2.4", items)
    assert notes.count("Outcome #32") == 1
    assert "Previously released" not in notes
    assert "https://github.com/acme/releases/pull/11" in notes
    assert "https://github.com/acme/releases/pull/12" in notes
    assert "https://github.com/acme/releases/commit/" + head in notes
    assert "External contribution" in notes
    assert "Direct commit" in notes
    assert "\\[untrusted\\]" in notes
    gh.reverse = True
    _, reversed_items = await discover_candidate(gh, rid)
    assert render_notes("1.2.4", reversed_items) == notes


async def test_compare_and_pr_pagination(cut_repo):
    gh, rid, baseline, _ = cut_repo
    # Use commit-tree for a large real history without repeated clone overhead.
    parent = await gh.branch_sha(gh.full_name, "main")
    tree = git("rev-parse", f"{parent}^{{tree}}", cwd=gh.remote)
    for index in range(105):
        parent = git("commit-tree", tree, "-p", parent, "-m", f"change {index}", cwd=gh.remote)
    git("update-ref", "refs/heads/main", parent, cwd=gh.remote)
    for number in range(1, 106):
        add_pr(gh, number, parent, f"PR {number}")
    result = await cut_release(gh, rid)
    assert result.deployable
    assert len([i for i in record(rid).included_items if i["kind"] == "pr"]) == 105
    assert any(path.startswith("compare/") and p.get("page") == 2 for path, p in gh.reads)
    assert any(path.endswith("/pulls") and p.get("page") == 2 for path, p in gh.reads)
    assert_cursor_unchanged(rid, baseline)


@pytest.mark.parametrize("path", ["VERSION", "nested/VERSION"])
async def test_plain_and_nested_version_file(cut_repo, path):
    gh, rid, _, head = cut_repo
    version_mode(rid, path=path, format="plain", key=None)
    result = await cut_release(gh, rid)
    assert result.status == "waiting"
    sha = await gh.branch_sha(gh.full_name, result.artifact["branch"])
    assert git("diff", "--name-only", head, sha, cwd=gh.remote) == path
    assert git("show", f"{sha}:{path}", cwd=gh.remote) == "1.2.4"


@pytest.mark.parametrize("content,cfg", [
    (b'{"version":"1.2.3","version":"1.2.4"}', VersionFile(path="v", format="json", key="version")),
    (b'{"version":7}', VersionFile(path="v", format="json", key="version")),
    (b"not-semver", VersionFile(path="v")),
])
def test_unsafe_version_content_rejected(content, cfg):
    with pytest.raises((CutConflict, ValueError)):
        edit_version(content, cfg, "1.2.4")


def test_json_edit_preserves_all_other_bytes():
    source = b'{ "nested": {"version": "old"}, "vers\\u0069on" : "1.2.3", "n": 1e2 }\r\n'
    old, updated = edit_version(source, VersionFile(path="p", format="json", key="version"), "1.2.4")
    assert old == "1.2.3"
    assert updated == source.replace(b"1.2.3", b"1.2.4")


async def test_no_changes_creates_no_artifact(cut_repo):
    gh, rid, _, head = cut_repo
    with db.session_scope() as s:
        s.get(Release, rid).baseline_sha = head
    result = await cut_release(gh, rid)
    assert result.status == "nothing"
    assert not result.deployable
    assert not gh.writes


async def test_version_conflict_before_write_and_while_waiting(cut_repo):
    gh, rid, baseline, _ = cut_repo
    version_mode(rid)
    from aifactory.release_cut import _prepare
    await _prepare(gh, rid)
    commit_file(gh.remote, "package.json", '{"version": "2.0.0"}\n', "Other version bump")
    result = await cut_release(gh, rid)
    assert result.status == "conflict"
    assert not gh.writes
    assert_cursor_unchanged(rid, baseline)


async def test_version_conflict_in_open_pr_does_not_deploy(cut_repo):
    gh, rid, baseline, _ = cut_repo
    version_mode(rid)
    assert (await cut_release(gh, rid)).status == "waiting"
    writes = list(gh.writes)
    commit_file(gh.remote, "package.json", '{"version": "2.0.0"}\n', "Other version bump")
    assert (await cut_release(gh, rid)).status == "conflict"
    assert gh.writes == writes
    assert_cursor_unchanged(rid, baseline)


async def test_merged_candidate_excludes_later_main_and_survives_branch_deletion(cut_repo):
    gh, rid, baseline, _ = cut_repo
    version_mode(rid)
    waiting = await cut_release(gh, rid)
    merged = await gh.merge_release(waiting.artifact["pr_number"])
    git("update-ref", "-d", "refs/heads/" + waiting.artifact["branch"], cwd=gh.remote)
    commit_file(gh.remote, content="after merged", message="NOT IN FINAL CANDIDATE")
    result = await cut_release(gh, rid)
    assert result.deployable and result.candidate_sha == merged
    assert "NOT IN FINAL CANDIDATE" not in record(rid).notes
    assert (await cut_release(gh, rid)) == result
    assert_cursor_unchanged(rid, baseline)


async def test_completed_tag_moved_is_not_deployable(cut_repo):
    gh, rid, baseline, _ = cut_repo
    result = await cut_release(gh, rid)
    assert result.deployable
    git("update-ref", "refs/tags/" + result.artifact["tag"], baseline, cwd=gh.remote)
    assert (await cut_release(gh, rid)).status == "conflict"
    assert gh.writes == ["git/refs"]


async def test_incomplete_comparison_is_not_released(cut_repo, monkeypatch):
    gh, rid, baseline, _ = cut_repo
    request = gh.request

    async def truncated(method, path, **kwargs):
        data = await request(method, path, **kwargs)
        if "/compare/" in path:
            data["total_commits"] += 1
        return data
    monkeypatch.setattr(gh, "request", truncated)
    assert (await cut_release(gh, rid)).status == "conflict"
    assert not gh.writes
    assert record(rid).candidate_sha is None
    assert_cursor_unchanged(rid, baseline)


async def test_concurrent_cut_elects_one_writer(cut_repo, monkeypatch):
    import asyncio
    gh, rid, _, _ = cut_repo
    request = gh.request
    claimed, resume = asyncio.Event(), asyncio.Event()

    async def pause(method, path, **kwargs):
        if method == "POST":
            claimed.set()
            await resume.wait()
        return await request(method, path, **kwargs)
    monkeypatch.setattr(gh, "request", pause)
    first = asyncio.create_task(cut_release(gh, rid))
    await claimed.wait()
    try:
        second = await cut_release(gh, rid)
        assert second.status == "unknown"
    finally:
        resume.set()
    assert (await first).deployable
    assert len(gh.writes) == 1
    assert (await cut_release(gh, rid)).deployable


async def test_snapshot_uses_existing_secret_redaction_boundary(cut_repo):
    from aifactory.models import Secret
    from aifactory.security import encrypt
    gh, rid, _, head = cut_repo
    with db.session_scope() as s:
        s.add(Secret(workspace_id=record(rid).workspace_id, kind="example", ciphertext=encrypt("very-private-token")))
    add_pr(gh, 1, head, "Fix very-private-token")
    assert (await cut_release(gh, rid)).deployable
    r = record(rid)
    assert "very-private-token" not in r.notes
    assert "very-private-token" not in json.dumps(r.included_items)


async def test_first_cut_without_baseline_includes_reachable_history(cut_repo):
    gh, rid, baseline, head = cut_repo
    with db.session_scope() as s:
        s.get(Release, rid).baseline_sha = None
    result = await cut_release(gh, rid)
    assert result.deployable
    shas = {i["sha"] for i in record(rid).included_items if i["kind"] == "commit"}
    assert shas == {baseline, head}


async def test_scheduler_captured_head_is_respected(cut_repo):
    gh, rid, _, head = cut_repo
    with db.session_scope() as s:
        s.get(Release, rid).candidate_sha = head
    commit_file(gh.remote, content="late", message="AFTER SCHEDULER CAPTURE")
    result = await cut_release(gh, rid)
    assert result.deployable and result.candidate_sha == head
    assert "AFTER SCHEDULER CAPTURE" not in record(rid).notes


async def test_version_file_symlink_is_refused(cut_repo):
    gh, rid, baseline, _ = cut_repo
    version_mode(rid)
    work = gh.remote.parent / "editor"
    (work / "package.json").unlink()
    (work / "package.json").symlink_to("VERSION")
    git("add", "-A", cwd=work)
    git("commit", "-qm", "version symlink", cwd=work)
    git("push", "-q", "origin", "main", cwd=work)
    assert (await cut_release(gh, rid)).status == "conflict"
    assert not gh.writes
    assert_cursor_unchanged(rid, baseline)


async def test_merge_commit_candidate_omits_both_bump_commits(cut_repo):
    gh, rid, baseline, _ = cut_repo
    version_mode(rid)
    waiting = await cut_release(gh, rid)
    pr = gh.pulls[waiting.artifact["pr_number"]]
    commit_file(gh.remote, content="before merge", message="Included before protected merge")
    work = gh.remote.parent / "editor"
    git("fetch", "-q", "origin", cwd=work)
    git("merge", "--no-ff", "-m", "Release bump merge", f"origin/{pr['head']['ref']}", cwd=work)
    git("push", "-q", "origin", "main", cwd=work)
    merged = git("rev-parse", "HEAD", cwd=work)
    pr.update(merged=True, state="closed", merge_commit_sha=merged, merged_at="2026-11-13T10:00:00Z")
    result = await cut_release(gh, rid)
    assert result.deployable and result.candidate_sha == merged
    assert "Included before protected merge" in record(rid).notes
    assert "Release bump merge" not in record(rid).notes
    assert "chore: release" not in record(rid).notes
    assert_cursor_unchanged(rid, baseline)


async def test_file_content_is_not_stored_in_encoded_intents(cut_repo):
    from aifactory.models import Secret
    from aifactory.security import encrypt
    gh, rid, _, _ = cut_repo
    private = "never-snapshot-this-token"
    commit_file(gh.remote, "package.json", json.dumps({"version": "1.2.3", "token": private}), "version fixture")
    with db.session_scope() as s:
        s.add(Secret(workspace_id=record(rid).workspace_id, kind="example", ciphertext=encrypt(private)))
    version_mode(rid)
    assert (await cut_release(gh, rid)).status == "waiting"
    with db.session_scope() as s:
        handles = [step.external_handle for step in s.scalars(select(ReleaseStep).where(ReleaseStep.release_id == rid))]
    assert all(private not in json.dumps(handle) for handle in handles)
    assert all("content" not in handle.get("payload", {}) for handle in handles)
    assert len(gh.pulls) == 1


async def test_foreign_workspace_task_is_not_associated(cut_repo, signed_in):
    from aifactory.models import Task
    gh, rid, _, head = cut_repo
    r = record(rid)
    with db.session_scope() as s:
        home = s.get(Workspace, r.workspace_id)
        other = Workspace(org_id=home.org_id, name="Other team", slug="release-other")
        s.add(other)
        s.flush()
        foreign = Outcome(workspace_id=other.id, number=999, title="Must not leak")
        s.add(foreign)
        s.flush()
        plan = Plan(outcome_id=foreign.id)
        s.add(plan)
        s.flush()
        s.add(Task(workspace_id=other.id, repo_id=r.repo_id, outcome_id=foreign.id, plan_id=plan.id,
                   key="wrong-team", title="cross-team", pr_number=7, merge_sha=head))
        # Even a corrupt local Task pointing to a foreign Outcome is excluded.
        s.add(Task(workspace_id=r.workspace_id, repo_id=r.repo_id, outcome_id=foreign.id, plan_id=plan.id,
                   key="wrong-outcome", title="cross-outcome", pr_number=7, merge_sha=head))
    add_pr(gh, 7, head)
    assert (await cut_release(gh, rid)).deployable
    assert "Must not leak" not in record(rid).notes
    assert not any(i["kind"] == "outcome" for i in record(rid).included_items)


async def test_matching_tag_is_reconciled_without_write(cut_repo):
    from aifactory.release_cut import _prepare
    gh, rid, _, head = cut_repo
    await _prepare(gh, rid)
    git("update-ref", "refs/tags/v1.2.4", head, cwd=gh.remote)
    result = await cut_release(gh, rid)
    assert result.deployable and result.candidate_sha == head
    assert not gh.writes
