"""Immutable release cuts, independent of scheduling and deployment.

Call ``cut_release(github, release_id)`` repeatedly. ``ready`` alone means the
candidate may be deployed. ``waiting`` needs another read; ``unknown`` needs
reconciliation (never an automatic resend); ``conflict`` needs a human decision.
The caller owns lifecycle transitions/decisions and the release cursor. This
module never approves, merges, force-pushes, or advances that cursor.

Every external write has its own durable ReleaseStep intent. A conditional
pending->dispatching transition elects one writer, without a database lock held
across GitHub I/O. A crashed writer can only be reconciled, never blindly retried.
"""
from __future__ import annotations

import base64
import hashlib
import json
import re
from dataclasses import dataclass
from datetime import timezone
from typing import Any, Literal
from urllib.parse import quote

from sqlalchemy import or_, select, update
from sqlalchemy.exc import IntegrityError

from . import db
from .github import GitHub, GitHubError
from .models import Outcome, Release, ReleaseStep, ReleaseTrainCursor, Repository, Task
from .release_config import ReleaseCut, VersionFile

PAGE_SIZE = 100
MAX_ITEMS = 10000
MAX_FILE_BYTES = 1024 * 1024
SHA = re.compile(r"[0-9a-f]{40}\Z")


class CutConflict(Exception):
    """Evidence is incompatible with the persisted release intent."""


class CutUnknown(Exception):
    """An external write may have happened; only reconciliation is safe."""


@dataclass(frozen=True)
class CutResult:
    status: Literal["ready", "waiting", "nothing", "unknown", "conflict"]
    release_id: str
    candidate_sha: str | None = None
    artifact: dict[str, Any] | None = None
    detail: str = ""

    @property
    def deployable(self) -> bool:
        return self.status == "ready" and bool(self.candidate_sha)


def _load(release_id: str):
    with db.session_scope() as s:
        r = s.get(Release, release_id)
        if r is None:
            raise CutConflict("Release not found")
        repo = s.get(Repository, r.repo_id)
        if repo is None or repo.workspace_id != r.workspace_id:
            raise CutConflict("Repository scope mismatch")
        cut_config = r.config_snapshot.get("cut")
        if cut_config is None:
            raise CutConflict("Release has no snapshotted cut configuration")
        cfg = ReleaseCut.model_validate(cut_config)
        return r, repo, cfg


async def _get(gh, path, **kwargs):
    return await gh.request("GET", path, **kwargs)


async def _optional(gh, path):
    try:
        return await _get(gh, path)
    except GitHubError as exc:
        if exc.status == 404:
            return None
        raise


async def _pages(gh, path, *, params=None, key=None):
    result = []
    for page in range(1, MAX_ITEMS // PAGE_SIZE + 2):
        data = await _get(gh, path, params={**(params or {}), "per_page": PAGE_SIZE, "page": page})
        batch = data[key] if key else data
        if not isinstance(batch, list) or len(batch) > PAGE_SIZE or len(result) + len(batch) > MAX_ITEMS:
            raise CutConflict("GitHub history exceeds the bounded evidence limit")
        result.extend(batch)
        if len(batch) < PAGE_SIZE:
            return result
    raise CutConflict("GitHub pagination did not terminate")


def _sha(value):
    if not isinstance(value, str) or not SHA.fullmatch(value):
        raise CutConflict("Invalid Git object identity")
    return value


async def _history(gh, root, baseline, head):
    _sha(head)
    if not baseline:
        commits = await _pages(gh, f"{root}/commits", params={"sha": head})
        if not commits or len({_sha(c["sha"]) for c in commits}) != len(commits):
            raise CutConflict("Incomplete initial repository history")
        return commits
    _sha(baseline)
    path = f"{root}/compare/{baseline}...{head}"
    first = await _get(gh, path, params={"per_page": PAGE_SIZE, "page": 1})
    if (first.get("status") not in {"ahead", "identical"}
            or first.get("merge_base_commit", {}).get("sha") != baseline):
        raise CutConflict("Release baseline is not an ancestor of the candidate")
    count = first.get("total_commits")
    if not isinstance(count, int) or count > MAX_ITEMS or count < 0:
        raise CutConflict("Incomplete or excessive comparison history")
    commits = list(first.get("commits", []))
    if len(commits) > PAGE_SIZE:
        raise CutConflict("Unbounded comparison page")
    for page in range(2, (count + PAGE_SIZE - 1) // PAGE_SIZE + 1):
        data = await _get(gh, path, params={"per_page": PAGE_SIZE, "page": page})
        batch = data.get("commits", [])
        if len(batch) > PAGE_SIZE:
            raise CutConflict("Unbounded comparison page")
        commits.extend(batch)
    if len(commits) != count or len({_sha(c["sha"]) for c in commits}) != count:
        raise CutConflict("Incomplete comparison history")
    return commits


def _text(value):
    # Titles are untrusted Markdown, not markup or instructions.
    value = " ".join(str(value).split())[:1000]
    return re.sub(r"([\\`*_{}\[\]<>#!|])", r"\\\1", value)


def render_notes(version: str, items: list[dict[str, Any]]) -> str:
    lines = [f"## Release {version}", ""]
    for kind, heading in (("outcome", "Outcomes"), ("pr", "Pull requests"), ("commit", "Other commits")):
        group = sorted((i for i in items if i["kind"] == kind), key=lambda i: (i.get("number", 0), i.get("sha", "")))
        if not group:
            continue
        lines.extend([f"### {heading}", ""])
        for item in group:
            if kind == "outcome":
                lines.append(f"- Outcome #{item['number']}: {_text(item['title'])}")
            elif kind == "pr":
                links = ", ".join(f"[{sha[:7]}]({item['commit_urls'][sha]})" for sha in sorted(item['commits']))
                lines.append(f"- [#{item['number']}]({item['url']}): {_text(item['title'])} ({links})")
            else:
                lines.append(f"- [{item['sha'][:7]}]({item['url']}): {_text(item['title'])}")
        lines.append("")
    return "\n".join(lines).rstrip() + "\n"


async def discover_candidate(gh: GitHub, release_id: str, *, head: str | None = None,
                             exclude_pr: int | None = None, exclude_shas: tuple[str, ...] = ()):
    """Snapshot actual reachable changes, including non-Factory PRs. No writes to GitHub."""
    r, repo, _ = _load(release_id)
    root = f"/repos/{repo.full_name}"
    head = head or r.candidate_sha or await gh.branch_sha(repo.full_name, repo.default_branch)
    commits = await _history(gh, root, r.baseline_sha, head)
    commits = {c["sha"]: c for c in commits if c["sha"] not in exclude_shas}
    prs: dict[int, dict] = {}
    by_commit: dict[str, set[int]] = {sha: set() for sha in commits}
    associations = 0
    for sha in sorted(commits):
        for pr in await _pages(gh, f"{root}/commits/{sha}/pulls"):
            number = pr["number"]
            if (number == exclude_pr or not pr.get("merged_at") or pr.get("merge_commit_sha") not in commits
                    or pr.get("base", {}).get("ref") != repo.default_branch
                    or pr.get("base", {}).get("repo", {}).get("full_name") != repo.full_name):
                continue
            if type(number) is not int or number < 1:
                raise CutConflict("Invalid pull request identity")
            prs[number] = pr
            if number not in by_commit[sha]:
                associations += 1
                by_commit[sha].add(number)
            if associations > MAX_ITEMS:
                raise CutConflict("Too many commit/PR associations")
            if len(prs) > MAX_ITEMS:
                raise CutConflict("Too many included pull requests")
    with db.session_scope() as s:
        # Both sides of the join are scoped; matching IDs alone are not authority.
        tasks = s.execute(select(Task, Outcome).join(Outcome, Task.outcome_id == Outcome.id).where(
            Task.workspace_id == r.workspace_id, Task.repo_id == r.repo_id,
            Outcome.workspace_id == r.workspace_id,
            or_(Task.merge_sha.in_(commits), Task.pr_number.in_(prs))).limit(MAX_ITEMS + 1)).all()
        if len(tasks) > MAX_ITEMS:
            raise CutConflict("Too many task associations")
        outcomes = {}
        pr_outcomes: dict[int, set[int]] = {}
        for task, outcome in tasks:
            outcomes[outcome.number] = {"kind": "outcome", "number": outcome.number, "title": outcome.title}
            matched_prs = by_commit.get(task.merge_sha, set()) | ({task.pr_number} if task.pr_number in prs else set())
            for number in matched_prs:
                pr_outcomes.setdefault(number, set()).add(outcome.number)
    items = list(outcomes.values())
    for number, pr in sorted(prs.items()):
        shas = sorted(sha for sha, numbers in by_commit.items() if number in numbers)
        items.append({"kind": "pr", "number": number, "title": pr["title"],
                      "url": f"https://github.com/{repo.full_name}/pull/{number}",
                      "outcomes": sorted(pr_outcomes.get(number, set())), "commits": shas,
                      "commit_urls": {sha: f"https://github.com/{repo.full_name}/commit/{sha}" for sha in shas}})
    for sha, commit in sorted(commits.items()):
        if not by_commit[sha]:
            items.append({"kind": "commit", "sha": sha, "title": commit["commit"]["message"].splitlines()[0],
                          "url": f"https://github.com/{repo.full_name}/commit/{sha}"})
    if len(items) > MAX_ITEMS:
        raise CutConflict("Release evidence exceeds the persisted item limit")
    items.sort(key=lambda i: (i["kind"], i.get("number", 0), i.get("sha", "")))
    return head, items


def _step(release_id, name, payload):
    r, _, _ = _load(release_id)
    try:
        with db.session_scope() as s:
            step = s.scalar(select(ReleaseStep).where(ReleaseStep.release_id == r.id, ReleaseStep.step == name,
                                                      ReleaseStep.attempt == 1))
            if step is None:
                step = ReleaseStep(release_id=r.id, workspace_id=r.workspace_id, repo_id=r.repo_id,
                                   step=name, operation_key=f"release:{r.id}:{name}:1", external_handle=payload)
                s.add(step)
                s.flush()
            elif step.external_handle != payload:
                raise CutConflict("Persisted external intent differs from the requested operation")
            return step.id
    except IntegrityError:
        # Only a proven unique-key race is recoverable, not arbitrary DB failures.
        with db.session_scope() as s:
            step = s.scalar(select(ReleaseStep).where(ReleaseStep.release_id == r.id, ReleaseStep.step == name,
                                                      ReleaseStep.attempt == 1))
            if step is None:
                raise
            if step.external_handle != payload:
                raise CutConflict("Concurrent external intent differs from this operation") from None
            return step.id


async def _effect(gh, release_id, name, path, payload, reconcile, *, intent=None):
    step_id = _step(release_id, name, intent or {"path": path, "payload": payload})
    found = await reconcile()
    if found is not None:
        with db.session_scope() as s:
            s.get(ReleaseStep, step_id).state = "succeeded"
        return found
    with db.session_scope() as s:
        claimed = s.execute(update(ReleaseStep).where(ReleaseStep.id == step_id, ReleaseStep.state == "pending")
                            .values(state="dispatching")).rowcount == 1
    if not claimed:
        raise CutUnknown(f"{name}: external intent already dispatched; artifact not yet visible")
    try:
        await gh.request("POST", path, json=payload, retries=0)
    except GitHubError:
        # Do not persist exception/provider text (it may contain credentials).
        found = await reconcile()
        if found is None:
            raise CutUnknown(f"{name}: response lost or write rejected; reconciliation required") from None
    else:
        found = await reconcile()
        if found is None:
            raise CutUnknown(f"{name}: write not yet visible")
    with db.session_scope() as s:
        s.get(ReleaseStep, step_id).state = "succeeded"
    return found


async def _ref(gh, root, ref):
    data = await _optional(gh, f"{root}/git/ref/{quote(ref, safe='/')}")
    return data["object"]["sha"] if data else None


async def _create_ref(gh, rid, root, ref, sha):
    async def reconcile():
        existing = await _ref(gh, root, ref)
        if existing is not None and existing != sha:
            raise CutConflict("Existing release ref points at a different commit")
        return existing
    return await _effect(gh, rid, "cut_ref", f"{root}/git/refs", {"ref": f"refs/{ref}", "sha": sha}, reconcile)


def _object_sha(kind, data):
    return hashlib.sha1(f"{kind} {len(data)}\0".encode() + data).hexdigest()


def edit_version(content: bytes, cfg: VersionFile, version: str, expected: str | None = None):
    """Change only the literal version token; preserve every other byte."""
    if len(content) > MAX_FILE_BYTES:
        raise CutConflict("Version file exceeds size limit")
    text = content.decode("utf-8")
    if cfg.format == "plain":
        match = re.fullmatch(r"(\s*)((?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*))(\s*)", text)
        if not match:
            raise CutConflict("Version file is not a plain semantic version")
        old, start, end = match[2], match.start(2), match.end(2)
    else:
        def unique(pairs):
            result = {}
            for key, value in pairs:
                if key in result:
                    raise CutConflict("Duplicate JSON keys in version file")
                result[key] = value
            return result
        data = json.loads(text, object_pairs_hook=unique)
        if not isinstance(data, dict) or not isinstance(data.get(cfg.key), str):
            raise CutConflict("Configured version key is not a string")
        old = data[cfg.key]
        decoder = json.JSONDecoder()
        pos = text.index("{") + 1
        while True:
            pos += len(text[pos:]) - len(text[pos:].lstrip())
            key, pos = decoder.raw_decode(text, pos)
            pos += len(text[pos:]) - len(text[pos:].lstrip())
            if text[pos] != ":":
                raise CutConflict("Invalid JSON version field")
            pos += 1
            pos += len(text[pos:]) - len(text[pos:].lstrip())
            start = pos
            _, end = decoder.raw_decode(text, pos)
            if key == cfg.key:
                break
            pos = end + len(text[end:]) - len(text[end:].lstrip())
            if text[pos] != ",":
                raise CutConflict("Version key not found")
            pos += 1
    ReleaseCut.semantic_version(old)
    if expected is not None and old != expected:
        raise CutConflict("Version field differs from the persisted release version")
    token = json.dumps(version) if cfg.format == "json" else version
    return old, (text[:start] + token + text[end:]).encode("utf-8")


async def _version_blob(gh, root, tree_sha, cfg):
    """Read path components, refusing symlinks/submodules and truncated trees."""
    levels = []
    parts = cfg.path.split("/")
    for index, part in enumerate(parts):
        data = await _get(gh, f"{root}/git/trees/{_sha(tree_sha)}")
        entries = data.get("tree", [])
        if data.get("truncated") or len(entries) > MAX_ITEMS:
            raise CutConflict("Incomplete version tree")
        entry = next((e for e in entries if e["path"] == part), None)
        if not entry or entry["mode"] not in ({"100644", "100755"} if index == len(parts) - 1 else {"040000"}):
            raise CutConflict("Version path must be an existing regular file")
        levels.append((entries, part))
        tree_sha = entry["sha"]
    blob = await _get(gh, f"{root}/git/blobs/{_sha(tree_sha)}")
    if blob.get("encoding") != "base64" or not 0 <= blob.get("size", MAX_FILE_BYTES + 1) <= MAX_FILE_BYTES:
        raise CutConflict("Invalid or excessive version blob")
    encoded = blob.get("content", "")
    if len(encoded) > (MAX_FILE_BYTES * 4 // 3 + 10000):
        raise CutConflict("Excessive encoded version blob")
    content = base64.b64decode("".join(encoded.split()), validate=True)
    if len(content) != blob["size"] or _object_sha("blob", content) != tree_sha:
        raise CutConflict("Version blob identity mismatch")
    return content, levels


def _trees(levels, blob_sha):
    result = []
    child = blob_sha
    for entries, part in reversed(levels):
        tree = [{"path": e["path"], "mode": e["mode"], "type": e["type"],
                 "sha": child if e["path"] == part else e["sha"]} for e in entries]
        tree.sort(key=lambda e: (e["path"] + ("/" if e["type"] == "tree" else "")).encode())
        raw = b"".join(e["mode"].lstrip("0").encode() + b" " + e["path"].encode() + b"\0" + bytes.fromhex(e["sha"]) for e in tree)
        child = _object_sha("tree", raw)
        result.append((child, {"tree": tree}))
    return child, result


async def _git_object(gh, rid, root, name, kind, sha, payload):
    async def reconcile():
        obj = await _optional(gh, f"{root}/git/{kind}/{sha}")
        if obj and obj.get("sha") != sha:
            raise CutConflict("Git object response mismatch")
        return obj
    # Content-addressed objects need only their hash as durable intent. In
    # particular, never snapshot a base64 copy of a repository file: encoded
    # credentials cannot be scrubbed by the ordinary evidence redactor.
    path = f"{root}/git/{kind}"
    intent = {"path": path, "object_sha": sha}
    return await _effect(gh, rid, name, path, payload, reconcile, intent=intent)


async def _prepare(gh, rid):
    r, repo, cfg = _load(rid)
    if r.candidate_sha and r.version:
        return
    head, items = await discover_candidate(gh, rid)
    root = f"/repos/{repo.full_name}"
    with db.session_scope() as s:
        cursor = s.get(ReleaseTrainCursor, r.repo_id)
        if cursor and cursor.workspace_id != r.workspace_id:
            raise CutConflict("Release cursor scope mismatch")
        previous = cursor.last_version if cursor else None
    if cfg.mode == "tag":
        tags = await _pages(gh, f"{root}/tags")
        versions = [t["name"][len(cfg.tag_prefix):] for t in tags if t["name"].startswith(cfg.tag_prefix)
                    and re.fullmatch(r"(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)", t["name"][len(cfg.tag_prefix):])]
        if previous:
            versions.append(previous)
        version = cfg.next_version(max(versions, key=lambda v: tuple(map(int, v.split(".")))) if versions else None)
    else:
        commit = await _get(gh, f"{root}/git/commits/{head}")
        content, _ = await _version_blob(gh, root, commit["tree"]["sha"], cfg.version_file)
        # Read and validate before calculating the bump.
        old, _ = edit_version(content, cfg.version_file, "0.0.0", previous)
        version = cfg.next_version(old)
    with db.session_scope() as s:
        claimed = s.execute(update(Release).where(Release.id == rid, Release.version.is_(None),
                                                or_(Release.candidate_sha.is_(None), Release.candidate_sha == head))
                            .values(candidate_sha=head)).rowcount == 1
        if claimed:
            # Use ORM writes for evidence so the model's secret-egress hooks run.
            record = s.get(Release, rid)
            record.version, record.included_items, record.notes = version, items, render_notes(version, items)


async def _version_pr(gh, r, repo, cfg):
    root = f"/repos/{repo.full_name}"
    # cut_plan is immutable even when the final candidate changes after merging.
    with db.session_scope() as s:
        plan = s.scalar(select(ReleaseStep).where(ReleaseStep.release_id == r.id, ReleaseStep.step == "cut_plan"))
    if plan:
        head = plan.external_handle["head"]
        initial_notes = plan.external_handle["notes"]
    else:
        head, initial_notes = r.candidate_sha, r.notes
        _step(r.id, "cut_plan", {"head": head, "notes": initial_notes})
    commit = await _get(gh, f"{root}/git/commits/{head}")
    content, levels = await _version_blob(gh, root, commit["tree"]["sha"], cfg.version_file)
    old, updated = edit_version(content, cfg.version_file, r.version)
    if cfg.next_version(old) != r.version:
        raise CutConflict("Version no longer agrees with the planned patch bump")
    blob_sha = _object_sha("blob", updated)
    tree_sha, trees = _trees(levels, blob_sha)
    date = r.created_at.strftime("%Y-%m-%dT%H:%M:%SZ")
    identity = {"name": "AI Factory", "email": "releases@ai-factory.invalid", "date": date}
    message = f"chore: release {r.version}\n\nFactory-Release: {r.id}\n"
    timestamp = int(r.created_at.replace(tzinfo=timezone.utc).timestamp())
    who = f"AI Factory <releases@ai-factory.invalid> {timestamp} +0000"
    raw = f"tree {tree_sha}\nparent {head}\nauthor {who}\ncommitter {who}\n\n{message}".encode()
    bump_sha = _object_sha("commit", raw)
    branch = f"factory/release/{r.id}"
    existing = await _ref(gh, root, f"heads/{branch}")
    if existing is not None and existing != bump_sha:
        raise CutConflict("Release branch diverged; refusing to update it")
    marker = f"<!-- factory-release:{r.id} -->"
    body = marker + "\n\n" + initial_notes
    if len(body.encode("utf-8")) > 60000:
        raise CutConflict("Release notes exceed the GitHub pull request body limit")

    async def reconcile():
        pulls = await _pages(gh, f"{root}/pulls", params={"state": "all", "head": f"{repo.full_name.split('/')[0]}:{branch}"})
        if len(pulls) > 1:
            raise CutConflict("Multiple release pull requests require a decision")
        if not pulls:
            return None
        pr = await _get(gh, f"{root}/pulls/{pulls[0]['number']}")
        if (marker not in (pr.get("body") or "") or pr["head"]["sha"] != bump_sha
                or pr["base"]["ref"] != repo.default_branch
                or pr["base"].get("repo", {}).get("full_name") != repo.full_name
                or pr["head"].get("repo", {}).get("full_name") != repo.full_name):
            raise CutConflict("Release pull request differs from its intent")
        return pr
    pr = await reconcile()
    if pr is None:
        # Check for version drift before creating even the bump branch/PR.
        current = await gh.branch_sha(repo.full_name, repo.default_branch)
        await _history(gh, root, head, current)
        current_commit = await _get(gh, f"{root}/git/commits/{current}")
        current_content, _ = await _version_blob(gh, root, current_commit["tree"]["sha"], cfg.version_file)
        edit_version(current_content, cfg.version_file, r.version, old)
        await _git_object(gh, r.id, root, "cut_blob", "blobs", blob_sha,
                          {"content": base64.b64encode(updated).decode(), "encoding": "base64"})
        for index, (sha, payload) in enumerate(trees):
            await _git_object(gh, r.id, root, f"cut_tree_{index}", "trees", sha, payload)
        await _git_object(gh, r.id, root, "cut_commit", "commits", bump_sha,
                          {"tree": tree_sha, "parents": [head], "message": message, "author": identity, "committer": identity})
        await _create_ref(gh, r.id, root, f"heads/{branch}", bump_sha)
    pr = await _effect(gh, r.id, "cut_pr", f"{root}/pulls",
                       {"head": branch, "base": repo.default_branch, "title": f"Release {r.version}", "body": body}, reconcile)
    artifact = {"pr_number": pr["number"], "url": f"https://github.com/{repo.full_name}/pull/{pr['number']}", "branch": branch}
    if not pr.get("merged"):
        if pr.get("state") == "closed":
            raise CutConflict("Release pull request was closed without merging")
        if await _ref(gh, root, f"heads/{branch}") != bump_sha:
            raise CutConflict("Open release pull request branch is missing")
        current = await gh.branch_sha(repo.full_name, repo.default_branch)
        await _history(gh, root, head, current)
        current_commit = await _get(gh, f"{root}/git/commits/{current}")
        current_content, _ = await _version_blob(gh, root, current_commit["tree"]["sha"], cfg.version_file)
        edit_version(current_content, cfg.version_file, r.version, old)
        return CutResult("waiting", r.id, artifact=artifact, detail="Awaiting GitHub merge and branch protection")
    merged = _sha(pr.get("merge_commit_sha"))
    merged_commit = await _get(gh, f"{root}/git/commits/{merged}")
    parent = merged_commit["parents"][0]["sha"]
    parent_commit = await _get(gh, f"{root}/git/commits/{parent}")
    parent_content, parent_levels = await _version_blob(gh, root, parent_commit["tree"]["sha"], cfg.version_file)
    _, final_content = edit_version(parent_content, cfg.version_file, r.version, old)
    expected_tree, _ = _trees(parent_levels, _object_sha("blob", final_content))
    if expected_tree != merged_commit["tree"]["sha"]:
        raise CutConflict("Merged release is not a bump-only change from its first parent")
    # Verify reachability on today's default branch, but freeze the actual merge SHA.
    current = await gh.branch_sha(repo.full_name, repo.default_branch)
    await _history(gh, root, merged, current)
    await _history(gh, root, head, merged)
    _, items = await discover_candidate(gh, r.id, head=merged, exclude_pr=pr["number"], exclude_shas=(bump_sha, merged))
    with db.session_scope() as s:
        record = s.get(Release, r.id)
        record.candidate_sha, record.included_items, record.notes = merged, items, render_notes(r.version, items)
    return CutResult("ready", r.id, merged, artifact)


async def cut_release(gh: GitHub, release_id: str) -> CutResult:
    """Prepare/resume one immutable cut. No deployment or release-cursor mutation."""
    try:
        r, repo, cfg = _load(release_id)
        with db.session_scope() as s:
            completed = s.scalar(select(ReleaseStep).where(ReleaseStep.release_id == release_id,
                                                          ReleaseStep.step == "cut_complete"))
        if completed and completed.state == "succeeded":
            sha, artifact = completed.external_handle["sha"], completed.external_handle["artifact"]
            root = f"/repos/{repo.full_name}"
            if cfg.mode == "tag":
                if await _ref(gh, root, f"tags/{artifact['tag']}") != sha:
                    raise CutConflict("Completed release tag is missing or has moved")
            else:
                pr = await _get(gh, f"{root}/pulls/{artifact['pr_number']}")
                if not pr.get("merged") or pr.get("merge_commit_sha") != sha:
                    raise CutConflict("Completed release merge evidence changed")
            return CutResult("ready", release_id, sha, artifact)
        if r.candidate_sha is None or r.version is None:
            await _prepare(gh, release_id)
        r, repo, cfg = _load(release_id)
        if not r.included_items:
            return CutResult("nothing", r.id, detail="No unreleased changes")
        if cfg.mode == "version_pr":
            result = await _version_pr(gh, r, repo, cfg)
        else:
            tag = cfg.tag_prefix + r.version
            await _create_ref(gh, r.id, f"/repos/{repo.full_name}", f"tags/{tag}", r.candidate_sha)
            result = CutResult("ready", r.id, r.candidate_sha,
                               {"tag": tag, "url": f"https://github.com/{repo.full_name}/releases/tag/{tag}"})
        if result.deployable:
            step_id = _step(r.id, "cut_complete", {"sha": result.candidate_sha, "artifact": result.artifact})
            with db.session_scope() as s:
                s.get(ReleaseStep, step_id).state = "succeeded"
        return result
    except CutConflict as exc:
        return CutResult("conflict", release_id, detail=str(exc))
    except CutUnknown as exc:
        return CutResult("unknown", release_id, detail=str(exc))
    except GitHubError:
        return CutResult("unknown", release_id, detail="GitHub evidence unavailable; reconcile before proceeding")
    except (ValueError, UnicodeError, RecursionError):
        return CutResult("conflict", release_id, detail="Invalid release version configuration or file content")
