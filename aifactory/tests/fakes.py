"""Test doubles: a scripted model and a GitHub backed by a local bare repository."""

from __future__ import annotations

import json
import subprocess
import tempfile
from copy import deepcopy
from pathlib import Path
from typing import Any, Callable

from aifactory.db import utcnow
from aifactory.github import CheckSummary, GitHubError
from aifactory.inference import ProviderConfig
from aifactory.review import FACTORY_REPLY_MARKER


def call(name: str, args: dict[str, Any], n: int = 0) -> dict[str, Any]:
    return {"id": f"call_{name}_{n}", "type": "function", "function": {"name": name, "arguments": json.dumps(args)}}


class ScriptedModel:
    """Replaces ModelClient. ``scripts[role]`` returns the next assistant message for a conversation."""

    scripts: dict[str, Callable[[list[dict[str, Any]]], dict[str, Any]]] = {}
    calls: list[str] = []
    judge_calls: list[dict[str, Any]] = []

    def __init__(self, provider: ProviderConfig, **_kwargs):
        self.provider = provider

    async def chat(self, *, model, messages, tools=None, ledger=None, **_kwargs):
        system = messages[0]["content"]
        role = "director" if "You are the Director" in system else "lead" if "You are the Lead" in system else "judge" if "You are a decision judge" in system else "scorer" if "You are a scorer" in system else "plan_reviewer" if "You are the plan reviewer" in system else "planner" if "planning engineer" in system else "verifier" if "verification engineer" in system else "builder"
        if role == "judge":
            ScriptedModel.judge_calls.append({"model": model, "messages": messages, "tools": tools, **_kwargs})
        else:
            ScriptedModel.calls.append(role)
        if ledger:
            ledger.check()
            ledger.add(model, (1000, 200))
        if role == "plan_reviewer" and role not in self.scripts:
            return approving_plan_review(messages)
        if role == "lead" and role not in self.scripts:
            return quiet_lead(messages)
        if role == "director" and role not in self.scripts:
            return {"content": "", "tool_calls": [call("finish", {"summary": "Nothing to decide."})]}
        return self.scripts[role](messages)

    async def session_cost(self, _session_id):
        return None

    async def list_models(self):
        return ["gpt-terra"]


def approving_plan_review(_messages: list[dict[str, Any]]) -> dict[str, Any]:
    """The plan reviewer's default in tests: approve without findings."""
    return {"content": "", "tool_calls": [call("finish", {"verdict": "approve", "summary": "The plan covers the request.",
                                                          "findings": []})]}


def quiet_lead(messages: list[dict[str, Any]]) -> dict[str, Any]:
    """The Lead's default in tests: post a short update, then finish."""
    if step(messages) == 0:
        return {"content": "", "tool_calls": [call("post_update", {"text": "Following this outcome."})]}
    return {"content": "", "tool_calls": [call("finish", {"summary": "Posted an update."})]}


def step(messages: list[dict[str, Any]]) -> int:
    return sum(1 for m in messages if m["role"] == "assistant")


def git(*args: str, cwd: Path | str | None = None) -> str:
    env = {"GIT_AUTHOR_NAME": "t", "GIT_AUTHOR_EMAIL": "t@t", "GIT_COMMITTER_NAME": "t", "GIT_COMMITTER_EMAIL": "t@t",
           "PATH": "/usr/bin:/bin:/usr/local/bin:/opt/homebrew/bin", "HOME": tempfile.gettempdir()}
    return subprocess.run(["git", *args], cwd=cwd, check=True, capture_output=True, text=True, env=env).stdout.strip()


def make_remote(base: Path, full_name: str, files: dict[str, str]) -> Path:
    bare = base / f"{full_name}.git"
    bare.parent.mkdir(parents=True, exist_ok=True)
    git("init", "-q", "--bare", "-b", "main", str(bare))
    work = base / "seed" / full_name
    work.mkdir(parents=True)
    git("init", "-q", "-b", "main", cwd=work)
    for path, content in files.items():
        target = work / path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(content)
    git("add", "-A", cwd=work)
    git("commit", "-qm", "init", cwd=work)
    git("push", "-q", str(bare), "main", cwd=work)
    return bare


class FakeGitHub:
    """Pull requests and merges against a local bare repository."""

    remotes: Path
    pulls: dict[int, dict[str, Any]] = {}
    check_state = "success"
    merged: list[int] = []

    def __init__(self, token: str):
        self.token = token

    def _bare(self, full_name: str) -> Path:
        return self.remotes / f"{full_name}.git"

    async def branch_sha(self, full_name: str, branch: str) -> str:
        return git("rev-parse", f"refs/heads/{branch}", cwd=self._bare(full_name))

    async def create_pull(self, full_name, *, head, base, title, body, draft=False):
        for pr in self.pulls.values():
            if pr["head"]["ref"] == head and pr["state"] == "open":
                pr["body"] = body
                pr["head"]["sha"] = await self.branch_sha(full_name, head)
                return pr
        number = len(self.pulls) + 1
        pr = {"number": number, "html_url": f"https://example.test/pr/{number}", "state": "open", "merged": False,
              "title": title, "body": body, "head": {"ref": head, "sha": await self.branch_sha(full_name, head)},
              "base": {"ref": base, "sha": await self.branch_sha(full_name, base)}, "mergeable_state": "clean"}
        self.pulls[number] = pr
        return pr

    async def pull(self, full_name, number):
        pr = self.pulls[number]
        if not pr["merged"]:
            pr["head"]["sha"] = await self.branch_sha(full_name, pr["head"]["ref"])
        return pr

    async def set_status(self, *args, **kwargs):
        return None

    comments: list[tuple[int, str]] = []

    async def comment(self, full_name=None, number=None, body="", *args, **kwargs):
        self._fail("comment")
        FakeGitHub.comments.append((number, body))
        FakeGitHub.issue_comments.append({
            "id": 9000 + len(FakeGitHub.issue_comments), "body": body, "html_url": "",
            "user": {"login": FakeGitHub.login, "type": "Bot"}, "author_association": "NONE",
            "created_at": utcnow().isoformat() + "Z"})
        return None

    # Reviews. A test fills ``reviews`` (REST reviews), ``threads`` (GraphQL review threads) and ``issue_comments``
    # (REST issue comments), or sets ``reviewer``: called as reviewer(number, head) before each snapshot, it stands
    # in for a reviewer reacting to the pull request. ``failures[name] = n`` makes the next n calls of a method fail.
    login = "factory-app[bot]"
    reviews: list[dict[str, Any]] = []
    threads: list[dict[str, Any]] = []
    issue_comments: list[dict[str, Any]] = []
    replies: list[tuple[int, str, str]] = []
    resolved: list[str] = []
    failures: dict[str, int] = {}
    reviewer: Callable[[int, str | None], None] | None = None

    def _fail(self, name: str) -> None:
        if FakeGitHub.failures.get(name, 0) > 0:
            FakeGitHub.failures[name] -= 1
            raise GitHubError(f"{name} failed", 503)

    async def review_snapshot(self, full_name, number):
        self._fail("review_snapshot")
        if FakeGitHub.reviewer:
            pr = self.pulls.get(number)
            FakeGitHub.reviewer(number, await self.branch_sha(full_name, pr["head"]["ref"]) if pr else None)
        return deepcopy({"identity": {"login": ""}, "pull_author": {"login": FakeGitHub.login, "type": "Bot"},
                         "reviews": FakeGitHub.reviews, "threads": FakeGitHub.threads,
                         "comments": FakeGitHub.issue_comments})

    async def reply_review_comment(self, full_name, number, comment_id, body):
        self._fail("reply_review_comment")
        FakeGitHub.replies.append((number, str(comment_id), body))
        for thread in FakeGitHub.threads:
            nodes = thread["comments"]["nodes"]
            if nodes and str(nodes[0].get("fullDatabaseId")) == str(comment_id):
                nodes.append({"fullDatabaseId": str(9000 + len(FakeGitHub.replies)), "url": "",
                              "body": f"{FACTORY_REPLY_MARKER}\n{body}", "authorAssociation": "NONE",
                              "author": {"login": FakeGitHub.login.removesuffix("[bot]"), "__typename": "Bot"},
                              "createdAt": utcnow().isoformat() + "Z", "updatedAt": utcnow().isoformat() + "Z"})
        return {}

    async def resolve_review_thread(self, thread_id):
        self._fail("resolve_review_thread")
        FakeGitHub.resolved.append(thread_id)
        for thread in FakeGitHub.threads:
            if thread["id"] == thread_id:
                thread["isResolved"] = True
        return {"id": thread_id, "isResolved": True}

    issues_state: dict[int, str] = {}

    async def request(self, method, path, *, json=None, params=None, **kwargs):
        number = int(path.rsplit("/", 1)[1]) if path.rsplit("/", 1)[1].isdigit() else None
        if method == "PATCH" and "/pulls/" in path:
            if "state" in (json or {}):
                self.pulls[number]["state"] = json["state"]
            if "body" in (json or {}):
                self.pulls[number]["body"] = json["body"]
            return {}
        if "/issues/" in path and number is not None:
            if method == "GET":
                return {"number": number, "state": FakeGitHub.issues_state.get(number, "open")}
            if method == "PATCH":
                FakeGitHub.issues_state[number] = json.get("state", "open")
                return {}
        raise AssertionError(f"unexpected request {method} {path}")

    # CI: one check, ``ci``, in ``check_state``. ``check_conclusion`` is its conclusion when that is not the state
    # ("cancelled"), ``check_run_id`` the Actions run it belongs to (its failed jobs can then be rerun; ``reruns``
    # lists the runs asked for), ``base_failing`` the checks failing on a commit asked about by name alone, and
    # ``workflows`` whether its pull requests run workflows.
    check_conclusion: str | None = None
    check_run_id: int | None = None
    reruns: list[int] = []
    base_failing: list[str] = []
    workflows = False

    async def checks(self, full_name, sha):
        check = {"name": "ci", "status": "completed", "conclusion": self.check_conclusion or self.check_state,
                 "url": "", "id": 1, "summary": "", "kind": "check_run"}
        if self.check_run_id:
            check["run_id"] = self.check_run_id
        return CheckSummary(self.check_state, [check])

    async def failing_checks(self, full_name, sha):
        self._fail("failing_checks")
        return list(FakeGitHub.base_failing)

    async def rerun_failed_jobs(self, full_name, run_id):
        self._fail("rerun_failed_jobs")
        FakeGitHub.reruns.append(run_id)

    async def has_pull_request_workflows(self, full_name):
        self._fail("has_pull_request_workflows")
        return FakeGitHub.workflows

    async def job_log_tail(self, full_name, check, limit=6000):
        return "Error: test failed in CI"

    async def merge(self, full_name, number, *, sha, method, title):
        import re as _re

        for n in _re.findall(r"Closes #(\d+)", self.pulls[number].get("body") or ""):  # GitHub's closing keywords
            FakeGitHub.issues_state[int(n)] = "closed"
        pr = self.pulls[number]
        bare = self._bare(full_name)
        with tempfile.TemporaryDirectory() as tmp:
            git("clone", "-q", str(bare), tmp)
            git("checkout", "-q", "main", cwd=tmp)
            git("merge", "-q", "--squash", f"origin/{pr['head']['ref']}", cwd=tmp)
            git("commit", "-qm", title, cwd=tmp)
            git("push", "-q", "origin", "main", cwd=tmp)
            merged_sha = git("rev-parse", "HEAD", cwd=tmp)
        pr["merged"], pr["state"], pr["merge_commit_sha"] = True, "closed", merged_sha
        self.merged.append(number)
        return {"sha": merged_sha, "merged": True}
