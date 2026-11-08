"""Test doubles: a scripted model and a GitHub backed by a local bare repository."""

from __future__ import annotations

import json
import subprocess
import tempfile
from pathlib import Path
from typing import Any, Callable

from aifactory.github import CheckSummary
from aifactory.inference import ProviderConfig


def call(name: str, args: dict[str, Any], n: int = 0) -> dict[str, Any]:
    return {"id": f"call_{name}_{n}", "type": "function", "function": {"name": name, "arguments": json.dumps(args)}}


class ScriptedModel:
    """Replaces ModelClient. ``scripts[role]`` returns the next assistant message for a conversation."""

    scripts: dict[str, Callable[[list[dict[str, Any]]], dict[str, Any]]] = {}
    calls: list[str] = []

    def __init__(self, provider: ProviderConfig, **_kwargs):
        self.provider = provider

    async def chat(self, *, model, messages, tools=None, ledger=None, **_kwargs):
        system = messages[0]["content"]
        role = "planner" if "planning engineer" in system else "verifier" if "verification engineer" in system else "builder"
        ScriptedModel.calls.append(role)
        if ledger:
            ledger.check()
            ledger.add(model, (1000, 200))
        return self.scripts[role](messages)

    async def session_cost(self, _session_id):
        return None

    async def list_models(self):
        return ["gpt-terra"]


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

    async def comment(self, *args, **kwargs):
        return None

    async def checks(self, full_name, sha):
        return CheckSummary(self.check_state, [{"name": "ci", "status": "completed", "conclusion": self.check_state,
                                                 "url": "", "id": 1, "summary": "", "kind": "check_run"}])

    async def job_log_tail(self, full_name, check, limit=6000):
        return "Error: test failed in CI"

    async def merge(self, full_name, number, *, sha, method, title):
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
