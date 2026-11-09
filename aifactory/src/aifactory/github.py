"""GitHub access for the control plane: repositories, pull requests, checks and merges.

The token is resolved per workspace and is only used by the control plane. Git transport
passes it through environment-scoped git config, so it never lands in a repository's
.git/config, on a command line, or inside a sandbox.
"""

from __future__ import annotations

import asyncio
import base64
import os
from dataclasses import dataclass
from typing import Any

import httpx

from .config import get_settings
from .db import session_scope
from .models import Secret
from .security import decrypt

API = "https://api.github.com"


REQUIRED_PERMISSIONS = {"contents": "write", "pull_requests": "write", "issues": "read", "checks": "read", "actions": "read"}
PERMISSION_INSTRUCTION = ("Create a fine-grained token for the selected repository with Contents and Pull requests: write; "
                          "Issues, Checks and Actions: read. Check organization approval and repository access, then retest.")
PERMISSION_EVIDENCE = {"not_verified", "classic_repo_scope", "classic_scope_missing", "private_repository_probe"}


def _rate_limited(response: httpx.Response) -> bool:
    if response.status_code == 429:
        return True
    if response.status_code != 403:
        return False
    # Primary quota can remain positive during a secondary/abuse limit.
    message = _message(response).lower()
    return ("retry-after" in response.headers or response.headers.get("x-ratelimit-remaining") == "0"
            or any(word in message for word in ("rate limit", "rate-limit", "secondary", "abuse")))


class GitHubError(RuntimeError):
    def __init__(self, message: str, status: int | None = None):
        super().__init__(message)
        self.status = status


def resolve_token(workspace_id: str) -> str:
    with session_scope() as s:
        sec = s.query(Secret).filter(Secret.workspace_id == workspace_id, Secret.kind == "github_token").one_or_none()
        if sec is not None and sec.status != "invalid":
            return decrypt(sec.ciphertext)
    token = get_settings().bootstrap_github_token or os.environ.get("GH_TOKEN") or os.environ.get("GITHUB_TOKEN", "")
    if not token:
        raise GitHubError("No GitHub access is configured for this workspace. Add a GitHub token in Settings.")
    return token


def git_auth_env(token: str) -> dict[str, str]:
    basic = base64.b64encode(f"x-access-token:{token}".encode()).decode()
    return {
        "GIT_CONFIG_COUNT": "1",
        "GIT_CONFIG_KEY_0": "http.https://github.com/.extraheader",
        "GIT_CONFIG_VALUE_0": f"AUTHORIZATION: basic {basic}",
        "GIT_TERMINAL_PROMPT": "0",
    }


@dataclass
class CheckSummary:
    state: str  # pending|success|failure|none
    checks: list[dict[str, Any]]

    @property
    def failing(self) -> list[dict[str, Any]]:
        return [c for c in self.checks if c["conclusion"] in ("failure", "timed_out", "cancelled", "action_required", "error", "startup_failure")]


class GitHub:
    def __init__(self, token: str):
        self.token = token

    def _headers(self) -> dict[str, str]:
        return {
            "Authorization": f"Bearer {self.token}",
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
            "User-Agent": "ai-factory",
        }

    async def request(self, method: str, path: str, *, json: Any = None, params: dict | None = None,
                      ok: tuple[int, ...] = (200, 201, 204), retries: int = 3) -> Any:
        url = path if path.startswith("http") else API + path
        last: Exception | None = None
        for attempt in range(retries + 1):
            try:
                async with httpx.AsyncClient(timeout=30.0) as client:
                    response = await client.request(method, url, headers=self._headers(), json=json, params=params)
                if response.status_code in ok:
                    if response.status_code == 204 or not response.content:
                        return {}
                    return response.json()
                message = _message(response)
                retryable = response.status_code >= 500 or (
                    response.status_code in (403, 429) and ("rate limit" in message.lower() or "secondary" in message.lower())
                )
                if not retryable or method not in ("GET", "HEAD") and response.status_code < 500:
                    raise GitHubError(f"GitHub {method} {path} failed ({response.status_code}): {message}", response.status_code)
                last = GitHubError(f"GitHub {method} {path} failed ({response.status_code}): {message}", response.status_code)
            except httpx.TransportError as exc:
                last = GitHubError(f"GitHub unreachable: {type(exc).__name__}")
            if attempt < retries:
                await asyncio.sleep(2 ** attempt)
        assert last is not None
        raise last

    async def diagnose(self, repository: str | None = None) -> tuple[str, str, dict[str, Any]]:
        """GET-only checks. Repository roles and accepted-permissions are not token grants."""
        from .credential_diagnostics import result

        permissions = [{"permission": name, "required": level, "state": "unverified", "evidence": "not_verified",
                        "instruction": PERMISSION_INSTRUCTION} for name, level in REQUIRED_PERMISSIONS.items()]
        metadata: dict[str, Any] = {"permissions": permissions}
        if repository:
            metadata["repository"] = repository

        def transient(code: str) -> tuple[str, str, dict[str, Any]]:
            # A failed check is not evidence, even if earlier probes succeeded.
            for entry in permissions:
                entry.update(state="unverified", evidence="not_verified", instruction=PERMISSION_INSTRUCTION)
            return result(code, "untested", **metadata)

        async with httpx.AsyncClient(timeout=10.0, follow_redirects=False) as client:
            async def get(path: str) -> httpx.Response:
                return await client.get(API + path, headers=self._headers(), params={"per_page": 1})

            try:
                me = await get("/user")
                if _rate_limited(me):
                    return transient("rate_limited")
                if me.status_code in (401, 403):
                    return result("github_invalid", "invalid", **metadata)
                if me.status_code != 200:
                    return transient("service_error")
                account = me.json()
                if not isinstance(account, dict) or not isinstance(account.get("login"), str):
                    return transient("service_error")
                metadata["login"] = account["login"]
                if not repository:
                    return result("github_permissions_unverified", "valid", **metadata)
                repo = await get(f"/repos/{repository}")
                if _rate_limited(repo):
                    return transient("rate_limited")
                if repo.status_code == 401:
                    return result("github_invalid", "invalid", **metadata)
                if repo.status_code in (403, 404):
                    return result("github_repository_unverified", "valid", **metadata)
                if repo.status_code != 200:
                    return transient("service_error")
                repo_data = repo.json()
                if not isinstance(repo_data, dict):
                    return transient("service_error")
                private = repo_data.get("private") is True
                # X-OAuth-Scopes describes actual classic token grants, unlike
                # X-Accepted-GitHub-Permissions which describes endpoint requirements.
                scope_header = me.headers.get("x-oauth-scopes")
                scopes = {scope.strip() for scope in (scope_header or "").split(",")}
                if "repo" in scopes:
                    for entry in permissions:
                        entry.update(state="present", evidence="classic_repo_scope", instruction="")
                    return result("github_verified", "valid", **metadata)
                if scope_header and private:
                    for entry in permissions:
                        entry.update(state="missing", evidence="classic_scope_missing")
                    return result("github_permissions_missing", "invalid", **metadata)
                # Public reads prove nothing about token grants. Neither contents
                # nor pull-request write can be safely probed without mutation.
                # /issues also accepts Pull requests:read, so never proves Issues.
                branch = repo_data.get("default_branch")
                probes = {"checks": f"commits/{branch}/check-runs" if isinstance(branch, str) and branch else None,
                          "actions": "actions/runs"}
                from urllib.parse import quote

                for entry in permissions:
                    suffix = probes.get(entry["permission"])
                    if suffix is None:
                        continue
                    if entry["permission"] == "checks":
                        suffix = "commits/" + quote(branch, safe="") + "/check-runs"
                    response = await get(f"/repos/{repository}/{suffix}")
                    if _rate_limited(response):
                        return transient("rate_limited")
                    if response.status_code == 401:
                        return result("github_invalid", "invalid", **metadata)
                    if response.status_code >= 500:
                        return transient("service_error")
                    if response.status_code == 200 and private:
                        entry.update(state="present", evidence="private_repository_probe", instruction="")
                return result("github_permissions_unverified", "valid", **metadata)
            except httpx.HTTPError:
                return transient("network_error")
            except (ValueError, TypeError):
                return transient("service_error")

    async def user(self) -> dict[str, Any]:
        return await self.request("GET", "/user")

    async def repo(self, full_name: str) -> dict[str, Any]:
        return await self.request("GET", f"/repos/{full_name}")

    async def accessible_repos(self, limit: int = 200) -> list[dict[str, Any]]:
        repos: list[dict[str, Any]] = []
        page = 1
        while len(repos) < limit:
            batch = await self.request("GET", "/user/repos", params={"per_page": 100, "page": page, "sort": "pushed"})
            if not batch:
                break
            repos.extend(batch)
            if len(batch) < 100:
                break
            page += 1
        return [
            {
                "full_name": r["full_name"],
                "default_branch": r.get("default_branch") or "main",
                "private": r.get("private", False),
                "description": r.get("description") or "",
                "can_push": bool((r.get("permissions") or {}).get("push")),
                "pushed_at": r.get("pushed_at"),
            }
            for r in repos[:limit]
        ]

    async def accessible_repos_page(self, page: int = 1, per_page: int = 100) -> dict[str, Any]:
        batch = await self.request("GET", "/user/repos", params={
            "per_page": per_page, "page": page, "sort": "pushed",
        })
        return {
            "items": [{
                "full_name": r["full_name"], "default_branch": r.get("default_branch") or "main",
                "private": r.get("private", False), "description": r.get("description") or "",
                "can_push": bool((r.get("permissions") or {}).get("push")), "pushed_at": r.get("pushed_at"),
            } for r in batch],
            "next_page": page + 1 if len(batch) == per_page else None,
        }

    async def branch_sha(self, full_name: str, branch: str) -> str:
        data = await self.request("GET", f"/repos/{full_name}/git/ref/heads/{branch}")
        return data["object"]["sha"]

    async def find_pull(self, full_name: str, head_branch: str) -> dict[str, Any] | None:
        owner = full_name.split("/")[0]
        pulls = await self.request("GET", f"/repos/{full_name}/pulls", params={"head": f"{owner}:{head_branch}", "state": "all"})
        return pulls[0] if pulls else None

    async def create_pull(self, full_name: str, *, head: str, base: str, title: str, body: str,
                          draft: bool = False) -> dict[str, Any]:
        existing = await self.find_pull(full_name, head)
        if existing and existing["state"] == "open":
            await self.request("PATCH", f"/repos/{full_name}/pulls/{existing['number']}", json={"title": title, "body": body})
            return await self.pull(full_name, existing["number"])
        payload = {"head": head, "base": base, "title": title, "body": body}
        if draft:
            payload["draft"] = True
        return await self.request("POST", f"/repos/{full_name}/pulls", json=payload)

    async def pull(self, full_name: str, number: int) -> dict[str, Any]:
        return await self.request("GET", f"/repos/{full_name}/pulls/{number}")

    async def comment(self, full_name: str, number: int, body: str) -> None:
        await self.request("POST", f"/repos/{full_name}/issues/{number}/comments", json={"body": body})

    async def _actions_checks(self, full_name: str, sha: str) -> list[dict[str, Any]]:
        """Fine-grained tokens often lack the Checks permission; Actions read covers workflow jobs."""
        runs = await self.request("GET", f"/repos/{full_name}/actions/runs", params={"head_sha": sha, "per_page": 50})
        checks: list[dict[str, Any]] = []
        for run in runs.get("workflow_runs", []):
            jobs = await self.request("GET", f"/repos/{full_name}/actions/runs/{run['id']}/jobs", params={"per_page": 100})
            for job in jobs.get("jobs", []):
                checks.append({"name": job["name"], "status": job["status"], "conclusion": job.get("conclusion") or "",
                               "url": job.get("html_url") or "", "id": job["id"], "summary": "", "kind": "check_run"})
            if not jobs.get("jobs"):
                checks.append({"name": run.get("name", "workflow"), "status": run["status"],
                               "conclusion": run.get("conclusion") or "", "url": run.get("html_url") or "",
                               "id": run["id"], "summary": "", "kind": "workflow"})
        return checks

    async def checks(self, full_name: str, sha: str) -> CheckSummary:
        checks: list[dict[str, Any]] = []
        try:
            runs = await self.request("GET", f"/repos/{full_name}/commits/{sha}/check-runs", params={"per_page": 100})
        except GitHubError as exc:
            if exc.status not in (403, 404):
                raise
            runs = {"check_runs": []}
            checks.extend(await self._actions_checks(full_name, sha))
        statuses = await self.request("GET", f"/repos/{full_name}/commits/{sha}/status")
        for run in runs.get("check_runs", []):
            checks.append({
                "name": run["name"],
                "status": run["status"],
                "conclusion": run.get("conclusion") or "",
                "url": run.get("html_url") or "",
                "id": run["id"],
                "summary": ((run.get("output") or {}).get("summary") or "")[:2000],
                "kind": "check_run",
            })
        for status in statuses.get("statuses", []):
            if status["context"].startswith("ai-factory/"):
                continue
            state = status["state"]
            checks.append({
                "name": status["context"],
                "status": "completed" if state != "pending" else "in_progress",
                "conclusion": {"success": "success", "failure": "failure", "error": "error"}.get(state, ""),
                "url": status.get("target_url") or "",
                "summary": status.get("description") or "",
                "kind": "status",
            })
        if not checks:
            return CheckSummary("none", [])
        if any(c["status"] != "completed" for c in checks):
            state = "pending"
        elif any(c["conclusion"] in ("failure", "timed_out", "cancelled", "action_required", "error", "startup_failure") for c in checks):
            state = "failure"
        else:
            state = "success"
        return CheckSummary(state, checks)

    async def set_status(self, full_name: str, sha: str, state: str, description: str, target_url: str = "") -> None:
        body = {"state": state, "context": "ai-factory/verification", "description": description[:140]}
        if target_url.startswith("https://"):
            body["target_url"] = target_url
        await self.request("POST", f"/repos/{full_name}/statuses/{sha}", json=body)

    async def merge(self, full_name: str, number: int, *, sha: str, method: str, title: str) -> dict[str, Any]:
        return await self.request(
            "PUT",
            f"/repos/{full_name}/pulls/{number}/merge",
            json={"sha": sha, "merge_method": method, "commit_title": title},
            retries=0,
        )

    async def job_log_tail(self, full_name: str, check: dict[str, Any], limit: int = 6000) -> str:
        """Best-effort failing-job log tail for GitHub Actions check runs."""
        if check.get("kind") != "check_run":
            return check.get("summary", "")
        try:
            async with httpx.AsyncClient(timeout=30.0, follow_redirects=True) as client:
                response = await client.get(f"{API}/repos/{full_name}/actions/jobs/{check['id']}/logs", headers=self._headers())
            if response.status_code == 200:
                return response.text[-limit:]
        except httpx.TransportError:
            pass
        return check.get("summary", "")

    async def issues(self, full_name: str, *, labels: str = "", since: str | None = None) -> list[dict[str, Any]]:
        params: dict[str, Any] = {"state": "open", "per_page": 50}
        if labels:
            params["labels"] = labels
        if since:
            params["since"] = since
        rows = await self.request("GET", f"/repos/{full_name}/issues", params=params)
        return [r for r in rows if "pull_request" not in r]


def _message(response: httpx.Response) -> str:
    try:
        return str(response.json().get("message", ""))[:400]
    except Exception:
        return response.text[:200]
