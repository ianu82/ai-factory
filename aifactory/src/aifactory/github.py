"""GitHub access for the control plane: repositories, pull requests, checks and merges.

The token is resolved per workspace and is only used by the control plane. Git transport
passes it through environment-scoped git config, so it never lands in a repository's
.git/config, on a command line, or inside a sandbox.
"""

from __future__ import annotations

import asyncio
import base64
import time
from dataclasses import dataclass
from typing import Any

import httpx

from .db import session_scope
from .models import Secret
from .github_app import InstallationCredential, installation_id
from .security import decrypt

API = "https://api.github.com"


# Keep this contract in sync with delivery, signals, handover and pulls. Both web
# token recipes consume this list through the credentials API.
REQUIRED_PERMISSIONS = {"contents": "write", "pull_requests": "write", "issues": "write",
                        "statuses": "write", "checks": "read", "actions": "read",
                        "deployments": "read", "metadata": "read"}
PERMISSION_LABELS = {"contents": "Contents", "pull_requests": "Pull requests", "issues": "Issues",
                     "statuses": "Commit statuses", "checks": "Checks", "actions": "Actions",
                     "deployments": "Deployments", "metadata": "Metadata"}
PERMISSION_INSTRUCTION = ("Create a fine-grained token for the correct resource owner and selected repositories with "
                          + "; ".join(f"{PERMISSION_LABELS[name]}: {'Read and write' if level == 'write' else 'Read-only'}"
                                      for name, level in REQUIRED_PERMISSIONS.items())
                          + ". Check organization approval and repository access, then retest.")
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
    def __init__(self, message: str, status: int | None = None, *, rate_limited: bool = False):
        super().__init__(message)
        self.status = status
        self.rate_limited = rate_limited


def resolve_credential(workspace_id: str) -> str | InstallationCredential:
    """Capture an installation binding, not an expiring token, for long-running work."""
    installation = installation_id(workspace_id)
    if installation is not None:
        return InstallationCredential(workspace_id, installation)
    return resolve_token(workspace_id)


def workspace_client(workspace_id: str) -> GitHub:
    return GitHub(resolve_credential(workspace_id))


def resolve_token(workspace_id: str) -> str:
    installation = installation_id(workspace_id)
    if installation is not None:
        return InstallationCredential(workspace_id, installation).resolve()
    with session_scope() as s:
        sec = s.query(Secret).filter(Secret.workspace_id == workspace_id, Secret.kind == "github_token").one_or_none()
        if sec is not None and sec.status != "invalid":
            return decrypt(sec.ciphertext)
    from .fallbacks import github_token

    token = github_token(workspace_id)  # the operator's server token serves only the bootstrap workspace
    if not token:
        raise GitHubError("No GitHub access is configured for this workspace. Add a GitHub token in Settings.")
    return token


def git_auth_env(token: str | InstallationCredential) -> dict[str, str]:
    if isinstance(token, InstallationCredential):
        token = token.resolve()
    basic = base64.b64encode(f"x-access-token:{token}".encode()).decode()
    return {
        "GIT_CONFIG_COUNT": "1",
        "GIT_CONFIG_KEY_0": "http.https://github.com/.extraheader",
        "GIT_CONFIG_VALUE_0": f"AUTHORIZATION: basic {basic}",
        "GIT_TERMINAL_PROMPT": "0",
    }


async def fresh_git_auth_env(token: str | InstallationCredential) -> dict[str, str]:
    if isinstance(token, InstallationCredential):
        return await asyncio.to_thread(git_auth_env, token)
    return git_auth_env(token)


@dataclass
class CheckSummary:
    state: str  # pending|success|failure|none
    checks: list[dict[str, Any]]

    @property
    def failing(self) -> list[dict[str, Any]]:
        return [c for c in self.checks if c["conclusion"] in ("failure", "timed_out", "cancelled", "action_required", "error", "startup_failure")]


class GitHub:
    def __init__(self, token: str | InstallationCredential):
        self.token = token

    @property
    def installation_bound(self) -> bool:
        return isinstance(self.token, InstallationCredential)

    async def _fresh_headers(self) -> dict[str, str]:
        if self.installation_bound:
            token = await asyncio.to_thread(self.token.resolve)
            return self._headers_for(token)
        return self._headers()

    @staticmethod
    def _headers_for(token: str) -> dict[str, str]:
        return {
            "Authorization": f"Bearer {token}",
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
            "User-Agent": "ai-factory",
        }

    def _headers(self) -> dict[str, str]:
        if self.installation_bound:
            raise GitHubError("Installation headers require refreshable authentication")
        return self._headers_for(self.token)

    async def request(self, method: str, path: str, *, json: Any = None, params: dict | None = None,
                      ok: tuple[int, ...] = (200, 201, 204), retries: int = 3, timeout: float = 30.0) -> Any:
        url = path if path.startswith("http") else API + path
        last: Exception | None = None
        for attempt in range(retries + 1):
            try:
                async with httpx.AsyncClient(timeout=timeout) as client:
                    response = await client.request(method, url, headers=await self._fresh_headers(), json=json, params=params)
                if response.status_code in ok:
                    if response.status_code == 204 or not response.content:
                        return {}
                    return response.json()
                rate_limited = _rate_limited(response)
                if self.installation_bound:
                    if response.status_code in (401, 403, 404) and not rate_limited:
                        self.token.invalidate()
                    # Keep only structured rate-limit evidence, never provider messages.
                    raise GitHubError(f"GitHub App {method} request failed ({response.status_code}). Manage GitHub access.",
                                      response.status_code, rate_limited=rate_limited)
                message = _message(response)
                retryable = response.status_code >= 500 or rate_limited
                if not retryable or method not in ("GET", "HEAD") and response.status_code < 500:
                    raise GitHubError(f"GitHub {method} {path} failed ({response.status_code}): {message}", response.status_code,
                                      rate_limited=rate_limited)
                last = GitHubError(f"GitHub {method} {path} failed ({response.status_code}): {message}", response.status_code,
                                   rate_limited=rate_limited)
            except httpx.TransportError as exc:
                last = GitHubError(f"GitHub unreachable: {type(exc).__name__}")
            if attempt < retries:
                await asyncio.sleep(2 ** attempt)
        assert last is not None
        raise last

    async def diagnose(self, repository: str | None = None) -> tuple[str, str, dict[str, Any]]:
        """GET-only checks. Repository roles and accepted-permissions are not token grants."""
        from .credential_diagnostics import result

        if self.installation_bound:
            if repository:
                await self.repo(repository)
            else:
                await self.accessible_repos_page(per_page=1)
            return result("github_verified", "valid")

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
                return await client.get(API + path, headers=await self._fresh_headers(), params={"per_page": 1})

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
                          "actions": "actions/runs", "deployments": "deployments"}
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
                    if entry["permission"] == "deployments" and response.status_code in (403, 404):
                        entry.update(state="missing", evidence="not_verified")
                        return result("github_permissions_missing", "invalid", **metadata)
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
        data = await self.request("GET", f"/repos/{full_name}")
        if self.installation_bound:
            # Public repository metadata can be readable even when a repository
            # is outside the installation selection. Only listing membership
            # proves access; PAT-style permissions.push is absent for Apps.
            page = 1
            while True:
                accessible = await self.accessible_repos_page(page)
                if any(r["full_name"].casefold() == data["full_name"].casefold() for r in accessible["items"]):
                    break
                if accessible["next_page"] is None:
                    raise GitHubError("Repository is not available to this installation. Manage GitHub access.", 404)
                page = accessible["next_page"]
            data = {**data, "permissions": {**(data.get("permissions") or {}), "push": True}}
        return data

    async def repository_author_permission(self, full_name: str, author: str) -> str | None:
        """Current repository access, never issue association or label ownership.

        GitHub may represent write access as ``push``. Only a recognized
        permission from this endpoint is evidence of access; association and
        nested flags cannot fill in a missing or unknown permission.
        """
        from urllib.parse import quote

        if not author:
            return None
        try:
            data = await self.request("GET", f"/repos/{full_name}/collaborators/{quote(author, safe='')}/permission")
        except GitHubError as exc:
            if exc.status in (403, 404):
                return None
            raise
        if not isinstance(data, dict):
            return None
        permission = data.get("permission")
        if isinstance(permission, str) and permission in ("admin", "maintain", "write", "push", "read", "pull", "triage", "none"):
            return permission
        # Missing or unknown permission is not evidence of write access, even
        # if an unrelated nested field happens to say push=True.
        return None

    async def accessible_repos(self, limit: int = 200) -> list[dict[str, Any]]:
        repos: list[dict[str, Any]] = []
        page = 1
        while len(repos) < limit:
            result = await self.accessible_repos_page(page)
            repos.extend(result["items"])
            if result["next_page"] is None:
                break
            page = result["next_page"]
        return repos[:limit]

    async def accessible_repos_page(self, page: int = 1, per_page: int = 100) -> dict[str, Any]:
        path = "/installation/repositories" if self.installation_bound else "/user/repos"
        params = {"per_page": per_page, "page": page}
        if not self.installation_bound:
            params["sort"] = "pushed"
        data = await self.request("GET", path, params=params)
        batch = data["repositories"] if self.installation_bound else data
        return {
            "items": [{
                "full_name": r["full_name"], "default_branch": r.get("default_branch") or "main",
                "private": r.get("private", False), "description": r.get("description") or "",
                "can_push": self.installation_bound or bool((r.get("permissions") or {}).get("push")),
                "pushed_at": r.get("pushed_at"),
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

    async def _deployment_pages(self, path: str, *, params: dict | None = None,
                                deadline: float, clock=time.monotonic) -> list[dict[str, Any]]:
        """Bound pagination and each read by the caller's monotonic budget.

        No retries here: discovery polls again within its overall deadline.
        Never follow URLs supplied by deployment payloads or Link headers.
        """
        rows: list[dict[str, Any]] = []
        for page in range(1, 101):
            remaining = deadline - clock()
            if remaining <= 0:
                raise TimeoutError
            batch = await asyncio.wait_for(self.request(
                "GET", path, params={**(params or {}), "per_page": 100, "page": page},
                retries=0, timeout=min(30.0, remaining)), timeout=remaining)
            if not isinstance(batch, list) or any(not isinstance(row, dict) for row in batch):
                raise GitHubError("Malformed GitHub deployment response")
            rows.extend(batch)
            if len(batch) < 100:
                return rows
        raise GitHubError("GitHub deployment pagination limit exceeded")

    async def deployments(self, full_name: str, sha: str, *, deadline: float, clock=time.monotonic) -> list[dict[str, Any]]:
        return await self._deployment_pages(f"/repos/{full_name}/deployments", params={"sha": sha},
                                            deadline=deadline, clock=clock)

    async def deployment_statuses(self, full_name: str, deployment_id: int, *,
                                  deadline: float, clock=time.monotonic) -> list[dict[str, Any]]:
        return await self._deployment_pages(f"/repos/{full_name}/deployments/{deployment_id}/statuses",
                                            deadline=deadline, clock=clock)

    async def _review_pages(self, path: str) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        page = 1
        while True:
            batch = await self.request("GET", path, params={"per_page": 100, "page": page})
            if not isinstance(batch, list):
                raise GitHubError("Malformed GitHub review response")
            rows.extend(batch)
            if len(batch) < 100:
                return rows
            page += 1

    async def reviews(self, full_name: str, number: int) -> list[dict[str, Any]]:
        return await self._review_pages(f"/repos/{full_name}/pulls/{number}/reviews")

    async def issue_comments(self, full_name: str, number: int) -> list[dict[str, Any]]:
        return await self._review_pages(f"/repos/{full_name}/issues/{number}/comments")

    async def graphql(self, query: str, variables: dict[str, Any]) -> dict[str, Any]:
        response = await self.request("POST", "/graphql", json={"query": query, "variables": variables})
        # Partial data MUST NOT be used to decide that feedback is clean.
        if not isinstance(response, dict) or response.get("errors") or not isinstance(response.get("data"), dict):
            raise GitHubError("GitHub GraphQL returned errors or missing data")
        return response["data"]

    async def review_threads(self, full_name: str, number: int) -> list[dict[str, Any]]:
        owner, name = full_name.split("/", 1)
        comment_fields = """id fullDatabaseId databaseId body url createdAt updatedAt path line originalLine diffHunk
            author { login __typename } pullRequestReview { fullDatabaseId databaseId state }
            replyTo { fullDatabaseId databaseId }"""
        connection = "pageInfo { hasNextPage endCursor }"
        query = """query($owner:String!, $name:String!, $number:Int!, $after:String) {
            repository(owner:$owner, name:$name) { pullRequest(number:$number) {
                reviewThreads(first:100, after:$after) { nodes {
                    id isResolved isOutdated path line originalLine
                    comments(first:100) { nodes { %s } %s }
                } %s }
            } }
        }""" % (comment_fields, connection, connection)
        nested_query = """query($id:ID!, $after:String) {
            node(id:$id) { ... on PullRequestReviewThread {
                comments(first:100, after:$after) { nodes { %s } %s }
            } }
        }""" % (comment_fields, connection)

        def page_info(value: Any) -> tuple[list, str | None]:
            if not isinstance(value, dict) or not isinstance(value.get("nodes"), list):
                raise GitHubError("Incomplete GitHub review connection")
            info = value.get("pageInfo") or {}
            if type(info.get("hasNextPage")) is not bool or any(not isinstance(n, dict) for n in value["nodes"]):
                raise GitHubError("Incomplete GitHub review page")
            cursor = info.get("endCursor") if info["hasNextPage"] else None
            if info["hasNextPage"] and not cursor:
                raise GitHubError("Missing GitHub review page cursor")
            return value["nodes"], cursor

        threads: list[dict[str, Any]] = []
        after = None
        seen = set()
        while True:
            data = await self.graphql(query, {"owner": owner, "name": name, "number": number, "after": after})
            pull = ((data.get("repository") or {}).get("pullRequest") or {})
            nodes, cursor = page_info(pull.get("reviewThreads"))
            for thread in nodes:
                if not thread.get("id") or type(thread.get("isResolved")) is not bool:
                    raise GitHubError("Incomplete GitHub review thread")
                comments, comment_cursor = page_info(thread.get("comments"))
                comments = list(comments)
                comment_seen = set()
                while comment_cursor:
                    if comment_cursor in comment_seen:
                        raise GitHubError("Repeated GitHub review comment cursor")
                    comment_seen.add(comment_cursor)
                    more = await self.graphql(nested_query, {"id": thread["id"], "after": comment_cursor})
                    batch, comment_cursor = page_info((more.get("node") or {}).get("comments"))
                    comments.extend(batch)
                thread = {**thread, "comments": {"nodes": comments}}
                threads.append(thread)
            if cursor is None:
                return threads
            if cursor in seen:
                raise GitHubError("Repeated GitHub review thread cursor")
            seen.add(cursor)
            after = cursor

    async def reply_review_comment(self, full_name: str, number: int, comment_id: str | int,
                                   body: str) -> dict[str, Any]:
        from .review import FACTORY_REPLY_MARKER

        return await self.request("POST", f"/repos/{full_name}/pulls/{number}/comments/{comment_id}/replies",
                                  json={"body": f"{body}\n\n{FACTORY_REPLY_MARKER}"}, retries=0)

    async def resolve_review_thread(self, thread_id: str) -> dict[str, Any]:
        data = await self.graphql("""mutation($id:ID!) {
            resolveReviewThread(input:{threadId:$id}) { thread { id isResolved } }
        }""", {"id": thread_id})
        thread = (data.get("resolveReviewThread") or {}).get("thread")
        if not isinstance(thread, dict) or thread.get("id") != thread_id or thread.get("isResolved") is not True:
            raise GitHubError("GitHub did not confirm review thread resolution")
        return thread

    async def review_feedback(self, full_name: str, number: int, policy: dict) -> list[dict[str, Any]]:
        """Collect a complete authenticated snapshot; never swallow provider errors."""
        from .review import normalize_feedback, review_policy

        policy = review_policy({"review": policy})
        if policy["address"] == "off":
            return []
        # Installation tokens have no /user identity. The PR author is already
        # excluded by normalization; bot comments are filtered independently.
        identity = {"login": ""} if self.installation_bound else await self.user()
        pull = await self.pull(full_name, number)
        if (not self.installation_bound and not identity.get("login")) or not (pull.get("user") or {}).get("login"):
            raise GitHubError("GitHub review identity is unavailable")
        return normalize_feedback(reviews=await self.reviews(full_name, number),
                                  threads=await self.review_threads(full_name, number),
                                  comments=await self.issue_comments(full_name, number),
                                  policy=policy, identity=identity, pull_author=pull["user"])

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
        actions_loaded = False
        try:
            runs = await self.request("GET", f"/repos/{full_name}/commits/{sha}/check-runs", params={"per_page": 100})
        except GitHubError as exc:
            if exc.status not in (403, 404):
                raise
            runs = {"check_runs": []}
            checks.extend(await self._actions_checks(full_name, sha))
            actions_loaded = True
        if self.installation_bound:
            statuses = {}
            if not actions_loaded:
                checks.extend(await self._actions_checks(full_name, sha))
        else:
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
        if self.installation_bound:
            return  # Verification evidence lives in Factory and the PR; no Commit statuses grant.
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
                response = await client.get(f"{API}/repos/{full_name}/actions/jobs/{check['id']}/logs", headers=await self._fresh_headers())
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
