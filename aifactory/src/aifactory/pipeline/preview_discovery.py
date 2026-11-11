"""Provider-neutral, commit-bound preview discovery (control plane only).

Manual templates support {owner}, {repo}, {pr_number}, {head_sha}, {branch}.
Values are percent-encoded; attribute/index access and format specs are forbidden.
Manual provenance is an operator assertion, not deployment evidence.
"""
from __future__ import annotations

import asyncio
from dataclasses import dataclass
from fnmatch import fnmatchcase
import math
import re
from string import Formatter
import time
from typing import Literal
from urllib.parse import quote, urlsplit

from ..github import GitHub, GitHubError
from ..security import redact


@dataclass(frozen=True)
class PreviewDiscoveryResult:
    outcome: Literal["success", "timeout", "cancelled", "api_error", "missing_permission", "invalid_template"]
    sha: str
    elapsed_seconds: float
    provenance: Literal["github_deployment", "operator_supplied"] = "github_deployment"
    deployment_id: int | None = None
    status_id: int | None = None
    environment: str | None = None
    url: str | None = None
    deployment_state: str | None = None
    message: str = ""


def _url(value, secrets: tuple[str, ...]) -> str | None:
    # Do not turn a credential-bearing URL into a different usable URL. Reject it.
    # Queries/fragments are disallowed: they can contain access/session tokens.
    if not isinstance(value, str) or not value or len(value) > 4096:
        return None
    if redact(value, secrets) != value or any(c.isspace() or ord(c) < 32 for c in value) or "\\" in value:
        return None
    try:
        parts = urlsplit(value)
        if (parts.scheme not in ("http", "https") or not parts.hostname or parts.username is not None
                or parts.password is not None or parts.query or parts.fragment):
            return None
        _ = parts.port
    except ValueError:
        return None
    return value


def _manual_url(template: str, values: dict[str, str], secrets: tuple[str, ...]) -> str | None:
    if len(template) > 4096:
        return None
    try:
        for _, field, spec, conversion in Formatter().parse(template):
            if field is not None and (field not in values or spec or conversion):
                return None
        return _url(template.format_map({k: quote(str(v), safe="") for k, v in values.items()}), secrets)
    except (ValueError, KeyError):
        return None


async def discover_preview(github: GitHub, full_name: str, head_sha: str, *,
                           environment_pattern: str = "*", timeout_seconds: float = 300,
                           poll_interval: float = 5, cancel: asyncio.Event | None = None,
                           manual_template: str | None = None, pr_number: int | None = None,
                           branch: str = "", clock=time.monotonic, sleep=asyncio.sleep) -> PreviewDiscoveryResult:
    """Wait for the greatest matching deployment ID and its greatest status ID.

    IDs are GitHub's creation order, including ties in created_at. A newer failed,
    inactive, pending or URL-less deployment never falls back to an older success.
    Requests have no internal retries and share a monotonic deadline. Cancelling
    the calling task propagates CancelledError; an event returns typed evidence.
    Evidence deliberately excludes raw provider errors, descriptions and payloads.
    """
    if not re.fullmatch(r"[0-9a-fA-F]{40}|[0-9a-fA-F]{64}", head_sha):
        raise ValueError("An exact full head SHA is required")
    if not re.fullmatch(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+", full_name):
        raise ValueError("A repository owner/name is required")
    if not math.isfinite(timeout_seconds) or timeout_seconds < 0 or not math.isfinite(poll_interval) or poll_interval <= 0:
        raise ValueError("Invalid preview discovery time budget")
    head_sha = head_sha.lower()
    started = clock()
    deadline = started + timeout_seconds
    secrets = (github.token,) if isinstance(github.token, str) else ()
    evidence = {}

    def result(outcome, message=""):
        return PreviewDiscoveryResult(outcome=outcome, sha=head_sha,
                                      elapsed_seconds=max(0, clock() - started), message=message,
                                      provenance="operator_supplied" if manual_template is not None else "github_deployment",
                                      **evidence)

    async def poll():
        if cancel is not None and cancel.is_set():
            return result("cancelled")
        if manual_template is not None:
            owner, repo = full_name.split("/")
            url = _manual_url(manual_template, {"owner": owner, "repo": repo, "head_sha": head_sha,
                                              "pr_number": str(pr_number) if pr_number is not None else "",
                                              "branch": branch}, secrets)
            if url is None:
                return result("invalid_template", "Invalid manual preview URL template")
            evidence["url"] = url
            return result("success", "URL-to-commit provenance is operator supplied")
        while clock() < deadline:
            try:
                deployments = await github.deployments(full_name, head_sha, deadline=deadline, clock=clock)
                matches = [d for d in deployments if d.get("sha") == head_sha
                           and isinstance(d.get("environment"), str)
                           and fnmatchcase(d["environment"], environment_pattern)]
                if any(type(d.get("id")) is not int or d["id"] <= 0 for d in matches):
                    raise GitHubError("Malformed deployment ID")
                evidence.clear()
                if matches:
                    deployment = max(matches, key=lambda d: d["id"])
                    evidence.update(deployment_id=deployment["id"],
                                    environment=redact(deployment["environment"], secrets)[:256])
                    statuses = await github.deployment_statuses(full_name, deployment["id"], deadline=deadline, clock=clock)
                    if any(type(s.get("id")) is not int or s["id"] <= 0 for s in statuses):
                        raise GitHubError("Malformed deployment status")
                    if statuses:
                        status = max(statuses, key=lambda s: s["id"])
                        state = status.get("state")
                        evidence.update(status_id=status["id"], deployment_state=state if state in (
                            "success", "failure", "error", "inactive", "pending", "queued", "in_progress") else "unknown")
                        url = _url(status.get("environment_url"), secrets)
                        if state == "success" and url and clock() < deadline:
                            evidence["url"] = url
                            return result("success")
            except TimeoutError:
                return result("timeout")
            except GitHubError as exc:
                if exc.rate_limited:
                    return result("api_error", "GitHub deployment discovery rate limited")
                if exc.status in (403, 404):
                    return result("missing_permission", "Missing Deployments read permission or repository deployment access")
                return result("api_error", "GitHub deployment discovery failed")
            except (ValueError, TypeError):
                return result("api_error", "Malformed GitHub deployment response")
            remaining = deadline - clock()
            if remaining > 0:
                await sleep(min(poll_interval, remaining))
        return result("timeout")

    # Race the entire operation, not just sleeps: cancellation interrupts an HTTP
    # request or installation-token refresh as well. Always reap child tasks.
    operation = asyncio.create_task(poll())
    cancelled = asyncio.create_task(cancel.wait()) if cancel is not None else None
    try:
        done, _ = await asyncio.wait({operation, cancelled} if cancelled else {operation},
                                     timeout=timeout_seconds if manual_template is None else None,
                                     return_when=asyncio.FIRST_COMPLETED)
        if cancelled is not None and cancelled in done:
            return result("cancelled")
        if operation in done:
            return operation.result()
        return result("timeout")
    finally:
        for task in (operation, cancelled):
            if task is not None:
                task.cancel()
        await asyncio.gather(*(t for t in (operation, cancelled) if t is not None), return_exceptions=True)
