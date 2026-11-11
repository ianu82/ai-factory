"""Bounded, observation-only review polling for recorded Factory task PRs.

No decisions, builders, acceptance or lifecycle transitions belong here. Delivery's
fresh final gate remains authoritative. Completed PRs are rotated indefinitely:
there is deliberately no task-completion lookback that can lose late feedback.
"""
from __future__ import annotations

import asyncio
from copy import deepcopy
from dataclasses import dataclass, field
from datetime import UTC, datetime
import hashlib
import logging
import time

from .db import session_scope
from .events import record
from .github import GitHub, GitHubError, resolve_credential
from .github_app import InstallationCredential
from .models import Repository, Task, Workspace
from .review import new_review_evidence, normalize_feedback, review_policy
from .signals import upsert_signal
from .task_evidence import locked_task, merge_evidence

log = logging.getLogger(__name__)


@dataclass
class ResourceQuota:
    remaining: int = 0
    reset: float = 0
    allowance: int = 0
    next_at: float = 0
    paused: bool = False

    def update(self, remaining: int, reset: float) -> None:
        if self.reset <= time.time():
            # Include the bootstrap read in the courtesy allowance.
            self.allowance = max(0, int(remaining * .2) - 1)
            self.reset = reset
            self.remaining = remaining
            self.next_at = 0
            self.paused = False
        else:
            # Other clients can spend quota too. Never replenish our allowance,
            # or undo a low-quota pause, from a later healthy response.
            self.remaining = min(self.remaining, remaining)
            self.allowance = min(self.allowance, int(remaining * .2))
        self.paused |= remaining < 1000


@dataclass
class Quota:
    resources: dict[str, ResourceQuota] = field(default_factory=dict)
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    bootstrap_at: float = 0

    def resource(self, name: str) -> ResourceQuota:
        return self.resources.setdefault(name, ResourceQuota())

    def headers(self, headers) -> None:
        if "x-ratelimit-remaining" not in headers:
            return
        name = headers.get("x-ratelimit-resource", "core")
        self.resource(name).update(int(headers["x-ratelimit-remaining"]),
                                   float(headers.get("x-ratelimit-reset", time.time() + 3600)))


def quota_key(credential):
    # Installation tokens rotate and bindings are re-created each cycle. GitHub
    # charges the installation, not the workspace or the reference's address.
    if isinstance(credential, InstallationCredential):
        return "installation:" + str(int(credential.installation))
    return "token:" + hashlib.sha256(credential.encode()).hexdigest()


class CourtesyGitHub(GitHub):
    """Reserve at least 80% of each resource's quota for delivery/other clients."""
    # The watcher's largest query has 100 threads x 100 comments: at most
    # 10,100 connection requests / 100 = 101 points. Reserve that upper bound
    # BEFORE sending; a response's actual charge settles the reservation.
    GRAPHQL_RESERVATION = 101

    def __init__(self, credential, quota):
        super().__init__(credential)
        self.quota = quota
        self._charge = None

    def observe_response(self, response):
        headers = response.headers
        if self._charge is not None:
            name, before, reserved = self._charge
            if headers.get("x-ratelimit-resource") == name and "x-ratelimit-remaining" in headers:
                # The remaining delta also accounts conservatively for concurrent
                # delivery traffic. Missing headers leave the full reservation spent.
                cost = max(1, before - int(headers["x-ratelimit-remaining"]))
                self.quota.resource(name).allowance += reserved - cost
        self.quota.headers(headers)

    async def request(self, method, path, **kwargs):
        name = "graphql" if path.rstrip("/").endswith("/graphql") else "core"
        reserved = self.GRAPHQL_RESERVATION if name == "graphql" else 1
        # Shared installations may have multiple workspace-bound clients. Serialize
        # budget reservations without sharing their authentication/binding checks.
        async with self.quota.lock:
            q = self.quota.resource(name)
            if q.reset <= time.time() and self.quota.bootstrap_at <= time.time():
                # Even a failed/malformed bootstrap must not produce one probe
                # per PR or workspace. /rate_limit reports both resources.
                self.quota.bootstrap_at = time.time() + 300
                data = await super().request("GET", "/rate_limit", retries=0)
                for resource in ("core", "graphql"):
                    reading = (data.get("resources") or {}).get(resource)
                    if reading:
                        self.quota.resource(resource).update(int(reading["remaining"]), float(reading["reset"]))
            if q.reset <= time.time() or q.paused or q.remaining < 1000 or q.allowance < reserved:
                raise GitHubError("Review observation deferred until quota reset")
            await asyncio.sleep(max(0, q.next_at - time.time()))
            # Sleeping must not carry a reservation into a different quota window.
            if q.reset <= time.time():
                raise GitHubError("Review observation deferred until quota reset")
            before = q.remaining
            q.allowance -= reserved
            self._charge = name, before, reserved
            try:
                # Retries must not escape the courtesy budget.
                kwargs["retries"] = 0
                return await super().request(method, path, **kwargs)
            finally:
                self._charge = None
                if q.remaining == before:
                    q.remaining = before - reserved
                spent = max(1, before - q.remaining)
                q.next_at = time.time() + max(1, (q.reset - time.time()) * spent / max(1, q.allowance))


def _timestamp(value):
    if not value:
        return 0
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return parsed.replace(tzinfo=parsed.tzinfo or UTC).timestamp()


def version(finding):
    return max((_timestamp(finding.get(k)) for k in ("created_at", "updated_at")), default=0)


def clear_pending(task):
    """Disable the observation hold without erasing delivery or signal history."""
    review = deepcopy((task.evidence or {}).get("review") or new_review_evidence())
    review.update(pending=[], count=0)
    merge_evidence(task, {"review": review})


def renewed(finding, entry):
    """An addressed version is not a permanent exemption for a stable ID."""
    previous = entry.get("finding")
    if previous and version(finding) > version(previous):
        return True
    return finding["resolved"] is False and (
        entry.get("observed_resolved") is True or
        bool(entry.get("resolved_at") and entry["resolved_at"] != entry.get("observed_resolution")))


async def trusted_findings(gh, repo, findings, threads, comments):
    permissions = {}

    async def writer(login):
        key = login.casefold()
        if key not in permissions:
            try:
                permissions[key] = await gh.repository_author_permission(repo, login) in ("admin", "write", "push", "maintain")
            except GitHubError as exc:
                if exc.status != 404:
                    raise
                # Public-repo outsiders are not collaborators, not a failed snapshot.
                # Continue through endorsements; do not discard trusted findings.
                permissions[key] = False
        return permissions[key]

    accepted = []
    for finding in findings:
        if finding["author_type"].lower() == "bot" or finding["author"].lower().endswith("[bot]") or await writer(finding["author"]):
            accepted.append(finding)
            continue
        replies = []
        if finding["thread_id"]:
            records = next((t["comments"]["nodes"] for t in threads if t["id"] == finding["thread_id"]), [])
            index = next((i for i, c in enumerate(records)
                          if str(c.get("fullDatabaseId") or c.get("databaseId")) == finding["comment_id"]), len(records))
            replies = records[index + 1:]
        elif finding["kind"] == "issue_comment":
            # PR comments have no reply relationship; only explicit +1 counts.
            replies = []
        endorsed = False
        for reply in replies:
            author = reply.get("author") or reply.get("user") or {}
            if author.get("login") and await writer(author["login"]):
                endorsed = True
                break
        if not endorsed and finding["kind"] != "review":
            endpoint = "pulls/comments" if finding["kind"] == "inline" else "issues/comments"
            cid = finding["id"].split(":", 1)[1]
            reactions = await gh._review_pages(f"/repos/{repo}/{endpoint}/{cid}/reactions")
            for reaction in reactions:
                login = (reaction.get("user") or {}).get("login")
                if reaction.get("content") == "+1" and login and await writer(login):
                    endorsed = True
                    break
        if endorsed:
            accepted.append(finding)
    return accepted


class ReviewWatcher:
    def __init__(self, *, batch_size=20, client_factory=CourtesyGitHub):
        self.batch_size = batch_size
        self.client_factory = client_factory
        self.quotas = {}

    async def run(self):
        while True:
            try:
                await self.poll()
            except Exception:
                log.exception("review watch cycle failed")
            await asyncio.sleep(300)

    async def poll(self):
        now = time.time()
        with session_scope() as s:
            rows = []
            disabled = []
            for task, repo, ws in s.query(Task, Repository, Workspace).join(
                    Repository, Task.repo_id == Repository.id).join(Workspace, Task.workspace_id == Workspace.id).filter(
                        Task.pr_number.isnot(None), Workspace.paused.is_(False)):
                if repo.workspace_id != ws.id:
                    continue
                policy = review_policy(ws.settings, repo.config)
                if policy["address"] == "off":
                    review = (task.evidence or {}).get("review") or {}
                    if review.get("pending") or review.get("count"):
                        disabled.append(task.id)
                    continue
                ledger = ((task.evidence or {}).get("review") or {}).get("prs", {}).get(str(task.pr_number), {})
                last = ledger.get("polled_at", 0)
                if ledger.get("merged_at") and now - last < 900:
                    continue
                priority = 0 if not ledger.get("merged_at") and task.status in ("awaiting_approval", "handed_over", "blocked", "needs_input") else 1
                rows.append((priority, last, task.id, ws.id, repo.id, repo.full_name, task.pr_number, task.head_sha, policy))
        for tid in disabled:
            with session_scope() as s:
                task = locked_task(s, tid)
                if not task:
                    continue
                ws = s.get(Workspace, task.workspace_id)
                repo = s.get(Repository, task.repo_id)
                if (ws and not ws.paused and repo and repo.workspace_id == ws.id
                        and review_policy(ws.settings, repo.config)["address"] == "off"):
                    clear_pending(task)
        rows.sort(key=lambda row: (row[0], row[1], row[2]))
        # Separate bounded lanes: backlog must never displace human-waiting
        # work, but an always-full urgent lane must not starve older PRs either.
        # Both lanes rotate by persisted polled_at, including failed snapshots.
        urgent = [r for r in rows if r[0] == 0]
        backlog = [r for r in rows if r[0] != 0]
        selected = urgent[:self.batch_size]
        backlog_slots = max(1, self.batch_size // 4, self.batch_size - len(selected))
        selected += backlog[:backlog_slots]
        clients = {}
        for _, _, tid, wid, rid, repo, number, head, policy in selected:
            try:
                if wid not in clients:
                    # Re-resolve every cycle: PAT removal/rotation and App rebinding
                    # must take effect without a process restart.
                    clients[wid] = None  # Failed credentials/identity are not retried per PR.
                    credential = resolve_credential(wid)
                    key = quota_key(credential)
                    gh = self.client_factory(credential, self.quotas.setdefault(key, Quota()))
                    identity = {"login": ""} if gh.installation_bound else await gh.user()
                    if not gh.installation_bound and not identity.get("login"):
                        raise GitHubError("GitHub review identity unavailable")
                    clients[wid] = gh, identity
                if clients[wid] is None:
                    continue
                gh, identity = clients[wid]
                await asyncio.wait_for(self._observe(gh, identity, tid, wid, rid, repo, number, head, policy), 180)
            except Exception:
                log.warning("review observation deferred for task %s", tid, exc_info=True)
            finally:
                # Every outcome rotates, including head mismatch, timeout and errors.
                with session_scope() as s:
                    task = locked_task(s, tid)
                    if task and task.pr_number == number:
                        review = deepcopy((task.evidence or {}).get("review") or new_review_evidence())
                        review.setdefault("prs", {}).setdefault(str(number), {})["polled_at"] = time.time()
                        merge_evidence(task, {"review": review})

    async def _observe(self, gh, identity, tid, wid, rid, repo, number, head, policy):
        pr = await gh.pull(repo, number)
        if not pr.get("merged_at") and (pr.get("head") or {}).get("sha") != head:
            return
        reviews = await gh.reviews(repo, number)
        threads = await gh.review_threads(repo, number)
        comments = await gh.issue_comments(repo, number)
        findings = normalize_feedback(reviews=reviews, threads=threads, comments=comments,
                                      identity=identity, pull_author=pr["user"], policy=policy)
        findings = await trusted_findings(gh, repo, findings, threads, comments)
        # A merge can happen during collection. Never publish a stale open snapshot.
        pr = await gh.pull(repo, number)
        merged_at = pr.get("merged_at")
        with session_scope() as s:
            task = locked_task(s, tid)
            ws = s.get(Workspace, wid)
            if not task or not ws or ws.paused or task.workspace_id != wid or task.repo_id != rid or task.pr_number != number:
                return
            repository = s.get(Repository, rid)
            if not repository or repository.workspace_id != wid:
                return
            current_policy = review_policy(ws.settings, repository.config)
            if current_policy["address"] == "off":
                clear_pending(task)
                return
            if current_policy != policy:
                return  # Never publish a snapshot collected under a stale policy.
            review = deepcopy((task.evidence or {}).get("review") or new_review_evidence())
            ledger = review.setdefault("prs", {}).setdefault(str(number), {})
            processed = ledger.setdefault("processed_versions", {})
            ledger["merged_at"] = merged_at
            if merged_at:
                for f in findings:
                    v = version(f)
                    if v <= _timestamp(merged_at) or v <= processed.get(f["id"], 0):
                        continue
                    signal, _ = upsert_signal(s, wid, source="review_feedback", title=f"Review feedback on {repo} #{number}",
                        body=f"{f['author']} on {f.get('path') or 'PR'}:{f.get('line') or ''}\n\n{f['body']}\n\n{f.get('diff_hunk') or ''}",
                        url=f["url"], external_id=f"{repo.lower()}:{number}:{f['id']}", repo_id=rid)
                    # Keep the standard 'new' proposal status so the Signals
                    # page offers human acceptance/ignore. Never auto-accept,
                    # or reset an already handled signal on a later edit.
                    signal.data = {"task_id": tid, "pr": number, "merged_at": merged_at, "finding": f}
                    processed[f["id"]] = v
            elif (pr.get("head") or {}).get("sha") == task.head_sha:
                # Publish observations into the shared, head-keyed FindingProgress
                # ledger contract for delivery's review gate. Only delivery owns
                # round execution, reply and resolution fields; this observer never
                # makes a merge decision or starts a builder.
                progress = review.setdefault("findings", {})
                pending = [f for f in findings if progress.get(f["id"], {}).get("status") != "addressed"
                           or renewed(f, progress[f["id"]])]
                has_renewed = any(progress.get(f["id"], {}).get("status") == "addressed" for f in pending)
                for finding in pending:
                    entry = progress.setdefault(finding["id"], {
                        "head": task.head_sha, "round": review.get("rounds", 0),
                        "status": "pending", "commit": None, "reply_id": None,
                        "replied_at": None, "resolved_at": None,
                    })
                    # Preserve in-flight delivery bookkeeping, including its head.
                    # A newer observation may refresh text, never settle a finding.
                    if entry["status"] == "addressed":
                        entry["status"] = "pending"
                        entry["head"] = task.head_sha
                    entry["finding"] = finding
                # Resolved threads are deliberately absent from normalized findings.
                # Remember their observed state so an explicit reopen also counts,
                # even if GitHub does not change the comment timestamp.
                thread_states = {t["id"]: t["isResolved"] for t in threads}
                for entry in progress.values():
                    thread_id = entry.get("finding", {}).get("thread_id")
                    if thread_id in thread_states:
                        entry["observed_resolved"] = thread_states[thread_id]
                        entry["observed_resolution"] = entry.get("resolved_at")
                signature = sorted((f["id"], version(f)) for f in pending)
                old = sorted((f["id"], version(f)) for f in review.get("pending", []))
                review["pending"], review["count"] = pending, len(pending)
                if pending and (signature != old or has_renewed):
                    authors = ", ".join(sorted({f["author"] for f in pending}))
                    record(wid, "review_pending", f"{len(pending)} review comments to address from {authors}",
                           task_id=tid, outcome_id=task.outcome_id, session=s)
            merge_evidence(task, {"review": review})
