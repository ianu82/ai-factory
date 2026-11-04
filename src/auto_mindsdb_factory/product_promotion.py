"""Controller-only GitHub publication and exact-base fast-forward promotion."""

from __future__ import annotations

import fnmatch
import hashlib
import json
import subprocess
import time
from pathlib import Path
from urllib.parse import urlencode

from .automation import FactoryRunStore
from .intake import repo_root
from .product_profile import ProductProfile
from .product_task import ProductTask
from .product_workspace import git, WorkspaceError
from .run_journal import RunJournal


class PromotionError(RuntimeError):
    pass


class GitHubPromotion:
    def __init__(self, profile: ProductProfile, task: ProductTask, target: Path,
                 store: Path, *, smoke, deadline: float, api=None) -> None:
        self.profile = profile
        self.settings = profile.document
        self.task = task
        self.target = target
        self.store = store
        self.smoke = smoke
        self.deadline = deadline
        self.api = api or self._api
        self.repo = self.settings["repository"]

    def _api(self, path: str, body: dict | None = None):
        command = ["gh", "api", "--hostname", "github.com", f"repos/{self.repo}/{path}"]
        if body is not None:
            command += ["--method", "POST", "--input", "-"]
        remaining = self.deadline - time.monotonic()
        if remaining < 1:
            raise PromotionError("Run time budget exhausted")
        try:
            result = subprocess.run(command, input=json.dumps(body) if body is not None else None,
                capture_output=True, text=True, timeout=min(60, remaining))
            if result.returncode:
                raise PromotionError("GitHub request failed; inspect the run before retrying publication")
            return json.loads(result.stdout)
        except (OSError, subprocess.TimeoutExpired, json.JSONDecodeError) as exc:
            raise PromotionError("GitHub result unavailable; inspect the remote state before retrying") from exc

    def _branch_sha(self, branch: str) -> str | None:
        rows = git(self.target, "ls-remote", "--heads", "origin", f"refs/heads/{branch}").splitlines()
        return rows[0].split("\t")[0] if rows else None

    def _validate(self, candidate: dict) -> None:
        sha, base = candidate["candidate_sha"], candidate["base_sha"]
        if candidate["profile_sha"] != self.profile.digest:
            raise PromotionError("Candidate was verified under a different profile")
        if candidate["run_id"] != self.task.document["id"] or candidate["task_sha"] != self.task.digest:
            raise PromotionError("Candidate was verified against a different task")
        if git(self.target, "rev-list", "--parents", "-n", "1", sha).split() != [sha, base]:
            raise PromotionError("Promotion requires a single commit based on the tested base")
        verification = candidate["verification"]
        expected = {**self.settings["checks"], **{"acceptance:" + c["id"]: c["command"] for c in self.task.document["acceptance_criteria"]}}
        checks = verification["checks"]
        if verification["commit_sha"] != sha or len(checks) != len(expected) or {
            c["name"] for c in checks
        } != set(expected) or any(c["exit_code"] != 0 or c["command"] != expected[c["name"]] for c in checks):
            raise PromotionError("Required executable evidence is missing, failed or stale")
        report = verification["report"]
        expected_ids = {c["id"] for c in self.task.document["acceptance_criteria"]}
        if (report["status"] != "passed" or report["findings"] or len(report["criteria"]) != len(expected_ids)
                or {c["id"] for c in report["criteria"]} != expected_ids
                or any(c["status"] != "passed" or not c["evidence"].strip() for c in report["criteria"])):
            raise PromotionError("Independent review evidence is incomplete")
        paths = git(self.target, "diff", "--name-only", "-z", base, sha, "--").split("\0")
        paths = sorted(path for path in paths if path)
        if paths != candidate["changed_paths"] or not paths:
            raise PromotionError("Candidate scope evidence is stale")
        for path in paths:
            if (not any(fnmatch.fnmatchcase(path, p) for p in self.settings["allowed_paths"])
                    or any(fnmatch.fnmatchcase(path, p) for p in self.settings["protected_paths"])):
                raise PromotionError("Actual candidate diff exceeds permitted scope")

    @staticmethod
    def checks_passed(statuses: dict, checks: dict, required: list[str]) -> bool:
        if statuses.get("total_count", 0) > 100 or checks.get("total_count", 0) > 100:
            raise PromotionError("Check evidence exceeds the supported page size")
        states = [(item["context"], item["state"]) for item in statuses.get("statuses", [])]
        states += [(check["name"], check.get("conclusion") if check.get("status") == "completed" else "pending")
                   for check in checks.get("check_runs", [])]
        failed = {"failure", "error", "cancelled", "timed_out", "action_required", "startup_failure", "stale"}
        if any(state in failed for _, state in states):
            raise PromotionError("GitHub reports a failed candidate check")
        return all(any(name == wanted for name, _ in states) and all(
            state == "success" for name, state in states if name == wanted
        ) for wanted in required) and all(state in {"success", "neutral", "skipped"} for _, state in states)

    def _wait_checks(self, sha: str) -> None:
        while time.monotonic() < self.deadline:
            statuses = self.api(f"commits/{sha}/status?per_page=100")
            checks = self.api(f"commits/{sha}/check-runs?per_page=100&filter=latest")
            if self.checks_passed(statuses, checks, self.settings["required_status_checks"]):
                return
            time.sleep(min(5, max(0, self.deadline - time.monotonic())))
        raise PromotionError("Candidate checks remain pending, missing or unverified")

    def __call__(self, candidate: dict, journal: RunJournal) -> dict:
        self.profile.validate_repository(self.target)
        self._validate(candidate)
        sha, base = candidate["candidate_sha"], candidate["base_sha"]
        branch = "codex/factory-" + self.task.document["id"]
        journal.append("publication_requested", candidate_sha=sha, branch=branch)
        remote = self._branch_sha(branch)
        if remote is not None and remote != sha:
            raise PromotionError("Delivery branch changed; refusing to replace it")
        if remote is None:
            git(self.target, "push", f"--force-with-lease=refs/heads/{branch}:", "origin", f"{sha}:refs/heads/{branch}")
        prs = self.api("pulls?" + urlencode({"state": "all", "head": self.repo.split('/')[0] + ':' + branch}))
        if len(prs) > 1:
            raise PromotionError("Delivery branch has ambiguous pull request history")
        pr = prs[0] if prs else self.api("pulls", {
            "title": self.task.document["title"], "head": branch, "base": self.settings["base_branch"],
            "body": "\n\n".join([
                self.task.document["goal"],
                "Acceptance criteria:\n" + "\n".join("- " + c["description"] for c in self.task.document["acceptance_criteria"]),
                f"Independently verified candidate: `{sha}`\n\nProfile: `{self.profile.digest}`",
                "Validation: " + ", ".join(c["name"] for c in candidate["verification"]["checks"]),
                "The controller retains command results, provider usage and independent review evidence for this run.",
            ]),
        })
        journal.append("pull_request_published", number=pr["number"], url=pr["html_url"], commit_sha=sha)
        self.api(f"statuses/{sha}", {"state": "success", "context": "ai-factory/verification",
            "description": "Independent executable checks and review passed", "target_url": pr["html_url"]})
        if not self.settings["auto_merge"]:
            if pr.get("state") != "open":
                raise PromotionError("Published pull request is no longer open")
            return {"status": "pr_ready", **candidate, "pull_request_url": pr["html_url"]}
        lock_id = "promotion-" + hashlib.sha256(self.repo.lower().encode()).hexdigest()[:24]
        locks = FactoryRunStore(self.store, repo_root_override=repo_root())
        with locks.run_lease(lock_id, ttl_seconds=60, renew_interval_seconds=15) as lease:
            self._wait_checks(sha)
            current = self.api(f"pulls/{pr['number']}")
            if current["head"]["sha"] != sha or current["base"]["ref"] != self.settings["base_branch"]:
                raise PromotionError("Pull request changed after verification")
            remote_base = self._branch_sha(self.settings["base_branch"])
            if remote_base != sha:
                if remote_base != base or current["state"] != "open":
                    raise PromotionError("Base branch advanced or pull request closed; verification is stale")
                lease.heartbeat()
                journal.append("promotion_requested", candidate_sha=sha, expected_base_sha=base, pr_number=pr["number"])
                try:
                    # Parent validation above makes this a fast-forward. The lease is an
                    # atomic compare-and-swap on the tested base, including remote writers.
                    # GitHub branch protections still apply; no bypass is requested.
                    git(self.target, "push", f"--force-with-lease=refs/heads/{self.settings['base_branch']}:{base}",
                        "origin", f"{sha}:refs/heads/{self.settings['base_branch']}")
                except WorkspaceError:
                    if self._branch_sha(self.settings["base_branch"]) != sha:
                        raise PromotionError("Promotion was rejected or its result is unknown")
            while time.monotonic() < self.deadline:
                merged = self.api(f"pulls/{pr['number']}")
                if merged.get("merged"):
                    break
                if merged["head"]["sha"] != sha:
                    raise PromotionError("Pull request head changed during promotion")
                time.sleep(1)
            else:
                raise PromotionError("Merge confirmation unavailable; inspect remote state")
            if merged["head"]["sha"] != sha or merged.get("merge_commit_sha") != sha:
                raise PromotionError("Merged commit differs from verified candidate")
            journal.append("merge_confirmed", commit_sha=sha, pr_number=pr["number"])
            self.smoke(sha, journal)
            return {"status": "complete", **candidate, "merge_sha": sha, "pull_request_url": pr["html_url"]}
