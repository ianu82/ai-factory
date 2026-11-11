"""Task delivery: build → checks → independent verification → PR → CI → merge → smoke → release.

Each stage is durable (``task.stage``) so work resumes after approvals or restarts without
repeating external side effects. Failures inside an attempt become precise feedback for the
next attempt; only genuine decisions or provider/access problems reach a person.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import re
import shlex
import time
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from typing import Any, Callable

from ..config import get_settings
from ..db import session_scope, utcnow
from ..events import notify, record
from ..guided import README_TEMPLATE, outcome_guidance
from .. import merge_manager
from ..github import CheckSummary, GitHub, GitHubError, resolve_credential as resolve_token
from ..gitops import (AUTHOR, DEFAULT_PROTECTED, BranchMoved, GitError, Patch, _mirror_path, git, apply_in_sandbox, checkout,
                      changed_paths, path_overlap, conflict_marker_files, extract_patch, protected_violations, publish, rebase_branch, revert_commit)
from ..models import Artifact, Decision, Outcome, Repository, Run, Signal, Task, new_id
from ..prompts import BUILDER, build_prompt, verifier_system, verify_prompt
from ..sandbox import LocalSandbox, SandboxError
from .check_slots import check_slot
from .context import plan_context, repo_instructions
from .qa import AppRun, change_checks, verifier_brief
from .runtime import (SessionSpec, budget_extension, guidance_history, open_decision, outcome_committed,
                      outcome_cost, precedents, publish_decision, run_agent, workspace_settings)

from .lifecycle import transition

log = logging.getLogger(__name__)

CI_POLL_TIMEOUT_SECONDS = 90

# Billing, quota and access problems: a person has to act (top up credits, fix a key), so the task blocks and no
# attempt is charged. Each harness words them differently: the native client ("MindsHub: no credit remaining"), Codex
# through the Factory gateway ("wallet_empty", "Provider returned 402: …") and Anton through the OpenAI SDK
# ("Error code: 402 - …", "insufficient_quota").
PROVIDER_PROBLEMS = ("refused the credential", "no credit", "No model provider", "limit reached", "wallet",
                     "No GitHub access", "Bad credentials", "credits are used up", "wallet_empty", "insufficient_quota",
                     "quota exceeded", "billing")
# HTTP 402 as a status code, never as digits inside a number such as a token count ("1,402,000").
_PAYMENT_REQUIRED = re.compile(r"(?:\b(?:returned|error code|status(?: code)?|http)\W{0,3}|\()402\b|\b402 payment",
                               re.IGNORECASE)
# An attempt's own caps (turns, time, tokens, cost) are not infrastructure: they stay charged.
RUN_CAPS = ("budget of", "budget_exhausted", "time budget", "Stopped after")
# The provider or the sandbox was unavailable after the client's own retries: the same attempt is redone later.
TRANSIENT_PROBLEMS = ("Model request failed", "unreachable", "timed out", "timeout", "connection error",
                      "connection reset", "connection refused", "stream disconnected", "server disconnected",
                      "bad gateway", "service unavailable", "gateway timeout", "overloaded", "upstream_error",
                      "VM bridge", "No sandbox identities", "Sandbox setup failed")
_SERVER_ERROR = re.compile(r"\b(?:returned|error(?: code)?|status(?: code)?|http)\W{0,3}(?:5\d\d|429)\b",
                           re.IGNORECASE)
INFRA_RETRIES = 3  # consecutive automatic retries of an infrastructure failure before a person decides
INFRA_BACKOFF_SECONDS = (120, 600)  # before the first retry, then before each later one
_PHASES = {"prepare": "Preparing the sandbox", "build": "The builder session",
           "checks": "Checking the base commit", "verify": "Independent verification"}
# Attempt records that do not end an attempt: an infrastructure failure (the same attempt is redone) and checks that
# fail on the base commit too (the attempt goes on to verification).
NOT_ATTEMPTS = ("infra", "checks_preexisting")
# How far an attempt got: a record ends a run of infrastructure retries only when it shows that the phase which
# failed (or a later one) has since completed.
_PHASE_ORDER = {"prepare": 0, "build": 1, "checks": 2, "verify": 3}
_COMPLETED_PHASE = {"builder_failed": "build", "builder_blocked": "build", "policy": "checks",
                    "checks_failed": "checks", "checks_preexisting": "checks", "verification_failed": "verify",
                    "verified": "verify"}


def recovered_from_infra(result: str, failing_phase: str | None) -> bool:
    """Whether an attempt record ends a run of infrastructure retries: it must show that the phase which failed
    (or a later one) has since completed. Records from earlier phases, such as checks that also fail on the base
    while the verifier keeps failing, leave the count alone, so retries stay bounded."""
    done = _COMPLETED_PHASE.get(result)
    if result == "infra" or done is None:
        return False
    return failing_phase is None or _PHASE_ORDER[done] >= _PHASE_ORDER.get(failing_phase, 0)


# Stages inside the build/verify loop; a task found in one after a restart was interrupted mid-attempt.
BUILD_STAGES = ("prepare", "build", "checks", "verify", "publish")
# Check results on clean checkouts of base commits, by (repository, base commit, command), shared by every task.
_BASE_CHECKS: dict[tuple[str, ...], dict[str, Any]] = {}


def infrastructure_problem(error: str) -> str:
    """Why a session or sandbox failed when it was not the attempt's doing: "provider" (billing, quota or access; a
    person acts), "transient" (the provider or the sandbox was unavailable; the attempt is redone later), or "" (the
    attempt's own failure, including its turn, time, token and cost caps)."""
    text = (error or "").lower()
    if any(p.lower() in text for p in PROVIDER_PROBLEMS) or _PAYMENT_REQUIRED.search(text):
        return "provider"
    if any(c.lower() in text for c in RUN_CAPS):
        return ""
    if any(p.lower() in text for p in TRANSIENT_PROBLEMS) or _SERVER_ERROR.search(text):
        return "transient"
    return ""


class Stop(Exception):
    """The task paused or ended for a recorded reason; the orchestrator should release its slot."""


class TaskRunner:
    def __init__(self, task_id: str, cancelled: asyncio.Event, repo_lock: Callable[[str], asyncio.Lock]):
        self.task_id = task_id
        self.cancelled = cancelled
        self.repo_lock = repo_lock
        self.builder: LocalSandbox | None = None
        self.feedback: list[str] = []
        self.patch: Patch | None = None
        self.builder_report: dict[str, Any] = {}
        self.verifier_report: dict[str, Any] = {}
        self.checks: list[dict[str, Any]] = []
        self.app_summary: dict[str, Any] | None = None
        self._ci_repairing = False
        # The repository's agent instructions, by base commit and whether they are for the verifier.
        self._instructions: dict[tuple[str, bool], list[str]] = {}
        self._load()
        self.feedback = self._prior_feedback()
        self.builder_report = (self.task.evidence or {}).get("builder_report") or {}
        revalidation = (self.task.evidence or {}).get("revalidation") or {}
        if revalidation.get("phase") in ("rejected", "handled"):
            self.feedback += revalidation.get("feedback") or []

    async def _sections(self, verifier: bool = False) -> list[str]:
        """The plan around this task and the repository's instructions for agents, read at the base commit (never
        from the candidate, which the builder may have changed)."""
        key = (self.task.base_sha or "", verifier)
        if key not in self._instructions:
            self._instructions[key] = await repo_instructions(self.repo.full_name, key[0], verifier=verifier)
        return plan_context(self.task_id) + self._instructions[key]

    def _prior_feedback(self) -> list[str]:
        """Reviewer findings from earlier attempts survive retries and restarts, so new attempts converge."""
        notes: list[str] = []
        for attempt in (self.task.evidence or {}).get("attempts", [])[-3:]:
            verdict = attempt.get("verdict") or {}
            if attempt.get("result") == "verification_failed" and verdict:
                notes.append(f"- An earlier attempt failed independent review: {str(verdict.get('summary', ''))[:800]}")
                for c in _objects(verdict.get("criteria")):
                    if c.get("status") != "pass":
                        notes.append(f"  - Criterion not met: {c.get('criterion')} — {str(c.get('evidence', ''))[:400]}")
                for f in _objects(verdict.get("findings")):
                    if f.get("severity") in ("blocker", "major"):
                        notes.append(f"  - {f.get('severity')}: {str(f.get('description', ''))[:400]} {f.get('location', '')}")
            elif attempt.get("result") == "checks_failed" and any(not c.get("ok") for c in attempt.get("checks") or []):
                for c in attempt["checks"]:
                    if not c.get("ok"):
                        notes.append(f"- In an earlier attempt, check `{c.get('command')}` failed (exit {c.get('exit_code')})"
                                     + (" (it fails the same way on the base commit)" if c.get("preexisting") else "")
                                     + (f":\n```\n{c['tail']}\n```" if c.get("tail") else ""))
            elif attempt.get("result") in ("checks_failed", "policy"):
                notes.append(f"- An earlier attempt failed: {attempt.get('detail') or attempt.get('result')}")
        return notes

    # state ---------------------------------------------------------------------
    def _load(self) -> None:
        with session_scope() as s:
            task = s.get(Task, self.task_id)
            self.task = Task(**{c.name: getattr(task, c.name) for c in Task.__table__.columns})
            outcome = s.get(Outcome, task.outcome_id)
            if outcome.source == "review":
                raise ValueError("Review outcomes cannot enter task delivery")
            self.outcome = Outcome(**{c.name: getattr(outcome, c.name) for c in Outcome.__table__.columns})
            repo = s.get(Repository, task.repo_id)
            self.repo = Repository(**{c.name: getattr(repo, c.name) for c in Repository.__table__.columns})
        self.ws = self.task.workspace_id
        self.settings = workspace_settings(self.ws)
        self.cfg = self.repo.config or {}

    def update(self, **fields: Any) -> None:
        with session_scope() as s:
            task = s.get(Task, self.task_id)
            if (task.status, task.stage) != (self.task.status, self.task.stage) or not transition(s, task, **fields):
                raise Stop()
            for key, value in fields.items():
                setattr(self.task, key, value)
        notify(self.ws, "task", self.task_id, outcome_id=self.task.outcome_id)

    def event(self, kind: str, message: str, level: str = "info", **data: Any) -> None:
        record(self.ws, kind, message, outcome_id=self.task.outcome_id, task_id=self.task_id, level=level,
               data={"attempt": self.task.attempt, "head": self.task.head_sha, **data})

    def evidence(self, key: str, value: Any) -> None:
        self._set_evidence({key: value})

    def _set_evidence(self, values: dict[str, Any]) -> None:
        """Record evidence keys in one transaction ("attempts" appends)."""
        with session_scope() as s:
            task = s.get(Task, self.task_id)
            # Deep copy: mutating the stored JSON in place would hide the change from the ORM.
            ev = json.loads(json.dumps(task.evidence or {}))
            for key, value in values.items():
                if key == "attempts":  # tagged with the budget it belongs to (attempt numbers restart per budget)
                    ev.setdefault("attempts", []).append({**value, "epoch": int(ev.get("budget_epoch", 0))})
                    if recovered_from_infra(str(value.get("result")), ev.get("infra_phase")):
                        ev.pop("infra_retries", None)  # the phase that failed has since completed
                        ev.pop("infra_phase", None)
                    continue
                if key in ("ci", "smoke"):
                    value = {**value, "at": utcnow().isoformat()}
                if key in ("ci", "smoke") and value.get("state") in ("failure", "failed"):
                    ev.setdefault("failure_facts", []).append({"kind": key, **value,
                                                              "attempt": task.attempt, "at": utcnow().isoformat()})
                ev[key] = value
            task.evidence = ev
            self.task.evidence = ev

    def artifact(self, kind: str, name: str, content: str = "", run_id: str | None = None, **data: Any) -> str:
        artifact_id = new_id()
        with session_scope() as s:
            s.add(Artifact(id=artifact_id, workspace_id=self.ws, outcome_id=self.task.outcome_id,
                           task_id=self.task_id, run_id=run_id, kind=kind, name=name,
                           content=content[:400_000], data=data))
        return artifact_id

    def stage(self, stage: str, message: str | None = None) -> None:
        self.update(stage=stage, status="running")
        if message:
            self.event("stage", message, stage=stage)

    def block(self, kind: str, title: str, body: str, *, status: str = "blocked",
              options: list[dict[str, Any]] | None = None) -> None:
        with session_scope() as s:
            task = s.get(Task, self.task_id)
            if not transition(s, task, status=status, blocked_reason=body[:2000], lease_owner=""):
                raise Stop()
            # The decision and state transition commit together; no second transaction
            # can create a retry decision after a concurrent stop.
            decision = Decision(workspace_id=self.ws, kind=kind, title=title, body=body,
                                outcome_id=self.task.outcome_id, task_id=self.task_id,
                                options=options or [{"value": "retry", "label": "Retry"},
                                                    {"value": "abandon", "label": "Abandon task"}])
            s.add(decision)
            s.flush()
            decision_id = decision.id
        publish_decision(self.ws, decision_id, kind, title, outcome_id=self.task.outcome_id, task_id=self.task_id)
        notify(self.ws, "task", self.task_id, outcome_id=self.task.outcome_id)
        raise Stop()

    # entry ---------------------------------------------------------------------
    async def run(self) -> None:
        try:
            if not self.task.started_at:
                self.update(started_at=utcnow())
            stage = self.task.stage
            phase = ((self.task.evidence or {}).get("revalidation") or {}).get("phase")
            if phase not in (None, "complete", "handled"):
                await self.merge()  # checks for an outside merge before resuming missing gates
                return
            if phase == "complete" and stage in ("verify", "checks"):
                await self.ci_then_merge()
                return
            if stage in BUILD_STAGES:  # interrupted mid-attempt: a restart, or requeued after an infrastructure failure
                if stage in ("verify", "publish") and self._restore_verified_candidate():
                    token = resolve_token(self.ws)
                    await self._publish(token)
                    await self.ci_then_merge()
                    return
                # Inside a fix round, the attempt continues that round on the pull request's branch; it must not
                # rebuild from the default branch. The rebuild after a rejected revalidation is such a round too.
                stage = (self.task.evidence or {}).get("fix_round") or ("revise" if phase == "handled" else stage)
            if stage == "merge":
                await self.merge()
            elif stage == "ci_repair":
                await self._fix_ci(GitHub(resolve_token(self.ws)))
            elif stage == "ci_refresh":
                await self._refresh_onto_main(resolve_token(self.ws), GitHub(resolve_token(self.ws)),
                                              self.task.evidence["ci_wait"]["stale_main"])
            elif stage == "revert_merge":
                await self._merge_revert()
            elif stage == "ci":
                await self.ci_then_merge()
            elif stage == "smoke":
                await self.smoke()
            elif stage == "release":
                await self.release()
            elif stage == "revise":
                await self.deliver(continue_from_branch=True)
            elif stage == "revert":
                await self.revert()
            else:
                await self.deliver()
        except Stop:
            pass
        finally:
            if self.builder:
                await self.builder.close()
                self.builder = None

    # build / verify loop ---------------------------------------------------------
    async def deliver(self, continue_from_branch: bool = False) -> None:
        merge_manager.leave(self)  # a rebuild re-enters the merge queue at the back when it is ready again
        max_attempts = int(self.settings.get("max_attempts", 3))
        token = resolve_token(self.ws)
        # Durable, so an attempt interrupted inside this round resumes the same round (see run()).
        fix_round = ("ci_repair" if self._ci_repairing else "revise") if continue_from_branch else ""
        if ((self.task.evidence or {}).get("fix_round") or "") != fix_round:
            self.evidence("fix_round", fix_round)
        finished = {a.get("n") for a in self._budget_attempts()}
        # Verified, but interrupted before publication and the candidate could not be reloaded (see run()).
        unpublished = bool(self._unpublished_candidate())
        if self.task.attempt and (self.task.attempt not in finished or unpublished):
            # Every attempt that ends records its result; one without a record was interrupted (a restart or
            # redeploy), so it is redone rather than counted against the limit.
            self.event("attempt_resumed", f"Attempt {self.task.attempt} was interrupted; running it again")
            self.update(attempt=self.task.attempt - 1)
        while True:
            attempt = self.task.attempt + 1
            if attempt - int((self.task.evidence or {}).get("attempt_base", 0)) > max_attempts and self._auto_retry():
                attempt = self.task.attempt + 1
            if attempt - int((self.task.evidence or {}).get("attempt_base", 0)) > max_attempts:
                summary = "\n".join(self.feedback[-6:])[:3000]
                retried = int((self.task.evidence or {}).get("auto_retries", 0))
                tried = f"{max_attempts} attempts" + (" and an automatic retry" if retried == 1 else
                                                      f" and {retried} automatic retries" if retried else "")
                self.block("unblock", f"{self.task.key} did not pass after {tried}",
                           f"The task could not be completed within the attempt limit. Latest feedback:\n\n{summary}",
                           status="failed",
                           options=[{"value": "retry", "label": "Try again (fresh attempts)"},
                                    {"value": "take_over", "label": "I'll take it from here"},
                                    {"value": "abandon", "label": "Abandon task"}])
            self.update(attempt=attempt)
            budget = self.settings["budgets"].get("outcome_usd")
            # Spending whose cost is known is a lower bound, so reaching the budget with it alone is conclusive.
            spent, _ = outcome_committed(self.task.outcome_id)  # including what a running Lead has reserved
            extra = budget_extension(self.settings)
            if (self.task.evidence or {}).get("budget_override"):
                # Continuing used to lift the limit for good. Such a task gets one allowance from here instead.
                with session_scope() as s:
                    task = s.get(Task, self.task_id)
                    ev = {k: v for k, v in (task.evidence or {}).items() if k != "budget_override"}
                    task.evidence = self.task.evidence = {**ev, "budget_allowance_usd": round(spent + extra, 2)}
            # Continuing past the budget allows spending up to an amount (decisions.resolve), not without limit.
            limit = max(budget or 0, (self.task.evidence or {}).get("budget_allowance_usd") or 0)
            if budget and spent >= limit:
                more = f"{extra:.2f}".removesuffix(".00")
                extended = limit > budget
                reached = f"the ${limit:.2f} it was allowed to spend" if extended else f"its ${budget:.0f} budget"
                self.block("budget", f"Outcome #{self.outcome.number} reached {reached}",
                           f"Spent ${spent:.2f}; the limit was ${limit:.2f}"
                           + (f" (the ${budget:.0f} budget, extended when it last continued)" if extended else "")
                           + f". Raise the budget in Settings, or continue with ${more} more than is spent now.",
                           status="needs_input",
                           options=[{"value": "retry", "label": f"Continue with ${more} more"},
                                    {"value": "abandon", "label": "Stop"}])
            if self.builder is None:
                try:
                    await self._prepare_builder(token, from_branch=continue_from_branch and bool(self.task.branch))
                except (SandboxError, GitError, OSError) as exc:  # the environment, not the attempt
                    error = f"Sandbox setup failed: {exc}"
                    self._infrastructure(attempt, "prepare", error, infrastructure_problem(error))
            self.stage("build", f"Attempt {attempt}: building with {self.task.harness}")
            sections = await self._sections()
            run, result, error = await run_agent(SessionSpec(
                workspace_id=self.ws, outcome_id=self.task.outcome_id, task_id=self.task_id, role="builder",
                effective_settings=self.settings, repositories={self.repo.full_name: self.cfg},
                harness=self.task.harness, system=BUILDER, sandbox=self.builder, attempt=attempt, writable=True,
                prompt=build_prompt(self.task, self.outcome, self.repo, self.feedback[-8:],
                                    precedents(self.ws, self.repo.id),
                                    guidance_history(self.task.outcome_id, self.task_id), sections)), self.cancelled)
            if result is None:
                cause = infrastructure_problem(error)
                if cause:
                    self._infrastructure(attempt, "build", error, cause)
                # A session stopped by its own cap (time, turns, tokens, cost) may have left the work done, so its
                # tree is checked as it stands: unless it is empty or the candidate this runner already judged.
                left = None
                if any(cap.lower() in error.lower() for cap in RUN_CAPS):
                    try:
                        left = await extract_patch(self.builder, self.task.base_sha)
                    except SandboxError:
                        pass
                if left is None or left.empty or (self.patch is not None and left.diff == self.patch.diff):
                    self.feedback.append(f"- The previous builder session ended without a report: {error[:500]}")
                    self.evidence("attempts", {"n": attempt, "result": "builder_failed", "error": error[:500]})
                    continue
                from ..harness import HarnessResult

                self.feedback.append(f"- The previous builder session reached its limit before it reported "
                                     f"({error[:300]}). The factory checked the change as that session left it. Work "
                                     "in small complete steps and finish with your report before the limit.")
                self.event("builder_unreported", f"The builder session reached its limit before it reported "
                           f"({error[:200]}); checking the {len(left.files)} file(s) it changed as they stand",
                           level="warn", error=error[:300], files=len(left.files))
                # A stand-in for the report, shown to the verifier and on the pull request in place of a summary.
                result = HarnessResult(turns=run.turns or 0, report={"status": "unreported", "summary": (
                    f"The builder session reached its limit before it reported ({error[:200]}), so there is no "
                    "builder summary. This change is the working tree as that session left it; the Factory checks "
                    "and verifies it as it stood.")})
            self.builder_report = result.report
            self.evidence("builder_report", self.builder_report)
            if result.report.get("status") == "blocked":
                reason = result.report.get("blocked_reason") or result.report.get("summary", "")
                self.evidence("attempts", {"n": attempt, "result": "builder_blocked", "reason": reason[:1000]})
                self.update(attempt=attempt - 1)  # a question is not a failed attempt
                self.block("question", f"{self.task.key} needs a decision", reason, status="needs_input",
                           options=[{"value": "answer", "label": "Answer and continue"},
                                    {"value": "abandon", "label": "Abandon task"}])

            # candidate gate
            self.stage("checks", "Checking the candidate change")
            try:
                patch = await extract_patch(self.builder, self.task.base_sha)
            except SandboxError as exc:
                self.feedback.append(f"- Could not read your changes: {exc}")
                continue
            problem = await self._policy_problem(patch)
            if problem:
                self.feedback.append(problem)
                self.evidence("attempts", {"n": attempt, "result": "policy", "detail": problem})
                self.event("candidate_rejected", problem, level="warn")
                continue
            self.patch = patch
            diff_id = self.artifact("diff", f"Attempt {attempt} diff", patch.diff.decode("utf-8", "replace"), run_id=run.id,
                          files=patch.files, additions=patch.additions, deletions=patch.deletions)
            commands = self.cfg.get("checks") or []
            async with self._check_slot(self.builder, bool(commands)):
                self.checks = await self._run_commands(self.builder, commands, "check")
            failed = [c for c in self.checks if not c["ok"]]
            if failed:
                try:
                    preexisting = await self._failing_on_base(token, failed)
                except (SandboxError, GitError, OSError) as exc:  # the base's sandbox, not the attempt
                    error = f"Sandbox setup failed: {exc}"
                    self._infrastructure(attempt, "checks", error, infrastructure_problem(error))
                if preexisting:
                    self.evidence("attempts", {"n": attempt, "result": "checks_preexisting",
                                               "checks": _brief(self.checks)})
                    self.event("checks_preexisting", f"{len(failed)} failing check(s) fail the same way on the base "
                               f"commit {self.task.base_sha[:8]}: not caused by this change and not counted against "
                               "the attempt; verification continues", level="warn",
                               commands=[c["command"] for c in failed])
                    failed = []
            if failed:
                self.feedback += [f"- Check `{c['command']}` failed (exit {c['exit_code']}){c.get('vs_base', '')}:\n"
                                  f"```\n{c['tail']}\n```" for c in failed]
                self.evidence("attempts", {"n": attempt, "result": "checks_failed", "checks": _brief(self.checks)})
                continue

            # independent verification; nothing the builder left running (databases, dev servers) may compete
            # with the verifier for processes or ports.
            await self.builder.kill_all()
            self.stage("verify", f"Independent verification by a fresh {self.task.verifier_harness} session")
            verdict = await self._verify(token, patch, attempt)
            if verdict is None:
                continue
            self.evidence("attempts", {"n": attempt, "result": "verified", "checks": _brief(self.checks),
                                       "verdict": verdict, "app_run": self.app_summary,
                                       "configured_checks": list(self.cfg.get("checks") or []),
                                       "diff_artifact_id": diff_id,
                                       # A restart before publication reloads exactly this candidate from the diff.
                                       "patch_sha256": hashlib.sha256(patch.diff).hexdigest()})
            break

        await self._publish(token)
        await self.ci_then_merge()

    async def _prepare_builder(self, token: str, from_branch: bool) -> None:
        self.builder = LocalSandbox(f"task-{self.task_id[:12]}-b{self.task.attempt}").create()
        branch = self.task.branch or f"factory/{self.outcome.number}-{self.task.key.lower()}-{self.task_id[:6]}"
        self.stage("prepare", f"Preparing a fresh checkout of {self.repo.full_name}")
        if from_branch:
            await checkout(self.builder, self.repo.full_name, token, f"refs/heads/{branch}", branch)
            await self._bring_branch_up_to_date(branch)
        else:
            base_sha = await checkout(self.builder, self.repo.full_name, token, self.repo.default_branch, branch)
            self.update(base_sha=base_sha, branch=branch)
        self.builder = await self._isolate_builder(self.builder)
        if getattr(self.builder, "prepared", None):
            self.event("setup_skipped", "Dependencies unchanged since the prepared environment was saved; setup skipped",
                       level="success", environment=self.builder.prepared)
            setup = []
        else:
            setup = await self._run_commands(self.builder, self.cfg.get("setup") or [], "setup", timeout=1200)
            key = getattr(self.builder, "environment_key", None)
            if key and setup and all(c["ok"] for c in setup):
                await self._save_environment(self.builder, key)
        broken = [c for c in setup if not c["ok"]]
        if broken:
            self.feedback.append("- Note: repository setup failed before you started:\n" + "\n".join(
                f"`{c['command']}` → exit {c['exit_code']}\n```\n{c['tail'][-1500:]}\n```" for c in broken))

    async def _bring_branch_up_to_date(self, branch: str) -> None:
        """Revise on top of today's base branch. GitHub runs no pull-request checks while a branch conflicts, so a
        revision pushed from a stale base would wait for CI forever. A clean rebase keeps the earlier work. When it
        conflicts, the earlier change is carried onto the latest base with a three-way apply: everything that merges
        cleanly is kept, and only the lines both sides changed are left with conflict markers for the builder to
        resolve, instead of re-implementing reviewed work from scratch."""
        sb, default = self.builder, self.repo.default_branch
        found = await sb.exec(f"git rev-parse --verify -q origin/{shlex.quote(default)}^{{commit}}", timeout=60)
        main = found.output.strip() if found.ok else ""
        if not main:
            return
        recorded = self.task.base_sha or ""
        record = (self.task.evidence or {}).get("published") or {}
        tip = await sb.exec("git rev-parse HEAD", timeout=60)
        # A publication record describes one pushed head. A stop between a push and its record leaves the record
        # describing the publication before it, so its base counts only while its head is still the branch's head.
        published = str(record.get("base") or "") if tip.ok and record.get("head") == tip.output.strip() else ""
        # The base recorded when the branch was last published is the branch's real base, so it is trusted first.
        # The task's recorded base can belong to an attempt that was never published: a restart begins the attempt
        # again from the latest code and records that base, while the branch still sits on another one. Trusting
        # it would skip this rebase when it is newer than the branch, and when it is an older commit the branch
        # also contains, every commit between the two would count as this task's change (republishing commits
        # removed from a rewound main, for one). Branches published before the base was recorded fall back to the
        # recorded base when the branch contains it, then to where the branch meets it (a later commit of the same
        # history), then main.
        actual = ""
        for known in (published, recorded):
            if known and not actual and (await sb.exec(
                    f"git merge-base --is-ancestor {shlex.quote(known)} HEAD", timeout=60)).ok:
                actual = known
        for other in (recorded, main):
            if actual or not other:
                continue
            merged = await sb.exec(f"git merge-base HEAD {shlex.quote(other)}", timeout=60)
            actual = merged.output.strip() if merged.ok else ""
        if not actual:
            return
        if actual != recorded:
            self.event("branch_base_corrected", f"{branch} sits on {actual[:8]}, not the recorded base "
                       f"{recorded[:8] or '(none)'}; bringing it up to date from there", level="warn",
                       recorded=recorded, actual=actual)
            self.update(base_sha=actual)
        if main == self.task.base_sha:
            return
        if (self.task.evidence or {}).get("ci", {}).get("state") == "failure":
            self.event("revalidate", "The base branch moved; rebasing the CI repair before verification")
        author = " ".join(f"-c user.{k}={shlex.quote(v)}" for k, v in
                          (("name", AUTHOR["GIT_AUTHOR_NAME"]), ("email", AUTHOR["GIT_AUTHOR_EMAIL"])))
        # --onto: replay only this task's commits, even if the default branch was rewritten since.
        rebased = await sb.exec(f"git {author} rebase -q --onto {main} {shlex.quote(self.task.base_sha)}", timeout=300)
        if rebased.ok:
            self.update(base_sha=main)
            self.event("rebased", f"Brought {branch} up to date with {default} ({main[:8]}) before revising")
            return
        await sb.exec("git rebase --abort", timeout=60)
        base = shlex.quote(self.task.base_sha)
        earlier = (await sb.exec("git rev-parse HEAD", timeout=60)).output.strip()
        reset = await sb.exec(f"git reset -q --hard {main}", timeout=120)
        if not reset.ok:
            raise SandboxError(f"Could not restart {branch} from {default}: {reset.output[-400:]}")
        self.update(base_sha=main)
        carried = await sb.exec(f"git diff --binary --full-index {base} {shlex.quote(earlier)} | "
                                "git apply --3way --whitespace=nowarn", timeout=300) if earlier else None
        unmerged = await sb.exec("git diff --name-only --diff-filter=U", timeout=60)
        conflicted = sorted(set(unmerged.output.splitlines())) if unmerged.ok else []
        # A conflict git cannot mark up in text (binary files) would be silently settled in main's favour by the
        # next `git add`: such a change is not carried over.
        marked = await sb.exec("grep -l -E '^<{7} ' -- " + " ".join(shlex.quote(f) for f in conflicted),
                               timeout=60) if conflicted else None
        unmarked = [f for f in conflicted if f not in set(marked.output.splitlines() if marked else [])]
        if carried is None or (not carried.ok and not conflicted) or unmarked:
            await sb.exec(f"git reset -q --hard {main}", timeout=120)
            why = f" ({', '.join(unmarked)} cannot be merged line by line)" if unmarked else ""
            self.feedback.append(f"- Your previous branch conflicts with newer commits on {default} and could not be "
                                 f"carried over{why}, so this revision starts from the latest {default}. Re-implement "
                                 "the change on top of it; all earlier feedback still applies.")
            self.event("rebuild_on_main", f"{branch} conflicts with {default}; the revision restarts from the latest "
                       "code", level="warn")
            return
        if conflicted:
            self.feedback.append(
                f"- Newer commits on {default} changed the same lines as your earlier change. This revision starts from "
                f"the latest {default} with your earlier change already re-applied; these files contain conflict markers "
                f"where both sides changed the same lines: {', '.join(conflicted)}. In each conflict the `ours` side is "
                f"the latest {default} and the `theirs` side is your earlier change. Resolve every conflict so both "
                "intents survive (never drop the newer main changes), remove all `<<<<<<<`, `=======` and `>>>>>>>` "
                "lines, then address the remaining feedback.")
            self.event("rebase_conflicts", f"Carried {branch} onto {default} ({main[:8]}); the builder resolves "
                       f"conflicts in {', '.join(conflicted)[:300]}", level="warn", files=conflicted)
        else:
            self.event("rebased", f"Carried {branch} onto {default} ({main[:8]}) before revising")

    async def _policy_problem(self, patch: Patch) -> str:
        if patch.empty:
            return "- No changes were made to the repository. Implement the task (write the code), then finish."
        with session_scope() as s:
            guidance = outcome_guidance(s, self.outcome.workspace_id, self.outcome.id)
        if guidance and guidance.get("template") == README_TEMPLATE:
            # Fail closed: one root README only, never a nested README or another repository.
            rejection = "- The guided README example may change only the existing root README in the selected repository (README.md if none exists)."
            if self.task.repo_id != guidance.get("repo_id") or self.repo.id != guidance.get("repo_id"):
                return rejection
            try:
                # Read the trusted mirror at the candidate's base, not the builder's
                # working tree: deleting/renaming a README must not expand scope.
                tree = await git("ls-tree", "--name-only", "-z", self.task.base_sha,
                                 cwd=_mirror_path(self.repo.full_name))
            except GitError:
                return rejection + " Could not establish the base repository's README."
            readmes = [name for name in tree.split("\0")
                       if re.fullmatch(r"README(?:\.(?:md|markdown|mdown|rst|txt|adoc))?", name, re.IGNORECASE)]
            allowed = readmes or ["README.md"]
            if len(patch.files) != 1 or patch.files[0] not in allowed:
                return rejection
        protected = list(self.cfg.get("protected_paths") or []) + DEFAULT_PROTECTED
        bad = protected_violations(patch.files, protected)
        if bad:
            return f"- You modified protected paths {bad}. Revert those files completely; they must not change."
        marked = conflict_marker_files(patch)
        if marked:
            return (f"- Unresolved merge conflict markers remain in {marked}. Resolve each conflict so both sides' "
                    "intent survives and remove every `<<<<<<<`, `=======` and `>>>>>>>` line.")
        limit = int(self.cfg.get("max_changed_lines") or self.settings["limits"].get("max_changed_lines", 1500))
        if patch.changed_lines > limit:
            return (f"- The change is {patch.changed_lines} lines, over the {limit}-line limit. Remove unrelated or "
                    "generated changes (lockfile churn, formatting, build output) and keep the change focused.")
        if len(patch.diff) > 5_000_000:
            return "- The diff is larger than 5 MB. Remove generated or binary files from the change."
        return ""

    @asynccontextmanager
    async def _check_slot(self, sandbox, needed: bool = True):
        """Hold a host check slot while the repository's checks run in a local sandbox (see check_slots), saying so
        when every slot is taken. Waiting spends none of a command's timeout. Checks in a VM run off this host and
        take no slot."""
        if not needed or getattr(sandbox, "kind", "local") != "local":
            yield
            return
        started = time.monotonic()
        waited = False

        def waiting(running: int, queued: int) -> None:
            nonlocal waited
            waited = True
            self.event("check_slot_wait", f"Waiting for a check slot: {running} running on this server, {queued} waiting",
                       running=running, waiting=queued)

        async with check_slot(on_wait=waiting):
            if waited:
                seconds = round(time.monotonic() - started, 1)
                self.event("check_slot", f"Got a check slot after waiting {seconds:.0f}s", waited=seconds)
            yield

    async def _run_commands(self, sandbox: LocalSandbox, commands: list[str], kind: str,
                            timeout: float = 1500) -> list[dict[str, Any]]:
        results = []
        for command in commands:
            started = time.monotonic()
            result = await sandbox.exec(command, timeout=timeout)
            row = {"command": command, "ok": result.ok, "exit_code": result.exit_code,
                   "duration": round(time.monotonic() - started, 1), "timed_out": result.timed_out,
                   "tail": result.output[-4000:]}
            if not result.ok:
                row["failures"] = _failing_tests(result.output)
                row["signature"] = _output_signature(result.output, sandbox)
            results.append(row)
            self.event(f"{kind}_result", f"{'✓' if result.ok else '✗'} {command}",
                       level="success" if result.ok else "warn", command=command, exit_code=result.exit_code,
                       duration=row["duration"])
            self.artifact(kind, command, result.output, ok=result.ok, exit_code=result.exit_code)
        return results

    async def _verify(self, token: str, patch: Patch, attempt: int) -> dict[str, Any] | None:
        qa_cfg = self.cfg.get("qa") or {}
        error = ""
        for tries in range(2):
            sandbox = None
            app: AppRun | None = None
            result = None
            try:
                try:
                    sandbox = await self._verifier_sandbox(token, patch, attempt, tries, qa_cfg)
                    await self._prepare(sandbox)
                except (SandboxError, GitError, OSError) as exc:  # the environment, not the candidate
                    error = f"Sandbox setup failed: {exc}"
                else:
                    brief = ""
                    if qa_cfg.get("start"):
                        app = await self._start_app(sandbox, qa_cfg)
                        brief = verifier_brief(app)
                    sections = await self._sections(verifier=True)
                    run, result, error = await run_agent(SessionSpec(
                        workspace_id=self.ws, outcome_id=self.task.outcome_id, task_id=self.task_id, role="verifier",
                        harness=self.task.verifier_harness, system=verifier_system(self.task.base_sha), sandbox=sandbox,
                        attempt=attempt, writable=False, effective_settings=self.settings,
                        repositories={self.repo.full_name: self.cfg},
                        prompt=verify_prompt(self.task, self.repo, self.task.base_sha, self.builder_report, self.checks,
                                             precedents(self.ws, self.repo.id),
                                             guidance_history(self.task.outcome_id, self.task_id),
                                             outcome=self.outcome, sections=sections)
                        + brief), self.cancelled)
                    if app is not None:
                        await self._store_app_evidence(app, attempt, run.id)
            finally:
                if app is not None:
                    app.stop()
                if sandbox is not None:
                    await sandbox.close()
            if result is None:
                if infrastructure_problem(error) == "provider":
                    self._infrastructure(attempt, "verify", error, "provider")
                self.event("verification_unavailable", f"Verifier session failed: {error[:300]}", level="warn")
                continue
            report = result.report
            runtime_problem = self._runtime_problem(app, qa_cfg)
            if runtime_problem:
                # Recorded as a finding whatever the verifier said, so later attempts (and the automatic retry) see
                # the concrete failure; a passing verdict is overridden.
                report = {**report, "findings": [*(report.get("findings") or []), {
                    "severity": "major", "description": f"Required runtime QA failed: {runtime_problem}",
                    "location": "runtime QA"}]}
                if report["verdict"] == "pass":
                    report = {**report, "verdict": "fail",
                              "summary": f"Runtime QA failed: {runtime_problem}. " + report.get("summary", "")}
            self.verifier_report = report
            self.artifact("report", f"Verification (attempt {attempt})", json.dumps(report, indent=2), run_id=run.id,
                          verdict=report.get("verdict"))
            if report["verdict"] == "pass":
                if (self.task.evidence or {}).get("infra_retries"):
                    # Verification completed, which ends a run of infrastructure retries. The build loop's "verified"
                    # record would do it too, but a revalidation that passes writes no attempt record.
                    self._set_evidence({"infra_retries": 0, "infra_retry_at": None, "infra_phase": None})
                self.event("verified", f"Independent verification passed: {report.get('summary', '')[:300]}",
                           level="success")
                return report
            failing = [c for c in _objects(report.get("criteria")) if c.get("status") != "pass"]
            findings = [f for f in _objects(report.get("findings")) if f.get("severity") in ("blocker", "major")]
            self.feedback.append("- Independent review FAILED. " + report.get("summary", "")[:1500])
            self.feedback += [f"  - Criterion not met: {c.get('criterion')} — {str(c.get('evidence', ''))[:600]}"
                              for c in failing]
            self.feedback += [f"  - {f['severity']}: {str(f.get('description', ''))[:600]} {f.get('location', '')}"
                              for f in findings]
            if runtime_problem and app is not None and app.evidence.get("log_tail"):
                self.feedback.append(f"  - App log when it failed:\n```\n{app.evidence['log_tail'][-1500:]}\n```")
            self.evidence("attempts", {"n": attempt, "result": "verification_failed", "verdict": report,
                                       "checks": _brief(self.checks), "app_run": self.app_summary})
            self.event("verification_failed", report.get("summary", "")[:400], level="warn")
            return None
        if infrastructure_problem(error) == "transient":
            self._infrastructure(attempt, "verify", error, "transient")
        self.block("unblock", f"{self.task.key}: independent verification could not run",
                   "Two verifier sessions failed to produce a verdict. The candidate remains unverified and was not "
                   "published.")
        return None

    async def _verifier_sandbox(self, token: str, patch: Patch, attempt: int, tries: int, qa_cfg: dict[str, Any]):
        """A fresh checkout with the candidate applied: in a Railway VM when QA asks for VM isolation."""
        local = LocalSandbox(f"task-{self.task_id[:12]}-v{attempt}-{tries}").create()
        try:
            await checkout(local, self.repo.full_name, token, self.task.base_sha, f"verify-{self.task_id[:8]}")
            await apply_in_sandbox(local, patch)
        except BaseException:
            await local.close()
            raise
        wants_vm = (qa_cfg.get("isolation") or (self.settings.get("qa") or {}).get("isolation")) == "vm"
        if not wants_vm:
            return local
        from ..vm import RailwayVMSandbox, vm_available

        ok, why = vm_available()
        if not ok:
            self.event("qa_vm_unavailable", f"VM isolation requested but unavailable ({why}); verifying in a local sandbox",
                       level="warn")
            return local
        from ..vm import environment_key

        key = environment_key(local.work, self.repo.full_name, list(self.cfg.get("setup") or [])) \
            if qa_cfg.get("prepared", True) and self.cfg.get("setup") else None
        try:
            self.event("qa_vm_starting", "Starting an isolated VM for verification and runtime QA")
            vm = await RailwayVMSandbox.from_local(local, prepared=key, cpu=float(qa_cfg.get("cpu", 4)),
                                                   memory_gb=float(qa_cfg.get("memory_gb", 8)))
        except Exception as exc:
            self.event("qa_vm_unavailable", f"Could not start a QA VM ({str(exc)[:300]}); verifying in a local sandbox",
                       level="warn")
            return local
        await local.close()
        vm.environment_key = key
        self.event("qa_vm_ready", f"Verification VM {vm.vm_id} ready" + (f" in {vm.region}" if vm.region else "")
                   + (" from the prepared environment" if vm.prepared else ""), level="success", vm=vm.vm_id,
                   prepared=vm.prepared)
        return vm

    def _budget_attempts(self) -> list[dict[str, Any]]:
        """Attempt records of the current budget. Numbers restart at 1 when a person retries or requests changes;
        records from earlier budgets are kept for their findings but are not this budget's attempts. Records that do
        not end an attempt (``NOT_ATTEMPTS``) are not attempts either."""
        ev = self.task.evidence or {}
        epoch = int(ev.get("budget_epoch", 0))
        return [a for a in ev.get("attempts", []) if isinstance(a, dict) and int(a.get("epoch", 0)) == epoch
                and a.get("result") not in NOT_ATTEMPTS]

    def _infrastructure(self, attempt: int, phase: str, error: str, cause: str) -> None:
        """The model provider or the sandbox failed, not the attempt, so it is not charged. Billing, quota and access
        problems (``cause`` "provider") wait for a person. Transient ones requeue the same attempt after a backoff;
        once ``INFRA_RETRIES`` consecutive retries have failed as well, a person decides."""
        record = {"n": attempt, "result": "infra", "cause": cause or "transient", "phase": phase, "error": error[:500]}
        ev = self.task.evidence or {}
        retries = int(ev.get("infra_retries", 0))
        # The run of retries ends only when the furthest phase that failed in it completes. A later failure in an
        # earlier phase must not replace it: the next build's records would then end a run the verifier started,
        # and a verifier that keeps failing would be retried without limit.
        unresolved = max((p for p in (ev.get("infra_phase"), phase) if p in _PHASE_ORDER), key=_PHASE_ORDER.get)
        # The build loop redoes the attempt that failed, so its number is handed back. A revalidation after the base
        # moved verifies a candidate whose attempt has ended, and run() resumes that revalidation: the number stays.
        revalidating = (ev.get("revalidation") or {}).get("phase") not in (None, "complete", "handled")
        resume_at = attempt if revalidating else attempt - 1
        if cause == "provider" or retries >= INFRA_RETRIES:
            # Cleared with the decision: a person's retry runs at once and starts a new series of automatic retries.
            self._set_evidence({"attempts": record, "infra_retries": 0, "infra_retry_at": None, "infra_phase": None})
            self.update(attempt=resume_at)
            if cause == "provider":
                what = "model provider" if phase in ("build", "verify") else "access"
                self.block("unblock", f"{self.task.key}: {what} problem"
                           + (" during verification" if phase == "verify" else ""), error,
                           options=[{"value": "retry", "label": "Retry"}, {"value": "abandon", "label": "Abandon"}])
            self.block("unblock", f"{self.task.key}: infrastructure problem",
                       f"{_PHASES[phase]} failed {retries + 1} times in a row for reasons outside the change (the "
                       "model provider or the sandbox was unavailable). None of these counted as an attempt, and "
                       f"retrying keeps the task's attempt budget. Last error:\n\n{error[:1500]}",
                       options=[{"value": "retry", "label": "Retry"}, {"value": "abandon", "label": "Abandon task"}])
        delay = INFRA_BACKOFF_SECONDS[min(retries, len(INFRA_BACKOFF_SECONDS) - 1)]
        self._set_evidence({"attempts": record, "infra_retries": retries + 1, "infra_phase": unresolved,
                            "infra_retry_at": (utcnow() + timedelta(seconds=delay)).isoformat()})
        self.event("infra_retry", f"Attempt {attempt}: {_PHASES[phase].lower()} hit an infrastructure problem "
                   f"({error[:200]}). Not counted as an attempt; retrying in {delay // 60} min "
                   f"({retries + 1} of {INFRA_RETRIES})", level="warn", phase=phase, delay=delay)
        self.update(attempt=resume_at, status="queued", lease_owner="")
        raise Stop()

    async def _failing_on_base(self, token: str, failed: list[dict[str, Any]]) -> bool:
        """Whether every failing check also fails on a clean checkout of the base commit, with no failure of the
        candidate's own: then the failures predate the change. Each failed check is annotated for the builder's
        feedback (``vs_base``) and for the verifier and the pull request (``preexisting``)."""
        base = await self._base_results(token, [c["command"] for c in failed])
        if base is None:
            return False
        for check in failed:
            theirs = base.get(check["command"]) or {}
            if _same_failure(check, theirs):
                check["preexisting"] = True
                check["vs_base"] = " (it fails the same way on the base commit, so your change did not cause it)"
            elif theirs.get("ok"):
                check["vs_base"] = " (it passes on the base commit)"
            elif check.get("failures") and theirs.get("failures"):
                new = sorted(set(check["failures"]) - set(theirs["failures"]))
                if new:
                    check["vs_base"] = (f" (failing here but not on the base commit: {', '.join(new[:20])}"
                                        + (f" and {len(new) - 20} more" if len(new) > 20 else "") + ")")
            elif theirs and not theirs.get("ok", True):
                check["vs_base"] = (" (it fails on the base commit too, but not with the same output, so it counts "
                                    "as a failure of your change)")
        return all(c.get("preexisting") for c in failed)

    def _base_key(self) -> tuple[str, ...]:
        """Base-check results depend on how the repository is set up and isolated, which differs between workspaces
        and can change: workspace and configuration are part of the key, so a foreign or stale base failure is
        never reused to excuse a candidate."""
        config = hashlib.sha256(json.dumps({k: self.cfg.get(k) for k in ("setup", "checks", "build", "qa", "env")},
                                           sort_keys=True, default=str).encode()).hexdigest()[:16]
        return (self.ws, self.repo.full_name, self.task.base_sha, config)

    async def _base_results(self, token: str, commands: list[str]) -> dict[str, dict[str, Any]] | None:
        """Results of ``commands`` on a clean checkout of the base commit in a fresh sandbox, set up and run the way
        checks normally run. Kept in memory per (repository, base commit, command), so tasks on the same base do not
        run them again. None when setup failed on the base: its results would prove nothing."""
        key = self._base_key()
        found = {c: _BASE_CHECKS[(*key, c)] for c in commands if (*key, c) in _BASE_CHECKS}
        missing = [c for c in dict.fromkeys(commands) if c not in found]
        if not missing:
            return found
        self.event("base_checks", f"Running {len(missing)} failing check(s) on the base commit "
                   f"{self.task.base_sha[:8]} to see whether they failed before this change", commands=missing)
        sandbox = LocalSandbox(f"task-{self.task_id[:12]}-base{self.task.attempt}").create()
        try:
            await checkout(sandbox, self.repo.full_name, token, self.task.base_sha, f"base-{self.task_id[:8]}")
            sandbox = await self._isolate_builder(sandbox, "checking the base commit")
            async with self._check_slot(sandbox):  # a second run of the suite on this host, like the candidate's
                setup = await self._prepare(sandbox, "base_setup")
                if any(not r["ok"] for r in setup):
                    self.event("base_checks_unavailable", "Setup failed on the base commit, so its check results "
                               "prove nothing; the failing checks count against this attempt", level="warn")
                    return None
                results = await self._run_commands(sandbox, missing, "base_check")
        finally:
            await sandbox.close()
        for row in results:
            found[row["command"]] = _BASE_CHECKS[(*key, row["command"])] = row
        while len(_BASE_CHECKS) > 2000:
            _BASE_CHECKS.pop(next(iter(_BASE_CHECKS)))
        return found

    def _unpublished_candidate(self) -> dict[str, Any]:
        """The current attempt's verified record when the task stopped between independent verification and
        publication: still in those stages, with nothing recorded since. Otherwise {} (a published candidate that
        failed CI is followed by the repair's records, and its number is an earlier attempt's)."""
        ev = self.task.evidence or {}
        epoch = int(ev.get("budget_epoch", 0))
        records = [a for a in ev.get("attempts", []) if isinstance(a, dict) and int(a.get("epoch", 0)) == epoch]
        last = records[-1] if records else {}
        if (self.task.stage in ("verify", "publish") and last.get("result") == "verified"
                and last.get("n") == self.task.attempt):
            return last
        return {}

    def _restore_verified_candidate(self) -> bool:
        """After a restart between independent verification and publication, reload the candidate exactly as it was
        verified, so it is published rather than rebuilt in a new, charged attempt. False when it cannot be reloaded
        byte for byte (a diff stored only in part); the attempt is then redone, uncharged (see deliver())."""
        last = self._unpublished_candidate()
        if not last.get("patch_sha256") or not last.get("diff_artifact_id"):
            return False
        with session_scope() as s:
            diff = s.get(Artifact, last["diff_artifact_id"])
            if diff is None:
                return False
            builder = s.get(Run, diff.run_id) if diff.run_id else None
            raw, data = (diff.content or "").encode(), dict(diff.data or {})
            # A builder stopped by its cap has no result on its run: the stand-in for its report is kept with the
            # task, like every builder report, and no builder has run since this candidate was verified.
            report = dict((builder.result if builder else None) or (self.task.evidence or {}).get("builder_report")
                          or {})
        if hashlib.sha256(raw).hexdigest() != last["patch_sha256"]:
            return False
        self.patch = Patch(diff=raw, files=list(data.get("files") or []), additions=int(data.get("additions") or 0),
                           deletions=int(data.get("deletions") or 0))
        self.builder_report, self.verifier_report = report, last.get("verdict") or {}
        self.checks, self.app_summary = list(last.get("checks") or []), last.get("app_run")
        self.event("publish_resumed", f"Attempt {self.task.attempt} was verified before the restart; publishing that "
                   "candidate")
        return True

    def _auto_retry(self) -> bool:
        """Attempts ran out. When the last one failed on concrete, actionable feedback, start a fresh budget with that
        feedback instead of asking a person, up to ``auto_retry_rounds`` times between human decisions."""
        ev = self.task.evidence or {}
        rounds = max(0, min(int(self.settings.get("auto_retry_rounds", 1) or 0), 3))  # bootstrap values skip the API clamp
        used = int(ev.get("auto_retries", 0))
        attempts = self._budget_attempts()
        # Only the attempt that just ran out counts: some failures (an unreadable sandbox) record nothing, and an
        # older record must not turn an infrastructure failure into an automatic retry.
        last = attempts[-1] if attempts and attempts[-1].get("n") == self.task.attempt else {}
        if used >= rounds or not _actionable(last):
            return False
        # Counter and fresh budget together: a crash in between must not spend the round without granting it.
        self._set_evidence({"auto_retries": used + 1, "attempt_base": self.task.attempt})
        self.event("auto_retry", f"{self.task.key} used its attempts on concrete findings; retrying with a fresh budget "
                   f"before asking a person ({used + 1} of {rounds})", level="warn")
        return True

    def _fresh_budget_after_base_moved(self, why: str) -> None:
        """A verified change that other work overtook is not the builder's failure: rebuilding it gets a fresh
        attempt budget, at most twice per task, after which a person decides."""
        rebuilds = int((self.task.evidence or {}).get("rebase_rebuilds", 0)) + 1
        if rebuilds > 2:
            self.block("unblock", f"{self.task.key} keeps conflicting with newer commits",
                       f"The verified change {why} on {self.repo.default_branch} again, after {rebuilds - 1} rebuilds.",
                       status="failed", options=[{"value": "retry", "label": "Rebuild again"},
                                                 {"value": "take_over", "label": "I'll take it from here"},
                                                 {"value": "abandon", "label": "Abandon task"}])
        values = {"rebase_rebuilds": rebuilds, "attempt_base": self.task.attempt}
        revalidation = (self.task.evidence or {}).get("revalidation") or {}
        if revalidation.get("phase") == "rejected":
            values["revalidation"] = {**revalidation, "phase": "handled"}
        self._set_evidence(values)
        self.event("rebuild_after_base_moved", f"The verified change {why}; rebuilding with a fresh attempt budget "
                   f"({rebuilds} of 2)")

    async def _isolate_builder(self, local: LocalSandbox, doing: str = "building"):
        """With ``build.isolation: "vm"`` the builder works in its own Railway VM (checkout uploaded, no Factory
        credentials inside); otherwise, or when VMs are unavailable, in the local per-job sandbox. Checks of the base
        commit run in the same kind of sandbox."""
        build_cfg = self.cfg.get("build") or {}
        if build_cfg.get("isolation") != "vm":
            return local
        from ..vm import RailwayVMSandbox, environment_key, vm_available

        ok, why = vm_available()
        if not ok:
            self.event("build_vm_unavailable", f"VM isolation requested but unavailable ({why}); {doing} locally",
                       level="warn")
            return local
        key = environment_key(local.work, self.repo.full_name, list(self.cfg.get("setup") or [])) \
            if self.cfg.get("setup") else None
        try:
            vm = await RailwayVMSandbox.from_local(local, prepared=key, cpu=float(build_cfg.get("cpu", 4)),
                                                   memory_gb=float(build_cfg.get("memory_gb", 8)))
        except Exception as exc:
            self.event("build_vm_unavailable", f"Could not start a build VM ({str(exc)[:300]}); {doing} locally",
                       level="warn")
            return local
        await local.close()
        vm.environment_key = key
        self.event("build_vm_ready", f"{doing.capitalize()} in VM {vm.vm_id}"
                   + (" from the prepared environment" if vm.prepared else ""),
                   level="success", vm=vm.vm_id, prepared=vm.prepared)
        return vm

    async def _save_environment(self, sandbox, key: str) -> None:
        started = time.monotonic()
        try:
            await sandbox.checkpoint(key)
            self.event("environment_prepared", f"Saved a prepared environment for these dependencies "
                       f"({time.monotonic() - started:.0f}s)", environment=key)
            from ..vm import prune_environments

            pruned = await prune_environments(key)
            if pruned:
                self.event("environments_pruned", f"Removed {len(pruned)} older prepared environment(s)",
                           environments=pruned)
        except Exception as exc:  # an optimisation only
            self.event("environment_prepare_failed", f"Could not save a prepared environment: {str(exc)[:200]}",
                       level="warn")

    async def _prepare(self, sandbox, kind: str = "verify_setup") -> list[dict[str, Any]]:
        """Install dependencies, unless the VM booted from a prepared environment for these exact lockfiles.
        After a clean install in a VM, save it so the next verification starts ready. Returns the setup results."""
        setup = self.cfg.get("setup") or []
        if getattr(sandbox, "prepared", None):
            self.event(f"{kind}_skipped", "Dependencies unchanged since the prepared environment was saved; "
                       "setup skipped", level="success", environment=sandbox.prepared)
            return []
        results = await self._run_commands(sandbox, setup, kind, timeout=1200)
        key = getattr(sandbox, "environment_key", None)
        if key and results and all(r["ok"] for r in results):
            await self._save_environment(sandbox, key)
        return results

    async def _start_app(self, sandbox, qa_cfg: dict[str, Any]) -> AppRun:
        """Boot the candidate in the verifier's sandbox, then let the Factory capture its own evidence first."""
        app = AppRun(sandbox, qa_cfg)
        self.event("qa_app_starting", f"Starting the app for runtime QA: {qa_cfg['start'][:200]}")
        ready = await app.start()
        if ready:
            checks, extra_pages = change_checks(self.builder_report)
            pages = list(dict.fromkeys(list(qa_cfg.get("pages") or []) + extra_pages))
            app.evidence["pages"] = await app.screenshots(pages) if pages else []
            app.evidence["probes"] = await app.probes()
            app.evidence["change_checks"] = await app.probes(checks) if checks else []
            ok_pages = sum(1 for p in app.evidence["pages"] if p.get("status") and p["status"] < 400)
            ok_probes = sum(1 for p in app.evidence["probes"] if p.get("ok"))
            ok_change = sum(1 for p in app.evidence["change_checks"] if p.get("ok"))
            all_ok = (ok_pages == len(app.evidence["pages"]) and ok_probes == len(app.evidence["probes"])
                      and ok_change == len(app.evidence["change_checks"]))
            self.event("qa_app_ready", f"App ready in {app.evidence['ready_seconds']}s · pages {ok_pages}/"
                       f"{len(app.evidence['pages'])} · probes {ok_probes}/{len(app.evidence['probes'])}"
                       + (f" · change checks {ok_change}/{len(checks)}" if checks else ""),
                       level="success" if all_ok else "warn")
        else:
            tail = " ".join((app.evidence.get("log_tail") or "").strip().splitlines()[-4:])[-400:]
            self.event("qa_app_failed", f"The app did not become ready for runtime QA: {tail or 'no output'}",
                       level="warn")
        return app

    async def _store_app_evidence(self, app: AppRun, attempt: int, run_id: str) -> None:
        rows = {p.get("name"): p for p in app.evidence.get("pages", []) if isinstance(p, dict)}
        images = await app.collect_images()
        for name, image in images:
            meta = rows.get(name, {"name": name, "by": "verifier"})
            self.artifact("screenshot", f"{'Factory' if meta.get('by') == 'factory' else 'Verifier'}: {meta.get('path', name)}",
                          image, run_id=run_id, attempt=attempt, **{k: meta.get(k) for k in
                                                                    ("path", "status", "title", "console_errors", "by")})
        self.artifact("log", f"App log (attempt {attempt})", await app.log_tail(20000), run_id=run_id)
        self.app_summary = {
            "started": app.evidence.get("started"), "ready_seconds": app.evidence.get("ready_seconds"),
            "isolation": app.evidence.get("isolation"),
            "pages": [{k: p.get(k) for k in ("path", "status", "title", "error")} for p in app.evidence.get("pages", [])],
            "probes": [{k: p.get(k) for k in ("method", "path", "status", "ok")} for p in app.evidence.get("probes", [])],
            "change_checks": [{k: p.get(k) for k in ("method", "path", "status", "ok", "expect_status", "why", "error")}
                              for p in app.evidence.get("change_checks", [])],
            "screenshots": len(images), "verifier_screenshots": sum(1 for n, _ in images if n not in rows)}

    def _runtime_problem(self, app: AppRun | None, qa_cfg: dict[str, Any]) -> str:
        # Runtime QA is evidence for the verifier; it only fails a candidate on its own when marked required
        # (a broken QA environment must not sink a good change).
        if app is None or not qa_cfg.get("required"):
            return ""
        if not app.evidence.get("started"):
            return "the app did not start"
        bad_pages = [f"{p.get('path')} ({p.get('error') or p.get('status')})"[:300] for p in app.evidence.get("pages", [])
                     if p.get("error") or (p.get("status") or 500) >= 500]
        bad_probes = [p.get("path") for p in [*app.evidence.get("probes", []),
                                               *app.evidence.get("change_checks", [])] if not p.get("ok")]
        if bad_pages:
            return f"pages failed to render: {bad_pages}"
        if bad_probes:
            return f"probes failed: {bad_probes}"
        return ""

    # publication ---------------------------------------------------------------
    async def _publish(self, token: str) -> None:
        assert self.patch is not None
        self.stage("publish", "Publishing the verified change")
        message = f"{self.task.title}\n\nFactory outcome #{self.outcome.number}, task {self.task.key}."
        try:
            head = await publish(self.repo.full_name, token, base_sha=self.task.base_sha, branch=self.task.branch,
                                 patch=self.patch, message=message, task_id=self.task_id)
        except GitError as exc:
            self.block("unblock", f"{self.task.key}: could not push the branch", str(exc))
        gh = GitHub(token)
        body = _factory_block(self._pr_body())
        try:
            if self.task.pr_number:
                pr = await gh.pull(self.repo.full_name, self.task.pr_number)
                try:
                    updated = _replace_factory_block(pr.get("body") or "", body)
                    await gh.request("PATCH", f"/repos/{self.repo.full_name}/pulls/{self.task.pr_number}",
                                     json={"body": updated})
                except (GitHubError, ValueError) as exc:
                    self.event("revalidation_warning", f"Could not update PR body: {str(exc)[:200]}", level="warn")
            else:
                pr = await gh.create_pull(self.repo.full_name, head=self.task.branch, base=self.repo.default_branch,
                                          title=f"{self.task.title}", body=body)
            try:
                await gh.set_status(self.repo.full_name, head, "success", "Independently verified by MindsHub AI Factory",
                                    self._task_url())
            except GitHubError as exc:
                self.event("revalidation_warning", f"Could not publish status: {str(exc)[:200]}", level="warn")
        except GitHubError as exc:
            self.block("unblock", f"{self.task.key}: could not open the pull request", str(exc))
        # Record the publication before advancing the head, as revalidation does: a stop between the two must not
        # leave the task at this head with a record of the publication before it.
        self.evidence("published", {"head": head, "base": self.task.base_sha, "at": utcnow().isoformat(),
                                    "pr": pr["number"]})
        self.update(head_sha=head, pr_number=pr["number"], pr_url=pr["html_url"], stage="ci")
        self.event("published", f"Opened PR #{pr['number']}", level="success", url=pr["html_url"], head=head)

    def _pr_body(self) -> str:
        lines = [self.builder_report.get("summary", "").strip() or self.task.title, "", "### Acceptance criteria"]
        by = {c.get("criterion"): c for c in _objects(self.verifier_report.get("criteria"))}
        for criterion in self.task.acceptance:
            c = by.get(criterion) or next((v for k, v in by.items() if k and criterion[:40] in k), {})
            mark = {"pass": "✅", "fail": "❌"}.get(c.get("status", ""), "◻️")
            lines.append(f"- {mark} {criterion}" + (f"  \n  <sub>{c.get('evidence', '')[:400]}</sub>" if c else ""))
        if self.app_summary:
            a = self.app_summary
            where = a.get("isolation") or "a fresh sandbox"
            if where.startswith("railway-vm"):
                vm_id, _, region = where.removeprefix("railway-vm ").partition(" ")
                where = f"an isolated Railway VM `{vm_id[:8]}`" + (f" in {region.strip('()')}" if region else "")
            lines += ["", f"### Runtime QA (app booted in {where})"]
            if a.get("started"):
                lines.append(f"- App started and became ready in {a.get('ready_seconds')}s")
                lines += [f"- {'✅' if (p.get('status') or 500) < 400 else '❌'} page `{p.get('path')}` → "
                          f"{p.get('status') or p.get('error', 'error')} {('· ' + p['title']) if p.get('title') else ''}"
                          for p in a.get("pages", [])]
                lines += [f"- {'✅' if p.get('ok') else '❌'} `{p.get('method')} {p.get('path')}` → {p.get('status')}"
                          for p in a.get("probes", [])]
                if a.get("change_checks"):
                    lines.append("- Checks for this change (proposed by the builder, run by the Factory):")
                    lines += [f"  - {'✅' if p.get('ok') else '❌'} `{p.get('method')} {p.get('path')}` → "
                              f"{p.get('status') or p.get('error')} (expected {p.get('expect_status')})"
                              + (f" — {p['why']}" if p.get("why") else "") for p in a["change_checks"]]
                lines.append(f"- {a.get('screenshots', 0)} screenshot(s) attached in MindsHub AI Factory "
                             f"({a.get('verifier_screenshots', 0)} taken by the verifier)")
            else:
                lines.append("- ❌ The app did not become ready, so there is no runtime evidence; the boot log is in"
                             " MindsHub AI Factory")
        if self.checks:
            lines += ["", "### Checks run by the Factory"]
            lines += [f"- {'✅' if c['ok'] else '❌'} `{c['command']}` ({c['duration']}s)"
                      + (" — fails the same way on the base commit; not caused by this change"
                         if c.get("preexisting") else "") for c in self.checks]
        spent, known = outcome_cost(self.task.outcome_id)
        lines += ["", "---",
                  f"Built by **{self.task.harness}**, independently verified by a fresh **{self.task.verifier_harness}** "
                  f"session. Outcome #{self.outcome.number}: {self.outcome.title}. "
                  f"Cost so far: {('$%.2f' % spent) if known else ('at least $%.2f (some runs not priced yet)' % spent) if spent else 'not priced yet'}."]
        url = self._task_url()
        if url:
            lines.append(f"[View in MindsHub AI Factory]({url})")
        issues = self._source_issues()
        if issues:
            lines.append("")
            if self._closes_issue():
                lines += [f"Closes #{n}" for n in issues]
            else:
                lines += [f"Part of #{n} (closed when the outcome's last task merges)" for n in issues]
        return "\n".join(lines)

    def _source_issues(self) -> list[str]:
        """Issues in this repository that the outcome came from."""
        import re

        found = set(re.findall(rf"github\.com/{re.escape(self.repo.full_name)}/issues/(\d+)", self.outcome.brief or ""))
        return sorted(found, key=int)

    async def _claim_issues_before_merge(self, gh: GitHub, pr: dict[str, Any]) -> None:
        """Whichever task merges last closes the source issue: other tasks may have finished since this PR opened."""
        issues = self._source_issues()
        if not issues or not self._closes_issue(merged_counts=True):
            return
        body = pr.get("body") or ""
        missing = [n for n in issues if f"Closes #{n}" not in body]
        if not missing:
            return
        import re

        body = re.sub(r"\nPart of #\d+ \(closed when the outcome's last task merges\)", "", body).rstrip()
        body += "\n\n" + "\n".join(f"Closes #{n}" for n in missing)
        try:
            await gh.request("PATCH", f"/repos/{self.repo.full_name}/pulls/{self.task.pr_number}", json={"body": body})
        except GitHubError as exc:
            self.event("issue_link_failed", f"Could not mark PR #{self.task.pr_number} as closing "
                       f"{', '.join('#' + n for n in missing)}: {str(exc)[:200]}", level="warn")

    async def _settle_issues(self) -> None:
        """After the outcome's last task: make sure its source issues closed, and say so plainly if they did not."""
        issues = self._source_issues()
        if not issues or not self._closes_issue():
            return
        gh = GitHub(resolve_token(self.ws))
        for n in issues:
            try:
                issue = await gh.request("GET", f"/repos/{self.repo.full_name}/issues/{n}")
                if issue.get("state") != "closed":
                    await asyncio.sleep(3)  # GitHub closes linked issues shortly after the merge
                    issue = await gh.request("GET", f"/repos/{self.repo.full_name}/issues/{n}")
                if issue.get("state") == "closed":
                    self.event("issue_closed", f"Issue #{n} is closed", level="success")
                    continue
                await gh.request("PATCH", f"/repos/{self.repo.full_name}/issues/{n}", json={"state": "closed"})
                self.event("issue_closed", f"Closed issue #{n}", level="success")
            except GitHubError as exc:
                reason = ("the GitHub token cannot write issues" if exc.status in (403, 404) else str(exc)[:160])
                self.event("issue_close_failed", f"Delivered, but issue #{n} is still open: {reason}. Close it on "
                           f"GitHub (it was delivered by PR #{self.task.pr_number}).", level="warn")

    def _closes_issue(self, merged_counts: bool = False) -> bool:
        """Only the last task of an outcome closes its source issue (when merging, siblings already merged and
        still smoke-testing count as done)."""
        with session_scope() as s:
            others = [t for t in s.query(Task).filter(Task.outcome_id == self.task.outcome_id, Task.id != self.task_id)
                      if t.status not in ("completed", "abandoned", "cancelled")]
            return not [t for t in others if not (merged_counts and t.merge_sha)]

    def _task_url(self) -> str:
        base = get_settings().public_url
        return f"{base}/w/{self.ws}/outcomes/{self.task.outcome_id}?task={self.task_id}" if base.startswith("https") else ""

    # CI ------------------------------------------------------------------------
    def _guard(self, status: str | None = None) -> None:
        """Revalidate after every network/lock wait, before taking another action."""
        if self.cancelled.is_set():
            raise Stop()
        with session_scope() as s:
            task = s.get(Task, self.task_id)
            if (status and task.status != status) or not transition(s, task, lease_owner=task.lease_owner):
                raise Stop()

    async def ci_then_merge(self) -> None:
        # Token resolution is inside the same guarded path as all later observations.
        candidate = (self.task.evidence or {}).get("revert_candidate")
        wait = (self.task.evidence or {}).get("ci_wait") or {}
        if candidate and wait.get("continuation") == "revert":
            await self._wait_ci(continuation="revert", head=candidate["head"], pr=candidate["pr"])
        else:
            await self._wait_ci()

    async def _wait_ci(self, gh: GitHub | None = None, *, continuation: str = "delivery",
                       head: str | None = None, pr: int | None = None) -> None:
        head, pr = head or self.task.head_sha, pr or self.task.pr_number
        now = utcnow()
        wait = (self.task.evidence or {}).get("ci_wait") or {}
        if (wait.get("head"), wait.get("pr"), wait.get("continuation")) != (head, pr, continuation):
            wait = {"head": head, "pr": pr, "continuation": continuation,
                    "first_wait_at": now.isoformat(),
                    "deadline": (now + timedelta(minutes=self.settings["limits"].get("ci_minutes", 45))).isoformat()}
        wait = {**wait, "next_poll_at": now.isoformat()}
        self.update(status="waiting_ci", stage="ci", lease_owner="",
                    evidence={**(self.task.evidence or {}), "ci_wait": wait})
        # All observations, including the first one, belong to the bounded poll
        # pool. Do not await GitHub here: Stop releases the builder sandbox and
        # execution slot before the scheduler can begin polling this candidate.
        raise Stop()

    async def poll_ci(self, gh: GitHub | None = None) -> None:
        """One bounded observation. No sleeps, sandbox, merge lock, or delivery slot."""
        try:
            await asyncio.wait_for(self._poll_ci(gh), timeout=CI_POLL_TIMEOUT_SECONDS)
        except (asyncio.TimeoutError, GitHubError) as exc:
            # Every failed observation uses the same lifecycle/deadline gate.
            # Neither a slow request nor an access error may leave an expired
            # candidate retrying forever (or reset its durable waiting budget).
            self._guard("waiting_ci")
            self._check_ci_deadline(self.task.evidence["ci_wait"])
            detail = (f"GitHub CI observation timed out after {CI_POLL_TIMEOUT_SECONDS} seconds."
                      if isinstance(exc, asyncio.TimeoutError) else str(exc))
            self.block("unblock", f"{self.task.key}: could not check CI", detail)

    def _check_ci_deadline(self, wait: dict[str, Any]) -> None:
        if utcnow() > datetime.fromisoformat(wait["deadline"]):
            wait = {**wait, "timed_out": True}
            self.update(evidence={**self.task.evidence, "ci_wait": wait})
            self.block("unblock", f"{self.task.key}: CI did not finish",
                       f"CI on PR #{wait['pr']} was still running after "
                       f"{self.settings['limits'].get('ci_minutes', 45)} minutes.",
                       options=[{"value": "retry", "label": "Keep waiting"},
                                {"value": "abandon", "label": "Abandon"}])

    async def _poll_ci(self, gh: GitHub | None) -> None:
        self._guard("waiting_ci")
        gh = gh or GitHub(resolve_token(self.ws))
        self._guard("waiting_ci")
        wait = dict(self.task.evidence["ci_wait"])
        summary = await gh.checks(self.repo.full_name, wait["head"])
        self._guard("waiting_ci")
        now = utcnow()
        elapsed = (now - datetime.fromisoformat(wait["first_wait_at"])).total_seconds()
        grace = int(self.cfg.get("ci_grace_seconds", 150))
        stage = None
        if summary.state == "success":
            stage = "revert_merge" if wait["continuation"] == "revert" else "merge"
        elif summary.state == "failure":
            if wait["continuation"] == "revert":
                self.block("unblock", f"{self.task.key}: the revert failed CI",
                           f"Revert PR #{wait['pr']} did not pass CI; the original change is still merged.",
                           status="smoke_failed", options=[{"value": "acknowledge", "label": "Acknowledge"}])
            stage = "ci_repair"
        elif summary.state == "none" and elapsed > grace:
            if self.cfg.get("require_ci", False):
                main = await gh.branch_sha(self.repo.full_name, self.repo.default_branch)
                self._guard("waiting_ci")
                if wait["continuation"] == "delivery" and main and main != self.task.base_sha:
                    wait["stale_main"] = main
                    stage = "ci_refresh"
                else:
                    self.block("unblock", f"{self.task.key}: no CI checks reported",
                               f"PR #{wait['pr']} received no CI checks within {grace}s, and this repository "
                               "requires CI before merging.")
            else:
                stage = "revert_merge" if wait["continuation"] == "revert" else "merge"
        if stage is None:
            self._check_ci_deadline(wait)
        changed = summary.state != wait.get("last_state")
        wait.update(last_state=summary.state, next_poll_at=(now + timedelta(
            seconds=15 if elapsed < 300 else 30)).isoformat())
        ci = {"state": summary.state, "head": wait["head"], "checks": summary.checks, "at": now.isoformat()}
        ev = {**self.task.evidence, "ci_wait": wait, "ci": ci}
        if summary.state == "failure":
            ev["failure_facts"] = [*ev.get("failure_facts", []), {"kind": "ci", **ci}]
        self.update(status="queued" if stage else "waiting_ci", stage=stage or "ci", evidence=ev,
                    lease_owner="", blocked_reason="")
        if changed:
            self.event("ci_status", f"CI {summary.state}", state=summary.state, checks=summary.checks)

    async def _fix_ci(self, gh: GitHub) -> None:
        summary = CheckSummary("failure", (self.task.evidence or {}).get("ci", {}).get("checks", []))
        details = []
        for check in (summary.failing if summary else [])[:3]:
            tail = await gh.job_log_tail(self.repo.full_name, check)
            details.append(f"- CI check `{check['name']}` failed ({check['url']}):\n```\n{tail[-3500:]}\n```")
        self._guard()
        self.feedback += details or ["- CI failed on the pull request (no logs available)."]
        self.event("ci_failed", f"CI failed on PR #{self.task.pr_number}; sending the failure back to the builder",
                   level="warn")
        self._ci_repairing = True  # a restart inside this round fetches the CI logs again (see run())
        await self.deliver(continue_from_branch=True)

    # merge ---------------------------------------------------------------------
    def _merge_needs_approval(self, settings: dict[str, Any] | None = None) -> bool:
        from ..onboarding_policy import active

        settings = workspace_settings(self.ws) if settings is None else settings
        if active(settings):
            return True
        merge_policy = (self.cfg.get("merge") or {}).get("mode") or settings["autonomy"].get("merge", "auto")
        release_cfg = self.cfg.get("release") or {}
        releases_on_merge = release_cfg.get("mode") == "on_merge"
        if releases_on_merge and settings["autonomy"].get("release") == "approval":
            return True
        return merge_policy != "auto"

    def _require_merge_approval(self) -> None:
        if self._merge_needs_approval() and not self._approved("approve_merge"):
            self._open_merge_approval()

    def _open_merge_approval(self) -> None:
        self.update(stage="merge")
        self.block("approve_merge", f"Approve merge: {self.task.title}",
                   f"PR #{self.task.pr_number} passed independent verification and CI at "
                   f"{self.task.head_sha[:10]}. {self.task.pr_url}",
                   status="awaiting_approval",
                   options=[{"value": "approve", "label": "Merge"}, {"value": "reject", "label": "Request changes"}])

    async def _serialized_merge(self, gh: GitHub, method: str) -> dict[str, Any]:
        # The synchronous DB lock may wait for another merge or settings writer.
        # Keep that wait off the event loop so the lock owner can finish its I/O.
        merge = asyncio.create_task(asyncio.to_thread(self._merge_under_workspace_lock, gh, method))
        try:
            result = await asyncio.shield(merge)
        except asyncio.CancelledError:
            # A thread cannot be cancelled. Keep the caller's repository lock
            # until this bounded transaction exits, even during shutdown.
            try:
                await merge
            finally:
                raise
        if result is None:
            # Open the decision only after releasing the lock: block/update use
            # their own transactions, which would otherwise deadlock on SQLite.
            self._open_merge_approval()
        return result

    def _merge_under_workspace_lock(self, gh: GitHub, method: str) -> dict[str, Any] | None:
        from ..onboarding_policy import lock_workspace
        from .runtime import merged_settings

        with session_scope() as s:
            ws = lock_workspace(s, self.ws)
            task = s.get(Task, self.task_id)
            if self.cancelled.is_set() or not transition(s, task, lease_owner=task.lease_owner):
                raise Stop()
            if self._merge_needs_approval(merged_settings(ws.settings)) and not self._approved("approve_merge"):
                return None

            async def merge():
                return await asyncio.wait_for(
                    gh.merge(self.repo.full_name, self.task.pr_number, sha=self.task.head_sha,
                             method=method, title=f"{self.task.title} (#{self.task.pr_number})"),
                    timeout=60,
                )

            try:
                # Hold the same workspace lock as onboarding/start and all
                # settings writers through the external side effect, not just
                # the policy read. The network wait is explicitly bounded.
                return asyncio.run(merge())
            except asyncio.TimeoutError as exc:
                raise GitHubError("GitHub merge timed out after 60 seconds; check the PR before retrying.") from exc

    def _approved(self, kind: str) -> bool:
        """An approval covers only the candidate that was published before it was given."""
        from datetime import datetime

        with session_scope() as s:
            row = (s.query(Decision).filter(Decision.task_id == self.task_id, Decision.kind == kind,
                                            Decision.status == "resolved").order_by(Decision.resolved_at.desc()).first())
            if not (row and row.resolution == "approve" and row.resolved_at):
                return False
            published = (self.task.evidence or {}).get("published", {}).get("at")
            return not published or row.resolved_at >= datetime.fromisoformat(published)

    async def merge(self) -> None:
        token = resolve_token(self.ws)
        gh = GitHub(token)
        if self.task.pr_number:
            current = await gh.pull(self.repo.full_name, self.task.pr_number)
            self._guard()
            if current.get("merged"):  # a person merged it on GitHub: that is the approval
                who = (current.get("merged_by") or {}).get("login") or "someone"
                self.update(merge_sha=current.get("merge_commit_sha") or "", status="running", blocked_reason="")
                self.evidence("merge", {"sha": self.task.merge_sha, "pr": self.task.pr_number, "by": who,
                                        "outside": True, "at": utcnow().isoformat()})
                self.event("merged", f"PR #{self.task.pr_number} was merged on GitHub by {who} "
                           f"({self.task.merge_sha[:8]})", level="success")
                await self.smoke()
                return
        pending = (self.task.evidence or {}).get("revalidation") or {}
        if pending.get("phase") not in (None, "complete", "handled"):
            await self._refresh_onto_main(token, gh, pending["new_base"])
            return
        self._require_merge_approval()
        # One merge turn per repository: wait in the merge queue (parked, no slot) until this task is at its head.
        await merge_manager.take_turn(self, gh)
        revalidate = (self.cfg.get("merge") or {}).get("revalidate", True)
        github_queue = await merge_manager.github_queue_state(gh, self)
        if github_queue is not None:
            if revalidate and not github_queue["entry"] and not (self.task.evidence or {}).get(
                    merge_manager.KEY, {}).get("github"):
                # Enqueue only a head verified against the current main; otherwise rebase and revalidate first.
                main_sha = await gh.branch_sha(self.repo.full_name, self.repo.default_branch)
                self._guard()
                if main_sha != self.task.base_sha:
                    await self._refresh_onto_main(token, gh, main_sha)
                    return
            await merge_manager.merge_via_github_queue(self, gh, github_queue)
        self.stage("merge", f"Merging PR #{self.task.pr_number}")
        refresh_onto: str | None = None
        # Merges into one repository are serialized; CI waits and rebuilds happen outside the lock.
        async with self.repo_lock(self.repo.id):
            self._guard()
            pr = await gh.pull(self.repo.full_name, self.task.pr_number)
            self._guard()
            if pr.get("merged"):
                self.update(merge_sha=pr.get("merge_commit_sha") or "")
            else:
                if pr.get("state") == "closed":
                    self.block("unblock", f"{self.task.key}: PR was closed", f"PR #{self.task.pr_number} was closed "
                               "outside the Factory.", status="needs_input")
                if pr.get("head", {}).get("sha") != self.task.head_sha:
                    self.block("unblock", f"{self.task.key}: PR head changed",
                               "The pull request head no longer matches the verified candidate; it was not merged.",
                               status="needs_input")
                main_sha = await gh.branch_sha(self.repo.full_name, self.repo.default_branch)
                self._guard()
                if revalidate and main_sha != self.task.base_sha:
                    refresh_onto = main_sha
                else:
                    try:
                        await self._claim_issues_before_merge(gh, pr)
                        self._guard()
                        method = (self.cfg.get("merge") or {}).get("method", "squash")
                        result = await self._serialized_merge(gh, method)
                        self.update(merge_sha=result.get("sha", ""))
                    except GitHubError as exc:
                        if exc.status in (405, 409) and main_sha != self.task.base_sha:
                            refresh_onto = main_sha
                        else:
                            # e.g. branch protection requires a human review: that is a real decision.
                            self.block("unblock", f"{self.task.key}: GitHub refused the merge", str(exc),
                                       status="needs_input",
                                       options=[{"value": "retry", "label": "Retry merge"},
                                                {"value": "abandon", "label": "Abandon"}])
        if refresh_onto:
            await self._refresh_onto_main(token, gh, refresh_onto)
            return
        self.evidence("merge", {"sha": self.task.merge_sha, "pr": self.task.pr_number, "at": utcnow().isoformat()})
        self.event("merged", f"Merged PR #{self.task.pr_number} ({self.task.merge_sha[:8]})", level="success")
        await self.smoke()

    async def _refresh_onto_main(self, token: str, gh: GitHub, main_sha: str) -> None:
        """Durable gates for the exact rebased candidate; no checks/QA/CI hold repo_lock."""
        state = dict((self.task.evidence or {}).get("revalidation") or {})
        if state.get("phase") in (None, "complete", "handled"):
            old_base, old_head = self.task.base_sha, self.task.head_sha
            candidate = await changed_paths(self.repo.full_name, token, old_base, old_head)
            main = await changed_paths(self.repo.full_name, token, old_base, main_sha)
            mode = (self.cfg.get("merge") or {}).get("reverify", "overlap")
            overlap = path_overlap(candidate, main)
            decision = {"mode": mode, "old_base": old_base, "old_head": old_head, "new_base": main_sha,
                        "candidate_paths": candidate[:20], "main_paths": main[:20],
                        "candidate_paths_count": len(candidate), "main_paths_count": len(main), **overlap,
                        "run": mode == "always" or (mode != "never" and overlap["overlap"])}
            self.artifact("report", "Revalidation changed paths", json.dumps({"candidate": candidate, "main": main}))
            state = {**decision, "phase": "rebasing"}
            self.evidence("revalidation", state)
            self.event("revalidate", "The base branch moved; rebasing and revalidating before merge")
            self.event("reverify_decision", "Rebased candidate: " + ("run" if decision["run"] else "skip")
                       + " independent verification", **decision)
        if state["phase"] == "rebasing":
            def before_push(head: str) -> None:
                state["expected_head"] = head
                self.evidence("revalidation", state)

            try:
                clean, detail = await rebase_branch(
                    self.repo.full_name, token, branch=self.task.branch, onto=state["new_base"],
                    task_id=self.task_id, base=state["old_base"], head=state["old_head"],
                    expected_head=state.get("expected_head", ""), before_push=before_push)
            except BranchMoved as exc:
                self.block("unblock", f"{self.task.key}: PR head changed",
                           f"The pull request branch no longer matches the verified candidate ({exc}); it was not "
                           "rebased or merged.", status="needs_input")
            if not clean:
                state.update(phase="rejected", reason="conflicts with newer commits", feedback=[detail])
                self.evidence("revalidation", state)
            else:
                state.update(phase="checks", head=detail)
                self.evidence("revalidation", state)
        if state["phase"] not in ("rejected", "handled"):
            if self.task.head_sha != state["head"]:
                # A merge approval belongs to the published candidate, not its branch name.
                # Persist the new publication before advancing the head, including on resume.
                self.evidence("published", {"head": state["head"], "base": state["new_base"],
                                            "at": utcnow().isoformat(), "pr": self.task.pr_number})
            self.update(head_sha=state["head"], base_sha=state["new_base"])
        if state["phase"] in ("checks", "verify"):
            sandbox = LocalSandbox(f"task-{self.task_id[:12]}-r").create()
            try:
                await checkout(sandbox, self.repo.full_name, token, state["head"], self.task.branch)
                if state["phase"] == "checks":
                    async with self._check_slot(sandbox, bool(self.cfg.get("setup") or self.cfg.get("checks"))):
                        setup = await self._run_commands(sandbox, self.cfg.get("setup") or [], "setup", timeout=1200)
                        self.checks = await self._run_commands(sandbox, self.cfg.get("checks") or [], "check")
                    state.update(setup=setup, checks=self.checks)
                    failed = [c for c in setup + self.checks if not c["ok"]]
                    if failed:
                        state.update(phase="rejected", reason="fails its checks on top of newer commits",
                                     feedback=[f"- After rebasing, `{c['command']}` failed:\n{c['tail']}" for c in failed])
                        self.evidence("attempts", {"n": self.task.attempt, "result": "checks_failed",
                                                   "checks": _brief(setup + self.checks)})
                    else:
                        state["phase"] = "verify" if state["run"] else "passed"
                    self.evidence("revalidation", state)
                if state["phase"] == "verify":
                    self.checks = state.get("checks") or []
                    patch = await extract_patch(sandbox, state["new_base"])
                    # Reuse the original harness, isolation, AppRun and evidence path, including
                    # persisted builder-proposed QA checks after runner reconstruction.
                    self.stage("verify", "Independent verification of the rebased candidate")
                    verdict = await self._verify(token, patch, self.task.attempt)
                    state["verifier"] = self.verifier_report
                    if verdict is None:
                        state.update(phase="rejected", reason="failed independent verification after rebase",
                                     feedback=self.feedback[-8:])
                    else:
                        state["phase"] = "passed"
                    self.evidence("revalidation", state)
            finally:
                await sandbox.close()
        await self._revalidation_body(gh, state)
        if state["phase"] == "rejected":
            self.feedback += state.get("feedback") or []
            # Set the rebuild continuation before marking rejection handled: an interruption
            # between these writes must never resume merge instead of the requested rebuild.
            self.update(stage="revise")
            self._fresh_budget_after_base_moved(state["reason"])
            if self.builder:
                await self.builder.close()
                self.builder = None
            await self.deliver(continue_from_branch=True)
            return
        if state["phase"] == "passed":
            try:
                await gh.set_status(self.repo.full_name, state["head"], "success",
                                    "Revalidated after rebase by MindsHub AI Factory", self._task_url())
            except GitHubError as exc:
                self.event("revalidation_warning", f"Could not publish revalidation status: {str(exc)[:200]}", level="warn")
            self.update(stage="ci")
            self.evidence("revalidation", {**state, "phase": "complete"})
            await self.ci_then_merge()

    async def _revalidation_body(self, gh: GitHub, state: dict[str, Any]) -> None:
        """Auxiliary publication must never prevent rejection feedback or a passing delivery."""
        try:
            pr = await gh.pull(self.repo.full_name, self.task.pr_number)
            body = pr.get("body") or ""
            section = _revalidation_section(state)
            body = _replace_section(body, section, REVALIDATION_BEGIN, REVALIDATION_END)
            if len(body) > 55_000:
                raise ValueError("PR body too large; full revalidation details remain in task evidence")
            await gh.request("PATCH", f"/repos/{self.repo.full_name}/pulls/{self.task.pr_number}", json={"body": body})
        except Exception as exc:  # auxiliary formatting and GitHub failures are both best-effort
            log.warning("revalidation PR update failed for %s: %s", self.task_id, exc)
            self.event("revalidation_warning", f"Could not update revalidation PR body: {str(exc)[:200]}", level="warn")

    # smoke & release -------------------------------------------------------------
    async def smoke(self) -> None:
        self.stage("smoke", "Running smoke checks on the merged result")
        commands = self.cfg.get("smoke") or self.cfg.get("checks") or []
        if not commands:
            self.evidence("smoke", {"state": "not_configured"})
            self.event("smoke", "No smoke checks configured for this repository", level="warn")
            await self.release()
            return
        token = resolve_token(self.ws)
        sandbox = LocalSandbox(f"task-{self.task_id[:12]}-s").create()
        try:
            gh = GitHub(token)
            ref = self.task.merge_sha or await gh.branch_sha(self.repo.full_name, self.repo.default_branch)
            await checkout(sandbox, self.repo.full_name, token, ref, f"smoke-{self.task_id[:8]}")
            async with self._check_slot(sandbox):
                await self._run_commands(sandbox, self.cfg.get("setup") or [], "setup", timeout=1200)
                results = await self._run_commands(sandbox, commands, "smoke")
        finally:
            await sandbox.close()
        ok = all(r["ok"] for r in results)
        self.evidence("smoke", {"state": "passed" if ok else "failed", "ref": self.task.merge_sha, "checks": _brief(results)})
        if not ok:
            failed = [r for r in results if not r["ok"]]
            _signal(self.ws, self.repo.id, "smoke_failure", f"Smoke failed after merging {self.task.title}",
                    "\n\n".join(f"`{r['command']}`\n{r['tail'][-1500:]}" for r in failed), self.task.pr_url)
            if (self.cfg.get("recovery") or {}).get("smoke_failure") == "revert":
                self.event("recovery", "Smoke failed; the repository's recovery policy reverts the merge", level="warn")
                await self.revert()
                return
            self.block("unblock", f"Smoke checks failed after merging {self.task.key}",
                       "The change is merged but post-merge smoke checks failed. Revert it, create a follow-up fix, "
                       "or acknowledge. A fix or an acknowledgement accepts the merged change, and the tasks that "
                       "depend on it start.", status="smoke_failed",
                       options=[{"value": "revert", "label": "Revert the merge"},
                                {"value": "fix", "label": "Create a fix outcome"},
                                {"value": "acknowledge", "label": "Acknowledge"}])
        self.event("smoke_passed", "Post-merge smoke checks passed", level="success")
        await self.release()

    async def revert(self) -> None:
        """Bounded recovery: open a revert PR for the merged change, wait for CI, merge it and re-run smoke."""
        existing = (self.task.evidence or {}).get("revert_candidate")
        if existing:
            await self._wait_ci(continuation="revert", head=existing["head"], pr=existing["pr"])
            return
        if not self.task.merge_sha:
            self.block("unblock", f"{self.task.key}: nothing to revert", "No merge commit is recorded for this task.")
        self.stage("revert", f"Reverting {self.task.merge_sha[:8]} from {self.repo.default_branch}")
        token = resolve_token(self.ws)
        gh = GitHub(token)
        branch = f"factory/revert-{self.outcome.number}-{self.task.key.lower()}-{self.task_id[:6]}"
        try:
            head = await revert_commit(self.repo.full_name, token, base=self.repo.default_branch,
                                       sha=self.task.merge_sha, branch=branch, task_id=self.task_id)
            pr = await gh.create_pull(self.repo.full_name, head=branch, base=self.repo.default_branch,
                                      title=f"Revert: {self.task.title}",
                                      body=(f"Post-merge smoke checks failed after #{self.task.pr_number} "
                                            f"({self.task.merge_sha[:10]}). The Factory is reverting it under the "
                                            "repository's recovery policy or a recorded decision."
                                            + (f"\n\n[View in MindsHub AI Factory]({self._task_url()})" if self._task_url() else "")))
        except (GitError, GitHubError) as exc:
            self.block("unblock", f"{self.task.key}: could not open a revert", str(exc), status="smoke_failed",
                       options=[{"value": "acknowledge", "label": "Acknowledge"}])
        self.event("revert_opened", f"Opened revert PR #{pr['number']}", level="warn", url=pr["html_url"])
        candidate = {"pr": pr["number"], "head": head, "url": pr["html_url"]}
        self.evidence("revert_candidate", candidate)
        await self._wait_ci(gh, continuation="revert", head=head, pr=pr["number"])

    async def _merge_revert(self) -> None:
        candidate = self.task.evidence["revert_candidate"]
        gh = GitHub(resolve_token(self.ws))
        async with self.repo_lock(self.repo.id):
            self._guard()
            pr = await gh.pull(self.repo.full_name, candidate["pr"])
            if not pr.get("merged"):
                self._guard()
                if pr.get("state") == "closed" or pr.get("head", {}).get("sha") != candidate["head"]:
                    self.block("unblock", f"{self.task.key}: revert PR changed",
                               "The revert PR is closed or no longer matches the CI-tested candidate.",
                               status="smoke_failed")
                try:
                    result = await gh.merge(self.repo.full_name, candidate["pr"], sha=candidate["head"],
                                            method="squash", title=f"Revert: {self.task.title} (#{candidate['pr']})")
                except GitHubError as exc:
                    self.block("unblock", f"{self.task.key}: could not merge the revert", str(exc),
                               status="smoke_failed", options=[{"value": "acknowledge", "label": "Acknowledge"}])
                pr = {**pr, "merged": True, "merge_commit_sha": result.get("sha", "")}
            # No lifecycle guard after an irreversible merge. A concurrent stop
            # must not erase what already happened on GitHub. No work follows.
            from ..reconcile import _handle_reverted

            _handle_reverted(self.task_id, pr)

    async def release(self) -> None:
        release_cfg = self.cfg.get("release") or {}
        if not release_cfg.get("health_url") and release_cfg.get("mode") != "on_merge":
            await self._complete("Merged and smoke-tested")
            return
        self.stage("release", "Watching the release")
        url = release_cfg.get("health_url")
        if url:
            import httpx

            deadline = time.monotonic() + int(release_cfg.get("timeout_minutes", 15)) * 60
            expect = (release_cfg.get("expect_text") or "").replace("{merge_sha}", self.task.merge_sha or "") or None
            delay = int(release_cfg.get("initial_delay_seconds", 90))
            await asyncio.sleep(delay)
            last = ""
            while time.monotonic() < deadline:
                try:
                    async with httpx.AsyncClient(timeout=20, follow_redirects=True) as client:
                        response = await client.get(url)
                    if response.status_code < 400 and (not expect or expect in response.text):
                        self.evidence("release", {"state": "healthy", "url": url, "status": response.status_code})
                        self.event("released", f"Release healthy at {url}", level="success")
                        await self._complete("Merged, smoke-tested and released")
                        return
                    last = f"HTTP {response.status_code}"
                except httpx.HTTPError as exc:
                    last = type(exc).__name__
                await asyncio.sleep(30)
            self.evidence("release", {"state": "unhealthy", "url": url, "last": last})
            _signal(self.ws, self.repo.id, "release_failure", f"Release check failed after {self.task.title}",
                    f"{url} did not become healthy: {last}", self.task.pr_url)
            self.block("unblock", f"Release check failed after {self.task.key}",
                       f"{url} did not report healthy within the window ({last}).", status="smoke_failed",
                       options=[{"value": "acknowledge", "label": "Acknowledge"}])
        self.evidence("release", {"state": "on_merge", "note": "Released by the repository's deploy-on-merge pipeline"})
        await self._complete("Merged, smoke-tested; released by deploy-on-merge")

    async def _complete(self, summary: str) -> None:
        try:
            await self._settle_issues()
        except Exception as exc:  # noqa: BLE001 - bookkeeping must never undo a delivery
            log.warning("issue settlement failed for %s: %s", self.task_id, exc)
        self.update(status="completed", stage="done", finished_at=utcnow(), summary=summary, blocked_reason="")
        self.event("task_completed", f"{self.task.key} complete: {summary}", level="success")


FACTORY_BEGIN, FACTORY_END = "<!-- factory:begin -->", "<!-- factory:end -->"
REVALIDATION_BEGIN, REVALIDATION_END = "<!-- factory:revalidation:begin -->", "<!-- factory:revalidation:end -->"


def _factory_block(body: str) -> str:
    return f"{FACTORY_BEGIN}\n{body}\n{FACTORY_END}"


def _replace_section(body: str, section: str, begin: str, end: str) -> str:
    start, stop = body.find(begin), body.find(end)
    if start >= 0 and stop >= start:
        return body[:start] + section + body[stop + len(end):]
    return body + "\n\n" + section


def _replace_factory_block(body: str, block: str) -> str:
    updated = _replace_section(body, block, FACTORY_BEGIN, FACTORY_END)
    if len(updated) > 55_000:
        raise ValueError("PR body too large; generated details remain in task evidence")
    return updated


def _revalidation_section(state: dict[str, Any]) -> str:
    """Bound serialized JSON *before* wrapping: even escaped Unicode must fit a closed fence."""
    summary = {k: state[k] for k in ("mode", "old_base", "new_base", "run", "overlap", "phase",
                                     "shared_files_count", "shared_directories_count") if k in state}
    for key in ("candidate_paths", "main_paths", "shared_files", "matching_pairs"):
        values = state.get(key) or []
        summary[key + "_count"] = state.get(key + "_count", len(values))
        summary[key] = values[:20]
    for key in ("setup", "checks"):
        commands = state.get(key) or []
        failed = [str(c.get("command", ""))[:200] for c in commands if not c.get("ok")]
        summary[key] = {"count": len(commands), "failed_count": len(failed), "failed": failed[:20]}
    report = state.get("verifier") or {}
    summary["verifier"] = {"verdict": report.get("verdict"), "summary": str(report.get("summary", ""))[:500]}
    # Trim samples until the actual serialized payload fits, not just source characters.
    samples = [summary[k] for k in ("candidate_paths", "main_paths", "shared_files", "matching_pairs")]
    samples += [summary[k]["failed"] for k in ("setup", "checks")]
    payload = json.dumps(summary, indent=2)
    while len(payload) > 7400 and any(samples):
        max((sample for sample in samples if sample), key=lambda x: len(json.dumps(x))).pop()
        payload = json.dumps(summary, indent=2)
    # All untrusted free text is bounded too, including a Unicode verifier summary.
    if len(payload) > 7400:
        summary["verifier"]["summary"] = "See full verifier report in task evidence."
        payload = json.dumps(summary, indent=2)
    return (f"{REVALIDATION_BEGIN}\n### Revalidation after base moved\n"
            f"```json\n{payload}\n```\n{REVALIDATION_END}")


def _objects(items: Any) -> list[dict[str, Any]]:
    """The objects in a model-written list (a verifier's criteria or findings); anything else in it is skipped."""
    return [item for item in items if isinstance(item, dict)] if isinstance(items, list) else []


def _actionable(attempt: dict[str, Any]) -> bool:
    """Whether an attempt's record gives the next attempt something concrete to fix."""
    result = attempt.get("result")
    if result == "checks_failed":
        return any(not c.get("ok") for c in attempt.get("checks") or [])
    if result == "policy":
        return bool(attempt.get("detail"))
    if result == "verification_failed":
        verdict = attempt.get("verdict") or {}  # optional report fields may be null
        # Exactly what the feedback relays to the next attempt: unmet criteria and blocker/major findings.
        return (any(c.get("status") != "pass" for c in _objects(verdict.get("criteria")))
                or any(f.get("severity") in ("blocker", "major") for f in _objects(verdict.get("findings"))))
    return False


def _brief(checks: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Check results for the evidence record; failed checks keep their output tail so the failure can be fed back
    to a later attempt even after a restart."""
    return [{k: c[k] for k in ("command", "ok", "exit_code", "duration")}
            | ({} if c["ok"] else {"tail": str(c.get("tail") or "")[-1500:]})
            | ({"preexisting": True} if c.get("preexisting") else {}) for c in checks]


_ANSI = re.compile(r"\x1b\[[0-9;]*m")
_PYTEST_FAILURE = re.compile(r"^(FAILED|ERROR) (.+?)(?: - .*)?$", re.MULTILINE)
_PYTEST_TOTALS = re.compile(r"\b(\d+) (?:failed|errors?)\b")


def _failing_tests(output: str) -> list[str] | None:
    """The FAILED/ERROR node ids of pytest's short test summary, or None when there are none, or when the summary
    lists fewer than its totals line counts (the output was clipped)."""
    text = _ANSI.sub("", output or "")
    ids = []
    for kind, node in _PYTEST_FAILURE.findall(text):
        node = node.strip()
        if "::" in node or node.endswith(".py"):
            ids.append(f"{kind} {node}")
    if not ids:
        return None
    totals = [line for line in text.splitlines() if re.search(r"\bin [\d.]+s\b", line) and _PYTEST_TOTALS.search(line)]
    if totals and len(ids) < sum(int(n) for n in _PYTEST_TOTALS.findall(totals[-1])):
        return None
    return sorted(set(ids))


_VOLATILE = re.compile(r"\d{4}-\d\d-\d\d[T ]\d\d:\d\d:\d\d(?:[.,]\d+)?(?:Z|[+-]\d\d:?\d\d)?"  # timestamps
                       r"|\b\d\d:\d\d:\d\d(?:[.,]\d+)?\b"  # times of day
                       r"|\b\d+(?:\.\d+)?\s?(?:ms|s)\b"  # durations
                       r"|\b0x[0-9a-fA-F]+\b")  # addresses


def _output_signature(output: str, sandbox: Any = None) -> str:
    """A digest of a failing command's whole output with what differs between two runs of the same failure taken
    out: colours, the sandbox's own paths, timestamps, durations and memory addresses. "" when there is no output,
    which proves nothing. Two runs with the same signature printed the same thing."""
    text = _ANSI.sub("", output or "")
    # A local sandbox's work directory is inside its root; a VM has only a work directory.
    for attr, name in (("root", "<sandbox>"), ("work", "<sandbox>/work")):
        if getattr(sandbox, attr, None):
            text = text.replace(str(getattr(sandbox, attr)), name)
    text = "\n".join(line.rstrip() for line in _VOLATILE.sub("<n>", text).strip().splitlines())
    return hashlib.sha256(text.encode("utf-8", "replace")).hexdigest()[:32] if text else ""


def _same_failure(candidate: dict[str, Any], base: dict[str, Any] | None) -> bool:
    """Whether a check failing on the candidate fails the same way on the base commit, with nothing new. For pytest
    output: no node id that the base does not fail too. Other output has no test names to compare, and "the base
    exits non-zero as well" is not enough (a build can fail on the base for one reason and on the candidate for
    another), so both runs must end with the same exit code and print the same thing (``_output_signature``).
    Anything else is inconclusive, and the failure counts against the attempt."""
    if not base or base.get("ok", True) or (candidate.get("timed_out") and not base.get("timed_out")):
        return False
    new, old = candidate.get("failures"), base.get("failures")
    if new is None or old is None:
        return (new is None and old is None and candidate.get("exit_code") == base.get("exit_code")
                and bool(candidate.get("signature")) and candidate.get("signature") == base.get("signature"))
    return set(new) <= set(old)


def _signal(workspace_id: str, repo_id: str | None, source: str, title: str, body: str, url: str) -> None:
    import hashlib

    fingerprint = hashlib.sha256(f"{source}:{title}".encode()).hexdigest()[:40]
    with session_scope() as s:
        row = s.query(Signal).filter(Signal.workspace_id == workspace_id, Signal.fingerprint == fingerprint).one_or_none()
        if row:
            row.count += 1
            row.last_seen_at = utcnow()
            row.body = body[:8000]
        else:
            s.add(Signal(workspace_id=workspace_id, repo_id=repo_id, source=source, fingerprint=fingerprint,
                         title=title[:300], body=body[:8000], url=url))
    notify(workspace_id, "signal", fingerprint)
