"""Task delivery: build → checks → independent verification → PR → CI → merge → smoke → release.

Each stage is durable (``task.stage``) so work resumes after approvals or restarts without
repeating external side effects. Failures inside an attempt become precise feedback for the
next attempt; only genuine decisions or provider/access problems reach a person.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from typing import Any, Callable

from ..config import get_settings
from ..db import session_scope, utcnow
from ..events import notify, record
from ..github import GitHub, GitHubError, resolve_token
from ..gitops import (DEFAULT_PROTECTED, GitError, Patch, apply_in_sandbox, checkout, extract_patch, protected_violations,
                      publish, rebase_branch, revert_commit)
from ..models import Artifact, Decision, Outcome, Repository, Signal, Task
from ..prompts import BUILDER, build_prompt, verifier_system, verify_prompt
from ..sandbox import LocalSandbox, SandboxError
from .qa import AppRun, verifier_brief
from .runtime import (SessionSpec, guidance_history, open_decision, outcome_cost, precedents, run_agent,
                      workspace_settings)

log = logging.getLogger(__name__)

PROVIDER_PROBLEMS = ("refused the credential", "no credit", "No model provider", "limit reached", "wallet",
                     "No GitHub access", "Bad credentials")


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
        self._load()
        self.feedback = self._prior_feedback()

    def _prior_feedback(self) -> list[str]:
        """Reviewer findings from earlier attempts survive retries and restarts, so new attempts converge."""
        notes: list[str] = []
        for attempt in (self.task.evidence or {}).get("attempts", [])[-3:]:
            verdict = attempt.get("verdict") or {}
            if attempt.get("result") == "verification_failed" and verdict:
                notes.append(f"- An earlier attempt failed independent review: {str(verdict.get('summary', ''))[:800]}")
                for c in verdict.get("criteria", []):
                    if c.get("status") != "pass":
                        notes.append(f"  - Criterion not met: {c.get('criterion')} — {str(c.get('evidence', ''))[:400]}")
                for f in verdict.get("findings", []):
                    if f.get("severity") in ("blocker", "major"):
                        notes.append(f"  - {f.get('severity')}: {str(f.get('description', ''))[:400]} {f.get('location', '')}")
            elif attempt.get("result") in ("checks_failed", "policy"):
                notes.append(f"- An earlier attempt failed: {attempt.get('detail') or attempt.get('result')}")
        return notes

    # state ---------------------------------------------------------------------
    def _load(self) -> None:
        with session_scope() as s:
            task = s.get(Task, self.task_id)
            self.task = Task(**{c.name: getattr(task, c.name) for c in Task.__table__.columns})
            outcome = s.get(Outcome, task.outcome_id)
            self.outcome = Outcome(**{c.name: getattr(outcome, c.name) for c in Outcome.__table__.columns})
            repo = s.get(Repository, task.repo_id)
            self.repo = Repository(**{c.name: getattr(repo, c.name) for c in Repository.__table__.columns})
        self.ws = self.task.workspace_id
        self.settings = workspace_settings(self.ws)
        self.cfg = self.repo.config or {}

    def update(self, **fields: Any) -> None:
        with session_scope() as s:
            task = s.get(Task, self.task_id)
            for key, value in fields.items():
                setattr(task, key, value)
                setattr(self.task, key, value)
        notify(self.ws, "task", self.task_id, outcome_id=self.task.outcome_id)

    def event(self, kind: str, message: str, level: str = "info", **data: Any) -> None:
        record(self.ws, kind, message, outcome_id=self.task.outcome_id, task_id=self.task_id, level=level, data=data)

    def evidence(self, key: str, value: Any) -> None:
        with session_scope() as s:
            task = s.get(Task, self.task_id)
            # Deep copy: mutating the stored JSON in place would hide the change from the ORM.
            ev = json.loads(json.dumps(task.evidence or {}))
            if key == "attempts":
                ev.setdefault("attempts", []).append(value)
            else:
                ev[key] = value
            task.evidence = ev
            self.task.evidence = ev

    def artifact(self, kind: str, name: str, content: str = "", run_id: str | None = None, **data: Any) -> None:
        with session_scope() as s:
            s.add(Artifact(workspace_id=self.ws, outcome_id=self.task.outcome_id, task_id=self.task_id, run_id=run_id,
                           kind=kind, name=name, content=content[:400_000], data=data))

    def stage(self, stage: str, message: str | None = None) -> None:
        self.update(stage=stage, status="running")
        if message:
            self.event("stage", message, stage=stage)

    def block(self, kind: str, title: str, body: str, *, status: str = "blocked",
              options: list[dict[str, Any]] | None = None) -> None:
        self.update(status=status, blocked_reason=body[:2000])
        open_decision(self.ws, kind, title, body, outcome_id=self.task.outcome_id, task_id=self.task_id,
                      options=options or [{"value": "retry", "label": "Retry"}, {"value": "abandon", "label": "Abandon task"}])
        raise Stop()

    # entry ---------------------------------------------------------------------
    async def run(self) -> None:
        try:
            if not self.task.started_at:
                self.update(started_at=utcnow())
            stage = self.task.stage
            if stage == "merge":
                await self.merge()
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
        max_attempts = int(self.settings.get("max_attempts", 3))
        token = resolve_token(self.ws)
        while True:
            attempt = self.task.attempt + 1
            if attempt > max_attempts:
                summary = "\n".join(self.feedback[-6:])[:3000]
                self.block("unblock", f"{self.task.key} did not pass after {max_attempts} attempts",
                           f"The task could not be completed within the attempt limit. Latest feedback:\n\n{summary}",
                           status="failed",
                           options=[{"value": "retry", "label": "Try again (fresh attempts)"},
                                    {"value": "abandon", "label": "Abandon task"}])
            self.update(attempt=attempt)
            budget = self.settings["budgets"].get("outcome_usd")
            spent, known = outcome_cost(self.task.outcome_id)
            if budget and known and spent >= budget and not (self.task.evidence or {}).get("budget_override"):
                self.block("budget", f"Outcome #{self.outcome.number} reached its ${budget:.0f} budget",
                           f"Spent ${spent:.2f}. Raise the budget in Settings or approve continuing.", status="needs_input",
                           options=[{"value": "retry", "label": "Continue anyway"}, {"value": "abandon", "label": "Stop"}])
            if self.builder is None:
                await self._prepare_builder(token, from_branch=continue_from_branch and bool(self.task.branch))
            self.stage("build", f"Attempt {attempt}: building with {self.task.harness}")
            run, result, error = await run_agent(SessionSpec(
                workspace_id=self.ws, outcome_id=self.task.outcome_id, task_id=self.task_id, role="builder",
                harness=self.task.harness, system=BUILDER, sandbox=self.builder, attempt=attempt, writable=True,
                prompt=build_prompt(self.task, self.outcome, self.repo, self.feedback[-8:],
                                    precedents(self.ws, self.repo.id),
                                    guidance_history(self.task.outcome_id, self.task_id))), self.cancelled)
            if result is None:
                if any(p.lower() in error.lower() for p in PROVIDER_PROBLEMS):
                    self.block("unblock", f"{self.task.key}: model provider problem", error,
                               options=[{"value": "retry", "label": "Retry"}, {"value": "abandon", "label": "Abandon"}])
                self.feedback.append(f"- The previous builder session ended without a report: {error[:500]}")
                self.evidence("attempts", {"n": attempt, "result": "builder_failed", "error": error[:500]})
                continue
            self.builder_report = result.report
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
            problem = self._policy_problem(patch)
            if problem:
                self.feedback.append(problem)
                self.evidence("attempts", {"n": attempt, "result": "policy", "detail": problem})
                self.event("candidate_rejected", problem, level="warn")
                continue
            self.patch = patch
            self.artifact("diff", f"Attempt {attempt} diff", patch.diff.decode("utf-8", "replace"), run_id=run.id,
                          files=patch.files, additions=patch.additions, deletions=patch.deletions)
            self.checks = await self._run_commands(self.builder, self.cfg.get("checks") or [], "check")
            failed = [c for c in self.checks if not c["ok"]]
            if failed:
                self.feedback += [f"- Check `{c['command']}` failed (exit {c['exit_code']}):\n```\n{c['tail']}\n```"
                                  for c in failed]
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
                                       "verdict": verdict, "app_run": self.app_summary})
            break

        await self._publish(token)
        await self.ci_then_merge()

    async def _prepare_builder(self, token: str, from_branch: bool) -> None:
        self.builder = LocalSandbox(f"task-{self.task_id[:12]}-b{self.task.attempt}").create()
        branch = self.task.branch or f"factory/{self.outcome.number}-{self.task.key.lower()}-{self.task_id[:6]}"
        self.stage("prepare", f"Preparing a fresh checkout of {self.repo.full_name}")
        if from_branch:
            await checkout(self.builder, self.repo.full_name, token, f"refs/heads/{branch}", branch)
        else:
            base_sha = await checkout(self.builder, self.repo.full_name, token, self.repo.default_branch, branch)
            self.update(base_sha=base_sha, branch=branch)
        setup = await self._run_commands(self.builder, self.cfg.get("setup") or [], "setup", timeout=1200)
        broken = [c for c in setup if not c["ok"]]
        if broken:
            self.feedback.append("- Note: repository setup failed before you started:\n" + "\n".join(
                f"`{c['command']}` → exit {c['exit_code']}\n```\n{c['tail'][-1500:]}\n```" for c in broken))

    def _policy_problem(self, patch: Patch) -> str:
        if patch.empty:
            return "- No changes were made to the repository. Implement the task (write the code), then finish."
        protected = list(self.cfg.get("protected_paths") or []) + DEFAULT_PROTECTED
        bad = protected_violations(patch.files, protected)
        if bad:
            return f"- You modified protected paths {bad}. Revert those files completely; they must not change."
        limit = int(self.cfg.get("max_changed_lines") or self.settings["limits"].get("max_changed_lines", 1500))
        if patch.changed_lines > limit:
            return (f"- The change is {patch.changed_lines} lines, over the {limit}-line limit. Remove unrelated or "
                    "generated changes (lockfile churn, formatting, build output) and keep the change focused.")
        if len(patch.diff) > 5_000_000:
            return "- The diff is larger than 5 MB. Remove generated or binary files from the change."
        return ""

    async def _run_commands(self, sandbox: LocalSandbox, commands: list[str], kind: str,
                            timeout: float = 1500) -> list[dict[str, Any]]:
        results = []
        for command in commands:
            started = time.monotonic()
            result = await sandbox.exec(command, timeout=timeout)
            row = {"command": command, "ok": result.ok, "exit_code": result.exit_code,
                   "duration": round(time.monotonic() - started, 1), "timed_out": result.timed_out,
                   "tail": result.output[-4000:]}
            results.append(row)
            self.event(f"{kind}_result", f"{'✓' if result.ok else '✗'} {command}",
                       level="success" if result.ok else "warn", command=command, exit_code=result.exit_code,
                       duration=row["duration"])
            self.artifact(kind, command, result.output, ok=result.ok, exit_code=result.exit_code)
        return results

    async def _verify(self, token: str, patch: Patch, attempt: int) -> dict[str, Any] | None:
        qa_cfg = self.cfg.get("qa") or {}
        for tries in range(2):
            sandbox = None
            app: AppRun | None = None
            try:
                sandbox = await self._verifier_sandbox(token, patch, attempt, tries, qa_cfg)
                await self._prepare(sandbox)
                brief = ""
                if qa_cfg.get("start"):
                    app = await self._start_app(sandbox, qa_cfg)
                    brief = verifier_brief(app)
                run, result, error = await run_agent(SessionSpec(
                    workspace_id=self.ws, outcome_id=self.task.outcome_id, task_id=self.task_id, role="verifier",
                    harness=self.task.verifier_harness, system=verifier_system(self.task.base_sha), sandbox=sandbox,
                    attempt=attempt, writable=False,
                    prompt=verify_prompt(self.task, self.repo, self.task.base_sha, self.builder_report, self.checks)
                    + brief), self.cancelled)
                if app is not None:
                    await self._store_app_evidence(app, attempt, run.id)
            finally:
                if app is not None:
                    app.stop()
                if sandbox is not None:
                    await sandbox.close()
            if result is None:
                if any(p.lower() in error.lower() for p in PROVIDER_PROBLEMS):
                    self.block("unblock", f"{self.task.key}: model provider problem during verification", error)
                self.event("verification_unavailable", f"Verifier session failed: {error[:300]}", level="warn")
                continue
            report = result.report
            runtime_problem = self._runtime_problem(app, qa_cfg)
            if runtime_problem and report["verdict"] == "pass":
                report = {**report, "verdict": "fail",
                          "summary": f"Runtime QA failed: {runtime_problem}. " + report.get("summary", "")}
            self.verifier_report = report
            self.artifact("report", f"Verification (attempt {attempt})", json.dumps(report, indent=2), run_id=run.id,
                          verdict=report.get("verdict"))
            if report["verdict"] == "pass":
                self.event("verified", f"Independent verification passed: {report.get('summary', '')[:300]}",
                           level="success")
                return report
            failing = [c for c in report.get("criteria", []) if c.get("status") != "pass"]
            findings = [f for f in report.get("findings", []) if f.get("severity") in ("blocker", "major")]
            self.feedback.append("- Independent review FAILED. " + report.get("summary", "")[:1500])
            self.feedback += [f"  - Criterion not met: {c['criterion']} — {c.get('evidence', '')[:600]}" for c in failing]
            self.feedback += [f"  - {f['severity']}: {f['description'][:600]} {f.get('location', '')}" for f in findings]
            if runtime_problem and app is not None and app.evidence.get("log_tail"):
                self.feedback.append(f"  - App log when it failed:\n```\n{app.evidence['log_tail'][-1500:]}\n```")
            self.evidence("attempts", {"n": attempt, "result": "verification_failed", "verdict": report,
                                       "checks": _brief(self.checks), "app_run": self.app_summary})
            self.event("verification_failed", report.get("summary", "")[:400], level="warn")
            return None
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
        if self.task.verifier_harness == "codex":
            self.event("qa_vm_skipped", "Codex verifiers run in a local sandbox; runtime QA stays local for this task",
                       level="warn")
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

    async def _prepare(self, sandbox) -> None:
        """Install dependencies, unless the VM booted from a prepared environment for these exact lockfiles.
        After a clean install in a VM, save it so the next verification starts ready."""
        setup = self.cfg.get("setup") or []
        if getattr(sandbox, "prepared", None):
            self.event("verify_setup_skipped", "Dependencies unchanged since the prepared environment was saved; "
                       "setup skipped", level="success", environment=sandbox.prepared)
            return
        results = await self._run_commands(sandbox, setup, "verify_setup", timeout=1200)
        key = getattr(sandbox, "environment_key", None)
        if key and results and all(r["ok"] for r in results):
            started = time.monotonic()
            try:
                await sandbox.checkpoint(key)
                self.event("environment_prepared", f"Saved a prepared environment for these dependencies "
                           f"({time.monotonic() - started:.0f}s)", environment=key)
            except Exception as exc:  # an optimisation only
                self.event("environment_prepare_failed", f"Could not save a prepared environment: {str(exc)[:200]}",
                           level="warn")

    async def _start_app(self, sandbox, qa_cfg: dict[str, Any]) -> AppRun:
        """Boot the candidate in the verifier's sandbox, then let the Factory capture its own evidence first."""
        app = AppRun(sandbox, qa_cfg)
        self.event("qa_app_starting", f"Starting the app for runtime QA: {qa_cfg['start'][:200]}")
        ready = await app.start()
        if ready:
            app.evidence["pages"] = await app.screenshots() if qa_cfg.get("pages") else []
            app.evidence["probes"] = await app.probes()
            ok_pages = sum(1 for p in app.evidence["pages"] if p.get("status") and p["status"] < 400)
            ok_probes = sum(1 for p in app.evidence["probes"] if p.get("ok"))
            self.event("qa_app_ready", f"App ready in {app.evidence['ready_seconds']}s · pages {ok_pages}/"
                       f"{len(app.evidence['pages'])} · probes {ok_probes}/{len(app.evidence['probes'])}",
                       level="success" if ok_pages == len(app.evidence["pages"]) and ok_probes == len(app.evidence["probes"]) else "warn")
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
        bad_probes = [p.get("path") for p in app.evidence.get("probes", []) if not p.get("ok")]
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
        body = self._pr_body()
        try:
            pr = await gh.create_pull(self.repo.full_name, head=self.task.branch, base=self.repo.default_branch,
                                      title=f"{self.task.title}", body=body)
            await gh.set_status(self.repo.full_name, head, "success", "Independently verified by AI Factory",
                                self._task_url())
        except GitHubError as exc:
            self.block("unblock", f"{self.task.key}: could not open the pull request", str(exc))
        self.update(head_sha=head, pr_number=pr["number"], pr_url=pr["html_url"], stage="ci")
        self.evidence("published", {"head": head, "at": utcnow().isoformat(), "pr": pr["number"]})
        self.event("published", f"Opened PR #{pr['number']}", level="success", url=pr["html_url"], head=head)

    def _pr_body(self) -> str:
        lines = [self.builder_report.get("summary", "").strip() or self.task.title, "", "### Acceptance criteria"]
        by = {c.get("criterion"): c for c in self.verifier_report.get("criteria", [])}
        for criterion in self.task.acceptance:
            c = by.get(criterion) or next((v for k, v in by.items() if k and criterion[:40] in k), {})
            mark = {"pass": "✅", "fail": "❌"}.get(c.get("status", ""), "◻️")
            lines.append(f"- {mark} {criterion}" + (f"  \n  <sub>{c.get('evidence', '')[:400]}</sub>" if c else ""))
        if self.app_summary:
            a = self.app_summary
            lines += ["", "### Runtime QA (app booted in a fresh sandbox)"]
            if a.get("started"):
                lines.append(f"- App started and became ready in {a.get('ready_seconds')}s")
                lines += [f"- {'✅' if (p.get('status') or 500) < 400 else '❌'} page `{p.get('path')}` → "
                          f"{p.get('status') or p.get('error', 'error')} {('· ' + p['title']) if p.get('title') else ''}"
                          for p in a.get("pages", [])]
                lines += [f"- {'✅' if p.get('ok') else '❌'} `{p.get('method')} {p.get('path')}` → {p.get('status')}"
                          for p in a.get("probes", [])]
                lines.append(f"- {a.get('screenshots', 0)} screenshot(s) attached in AI Factory "
                             f"({a.get('verifier_screenshots', 0)} taken by the verifier)")
            else:
                lines.append("- ❌ The app did not start")
        if self.checks:
            lines += ["", "### Checks run by the Factory"]
            lines += [f"- {'✅' if c['ok'] else '❌'} `{c['command']}` ({c['duration']}s)" for c in self.checks]
        spent, known = outcome_cost(self.task.outcome_id)
        lines += ["", "---",
                  f"Built by **{self.task.harness}**, independently verified by a fresh **{self.task.verifier_harness}** "
                  f"session. Outcome #{self.outcome.number}: {self.outcome.title}. "
                  f"Cost so far: {'$%.2f' % spent if known else 'unknown'}."]
        url = self._task_url()
        if url:
            lines.append(f"[View in AI Factory]({url})")
        import re

        issues = set(re.findall(rf"github\.com/{re.escape(self.repo.full_name)}/issues/(\d+)", self.outcome.brief or ""))
        if issues and self._closes_issue():
            lines.append("")
            lines += [f"Closes #{n}" for n in sorted(issues, key=int)]
        return "\n".join(lines)

    def _closes_issue(self) -> bool:
        """Only the last task of an outcome closes its source issue."""
        with session_scope() as s:
            others = s.query(Task).filter(Task.outcome_id == self.task.outcome_id, Task.id != self.task_id,
                                          Task.status != "completed").count()
        return others == 0

    def _task_url(self) -> str:
        base = get_settings().public_url
        return f"{base}/w/{self.ws}/outcomes/{self.task.outcome_id}?task={self.task_id}" if base.startswith("https") else ""

    # CI ------------------------------------------------------------------------
    async def ci_then_merge(self) -> None:
        token = resolve_token(self.ws)
        gh = GitHub(token)
        outcome = await self._wait_ci(gh)
        if outcome == "failure":
            await self._fix_ci(gh)
            return
        await self.merge()

    async def _wait_ci(self, gh: GitHub) -> str:
        self.stage("ci", f"Waiting for CI on PR #{self.task.pr_number}")
        limit = self.settings["limits"].get("ci_minutes", 45) * 60
        grace = int(self.cfg.get("ci_grace_seconds", 150))
        started = time.monotonic()
        last_state = None
        while True:
            if self.cancelled.is_set():
                raise asyncio.CancelledError()
            summary = await gh.checks(self.repo.full_name, self.task.head_sha)
            if summary.state != last_state:
                self.event("ci_status", f"CI {summary.state}", level={"success": "success", "failure": "warn"}.get(
                    summary.state, "info"), checks=[{k: c[k] for k in ("name", "status", "conclusion", "url")}
                                                    for c in summary.checks])
                last_state = summary.state
            elapsed = time.monotonic() - started
            if summary.state == "success":
                self.evidence("ci", {"state": "success", "head": self.task.head_sha,
                                     "checks": [{k: c[k] for k in ("name", "conclusion", "url")} for c in summary.checks]})
                return "success"
            if summary.state == "failure":
                self._ci_failure = summary
                self.evidence("ci", {"state": "failure", "head": self.task.head_sha,
                                     "checks": [{k: c[k] for k in ("name", "conclusion", "url")} for c in summary.checks]})
                return "failure"
            if summary.state == "none" and elapsed > grace:
                required = self.cfg.get("require_ci", False)
                self.evidence("ci", {"state": "none", "head": self.task.head_sha})
                if required:
                    self.block("unblock", f"{self.task.key}: no CI checks reported",
                               f"PR #{self.task.pr_number} received no CI checks within {grace}s, and this repository "
                               "requires CI before merging.")
                self.event("ci_status", "No CI checks are configured for this repository; relying on Factory checks "
                           "and independent verification", level="warn")
                return "none"
            if elapsed > limit:
                self.block("unblock", f"{self.task.key}: CI did not finish",
                           f"CI on PR #{self.task.pr_number} was still running after {limit // 60} minutes.",
                           options=[{"value": "retry", "label": "Keep waiting"}, {"value": "abandon", "label": "Abandon"}])
            await asyncio.sleep(15 if elapsed < 300 else 30)

    async def _fix_ci(self, gh: GitHub) -> None:
        summary = getattr(self, "_ci_failure", None)
        details = []
        for check in (summary.failing if summary else [])[:3]:
            tail = await gh.job_log_tail(self.repo.full_name, check)
            details.append(f"- CI check `{check['name']}` failed ({check['url']}):\n```\n{tail[-3500:]}\n```")
        self.feedback += details or ["- CI failed on the pull request (no logs available)."]
        self.event("ci_failed", f"CI failed on PR #{self.task.pr_number}; sending the failure back to the builder",
                   level="warn")
        await self.deliver(continue_from_branch=True)

    # merge ---------------------------------------------------------------------
    def _merge_needs_approval(self) -> bool:
        merge_policy = (self.cfg.get("merge") or {}).get("mode") or self.settings["autonomy"].get("merge", "auto")
        release_cfg = self.cfg.get("release") or {}
        releases_on_merge = release_cfg.get("mode") == "on_merge"
        if releases_on_merge and self.settings["autonomy"].get("release") == "approval":
            return True
        return merge_policy != "auto"

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
        if self._merge_needs_approval() and not self._approved("approve_merge"):
            self.update(stage="merge")
            self.block("approve_merge", f"Approve merge: {self.task.title}",
                       f"PR #{self.task.pr_number} passed independent verification and CI at "
                       f"{self.task.head_sha[:10]}. {self.task.pr_url}",
                       status="awaiting_approval",
                       options=[{"value": "approve", "label": "Merge"}, {"value": "reject", "label": "Request changes"}])
        self.stage("merge", f"Merging PR #{self.task.pr_number}")
        revalidate = (self.cfg.get("merge") or {}).get("revalidate", True)
        refresh_onto: str | None = None
        # Merges into one repository are serialized; CI waits and rebuilds happen outside the lock.
        async with self.repo_lock(self.repo.id):
            pr = await gh.pull(self.repo.full_name, self.task.pr_number)
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
                if revalidate and main_sha != self.task.base_sha:
                    refresh_onto = main_sha
                else:
                    try:
                        method = (self.cfg.get("merge") or {}).get("method", "squash")
                        result = await gh.merge(self.repo.full_name, self.task.pr_number, sha=self.task.head_sha,
                                                method=method, title=f"{self.task.title} (#{self.task.pr_number})")
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
        """Main moved: rebase the verified change, then re-run checks and CI before merging."""
        self.event("revalidate", "The base branch moved; rebasing and revalidating before merge")
        clean, detail = await rebase_branch(self.repo.full_name, token, branch=self.task.branch,
                                            onto=self.repo.default_branch, task_id=self.task_id)
        if not clean:
            self.feedback.append(f"- Your change conflicts with newer commits on {self.repo.default_branch}. "
                                 f"Re-implement it on top of the latest code.\n{detail[-1500:]}")
            self.update(branch=self.task.branch, stage="build")
            if self.builder:
                await self.builder.close()
                self.builder = None
            await self.deliver(continue_from_branch=False)
            return
        self.update(head_sha=detail, base_sha=main_sha)
        sandbox = LocalSandbox(f"task-{self.task_id[:12]}-r").create()
        try:
            await checkout(sandbox, self.repo.full_name, token, f"refs/heads/{self.task.branch}", self.task.branch)
            await self._run_commands(sandbox, self.cfg.get("setup") or [], "setup", timeout=1200)
            checks = await self._run_commands(sandbox, self.cfg.get("checks") or [], "check")
        finally:
            await sandbox.close()
        if any(not c["ok"] for c in checks):
            self.feedback += [f"- After rebasing onto the latest {self.repo.default_branch}, `{c['command']}` failed:\n"
                              f"```\n{c['tail']}\n```" for c in checks if not c["ok"]]
            await self.deliver(continue_from_branch=True)
            return
        try:
            await gh.set_status(self.repo.full_name, detail, "success", "Revalidated after rebase by AI Factory",
                                self._task_url())
        except GitHubError:
            pass
        await self.ci_then_merge()

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
                       "or acknowledge.", status="smoke_failed",
                       options=[{"value": "revert", "label": "Revert the merge"},
                                {"value": "fix", "label": "Create a fix outcome"},
                                {"value": "acknowledge", "label": "Acknowledge"}])
        self.event("smoke_passed", "Post-merge smoke checks passed", level="success")
        await self.release()

    async def revert(self) -> None:
        """Bounded recovery: open a revert PR for the merged change, wait for CI, merge it and re-run smoke."""
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
                                            + (f"\n\n[View in AI Factory]({self._task_url()})" if self._task_url() else "")))
        except (GitError, GitHubError) as exc:
            self.block("unblock", f"{self.task.key}: could not open a revert", str(exc), status="smoke_failed",
                       options=[{"value": "acknowledge", "label": "Acknowledge"}])
        self.event("revert_opened", f"Opened revert PR #{pr['number']}", level="warn", url=pr["html_url"])
        deadline = time.monotonic() + self.settings["limits"].get("ci_minutes", 45) * 60
        state = "pending"
        while time.monotonic() < deadline:
            summary = await gh.checks(self.repo.full_name, head)
            state = summary.state
            if state in ("success", "failure") or (state == "none" and time.monotonic() > deadline - 2400):
                break
            await asyncio.sleep(20)
        if state == "failure":
            self.block("unblock", f"{self.task.key}: the revert failed CI",
                       f"Revert PR #{pr['number']} did not pass CI; the original change is still merged.",
                       status="smoke_failed", options=[{"value": "acknowledge", "label": "Acknowledge"}])
        try:
            result = await gh.merge(self.repo.full_name, pr["number"], sha=head, method="squash",
                                    title=f"Revert: {self.task.title} (#{pr['number']})")
        except GitHubError as exc:
            self.block("unblock", f"{self.task.key}: could not merge the revert", str(exc), status="smoke_failed",
                       options=[{"value": "acknowledge", "label": "Acknowledge"}])
        self.evidence("revert", {"pr": pr["number"], "sha": result.get("sha", ""), "ci": state, "at": utcnow().isoformat()})
        self.update(status="reverted", stage="done", finished_at=utcnow(),
                    summary=f"Reverted after post-merge smoke failed (revert PR #{pr['number']})")
        self.event("reverted", f"Reverted via PR #{pr['number']}; {self.repo.default_branch} is back to its previous behaviour",
                   level="warn", url=pr["html_url"])

    async def release(self) -> None:
        release_cfg = self.cfg.get("release") or {}
        if not release_cfg.get("health_url") and release_cfg.get("mode") != "on_merge":
            self._complete("Merged and smoke-tested")
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
                        self._complete("Merged, smoke-tested and released")
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
        self._complete("Merged, smoke-tested; released by deploy-on-merge")

    def _complete(self, summary: str) -> None:
        self.update(status="completed", stage="done", finished_at=utcnow(), summary=summary, blocked_reason="")
        self.event("task_completed", f"{self.task.key} complete: {summary}", level="success")


def _brief(checks: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [{k: c[k] for k in ("command", "ok", "exit_code", "duration")} for c in checks]


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
