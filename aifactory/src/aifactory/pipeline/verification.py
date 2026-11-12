"""Read-only candidate gates shared by delivery and external PR review.

Sandbox selection, credentials, publication, retries and repairs belong to callers.
The review interface accepts the sandbox selected by review_sandbox, not a deliverable
Task. Production PR intake and invocation belong to T4; they are not wired here.
Optional adapters are explicit callables, not guessed imports: no compatible risk,
security-review, semantic-diff or gate-metrics callable exists in this checkout.
"""
from __future__ import annotations

import asyncio
import hashlib
import inspect
import json
import re
import time
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from typing import Any, Callable, Mapping, Protocol

from ..prompts import verifier_system, verify_prompt
from ..gitops import tree_changes, tree_id
from ..sandbox import COMMAND_TIMEOUT, LocalSandbox
from . import runtime
from .check_slots import check_slot
from .qa import AppRun, change_checks, verifier_brief


class VerificationSpec(Protocol):
    key: str
    title: str
    spec: str
    acceptance: list[str]


@dataclass
class VerificationResult:
    setup: list[dict[str, Any]] = field(default_factory=list)
    checks: list[dict[str, Any]] = field(default_factory=list)
    qa: dict[str, Any] | None = None
    report: dict[str, Any] = field(default_factory=dict)
    verdict: str = "fail"
    error: str = ""
    failure_cause: str | None = None
    app_log: str = ""
    run_id: str = ""
    feedback: list[str] = field(default_factory=list)
    optional: dict[str, dict[str, Any]] = field(default_factory=dict)


class VerificationGates:
    def __init__(self, config: dict[str, Any], settings: dict[str, Any], *,
                 event: Callable[..., Any], artifact: Callable[..., Any],
                 claims: dict[str, Any] | None = None):
        self.cfg = config
        self.settings = settings
        self.event = event
        self.artifact = artifact
        self.builder_report = claims or {}
        self.app_summary = None

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
                            timeout: float = COMMAND_TIMEOUT) -> list[dict[str, Any]]:
        """Run each command and record how it ended. A failed row says why in ``cause``: "timeout", or "real" until
        the comparison with the base commit finds it "preexisting". A candidate's check (``kind == "check"``) that
        fails without timing out runs once more in the same sandbox, inside the caller's check slot. If it passes
        then, it counts as passed, labelled "flaky", and keeps the first run's output in ``first_failure``. If it
        fails again, the row describes the second run and keeps the first under ``first``: the comparison with the
        base commit must hold for both, and the cause is "real" even when the second ran out of time. When the two
        did not fail the same way, ``tail`` is the first run's output, the failure that started it. A timeout is
        not run again: a second wait of the same length teaches nothing. Nor is a check whose failed run changed
        the checkout's files (``not_rerun`` is "changed", with ``changed_files``): a second run would test what the
        first run left behind, not the candidate, and could pass where a fresh checkout fails. Only files git can
        see are compared; what a run leaves in ignored files or outside the checkout is not noticed."""
        results = []
        for command in commands:
            if getattr(self, "cancelled", None) is not None and self.cancelled.is_set():
                raise asyncio.CancelledError()
            before = await tree_id(sandbox) if kind == "check" else None
            row, output = await self._run_command(sandbox, command, timeout)
            rerun = kind == "check" and not row["ok"] and not row["timed_out"]
            if rerun:
                after = await tree_id(sandbox)
                if not before or not after:
                    rerun, row["not_rerun"] = False, "unknown"
                elif after != before:
                    rerun = False
                    row |= {"not_rerun": "changed", "changed_files": await tree_changes(sandbox, before, after)}
            if rerun:
                first, first_output = row, output
                row, output = await self._run_command(sandbox, command, timeout)
                if row["ok"]:
                    row |= {"cause": "flaky", "first_failure": first["tail"]}
                    self.event("check_flaky", f"`{command}` failed (exit {first['exit_code']}), then passed when run "
                               "again in the same sandbox: counted as passed", level="warn", command=command,
                               exit_code=first["exit_code"], duration=first["duration"])
                else:
                    same = _same_run(first, row)
                    row |= {"cause": "real", "first": {k: first.get(k) for k in _FIRST_RUN} | {"same": same}}
                    if not same:
                        row["tail"] = first["tail"]
                if row["ok"] or not row["first"]["same"]:  # the output kept below is the second run's
                    self.artifact(kind, f"first run (failed): {command}"[:300], first_output, ok=False,
                                  exit_code=first["exit_code"])
            results.append(row)
            note = (f" — {_how_failed(row)}" if row["timed_out"] or row.get("not_rerun") else
                    " — on a second run" if row.get("cause") == "flaky" else "")
            self.event(f"{kind}_result", f"{'✓' if row['ok'] else '✗'} {command}{note}",
                       level="success" if row["ok"] else "warn", command=command, exit_code=row["exit_code"],
                       duration=row["duration"], **({"cause": row["cause"]} if row.get("cause") else {}))
            self.artifact(kind, command, output, ok=row["ok"], exit_code=row["exit_code"])
        return results

    async def _run_command(self, sandbox: LocalSandbox, command: str, timeout: float) -> tuple[dict[str, Any], str]:
        started = time.monotonic()
        result = await sandbox.exec(command, timeout=timeout)
        row = {"command": command, "ok": result.ok, "exit_code": result.exit_code,
               "duration": round(time.monotonic() - started, 1), "timed_out": result.timed_out,
               "tail": result.output[-4000:]}
        if not result.ok:
            row["failures"] = _failing_tests(result.output)
            row["signature"] = _output_signature(result.output, sandbox)
            printed = result.total_bytes if result.truncated else 0  # the output kept was cut; the count was not
            row |= ({"cause": "timeout", "limit": timeout, "progress": _progress(result.output, printed)}
                    if result.timed_out else {"cause": "real"})
        return row, result.output

    async def _prepare(self, sandbox, kind: str = "verify_setup") -> list[dict[str, Any]]:
        """Install dependencies, unless the VM booted from a prepared environment for these exact lockfiles.
        After a clean install in a VM, save it so the next verification starts ready."""
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


def _normalize_criterion(text: str) -> str:
    """Ignore list formatting, whitespace, trailing punctuation and case, not content."""
    text = re.sub(r"^\s*(?:\d+[.)]|[-*•])\s*", "", text)
    text = " ".join(text.split())
    return re.sub(r"[\W_]+$", "", text).casefold()


async def verify_candidate(
    sandbox: Any, spec: VerificationSpec, repo: Any, base_sha: str, cancelled: asyncio.Event, *,
    workspace_id: str, outcome_id: str, config: dict[str, Any], settings: dict[str, Any],
    event: Callable[..., Any] | None = None, artifact: Callable[..., Any] | None = None,
    claims: dict[str, Any] | None = None, task_id: str | None = None, attempt: int = 1,
    sections: list[str] | None = None, decisions: list[Any] | None = None,
    guidance: list[str] | None = None, outcome: Any = None,
    optional_gates: Mapping[str, Callable[..., Any]] | None = None,
    gates: VerificationGates | None = None, checks: list[dict[str, Any]] | None = None,
    review: bool = True, harness: str = "native", previous: dict[str, Any] | None = None,
    session: Callable[..., Any] | None = None, extra_prompt: str = "",
) -> VerificationResult:
    """Verify an already selected candidate; never close it, publish or repair it.

    By default this is external review: native inference stays in the control
    plane, with no gateway grant. Delivery may retain its configured harness and
    supply checks already run on the identical candidate. Adapters accept keyword
    arguments sandbox, spec, base_sha, cancelled and result; return an evidence
    dict (optionally verdict='fail'). Exceptions are recorded and fail closed.
    Metrics adapters can observe the same result without a separate code path.
    """
    if gates is None:
        if event is None or artifact is None:
            raise ValueError("Verification requires event and artifact sinks")
        gates = VerificationGates(config, settings, event=event, artifact=artifact, claims=claims)
    result = VerificationResult()
    gates.cancelled = cancelled
    app = None
    gates.app_summary = None
    qa_cfg = config.get("qa") or {}
    try:
        if cancelled.is_set():
            raise asyncio.CancelledError()
        async with gates._check_slot(sandbox, bool(config.get("setup") or (checks is None and config.get("checks")))):
            result.setup = await gates._prepare(sandbox)
            result.checks = (await gates._run_commands(sandbox, config.get("checks") or [], "check")
                             if checks is None else checks)
        brief = ""
        if qa_cfg.get("start"):
            app = await gates._start_app(sandbox, qa_cfg)
            brief = verifier_brief(app)
        prompt = verify_prompt(spec, repo, base_sha, gates.builder_report, result.checks,
                               decisions, guidance, outcome=outcome, sections=sections, review=review, previous=previous)
        if cancelled.is_set():
            raise asyncio.CancelledError()
        # Reviews always use the control-plane runner; delivery retains its
        # provider-failure handling around that same runner.
        run_session = runtime.run_agent if review or session is None else session
        run, agent_result, error = await run_session(runtime.SessionSpec(
            workspace_id=workspace_id, outcome_id=outcome_id, task_id=task_id, role="verifier",
            harness="native" if review else harness, system=verifier_system(base_sha), sandbox=sandbox,
            attempt=attempt, writable=False, effective_settings=settings,
            repositories={repo.full_name: config}, expected_criteria=list(spec.acceptance or []),
            prompt=prompt + extra_prompt + brief), cancelled)
        result.run_id = run.id
        result.failure_cause = runtime.carried_cause(run)
        if app is not None:
            await gates._store_app_evidence(app, attempt, run.id)
            result.qa = gates.app_summary
        if agent_result is None or not isinstance(agent_result.report, dict):
            result.error = error or "Verifier session did not produce a report"
        else:
            result.report = dict(agent_result.report)
            report = result.report
            problems = []
            # Delivery already proved some check failures identical on the base.
            # External reviews must never excuse a failing configured check.
            failed = [c for c in result.setup if not c["ok"]] + [
                c for c in result.checks if not c["ok"] and (review or not c.get("preexisting"))]
            if failed:
                problems.append("Configured setup or checks failed")
                result.feedback += [f"- Check `{c['command']}` failed (exit {c['exit_code']}):\n```\n{c['tail']}\n```"
                                    for c in failed]
            criteria = report.get("criteria")
            criteria = criteria if isinstance(criteria, list) else []
            # List formatting is not evidence of a different criterion. Missing
            # criteria remain unknown, never implicit passes (even an empty report).
            def matches(row, criterion):
                return (isinstance(row, dict) and isinstance(row.get("criterion"), str)
                        and _normalize_criterion(row["criterion"]) == _normalize_criterion(criterion))

            missing = [c for c in spec.acceptance if not any(
                matches(row, c) and row.get("status") == "pass" for row in criteria)]
            if missing or any(not isinstance(c, dict) or c.get("status") != "pass" for c in criteria):
                problems.append("Required criteria are not verified")
                report["criteria"] = [*criteria, *[
                    {"criterion": c, "status": "unverified", "evidence": "No passing verifier evidence"}
                    for c in missing if not any(matches(row, c) for row in criteria)]]
            findings = report.get("findings") or []
            if any(isinstance(f, dict) and f.get("severity") == "blocker" for f in findings):
                problems.append("Blocking verifier findings")
            runtime_problem = gates._runtime_problem(app, qa_cfg)
            if runtime_problem:
                problems.append(f"Runtime QA failed: {runtime_problem}")
                report["findings"] = [*findings, {
                    "severity": "blocker", "description": f"Required runtime QA failed: {runtime_problem}",
                    "location": "runtime QA"}]
                if app is not None and app.evidence.get("log_tail"):
                    result.app_log = str(app.evidence['log_tail'])[-1500:]
            if problems and report.get("verdict") == "pass":
                report.update(verdict="fail", summary=". ".join(problems) + ". " + report.get("summary", ""))
            result.verdict = "pass" if report.get("verdict") == "pass" else "fail"
    except Exception as exc:
        if not review:
            raise
        # Review records unavailable evidence, never a weaker fallback or pass.
        result.error = str(exc)[:1000] or type(exc).__name__
        result.verdict = "fail"
    finally:
        if app is not None:
            app.stop()
    await _optional_gates(optional_gates or {}, sandbox, spec, base_sha, cancelled, result, gates)
    if result.report:
        gates.artifact("report", f"Verification (attempt {attempt})", json.dumps(result.report, indent=2),
                       run_id=result.run_id, verdict=result.verdict)
    if result.verdict != "pass":
        report = result.report
        result.feedback.append("- Independent review FAILED. " + report.get("summary", result.error)[:1500])
        result.feedback += [f"  - Criterion not met: {c.get('criterion')} — {str(c.get('evidence', ''))[:600]}"
                            for c in report.get("criteria") or [] if isinstance(c, dict) and c.get("status") != "pass"]
        result.feedback += [f"  - {f['severity']}: {str(f.get('description', ''))[:600]} {f.get('location', '')}"
                            for f in report.get("findings") or []
                            if isinstance(f, dict) and f.get("severity") in ("blocker", "major")]
    if result.app_log:
        result.feedback.append(f"  - App log when it failed:\n```\n{result.app_log}\n```")
    return result


async def _optional_gates(adapters, sandbox, spec, base_sha, cancelled, result, gates):
    for name, adapter in adapters.items():
        if cancelled.is_set():
            raise asyncio.CancelledError()
        try:
            evidence = adapter(sandbox=sandbox, spec=spec, base_sha=base_sha, cancelled=cancelled, result=result)
            if inspect.isawaitable(evidence):
                evidence = await evidence
            if not isinstance(evidence, dict):
                raise TypeError("Optional gate must return an evidence dict")
            result.optional[name] = evidence
        except Exception as exc:
            # An installed adapter's import/runtime errors are failures, not absence.
            result.optional[name] = {"verdict": "fail", "error": str(exc)[:1000]}
        if result.optional[name].get("verdict") == "fail":
            result.verdict = "fail"
            # Keep the concrete evidence in the verdict as well as the adapter
            # artifact, so delivery feedback (including after restart) can act on it.
            detail = json.dumps(result.optional[name], ensure_ascii=False)[:2000]
            finding = {"severity": "blocker", "location": f"optional gate: {name}",
                       "description": f"Optional gate {name} failed: {detail}"}
            result.report = {**result.report, "verdict": "fail", "summary":
                             f"Optional gate {name} failed. " + result.report.get("summary", ""),
                             "findings": [*(result.report.get("findings") or []), finding]}
        gates.artifact("report", f"Optional gate: {name}", json.dumps(result.optional[name]),
                       run_id=result.run_id, gate=name)
        gates.event("verification_optional_gate", f"Optional gate {name}", gate=name, **result.optional[name])


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


def _how_failed(check: dict[str, Any]) -> str:
    """How a failed command ended, in words: a timeout is a timeout, not "exit 124". A check that failed on two
    runs says how each ended, and "in a row" only when both failed the same way. A check that was not run again
    after failing says why not."""
    first, limit = check.get("first") or {}, check.get("limit")
    last = (f"timed out after {float(check.get('duration') or 0):.0f}s"
            + (f" (the limit is {float(limit):.0f}s)" if limit else "")) if check.get("timed_out") else ""
    if not first and check.get("not_rerun"):
        names = ", ".join(str(name) for name in check.get("changed_files") or [])
        return f"failed (exit {check.get('exit_code')}) " + (
            f"and changed files in the checkout{f' ({names})' if names else ''}, so it was not run again"
            if check["not_rerun"] == "changed" else
            "and was not run again, because the state of the checkout could not be read")
    if not first:
        return last or f"failed (exit {check.get('exit_code')})"
    if first.get("same"):
        return f"failed (exit {check.get('exit_code')}) on two runs in a row"
    return (f"failed (exit {first.get('exit_code')}) on its first run, then "
            + (last or f"failed differently (exit {check.get('exit_code')})") + " when run again")


_FIRST_RUN = ("exit_code", "timed_out", "failures", "signature")  # what the comparison with the base commit reads


def _same_run(first: dict[str, Any], second: dict[str, Any]) -> bool:
    """Whether two failed runs of one check failed the same way: neither ran out of time, both ended with the same
    exit code, and they failed the same tests or, where the output names none, printed the same thing."""
    if first.get("timed_out") or second.get("timed_out") or first.get("exit_code") != second.get("exit_code"):
        return False
    key = "signature" if first.get("failures") is None and second.get("failures") is None else "failures"
    return first.get(key) == second.get(key)


def _progress(output: str, total_bytes: int = 0) -> int:
    """How far a command had got when it was stopped at the time limit: how much it had printed, colours removed.
    A sandbox keeps only the start and the end of long output, so what is left is about as long whether the command
    printed a little more than that or gigabytes. ``total_bytes`` is the sandbox's count of everything printed, given
    when the output was cut, and is then the measure."""
    return max(len(_ANSI.sub("", output or "").strip()), total_bytes)
