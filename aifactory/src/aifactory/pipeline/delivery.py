"""Task delivery: build → checks → independent verification → PR → CI → merge → smoke → release.

Each stage is durable (``task.stage``) so work resumes after approvals or restarts without
repeating external side effects. Failures inside an attempt become precise feedback for the
next attempt; only genuine decisions or provider/access problems reach a person.
"""

from __future__ import annotations

import asyncio
import ast
import hashlib
import json
import logging
import posixpath
import re
import shlex
import time
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from typing import Any, Callable

from ..config import get_settings
from ..db import session_scope, utcnow
from ..events import notify, record
from ..failures import RUN_CAPS, failure_cause, text_cause
from ..guided import README_TEMPLATE, outcome_guidance
from .. import merge_manager
from ..github import CheckSummary, GitHub, GitHubError, resolve_credential as resolve_token
from ..gitops import (AUTHOR, DEFAULT_PROTECTED, BranchMoved, GitError, Patch, _mirror_path, git, apply_in_sandbox, checkout,
                      changed_paths, path_overlap, conflict_marker_files, extract_patch, protected_violations, publish, rebase_branch, revert_commit,
                      tree_changes, tree_id)
from ..gitops import patch_id, snapshot_patch
from ..models import Artifact, Decision, Event, Outcome, Repository, Run, Signal, Task, new_id
from ..harness.schemas import _criterion_key
from ..prompts import BUILDER, build_prompt, review_findings, verifier_system, verify_prompt
from ..review import (ACTIONS, FACTORY_REPLY_MARKER, MAX_FINDINGS, SEVERITIES, findings_unseen, headline,
                      new_review_evidence, normalize_feedback, open_findings, reply_text, review_policy, reviewed_by)
from ..sandbox import COMMAND_TIMEOUT, LocalSandbox, SandboxError
from .check_slots import check_slot
from .context import plan_context, repo_instructions
from .qa import AppRun, change_checks, verifier_brief
from .runtime import (SessionSpec, budget_extension, carried_cause, guidance_history, open_decision,
                      outcome_committed, outcome_cost, precedents, publish_decision, run_agent, workspace_settings)

from .lifecycle import transition

log = logging.getLogger(__name__)

CI_POLL_TIMEOUT_SECONDS = 90
REVIEW_POLL_SECONDS = 60
REVIEW_REPLY_TRIES = 3  # polls that may fail to post a round's answers on their threads before the Factory gives up
CHECKPOINT_SECONDS = 300  # between saves of the builder's work in progress while its session runs

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


# These criteria supplement every steward task, including fix-forward repairs. Authorization
# to repair a red build is deliberately separate from approval to weaken its tests.
STEWARD_TEST_CRITERION = (
    "Do not disable, skip, remove or weaken existing tests or check configuration without an "
    "attributed human approval for this exact steward episode and candidate."
)


def _check_paths(commands: list[str]) -> tuple[set[str], bool]:
    """Resolve simple shell command paths; unsupported shell syntax fails closed.

    This is not a shell interpreter. Subshells, expansions and directory stacks need
    human review rather than an optimistic guess about which check script runs.
    """
    paths: set[str] = set()
    unresolved = False
    for command in commands:
        cwd = ""
        try:
            lexer = shlex.shlex(command, posix=True, punctuation_chars=";&|()")
            lexer.whitespace_split = True
            words = list(lexer)
        except ValueError:
            unresolved = True
            continue
        segment: list[str] = []
        if any(word in ("pushd", "popd", "eval", "source", ".", "if", "for", "while") for word in words):
            unresolved = True
        for word in [*words, ";"]:
            if any(c in word for c in "$`\\") or word in ("(", ")", "||", "|", "&"):
                unresolved = True
            if word in ("&&", ";"):
                if segment and segment[0] == "cd":
                    if word != "&&":
                        unresolved = True  # a failed cd followed by ; executes in the old directory
                    if len(segment) != 2 or segment[1].startswith(("-", "/", "~")):
                        unresolved = True
                    else:
                        cwd = posixpath.normpath(posixpath.join(cwd, segment[1]))
                else:
                    if segment and segment[0] not in {"bash", "sh", "python", "python3", "node", "npm", "npx",
                                                       "yarn", "pnpm", "pytest", "uv", "poetry", "make", "cargo",
                                                       "go", "dotnet", "mvn", "gradle", "bundle", "ruby"}:
                        # A bare command may be a repository script reached through PATH.
                        unresolved = True
                    for part in segment:
                        if "/" in part or re.search(r"\.(?:sh|py|js|ts|mjs|cjs)$", part):
                            path = posixpath.normpath(posixpath.join(cwd, part))
                            if path.startswith(("/", "../")):
                                unresolved = True
                            else:
                                paths.add(path)
                segment = []
            else:
                segment.append(word)
    return paths, unresolved


def _test_path(path: str) -> bool:
    return bool(re.search(r"(^|/)(?:tests?|__tests__|specs?)(/|[_.-])|(?:^|/)(?:test_|conftest\.)|"
                          r"(?:[._-](?:test|spec)\.|_test\.)", path, re.I))


def _check_config(path: str) -> bool:
    name = posixpath.basename(path).lower()
    return (name in {"pyproject.toml", "setup.cfg", "tox.ini", "pytest.ini", "package.json", "makefile",
                     ".coveragerc", "conftest.py", "noxfile.py", "setup.py"}
            or bool(re.search(r"(?:jest|vitest|playwright|cypress|karma|mocha|nyc|ava|pytest|coverage|test).*"
                              r"(?:config|rc)|^(?:\.mocharc|\.nycrc|\.taprc)", name))
            or path.startswith(".github/workflows/"))


def _only_new_python_tests(before: str, after: str) -> bool:
    """Only additions of new test definitions are unattended. Module/class directives,
    decorators and every existing function body must remain structurally identical."""
    try:
        old, new = ast.parse(before), ast.parse(after)
    except (SyntaxError, ValueError, RecursionError):
        return False

    def strip_added(old_body, new_body):
        names = {n.name for n in old_body if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef))}
        kept = []
        for node in new_body:
            if (isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
                    and node.name.startswith("test") and node.name not in names):
                # These expressions execute at definition/import time, outside the new
                # test body. Only literal defaults/annotations are safe to ignore.
                annotations = [arg.annotation for arg in [*node.args.posonlyargs, *node.args.args,
                                                          *node.args.kwonlyargs,
                                                          *([node.args.vararg] if node.args.vararg else []),
                                                          *([node.args.kwarg] if node.args.kwarg else [])]]
                expressions = [*node.args.defaults, *node.args.kw_defaults, *annotations, node.returns]
                if (node.decorator_list or getattr(node, "type_params", [])
                        or any(expr is not None and not isinstance(expr, ast.Constant) for expr in expressions)):
                    kept.append(node)  # executable additions must be reviewed, not stripped
                continue
            if isinstance(node, ast.ClassDef):
                prior = next((n for n in old_body if isinstance(n, ast.ClassDef) and n.name == node.name), None)
                if prior:
                    node.body = strip_added(prior.body, node.body)
            kept.append(node)
        return kept

    new.body = strip_added(old.body, new.body)
    return ast.dump(old) == ast.dump(new)


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


def infrastructure_problem(error: str, cause: str | None = None) -> str:
    """Why a session or sandbox failed when it was not the attempt's doing: "provider" (billing, quota or access; a
    person acts), "transient" (the provider or the sandbox was unavailable; the attempt is redone later), or "" (the
    attempt's own failure, including its turn, time, token and cost caps).

    ``cause`` is what the failure itself said, where it happened (runtime.carried_cause), and it decides, whatever
    the wording. Without one the error's wording is read (failures.text_cause): the Codex CLI, sandbox and GitHub
    errors, and runs recorded before causes were carried."""
    return text_cause(error) if cause is None else cause


class Stop(Exception):
    """The task paused or ended for a recorded reason; the orchestrator should release its slot."""


class TaskRunner:
    def __init__(self, task_id: str, cancelled: asyncio.Event, repo_lock: Callable[[str], asyncio.Lock],
                 owner: str | None = None):
        self.task_id = task_id
        self.cancelled = cancelled
        self.repo_lock = repo_lock
        # The instance whose lease this runner works under: once another instance holds the task, its writes stop
        # (see lifecycle.transition). None: no check (CI polling, which is not leased).
        self.owner = owner
        self.builder: LocalSandbox | None = None
        self.feedback: list[str] = []
        self.patch: Patch | None = None
        self.builder_report: dict[str, Any] = {}
        self.verifier_report: dict[str, Any] = {}
        self.checks: list[dict[str, Any]] = []
        self.app_summary: dict[str, Any] | None = None
        # What the builder's sandbox holds, for the builder's prompt (see prompts.working_tree).
        self._tree: dict[str, Any] = {}
        self._ci_repairing = False
        # The repository's agent instructions, by base commit and whether they are for the verifier.
        self._instructions: dict[tuple[str, bool], list[str]] = {}
        self._load()
        self.feedback = self._prior_feedback()
        # Where the latest attempt's results sit in ``self.feedback``. Every other line is a note for the round (a
        # setup problem, a branch conflict, a CI failure, revalidation feedback), kept until a review passes.
        self._results = slice(0, len(self.feedback))
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
        """The latest attempt's results, rebuilt from the evidence so they survive retries and restarts. What an
        earlier review left open is added when the builder's prompt is built (see ``_builder_feedback``)."""
        for attempt in reversed((self.task.evidence or {}).get("attempts", [])):
            result = attempt.get("result") if isinstance(attempt, dict) else None
            if result is None or result in NOT_ATTEMPTS or result == "builder_blocked":
                continue  # not the end of an attempt: what came before it still stands
            verdict, failed = attempt.get("verdict"), [c for c in _objects(attempt.get("checks")) if not c.get("ok")]
            if result == "verification_failed" and isinstance(verdict, dict) and verdict:
                return _review_feedback(verdict, str(attempt.get("app_log") or ""))
            if result == "checks_failed" and failed:
                return [f"- Check `{c.get('command')}` failed (exit {c.get('exit_code')})"
                        + (" (it fails the same way on the base commit)" if c.get("preexisting") else "")
                        + (f":\n```\n{c['tail']}\n```" if c.get("tail") else "") for c in failed]
            if result == "builder_failed":
                return [f"- The previous builder session ended without a report: {attempt.get('error') or ''}"]
            if result == "policy" and attempt.get("detail"):
                return [str(attempt["detail"])]
            return [f"- The previous attempt failed: {result}"] if result in ("checks_failed", "policy") else []
        return []

    def _builder_feedback(self) -> list[str]:
        """What the next builder session is told: the latest attempt's results in full, then the round's notes, then
        what the most recent review left open when the latest attempt did not reach review. From here on, lines
        added to ``self.feedback`` are the results of the attempt that is starting."""
        results = self.feedback[self._results]
        del self.feedback[self._results]
        self._results = slice(len(self.feedback), None)
        lines, room = [], FEEDBACK_CHARS
        items = list(dict.fromkeys(results + self.feedback))
        for i, item in enumerate(items):
            if len(item) > room:
                left = len(items) - i - 1
                lines.append(f"{item[:room]}\n… (cut for length; {left} more feedback item(s) left out)")
                break
            lines.append(item)
            room -= len(item)
        review = self._open_review()
        if review and _review_feedback(review[1])[0] not in results:
            points = _open_points(review[1])
            if points:
                lines += ["", f"## Still open from the review of attempt {review[0]}", *points]
        return lines

    def _open_review(self) -> tuple[Any, dict[str, Any]] | None:
        """The task's most recent review, when it failed: (attempt, verdict). None once a review has passed."""
        for attempt in reversed((self.task.evidence or {}).get("attempts", [])):
            if isinstance(attempt, dict) and attempt.get("result") in ("verified", "verification_failed"):
                verdict = attempt.get("verdict")
                failed = attempt["result"] == "verification_failed" and isinstance(verdict, dict) and verdict
                return (attempt.get("n"), verdict) if failed else None
        return None

    def _previous_review(self, patch: Patch) -> dict[str, Any] | None:
        """The last verdict on this task within the current budget and which files' diffs changed since, for the
        verifier's prompt (see ``prompts.previous_review``). The file lists are None when the diff that review saw
        cannot be loaded."""
        last = next((a for a in reversed(self._budget_attempts())
                     if isinstance(a.get("verdict"), dict) and a["verdict"].get("verdict")), None)
        if last is None:
            return None
        before = self._stored_diff(last)
        changed, unchanged = _diff_changes(before, patch.diff) if before is not None else (None, None)
        return {"attempt": last.get("n"), "verdict": last["verdict"], "changed": changed, "unchanged": unchanged,
                "base": last.get("base_sha") or ""}

    def _stored_diff(self, attempt: dict[str, Any]) -> bytes | None:
        """The diff an attempt's review saw, or None when it was not stored whole."""
        if not attempt.get("diff_artifact_id") or not attempt.get("patch_sha256"):
            return None
        with session_scope() as s:
            diff = s.get(Artifact, attempt["diff_artifact_id"])
            raw = (diff.content or "").encode() if diff is not None else None
        return raw if raw is not None and hashlib.sha256(raw).hexdigest() == attempt["patch_sha256"] else None

    def _reviewed_diff(self, patch: Patch, attempt: int) -> str:
        """The stored diff of the candidate under review: the attempt's own diff artifact, or a new one when the
        candidate has none (a revalidation reviews the rebased change)."""
        content = patch.diff.decode("utf-8", "replace")
        with session_scope() as s:
            latest = s.query(Artifact).filter(Artifact.task_id == self.task_id, Artifact.kind == "diff").order_by(
                Artifact.created_at.desc()).first()
            if latest is not None and latest.content == content[:400_000]:
                return latest.id
        return self.artifact("diff", f"Attempt {attempt} diff as reviewed", content, files=patch.files,
                             additions=patch.additions, deletions=patch.deletions)

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
            if (task.status, task.stage) != (self.task.status, self.task.stage) or not transition(
                    s, task, owner=self.owner, **fields):
                raise Stop()
            for key, value in fields.items():
                setattr(self.task, key, value)
        notify(self.ws, "task", self.task_id, outcome_id=self.task.outcome_id)

    def event(self, kind: str, message: str, level: str = "info", **data: Any) -> None:
        record(self.ws, kind, message, outcome_id=self.task.outcome_id, task_id=self.task_id, level=level,
               data={"attempt": self.task.attempt, "head": self.task.head_sha, **data})

    def evidence(self, key: str, value: Any) -> None:
        self._set_evidence({key: value})

    def _set_evidence(self, values: dict[str, Any], wip: Patch | None = None) -> None:
        """Record evidence keys in one transaction ("attempts" appends). ``wip`` is the work in progress that a new
        "wip" record describes: the task's one artifact of that kind holds it from this write on."""
        with session_scope() as s:
            task = s.get(Task, self.task_id)
            # Deep copy: mutating the stored JSON in place would hide the change from the ORM.
            ev = json.loads(json.dumps(task.evidence or {}))
            stored = (ev.get("wip") or {}).get("artifact_id")
            for key, value in values.items():
                if key == "attempts":  # tagged with the budget it belongs to (attempt numbers restart per budget)
                    ev.setdefault("attempts", []).append({**value, "epoch": int(ev.get("budget_epoch", 0))})
                    if value.get("result") not in NOT_ATTEMPTS:
                        ev.pop("candidate", None)  # the attempt ended: its saved candidate is never resumed
                        ev.pop("wip", None)  # and no later attempt starts from its work in progress
                    if recovered_from_infra(str(value.get("result")), ev.get("infra_phase")):
                        ev.pop("infra_retries", None)  # the phase that failed has since completed
                        ev.pop("infra_phase", None)
                    continue
                if key in ("ci", "smoke"):
                    value = {**value, "at": utcnow().isoformat()}
                if key in ("ci", "smoke") and value.get("state") in ("failure", "failed"):
                    ev.setdefault("failure_facts", []).append({"kind": key, **value,
                                                              "attempt": task.attempt, "at": utcnow().isoformat()})
                if key == "candidate" and value:
                    ev.pop("wip", None)  # built: a stop from here on resumes the candidate, not the work behind it
                ev[key] = value
            if wip is not None or (stored and not (ev.get("wip") or {}).get("artifact_id")):
                self._store_wip(s, ev.get("wip"), wip)
            if self.owner is None:
                task.evidence = ev
            elif not s.query(Task).filter(Task.id == self.task_id, Task.lease_owner.in_(("", self.owner))).update(
                    {Task.evidence: ev}, synchronize_session=False):
                raise Stop()  # another instance holds the task now: its evidence is not this runner's to write
            self.task.evidence = ev

    def _store_wip(self, s, saved: dict[str, Any] | None, patch: Patch | None) -> None:
        """Keep the task's one work-in-progress artifact in step with its record (``evidence["wip"]``), in the
        transaction that writes the record: a new save replaces the artifact's content in place, and a record that
        no longer names an artifact leaves none behind."""
        rows = s.query(Artifact).filter(Artifact.task_id == self.task_id, Artifact.kind == "wip")
        if patch is None:
            rows.delete(synchronize_session=False)
            return
        row, *extra = rows.order_by(Artifact.created_at).all() or [None]
        for other in extra:
            s.delete(other)
        if row is None:
            row = Artifact(id=new_id(), workspace_id=self.ws, outcome_id=self.task.outcome_id, task_id=self.task_id,
                           kind="wip")
            s.add(row)
        row.name, row.content = f"Attempt {saved['attempt']} work in progress", patch.diff.decode()
        row.data = {"files": patch.files, "additions": patch.additions, "deletions": patch.deletions}
        row.created_at = utcnow()
        saved["artifact_id"] = row.id

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
              options: list[dict[str, Any]] | None = None,
              metadata: dict[str, Any] | None = None) -> None:
        with session_scope() as s:
            task = s.get(Task, self.task_id)
            if not transition(s, task, owner=self.owner, status=status, blocked_reason=body[:2000], lease_owner=""):
                raise Stop()
            # The decision and state transition commit together; no second transaction
            # can create a retry decision after a concurrent stop.
            decision = Decision(workspace_id=self.ws, kind=kind, title=title, body=body,
                                outcome_id=self.task.outcome_id, task_id=self.task_id, metadata_json=metadata,
                                options=options or [{"value": "retry", "label": "Retry"},
                                                    {"value": "abandon", "label": "Abandon task"}])
            s.add(decision)
            s.flush()
            decision_id = decision.id
        publish_decision(self.ws, decision_id, kind, title, outcome_id=self.task.outcome_id, task_id=self.task_id)
        notify(self.ws, "task", self.task_id, outcome_id=self.task.outcome_id)
        raise Stop()

    # steward candidates ---------------------------------------------------------
    def _steward_question(self, title: str, body: str, *, status: str = "needs_input",
                          options: list[dict[str, Any]] | None = None,
                          metadata: dict[str, Any] | None = None) -> None:
        """Task questions resume on ``answer``, not the default ``unblock`` retry.

        Keep test approval's explicit, candidate-scoped answer separate from repair
        authorization. Other steward questions let a person give guidance and
        requeue delivery through its existing rebuild/verification gates.
        """
        self.block("question", title, body, status=status,
                   options=options or [{"value": "answer", "label": "Answer and continue"},
                                       {"value": "abandon", "label": "Abandon task"}],
                   metadata=metadata)

    def _steward_identity(self) -> dict[str, Any] | None:
        """T5/T6 contract: persist evidence.steward with episode_id, action (fix/revert),
        authorized=True and an exact culprit_sha (mandatory for revert). A merge revert
        additionally needs confirmed first_parent_sha evidence. Never use merge_sha here.
        """
        with session_scope() as s:
            ev = s.get(Task, self.task_id).evidence or {}
            meta = ev.get("steward")
        if meta is None:
            return None
        if not (isinstance(meta, dict) and meta.get("episode_id") and meta.get("authorized") is True
                and meta.get("action") in ("fix", "revert")
                and (meta.get("action") != "revert" or re.fullmatch(r"[0-9a-f]{40,64}", str(meta.get("culprit_sha", ""))))):
            self._steward_question("Steward authorization is incomplete",
                       "Record an authorized episode_id, action and exact culprit SHA before delivery.",
                       status="needs_input")
        return {k: meta.get(k) for k in ("episode_id", "action", "culprit_sha", "first_parent_sha",
                                        "first_parent_confirmed")}

    def _bind_steward_candidate(self, patch: Patch) -> None:
        identity = self._steward_identity()
        if identity is not None:
            self.evidence("steward_candidate", {"identity": identity, "base": self.task.base_sha,
                                                 "patch_sha256": hashlib.sha256(patch.diff).hexdigest()})

    def _guard_steward_candidate(self) -> None:
        identity = self._steward_identity()
        with session_scope() as s:
            candidate = (s.get(Task, self.task_id).evidence or {}).get("steward_candidate")
        if identity is None and not candidate:
            return
        if not candidate or candidate.get("identity") != identity:
            self._set_evidence({"steward_rejection": {"reason": "authorization changed", "candidate": candidate},
                                "candidate": None, "steward_candidate": None, "steward_test_gate": None,
                                "revalidation": None, "fix_round": ""})
            self.update(stage="prepare")
            self._steward_question("Steward candidate authorization changed",
                       "Discard this reviewed candidate and rebuild for the current episode/action/culprit; "
                       "it was not published or merged.", status="needs_input")
        gate = (self.task.evidence or {}).get("steward_test_gate") or {}
        scope = gate.get("scope") or gate
        if gate and (scope.get("checks") != list(self.cfg.get("checks") or [])
                     or scope.get("setup") != list(self.cfg.get("setup") or [])):
            self._set_evidence({"steward_rejection": {"reason": "check configuration changed",
                                                       "candidate": candidate},
                                "candidate": None, "steward_candidate": None, "steward_test_gate": None,
                                "revalidation": None, "fix_round": ""})
            self.update(stage="prepare")
            self._steward_question("Steward check configuration changed",
                       "Rebuild and rerun checks and independent verification with the current configuration.",
                       status="needs_input")
        if self.patch is not None and candidate.get("patch_sha256") != hashlib.sha256(self.patch.diff).hexdigest():
            self._set_evidence({"candidate": None, "steward_candidate": None, "steward_test_gate": None,
                                "revalidation": None, "fix_round": ""})
            self.update(stage="prepare")
            self._steward_question("Steward candidate changed", "Rebuild and independently verify the exact patch.",
                       status="needs_input")

    async def _steward_merge_policy_gate(self, token: str) -> None:
        """Evaluate today's path/size policy against the exact published head, in isolation."""
        if self._steward_identity() is None:
            return
        sandbox = LocalSandbox(f"task-{self.task_id[:12]}-merge-policy").create()
        try:
            await checkout(sandbox, self.repo.full_name, token, self.task.head_sha, self.task.branch)
            patch = await extract_patch(sandbox, self.task.base_sha)
            candidate = (self.task.evidence or {}).get("steward_candidate") or {}
            # Git may serialize the same tree differently after a rebase; the binding is refreshed
            # only by the revalidation verifier, never here at the merge boundary.
            if candidate.get("patch_sha256") != hashlib.sha256(patch.diff).hexdigest():
                self._steward_question("Published steward candidate changed",
                           "The published patch differs from the reviewed candidate. Rebuild and verify it.",
                           status="needs_input")
            problem = await self._policy_problem(patch)
            if problem:
                self._steward_question("Steward candidate violates current policy", problem, status="needs_input")
            previous, self.builder = self.builder, sandbox
            try:
                await self._steward_test_gate(patch)
            finally:
                self.builder = previous
        except (GitError, SandboxError, OSError) as exc:
            self._steward_question("Steward candidate could not be rechecked",
                       f"No merge was attempted. Rebuild or retry after restoring sandbox access: {exc}",
                       status="needs_input")
        finally:
            await sandbox.close()

    async def _prepare_steward_revert(self, identity: dict[str, Any]) -> None:
        sha = identity["culprit_sha"]
        if not (await extract_patch(self.builder, self.task.base_sha)).empty:
            self._steward_question("Steward revert checkout is not clean",
                       "Repository setup changed tracked files. A deterministic inverse must start from the exact "
                       "default-branch base; review setup before retrying.", status="needs_input")

        async def checked(command: str):
            result = await self.builder.exec(command, timeout=120)
            if not result.ok:
                self.evidence("steward_rejection", {"identity": identity, "base": self.task.base_sha,
                                                    "reason": result.output[-1500:]})
                self._steward_question("Steward revert needs a human decision",
                           f"Cannot revert {sha} on {self.task.base_sha}: nonancestor, already reverted, empty or "
                           f"conflicting candidate. Inspect the culprit and choose a fix or a new revert.\n{result.output[-1500:]}",
                           status="needs_input")
            return result.output.strip()
        await checked(f"git merge-base --is-ancestor {sha} HEAD")
        parents = (await checked(f"git rev-list --parents -n 1 {sha}")).split()
        mainline = ""
        if len(parents) > 2:
            if not (len(parents) == 3 and identity.get("first_parent_confirmed") is True
                    and identity.get("first_parent_sha") == parents[1]):
                self._steward_question("Merge revert mainline is unconfirmed",
                           f"Confirm first-parent evidence for {sha} (actual first parent {parents[1]}) or choose "
                           "a fix-forward repair. No mainline was guessed.", status="needs_input")
            mainline = "-m 1 "
        await checked(f"git revert --no-commit {mainline}{sha}")
        patch = await extract_patch(self.builder, self.task.base_sha)
        if patch.empty:
            self._steward_question("Steward revert is empty", f"{sha} has no inverse change on the latest default "
                       "branch; it may already be reverted. Choose a different repair.", status="needs_input")
        self.builder_report = {"summary": f"Deterministic revert of {sha}", "tests": []}
        self.evidence("builder_report", self.builder_report)

    async def _steward_test_gate(self, patch: Patch) -> None:
        identity = self._steward_identity()
        if identity is None:
            return
        paths, unresolved = _check_paths(list(self.cfg.get("checks") or []) + list(self.cfg.get("setup") or []))
        risky = []
        # Read trusted base blobs from the mirror, bounded before crossing the sandbox boundary.
        for path in patch.files:
            if _check_config(path) or path in paths or unresolved:
                risky.append(path)
                continue
            if not _test_path(path):
                continue
            try:
                spec = self.task.base_sha + ":" + path
                # The mirror is trusted, and the size is established before loading the blob.
                size = int((await git("cat-file", "-s", spec, cwd=_mirror_path(self.repo.full_name))).strip())
            except GitError:
                # Distinguish a genuinely new test from an unreadable/missing base.
                try:
                    tree = await git("--literal-pathspecs", "ls-tree", self.task.base_sha, "--", path,
                                     cwd=_mirror_path(self.repo.full_name))
                except GitError:
                    risky.append(path)
                else:
                    if tree.strip():
                        risky.append(path)
                    elif path.endswith(".py"):
                        from ..sandbox import read_text
                        try:
                            if not _only_new_python_tests("", await read_text(self.builder, path)):
                                risky.append(path)
                        except (SandboxError, OSError, UnicodeError):
                            risky.append(path)
                    else:
                        risky.append(path)
                continue
            except ValueError:
                risky.append(path)
                continue
            if size > 400_000 or not path.endswith(".py"):
                risky.append(path)
                continue
            from ..sandbox import read_text
            try:
                before = await git("show", spec, cwd=_mirror_path(self.repo.full_name))
                if not _only_new_python_tests(before, await read_text(self.builder, path)):
                    risky.append(path)
            except (GitError, SandboxError, OSError, UnicodeError):
                risky.append(path)
        if not risky:
            self.evidence("steward_test_gate", {
                "approved": True, "patch_sha256": hashlib.sha256(patch.diff).hexdigest(),
                "checks": list(self.cfg.get("checks") or []), "setup": list(self.cfg.get("setup") or [])})
            return
        scope = {"episode_id": identity["episode_id"], "identity": identity, "base": self.task.base_sha,
                 "patch_sha256": hashlib.sha256(patch.diff).hexdigest(), "paths": sorted(risky),
                 "checks": list(self.cfg.get("checks") or []), "setup": list(self.cfg.get("setup") or [])}
        with session_scope() as s:
            rows = s.query(Decision).filter_by(task_id=self.task_id, kind="question", status="resolved",
                                               resolution="answer").all()
            approved = any(row.resolved_by and row.resolved_at and row.metadata_json == {"steward_test_approval": scope}
                           and (not row.attribution or row.attribution.get("source") in ("human", "mind")) for row in rows)
        self.evidence("steward_test_gate", {"scope": scope, "approved": approved})
        if not approved:
            self._steward_question("Approve test/check changes for this steward candidate",
                       "This repair changes existing tests or check configuration: " + ", ".join(risky)
                       + ". Auto fix/revert authorization does not approve disabling tests. Review the exact diff "
                       "before approving; approval covers only this episode, base and patch.", status="needs_input",
                       options=[{"value": "answer", "label": "Approve these test/check changes"},
                                {"value": "abandon", "label": "Abandon task"}],
                       metadata={"steward_test_approval": scope})

    # entry ---------------------------------------------------------------------
    async def run(self) -> None:
        try:
            if not self.task.started_at:
                self.update(started_at=utcnow())
            identity = self._steward_identity()
            if identity is not None and STEWARD_TEST_CRITERION not in self.task.acceptance:
                self.update(acceptance=[*self.task.acceptance, STEWARD_TEST_CRITERION])
            stage = self.task.stage
            if identity is not None and self.task.merge_sha and stage not in ("smoke", "release"):
                self._steward_question("Steward repair task already has a merge record",
                           "Use a new repair task rather than overwriting a completed merge.", status="needs_input")
            if stage in ("merge", "ci", "ci_refresh", "ci_repair"):
                self._guard_steward_candidate()
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
            elif stage in ("ci", "review"):  # a review wait that was requeued passes through the CI wait again
                await self.ci_then_merge()
            elif stage == "smoke":
                await self.smoke()
            elif stage == "release":
                await self.release()
            elif stage == "revise":
                await self.deliver(continue_from_branch=True)
            elif stage == "revert":
                if identity is not None:
                    await self.deliver()
                else:
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
        identity = self._steward_identity()
        if identity is not None and identity["action"] == "revert":
            if self.task.merge_sha:
                self._steward_question("A steward repair requires a separate task",
                           "This task already records a completed merge. Create a new repair task; its merge "
                           "record must not be overwritten.", status="needs_input")
            continue_from_branch = False  # regenerate the inverse, never replay or revise a prior inverse
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
        # Built before the task stopped (a restart, or an infrastructure failure at its checks or verification):
        # the attempt goes on from there below, and the builder does not run again.
        saved = self._saved_candidate()
        if saved:
            if saved["n"] == self.task.attempt:
                self.update(attempt=self.task.attempt - 1)
        elif self.task.attempt and (self.task.attempt not in finished or unpublished):
            # Every attempt that ends records its result; one without a record was interrupted (a restart or
            # redeploy), so it is redone rather than counted against the limit.
            self.event("attempt_resumed", f"Attempt {self.task.attempt} was interrupted; running it again")
            self.update(attempt=self.task.attempt - 1)
        while True:
            attempt = self.task.attempt + 1
            if attempt - int((self.task.evidence or {}).get("attempt_base", 0)) > max_attempts and self._auto_retry():
                attempt = self.task.attempt + 1
            if attempt - int((self.task.evidence or {}).get("attempt_base", 0)) > max_attempts:
                summary = "\n".join(self.feedback[self._results] or self.feedback)[:3000]
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
            if saved:
                self._guard_steward_candidate()
                identity = self._steward_identity()
                if identity is not None and identity["action"] == "revert":
                    main_sha = await GitHub(token).branch_sha(self.repo.full_name, self.repo.default_branch)
                    if main_sha != self.task.base_sha:
                        await self._refresh_onto_main(token, GitHub(token), main_sha)
                candidate, saved = saved, None
                patch = await self._resume_candidate(token, attempt, candidate)
                if patch is not None:
                    if self._steward_identity() is not None:
                        # Setup is rerun on restoration. Check the actual tree, not just the saved diff.
                        self.patch = await extract_patch(self.builder, self.task.base_sha)
                        self._guard_steward_candidate()
                        problem = await self._policy_problem(self.patch)
                        if problem:
                            self._steward_question("Resumed steward candidate violates policy", problem)
                        await self._steward_test_gate(self.patch)
                    if await self._check_and_verify(token, attempt, patch, candidate["diff_artifact_id"],
                                                    candidate.get("checks")):
                        break
                    continue
            identity = self._steward_identity()
            if self.builder is not None and identity is not None and identity["action"] == "revert":
                await self.builder.close()
                self.builder = None

            if self.builder is None:
                try:
                    await self._prepare_builder(token, from_branch=continue_from_branch and bool(self.task.branch))
                except (SandboxError, GitError, OSError) as exc:  # the environment, not the attempt
                    error = f"Sandbox setup failed: {exc}"
                    self._infrastructure(attempt, "prepare", error, infrastructure_problem(error))
            self._describe_tree(attempt)
            self.stage("build", f"Attempt {attempt}: building with {self.task.harness}")
            sections = await self._sections()
            if identity != self._steward_identity():
                self._steward_question("Steward authorization changed during preparation",
                           "Rebuild for the current episode, action and culprit.", status="needs_input")
            run = None
            if identity is not None and identity["action"] == "revert":
                await self._prepare_steward_revert(identity)
            else:
                findings, unlisted = self._round_findings()
                spec = SessionSpec(
                    workspace_id=self.ws, outcome_id=self.task.outcome_id, task_id=self.task_id, role="builder",
                    effective_settings=self.settings, repositories={self.repo.full_name: self.cfg},
                    harness=self.task.harness, system=BUILDER, sandbox=self.builder, attempt=attempt, writable=True,
                    expected_findings=[f["id"] for f in findings] or None,
                    prompt=build_prompt(self.task, self.outcome, self.repo, self._builder_feedback(),
                                        precedents(self.ws, self.repo.id),
                                        guidance_history(self.task.outcome_id, self.task_id), sections,
                                        review_findings(findings, unlisted)))
                async with self._checkpoints(fix_round):
                    run, result, error = await self._session(attempt, "build", spec)
                if result is None:
                    cause = infrastructure_problem(error, carried_cause(run))
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
                if identity is not None and identity["action"] == "revert":
                    self._steward_question("Steward revert violates candidate policy", problem, status="needs_input")
                self.feedback.append(problem)
                self.evidence("attempts", {"n": attempt, "result": "policy", "detail": problem,
                                           "files": len(patch.files)})
                self.event("candidate_rejected", problem, level="warn")
                continue
            if identity != self._steward_identity():
                self._steward_question("Steward authorization changed during building",
                           "Discard the candidate and rebuild for the current authorization.", status="needs_input")
            self._bind_steward_candidate(patch)
            diff_id = self.artifact("diff", f"Attempt {attempt} diff", patch.diff.decode("utf-8", "replace"),
                                    run_id=run.id if run else None, files=patch.files,
                                    additions=patch.additions, deletions=patch.deletions)
            await self._steward_test_gate(patch)
            self.patch = patch
            # Saved as soon as it exists: a stop from here on resumes this candidate instead of building it again.
            self.evidence("candidate", {
                "n": attempt, "epoch": int((self.task.evidence or {}).get("budget_epoch", 0)),
                "diff_artifact_id": diff_id, "patch_sha256": hashlib.sha256(patch.diff).hexdigest(),
                "base_sha": self.task.base_sha, "run_id": run.id if run else None, "files": patch.files,
                "additions": patch.additions, "deletions": patch.deletions, "fix_round": fix_round, "checks": None})
            if await self._check_and_verify(token, attempt, patch, diff_id):
                break

        await self._publish(token)
        await self.ci_then_merge()

    @asynccontextmanager
    async def _checkpoints(self, fix_round: str):
        """While the builder's session runs, save its work in progress every ``CHECKPOINT_SECONDS``, and once more
        when the service stops the session, so a restart continues from the save (see ``_earlier_changes``). Not in
        a fix round: its checkout of the pull request's branch restores nothing. No save starts or is stored once
        this returns; a snapshot cut off at that moment only ever wrote files of its own (see ``snapshot_patch``)."""
        async def periodically() -> None:
            while True:
                await asyncio.sleep(CHECKPOINT_SECONDS)
                await self._save_progress()

        async def stopped(saving: asyncio.Task | None) -> None:
            if saving is not None:
                saving.cancel()
                await asyncio.gather(saving, return_exceptions=True)

        saving = None if fix_round else asyncio.create_task(periodically())
        try:
            yield
        except asyncio.CancelledError:
            await stopped(saving)
            # The service is stopping (a person's cancel sets the event): the sandbox is there until run() closes
            # it, and stop() waits a few seconds for this task. A second cancellation ends the save.
            if not fix_round and not self.cancelled.is_set():
                await self._save_progress(timeout=5)
            raise
        finally:
            await stopped(saving)

    async def _save_progress(self, timeout: float = 60) -> None:
        """Save what the builder's tree holds beyond the base commit, read without touching its index: kept in the
        task's one "wip" artifact and described in ``evidence["wip"]``. Only when the session has changed something
        (the tree is not empty, and not the candidate this runner last judged, which an earlier attempt left in it)
        and it differs from the last save. A diff that cannot be stored (see ``_unsaveable``) is not saved: the last
        save stays, and the reason is recorded once. Never fails: the attempt goes on without the save."""
        try:
            async with asyncio.timeout(timeout):
                patch = await snapshot_patch(self.builder, self.task.base_sha, timeout=timeout)
            last = self._saved_progress() or {}  # another attempt's save is replaced by this attempt's first one
            mine = {"attempt": self.task.attempt, "epoch": int((self.task.evidence or {}).get("budget_epoch", 0))}
            digest = hashlib.sha256(patch.diff).hexdigest()
            if (patch.empty or digest == last.get("patch_sha256")
                    or (self.patch is not None and patch.diff == self.patch.diff)):
                return
            why = self._unsaveable(patch)
            if why:
                if not last.get("skipped"):
                    self.evidence("wip", {**(last or mine), "skipped": why})
                return
            self._set_evidence({"wip": {
                **mine, "artifact_id": None, "patch_sha256": digest, "base_sha": self.task.base_sha,
                "saved_at": utcnow().isoformat(), "files": len(patch.files), "additions": patch.additions,
                "deletions": patch.deletions}}, wip=patch)
        except Exception as exc:
            log.warning("Task %s: could not save the builder's work in progress: %s", self.task_id, str(exc)[:300])

    def _unsaveable(self, patch: Patch) -> str:
        """Why a snapshot of work in progress is not stored, or "". A save must reload byte for byte (see
        ``_stored_candidate``). And it must hold nothing the candidate gate keeps out of a stored diff: of the
        gate's rules only the protected paths do that (they include the files that hold secrets), the others
        judge what may be published, which a save never is."""
        bad = protected_violations(patch.files, list(self.cfg.get("protected_paths") or []) + DEFAULT_PROTECTED)
        if bad:
            return f"the changes include protected paths: {', '.join(bad[:5])}"[:300]
        try:
            text = patch.diff.decode()
        except UnicodeDecodeError:
            return "the diff is not all UTF-8 text"
        if "\0" in text:
            return "the diff is not all text"
        return "the diff is larger than 400,000 characters" if len(text) > 400_000 else ""

    def _saved_progress(self) -> dict[str, Any] | None:
        """The record of the work in progress saved for the attempt this run is on (``evidence["wip"]``); the task
        has that attempt's number from before its sandbox is prepared until it ends. A record of another attempt or
        budget is never used."""
        ev = self.task.evidence or {}
        saved = ev.get("wip")
        if (isinstance(saved, dict) and saved.get("attempt") == self.task.attempt
                and saved.get("epoch") == int(ev.get("budget_epoch", 0))):
            return saved
        return None

    async def _check_and_verify(self, token: str, attempt: int, patch: Patch, diff_id: str,
                                checks: list[dict[str, Any]] | None = None) -> bool:
        """The candidate gate from the configured checks on; True once the candidate is verified. ``checks`` are the
        results a resumed candidate's checks had before the task stopped: they are not run again."""
        identity = self._steward_identity()
        seen = {"diff_artifact_id": diff_id, "patch_sha256": hashlib.sha256(patch.diff).hexdigest()}
        if checks is None:
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
                if identity is not None and identity["action"] == "revert":
                    self._steward_question("Steward revert failed configured checks",
                               "The exact inverse failed checks. Inspect the check artifacts and choose a repair.",
                               status="needs_input")
                self.feedback += [f"- Check `{c['command']}` {_how_failed(c)}{c.get('vs_base', '')}"
                                  f"{_whose_output(c)}:\n```\n{c['tail']}\n```" for c in failed]
                # With its diff: a new sandbox for the next attempt starts from these changes (_earlier_changes).
                self.evidence("attempts", {"n": attempt, "result": "checks_failed", "checks": _brief(self.checks),
                                           **seen, "base_sha": self.task.base_sha})
                return False
            # A stop during verification resumes there, with these results.
            self.evidence("candidate", {**self.task.evidence["candidate"], "checks": _shown(self.checks)})
        else:
            self.checks = checks

        # independent verification; nothing the builder left running (databases, dev servers) may compete
        # with the verifier for processes or ports.
        await self.builder.kill_all()
        self.stage("verify", f"Independent verification by a fresh {self.task.verifier_harness} session")
        verdict = await self._verify(token, patch, attempt)
        if verdict is None:
            if identity is not None and identity["action"] == "revert":
                self._steward_question("Steward revert failed independent verification",
                           "Review verifier findings and choose a fix-forward repair or a different culprit.",
                           status="needs_input")
            return False
        # A restart before publication reloads exactly this candidate from the diff.
        self.evidence("attempts", {"n": attempt, "result": "verified", "checks": _brief(self.checks),
                                   "verdict": verdict, "app_run": self.app_summary,
                                   "configured_checks": list(self.cfg.get("checks") or []), **seen})
        return True

    async def _prepare_builder(self, token: str, from_branch: bool) -> None:
        self.builder = LocalSandbox(f"task-{self.task_id[:12]}-b{self.task.attempt}").create()
        branch = self.task.branch or f"factory/{self.outcome.number}-{self.task.key.lower()}-{self.task_id[:6]}"
        if from_branch:
            self.stage("prepare", f"Preparing the pull request's branch {branch} of {self.repo.full_name}")
            await checkout(self.builder, self.repo.full_name, token, f"refs/heads/{branch}", branch)
            await self._bring_branch_up_to_date(branch)
            last = self._left_behind()  # never pushed, so not on the branch: only a default-branch checkout restores it
            self._tree = {"from": "branch", "pushed": await self._branch_holds(),
                          **({"changes": "lost", "attempt": last.get("n")} if last else {})}
        else:
            self.stage("prepare", f"Preparing a fresh checkout of {self.repo.full_name}")
            base_sha = await checkout(self.builder, self.repo.full_name, token, self.repo.default_branch, branch)
            self.update(base_sha=base_sha, branch=branch)
            identity = self._steward_identity()
            # A revert is always generated from pristine main, never a previous attempt's inverse.
            earlier = {} if identity is not None and identity["action"] == "revert" else await self._earlier_changes()
            self._tree = {"from": "default", **earlier}
        await self._set_up_builder()

    async def _set_up_builder(self) -> None:
        """Isolate the prepared checkout and run the repository's setup in it."""
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
                f"`{c['command']}` {_how_failed(c)}\n```\n{c['tail'][-1500:]}\n```" for c in broken))

    async def _branch_holds(self) -> str | None:
        """How the pull request's earlier change sits in a sandbox prepared from its branch: as "commits" on the
        base, as "uncommitted" changes (carried over a conflict, see ``_bring_branch_up_to_date``), or "" when none
        of it is there. None when git cannot say."""
        base = shlex.quote(self.task.base_sha or "")
        ahead = await self.builder.exec(f"git rev-list --count {base}..HEAD", timeout=60)
        dirty = await self.builder.exec("git status --porcelain --untracked-files=no", timeout=60)
        if not ahead.ok or not dirty.ok or not ahead.output.strip().isdigit():
            return None
        return "commits" if int(ahead.output.strip()) else "uncommitted" if dirty.output.strip() else ""

    def _left_behind(self) -> dict[str, Any] | None:
        """The record of this budget's latest attempt, when what it changed was only in its sandbox, which a new
        sandbox does not have. None when nothing was left behind: no attempt ended, it changed nothing, or its
        candidate was verified (a later record with its number judges that candidate again after a rebase, and it
        is on the pull request's branch)."""
        attempts = [a for a in self._budget_attempts() if a.get("result") != "builder_blocked"]
        last = attempts[-1] if attempts else None
        if (last is None or (last.get("result") == "policy" and last.get("files") == 0)
                or any(a.get("result") == "verified" and a.get("n") == last.get("n") for a in attempts)):
            return None
        return last

    async def _earlier_changes(self) -> dict[str, Any]:
        """A new checkout starts from the work in progress saved for this attempt, when a session of it was
        interrupted (see ``_save_progress``): that was read from a tree that already held what earlier attempts had
        left. Otherwise from what the latest failed attempt of this budget changed: the feedback the builder gets is
        about that code, as it is when the next attempt runs in the same sandbox. Either only when its stored diff
        reloads whole and still applies. Returns what to tell the builder about it (see prompts.working_tree)."""
        last, saved, told = self._left_behind(), self._saved_progress(), {}
        if saved:
            n, size = saved["attempt"], {key: saved.get(key) for key in ("files", "additions", "deletions")}
            patch, why = await self._restore({"patch_sha256": saved.get("patch_sha256"),
                                              "diff_artifact_id": saved.get("artifact_id")})
            if patch is not None:
                judged = self._stored_candidate(last) if last else None
                if judged:
                    self.patch = judged[0]  # what this runner last judged, as in the sandbox that was lost
                at = f"{str(saved.get('saved_at'))[:16].replace('T', ' ')} UTC"
                self.event("wip_restored", f"Attempt {n} was interrupted: restored the work in progress saved at {at} "
                           f"({size['files']} file(s), +{size['additions']} -{size['deletions']}) into the new "
                           "checkout, so the builder continues from it", saved_at=saved.get("saved_at"), **size)
                return {"wip": {"restored": True, "at": at, **size}}
            if not saved.get("artifact_id"):
                why = f"they were not saved: {saved.get('skipped')}"
            self.event("wip_not_restored", f"Attempt {n} was interrupted and the changes of its session could not be "
                       f"restored into the new checkout ({why}); the builder starts without them", level="warn",
                       reason=why)
            told = {"wip": {"restored": False, "why": why}}
        if last is None:
            return told
        patch, why = await self._restore(last)
        if patch is not None:
            self.patch = patch  # what this runner last judged, as in the sandbox that was lost
            self.event("changes_restored", f"Restored the changes of attempt {last.get('n')} into the new "
                       "checkout, so the builder continues from them", files=len(patch.files))
            return {**told, "changes": "restored", "attempt": last.get("n")}
        self.event("changes_not_restored", f"The changes of attempt {last.get('n')} could not be restored into the "
                   f"new checkout ({why}); the builder starts from a clean checkout", level="warn")
        return {**told, "changes": "lost", "attempt": last.get("n")}

    async def _restore(self, record: dict[str, Any]) -> tuple[Patch | None, str]:
        """Put the diff a record names (see ``_stored_candidate``) into the new checkout. The patch, or None and why
        not: the checkout is then as it was, because git applies all of a diff or none of it."""
        found = self._stored_candidate(record)
        if found is None:
            return None, "no whole copy of them was saved"
        try:
            await apply_in_sandbox(self.builder, found[0])
        except SandboxError:
            return None, f"they no longer apply to {self.repo.default_branch} at {self.task.base_sha[:8]}"
        return found[0], ""

    def _describe_tree(self, attempt: int) -> None:
        """Record what the builder's working tree holds as its session starts, for its prompt. A later session in
        the same sandbox finds this attempt's changes in it too, and was not interrupted."""
        self.evidence("tree", {**self._tree, "sha": self.task.base_sha, "branch": self.task.branch,
                               "default": self.repo.default_branch})
        self._tree = {**{key: value for key, value in self._tree.items() if key != "wip"},
                      "changes": "left", "attempt": attempt}

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

    async def _verify(self, token: str, patch: Patch, attempt: int) -> dict[str, Any] | None:
        self._guard_steward_candidate()
        if self._steward_identity() is not None:
            problem = await self._policy_problem(patch)
            if problem:
                self._steward_question("Steward candidate violates policy", problem, status="needs_input")
        qa_cfg = self.cfg.get("qa") or {}
        error = ""
        previous = self._previous_review(patch)
        for tries in range(2):
            sandbox = None
            app: AppRun | None = None
            result = None
            carried = None  # what the session's failure said it was; a sandbox that failed says nothing
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
                    run, result, error = await self._session(attempt, "verify", SessionSpec(
                        workspace_id=self.ws, outcome_id=self.task.outcome_id, task_id=self.task_id, role="verifier",
                        harness=self.task.verifier_harness, system=verifier_system(self.task.base_sha), sandbox=sandbox,
                        attempt=attempt, writable=False, effective_settings=self.settings,
                        repositories={self.repo.full_name: self.cfg},
                        expected_criteria=list(self.task.acceptance or []),
                        prompt=verify_prompt(self.task, self.repo, self.task.base_sha, self.builder_report, self.checks,
                                             precedents(self.ws, self.repo.id),
                                             guidance_history(self.task.outcome_id, self.task_id),
                                             outcome=self.outcome, sections=sections, previous=previous)
                        + ("\nExplicit verification criterion: " + STEWARD_TEST_CRITERION if self._steward_identity() else "")
                        + brief))
                    carried = carried_cause(run)
                    if app is not None:
                        await self._store_app_evidence(app, attempt, run.id)
            finally:
                if app is not None:
                    app.stop()
                if sandbox is not None:
                    await sandbox.close()
            if result is None:
                if infrastructure_problem(error, carried) == "provider":
                    self._infrastructure(attempt, "verify", error, "provider")
                self.event("verification_unavailable", f"Verifier session failed: {error[:300]}", level="warn")
                continue
            report = result.report
            runtime_problem = self._runtime_problem(app, qa_cfg)
            if runtime_problem:
                # Recorded as a finding whatever the verifier said, so later attempts (and the automatic retry) see
                # the concrete failure; a passing verdict is overridden. A blocker: the change must not merge as it is.
                report = {**report, "findings": [*(report.get("findings") or []), {
                    "severity": "blocker", "description": f"Required runtime QA failed: {runtime_problem}",
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
                self.feedback.clear()  # a passing review settles everything raised before it
                self._results = slice(0, 0)
                return report
            app_log = str(app.evidence.get("log_tail") or "")[-1500:] if runtime_problem and app is not None else ""
            if self._results.stop is not None:  # no attempt is running (a revalidation): this replaces the last results
                del self.feedback[self._results]
                self._results = slice(len(self.feedback), None)
            self.feedback += _review_feedback(report, app_log)
            self._results = slice(self._results.start, len(self.feedback))
            # With what this review saw, so the next one can be told what changed since, and the criteria it failed
            # that the previous review had passed.
            seen = {"diff_artifact_id": self._reviewed_diff(patch, attempt), "base_sha": self.task.base_sha,
                    "patch_sha256": hashlib.sha256(patch.diff).hexdigest()} | ({"app_log": app_log} if app_log else {})
            flipped = {"flipped": _flipped(previous["verdict"], report)} if previous else {}
            self.evidence("attempts", {"n": attempt, "result": "verification_failed", "verdict": report,
                                       "checks": _brief(self.checks), "app_run": self.app_summary, **seen, **flipped})
            self.event("verification_failed", str(report.get("summary") or "")[:400], level="warn", **flipped)
            return None
        if infrastructure_problem(error, carried) == "transient":
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

    async def _session(self, attempt: int, phase: str, spec: SessionSpec):
        """One builder or verifier session (run_agent). A provider that refuses before the session starts (no valid
        credential: there is no run yet) holds the workspace as a refusal inside the session does."""
        try:
            return await run_agent(spec, self.cancelled)
        except Exception as exc:
            if failure_cause(exc) != "provider":
                raise
            self._infrastructure(attempt, phase, str(exc), "provider")

    def _infrastructure(self, attempt: int, phase: str, error: str, cause: str) -> None:
        """The model provider or the sandbox failed, not the attempt, so it is not charged. Billing, quota and access
        problems (``cause`` "provider") wait for a person: a model session the provider refused holds the whole
        workspace under one decision and the task waits in the queue; an access problem while setting up a sandbox
        blocks this task. Transient ones requeue the same attempt after a backoff; once ``INFRA_RETRIES`` consecutive
        retries have failed as well, a person decides."""
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
            if cause == "provider" and phase in ("build", "verify") and not error.startswith("Sandbox setup failed"):
                # Every model session of the workspace is refused the same way, so there is one hold and one decision
                # for all of them, not one per task. The hold opens first: the scheduler must not start the task
                # again before it exists.
                from .holds import open_provider_hold

                what = _PHASES[phase].lower()
                open_provider_hold(self.ws, error, f"{self.task.key} of outcome #{self.outcome.number}, {what}")
                self.event("provider_hold", f"Attempt {attempt}: the model provider refused {what} ({error[:200]}). "
                           "Not counted as an attempt; the task waits in the queue until the provider hold in the "
                           "inbox is resumed", level="warn", phase=phase)
                self.update(attempt=resume_at, status="queued", lease_owner="")
                raise Stop()
            self.update(attempt=resume_at)
            if cause == "provider":
                self.block("unblock", f"{self.task.key}: access problem", error,
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
            theirs, first = base.get(check["command"]) or {}, check.get("first") or {}
            new = sorted((set(check.get("failures") or []) | set(first.get("failures") or []))
                         - set(theirs.get("failures") or []))
            if _same_failure(check, theirs):
                check["preexisting"], check["cause"] = True, "preexisting"
                check["vs_base"] = (" (it times out on the base commit too, so your change did not cause it)"
                                    if check.get("timed_out") else
                                    " (it fails the same way on the base commit, so your change did not cause it)")
            elif theirs.get("ok"):
                check["vs_base"] = " (it passes on the base commit)"
            elif new and theirs.get("failures"):
                check["vs_base"] = (f" (failing here but not on the base commit: {', '.join(new[:20])}"
                                    + (f" and {len(new) - 20} more" if len(new) > 20 else "") + ")")
            elif first and _same_failure({**check, "first": None}, theirs):
                check["vs_base"] = (" (the base commit fails as the second run did, not as the first, so the first "
                                    "run's failure counts as a failure of your change)")
            elif check.get("timed_out") and theirs.get("timed_out"):
                check["vs_base"] = (" (it times out on the base commit too, but the two runs had not got equally far "
                                    "when they were stopped, so this timeout counts as a failure of your change)")
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
        candidate = (self.task.evidence or {}).get("steward_candidate")
        identity = self._steward_identity()
        if (candidate or identity) and (not candidate or candidate.get("identity") != identity):
            self._set_evidence({"steward_candidate": None, "fix_round": "", "revalidation": None})
            return False
        last = self._unpublished_candidate()
        found = self._stored_candidate(last)
        if found is None:
            return False
        self.patch, self.builder_report = found
        self.verifier_report = last.get("verdict") or {}
        self.checks, self.app_summary = list(last.get("checks") or []), last.get("app_run")
        self.event("publish_resumed", f"Attempt {self.task.attempt} was verified before the restart; publishing that "
                   "candidate")
        return True

    def _stored_candidate(self, record: dict[str, Any]) -> tuple[Patch, dict[str, Any]] | None:
        """The candidate a record names (``diff_artifact_id``, ``patch_sha256``), reloaded from its stored diff, with
        its builder's report. None unless the diff reloads byte for byte: it is stored as text, the first 400,000
        characters of it, so a larger one, or one with bytes that are not UTF-8, does not."""
        if not record.get("patch_sha256") or not record.get("diff_artifact_id"):
            return None
        with session_scope() as s:
            diff = s.get(Artifact, record["diff_artifact_id"])
            if diff is None:
                return None
            builder = s.get(Run, diff.run_id) if diff.run_id else None
            raw, data = (diff.content or "").encode(), dict(diff.data or {})
            # A builder stopped by its cap has no result on its run: the stand-in for its report is kept with the
            # task, like every builder report, and no builder has run since a candidate that is being resumed.
            report = dict((builder.result if builder else None) or (self.task.evidence or {}).get("builder_report")
                          or {})
        if hashlib.sha256(raw).hexdigest() != record["patch_sha256"]:
            return None
        return Patch(diff=raw, files=list(data.get("files") or []), additions=int(data.get("additions") or 0),
                     deletions=int(data.get("deletions") or 0)), report

    def _saved_candidate(self) -> dict[str, Any] | None:
        """The candidate built for the attempt this run continues (``evidence["candidate"]``), when the task stopped
        before that attempt ended: at its checks or its verification. An infrastructure failure hands the attempt's
        number back, so it is the current number or the next. A record of another attempt, budget, base commit or
        fix round is never used."""
        ev = self.task.evidence or {}
        saved = ev.get("candidate")
        if (isinstance(saved, dict) and saved.get("n") in (self.task.attempt, self.task.attempt + 1)
                and saved.get("epoch") == int(ev.get("budget_epoch", 0)) and saved.get("base_sha") == self.task.base_sha
                and (saved.get("fix_round") or "") == (ev.get("fix_round") or "")):
            return saved
        return None

    async def _resume_candidate(self, token: str, attempt: int, saved: dict[str, Any]) -> Patch | None:
        """Put a candidate built before the task stopped back into a builder's sandbox: its base commit with its diff
        applied, set up as for a builder. The attempt goes on from its checks, or from its verification when the
        checks had passed. None when the candidate cannot be restored exactly: the attempt is then built again."""
        found = self._stored_candidate(saved)
        if found is None:
            return self._drop_candidate(attempt, "its diff was not stored whole: it is too large, or not all text")
        patch, report = found
        self.stage("prepare", f"Attempt {attempt}: restoring the candidate built before the interruption")
        try:
            self.builder = LocalSandbox(f"task-{self.task_id[:12]}-b{attempt}").create()
            await checkout(self.builder, self.repo.full_name, token, saved["base_sha"], self.task.branch)
            await apply_in_sandbox(self.builder, patch)
            await self._set_up_builder()
        except (SandboxError, GitError, OSError) as exc:
            if self.builder is not None:
                await self.builder.close()
                self.builder = None
            if str(exc).startswith("Candidate patch does not apply"):
                return self._drop_candidate(attempt, "its diff no longer applies to its base commit")
            error = f"Sandbox setup failed: {exc}"
            cause = infrastructure_problem(error)
            if cause == "provider" or int((self.task.evidence or {}).get("infra_retries", 0)) < INFRA_RETRIES:
                self._infrastructure(attempt, "prepare", error, cause)  # the candidate is kept for the retry
            # A candidate whose base commit can no longer be checked out must not hold the task for good.
            return self._drop_candidate(attempt, f"its sandbox could not be prepared: {str(exc)[:200]}")
        # Everything checks, verification and publication read from the runner (the verifier's report and the app
        # run are set by the verification that follows).
        self.patch, self.builder_report, self.app_summary = patch, report, None
        del self.feedback[self._results]  # the builder had these before it built this candidate
        self._results = slice(len(self.feedback), None)
        self._tree = {"from": "default", "changes": "restored", "attempt": attempt}
        at = "checks" if saved.get("checks") is None else "verification"
        if at == "checks":
            self.stage("checks")
        self.event("candidate_resumed", f"Attempt {attempt}: resuming the built candidate at {at} after an "
                   "interruption", files=len(patch.files))
        return patch

    def _drop_candidate(self, attempt: int, why: str) -> None:
        self.event("candidate_not_resumed", f"Attempt {attempt}: the candidate built before the interruption could "
                   f"not be restored ({why}); building it again", level="warn")
        self.evidence("candidate", None)

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
        self._guard_steward_candidate()
        if self._steward_identity() is not None:
            problem = await self._policy_problem(self.patch)
            if problem:
                self._steward_question("Steward candidate violates policy", problem, status="needs_input")
            # Publication after restart has no builder. The exact gated hash is durable.
            gate = (self.task.evidence or {}).get("steward_test_gate") or {}
            scope = gate.get("scope") or {}
            digest = scope.get("patch_sha256") or gate.get("patch_sha256")
            if (not gate.get("approved") or digest != hashlib.sha256(self.patch.diff).hexdigest()
                    or (scope and scope.get("checks") != list(self.cfg.get("checks") or []))):
                self._steward_question("Steward test approval is missing", "Review the candidate test/check changes.",
                           status="needs_input")
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
        change_id = await self._patch_id(token, self.task.base_sha, head)
        # Record the publication before advancing the head, as revalidation does: a stop between the two must not
        # leave the task at this head with a record of the publication before it.
        self.evidence("published", {"head": head, "base": self.task.base_sha, "at": utcnow().isoformat(),
                                    "pr": pr["number"], "patch_id": change_id})
        self._carry_approval()
        self.update(head_sha=head, pr_number=pr["number"], pr_url=pr["html_url"], stage="ci")
        self.event("published", f"Opened PR #{pr['number']}", level="success", url=pr["html_url"], head=head)

    async def _patch_id(self, token: str, base: str, head: str) -> str:
        """What the published ``head`` changes (gitops.patch_id), read in a throwaway checkout. "" when it cannot be
        read: the candidate is published all the same and an earlier merge approval does not carry over to it."""
        sandbox = LocalSandbox(f"task-{self.task_id[:12]}-p")
        try:
            sandbox.create()
            await checkout(sandbox, self.repo.full_name, token, head, self.task.branch)
            return await patch_id(sandbox, base, head)
        except (GitError, GitHubError, SandboxError, OSError) as exc:
            log.warning("patch id unavailable for %s at %s: %s", self.task_id, head[:10], exc)
            return ""
        finally:
            await sandbox.close()

    def _carry_approval(self) -> None:
        """After a head is published: when the merge approval given for an earlier head still holds for it (the
        same patch, see ``_approved``), say so once and remember the head it was carried to."""
        evidence = self.task.evidence or {}
        approved, head = evidence.get("approved") or {}, (evidence.get("published") or {}).get("head")
        if not approved or approved.get("head") == head or not self._approved("approve_merge"):
            return
        self.evidence("approved", {**approved, "head": head})
        self.event("approval_carried", f"The merge approval for {str(approved.get('head'))[:10]} still holds at "
                   f"{head[:10]}: the change itself is the same", approved_head=approved.get("head"), head=head)

    def _pr_body(self) -> str:
        lines = [self.builder_report.get("summary", "").strip() or self.task.title, "", "### Acceptance criteria"]
        by = {c.get("criterion"): c for c in _objects(self.verifier_report.get("criteria"))}
        for criterion in self.task.acceptance:
            c = by.get(criterion) or next((v for k, v in by.items() if k and criterion[:40] in k), {})
            mark = {"pass": "✅", "fail": "❌"}.get(c.get("status", ""), "◻️")
            lines.append(f"- {mark} {criterion}" + (f"  \n  <sub>{c.get('evidence', '')[:400]}</sub>" if c else ""))
        notes = [f for f in _objects(self.verifier_report.get("findings")) if f.get("severity") == "major"]
        if notes:  # advice on a passing review: it did not stop the merge
            lines += ["", "### Reviewer notes", "The independent review passed, with these points to follow up:"]
            lines += [f"- {_one_line(f.get('description'))}" + (f" (`{_one_line(f['location'], 120)}`)"
                                                                if f.get("location") else "") for f in notes[:10]]
            if len(notes) > 10:
                lines.append(f"- … and {len(notes) - 10} more in the verification report")
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
            lines += [f"- {'✅' if c['ok'] else '❌'} `{c['command']}` ({c['duration']}s){_check_note(c)}"
                      for c in self.checks]
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
        """Revalidate after every network/lock wait, before taking another action. What follows a guard is a push, a
        comment or a merge, so a runner whose task is no longer its own stops here as well."""
        if self.cancelled.is_set():
            raise Stop()
        self._guard_steward_candidate()
        with session_scope() as s:
            task = s.get(Task, self.task_id)
            if (status and task.status != status) or not self._holds(s, task):
                raise Stop()

    def _holds(self, s, task: Task) -> bool:
        """Whether the task is still this runner's to act on, asked in one conditional write.

        A scheduled runner works on a running task under its instance's lease from its first step to its last: it
        gives the lease up only in the write that parks, requeues or blocks the task, and stops there. So for it a
        lease that is nobody's is a takeover as much as another instance's: whoever took the task has let go of it
        since (parked it for CI, or its server stopped) and the candidate this runner holds may be out of date.
        A runner without an owner (a CI poll: those tasks are parked, leased to nobody) is asked only whether the
        task may still change."""
        if self.owner is not None and task.status != "running":
            return False
        return transition(s, task, owner=self.owner, exclusive=True, lease_owner=task.lease_owner)

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
        if self.task.stage == "review":
            return await self.poll_review(gh)
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
        # A green head waits for its reviewers before the merge (never a revert, which goes straight in).
        review = stage == "merge" and self._enter_review(ev, wait["head"], now)
        self.update(status="queued" if stage and not review else "waiting_ci",
                    stage="review" if review else stage or "ci", evidence=ev, lease_owner="", blocked_reason="")
        if changed:
            self.event("ci_status", f"CI {summary.state}", state=summary.state, checks=summary.checks)

    async def _fix_ci(self, gh: GitHub) -> None:
        identity = self._steward_identity()
        if identity is not None and identity["action"] == "revert":
            self._steward_question("Steward revert failed CI",
                       "The deterministic inverse failed CI. Choose fix-forward or a different culprit; "
                       "a builder may not alter the authorized inverse.", status="needs_input")
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

    # review ----------------------------------------------------------------------
    def _review_policy(self) -> dict[str, Any]:
        try:
            return review_policy(self.settings, self.cfg)
        except ValueError:  # a stored policy that no longer validates: the defaults apply
            return review_policy()

    def _review_patch(self, head: str) -> str | None:
        published = (self.task.evidence or {}).get("published") or {}
        return published.get("patch_id") if published.get("head") == head else None

    def _round_findings(self) -> tuple[list[dict[str, Any]], int]:
        """The findings the review round in progress gives the builder, and how many more were left out."""
        review = (self.task.evidence or {}).get("review") or {}
        ids = (review.get("round") or {}).get("findings") or []
        if not ids or self._review_policy()["address"] == "off":
            return [], 0
        result = review.get("result") or {}
        return [f for f in _objects(result.get("open")) if f.get("id") in ids], int(result.get("more") or 0)

    def _enter_review(self, ev: dict[str, Any], head: str, now: datetime) -> bool:
        """Whether this green head waits for its review before the merge; ``ev`` then carries the wait."""
        policy = self._review_policy()
        if policy["address"] == "off" or not self.task.pr_number:
            return False
        review = {**new_review_evidence(), **(ev.get("review") or {})}
        result = review.get("result") or {}
        published = ev.get("published") or {}
        patch = published.get("patch_id") if published.get("head") == head else None
        if (patch and result.get("patch_id") == patch and result.get("head") != head
                and result.get("state") == "reviewed" and not result.get("unseen")):
            # The same change on a newer base: what its review found stands. Nothing is asked for or awaited.
            # A head that went on without a review carries nothing: the new head waits for its own.
            result = review["result"] = {**result, "head": head, "of": result.get("of") or result.get("head")}
        ev["review"] = review
        if result.get("head") == head and not review.get("round"):
            return False
        if review.get("head") != head:
            review.update(head=head, started_at=now.isoformat(),
                          deadline=(now + timedelta(seconds=policy["wait_seconds"])).isoformat())
        review["next_poll_at"] = now.isoformat()
        return True

    async def poll_review(self, gh: GitHub | None = None) -> None:
        """One bounded look at the pull request's reviews, like a CI poll. A failure to read or write them is tried
        again at the next poll: it never uses an attempt and never blocks the task, and once the deadline has
        passed the head goes on as not reviewed."""
        try:
            await asyncio.wait_for(self._poll_review(gh), timeout=CI_POLL_TIMEOUT_SECONDS)
        except Stop:
            raise
        except Exception as exc:  # a fault here must not leave the task polled forever: it counts as unread too
            self._guard("waiting_ci")
            log.warning("review poll failed for %s: %r", self.task_id, exc,
                        exc_info=not isinstance(exc, (asyncio.TimeoutError, GitHubError)))
            self._review_unread()

    async def _poll_review(self, gh: GitHub | None) -> None:
        self._guard("waiting_ci")
        gh = gh or GitHub(resolve_token(self.ws))
        self._guard("waiting_ci")
        policy = self._review_policy()
        review = json.loads(json.dumps((self.task.evidence or {}).get("review") or {}))
        head = self.task.head_sha
        if policy["address"] == "off" or review.get("head") != head or not self.task.pr_number:
            # Switched off while waiting, or not this head's wait: the CI wait decides again what comes next.
            self.update(status="queued", stage="ci", lease_owner="", blocked_reason="")
            return
        snapshot = await gh.review_snapshot(self.repo.full_name, self.task.pr_number)
        self._guard("waiting_ci")
        if review.get("round") and not await self._answer_round(gh, review, snapshot["threads"]):
            return self._review_later(review)
        seen = {"reviews": snapshot["reviews"], "comments": snapshot["comments"], "policy": policy,
                "identity": snapshot["identity"], "pull_author": snapshot["pull_author"]}
        reviewers = reviewed_by(head, **seen)
        # Codex says it found something, and its review is not in what was read: wait for a reading that has it.
        unseen = findings_unseen(head, **seen)
        standing = review.get("result") or {}
        carried = not reviewers and standing.get("head") == head and standing.get("state") == "reviewed"
        if carried:  # the same change as a head they reviewed (see _enter_review)
            reviewers = standing.get("reviewers") or []
        if (not reviewers or unseen) and utcnow() < datetime.fromisoformat(review["deadline"]):
            if not reviewers and not unseen:
                await self._request_review(gh, review, snapshot, policy)
            return self._review_later(review)
        self._review_result(review, snapshot, policy, reviewers, standing.get("of") if carried else None, unseen)

    def _save_review(self, review: dict[str, Any]) -> None:
        # A copy: the caller goes on changing ``review``, which must not reach the stored evidence unsaved.
        self.update(evidence={**self.task.evidence, "review": json.loads(json.dumps(review))})

    def _review_later(self, review: dict[str, Any]) -> None:
        review["next_poll_at"] = (utcnow() + timedelta(seconds=REVIEW_POLL_SECONDS)).isoformat()
        self._save_review(review)

    def _review_unread(self) -> None:
        """A poll failed. Before the deadline the next poll tries again; after it this head was not reviewed."""
        review = json.loads(json.dumps((self.task.evidence or {}).get("review") or {}))
        if not review.get("deadline"):  # no wait on record to resume: the CI wait starts one
            return self.update(status="queued", stage="ci", lease_owner="", blocked_reason="")
        if utcnow() < datetime.fromisoformat(review["deadline"]):
            return self._review_later(review)
        if review.get("round"):  # its answers could not be posted either: the merge approval says so
            self._close_round(review, self._unposted(review))
        head = review["head"]
        self._leave_review(review, self._review_policy(), {
            "head": head, "patch_id": self._review_patch(head), "state": "unavailable", "reason": "unreadable",
            "reviewers": [], "open": [], "more": 0, "counts": {}, "at": utcnow().isoformat()})

    def _round_answers(self, review: dict[str, Any]) -> dict[str, tuple[dict[str, Any], str, str]]:
        """The builder's answers to the round's findings, from its latest report: id -> (finding, action, note)."""
        ids = (review.get("round") or {}).get("findings") or []
        findings = {f.get("id"): f for f in _objects((review.get("result") or {}).get("open")) if f.get("id") in ids}
        answers = {}
        for answer in _objects(self.builder_report.get("review_responses")):
            fid, action = answer.get("id"), answer.get("action")
            if isinstance(fid, str) and fid in findings and action in ACTIONS:
                answers[fid] = (findings[fid], action, str(answer.get("note") or ""))
        return answers

    def _unposted(self, review: dict[str, Any]) -> list[str]:
        """Links to the round's answered findings whose reply or resolution is not on GitHub yet."""
        replies = (review.get("round") or {}).get("replies") or {}
        missing = []
        for fid, (finding, action, _note) in self._round_answers(review).items():
            done = replies.get(fid) or {}
            if (finding.get("reply_to_id") and not done.get("replied")) or (
                    action == "fixed" and finding.get("thread_id") and not done.get("resolved")):
                missing.append(finding.get("url") or fid)
        return missing

    async def _answer_round(self, gh: GitHub, review: dict[str, Any], threads: list[dict[str, Any]]) -> bool:
        """Put the published revision's answers on their threads, each once: a reply, and the thread resolved when
        the builder fixed it. Every step is recorded as it happens, and a reply already on the thread is not posted
        again. A failure is tried again at the next poll (False); after REVIEW_REPLY_TRIES polls the round closes
        without the missing answers, which the merge approval then lists."""
        current = review["round"]
        by_id = {t.get("id"): t for t in threads}
        for fid, (finding, action, note) in self._round_answers(review).items():
            done = current.setdefault("replies", {}).setdefault(fid, {})
            thread = by_id.get(finding.get("thread_id")) or {}
            text = reply_text(action, note, self.task.head_sha)
            try:
                if finding.get("reply_to_id") and not done.get("replied"):
                    if not _replied(thread, text):
                        self._guard("waiting_ci")
                        await gh.reply_review_comment(self.repo.full_name, self.task.pr_number,
                                                      finding["reply_to_id"], text)
                    done["replied"] = True
                    self._save_review(review)
                if action == "fixed" and finding.get("thread_id") and not done.get("resolved"):
                    if not thread.get("isResolved"):
                        self._guard("waiting_ci")
                        await gh.resolve_review_thread(finding["thread_id"])
                    thread["isResolved"] = True  # this poll reads the open findings from the same snapshot
                    done["resolved"] = True
                    self._save_review(review)
            except GitHubError as exc:
                log.warning("review answer for %s on %s failed: %s", self.task_id, fid, exc)
        failed = self._unposted(review)
        if failed:
            current["tries"] = int(current.get("tries") or 0) + 1
            if current["tries"] < REVIEW_REPLY_TRIES:
                self._save_review(review)
                return False
        self._close_round(review, failed)
        return True

    def _close_round(self, review: dict[str, Any], failed: list[str]) -> None:
        """The round is over. What the builder answered stops counting until its reviewer writes again."""
        answered = review.setdefault("answered", {})
        for fid, (finding, action, note) in self._round_answers(review).items():
            answered[fid] = {"action": action, "note": _one_line(note), "updated_at": finding.get("updated_at")}
        review["unanswered"] = list(dict.fromkeys([*(review.get("unanswered") or []), *failed]))
        review["round"] = None

    async def _request_review(self, gh: GitHub, review: dict[str, Any], snapshot: dict[str, Any],
                              policy: dict[str, Any]) -> None:
        """Ask for a review of a head that is not the pull request's first, exactly once. The intent is recorded
        before the comment is posted; a poll that finds the intent looks for the comment before posting it."""
        head, text = review["head"], policy["request"]
        requested = review.get("requested") or {}
        if not text or (requested.get("head") == head and requested.get("state") != "intent"):
            return
        if requested.get("head") != head:
            first = self._first_publication(head)
            requested = review["requested"] = {"head": head, "at": utcnow().isoformat(),
                                               "state": "first" if first else "intent"}
            self._save_review(review)
            if first:
                return
        published = (self.task.evidence or {}).get("published") or {}
        since = _github_time(published.get("at") if published.get("head") == head else requested.get("at"))
        own = {str(a.get("login") or "").casefold() for a in (snapshot["identity"], snapshot["pull_author"])} - {""}
        posted = since is not None and any(
            (c.get("body") or "").strip() == text and str((c.get("user") or {}).get("login") or "").casefold() in own
            and (_github_time(c.get("created_at")) or datetime.min) >= since for c in snapshot["comments"])
        if not posted:
            self._guard("waiting_ci")
            await gh.comment(self.repo.full_name, self.task.pr_number, text, retries=0)
        requested["state"] = "posted"
        self._save_review(review)
        self.event("review_requested", f"Asked for a review of {head[:10]} on PR #{self.task.pr_number}", head=head)

    def _first_publication(self, head: str) -> bool:
        """Whether this head is the only one the Factory has published on the pull request, by its own record: the
        reviewers of a new pull request start on their own. Any other head, a rebased one included, is asked for."""
        with session_scope() as s:
            events = s.query(Event).filter(Event.task_id == self.task_id, Event.kind == "published").all()
            heads = {(e.data or {}).get("head") for e in events if (e.data or {}).get("url") == self.task.pr_url}
        return heads == {head}

    def _review_result(self, review: dict[str, Any], snapshot: dict[str, Any], policy: dict[str, Any],
                       reviewers: list[str], of: str | None = None, unseen: bool = False) -> None:
        """Record what the review of this head left open, or that none arrived in time, and leave the wait."""
        head = review["head"]
        found = open_findings(normalize_feedback(policy=policy, **snapshot), policy, review.get("answered") or {})
        counts = {level: sum(f["severity"] == level for f in found) for level in SEVERITIES}
        result = {"head": head, "patch_id": self._review_patch(head),
                  "state": "reviewed" if reviewers else "unavailable", "reviewers": reviewers,
                  "open": found[:MAX_FINDINGS], "more": max(0, len(found) - MAX_FINDINGS),
                  "counts": {level: n for level, n in counts.items() if n}, "at": utcnow().isoformat()}
        if of:
            result["of"] = of
        if not reviewers:
            result.update(reason="timeout", waited_seconds=policy["wait_seconds"])
        if unseen:  # a reviewer reported findings that never became readable: a person decides the merge
            result["unseen"] = True
        self._leave_review(review, policy, result)

    def _leave_review(self, review: dict[str, Any], policy: dict[str, Any], result: dict[str, Any]) -> None:
        """Leave the wait with this head's result. A review that left a blocking finding starts a round while
        rounds remain: a revision with a fresh attempt budget. Otherwise the merge is next, and the result says
        when a person has to decide it whatever the merge autonomy (see _review_asks)."""
        from ..decisions import fresh_budget

        head, reviewed = result["head"], result["state"] == "reviewed"
        blockers = [f["id"] for f in result["open"] if f.get("blocking")]
        rounds = int(review.get("rounds") or 0)
        again = bool(blockers) and reviewed and rounds < policy["max_rounds"]
        review["result"] = result
        if again:
            # Everything still open goes to the builder, except what it already answered.
            ids = [f["id"] for f in result["open"] if not f.get("answer")]
            review.update(rounds=rounds + 1,
                          round={"n": rounds + 1, "head": head, "findings": ids, "replies": {}, "tries": 0})
            self.update(status="queued", stage="revise", attempt=0, lease_owner="", blocked_reason="",
                        evidence=fresh_budget({**self.task.evidence, "review": review}))
        else:
            if blockers:  # a known blocking finding never merges on its own
                result["ask"] = "limit" if reviewed else "open"
            elif result.get("unseen") or (not reviewed and policy["on_timeout"] == "ask"):
                result["ask"] = "unreviewed"
            self.update(status="queued", stage="merge", lease_owner="", blocked_reason="",
                        evidence={**self.task.evidence, "review": review})
        total = len(result["open"]) + result["more"]
        if reviewed:
            self.event("review_received", f"{', '.join(result['reviewers'])} reviewed {head[:10]}: "
                       + (f"{total} finding(s) open" if total else "nothing open"),
                       head=head, reviewers=result["reviewers"], counts=result["counts"])
        else:
            self.event("review_unavailable", f"No review of {head[:10]} to go on: "
                       + ("the reviews could not be read" if result.get("reason") == "unreadable" else
                          "a reviewer's findings could not be read" if result.get("unseen") else
                          f"none arrived within {policy['wait_seconds']} seconds"),
                       level="warn", head=head, reason=result.get("reason"))
        if again:
            self.event("review_round", f"Review round {rounds + 1}: {len(ids)} finding(s) go back to the builder",
                       head=head, round=rounds + 1, findings=ids)
        elif result.get("ask") == "limit":
            self.event("review_limit", f"{len(blockers)} blocking finding(s) are still open after {rounds} review "
                       "round(s); a person decides the merge", level="warn", head=head, rounds=rounds,
                       findings=blockers)

    def _review_summary(self) -> str:
        """What the merge approval says about the review of this head: who reviewed it and what is still open."""
        review = (self.task.evidence or {}).get("review") or {}
        result = review.get("result") or {}
        head = self.task.head_sha or ""
        if result.get("head") != head or self._review_policy()["address"] == "off":
            return ""
        found = _objects(result.get("open"))
        more = int(result.get("more") or 0)
        total = len(found) + more
        if result.get("state") == "reviewed":
            same = f" (the same change as {str(result['of'])[:10]}, which they reviewed)" if result.get("of") else ""
            lines = [f"Reviewed at {head[:10]} by {', '.join(result.get('reviewers') or [])}{same}: "
                     + (f"{total} finding(s) open." if total else "nothing open.")]
        elif result.get("reason") == "unreadable":
            lines = [f"Not reviewed: the reviews of {head[:10]} could not be read from GitHub."]
        else:
            minutes = round(int(result.get("waited_seconds") or 0) / 60)
            lines = [f"No review of {head[:10]} arrived within {minutes} minute{'' if minutes == 1 else 's'}."
                     + (f" {total} finding(s) from earlier reviews are still open." if total else "")]
        for f in found:
            where = f"{f.get('path')}:{f.get('line')}" if f.get("path") and f.get("line") else f.get("path") or ""
            answer = f.get("answer") or {}
            said = {"disputed": "the builder disagreed", "deferred": "the builder left it for later",
                    "fixed": "the builder reports it fixed"}.get(answer.get("action"))
            lines.append(" ".join(filter(None, [f"- {f.get('severity')}", f"{where}:" if where else "",
                                                 headline(str(f.get("body") or "")), str(f.get("url") or ""),
                                                 f"({said}: {answer.get('note') or ''})" if said else ""])))
        if more:
            lines.append(f"- and {more} more")
        if review.get("unanswered"):
            lines.append("The Factory could not post its answer on: " + ", ".join(review["unanswered"][:MAX_FINDINGS]))
        if result.get("unseen"):
            lines.append(f"A reviewer reported findings on {head[:10]} that could not be read from GitHub. "
                         "Read its review on the pull request before you approve.")
        if result.get("ask") == "limit":
            lines.append(f"The review rounds are used up ({int(review.get('rounds') or 0)}). "
                         "\"Request changes\" runs one more.")
        return "\n\n" + "\n".join(lines)

    # merge ---------------------------------------------------------------------
    def _merge_needs_approval(self, settings: dict[str, Any] | None = None) -> bool:
        from ..onboarding_policy import active

        settings = workspace_settings(self.ws) if settings is None else settings
        if active(settings) or _review_asks(getattr(self, "task", None), settings, self.cfg):
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
                   f"{self.task.head_sha[:10]}. {self.task.pr_url}{self._review_summary()}",
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
            # The last check before the merge, which cannot be taken back: the task must still be this runner's.
            if self.cancelled.is_set() or not self._holds(s, task):
                raise Stop()
            if self._merge_needs_approval(merged_settings(ws.settings)) and not self._approved("approve_merge"):
                return None

            self._guard_steward_candidate()

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
        """An approval covers the candidate that was published before it was given. A merge approval also covers
        the same patch published again later, at a new head on a newer base, unless ``merge.approval`` is "head".
        Read only: the merge consults it again while it holds the workspace lock."""
        with session_scope() as s:
            row = (s.query(Decision).filter(Decision.task_id == self.task_id, Decision.kind == kind,
                                            Decision.status == "resolved").order_by(Decision.resolved_at.desc()).first())
            if not (row and row.resolution == "approve" and row.resolved_at):
                return False
            given = row.resolved_at
        evidence = self.task.evidence or {}
        published, approved = evidence.get("published") or {}, evidence.get("approved") or {}
        if not published.get("at") or given >= datetime.fromisoformat(published["at"]):
            return True
        if kind != "approve_merge" or not approved.get("patch_id") or approved["patch_id"] != published.get("patch_id"):
            return False
        try:
            # The record is the latest approval's own: a correction that overruled a rejection wrote none.
            own = datetime.fromisoformat(approved.get("at") or "") >= given
        except (TypeError, ValueError):
            return False
        mode = (self.cfg.get("merge") or {}).get("approval") or (workspace_settings(self.ws).get("merge") or {}).get(
            "approval")
        return own and mode == "patch"

    async def merge(self) -> None:
        self._guard_steward_candidate()
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
        if self._steward_identity() is not None:
            if not self.task.pr_number:
                self._steward_question("Steward candidate has no PR", "Publish and verify a repair PR before merging.",
                           status="needs_input")
            if current.get("head", {}).get("sha") != self.task.head_sha:
                self._steward_question("Steward PR head changed", "The verified head is stale; rebuild the candidate.",
                           status="needs_input")
        await self._steward_merge_policy_gate(token)
        self._require_merge_approval()
        # One merge turn per repository: wait in the merge queue (parked, no slot) until this task is at its head.
        await merge_manager.take_turn(self, gh)
        revalidate = bool(self._steward_identity()) or (self.cfg.get("merge") or {}).get("revalidate", True)
        if self._steward_identity() is not None:
            pr = await gh.pull(self.repo.full_name, self.task.pr_number)
            summary = await gh.checks(self.repo.full_name, self.task.head_sha)
            self._guard()
            if pr.get("head", {}).get("sha") != self.task.head_sha:
                self._steward_question("Steward PR head changed", "The verified head is stale; rebuild the candidate.",
                           status="needs_input")
            if summary.state != "success" and not (summary.state == "none" and not self.cfg.get("require_ci", False)):
                await self._wait_ci()
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
            self._guard()
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
                    if self._steward_identity() is not None:
                        summary = await gh.checks(self.repo.full_name, self.task.head_sha)
                        self._guard()
                        if summary.state != "success" and not (summary.state == "none" and not self.cfg.get("require_ci", False)):
                            await self._wait_ci()
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
        self._guard_steward_candidate()
        identity = self._steward_identity()
        if identity is not None and identity["action"] == "revert":
            self._set_evidence({"candidate": None, "steward_candidate": None, "steward_test_gate": None,
                                "revalidation": None, "fix_round": ""})
            self.update(stage="prepare")
            self._steward_question("Steward revert base moved",
                       "Rebuild the exact culprit inverse against the latest default branch, then rerun all gates; "
                       "the old revert was not rebased or merged. Answer to approve rebuilding through all gates.",
                       status="needs_input")
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
                        "run": bool(identity) or mode == "always" or (mode != "never" and overlap["overlap"])}
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
                change_id = await self._patch_id(token, state["new_base"], state["head"])
                # A merge approval belongs to the published candidate, not its branch name.
                # Persist the new publication before advancing the head, including on resume.
                self.evidence("published", {"head": state["head"], "base": state["new_base"],
                                            "at": utcnow().isoformat(), "pr": self.task.pr_number,
                                            "patch_id": change_id})
                self._carry_approval()
            self.update(head_sha=state["head"], base_sha=state["new_base"])
        if state["phase"] in ("checks", "verify"):
            sandbox = LocalSandbox(f"task-{self.task_id[:12]}-r").create()
            try:
                await checkout(sandbox, self.repo.full_name, token, state["head"], self.task.branch)
                if identity is not None:
                    if identity != self._steward_identity():
                        self._guard_steward_candidate()
                    patch = await extract_patch(sandbox, state["new_base"])
                    problem = await self._policy_problem(patch)
                    if problem:
                        self._steward_question("Rebased steward candidate violates policy", problem,
                                   status="needs_input")
                    self.patch = patch
                    self._bind_steward_candidate(patch)
                    previous, self.builder = self.builder, sandbox
                    try:
                        await self._steward_test_gate(patch)
                    finally:
                        self.builder = previous
                if state["phase"] == "checks":
                    async with self._check_slot(sandbox, bool(self.cfg.get("setup") or self.cfg.get("checks"))):
                        setup = await self._run_commands(sandbox, self.cfg.get("setup") or [], "setup", timeout=1200)
                        self.checks = await self._run_commands(sandbox, self.cfg.get("checks") or [], "check")
                    state.update(setup=setup, checks=self.checks)
                    failed = [c for c in setup + self.checks if not c["ok"]]
                    if failed:
                        state.update(phase="rejected", reason="fails its checks on top of newer commits",
                                     feedback=[f"- After rebasing, `{c['command']}` {_how_failed(c)}"
                                               f"{_whose_output(c)}:\n{c['tail']}" for c in failed])
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
                        # The review is the latest results in ``self.feedback`` and an attempt record, which
                        # ``_prior_feedback`` reads after a restart: nothing to carry in the revalidation state.
                        state.update(phase="rejected", reason="failed independent verification after rebase",
                                     feedback=[])
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


FEEDBACK_CHARS = 40_000  # the most feedback one builder prompt carries; each item is already cut to its own limit


def _one_line(text: Any, limit: int = 300) -> str:
    return " ".join(str(text or "").split())[:limit]


def _review_asks(task: Any, settings: dict[str, Any], cfg: dict[str, Any]) -> bool:
    """Whether the review of the task's head left its merge to a person, whatever the merge autonomy: a blocking
    finding is open and no round is left, or no review arrived under ``on_timeout: ask``. No database access: the
    merge calls this while it holds the workspace lock."""
    result = ((getattr(task, "evidence", None) or {}).get("review") or {}).get("result") or {}
    if not result.get("ask") or result.get("head") != getattr(task, "head_sha", None):
        return False
    try:
        return review_policy(settings, cfg)["address"] != "off"
    except ValueError:
        return True


def _github_time(value: Any) -> datetime | None:
    """A GitHub timestamp (or one of the Factory's own) as naive UTC, like utcnow()."""
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    return (parsed - parsed.utcoffset()).replace(tzinfo=None) if parsed.tzinfo else parsed


def _replied(thread: dict[str, Any], text: str) -> bool:
    """Whether the Factory's reply with this text is already on the review thread."""
    want = " ".join(f"{FACTORY_REPLY_MARKER} {text}".split())
    nodes = (thread.get("comments") or {}).get("nodes") or []
    return any(" ".join(str(node.get("body") or "").split()) == want for node in _objects(nodes))


def _review_feedback(verdict: dict[str, Any], app_log: str = "") -> list[str]:
    """A failed review as the builder's feedback: its summary, each criterion that is not met and each blocker or
    major finding."""
    lines = ["- Independent review FAILED. " + str(verdict.get("summary") or "")[:1500]]
    lines += [f"  - Criterion not met: {c.get('criterion')} — {str(c.get('evidence', ''))[:600]}"
              for c in _objects(verdict.get("criteria")) if c.get("status") != "pass"]
    lines += [f"  - {f['severity']}: {str(f.get('description', ''))[:600]} {f.get('location', '')}"
              for f in _objects(verdict.get("findings")) if f.get("severity") in ("blocker", "major")]
    if app_log:
        lines.append(f"  - App log when it failed:\n```\n{app_log}\n```")
    return lines


def _open_points(verdict: dict[str, Any]) -> list[str]:
    """What a failed review left open, one line each: criteria that are not met and blocker or major findings."""
    points = [f"- Criterion not met: {_one_line(c.get('criterion'))} — {_one_line(c.get('evidence'))}"
              for c in _objects(verdict.get("criteria")) if c.get("status") != "pass"]
    points += [f"- {f['severity']}: {_one_line(f.get('description'))} {_one_line(f.get('location'), 120)}".rstrip()
               for f in _objects(verdict.get("findings")) if f.get("severity") in ("blocker", "major")]
    if not points and verdict.get("summary"):  # an older verdict that failed on its summary alone
        points = [f"- {_one_line(verdict['summary'])}"]
    return points[:30] + ([f"- … and {len(points) - 30} more"] if len(points) > 30 else [])


def _diff_by_file(diff: bytes) -> dict[str, bytes]:
    """A unified diff split into each file's part, by its ``diff --git`` headers."""
    parts: dict[str, bytes] = {}
    for part in re.split(rb"^(?=diff --git )", diff, flags=re.MULTILINE):
        if part.startswith(b"diff --git "):
            header = part.split(b"\n", 1)[0]
            same = re.fullmatch(rb"diff --git a/(.+) b/\1", header)
            parts[(same.group(1) if same else header[len(b"diff --git "):]).decode("utf-8", "replace")] = part
    return parts


def _diff_changes(before: bytes, after: bytes) -> tuple[list[str], list[str]]:
    """Between two diffs of the same change: the files whose diff is different now (or that are in only one of
    them), and the files whose diff is the same."""
    old, new = _diff_by_file(before), _diff_by_file(after)
    gone = [f"{name} (no longer part of the change)" for name in old if name not in new]
    return [name for name in new if old.get(name) != new[name]] + gone, [name for name in new
                                                                         if old.get(name) == new[name]]


def _flipped(before: dict[str, Any], after: dict[str, Any]) -> list[str]:
    """Criteria the previous verdict passed and this one does not, matched the way a report's criteria are matched to
    the task's (schemas._criterion_key: case, spacing, a list number and quoting aside): a reworded criterion does not
    match."""
    def key(c: dict[str, Any]) -> str:
        return _criterion_key(c.get("criterion"))

    passed = {key(c) for c in _objects(before.get("criteria")) if c.get("status") == "pass"} - {""}
    return [str(c.get("criterion")) for c in _objects(after.get("criteria"))
            if c.get("status") != "pass" and key(c) in passed]


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
    to a later attempt even after a restart. ``cause`` says what kind of failure it was: "timeout" (with the
    ``limit``), "flaky" (passed on a second run; the first run's tail is ``first_failure``), "preexisting" or "real".
    A check that failed on two runs has ``first``: the first run's exit code, and whether the second failed the
    ``same`` way. When it did not, ``tail`` is the first run's output and the other fields describe the second.
    A failed check that was run only once because its run changed the checkout has ``not_rerun`` ("changed") and
    the ``changed_files``."""
    return [{k: c[k] for k in ("command", "ok", "exit_code", "duration")}
            | {k: c[k] for k in ("cause", "timed_out", "limit", "not_rerun", "changed_files") if c.get(k)}
            | ({} if c["ok"] else {"tail": str(c.get("tail") or "")[-1500:]})
            | ({"first_failure": str(c["first_failure"])[-1500:]} if c.get("first_failure") else {})
            | ({"first": {k: c["first"].get(k) for k in ("exit_code", "same")}} if c.get("first") else {})
            | ({"preexisting": True} if c.get("preexisting") else {}) for c in checks]


def _shown(checks: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Check results kept with a saved candidate: the evidence record's, with the failing tests the verifier is
    shown for failures that predate the change (those of both runs, for a check that failed twice)."""
    rows = _brief(checks)
    for row, c in zip(rows, checks):
        ids = sorted({*(c.get("failures") or []), *((c.get("first") or {}).get("failures") or [])})
        if c.get("preexisting") and ids:
            row["failures"] = ids[:200]
    return rows


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


def _whose_output(check: dict[str, Any]) -> str:
    """For feedback that quotes a failed check's ``tail``: says so when it is the first of two runs' output."""
    first = check.get("first") or {}
    return ". Output of the first run" if first and not first.get("same") else ""


_FIRST_RUN = ("exit_code", "timed_out", "failures", "signature")  # what the comparison with the base commit reads


def _same_run(first: dict[str, Any], second: dict[str, Any]) -> bool:
    """Whether two failed runs of one check failed the same way: neither ran out of time, both ended with the same
    exit code, and they failed the same tests or, where the output names none, printed the same thing."""
    if first.get("timed_out") or second.get("timed_out") or first.get("exit_code") != second.get("exit_code"):
        return False
    key = "signature" if first.get("failures") is None and second.get("failures") is None else "failures"
    return first.get(key) == second.get(key)


def _check_note(check: dict[str, Any]) -> str:
    """What the pull request says about a check beyond pass or fail."""
    if check.get("cause") == "flaky":
        return " — failed on its first run, passed on a second"
    if check.get("timed_out"):
        return f" — {_how_failed(check)}" + ("; it times out on the base commit too, so not caused by this change"
                                             if check.get("preexisting") else "")
    return " — fails the same way on the base commit; not caused by this change" if check.get("preexisting") else ""


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


def _progress(output: str, total_bytes: int = 0) -> int:
    """How far a command had got when it was stopped at the time limit: how much it had printed, colours removed.
    A sandbox keeps only the start and the end of long output, so what is left is about as long whether the command
    printed a little more than that or gigabytes. ``total_bytes`` is the sandbox's count of everything printed, given
    when the output was cut, and is then the measure."""
    return max(len(_ANSI.sub("", output or "").strip()), total_bytes)


def _as_far(candidate: dict[str, Any], base: dict[str, Any]) -> bool:
    """Whether two runs stopped at the time limit had got about as far: neither had printed more than twice what
    the other had. What a run had printed by then depends on the machine's load, so the margin is wide; it still
    tells a suite that is too slow for the limit (both runs get about as far) from a change that makes the command
    hang or loop (one run stops printing early, or prints far more). Progress that was not recorded proves
    nothing."""
    mine, theirs = candidate.get("progress"), base.get("progress")
    if mine is None or theirs is None:
        return False
    return max(mine, theirs) <= 2 * min(mine, theirs)


def _same_failure(candidate: dict[str, Any], base: dict[str, Any] | None) -> bool:
    """Whether a check failing on the candidate fails the same way on the base commit, with nothing new. For pytest
    output: no node id that the base does not fail too. Other output has no test names to compare, and "the base
    exits non-zero as well" is not enough (a build can fail on the base for one reason and on the candidate for
    another), so both runs must end with the same exit code and print the same thing (``_output_signature``).
    A check that runs out of time on the candidate and on the base is too slow for the limit whatever the change
    does, provided the two runs had got about as far by then (``_as_far``: a candidate that stopped printing early,
    or printed far more, may hang or loop because of the change) and the candidate printed no failing test the base
    did not; one that runs out of time on the candidate only is the candidate's failure.
    A check that failed on two runs (``first`` holds the first) must match the base on both: what only one of them
    failed is still the candidate's, whichever run it was.
    Anything else is inconclusive, and the failure counts against the attempt."""
    if not base or base.get("ok", True):
        return False
    if candidate.get("first") and not _same_failure(candidate["first"], base):
        return False
    if candidate.get("timed_out"):
        return (bool(base.get("timed_out")) and _as_far(candidate, base)
                and set(candidate.get("failures") or []) <= set(base.get("failures") or []))
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
