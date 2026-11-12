"""Versioned benchmark snapshots and database-only delivered-task export.

Eval adapter contracts (T2-T7; replaceable by #208):
* load_eval_suite(id="smoke", root=None) -> frozen SuiteSpec; snapshot() is
  JSON {schema_version:1,id,cases:[{id,revision,task,acceptance,fixtures,
  setup_commands,check_commands,scorer_ids,runtime_qa}]}. Revisions are SHA-256
  over canonical content (including fixture bytes), never mutable branch names.
* get_scorer(id)(case, evidence) -> EvalScore; model_dump() returns JSON
  {scorer_id,scorer_revision,verdict:pass|fail|error,score:0..1|null,explanation}.
  checks-v1 consumes checks:[{command,exit_code}] in declaration order.
* snapshot_configuration(workspace_id, settings=..., prompts=..., prompt_version=...,
  execution=..., credential_values=()) -> EvalConfiguration; get_configuration
  scopes immutable lookup to a workspace and rejects unknown IDs. Snapshot JSON:
  {settings:<merged>,prompts:{name:text},prompt_version,execution:<public inputs>}.
  Environment mappings and credential-bearing fields (normalized key spellings)
  are omitted before hashing. Public text is preserved verbatim: credential values
  are never substring-replaced or included in the hash. credential_values is a
  compatibility argument, not a text-redaction dictionary. Callers must keep
  credentials in dedicated fields/env, not interpolate them into prompts or
  commands; recognizable credential-bearing URLs/bearer text are refused,
  including normalized/percent-encoded query and fragment parameter names.
  Behavioral token counts/limits are public snapshot inputs; credential names
  such as access_token remain private even when suffixed with budget/limit.
* EvalSchedule.suites holds {id,revision} entries; matrix holds
  {worker,model,configuration_id} cells. Nightly periods are UTC YYYY-MM-DD;
  on-demand runs have null schedule/period. invocation_id groups a launch.
  Default thresholds: pass_rate_drop_points=10, cost_per_pass_increase_percent=30.
* T2 reserves against EvalDailyBudget(workspace, UTC day) under a lock, persisting
  reservation_id/reserved_usd before execution; releases once using
  reservation_released_at. Unknown cost must stop admission, never count as zero.
* T3 writes suite/case snapshots, requested_identity before external effects;
  effective_identity describes the actual {worker,model,configuration_id,provider,
  sandbox_kind}. T3 uses the existing harness.base.HarnessSession/HarnessResult
  contracts, not a second delivery pipeline. No credentials. Usage JSON: {input_tokens,output_tokens}; attempts
  includes failures. evidence JSON: {checks,runtime_qa,artifacts}; artifact paths
  are sandbox-relative. diff JSON: {previous_passing_run_id,summary,patch}.
  Transcript/evidence must be scrubbed and bounded at their egress boundary.
* T4 claims schedule/run/case rows in short locked transactions, changing owner,
  expiry and generation on every claim. ORM version_id_col fences ALL writes;
  bulk SQL MUST explicitly predicate on claim_generation and increment it.
  Check the schedule generation under lock before inserting any launch. Never
  hold locks across execution. StaleDataError means discard stale work.
* Lifecycle: queued -> running -> passed|failed|error|cancelled|budget_exceeded;
  case verdict is pass|fail|error or null before completion. reason explains stops.
* T5 aggregate JSON: {case_count,pass_count,pass_rate,mean_cost_usd,cost_per_pass_usd,
  p50_duration_seconds,p95_duration_seconds,cost_known,comparison}. pass_rate is
  0..1, unavailable costs/percentiles are null, not zero; comparison references
  previous_run_id of the same suite revision/requested cell, workspace only.
  T6/T7 consume these records without creating Outcome/Task/delivery Run rows.

Existing delivered-task schema v1 is deliberately self-contained: no branch resolution, credentials, or
provider calls are needed to parse, load, or export a suite.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import tempfile
from collections import Counter
from collections.abc import Mapping
from importlib import resources
from pathlib import Path
from typing import Any, Protocol
from urllib.parse import unquote_plus

from pydantic import BaseModel, ConfigDict, Field, field_validator
from sqlalchemy import select, text

from .config_versions import canonical_json
from .db import session_scope
from .models import BenchmarkSuite, EvalConfiguration, Repository, Task, Workspace


class SuiteError(ValueError):
    """Invalid suite content or workspace scope."""


def default_output_root() -> Path:
    """Use the current source checkout, never a path relative to site-packages.

    Packaged deployments without a checkout must choose a durable location with
    --output-root instead of silently writing into the Python installation.
    """
    for root in (Path.cwd(), *Path.cwd().parents):
        product = root / "aifactory"
        if (product / "src/aifactory/cli.py").is_file():
            return product / "benchmarks"
        if (root / "src/aifactory/cli.py").is_file():
            return root / "benchmarks"
    raise SuiteError("No source checkout found; specify --output-root with a writable export directory")


def _safe_name(value: Any, field: str, limit: int = 80) -> str:
    if not isinstance(value, str) or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]*", value) or len(value) > limit:
        raise SuiteError(f"{field} must be a safe name (letters, digits, underscore, hyphen; max {limit})")
    return value


def _string(value: Any, field: str, limit: int | None = None) -> None:
    if not isinstance(value, str) or not value.strip() or (limit is not None and len(value) > limit):
        raise SuiteError(f"{field} must be a nonempty string" + (f" of at most {limit} characters" if limit else ""))


def _commit(value: Any, field: str) -> None:
    if not isinstance(value, str) or not re.fullmatch(r"(?:[0-9a-fA-F]{40}|[0-9a-fA-F]{64})", value):
        raise SuiteError(f"{field} must be a full 40- or 64-character hexadecimal commit identifier")


def validate_suite(value: Any) -> dict[str, Any]:
    if not isinstance(value, dict) or set(value) != {"schema_version", "name", "tasks"}:
        raise SuiteError("Suite must contain schema_version, name and tasks only")
    if type(value["schema_version"]) is not int or value["schema_version"] != 1:
        raise SuiteError("Unsupported schema_version (expected 1)")
    _safe_name(value["name"], "Suite name")
    if not isinstance(value["tasks"], list) or not value["tasks"]:
        raise SuiteError("Suite tasks must be a nonempty list")
    keys = set()
    required = {"key", "repository", "base_commit", "title", "specification", "acceptance"}
    for task in value["tasks"]:
        if not isinstance(task, dict) or not required <= set(task) or set(task) - required - {"reference_change"}:
            raise SuiteError("Task must contain key, repository, base_commit, title, specification and acceptance")
        key = _safe_name(task["key"], "Task key", 100)
        if key in keys:
            raise SuiteError(f"Duplicate task key: {key}")
        keys.add(key)
        repo = task["repository"]
        if not isinstance(repo, str) or len(repo) > 200 or not re.fullmatch(r"[A-Za-z0-9_-]+/[A-Za-z0-9_.-]+", repo) or repo.split("/")[1] in {".", ".."}:
            raise SuiteError(f"Invalid repository for {key}: expected owner/name")
        _commit(task["base_commit"], f"{key}.base_commit")
        _string(task["title"], f"{key}.title", 300)
        _string(task["specification"], f"{key}.specification")
        if not isinstance(task["acceptance"], list) or not task["acceptance"]:
            raise SuiteError(f"{key}.acceptance must be a nonempty list")
        for criterion in task["acceptance"]:
            _string(criterion, f"{key}.acceptance item")
        if "reference_change" in task:
            ref = task["reference_change"]
            if not isinstance(ref, dict) or set(ref) != {"pr_number", "pr_url", "merge_sha"}:
                raise SuiteError(f"{key}.reference_change must contain pr_number, pr_url and merge_sha")
            if type(ref["pr_number"]) is not int or ref["pr_number"] <= 0:
                raise SuiteError(f"{key}.pr_number must be a positive integer")
            if ref["pr_url"] != f"https://github.com/{repo}/pull/{ref['pr_number']}":
                raise SuiteError(f"{key}.pr_url must match repository and PR number")
            _commit(ref["merge_sha"], f"{key}.merge_sha")
    # Detach caller-owned mutable objects from the stored snapshot.
    return json.loads(json.dumps(value, ensure_ascii=False))


def _object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise SuiteError(f"Duplicate JSON field: {key}")
        result[key] = value
    return result


def parse_suite(path: str | Path) -> dict[str, Any]:
    path = Path(path)
    if path.is_dir():
        path = path / "suite.json"
    try:
        return validate_suite(json.loads(path.read_text(encoding="utf-8"), object_pairs_hook=_object))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise SuiteError(f"Cannot read suite JSON: {exc}") from exc


def load_suite(workspace_id: str, path: str | Path) -> BenchmarkSuite:
    suite = parse_suite(path)  # Validate the entire document before any writes.
    digest = hashlib.sha256(json.dumps(suite, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()).hexdigest()
    with session_scope() as session:
        # FOR UPDATE is ignored on SQLite. Acquire the writer lock *before* any
        # reads so competing CLI processes see the committed revision/hash.
        if session.get_bind().dialect.name == "sqlite":
            session.execute(text("BEGIN IMMEDIATE"))
        workspace = session.scalar(select(Workspace).where(Workspace.id == workspace_id).with_for_update())
        if workspace is None:
            raise SuiteError("Workspace not found")
        repos = set(session.scalars(select(Repository.full_name).where(Repository.workspace_id == workspace_id)))
        foreign = {t["repository"] for t in suite["tasks"]} - repos
        if foreign:
            raise SuiteError(f"Repositories not in workspace: {', '.join(sorted(foreign))}")
        revisions = list(session.scalars(select(BenchmarkSuite).where(
            BenchmarkSuite.workspace_id == workspace_id, BenchmarkSuite.name == suite["name"],
        )))
        for existing in revisions:
            if existing.content_hash == digest:
                return existing
        snapshot = BenchmarkSuite(
            workspace_id=workspace_id, name=suite["name"], schema_version=1,
            revision=max((r.revision for r in revisions), default=0) + 1,
            content_hash=digest, tasks=suite["tasks"],
        )
        session.add(snapshot)
        session.flush()
        return snapshot


def export_history(workspace_id: str, repository: str, name: str,
                   output_root: str | Path | None = None, report=print) -> Path:
    """Export only stored evidence; exclusions are reported even if export is empty."""
    _safe_name(name, "Suite name")
    root = Path(output_root) if output_root is not None else default_output_root()
    tasks = []
    excluded: Counter[str] = Counter()
    with session_scope() as session:
        if session.get(Workspace, workspace_id) is None:
            raise SuiteError("Workspace not found")
        repo = session.scalar(select(Repository).where(
            Repository.workspace_id == workspace_id, Repository.full_name == repository,
        ))
        if repo is None:
            raise SuiteError("Repository not in workspace")
        records = session.scalars(select(Task).where(
            Task.workspace_id == workspace_id, Task.repo_id == repo.id,
        ).order_by(Task.id))
        for task in records:
            if task.status != "completed":
                excluded["not completed"] += 1
                continue
            if not task.base_sha:
                excluded["missing base_sha"] += 1
                continue
            if not task.merge_sha or not task.pr_number or not task.pr_url:
                excluded["missing merge/PR evidence"] += 1
                continue
            candidate = {
                "key": f"task-{task.id}", "repository": repo.full_name,
                "base_commit": task.base_sha, "title": task.title,
                "specification": task.spec, "acceptance": task.acceptance,
                "reference_change": {"pr_number": task.pr_number, "pr_url": task.pr_url, "merge_sha": task.merge_sha},
            }
            try:
                validate_suite({"schema_version": 1, "name": name, "tasks": [candidate]})
            except SuiteError as exc:
                excluded[f"invalid stored record: {exc}"] += 1
                continue
            tasks.append(candidate)
    report(f"Included {len(tasks)} task(s); excluded {sum(excluded.values())} task(s)")
    for reason, count in sorted(excluded.items()):
        report(f"  {count}: {reason}")
    if not tasks:
        raise SuiteError("No complete delivered tasks to export")
    suite = validate_suite({"schema_version": 1, "name": name, "tasks": tasks})
    directory = root / name
    directory.mkdir(parents=True, exist_ok=True)
    destination = directory / "suite.json"
    # Write-then-rename prevents a failed export from leaving partial JSON.
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", dir=directory, delete=False) as stream:
            temporary = stream.name
            stream.write(json.dumps(suite, indent=2, ensure_ascii=False) + "\n")
        os.replace(temporary, destination)
    finally:
        if temporary and os.path.exists(temporary):
            os.unlink(temporary)
    return destination


# Eval adapter v1. Kept separate from delivered-task imports above: a fixture
# suite is self-contained and must never be resolved to a real repository.
MAX_SUITE_BYTES = 1_048_576
MAX_FIXTURE_BYTES = 262_144
DEFAULT_THRESHOLDS = {"pass_rate_drop_points": 10, "cost_per_pass_increase_percent": 30}


def _revision(value: Any) -> str:
    return hashlib.sha256(canonical_json(value).encode()).hexdigest()


def fixture_path(value: str) -> str:
    """Portable relative POSIX path, validated again when materializing fixtures."""
    if (not isinstance(value, str) or not value or len(value) > 240 or value.startswith("/") or "\\" in value or ":" in value
            or any(p in ("", ".", "..") for p in value.split("/")) or "\x00" in value):
        raise SuiteError("Fixture paths must be relative, without traversal")
    return value


def _read_resource(root, name: str, limit: int) -> bytes:
    path = root
    # A filesystem root (or any of its ancestors) must not itself be a symlink.
    if isinstance(root, Path) and any(p.is_symlink() for p in (root, *root.parents)):
        raise SuiteError("Symlinks are forbidden in fixture paths")
    for part in fixture_path(name).split("/"):
        path = path.joinpath(part)
        if isinstance(path, Path) and path.is_symlink():
            raise SuiteError("Symlinks are forbidden in fixture paths")
    try:
        if not path.is_file():
            raise SuiteError("Fixture must be a regular file")
        with path.open("rb") as stream:
            data = stream.read(limit + 1)
    except OSError as exc:
        raise SuiteError("Cannot read fixture") from exc
    if len(data) > limit:
        raise SuiteError("Fixture exceeds byte limit")
    return data


class CaseSpec(BaseModel):
    """JSON-ready immutable input. fixtures maps destination paths to UTF-8 text."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    id: str = Field(pattern=r"^[a-z][a-z0-9-]{0,79}$")
    task: str = Field(min_length=1, max_length=32000)
    acceptance: tuple[str, ...] = Field(min_length=1, max_length=40)
    # Store JSON internally to prevent accidental mutation of content-addressed input.
    fixtures_json: str
    setup_commands: tuple[str, ...] = Field(default=(), max_length=20)
    check_commands: tuple[str, ...] = Field(min_length=1, max_length=20)
    scorer_ids: tuple[str, ...] = ("checks-v1",)
    runtime_qa_json: str = "null"

    @field_validator("fixtures_json")
    @classmethod
    def valid_fixtures(cls, value):
        if len(value.encode()) > MAX_SUITE_BYTES:
            raise SuiteError("Fixtures exceed byte limit")
        files = json.loads(value, object_pairs_hook=_object)
        if not isinstance(files, dict) or not 1 <= len(files) <= 40:
            raise SuiteError("Expected 1-40 fixture files")
        for path, content in files.items():
            fixture_path(path)
            if not isinstance(content, str) or len(content.encode()) > MAX_FIXTURE_BYTES:
                raise SuiteError("Invalid fixture content")
        return canonical_json(files)

    @field_validator("scorer_ids")
    @classmethod
    def valid_scorers(cls, value):
        if not value or len(set(value)) != len(value):
            raise SuiteError("Scorer IDs must be nonempty and unique")
        for scorer in value:
            get_scorer(scorer)
        return value

    @field_validator("runtime_qa_json")
    @classmethod
    def valid_qa(cls, value):
        if len(value.encode()) > MAX_SUITE_BYTES:
            raise SuiteError("runtime_qa exceeds byte limit")
        qa = json.loads(value, object_pairs_hook=_object)
        if qa is not None and not isinstance(qa, dict):
            raise SuiteError("runtime_qa must be an object or null")
        return canonical_json(qa)

    @property
    def fixtures(self) -> dict[str, str]:
        return json.loads(self.fixtures_json)

    @property
    def runtime_qa(self) -> dict | None:
        return json.loads(self.runtime_qa_json)

    def snapshot(self) -> dict:
        value = self.model_dump(mode="json", exclude={"fixtures_json", "runtime_qa_json"})
        return {**value, "fixtures": self.fixtures, "runtime_qa": self.runtime_qa}

    @property
    def revision(self) -> str:
        return _revision(self.snapshot())


class SuiteSpec(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    id: str = Field(pattern=r"^[a-z][a-z0-9-]{0,79}$")
    schema_version: int = Field(default=1, ge=1, le=1)
    cases: tuple[CaseSpec, ...] = Field(min_length=1, max_length=40)

    def snapshot(self) -> dict:
        return {"schema_version": self.schema_version, "id": self.id,
                "cases": [{**c.snapshot(), "revision": c.revision} for c in self.cases]}

    @property
    def revision(self) -> str:
        return _revision(self.snapshot())


class EvalScore(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False)
    scorer_id: str
    scorer_revision: str
    verdict: str = Field(pattern=r"^(pass|fail|error)$")
    score: float | None = Field(default=None, ge=0, le=1)
    explanation: str = Field(min_length=1, max_length=16000)


class SuiteLoader(Protocol):
    def __call__(self, suite_id: str, *, root: Path | None = None) -> SuiteSpec: ...


class EvalScorer(Protocol):
    def __call__(self, case: CaseSpec, evidence: Mapping[str, Any]) -> EvalScore: ...


class ConfigurationLookup(Protocol):
    def __call__(self, workspace_id: str, configuration_id: str) -> EvalConfiguration: ...


def checks_scorer(case: CaseSpec, evidence: Mapping[str, Any]) -> EvalScore:
    """Fail closed on missing/mismatched check evidence; never execute commands here."""
    checks = evidence.get("checks")
    valid = (isinstance(checks, list) and len(checks) == len(case.check_commands)
             and all(isinstance(c, dict) and c.get("command") == command
                     and type(c.get("exit_code")) is int
                     for c, command in zip(checks, case.check_commands)))
    verdict = "error" if not valid else "pass" if all(
        c["exit_code"] == 0 and not c.get("timed_out", False) for c in checks) else "fail"
    return EvalScore(scorer_id="checks-v1", scorer_revision=_revision("checks-v1"), verdict=verdict,
                     score=None if not valid else float(verdict == "pass"),
                     explanation="Missing or mismatched check evidence" if not valid else
                     "All declared checks passed" if verdict == "pass" else "A declared check failed")


def get_scorer(scorer_id: str) -> EvalScorer:
    if scorer_id != "checks-v1":
        raise SuiteError("Unknown eval scorer ID")
    return checks_scorer


def load_eval_suite(suite_id: str = "smoke", *, root: Path | None = None) -> SuiteSpec:
    """Read a bounded offline fixture suite; root is an explicit suite directory."""
    _safe_name(suite_id, "Suite ID")
    directory = root if root is not None else resources.files("aifactory").joinpath("eval_data", suite_id)
    try:
        raw = json.loads(_read_resource(directory, "suite.json", MAX_SUITE_BYTES), object_pairs_hook=_object)
        if (not isinstance(raw, dict) or set(raw) != {"schema_version", "id", "cases"}
                or type(raw["schema_version"]) is not int or raw["schema_version"] != 1
                or raw["id"] != suite_id or not isinstance(raw["cases"], list)
                or not 1 <= len(raw["cases"]) <= 40):
            raise SuiteError("Invalid eval suite schema")
        cases = []
        total = 0
        for entry in raw["cases"]:
            entry = dict(entry)
            files = entry.pop("fixtures")
            if not isinstance(files, dict) or not 1 <= len(files) <= 40:
                raise SuiteError("Expected 1-40 fixture files")
            fixtures = {}
            for destination, source in files.items():
                fixture_path(destination)
                data = _read_resource(directory, source, MAX_FIXTURE_BYTES)
                total += len(data)
                if total > MAX_SUITE_BYTES:
                    raise SuiteError("Suite fixtures exceed byte limit")
                fixtures[destination] = data.decode("utf-8")
            qa = entry.pop("runtime_qa", None)
            if qa is not None and not isinstance(qa, dict):
                raise SuiteError("runtime_qa must be an object or null")
            case = CaseSpec(**entry, fixtures_json=canonical_json(fixtures), runtime_qa_json=canonical_json(qa))
            for scorer in case.scorer_ids:
                get_scorer(scorer)
            if not case.scorer_ids or any(not s.strip() for s in (*case.acceptance, *case.setup_commands, *case.check_commands)):
                raise SuiteError("Empty scorer, acceptance or command")
            cases.append(case)
        if len({c.id for c in cases}) != len(cases):
            raise SuiteError("Duplicate eval case ID")
        return SuiteSpec(id=suite_id, cases=tuple(cases))
    except (ValueError, TypeError, KeyError, UnicodeError, RecursionError) as exc:
        raise SuiteError("Invalid eval suite") from exc


def _normalized_key(key: str) -> str:
    key = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", key)
    return re.sub(r"[^a-z0-9]+", "_", key.lower()).strip("_")


def _private_key(key: str) -> bool:
    key = _normalized_key(key)
    parts = set(key.split("_"))
    # Token counts/ceilings are public execution settings, not bearer tokens.
    # Recognize their structure rather than a few exact spellings, but require
    # every component to describe a count or limit: access_token_budget and
    # max_auth_token must still be treated as credentials.
    token_limit_parts = {"token", "tokens", "budget", "limit", "ceiling", "max", "min",
                         "input", "output", "prompt", "completion", "context", "total",
                         "run", "request", "case", "day", "daily", "per", "minute", "second",
                         "count", "counts", "hour", "hourly"}
    if (parts & {"token", "tokens"} and parts <= token_limit_parts
            and (parts & {"budget", "limit", "ceiling", "max", "min", "count", "counts"}
                 or "tokens" in parts)):
        return False
    # Only credential-bearing key names are private. A generic *_key rule also
    # removes behavioural inputs such as sort_key, cache_key and partition_key.
    credential_keys = {"api_key", "access_key", "private_key", "signing_key", "encryption_key",
                       "client_key", "subscription_key"}
    return bool(parts & {"secret", "secrets", "password", "credentials", "credential", "authorization",
                         "auth", "authentication", "pat", "cookie", "cookies", "token", "feedback"}
                or key in {"apikey", "accesstoken", "key"}
                or any(key == name or key.endswith("_" + name) for name in credential_keys))


def _public_settings(value: Any) -> Any:
    """Remove credential-bearing fields, never rewrite incidental public text.

    A credential can be any string, including 'run' or 'model'. Matching those
    values in prompts/commands would change execution identity when only the
    credential rotates. Classify by field meaning instead, and refuse explicit
    credential syntax in public text rather than storing a modified instruction.
    """
    if isinstance(value, dict):
        return {key: _public_settings(item) for key, item in value.items()
                if not _private_key(key) and _normalized_key(key) != "env"}
    if isinstance(value, (list, tuple)):
        return [_public_settings(v) for v in value]
    if isinstance(value, str):
        # Query names use the same credential classifier as JSON fields, including
        # separator/case variants and percent-encoded names. Check public text
        # (prompts and commands too), not just dedicated URL fields; never rewrite
        # the instructions or include an embedded credential in the content hash.
        private_query = any(_private_key(unquote_plus(match.group(1)))
                            for match in re.finditer(r"[?&#]([^?&#=\s]+)=", value))
        if private_query or re.search(r"(?i)(://[^/\s]*@|bearer\s+)", value):
            raise SuiteError("Credential-bearing execution text is forbidden")
    return value


def _bound_json(value: Any) -> None:
    """Reject oversized/deep inputs before canonical serialization creates a copy."""
    remaining = MAX_SUITE_BYTES

    def visit(item, depth):
        nonlocal remaining
        remaining -= 1
        if remaining < 0 or depth > 30:
            raise SuiteError("Configuration exceeds size/depth limit")
        if isinstance(item, str):
            if len(item) > remaining:
                raise SuiteError("Configuration exceeds byte limit")
            remaining -= len(item.encode("utf-8"))
        elif isinstance(item, dict):
            for key, val in item.items():
                if not isinstance(key, str):
                    raise SuiteError("Configuration keys must be strings")
                visit(key, depth + 1)
                visit(val, depth + 1)
        elif isinstance(item, (list, tuple)):
            for val in item:
                visit(val, depth + 1)
        elif item is not None and not isinstance(item, (bool, int, float)):
            raise SuiteError("Configuration must contain JSON values only")
        if remaining < 0:
            raise SuiteError("Configuration exceeds byte limit")

    visit(value, 0)


def snapshot_configuration(workspace_id: str, *, settings: dict, prompts: dict[str, str],
                           prompt_version: str, execution: dict,
                           credential_values: tuple[str, ...] = ()) -> EvalConfiguration:
    """Persist/reuse a secret-free hash; no provider resolution or credential lookup."""
    from .pipeline.runtime import merged_settings

    raw = {"settings": settings, "prompts": prompts, "prompt_version": prompt_version, "execution": execution}
    # Bound before recursive processing; callers must pass JSON primitives, not provider objects.
    _bound_json(raw)
    encoded = canonical_json(raw)
    if len(encoded.encode()) > MAX_SUITE_BYTES:
        raise SuiteError("Configuration exceeds byte limit")
    public = _public_settings(raw)
    public["settings"] = _public_settings(merged_settings(public["settings"]))
    public = json.loads(canonical_json(public))
    digest = _revision(public)
    with session_scope() as session:
        if session.get(Workspace, workspace_id) is None:
            raise SuiteError("Workspace not found")
        existing = session.get(EvalConfiguration, (workspace_id, digest))
        if existing is not None:
            return existing
        from sqlalchemy.exc import IntegrityError
        row = EvalConfiguration(workspace_id=workspace_id, id=digest, snapshot=public)
        try:
            with session.begin_nested():
                session.add(row)
                session.flush()
        except IntegrityError:
            return session.get(EvalConfiguration, (workspace_id, digest))
        return row


def get_configuration(workspace_id: str, configuration_id: str) -> EvalConfiguration:
    with session_scope() as session:
        row = session.get(EvalConfiguration, (workspace_id, configuration_id))
        if row is None:
            raise SuiteError("Unknown eval configuration ID in workspace")
        return row
