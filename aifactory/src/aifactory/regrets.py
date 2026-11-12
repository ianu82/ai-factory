"""Canonical post-merge facts and gate evidence (no network calls or implicit commits).

Callers own the transaction. Source keys identify facts, not ingestion sessions.
Dates are UTC; naive legacy dates are interpreted as UTC, never as local time.
"""
from __future__ import annotations

import hashlib
import json
import math
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, Literal

from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from . import signals
from .db import utcnow
from .events import record
from .models import Artifact, GateVerdict, MergeRegret, Outcome, Repository, Run, Signal, Task, Workspace

RegretKind = Literal["smoke_failure", "factory_revert", "github_revert", "linked_cause"]
Verdict = Literal["pass", "fail", "skipped", "unknown"]
WindowStatus = Literal["in_window", "outside_window", "unknown"]
KINDS = {"smoke_failure", "factory_revert", "github_revert", "linked_cause"}


def timestamp(value: datetime | str | None) -> datetime | None:
    """Malformed/empty legacy evidence stays unknown."""
    try:
        at = datetime.fromisoformat(value) if isinstance(value, str) else value
        if not isinstance(at, datetime):
            return None
        return at.astimezone(UTC).replace(tzinfo=None) if at.tzinfo else at
    except ValueError:
        return None


def _iso(at: datetime | None) -> str | None:
    return at.isoformat() + "Z" if at is not None else None


@dataclass(frozen=True)
class ObservationWindow:
    days: float = 14

    def __post_init__(self) -> None:
        if not math.isfinite(self.days) or self.days <= 0:
            raise ValueError("Observation window must be finite and positive")

    def classify(self, merge_at: datetime | None, occurred_at: datetime | None) -> WindowStatus:
        start, occurrence = timestamp(merge_at), timestamp(occurred_at)
        if start is None or occurrence is None:
            return "unknown"
        return "in_window" if start <= occurrence <= start + timedelta(days=self.days) else "outside_window"

    @classmethod
    def for_workspace(cls, db: Session, workspace_id: str) -> ObservationWindow:
        workspace = db.get(Workspace, workspace_id)
        if workspace is None:
            raise ValueError("Workspace not found")
        return cls(days=float((workspace.settings or {}).get("regret_window_days", 14)))


@dataclass(frozen=True)
class RegretScope:
    workspace_id: str
    repo_id: str | None = None
    task_id: str | None = None
    run_id: str | None = None


def _task(db: Session, workspace_id: str, task_id: str, repo_id: str | None = None,
          run_id: str | None = None) -> Task:
    task = db.get(Task, task_id)
    if task is None or task.workspace_id != workspace_id:
        raise ValueError("Task does not belong to workspace")
    repo = db.get(Repository, repo_id or task.repo_id)
    if repo is None or repo.workspace_id != workspace_id or repo.id != task.repo_id:
        raise ValueError("Repository does not belong to task/workspace")
    outcome = db.get(Outcome, task.outcome_id)
    if outcome is None or outcome.workspace_id != workspace_id:
        raise ValueError("Task outcome does not belong to workspace")
    if run_id is not None:
        run = db.get(Run, run_id)
        if run is None or run.workspace_id != workspace_id or run.task_id != task_id:
            raise ValueError("Run does not belong to task/workspace")
    return task


def _signal(db: Session, workspace_id: str, repo_id: str, signal_id: str) -> Signal:
    signal = db.get(Signal, signal_id)
    if signal is None or signal.workspace_id != workspace_id or signal.repo_id != repo_id:
        raise ValueError("Signal does not belong to repository/workspace")
    return signal


def _fix(db: Session, workspace_id: str, repo_id: str, outcome_id: str | None) -> None:
    if outcome_id is None:
        return
    fix = db.get(Outcome, outcome_id)
    if fix is None or fix.workspace_id != workspace_id or repo_id not in (fix.repo_ids or []):
        raise ValueError("Fix outcome does not belong to repository/workspace")


def merge_time(task: Task) -> datetime | None:
    """Only explicit merge evidence establishes a merge date, not finished_at."""
    merge = (task.evidence or {}).get("merge")
    return timestamp(merge.get("at")) if isinstance(merge, dict) else None


def _key(value: str, name: str, length: int) -> str:
    # db._fit_strings clips oversized strings: never silently clip an identity key.
    if not value.strip() or len(value) > length:
        raise ValueError(f"{name} must be non-empty and at most {length} characters")
    return value


def _savepoint(db: Session):
    # Python's sqlite legacy transaction mode does not BEGIN for SELECT/SAVEPOINT.
    # Ensure releasing a savepoint cannot commit independently of the caller.
    connection = db.connection()
    if connection.dialect.name == "sqlite" and not connection.connection.driver_connection.in_transaction:
        connection.exec_driver_sql("BEGIN")
    return db.begin_nested()


def record_regret(
    db: Session, workspace_id: str, task_id: str, *, kind: RegretKind, source_key: str,
    reporter: str, occurred_at: datetime | str | None = None,
    detected_at: datetime | str | None = None, url: str = "", repo_id: str | None = None,
    run_id: str | None = None, source_signal_id: str | None = None,
    fix_outcome_id: str | None = None, fix_url: str = "", evidence: dict[str, Any] | None = None,
) -> tuple[MergeRegret, bool]:
    """Atomically insert a fact and its feedback, or return the canonical winner.

    Replay never reopens a signal or overwrites fix links. Link enrichment belongs
    to an explicit edit, not ingestion. Unknown occurrence is not detection time.
    """
    if kind not in KINDS:
        raise ValueError("Unknown regret kind")
    _key(source_key, "source_key", 500)
    _key(reporter, "reporter", 200)
    task = _task(db, workspace_id, task_id, repo_id, run_id)
    source = _signal(db, workspace_id, task.repo_id, source_signal_id) if source_signal_id else None
    _fix(db, workspace_id, task.repo_id, fix_outcome_id)
    if source and source.outcome_id:
        _fix(db, workspace_id, task.repo_id, source.outcome_id)
        fix_outcome_id = fix_outcome_id or source.outcome_id
    detection = utcnow() if detected_at is None else timestamp(detected_at)
    if detection is None:
        raise ValueError("Detection timestamp must be valid")
    query = db.query(MergeRegret).filter_by(workspace_id=workspace_id, task_id=task_id,
                                          kind=kind, source_key=source_key)
    existing = query.one_or_none()
    if existing is not None:
        return existing, False
    row = MergeRegret(workspace_id=workspace_id, task_id=task_id, repo_id=task.repo_id, run_id=run_id,
                      kind=kind, source_key=source_key, reporter=reporter, url=url,
                      occurred_at=timestamp(occurred_at), detected_at=detection, merge_at=merge_time(task),
                      source_signal_id=source_signal_id, fix_outcome_id=fix_outcome_id, fix_url=fix_url,
                      evidence=evidence or {})
    try:
        with _savepoint(db):
            db.add(row)
            db.flush()  # uniqueness arbitrates concurrent ingestion before any side effects
            if not fix_outcome_id and not fix_url:
                if source is not None:
                    row.signal_id = source.id
                else:
                    feedback, _ = signals.upsert_signal(
                        db, workspace_id, source="merge_regret", title=f"{kind}: {task.title}",
                        body=f"Regret {row.id} against {task.pr_url or task.id}", url=url or task.pr_url,
                        external_id=f"regret:{row.id}", repo_id=task.repo_id)
                    row.signal_id = feedback.id
            refs = {"regret_id": row.id, "pr_url": task.pr_url, "pr_number": task.pr_number}
            record(workspace_id, "merge_regret", f"{kind}: {task.title}", actor=reporter,
                   outcome_id=task.outcome_id, task_id=task.id, run_id=run_id,
                   data=refs, session=db, publish=False)
            db.add(Artifact(workspace_id=workspace_id, outcome_id=task.outcome_id, task_id=task.id,
                            run_id=run_id, kind="regret", name=f"{kind}: {source_key}", data=refs))
            db.flush()
    except IntegrityError:
        existing = query.one_or_none()
        if existing is None:
            raise
        return existing, False
    return row, True


def regret_dict(row: MergeRegret, window: ObservationWindow = ObservationWindow()) -> dict[str, Any]:
    status = window.classify(row.merge_at, row.occurred_at)
    return {"id": row.id, "workspace_id": row.workspace_id, "task_id": row.task_id,
            "repo_id": row.repo_id, "run_id": row.run_id, "kind": row.kind,
            "source_key": row.source_key, "reporter": row.reporter, "url": row.url,
            "occurred_at": _iso(row.occurred_at), "detected_at": _iso(row.detected_at),
            "merge_at": _iso(row.merge_at), "window_status": status, "in_window": status == "in_window",
            "source_signal_id": row.source_signal_id, "signal_id": row.signal_id,
            "fix_outcome_id": row.fix_outcome_id, "fix_url": row.fix_url, "evidence": row.evidence}


def regrets_for_scope(db: Session, scope: RegretScope,
                      window: ObservationWindow | None = None) -> list[dict[str, Any]]:
    """Resolve canonical evidence for a task or any of its runs without copying Run.result."""
    window = window or ObservationWindow.for_workspace(db, scope.workspace_id)
    task_id = scope.task_id
    if scope.run_id is not None:
        run = db.get(Run, scope.run_id)
        if run is None or run.workspace_id != scope.workspace_id or run.task_id is None:
            raise ValueError("Run does not belong to workspace/task")
        if task_id is not None and task_id != run.task_id:
            raise ValueError("Run does not belong to task")
        task_id = run.task_id
    if task_id is not None:
        _task(db, scope.workspace_id, task_id, scope.repo_id)
    if scope.repo_id is not None:
        repo = db.get(Repository, scope.repo_id)
        if repo is None or repo.workspace_id != scope.workspace_id:
            raise ValueError("Repository does not belong to workspace")
    query = db.query(MergeRegret).filter_by(workspace_id=scope.workspace_id)
    if task_id is not None:
        query = query.filter_by(task_id=task_id)
    if scope.repo_id is not None:
        query = query.filter_by(repo_id=scope.repo_id)
    return [regret_dict(row, window) for row in query.order_by(MergeRegret.detected_at, MergeRegret.id)]


def record_gate_verdict(
    db: Session, workspace_id: str, task_id: str, *, gate: str, attempt: int, verdict: Verdict,
    candidate_fingerprint: str = "", head_sha: str = "", run_id: str | None = None,
    session_key: str = "", at: datetime | str | None = None, evidence: dict[str, Any] | None = None,
    score: float | None = None,
) -> tuple[GateVerdict, bool]:
    """Each verifier run/session is distinct; replay of the same session is not."""
    _task(db, workspace_id, task_id, run_id=run_id)
    _key(gate, "gate", 100)
    if verdict not in {"pass", "fail", "skipped", "unknown"} or attempt < 1:
        raise ValueError("Invalid gate verdict/attempt")
    if score is not None and not math.isfinite(score):
        raise ValueError("Gate score must be finite")
    if not candidate_fingerprint and not head_sha:
        raise ValueError("Gate requires a candidate fingerprint or head")
    if len(candidate_fingerprint) > 200 or len(head_sha) > 64:
        raise ValueError("Candidate identity too long")
    # The run is always part of session identity even when a caller also names a session.
    if len(session_key) > 160:
        raise ValueError("Session key too long")
    session = hashlib.sha256(json.dumps([run_id, session_key]).encode()).hexdigest()
    candidate = hashlib.sha256(json.dumps([candidate_fingerprint, head_sha]).encode()).hexdigest()
    identity = dict(workspace_id=workspace_id, task_id=task_id, gate=gate, attempt=attempt,
                    candidate_key=candidate, session_key=session)
    query = db.query(GateVerdict).filter_by(**identity)
    existing = query.one_or_none()
    if existing is not None:
        return existing, False
    row = GateVerdict(**identity, candidate_fingerprint=candidate_fingerprint, head_sha=head_sha,
                      run_id=run_id, verdict=verdict, at=timestamp(at), evidence=evidence or {}, score=score)
    try:
        with _savepoint(db):
            db.add(row)
            db.flush()
    except IntegrityError:
        existing = query.one_or_none()
        if existing is None:
            raise
        return existing, False
    return row, True


def merge_gate_passes(db: Session, workspace_id: str) -> set[tuple[str, str]]:
    """Unique (task, gate) accepts of the merged head, never session counts.

    Unknown/missing heads cannot establish that a gate accepted the merged candidate.
    """
    rows = (db.query(GateVerdict.task_id, GateVerdict.gate)
            .join(Task, Task.id == GateVerdict.task_id)
            .filter(GateVerdict.workspace_id == workspace_id, Task.workspace_id == workspace_id,
                    GateVerdict.verdict == "pass", GateVerdict.head_sha != "",
                    GateVerdict.head_sha == Task.head_sha, Task.merge_sha != "").distinct())
    return set(rows.all())


def normalized_diff_signature(diff: bytes | None, *, complete: bool = True) -> str | None:
    """v1 exact-payload heuristic, shared by delivery and historical analytics.

    Sort files and added/deleted payloads, removing whitespace and ignoring context,
    hunk coordinates and transport headers. Renames and binary payloads are facts,
    not headers. Missing, explicitly clipped or incomplete hunks stay unknown (None);
    callers must never use that unknown value as an equality key.
    """
    import re

    if not complete or not diff or len(diff) > 16_000_000 or not diff.endswith(b"\n"):
        return None
    # Git binary patches encode zlib streams. Validate them before hashing so a
    # clipped stream (even one ending at a line boundary) cannot look complete.
    if b"GIT binary patch\n" in diff:
        import base64
        import zlib

        blocks = diff.split(b"GIT binary patch\n")[1:]
        try:
            for section in blocks:
                body = section.split(b"diff --git ", 1)[0].strip()
                for block in body.split(b"\n\n"):
                    lines = block.splitlines()
                    kind, size = lines[0].split()
                    if kind not in (b"literal", b"delta"):
                        return None
                    compressed = bytearray()
                    for line in lines[1:]:
                        length = line[0] - 64 if 65 <= line[0] <= 90 else line[0] - 70
                        if not 1 <= length <= 52 or len(line[1:]) != ((length + 3) // 4) * 5:
                            return None
                        compressed.extend(base64.b85decode(line[1:])[:length])
                    stream = zlib.decompressobj()
                    # Bound expansion independently of the untrusted size header.
                    content = stream.decompress(bytes(compressed), 16_000_001)
                    if not stream.eof or stream.unused_data or len(content) != int(size):
                        return None
        except (ValueError, IndexError, zlib.error):
            return None
    files: list[tuple[bytes, list[bytes]]] = []
    path, payload = b"", []
    old = new = 0
    binary = False
    saw_change = False
    for line in diff.splitlines():
        if line.startswith(b"diff --git "):
            if old or new:
                return None
            if path:
                files.append((path, sorted(payload)))
            path, payload = line[11:], []
            binary = False
        elif not path:
            return None
        elif line.startswith(b"@@ "):
            if old or new:
                return None
            hunk = re.match(rb"@@ -(\d+)(?:,(\d+))? \+(\d+)(?:,(\d+))? @@", line)
            if not hunk:
                return None
            old = int(hunk[2] if hunk[2] is not None else b"1")
            new = int(hunk[4] if hunk[4] is not None else b"1")
        elif old or new:
            if line.startswith(b"\\ No newline"):
                continue
            if line.startswith(b"+"):
                new -= 1
            elif line.startswith(b"-"):
                old -= 1
            elif line.startswith(b" "):
                old -= 1
                new -= 1
            else:
                return None
            if old < 0 or new < 0:
                return None
            if line[:1] in (b"+", b"-"):
                payload.append(line[:1] + re.sub(rb"\s+", b"", line[1:]))
                saw_change = True
        elif line.startswith(b"Binary files "):
            # GitHub's binary-only marker omits the actual bytes. It cannot
            # establish equivalence between two different binary changes.
            return None
        elif line.startswith((b"rename from ", b"rename to ", b"GIT binary patch")):
            payload.append(line)
            binary = line == b"GIT binary patch"
            saw_change = True
        elif binary and line:
            payload.append(line)
        elif line.startswith((b"--- ", b"+++ ", b"index ", b"similarity index ", b"dissimilarity index ",
                              b"old mode ", b"new mode ", b"new file mode ", b"deleted file mode ",
                              b"\\ No newline")) or not line:
            continue
        else:
            # Includes truncation notices and payload outside a complete hunk.
            return None
    if old or new or not path or not saw_change:
        return None
    files.append((path, sorted(payload)))
    canonical = [[p.hex(), [v.hex() for v in values]] for p, values in sorted(files)]
    return "diff-v1:" + hashlib.sha256(json.dumps(canonical, separators=(",", ":")).encode()).hexdigest()
