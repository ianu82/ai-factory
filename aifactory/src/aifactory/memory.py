"""Tenant-bound memory CRUD for SDK adapters and management clients; no Anton dependency.

Bind a service to trusted workspace/repository/run IDs, never tool-supplied model
attributes. A repository view includes workspace-wide memories. An unbound repository
view is workspace management scope. Writes require a source run; reads/deletes may
be bound to a run to restrict them to that run's lessons. Authorization of human
roles belongs to the caller (the HTTP API uses _ws).
"""

from __future__ import annotations

import hashlib
import math
from dataclasses import dataclass

from sqlalchemy import and_, func, or_, update
from sqlalchemy.orm import Session

from .db import session_scope, utcnow
from .events import record
from .models import MemoryEntry, Outcome, Repository, Run, Task, Workspace


class MemoryScopeError(LookupError):
    """Missing or unauthorized scope/object. Do not disclose which ID failed."""


@dataclass(frozen=True)
class MemoryService:
    workspace_id: str
    repo_id: str | None = None
    run_id: str | None = None

    def _scope(self, db: Session) -> Run | None:
        if db.get(Workspace, self.workspace_id) is None:
            raise MemoryScopeError("Memory scope not found")
        if self.repo_id is not None:
            repo = db.get(Repository, self.repo_id)
            if repo is None or repo.workspace_id != self.workspace_id:
                raise MemoryScopeError("Memory scope not found")
        run = None
        if self.run_id is not None:
            run = db.get(Run, self.run_id)
            if run is None or run.workspace_id != self.workspace_id:
                raise MemoryScopeError("Memory scope not found")
            outcome = db.get(Outcome, run.outcome_id)
            if outcome is None or outcome.workspace_id != self.workspace_id:
                raise MemoryScopeError("Memory scope not found")
            allowed = outcome.repo_ids or []
            if run.task_id is not None:
                task = db.get(Task, run.task_id)
                if (task is None or task.workspace_id != self.workspace_id
                        or task.outcome_id != outcome.id):
                    raise MemoryScopeError("Memory scope not found")
                allowed = [task.repo_id]
            if self.repo_id is not None and self.repo_id not in allowed:
                raise MemoryScopeError("Memory scope not found")
        return run

    def _query(self, db: Session):
        query = db.query(MemoryEntry).filter(MemoryEntry.workspace_id == self.workspace_id)
        if self.repo_id is not None:
            query = query.filter(or_(MemoryEntry.repo_id.is_(None), MemoryEntry.repo_id == self.repo_id))
        if self.run_id is not None:
            query = query.filter(MemoryEntry.run_id == self.run_id)
        return query

    def list(self, *, limit: int = 100, offset: int = 0) -> list[MemoryEntry]:
        if not 1 <= limit <= 200 or not 0 <= offset <= 1_000_000:
            raise ValueError("Memory pagination out of range")
        with session_scope() as db:
            self._scope(db)
            return (self._query(db).order_by(MemoryEntry.created_at.desc(), MemoryEntry.id.desc())
                    .offset(offset).limit(limit).all())

    def read(self, entry_id: str) -> MemoryEntry | None:
        with session_scope() as db:
            self._scope(db)
            return self._query(db).filter(MemoryEntry.id == entry_id).first()

    def create(self, *, text: str, kind: str = "lesson", topic: str = "",
               confidence: float | None = None, source: str = "") -> MemoryEntry:
        for name, value, maximum, required in (
            ("text", text, 16_000, True), ("kind", kind, 40, True),
            ("topic", topic, 200, False), ("source", source, 2_000, False),
        ):
            if not isinstance(value, str) or len(value) > maximum or (required and not value.strip()):
                raise ValueError(f"Invalid memory {name}")
        if confidence is not None and (not math.isfinite(confidence) or not 0 <= confidence <= 1):
            raise ValueError("Memory confidence must be between 0 and 1")
        with session_scope() as db:
            run = self._scope(db)
            if run is None:
                raise MemoryScopeError("A source run is required")
            entry = MemoryEntry(workspace_id=self.workspace_id, repo_id=self.repo_id, run_id=run.id,
                                kind=kind, text=text, topic=topic, confidence=confidence, source=source)
            db.add(entry)
            db.flush()
            self._journal(db, run, entry, "saved", "factory")
            return entry

    def _lock(self, db: Session) -> None:
        # Same portable workspace row lock as outcome numbering. On SQLite an
        # UPDATE also acquires the writer lock, unlike SELECT FOR UPDATE.
        db.execute(update(Workspace).where(Workspace.id == self.workspace_id).values(
            outcome_seq=Workspace.outcome_seq))

    def _exact(self, db: Session, kinds: tuple[str, ...] | None = None):
        query = db.query(MemoryEntry).filter(
            MemoryEntry.workspace_id == self.workspace_id, MemoryEntry.repo_id == self.repo_id)
        return query.filter(MemoryEntry.kind.in_(kinds)) if kinds is not None else query

    def context_entries(self, *, kinds: tuple[str, ...], limit: int = 200,
                        topic: str | None = None, scratchpad: bool | None = None,
                        exclude_scratchpad_when: bool = False) -> list[MemoryEntry]:
        """Fresh, category-bounded SDK reads, collapsing historical duplicate Engrams."""
        if not 1 <= limit <= 200:
            raise ValueError("Memory pagination out of range")
        with session_scope() as db:
            self._scope(db)
            # Deduplicate before the cap, using one bounded query rather than
            # loading legacy duplicates into Python or issuing one query per row.
            query = self._exact(db, kinds)
            if topic is not None:
                query = query.filter(MemoryEntry.topic == topic)
            is_scratchpad = or_(func.lower(MemoryEntry.text).contains("scratchpad"),
                                func.lower(MemoryEntry.topic).contains("scratchpad"))
            if scratchpad is not None:
                query = query.filter(is_scratchpad if scratchpad else ~is_scratchpad)
            if exclude_scratchpad_when:
                query = query.filter(~and_(MemoryEntry.kind == "when", is_scratchpad))
            ranked = query.with_entities(
                MemoryEntry.id.label("entry_id"),
                func.row_number().over(partition_by=MemoryEntry.text, order_by=(
                    MemoryEntry.updated_at.desc(), MemoryEntry.id.desc())).label("rank")
            ).subquery()
            return (db.query(MemoryEntry).join(ranked, MemoryEntry.id == ranked.c.entry_id)
                    .filter(ranked.c.rank == 1)
                    .order_by(MemoryEntry.updated_at.desc(), MemoryEntry.id.desc())
                    .limit(limit).all())

    def encode(self, *, text: str, kind: str, topic: str = "", source: str = "llm",
               confidence: float | None = None) -> MemoryEntry:
        """Idempotent encoding in an exact scope, serialized across processes."""
        if (not isinstance(text, str) or not text.strip() or len(text) > 16_000
                or kind not in ("always", "never", "when", "lesson", "profile")
                or not isinstance(topic, str) or len(topic) > 200
                or not isinstance(source, str) or len(source) > 2_000):
            raise ValueError("Invalid memory entry")
        if confidence is not None and (not math.isfinite(confidence) or not 0 <= confidence <= 1):
            raise ValueError("Memory confidence must be between 0 and 1")
        with session_scope() as db:
            self._lock(db)  # lock BEFORE any read/dedupe lookup
            run = self._scope(db)
            if run is None:
                raise MemoryScopeError("A source run is required")
            entries = self._exact(db).filter(MemoryEntry.text == text).order_by(
                MemoryEntry.updated_at.desc(), MemoryEntry.id.desc()).all()
            entry = entries[0] if entries else MemoryEntry(
                workspace_id=self.workspace_id, repo_id=self.repo_id, text=text)
            for duplicate in entries[1:]:
                self._delete(db, duplicate, "anton")
            entry.run_id, entry.kind, entry.topic = run.id, kind, topic
            entry.source, entry.confidence, entry.updated_at = source, confidence, utcnow()
            db.add(entry)
            db.flush()
            self._journal(db, run, entry, "saved", "anton")
            return entry

    def mutate_engram(self, engram_id: str | None = None, *, kinds: tuple[str, ...] | None = None,
                      text: str | None = None) -> None:
        """Delete every represented row (or update once), with no read cap."""
        if text is not None and (not isinstance(text, str) or not text.strip() or len(text) > 16_000):
            raise ValueError("Invalid memory text")
        with session_scope() as db:
            self._lock(db)
            run = self._scope(db)
            if run is None:
                raise MemoryScopeError("A source run is required")
            query = self._exact(db, kinds).order_by(MemoryEntry.updated_at.desc(), MemoryEntry.id.desc())
            matched = [e for e in query if engram_id is None or
                       hashlib.sha256(e.text.encode()).hexdigest() == engram_id]
            if not matched:
                return
            if text is not None:
                # Also merge a pre-existing target-text row: one Engram, one row.
                target_ids = {e.id for e in matched}
                matched.extend(e for e in self._exact(db).filter(MemoryEntry.text == text)
                               if e.id not in target_ids)
                kept, matched = matched[0], matched[1:]
                kept.text, kept.run_id, kept.updated_at = text, run.id, utcnow()
                self._journal(db, run, kept, "updated", "anton")
            for entry in matched:
                self._delete(db, entry, "anton")

    def delete(self, entry_id: str, *, actor: str = "factory") -> bool:
        with session_scope() as db:
            self._scope(db)
            entry = self._query(db).filter(MemoryEntry.id == entry_id).first()
            if entry is None:
                return False
            self._delete(db, entry, actor)
            return True

    def delete_repository_entries(self, *, session: Session, actor: str = "factory") -> None:
        """Forget a disconnected repository's lessons in the caller's transaction.

        Unlike a repository list, cleanup excludes workspace-wide entries. Require
        an explicit repository so this cannot accidentally erase workspace memory.
        """
        self._scope(session)
        if self.repo_id is None:
            raise MemoryScopeError("A repository scope is required")
        entries = self._query(session).filter(MemoryEntry.repo_id == self.repo_id)
        for entry in entries.yield_per(100):
            self._delete(session, entry, actor)
        # Remove referencing rows before the caller deletes the repository, including
        # on existing databases whose foreign key has no ON DELETE action.
        session.flush()

    def _delete(self, db: Session, entry: MemoryEntry, actor: str) -> None:
        run = db.get(Run, entry.run_id)
        self._journal(db, run, entry, "deleted", actor)
        db.delete(entry)

    def _journal(self, db: Session, run: Run, entry: MemoryEntry, action: str, actor: str) -> None:
        # Metadata only: deletion must not make another copy of a forgotten lesson.
        record(self.workspace_id, f"memory_{action}", f"Memory entry {action}",
               run_id=entry.run_id, outcome_id=run.outcome_id, task_id=run.task_id, actor=actor,
               data={"entry_id": entry.id, "repo_id": entry.repo_id,
                     "scope": "workspace" if entry.repo_id is None else "repository"}, session=db)
