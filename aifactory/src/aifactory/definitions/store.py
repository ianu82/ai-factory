"""Transactional workspace snapshots. Callers own commit/rollback.

All writers lock workspace first, then repository rows. SQLite's no-op UPDATE
acquires its single-writer lock; PostgreSQL uses SELECT FOR UPDATE. This also
serializes first saves (when no DefinitionState row exists yet).
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime

from sqlalchemy import select, update
from sqlalchemy.orm import Session

from ..onboarding_policy import check_merge_reservation
from ..models import DefinitionRevision, DefinitionState, Repository, Workspace
from .schema import Bundle, KnowledgePage, Source
from .validation import DefinitionError, validate_bundle


class RevisionConflict(DefinitionError):
    pass


@dataclass(frozen=True)
class Snapshot:
    workspace_id: str
    revision: int
    bundle: Bundle
    author: str = ""
    created_at: datetime | None = None
    warnings: list[str] = field(default_factory=list)


def lock_workspace(db: Session, workspace_id: str) -> None:
    with db.no_autoflush:
        if db.get_bind().dialect.name == "sqlite":
            found = db.execute(update(Workspace).where(Workspace.id == workspace_id)
                               .values(outcome_seq=Workspace.outcome_seq)).rowcount
        else:
            found = db.scalar(select(Workspace).where(Workspace.id == workspace_id).with_for_update())
    if not found:
        raise DefinitionError("Workspace not found")
    ws = db.get(Workspace, workspace_id, populate_existing=True)
    check_merge_reservation(ws)


def lock_repositories(db: Session, workspace_id: str) -> list[Repository]:
    # Workspace first, even for direct callers: row locks alone cannot fence a
    # concurrent authorization or serialize repository insertion on SQLite.
    lock_workspace(db, workspace_id)
    # Lock all owned rows in stable order, including source-only references.
    return list(db.scalars(select(Repository).where(Repository.workspace_id == workspace_id)
                           .order_by(Repository.id).with_for_update().execution_options(populate_existing=True)))


def _row(db: Session, workspace_id: str, revision: int) -> DefinitionRevision | None:
    return db.scalar(select(DefinitionRevision).where(DefinitionRevision.workspace_id == workspace_id,
                                                     DefinitionRevision.revision == revision))


def _head(db: Session, workspace_id: str) -> int:
    return db.scalar(select(DefinitionState.revision).where(DefinitionState.workspace_id == workspace_id)) or 0


def _context_pages(bundle: Bundle, repositories: list[Repository]) -> Bundle:
    """Rebuild canonical repository scopes; never trust a submitted projection's scope."""
    result = bundle.model_copy(deep=True)
    result.knowledge = [p for p in result.knowledge if p.source.kind != "repository_context"]
    for repo in repositories:
        context = (repo.config or {}).get("context") or ""
        if context:
            result.knowledge.append(KnowledgePage(
                id=f"repository-context-{repo.id}", name="team-notes",
                description=f"Team notes for {repo.full_name}", body=context, repository_id=repo.id,
                source=Source(kind="repository_context", repository_id=repo.id, path="config/context"),
            ))
    return result


def get_snapshot(db: Session, workspace_id: str, revision: int | None = None) -> Snapshot:
    if db.get(Workspace, workspace_id) is None:
        raise DefinitionError("Workspace not found")
    number = _head(db, workspace_id) if revision is None else revision
    row = _row(db, workspace_id, number) if number else None
    if number and row is None:
        raise DefinitionError(f"Definition revision {number} not found in this workspace")
    bundle = Bundle.model_validate(row.bundle) if row else Bundle()
    warnings = []
    if revision is None:
        repos = list(db.scalars(select(Repository).where(Repository.workspace_id == workspace_id)))
        owned = {r.id for r in repos}
        # Legacy/disconnected references must not take unrelated definitions offline.
        for collection in (bundle.skills, bundle.instructions, bundle.knowledge):
            for entry in collection[:]:
                if any(r is not None and r not in owned for r in
                       (entry.repository_id, entry.source.repository_id)):
                    collection.remove(entry)
                    warnings.append(f"Skipped {entry.id}: repository is no longer connected")
        bundle = _context_pages(bundle, repos)
    return Snapshot(workspace_id, number, bundle, row.author if row else "",
                    row.created_at if row else None, warnings)


def save_bundle(db: Session, workspace_id: str, bundle: Bundle, *, expected_revision: int,
                author: str, restored_from: int | None = None,
                active_source: dict | None = None) -> Snapshot:
    """Validate and stage a new revision atomically. No commit; caller owns transaction.

    Validation and conflicts do not mutate the head. History does not consume quota.
    """
    lock_workspace(db, workspace_id)
    repos = lock_repositories(db, workspace_id)
    current = _head(db, workspace_id)
    if current != expected_revision:
        raise RevisionConflict(f"Stale definition revision {expected_revision}; active revision is {current}")
    if not author.strip():
        raise DefinitionError("Revision author must be nonempty")
    bundle = Bundle.model_validate(bundle.model_dump())
    owned = {r.id for r in repos}
    # Validate even projected inputs before replacing them, to reject forged scopes.
    validate_bundle(bundle, owned)
    bundle = _context_pages(bundle, repos)
    validate_bundle(bundle, owned)
    number = current + 1
    state = db.get(DefinitionState, workspace_id, populate_existing=True)
    if state is None:
        state = DefinitionState(workspace_id=workspace_id, revision=number)
        db.add(state)
    else:
        state.revision = number
    if active_source is not None:
        state.active_source = active_source
    row = DefinitionRevision(workspace_id=workspace_id, revision=number, bundle=bundle.model_dump(),
                             author=author, restored_from=restored_from,
                             source=(state.active_source if state else None),
                             desired_source=state.desired_source)
    db.add(row)
    db.flush()
    return Snapshot(workspace_id, number, bundle.model_copy(deep=True), author, row.created_at)


def restore_revision(db: Session, workspace_id: str, revision: int, *, expected_revision: int,
                     author: str, active_source: dict | None = None) -> Snapshot:
    lock_workspace(db, workspace_id)
    repos = lock_repositories(db, workspace_id)
    if _head(db, workspace_id) != expected_revision:
        raise RevisionConflict("Stale definition revision; reload before restoring")
    old = get_snapshot(db, workspace_id, revision)
    owned = {r.id for r in repos}
    # Context pages are generated projections, not explicit definitions. Keep
    # history intact, but do not restore projections for disconnected repositories.
    old.bundle.knowledge = [p for p in old.bundle.knowledge
                            if p.source.kind != "repository_context" or p.repository_id in owned]
    validate_bundle(old.bundle, owned)
    # Restoring includes the old notes, not just their live projection. A savepoint
    # keeps repository config untouched even if later validation rejects the restore.
    with db.begin_nested():
        notes = {p.repository_id: p.body for p in old.bundle.knowledge
                 if p.source.kind == "repository_context"}
        for repo in repos:
            repo.config = {**(repo.config or {}), "context": notes.get(repo.id, "")}
        db.flush()
        return save_bundle(db, workspace_id, old.bundle, expected_revision=expected_revision,
                           author=author, restored_from=revision, active_source=active_source)


def history(db: Session, workspace_id: str) -> list[Snapshot]:
    return [get_snapshot(db, workspace_id, n) for n in db.scalars(
        select(DefinitionRevision.revision).where(DefinitionRevision.workspace_id == workspace_id)
        .order_by(DefinitionRevision.revision))]


def validate_repository_change(db: Session, workspace_id: str, *, author: str | None = None) -> None:
    """Caller holds the workspace/repository locks before changing repository config."""
    db.flush()
    snapshot = get_snapshot(db, workspace_id)
    owned = set(db.scalars(select(Repository.id).where(Repository.workspace_id == workspace_id)))
    validate_bundle(snapshot.bundle, owned)
    if author is not None:
        # Compare with immutable content, not the live context projection. Config
        # remains the sole editable source; history stores a read-only copy.
        previous = get_snapshot(db, workspace_id, snapshot.revision).bundle
        before = {p.id: p.body for p in previous.knowledge if p.source.kind == "repository_context"}
        after = {p.id: p.body for p in snapshot.bundle.knowledge if p.source.kind == "repository_context"}
        if before != after:
            save_bundle(db, workspace_id, snapshot.bundle,
                        expected_revision=snapshot.revision, author=author)


def guard_disconnect(db: Session, workspace_id: str, repository_id: str) -> None:
    """Block disconnects with explicit references; live team-note projections may go."""
    lock_workspace(db, workspace_id)
    lock_repositories(db, workspace_id)
    snapshot = get_snapshot(db, workspace_id)
    state = db.get(DefinitionState, workspace_id, populate_existing=True)
    if state and any((source or {}).get("repository_id") == repository_id
                     for source in (state.desired_source, state.active_source)):
        raise DefinitionError("Change the definitions source before disconnecting this repository")
    refs = [e.name for e in snapshot.bundle.entries() if e.source.kind != "repository_context"
            and repository_id in (e.repository_id, e.source.repository_id)]
    if refs:
        raise DefinitionError("Remove repository definitions before disconnecting: " + ", ".join(refs))
