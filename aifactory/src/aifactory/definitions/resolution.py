"""Union scope resolution with workspace-qualified handles; no name shadowing."""
from __future__ import annotations

from dataclasses import dataclass

from sqlalchemy import select
from sqlalchemy.orm import Session

from ..models import Repository
from .schema import Definition
from .store import Snapshot, get_snapshot
from .validation import DefinitionError


@dataclass(frozen=True)
class ResolvedDefinitions:
    snapshot: Snapshot
    skills: dict[str, Definition]
    instructions: dict[str, Definition]
    knowledge: dict[str, Definition]

    def lookup(self, handle: str) -> Definition:
        entries = {**self.skills, **self.instructions, **self.knowledge}
        if handle in entries:
            return entries[handle]
        matches = [entry for entry in entries.values() if entry.name == handle]
        if len(matches) == 1:
            return matches[0]
        if matches:
            raise DefinitionError(f"Ambiguous definition name {handle!r}; use a workspace-qualified ID")
        raise DefinitionError("Definition not found in this workspace/session scope")


def qualified_id(workspace_id: str, kind: str, entry: Definition) -> str:
    return f"{workspace_id}/{kind}/{entry.id}"


def resolve_definitions(db: Session, workspace_id: str, role: str, *,
                        repository_ids: list[str] | None = None,
                        task_repository_id: str | None = None) -> ResolvedDefinitions:
    if role not in {"planner", "builder", "verifier"}:
        raise DefinitionError("Role must be planner, builder or verifier")
    if role == "planner":
        selected = set(repository_ids or [])
    else:
        if repository_ids:
            raise DefinitionError("Builder/verifier scope requires only task_repository_id")
        # Without a task repository only workspace-scoped definitions apply.
        selected = {task_repository_id} if task_repository_id else set()
    owned = set(db.scalars(select(Repository.id).where(Repository.workspace_id == workspace_id)))
    if not selected <= owned:
        raise DefinitionError("Selected repository does not belong to this workspace")
    snapshot = get_snapshot(db, workspace_id)

    def applicable(kind, entries):
        return {qualified_id(workspace_id, kind, e): e for e in entries
                if (e.role is None or e.role == role)
                and (e.repository_id is None or e.repository_id in selected)}

    return ResolvedDefinitions(snapshot, applicable("skill", snapshot.bundle.skills),
                               applicable("instruction", snapshot.bundle.instructions),
                               applicable("knowledge", snapshot.bundle.knowledge))
