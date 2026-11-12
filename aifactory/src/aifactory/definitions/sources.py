"""Desired source intent is separate from the last validated active snapshot.

T5 owns sync diagnostic updates and activating validated repository snapshots.
Source intent changes append an attributed checkpoint so the same revision CAS
protects both bundle and configuration writes, including first-source selection.
"""
from __future__ import annotations

from typing import Literal
from urllib.parse import quote

from pydantic import Field
from sqlalchemy.orm import Session

from ..models import DefinitionRevision, DefinitionState, Repository
from .schema import Model
from .store import RevisionConflict, get_snapshot, lock_repositories, lock_workspace
from .validation import DefinitionError, safe_path, validate_bundle


class SourceConfig(Model):
    kind: Literal["managed", "repository"] = "managed"
    repository_id: str | None = None
    folder: str = Field(default=".factory", max_length=1000)


def configuration(db: Session, workspace_id: str) -> dict:
    state = db.get(DefinitionState, workspace_id, populate_existing=True)
    return {"desired_source": (state.desired_source if state else None) or {"kind": "managed"},
            "active_source": (state.active_source if state else None) or {"kind": "managed"},
            "sync_diagnostics": (state.sync_diagnostics if state else None) or {
                "status": "not_synced", "last_attempt_at": None, "last_success_at": None,
                "commit": None, "errors": []}}


def require_managed(db: Session, workspace_id: str) -> None:
    lock_workspace(db, workspace_id)
    if configuration(db, workspace_id)["desired_source"]["kind"] != "managed":
        raise RevisionConflict("Repository-backed definitions are read-only; select managed mode before editing")


def select_source(db: Session, workspace_id: str, source: SourceConfig, *,
                  expected_revision: int, author: str) -> None:
    lock_workspace(db, workspace_id)
    repos = lock_repositories(db, workspace_id)
    snapshot = get_snapshot(db, workspace_id)
    if snapshot.revision != expected_revision:
        raise RevisionConflict("Stale definition revision; reload before selecting a source")
    if source.kind == "repository":
        if source.repository_id not in {r.id for r in repos}:
            raise DefinitionError("Repository not found in this workspace", path="source.repository_id",
                                  field="repository_id")
        folder = source.folder.rstrip("/")
        try:
            safe_path(folder)
        except DefinitionError as exc:
            raise DefinitionError(str(exc), path="source.folder", field="folder") from exc
        desired = {"kind": "repository", "repository_id": source.repository_id, "folder": folder}
    else:
        if source.repository_id is not None:
            raise DefinitionError("Managed source cannot reference a repository",
                                  path="source.repository_id", field="repository_id")
        desired = {"kind": "managed"}
    validate_bundle(snapshot.bundle, {r.id for r in repos})
    if not author.strip():
        raise DefinitionError("Source author must be nonempty")
    state = db.get(DefinitionState, workspace_id, populate_existing=True)
    if state is None:
        state = DefinitionState(workspace_id=workspace_id, revision=0)
        db.add(state)
    state.desired_source = desired
    state.revision = snapshot.revision + 1
    # Preserve active data and provenance until a replacement validates. Do not
    # overwrite sync diagnostics: source selection is not a successful sync.
    db.add(DefinitionRevision(workspace_id=workspace_id, revision=state.revision,
                              bundle=snapshot.bundle.model_dump(), author=author,
                              source=state.active_source or {"kind": "managed"}, desired_source=desired))
    db.flush()


def source_links(db: Session, workspace_id: str, bundle) -> list[dict]:
    repos = {r.id: r for r in db.query(Repository).filter_by(workspace_id=workspace_id)}
    links = []
    for purpose, config in configuration(db, workspace_id).items():
        if purpose == "sync_diagnostics" or config.get("kind") != "repository":
            continue
        repo = repos.get(config.get("repository_id"))
        if repo:
            links.append({"source": purpose, "url": f"https://github.com/{quote(repo.full_name, safe='/')}/tree/"
                          f"{quote(config.get('commit') or repo.default_branch, safe='')}/"
                          f"{quote(config.get('folder', '.factory'), safe='/')}"})
    for entry in bundle.entries():
        source = entry.source
        repo = repos.get(source.repository_id)
        if repo and source.kind == "repository" and source.path:
            links.append({"id": entry.id, "url": f"https://github.com/{quote(repo.full_name, safe='/')}/blob/"
                          f"{quote(source.commit or repo.default_branch, safe='')}/{quote(source.path, safe='/')}"})
    return links
