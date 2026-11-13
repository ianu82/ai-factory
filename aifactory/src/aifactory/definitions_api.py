"""Authorized workspace definitions editor contract (sync is handled separately)."""
from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.exceptions import RequestValidationError
from fastapi.routing import APIRoute
from pydantic import Field
from sqlalchemy.orm import Session

from .auth import current_user, require_member
from .db import get_db
from .definitions import Bundle, DefinitionError, RevisionConflict, get_snapshot, restore_revision, save_bundle, validate_bundle
from .definitions.knowledge import read_knowledge, search_knowledge
from .definitions.schema import Model
from .definitions.sources import SourceConfig, configuration, require_managed, select_source, source_links
from .models import DefinitionRevision, Repository, User


class BoundedRoute(APIRoute):
    def get_route_handler(self):
        handler = super().get_route_handler()

        async def bounded(request: Request):
            # A 1 MiB UTF-8 bundle can expand with JSON escaping. Bound wire
            # input before FastAPI decodes/deserializes it, including chunked input.
            if request.method in {"PUT", "POST"}:
                data = bytearray()
                async for chunk in request.stream():
                    if len(data) + len(chunk) > 8 * 1024 * 1024:
                        raise HTTPException(422, {"errors": [{"path": "body", "field": "body",
                                                            "message": "Request exceeds 8 MiB"}]})
                    data.extend(chunk)
                request._body = bytes(data)
            try:
                return await handler(request)
            except RequestValidationError as exc:
                errors = []
                for error in exc.errors():
                    loc = [str(part) for part in error["loc"] if part != "body"]
                    errors.append({"path": ".".join(loc), "field": loc[-1] if loc else "body",
                                   "message": error["msg"]})
                raise HTTPException(422, {"errors": errors}) from exc

        return bounded


router = APIRouter(prefix="/api/workspaces/{workspace_id}/definitions", route_class=BoundedRoute)


class Save(Model):
    bundle: Bundle
    expected_revision: int = Field(ge=0)


class Restore(Model):
    revision_id: str = Field(min_length=1, max_length=100)
    expected_revision: int = Field(ge=0)


class SelectSource(Model):
    source: SourceConfig
    expected_revision: int = Field(ge=0)


def _error(db, exc):
    db.rollback()
    raise HTTPException(409 if isinstance(exc, RevisionConflict) else 422,
                        {"errors": [exc.detail()]}) from exc


def _version(db, workspace_id, revision_id):
    # Opaque IDs, never global numeric revision numbers, prevent foreign history
    # IDs from accidentally resolving to a different workspace's local revision.
    row = db.query(DefinitionRevision).filter_by(workspace_id=workspace_id, id=revision_id).first()
    if row is None:
        raise HTTPException(404, "Definition version not found")
    return row


def _metadata(row):
    return {"id": row.id, "revision": row.revision, "author": row.author,
            "created_at": row.created_at, "restored_from": row.restored_from,
            "active_source": row.source or {"kind": "managed"},
            "desired_source": row.desired_source or {"kind": "managed"}}


def _active(db, workspace_id):
    snapshot = get_snapshot(db, workspace_id)
    row = db.query(DefinitionRevision).filter_by(workspace_id=workspace_id, revision=snapshot.revision).first()
    return {"revision": snapshot.revision, "revision_id": row.id if row else None,
            "author": snapshot.author, "created_at": snapshot.created_at,
            "bundle": snapshot.bundle.model_dump(), "entries": [e.model_dump() for e in snapshot.bundle.entries()],
            "warnings": snapshot.warnings, "source_links": source_links(db, workspace_id, snapshot.bundle),
            **configuration(db, workspace_id)}


@router.get("")
def active(workspace_id: str, user: User = Depends(current_user), db: Session = Depends(get_db)):
    require_member(db, user, workspace_id, "viewer")
    return _active(db, workspace_id)


@router.put("")
def save(workspace_id: str, body: Save, user: User = Depends(current_user), db: Session = Depends(get_db)):
    require_member(db, user, workspace_id, "admin")
    try:
        require_managed(db, workspace_id)
        current = get_snapshot(db, workspace_id)
        if current.revision != body.expected_revision:
            raise RevisionConflict("Stale definition revision; reload before saving")
        validate_bundle(body.bundle, {r.id for r in db.query(Repository).filter_by(workspace_id=workspace_id)})
        projected = {p.id: p for p in current.bundle.knowledge if p.source.kind == "repository_context"}
        for entry in body.bundle.entries():
            if entry.source.kind != "managed" and projected.get(entry.id) != entry:
                raise DefinitionError("Edit repository team notes through repository configuration; managed entries require managed sources",
                                      path=f"entries.{entry.id}.source", field="source")
        save_bundle(db, workspace_id, body.bundle, expected_revision=body.expected_revision,
                    author=user.id, active_source={"kind": "managed"})
        db.commit()
    except DefinitionError as exc:
        _error(db, exc)
    return _active(db, workspace_id)


@router.get("/versions")
def versions(workspace_id: str, limit: int = Query(50, ge=1, le=100), offset: int = Query(0, ge=0),
             user: User = Depends(current_user), db: Session = Depends(get_db)):
    require_member(db, user, workspace_id, "viewer")
    rows = (db.query(DefinitionRevision).filter_by(workspace_id=workspace_id)
            .order_by(DefinitionRevision.revision.desc()).offset(offset).limit(limit).all())
    return {"versions": [_metadata(row) for row in rows], "limit": limit, "offset": offset}


@router.get("/versions/{revision_id}")
def version(workspace_id: str, revision_id: str, user: User = Depends(current_user), db: Session = Depends(get_db)):
    require_member(db, user, workspace_id, "viewer")
    row = _version(db, workspace_id, revision_id)
    return {**_metadata(row), "bundle": row.bundle}


@router.post("/restore")
def restore(workspace_id: str, body: Restore, user: User = Depends(current_user), db: Session = Depends(get_db)):
    require_member(db, user, workspace_id, "admin")
    row = _version(db, workspace_id, body.revision_id)
    try:
        require_managed(db, workspace_id)
        if ((row.source or {}).get("kind", "managed") != "managed"
                or any(entry.get("source", {}).get("kind") == "repository"
                       for kind in ("skills", "instructions", "knowledge") for entry in row.bundle.get(kind, []))):
            raise RevisionConflict("Repository-backed versions cannot be restored in the app")
        restore_revision(db, workspace_id, row.revision, expected_revision=body.expected_revision, author=user.id,
                         active_source={"kind": "managed"})
        db.commit()
    except DefinitionError as exc:
        _error(db, exc)
    return _active(db, workspace_id)


@router.put("/source")
def source(workspace_id: str, body: SelectSource, user: User = Depends(current_user), db: Session = Depends(get_db)):
    require_member(db, user, workspace_id, "admin")
    try:
        select_source(db, workspace_id, body.source, expected_revision=body.expected_revision, author=user.id)
        db.commit()
    except DefinitionError as exc:
        _error(db, exc)
    return _active(db, workspace_id)


@router.post("/sync")
async def sync(workspace_id: str, user: User = Depends(current_user), db: Session = Depends(get_db)):
    require_member(db, user, workspace_id, "admin")
    # The service owns short transactions; release the request's read transaction
    # before any network I/O and reload the response after publication.
    db.rollback()
    from .definitions.repository import sync_definitions

    try:
        await sync_definitions(workspace_id)
    except DefinitionError as exc:
        _error(db, exc)
    db.expire_all()
    return _active(db, workspace_id)


@router.get("/knowledge")
def search(workspace_id: str, q: str = Query("", max_length=1000), limit: int = Query(20, ge=1, le=50),
           user: User = Depends(current_user), db: Session = Depends(get_db)):
    require_member(db, user, workspace_id, "viewer")
    return {"pages": search_knowledge(get_snapshot(db, workspace_id), q, limit=limit)}


@router.get("/knowledge/{page_id}")
def page(workspace_id: str, page_id: str, user: User = Depends(current_user), db: Session = Depends(get_db)):
    require_member(db, user, workspace_id, "viewer")
    try:
        return read_knowledge(get_snapshot(db, workspace_id), page_id).model_dump()
    except DefinitionError as exc:
        raise HTTPException(404, "Knowledge page not found") from exc
