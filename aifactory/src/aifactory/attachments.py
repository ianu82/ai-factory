"""Validation and transaction-local storage helpers for staged outcome attachments."""
from __future__ import annotations

import base64
import binascii
import csv
import io
import json
import re
import unicodedata
from datetime import timedelta
from pathlib import PurePosixPath

from sqlalchemy.orm import Session

from .db import utcnow
from .models import Artifact, Membership
from .pipeline.image_validation import validate_image

MAX_FILE_BYTES = 10 * 1024 * 1024
MAX_TOTAL_BYTES = 25 * 1024 * 1024
MAX_FILES = 10
MAX_ENCODED_LENGTH = 4 * ((MAX_FILE_BYTES + 2) // 3)
STAGED_TTL = timedelta(hours=24)
# The default CSV field limit is only 128 KiB; permit any otherwise valid upload.
csv.field_size_limit(MAX_FILE_BYTES)
IMAGE_EXTENSIONS = {".png": "image/png", ".jpg": "image/jpeg", ".jpeg": "image/jpeg",
                    ".gif": "image/gif", ".webp": "image/webp"}
TEXT_EXTENSIONS = {".txt": "text/plain", ".md": "text/markdown", ".csv": "text/csv", ".json": "application/json"}
# Match browser-recognised tag boundaries, including slash-separated attributes,
# anywhere in the document (also inside JSON strings). Ordinary code examples
# containing e.g. <script> alone are not HTML/SVG documents.
MARKUP = re.compile(r"<(?:html|head|body|svg|!doctype\s+html|\?xml)(?=[\s/>]|$)", re.I)


def _invalid_json_constant(value: str):
    raise ValueError(f"Invalid JSON constant: {value}")


def sanitise_name(name: str) -> str:
    name = name.replace("\\", "/").rsplit("/", 1)[-1]
    name = re.sub(r"[^a-zA-Z0-9._-]", "_", name).strip(". ")
    suffix = PurePosixPath(name).suffix
    if len(name) > 180:
        name = name[:180 - min(len(suffix), 20)] + suffix[:20]
    return name or "attachment"


def decode_content(content: str) -> bytes:
    if len(content) > MAX_ENCODED_LENGTH:
        raise ValueError("Attachment exceeds 10 MiB")
    try:
        raw = base64.b64decode(content, validate=True)
    except (ValueError, binascii.Error) as exc:
        raise ValueError("Attachment content must be strict base64, without a data-URL prefix") from exc
    if not raw or len(raw) > MAX_FILE_BYTES:
        raise ValueError("Attachment must contain between 1 byte and 10 MiB")
    return raw


def validate_upload(name: str, content: str) -> tuple[str, dict]:
    name = sanitise_name(name)
    extension = PurePosixPath(name).suffix.lower()
    if extension not in IMAGE_EXTENSIONS | TEXT_EXTENSIONS | {".pdf": "application/pdf"}:
        raise ValueError("Unsupported attachment extension; HTML and SVG are not allowed")
    raw = decode_content(content)
    mime = None
    if raw.startswith(b"\x89PNG\r\n\x1a\n"):
        mime = "image/png"
    elif raw.startswith(b"\xff\xd8\xff"):
        mime = "image/jpeg"
    elif raw.startswith((b"GIF87a", b"GIF89a")):
        mime = "image/gif"
    elif raw.startswith(b"RIFF") and raw[8:12] == b"WEBP":
        mime = "image/webp"
    if mime is not None:
        if IMAGE_EXTENSIONS.get(extension) != mime:
            raise ValueError("Attachment image type does not match its extension")
        validate_image(raw, mime)
    elif extension in IMAGE_EXTENSIONS:
        raise ValueError("Invalid attachment image content")
    elif raw.startswith(b"%PDF-"):
        if extension != ".pdf" or not re.match(rb"%PDF-\d\.\d[\r\n]", raw) or not raw.rstrip().endswith(b"%%EOF"):
            raise ValueError("Invalid PDF attachment")
        if not re.search(rb"\b\d+\s+\d+\s+obj\b", raw) or b"endobj" not in raw or b"startxref" not in raw:
            raise ValueError("Invalid PDF attachment structure")
        mime = "application/pdf"
    else:
        if extension not in TEXT_EXTENSIONS:
            raise ValueError("Invalid PDF attachment")
        try:
            text = raw.decode("utf-8-sig")
        except UnicodeDecodeError as exc:
            raise ValueError("Text attachments must be UTF-8") from exc
        if any(unicodedata.category(c) == "Cc" and c not in "\t\r\n" for c in text):
            raise ValueError("Binary or control data is not allowed in text attachments")
        if MARKUP.search(text):
            raise ValueError("HTML and SVG documents are not allowed")
        mime = TEXT_EXTENSIONS[extension]
        if extension == ".json":
            try:
                parsed = json.loads(text, parse_constant=_invalid_json_constant)
            except (ValueError, RecursionError) as exc:
                raise ValueError("Invalid JSON attachment") from exc
            # Escaped tag names in JSON are still markup after parsing.
            pending = [parsed]
            while pending:
                value = pending.pop()
                if isinstance(value, str) and MARKUP.search(value):
                    raise ValueError("HTML and SVG documents are not allowed")
                if isinstance(value, list):
                    pending.extend(value)
                elif isinstance(value, dict):
                    pending.extend(value.keys())
                    pending.extend(value.values())
        elif extension == ".csv":
            try:
                for _ in csv.reader(io.StringIO(text, newline=""), strict=True):
                    pass
            except csv.Error as exc:
                raise ValueError("Invalid CSV attachment") from exc
    return name, {"mime_type": mime, "size_bytes": len(raw)}


def cleanup_staged(db: Session) -> None:
    """Never delete linked references, even if they predate the staging TTL."""
    db.query(Artifact).filter(Artifact.kind == "attachment", Artifact.outcome_id.is_(None),
                              Artifact.created_at <= utcnow() - STAGED_TTL).delete(synchronize_session=False)


def staged_query(db: Session, workspace_id: str, uploader: str):
    return db.query(Artifact).filter(
        Artifact.kind == "attachment", Artifact.workspace_id == workspace_id,
        Artifact.outcome_id.is_(None), Artifact.created_at > utcnow() - STAGED_TTL,
        Artifact.data["uploaded_by"].as_string() == uploader)


def check_staged_quota(db: Session, workspace_id: str, uploader: str, size_bytes: int) -> None:
    """Serialize quota checks and inserts until the caller commits or rolls back.

    A no-op membership UPDATE takes a row lock on Postgres and a writer lock on
    SQLite, even when this uploader has no staged artifacts yet. Unlike a Python
    lock, it also protects uploads handled by other API processes.
    """
    db.query(Membership).filter_by(workspace_id=workspace_id, user_id=uploader).update(
        {Membership.id: Membership.id}, synchronize_session=False)
    cleanup_staged(db)
    rows = staged_query(db, workspace_id, uploader).all()
    if len(rows) >= MAX_FILES or size_bytes + sum(len(decode_content(row.content)) for row in rows) > MAX_TOTAL_BYTES:
        raise ValueError("Staged attachment quota exceeded: at most 10 files and 25 MiB per uploader per workspace; "
                         "delete staged attachments or create an outcome before uploading more")


def validate_staged(db: Session, workspace_id: str, uploader: str, ids: list[str]) -> list[Artifact]:
    if len(ids) > MAX_FILES or len(set(ids)) != len(ids):
        raise ValueError("At most 10 unique attachment IDs are allowed")
    rows = staged_query(db, workspace_id, uploader).filter(Artifact.id.in_(ids)).order_by(Artifact.created_at, Artifact.id).all()
    if len(rows) != len(ids):
        raise LookupError("Attachment not found or no longer available")
    # Measure actual bytes, not potentially stale/client-supplied metadata.
    if sum(len(decode_content(row.content)) for row in rows) > MAX_TOTAL_BYTES:
        raise ValueError("Attachments exceed 25 MiB in total")
    return rows
