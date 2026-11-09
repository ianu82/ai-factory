"""Server-owned first-outcome constraints and durable admission identity."""
from __future__ import annotations

from sqlalchemy.orm import Session

from .models import Event

README_TEMPLATE = "readme-contributing"
README_TITLE = "Add or improve the CONTRIBUTING section in README"
README_BRIEF = (
    "In the selected repository only, add or improve a CONTRIBUTING section in its root README. "
    "Edit the existing root README in place, preserving its exact filename (case-insensitive README, "
    "with no extension or .md, .markdown, .mdown, .rst, .txt or .adoc). Create README.md only if no root README exists. "
    "Never add a second README. The only deliverable is that README. Preserve unrelated documentation and existing README content. "
    "Derive all instructions from repository facts (existing commands, configuration and conventions); "
    "do not invent commands or requirements. Do not change code, dependencies, lockfiles, workflows, "
    "or any other file. If facts are unavailable, say what is unknown instead of guessing."
)
README_ACCEPTANCE = [
    "Only the selected repository's root README is changed; no other repository or file is changed.",
    "The README has a useful CONTRIBUTING section with instructions supported by repository facts.",
    "Unrelated documentation and existing README content are preserved.",
    "No code, dependency, lockfile, workflow, or other non-README changes are made.",
]


def outcome_guidance(db: Session, workspace_id: str, outcome_id: str) -> dict | None:
    # Admission events are queried independently of the capped activity timeline.
    event = (db.query(Event).filter_by(workspace_id=workspace_id, outcome_id=outcome_id,
                                     kind="outcome_created", task_id=None)
             .order_by(Event.id).first())
    marker = (event.data or {}).get("guidance") if event else None
    return marker if isinstance(marker, dict) else None
