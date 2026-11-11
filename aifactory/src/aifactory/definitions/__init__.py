"""Validated definition snapshots, immutable history and session scope resolution.

Stores stage changes in the supplied SQLAlchemy session; commit/rollback belongs
to the caller. Reads of the active bundle project live Repository.config.context;
explicit historical revisions always retain the original notes.
"""
from .knowledge import read_knowledge, search_knowledge
from .resolution import ResolvedDefinitions, qualified_id, resolve_definitions
from .schema import Bundle, KnowledgePage, RoleInstruction, Skill, Source
from .store import (
    RevisionConflict, Snapshot, get_snapshot, guard_disconnect, history,
    lock_repositories, lock_workspace, restore_revision, save_bundle, validate_repository_change,
)
from .validation import (
    BUNDLE_LIMIT, SKILL_BODY_LIMIT, DefinitionError, bundle_size, parse_frontmatter,
    read_skill_folder, render_skill, validate_bundle,
)

__all__ = [
    "read_knowledge", "search_knowledge",
    "Bundle", "Skill", "RoleInstruction", "KnowledgePage", "Source", "Snapshot",
    "DefinitionError", "RevisionConflict", "ResolvedDefinitions", "BUNDLE_LIMIT", "SKILL_BODY_LIMIT",
    "get_snapshot", "save_bundle", "restore_revision", "history", "resolve_definitions", "qualified_id",
    "parse_frontmatter", "render_skill", "read_skill_folder", "validate_bundle", "bundle_size",
    "guard_disconnect", "lock_workspace", "lock_repositories", "validate_repository_change",
]
