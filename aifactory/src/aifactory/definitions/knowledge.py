"""Workspace/session scoped, literal knowledge lookup; no SQL wildcard semantics."""
from __future__ import annotations

from .resolution import ResolvedDefinitions, qualified_id
from .store import Snapshot
from .validation import DefinitionError

SNIPPET_LIMIT = 320
RESULT_LIMIT = 50


def _pages(scope: Snapshot | ResolvedDefinitions):
    if isinstance(scope, ResolvedDefinitions):
        return scope.knowledge
    return {qualified_id(scope.workspace_id, "knowledge", page): page for page in scope.bundle.knowledge}


def read_knowledge(scope: Snapshot | ResolvedDefinitions, page_id: str):
    pages = _pages(scope)
    if page_id in pages:
        return pages[page_id]
    # Raw page IDs are useful to the editor, but resolve only inside this scope.
    for page in pages.values():
        if page.id == page_id:
            return page
    raise DefinitionError("Knowledge page not found in this workspace/session scope")


def search_knowledge(scope: Snapshot | ResolvedDefinitions, query: str, *, limit: int = 20) -> list[dict]:
    if not 1 <= limit <= RESULT_LIMIT or len(query) > 1000:
        raise DefinitionError("Search requires limit 1-50 and query at most 1000 characters")
    needle = query.casefold()
    matches = []
    for handle, page in sorted(_pages(scope).items(), key=lambda item: (item[1].name.casefold(), item[0])):
        if needle in page.name.casefold() or needle in page.body.casefold():
            # Prefix snippets are bounded in original characters even when case
            # folding changes string length (e.g. sharp-s).
            matches.append({"id": page.id, "handle": handle, "title": page.name,
                            "description": page.description, "snippet": page.body[:SNIPPET_LIMIT],
                            "source": page.source.model_dump(), "repository_id": page.repository_id})
            if len(matches) == limit:
                break
    return matches
