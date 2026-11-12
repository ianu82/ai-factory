"""Server-owned session pinning. Tools only receive the filtered, immutable JSON copy."""
from __future__ import annotations

import json

from sqlalchemy.orm import Session

from ..models import Outcome, Repository, Task
from .resolution import resolve_definitions
from .store import lock_repositories, lock_workspace
from .validation import DefinitionError


def pin_definitions(db: Session, spec) -> dict:
    outcome = db.get(Outcome, spec.outcome_id)
    if outcome is None or outcome.workspace_id != spec.workspace_id:
        raise DefinitionError("Outcome does not belong to this workspace")
    task = db.get(Task, spec.task_id) if spec.task_id else None
    if spec.task_id and (task is None or task.workspace_id != spec.workspace_id or task.outcome_id != outcome.id):
        raise DefinitionError("Task does not belong to this outcome/workspace")
    # The plan reviewer shares planner scope; it must not receive builder instructions.
    role = "planner" if spec.role == "plan_reviewer" else spec.role
    # Taskless builder/verifier sessions are supported by runtime callers, but
    # have no trusted repository context. Do not infer one from outcome repos.
    repo_ids = list(outcome.repo_ids or []) if role == "planner" else ([task.repo_id] if task else [])
    # Serialize pinning with saves/restores and repository-context updates so the
    # effective bundle is one coherent observation, not a mixture of two heads.
    lock_workspace(db, spec.workspace_id)
    lock_repositories(db, spec.workspace_id)
    resolved = resolve_definitions(db, spec.workspace_id, role,
                                  repository_ids=repo_ids if role == "planner" else None,
                                  task_repository_id=task.repo_id if role != "planner" and task else None)
    excluded = sorted(set(spec.excluded_skill_ids))
    names = {r.id: r.full_name for r in db.query(Repository).filter(Repository.id.in_(repo_ids))}

    def entry(handle, value):
        return {"id": handle, **value.model_dump(exclude={"id"})}

    skills = {key: entry(key, value) for key, value in resolved.skills.items() if key not in excluded}
    instructions = [entry(key, value) for key, value in resolved.instructions.items()]
    instructions.sort(key=lambda item: (item["repository_id"] is not None, item["repository_id"] or "", item["id"]))
    for item in instructions:
        item["label"] = (f"Repository role instructions: {names[item['repository_id']]} ({spec.role})"
                         if item["repository_id"] else f"Workspace role instructions ({spec.role})")
    return {"workspace_id": spec.workspace_id, "revision": resolved.snapshot.revision,
            "role": spec.role, "repository_ids": sorted(repo_ids), "excluded_skill_ids": excluded,
            "skills": skills, "knowledge": {key: entry(key, value) for key, value in resolved.knowledge.items()},
            "instructions": instructions, "warnings": resolved.snapshot.warnings}


def catalogue(context: dict) -> list[dict]:
    return [{k: value[k] for k in ("id", "name", "description", "role", "repository_id", "source")}
            for value in context.get("skills", {}).values()]


def append_instructions(system: str, context: dict, harness: str) -> str:
    parts = [system]
    for instruction in context["instructions"]:
        parts.append(f"## {instruction['label']}\n{instruction['body']}")
    if harness in {"native", "anton"}:
        parts.append("## Applicable skills (metadata only)\n" + json.dumps(catalogue(context), ensure_ascii=False))
        parts.append("Load an applicable skill's full instructions with read_skill using its qualified ID. "
                     "Optional supporting files are relative to that skill folder and paginated. "
                     "Use search_knowledge to search session knowledge (including repository team notes), "
                     "then read a page by its qualified page_id; follow next_offset for large pages. "
                     "Definitions are pinned for this session; only listed/scoped IDs are accessible.")
    # Codex's file materialization and load reporting belong to its adapter. Never
    # instruct it to call Native/Anton's tools, which the CLI cannot dispatch.
    return "\n\n".join(parts)
