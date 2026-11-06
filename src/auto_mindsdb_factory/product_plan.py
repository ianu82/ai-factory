"""Bounded planning guidance, bound to trusted inputs and frozen outside source."""

from __future__ import annotations

import hashlib
import json
import os
import re
from dataclasses import dataclass
from pathlib import PurePosixPath

from jsonschema import ValidationError, validate

from .product_roles import RoleDefinitionError, select_role
from .product_task import ProductTask
from .run_journal import RunJournal


class PlanError(ValueError):
    pass


TEXT = {"type": "string", "minLength": 1, "maxLength": 2000, "pattern": r"\S"}
TEXTS = {"type": "array", "maxItems": 30, "items": TEXT}
PLAN_SCHEMA = {
    "type": "object", "additionalProperties": False,
    "required": ["status", "reason", "goal", "constraints", "source_paths", "changes",
                 "risks", "questions", "validation"],
    "properties": {
        "status": {"enum": ["ready", "blocked", "unverified"]},
        **{key: TEXT for key in ("reason", "goal")},
        **{key: TEXTS for key in ("constraints", "source_paths", "changes", "risks", "questions")},
        "validation": {"type": "array", "minItems": 1, "maxItems": 20, "items": {
            "type": "object", "additionalProperties": False,
            "required": ["criterion_id", "evidence"],
            "properties": {"criterion_id": TEXT, "evidence": TEXT},
        }},
    },
}


@dataclass(frozen=True)
class ProductPlan:
    canonical_json: str

    @classmethod
    def create(cls, content: dict, *, task: ProductTask, base_sha: str,
               profile_sha: str, planner_role: dict) -> "ProductPlan":
        document = {"version": 1, "task_sha": task.digest, "base_sha": base_sha,
                    "profile_sha": profile_sha, "planner_role": planner_role, "content": content}
        return cls.from_document(document, task=task, base_sha=base_sha, profile_sha=profile_sha)

    @classmethod
    def from_document(cls, document: dict, *, task: ProductTask, base_sha: str,
                      profile_sha: str) -> "ProductPlan":
        try:
            encoded = json.dumps(document, sort_keys=True, separators=(",", ":"),
                                 ensure_ascii=False, allow_nan=False)
            if len(encoded.encode()) > 32768:
                raise PlanError("Plan exceeds the artifact size limit")
            if (type(document) is not dict
                    or set(document) != {"version", "task_sha", "base_sha", "profile_sha", "planner_role", "content"}
                    or type(document["version"]) is not int or document["version"] != 1
                    or not isinstance(base_sha, str) or not re.fullmatch("[0-9a-f]{40}", base_sha)
                    or not isinstance(profile_sha, str) or not re.fullmatch("[0-9a-f]{64}", profile_sha)
                    or (document["task_sha"], document["base_sha"], document["profile_sha"])
                    != (task.digest, base_sha, profile_sha)):
                raise PlanError("Plan does not match the trusted task, source and profile")
            role = document["planner_role"]
            if type(role) is not dict or role != select_role("planner", role.get("version")):
                raise PlanError("Plan has an untrusted planner role")
            content = document["content"]
            validate(content, PLAN_SCHEMA)
            expected = {item["id"] for item in task.document["acceptance_criteria"]}
            actual = [item["criterion_id"] for item in content["validation"]]
            if set(actual) != expected or len(actual) != len(expected):
                raise PlanError("Plan must map every criterion exactly once")
            for path in content["source_paths"]:
                if (PurePosixPath(path).is_absolute() or ".." in path.split("/")
                        or "\\" in path or "\x00" in path):
                    raise PlanError("Plan source paths must remain relative")
            if content["status"] == "ready" and (content["questions"] or any(
                    not content[key] for key in ("constraints", "source_paths", "changes", "risks"))):
                raise PlanError("Ready plan lacks investigation or has unresolved questions")
        except (TypeError, KeyError, ValueError, ValidationError, RoleDefinitionError) as exc:
            if isinstance(exc, PlanError):
                raise
            raise PlanError("Malformed or untrusted product plan") from None
        return cls(encoded)

    @property
    def document(self) -> dict:
        try:
            return json.loads(self.canonical_json)
        except (TypeError, ValueError):
            raise PlanError("Unreadable product plan") from None

    @property
    def digest(self) -> str:
        return hashlib.sha256(self.canonical_json.encode()).hexdigest()


def read_plan(journal: RunJournal, *, task: ProductTask, base_sha: str,
              profile_sha: str) -> ProductPlan:
    path = journal.root / "plan.json"
    try:
        if path.is_symlink():
            raise PlanError("Plan artifact cannot be a symbolic link")
        if path.stat().st_size > 65536:
            raise PlanError("Plan artifact exceeds the size limit")
        record = json.loads(path.read_text(encoding="utf-8"))
        if type(record) is not dict or set(record) != {"digest", "plan"}:
            raise PlanError("Malformed frozen plan artifact")
        plan = ProductPlan.from_document(record["plan"], task=task, base_sha=base_sha, profile_sha=profile_sha)
        if record["digest"] != plan.digest:
            raise PlanError("Frozen plan digest does not match its content")
        return plan
    except (OSError, ValueError, KeyError) as exc:
        if isinstance(exc, PlanError):
            raise
        raise PlanError("Missing or unreadable frozen plan") from None


def freeze_plan(journal: RunJournal, plan: ProductPlan, *, task: ProductTask,
                base_sha: str, profile_sha: str) -> ProductPlan:
    # Revalidate even directly constructed dataclass instances before writing.
    plan = ProductPlan.from_document(plan.document, task=task, base_sha=base_sha, profile_sha=profile_sha)
    with journal._lock():
        path = journal.root / "plan.json"
        if path.exists() or path.is_symlink():
            retained = read_plan(journal, task=task, base_sha=base_sha, profile_sha=profile_sha)
            if retained != plan:
                raise PlanError("A different plan is already frozen for this run")
            return retained
        with path.open("x", encoding="utf-8") as stream:
            json.dump({"digest": plan.digest, "plan": plan.document}, stream, ensure_ascii=False, sort_keys=True)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
    return read_plan(journal, task=task, base_sha=base_sha, profile_sha=profile_sha)
