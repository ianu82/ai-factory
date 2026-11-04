"""An authenticated task request; executable criteria are controller-owned."""

from __future__ import annotations

import json
import hashlib
from dataclasses import dataclass
from pathlib import Path

from jsonschema import validate, ValidationError

from .product_profile import COMMAND


TASK_SCHEMA = {
    "type": "object", "additionalProperties": False,
    "required": ["id", "title", "goal", "acceptance_criteria"],
    "properties": {
        "id": {"type": "string", "pattern": "^[a-zA-Z0-9][a-zA-Z0-9_-]{0,63}$"},
        "title": {"type": "string", "minLength": 1, "maxLength": 200},
        "goal": {"type": "string", "minLength": 1, "maxLength": 20000},
        "acceptance_criteria": {"type": "array", "minItems": 1, "maxItems": 20, "items": {
            "type": "object", "additionalProperties": False, "required": ["id", "description", "command"],
            "properties": {"id": {"type": "string", "pattern": "^[a-zA-Z0-9_-]{1,64}$"},
                           "description": {"type": "string", "minLength": 1}, "command": COMMAND},
        }},
    },
}


@dataclass(frozen=True)
class ProductTask:
    canonical_json: str

    @classmethod
    def from_document(cls, document: dict) -> "ProductTask":
        try:
            validate(document, TASK_SCHEMA)
        except ValidationError as exc:
            raise ValueError("Invalid product task") from exc
        ids = [item["id"] for item in document["acceptance_criteria"]]
        if len(ids) != len(set(ids)):
            raise ValueError("Acceptance criterion ids must be unique")
        return cls(json.dumps(document, sort_keys=True))

    @classmethod
    def load(cls, path: Path, *, target_root: Path) -> "ProductTask":
        if path.resolve().is_relative_to(target_root.resolve()):
            raise ValueError("Executable task criteria must live outside candidate source")
        return cls.from_document(json.loads(path.read_text(encoding="utf-8")))

    @property
    def document(self) -> dict:
        return json.loads(self.canonical_json)

    @property
    def digest(self) -> str:
        return hashlib.sha256(self.canonical_json.encode()).hexdigest()

    @property
    def public_context(self) -> dict:
        document = self.document
        for criterion in document["acceptance_criteria"]:
            del criterion["command"]
        return document
