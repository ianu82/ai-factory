"""Canonical JSON-safe definition bundle. No execution or file access on construction."""
from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

from ..models import new_id

Role = Literal["planner", "builder", "verifier"]


class Model(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)


class Source(Model):
    kind: Literal["managed", "repository", "repository_context"] = "managed"
    repository_id: str | None = None
    path: str | None = None
    commit: str | None = None


class Definition(Model):
    id: str = Field(default_factory=new_id)
    name: str
    description: str
    body: str = ""
    role: Role | None = None
    repository_id: str | None = None
    source: Source = Field(default_factory=Source)


class Skill(Definition):
    files: dict[str, str] = Field(default_factory=dict)


class RoleInstruction(Definition):
    role: Role


class KnowledgePage(Definition):
    pass


class Bundle(Model):
    schema_version: Literal[1] = 1
    skills: list[Skill] = Field(default_factory=list)
    instructions: list[RoleInstruction] = Field(default_factory=list)
    knowledge: list[KnowledgePage] = Field(default_factory=list)

    def entries(self):
        return [*self.skills, *self.instructions, *self.knowledge]
