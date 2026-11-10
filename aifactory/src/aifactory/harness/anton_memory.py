"""Factory stores for the pinned Anton SDK (import only when Anton is available).

Global means workspace, never the controller user's ~/.anton. Trusted IDs come
from the host, not tool arguments. There are no snapshots to flush back at close:
admin deletion cannot be resurrected by a stale file/cache. T6 owns wiring these
objects into ChatSessionConfig and calling settle before closing the session.
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import timedelta

from anton.core.memory.base import Engram
from anton.core.memory.cortex import Cortex
from anton.core.memory.episodes import Episode, EpisodicMemory
from sqlalchemy import String, and_, cast, or_

from ..db import session_scope, utcnow
from ..memory import MemoryScopeError, MemoryService
from ..models import Artifact, Event, Outcome, Run, Task
from ..security import redact

_RULES = ("always", "never", "when")
_SENSITIVE = re.compile(
    r"(?i)\b(?:password|secret|api[_ -]?key|access[_ -]?token|token|authorization)\b"
    r"\s*[:=]\s*[^\n,;]+"
)


def _safe(text: str) -> str:
    if not isinstance(text, str):
        return ""
    return _SENSITIVE.sub("[redacted]", redact(text))[:2000]


def _budget(entries: list[Engram], tokens: int | None) -> list[Engram]:
    if tokens is None:
        return entries
    result, remaining = [], max(0, tokens) * 4
    for entry in entries:
        if len(entry.text) <= remaining:
            result.append(entry)
            remaining -= len(entry.text)
    return result


def _lines(entries: list[Engram]) -> str:
    return "\n".join(f"- {e.text}" for e in entries)


class FactoryHippocampus:
    """HippocampusProtocol over one or more exact scopes; writes need a selector."""

    def __init__(self, workspace_id: str, run_id: str, repo_ids: tuple[str, ...], *,
                 global_scope: bool = False, write_repo_id: str | None = None):
        self.workspace_id, self.run_id = workspace_id, run_id
        self.repo_ids, self.global_scope = repo_ids, global_scope
        self.write_repo_id = write_repo_id

    def _reader(self, repo_id):
        # Reads span runs, not just this run's contributions.
        return MemoryService(self.workspace_id, repo_id)

    def _writer(self):
        if not self.global_scope and self.write_repo_id is None:
            raise MemoryScopeError("Select an allowed repository for project writes")
        return MemoryService(self.workspace_id, None if self.global_scope else self.write_repo_id, self.run_id)

    def _engrams(self, kinds, **filters):
        entries = []
        for repo in (None,) if self.global_scope else self.repo_ids:
            entries.extend(self._reader(repo).context_entries(kinds=kinds, **filters))
        entries.sort(key=lambda e: (e.updated_at, e.id), reverse=True)
        seen, result = set(), []
        for entry in entries:
            engram = Engram(text=entry.text, kind=entry.kind,
                            scope="global" if self.global_scope else "project", topic=entry.topic,
                            confidence=("medium" if entry.confidence is None else
                                        "high" if entry.confidence >= .8 else
                                        "medium" if entry.confidence >= .5 else "low"),
                            source=entry.source, updated_at=entry.updated_at)
            if engram.id not in seen:
                seen.add(engram.id)
                result.append(engram)
        return result[:200]

    def get_identities(self):
        return self._engrams(("profile",))

    def get_rules(self, exclude_scratchpad_when=False):
        return self._engrams(_RULES, exclude_scratchpad_when=exclude_scratchpad_when)

    def get_lessons(self, token_budget=None, exclude_scratchpad=False):
        return _budget(self._engrams(("lesson",), scratchpad=False if exclude_scratchpad else None),
                       token_budget)

    def get_scratchpad_wisdom(self, token_budget=2000):
        return _budget(self._engrams(("when", "lesson"), scratchpad=True), token_budget)

    def recall_identities(self):
        return _lines(self.get_identities())

    def recall_rules(self):
        return _lines(self.get_rules())

    def recall_lessons(self, token_budget=1000, exclude_scratchpad=False):
        return _lines(self.get_lessons(token_budget, exclude_scratchpad))

    def recall_topic(self, slug):
        return _lines(self._engrams(("lesson",), topic=slug))

    def recall_scratchpad_wisdom(self, token_budget=2000):
        return _lines(self.get_scratchpad_wisdom(token_budget))

    def encode_rule(self, text, kind, confidence="medium", source="llm"):
        self._writer().encode(text=text, kind=kind, source=source or "llm",
                              confidence={"high": 1., "medium": .5, "low": .2}.get(confidence))

    def encode_lesson(self, text, topic="", source="llm"):
        self._writer().encode(text=text, kind="lesson", topic=topic or "", source=source or "llm")

    def rewrite_identity(self, entries):
        # Merge, never replace a cached snapshot (and never migrate project profiles).
        for text in entries:
            self._writer().encode(text=text, kind="profile")

    def clear_identity(self):
        self._writer().mutate_engram(kinds=("profile",))

    def del_rule(self, id):
        self._writer().mutate_engram(id, kinds=_RULES)

    def update_rule(self, id, text):
        self._writer().mutate_engram(id, kinds=_RULES, text=text)

    def del_lesson(self, id):
        self._writer().mutate_engram(id, kinds=("lesson",))

    def update_lesson(self, id, text):
        self._writer().mutate_engram(id, kinds=("lesson",), text=text)

    def del_identity(self, id):
        self._writer().mutate_engram(id, kinds=("profile",))

    def update_identity(self, id, text):
        self._writer().mutate_engram(id, kinds=("profile",), text=text)

    def clear(self):
        self._writer().mutate_engram()

    def entry_count(self):
        return len(self.get_rules()) + len(self.get_lessons()) + len(self.get_identities())

    def memory_presence(self):
        return "present" if self.entry_count() else "empty"


class FactoryCortex(Cortex):
    def _move_project_identity(self):
        # SDK's file-store migration would copy repo-scoped data into workspace
        # scope at construction, potentially restoring an administrator deletion.
        pass


class FactoryEpisodicMemory(EpisodicMemory):
    """Read-only view of durable Factory history, including non-Anton runs.

    Never ingest tool inputs, event.data, artifact.content/data, raw transcripts,
    or arbitrary run.result fields. SDK log calls are no-ops: the Factory's
    existing run events and MemoryService journal are already durable.
    """

    def __init__(self, workspace_id: str, run_id: str, repo_ids: tuple[str, ...]):
        self.workspace_id, self._session_id, self.repo_ids = workspace_id, run_id, repo_ids
        self._enabled = True

    def start_session(self):
        return self._session_id

    def resume_session(self, session_id):
        if session_id != self._session_id:
            raise MemoryScopeError("Cannot change the attributed run")
        return self._session_id

    def log(self, episode):
        pass

    def log_turn(self, turn, role, content, **meta):
        pass

    def clear(self):
        raise MemoryScopeError("Factory run history is managed by the Factory")

    def del_episode_entry(self, turn):
        return False

    def get_items(self):
        return []

    def session_count(self):
        return len({e.session for e in self.recall("", max_results=50)})

    def recall(self, query, *, max_results=20, days_back=None):
        if not self.enabled or not self.repo_ids:
            return []
        limit = min(50, max(0, int(max_results)))
        if not limit:
            return []
        with session_scope() as db:
            current = MemoryService(self.workspace_id, run_id=self.session_id)._scope(db)
            # SQL prefilter before bounded retrieval; quoted IDs avoid substring
            # matches in JSON on both SQLite and PostgreSQL. Recheck exact IDs below.
            planner_repos = or_(*(cast(Outcome.repo_ids, String).contains(f'"{rid}"')
                                  for rid in self.repo_ids))
            runs = (db.query(Run.id, Run.started_at, Run.finished_at, Run.role, Run.status,
                             Run.result["summary"].as_string(), Outcome.repo_ids, Task.repo_id)
                    .join(Outcome, Run.outcome_id == Outcome.id)
                    .outerjoin(Task, Run.task_id == Task.id)
                    .filter(Run.workspace_id == self.workspace_id, Outcome.workspace_id == self.workspace_id,
                            Run.id != current.id, Run.started_at < current.started_at,
                            Run.finished_at < current.started_at,
                            or_(and_(Run.task_id.is_(None), planner_repos),
                                and_(Task.workspace_id == self.workspace_id,
                                     Task.outcome_id == Outcome.id, Task.repo_id.in_(self.repo_ids)))))
            if days_back is not None:
                runs = runs.filter(Run.started_at >= utcnow() - timedelta(days=max(0, min(3650, int(days_back)))))
            matches = []
            for rid, started, finished, role, status, summary, outcome_repos, task_repo in runs.order_by(
                    Run.started_at.desc(), Run.id.desc()).limit(100):
                if task_repo is None and not set(outcome_repos or []) & set(self.repo_ids):
                    continue
                parts = [_safe(f"Factory run {rid} ({role}, {status})"), _safe(summary or "")]
                events = (db.query(Event.message).filter(
                    Event.workspace_id == self.workspace_id, Event.run_id == rid,
                    Event.created_at < current.started_at,
                    Event.kind.in_(("summary", "progress", "harness", "thought", "status", "error", "check",
                                    "run_started", "run_finished", "agent_thought", "agent_harness",
                                    "agent_progress", "agent_check")))
                    .order_by(Event.created_at.desc(), Event.id.desc()).limit(20))
                parts.extend(_safe(message) for (message,) in events)
                artifacts = (db.query(Artifact.name).filter(
                    Artifact.workspace_id == self.workspace_id, Artifact.run_id == rid,
                    Artifact.created_at < current.started_at)
                    .order_by(Artifact.created_at.desc(), Artifact.id.desc()).limit(10))
                parts.extend(_safe(name) for (name,) in artifacts)
                content = "\n".join(parts)[:6000]
                if query.lower() in content.lower():
                    matches.append(Episode(ts=started.isoformat(), session=rid, turn=0,
                                           role="assistant", content=content))
                    if len(matches) >= limit:
                        break
            return matches

    def recall_formatted(self, query, **kwargs):
        episodes = self.recall(query, **kwargs)
        if not episodes:
            return f"No episodes found matching '{_safe(query)}'."
        return "\n".join(f"[{ep.ts}] ({ep.role}) {ep.content}" for ep in episodes)


@dataclass(frozen=True)
class FactoryMemory:
    cortex: FactoryCortex
    episodic: FactoryEpisodicMemory

    async def settle(self, chat):
        """Call before ChatSession.close(), including cancellation/budget exits."""
        await chat.settle_memory_writes()


def build_factory_memory(*, workspace_id: str, run_id: str, allowed_repo_ids: list[str] | tuple[str, ...],
                         project_repo_id: str | None = None, llm_client=None) -> FactoryMemory:
    """Construct from trusted host context; never accept model-supplied scope IDs.

    Planners may read selected repositories and write workspace memory. With more
    than one repository, the host must explicitly select an allowed destination
    before exposing project writes. No mutable ambient 'current run' is used.
    """
    repo_ids = tuple(dict.fromkeys(allowed_repo_ids))
    with session_scope() as db:
        MemoryService(workspace_id, run_id=run_id)._scope(db)
        for rid in repo_ids:
            MemoryService(workspace_id, rid, run_id)._scope(db)
        if project_repo_id is not None and project_repo_id not in repo_ids:
            raise MemoryScopeError("Select an allowed repository for project writes")
    selected = project_repo_id if project_repo_id is not None else (repo_ids[0] if len(repo_ids) == 1 else None)
    global_hc = FactoryHippocampus(workspace_id, run_id, repo_ids, global_scope=True)
    project_hc = FactoryHippocampus(workspace_id, run_id, repo_ids, write_repo_id=selected)
    episodic = FactoryEpisodicMemory(workspace_id, run_id, repo_ids)
    return FactoryMemory(FactoryCortex(global_hc, project_hc, llm_client=llm_client, episodic=episodic), episodic)
