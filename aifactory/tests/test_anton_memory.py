"""Run against the real optional pinned SDK; no stand-in Cortex implementations."""
import asyncio
from datetime import timedelta
from types import SimpleNamespace

import pytest

pytest.importorskip("anton.core.memory.cortex")
from anton.core.memory.base import Engram, HippocampusProtocol
from anton.core.session import ChatSession
from anton.core.tools.tool_handlers import handle_memorize, handle_recall

from aifactory.db import session_scope, utcnow
from aifactory.harness.anton_memory import build_factory_memory
from aifactory.memory import MemoryScopeError, MemoryService
from aifactory.models import Artifact, Event, Run, Task
from tests.test_memory import seed
from tests.test_security_redaction import fake_credentials


def bind(ws, repos, run, **kwargs):
    return build_factory_memory(workspace_id=ws, run_id=run, allowed_repo_ids=repos, **kwargs)


def chat(memory):
    # Real SDK write tracking/finalization, without constructing an LLM provider.
    session = SimpleNamespace(_cortex=memory.cortex, _episodic=memory.episodic, _memory_writes=set())
    session._track_memory_write = lambda task: ChatSession._track_memory_write(session, task)
    session.settle_memory_writes = lambda: ChatSession.settle_memory_writes(session)
    return session


async def test_memorize_later_context_and_admin_deletion(client, signed_in):
    ws, repos, runs, outcome, _ = seed(signed_in["workspaces"][0]["id"])
    memory = bind(ws, [repos[0]], runs[1])
    assert isinstance(memory.cortex.project_hc, HippocampusProtocol)
    session = chat(memory)
    await handle_memorize(session, {"entries": [
        {"text": "repository memory", "kind": "lesson"},
        {"text": "workspace memory", "kind": "always", "scope": "global"},
        {"text": "Role: reviewer", "kind": "profile"}]})
    await memory.settle(session)
    await asyncio.sleep(0)  # allow tracked SDK tasks' discard callbacks to run
    assert not session._memory_writes
    entries = MemoryService(ws).list()
    assert len(entries) == 3
    assert all(e.run_id == runs[1] for e in entries)
    with session_scope() as db:
        later = Run(workspace_id=ws, outcome_id=outcome, role="planner", harness="anton")
        db.add(later)
        db.flush()
        later_id = later.id
    next_memory = bind(ws, [repos[0]], later_id)
    context = await next_memory.cortex.build_memory_context()
    assert all(text in context for text in ("repository memory", "workspace memory", "Role: reviewer"))
    lesson = next(e for e in entries if e.kind == "lesson")
    assert client.delete(f"/api/workspaces/{ws}/memory/{lesson.id}").status_code == 200
    # Both existing and newly constructed contexts consult the database, not caches.
    assert "repository memory" not in await next_memory.cortex.build_memory_context()
    assert "repository memory" not in await bind(ws, [repos[0]], later_id).cortex.build_memory_context()
    await next_memory.cortex.compact_all()
    assert MemoryService(ws).read(lesson.id) is None
    ws2, repos2, runs2, _, _ = seed()
    assert "workspace memory" not in await bind(ws2, [repos2[0]], runs2[1]).cortex.build_memory_context()
    other = await bind(ws, [repos[1]], runs[0]).cortex.build_memory_context()
    assert "workspace memory" in other
    assert "repository memory" not in other


async def test_multi_repo_requires_selector_and_validates_trusted_context():
    ws, repos, runs, _, _ = seed()
    memory = bind(ws, repos, runs[0])
    await memory.cortex.encode([Engram(text="shared", kind="lesson", scope="global")])
    with pytest.raises(MemoryScopeError, match="Select an allowed"):
        await memory.cortex.encode([Engram(text="ambiguous", kind="lesson", scope="project")])
    # The SDK backgrounds memorize and swallows store errors: no guessed write.
    session = chat(memory)
    await handle_memorize(session, {"entries": [{"text": "ambiguous tool call"}]})
    await memory.settle(session)
    assert [e.text for e in MemoryService(ws).list()] == ["shared"]
    selected = bind(ws, repos, runs[0], project_repo_id=repos[1])
    await selected.cortex.encode([Engram(text="selected", kind="lesson", scope="project")])
    assert MemoryService(ws, repos[1]).list()[0].text == "selected"
    assert "selected" in await memory.cortex.build_memory_context()
    assert "selected" not in await bind(ws, [repos[0]], runs[0]).cortex.build_memory_context()
    ws2, repos2, runs2, _, _ = seed()
    for args in ((ws2, repos, runs[0], {}), (ws, repos2, runs[0], {}),
                 (ws, repos, runs2[0], {}), (ws, repos, runs[0], {"project_repo_id": repos2[0]}),
                 (ws, repos, runs[1], {})):
        with pytest.raises(MemoryScopeError):
            bind(*args[:3], **args[3])



async def test_recall_factory_history_is_bounded_authorized_and_redacted():
    ws, repos, runs, outcome, task = seed()
    other_ws, other_repos, other_runs, _, _ = seed()  # same repo full_name
    secret = "\n".join(fake_credentials())
    before = utcnow() - timedelta(days=2)
    with session_scope() as db:
        for rid in runs + other_runs:
            r = db.get(Run, rid)
            r.started_at, r.finished_at = before, before + timedelta(hours=1)
            r.result = {"summary": "migration narrative " + secret, "raw": "DO_NOT_COPY"}
        unrelated_task = Task(workspace_id=ws, outcome_id=outcome, plan_id=db.get(Task, task).plan_id,
                              key="T2", title="other", repo_id=repos[1])
        db.add(unrelated_task)
        db.flush()
        unrelated = Run(workspace_id=ws, outcome_id=outcome, task_id=unrelated_task.id,
                        role="builder", harness="codex", started_at=before, finished_at=before,
                        result={"summary": "UNRELATED migration"})
        future = Run(workspace_id=ws, outcome_id=outcome, task_id=task, role="builder", harness="native",
                     started_at=utcnow() + timedelta(days=1), finished_at=utcnow() + timedelta(days=2),
                     result={"summary": "FUTURE migration"})
        current = Run(workspace_id=ws, outcome_id=outcome, task_id=task, role="builder", harness="anton",
                      result={"summary": "CURRENT migration"})
        db.add_all([unrelated, future, current])
        db.flush()
        current_id = current.id
        for rid in runs + other_runs:
            tenant = ws if rid in runs else other_ws
            db.add(Event(workspace_id=tenant, run_id=rid, kind="agent_thought", created_at=before,
                         message="migration event " + secret, data={"raw": "DO_NOT_COPY"}))
            db.add(Artifact(workspace_id=tenant, run_id=rid, kind="report", created_at=before,
                            name="migration artifact " + secret, content="DO_NOT_COPY"))
        db.add(Event(workspace_id=ws, run_id=runs[1], kind="progress", message="LATE migration"))
        db.add(Event(workspace_id=ws, run_id=runs[1], kind="tool", created_at=before,
                     message="RAW_TOOL migration"))
        db.add(Event(workspace_id=other_ws, run_id=runs[1], kind="progress", created_at=before,
                     message="FORGED_WORKSPACE migration"))
    memory = bind(ws, [repos[0]], current_id)
    result = await handle_recall(chat(memory), {"query": "migration"})
    assert all(word in result for word in ("narrative", "event", "artifact"))
    assert runs[0] in result and runs[1] in result  # native planner + native builder
    assert all(rid not in result for rid in other_runs)
    assert all(token not in result for token in fake_credentials())
    assert all(word not in result for word in ("DO_NOT_COPY", "UNRELATED", "FUTURE", "CURRENT",
                                               "LATE", "RAW_TOOL", "FORGED_WORKSPACE"))
    assert "[redacted]" in result
    assert len(memory.episodic.recall("migration", max_results=1)) == 1
    assert memory.episodic.recall("migration", days_back=1) == []
    assert memory.episodic.recall("migration", max_results=0) == []
    assert len(memory.episodic.recall_formatted("")) <= 50 * 6100
    # SDK logging/resume cannot create another journal or change the run.
    memory.episodic.start_session()
    memory.episodic.log_turn(1, "tool_call", secret)
    assert memory.episodic.get_items() == []
    with pytest.raises(MemoryScopeError):
        memory.episodic.resume_session(runs[0])


async def test_adapter_category_update_dedupe_clear():
    ws, repos, runs, _, _ = seed()
    hc = bind(ws, [repos[0]], runs[1]).cortex.project_hc
    hc.encode_lesson("oldest")
    hc.encode_rule("rule", "always")
    hc.rewrite_identity(["Role: builder"])
    for i in range(201):
        hc.encode_lesson(f"lesson {i}")
    assert hc.get_rules()[0].text == "rule"
    assert hc.get_identities()[0].text == "Role: builder"
    hc.update_lesson(Engram(text="oldest").id, "updated oldest")
    assert hc.get_lessons()[0].text == "updated oldest"
    hc.encode_lesson("duplicate")
    hc.encode_lesson("duplicate")
    hc.del_lesson(Engram(text="duplicate").id)
    assert not any(e.text == "duplicate" for e in hc.get_lessons())
    hc.clear()
    assert MemoryService(ws).list() == []


async def test_cancellation_finalization_drains_sdk_writes(monkeypatch):
    ws, repos, runs, _, _ = seed()
    memory = bind(ws, [repos[0]], runs[1])
    session = chat(memory)
    encode = memory.cortex.encode
    pending = asyncio.Event()

    async def delayed(entries):
        pending.set()
        await asyncio.sleep(.01)
        return await encode(entries)

    monkeypatch.setattr(memory.cortex, "encode", delayed)

    async def turn():
        try:
            await handle_memorize(session, {"entries": [{"text": "last round"}]})
            await asyncio.Event().wait()
        finally:
            await memory.settle(session)

    task = asyncio.create_task(turn())
    await pending.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    await asyncio.sleep(0)
    assert not session._memory_writes
    assert MemoryService(ws).list()[0].text == "last round"


@pytest.mark.parametrize("confidence", ["low", "medium", "high"])
async def test_rule_confidence_round_trip(confidence):
    ws, repos, runs, _, _ = seed()
    memory = bind(ws, [repos[0]], runs[1])
    await memory.cortex.encode([Engram(text="check confidence", kind="always",
                                      scope="project", confidence=confidence)])
    assert memory.cortex.project_hc.get_rules()[0].confidence == confidence


async def test_adapter_topic_and_scratchpad_filters_before_cap():
    from aifactory.models import MemoryEntry

    ws, repos, runs, _, _ = seed()
    memory = bind(ws, [repos[0]], runs[1])
    hc = memory.cortex.project_hc
    hc.encode_lesson("ordinary lesson", topic="older-topic")
    hc.encode_rule("ordinary rule", "when")
    with session_scope() as s:
        s.add_all(MemoryEntry(workspace_id=ws, repo_id=repos[0], run_id=runs[1],
                              kind=kind, text=f"Scratchpad {kind} {i}")
                  for kind in ("lesson", "when") for i in range(200))
    assert hc.recall_topic("older-topic") == "- ordinary lesson"
    assert [e.text for e in hc.get_lessons(exclude_scratchpad=True)] == ["ordinary lesson"]
    assert [e.text for e in hc.get_rules(exclude_scratchpad_when=True)] == ["ordinary rule"]
    hc.encode_lesson("older scratch wisdom", topic="scratchpad")
    with session_scope() as s:
        s.add_all(MemoryEntry(workspace_id=ws, repo_id=repos[0], run_id=runs[1],
                              kind="lesson", text=f"ordinary {i}") for i in range(200))
    assert "older scratch wisdom" in hc.recall_scratchpad_wisdom()
