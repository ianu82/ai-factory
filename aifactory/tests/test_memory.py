"""Persistent, attributed memory and tenant-safe management API."""

import json

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import inspect

from aifactory import db
from aifactory.db import session_scope
from aifactory.memory import MemoryScopeError, MemoryService
from aifactory.models import Event, Membership, Organization, Outcome, Plan, Repository, Run, Task, Workspace


def seed(workspace_id=None):
    with session_scope() as s:
        if workspace_id is None:
            org = Organization(name="org")
            s.add(org)
            s.flush()
            ws = Workspace(org_id=org.id, name="ws", slug="ws")
            s.add(ws)
            s.flush()
            workspace_id = ws.id
        repos = [Repository(workspace_id=workspace_id, full_name=name) for name in ("same/app", "other/app")]
        s.add_all(repos)
        s.flush()
        outcome = Outcome(workspace_id=workspace_id, number=1, title="o", repo_ids=[r.id for r in repos])
        s.add(outcome)
        s.flush()
        plan = Plan(outcome_id=outcome.id)
        s.add(plan)
        s.flush()
        task = Task(workspace_id=workspace_id, outcome_id=outcome.id, plan_id=plan.id,
                    key="T1", title="task", repo_id=repos[0].id)
        s.add(task)
        s.flush()
        runs = [Run(workspace_id=workspace_id, outcome_id=outcome.id, role="planner", harness="native"),
                Run(workspace_id=workspace_id, outcome_id=outcome.id, task_id=task.id,
                    role="builder", harness="native")]
        s.add_all(runs)
        s.flush()
        return workspace_id, [r.id for r in repos], [r.id for r in runs], outcome.id, task.id


def test_persistence_scope_and_journal():
    ws, repos, runs, _, _ = seed()
    workspace = MemoryService(ws, run_id=runs[0]).create(text="workspace lesson", topic="shared")
    first = MemoryService(ws, repos[0], runs[1]).create(text="first lesson", confidence=.8, source="test")
    second = MemoryService(ws, repos[1], runs[0]).create(text="second lesson")
    db.init_engine()  # same persisted database, new engine and sessions; additive table creation is idempotent
    assert "memory_entries" in inspect(db.engine()).get_table_names()
    saved = MemoryService(ws).read(first.id)
    assert (saved.text, saved.workspace_id, saved.repo_id, saved.run_id) == ("first lesson", ws, repos[0], runs[1])
    assert (saved.kind, saved.confidence, saved.source) == ("lesson", .8, "test")
    assert saved.created_at and saved.updated_at
    assert {e.id for e in MemoryService(ws, repos[0]).list()} == {workspace.id, first.id}
    assert {e.id for e in MemoryService(ws, repos[1]).list()} == {workspace.id, second.id}
    assert {e.id for e in MemoryService(ws, run_id=runs[1]).list()} == {first.id}
    assert MemoryService(ws, repos[1]).read(first.id) is None
    assert not MemoryService(ws, repos[1]).delete(first.id)
    assert MemoryService(ws).delete(first.id, actor="admin")
    assert MemoryService(ws).read(first.id) is None
    assert not MemoryService(ws).delete(first.id)
    with session_scope() as s:
        events = s.query(Event).filter(Event.run_id == runs[1]).order_by(Event.created_at).all()
        assert [e.kind for e in events] == ["memory_saved", "memory_deleted"]
        assert all(e.workspace_id == ws and e.task_id for e in events)
        assert events[-1].actor == "admin"
        assert "first lesson" not in json.dumps([{"message": e.message, "data": e.data} for e in events])


def test_tenant_and_repository_substitution():
    a, ar, au, _, _ = seed()
    b, br, bu, _, _ = seed()  # deliberately identical repository names across tenants
    entry = MemoryService(a, ar[0], au[1]).create(text="private")
    assert MemoryService(b).list() == []
    assert MemoryService(b).read(entry.id) is None
    assert not MemoryService(b).delete(entry.id)
    for service in (MemoryService(b, ar[0]), MemoryService(b, run_id=au[0]),
                    MemoryService(a, br[0], au[0]), MemoryService(a, ar[0], bu[0]),
                    MemoryService(a, ar[1], au[1]), MemoryService("missing")):
        with pytest.raises(MemoryScopeError):
            service.create(text="forged")
        with pytest.raises(MemoryScopeError):
            service.list()
        with pytest.raises(MemoryScopeError):
            service.read(entry.id)
        with pytest.raises(MemoryScopeError):
            service.delete(entry.id)
    assert MemoryService(a).read(entry.id).text == "private"


def test_invalid_provenance():
    a, repos, runs, outcome, task = seed()
    b, _, _, foreign_outcome, foreign_task = seed()
    with pytest.raises(MemoryScopeError):
        MemoryService(a).create(text="no source run")
    with session_scope() as s:
        s.get(Run, runs[0]).outcome_id = foreign_outcome
        s.get(Run, runs[1]).task_id = foreign_task
    for run_id in runs:
        with pytest.raises(MemoryScopeError):
            MemoryService(a, repos[0], run_id).create(text="invalid provenance")
    with session_scope() as s:
        s.get(Run, runs[1]).task_id = task
        s.get(Task, task).key = "T-mismatch"
        s.get(Task, task).outcome_id = foreign_outcome
    with pytest.raises(MemoryScopeError):
        MemoryService(a, repos[0], runs[1]).create(text="mismatched task outcome")


@pytest.mark.parametrize("fields", [{"text": ""}, {"text": "x" * 16001}, {"kind": " "},
                                    {"topic": "x" * 201}, {"source": "x" * 2001},
                                    {"confidence": float("nan")}, {"confidence": float("inf")},
                                    {"confidence": -.1}, {"confidence": 1.1}])
def test_bounded_input(fields):
    ws, repos, runs, _, _ = seed()
    with pytest.raises(ValueError):
        MemoryService(ws, repos[0], runs[1]).create(**({"text": "lesson"} | fields))
    assert MemoryService(ws).list() == []


def test_bounded_listing():
    ws, repos, runs, _, _ = seed()
    service = MemoryService(ws, repos[0], runs[1])
    for i in range(3):
        service.create(text=str(i))
    all_entries = service.list()
    assert service.list(limit=1, offset=1)[0].id == all_entries[1].id
    for args in ({"limit": 0}, {"limit": 201}, {"offset": -1}, {"offset": 1_000_001}):
        with pytest.raises(ValueError):
            service.list(**args)


@pytest.mark.parametrize("role,delete_status", [("viewer", 403), ("member", 403), ("admin", 200), ("owner", 200)])
def test_api_roles_and_serialization(client, signed_in, role, delete_status):
    ws, repos, runs, _, _ = seed(signed_in["workspaces"][0]["id"])
    shared = MemoryService(ws, run_id=runs[0]).create(text="shared")
    entry = MemoryService(ws, repos[0], runs[1]).create(text="repository lesson")
    MemoryService(ws, repos[1], runs[0]).create(text="other repository")
    with session_scope() as s:
        s.query(Membership).filter(Membership.workspace_id == ws).one().role = role
    path = f"/api/workspaces/{ws}/memory"
    response = client.get(path, params={"repo_id": repos[0]})
    assert response.status_code == 200
    rows = {e["id"]: e for e in response.json()}
    assert set(rows) == {shared.id, entry.id}
    assert rows[shared.id]["scope"] == "workspace"
    assert rows[entry.id]["scope"] == "repository"
    assert rows[entry.id]["run_id"] == runs[1]
    assert rows[entry.id]["created_at"].endswith("Z")
    assert [e["id"] for e in client.get(path, params={"run_id": runs[1]}).json()] == [entry.id]
    assert client.get(path, params={"repo_id": repos[1], "run_id": runs[1]}).status_code == 404
    assert client.delete(f"{path}/{entry.id}").status_code == delete_status
    assert (MemoryService(ws).read(entry.id) is None) == (delete_status == 200)
    if delete_status == 200:
        assert client.delete(f"{path}/{shared.id}").status_code == 200
    for params in ({"limit": 201}, {"limit": 0}, {"offset": -1}):
        assert client.get(path, params=params).status_code == 422


def test_api_tenant_substitution(client, signed_in):
    a, repos, runs, _, _ = seed(signed_in["workspaces"][0]["id"])
    entry = MemoryService(a, repos[0], runs[1]).create(text="private")
    other = TestClient(client.app)
    assert other.post("/auth/dev", json={"email": "other@test.com", "organization": "other"}).status_code == 200
    b = other.get("/api/me").json()["workspaces"][0]["id"]
    path = f"/api/workspaces/{b}/memory"
    assert other.get(path).json() == []
    assert other.get(f"/api/workspaces/{a}/memory").status_code == 404
    assert other.delete(f"/api/workspaces/{a}/memory/{entry.id}").status_code == 404
    assert other.delete(f"{path}/{entry.id}").status_code == 404
    for params in ({"repo_id": repos[0]}, {"run_id": runs[1]}, {"repo_id": "missing"}, {"run_id": "missing"}):
        assert other.get(path, params=params).status_code == 404
    assert MemoryService(a).read(entry.id)
    anonymous = TestClient(client.app)
    assert anonymous.get(path).status_code == 401
    assert anonymous.delete(f"{path}/{entry.id}").status_code == 401


def test_disconnect_repository_with_planner_memory(client, signed_in):
    ws = signed_in["workspaces"][0]["id"]
    with session_scope() as s:
        repos = [Repository(workspace_id=ws, full_name=name) for name in ("same/app", "other/app")]
        s.add_all(repos)
        s.flush()
        repo_id, other_repo_id = [r.id for r in repos]
        outcome = Outcome(workspace_id=ws, number=1, title="planning", repo_ids=[repo_id, other_repo_id])
        s.add(outcome)
        s.flush()
        run = Run(workspace_id=ws, outcome_id=outcome.id, role="planner", harness="native")
        s.add(run)
        s.flush()
        run_id = run.id
    lesson = MemoryService(ws, repo_id, run_id).create(text="disconnected repository secret")
    shared = MemoryService(ws, run_id=run_id).create(text="workspace lesson")
    other = MemoryService(ws, other_repo_id, run_id).create(text="other repository lesson")
    foreign_ws, foreign_repos, foreign_runs, _, _ = seed()
    foreign = MemoryService(foreign_ws, foreign_repos[0], foreign_runs[0]).create(text="foreign lesson")
    path = f"/api/workspaces/{ws}/repositories"

    # Substituting a foreign repository must not trigger cleanup.
    assert client.delete(f"{path}/{foreign_repos[0]}").status_code == 404
    assert client.delete(f"{path}/{repo_id}").status_code == 200
    assert MemoryService(ws).read(lesson.id) is None
    assert {e.id for e in MemoryService(ws).list()} == {shared.id, other.id}
    assert MemoryService(foreign_ws).read(foreign.id) is not None
    assert client.get(f"/api/workspaces/{ws}/memory").status_code == 200
    assert client.get(f"/api/workspaces/{ws}/memory", params={"repo_id": repo_id}).status_code == 404
    with session_scope() as s:
        assert s.get(Repository, repo_id) is None
        assert s.query(Task).filter(Task.workspace_id == ws).count() == 0
        event = s.query(Event).filter(Event.kind == "memory_deleted", Event.workspace_id == ws).one()
        assert event.run_id == run_id
        assert event.actor == signed_in["user"]["id"]
        assert event.data == {"entry_id": lesson.id, "repo_id": repo_id, "scope": "repository"}
        assert lesson.text not in json.dumps({"message": event.message, "data": event.data})


def test_disconnect_active_repository_preserves_memory(client, signed_in):
    ws, repos, runs, _, task_id = seed(signed_in["workspaces"][0]["id"])
    with session_scope() as s:
        s.get(Task, task_id).status = "queued"
    lesson = MemoryService(ws, repos[0], runs[0]).create(text="still needed")
    assert client.delete(f"/api/workspaces/{ws}/repositories/{repos[0]}").status_code == 409
    assert MemoryService(ws).read(lesson.id) is not None
    with session_scope() as s:
        assert s.query(Event).filter(Event.kind == "memory_deleted").count() == 0


def test_repository_memory_cleanup_is_transactional():
    ws, repos, runs, _, _ = seed()
    lesson = MemoryService(ws, repos[0], runs[0]).create(text="keep on rollback")
    with pytest.raises(RuntimeError, match="abort disconnect"):
        with session_scope() as s:
            MemoryService(ws, repos[0]).delete_repository_entries(session=s)
            raise RuntimeError("abort disconnect")
    assert MemoryService(ws).read(lesson.id) is not None
    with session_scope() as s:
        assert s.query(Event).filter(Event.kind == "memory_deleted").count() == 0
        with pytest.raises(MemoryScopeError):
            MemoryService(ws).delete_repository_entries(session=s)


def test_atomic_concurrent_encoding_and_legacy_duplicate():
    from concurrent.futures import ThreadPoolExecutor
    from threading import Barrier

    from sqlalchemy import event

    from aifactory.models import MemoryEntry

    ws, repos, runs, _, _ = seed()
    service = MemoryService(ws, repos[0], runs[1])
    barrier = Barrier(2)
    locks = []

    def observe(conn, cursor, statement, parameters, context, executemany):
        if statement.startswith("UPDATE workspaces SET outcome_seq="):
            locks.append(statement)

    def encode():
        barrier.wait()
        return service.encode(text="concurrent", kind="lesson").id

    event.listen(db.engine(), "before_cursor_execute", observe)
    try:
        with ThreadPoolExecutor(max_workers=2) as pool:
            ids = list(pool.map(lambda _: encode(), range(2)))
    finally:
        event.remove(db.engine(), "before_cursor_execute", observe)
    assert len(locks) == 2
    assert len(set(ids)) == 1
    assert len(service.list()) == 1
    legacy = service.create(text="concurrent", kind="lesson")
    with session_scope() as s:
        assert s.query(MemoryEntry).count() == 2
    assert len(service.context_entries(kinds=("lesson",))) == 1
    assert service.context_entries(kinds=("lesson",))[0].id == legacy.id


def test_category_caps_recency_duplicate_mutation_and_clear():
    import hashlib

    from aifactory.models import MemoryEntry

    ws, repos, runs, _, _ = seed()
    service = MemoryService(ws, repos[0], runs[1])
    profile = service.encode(text="Role: reviewer", kind="profile")
    rule = service.encode(text="check boundaries", kind="always")
    oldest = service.encode(text="oldest", kind="lesson")
    # Bulk fixtures intentionally bypass encode to cover legacy rows as well.
    with session_scope() as s:
        s.add_all(MemoryEntry(workspace_id=ws, repo_id=repos[0], run_id=runs[1],
                              kind="lesson", text=f"lesson {i}") for i in range(205))
    assert len(service.context_entries(kinds=("lesson",))) == 200
    assert service.context_entries(kinds=("profile",))[0].id == profile.id
    assert service.context_entries(kinds=("always", "never", "when"))[0].id == rule.id
    service.mutate_engram(hashlib.sha256(oldest.text.encode()).hexdigest(), text="updated oldest")
    assert service.context_entries(kinds=("lesson",))[0].text == "updated oldest"
    first = service.encode(text="duplicate", kind="lesson")
    assert service.encode(text="duplicate", kind="lesson").id == first.id
    service.create(text="duplicate")
    service.mutate_engram(hashlib.sha256(b"duplicate").hexdigest(), text="merged")
    assert [e.text for e in service.list() if e.text in ("duplicate", "merged")] == ["merged"]
    service.create(text="merged")
    service.mutate_engram(hashlib.sha256(b"merged").hexdigest())
    assert not any(e.text == "merged" for e in service.list())
    shared = MemoryService(ws, run_id=runs[0]).create(text="keep workspace")
    foreign = MemoryService(ws, repos[1], runs[0]).create(text="keep other repo")
    service.mutate_engram()
    assert service.context_entries(kinds=("lesson", "profile", "always")) == []
    assert {e.id for e in MemoryService(ws).list()} == {shared.id, foreign.id}


def test_encoding_validates_exact_scope_and_attributes_new_run():
    ws, repos, runs, outcome, _ = seed()
    other_ws, other_repos, other_runs, _, _ = seed()
    initial = MemoryService(ws, repos[0], runs[1]).encode(text="same", kind="lesson")
    with session_scope() as s:
        later = Run(workspace_id=ws, outcome_id=outcome, role="planner", harness="native")
        s.add(later)
        s.flush()
        later_id = later.id
    updated = MemoryService(ws, repos[0], later_id).encode(text="same", kind="lesson")
    assert updated.id == initial.id
    assert updated.run_id == later_id
    assert MemoryService(ws, run_id=later_id).list()[0].id == initial.id
    for service in (MemoryService(other_ws, repos[0], other_runs[0]),
                    MemoryService(ws, other_repos[0], runs[0]),
                    MemoryService(ws, repos[1], runs[1]), MemoryService(ws, repos[0])):
        with pytest.raises(MemoryScopeError):
            service.encode(text="forged", kind="lesson")
        with pytest.raises(MemoryScopeError):
            service.mutate_engram()
    # Exact context scopes do not include workspace or other repository entries.
    MemoryService(ws, run_id=runs[0]).encode(text="workspace", kind="lesson")
    MemoryService(ws, repos[1], runs[0]).encode(text="other repo", kind="lesson")
    assert [e.text for e in MemoryService(ws, repos[0]).context_entries(kinds=("lesson",))] == ["same"]
    assert [e.text for e in MemoryService(ws).context_entries(kinds=("lesson",))] == ["workspace"]
    assert MemoryService(other_ws).context_entries(kinds=("lesson",)) == []


def test_context_filters_apply_before_cap():
    from aifactory.models import MemoryEntry

    ws, repos, runs, _, _ = seed()
    service = MemoryService(ws, repos[0], runs[1])
    ordinary = service.encode(text="ordinary lesson", kind="lesson", topic="older-topic")
    scratch = service.encode(text="older wisdom", kind="lesson", topic="Scratchpad")
    rule = service.encode(text="ordinary rule", kind="when")
    with session_scope() as s:
        s.add_all(MemoryEntry(workspace_id=ws, repo_id=repos[0], run_id=runs[1],
                              kind=kind, text=f"Scratchpad {kind} {i}", topic="newer-topic")
                  for kind in ("lesson", "when") for i in range(200))
    assert service.context_entries(kinds=("lesson",), topic="older-topic")[0].id == ordinary.id
    assert [e.id for e in service.context_entries(kinds=("lesson",), scratchpad=False)] == [ordinary.id]
    assert [e.id for e in service.context_entries(kinds=("when",), exclude_scratchpad_when=True)] == [rule.id]
    # Inverse filtering also precedes the cap: newer ordinary lessons cannot
    # crowd older scratchpad wisdom out of the specialized read.
    with session_scope() as s:
        s.add_all(MemoryEntry(workspace_id=ws, repo_id=repos[0], run_id=runs[1],
                              kind="lesson", text=f"ordinary {i}") for i in range(200))
    assert service.context_entries(kinds=("lesson",), scratchpad=True, topic="Scratchpad")[0].id == scratch.id
