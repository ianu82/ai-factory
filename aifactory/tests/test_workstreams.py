"""Workstream persistence, tenancy, permissions and atomic audit publication."""
from __future__ import annotations

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import inspect, text
from sqlalchemy.orm import Session

from aifactory import db, events
from aifactory.models import Event, Membership, Outcome, Repository, Workstream


@pytest.fixture
def root(signed_in):
    return f'/api/workspaces/{signed_in["workspaces"][0]["id"]}/workstreams'


def _create(client, root, **extra):
    response = client.post(root, json={"name": "Platform", "goal": "Make delivery reliable", **extra})
    assert response.status_code == 200, response.text
    return response.json()


def _audits():
    with db.session_scope() as s:
        return [events.event_dict(e) for e in s.query(Event).filter(Event.kind.like("workstream_%")).order_by(Event.created_at)]


def test_lifecycle_defaults_overrides_archive_and_attributed_events(client, signed_in, root, monkeypatch):
    ws = signed_in["workspaces"][0]["id"]
    actor = signed_in["user"]["id"]
    published = []
    monkeypatch.setattr(events.bus, "publish", published.append)
    with db.session_scope() as s:
        repo = Repository(workspace_id=ws, full_name="acme/app")
        s.add(repo)
        s.flush()
        repo_id = repo.id
    overrides = {"worker_override": "codex", "plan_approval_override": "approval",
                 "merge_approval_override": "auto", "priority_override": 0, "outcome_budget_override": 1.25}
    stream = _create(client, root, default_repo_ids=[repo_id, repo_id], **overrides)
    sid = stream["id"]
    assert stream["state"] == "active" and stream["owner_id"] == actor
    assert stream["default_repo_ids"] == [repo_id]
    assert stream["created_at"] and stream["updated_at"]
    assert client.get(root).json() == [stream]
    assert client.get(root + "/" + sid).json() == stream
    assert _create(client, root)["slug"] == "platform-2"
    renamed = client.patch(root + "/" + sid, json={"name": "Renamed", **dict.fromkeys(overrides)}).json()
    assert renamed["slug"] == "platform"
    assert all(renamed[k] is None for k in overrides)
    with db.session_scope() as s:
        outcome = Outcome(workspace_id=ws, workstream_id=sid, number=1, title="Existing outcome")
        s.add(outcome)
        s.flush()
        oid = outcome.id
    archived = client.patch(root + "/" + sid, json={"state": "archived"}).json()
    assert archived["state"] == "archived"
    assert archived["goal"] == stream["goal"] and archived["default_repo_ids"] == [repo_id]
    assert client.get(root + "/" + sid).json() == archived
    assert len(client.get(root).json()) == 1
    assert len(client.get(root + "?state=all").json()) == 2
    assert client.get(root + "?state=archived").json() == [archived]
    assert client.get(root + "?state=invalid").status_code == 422
    assert _create(client, root)["slug"] == "platform-3"
    assert client.delete(root + "/" + sid).status_code == 405
    with db.session_scope() as s:
        assert s.get(Outcome, oid).workstream_id == sid
    audit = _audits()
    assert [e["kind"] for e in audit] == ["workstream_created", "workstream_created", "workstream_updated",
                                            "workstream_archived", "workstream_created"]
    assert all(e["actor"] == actor and e["workspace_id"] == ws for e in audit)
    assert audit[0]["data"] == {"workstream_id": sid, "before": None, "after": stream}
    assert audit[2]["data"]["before"] == stream
    assert audit[2]["data"]["after"] == renamed
    assert audit[3]["data"]["before"] == renamed and audit[3]["data"]["after"] == archived
    assert [p["type"] for p in published] == ["event", "changed"] * 5


@pytest.mark.parametrize("patch", [
    {"name": " "}, {"name": "x" * 201}, {"goal": "\n\t"}, {"goal": "x" * 2001},
    {"name": None}, {"goal": None}, {"owner_id": None}, {"default_repo_ids": None}, {"state": None},
    {"priority_override": -1}, {"priority_override": 4}, {"priority_override": 1.5}, {"priority_override": True},
    {"worker_override": "unknown"}, {"plan_approval_override": "sometimes"}, {"merge_approval_override": "yes"},
    {"outcome_budget_override": 0}, {"outcome_budget_override": -1}, {"outcome_budget_override": "NaN"},
    {"outcome_budget_override": "Infinity"}, {"state": "deleted"}, {"slug": "another"},
    {"owner_id": "missing"}, {"default_repo_ids": ["missing"]},
])
def test_invalid_input_is_not_a_mutation(client, root, patch):
    stream = _create(client, root)
    before = _audits()
    assert client.patch(root + "/" + stream["id"], json=patch).status_code == 422
    assert client.post(root, json={"name": "Valid", "goal": "Valid", **patch}).status_code == 422
    assert client.get(root).json() == [stream]
    assert _audits() == before


def test_permissions_and_workspace_isolation(client, signed_in, root):
    ws = signed_in["workspaces"][0]["id"]
    user_id = signed_in["user"]["id"]
    stream = _create(client, root)
    path = root + "/" + stream["id"]
    other = TestClient(client.app)
    assert other.get(root).status_code == 401
    other.post("/auth/dev", json={"email": "other@example.test", "organization": "Other"})
    me = other.get("/api/me").json()
    other_id, other_ws = me["user"]["id"], me["workspaces"][0]["id"]
    foreign_root = f"/api/workspaces/{other_ws}/workstreams"
    foreign_stream = _create(other, foreign_root)
    with db.session_scope() as s:
        repo = Repository(workspace_id=other_ws, full_name="foreign/private")
        s.add(repo)
        s.flush()
        repo_id = repo.id
    before = _audits()
    for method, suffix, body in [("get", "", None), ("get", "/" + stream["id"], None),
                                 ("post", "", {"name": "Intrusion", "goal": "No"}),
                                 ("patch", "/" + stream["id"], {"state": "archived"})]:
        assert getattr(other, method)(root + suffix, **({"json": body} if body else {})).status_code == 404
    assert client.get(root + "/" + foreign_stream["id"]).status_code == 404
    assert client.patch(root + "/" + foreign_stream["id"], json={"name": "No"}).status_code == 404
    for patch in [{"owner_id": other_id}, {"default_repo_ids": [repo_id]}]:
        assert client.patch(path, json=patch).status_code == 422
        assert client.post(root, json={"name": "Bad", "goal": "Bad", **patch}).status_code == 422
    assert _audits() == before
    assert client.get(path).json() == stream
    assert client.get(root + "?state=all").json() == [stream]

    with db.session_scope() as s:
        s.add(Membership(workspace_id=ws, user_id=other_id, role="viewer"))
    before = _audits()
    assert other.get(path).status_code == 200
    assert other.post(root, json={"name": "No", "goal": "No"}).status_code == 403
    assert other.patch(path, json={"state": "archived"}).status_code == 403
    assert _audits() == before
    with db.session_scope() as s:
        s.query(Membership).filter_by(workspace_id=ws, user_id=other_id).one().role = "member"
    before = _audits()
    assert other.patch(path, json={"name": "No"}).status_code == 403
    assert other.post(root, json={"name": "No", "goal": "No", "owner_id": user_id}).status_code == 403
    assert _audits() == before
    owned = _create(other, root)
    owned_path = root + "/" + owned["id"]
    assert other.patch(owned_path, json={"goal": "Updated"}).status_code == 200
    before = _audits()
    assert other.patch(owned_path, json={"owner_id": user_id}).status_code == 403
    assert _audits() == before
    assert other.patch(owned_path, json={"state": "archived"}).status_code == 200
    # Workspace owner and admin can manage streams they do not own, including transfer.
    assert client.patch(owned_path, json={"state": "active"}).status_code == 200
    with db.session_scope() as s:
        s.query(Membership).filter_by(workspace_id=ws, user_id=other_id).one().role = "admin"
    assert other.patch(path, json={"goal": "Admin edit", "owner_id": other_id}).status_code == 200
    assert other.patch(path, json={"state": "archived"}).status_code == 200
    assert _create(other, root, owner_id=user_id)["owner_id"] == user_id
    assert _audits()[-1]["actor"] == other_id


@pytest.mark.parametrize("operation", ["create", "update", "archive"])
def test_commit_failure_rolls_back_audit_and_mutation_without_publishing(client, root, monkeypatch, operation):
    stream = _create(client, root)
    before = _audits()
    published = []
    monkeypatch.setattr(events.bus, "publish", published.append)

    def fail_commit(self):
        # Both rows were flushed inside this transaction before failure.
        assert self.query(Event).filter(Event.kind.like("workstream_%")).count() == len(before) + 1
        raise RuntimeError("commit failed")

    with monkeypatch.context() as m:
        m.setattr(Session, "commit", fail_commit)
        with pytest.raises(RuntimeError, match="commit failed"):
            if operation == "create":
                client.post(root, json={"name": "New", "goal": "Goal"})
            else:
                client.patch(root + "/" + stream["id"], json={"state": "archived"} if operation == "archive"
                             else {"name": "New"})
    assert published == []
    assert _audits() == before
    assert client.get(root + "?state=all").json() == [stream]


def test_publication_happens_after_commit(client, root, monkeypatch):
    published = []

    def publish(payload):
        # A separate connection must see the committed audit AND entity, for both notifications.
        with db.session_scope() as s:
            assert s.query(Workstream).count() == 1
            assert s.query(Event).filter_by(kind="workstream_created").count() == 1
        published.append(payload)

    monkeypatch.setattr(events.bus, "publish", publish)
    _create(client, root)
    assert [p["type"] for p in published] == ["event", "changed"]


def test_fresh_schema():
    inspector = inspect(db.engine())
    assert "workstreams" in inspector.get_table_names()
    column = next(c for c in inspector.get_columns("outcomes") if c["name"] == "workstream_id")
    assert column["nullable"]
    assert any(i["column_names"] == ["workstream_id"] for i in inspector.get_indexes("outcomes"))
    assert any(k["constrained_columns"] == ["workstream_id"] for k in inspector.get_foreign_keys("outcomes"))
    assert any(k["column_names"] == ["workspace_id", "slug"] for k in inspector.get_unique_constraints("workstreams"))


def test_upgrade_existing_outcomes_preserves_data(tmp_path, client, signed_in):
    # Build a real pre-feature schema from model metadata with just the new column/table removed.
    from sqlalchemy import MetaData, create_engine
    from aifactory.db import Base

    old_engine = create_engine(f"sqlite:///{tmp_path / 'old.db'}")
    metadata = MetaData()
    for table in Base.metadata.sorted_tables:
        if table.name != "workstreams":
            table.to_metadata(metadata)
    outcomes = metadata.tables["outcomes"]
    column = outcomes.c.workstream_id
    for index in list(outcomes.indexes):
        if column in list(index.columns):
            outcomes.indexes.remove(index)
    for constraint in list(outcomes.constraints):
        if column in list(constraint.columns):
            outcomes.constraints.remove(constraint)
    outcomes._columns.remove(column)
    outcomes.foreign_keys.clear()
    metadata.create_all(old_engine)
    with old_engine.begin() as conn:
        conn.execute(text("INSERT INTO outcomes (id, workspace_id, number, title, brief, acceptance, repo_ids, status, "
                          "priority, source, summary, blocked_reason, created_at, updated_at) VALUES "
                          "('old', 'ws', 7, 'Original title', 'Original brief', '[]', '[\"repo\"]', 'running', "
                          "1, 'user', 'Summary', '', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)"))
    old_engine.dispose()
    engine = db.init_engine(f"sqlite:///{tmp_path / 'old.db'}")
    db.init_engine(f"sqlite:///{tmp_path / 'old.db'}")  # idempotent
    with engine.connect() as conn:
        row = conn.execute(text("SELECT * FROM outcomes WHERE id='old'")).mappings().one()
        assert row["workstream_id"] is None
        assert row["title"] == "Original title" and row["brief"] == "Original brief"
        assert row["number"] == 7 and row["repo_ids"] == '["repo"]'
        assert row["priority"] == 1 and row["status"] == "running" and row["summary"] == "Summary"
    assert "workstreams" in inspect(engine).get_table_names()
    assert any(i["column_names"] == ["workstream_id"] for i in inspect(engine).get_indexes("outcomes"))


def test_concurrent_creators_get_distinct_slugs(client, root):
    from concurrent.futures import ThreadPoolExecutor

    def create(_):
        return client.post(root, json={"name": "Concurrent", "goal": "Safe slugs"})

    with ThreadPoolExecutor(max_workers=4) as pool:
        responses = list(pool.map(create, range(4)))
    assert all(r.status_code == 200 for r in responses), [r.text for r in responses]
    assert {r.json()["slug"] for r in responses} == {"concurrent", "concurrent-2", "concurrent-3", "concurrent-4"}
    assert len(_audits()) == 4


def test_audit_failure_rolls_back_creation(client, root, monkeypatch):
    def fail_record(*args, **kwargs):
        raise RuntimeError("audit failed")

    published = []
    monkeypatch.setattr(events, "record", fail_record)
    monkeypatch.setattr(events.bus, "publish", published.append)
    with pytest.raises(RuntimeError, match="audit failed"):
        client.post(root, json={"name": "New", "goal": "Goal"})
    assert client.get(root).json() == []
    assert _audits() == [] and published == []


def test_disconnect_prunes_defaults_and_audits_active_and_archived_streams(client, signed_in, root, monkeypatch):
    from aifactory import workstreams

    ws = signed_in["workspaces"][0]["id"]
    other = TestClient(client.app)
    other.post("/auth/dev", json={"email": "foreign@example.test", "organization": "Foreign"})
    foreign_ws = other.get("/api/me").json()["workspaces"][0]["id"]
    foreign_root = f"/api/workspaces/{foreign_ws}/workstreams"
    with db.session_scope() as s:
        repos = [Repository(workspace_id=ws, full_name="acme/removed"),
                 Repository(workspace_id=ws, full_name="acme/retained"),
                 Repository(workspace_id=foreign_ws, full_name="foreign/private")]
        s.add_all(repos)
        s.flush()
        removed, retained, foreign_repo = [repo.id for repo in repos]
    active = _create(client, root, default_repo_ids=[removed, retained], priority_override=1)
    archived = _create(client, root, default_repo_ids=[removed], outcome_budget_override=2)
    archived = client.patch(root + "/" + archived["id"], json={"state": "archived"}).json()
    unaffected = _create(client, root, default_repo_ids=[retained])
    foreign = _create(other, foreign_root, default_repo_ids=[foreign_repo])
    before = _audits()
    published = []

    def publish(payload):
        # Audit publication and changed notifications must see the committed deletion and cleanup.
        with db.session_scope() as s:
            assert s.get(Repository, removed) is None
            assert all(removed not in stream.default_repo_ids
                       for stream in s.query(Workstream).filter_by(workspace_id=ws))
            assert s.query(Event).filter(Event.kind.like("workstream_%")).count() == len(before) + 2
        published.append(payload)

    monkeypatch.setattr(events.bus, "publish", publish)
    path = root.removesuffix("/workstreams") + "/repositories/"
    # Foreign repository IDs cannot trigger cleanup or audit writes.
    assert client.delete(path + foreign_repo).status_code == 404
    assert _audits() == before and published == []
    assert client.delete(path + removed).status_code == 200
    updated = [client.get(root + "/" + stream["id"]).json() for stream in (active, archived)]
    assert updated[0]["default_repo_ids"] == [retained]
    assert updated[1]["default_repo_ids"] == [] and updated[1]["state"] == "archived"
    assert updated[0]["priority_override"] == 1 and updated[1]["outcome_budget_override"] == 2
    assert client.get(root + "/" + unaffected["id"]).json() == unaffected
    assert other.get(foreign_root + "/" + foreign["id"]).json() == foreign
    audits = _audits()[len(before):]
    assert {e["data"]["workstream_id"] for e in audits} == {active["id"], archived["id"]}
    for original, result in zip((active, archived), updated):
        audit = next(e for e in audits if e["data"]["workstream_id"] == original["id"])
        assert audit["kind"] == "workstream_updated"
        assert audit["actor"] == signed_in["user"]["id"] and audit["workspace_id"] == ws
        assert audit["data"]["before"] == original and audit["data"]["after"] == result
        with db.session_scope() as s:
            workstreams._references(s, ws, {"default_repo_ids": result["default_repo_ids"]})
    assert [p["type"] for p in published] == ["event", "changed"] * 2


@pytest.mark.parametrize("failure", ["audit", "commit"])
def test_disconnect_failure_rolls_back_defaults_repository_and_audit(client, signed_in, root, monkeypatch, failure):
    ws = signed_in["workspaces"][0]["id"]
    with db.session_scope() as s:
        repo = Repository(workspace_id=ws, full_name="acme/app")
        s.add(repo)
        s.flush()
        repo_id = repo.id
    streams = [_create(client, root, default_repo_ids=[repo_id]) for _ in range(2)]
    before = _audits()
    published = []
    monkeypatch.setattr(events.bus, "publish", published.append)
    original_record = events.record

    def fail_record(*args, **kwargs):
        if kwargs["data"]["workstream_id"] == streams[1]["id"]:
            raise RuntimeError("disconnect failed")
        return original_record(*args, **kwargs)

    def fail_commit(self):
        self.flush()
        assert self.get(Repository, repo_id) is None
        assert self.query(Event).filter(Event.kind.like("workstream_%")).count() == len(before) + 2
        raise RuntimeError("disconnect failed")

    with monkeypatch.context() as m:
        if failure == "audit":
            m.setattr(events, "record", fail_record)
        else:
            m.setattr(Session, "commit", fail_commit)
        with pytest.raises(RuntimeError, match="disconnect failed"):
            client.delete(root.removesuffix("/workstreams") + "/repositories/" + repo_id)
    with db.session_scope() as s:
        assert s.get(Repository, repo_id) is not None
    assert [client.get(root + "/" + stream["id"]).json() for stream in streams] == streams
    assert _audits() == before and published == []


def test_outcome_assignment_defaults_reassignment_and_audit(client, signed_in, root, monkeypatch):
    ws = signed_in['workspaces'][0]['id']
    base = f'/api/workspaces/{ws}/outcomes'
    with db.session_scope() as s:
        repos = [Repository(workspace_id=ws, full_name=f'acme/{n}') for n in ('app', 'other')]
        s.add_all(repos)
        s.flush()
        first, second = [r.id for r in repos]
    stream = _create(client, root, default_repo_ids=[first])
    other = _create(client, root, name='Other', default_repo_ids=[second])
    assert client.post(base, json={'title': 'No repositories'}).status_code == 400
    created = client.post(base, json={'title': 'Prefill repositories', 'workstream_id': stream['id']}).json()
    assert created['repo_ids'] == [first] and created['workstream_id'] == stream['id']
    explicit = client.post(base, json={'title': 'Explicit repositories', 'workstream_id': stream['id'],
                                       'repo_ids': [second]}).json()
    assert explicit['repo_ids'] == [second]
    path = base + '/' + created['id']
    with db.session_scope() as s:
        s.get(Outcome, created['id']).status = 'running'
    for sid in (other['id'], None):
        response = client.patch(path, json={'workstream_id': sid})
        assert response.status_code == 200, response.text
        assert response.json()['workstream_id'] == sid
        assert response.json()['repo_ids'] == [first] and response.json()['status'] == 'running'
    for body in ({'status': 'completed'}, {'workstream_id': stream['id'], 'status': 'completed'}, {}):
        assert client.patch(path, json=body).status_code == 422
    client.patch(root + '/' + stream['id'], json={'state': 'archived'})
    assert client.patch(path, json={'workstream_id': stream['id']}).status_code == 422
    assert client.post(base, json={'title': 'Archived assignment', 'workstream_id': stream['id'],
                                  'repo_ids': [first]}).status_code == 422
    with db.session_scope() as s:
        audits = s.query(Event).filter_by(kind='outcome_workstream_changed').order_by(Event.id).all()
        assert len(audits) == 2
        assert all(e.actor == signed_in['user']['id'] for e in audits)
        assert audits[0].data == {'old_workstream_id': stream['id'], 'new_workstream_id': other['id']}
        assert audits[1].data == {'old_workstream_id': other['id'], 'new_workstream_id': None}
        s.query(Membership).filter_by(workspace_id=ws, user_id=signed_in['user']['id']).one().role = 'viewer'
    assert client.patch(path, json={'workstream_id': None}).status_code == 403
    assert client.post(base, json={'title': 'Viewer creation', 'repo_ids': [first]}).status_code == 403


@pytest.mark.parametrize("assigned", [False, True])
def test_outcome_creation_deduplicates_explicit_repositories(client, signed_in, root, assigned):
    ws = signed_in["workspaces"][0]["id"]
    with db.session_scope() as s:
        repo = Repository(workspace_id=ws, full_name="acme/app")
        s.add(repo)
        s.flush()
        repo_id = repo.id
    body = {"title": "Repeated repository", "repo_ids": [repo_id, repo_id]}
    if assigned:
        # Explicit repositories still win over empty stream defaults.
        body["workstream_id"] = _create(client, root)["id"]
    response = client.post(f"/api/workspaces/{ws}/outcomes", json=body)
    assert response.status_code == 200, response.text
    assert response.json()["repo_ids"] == [repo_id]
    assert response.json()["workstream_id"] == body.get("workstream_id")


def test_session_publications_commit_order_and_savepoint_rollback(signed_in, monkeypatch):
    ws = signed_in['workspaces'][0]['id']
    published = []
    monkeypatch.setattr(events.bus, 'publish', published.append)
    with db.session_scope() as s:
        events.record(ws, 'outer', session=s)
        events.notify(ws, 'signal', 'outer', session=s)
        with pytest.raises(ValueError):
            with s.begin_nested():
                events.record(ws, 'rolled_back', session=s)
                events.notify(ws, 'signal', 'rolled_back', session=s)
                raise ValueError('rollback savepoint')
        with s.begin_nested():
            events.record(ws, 'nested', session=s)
        assert published == []
        events.record(ws, 'last', session=s)
    assert [(p['type'], p.get('kind', p.get('id'))) for p in published] == [
        ('event', 'outer'), ('changed', 'outer'), ('event', 'nested'), ('event', 'last')]
    published.clear()
    with pytest.raises(ValueError):
        with db.session_scope() as s:
            events.record(ws, 'discard', session=s)
            events.notify(ws, 'signal', 'discard', session=s)
            raise ValueError('rollback outer')
    assert published == []


def test_assignment_audit_failure_rolls_back(client, signed_in, root, monkeypatch):
    ws = signed_in['workspaces'][0]['id']
    stream = _create(client, root)
    with db.session_scope() as s:
        o = Outcome(workspace_id=ws, number=1, title='Untouched', status='running')
        s.add(o)
        s.flush()
        oid = o.id
    original = events.record
    def fail(*args, **kwargs):
        if args[1] == 'outcome_workstream_changed':
            raise RuntimeError('audit failed')
        return original(*args, **kwargs)
    monkeypatch.setattr('aifactory.api.record', fail)
    published = []
    monkeypatch.setattr(events.bus, 'publish', published.append)
    with pytest.raises(RuntimeError, match='audit failed'):
        client.patch(f'/api/workspaces/{ws}/outcomes/{oid}', json={'workstream_id': stream['id']})
    with db.session_scope() as s:
        assert s.get(Outcome, oid).workstream_id is None
    assert not published


def test_rollup_notification_follows_committed_event(signed_in, monkeypatch):
    from types import SimpleNamespace
    from aifactory.pipeline.orchestrator import orchestrator
    ws = signed_in['workspaces'][0]['id']
    published = []
    monkeypatch.setattr(events.bus, 'publish', published.append)
    with db.session_scope() as s:
        outcome = Outcome(workspace_id=ws, number=1, title='Complete', status='running')
        s.add(outcome)
        s.flush()
        task = SimpleNamespace(status='completed')
        orchestrator._rollup(outcome, [task], s)
        assert published == []
    assert [(p['type'], p.get('kind')) for p in published] == [('event', 'outcome_completed'), ('changed', None)]


def test_assignment_route_preserves_existing_priority_edits(client, signed_in, root):
    ws = signed_in["workspaces"][0]["id"]
    stream = _create(client, root)
    with db.session_scope() as s:
        outcome = Outcome(workspace_id=ws, number=1, title="Shared endpoint", status="running", priority=2)
        s.add(outcome)
        s.flush()
        oid = outcome.id
    path = f"/api/workspaces/{ws}/outcomes/{oid}"
    assert client.patch(path, json={"priority": 0}).status_code == 200
    assigned = client.patch(path, json={"workstream_id": stream["id"]})
    assert assigned.status_code == 200
    assert assigned.json()["priority"] == 0 and assigned.json()["status"] == "running"
    assert assigned.json()["workstream_id"] == stream["id"]
    assert client.patch(path, json={"priority": 1, "workstream_id": None}).status_code == 422
    assert client.patch(path, json={"priority": 1, "status": "completed"}).status_code == 422
