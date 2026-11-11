"""Workspace goals, optional capability contracts and strict triage settings."""
import pytest
from fastapi.testclient import TestClient

from aifactory import db
from aifactory.goals import TriageCapabilities, effective_admission, resolve_goals
from aifactory.models import Goal, Membership, Repository, Workstream


@pytest.fixture
def setup(client, signed_in):
    ws = signed_in["workspaces"][0]["id"]
    with db.session_scope() as s:
        repo = Repository(workspace_id=ws, full_name="acme/app")
        s.add(repo)
        s.flush()
        repo_id = repo.id
    return ws, f"/api/workspaces/{ws}", {"name": "Reliability", "description": "Keep main green",
                                               "owner_id": signed_in["user"]["id"], "repo_ids": [repo_id]}


def test_lifecycle_and_defaults(client, setup):
    ws, base, body = setup
    result = client.post(base + "/goals", json=body)
    assert result.status_code == 200, result.text
    goal = result.json()
    assert goal["admission"] == {"enabled": False, "repo_ids": [], "max_urgency": 3}
    assert goal["workstream_id"] is None
    assert client.get(base + "/goals").json() == [goal]
    selected = client.get(base + "/triage/goals").json()
    assert selected["source"] == "workspace"
    assert selected["goals"][0]["id"] == goal["id"]
    assert not selected["goals"][0]["effective_admission"]["enabled"]
    updated = client.patch(base + "/goals/" + goal["id"], json={"name": "Delivery"})
    assert updated.status_code == 200 and updated.json()["id"] == goal["id"]
    assert client.patch(base + "/goals/" + goal["id"], json={"state": "archived"}).status_code == 200
    assert client.get(base + "/goals").json() == []
    assert client.get(base + "/triage/goals").json()["goals"] == []
    assert len(client.get(base + "/goals?state=all").json()) == 1
    assert client.get(base + "/goals?limit=201").status_code == 422
    with db.session_scope() as s:
        assert s.get(Goal, goal["id"]).state == "archived"


@pytest.mark.parametrize("patch", [
    {"name": " "}, {"description": ""}, {"owner_id": "missing"}, {"repo_ids": ["missing"]},
    {"repo_ids": []}, {"workstream_id": "missing"}, {"admission": {"repo_ids": ["missing"]}},
    {"admission": {"max_urgency": 4}}, {"admission": {"max_urgency": -1}},
    {"admission": {"max_urgency": 1.5}}, {"admission": {"max_urgency": "NaN"}},
    {"admission": {"max_urgency": True}}, {"admission": {"enabled": "true"}},
    {"name": None}, {"description": None}, {"repo_ids": None}, {"admission": None},
    {"owner_id": None}, {"state": None}, {"unexpected": 1},
])
def test_invalid_goals_do_not_mutate(client, setup, patch):
    _, base, body = setup
    original = client.post(base + "/goals", json=body).json()
    assert client.post(base + "/goals", json={**body, **patch}).status_code == 422
    assert client.patch(base + "/goals/" + original["id"], json=patch).status_code == 422
    assert client.get(base + "/goals").json() == [original]


def test_permissions_and_tenancy(client, setup, signed_in):
    ws, base, body = setup
    goal = client.post(base + "/goals", json=body).json()
    path = base + "/goals/" + goal["id"]
    other = TestClient(client.app)
    assert other.get(base + "/goals").status_code == 401
    other.post("/auth/dev", json={"email": "other@example.test", "organization": "Other"})
    me = other.get("/api/me").json()
    foreign_ws, foreign_user = me["workspaces"][0]["id"], me["user"]["id"]
    with db.session_scope() as s:
        repo = Repository(workspace_id=foreign_ws, full_name="foreign/private")
        stream = Workstream(workspace_id=foreign_ws, slug="foreign", name="Foreign", goal="No", owner_id=foreign_user)
        s.add_all([repo, stream])
        s.flush()
        invalid = [{"owner_id": foreign_user}, {"repo_ids": [repo.id]}, {"workstream_id": stream.id}]
    for patch in invalid:
        assert client.post(base + "/goals", json={**body, **patch}).status_code == 422
        assert client.patch(path, json=patch).status_code == 422
    assert other.get(base + "/goals").status_code == 404
    assert other.patch(path, json={"state": "archived"}).status_code == 404
    assert client.patch(f"/api/workspaces/{foreign_ws}/goals/{goal['id']}", json={"name": "No"}).status_code == 404
    for role in ("viewer", "member", "admin"):
        with db.session_scope() as s:
            member = s.query(Membership).filter_by(workspace_id=ws, user_id=foreign_user).first()
            if member:
                member.role = role
            else:
                s.add(Membership(workspace_id=ws, user_id=foreign_user, role=role))
        assert other.get(base + "/goals").status_code == 200
        assert other.get(base + "/triage/goals").status_code == 200
        expected = 200 if role == "admin" else 403
        assert other.post(base + "/goals", json=body).status_code == expected
        assert other.patch(path, json={"name": "Renamed"}).status_code == expected
        assert other.patch(base, json={"settings": {"triage": {"enabled": False}}}).status_code == expected


def test_optional_workstreams_and_stable_attachment(client, setup):
    ws, base, body = setup
    goal = client.post(base + "/goals", json=body).json()
    # No capability: workspace goals even with optional feature absent.
    with db.session_scope() as s:
        assert resolve_goals(s, ws, TriageCapabilities())["goals"][0]["id"] == goal["id"]
    streams = [{"id": "injected", "workspace_id": ws, "name": "Stream", "goal": "Ship safely",
                "owner_id": body["owner_id"], "default_repo_ids": body["repo_ids"], "state": "active"},
               {"id": "inactive", "workspace_id": ws, "state": "archived"},
               {"id": "foreign", "workspace_id": "elsewhere", "state": "active"}]
    caps = TriageCapabilities(workstreams=lambda s, w: streams)
    client.app.state.triage_capabilities = caps
    effective = client.get(base + "/triage/goals").json()
    assert effective["source"] == "workstream" and len(effective["goals"]) == 1
    assert effective["goals"][0]["id"] == "workstream:injected"
    with db.session_scope() as s:
        stream = Workstream(workspace_id=ws, slug="stream", name="Stream", goal="Ship safely", owner_id=body["owner_id"], default_repo_ids=body["repo_ids"])
        s.add(stream)
        s.flush()
        stream_id = stream.id
    del client.app.state.triage_capabilities
    attached = client.patch(base + "/goals/" + goal["id"], json={"workstream_id": stream_id}).json()
    assert attached["id"] == goal["id"]
    assert client.get(base + "/triage/goals").json()["goals"][0]["id"] == goal["id"]
    with db.session_scope() as s:
        s.get(Workstream, stream_id).state = "archived"
    assert client.get(base + "/triage/goals").json()["goals"] == []


def test_archived_attached_goals_do_not_consume_active_limit(client, setup):
    ws, base, body = setup
    with db.session_scope() as s:
        stream = Workstream(workspace_id=ws, slug="stream", name="Stream", goal="Ship safely", owner_id=body["owner_id"], default_repo_ids=body["repo_ids"])
        s.add(stream)
        s.flush()
        for i in range(205):
            s.add(Goal(workspace_id=ws, workstream_id=stream.id, state="archived", **body))
    response = client.get(base + "/triage/goals")
    assert response.status_code == 200, response.text
    assert len(response.json()["goals"]) == 1
    assert response.json()["goals"][0]["id"].startswith("workstream:")


def test_admission_level_precedence_scope_and_unknowns(client, setup):
    ws, base, body = setup
    client.post(base + "/goals", json=body)
    policy = {"enabled": True, "repo_ids": [*body["repo_ids"], "foreign"], "max_urgency": 2}
    caps = TriageCapabilities(admission_policy=lambda s, w, g: policy)
    client.app.state.triage_capabilities = caps
    effective = client.get(base + "/triage/goals").json()["goals"][0]["effective_admission"]
    assert effective == {"enabled": True, "repo_ids": body["repo_ids"], "max_urgency": 2, "source": "admission-level"}
    for invalid in (None, "auto", "L99", {"level": "unknown"}, {"max_urgency": "NaN"}):
        policy = invalid
        effective = client.get(base + "/triage/goals").json()["goals"][0]["effective_admission"]
        assert not effective["enabled"] and effective["invalid"]
    goal = {**body, "admission": {"enabled": True, "repo_ids": body["repo_ids"], "max_urgency": 1}}
    assert effective_admission(goal)["enabled"]
    assert not effective_admission(goal, {"enabled": False}, installed=True)["enabled"]
    assert not effective_admission(goal, {"enabled": True, "repo_ids": ["foreign"]}, installed=True)["enabled"]
    del client.app.state.triage_capabilities
    # Existing execution autonomy is not an admission level.
    assert client.patch(base, json={"settings": {"autonomy": {"plan": "auto", "merge": "auto"}}}).status_code == 200
    assert not client.get(base + "/triage/goals").json()["goals"][0]["effective_admission"]["enabled"]


def test_capability_failure_is_closed(client, setup):
    _, base, _ = setup
    def broken(*args):
        raise RuntimeError("unavailable")
    client.app.state.triage_capabilities = TriageCapabilities(workstreams=broken)
    assert client.get(base + "/triage/goals").status_code == 503


def test_disconnect_prunes_scopes_archives_empty_and_keeps_editable(client, setup):
    ws, base, body = setup
    with db.session_scope() as s:
        repo = Repository(workspace_id=ws, full_name="acme/other")
        s.add(repo)
        s.flush()
        retained = repo.id
    admission = {"enabled": True, "repo_ids": body["repo_ids"], "max_urgency": 3}
    single = client.post(base + "/goals", json={**body, "admission": admission}).json()
    multi = client.post(base + "/goals", json={**body, "repo_ids": [*body["repo_ids"], retained], "admission": admission}).json()
    assert client.delete(base + "/repositories/" + body["repo_ids"][0]).status_code == 200
    goals = {g["id"]: g for g in client.get(base + "/goals?state=all").json()}
    assert goals[single["id"]]["state"] == "archived"
    assert goals[single["id"]]["repo_ids"] == []
    assert not goals[single["id"]]["admission"]["enabled"]
    assert goals[multi["id"]]["repo_ids"] == [retained]
    assert goals[multi["id"]]["admission"]["repo_ids"] == []
    for g in (single, multi):
        assert client.patch(base + "/goals/" + g["id"], json={"state": "archived"}).status_code == 200
    assert client.patch(base + "/goals/" + single["id"], json={"state": "active"}).status_code == 422
    assert client.patch(base + "/goals/" + single["id"], json={"state": "active", "repo_ids": [retained]}).status_code == 200


def test_settings_defaults_partial_updates_and_reload(client, setup):
    _, base, _ = setup
    config = client.get("/api/me").json()["workspaces"][0]["settings"]["triage"]
    assert config == {"enabled": True, "interval_seconds": 1800, "run_usd": 1, "day_usd": 10, "run_tokens": 100000, "max_turns": 20}
    patch = {"enabled": False, "interval_seconds": 3600, "run_usd": 0.5, "day_usd": 4, "run_tokens": 50000, "max_turns": 10}
    assert client.patch(base, json={"settings": {"triage": patch}}).status_code == 200
    assert client.get("/api/me").json()["workspaces"][0]["settings"]["triage"] == patch
    assert client.patch(base, json={"settings": {"triage": {"enabled": True}}}).status_code == 200
    # A new engine/app session reads the persisted values, not an in-memory cache.
    from aifactory.app import create_app
    with TestClient(create_app(start_orchestrator=False)) as reloaded:
        reloaded.post("/auth/dev", json={"email": "ian@example.com", "organization": "MindsDB"})
        assert reloaded.get("/api/me").json()["workspaces"][0]["settings"]["triage"] == {**patch, "enabled": True}


@pytest.mark.parametrize("patch", [
    {"run_usd": 0}, {"run_usd": -1}, {"run_usd": "NaN"}, {"day_usd": "Infinity"},
    {"day_usd": "2"}, {"run_usd": True}, {"interval_seconds": 0}, {"interval_seconds": 1.2},
    {"interval_seconds": True}, {"run_tokens": 6000001}, {"run_tokens": 0}, {"max_turns": 121},
    {"max_turns": 0}, {"enabled": "off"}, {"extra": 1}, {"day_usd": None},
])
def test_invalid_settings_rejected_without_mutation(client, setup, patch):
    _, base, _ = setup
    before = client.get("/api/me").json()["workspaces"][0]["settings"]
    assert client.patch(base, json={"settings": {"triage": patch}}).status_code == 422
    assert client.get("/api/me").json()["workspaces"][0]["settings"] == before


def test_actual_nonfinite_json_values_are_rejected(client, setup):
    _, base, _ = setup
    for key in ("run_usd", "day_usd"):
        for value in ("NaN", "Infinity", "-Infinity"):
            response = client.patch(base, content='{"settings":{"triage":{"' + key + '":' + value + '}}}', headers={"Content-Type": "application/json"})
            assert response.status_code == 422


def test_absent_optional_model_needs_no_import(client, setup, monkeypatch):
    from aifactory import goals
    _, base, body = setup
    created = client.post(base + "/goals", json=body).json()
    monkeypatch.setattr(goals, "_workstream_model", lambda: None)
    effective = client.get(base + "/triage/goals").json()
    assert effective["source"] == "workspace"
    assert effective["goals"][0]["id"] == created["id"]


def test_disconnect_rollback_keeps_goal_and_repository(client, setup, monkeypatch):
    from sqlalchemy.orm import Session
    _, base, body = setup
    created = client.post(base + "/goals", json=body).json()
    def fail(self):
        self.flush()
        raise RuntimeError("commit failed")
    with monkeypatch.context() as patch:
        patch.setattr(Session, "commit", fail)
        with pytest.raises(RuntimeError, match="commit failed"):
            client.delete(base + "/repositories/" + body["repo_ids"][0])
    assert client.get(base + "/goals").json() == [created]
    with db.session_scope() as s:
        assert s.get(Repository, body["repo_ids"][0]) is not None


def test_nonfinite_priority_json_rejected(client, setup):
    import json
    _, base, body = setup
    created = client.post(base + "/goals", json=body).json()
    for value in (float("nan"), float("inf"), float("-inf")):
        invalid = {**body, "admission": {"max_urgency": value}}
        assert client.post(base + "/goals", content=json.dumps(invalid), headers={"Content-Type": "application/json"}).status_code == 422
        assert client.patch(base + "/goals/" + created["id"], content=json.dumps({"admission": {"max_urgency": value}}), headers={"Content-Type": "application/json"}).status_code == 422
