"""Strict, member-authorized priority edits with atomic audit and post-commit publication."""
from __future__ import annotations

import pytest
from sqlalchemy.orm import Session

from aifactory import api, events
from aifactory.db import session_scope
from aifactory.models import Decision, Event, Membership, Outcome, Plan, Repository, Run, Task, User, Workspace


@pytest.fixture
def outcome(signed_in):
    ws = signed_in["workspaces"][0]["id"]
    with session_scope() as s:
        o = Outcome(workspace_id=ws, number=19, title="Priority test", priority=2, status="running",
                    created_by=signed_in["user"]["id"])
        s.add(o)
        s.flush()
        repo = Repository(workspace_id=ws, full_name="test/app")
        plan = Plan(outcome_id=o.id)
        s.add_all([repo, plan])
        s.flush()
        s.add_all([
            Task(workspace_id=ws, outcome_id=o.id, plan_id=plan.id, repo_id=repo.id, key="T1",
                 title="Done", status="completed", pr_number=7, pr_url="https://github.com/test/app/pull/7"),
            Task(workspace_id=ws, outcome_id=o.id, plan_id=plan.id, repo_id=repo.id, key="T2",
                 title="Active", status="running"),
            Run(workspace_id=ws, outcome_id=o.id, role="builder", harness="native", cost_usd=1.25,
                cost_known=False),
            Decision(workspace_id=ws, outcome_id=o.id, kind="question", title="Human input"),
        ])
        return ws, o.id


def path(outcome):
    ws, oid = outcome
    return f"/api/workspaces/{ws}/outcomes/{oid}"


def audits():
    with session_scope() as s:
        return [events.event_dict(e) for e in s.query(Event).filter_by(kind="outcome_priority_changed").order_by(Event.id)]


@pytest.mark.parametrize("role", ["member", "admin", "owner", "viewer"])
@pytest.mark.parametrize("priority", range(4))
def test_roles_and_priorities(client, signed_in, outcome, role, priority):
    ws, oid = outcome
    with session_scope() as s:
        s.query(Membership).filter_by(workspace_id=ws, user_id=signed_in["user"]["id"]).one().role = role
    response = client.patch(path(outcome), json={"priority": priority})
    assert response.status_code == (403 if role == "viewer" else 200), response.text
    expected = 2 if role == "viewer" else priority
    with session_scope() as s:
        assert s.get(Outcome, oid).priority == expected
    assert len(audits()) == int(role != "viewer" and priority != 2)
    if role != "viewer":
        assert response.json()["priority"] == priority


def test_signed_out_is_refused(client, outcome):
    client.post("/auth/logout")
    client.cookies.clear()
    assert client.patch(path(outcome), json={"priority": 0}).status_code == 401
    with session_scope() as s:
        assert s.get(Outcome, outcome[1]).priority == 2
    assert audits() == []


@pytest.mark.parametrize("body", [
    {}, {"priority": None}, {"priority": True}, {"priority": False}, {"priority": 1.5},
    {"priority": 1.0}, {"priority": "0"}, {"priority": -1}, {"priority": 4},
    {"priority": []}, {"priority": {}}, None,
])
def test_invalid_priority_does_not_mutate_or_record(client, outcome, body):
    before = client.get(path(outcome)).json()["outcome"]
    assert client.patch(path(outcome), json=body).status_code == 422
    assert client.get(path(outcome)).json()["outcome"] == before
    assert audits() == []


def test_workspace_isolation_and_missing_outcomes(client, signed_in, outcome):
    ws, oid = outcome
    with session_scope() as s:
        org_id = s.get(Workspace, ws).org_id
        foreign = Workspace(org_id=org_id, name="Foreign", slug="foreign")
        s.add(foreign)
        s.flush()
        foreign_id = foreign.id
        s.add(Membership(workspace_id=foreign_id, user_id=signed_in["user"]["id"], role="member"))
        other = Outcome(workspace_id=foreign_id, number=1, title="Private", priority=3)
        s.add(other)
        s.flush()
        other_id = other.id
        s.query(Membership).filter_by(workspace_id=ws, user_id=signed_in["user"]["id"]).one().role = "member"
    for workspace_id, outcome_id in [(ws, other_id), (foreign_id, oid), (ws, "missing")]:
        response = client.patch(f"/api/workspaces/{workspace_id}/outcomes/{outcome_id}", json={"priority": 0})
        assert response.status_code == 404, response.text
    # No membership must not reveal whether the outcome exists.
    with session_scope() as s:
        s.query(Membership).filter_by(workspace_id=foreign_id).delete()
    assert client.patch(f"/api/workspaces/{foreign_id}/outcomes/{other_id}", json={"priority": 0}).status_code == 404
    assert audits() == []
    with session_scope() as s:
        assert s.get(Outcome, oid).priority == 2
        assert s.get(Outcome, other_id).priority == 3


@pytest.mark.parametrize("name", ["Ian", ""])
def test_persistence_aggregates_attribution_and_noop(client, signed_in, outcome, monkeypatch, name):
    ws, oid = outcome
    actor = signed_in["user"]["id"]
    with session_scope() as s:
        s.get(User, actor).name = name
    before = client.get(path(outcome)).json()["outcome"]
    published, wakes = [], []
    monkeypatch.setattr(events.bus, "publish", published.append)
    monkeypatch.setattr(api.orchestrator, "wake", lambda: wakes.append(True))
    response = client.patch(path(outcome), json={"priority": 0})
    assert response.status_code == 200, response.text
    updated = response.json()
    assert updated["priority"] == 0
    assert updated["status"] == "running"
    assert updated["progress"] == {"done": 1, "total": 2, "started": 2, "active": 1}
    assert updated["cost_usd"] == 1.25 and updated["cost_known"] is False
    assert updated["open_decisions"] == 1
    for key in ("progress", "cost_usd", "cost_known", "open_decisions", "prs", "track"):
        assert updated[key] == before[key]
    assert client.get(f"/api/workspaces/{ws}/outcomes").json() == [updated]
    detail = client.get(path(outcome)).json()
    assert detail["outcome"] == updated | {"created_by_name": name or "ian@example.com"}
    assert [t["status"] for t in detail["tasks"]] == ["completed", "running"]
    event, = audits()
    assert event["workspace_id"] == ws and event["outcome_id"] == oid
    assert event["actor"] == actor
    assert event["message"] == f"{name or 'ian@example.com'} changed priority of #19 from 2 to 0"
    assert event["data"] == {"from": 2, "to": 0}
    assert all(type(value) is int for value in event["data"].values())
    assert published == [{"type": "event", **event},
                         {"type": "changed", "workspace_id": ws, "entity": "outcome", "id": oid}]
    assert wakes == [True]
    replay = client.patch(path(outcome), json={"priority": 0})
    assert replay.status_code == 200 and replay.json() == updated
    assert audits() == [event]
    assert len(published) == 2 and wakes == [True]


@pytest.mark.parametrize("failure", ["audit", "commit"])
def test_failure_rolls_back_priority_and_audit_without_publication(client, outcome, monkeypatch, failure):
    published, wakes = [], []
    monkeypatch.setattr(events.bus, "publish", published.append)
    monkeypatch.setattr(api.orchestrator, "wake", lambda: wakes.append(True))

    def fail(*args, **kwargs):
        raise RuntimeError("injected failure")

    with monkeypatch.context() as patch:
        if failure == "audit":
            real_record = api.record

            def fail_after_record(*args, **kwargs):
                real_record(*args, **kwargs)
                fail()

            patch.setattr(api, "record", fail_after_record)
        else:
            patch.setattr(Session, "commit", fail)
        with pytest.raises(RuntimeError, match="injected failure"):
            client.patch(path(outcome), json={"priority": 0})
    with session_scope() as s:
        assert s.get(Outcome, outcome[1]).priority == 2
    assert audits() == []
    assert published == [] and wakes == []


def test_publication_and_wake_happen_after_commit(client, outcome, monkeypatch):
    calls = []

    def check_committed(kind):
        with session_scope() as s:
            assert s.get(Outcome, outcome[1]).priority == 0
            assert s.query(Event).filter_by(kind="outcome_priority_changed").count() == 1
        calls.append(kind)

    monkeypatch.setattr(events.bus, "publish", lambda payload: check_committed(payload["type"]))
    monkeypatch.setattr(api.orchestrator, "wake", lambda: check_committed("wake"))
    assert client.patch(path(outcome), json={"priority": 0}).status_code == 200
    assert calls == ["event", "changed", "wake"]
