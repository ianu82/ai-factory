"""Supervisor HTTP and Mind reads use the same durable Inbox decisions on either DB."""
from datetime import datetime, timedelta

import pytest

from aifactory import api as api_module
from aifactory import serializers as S
from aifactory.db import session_scope
from aifactory.mind.tools import Context, Tools, read_call
from aifactory.models import Decision, Event, Membership, Organization, Signal, Workspace
from aifactory.supervisor import Finding, surface, sweep
from aifactory.supervisor_policy import supervisor_policy

NOW = datetime(2026, 11, 12, 2, 40)


def test_signed_out_supervisor_routes(client):
    assert client.get("/api/workspaces/unknown/supervisor").status_code == 401
    assert client.post("/api/workspaces/unknown/supervisor/run").status_code == 401


def set_role(me, role):
    ws = me["workspaces"][0]["id"]
    with session_scope() as db:
        db.query(Membership).filter_by(workspace_id=ws, user_id=me["user"]["id"]).one().role = role
    return ws


@pytest.mark.parametrize("role", ["member", "viewer"])
def test_members_read_but_cannot_run(client, signed_in, role):
    ws = set_role(signed_in, role)
    r = client.get(f"/api/workspaces/{ws}/supervisor")
    assert r.status_code == 200, r.text
    assert r.json() == {"policy": supervisor_policy(), "last_sweep_at": None, "counts": {},
                        "findings": [], "next_offset": None}
    assert client.post(f"/api/workspaces/{ws}/supervisor/run").status_code == 403
    with session_scope() as db:
        assert db.query(Event).filter_by(workspace_id=ws, kind="supervisor_sweep").count() == 0


@pytest.mark.parametrize("role", ["admin", "owner"])
def test_admin_runs_synchronously_and_reads_persisted_state(client, signed_in, monkeypatch, role):
    ws = set_role(signed_in, role)
    monkeypatch.setattr(api_module, "utcnow", lambda: NOW)
    with session_scope() as db:
        db.add(Signal(workspace_id=ws, source="smoke_failure", title="Smoke failed after merging",
                      fingerprint="smoke", status="new", first_seen_at=NOW - timedelta(hours=30)))
    path = f"/api/workspaces/{ws}/supervisor"
    r = client.post(path + "/run")
    assert r.status_code == 200, r.text
    data = r.json()
    assert data["last_sweep_at"] == NOW.isoformat() + "+00:00"
    assert data["counts"] == {"untriaged_smoke": 1}
    assert len(data["findings"]) == 1
    finding = data["findings"][0]
    assert finding["finding_kind"] == "untriaged_smoke"
    assert finding["confidence"] == "observed"
    # Smoke triage uses the existing Signals actions, not the generic Supervisor choices.
    assert [(o["value"], o["label"]) for o in finding["options"]] == [
        ("fix", "Create the fix"), ("dismiss", "Ignore")]
    assert all(o["effect"] for o in finding["options"])
    with session_scope() as db:
        decision = db.get(Decision, finding["id"])
        assert all(finding[k] == v for k, v in S.decision(decision).items())
    assert client.get(path).json() == data
    assert client.post(path + "/run").json() == data  # durable deduplication
    inbox = client.get(f"/api/workspaces/{ws}/inbox").json()
    assert any(d["id"] == finding["id"] and d["kind"] == "supervisor" for d in inbox["decisions"])


@pytest.mark.parametrize("resolution,status", [("acknowledge", "resolved"), ("dismiss", "dismissed")])
def test_existing_resolve_flow(client, signed_in, resolution, status):
    ws = set_role(signed_in, "member")
    decision_id = surface(ws, Finding("main_red", "repo", "Main failing", "Check CI", {}))
    finding = client.get(f"/api/workspaces/{ws}/supervisor").json()["findings"][0]
    assert finding["options"] == [{"value": "acknowledge", "label": "Acknowledge"},
                                  {"value": "dismiss", "label": "Dismiss"}]
    result = client.post(f"/api/workspaces/{ws}/decisions/{decision_id}/resolve", json={"resolution": resolution})
    assert result.status_code == 200, result.text
    assert client.get(f"/api/workspaces/{ws}/supervisor").json()["findings"] == []
    with session_scope() as db:
        assert db.get(Decision, decision_id).status == status


def test_status_scope_and_pagination(client, signed_in):
    ws = signed_in["workspaces"][0]["id"]
    with session_scope() as db:
        org = Organization(name="Other")
        db.add(org)
        db.flush()
        other = Workspace(org_id=org.id, name="Other", slug="other")
        db.add(other)
        db.flush()
        other_id = other.id
        db.add(Decision(workspace_id=other.id, kind="supervisor", title="Private finding"))
        for i in range(3):
            db.add(Decision(workspace_id=ws, kind="supervisor", title=str(i), created_at=NOW + timedelta(seconds=i)))
        db.add_all([Decision(workspace_id=ws, kind="supervisor", status="resolved", title="Resolved"),
                    Decision(workspace_id=ws, kind="question", title="Question")])
    path = f"/api/workspaces/{ws}/supervisor"
    first = client.get(path + "?limit=2").json()
    second = client.get(path + f"?limit=2&offset={first['next_offset']}").json()
    assert [d["title"] for d in first["findings"] + second["findings"]] == ["0", "1", "2"]
    assert second["next_offset"] is None
    assert client.get(path + "?limit=201").status_code == 422
    assert client.get(path + "?offset=-1").status_code == 422
    assert client.get(f"/api/workspaces/{other_id}/supervisor").status_code == 404
    assert client.post(f"/api/workspaces/{other_id}/supervisor/run").status_code == 404


def test_policy_saves_and_run_obeys_off_and_pause(client, signed_in, monkeypatch):
    ws = signed_in["workspaces"][0]["id"]
    monkeypatch.setattr(api_module, "utcnow", lambda: NOW)
    policy = {"mode": "fix", "interval_minutes": 5, "stuck_minutes": 10, "unaccepted_issue_hours": 2}
    response = client.patch(f"/api/workspaces/{ws}", json={"settings": {"supervisor": policy}})
    assert response.status_code == 200, response.text
    assert response.json()["settings"]["supervisor"] == policy
    path = f"/api/workspaces/{ws}/supervisor"
    assert client.get(path).json()["policy"] == policy
    assert client.patch(f"/api/workspaces/{ws}", json={"settings": {"supervisor": {"mode": "off"}}}).status_code == 200
    assert client.post(path + "/run").json()["last_sweep_at"] == NOW.isoformat() + "+00:00"
    assert client.patch(f"/api/workspaces/{ws}", json={"paused": True}).status_code == 200
    monkeypatch.setattr(api_module, "utcnow", lambda: NOW + timedelta(hours=1))
    assert client.post(path + "/run").json()["last_sweep_at"] == NOW.isoformat() + "+00:00"


@pytest.mark.parametrize("patch,message", [
    ({"mode": "auto"}, "supervisor.mode must be off, observe or fix"),
    ({"interval_minutes": 4}, "supervisor.interval_minutes must be an integer >= 5"),
    ({"stuck_minutes": 0}, "supervisor.stuck_minutes must be an integer >= 1"),
    ({"unaccepted_issue_hours": 1.5}, "supervisor.unaccepted_issue_hours must be an integer >= 1"),
    ({"stuck_minutes": True}, "supervisor.stuck_minutes must be an integer >= 1"),
])
def test_policy_validation_returns_422(client, signed_in, patch, message):
    ws = signed_in["workspaces"][0]["id"]
    response = client.patch(f"/api/workspaces/{ws}", json={"settings": {"supervisor": patch}})
    assert response.status_code == 422
    assert response.json()["detail"] == message
    assert client.get(f"/api/workspaces/{ws}/supervisor").json()["policy"] == supervisor_policy()


def test_legacy_settings_use_sweep_journal(client, signed_in):
    ws = signed_in["workspaces"][0]["id"]
    with session_scope() as db:
        db.get(Workspace, ws).settings = ["legacy"]
    sweep(ws, NOW)
    status = client.get(f"/api/workspaces/{ws}/supervisor").json()
    assert status["policy"] == supervisor_policy()
    assert status["last_sweep_at"] == NOW.isoformat() + "+00:00"


def test_mind_tool_is_listed_scoped_bounded_and_cites_open_findings(client, signed_in):
    ws = signed_in["workspaces"][0]["id"]
    secret = "sk-" + "s" * 40
    finding_id = surface(ws, Finding("main_red", "repo", "Main failing", secret, {}))
    with session_scope() as db:
        db.add_all([Decision(workspace_id=ws, kind="supervisor", status="dismissed", title="Dismissed"),
                    Decision(workspace_id=ws, kind="question", title="Question")])
        context = Context(db, signed_in["user"]["id"])
        assert "supervisor_findings" in {d["function"]["name"] for d in Tools(context).definitions()}
        result = read_call("supervisor_findings", {"workspace_id": ws, "limit": 1}, context)
        assert [d["id"] for d in result["items"]] == [finding_id]
        assert result["items"][0]["kind"] == "supervisor"
        assert secret not in result["items"][0]["body"]
        assert not result["truncated"]
        assert result["citations"][0]["href"] == f"/w/{ws}/inbox"
        assert result["citations"][0]["id"] == finding_id
        db.add(Decision(workspace_id=ws, kind="supervisor", title="Another"))
        db.flush()
        assert read_call("supervisor_findings", {"workspace_id": ws, "limit": 1}, context)["truncated"]


def test_inferred_status_never_claims_observed_evidence(client, signed_in):
    ws = signed_in["workspaces"][0]["id"]
    finding = Finding("combined_break", "repo", "Combined break", "Possible interaction", {}, confidence="inferred")
    decision_id = surface(ws, finding)
    with session_scope() as db:
        db.get(Workspace, ws).settings = {"supervisor_state": {"last_sweep_at": NOW.isoformat(), "findings": [
            {"finding_kind": finding.kind, "fingerprint": finding.fingerprint, "confidence": finding.confidence}]}}
    status = client.get(f"/api/workspaces/{ws}/supervisor").json()
    assert status["counts"] == {"combined_break": 1}
    assert status["findings"][0]["id"] == decision_id
    assert status["findings"][0]["confidence"] == "inferred"
    assert "suspected" in status["findings"][0]["body"]
    with session_scope() as db:
        db.get(Workspace, ws).settings = {}
    assert client.get(f"/api/workspaces/{ws}/supervisor").json()["findings"][0]["confidence"] is None


def test_supervisor_settings_require_admin(client, signed_in):
    ws = set_role(signed_in, "member")
    response = client.patch(f"/api/workspaces/{ws}", json={"settings": {"supervisor": {"mode": "fix"}}})
    assert response.status_code == 403
    assert client.get(f"/api/workspaces/{ws}/supervisor").json()["policy"]["mode"] == "observe"
