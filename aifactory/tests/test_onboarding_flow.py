"""API journey for the final guided steps and lost-response creation retries."""
from __future__ import annotations

from copy import deepcopy
from types import SimpleNamespace
from uuid import uuid4

import pytest

from aifactory import db
from aifactory.models import Membership, Outcome, Repository, User, Workspace
from aifactory.pipeline.delivery import TaskRunner


def setup_repo(workspace_id):
    with db.session_scope() as s:
        repo = Repository(workspace_id=workspace_id, full_name="acme/app", config={
            "detected": ["Python"], "checks": ["pytest"], "release": {"mode": "on_merge"}})
        s.add(repo)
        s.flush()
        return repo.id


@pytest.mark.parametrize("plan,merge", [("approval", "approval"), ("auto", "approval"), ("auto", "auto")])
def test_guided_final_steps(client, signed_in, plan, merge):
    wid = signed_in["workspaces"][0]["id"]
    uid = signed_in["user"]["id"]
    base = f"/api/workspaces/{wid}"
    rid = setup_repo(wid)
    with db.session_scope() as s:
        ws = s.get(Workspace, wid)
        ws.settings = {**ws.settings, "budgets": {"run_usd": 7, "run_tokens": 12345, "outcome_usd": 25},
                       "custom_setting": {"keep": True}}
        before = deepcopy(ws.settings)
        colleague = User(name="Colleague", email="colleague@example.test")
        s.add(colleague)
        s.flush()
        colleague_id = colleague.id
        s.add(Membership(workspace_id=wid, user_id=colleague_id, role="member"))
    policy = {"plan": plan, "merge": merge, "release": "approval"}
    budgets = {**before["budgets"], "outcome_usd": 42.5}
    response = client.patch(base, json={"settings": {"autonomy": policy, "budgets": budgets}})
    assert response.status_code == 200
    settings = response.json()["settings"]
    assert settings["autonomy"] == {"plan": "approval", "merge": "approval", "release": "approval"}
    assert settings["learning"]["preset"] == policy
    assert settings["budgets"] == budgets
    for key, value in before.items():
        if key not in ("autonomy", "budgets", "learning"):
            assert settings[key] == value
    # Exercise the actual delivery policy, not a copy of its logic.
    runtime = SimpleNamespace(cfg={"release": {"mode": "on_merge"}}, ws=wid)
    assert TaskRunner._merge_needs_approval(runtime) is True
    runtime.cfg = {"release": {"mode": "none"}}
    assert TaskRunner._merge_needs_approval(runtime) is True  # learning overrides the desired preset
    for step in ("autonomy", "team"):
        assert client.patch(base + "/onboarding", json={"action": "complete", "step": step, "acknowledged": True}).status_code == 200
    members = client.get(base + "/members").json()
    assert {m["user"]["id"] for m in members} == {uid, colleague_id}
    assert client.patch(base + f"/members/{colleague_id}", json={"role": "viewer"}).status_code == 200
    with db.session_scope() as s:
        s.query(Membership).filter_by(workspace_id=wid, user_id=uid).one().role = "admin"
    assert client.patch(base + f"/members/{colleague_id}", json={"role": "owner"}).status_code == 400
    assert client.patch(base + f"/members/{colleague_id}", json={"role": "member"}).status_code == 200
    draft = {"title": "Document getting started", "brief": "Verify setup instructions", "repo_ids": [rid], "request_id": str(uuid4())}
    first = client.post(base + "/outcomes", json=draft)
    assert first.status_code == 200
    oid = first.json()["id"]
    # Simulate losing the create response: retry the same draft before saving progress.
    retry = client.post(base + "/outcomes", json=draft)
    assert retry.status_code == 200
    assert retry.json()["id"] == oid
    assert [o["id"] for o in client.get(base + "/outcomes").json()] == [oid]
    with db.session_scope() as s:
        outcome = s.get(Outcome, oid)
        assert outcome.created_by == uid
        assert outcome.repo_ids == [rid]
        assert outcome.signal_id.startswith("request:")
        assert len(outcome.signal_id) <= 32
    # Reload recovery can find the outcome even before progress is recorded.
    assert client.get(base + "/onboarding").json()["first_outcome_id"] is None
    assert client.get(base + f"/outcomes/{oid}").status_code == 200
    completed = client.patch(base + "/onboarding", json={"action": "complete", "step": "outcome", "first_outcome_id": oid})
    assert completed.status_code == 200
    for _ in range(2):
        state = client.get(base + "/onboarding").json()
        assert state["first_outcome_id"] == oid
        assert {"autonomy", "team", "outcome"} <= set(state["completed_steps"])
    with db.session_scope() as s:
        s.query(Membership).filter_by(workspace_id=wid, user_id=uid).one().role = "member"
    assert client.get(base + "/members").status_code == 200
    assert client.patch(base + f"/members/{colleague_id}", json={"role": "viewer"}).status_code == 403
    assert client.patch(base + "/onboarding", json={"action": "complete", "step": "team", "acknowledged": True}).status_code == 403


def test_request_id_scoped_to_user_and_workspace(client, signed_in):
    wid = signed_in["workspaces"][0]["id"]
    uid = signed_in["user"]["id"]
    rid = setup_repo(wid)
    request_id = str(uuid4())
    body = {"title": "First outcome", "repo_ids": [rid], "request_id": request_id}
    base = f"/api/workspaces/{wid}"
    first = client.post(base + "/outcomes", json=body).json()["id"]
    with db.session_scope() as s:
        ws = s.get(Workspace, wid)
        other = Workspace(org_id=ws.org_id, name="Other", slug="other")
        s.add(other)
        s.flush()
        other_id = other.id
        s.add(Membership(workspace_id=other_id, user_id=uid, role="owner"))
    other_repo = setup_repo(other_id)
    second = client.post(f"/api/workspaces/{other_id}/outcomes", json={**body, "repo_ids": [other_repo]})
    assert second.status_code == 200
    assert second.json()["id"] != first
    assert client.post("/auth/logout").status_code == 200
    assert client.post("/auth/dev", json={"email": "other@example.com", "name": "Other", "organization": "MindsDB"}).status_code == 200
    third = client.post(base + "/outcomes", json=body)
    assert third.status_code == 200
    assert third.json()["id"] not in {first, second.json()["id"]}
    assert client.post(base + "/outcomes", json=body).json()["id"] == third.json()["id"]
    assert client.post(base + "/outcomes", json={**body, "request_id": "invalid"}).status_code == 422
