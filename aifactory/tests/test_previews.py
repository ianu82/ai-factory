"""Durable preview identity, isolation and policy contracts (no VM required)."""
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
from threading import Barrier

import pytest
from fastapi import HTTPException
from pydantic import ValidationError
from sqlalchemy import inspect, select
from sqlalchemy.exc import IntegrityError

from aifactory import db
from aifactory.models import (Membership, Organization, Outcome, Preview, PreviewAllocation,
                             PreviewPolicy, PreviewUsageInterval, Repository, Run, Task, User, Workspace)
from aifactory.previews import FactoryPolicyUpdate, LocalPolicyUpdate, PreviewRequest, request_preview


@pytest.fixture
def identities():
    with db.session_scope() as s:
        org = Organization(name="test")
        s.add(org)
        s.flush()
        ws = Workspace(org_id=org.id, name="test", slug="test")
        other = Workspace(org_id=org.id, name="other", slug="other")
        s.add_all([ws, other])
        s.flush()
        repo = Repository(workspace_id=ws.id, full_name="org/repo")
        outcome = Outcome(workspace_id=ws.id, number=1, title="outcome")
        s.add_all([repo, outcome])
        s.flush()
        outcome.repo_ids = [repo.id]
        return ws.id, repo.id, outcome.id, other.id


def request(repo_id, outcome_id=None):
    return PreviewRequest(repository_id=None if outcome_id else repo_id,
                          pr_number=None if outcome_id else 7, outcome_id=outcome_id,
                          desired_manifest=[{"repository_id": repo_id, "commit_sha": "a" * 40}])


@pytest.mark.parametrize("kind", ["pr", "outcome"])
def test_concurrent_identity_and_no_delivery_work(identities, kind, monkeypatch):
    from aifactory import gateway

    def forbidden(*args, **kwargs):
        pytest.fail("Preview must not issue a gateway grant")

    monkeypatch.setattr(gateway, "issue", forbidden)
    grants = dict(gateway._grants)
    ws, repo, outcome, _ = identities
    barrier = Barrier(4)

    def create(_):
        with db.session_scope() as s:
            barrier.wait()
            p = request_preview(s, ws, request(repo, outcome if kind == "outcome" else None))
            return p.id, p.expires_at

    with ThreadPoolExecutor(max_workers=4) as pool:
        results = list(pool.map(create, range(4)))
    assert len(set(results)) == 1
    assert gateway._grants == grants
    with db.session_scope() as s:
        assert s.query(Preview).count() == 1
        p = s.get(Preview, results[0][0])
        assert p.state == "queued" and p.generation == 1
        assert p.expires_at - p.created_at == timedelta(hours=72)
        for model in (Run, Task):
            assert s.query(model).count() == 0
        duplicate = Preview(workspace_id=ws, repository_id=p.repository_id, pr_number=p.pr_number,
                            outcome_id=p.outcome_id, expires_at=p.expires_at)
        with pytest.raises(IntegrityError), s.begin_nested():
            s.add(duplicate)
            s.flush()


@pytest.mark.parametrize("field", ["active_cap", "idle_minutes", "lifetime_hours"])
@pytest.mark.parametrize("value", [0, -1, 1.5, float("nan"), float("inf"), -float("inf"), True, "5", None, 2**40])
def test_invalid_factory_policy(field, value):
    with pytest.raises(ValidationError):
        FactoryPolicyUpdate(**{field: value})


@pytest.mark.parametrize("value", [0, -1, 1.5, float("nan"), float("inf"), True, "5"])
def test_invalid_local_policy(value):
    with pytest.raises(ValidationError):
        LocalPolicyUpdate(idle_minutes=value)


def test_default_policy_and_authorization(client, signed_in, identities):
    ws = signed_in["workspaces"][0]["id"]
    path = f"/api/workspaces/{ws}/previews/policy"
    assert client.get(path).json() == {
        "factory": {"active_cap": 5, "idle_minutes": 30, "lifetime_hours": 72},
        "effective": {"active_cap": 5, "idle_minutes": 30, "lifetime_hours": 72},
        "overrides": {"idle_minutes": None, "lifetime_hours": None}}
    assert client.put(path + "/factory", json={"active_cap": 9}).status_code == 403
    assert client.put(path, json={"active_cap": 9}).status_code == 422
    for value in ("NaN", "Infinity", "-Infinity"):
        assert client.put(path, content='{"idle_minutes":' + value + '}',
                          headers={"Content-Type": "application/json"}).status_code == 422
    assert client.put(path, json={"idle_minutes": 31}).status_code == 422
    assert client.put(path, json={"lifetime_hours": 73}).status_code == 422
    assert client.put(path, json={"idle_minutes": 10, "lifetime_hours": 24}).json()["effective"] == {
        "active_cap": 5, "idle_minutes": 10, "lifetime_hours": 24}
    assert client.put(path, json={}).json()["effective"]["idle_minutes"] == 30
    other = identities[0]
    other_path = f"/api/workspaces/{other}/previews/policy"
    assert client.get(other_path).status_code == 404
    assert client.put(other_path, json={}).status_code == 404
    assert client.put(other_path + "/factory", json={}).status_code == 404
    with db.session_scope() as s:
        w = s.get(Workspace, ws)
        w.settings = {**w.settings, "_bootstrap_workspace": True}
    assert client.put(path + "/factory", json={"active_cap": 7}).status_code == 200
    with db.session_scope() as s:
        assert s.get(PreviewPolicy, 1).active_cap == 7
        from aifactory.models import Event
        event = s.scalars(select(Event).where(Event.kind == "preview_policy_changed").order_by(Event.created_at.desc())).first()
        assert event.actor != "factory" and event.data["scope"] == "factory"
        member = s.query(Membership).filter_by(workspace_id=ws).one()
        member.role = "member"
    assert client.put(path, json={}).status_code == 403
    assert client.put(path + "/factory", json={}).status_code == 403
    assert client.get(path).status_code == 200


def test_isolation(client, signed_in, identities):
    ws, repo, outcome, other = identities
    with db.session_scope() as s:
        p = request_preview(s, ws, request(repo))
        pid = p.id
    assert client.get(f"/api/workspaces/{ws}/previews/{pid}").status_code == 404
    own_ws = signed_in["workspaces"][0]["id"]
    assert client.get(f"/api/workspaces/{own_ws}/previews/{pid}").status_code == 404
    with db.session_scope() as s:
        user = s.query(User).filter_by(email="ian@example.com").one()
        s.add(Membership(workspace_id=ws, user_id=user.id, role="viewer"))
    result = client.get(f"/api/workspaces/{ws}/previews/{pid}")
    assert result.status_code == 200
    assert "vm_id" not in result.json() and "claim" not in result.json()
    with db.session_scope() as s:
        with pytest.raises(HTTPException) as exc:
            request_preview(s, other, request(repo))
        assert exc.value.status_code == 404


def test_expiry_immutable_and_usage_reopen(identities):
    ws, repo, _, _ = identities
    with db.session_scope() as s:
        p = request_preview(s, ws, request(repo))
        pid, expiry = p.id, p.expires_at
        allocation = PreviewAllocation(preview_id=pid, generation=1, provider="fake")
        s.add(allocation)
        s.flush()
        s.add(PreviewUsageInterval(allocation_id=allocation.id))
    url = db.engine().url.render_as_string(hide_password=False)
    db.engine().dispose()
    db.init_engine(url)
    with db.session_scope() as s:
        p = s.get(Preview, pid)
        p.state = "waking"
        p.generation += 1
        p.desired_manifest = [{"repository_id": repo, "commit_sha": "b" * 40}]
        assert request_preview(s, ws, request(repo)).expires_at == expiry
        assert s.query(PreviewUsageInterval).one().cost_usd is None
    with db.session_scope() as s:
        p = s.get(Preview, pid)
        assert p.state == "waking" and p.expires_at == expiry
        p.expires_at += timedelta(hours=1)
        with pytest.raises(ValueError, match="immutable"):
            s.flush()
        s.rollback()


def test_additive_schema_existing_database(identities):
    # Simulate the pre-preview schema, preserving populated legacy tables.
    tables = [PreviewUsageInterval.__table__, PreviewAllocation.__table__, Preview.__table__,
              PreviewPolicy.__table__]
    from aifactory.models import WorkspacePreviewPolicy
    tables.append(WorkspacePreviewPolicy.__table__)
    engine = db.engine()
    for table in tables:
        table.drop(engine)
    db.init_engine(engine.url.render_as_string(hide_password=False))
    assert "previews" in inspect(db.engine()).get_table_names()
    ws, repo, _, _ = identities
    with db.session_scope() as s:
        assert s.get(Workspace, ws) is not None
        request_preview(s, ws, request(repo))


def test_policy_persists_and_reductions_do_not_reset_expiry(client, signed_in):
    ws = signed_in["workspaces"][0]["id"]
    path = f"/api/workspaces/{ws}/previews/policy"
    with db.session_scope() as s:
        w = s.get(Workspace, ws)
        w.settings = {**w.settings, "_bootstrap_workspace": True}
        repo = Repository(workspace_id=ws, full_name="org/test")
        s.add(repo)
        s.flush()
        repo_id = repo.id
    assert client.put(path, json={"idle_minutes": 20, "lifetime_hours": 48}).status_code == 200
    with db.session_scope() as s:
        p = request_preview(s, ws, request(repo_id))
        pid, expiry = p.id, p.expires_at
        assert p.expires_at - p.created_at == timedelta(hours=48)
    assert client.put(path + "/factory", json={"active_cap": 2, "idle_minutes": 10,
                                               "lifetime_hours": 24}).status_code == 200
    url = db.engine().url.render_as_string(hide_password=False)
    db.engine().dispose()
    db.init_engine(url)
    policy = client.get(path).json()
    assert policy["effective"] == {"active_cap": 2, "idle_minutes": 10, "lifetime_hours": 24}
    assert policy["overrides"] == {"idle_minutes": 20, "lifetime_hours": 48}
    with db.session_scope() as s:
        assert s.get(Preview, pid).expires_at == expiry


@pytest.mark.parametrize("values", [
    {"repository_id": "r", "pr_number": None},
    {"repository_id": "r", "pr_number": 1, "outcome_id": "o"},
    {},
])
def test_invalid_identity_shape(identities, values):
    ws, repo, _, _ = identities
    with db.session_scope() as s:
        body = PreviewRequest(**values, desired_manifest=[{"repository_id": repo, "commit_sha": "a" * 40}])
        with pytest.raises(HTTPException) as exc:
            request_preview(s, ws, body)
        assert exc.value.status_code == 422


def test_unauthenticated_policy_and_record_access(client):
    assert client.get("/api/workspaces/unknown/previews/policy").status_code == 401
    assert client.put("/api/workspaces/unknown/previews/policy", json={}).status_code == 401
    assert client.put("/api/workspaces/unknown/previews/policy/factory", json={}).status_code == 401
    assert client.get("/api/workspaces/unknown/previews/unknown").status_code == 401


def test_generic_settings_cannot_grant_factory_authority(client, signed_in):
    ws = signed_in["workspaces"][0]["id"]
    assert client.patch(f"/api/workspaces/{ws}", json={"settings": {
        "_bootstrap_workspace": True, "preview_policy": {"active_cap": 999}}}).status_code == 200
    path = f"/api/workspaces/{ws}/previews/policy"
    assert client.get(path).json()["factory"]["active_cap"] == 5
    assert client.put(path + "/factory", json={"active_cap": 999}).status_code == 403


def test_concurrent_first_policy_updates_share_singleton(identities):
    from aifactory.previews import factory_policy

    barrier = Barrier(4)

    def write(value):
        with db.session_scope() as s:
            barrier.wait()
            policy = factory_policy(s, lock=True)
            policy.active_cap = value
            return policy.id

    with ThreadPoolExecutor(max_workers=4) as pool:
        assert list(pool.map(write, range(1, 5))) == [1] * 4
    with db.session_scope() as s:
        assert s.query(PreviewPolicy).count() == 1
        assert s.get(PreviewPolicy, 1).active_cap in range(1, 5)


@pytest.mark.parametrize("changes", [
    {"state": "unknown"}, {"generation": 0}, {"pr_number": 0},
    {"repository_id": None}, {"outcome_id": "outcome"},
])
def test_database_rejects_invalid_preview_records(identities, changes):
    ws, repo, _, _ = identities
    values = {"workspace_id": ws, "repository_id": repo, "pr_number": 1,
              "expires_at": db.utcnow() + timedelta(hours=72), **changes}
    with db.session_scope() as s:
        with pytest.raises(IntegrityError), s.begin_nested():
            s.add(Preview(**values))
            s.flush()


@pytest.mark.parametrize("field", ["idle_minutes", "lifetime_hours"])
@pytest.mark.parametrize("value", [0, -1, 1.5, float("nan"), float("inf"), True, "5", 2**40])
def test_local_policy_validates_both_overrides(field, value):
    with pytest.raises(ValidationError):
        LocalPolicyUpdate(**{field: value})


def test_manifest_and_association_validation(identities):
    ws, repo, outcome, _ = identities
    source = {"repository_id": repo, "commit_sha": "a" * 40}
    for sources in ([], [source] * 101, [{**source, "commit_sha": "main"}],
                    [{**source, "token": "not-allowed"}]):
        with pytest.raises(ValidationError):
            PreviewRequest(repository_id=repo, pr_number=1, desired_manifest=sources)
    with db.session_scope() as s:
        with pytest.raises(HTTPException, match="Duplicate manifest repository"):
            request_preview(s, ws, PreviewRequest(outcome_id=outcome,
                                                 desired_manifest=[source, source]))
        with pytest.raises(HTTPException, match="Task not found"):
            request_preview(s, ws, request(repo).model_copy(update={"task_id": "missing"}))


@pytest.mark.parametrize("state", ["queued", "waking", "starting", "running", "asleep", "stopped", "failed"])
def test_disconnect_refuses_retained_previews(client, signed_in, state):
    ws = signed_in["workspaces"][0]["id"]
    with db.session_scope() as s:
        repo = Repository(workspace_id=ws, full_name="org/preview")
        unrelated = Repository(workspace_id=ws, full_name="org/unrelated")
        s.add_all([repo, unrelated])
        s.flush()
        repo_id, unrelated_id = repo.id, unrelated.id
        preview = request_preview(s, ws, request(repo_id))
        preview.state = state
        preview_id, expiry = preview.id, preview.expires_at
        allocation = PreviewAllocation(preview_id=preview_id, generation=1, provider="fake")
        s.add(allocation)
        s.flush()
        usage = PreviewUsageInterval(allocation_id=allocation.id, cost_usd=1.25)
        s.add(usage)
        s.flush()
        usage_id = usage.id

    response = client.delete(f"/api/workspaces/{ws}/repositories/{repo_id}")
    assert response.status_code == 409, response.text
    detail = response.json()["detail"]
    assert preview_id in detail and f"PR #7, {state}" in detail
    assert "Stopping a preview does not remove" in detail
    with db.session_scope() as s:
        assert s.get(Repository, repo_id) is not None
        preview = s.get(Preview, preview_id)
        assert preview.state == state and preview.expires_at == expiry
        assert s.get(PreviewUsageInterval, usage_id).cost_usd == 1.25
    assert client.delete(f"/api/workspaces/{ws}/repositories/{unrelated_id}").status_code == 200


def test_disconnect_preview_diagnostic_is_bounded(client, signed_in):
    ws = signed_in["workspaces"][0]["id"]
    with db.session_scope() as s:
        repo = Repository(workspace_id=ws, full_name="org/many-previews")
        s.add(repo)
        s.flush()
        repo_id = repo.id
        for number in range(1, 23):
            request_preview(s, ws, request(repo_id).model_copy(update={"pr_number": number}))
    response = client.delete(f"/api/workspaces/{ws}/repositories/{repo_id}")
    assert response.status_code == 409
    assert response.json()["detail"].count("(PR #") == 20
    assert ", and more" in response.json()["detail"]


@pytest.mark.parametrize("kind", ["pr", "outcome"])
def test_disconnect_serializes_with_preview_creation(client, signed_in, kind):
    ws = signed_in["workspaces"][0]["id"]
    with db.session_scope() as s:
        repo = Repository(workspace_id=ws, full_name="org/racing-preview")
        outcome = Outcome(workspace_id=ws, number=1, title="racing outcome")
        s.add_all([repo, outcome])
        s.flush()
        repo_id, outcome_id = repo.id, outcome.id
        outcome.repo_ids = [repo_id]
    barrier = Barrier(2)

    def create():
        barrier.wait()
        try:
            with db.session_scope() as s:
                return request_preview(s, ws, request(repo_id, outcome_id if kind == "outcome" else None)).id
        except HTTPException as exc:
            assert exc.status_code == 404
            return None

    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(create)
        barrier.wait()
        response = client.delete(f"/api/workspaces/{ws}/repositories/{repo_id}")
        preview_id = future.result()
    with db.session_scope() as s:
        if preview_id is None:
            assert response.status_code == 200, response.text
            assert s.get(Repository, repo_id) is None
            assert s.query(Preview).count() == 0
        else:
            assert response.status_code == 409, response.text
            assert preview_id in response.json()["detail"]
            assert s.get(Repository, repo_id) is not None
            assert s.get(Preview, preview_id) is not None


@pytest.mark.parametrize("state", ["queued", "waking", "starting", "running", "asleep", "stopped", "failed"])
@pytest.mark.parametrize("manifest", ["desired_manifest", "running_manifest", "both"])
def test_disconnect_refuses_outcome_manifest_references(client, signed_in, state, manifest):
    ws = signed_in["workspaces"][0]["id"]
    with db.session_scope() as s:
        repo = Repository(workspace_id=ws, full_name="org/outcome-preview")
        unrelated = Repository(workspace_id=ws, full_name="org/unrelated")
        outcome = Outcome(workspace_id=ws, number=1, title="combined preview")
        s.add_all([repo, unrelated, outcome])
        s.flush()
        repo_id, unrelated_id, outcome_id = repo.id, unrelated.id, outcome.id
        outcome.repo_ids = [unrelated_id, repo_id]
        preview = request_preview(s, ws, request(repo_id, outcome_id))
        source = preview.desired_manifest
        # Put the target second in a multi-repository manifest, and keep the other
        # manifest unrelated (or null) to prove either reference independently blocks.
        other_source = [{"repository_id": unrelated_id, "commit_sha": "b" * 40}]
        preview.desired_manifest = other_source + source if manifest != "running_manifest" else other_source
        if manifest != "desired_manifest":
            preview.running_manifest = other_source + source
        preview.state = state
        preview_id, expiry = preview.id, preview.expires_at
        allocation = PreviewAllocation(preview_id=preview_id, generation=1, provider="fake")
        s.add(allocation)
        s.flush()
        usage = PreviewUsageInterval(allocation_id=allocation.id, cost_usd=1.25)
        s.add(usage)
        s.flush()
        usage_id = usage.id

    response = client.delete(f"/api/workspaces/{ws}/repositories/{repo_id}")
    assert response.status_code == 409, response.text
    detail = response.json()["detail"]
    assert preview_id in detail and f"outcome {outcome_id}, {state}" in detail
    assert "Stopping a preview does not remove" in detail
    with db.session_scope() as s:
        assert s.get(Repository, repo_id) is not None
        preview = s.get(Preview, preview_id)
        assert preview.repository_id is None
        assert preview.state == state and preview.expires_at == expiry
        assert s.get(PreviewUsageInterval, usage_id).cost_usd == 1.25


def test_disconnect_ignores_unrelated_outcome_manifests(client, signed_in):
    ws = signed_in["workspaces"][0]["id"]
    with db.session_scope() as s:
        repo = Repository(workspace_id=ws, full_name="org/disconnect")
        unrelated = Repository(workspace_id=ws, full_name="org/retained")
        outcome = Outcome(workspace_id=ws, number=1, title="unrelated preview")
        s.add_all([repo, unrelated, outcome])
        s.flush()
        repo_id, unrelated_id = repo.id, unrelated.id
        outcome.repo_ids = [unrelated_id]
        preview = request_preview(s, ws, request(unrelated_id, outcome.id))
        preview.running_manifest = preview.desired_manifest
        preview_id = preview.id
    assert client.delete(f"/api/workspaces/{ws}/repositories/{repo_id}").status_code == 200
    with db.session_scope() as s:
        assert s.get(Repository, repo_id) is None
        assert s.get(Preview, preview_id) is not None


def test_disconnect_outcome_diagnostic_is_bounded(client, signed_in):
    ws = signed_in["workspaces"][0]["id"]
    with db.session_scope() as s:
        repo = Repository(workspace_id=ws, full_name="org/many-outcome-previews")
        s.add(repo)
        s.flush()
        repo_id = repo.id
        for number in range(1, 23):
            outcome = Outcome(workspace_id=ws, number=number, title=f"outcome {number}", repo_ids=[repo_id])
            s.add(outcome)
            s.flush()
            preview = request_preview(s, ws, request(repo_id, outcome.id))
            preview.running_manifest = preview.desired_manifest
    response = client.delete(f"/api/workspaces/{ws}/repositories/{repo_id}")
    assert response.status_code == 409
    assert response.json()["detail"].count("(outcome ") == 20
    assert ", and more" in response.json()["detail"]
