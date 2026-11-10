"""Effective settings, provenance, cautious truth tables and API validation."""
from itertools import product

import pytest

from aifactory.db import session_scope
from aifactory.models import Outcome, Repository, Workspace, Workstream
from aifactory.outcome_settings import resolve_settings


@pytest.fixture
def configured(client, signed_in):
    ws = signed_in["workspaces"][0]["id"]
    with session_scope() as s:
        workspace = s.get(Workspace, ws)
        workspace.settings = {"harness": {"planner": "anton", "builder": "codex", "verifier": "native"},
                              "budgets": {"outcome_usd": 40, "run_usd": 7, "run_tokens": 123},
                              "autonomy": {"plan": "auto", "merge": "auto", "release": "approval"}}
        repos = [Repository(workspace_id=ws, full_name=f"acme/r{i}") for i in range(2)]
        s.add_all(repos)
        s.flush()
        ids = [r.id for r in repos]
    root = f"/api/workspaces/{ws}"
    stream = client.post(root + "/workstreams", json={"name": "Policy", "goal": "Safe delivery"}).json()
    with session_scope() as s:
        o = Outcome(workspace_id=ws, workstream_id=stream["id"], number=1, title="Test", repo_ids=ids, priority=3)
        s.add(o)
        s.flush()
        oid = o.id
    return root, stream["id"], oid, ids


def detail(client, configured):
    root, _, oid, _ = configured
    response = client.get(root + f"/outcomes/{oid}")
    assert response.status_code == 200, response.text
    return response.json()["settings"]


@pytest.mark.parametrize("budget", [1.25, 100])
@pytest.mark.parametrize("explicit,stream_worker,expected", [
    (None, None, {"planner": "anton", "builder": "codex", "verifier": "native"}),
    (None, "anton", dict.fromkeys(("planner", "builder", "verifier"), "anton")),
    ("native", "codex", dict.fromkeys(("planner", "builder", "verifier"), "native")),
])
def test_api_overrides_and_clearing(client, configured, budget, explicit, stream_worker, expected):
    root, sid, oid, _ = configured
    overrides = {"worker_override": stream_worker, "priority_override": 0, "outcome_budget_override": budget,
                 "plan_approval_override": "approval", "merge_approval_override": "approval"}
    assert client.patch(root + f"/workstreams/{sid}", json=overrides).status_code == 200
    with session_scope() as s:
        s.get(Outcome, oid).harness = explicit
    resolved = detail(client, configured)
    effective, sources = resolved["effective"], resolved["sources"]
    assert effective["harness"] == expected
    assert effective["priority"] == 0
    assert effective["budgets"] == {"outcome_usd": budget, "run_usd": 7, "run_tokens": 123}
    assert effective["autonomy"]["plan"] == effective["autonomy"]["merge"] == "approval"
    assert sources["priority"] == [{"scope": "workstream", "id": sid, "field": "priority_override", "value": 0}]
    assert sources["budgets"]["outcome_usd"][0]["id"] == sid
    scope = "outcome" if explicit else "workstream" if stream_worker else "workspace"
    assert all(v[0]["scope"] == scope for v in sources["harness"].values())
    assert client.patch(root + f"/workstreams/{sid}", json=dict.fromkeys(overrides)).status_code == 200
    cleared = detail(client, configured)["effective"]
    assert cleared["priority"] == 3
    assert cleared["budgets"]["outcome_usd"] == 40
    assert cleared["autonomy"]["plan"] == cleared["autonomy"]["merge"] == "auto"
    assert cleared["harness"] == (dict.fromkeys(expected, explicit) if explicit else
                                   {"planner": "anton", "builder": "codex", "verifier": "native"})
    with session_scope() as s:
        assert s.get(Outcome, oid).priority == 3
        assert s.get(Workspace, root.split("/")[-1]).settings["budgets"]["outcome_usd"] == 40


@pytest.mark.parametrize("kind", ["plan", "merge"])
@pytest.mark.parametrize("policies", list(product(("auto", "approval"), repeat=4)))
def test_cautious_truth_table(client, configured, kind, policies):
    root, sid, oid, ids = configured
    ws_mode, stream_mode, first, second = policies
    with session_scope() as s:
        ws = s.get(Workspace, root.split("/")[-1])
        ws.settings = {**ws.settings, "autonomy": {kind: ws_mode}}
        setattr(s.get(Workstream, sid), f"{kind}_approval_override", stream_mode)
        for rid, mode in zip(ids, (first, second)):
            s.get(Repository, rid).config = {kind: {"mode": mode}}
    resolved = detail(client, configured)
    expected = "approval" if "approval" in policies else "auto"
    assert resolved["effective"]["autonomy"][kind] == expected
    sources = resolved["sources"]["autonomy"][kind]
    assert len(sources) == (policies.count("approval") or 4)
    assert all(p["value"] == expected for p in sources)
    if kind == "merge":
        with session_scope() as s:
            task_policy = resolve_settings(s, s.get(Outcome, oid), repo_id=ids[0])
        assert task_policy["effective"]["autonomy"][kind] == (
            "approval" if "approval" in policies[:3] else "auto")


@pytest.mark.parametrize("kind", ["plan", "merge"])
@pytest.mark.parametrize("mode,status", [("auto", 200), ("approval", 200), (None, 200), ("", 200),
                                         ("always", 422), (False, 422), (3, 422), ({}, 422)])
def test_repository_policy_validation(client, configured, kind, mode, status):
    root, _, _, ids = configured
    response = client.patch(root + f"/repositories/{ids[0]}", json={"config": {kind: {"mode": mode}}})
    assert response.status_code == status, response.text


def test_release_gate_and_isolation(client, configured, signed_in):
    root, sid, oid, ids = configured
    with session_scope() as s:
        s.get(Repository, ids[0]).config = {"merge": {"mode": "auto"}, "release": {"mode": "on_merge"}}
    sources = detail(client, configured)["sources"]["autonomy"]["merge"]
    assert {(p["field"], p["value"]) for p in sources} == {
        ("autonomy.release", "approval"), ("release.mode", "on_merge")}
    with session_scope() as s:
        ws = s.get(Workspace, root.split("/")[-1])
        other = Workspace(org_id=ws.org_id, name="Other", slug="other")
        s.add(other)
        s.flush()
        stream = Workstream(workspace_id=other.id, slug="foreign", name="Foreign", goal="Isolated",
                            owner_id=signed_in["user"]["id"], worker_override="anton", priority_override=0,
                            plan_approval_override="approval", merge_approval_override="approval",
                            outcome_budget_override=1)
        repo = Repository(workspace_id=other.id, full_name="foreign/private", config={"plan": {"mode": "approval"}})
        s.add_all([stream, repo])
        s.flush()
        outcome = s.get(Outcome, oid)
        outcome.workstream_id = stream.id
        outcome.repo_ids = [repo.id]
    effective = detail(client, configured)["effective"]
    assert effective["priority"] == 3
    assert effective["budgets"]["outcome_usd"] == 40
    assert effective["autonomy"]["plan"] == effective["autonomy"]["merge"] == "auto"
    assert effective["harness"]["builder"] == "codex"


@pytest.mark.parametrize("kind", ["plan", "merge"])
@pytest.mark.parametrize("mode,status", [("auto", 200), ("approval", 200), (None, 200), ("", 200), ("always", 422)])
def test_repository_connect_policy_validation(client, configured, monkeypatch, kind, mode, status):
    from aifactory import api

    async def detect(*args):
        return {"full_name": "acme/new", "default_branch": "main"}, {}

    monkeypatch.setattr(api, "_detect_repository", detect)
    root, _, _, _ = configured
    response = client.post(root + "/repositories", json={"full_name": "acme/new", "config": {kind: {"mode": mode}}})
    assert response.status_code == status, response.text
    if status == 200:
        assert response.json()["config"][kind]["mode"] == mode


def test_default_priority_and_settings_snapshot_are_independent(client, configured):
    _, _, oid, _ = configured
    with session_scope() as s:
        outcome = s.get(Outcome, oid)
        outcome.workstream_id = None
        outcome.priority = 2
        ws = s.get(Workspace, outcome.workspace_id)
        ws.settings = {}
        resolved = resolve_settings(s, outcome)
        assert resolved["effective"]["priority"] == 2
        assert resolved["sources"]["harness"]["planner"][0]["scope"] == "default"
        resolved["effective"]["limits"]["planner_turns"] = 1
        resolved["effective"]["budgets"]["run_tokens"] = 1
        fresh = resolve_settings(s, outcome)
        assert fresh["effective"]["limits"]["planner_turns"] == 60
        assert fresh["effective"]["budgets"]["run_tokens"] == 6_000_000
        assert ws.settings == {}
