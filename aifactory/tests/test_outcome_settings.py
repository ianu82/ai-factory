"""Shared effective settings, policy truth tables, and the outcome-detail contract."""
from itertools import product

import pytest

from aifactory.db import session_scope
from aifactory.models import Outcome, Repository, Workspace, Workstream
from aifactory.outcome_settings import resolve_settings


@pytest.fixture
def settings_world(signed_in):
    wid = signed_in["workspaces"][0]["id"]
    with session_scope() as s:
        ws = s.get(Workspace, wid)
        ws.settings = {"harness": {"planner": "anton", "builder": "codex", "verifier": "native"},
                       "autonomy": {"plan": "auto", "merge": "auto", "release": "auto"},
                       "budgets": {"outcome_usd": 40, "run_usd": 8, "run_tokens": 1000}}
        stream = Workstream(workspace_id=wid, name="Platform", slug="platform", goal="Reliable",
                            owner_id=signed_in["user"]["id"])
        repos = [Repository(workspace_id=wid, full_name=f"acme/repo{n}") for n in range(2)]
        s.add_all([stream, *repos])
        s.flush()
        outcome = Outcome(workspace_id=wid, workstream_id=stream.id, number=1, title="Resolve",
                          priority=3, repo_ids=[r.id for r in repos])
        s.add(outcome)
        s.flush()
        return wid, stream.id, outcome.id, [r.id for r in repos]


@pytest.mark.parametrize("worker,explicit,expected", [
    (None, None, ["anton", "codex", "native"]),
    ("codex", None, ["codex"] * 3),
    ("anton", "native", ["native"] * 3),
    (None, "codex", ["codex"] * 3),
])
@pytest.mark.parametrize("budget", [None, 1.25, 125])
def test_api_effective_values_clear_and_provenance(client, settings_world, worker, explicit, expected, budget):
    wid, sid, oid, _ = settings_world
    root = f"/api/workspaces/{wid}"
    patch = {"worker_override": worker, "priority_override": 0, "outcome_budget_override": budget}
    assert client.patch(f"{root}/workstreams/{sid}", json=patch).status_code == 200
    with session_scope() as s:
        s.get(Outcome, oid).harness = explicit
    result = client.get(f"{root}/outcomes/{oid}")
    assert result.status_code == 200
    resolved = result.json()["effective_settings"]
    effective, sources = resolved["effective"], resolved["sources"]
    assert [effective[role] for role in ("planner", "builder", "verifier")] == expected
    origin = "outcome" if explicit else "workstream" if worker else "workspace"
    assert sources["builder"][0]["kind"] == origin
    assert effective["priority"] == 0
    assert sources["priority"][0]["id"] == sid
    assert effective["outcome_usd"] == (40 if budget is None else budget)
    assert sources["outcome_usd"][0]["kind"] == ("workspace" if budget is None else "workstream")
    with session_scope() as s:
        resolved = resolve_settings(s, s.get(Outcome, oid))
        assert resolved.settings["budgets"]["run_usd"] == 8
        assert resolved.settings["budgets"]["run_tokens"] == 1000
        assert s.get(Workspace, wid).settings["budgets"]["outcome_usd"] == 40
        assert s.get(Outcome, oid).priority == 3
    assert client.patch(f"{root}/workstreams/{sid}", json=dict.fromkeys(patch)).status_code == 200
    cleared = client.get(f"{root}/outcomes/{oid}").json()["effective_settings"]["effective"]
    assert cleared["priority"] == 3 and cleared["outcome_usd"] == 40
    assert cleared["builder"] == (explicit or "codex")


@pytest.mark.parametrize("policy", ["plan", "merge"])
@pytest.mark.parametrize("workspace,stream,repo1,repo2", list(product(("auto", "approval"), (None, "auto", "approval"), (None, "auto", "approval"), (None, "auto", "approval"))))
def test_approval_truth_table(settings_world, policy, workspace, stream, repo1, repo2):
    wid, sid, oid, repos = settings_world
    with session_scope() as s:
        ws = s.get(Workspace, wid)
        ws.settings = {**ws.settings, "autonomy": {"plan": workspace, "merge": workspace, "release": "auto"}}
        setattr(s.get(Workstream, sid), f"{policy}_approval_override", stream)
        for rid, mode in zip(repos, (repo1, repo2)):
            s.get(Repository, rid).config = {policy: {"mode": mode}}
        s.flush()
        resolved = resolve_settings(s, s.get(Outcome, oid))
        expected = "approval" if "approval" in (workspace, stream, repo1, repo2) else "auto"
        assert resolved.effective[policy] == expected
        if expected == "approval":
            assert len(resolved.sources[policy]) == (workspace, stream, repo1, repo2).count("approval")
        else:
            assert len(resolved.sources[policy]) >= 1
        assert all(src["value"] == expected for src in resolved.sources[policy])
        if policy == "merge":
            own = resolve_settings(s, s.get(Outcome, oid), repo_id=repos[0])
            assert own.effective[policy] == ("approval" if "approval" in (workspace, stream, repo1) else "auto")


def test_release_gate_retains_tied_sources(settings_world):
    wid, sid, oid, repos = settings_world
    with session_scope() as s:
        ws = s.get(Workspace, wid)
        ws.settings = {**ws.settings, "autonomy": {"merge": "approval", "release": "approval"}}
        s.get(Workstream, sid).merge_approval_override = "approval"
        s.get(Repository, repos[0]).config = {"merge": {"mode": "approval"}, "release": {"mode": "on_merge"}}
        s.flush()
        result = resolve_settings(s, s.get(Outcome, oid), repo_id=repos[0])
        assert result.effective["merge"] == "approval"
        assert len(result.sources["merge"]) == 4
        assert result.sources["merge"][-1]["gate"] == "release_on_merge"


def test_tenant_isolation_and_unassigned_outcome(client, settings_world, signed_in):
    wid, sid, oid, repos = settings_world
    with session_scope() as s:
        other = Workspace(org_id=s.get(Workspace, wid).org_id, name="Other", slug="other")
        s.add(other)
        s.flush()
        foreign = Workstream(workspace_id=other.id, name="Other", slug="other", goal="Other",
                             owner_id=signed_in["user"]["id"], worker_override="native", priority_override=0,
                             outcome_budget_override=1, plan_approval_override="approval")
        repo = Repository(workspace_id=other.id, full_name="other/private", config={"plan": {"mode": "approval"}})
        s.add_all([foreign, repo])
        s.flush()
        outcome = s.get(Outcome, oid)
        outcome.workstream_id, outcome.repo_ids = foreign.id, [repo.id]
    data = client.get(f"/api/workspaces/{wid}/outcomes/{oid}").json()["effective_settings"]
    assert data["effective"]["planner"] == "anton"
    assert data["effective"]["priority"] == 3 and data["effective"]["outcome_usd"] == 40
    assert foreign.id not in str(data) and repo.id not in str(data)
    with session_scope() as s:
        s.get(Outcome, oid).workstream_id = None
    assert client.get(f"/api/workspaces/{wid}/outcomes/{oid}").json()["effective_settings"] == data


@pytest.mark.parametrize("policy", ["plan", "merge"])
@pytest.mark.parametrize("mode,status", [("auto", 200), ("approval", 200), (None, 200), ("", 422), ("never", 422), (False, 422), ({}, 422)])
def test_repository_policy_write_validation(client, settings_world, policy, mode, status):
    wid, _, _, repos = settings_world
    response = client.patch(f"/api/workspaces/{wid}/repositories/{repos[0]}", json={"config": {policy: {"mode": mode}}})
    assert response.status_code == status, response.text
    if status == 200:
        assert response.json()["config"][policy]["mode"] == mode
