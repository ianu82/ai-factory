from __future__ import annotations

import asyncio
import json

from aifactory import auth as auth_module
from aifactory import signals as signals_module
from aifactory.bootstrap import apply_bootstrap
from aifactory.db import SessionLocal, session_scope
from aifactory.models import Membership, Organization, Outcome, Repository, Signal, Workspace


def test_bootstrap_invite_links_real_mindshub_org_on_first_sign_in(client, monkeypatch):
    monkeypatch.setenv("AIFACTORY_BOOTSTRAP", json.dumps({
        "organization": "MindsDB", "workspace": "AI Factory", "admins": ["Lead@MindsDB.com"],
        "settings": {"autonomy": {"merge": "approval"}}}))

    async def run():
        return apply_bootstrap()

    ws_id = asyncio.run(run())
    assert asyncio.run(run()) == ws_id  # idempotent

    async def identity(bearer):
        return {"valid": True, "user_id": "u-9", "email": "lead@mindsdb.com", "organization_id": "mh-org-1",
                "organization_name": "MindsDB", "workspace_id": "mh-ws-1", "roles": []}

    monkeypatch.setattr(auth_module, "mindshub_identity", identity)
    r = client.post("/auth/api-key", json={"key": "mdb_lead.secretvalue", "use_for_inference": False})
    assert r.status_code == 200, r.text
    me = client.get("/api/me").json()
    assert [w["id"] for w in me["workspaces"]] == [ws_id]
    assert me["workspaces"][0]["role"] == "owner"
    assert me["workspaces"][0]["settings"]["autonomy"]["merge"] == "approval"
    with session_scope() as s:
        assert s.query(Organization).count() == 1
        assert s.query(Organization).one().mindshub_org_id == "mh-org-1"


def test_labelled_issues_become_outcomes_once_and_plain_label_is_only_proposed(monkeypatch):
    with session_scope() as s:
        org = Organization(name="Acme")
        s.add(org)
        s.flush()
        ws = Workspace(org_id=org.id, name="Acme", slug="acme", settings={})
        s.add(ws)
        s.flush()
        s.add(Repository(workspace_id=ws.id, full_name="acme/app", default_branch="main", config={}))
        ws_id = ws.id
    comments = []

    class FakeGH:
        def __init__(self, token):
            pass

        async def issues(self, full_name, labels="", since=None):
            if labels == "factory:auto":
                return [{"number": 7, "title": "Add CSV export", "body": "Please", "html_url": "https://gh/7",
                         "user": {"login": "ian"}}]
            if labels == "factory":
                return [{"number": 8, "title": "Slow log page", "body": "", "html_url": "https://gh/8", "user": {}}]
            return []

        async def comment(self, full_name, number, body):
            comments.append((number, body))

        async def request(self, *args, **kwargs):
            return {"workflow_runs": []}

    monkeypatch.setattr(signals_module, "GitHub", FakeGH)
    for _ in range(2):
        db = SessionLocal()
        result = asyncio.run(signals_module.sync_github(db, ws_id, "t"))
        db.close()
    assert result["accepted"] == 0  # second pass does not duplicate
    with session_scope() as s:
        outcomes = s.query(Outcome).filter(Outcome.workspace_id == ws_id).all()
        assert [o.title for o in outcomes] == ["Add CSV export"] and outcomes[0].source == "signal"
        sigs = {x.external_id: x for x in s.query(Signal)}
        assert sigs["acme/app#7"].status == "accepted" and sigs["acme/app#8"].status == "new"
    assert comments == [(7, comments[0][1])] and "outcome #1" in comments[0][1]


def test_issue_already_addressed_by_an_outcome_is_linked_not_duplicated(monkeypatch):
    with session_scope() as s:
        org = Organization(name="Acme")
        s.add(org)
        s.flush()
        ws = Workspace(org_id=org.id, name="Acme", slug="acme", settings={}, outcome_seq=1)
        s.add(ws)
        s.flush()
        repo = Repository(workspace_id=ws.id, full_name="acme/app", default_branch="main", config={})
        s.add(repo)
        s.flush()
        s.add(Outcome(workspace_id=ws.id, number=1, title="Declared", brief="Fix it.\n\nSource: https://gh/7",
                      repo_ids=[repo.id], status="running"))
        ws_id = ws.id

    class FakeGH:
        def __init__(self, token):
            pass

        async def issues(self, full_name, labels="", since=None):
            return [{"number": 7, "title": "Fix it", "body": "", "html_url": "https://gh/7", "user": {}}] if labels == "factory:auto" else []

        async def comment(self, *a):
            raise AssertionError("should not comment on a linked issue")

        async def request(self, *args, **kwargs):
            return {"workflow_runs": []}

    monkeypatch.setattr(signals_module, "GitHub", FakeGH)
    db = SessionLocal()
    asyncio.run(signals_module.sync_github(db, ws_id, "t"))
    db.close()
    with session_scope() as s:
        assert s.query(Outcome).filter(Outcome.workspace_id == ws_id).count() == 1
        assert s.query(Signal).one().status == "accepted"


def test_delegated_operator_resolves_a_merge_approval_once(monkeypatch):
    from aifactory.models import Decision, Event, Plan, Task, User

    with session_scope() as s:
        org = Organization(name="Acme")
        s.add(org)
        s.flush()
        ws = Workspace(org_id=org.id, name="Acme", slug="acme", settings={})
        s.add(ws)
        s.flush()
        repo = Repository(workspace_id=ws.id, full_name="acme/app", default_branch="main", config={})
        s.add(repo)
        s.flush()
        o = Outcome(workspace_id=ws.id, number=7, title="Self change", repo_ids=[repo.id], status="attention")
        s.add(o)
        s.flush()
        plan = Plan(outcome_id=o.id, status="approved", content={})
        s.add(plan)
        s.flush()
        t = Task(workspace_id=ws.id, outcome_id=o.id, plan_id=plan.id, key="T1", title="t", repo_id=repo.id,
                 status="awaiting_approval", stage="merge", pr_number=196)
        s.add(t)
        s.flush()
        s.add(Decision(workspace_id=ws.id, outcome_id=o.id, task_id=t.id, kind="approve_merge", title="Approve merge",
                       options=[{"value": "approve", "label": "Merge"}, {"value": "reject", "label": "Request changes"}]))
        ws_id, task_id = ws.id, t.id
    monkeypatch.setenv("AIFACTORY_BOOTSTRAP", json.dumps({
        "organization": "Acme", "workspace": "Acme", "delegated_by": "ian@mindsdb.com",
        "resolve": [{"outcome": 7, "kind": "approve_merge", "resolution": "approve", "nonce": "n1"}]}))

    async def run():
        return apply_bootstrap()

    asyncio.run(run())
    asyncio.run(run())  # same nonce: applied once
    with session_scope() as s:
        task = s.get(Task, task_id)
        assert task.status == "queued" and task.stage == "merge"
        decision = s.query(Decision).one()
        operator = s.get(User, decision.resolved_by)
        assert decision.resolution == "approve" and operator.is_system and "ian@mindsdb.com" in operator.name
        assert s.query(Event).filter(Event.kind == "operator_decision").count() == 1


def test_operator_retry_gives_a_failed_task_a_fresh_budget_and_its_automatic_round(monkeypatch):
    from aifactory.models import Plan, Task

    with session_scope() as s:
        org = Organization(name="Acme")
        s.add(org)
        s.flush()
        ws = Workspace(org_id=org.id, name="Acme", slug="acme", settings={})
        s.add(ws)
        s.flush()
        repo = Repository(workspace_id=ws.id, full_name="acme/app", default_branch="main", config={})
        s.add(repo)
        s.flush()
        o = Outcome(workspace_id=ws.id, number=9, title="Stuck", repo_ids=[repo.id], status="attention")
        s.add(o)
        s.flush()
        plan = Plan(outcome_id=o.id, status="approved", content={})
        s.add(plan)
        s.flush()
        t = Task(workspace_id=ws.id, outcome_id=o.id, plan_id=plan.id, key="T1", title="t", repo_id=repo.id,
                 status="failed", attempt=6, evidence={"attempt_base": 3, "auto_retries": 1, "attempts": []})
        s.add(t)
        s.flush()
        task_id = t.id
    monkeypatch.setenv("AIFACTORY_BOOTSTRAP", json.dumps({
        "organization": "Acme", "workspace": "Acme", "retry": [{"outcome": 9, "nonce": "r1"}]}))

    async def run():
        return apply_bootstrap()

    asyncio.run(run())
    with session_scope() as s:
        task = s.get(Task, task_id)
        assert task.status == "queued" and task.attempt == 0
        assert "attempt_base" not in task.evidence and "auto_retries" not in task.evidence


def test_bootstrap_applies_declared_values_once_and_keeps_later_app_changes(monkeypatch):
    def boot(config):
        monkeypatch.setenv("AIFACTORY_BOOTSTRAP", json.dumps(config))

        async def run():
            return apply_bootstrap()

        return asyncio.run(run())

    base = {"organization": "Acme", "workspace": "Delivery", "settings": {"budgets": {"run_usd": 5}},
            "repositories": [{"full_name": "acme/app", "config": {"merge": {"mode": "approval"}, "checks": ["npm test"]}}]}
    ws_id = boot(base)
    with session_scope() as s:
        s.add(Repository(workspace_id=ws_id, full_name="acme/app", config={}))
    boot(base)  # connects the declared config to the existing repository
    with session_scope() as s:
        repo = s.query(Repository).filter(Repository.full_name == "acme/app").one()
        assert repo.config["merge"] == {"mode": "approval"}
        # People change things in the app (for example by applying an improvement proposal).
        repo.config = {**repo.config, "merge": {"mode": "auto"}, "context": "Use pnpm."}
        ws = s.get(Workspace, ws_id)
        ws.settings = {**ws.settings, "budgets": {**ws.settings["budgets"], "run_usd": 12}}

    boot(base)  # a restart with the same bootstrap keeps those changes
    with session_scope() as s:
        repo = s.query(Repository).filter(Repository.full_name == "acme/app").one()
        assert repo.config["merge"] == {"mode": "auto"} and repo.config["context"] == "Use pnpm."
        assert s.get(Workspace, ws_id).settings["budgets"]["run_usd"] == 12

    changed = json.loads(json.dumps(base))
    changed["repositories"][0]["config"]["checks"] = ["npm test", "npm run lint"]
    changed["settings"]["budgets"]["run_usd"] = 6
    boot(changed)  # the operator changed the bootstrap: those keys apply, untouched ones stay
    with session_scope() as s:
        repo = s.query(Repository).filter(Repository.full_name == "acme/app").one()
        assert repo.config["checks"] == ["npm test", "npm run lint"] and repo.config["merge"] == {"mode": "auto"}
        assert s.get(Workspace, ws_id).settings["budgets"]["run_usd"] == 6


def test_delegated_resolve_can_target_one_task_of_an_outcome(monkeypatch):
    from aifactory.models import Decision, Plan, Task

    with session_scope() as s:
        org = Organization(name="Acme")
        s.add(org)
        s.flush()
        ws = Workspace(org_id=org.id, name="Acme", slug="acme", settings={})
        s.add(ws)
        s.flush()
        repo = Repository(workspace_id=ws.id, full_name="acme/app", default_branch="main", config={})
        s.add(repo)
        s.flush()
        o = Outcome(workspace_id=ws.id, number=13, title="Two PRs", repo_ids=[repo.id], status="attention")
        s.add(o)
        s.flush()
        plan = Plan(outcome_id=o.id, status="approved", content={})
        s.add(plan)
        s.flush()
        ids = {}
        for key, pr in (("T1", 212), ("T3", 213)):  # T3's decision is the newer one
            t = Task(workspace_id=ws.id, outcome_id=o.id, plan_id=plan.id, key=key, title=key, repo_id=repo.id,
                     status="awaiting_approval", stage="merge", pr_number=pr)
            s.add(t)
            s.flush()
            s.add(Decision(workspace_id=ws.id, outcome_id=o.id, task_id=t.id, kind="approve_merge", title=f"Merge {key}",
                           options=[{"value": "approve", "label": "Merge"}]))
            s.flush()
            ids[key] = t.id
    monkeypatch.setenv("AIFACTORY_BOOTSTRAP", json.dumps({
        "organization": "Acme", "workspace": "Acme", "delegated_by": "ian@mindsdb.com",
        "resolve": [{"outcome": 13, "task": "T1", "kind": "approve_merge", "resolution": "approve", "nonce": "t1"}]}))

    async def run():
        return apply_bootstrap()

    asyncio.run(run())
    with session_scope() as s:
        assert s.get(Task, ids["T1"]).status == "queued"
        assert s.get(Task, ids["T3"]).status == "awaiting_approval"
        assert s.query(Decision).filter(Decision.task_id == ids["T3"]).one().status == "open"


def test_bootstrap_cancels_by_number_only_when_idle_and_declares_priority(monkeypatch):
    from aifactory.models import Decision, Plan, Task

    def boot(config):
        monkeypatch.setenv("AIFACTORY_BOOTSTRAP", json.dumps(config))

        async def run():
            ws_id = apply_bootstrap()
            for _ in range(20):  # declared outcomes are admitted by a background task
                await asyncio.sleep(0)
            return ws_id

        return asyncio.run(run())

    base = {"organization": "Acme", "workspace": "Delivery", "repositories": [{"full_name": "acme/app"}]}
    ws_id = boot(base)
    with session_scope() as s:
        repo = Repository(workspace_id=ws_id, full_name="acme/app", config={})
        s.add(repo)
        s.flush()
        ids = {}
        for number, status in ((7, "queued"), (8, "running"), (9, "interrupted")):
            o = Outcome(workspace_id=ws_id, number=number, title=f"Intake {number}", repo_ids=[repo.id],
                        status="running", source="signal")
            s.add(o)
            s.flush()
            plan = Plan(outcome_id=o.id, status="approved", content={})
            s.add(plan)
            s.flush()
            # An interrupted task is persisted as queued but keeps its resumption markers.
            first = Task(workspace_id=ws_id, outcome_id=o.id, plan_id=plan.id, key="T1", title="First",
                         repo_id=repo.id, status="queued" if status == "interrupted" else status)
            if status == "interrupted":
                first.stage, first.attempt = "verify", 1
            s.add_all([first,
                       Task(workspace_id=ws_id, outcome_id=o.id, plan_id=plan.id, key="T2", title="Second",
                            repo_id=repo.id, status="pending", depends_on=["T1"])])
            ids[number] = o.id
        s.add(Decision(workspace_id=ws_id, outcome_id=ids[7], kind="question", title="Planner question", body=""))

    boot({**base, "cancel": ["#7", "#8", "#9", "#99"],
          "outcomes": [{"key": "mind-v1", "title": "Mind v1", "brief": "Thin slice.", "priority": 0,
                        "repositories": ["acme/app"]},
                       {"key": "other", "title": "Other", "priority": 9, "repositories": ["acme/app"]}]})
    with session_scope() as s:
        idle = s.get(Outcome, ids[7])
        assert idle.status == "cancelled"
        assert {t.status for t in s.query(Task).filter(Task.outcome_id == idle.id)} == {"cancelled"}
        assert [d.status for d in s.query(Decision).filter(Decision.outcome_id == idle.id)] == ["superseded"]
        # Work in flight (running, or interrupted by a restart and queued to resume) is never stopped.
        for number, first in ((8, "running"), (9, "queued")):
            busy = s.get(Outcome, ids[number])
            assert busy.status == "running"
            assert {t.key: t.status for t in s.query(Task).filter(Task.outcome_id == busy.id)} == {
                "T1": first, "T2": "pending"}
        declared = {o.title: o.priority for o in s.query(Outcome).filter(Outcome.source == "operator")}
        assert declared == {"Mind v1": 0, "Other": 2}
