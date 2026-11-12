from __future__ import annotations

import asyncio
import json

import pytest

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

        async def repository_author_permission(self, full_name, author):
            return "write"

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
        for number, status in ((7, "queued"), (8, "running"), (9, "interrupted"), (10, "interrupted"),
                               (11, "with_pr")):
            o = Outcome(workspace_id=ws_id, number=number, title=f"Intake {number}", repo_ids=[repo.id],
                        status="running", source="signal")
            s.add(o)
            s.flush()
            plan = Plan(outcome_id=o.id, status="approved", content={})
            s.add(plan)
            s.flush()
            # An interrupted task is persisted as queued but keeps its resumption markers.
            first = Task(workspace_id=ws_id, outcome_id=o.id, plan_id=plan.id, key="T1", title="First",
                         repo_id=repo.id, status=status if status in ("queued", "running") else "queued")
            if status in ("interrupted", "with_pr"):
                first.stage, first.attempt = "verify", 1
            if status == "with_pr":
                first.pr_number = 42
            s.add_all([first,
                       Task(workspace_id=ws_id, outcome_id=o.id, plan_id=plan.id, key="T2", title="Second",
                            repo_id=repo.id, status="pending", depends_on=["T1"])])
            ids[number] = o.id
        s.add(Decision(workspace_id=ws_id, outcome_id=ids[7], kind="question", title="Planner question", body=""))

    boot({**base, "cancel": ["#7", "#8", "#9", "#99", {"outcome": 10, "discard_started": True},
                             {"outcome": 11, "discard_started": True}],
          "outcomes": [{"key": "mind-v1", "title": "Mind v1", "brief": "Thin slice.", "priority": 0,
                        "repositories": ["acme/app"]},
                       {"key": "other", "title": "Other", "priority": 9, "repositories": ["acme/app"]}]})
    with session_scope() as s:
        idle = s.get(Outcome, ids[7])
        assert idle.status == "cancelled"
        assert {t.status for t in s.query(Task).filter(Task.outcome_id == idle.id)} == {"cancelled"}
        assert [d.status for d in s.query(Decision).filter(Decision.outcome_id == idle.id)] == ["superseded"]
        # Only the explicit form discards started work, and never where a pull request is open.
        assert s.get(Outcome, ids[10]).status == "cancelled"
        assert {t.status for t in s.query(Task).filter(Task.outcome_id == ids[10])} == {"cancelled"}
        # Work in flight (running, or interrupted by a restart and queued to resume) is never stopped.
        for number, first in ((8, "running"), (9, "queued"), (11, "queued")):
            busy = s.get(Outcome, ids[number])
            assert busy.status == "running"
            assert {t.key: t.status for t in s.query(Task).filter(Task.outcome_id == busy.id)} == {
                "T1": first, "T2": "pending"}
        declared = {o.title: o.priority for o in s.query(Outcome).filter(Outcome.source == "operator")}
        assert declared == {"Mind v1": 0, "Other": 2}


def test_bootstrap_guidance_reaches_tasks_once_and_is_attributed_to_the_operator(monkeypatch):
    from aifactory.models import Event, Message, Plan, Task, User
    from aifactory.pipeline.runtime import guidance_history

    def boot(config):
        monkeypatch.setenv("AIFACTORY_BOOTSTRAP", json.dumps(config))

        async def run():
            return apply_bootstrap()

        return asyncio.run(run())

    base = {"organization": "Acme", "workspace": "Delivery", "delegated_by": "ian@example.com"}
    ws_id = boot(base)
    with session_scope() as s:
        repo = Repository(workspace_id=ws_id, full_name="acme/app", config={})
        s.add(repo)
        s.flush()
        o = Outcome(workspace_id=ws_id, number=5, title="Review loop", repo_ids=[repo.id], status="running")
        s.add(o)
        s.flush()
        plan = Plan(outcome_id=o.id, status="approved", content={})
        s.add(plan)
        s.flush()
        s.add_all([Task(workspace_id=ws_id, outcome_id=o.id, plan_id=plan.id, key=k, title=k, repo_id=repo.id,
                        status="pending") for k in ("T1", "T2")])
        outcome_id = o.id
    cfg = {**base, "guidance": [{"outcome": 5, "body": "Answer question-only comments in the thread.", "nonce": "g1"},
                                {"outcome": 5, "task": "T2", "body": "Ignore commenters without write access.",
                                 "nonce": "g2"},
                                {"outcome": 5, "task": "T9", "body": "No such task.", "nonce": "g3"},
                                {"outcome": 99, "body": "No such outcome.", "nonce": "g4"}]}
    boot(cfg)
    boot(cfg)  # a restart never posts the same guidance twice
    with session_scope() as s:
        t1, t2 = (s.query(Task).filter(Task.outcome_id == outcome_id, Task.key == k).one().id for k in ("T1", "T2"))
        messages = s.query(Message).filter(Message.outcome_id == outcome_id).all()
        assert len(messages) == 2
        author = s.get(User, messages[0].author_id)
        assert author.is_system and author.name == "Operator (delegated by ian@example.com)"
        events = s.query(Event).filter(Event.outcome_id == outcome_id, Event.kind == "guidance").all()
        assert sorted(e.data["nonce"] for e in events) == ["g1", "g2"]
        assert all(e.data["delegated_by"] == "ian@example.com" for e in events)
    assert guidance_history(outcome_id, t1) == ["Answer question-only comments in the thread."]
    assert guidance_history(outcome_id, t2) == ["Answer question-only comments in the thread.",
                                                "Ignore commenters without write access."]

    # A nonce reused for another outcome never posts again, and a later delegator gets their own identity:
    # earlier guidance keeps naming who delegated it.
    with session_scope() as s:
        o2 = Outcome(workspace_id=ws_id, number=6, title="Other", repo_ids=[], status="running")
        s.add(o2)
        s.flush()
        other_id = o2.id
    boot({**base, "delegated_by": "ceo@example.com",
          "guidance": [{"outcome": 6, "body": "Reused nonce.", "nonce": "g1"},
                       {"outcome": 6, "body": "New guidance.", "nonce": "g5"}]})
    with session_scope() as s:
        assert [m.body for m in s.query(Message).filter(Message.outcome_id == other_id)] == ["New guidance."]
        names = {s.get(User, m.author_id).name for m in s.query(Message).filter(Message.workspace_id == ws_id)}
        assert names == {"Operator (delegated by ian@example.com)", "Operator (delegated by ceo@example.com)"}


def test_delegation_reaches_another_workspace_only_to_relay_and_only_where_the_delegator_is_a_member(monkeypatch):
    from aifactory.models import Decision, Message, Plan, Task, User

    with session_scope() as s:
        org = Organization(name="Acme")
        s.add(org)
        s.flush()
        ian = User(email="ian@mindsdb.com", name="Ian")
        s.add(ian)
        s.flush()
        ids = {}
        for name, member in (("Products", True), ("Elsewhere", False)):
            ws = Workspace(org_id=org.id, name=name, slug=name.lower(), settings={})
            s.add(ws)
            s.flush()
            if member:
                s.add(Membership(workspace_id=ws.id, user_id=ian.id, role="owner"))
            repo = Repository(workspace_id=ws.id, full_name=f"acme/{name.lower()}", default_branch="main", config={})
            s.add(repo)
            s.flush()
            o = Outcome(workspace_id=ws.id, number=1, title="Work", repo_ids=[repo.id], status="attention")
            s.add(o)
            s.flush()
            plan = Plan(outcome_id=o.id, status="approved", content={})
            s.add(plan)
            s.flush()
            tasks = {}
            for key in ("T1", "T2"):
                t = Task(workspace_id=ws.id, outcome_id=o.id, plan_id=plan.id, key=key, title=key, repo_id=repo.id,
                         status="awaiting_approval", stage="merge", pr_number=7)
                s.add(t)
                s.flush()
                s.add(Decision(workspace_id=ws.id, outcome_id=o.id, task_id=t.id, kind="approve_merge",
                               title=f"Merge {key}", options=[{"value": "approve", "label": "Merge"},
                                                              {"value": "reject", "label": "Request changes"}]))
                tasks[key] = t.id
            # GitHub refused a merge: retrying would merge again, so it is never a relay.
            t = Task(workspace_id=ws.id, outcome_id=o.id, plan_id=plan.id, key="T3", title="T3", repo_id=repo.id,
                     status="blocked", stage="merge", pr_number=8)
            s.add(t)
            s.flush()
            s.add(Decision(workspace_id=ws.id, outcome_id=o.id, task_id=t.id, kind="unblock",
                           title="GitHub refused the merge", options=[{"value": "retry", "label": "Retry"}]))
            tasks["T3"] = t.id
            ids[name] = (ws.id, o.id, tasks)
    products, elsewhere = ids["Products"], ids["Elsewhere"]
    monkeypatch.setenv("AIFACTORY_BOOTSTRAP", json.dumps({
        "organization": "Acme", "workspace": "Operator", "delegated_by": "ian@mindsdb.com",
        "resolve": [
            {"workspace": products[0], "outcome": 1, "task": "T1", "kind": "approve_merge", "resolution": "reject",
             "comment": "Address the reviewer's comment on api.py.", "reusable": True, "nonce": "p-reject"},
            {"workspace": products[0], "outcome": 1, "task": "T2", "kind": "approve_merge", "resolution": "approve",
             "nonce": "p-approve"},
            {"workspace": products[0], "outcome": 1, "task": "T3", "kind": "unblock", "resolution": "retry",
             "nonce": "p-retry-merge"},
            {"workspace": elsewhere[0], "outcome": 1, "task": "T1", "kind": "approve_merge", "resolution": "reject",
             "comment": "Not a member here.", "nonce": "e-reject"}],
        "guidance": [{"workspace": products[0], "outcome": 1, "body": "Reply on each thread.", "nonce": "p-guide"},
                     {"workspace": elsewhere[0], "outcome": 1, "body": "Not a member here.", "nonce": "e-guide"}]}))

    async def run():
        return apply_bootstrap()

    asyncio.run(run())
    with session_scope() as s:
        # Change requests and guidance reach a workspace where the delegator is a member...
        assert (s.get(Task, products[2]["T1"]).status, s.get(Task, products[2]["T1"]).stage) == ("queued", "revise")
        rejected = s.query(Decision).filter(Decision.task_id == products[2]["T1"]).one()
        assert rejected.status == "resolved" and rejected.reusable is False
        assert [m.body for m in s.query(Message).filter(Message.outcome_id == products[1])
                if m.body == "Reply on each thread."] == ["Reply on each thread."]
        # ...but the operator never approves there, and does nothing where the delegator is not a member.
        assert s.get(Task, products[2]["T2"]).status == "awaiting_approval"
        assert s.get(Task, products[2]["T3"]).status == "blocked"  # a retry at the merge stage would merge
        assert s.get(Task, elsewhere[2]["T1"]).status == "awaiting_approval"
        assert not s.query(Message).filter(Message.outcome_id == elsewhere[1]).count()


class IntakeGitHub:
    """Fake current permissions independent of issue association and labels."""
    permission = "read"

    def __init__(self, token):
        self.lookups = []
        self.comments = []
        self.ci_requests = []
        self.rows = {}

    async def issues(self, full_name, labels="", since=None):
        return self.rows.get(full_name, []) if labels == "factory:auto" else []

    async def repository_author_permission(self, full_name, author):
        self.lookups.append((full_name, author))
        if isinstance(self.permission, Exception):
            raise self.permission
        return self.permission

    async def comment(self, full_name, number, body):
        self.comments.append((full_name, number))

    async def request(self, method, path, **kwargs):
        self.ci_requests.append(path)
        return {"workflow_runs": []}


def intake_issue(number=1, author="contributor", worker="codex"):
    return {"number": number, "title": f"Issue {number}", "body": "Do work", "html_url": f"https://gh/{number}",
            "user": {"login": author} if author else None, "author_association": "OWNER",
            "labels": [{"name": "factory:auto"}, {"name": f"worker:{worker}"}]}


def sync_intake(ws_id):
    with SessionLocal() as db:
        return asyncio.run(signals_module.sync_github(db, ws_id, "fake-token"))


def seed_intake_repo(ws_id, name="acme/app", config=None):
    with session_scope() as db:
        repo = Repository(workspace_id=ws_id, full_name=name, config=config or {})
        db.add(repo)
        db.flush()
        return repo.id


@pytest.mark.parametrize("permission", ["write", "admin", "push", "read", "triage", "maintain", "unknown", None])
def test_auto_intake_checks_current_author_permission(client, signed_in, monkeypatch, permission):
    ws_id = signed_in["workspaces"][0]["id"]
    seed_intake_repo(ws_id)
    gh = IntakeGitHub("t")
    gh.permission = permission
    gh.rows = {"acme/app": [intake_issue()]}
    monkeypatch.setattr(signals_module, "GitHub", lambda token: gh)
    trusted = permission in ("write", "admin", "push")
    assert sync_intake(ws_id)["accepted"] == int(trusted)
    gh.permission = "admin"  # a held proposal must not be promoted on the next poll
    assert sync_intake(ws_id)["accepted"] == 0
    with session_scope() as db:
        sig = db.query(Signal).one()
        assert sig.count == 2
        assert sig.status == ("accepted" if trusted else "new")
        assert db.query(Outcome).count() == int(trusted)
        if trusted:
            assert db.query(Outcome).one().harness == "codex"
        else:
            assert "Acceptance needed" in sig.data["acceptance_reason"]
            assert len(sig.data["acceptance_reason"]) < 200
    assert gh.lookups == [("acme/app", "contributor")]
    assert len(gh.comments) == int(trusted)


@pytest.mark.parametrize("failure", ["missing", "denied", "network"])
def test_unverifiable_authors_fail_closed_without_stopping_sync(client, signed_in, monkeypatch, failure):
    import httpx
    from aifactory.github import GitHubError

    ws_id = signed_in["workspaces"][0]["id"]
    seed_intake_repo(ws_id)
    seed_intake_repo(ws_id, "acme/second", {"require_trusted_issue_author": False})
    gh = IntakeGitHub("t")
    gh.permission = GitHubError("secret-provider-detail", 403) if failure == "denied" else httpx.ConnectError("secret-provider-detail")
    gh.rows = {"acme/app": [intake_issue(1, None if failure == "missing" else "author"),
                            intake_issue(2, None if failure == "missing" else "author")],
               "acme/second": [intake_issue(3)]}
    monkeypatch.setattr(signals_module, "GitHub", lambda token: gh)
    assert sync_intake(ws_id)["accepted"] == 1
    assert len(gh.ci_requests) == 4
    with session_scope() as db:
        assert db.query(Outcome).count() == 1
        held = db.query(Signal).filter(Signal.status == "new").all()
        assert len(held) == 2
        assert all("secret-provider-detail" not in json.dumps(s.data) for s in held)
        assert all("Acceptance needed" in s.data["acceptance_reason"] for s in held)
    assert len(gh.lookups) == (0 if failure == "missing" else 2)


@pytest.mark.parametrize("worker", ["codex", "native", "anton"])
def test_human_acceptance_retains_held_worker_and_is_idempotent(client, signed_in, monkeypatch, worker):
    ws_id = signed_in["workspaces"][0]["id"]
    repo_id = seed_intake_repo(ws_id)
    gh = IntakeGitHub("t")
    gh.rows = {"acme/app": [intake_issue(worker=worker)]}
    monkeypatch.setattr(signals_module, "GitHub", lambda token: gh)
    assert sync_intake(ws_id)["accepted"] == 0
    # Even turning the rule off must not silently accept an already held proposal.
    with session_scope() as db:
        db.get(Repository, repo_id).config = {"require_trusted_issue_author": False}
        sig_id = db.query(Signal).one().id
    gh.rows["acme/app"][0]["labels"] = []
    assert sync_intake(ws_id)["accepted"] == 0
    for _ in range(2):
        response = client.post(f"/api/workspaces/{ws_id}/signals/{sig_id}", json={"action": "accept"})
        assert response.status_code == 200, response.text
        assert response.json()["status"] == "accepted"
    assert sync_intake(ws_id)["accepted"] == 0
    with session_scope() as db:
        outcome = db.query(Outcome).one()
        assert outcome.harness == worker
        assert outcome.signal_id == sig_id
        assert db.query(Signal).count() == 1


@pytest.mark.parametrize("role", ["owner", "admin", "member", "viewer"])
def test_only_admins_can_change_issue_author_trust(client, signed_in, role):
    ws_id = signed_in["workspaces"][0]["id"]
    repo_id = seed_intake_repo(ws_id, config={"context": "keep"})
    with session_scope() as db:
        db.query(Membership).filter_by(workspace_id=ws_id, user_id=signed_in["user"]["id"]).one().role = role
    for value in (False, True):
        response = client.patch(f"/api/workspaces/{ws_id}/repositories/{repo_id}",
                                json={"config": {"require_trusted_issue_author": value}})
        assert response.status_code == (200 if role in ("owner", "admin") else 403), response.text
        if response.status_code == 200:
            assert response.json()["config"] == {"context": "keep", "require_trusted_issue_author": value,
                                                 "steward": {"mode": "propose", "fix_minutes": 60},
                                                 "review": {"mode": "off", "allow_untrusted_local": False}}
            with session_scope() as db:
                assert db.get(Repository, repo_id).config == {"context": "keep", "require_trusted_issue_author": value}
    with session_scope() as db:
        if role in ("member", "viewer"):
            assert db.get(Repository, repo_id).config == {"context": "keep"}


@pytest.mark.parametrize("value", [None, 0, 1, "false", "true", [], {}])
def test_issue_author_trust_requires_a_json_boolean(client, signed_in, value):
    ws_id = signed_in["workspaces"][0]["id"]
    repo_id = seed_intake_repo(ws_id)
    response = client.patch(f"/api/workspaces/{ws_id}/repositories/{repo_id}",
                            json={"config": {"require_trusted_issue_author": value}})
    assert response.status_code == 422
    with session_scope() as db:
        assert db.get(Repository, repo_id).config == {}


@pytest.mark.parametrize("payload,expected", [
    ({"permission": "write"}, "write"), ({"permission": "admin"}, "admin"),
    ({"permission": "push"}, "push"), ({"permission": "read"}, "read"),
    ({"permission": "triage"}, "triage"), ({}, None), ([], None),
    # Missing top-level permission is not proof of access, even with nested flags.
    ({"user": {"permissions": {"push": True}}}, None),
    ({"user": {"permissions": {"admin": True}}}, None),
    ({"user": {"permissions": {"push": "true"}}}, None),
])
def test_github_author_permission_lookup(monkeypatch, payload, expected):
    from aifactory.github import GitHub

    gh = GitHub("not-a-real-token")

    async def request(method, path):
        assert method == "GET"
        assert path == "/repos/acme/app/collaborators/issue-author/permission"
        return payload

    monkeypatch.setattr(gh, "request", request)
    assert asyncio.run(gh.repository_author_permission("acme/app", "issue-author")) == expected


@pytest.mark.parametrize("config", [{}, {"require_trusted_issue_author": True}])
def test_opt_out_is_repository_scoped_and_deduplicated(client, signed_in, monkeypatch, config):
    ws_id = signed_in["workspaces"][0]["id"]
    seed_intake_repo(ws_id, config=config)
    seed_intake_repo(ws_id, "acme/opt-out", {"require_trusted_issue_author": False})
    gh = IntakeGitHub("t")
    gh.rows = {"acme/app": [intake_issue(1, author=None)],
               "acme/opt-out": [intake_issue(2, author=None)]}
    monkeypatch.setattr(signals_module, "GitHub", lambda token: gh)
    assert sync_intake(ws_id)["accepted"] == 1
    assert sync_intake(ws_id)["accepted"] == 0
    assert gh.lookups == []
    assert gh.comments == [("acme/opt-out", 2)]
    with session_scope() as db:
        assert db.query(Outcome).count() == 1
        sigs = {s.external_id: s for s in db.query(Signal)}
        assert sigs["acme/app#1"].status == "new"
        assert sigs["acme/opt-out#2"].status == "accepted"
        assert all(s.count == 2 for s in sigs.values())


@pytest.mark.parametrize("action", ["accept", "ignore"])
def test_handling_held_issue_clears_acceptance_prompt(client, signed_in, monkeypatch, action):
    ws_id = signed_in["workspaces"][0]["id"]
    seed_intake_repo(ws_id)
    gh = IntakeGitHub("t")
    gh.rows = {"acme/app": [intake_issue(worker="codex")]}
    monkeypatch.setattr(signals_module, "GitHub", lambda token: gh)
    assert sync_intake(ws_id)["accepted"] == 0
    with session_scope() as db:
        sig = db.query(Signal).one()
        assert "Acceptance needed" in sig.data["acceptance_reason"]
        sig_id = sig.id
    for _ in range(2):
        response = client.post(f"/api/workspaces/{ws_id}/signals/{sig_id}", json={"action": action})
        assert response.status_code == 200, response.text
        assert response.json()["status"] == ("accepted" if action == "accept" else "ignored")
        assert "acceptance_reason" not in response.json()["data"]
        assert response.json()["data"]["worker"] == "codex"
    gh.permission = "admin"
    assert sync_intake(ws_id)["accepted"] == 0
    response = client.get(f"/api/workspaces/{ws_id}/signals")
    assert response.status_code == 200
    assert all("acceptance_reason" not in sig["data"] for sig in response.json())
    with session_scope() as db:
        sig = db.get(Signal, sig_id)
        assert "acceptance_reason" not in sig.data
        assert db.query(Outcome).count() == int(action == "accept")
        if action == "accept":
            assert db.query(Outcome).one().harness == "codex"


def test_delegated_resolve_applies_only_to_the_head_it_was_judged_on(monkeypatch):
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
        o = Outcome(workspace_id=ws.id, number=4, title="Heads", repo_ids=[repo.id], status="attention")
        s.add(o)
        s.flush()
        plan = Plan(outcome_id=o.id, status="approved", content={})
        s.add(plan)
        s.flush()
        ids = {}
        for key, head in (("T1", "aaaaaaa1111111"), ("T2", "bbbbbbb2222222")):
            t = Task(workspace_id=ws.id, outcome_id=o.id, plan_id=plan.id, key=key, title=key, repo_id=repo.id,
                     status="awaiting_approval", stage="merge", pr_number=9, head_sha=head)
            s.add(t)
            s.flush()
            s.add(Decision(workspace_id=ws.id, outcome_id=o.id, task_id=t.id, kind="approve_merge",
                           title=f"Merge {key}", options=[{"value": "approve", "label": "Merge"},
                                                          {"value": "reject", "label": "Request changes"}]))
            ids[key] = t.id
    monkeypatch.setenv("AIFACTORY_BOOTSTRAP", json.dumps({
        "organization": "Acme", "workspace": "Acme", "delegated_by": "ian@mindsdb.com",
        "resolve": [
            # Judged on an older revision: the task has since pushed a new head, so this must not merge it.
            {"outcome": 4, "task": "T1", "kind": "approve_merge", "resolution": "approve", "head": "0123abcd",
             "nonce": "stale"},
            {"outcome": 4, "task": "T2", "kind": "approve_merge", "resolution": "approve", "head": "bbbbbbb",
             "nonce": "current"}]}))

    async def run():
        return apply_bootstrap()

    asyncio.run(run())
    with session_scope() as s:
        assert s.get(Task, ids["T1"]).status == "awaiting_approval"
        assert s.query(Decision).filter(Decision.task_id == ids["T1"]).one().status == "open"
        assert s.get(Task, ids["T2"]).status == "queued"


def seed_stream(ws, owner, slug='platform', **extra):
    from aifactory.models import Workstream
    with session_scope() as db:
        stream = Workstream(workspace_id=ws, owner_id=owner, slug=slug, name=slug, goal='Delivery', **extra)
        db.add(stream)
        db.flush()
        return stream.id


@pytest.mark.parametrize('auto', [False, True])
def test_workstream_label_proposal_auto_and_no_later_reassignment(client, signed_in, monkeypatch, auto):
    ws = signed_in['workspaces'][0]['id']
    repo = seed_intake_repo(ws)
    sid = seed_stream(ws, signed_in['user']['id'])
    row = intake_issue()
    row['labels'].append({'name': 'workstream:platform'})
    gh = IntakeGitHub('t')
    gh.permission = 'write'
    gh.rows = {'acme/app': [row]}
    async def issues(full_name, labels='', since=None):
        return gh.rows[full_name] if labels == ('factory:auto' if auto else 'factory') else []
    gh.issues = issues
    monkeypatch.setattr(signals_module, 'GitHub', lambda token: gh)
    assert sync_intake(ws)['accepted'] == int(auto)
    with session_scope() as db:
        sig = db.query(Signal).one()
        sig_id = sig.id
        assert sig.data['proposed_workstream_id'] == sid
        if not auto:
            assert sig.status == 'new' and db.query(Outcome).count() == 0
    for _ in range(2):
        response = client.post(f'/api/workspaces/{ws}/signals/{sig_id}', json={'action': 'accept'})
        assert response.status_code == 200, response.text
    row['labels'] = [{'name': 'workstream:missing'}]
    sync_intake(ws)
    with session_scope() as db:
        outcome = db.query(Outcome).one()
        assert outcome.workstream_id == sid and outcome.repo_ids == [repo]
        assert db.query(Signal).one().data['proposed_workstream_id'] == sid


@pytest.mark.parametrize('labels,reason', [(['missing'], 'Unknown'), (['platform', 'missing'], 'Conflicting'),
                                          (['archived'], 'Archived')])
@pytest.mark.parametrize('clear', [False, True])
def test_invalid_label_held_until_explicit_assignment(client, signed_in, monkeypatch, labels, reason, clear):
    ws = signed_in['workspaces'][0]['id']
    repo = seed_intake_repo(ws)
    sid = seed_stream(ws, signed_in['user']['id'])
    seed_stream(ws, signed_in['user']['id'], 'archived', state='archived')
    row = intake_issue()
    row['labels'] += [{'name': 'workstream:' + slug} for slug in labels]
    gh = IntakeGitHub('t')
    gh.permission = 'write'
    gh.rows = {'acme/app': [row]}
    monkeypatch.setattr(signals_module, 'GitHub', lambda token: gh)
    for _ in range(2):
        assert sync_intake(ws)['accepted'] == 0
    with session_scope() as db:
        sig = db.query(Signal).one()
        sig_id = sig.id
        assert reason in sig.data['workstream_assignment_reason']
        assert sig.status == 'new' and db.query(Outcome).count() == 0
    path = f'/api/workspaces/{ws}/signals/{sig_id}'
    assert client.post(path, json={'action': 'accept'}).status_code == 422
    for _ in range(2):
        assert client.post(path, json={'action': 'accept', 'workstream_id': None if clear else sid}).status_code == 200
    with session_scope() as db:
        outcome = db.query(Outcome).one()
        assert outcome.workstream_id == (None if clear else sid)
        assert outcome.repo_ids == [repo]


def test_workstream_label_alone_is_not_admitted(signed_in, monkeypatch):
    ws = signed_in['workspaces'][0]['id']
    seed_intake_repo(ws)
    seed_stream(ws, signed_in['user']['id'])
    class GH(IntakeGitHub):
        async def issues(self, full_name, labels='', since=None):
            assert labels in ('factory', 'factory:auto')
            return []  # the issue has only workstream:platform, so matches neither query
    monkeypatch.setattr(signals_module, 'GitHub', GH)
    assert sync_intake(ws) == {'created': 0, 'updated': 0, 'accepted': 0}
    with session_scope() as db:
        assert db.query(Signal).count() == db.query(Outcome).count() == 0


@pytest.mark.parametrize('source_repo', [False, True])
def test_signal_defaults_only_without_source_repo(client, signed_in, source_repo):
    ws = signed_in['workspaces'][0]['id']
    first = seed_intake_repo(ws)
    second = seed_intake_repo(ws, 'acme/second')
    sid = seed_stream(ws, signed_in['user']['id'], default_repo_ids=[second])
    with session_scope() as db:
        sig = Signal(workspace_id=ws, source='feedback', repo_id=first if source_repo else None,
                     fingerprint='feedback', title='Feedback')
        db.add(sig)
        db.flush()
        sig_id = sig.id
    assert client.post(f'/api/workspaces/{ws}/signals/{sig_id}',
                       json={'action': 'accept', 'workstream_id': sid}).status_code == 200
    with session_scope() as db:
        assert db.query(Outcome).one().repo_ids == ([first] if source_repo else [second])


def test_acceptance_during_author_check_does_not_duplicate(client, signed_in, monkeypatch):
    ws = signed_in['workspaces'][0]['id']
    seed_intake_repo(ws)
    sid = seed_stream(ws, signed_in['user']['id'])
    gh = IntakeGitHub('t')
    gh.rows = {'acme/app': [intake_issue()]}
    async def permission(full_name, author):
        with session_scope() as db:
            sig_id = db.query(Signal).one().id
        response = client.post(f'/api/workspaces/{ws}/signals/{sig_id}',
                               json={'action': 'accept', 'workstream_id': sid})
        assert response.status_code == 200, response.text
        return 'write'
    gh.repository_author_permission = permission
    monkeypatch.setattr(signals_module, 'GitHub', lambda token: gh)
    assert sync_intake(ws)['accepted'] == 0
    with session_scope() as db:
        assert db.query(Outcome).one().workstream_id == sid


def test_later_sync_proposal_is_used_by_acceptance_during_permission_check(client, signed_in, monkeypatch):
    """A poll cannot overwrite the accepted assignment using its older in-memory signal."""
    ws = signed_in['workspaces'][0]['id']
    seed_intake_repo(ws)
    owner = signed_in['user']['id']
    seed_stream(ws, owner)
    second = seed_stream(ws, owner, 'second')
    outer = IntakeGitHub('t')
    row = intake_issue()
    row['labels'].append({'name': 'workstream:platform'})
    outer.rows = {'acme/app': [row]}
    inner = IntakeGitHub('t')
    changed = intake_issue()
    changed['labels'] = [{'name': 'workstream:second'}]
    async def proposed(full_name, labels='', since=None):
        return [changed] if labels == 'factory' else []
    inner.issues = proposed
    async def permission(full_name, author):
        monkeypatch.setattr(signals_module, 'GitHub', lambda token: inner)
        with SessionLocal() as poll:
            assert (await signals_module.sync_github(poll, ws, 'fake-token'))['accepted'] == 0
        with session_scope() as db:
            sig_id = db.query(Signal).one().id
        response = client.post(f'/api/workspaces/{ws}/signals/{sig_id}', json={'action': 'accept'})
        assert response.status_code == 200, response.text
        return 'write'
    outer.repository_author_permission = permission
    monkeypatch.setattr(signals_module, 'GitHub', lambda token: outer)
    assert sync_intake(ws)['accepted'] == 0
    with session_scope() as db:
        assert db.query(Outcome).one().workstream_id == second


def test_foreign_workstream_label_is_unknown_in_source_workspace(client, signed_in, monkeypatch):
    from fastapi.testclient import TestClient
    ws = signed_in['workspaces'][0]['id']
    seed_intake_repo(ws)
    other = TestClient(client.app)
    other.post('/auth/dev', json={'email': 'foreign-label@elsewhere.test', 'organization': 'Elsewhere'})
    me = other.get('/api/me').json()
    seed_stream(me['workspaces'][0]['id'], me['user']['id'], 'private')
    gh = IntakeGitHub('t')
    gh.permission = 'write'
    row = intake_issue()
    row['labels'].append({'name': 'workstream:private'})
    gh.rows = {'acme/app': [row]}
    monkeypatch.setattr(signals_module, 'GitHub', lambda token: gh)
    assert sync_intake(ws)['accepted'] == 0
    with session_scope() as db:
        sig = db.query(Signal).filter_by(workspace_id=ws).one()
        assert 'Unknown' in sig.data['workstream_assignment_reason']
        assert sig.data['proposed_workstream_id'] is None
        assert db.query(Outcome).count() == 0


def test_concurrent_acceptance_creates_one_outcome(client, signed_in):
    from concurrent.futures import ThreadPoolExecutor
    from threading import Barrier
    ws = signed_in['workspaces'][0]['id']
    repo = seed_intake_repo(ws)
    with session_scope() as db:
        sig = Signal(workspace_id=ws, repo_id=repo, source='feedback', title='Concurrent feedback', fingerprint='once')
        db.add(sig)
        db.flush()
        sid = sig.id
    barrier = Barrier(2)
    def accept():
        barrier.wait()
        response = client.post(f'/api/workspaces/{ws}/signals/{sid}', json={'action': 'accept'})
        assert response.status_code == 200, response.text
        return response.json()['outcome_id']
    with ThreadPoolExecutor(max_workers=2) as executor:
        ids = list(executor.map(lambda _: accept(), range(2)))
    assert ids[0] == ids[1]
    with session_scope() as db:
        assert db.query(Outcome).count() == 1


def test_shared_triage_preserves_priority_and_requires_assignment_for_held_signal(client, signed_in):
    from aifactory.models import User
    ws = signed_in["workspaces"][0]["id"]
    repo = seed_intake_repo(ws)
    sid = seed_stream(ws, signed_in["user"]["id"])
    with session_scope() as db:
        sig = Signal(workspace_id=ws, repo_id=repo, source="github_issue", fingerprint="shared-triage",
                     title="Held proposal", data={"priority": 0, "proposed_workstream_id": sid,
                     "workstream_assignment_reason": "Choose an active workstream or No workstream"})
        db.add(sig)
        db.flush()
        signal_id = sig.id
        user = db.get(User, signed_in["user"]["id"])
        with pytest.raises(ValueError, match="Choose an active workstream"):
            signals_module.triage(db, sig, user, "accept")
        assert db.query(Outcome).count() == 0
    response = client.post(f"/api/workspaces/{ws}/signals/{signal_id}",
                           json={"action": "accept", "workstream_id": None})
    assert response.status_code == 200, response.text
    with session_scope() as db:
        outcome = db.query(Outcome).one()
        assert outcome.priority == 0 and outcome.workstream_id is None
