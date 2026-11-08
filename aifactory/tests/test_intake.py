from __future__ import annotations

import asyncio
import json

import pytest

from aifactory import auth as auth_module
from aifactory import signals as signals_module
from aifactory.bootstrap import apply_bootstrap
from aifactory.db import SessionLocal, init_engine, session_scope
from aifactory.github import GitHubError
from aifactory.models import Event, IssueAcknowledgementState, Membership, Organization, Outcome, Repository, Signal, Workspace


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


def test_forbidden_comment_disables_workspace_but_preserves_intake(monkeypatch):
    with session_scope() as s:
        org = Organization(name="Acme")
        s.add(org)
        s.flush()
        first = Workspace(org_id=org.id, name="First", slug="first", settings={}, outcome_seq=1)
        second = Workspace(org_id=org.id, name="Second", slug="second", settings={})
        s.add_all([first, second])
        s.flush()
        repos = [Repository(workspace_id=first.id, full_name="acme/one", default_branch="main", config={}),
                 Repository(workspace_id=first.id, full_name="acme/two", default_branch="main", config={}),
                 Repository(workspace_id=second.id, full_name="acme/other", default_branch="main", config={})]
        s.add_all(repos)
        s.flush()
        s.add(Outcome(workspace_id=first.id, number=1, title="Existing", brief="https://gh/acme/one/3",
                      repo_ids=[repos[0].id], status="running"))
        first_id, second_id = first.id, second.id

    comments = []
    current = {"extra": False}

    class FakeGH:
        def __init__(self, token):
            pass

        async def issues(self, full_name, labels="", since=None):
            numbers = ([1, 2, 3] if full_name == "acme/one" else [4]) if labels == "factory:auto" else [5]
            if current["extra"] and labels == "factory:auto":
                numbers = [6]
            return [{"number": number, "title": f"Issue {number}", "body": "",
                     "html_url": f"https://gh/{full_name}/{number}", "user": {}} for number in numbers]

        async def comment(self, full_name, number, body):
            comments.append((full_name, number))
            raise GitHubError("forbidden", 403)

        async def request(self, *args, **kwargs):
            return {"workflow_runs": []}

    monkeypatch.setattr(signals_module, "GitHub", FakeGH)
    with SessionLocal() as db:
        result = asyncio.run(signals_module.sync_github(db, first_id, "secret"))
        assert result["accepted"] == 3
    current["extra"] = True
    with SessionLocal() as db:
        asyncio.run(signals_module.sync_github(db, first_id, "secret"))
    assert comments == [("acme/one", 1)]
    with session_scope() as s:
        events = s.query(Event).filter_by(workspace_id=first_id, kind="issue_acknowledgements_disabled").all()
        assert len(events) == 1 and events[0].level == "warn"
        assert "disabled" in events[0].message and "cannot write issues" in events[0].message
        assert "secret" not in events[0].message and "secret" not in json.dumps(events[0].data)
        assert s.get(IssueAcknowledgementState, first_id).disabled
        assert s.get(IssueAcknowledgementState, second_id) is None
        assert s.query(Signal).filter_by(workspace_id=first_id, external_id="acme/one#5").one().status == "new"
        linked = s.query(Signal).filter_by(workspace_id=first_id, external_id="acme/one#3").one()
        assert linked.status == "accepted" and s.get(Outcome, linked.outcome_id).title == "Existing"
    with SessionLocal() as db:
        asyncio.run(signals_module.sync_github(db, second_id, "other"))
    assert comments == [("acme/one", 1), ("acme/other", 6)]


def test_other_comment_errors_do_not_disable_and_fetch_errors_propagate(monkeypatch):
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
            return [{"number": number, "title": str(number), "html_url": f"https://gh/{number}"}
                    for number in (1, 2, 3)] if labels == "factory:auto" else []

        async def comment(self, full_name, number, body):
            comments.append(number)
            if number == 1:
                raise GitHubError("not found", 404)
            if number == 2:
                raise RuntimeError("network")

        async def request(self, *args, **kwargs):
            return {}

    monkeypatch.setattr(signals_module, "GitHub", FakeGH)
    with SessionLocal() as db:
        asyncio.run(signals_module.sync_github(db, ws_id, "token"))
    assert comments == [1, 2, 3]
    with session_scope() as s:
        assert s.get(IssueAcknowledgementState, ws_id) is None
    async def fail(*args, **kwargs):
        raise GitHubError("fetch failed", 403)
    monkeypatch.setattr(FakeGH, "issues", fail)
    with SessionLocal() as db, pytest.raises(GitHubError, match="fetch failed"):
        asyncio.run(signals_module.sync_github(db, ws_id, "token"))


def test_ci_signals_roll_back_when_later_repository_fetch_fails(monkeypatch):
    with session_scope() as s:
        org = Organization(name="Acme")
        s.add(org)
        s.flush()
        ws = Workspace(org_id=org.id, name="Acme", slug="acme", settings={})
        s.add(ws)
        s.flush()
        s.add_all([Repository(workspace_id=ws.id, full_name=name, default_branch="main", config={})
                   for name in ("acme/first", "acme/second")])
        ws_id = ws.id

    class FakeGH:
        def __init__(self, token):
            pass

        async def issues(self, full_name, labels="", since=None):
            if full_name == "acme/second":
                raise GitHubError("fetch failed", 403)
            return []

        async def request(self, method, path, params):
            return {"workflow_runs": [{"name": "CI", "html_url": "https://gh/ci"}]} if params["status"] == "failure" else {}

    monkeypatch.setattr(signals_module, "GitHub", FakeGH)
    with SessionLocal() as db, pytest.raises(GitHubError, match="fetch failed"):
        asyncio.run(signals_module.sync_github(db, ws_id, "token"))
    with session_scope() as s:
        assert s.query(Signal).filter_by(workspace_id=ws_id).count() == 0


def test_acknowledgement_state_survives_engine_restart(monkeypatch):
    with session_scope() as s:
        org = Organization(name="Acme")
        s.add(org)
        s.flush()
        ws = Workspace(org_id=org.id, name="Acme", slug="acme", settings={})
        s.add(ws)
        s.flush()
        s.add(Repository(workspace_id=ws.id, full_name="acme/app", default_branch="main", config={}))
        ws_id = ws.id
    attempts = []

    class FakeGH:
        def __init__(self, token):
            pass

        async def issues(self, full_name, labels="", since=None):
            return [{"number": len(attempts) + 1, "title": "Issue", "html_url": f"https://gh/{len(attempts) + 1}"}] if labels == "factory:auto" else []

        async def comment(self, full_name, number, body):
            attempts.append(number)
            raise GitHubError("forbidden", 403)

        async def request(self, *args, **kwargs):
            return {}

    monkeypatch.setattr(signals_module, "GitHub", FakeGH)
    with SessionLocal() as db:
        asyncio.run(signals_module.sync_github(db, ws_id, "token"))
    init_engine()
    with SessionLocal() as db:
        assert asyncio.run(signals_module.sync_github(db, ws_id, "token"))["accepted"] == 1
    assert attempts == [1]
    with session_scope() as s:
        assert s.get(IssueAcknowledgementState, ws_id).disabled
        assert s.query(Event).filter_by(workspace_id=ws_id, kind="issue_acknowledgements_disabled").count() == 1
