"""Improvement proposals from delivery history, applying them, and product feedback intake."""

from __future__ import annotations

from datetime import timedelta

from aifactory import insights
from aifactory.db import session_scope, utcnow
from aifactory.models import Decision, Event, Outcome, Plan, Repository, Run, Signal, Task, User, Workspace


def _world(ws_id: str, merge_mode: str = "approval") -> tuple[str, str, str]:
    with session_scope() as s:
        repo = Repository(workspace_id=ws_id, full_name="acme/shop", config={"merge": {"mode": merge_mode},
                                                                            "context": "Use pnpm."})
        s.add(repo)
        s.flush()
        outcome = Outcome(workspace_id=ws_id, number=1, title="Things", brief="", repo_ids=[repo.id], status="running")
        s.add(outcome)
        s.flush()
        plan = Plan(outcome_id=outcome.id, status="approved", content={})
        s.add(plan)
        s.flush()
        user = s.query(User).first()
        return repo.id, outcome.id, plan.id + ":" + (user.id if user else "")


def _task(s, ws_id, repo_id, outcome_id, plan_id, key, **kw) -> Task:
    task = Task(workspace_id=ws_id, outcome_id=outcome_id, plan_id=plan_id, key=key, title=key, repo_id=repo_id, **kw)
    s.add(task)
    s.flush()
    return task


def test_consistent_merge_approvals_propose_auto_merge_and_apply_attributed(client, signed_in):
    ws_id = signed_in["workspaces"][0]["id"]
    repo_id, outcome_id, rest = _world(ws_id)
    plan_id, user_id = rest.split(":")
    with session_scope() as s:
        for i in range(5):
            t = _task(s, ws_id, repo_id, outcome_id, plan_id, f"T{i}", status="completed", pr_number=10 + i,
                      finished_at=utcnow())
            s.add(Decision(workspace_id=ws_id, outcome_id=outcome_id, task_id=t.id, kind="approve_merge", title="Merge?",
                           status="resolved", resolution="approve", resolved_by=user_id,
                           resolved_at=utcnow() - timedelta(hours=5 - i)))
    assert client.post(f"/api/workspaces/{ws_id}/signals/analyze").json()["created"] == 1
    sig = next(x for x in client.get(f"/api/workspaces/{ws_id}/signals").json() if x["source"] == "improvement")
    assert sig["data"]["rule"] == "merge_autonomy" and "5 merges" in sig["body"] and "Ian" in sig["body"]
    assert len(sig["data"]["evidence"]) == 5

    r = client.post(f"/api/workspaces/{ws_id}/signals/{sig['id']}", json={"action": "accept"})
    assert r.status_code == 200 and r.json()["status"] == "accepted"
    with session_scope() as s:
        assert s.get(Repository, repo_id).config["merge"] == {"mode": "auto"}
        applied = s.query(Event).filter(Event.kind == "improvement_applied").one()
        assert applied.actor == user_id
    # The policy changed, so the rule is satisfied; nothing new is proposed and the record stays applied.
    assert client.post(f"/api/workspaces/{ws_id}/signals/analyze").json() == {"created": 0, "updated": 0, "retired": 0}


def test_a_rejected_merge_breaks_the_streak(client, signed_in):
    ws_id = signed_in["workspaces"][0]["id"]
    repo_id, outcome_id, rest = _world(ws_id)
    plan_id, user_id = rest.split(":")
    with session_scope() as s:
        for i in range(6):
            t = _task(s, ws_id, repo_id, outcome_id, plan_id, f"T{i}", status="completed")
            s.add(Decision(workspace_id=ws_id, task_id=t.id, kind="approve_merge", title="Merge?", status="resolved",
                           resolution="reject" if i == 3 else "approve", resolved_by=user_id,
                           resolved_at=utcnow() - timedelta(hours=10 - i)))
    with session_scope() as s:
        assert [p.rule for p in insights.proposals(s, ws_id)] == []


def test_recurring_review_findings_become_an_editable_team_note(client, signed_in):
    ws_id = signed_in["workspaces"][0]["id"]
    repo_id, outcome_id, rest = _world(ws_id, merge_mode="auto")
    plan_id, _ = rest.split(":")
    with session_scope() as s:
        for i, text in enumerate(["Missing migration for the new column", "No test covers the empty cart case",
                                  "Missing migration for the new column"]):
            t = _task(s, ws_id, repo_id, outcome_id, plan_id, f"T{i}", status="completed")
            s.add(Run(workspace_id=ws_id, outcome_id=outcome_id, task_id=t.id, role="verifier", harness="native",
                      status="succeeded", result={"verdict": "fail", "summary": "x", "criteria": [],
                                                  "findings": [{"severity": "blocker", "description": text}]}))
    client.post(f"/api/workspaces/{ws_id}/signals/analyze")
    sig = next(x for x in client.get(f"/api/workspaces/{ws_id}/signals").json() if x["source"] == "improvement")
    assert sig["data"]["rule"] == "review_themes"
    note = sig["data"]["action"]["note"]
    assert note.count("Missing migration") == 1 and "empty cart" in note

    edited = "Every schema change ships with a migration and a test for empty states."
    client.post(f"/api/workspaces/{ws_id}/signals/{sig['id']}", json={"action": "accept", "note": edited})
    with session_scope() as s:
        assert s.get(Repository, repo_id).config["context"] == "Use pnpm.\n\n" + edited


def test_ignored_proposal_stays_quiet_until_the_evidence_grows(client, signed_in):
    ws_id = signed_in["workspaces"][0]["id"]
    repo_id, outcome_id, rest = _world(ws_id, merge_mode="auto")
    plan_id, _ = rest.split(":")

    def fail_check(n):
        with session_scope() as s:
            for i in range(n):
                t = _task(s, ws_id, repo_id, outcome_id, plan_id, f"C{i}{n}", status="completed")
                s.add(Event(workspace_id=ws_id, outcome_id=outcome_id, task_id=t.id, kind="check_result", level="warn",
                            message="✗ npm run lint", data={"command": "npm run lint", "exit_code": 1}))

    fail_check(3)
    assert client.post(f"/api/workspaces/{ws_id}/signals/analyze").json()["created"] == 1
    sig = next(x for x in client.get(f"/api/workspaces/{ws_id}/signals").json() if x["source"] == "improvement")
    assert "npm run lint" in sig["title"]
    client.post(f"/api/workspaces/{ws_id}/signals/{sig['id']}", json={"action": "ignore"})
    fail_check(1)  # 4 failures: same evidence band (basis 4//3 == 1)
    client.post(f"/api/workspaces/{ws_id}/signals/analyze")
    with session_scope() as s:
        assert s.get(Signal, sig["id"]).status == "ignored"
    fail_check(2)  # 6 failures: materially more evidence
    client.post(f"/api/workspaces/{ws_id}/signals/analyze")
    with session_scope() as s:
        row = s.get(Signal, sig["id"])
        assert row.status == "new" and "6 times" in row.body


def test_settings_proposal_applies_to_workspace_settings(client, signed_in):
    ws_id = signed_in["workspaces"][0]["id"]
    repo_id, outcome_id, rest = _world(ws_id, merge_mode="auto")
    plan_id, _ = rest.split(":")
    with session_scope() as s:
        for _ in range(2):
            s.add(Run(workspace_id=ws_id, outcome_id=outcome_id, role="builder", harness="native", status="failed",
                      cost_usd=8.01, error="Cost budget of $8.00 reached"))
    client.post(f"/api/workspaces/{ws_id}/signals/analyze")
    sig = next(x for x in client.get(f"/api/workspaces/{ws_id}/signals").json() if x["source"] == "improvement")
    assert sig["data"]["action"] == {"type": "settings", "set": {"budgets.run_usd": 12.0}, "label": "Raise to $12.00"}
    client.post(f"/api/workspaces/{ws_id}/signals/{sig['id']}", json={"action": "accept"})
    with session_scope() as s:
        settings = s.get(Workspace, ws_id).settings
        assert settings["budgets"]["run_usd"] == 12.0


def test_product_feedback_is_ingested_with_a_workspace_key(client, signed_in):
    ws_id = signed_in["workspaces"][0]["id"]
    _world(ws_id)
    assert client.post(f"/api/feedback/{ws_id}", json={"title": "Export is broken"}).status_code == 401
    key = client.post(f"/api/workspaces/{ws_id}/feedback-key").json()["key"]
    assert client.get(f"/api/workspaces/{ws_id}/feedback-key").json()["configured"] is True
    anonymous = client.__class__(client.app)  # a product server, not a signed-in browser
    headers = {"Authorization": f"Bearer {key}"}
    body = {"title": "Export to CSV is broken", "body": "Clicking export does nothing", "repo": "acme/shop",
            "source_id": "fb-1", "reporter": "a customer"}
    first = anonymous.post(f"/api/feedback/{ws_id}", json=body, headers=headers)
    assert first.status_code == 200 and first.json()["count"] == 1
    again = anonymous.post(f"/api/feedback/{ws_id}", json=body, headers=headers).json()
    assert again["id"] == first.json()["id"] and again["count"] == 2
    assert anonymous.post(f"/api/feedback/{ws_id}", json=body,
                          headers={"Authorization": "Bearer aff_wrong"}).status_code == 401
    with session_scope() as s:
        sig = s.get(Signal, first.json()["id"])
        assert sig.source == "feedback" and sig.repo_id and "a customer" in sig.body
    # Members can log feedback by hand as well.
    r = client.post(f"/api/workspaces/{ws_id}/signals", json={"title": "Search feels slow"})
    assert r.status_code == 200 and r.json()["source"] == "feedback"


def test_existing_databases_gain_new_nullable_columns(tmp_path):
    import sqlite3

    from sqlalchemy import inspect

    from aifactory import db

    path = tmp_path / "old.db"
    con = sqlite3.connect(path)
    con.execute("CREATE TABLE signals (id VARCHAR(32) PRIMARY KEY, workspace_id VARCHAR(32), repo_id VARCHAR(32),"
                " source VARCHAR(30), external_id VARCHAR(200), fingerprint VARCHAR(64), title VARCHAR(300), body TEXT,"
                " url VARCHAR(400), count INTEGER, status VARCHAR(20), outcome_id VARCHAR(32), first_seen_at DATETIME,"
                " last_seen_at DATETIME)")
    con.execute("INSERT INTO signals (id, workspace_id, source, fingerprint, title) VALUES ('s1','w','feedback','f','t')")
    con.commit()
    con.close()
    engine = db.init_engine(f"sqlite:///{path}")
    assert "data" in {c["name"] for c in inspect(engine).get_columns("signals")}
    with db.session_scope() as s:
        assert s.get(Signal, "s1").data is None


def test_long_strings_are_clipped_to_column_length(client, signed_in):
    from aifactory.models import Artifact

    with session_scope() as s:
        s.add(Artifact(workspace_id="w", kind="check", name="x" * 900, content="ok"))
    with session_scope() as s:
        name = s.query(Artifact).one().name
        assert len(name) == 300 and name.endswith("…")
