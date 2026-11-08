"""Workspaces are isolated: another organization's member can neither see nor change anything,
and object ids from one workspace never resolve through another workspace's routes."""

from __future__ import annotations

from fastapi.testclient import TestClient

from aifactory.db import session_scope
from aifactory.models import Artifact, Decision, Outcome, Plan, Repository, Signal, Task


def _seed(ws_id: str) -> dict[str, str]:
    with session_scope() as s:
        repo = Repository(workspace_id=ws_id, full_name="acme/secret-app", config={"context": "internal notes"})
        s.add(repo)
        s.flush()
        outcome = Outcome(workspace_id=ws_id, number=1, title="Acme roadmap item", brief="", repo_ids=[repo.id],
                          status="running")
        s.add(outcome)
        s.flush()
        plan = Plan(outcome_id=outcome.id, status="approved", content={})
        s.add(plan)
        s.flush()
        task = Task(workspace_id=ws_id, outcome_id=outcome.id, plan_id=plan.id, key="T1", title="t", repo_id=repo.id,
                    status="needs_input")
        s.add(task)
        s.flush()
        artifact = Artifact(workspace_id=ws_id, outcome_id=outcome.id, task_id=task.id, kind="diff", name="d",
                            content="secret diff")
        decision = Decision(workspace_id=ws_id, outcome_id=outcome.id, task_id=task.id, kind="question", title="q",
                            options=[{"value": "answer", "label": "Answer"}])
        signal = Signal(workspace_id=ws_id, source="improvement", fingerprint="fp", title="p",
                        data={"action": {"type": "settings", "set": {"autonomy.merge": "auto"}}})
        s.add_all([artifact, decision, signal])
        s.flush()
        return {"repo": repo.id, "outcome": outcome.id, "task": task.id, "artifact": artifact.id,
                "decision": decision.id, "signal": signal.id}


def test_other_organizations_cannot_reach_a_workspace_or_its_objects(client):
    assert client.post("/auth/dev", json={"email": "a@acme.test", "name": "A", "organization": "Acme"}).status_code == 200
    a_ws = client.get("/api/me").json()["workspaces"][0]["id"]
    ids = _seed(a_ws)

    other = TestClient(client.app)
    assert other.post("/auth/dev", json={"email": "b@globex.test", "name": "B", "organization": "Globex"}).status_code == 200
    b_ws = other.get("/api/me").json()["workspaces"][0]["id"]
    assert b_ws != a_ws

    routes = [
        ("patch", "", {"name": "x"}), ("get", "/members", None), ("get", "/credentials", None),
        ("put", "/credentials/github_token", {"value": "ghp_xxxxxxxxxxxxxxxx"}), ("get", "/models", None),
        ("get", "/repositories", None), ("patch", f"/repositories/{ids['repo']}", {"config": {"context": "x"}}),
        ("delete", f"/repositories/{ids['repo']}", None), ("get", "/outcomes", None),
        ("post", "/outcomes", {"title": "xyz", "brief": "y", "repo_ids": [ids["repo"]]}), ("get", f"/outcomes/{ids['outcome']}", None),
        ("post", f"/outcomes/{ids['outcome']}/cancel", None), ("post", f"/tasks/{ids['task']}/cancel", None),
        ("post", f"/tasks/{ids['task']}/take-over", None), ("post", f"/outcomes/{ids['outcome']}/messages", {"body": "x"}),
        ("get", f"/tasks/{ids['task']}/artifacts", None), ("get", f"/artifacts/{ids['artifact']}", None),
        ("get", "/inbox", None), ("post", f"/decisions/{ids['decision']}/resolve", {"resolution": "answer", "comment": "x"}),
        ("get", "/decisions", None), ("get", "/runs", None), ("get", "/stats", None), ("get", "/events", None),
        ("get", "/signals", None), ("post", "/signals/analyze", None), ("post", "/signals", {"title": "spam"}),
        ("post", "/feedback-key", None), ("get", "/feedback-key", None),
        ("post", f"/signals/{ids['signal']}", {"action": "accept"}),
    ]
    leaks = []
    for method, path, body in routes:
        r = getattr(other, method)(f"/api/workspaces/{a_ws}{path}", **({"json": body} if body is not None else {}))
        if r.status_code not in (401, 403, 404):
            leaks.append((method, path, r.status_code))
    assert not leaks, leaks

    # Object ids from Acme never resolve through Globex's own workspace either.
    cross = [
        ("get", f"/outcomes/{ids['outcome']}", None), ("get", f"/tasks/{ids['task']}/artifacts", None),
        ("get", f"/artifacts/{ids['artifact']}", None), ("post", f"/tasks/{ids['task']}/take-over", None),
        ("post", f"/tasks/{ids['task']}/cancel", None), ("post", f"/outcomes/{ids['outcome']}/cancel", None),
        ("post", f"/decisions/{ids['decision']}/resolve", {"resolution": "answer", "comment": "x"}),
        ("post", f"/signals/{ids['signal']}", {"action": "accept"}),
        ("patch", f"/repositories/{ids['repo']}", {"config": {"context": "x"}}),
        ("delete", f"/repositories/{ids['repo']}", None),
    ]
    for method, path, body in cross:
        r = getattr(other, method)(f"/api/workspaces/{b_ws}{path}", **({"json": body} if body is not None else {}))
        if r.status_code not in (400, 404):
            leaks.append(("cross", method, path, r.status_code))
    assert not leaks, leaks

    with session_scope() as s:  # nothing of Acme's changed
        assert s.get(Task, ids["task"]).status == "needs_input"
        assert s.get(Repository, ids["repo"]).config == {"context": "internal notes"}
        assert s.get(Decision, ids["decision"]).status == "open"
        assert s.get(Signal, ids["signal"]).status == "new"

    # And Acme's product-feedback key cannot write into Globex.
    key = client.post(f"/api/workspaces/{a_ws}/feedback-key").json()["key"]
    r = TestClient(client.app).post(f"/api/feedback/{b_ws}", json={"title": "cross-tenant"},
                                    headers={"Authorization": f"Bearer {key}"})
    assert r.status_code == 401
