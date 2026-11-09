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
        ("put", "/credentials/github_token", {"value": "ghp_xxxxxxxxxxxxxxxx"}), ("get", "/models", None), ("post", "/models/check", None),
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


def test_attachment_workspace_uploader_and_viewer_boundaries(client, signed_in):
    import base64
    from aifactory.models import Membership, Workspace

    ws = signed_in["workspaces"][0]["id"]
    root = f"/api/workspaces/{ws}"
    ids = _seed(ws)
    with session_scope() as s:
        s.get(Workspace, ws).outcome_seq = 1
    body = {"name": "example.txt", "content": base64.b64encode(b"example").decode()}
    staged = client.post(root + "/attachments", json=body).json()["id"]
    linked = client.post(root + "/attachments", json=body).json()["id"]
    outcome_body = {"title": "Attachments outcome", "repo_ids": [ids["repo"]], "attachment_ids": [linked]}
    outcome = client.post(root + "/outcomes", json=outcome_body).json()["id"]

    other = TestClient(client.app)
    assert other.post("/auth/dev", json={"email": "other@example.test", "organization": "Other"}).status_code == 200
    me = other.get("/api/me").json()
    foreign_root = f'/api/workspaces/{me["workspaces"][0]["id"]}'
    for aid in [staged, linked]:
        assert other.get(root + "/artifacts/" + aid).status_code == 404
        assert other.delete(root + "/attachments/" + aid).status_code == 404
        assert other.get(foreign_root + "/artifacts/" + aid).status_code == 404
        assert other.delete(foreign_root + "/attachments/" + aid).status_code == 404
    assert other.post(root + "/attachments", json=body).status_code == 404
    foreign_ids = _seed(me["workspaces"][0]["id"])
    foreign_repo = foreign_ids["repo"]
    assert other.post(foreign_root + "/outcomes", json={**outcome_body, "repo_ids": [foreign_repo],
                                                       "attachment_ids": [staged]}).status_code == 404
    assert other.get(foreign_root + "/outcomes/" + outcome).status_code == 404

    with session_scope() as s:
        member = Membership(workspace_id=ws, user_id=me["user"]["id"], role="viewer")
        s.add(member)
    assert other.post(root + "/attachments", json=body).status_code == 403
    assert other.delete(root + "/attachments/" + staged).status_code == 403
    assert other.get(root + "/artifacts/" + linked).status_code == 200
    assert other.get(root + "/artifacts/" + staged).status_code == 404
    detail = other.get(root + "/outcomes/" + outcome).json()
    assert detail["attachments"][0]["id"] == linked and "content" not in detail["attachments"][0]

    with session_scope() as s:
        s.query(Membership).filter_by(workspace_id=ws, user_id=me["user"]["id"]).one().role = "member"
    assert other.post(root + "/attachments", json=body).status_code == 200
    assert other.delete(root + "/attachments/" + staged).status_code == 404
    assert other.post(root + "/outcomes", json={**outcome_body, "attachment_ids": [staged]}).status_code == 404
    assert other.post(root + "/outcomes", json={**outcome_body, "attachment_ids": [linked]}).status_code == 404
    # A corrupted reference to a foreign outcome must not resolve via its owning workspace.
    with session_scope() as s:
        corrupt = Artifact(workspace_id=ws, outcome_id=foreign_ids["outcome"],
                           kind="attachment", name="bad.txt", content=body["content"], data={})
        s.add(corrupt)
        s.flush()
        corrupt_id = corrupt.id
    assert client.get(root + "/artifacts/" + corrupt_id).status_code == 404


def test_staged_attachment_quota_is_per_uploader_and_workspace(client, signed_in):
    from aifactory.models import Membership

    ws = signed_in["workspaces"][0]["id"]
    root = f"/api/workspaces/{ws}"
    body = {"name": "reference.txt", "content": "cmVmZXJlbmNl"}
    for _ in range(10):
        assert client.post(root + "/attachments", json=body).status_code == 200
    assert client.post(root + "/attachments", json=body).status_code == 413

    other = TestClient(client.app)
    assert other.post("/auth/dev", json={"email": "quota@example.test", "organization": "Other"}).status_code == 200
    me = other.get("/api/me").json()
    other_ws = me["workspaces"][0]["id"]
    with session_scope() as s:
        s.add(Membership(workspace_id=ws, user_id=me["user"]["id"], role="member"))
        s.add(Membership(workspace_id=other_ws, user_id=signed_in["user"]["id"], role="member"))
    # Another uploader in the full workspace still has their own quota.
    assert other.post(root + "/attachments", json=body).status_code == 200
    # The original uploader also has independent capacity in another workspace.
    assert client.post(f"/api/workspaces/{other_ws}/attachments", json=body).status_code == 200
    assert client.post(root + "/attachments", json=body).status_code == 413


def test_model_checks_require_workspace_admin(client, signed_in, monkeypatch):
    from aifactory import model_checks
    from aifactory.models import Membership

    def forbidden(*args):
        raise AssertionError("Authorization must happen before provider access")

    monkeypatch.setattr(model_checks, "resolve_provider", forbidden)
    ws_id = signed_in["workspaces"][0]["id"]
    with session_scope() as s:
        member = s.query(Membership).filter(Membership.workspace_id == ws_id).one()
        member.role = "member"
    assert client.post(f"/api/workspaces/{ws_id}/models/check").status_code == 403
    with session_scope() as s:
        s.query(Membership).filter(Membership.workspace_id == ws_id).one().role = "viewer"
    assert client.post(f"/api/workspaces/{ws_id}/models/check").status_code == 403
    client.post("/auth/logout")
    assert client.post(f"/api/workspaces/{ws_id}/models/check").status_code == 401


def test_scorer_routes_workspace_object_and_paid_judge_boundaries(client, signed_in, monkeypatch):
    from aifactory import scorers
    from aifactory.models import Membership, Run

    ws = signed_in['workspaces'][0]['id']
    ids = _seed(ws)
    with session_scope() as s:
        run = Run(workspace_id=ws, outcome_id=ids['outcome'], role='planner', harness='native', status='succeeded')
        s.add(run)
        s.flush()
        run_id = run.id
    root = f'/api/workspaces/{ws}'
    other = TestClient(client.app)
    other.post('/auth/dev', json={'email': 'judge@example.test', 'organization': 'Other'})
    me = other.get('/api/me').json()
    foreign = f"/api/workspaces/{me['workspaces'][0]['id']}"
    def forbidden(_):
        raise AssertionError('unauthorized judge dispatch')
    monkeypatch.setattr(scorers, 'resolve_provider', forbidden)
    for suffix in ['/scorers', f'/runs/{run_id}/scores', f'/runs/{run_id}/scores?limit=1&offset=1']:
        assert other.get(root + suffix).status_code == 404
    assert other.post(root + f'/runs/{run_id}/scores', json={'scorer_id': 'planner-quality'}).status_code == 404
    assert other.get(foreign + f'/runs/{run_id}/scores').status_code == 404
    assert other.post(foreign + f'/runs/{run_id}/scores', json={'scorer_id': 'planner-quality'}).status_code == 404
    with session_scope() as s:
        s.add(Membership(workspace_id=ws, user_id=me['user']['id'], role='viewer'))
    assert other.get(root + '/scorers').status_code == 200
    assert other.get(root + f'/runs/{run_id}/scores').json() == []
    assert other.get(root + f'/runs/{run_id}/scores?limit=1&offset=1').json() == []
    assert other.post(root + f'/runs/{run_id}/scores', json={'scorer_id': 'planner-quality'}).status_code == 403
    with session_scope() as s:
        from aifactory.models import ScorerResult
        assert s.query(ScorerResult).count() == 0


def test_scorer_cost_routes_tenancy_and_admin_only(client, signed_in):
    from aifactory import scorers
    from aifactory.models import Membership, Run, ScorerAttempt
    from aifactory.db import utcnow

    ws = signed_in['workspaces'][0]['id']
    ids = _seed(ws)
    with session_scope() as s:
        run = Run(workspace_id=ws, outcome_id=ids['outcome'], role='planner', harness='native', status='succeeded')
        s.add(run)
        s.flush()
        score = scorers.reserve(s, run, scorers.BUILTIN)
        attempt = ScorerAttempt(workspace_id=ws, result_id=score.id, provider='openai',
                                cost_known=False, finished_at=utcnow())
        s.add(attempt)
        s.flush()
        suffix = f'/runs/{run.id}/scores/{score.id}/attempts'
        cost_suffix = suffix + f'/{attempt.id}/cost'
    root = f'/api/workspaces/{ws}'
    body = {'cost_usd': .2, 'evidence': 'Provider billing export'}
    other = TestClient(client.app)
    assert other.get(root + suffix).status_code == 401
    assert other.post(root + cost_suffix, json=body).status_code == 401
    other.post('/auth/dev', json={'email': 'cost-auditor@example.test', 'organization': 'Other'})
    me = other.get('/api/me').json()
    foreign = f"/api/workspaces/{me['workspaces'][0]['id']}"
    for prefix in (root, foreign):
        assert other.get(prefix + suffix).status_code == 404
        assert other.post(prefix + cost_suffix, json=body).status_code == 404
    for role in ('viewer', 'member'):
        with session_scope() as s:
            member = s.query(Membership).filter_by(workspace_id=ws, user_id=me['user']['id']).first()
            if member:
                member.role = role
            else:
                s.add(Membership(workspace_id=ws, user_id=me['user']['id'], role=role))
        assert other.get(root + suffix).status_code == 200
        assert other.post(root + cost_suffix, json=body).status_code == 403
    # Object checks apply even for an authorized owner, at every path level.
    assert client.get(root + suffix.replace('/scores/', '/scores/wrong')).status_code == 404
    assert client.post(root + cost_suffix.replace('/attempts/', '/attempts/wrong'), json=body).status_code == 404
    assert client.post(root + cost_suffix, json=body).status_code == 200
