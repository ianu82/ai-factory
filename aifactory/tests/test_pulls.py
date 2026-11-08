"""Pull requests page: GitHub PRs across repositories, grouped by what they need, linked to Factory tasks."""

from __future__ import annotations

from aifactory import pulls
from aifactory.db import session_scope
from aifactory.github import CheckSummary
from aifactory.models import Decision, Outcome, Plan, Repository, Task


def _pr(number, title, *, draft=False, sha=None, merged_at=None, state="open", login="ianu82"):
    return {"number": number, "title": title, "html_url": f"https://github.com/acme/app/pull/{number}", "draft": draft,
            "state": state, "user": {"login": login, "avatar_url": f"https://avatars.test/{login}"},
            "created_at": "2026-11-08T10:00:00Z", "updated_at": f"2026-11-08T1{number % 10}:00:00Z",
            "merged_at": merged_at, "head": {"sha": sha or f"sha{number}"}, "base": {"ref": "main"}}


class FakeGitHub:
    calls: list[str] = []

    def __init__(self, token):
        pass

    async def request(self, method, path, params=None, json=None):
        FakeGitHub.calls.append(path)
        if path.endswith("/pulls") and (params or {}).get("state") == "open":
            return [_pr(1, "Ready one"), _pr(2, "Failing CI"), _pr(3, "Conflicted"), _pr(4, "Still checking"),
                    _pr(5, "Work in progress", draft=True), _pr(6, "Factory awaiting approval"), _pr(7, "Reviewed")]
        if path.endswith("/pulls"):
            return [_pr(8, "Shipped", state="closed", merged_at="2026-11-08T09:00:00Z"),
                    _pr(9, "Closed unmerged", state="closed")]
        if path.endswith("/reviews"):
            number = int(path.split("/")[-2])
            return [{"user": {"login": "lead"}, "state": "CHANGES_REQUESTED"}] if number == 7 else []
        number = int(path.rsplit("/", 1)[1])
        return {"additions": 10 * number, "deletions": number, "changed_files": 1,
                "mergeable_state": "dirty" if number == 3 else "clean"}

    async def checks(self, repo, sha):
        state = {"sha2": "failure", "sha4": "pending"}.get(sha, "success")
        checks = [{"name": "test", "status": "completed" if state != "pending" else "in_progress",
                   "conclusion": {"failure": "failure", "success": "success"}.get(state, "")}]
        return CheckSummary(state=state, checks=checks)


def test_pull_requests_are_grouped_and_linked_to_factory_work(client, signed_in, monkeypatch):
    ws_id = signed_in["workspaces"][0]["id"]
    monkeypatch.setattr(pulls, "GitHub", FakeGitHub)
    monkeypatch.setattr(pulls, "resolve_token", lambda ws: "token")
    pulls._cache.clear()
    pulls._details.clear()
    pulls._checks.clear()
    with session_scope() as s:
        repo = Repository(workspace_id=ws_id, full_name="acme/app")
        s.add(repo)
        s.flush()
        outcome = Outcome(workspace_id=ws_id, number=12, title="t", brief="", repo_ids=[repo.id], status="running")
        s.add(outcome)
        s.flush()
        plan = Plan(outcome_id=outcome.id, status="approved", content={})
        s.add(plan)
        s.flush()
        task = Task(workspace_id=ws_id, outcome_id=outcome.id, plan_id=plan.id, key="T1", title="t", repo_id=repo.id,
                    status="awaiting_approval", stage="merge", pr_number=6)
        s.add(task)
        s.flush()
        s.add(Decision(workspace_id=ws_id, outcome_id=outcome.id, task_id=task.id, kind="approve_merge", title="Merge?"))

    body = client.get(f"/api/workspaces/{ws_id}/pulls").json()
    by_number = {r["number"]: r for r in body["items"]}
    assert {n: r["group"] for n, r in by_number.items()} == {
        1: "ready", 2: "attention", 3: "attention", 4: "checks", 5: "draft", 6: "ready", 7: "attention"}
    assert by_number[2]["reason"] == "Checks failed: test"
    assert by_number[3]["reason"].startswith("Merge conflict")
    assert by_number[7]["reason"] == "Changes requested by lead"
    assert "needs approval in AI Factory" in by_number[6]["reason"]
    link = by_number[6]["factory"]
    assert link["outcome_number"] == 12 and link["task_key"] == "T1" and link["decision_id"]
    assert by_number[1]["detail"]["additions"] == 10 and by_number[1]["author"]["avatar"].startswith("https://")
    assert body["counts"] == {"ready": 2, "attention": 3, "checks": 1, "draft": 1}
    assert [r["group"] for r in body["items"]][:2] == ["ready", "ready"]  # ready to merge first

    calls = len(FakeGitHub.calls)
    client.get(f"/api/workspaces/{ws_id}/pulls")  # served from the short cache
    assert len(FakeGitHub.calls) == calls

    merged = client.get(f"/api/workspaces/{ws_id}/pulls?state=merged").json()["items"]
    assert [(r["number"], r["group"]) for r in merged] == [(8, "merged")]
    ready = client.get(f"/api/workspaces/{ws_id}/pulls?state=ready").json()["items"]
    assert sorted(r["number"] for r in ready) == [1, 6]


def test_pull_requests_need_workspace_membership(client, signed_in):
    from fastapi.testclient import TestClient

    ws_id = signed_in["workspaces"][0]["id"]
    other = TestClient(client.app)
    other.post("/auth/dev", json={"email": "x@other.test", "organization": "Other"})
    assert other.get(f"/api/workspaces/{ws_id}/pulls").status_code == 404
