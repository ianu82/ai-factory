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
        if path == "/graphql":
            number, after = json["variables"]["number"], json["variables"].get("after")
            # PR 10: page 1 holds only resolved/outdated threads; the open bot finding is on page 2.
            pages = {10: [[(True, False, "lead"), (False, True, "lead")], [(False, False, "chatgpt-codex-connector[bot]")]],
                     11: [[(False, False, "lead")]]}.get(number, [[]])
            index = int(after or 0)
            return {"data": {"repository": {"pullRequest": {"reviewThreads": {
                "nodes": [{"isResolved": r, "isOutdated": o, "comments": {"nodes": [{"author": {"login": a}}]}}
                          for r, o, a in pages[index]],
                "pageInfo": {"hasNextPage": index + 1 < len(pages), "endCursor": str(index + 1)}}}}}}
        if path.endswith("/pulls") and (params or {}).get("state") == "open":
            return [_pr(1, "Ready one"), _pr(2, "Failing CI"), _pr(3, "Conflicted"), _pr(4, "Still checking"),
                    _pr(5, "Work in progress", draft=True), _pr(6, "Factory awaiting approval"), _pr(7, "Reviewed"),
                    _pr(10, "Unresolved bot finding"), _pr(11, "Factory revising after review")]
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
        s.add(Task(workspace_id=ws_id, outcome_id=outcome.id, plan_id=plan.id, key="T2", title="t2", repo_id=repo.id,
                   status="queued", stage="revise", pr_number=11))

    body = client.get(f"/api/workspaces/{ws_id}/pulls").json()
    by_number = {r["number"]: r for r in body["items"]}
    assert {n: r["group"] for n, r in by_number.items()} == {
        1: "ready", 2: "attention", 3: "attention", 4: "checks", 5: "draft", 6: "ready", 7: "attention", 10: "attention",
        11: "checks"}
    assert by_number[10]["reason"] == "1 unresolved review comment from chatgpt-codex-connector"
    assert by_number[11]["reason"] == "The Factory is revising this"
    assert by_number[2]["reason"] == "Checks failed: test"
    assert by_number[3]["reason"].startswith("Merge conflict")
    assert by_number[7]["reason"] == "Changes requested by lead"
    assert "needs approval in AI Factory" in by_number[6]["reason"]
    link = by_number[6]["factory"]
    assert link["outcome_number"] == 12 and link["task_key"] == "T1" and link["decision_id"]
    assert by_number[1]["detail"]["additions"] == 10 and by_number[1]["author"]["avatar"].startswith("https://")
    assert body["counts"] == {"ready": 2, "attention": 4, "checks": 2, "draft": 1}
    assert [r["group"] for r in body["items"]][:2] == ["ready", "ready"]  # ready to merge first

    calls = len(FakeGitHub.calls)
    client.get(f"/api/workspaces/{ws_id}/pulls")  # served from the short cache
    assert len(FakeGitHub.calls) == calls

    merged = client.get(f"/api/workspaces/{ws_id}/pulls?state=merged").json()["items"]
    assert [(r["number"], r["group"]) for r in merged] == [(8, "merged")]
    ready = client.get(f"/api/workspaces/{ws_id}/pulls?state=ready").json()["items"]
    assert sorted(r["number"] for r in ready) == [1, 6]


def test_a_pr_with_more_review_threads_than_can_be_read_is_never_ready():
    assert pulls.classify({"detail": {"threads_unchecked": True, "mergeable_state": "clean"},
                           "ci": {"state": "success"}}) == (
        "attention", "Too many review threads to check; review them on GitHub")


def test_pull_requests_need_workspace_membership(client, signed_in):
    from fastapi.testclient import TestClient

    ws_id = signed_in["workspaces"][0]["id"]
    other = TestClient(client.app)
    other.post("/auth/dev", json={"email": "x@other.test", "organization": "Other"})
    assert other.get(f"/api/workspaces/{ws_id}/pulls").status_code == 404


class ChangingGitHub(FakeGitHub):
    """One PR whose CI and mergeability change without the PR itself changing."""
    ci_state = "success"
    mergeable = "clean"
    open_count = 1

    async def request(self, method, path, params=None, json=None):
        if path == "/graphql":
            return {"data": {"repository": {"pullRequest": {"reviewThreads": {"nodes": []}}}}}
        if path.endswith("/pulls"):
            page = (params or {}).get("page", 1)
            if (params or {}).get("state") != "open":
                return []
            total = ChangingGitHub.open_count
            start = (page - 1) * 100
            return [_pr(n, f"PR {n}") for n in range(start + 1, min(total, start + 100) + 1)]
        if path.endswith("/reviews"):
            return []
        return {"additions": 1, "deletions": 1, "changed_files": 1, "mergeable_state": ChangingGitHub.mergeable}

    async def checks(self, repo, sha):
        state = ChangingGitHub.ci_state
        return CheckSummary(state=state, checks=[{"name": "test", "status": "completed",
                                                  "conclusion": "failure" if state == "failure" else "success"}])


def _setup(ws_id, monkeypatch, cls):
    monkeypatch.setattr(pulls, "GitHub", cls)
    monkeypatch.setattr(pulls, "resolve_token", lambda ws: "token")
    for cache in (pulls._cache, pulls._details, pulls._checks):
        cache.clear()
    with session_scope() as s:
        s.add(Repository(workspace_id=ws_id, full_name="acme/app"))


def test_a_rerun_or_a_moved_base_shows_up_on_refresh(client, signed_in, monkeypatch):
    ws_id = signed_in["workspaces"][0]["id"]
    _setup(ws_id, monkeypatch, ChangingGitHub)
    ChangingGitHub.ci_state, ChangingGitHub.mergeable, ChangingGitHub.open_count = "success", "clean", 1
    assert client.get(f"/api/workspaces/{ws_id}/pulls").json()["items"][0]["group"] == "ready"

    ChangingGitHub.ci_state = "failure"  # checks re-run on the same commit and now fail
    row = client.get(f"/api/workspaces/{ws_id}/pulls?refresh=true").json()["items"][0]
    assert row["group"] == "attention" and row["reason"] == "Checks failed: test"

    ChangingGitHub.ci_state, ChangingGitHub.mergeable = "success", "dirty"  # the base branch moved under it
    row = client.get(f"/api/workspaces/{ws_id}/pulls?refresh=true").json()["items"][0]
    assert row["group"] == "attention" and row["reason"].startswith("Merge conflict")


def test_settled_ci_results_expire(monkeypatch):
    import asyncio

    ChangingGitHub.ci_state = "success"
    pulls._checks.clear()
    asyncio.run(pulls._ci(ChangingGitHub("t"), "acme/app", "abc"))
    ChangingGitHub.ci_state = "failure"
    assert asyncio.run(pulls._ci(ChangingGitHub("t"), "acme/app", "abc"))["state"] == "success"  # within the TTL
    monkeypatch.setattr(pulls, "SETTLED_TTL_SECONDS", 0)
    assert asyncio.run(pulls._ci(ChangingGitHub("t"), "acme/app", "abc"))["state"] == "failure"


def test_github_blocking_wins_over_a_pending_factory_approval():
    row = {"factory": {"status": "awaiting_approval"}, "ci": {"state": "success"},
           "detail": {"mergeable_state": "blocked"}}
    assert pulls.classify(row)[0] == "attention"


def test_open_pull_requests_are_paginated_and_truncation_is_said(client, signed_in, monkeypatch):
    ws_id = signed_in["workspaces"][0]["id"]
    _setup(ws_id, monkeypatch, ChangingGitHub)
    ChangingGitHub.ci_state, ChangingGitHub.mergeable, ChangingGitHub.open_count = "success", "clean", 150
    body = client.get(f"/api/workspaces/{ws_id}/pulls").json()
    assert len(body["items"]) == 150 and body["notes"] == []
    monkeypatch.setattr(pulls, "MAX_PAGES", 1)
    body = client.get(f"/api/workspaces/{ws_id}/pulls?refresh=true").json()
    assert len(body["items"]) == 100 and "acme/app" in body["notes"][0]
