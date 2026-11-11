from __future__ import annotations

import asyncio
from copy import deepcopy
import time
from unittest.mock import AsyncMock

import httpx
import pytest

from aifactory import review_watch as watch
from aifactory.db import session_scope
from aifactory.github import GitHub, GitHubError
from aifactory.models import Decision, Event, Organization, Outcome, Plan, Repository, Signal, Task, Workspace
from aifactory.pipeline.delivery import Stop, TaskRunner
from aifactory.pulls import _factory_links, classify
from .test_reviews import inline, thread, user


def candidate(*, ws=None, status="awaiting_approval", number=1, evidence=None):
    with session_scope() as s:
        if ws is None:
            org = Organization(name="test")
            s.add(org)
            s.flush()
            workspace = Workspace(org_id=org.id, name="test", slug=org.id, settings={"signals": {"auto_sync": False}})
            s.add(workspace)
            s.flush()
            ws = workspace.id
        repo = s.query(Repository).filter_by(workspace_id=ws).first()
        if not repo:
            repo = Repository(workspace_id=ws, full_name="acme/demo")
            s.add(repo)
            s.flush()
        workspace = s.get(Workspace, ws)
        workspace.outcome_seq = max(workspace.outcome_seq, number)
        outcome = Outcome(workspace_id=ws, number=number, title="Work", status="attention")
        s.add(outcome)
        s.flush()
        plan = Plan(outcome_id=outcome.id)
        s.add(plan)
        s.flush()
        task = Task(workspace_id=ws, outcome_id=outcome.id, plan_id=plan.id, repo_id=repo.id, key="T1",
                    title="Work", status=status, stage="merge", head_sha="head", pr_number=number, evidence=evidence or {})
        s.add(task)
        s.flush()
        decision = Decision(workspace_id=ws, outcome_id=outcome.id, task_id=task.id, kind="merge", title="Merge?")
        s.add(decision)
        return task.id, ws


def evidence(tid):
    with session_scope() as s:
        return deepcopy(s.get(Task, tid).evidence)


class Fake:
    installation_bound = False

    def __init__(self):
        self.user = AsyncMock(return_value=user("factory"))
        self.pull = AsyncMock(return_value={"user": user("factory"), "head": {"sha": "head"}, "state": "open"})
        self.reviews = AsyncMock(return_value=[])
        self.review_threads = AsyncMock(return_value=[thread(inline())])
        self.issue_comments = AsyncMock(return_value=[])
        self.repository_author_permission = AsyncMock(return_value="write")
        self._review_pages = AsyncMock(return_value=[])


@pytest.fixture
def setup(monkeypatch):
    fake = Fake()
    credentials = []
    monkeypatch.setattr(watch, "resolve_credential", lambda ws: credentials.append(ws) or ws)
    watcher = watch.ReviewWatcher(client_factory=lambda credential, quota: fake)
    return watcher, fake, credentials


def test_waiting_feedback_no_runner_or_decision_and_ci_merge_preserves_ledger(setup, monkeypatch):
    watcher, fake, _ = setup
    monkeypatch.setenv("AIFACTORY_SIGNAL_INTERVAL_SECONDS", "0")
    tid, ws = candidate(evidence={"review": {"rounds": 1, "active_round": 2, "findings": {"old": {"reply_id": 45}}}})
    runner = TaskRunner(tid, asyncio.Event(), lambda _: asyncio.Lock())  # stale before observation
    for _ in range(3):
        asyncio.run(watcher.poll())
    review = evidence(tid)["review"]
    assert review["count"] == 1 and review["rounds"] == 1 and review["active_round"] == 2
    assert review["findings"]["old"]["reply_id"] == 45
    with session_scope() as s:
        assert s.get(Task, tid).status == "awaiting_approval"
        assert s.query(Decision).count() == 1
        assert s.query(Event).filter_by(kind="review_pending").count() == 1
        assert s.query(Signal).count() == 0
    link = _factory_links(ws)[("acme/demo", 1)]
    assert classify({"factory": link}) == ("attention", "Review comments to address (1)")
    with pytest.raises(Stop):
        asyncio.run(runner._wait_ci())
    after = evidence(tid)["review"]
    assert after == review
    assert fake.user.await_count == 3  # once per cycle, not once per PR


@pytest.mark.parametrize("endorsement", [None, "reply", "reaction"])
def test_outsider_404_keeps_trusted_and_allows_endorsements(setup, endorsement):
    watcher, fake, _ = setup
    tid, _ = candidate()
    outsider = inline(11, login="outsider")
    replies = [inline(12, login="writer")] if endorsement == "reply" else []
    fake.review_threads.return_value = [thread(inline()), thread(outsider, *replies, id="other")]

    async def permission(repo, author):
        if author == "outsider":
            raise GitHubError("Not Found", 404)
        return "write"

    fake.repository_author_permission.side_effect = permission
    if endorsement == "reaction":
        fake._review_pages.return_value = [{"content": "+1", "user": user("writer")}]
    asyncio.run(watcher.poll())
    ids = {f["id"] for f in evidence(tid)["review"]["pending"]}
    assert "comment:10" in ids
    assert ("comment:11" in ids) == (endorsement is not None)


def test_late_old_completed_feedback_restart_edits_and_premerge_filter(setup):
    watcher, fake, _ = setup
    tid, _ = candidate(status="completed", evidence={"review": {"findings": {"comment:10": {"status": "addressed"}}}})
    fake.pull.return_value["merged_at"] = "2020-01-01T00:00:00Z"
    old = inline(11)
    old["createdAt"] = old["updatedAt"] = "2019-01-01T00:00:00Z"
    fake.review_threads.return_value = [thread(inline(), old)]
    for cycle in range(3):
        watcher = watch.ReviewWatcher(client_factory=lambda credential, quota: fake)
        if cycle:
            with session_scope() as s:
                t = s.get(Task, tid)
                ev = deepcopy(t.evidence)
                ev["review"]["prs"]["1"]["polled_at"] = 0
                t.evidence = ev
        asyncio.run(watcher.poll())
    with session_scope() as s:
        sig = s.query(Signal).one()
        assert sig.status == "new" and sig.count == 1
        assert sig.source == "review_feedback" and sig.data["pr"] == 1
        assert s.get(Task, tid).status == "completed"
    # A pre-merge comment edited after merge is late feedback too.
    old["updatedAt"] = "2026-11-10T00:00:00Z"
    with session_scope() as s:
        t = s.get(Task, tid)
        ev = deepcopy(t.evidence)
        ev["review"]["prs"]["1"]["polled_at"] = 0
        t.evidence = ev
    asyncio.run(watcher.poll())
    with session_scope() as s:
        assert s.query(Signal).count() == 2


def test_own_ignored_off_and_addressed(setup):
    watcher, fake, _ = setup
    tid, ws = candidate(evidence={"review": {"findings": {"comment:10": {"status": "addressed"}}}})
    fake.review_threads.return_value = [thread(inline(), inline(11, login="factory"), inline(12, login="noise"))]
    with session_scope() as s:
        repo = s.query(Repository).one()
        repo.config = {"review": {"ignore": ["noise"]}}
    asyncio.run(watcher.poll())
    assert evidence(tid)["review"]["pending"] == []
    fake.pull.reset_mock()
    with session_scope() as s:
        s.query(Repository).one().config = {"review": {"address": "off"}}
    asyncio.run(watcher.poll())
    fake.pull.assert_not_awaited()


def test_isolation_pause_and_partial_failure(setup):
    watcher, fake, credentials = setup
    tid, ws = candidate()
    other, ws2 = candidate()
    paused, ws3 = candidate()
    with session_scope() as s:
        s.get(Workspace, ws3).paused = True
    original = fake.pull.return_value

    async def pull(repo, number):
        # fail the first selected PR; another workspace still succeeds
        if fake.pull.await_count == 1:
            raise GitHubError("unavailable", 503)
        return original

    fake.pull.side_effect = pull
    asyncio.run(watcher.poll())
    assert set(credentials) == {ws, ws2}
    assert "review" not in evidence(paused)
    assert sum(evidence(t).get("review", {}).get("count", 0) for t in (tid, other)) == 1
    assert all(evidence(t)["review"]["prs"]["1"]["polled_at"] for t in (tid, other))


def test_pause_during_network_and_merge_during_collection(setup):
    watcher, fake, _ = setup
    tid, ws = candidate()

    async def comments(repo, number):
        with session_scope() as s:
            s.get(Workspace, ws).paused = True
        return []

    fake.issue_comments.side_effect = comments
    asyncio.run(watcher.poll())
    assert not evidence(tid).get("review", {}).get("pending")
    with session_scope() as s:
        s.get(Workspace, ws).paused = False
    fake.issue_comments.side_effect = None
    fake.pull.side_effect = [fake.pull.return_value, {**fake.pull.return_value, "merged_at": "2020-01-01T00:00:00Z"}]
    asyncio.run(watcher.poll())
    with session_scope() as s:
        assert s.query(Signal).one().status == "new"
        assert s.get(Task, tid).status == "awaiting_approval"


def test_urgent_open_pr_not_displaced_by_merged_backlog_and_credentials_refresh(setup, monkeypatch):
    watcher, fake, _ = setup
    tid, ws = candidate()
    for n in range(2, 23):
        candidate(ws=ws, status="completed", number=n, evidence={"review": {"prs": {str(n): {"merged_at": "2020-01-01T00:00:00Z", "polled_at": 1}}}})
    watcher.batch_size = 1
    tokens = iter(["old", "new"])
    monkeypatch.setattr(watch, "resolve_credential", lambda _: next(tokens))
    seen = []
    watcher.client_factory = lambda token, quota: seen.append(token) or fake
    for _ in range(2):
        asyncio.run(watcher.poll())
    assert seen == ["old", "new"]
    assert sum(call.args[1] == 1 for call in fake.pull.await_args_list) == 4
    assert any(call.args[1] != 1 for call in fake.pull.await_args_list)
    assert fake.user.await_count == 2


def test_head_mismatch_and_errors_rotate_nonurgent_batch(setup):
    watcher, fake, _ = setup
    tids = [candidate(status="running")[0] for _ in range(3)]
    watcher.batch_size = 1
    fake.pull.return_value["head"]["sha"] = "different"
    for _ in tids:
        asyncio.run(watcher.poll())
    assert all(evidence(t)["review"]["prs"]["1"]["polled_at"] for t in tids)


def test_cancel_during_poll_is_propagated(setup):
    watcher, fake, _ = setup
    tid, _ = candidate()
    fake.pull.side_effect = asyncio.CancelledError
    with pytest.raises(asyncio.CancelledError):
        asyncio.run(watcher.poll())
    assert evidence(tid)["review"]["prs"]["1"]["polled_at"]


def test_quota_20_percent_and_low_remaining_pause(monkeypatch):
    now = [10000.0]
    monkeypatch.setattr(watch.time, "time", lambda: now[0])

    async def sleep(seconds):
        now[0] += seconds

    monkeypatch.setattr(watch.asyncio, "sleep", sleep)
    responses = []

    async def request(self, method, path, **kwargs):
        responses.append(path)
        self.observe_response(httpx.Response(200, headers={"x-ratelimit-remaining": str(4999 - len(responses)),
                                                        "x-ratelimit-reset": "13600"}))
        return {}

    monkeypatch.setattr(GitHub, "request", request)
    quota = watch.Quota()
    gh = watch.CourtesyGitHub("token", quota)
    async def cycle():
        for _ in range(2000):
            if now[0] >= 13600:
                break
            try:
                await gh.request("GET", "/pulls")
            except GitHubError:
                break
    asyncio.run(cycle())
    assert len(responses) <= 1000
    quota.resource("core").reset = now[0] + 3600
    quota.resource("core").remaining = 999
    before = len(responses)
    with pytest.raises(GitHubError):
        asyncio.run(gh.request("GET", "/pulls"))
    assert len(responses) == before


def test_late_edit_refreshes_existing_signal_once(setup):
    watcher, fake, _ = setup
    tid, _ = candidate(status="completed")
    fake.pull.return_value["merged_at"] = "2020-01-01T00:00:00Z"
    for iteration in range(3):
        with session_scope() as s:
            t = s.get(Task, tid)
            ev = deepcopy(t.evidence)
            ev.setdefault("review", {}).setdefault("prs", {}).setdefault("1", {})["polled_at"] = 0
            t.evidence = ev
        if iteration == 1:
            fake.review_threads.return_value[0]["comments"]["nodes"][0].update(
                body="Edited finding", updatedAt="2026-11-10T00:00:00Z")
        asyncio.run(watcher.poll())
    with session_scope() as s:
        sig = s.query(Signal).one()
        assert sig.count == 2 and sig.status == "new"
        assert "Edited finding" in sig.body


def test_review_state_exposed_in_outcome_api(setup, signed_in, client):
    watcher, _, _ = setup
    ws = signed_in["workspaces"][0]["id"]
    tid, _ = candidate(ws=ws)
    asyncio.run(watcher.poll())
    with session_scope() as s:
        oid = s.get(Task, tid).outcome_id
    response = client.get(f"/api/workspaces/{ws}/outcomes/{oid}")
    assert response.status_code == 200
    task = next(t for t in response.json()["tasks"] if t["id"] == tid)
    assert task["evidence"]["review"]["count"] == 1


def test_removed_credential_not_reused(setup, monkeypatch):
    watcher, fake, _ = setup
    tid, _ = candidate()
    asyncio.run(watcher.poll())
    fake.pull.reset_mock()

    def removed(_):
        raise GitHubError("No GitHub access")

    monkeypatch.setattr(watch, "resolve_credential", removed)
    asyncio.run(watcher.poll())
    fake.pull.assert_not_awaited()
    assert evidence(tid)["review"]["count"] == 1


def test_error_rotates_batch_and_other_permission_errors_do_not_clear_snapshot(setup):
    watcher, fake, _ = setup
    tids = [candidate(status="running")[0] for _ in range(3)]
    watcher.batch_size = 1
    fake.repository_author_permission.side_effect = GitHubError("Forbidden", 403)
    for _ in tids:
        asyncio.run(watcher.poll())
    assert all(evidence(t)["review"]["prs"]["1"]["polled_at"] for t in tids)
    assert fake.pull.await_count == 3
    assert all(not evidence(t)["review"].get("count") for t in tids)


@pytest.mark.parametrize("batch_size", [2, 20])
def test_full_urgent_lane_cannot_starve_completed_or_active_prs(setup, batch_size):
    watcher, fake, _ = setup
    watcher.batch_size = batch_size
    urgent, ws = candidate()
    urgent_ids = [urgent] + [candidate(ws=ws, number=n)[0] for n in range(2, batch_size + 1)]
    completed, _ = candidate(ws=ws, status="completed", number=batch_size + 1)
    active, _ = candidate(ws=ws, status="running", number=batch_size + 2)
    original = deepcopy(fake.pull.return_value)

    async def pull(repo, number):
        return {**original, **({"merged_at": "2020-01-01T00:00:00Z"} if number == batch_size + 1 else {})}

    fake.pull.side_effect = pull
    # Restart after each cycle: selection fairness must come from persisted state.
    for _ in range(3):
        watcher = watch.ReviewWatcher(batch_size=batch_size, client_factory=lambda credential, quota: fake)
        before = fake.pull.await_count
        asyncio.run(watcher.poll())
        calls = fake.pull.await_args_list[before:]
        assert set(range(1, batch_size + 1)) <= {c.args[1] for c in calls}
        assert len({c.args[1] for c in calls}) <= batch_size + max(1, batch_size // 4)
    assert all(evidence(t)["review"]["count"] == 1 for t in urgent_ids + [active])
    assert evidence(completed)["review"]["prs"][str(batch_size + 1)]["processed_versions"]
    with session_scope() as s:
        assert s.query(Signal).one().count == 1
        assert s.get(Task, completed).status == "completed"


def test_late_feedback_is_actionable_only_by_human(setup, signed_in, client):
    watcher, fake, _ = setup
    ws = signed_in["workspaces"][0]["id"]
    tid, _ = candidate(ws=ws, status="completed")
    fake.pull.return_value["merged_at"] = "2020-01-01T00:00:00Z"
    # Neither source auto-accept settings nor signal sync enablement matter.
    with session_scope() as s:
        s.get(Workspace, ws).settings = {"signals": {"auto_sync": False, "auto_accept": ["review_feedback"]}}
    asyncio.run(watcher.poll())
    response = client.get(f"/api/workspaces/{ws}/signals")
    assert response.status_code == 200
    sig, = response.json()
    assert sig["status"] == "new"  # SignalsPage's actionable section, not Handled
    assert sig["outcome_id"] is None and sig["source"] == "review_feedback"
    response = client.post(f"/api/workspaces/{ws}/signals/{sig['id']}", json={"action": "accept"})
    assert response.status_code == 200, response.text
    assert response.json()["status"] == "accepted" and response.json()["outcome_id"]
    # A later edit refreshes context, never reopens either the task or signal.
    fake.review_threads.return_value[0]["comments"]["nodes"][0]["updatedAt"] = "2026-11-10T00:00:00Z"
    with session_scope() as s:
        task = s.get(Task, tid)
        ev = deepcopy(task.evidence)
        ev["review"]["prs"]["1"]["polled_at"] = 0
        task.evidence = ev
    asyncio.run(watcher.poll())
    with session_scope() as s:
        assert s.query(Signal).one().status == "accepted"
        assert s.get(Task, tid).status == "completed"
        assert s.query(Outcome).count() == 2  # original work plus explicit acceptance


@pytest.mark.parametrize("low_resource", ["core", "graphql"])
def test_resource_budgets_and_low_quota_pause_are_independent(monkeypatch, low_resource):
    now = [10000.0]
    monkeypatch.setattr(watch.time, "time", lambda: now[0])

    async def sleep(seconds):
        now[0] += seconds

    monkeypatch.setattr(watch.asyncio, "sleep", sleep)
    remaining = {"core": 5000, "graphql": 5000}
    calls = []

    async def request(self, method, path, **kwargs):
        assert kwargs["retries"] == 0
        name = "graphql" if path == "/graphql" else "core"
        cost = 101 if name == "graphql" else 1
        remaining[name] -= cost
        calls.append((name, cost))
        self.observe_response(httpx.Response(200, headers={
            "x-ratelimit-resource": name,
            "x-ratelimit-remaining": str(remaining[name]),
            "x-ratelimit-reset": "13600",
        }))
        return {}

    monkeypatch.setattr(GitHub, "request", request)
    quota = watch.Quota()
    for name in remaining:
        quota.resource(name).update(5000, 13600)
    gh = watch.CourtesyGitHub("token", quota)

    async def cycle():
        for _ in range(1000):
            for method, path in (("GET", "/pulls"), ("POST", "/graphql")):
                try:
                    await gh.request(method, path)
                except GitHubError:
                    pass
            if now[0] >= 13600 or quota.resource("graphql").allowance < 101:
                break

    asyncio.run(cycle())
    assert 0 < sum(cost for name, cost in calls if name == "graphql") <= 1000
    assert sum(cost for name, cost in calls if name == "core") <= 1000
    assert quota.resource("graphql").remaining == remaining["graphql"]

    # A healthy response for either resource cannot unpause the other resource,
    # nor can a later healthy reading unpause the same resource before reset.
    gh.observe_response(httpx.Response(200, headers={
        "x-ratelimit-resource": low_resource, "x-ratelimit-remaining": "999", "x-ratelimit-reset": "13600"}))
    for resource in ("core", "graphql"):
        gh.observe_response(httpx.Response(200, headers={
            "x-ratelimit-resource": resource, "x-ratelimit-remaining": "4900", "x-ratelimit-reset": "13600"}))
    before = len(calls)
    path = "/graphql" if low_resource == "graphql" else "/pulls"
    with pytest.raises(GitHubError):
        asyncio.run(gh.request("POST" if low_resource == "graphql" else "GET", path))
    assert len(calls) == before


def test_installation_quota_shared_across_fresh_workspace_bindings(monkeypatch):
    from aifactory.github_app import InstallationCredential

    now = [10000.0]
    monkeypatch.setattr(watch.time, "time", lambda: now[0])

    async def sleep(seconds):
        now[0] += seconds

    monkeypatch.setattr(watch.asyncio, "sleep", sleep)
    # Twenty recorded PRs in distinct workspaces use the same installation.
    for _ in range(20):
        candidate()
    monkeypatch.setattr(watch, "resolve_credential", lambda ws: InstallationCredential(ws, "123"))
    remaining = [5000]
    calls = []

    async def request(self, method, path, **kwargs):
        if now[0] >= 13600:
            raise GitHubError("End of simulated hour")
        remaining[0] -= 1
        calls.append(path)
        self.observe_response(httpx.Response(200, headers={
            "x-ratelimit-resource": "core", "x-ratelimit-remaining": str(remaining[0]),
            "x-ratelimit-reset": "13600"}))
        if path == "/rate_limit":
            return {"resources": {"core": {"remaining": remaining[0], "reset": 13600}}}
        return {"user": user("factory"), "head": {"sha": "different"}}

    monkeypatch.setattr(GitHub, "request", request)
    watcher = watch.ReviewWatcher()

    async def cycles():
        while now[0] < 13600:
            await watcher.poll()
            now[0] += 300

    asyncio.run(cycles())
    assert 20 < len(calls) <= 1000
    assert len(watcher.quotas) == 1
    assert watch.quota_key(InstallationCredential("a", "123")) == watch.quota_key(InstallationCredential("b", "123"))
    assert watch.quota_key(InstallationCredential("a", "124")) not in watcher.quotas
    assert watch.quota_key("secret-token") != "secret-token"


def test_graphql_bootstrap_reports_both_resources_and_missing_headers_spend_reservation(monkeypatch):
    now = [10000.0]
    monkeypatch.setattr(watch.time, "time", lambda: now[0])
    calls = []

    async def request(self, method, path, **kwargs):
        calls.append(path)
        if path == "/rate_limit":
            return {"resources": {name: {"remaining": 5000, "reset": 13600} for name in ("core", "graphql")}}
        return {}

    monkeypatch.setattr(GitHub, "request", request)
    quota = watch.Quota()
    gh = watch.CourtesyGitHub("token", quota)
    asyncio.run(gh.request("POST", "/graphql"))
    assert calls == ["/rate_limit", "/graphql"]
    assert quota.resource("graphql").allowance == 999 - 101
    assert quota.resource("graphql").remaining == 5000 - 101
    assert quota.resource("core").allowance == 999


def test_failed_bootstrap_is_not_repeated_per_client(monkeypatch):
    calls = []

    async def request(self, method, path, **kwargs):
        calls.append(path)
        raise GitHubError("Unavailable", 503)

    monkeypatch.setattr(GitHub, "request", request)
    quota = watch.Quota()
    for _ in range(20):
        gh = watch.CourtesyGitHub("token", quota)
        with pytest.raises(GitHubError):
            asyncio.run(gh.request("POST", "/graphql"))
    assert calls == ["/rate_limit"]


def test_observations_populate_shared_head_keyed_finding_progress(setup):
    watcher, fake, _ = setup
    tid, _ = candidate(evidence={"review": {
        "rounds": 1, "active_round": 2, "active_head": "head",
        "wait_deadlines": {"head": "2026-11-11T00:00:00Z"},
        "findings": {"comment:11": {"head": "head", "round": 2, "status": "disputed",
                                      "commit": "commit", "reply_id": 77, "replied_at": "then"}},
    }})
    fake.review_threads.return_value = [thread(inline(), inline(11))]
    asyncio.run(watcher.poll())
    # FindingProgress is the shared delivery review contract, not a watcher-only
    # pending list. Each newly observed finding has a candidate head and status.
    review = evidence(tid)["review"]
    progress = review["findings"]["comment:10"]
    assert progress == {"finding": review["pending"][0], "head": "head", "round": 1,
                        "status": "pending", "commit": None, "reply_id": None,
                        "replied_at": None, "resolved_at": None,
                        "observed_resolved": False, "observed_resolution": None}
    assert review["findings"]["comment:11"]["reply_id"] == 77
    assert review["findings"]["comment:11"]["status"] == "disputed"
    assert review["active_round"] == 2 and review["active_head"] == "head"
    assert review["wait_deadlines"] == {"head": "2026-11-11T00:00:00Z"}


def test_ci_pending_past_deadline_records_timeout_decision_and_retains_review(setup):
    from datetime import timedelta

    from aifactory.db import utcnow
    from aifactory.github import CheckSummary

    watcher, fake, _ = setup
    tid, _ = candidate(status="running")
    runner = TaskRunner(tid, asyncio.Event(), lambda _: asyncio.Lock())
    # Observe after the runner's snapshot, before its first CI evidence write.
    asyncio.run(watcher.poll())
    review = evidence(tid)["review"]
    with pytest.raises(Stop):
        asyncio.run(runner._wait_ci())
    assert evidence(tid)["review"] == review
    runner.evidence("ci_wait", {**evidence(tid)["ci_wait"],
                               "deadline": (utcnow() - timedelta(seconds=1)).isoformat()})
    gh = Fake()
    gh.checks = AsyncMock(return_value=CheckSummary("pending", []))
    with pytest.raises(Stop):
        asyncio.run(runner.poll_ci(gh))
    with session_scope() as s:
        task = s.get(Task, tid)
        assert task.status == "blocked"
        assert task.evidence["ci_wait"]["timed_out"] is True
        assert task.evidence["review"] == review
        timeout = s.query(Decision).filter(Decision.task_id == tid, Decision.kind == "unblock").one()
        assert "CI did not finish" in timeout.title
        assert {o["value"] for o in timeout.options} == {"retry", "abandon"}


@pytest.mark.parametrize("status", ["awaiting_approval", "handed_over"])
def test_human_waiting_pr_polled_every_cycle_with_large_history(setup, status):
    watcher, fake, _ = setup
    tid, ws = candidate(status=status)
    history = [candidate(ws=ws, status="completed", number=n,
                         evidence={"review": {"prs": {str(n): {
                             "merged_at": "2020-01-01T00:00:00Z", "polled_at": 1}}}})[0]
               for n in range(2, 29)]
    original = deepcopy(fake.pull.return_value)

    async def pull(repo, number):
        return {**original, **({"merged_at": "2020-01-01T00:00:00Z"} if number != 1 else {})}

    fake.pull.side_effect = pull
    merged_seen = set()
    for _ in range(3):
        before = fake.pull.await_count
        asyncio.run(watcher.poll())
        numbers = [c.args[1] for c in fake.pull.await_args_list[before:]]
        assert numbers.count(1) == 2  # collection and fresh merge/head check
        merged = set(numbers) - {1}
        assert not (merged & merged_seen)  # merged PRs never more than once / 15m
        merged_seen |= merged
    assert len(merged_seen) == len(history)
    assert evidence(tid)["review"]["count"] == 1


def test_twenty_prs_on_user_token_share_courtesy_budget_and_identity(monkeypatch):
    now = [10000.0]
    monkeypatch.setattr(watch.time, "time", lambda: now[0])

    async def sleep(seconds):
        now[0] += seconds

    monkeypatch.setattr(watch.asyncio, "sleep", sleep)
    _, ws = candidate()
    for number in range(2, 21):
        candidate(ws=ws, number=number)
    monkeypatch.setattr(watch, "resolve_credential", lambda _: "user-token")
    calls = []
    remaining = [5000]

    async def request(self, method, path, **kwargs):
        remaining[0] -= 1
        calls.append(path)
        self.observe_response(httpx.Response(200, headers={
            "x-ratelimit-resource": "core", "x-ratelimit-remaining": str(remaining[0]),
            "x-ratelimit-reset": "13600"}))
        if path == "/rate_limit":
            return {"resources": {"core": {"remaining": remaining[0], "reset": 13600}}}
        if path == "/user":
            return user("factory")
        return {"user": user("factory"), "head": {"sha": "different"}}

    monkeypatch.setattr(GitHub, "request", request)
    watcher = watch.ReviewWatcher()

    async def cycles():
        while now[0] < 13600:
            before = len(calls)
            await watcher.poll()
            assert calls[before:].count("/user") <= 1
            now[0] += 300

    asyncio.run(cycles())
    assert 20 < len(calls) <= 1000
    # A low-remaining header suspends further observations, including identity.
    quota = watcher.quotas[watch.quota_key("user-token")]
    now[0] = 12000
    quota.headers({"x-ratelimit-resource": "core", "x-ratelimit-remaining": "999",
                   "x-ratelimit-reset": "13600"})
    before = len(calls)
    asyncio.run(watcher.poll())
    assert len(calls) == before


def test_runner_review_patch_keeps_observer_cursors(setup):
    watcher, _, _ = setup
    tid, _ = candidate()
    runner = TaskRunner(tid, asyncio.Event(), lambda _: asyncio.Lock())
    asyncio.run(watcher.poll())
    before = evidence(tid)["review"]
    runner.evidence("review", {"rounds": 1, "active_round": 2})
    after = evidence(tid)["review"]
    assert after["prs"] == before["prs"]
    assert after["findings"] == before["findings"]
    assert after["count"] == before["count"]
    assert after["rounds"] == 1 and after["active_round"] == 2


def test_orchestrator_shutdown_cancels_review_watch_and_vm_sweep():
    from aifactory.pipeline.orchestrator import Orchestrator

    async def shutdown():
        orchestrator = Orchestrator()
        started = set()
        stopped = set()

        async def background(name):
            started.add(name)
            try:
                await asyncio.Event().wait()
            finally:
                stopped.add(name)

        orchestrator._review_task = asyncio.create_task(background("review"))
        orchestrator._vm_sweep_task = asyncio.create_task(background("vm"))
        # Yield to both jobs without a wall-clock sleep or external service.
        await asyncio.sleep(0)
        assert started == {"review", "vm"}
        await orchestrator.stop()
        assert stopped == started
        assert orchestrator._review_task.cancelled()
        assert orchestrator._vm_sweep_task.cancelled()

    asyncio.run(shutdown())


@pytest.mark.parametrize("during_collection", [False, True])
def test_policy_off_clears_observed_hold_preserving_history(setup, during_collection):
    watcher, fake, _ = setup
    tid, ws = candidate()
    asyncio.run(watcher.poll())
    before = evidence(tid)["review"]

    def disable():
        with session_scope() as s:
            s.query(Repository).filter_by(workspace_id=ws).one().config = {"review": {"address": "off"}}

    if during_collection:
        async def comments(repo, number):
            disable()
            return []
        fake.issue_comments.side_effect = comments
    else:
        disable()
        fake.pull.reset_mock()
    asyncio.run(watcher.poll())
    after = evidence(tid)["review"]
    assert after["pending"] == [] and after["count"] == 0
    assert after["findings"] == before["findings"]
    assert after["prs"]["1"].get("processed_versions") == before["prs"]["1"].get("processed_versions")
    assert classify({"factory": _factory_links(ws)[("acme/demo", 1)]})[1] != "Review comments to address (1)"
    if not during_collection:
        fake.pull.assert_not_awaited()


@pytest.mark.parametrize("change", ["edit", "reopen", "reopen_between_polls"])
def test_addressed_feedback_can_be_renewed_without_erasing_reply_history(setup, change):
    watcher, fake, _ = setup
    tid, _ = candidate()
    asyncio.run(watcher.poll())
    with session_scope() as s:
        task = s.get(Task, tid)
        ev = deepcopy(task.evidence)
        entry = ev["review"]["findings"]["comment:10"]
        entry.update(status="addressed", commit="fix", reply_id=42,
                     replied_at="2026-11-10T00:00:00Z",
                     resolved_at=None if change == "edit" else "2026-11-10T00:00:00Z")
        task.evidence = ev
    if change != "reopen_between_polls":
        fake.review_threads.return_value[0]["isResolved"] = change == "reopen"
        asyncio.run(watcher.poll())
        assert evidence(tid)["review"]["count"] == 0
    if change == "edit":
        fake.review_threads.return_value[0]["comments"]["nodes"][0].update(
            body="A materially different finding", updatedAt="2026-11-11T00:00:00Z")
    else:
        fake.review_threads.return_value[0]["isResolved"] = False
    # Restart must retain detection and event deduplication.
    watcher = watch.ReviewWatcher(client_factory=lambda credential, quota: fake)
    asyncio.run(watcher.poll())
    asyncio.run(watcher.poll())
    review = evidence(tid)["review"]
    assert review["count"] == 1
    entry = review["findings"]["comment:10"]
    assert entry["status"] == "pending" and entry["head"] == "head"
    assert entry["reply_id"] == 42 and entry["commit"] == "fix"
    with session_scope() as s:
        assert s.query(Event).filter_by(kind="review_pending").count() == 2
        assert s.get(Task, tid).status == "awaiting_approval"
        assert s.query(Decision).count() == 1
