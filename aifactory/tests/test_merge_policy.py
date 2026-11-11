"""Policy gates share one live snapshot, including recovery and obsolete waits."""
import asyncio

import pytest

from aifactory.db import session_scope
from aifactory.merge_policy import reroute_policy_waits
from aifactory.models import Decision, Outcome, Repository, Task, User, Workspace
from aifactory.pipeline import delivery
from .fakes import FakeGitHub
from .test_pipeline import factory, merge_gate_runner, _waiting_task_with_pr, drive  # noqa: F401


def set_policy(ids, policy, *, repository=False):
    with session_scope() as s:
        if repository:
            repo = s.get(Repository, ids["repo"])
            repo.config = {**repo.config, "merge": {"mode": policy}}
        else:
            ws = s.get(Workspace, ids["ws"])
            ws.settings = {**ws.settings, "autonomy": {**ws.settings["autonomy"], "merge": policy}}


@pytest.mark.parametrize("repository", [False, True])
@pytest.mark.parametrize("policy,kind", [("github", "merge_on_github"), ("approval", "approve_merge")])
def test_live_policy_after_lock_wait_blocks_factory_merge(factory, monkeypatch, policy, kind, repository):
    runner = merge_gate_runner(factory)
    class GH:
        def __init__(self, token):
            pass
        async def pull(self, *args):
            return {"state": "open", "head": {"sha": "head"}}
        async def branch_sha(self, *args):
            return "base"
        async def merge(self, *args, **kwargs):
            pytest.fail("Live human policy must prevent merge")
    class Lock:
        async def __aenter__(self):
            set_policy(factory, policy, repository=repository)
        async def __aexit__(self, *args):
            pass
    runner.repo_lock = lambda _: Lock()
    monkeypatch.setattr(delivery, "GitHub", GH)
    with pytest.raises(delivery.Stop):
        asyncio.run(runner.merge())
    with session_scope() as s:
        d = s.query(Decision).filter_by(task_id=runner.task_id, status="open").one()
        assert d.kind == kind
        if policy == "github":
            assert "approve" not in {o["value"] for o in d.options}


def test_policy_switched_while_waiting_for_ci(factory):
    from .test_ci_parking import park, runner
    _, tid, _ = _waiting_task_with_pr(factory)
    with session_scope() as s:
        s.get(Task, tid).status = "running"
        s.query(Decision).filter_by(task_id=tid).delete()
    asyncio.run(park(tid))
    old_runner = runner(tid)
    set_policy(factory, "github")
    asyncio.run(old_runner.poll_ci(FakeGitHub("t")))
    with pytest.raises(delivery.Stop):
        asyncio.run(old_runner.merge())
    assert not FakeGitHub.merged
    with session_scope() as s:
        assert s.query(Decision).filter_by(task_id=tid, kind="merge_on_github", status="open").count() == 1


def test_one_policy_snapshot_per_locked_merge_routing(factory, monkeypatch):
    runner = merge_gate_runner(factory)
    reads = []
    def policy(*args):
        reads.append(1)
        return "github" if len(reads) == 1 else "auto"
    monkeypatch.setattr(delivery, "effective_merge_policy", policy)
    with pytest.raises(delivery.Stop):
        asyncio.run(runner._serialized_merge(FakeGitHub("t"), "squash"))
    assert len(reads) == 1
    assert not FakeGitHub.merged


def test_human_merge_in_github_mode_finishes_existing_smoke_flow(factory, monkeypatch):
    from aifactory import reconcile
    from aifactory.decisions import resolve
    set_policy(factory, "github")
    oid, tid, number = _waiting_task_with_pr(factory)
    with session_scope() as s:
        s.query(Decision).filter_by(task_id=tid).delete()
    runner = delivery.TaskRunner(tid, asyncio.Event(), lambda _: asyncio.Lock())
    with pytest.raises(delivery.Stop):
        asyncio.run(runner.merge())
    with session_scope() as s:
        d = s.query(Decision).filter_by(task_id=tid).one()
        resolve(s, d, s.get(User, factory["user"]), "merged")
    with pytest.raises(delivery.Stop):
        asyncio.run(delivery.TaskRunner(tid, asyncio.Event(), lambda _: asyncio.Lock()).merge())
    assert not FakeGitHub.merged  # A claim is not permission for the Factory to merge.
    asyncio.run(FakeGitHub("t").merge("acme/demo", number, sha="", method="squash", title="human merge"))
    monkeypatch.setattr(reconcile, "GitHub", FakeGitHub)
    monkeypatch.setattr(reconcile, "resolve_token", lambda _: "t")
    assert asyncio.run(reconcile.reconcile_pull_requests()) == 1
    def settled():
        with session_scope() as s:
            return s.get(Task, tid).status == "completed"
    assert drive(settled)
    with session_scope() as s:
        assert s.get(Task, tid).evidence["merge"]["outside"] is True


@pytest.mark.parametrize("kind,old,new", [
    ("merge_on_github", "github", "auto"), ("merge_on_github", "github", "approval"),
    ("approve_merge", "approval", "github"), ("approve_merge", "approval", "auto"),
    ("approve_revert", "github", "auto"), ("approve_revert", "approval", "auto"),
    ("approve_revert", "approval", "github"), ("approve_revert", "github", "approval")])
def test_all_obsolete_policy_gates_requeue(factory, kind, old, new):
    runner = merge_gate_runner(factory)
    set_policy(factory, old)
    with session_scope() as s:
        t = s.get(Task, runner.task_id)
        t.status = "smoke_failed" if kind == "approve_revert" else "awaiting_approval"
        s.add(Decision(workspace_id=factory["ws"], task_id=t.id, kind=kind, title="wait",
                       options=[{"value": "revert_merged" if old == "github" else "approve"}]))
    set_policy(factory, new)
    with session_scope() as s:
        assert reroute_policy_waits(s, factory["ws"]) == 1
        assert s.get(Task, runner.task_id).stage == ("revert_merge" if kind == "approve_revert" else "merge")
        assert s.get(Task, runner.task_id).status == "queued"
        assert s.query(Decision).filter_by(task_id=runner.task_id).one().status == "superseded"


def test_empty_review_snapshot_clears_pending_but_failure_preserves_it():
    from aifactory.github import GitHubError
    from aifactory.review import github_wait_review_feedback
    ev = {"review": {"pending": [{"id": "old"}], "rounds": 1}}
    class GH:
        async def review_feedback(self, *args):
            return []
    after = asyncio.run(github_wait_review_feedback(GH(), "acme/app", 1, {}, ev))
    assert after["review"]["pending"] == [] and after["review"]["rounds"] == 1
    class Failed:
        async def review_feedback(self, *args):
            raise GitHubError("unavailable")
    with pytest.raises(GitHubError):
        asyncio.run(github_wait_review_feedback(Failed(), "acme/app", 1, {}, ev))
    assert ev["review"]["pending"] == [{"id": "old"}]


@pytest.mark.parametrize("policy", ["github", "approval"])
def test_revert_merge_obeys_live_policy(factory, policy):
    from .test_ci_parking import prepare_revert, runner
    tid = prepare_revert(factory)
    FakeGitHub.check_state = "success"
    asyncio.run(runner(tid).poll_ci())
    set_policy(factory, policy)
    asyncio.run(runner(tid).run())
    assert not FakeGitHub.merged
    with session_scope() as s:
        d = s.query(Decision).filter_by(task_id=tid, status="open").one()
        assert d.kind == "approve_revert"
        options = {o["value"] for o in d.options}
        assert ("revert_merged" in options) == (policy == "github")
        assert ("approve" in options) == (policy == "approval")


@pytest.mark.parametrize("case", ["unmerged", "changed_head", "wrong_base", "merged", "acknowledge"])
def test_revert_merged_resolution_requires_verified_head_and_default_base(factory, client, monkeypatch, case):
    from aifactory import api as api_module, auth
    from .test_ci_parking import prepare_revert, runner

    tid = prepare_revert(factory)
    FakeGitHub.check_state = "success"
    asyncio.run(runner(tid).poll_ci())
    set_policy(factory, "github")
    asyncio.run(runner(tid).run())
    with session_scope() as s:
        d = s.query(Decision).filter_by(task_id=tid, status="open").one()
        did = d.id
        candidate = s.get(Task, tid).evidence["revert_candidate"]
        user = s.get(User, factory["user"])
    # Use the real self/member authorization dependency with the fixture's owner.
    client.app.dependency_overrides[auth.current_user] = lambda: user
    pr = FakeGitHub.pulls[candidate["pr"]]
    pr["merged"] = case not in ("unmerged", "acknowledge")
    pr["merge_commit_sha"] = "revert-merge-sha"
    if case == "changed_head":
        pr["head"] = {**pr["head"], "sha": "force-pushed"}
    if case == "wrong_base":
        pr["base"] = {**pr["base"], "ref": "elsewhere"}
    monkeypatch.setattr(api_module, "GitHub", FakeGitHub)
    monkeypatch.setattr(api_module, "resolve_token", lambda _: "t")
    response = client.post(f"/api/workspaces/{factory['ws']}/decisions/{did}/resolve",
                           json={"resolution": "acknowledge" if case == "acknowledge" else "revert_merged"})
    assert response.status_code == (200 if case in ("merged", "acknowledge") else 409), response.text
    with session_scope() as s:
        task, decision = s.get(Task, tid), s.get(Decision, did)
        assert (task.status == "reverted") == (case == "merged")
        if case == "merged":
            assert task.evidence["revert"]["sha"] == "revert-merge-sha"
        elif case == "acknowledge":
            assert (task.status, task.stage) == ("completed", "done")
            assert task.finished_at and not task.blocked_reason
            assert task.evidence["revert_candidate"] == candidate
            assert "revert" not in task.evidence
            assert decision.status == "resolved"
            assert not FakeGitHub.merged
        else:
            assert decision.status == "open" and "revert" not in task.evidence
    client.app.dependency_overrides.clear()


@pytest.mark.parametrize("source", ["workspace", "repository", "preset", "reconciliation"])
def test_settings_writers_and_reconciliation_reroute_waits(factory, client, source, monkeypatch):
    from aifactory import auth, reconcile
    from aifactory.onboarding_policy import begin
    runner = merge_gate_runner(factory)
    set_policy(factory, "github")
    with session_scope() as s:
        task = s.get(Task, runner.task_id)
        task.status = "smoke_failed"
        s.add(Decision(workspace_id=factory["ws"], task_id=task.id, kind="approve_revert", title="Revert",
                       options=[{"value": "revert_merged"}]))
        user = s.get(User, factory["user"])
        if source == "preset":
            ws = s.get(Workspace, factory["ws"])
            ws.settings = begin({**ws.settings, "autonomy": {"plan": "auto", "merge": "auto", "release": "auto"}})
            s.get(Outcome, task.outcome_id).status = "completed"
    client.app.dependency_overrides[auth.current_user] = lambda: user
    base = f"/api/workspaces/{factory['ws']}"
    if source == "workspace":
        response = client.patch(base, json={"settings": {"autonomy": {"merge": "auto"}}})
    elif source == "repository":
        response = client.patch(f"{base}/repositories/{factory['repo']}", json={"config": {"merge": {"mode": "auto"}}})
    elif source == "preset":
        response = client.post(f"{base}/onboarding/policy/apply")
    else:
        set_policy(factory, "auto")
        monkeypatch.setattr(reconcile, "GitHub", FakeGitHub)
        monkeypatch.setattr(reconcile, "resolve_token", lambda _: "t")
        assert asyncio.run(reconcile.reconcile_pull_requests()) == 1
        response = None
    if response:
        assert response.status_code == 200, response.text
    with session_scope() as s:
        task = s.get(Task, runner.task_id)
        assert (task.status, task.stage) == ("queued", "revert_merge")
        assert s.query(Decision).filter_by(task_id=task.id).one().status == "superseded"
    client.app.dependency_overrides.clear()


def test_identity_schema_evolution_adds_only_nullable_columns(tmp_path):
    from sqlalchemy import create_engine, inspect, text
    from aifactory.db import _add_missing_columns
    engine = create_engine(f"sqlite:///{tmp_path / 'legacy.db'}")
    # Other tables exist on old installations; create_all does not alter users.
    from aifactory.db import Base
    Base.metadata.create_all(engine)
    with engine.begin() as conn:
        conn.execute(text('ALTER TABLE users DROP COLUMN github_id'))
        conn.execute(text('ALTER TABLE users DROP COLUMN github_login'))
    before = {c["name"] for c in inspect(engine).get_columns("users")}
    _add_missing_columns(engine)
    after = inspect(engine).get_columns("users")
    assert {c["name"] for c in after} - before == {"github_id", "github_login"}
    assert all(c["nullable"] for c in after if c["name"].startswith("github_"))


def test_revert_merge_retains_policy_serialization_through_external_call(factory):
    from concurrent.futures import ThreadPoolExecutor
    from threading import Event as ThreadEvent
    from sqlalchemy import event
    from aifactory.onboarding_policy import lock_workspace
    from .test_ci_parking import prepare_revert, runner

    tid = prepare_revert(factory)
    candidate = runner(tid).task.evidence["revert_candidate"]
    merging, release, attempted, changed = (ThreadEvent() for _ in range(4))
    class GH:
        async def merge(self, *args, **kwargs):
            merging.set()
            assert await asyncio.to_thread(release.wait, 5)
            assert not changed.is_set()
            return {"sha": "revert-merge"}
    def change_policy():
        with session_scope() as s:
            ws = lock_workspace(s, factory["ws"])
            ws.settings = {**ws.settings, "autonomy": {**ws.settings["autonomy"], "merge": "github"}}
        changed.set()
    def observe(conn, cursor, statement, parameters, context, executemany):
        if statement.startswith("UPDATE workspaces") and merging.is_set():
            attempted.set()
    with session_scope() as s:
        engine = s.get_bind()
    event.listen(engine, "before_cursor_execute", observe)
    try:
        with ThreadPoolExecutor(max_workers=2) as pool:
            operation = pool.submit(runner(tid)._revert_under_workspace_lock, GH(), candidate)
            assert merging.wait(5)
            writer = pool.submit(change_policy)
            try:
                assert attempted.wait(5)
                assert not changed.is_set()
            finally:
                release.set()
            assert operation.result(timeout=5) == {"sha": "revert-merge"}
            writer.result(timeout=5)
    finally:
        release.set()
        event.remove(engine, "before_cursor_execute", observe)
    assert changed.is_set()


def test_github_wait_reuses_decision_and_calls_shared_feedback_hook(factory, monkeypatch):
    from aifactory import reconcile, review
    set_policy(factory, "github")
    _, tid, number = _waiting_task_with_pr(factory)
    with session_scope() as s:
        s.query(Decision).filter_by(task_id=tid).delete()
    for _ in range(2):
        runner = delivery.TaskRunner(tid, asyncio.Event(), lambda _: asyncio.Lock())
        with pytest.raises(delivery.Stop):
            asyncio.run(runner.merge())
    calls = []
    async def hook(gh, name, pr, policy, evidence):
        calls.append((name, pr))
        return {**evidence, "review": {"pending": [{"id": "comment:1"}]}}
    monkeypatch.setattr(review, "github_wait_review_feedback", hook)
    monkeypatch.setattr(reconcile, "GitHub", FakeGitHub)
    monkeypatch.setattr(reconcile, "resolve_token", lambda _: "t")
    asyncio.run(reconcile.reconcile_pull_requests())
    assert calls == [("acme/demo", number)]
    with session_scope() as s:
        assert s.query(Decision).filter_by(task_id=tid, kind="merge_on_github", status="open").count() == 1
        assert s.get(Task, tid).evidence["review"]["pending"] == [{"id": "comment:1"}]


@pytest.mark.parametrize("observation", ["initial", "after_lock"])
@pytest.mark.parametrize("case", ["changed_head", "wrong_base", "missing_head", "missing_base", "verified"])
def test_outside_merge_requires_verified_head_and_default_branch(factory, monkeypatch, observation, case):
    runner = merge_gate_runner(factory)
    # The post-lock observation exercises the automatic route; the initial one
    # also covers the human-only policy before any ordinary approval gate.
    set_policy(factory, "github" if observation == "initial" else "auto")
    pr = {"merged": True, "merge_commit_sha": "human-merge", "merged_by": {"login": "person"},
          "head": {"sha": "head"}, "base": {"ref": "main"}}
    if case == "changed_head":
        pr["head"]["sha"] = "unverified"
    elif case == "wrong_base":
        pr["base"]["ref"] = "other"
    elif case == "missing_head":
        del pr["head"]
    elif case == "missing_base":
        del pr["base"]
    pulls = []
    class GH:
        def __init__(self, token):
            pass
        async def pull(self, *args):
            pulls.append(1)
            if observation == "after_lock" and len(pulls) == 1:
                return {"state": "open", "head": {"sha": "head"}, "base": {"ref": "main"}}
            return pr
        async def merge(self, *args, **kwargs):
            pytest.fail("An outside merge must never call the Factory merge API")
    smoked = []
    async def smoke():
        smoked.append(True)
    monkeypatch.setattr(delivery, "GitHub", GH)
    monkeypatch.setattr(runner, "smoke", smoke)
    if case == "verified":
        asyncio.run(runner.merge())
    else:
        with pytest.raises(delivery.Stop):
            asyncio.run(runner.merge())
    assert bool(smoked) == (case == "verified")
    with session_scope() as s:
        task = s.get(Task, runner.task_id)
        if case == "verified":
            assert task.merge_sha == "human-merge"
        else:
            assert task.status == "needs_input" and not task.merge_sha
            assert "merge" not in task.evidence
            assert s.query(Decision).filter_by(task_id=task.id, status="open", kind="unblock").count() == 1
