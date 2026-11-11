"""Review gate regressions use durable database state, fake GitHub and UTC time."""
from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from aifactory.db import session_scope
from aifactory.decisions import resolve
from aifactory.github import CheckSummary, GitHubError
from aifactory.models import Decision, Plan, Task, User
from aifactory.pipeline import delivery, reviews
from aifactory.pipeline.delivery import Stop, TaskRunner
from aifactory.review import new_review_evidence

from .test_pipeline import factory, new_outcome  # noqa: F401
from .test_reviews import normalized, thread, inline


def finding(n=10, **kw):
    return normalized(threads=[thread(inline(n, **kw), id=f"thread-{n}")])[0]


class GH:
    def __init__(self):
        self.found = []
        self.review_feedback = AsyncMock(side_effect=lambda *a: self.found)
        self.reply_review_comment = AsyncMock(return_value={"id": 123})
        self.resolve_review_thread = AsyncMock()
        self.comment = AsyncMock(return_value={"id": 124})
        self.checks = AsyncMock(return_value=CheckSummary("success", []))
        self.pull = AsyncMock(return_value={"state": "open", "head": {"sha": "head"}})
        self.branch_sha = AsyncMock(return_value="base")
        self.merge = AsyncMock(return_value={"sha": "merged"})


@pytest.fixture
def gate(factory, monkeypatch):
    oid = new_outcome(factory)
    with session_scope() as s:
        p = Plan(outcome_id=oid, version=1, status="approved", content={})
        s.add(p)
        s.flush()
        t = Task(workspace_id=factory["ws"], outcome_id=oid, plan_id=p.id,
                 key="T1", title="Change", spec="s", acceptance=["c"], repo_id=factory["repo"],
                 status="running", stage="merge", pr_number=1, head_sha="head", base_sha="base",
                 branch="factory/same", attempt=3)
        s.add(t)
        s.flush()
        tid = t.id
    gh = GH()
    monkeypatch.setattr(delivery, "GitHub", lambda *_: gh)
    monkeypatch.setattr(reviews, "GitHub", lambda *_: gh)
    clock = [datetime(2026, 11, 10, tzinfo=timezone.utc)]
    monkeypatch.setattr(reviews, "utcnow", lambda: clock[0])
    monkeypatch.setattr(delivery, "utcnow", lambda: clock[0])
    locks = {}
    runner = TaskRunner(tid, asyncio.Event(), lambda key: locks.setdefault(key, asyncio.Lock()))
    runner.settings["auto_retry_rounds"] = 0
    monkeypatch.setattr(runner, "smoke", AsyncMock())
    monkeypatch.setattr(runner, "_claim_issues_before_merge", AsyncMock())
    return runner, gh, clock, factory


def run(coro):
    return asyncio.run(coro)


def reload(r):
    r._load()
    r.settings["auto_retry_rounds"] = 0


def test_unresolved_finding_stops_merge_and_is_durable(gate):
    r, gh, _, _ = gate
    gh.found = [finding()]
    with pytest.raises(Stop):
        run(r.merge())
    gh.merge.assert_not_awaited()
    reload(r)
    assert r.task.stage == "review_revise"
    assert r._review()["selected"] == gh.found
    text = reviews.feedback(r._review()["selected"])
    for part in ("@reviewer", "file.py:7", "fix this", "@@ -6 +7 @@"):
        assert part in text


@pytest.mark.parametrize("wait", [0, 120])
def test_wait_restart_late_feedback_and_default(gate, wait):
    r, gh, clock, _ = gate
    r.settings["review"].pop("wait_seconds")
    if wait == 0:
        r.cfg["review"] = {"wait_seconds": 0}
    if wait:
        with pytest.raises(Stop):
            run(r._review_gate(gh))
        deadline = r._review()["wait_deadlines"]["head"]
        assert datetime.fromisoformat(deadline) == clock[0] + timedelta(seconds=120)
        clock[0] += timedelta(seconds=80)
        reload(r)
        r.settings["review"].pop("wait_seconds")
        gh.found = [finding()]
        with pytest.raises(Stop):
            run(r._review_gate(gh))
        assert r._review()["wait_deadlines"]["head"] == deadline
        assert r.task.stage == "review_revise"
    else:
        run(r._review_gate(gh))
        assert r._review()["wait_deadlines"]["head"] == clock[0].isoformat()


@pytest.mark.parametrize("state", ["success", "none"])
def test_ci_readiness_starts_deadline_before_delivery_resume(gate, state):
    r, gh, clock, _ = gate
    r.settings["review"].pop("wait_seconds")
    gh.checks.return_value = CheckSummary(state, [])
    with pytest.raises(Stop):
        run(r.ci_then_merge())
    clock[0] += timedelta(seconds=3)
    run(r.poll_ci(gh))
    assert r.task.stage == "merge"
    deadline = r._review()["wait_deadlines"]["head"]
    assert datetime.fromisoformat(deadline) == clock[0] + timedelta(seconds=120)
    clock[0] += timedelta(seconds=125)
    reload(r)
    run(r.merge())
    assert r._review()["wait_deadlines"]["head"] == deadline
    gh.merge.assert_awaited_once()


def test_off_skips_reads_and_review_read_errors_hold(gate):
    r, gh, _, _ = gate
    r.cfg["review"] = {"address": "off"}
    run(r.merge())
    gh.review_feedback.assert_not_awaited()
    gh.merge.reset_mock()
    r.cfg["review"] = {"address": "unresolved"}
    gh.review_feedback.side_effect = GitHubError("unavailable")
    with pytest.raises(Stop):
        run(r.merge())
    gh.merge.assert_not_awaited()


def test_final_premerge_recheck_inside_repo_lock(gate):
    r, gh, _, _ = gate
    async def snapshot(*_):
        if gh.review_feedback.await_count == 2:
            assert r.repo_lock(r.repo.id).locked()
            return [finding()]
        return []
    gh.review_feedback.side_effect = snapshot
    with pytest.raises(Stop):
        run(r.merge())
    gh.merge.assert_not_awaited()
    assert r.task.stage == "review_check"


def test_revert_uses_same_gate(gate):
    r, gh, _, _ = gate
    r.evidence("revert_candidate", {"pr": 2, "head": "revert-head"})
    gh.found = [finding()]
    with pytest.raises(Stop):
        run(r._merge_revert())
    gh.merge.assert_not_awaited()
    assert gh.review_feedback.call_args.args[1] == 2


def decide(r, ids, choice):
    with session_scope() as s:
        d = s.query(Decision).filter(Decision.task_id == r.task_id, Decision.status == "open",
                                     Decision.kind == "review_limit").one()
        resolve(s, d, s.get(User, ids["user"]), choice)
    reload(r)


@pytest.mark.parametrize("choice", ["address_again", "merge_anyway", "abandon", "take_over"])
def test_round_limit_single_decision_and_resolutions(gate, monkeypatch, choice):
    r, gh, _, ids = gate
    r.evidence("review", {**new_review_evidence(), "rounds": 2})
    gh.found = [finding()]
    with pytest.raises(Stop):
        run(r.merge())
    with pytest.raises(Stop):
        run(r.merge())
    from aifactory.pipeline import handover
    handoff = lambda *_: {"ok": True}
    monkeypatch.setattr(handover, "mark_handed_over", handoff)
    before = r.task.attempt
    decide(r, ids, choice)
    assert r.task.attempt == before
    if choice == "address_again":
        with pytest.raises(Stop):
            run(r.merge())
        assert r.task.stage == "review_revise"
    elif choice == "merge_anyway":
        run(r.merge())
        gh.merge.assert_awaited_once()
    elif choice == "abandon":
        assert r.task.status == "abandoned"


def test_cumulative_waivers_only_displayed_versions_current_head(gate):
    r, gh, _, ids = gate
    r.evidence("review", {**new_review_evidence(), "rounds": 2})
    for items in ([finding()], [finding(), finding(11)]):
        gh.found = items
        with pytest.raises(Stop):
            run(r.merge())
        decide(r, ids, "merge_anyway")
    assert len(r._review()["waivers"]["head"]) == 2
    assert r._pending_review(gh.found, "head") == []
    assert len(r._pending_review(gh.found, "other-head")) == 2
    assert r._pending_review([finding(body="changed version")], "head")


@pytest.mark.parametrize("gate_name", ["approval", "head", "base", "ci", "new_finding"])
def test_waivers_do_not_bypass_independent_gates(gate, monkeypatch, gate_name):
    r, gh, _, ids = gate
    r.evidence("review", {**new_review_evidence(), "rounds": 2})
    gh.found = [finding()]
    with pytest.raises(Stop):
        run(r.merge())
    decide(r, ids, "merge_anyway")
    if gate_name == "approval":
        r.cfg["merge"] = {"mode": "approval"}
    elif gate_name == "head":
        gh.pull.return_value = {"state": "open", "head": {"sha": "other"}}
    elif gate_name == "base":
        gh.branch_sha.return_value = "new-main"
        monkeypatch.setattr(r, "_refresh_onto_main", AsyncMock())
    elif gate_name == "ci":
        gh.checks.return_value = CheckSummary("pending", [])
    else:
        gh.found.append(finding(11))
    if gate_name == "base":
        run(r.merge())
        r._refresh_onto_main.assert_awaited_once()
    else:
        with pytest.raises(Stop):
            run(r.merge())
    gh.merge.assert_not_awaited()


@pytest.mark.parametrize("extra", [False, True])
def test_next_round_keeps_success_credit(gate, monkeypatch, extra):
    """Actual completed reply -> next selection -> real durable build entry."""
    r, gh, _, ids = gate
    r.update(attempt=4)
    state = {**new_review_evidence(), "active_round": 1, "active_head": "old",
             "selected": [finding()], "phase": "replying", "success_credits": 1,
             "credited_attempt": 4, "start_attempt": 3,
             "findings": {"comment:10": {"finding": finding(), "version": reviews.version(finding()),
                                         "status": "pending"}}}
    r.evidence("review", state)
    r.builder_report = {"review_responses": [{"id": "comment:10", "status": "addressed", "reason": "fixed file.py"}]}
    gh.found = [finding()]
    run(r._reply_review(gh))
    assert r._review()["credited_attempt"] is None
    gh.found = [finding(11)]
    if extra:
        r.cfg["review"] = {"max_rounds": 1}
    with pytest.raises(Stop):
        run(r.merge())
    if extra:
        decide(r, ids, "address_again")
        with pytest.raises(Stop):
            run(r.merge())
    prepared = []
    async def prepare(*args, **kw):
        prepared.append(kw)
        raise Stop()
    monkeypatch.setattr(r, "_prepare_builder", prepare)
    run(r.run())
    assert prepared == [{"from_branch": True}]
    assert r.task.attempt == 5
    assert r._review()["success_credits"] == 1


@pytest.mark.parametrize("failure", ["checks", "verify"])
def test_real_revision_failures_remain_bounded(gate, monkeypatch, failure):
    r, gh, _, _ = gate
    gh.found = [finding()]
    with pytest.raises(Stop):
        run(r.merge())
    async def prepare(*a, **kw):
        r.builder = SimpleNamespace(kill_all=AsyncMock(), close=AsyncMock())
    monkeypatch.setattr(r, "_prepare_builder", prepare)
    monkeypatch.setattr(delivery, "run_agent", AsyncMock(return_value=(SimpleNamespace(id="run"), SimpleNamespace(report={"status": "done"}), "")))
    monkeypatch.setattr(delivery, "extract_patch", AsyncMock(return_value=SimpleNamespace(diff=b"diff", files=[], additions=1, deletions=0)))
    monkeypatch.setattr(r, "_policy_problem", AsyncMock(return_value=""))
    check = {"command": "test", "ok": failure != "checks", "exit_code": 1, "duration": 0, "tail": "failed"}
    monkeypatch.setattr(r, "_run_commands", AsyncMock(return_value=[check]))
    monkeypatch.setattr(r, "_verify", AsyncMock(return_value=None))
    monkeypatch.setattr(r, "_failing_on_base", AsyncMock(return_value=False))
    run(r.run())
    reload(r)
    assert r.task.status == "failed"
    assert r.task.attempt == 4
    assert r._review().get("success_credits", 0) == 0


def test_disputed_sibling_or_new_comment_prevents_resolution(gate):
    r, gh, _, _ = gate
    first = finding()
    second = {**finding(11), "thread_id": first["thread_id"]}
    gh.found = [first, second]
    with pytest.raises(Stop):
        r._select_review([first], "head")
    r.builder_report = {"review_responses": [{"id": first["id"], "status": "addressed", "reason": "fixed"}]}
    run(r._reply_review(gh))
    gh.resolve_review_thread.assert_not_awaited()
    assert r._pending_review(gh.found, "head") == gh.found


def test_question_answer_uses_published_head_not_rebase(gate, monkeypatch):
    r, gh, _, _ = gate
    q = finding(body="Why is this safe?")
    monkeypatch.setattr(reviews, "checkout", AsyncMock())
    class Sandbox:
        def __init__(self, *_): pass
        def create(self): return self
        async def close(self): pass
    monkeypatch.setattr(reviews, "LocalSandbox", Sandbox)
    monkeypatch.setattr(reviews, "run_agent", AsyncMock(return_value=(None, SimpleNamespace(report={
        "review_responses": [{"id": q["id"], "status": "answered", "reason": "file.py:7 checks the bound"}]}), "")))
    monkeypatch.setattr(r, "_prepare_builder", AsyncMock(side_effect=AssertionError("must not rebase")))
    with pytest.raises(Stop):
        run(r._answer_review(gh, [q]))
    reload(r)
    assert (r.task.base_sha, r.task.head_sha) == ("base", "head")
    assert r._review()["rounds"] == 0
    gh.reply_review_comment.assert_awaited_once()
    gh.resolve_review_thread.assert_not_awaited()
    gh.branch_sha.return_value = "new-main"
    gh.found = []
    # An explicit operator waiver settles only this question version. Even then
    # answering must not have altered the base used by the subsequent merge gate.
    state = r._review()
    state["waivers"] = {"head": [reviews.version(q)]}
    r.evidence("review", state)
    monkeypatch.setattr(r, "_refresh_onto_main", AsyncMock())
    run(r.merge())
    r._refresh_onto_main.assert_awaited_once()
    gh.merge.assert_not_awaited()


def test_approval_resume_and_watcher_select_feedback(gate):
    r, gh, _, _ = gate
    r.update(status="awaiting_approval", stage="merge")
    assert reviews.watches_reviews(r.task)
    gh.found = [finding()]
    with pytest.raises(Stop):
        run(r.watch_review())
    assert r.task.stage == "review_check"
    gh.merge.assert_not_awaited()


def test_revision_full_context_fresh_checkout_same_pr_verified(factory, monkeypatch):
    from .test_pipeline import drive, outcome_status, step, call
    from .fakes import FakeGitHub, ScriptedModel
    from aifactory.models import Workspace
    with session_scope() as s:
        ws = s.get(Workspace, factory["ws"])
        ws.settings = {**ws.settings, "autonomy": {"plan": "auto", "merge": "approval", "release": "auto"}}
    oid = new_outcome(factory)
    assert drive(lambda: outcome_status(oid) == "attention")
    with session_scope() as s:
        t = s.query(Task).filter(Task.outcome_id == oid).one()
        assert t.status == "awaiting_approval"
        tid, branch, old_head = t.id, t.branch, t.head_sha
        t.attempt = 3
        t.evidence = {**t.evidence, "attempts": [{"n": 3, "epoch": 0, "result": "verified"}]}
    found = [finding(n) for n in range(10, 21)]
    state = {"resolved": False}
    async def read(*_):
        return [] if state["resolved"] else found
    async def resolve_thread(*_):
        state["resolved"] = True
        return {"isResolved": True}
    monkeypatch.setattr(FakeGitHub, "review_feedback", read)
    monkeypatch.setattr(FakeGitHub, "reply_review_comment", AsyncMock(return_value={"id": 101}), raising=False)
    monkeypatch.setattr(FakeGitHub, "resolve_review_thread", resolve_thread, raising=False)
    monkeypatch.setattr(reviews, "GitHub", FakeGitHub)
    seen = []
    def builder(messages):
        if step(messages) == 0:
            text = messages[1]["content"]
            seen.append(text)
            for f in found:
                assert f["id"] in text
            assert "@@ -6 +7 @@" in text and "file.py:7" in text and "@reviewer" in text
            return {"content": "", "tool_calls": [call("write_file", {"path": "review.txt", "content": "Review addressed\n"})]}
        return {"content": "", "tool_calls": [call("finish", {"status": "done", "summary": "Address review",
            "review_responses": [{"id": f["id"], "status": "addressed", "reason": "Added review.txt"} for f in found]})]}
    ScriptedModel.scripts["builder"] = builder
    runner = TaskRunner(tid, asyncio.Event(), lambda _: asyncio.Lock())
    runner.settings["auto_retry_rounds"] = 0
    with pytest.raises(Stop):
        run(runner.merge())
    run(runner.run())
    reload(runner)
    assert runner.task.status == "waiting_ci"
    assert runner.task.head_sha != old_head
    assert runner.task.branch == branch and runner.task.pr_number == 1
    assert runner.task.attempt == 4
    assert runner._review()["success_credits"] == 1
    assert runner._review()["credited_attempt"] is None
    assert runner._review()["rounds"] == 1
    assert runner.task.evidence["attempts"][-1]["result"] == "verified"
    assert state["resolved"] and seen
    run(runner.poll_ci(FakeGitHub("token")))
    assert runner.task.evidence["ci"]["head"] == runner.task.head_sha
    assert FakeGitHub.merged == []


def test_interrupted_reply_reconciles_without_duplicate(gate):
    r, gh, _, _ = gate
    f = finding()
    posted = []
    async def lost_response(*args):
        posted.append({"databaseId": 401, "body": args[-1]})
        raise GitHubError("connection lost after posting")
    gh.reply_review_comment.side_effect = lost_response
    with pytest.raises(GitHubError):
        run(r._post_review_reply(gh, f, "Fixed in head"))
    reload(r)
    gh.review_threads = AsyncMock(return_value=[{"comments": {"nodes": posted}}])
    assert run(r._post_review_reply(gh, f, "Fixed in head")) == {"id": "401"}
    assert gh.reply_review_comment.await_count == 1


def test_question_only_pr_comment_answers_without_revision(gate, monkeypatch):
    r, gh, _, _ = gate
    q = {**finding(body="Why is this safe? Please explain."), "id": "issue_comment:10",
         "kind": "issue_comment", "thread_id": None, "path": None}
    monkeypatch.setattr(reviews, "checkout", AsyncMock())
    sb = SimpleNamespace(create=lambda: sb, close=AsyncMock())
    monkeypatch.setattr(reviews, "LocalSandbox", lambda *_: sb)
    monkeypatch.setattr(reviews, "run_agent", AsyncMock(return_value=(None, SimpleNamespace(report={
        "review_responses": [{"id": q["id"], "status": "answered", "reason": "file.py:7 bounds input"}]}), "")))
    gh.found = [q]
    before = r.task.attempt
    run(r._review_gate(gh))
    run(r._review_gate(gh))
    assert r.task.attempt == before
    assert r._review()["rounds"] == 0
    gh.comment.assert_awaited_once()
    gh.resolve_review_thread.assert_not_awaited()


def test_interrupted_review_attempt_is_redone_without_budget_loss(gate, monkeypatch):
    r, gh, _, _ = gate
    r.evidence("attempts", {"n": 3, "result": "verified"})
    gh.found = [finding()]
    with pytest.raises(Stop):
        run(r.merge())
    r.update(attempt=4, stage="build")
    prepared = AsyncMock(side_effect=Stop())
    monkeypatch.setattr(r, "_prepare_builder", prepared)
    run(r.run())
    assert r.task.attempt == 4
    prepared.assert_awaited_once_with("token", from_branch=True)


def test_completed_review_ci_failure_revokes_only_its_credit(gate, monkeypatch):
    r, gh, _, _ = gate
    r.evidence("review", {**new_review_evidence(), "rounds": 2, "success_credits": 2, "credit_head": "head"})
    gh.job_log_tail = AsyncMock(return_value="failed")
    monkeypatch.setattr(r, "deliver", AsyncMock(side_effect=Stop()))
    with pytest.raises(Stop):
        run(r._fix_ci(gh))
    assert r._review()["success_credits"] == 1
    with pytest.raises(Stop):
        run(r._fix_ci(gh))
    assert r._review()["success_credits"] == 1


def test_reply_phase_restart_does_not_build_again(gate, monkeypatch):
    r, gh, _, _ = gate
    f = finding()
    r.evidence("review", {**new_review_evidence(), "active_round": 1, "active_head": "old",
        "phase": "replying", "selected": [f], "success_credits": 1, "credited_attempt": 4,
        "findings": {f["id"]: {"finding": f, "status": "pending", "version": reviews.version(f)}}})
    r.evidence("builder_report", {"review_responses": [{"id": f["id"], "status": "addressed", "reason": "fixed file.py"}]})
    r.update(stage="review_revise", attempt=4)
    reload(r)
    r.builder_report = r.task.evidence["builder_report"]
    monkeypatch.setattr(r, "_prepare_builder", AsyncMock(side_effect=AssertionError("no rebuilding")))
    gh.found = [f]
    run(r.run())
    assert r.task.status == "waiting_ci"
    assert r._review()["rounds"] == 1
    assert r._review()["success_credits"] == 1
    assert r._review()["credited_attempt"] is None


@pytest.mark.parametrize("status", ["pending", "disputed", "addressed"])
def test_missing_finding_without_confirmed_resolution_stays_blocking(gate, status):
    r, gh, _, _ = gate
    f = finding()
    r.evidence("review", {**new_review_evidence(), "findings": {f["id"]: {
        "finding": f, "status": status, "version": reviews.version(f), "reply_id": 12}}})
    assert r._pending_review([], "head") == [f]
    state = r._review()
    state["findings"][f["id"]].update(status="addressed", resolved_at="confirmed")
    r.evidence("review", state)
    assert r._pending_review([], "head") == []


def test_completed_base_refresh_does_not_skip_interrupted_review(gate, monkeypatch):
    r, gh, _, _ = gate
    r.evidence("revalidation", {"phase": "complete"})
    r.evidence("review", {**new_review_evidence(), "active_round": 1, "active_head": "head",
                         "selected": [finding()], "phase": "building", "start_attempt": 3})
    r.update(stage="checks", attempt=4)
    prepared = AsyncMock(side_effect=Stop())
    monkeypatch.setattr(r, "_prepare_builder", prepared)
    run(r.run())
    prepared.assert_awaited_once_with("token", from_branch=True)
    assert r.task.attempt == 4


def test_review_success_credits_do_not_extend_fresh_base_failure_budget(gate):
    r, gh, _, _ = gate
    r.update(attempt=5)
    r.evidence("review", {**new_review_evidence(), "success_credits": 2})
    r._fresh_budget_after_base_moved("conflicts")
    assert r.task.evidence["attempt_base"] == 3


def test_revert_review_takeover_ends_hold_without_merging(gate):
    r, gh, _, ids = gate
    r.update(stage="revert_merge", merge_sha="original-merge")
    r.evidence("revert_candidate", {"pr": 2, "head": "revert-head"})
    gh.found = [finding()]
    with pytest.raises(Stop):
        run(r._merge_revert())
    with session_scope() as s:
        d = s.query(Decision).filter(Decision.task_id == r.task_id, Decision.status == "open").one()
        assert {o["value"] for o in d.options} == {"retry", "take_over"}
        resolve(s, d, s.get(User, ids["user"]), "take_over")
    reload(r)
    assert r.task.status == "handed_over"
    with session_scope() as s:
        assert s.query(Decision).filter(Decision.task_id == r.task_id, Decision.status == "open").count() == 0
    gh.merge.assert_not_awaited()


@pytest.mark.parametrize("resume_reply", [False, True])
def test_interrupted_resolution_reconciles_raw_thread_before_next_round(gate, resume_reply):
    r, gh, _, _ = gate
    f = finding()
    with pytest.raises(Stop):
        r._select_review([f], "head")
    r.builder_report = {"review_responses": [{"id": f["id"], "status": "addressed", "reason": "fixed"}]}
    r.evidence("builder_report", r.builder_report)
    gh.found = [f]
    async def lost_resolution(tid):
        assert tid in r._review()["resolution_intents"]
        gh.found = []  # remote mutation succeeded, process stopped before checkpoint
        raise Stop()
    gh.resolve_review_thread.side_effect = lost_resolution
    with pytest.raises(Stop):
        run(r._reply_review(gh))
    gh.review_threads = AsyncMock(return_value=[thread(inline(), id=f["thread_id"], resolved=True)])
    reload(r)
    if resume_reply:
        run(r._reply_review(gh))
        assert r._review()["rounds"] == 1
    else:
        run(r._review_gate(gh))
    state = r._review()
    assert state["findings"][f["id"]]["resolved_at"]
    assert state["resolution_intents"] == {}
    assert r._pending_review([], "head") == []
    assert gh.resolve_review_thread.await_count == 1


@pytest.mark.parametrize("raw_state", ["missing", "unresolved", "edited", "unreadable"])
def test_resolution_intent_never_uses_absence_as_proof(gate, raw_state):
    r, gh, _, _ = gate
    f = finding()
    state = {**new_review_evidence(), "findings": {f["id"]: {
        "finding": f, "version": reviews.version(f), "status": "addressed", "reply_id": 12}},
        "resolution_intents": {f["thread_id"]: {"pr": 1, "head": "head",
                                               "versions": {f["id"]: reviews.version(f)}}}}
    r.evidence("review", state)
    records = [] if raw_state == "missing" else [thread(
        inline(body="new feedback" if raw_state == "edited" else "fix this"),
        id=f["thread_id"], resolved=raw_state != "unresolved")]
    gh.review_threads = AsyncMock(return_value=records)
    if raw_state == "unreadable":
        gh.review_threads.side_effect = GitHubError("unavailable")
        with pytest.raises(Stop):
            run(r._read_review(gh, 1))
    else:
        run(r._read_review(gh, 1))
    assert r._pending_review([], "head") == [f]
    gh.merge.assert_not_awaited()


@pytest.mark.parametrize("queue", ["auto", "github"])
@pytest.mark.parametrize("late", [False, True])
def test_review_gate_never_hands_merge_to_github_queue(gate, monkeypatch, queue, late):
    """Even a late finding must hit the locked gate, never asynchronous enqueue."""
    r, gh, _, _ = gate
    r.cfg["merge"] = {"queue": queue}
    lookup = AsyncMock(return_value={"pr_id": "PR1", "entry": None})
    enqueue = AsyncMock(side_effect=AssertionError("review gate cannot delegate merging"))
    monkeypatch.setattr(delivery.merge_manager, "github_queue_state", lookup)
    monkeypatch.setattr(delivery.merge_manager, "merge_via_github_queue", enqueue)
    async def snapshot(*_):
        return [finding()] if late and gh.review_feedback.await_count == 2 else []
    gh.review_feedback.side_effect = snapshot
    if late:
        with pytest.raises(Stop):
            run(r.merge())
        assert r.task.stage == "review_check"
        gh.merge.assert_not_awaited()
    else:
        run(r.merge())
        gh.merge.assert_awaited_once()
    lookup.assert_not_awaited()
    enqueue.assert_not_awaited()


def test_off_preserves_github_queue_policy(gate, monkeypatch):
    r, gh, _, _ = gate
    r.cfg["review"] = {"address": "off"}
    lookup = AsyncMock(return_value={"pr_id": "PR1", "entry": None})
    enqueue = AsyncMock(side_effect=Stop())
    monkeypatch.setattr(delivery.merge_manager, "github_queue_state", lookup)
    monkeypatch.setattr(delivery.merge_manager, "merge_via_github_queue", enqueue)
    with pytest.raises(Stop):
        run(r.merge())
    enqueue.assert_awaited_once()
    gh.review_feedback.assert_not_awaited()
    gh.merge.assert_not_awaited()


@pytest.mark.parametrize("credited", [False, True])
def test_verified_review_restart_publishes_once_without_rebuilding(gate, monkeypatch, credited):
    r, gh, _, _ = gate
    r.update(stage="publish", attempt=4)
    r.evidence("review", {**new_review_evidence(), "active_round": 1, "phase": "building",
        "start_attempt": 3, "success_credits": int(credited), "credited_attempt": 4 if credited else None})
    monkeypatch.setattr(r, "_restore_verified_candidate", lambda: True)
    monkeypatch.setattr(r, "_prepare_builder", AsyncMock(side_effect=AssertionError("no rebuild")))
    monkeypatch.setattr(r, "_publish", AsyncMock())
    monkeypatch.setattr(r, "_reply_review", AsyncMock())
    monkeypatch.setattr(r, "ci_then_merge", AsyncMock())
    run(r.run())
    assert r.task.attempt == 4
    assert r._review()["success_credits"] == 1
    r._publish.assert_awaited_once_with("token")
    r._reply_review.assert_awaited_once()
    r.ci_then_merge.assert_awaited_once()


def test_revert_review_cannot_offer_stuck_acknowledgement(gate):
    """A failed revert is not an accepted smoke failure: only retry/take-over are safe."""
    from aifactory.decisions import DecisionError
    r, gh, _, ids = gate
    r.update(stage="revert_merge", merge_sha="original-merge")
    r.evidence("revert_candidate", {"pr": 2, "head": "revert-head"})
    gh.found = [finding()]
    with pytest.raises(Stop):
        run(r._merge_revert())
    with session_scope() as s:
        d = s.query(Decision).filter(Decision.task_id == r.task_id, Decision.status == "open").one()
        with pytest.raises(DecisionError):
            resolve(s, d, s.get(User, ids["user"]), "acknowledge")
        assert d.status == "open"
        resolve(s, d, s.get(User, ids["user"]), "retry")
    reload(r)
    assert (r.task.status, r.task.stage) == ("queued", "revert_merge")
    gh.merge.assert_not_awaited()


def test_repeated_limit_observation_never_waives_undisplayed_findings(gate):
    r, gh, _, ids = gate
    r.evidence("review", {**new_review_evidence(), "rounds": 2})
    gh.found = [finding()]
    with pytest.raises(Stop):
        run(r.merge())
    displayed = r._review()["decision"]
    gh.found.append(finding(11))
    with pytest.raises(Stop):
        run(r.merge())
    assert r._review()["decision"] == displayed
    decide(r, ids, "merge_anyway")
    assert r._pending_review(gh.found, "head") == [finding(11)]
    gh.merge.assert_not_awaited()


def test_review_limit_takeover_uses_confirmed_action_transaction(gate, monkeypatch):
    from aifactory.pipeline import handover
    from unittest.mock import Mock
    r, gh, _, ids = gate
    r.evidence("review", {**new_review_evidence(), "rounds": 2})
    gh.found = [finding()]
    with pytest.raises(Stop):
        run(r.merge())
    handoff = Mock(return_value={"ok": True})
    monkeypatch.setattr(handover, "mark_handed_over", handoff)
    after, events = [], []
    with session_scope() as s:
        d = s.query(Decision).filter(Decision.task_id == r.task_id, Decision.status == "open").one()
        resolve(s, d, s.get(User, ids["user"]), "take_over", action_id="confirmed",
                pending_events=events, after=after)
        assert handoff.call_args.kwargs["session"] is s
        assert handoff.call_args.kwargs["action_id"] == "confirmed"
    assert [item[0] for item in after] == ["stop_task", "publish_candidate"]
