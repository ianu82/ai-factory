"""End-to-end pipeline tests against a local git remote, a scripted model and a fake GitHub."""

from __future__ import annotations

import asyncio
import json
import time

import pytest

from aifactory import gitops
from aifactory.db import session_scope
from aifactory.inference import ProviderConfig
from aifactory.models import Decision, Event, Membership, Organization, Outcome, Repository, Run, Task, User, Workspace
from aifactory.pipeline import delivery, planning, runtime
from aifactory.pipeline.orchestrator import Orchestrator
from aifactory.pipeline.runtime import DEFAULT_SETTINGS

from .fakes import FakeGitHub, ScriptedModel, call, git, make_remote, step

CRITERION = "greet.sh prints exactly 'Hello, Factory!'"
FILES = {
    "greet.sh": "echo 'Hello'\n",
    "test.sh": "out=$(sh greet.sh); [ \"$out\" = 'Hello, Factory!' ] || { echo \"expected greeting, got: $out\"; exit 1; }\n",
}


def planner_script(messages):
    if step(messages) == 0:
        return {"content": "Looking around.", "tool_calls": [call("list_files", {})]}
    return {"content": "", "tool_calls": [call("finish", {
        "summary": "Change the greeting.",
        "tasks": [{"key": "T1", "title": "Update greeting", "repo": "acme/demo",
                   "spec": "Make greet.sh print Hello, Factory!", "acceptance": [CRITERION], "depends_on": []}],
        "questions": [], "risks": []})]}


def good_builder(messages):
    n = step(messages)
    if n == 0:
        return {"content": "", "tool_calls": [call("read_file", {"path": "greet.sh"}),
                                              call("write_file", {"path": "greet.sh", "content": "echo 'Hello, Factory!'\n"}, 1)]}
    if n == 1:
        return {"content": "", "tool_calls": [call("run", {"command": "sh test.sh"})]}
    return {"content": "", "tool_calls": [call("finish", {"status": "done", "summary": "Updated the greeting.",
                                                          "tests": ["sh test.sh: passed"]})]}


def passing_verifier(messages):
    if step(messages) == 0:
        return {"content": "", "tool_calls": [call("run", {"command": "sh test.sh && git diff --stat HEAD"})]}
    return {"content": "", "tool_calls": [call("finish", {
        "verdict": "pass", "summary": "Greeting verified.",
        "criteria": [{"criterion": CRITERION, "status": "pass", "evidence": "sh test.sh exit 0"}], "findings": []})]}


@pytest.fixture
def factory(tmp_path, monkeypatch):
    remotes = tmp_path / "remotes"
    make_remote(remotes, "acme/demo", FILES)
    monkeypatch.setattr(gitops, "GIT_BASE", str(remotes))
    monkeypatch.setattr(runtime, "ModelClient", ScriptedModel)
    monkeypatch.setattr(runtime, "resolve_provider", lambda ws: ProviderConfig("openai", "http://model.test", "k", "Test"))
    FakeGitHub.remotes = remotes
    FakeGitHub.pulls = {}
    FakeGitHub.merged = []
    FakeGitHub.check_state = "success"
    monkeypatch.setattr(delivery, "GitHub", FakeGitHub)
    monkeypatch.setattr(delivery, "resolve_token", lambda ws: "token")
    monkeypatch.setattr(planning, "resolve_token", lambda ws: "token")
    ScriptedModel.calls = []
    ScriptedModel.scripts = {"planner": planner_script, "builder": good_builder, "verifier": passing_verifier}
    with session_scope() as s:
        org = Organization(name="Acme")
        s.add(org)
        s.flush()
        settings = json.loads(json.dumps(DEFAULT_SETTINGS))
        settings["limits"]["ci_minutes"] = 1
        ws = Workspace(org_id=org.id, name="Acme", slug="acme", settings=settings)
        user = User(email="dev@acme.test", name="Dev")
        s.add_all([ws, user])
        s.flush()
        s.add(Membership(workspace_id=ws.id, user_id=user.id, role="owner"))
        repo = Repository(workspace_id=ws.id, full_name="acme/demo", default_branch="main",
                          config={"setup": [], "checks": ["sh test.sh"], "smoke": ["sh test.sh"], "ci_grace_seconds": 1})
        s.add(repo)
        s.flush()
        ids = {"ws": ws.id, "repo": repo.id, "user": user.id, "remotes": remotes}
    return ids


def new_outcome(ids, title="Friendlier greeting") -> str:
    with session_scope() as s:
        ws = s.get(Workspace, ids["ws"])
        ws.outcome_seq += 1
        o = Outcome(workspace_id=ids["ws"], number=ws.outcome_seq, title=title, brief="Say hello to the Factory.",
                    repo_ids=[ids["repo"]], status="planning")
        s.add(o)
        s.flush()
        return o.id


def drive(until, timeout=60.0):
    async def go():
        orch = Orchestrator()
        orch.start()
        try:
            deadline = time.monotonic() + timeout
            while time.monotonic() < deadline:
                if until():
                    return True
                orch.wake()
                await asyncio.sleep(0.2)
            return False
        finally:
            await orch.stop()
            await asyncio.sleep(0.1)

    return asyncio.run(go())


def outcome_status(outcome_id):
    with session_scope() as s:
        return s.get(Outcome, outcome_id).status


def test_outcome_is_planned_built_verified_merged_and_smoke_tested(factory):
    outcome_id = new_outcome(factory)
    assert drive(lambda: outcome_status(outcome_id) in ("completed", "attention", "blocked")), "pipeline did not finish"
    with session_scope() as s:
        outcome = s.get(Outcome, outcome_id)
        task = s.query(Task).filter(Task.outcome_id == outcome_id).one()
        runs = s.query(Run).filter(Run.outcome_id == outcome_id).all()
        kinds = [e.kind for e in s.query(Event).filter(Event.outcome_id == outcome_id).order_by(Event.id)]
        assert outcome.status == "completed", (outcome.status, task.status, task.blocked_reason, kinds)
        assert task.status == "completed" and task.merge_sha and task.pr_number == 1
        assert task.evidence["smoke"]["state"] == "passed"
        assert task.evidence["ci"]["state"] == "success"
        assert {r.role for r in runs} == {"planner", "builder", "verifier"}
        assert all(r.input_tokens > 0 for r in runs)
    assert FakeGitHub.merged == [1]
    main_greet = git("show", "main:greet.sh", cwd=factory["remotes"] / "acme/demo.git")
    assert "Hello, Factory!" in main_greet
    for kind in ("plan_proposed", "plan_approved", "verified", "published", "merged", "smoke_passed", "outcome_completed"):
        assert kind in kinds, kinds


def test_failed_verification_feeds_back_and_second_attempt_passes(factory):
    verdicts = iter(["fail", "pass"])
    seen_feedback = []

    def builder(messages):
        if step(messages) == 0 and "Independent review FAILED" in messages[1]["content"]:
            seen_feedback.append(True)
        return good_builder(messages)

    def verifier(messages):
        if step(messages) == 0:
            verdict = next(verdicts)
            verifier.current = verdict
            return {"content": "", "tool_calls": [call("run", {"command": "sh test.sh"})]}
        if verifier.current == "fail":
            return {"content": "", "tool_calls": [call("finish", {
                "verdict": "fail", "summary": "Missing test for edge case.",
                "criteria": [{"criterion": CRITERION, "status": "fail", "evidence": "no test"}],
                "findings": [{"severity": "major", "description": "Add a regression test"}]})]}
        return passing_verifier(messages)

    ScriptedModel.scripts.update(builder=builder, verifier=verifier)
    outcome_id = new_outcome(factory)
    assert drive(lambda: outcome_status(outcome_id) in ("completed", "attention", "blocked"))
    with session_scope() as s:
        task = s.query(Task).filter(Task.outcome_id == outcome_id).one()
        assert task.status == "completed", task.blocked_reason
        assert task.attempt == 2
        results = [a["result"] for a in task.evidence["attempts"]]
        assert results == ["verification_failed", "verified"]
    assert seen_feedback, "builder never saw the verifier's feedback"


def test_plan_approval_policy_waits_for_a_person(factory, client):
    with session_scope() as s:
        ws = s.get(Workspace, factory["ws"])
        ws.settings = {**ws.settings, "autonomy": {"plan": "approval", "merge": "approval", "release": "approval"}}
    outcome_id = new_outcome(factory)
    assert drive(lambda: outcome_status(outcome_id) == "awaiting_approval")
    with session_scope() as s:
        decision = s.query(Decision).filter(Decision.outcome_id == outcome_id, Decision.status == "open").one()
        assert decision.kind == "approve_plan"
        user = s.get(User, factory["user"])
    from aifactory.decisions import resolve
    from aifactory.db import SessionLocal

    db = SessionLocal()
    resolve(db, db.get(Decision, decision.id), db.get(User, user.id), "approve", reusable=False)
    db.close()

    def waiting_merge():
        with session_scope() as s:
            return s.query(Task).filter(Task.outcome_id == outcome_id, Task.status == "awaiting_approval").count() == 1

    assert drive(waiting_merge)
    assert FakeGitHub.merged == []
    with session_scope() as s:
        merge_decision = s.query(Decision).filter(Decision.outcome_id == outcome_id, Decision.kind == "approve_merge",
                                                  Decision.status == "open").one()
    db = SessionLocal()
    resolve(db, db.get(Decision, merge_decision.id), db.get(User, factory["user"]), "approve")
    db.close()
    assert drive(lambda: outcome_status(outcome_id) == "completed")
    assert FakeGitHub.merged == [1]


def test_builder_question_reaches_a_person_and_answer_resumes(factory):
    def blocked_builder(messages):
        return {"content": "", "tool_calls": [call("finish", {"status": "blocked", "summary": "Need wording",
                                                              "blocked_reason": "Which greeting wording should we use?"})]}

    ScriptedModel.scripts["builder"] = blocked_builder
    outcome_id = new_outcome(factory)

    def asked():
        with session_scope() as s:
            return s.query(Decision).filter(Decision.outcome_id == outcome_id, Decision.kind == "question",
                                            Decision.status == "open").count() == 1

    assert drive(asked)
    with session_scope() as s:
        decision_id = s.query(Decision).filter(Decision.outcome_id == outcome_id, Decision.kind == "question").one().id
        task = s.query(Task).filter(Task.outcome_id == outcome_id).one()
        assert task.status == "needs_input" and task.attempt == 0
    from aifactory.db import SessionLocal
    from aifactory.decisions import resolve

    heard = []

    def listening_builder(messages):
        if any("Use 'Hello, Factory!'" in str(m.get("content")) for m in messages):
            heard.append(True)
        return good_builder([m for m in messages if "Guidance from your team" not in str(m.get("content"))])

    ScriptedModel.scripts["builder"] = listening_builder
    db = SessionLocal()
    resolve(db, db.get(Decision, decision_id), db.get(User, factory["user"]), "answer",
            comment="Use 'Hello, Factory!'", reusable=True, scope="workspace")
    db.close()
    assert drive(lambda: outcome_status(outcome_id) == "completed")
    assert heard, "the answer was not delivered to the builder"
    with session_scope() as s:
        precedent = s.get(Decision, decision_id)
        assert precedent.reusable and precedent.scope == "workspace"


def test_ci_failure_returns_to_builder_with_logs(factory):
    states = iter(["failure", "success", "success", "success"])
    original = FakeGitHub.checks

    async def checks(self, full_name, sha):
        FakeGitHub.check_state = next(states, "success")
        return await original(self, full_name, sha)

    FakeGitHub.checks = checks
    saw_ci = []

    def builder(messages):
        if step(messages) == 0 and "CI check" in messages[1]["content"]:
            saw_ci.append(True)
        return good_builder(messages)

    ScriptedModel.scripts["builder"] = builder
    try:
        outcome_id = new_outcome(factory)
        assert drive(lambda: outcome_status(outcome_id) in ("completed", "attention"))
        assert outcome_status(outcome_id) == "completed"
        assert saw_ci
    finally:
        FakeGitHub.checks = original


def test_parallel_tasks_on_one_repository_are_revalidated_before_merge(factory):
    def planner(messages):
        if step(messages) == 0:
            return {"content": "", "tool_calls": [call("list_files", {})]}
        return {"content": "", "tool_calls": [call("finish", {"summary": "Two files", "questions": [], "tasks": [
            {"key": "T1", "title": "Add a.txt", "repo": "acme/demo", "spec": "create a.txt", "acceptance": ["a.txt exists"],
             "depends_on": []},
            {"key": "T2", "title": "Add b.txt", "repo": "acme/demo", "spec": "create b.txt", "acceptance": ["b.txt exists"],
             "depends_on": []}]})]}

    def builder(messages):
        name = "a.txt" if "Add a.txt" in messages[1]["content"] else "b.txt"
        if step(messages) == 0:
            return {"content": "", "tool_calls": [call("write_file", {"path": name, "content": name}),
                                                  call("write_file", {"path": "greet.sh", "content": "echo 'Hello, Factory!'\n"}, 1)
                                                  if name == "a.txt" else call("run", {"command": "true"}, 1)]}
        return {"content": "", "tool_calls": [call("finish", {"status": "done", "summary": f"added {name}"})]}

    def verifier(messages):
        criterion = "a.txt exists" if "a.txt exists" in messages[1]["content"] else "b.txt exists"
        if step(messages) == 0:
            return {"content": "", "tool_calls": [call("run", {"command": "ls"})]}
        return {"content": "", "tool_calls": [call("finish", {"verdict": "pass", "summary": "ok",
                "criteria": [{"criterion": criterion, "status": "pass", "evidence": "ls"}], "findings": []})]}

    ScriptedModel.scripts.update(planner=planner, builder=builder, verifier=verifier)
    with session_scope() as s:
        repo = s.get(Repository, factory["repo"])
        repo.config = {**repo.config, "checks": [], "smoke": ["test -f greet.sh"]}
    outcome_id = new_outcome(factory)
    assert drive(lambda: outcome_status(outcome_id) in ("completed", "attention"), timeout=90)
    assert outcome_status(outcome_id) == "completed"
    tree = git("ls-tree", "--name-only", "main", cwd=factory["remotes"] / "acme/demo.git").split()
    assert "a.txt" in tree and "b.txt" in tree
    with session_scope() as s:
        kinds = [e.kind for e in s.query(Event).filter(Event.outcome_id == outcome_id)]
    assert "revalidate" in kinds, "second merge should have rebased onto the first"


def test_planner_question_with_a_plan_can_be_approved_as_proposed(factory):
    def planner(messages):
        if step(messages) == 0:
            return {"content": "", "tool_calls": [call("list_files", {})]}
        report = json.loads(planner_script(messages)["tool_calls"][0]["function"]["arguments"])
        report["questions"] = [{"question": "Exclamation mark or full stop?", "why": "Brand voice"}]
        return {"content": "", "tool_calls": [call("finish", report)]}

    ScriptedModel.scripts["planner"] = planner
    outcome_id = new_outcome(factory)
    assert drive(lambda: outcome_status(outcome_id) == "needs_input")
    with session_scope() as s:
        decision = s.query(Decision).filter(Decision.outcome_id == outcome_id, Decision.status == "open").one()
        assert [o["value"] for o in decision.options][:2] == ["approve", "answer"]
    from aifactory.db import SessionLocal
    from aifactory.decisions import resolve

    db = SessionLocal()
    resolve(db, db.get(Decision, decision.id), db.get(User, factory["user"]), "approve", comment="Exclamation mark.")
    db.close()
    assert drive(lambda: outcome_status(outcome_id) == "completed")


def test_reviewer_findings_survive_a_retry(factory):
    from aifactory.pipeline.delivery import TaskRunner

    outcome_id = new_outcome(factory)
    with session_scope() as s:
        from aifactory.models import Plan

        plan = Plan(outcome_id=outcome_id, version=1, status="approved", content={})
        s.add(plan)
        s.flush()
        task = Task(workspace_id=factory["ws"], outcome_id=outcome_id, plan_id=plan.id, key="T1", title="t", spec="s",
                    acceptance=["c"], repo_id=factory["repo"], status="queued",
                    evidence={"attempts": [{"n": 3, "result": "verification_failed", "verdict": {
                        "verdict": "fail", "summary": "Race on credential save",
                        "criteria": [{"criterion": "c", "status": "fail", "evidence": "stale token"}],
                        "findings": [{"severity": "major", "description": "events published before commit"}]}}]})
        s.add(task)
        s.flush()
        task_id = task.id
    runner = TaskRunner(task_id, asyncio.Event(), lambda _: asyncio.Lock())
    joined = "\n".join(runner.feedback)
    assert "Race on credential save" in joined and "stale token" in joined and "events published before commit" in joined


def test_runtime_qa_boots_the_app_and_attaches_screenshots(factory):
    pytest.importorskip("playwright")
    from aifactory.models import Artifact

    with session_scope() as s:
        repo = s.get(Repository, factory["repo"])
        repo.config = {**repo.config, "qa": {
            "start": "printf '<html><head><title>Greeting</title></head><body><h1>Hello</h1></body></html>' > index.html"
                     " && exec python3 -m http.server $PORT --bind 127.0.0.1",
            "ready_path": "/", "ready_timeout": 30, "pages": ["/"], "required": True,
            "probes": [{"method": "GET", "path": "/", "expect_status": 200}]}}

    def builder_with_checks(messages):
        reply = good_builder(messages)
        for tool_call in reply.get("tool_calls", []):
            if tool_call["function"]["name"] == "finish":
                args = json.loads(tool_call["function"]["arguments"])
                args["runtime_checks"] = [
                    {"method": "GET", "path": "/index.html", "expect_status": 200, "why": "the greeting page is served"},
                    {"method": "GET", "path": "/missing", "expect_status": 404, "why": "unknown paths are not served"},
                    {"method": "GET", "path": "//evil.example/x", "expect_status": 200, "why": "must be dropped"}]
                args["runtime_pages"] = ["/index.html"]
                tool_call["function"]["arguments"] = json.dumps(args)
        return reply

    ScriptedModel.scripts["builder"] = builder_with_checks
    outcome_id = new_outcome(factory)
    assert drive(lambda: outcome_status(outcome_id) in ("completed", "attention"), timeout=120)
    with session_scope() as s:
        task = s.query(Task).filter(Task.outcome_id == outcome_id).one()
        assert task.status == "completed", task.blocked_reason
        app = task.evidence["attempts"][-1]["app_run"]
        assert app["started"] and app["probes"][0]["ok"], app
        assert app["pages"][0]["status"] == 200 and app["pages"][0]["title"] == "Greeting", app
        shots = s.query(Artifact).filter(Artifact.task_id == task.id, Artifact.kind == "screenshot").all()
        assert shots and shots[0].content.startswith("iVBOR")  # base64 PNG
        assert [(c["path"], c["ok"]) for c in app["change_checks"]] == [("/index.html", True), ("/missing", True)]
        assert [p["path"] for p in app["pages"]] == ["/", "/index.html"]
    pr = FakeGitHub.pulls[1]
    assert "Runtime QA" in pr["body"] and "page `/` → 200" in pr["body"]
    assert "`GET /missing` → 404 (expected 404) — unknown paths are not served" in pr["body"]


def test_merge_approval_does_not_cover_a_later_candidate(factory):
    from datetime import timedelta

    from aifactory.db import utcnow
    from aifactory.models import Plan
    from aifactory.pipeline.delivery import TaskRunner

    outcome_id = new_outcome(factory)
    approved_at = utcnow()
    with session_scope() as s:
        plan = Plan(outcome_id=outcome_id, version=1, status="approved", content={})
        s.add(plan)
        s.flush()
        task = Task(workspace_id=factory["ws"], outcome_id=outcome_id, plan_id=plan.id, key="T1", title="t",
                    repo_id=factory["repo"], status="queued", stage="merge",
                    evidence={"published": {"head": "new", "at": (approved_at + timedelta(minutes=5)).isoformat()}})
        s.add(task)
        s.flush()
        s.add(Decision(workspace_id=factory["ws"], outcome_id=outcome_id, task_id=task.id, kind="approve_merge",
                       title="Approve", status="resolved", resolution="approve", resolved_at=approved_at))
        task_id = task.id
    runner = TaskRunner(task_id, asyncio.Event(), lambda _: asyncio.Lock())
    assert runner._approved("approve_merge") is False
    with session_scope() as s:
        s.get(Task, task_id).evidence = {"published": {"head": "old", "at": (approved_at - timedelta(minutes=5)).isoformat()}}
    assert TaskRunner(task_id, asyncio.Event(), lambda _: asyncio.Lock())._approved("approve_merge") is True


def test_shutdown_interrupts_tasks_without_cancelling_them(factory):
    def slow_builder(messages):
        import time as _t

        _t.sleep(0.05)
        return {"content": "", "tool_calls": [call("run", {"command": "sleep 30"}, step(messages))]}

    ScriptedModel.scripts["builder"] = slow_builder
    outcome_id = new_outcome(factory)

    def building():
        with session_scope() as s:
            return s.query(Task).filter(Task.outcome_id == outcome_id, Task.stage == "build").count() == 1

    assert drive(building, timeout=60)  # drive() stops the orchestrator once the task is building
    with session_scope() as s:
        task = s.query(Task).filter(Task.outcome_id == outcome_id).one()
        assert task.status == "queued", task.status
        assert task.attempt == 1

    # After the restart the interrupted attempt is redone, not counted against the limit.
    ScriptedModel.scripts["builder"] = good_builder
    assert drive(lambda: outcome_status(outcome_id) == "completed", timeout=90)
    with session_scope() as s:
        task = s.query(Task).filter(Task.outcome_id == outcome_id).one()
        assert task.attempt == 1 and [a["n"] for a in task.evidence["attempts"]] == [1]
        assert s.query(Event).filter(Event.task_id == task.id, Event.kind == "attempt_resumed").count() == 1


def test_smoke_failure_is_reverted_under_recovery_policy(factory):
    with session_scope() as s:
        repo = s.get(Repository, factory["repo"])
        # Checks pass before merge; the post-merge smoke check fails for the new greeting.
        repo.config = {**repo.config, "smoke": ["! grep -q Factory greet.sh"], "recovery": {"smoke_failure": "revert"}}
    outcome_id = new_outcome(factory)
    assert drive(lambda: outcome_status(outcome_id) in ("partially_completed", "completed", "attention"), timeout=90)
    with session_scope() as s:
        task = s.query(Task).filter(Task.outcome_id == outcome_id).one()
        assert task.status == "reverted", (task.status, task.blocked_reason)
        assert task.evidence["revert"]["pr"] == 2 and task.evidence["smoke"]["state"] == "failed"
    assert FakeGitHub.merged == [1, 2]
    main_greet = git("show", "main:greet.sh", cwd=factory["remotes"] / "acme/demo.git")
    assert main_greet.strip() == "echo 'Hello'"


def test_take_over_pushes_the_latest_candidate_as_a_draft_pr_and_stops_the_factory(factory, monkeypatch):
    from aifactory.models import Artifact, Plan
    from aifactory.pipeline import handover

    monkeypatch.setattr(handover, "GitHub", FakeGitHub)
    monkeypatch.setattr(handover, "resolve_token", lambda ws: "token")
    base = git("rev-parse", "refs/heads/main", cwd=factory["remotes"] / "acme/demo.git")
    diff = ("diff --git a/greet.sh b/greet.sh\n--- a/greet.sh\n+++ b/greet.sh\n@@ -1 +1 @@\n"
            "-echo 'Hello'\n+echo 'Hello, Factory!'\n")
    outcome_id = new_outcome(factory)
    with session_scope() as s:
        plan = Plan(outcome_id=outcome_id, version=1, status="approved", content={})
        s.add(plan)
        s.flush()
        task = Task(workspace_id=factory["ws"], outcome_id=outcome_id, plan_id=plan.id, key="T1", title="Greeting",
                    repo_id=factory["repo"], status="failed", base_sha=base, attempt=3)
        s.add(task)
        s.flush()
        s.add(Artifact(workspace_id=factory["ws"], outcome_id=outcome_id, task_id=task.id, kind="diff",
                       name="Attempt 3 diff", content=diff, data={"files": ["greet.sh"]}))
        s.add(Decision(workspace_id=factory["ws"], outcome_id=outcome_id, task_id=task.id, kind="unblock",
                       title="T1 did not pass after 3 attempts"))
        task_id = task.id
        user = s.get(User, factory["user"])

    info = handover.mark_handed_over(task_id, user)
    result = asyncio.run(handover.publish_candidate(task_id, info))
    assert result["published"] and result["pr_url"]
    pr = next(iter(FakeGitHub.pulls.values()))
    assert pr["title"].startswith("[handed over]") and "not** passed independent verification" in pr["body"]
    content = git("show", f"{result['branch']}:greet.sh", cwd=factory["remotes"] / "acme/demo.git")
    assert content.strip() == "echo 'Hello, Factory!'"
    with session_scope() as s:
        task = s.get(Task, task_id)
        assert task.status == "handed_over" and task.pr_url == result["pr_url"] and "Dev" in task.blocked_reason
        assert s.query(Decision).filter(Decision.task_id == task_id).one().status == "superseded"
    with pytest.raises(handover.HandoverError):
        handover.mark_handed_over(task_id, user)


def test_a_stuck_task_can_be_taken_over_from_its_decision(factory, client, monkeypatch):
    from aifactory.models import Artifact, Plan
    from aifactory.pipeline import handover

    monkeypatch.setattr(handover, "GitHub", FakeGitHub)
    monkeypatch.setattr(handover, "resolve_token", lambda ws: "token")
    base = git("rev-parse", "refs/heads/main", cwd=factory["remotes"] / "acme/demo.git")
    outcome_id = new_outcome(factory)
    with session_scope() as s:
        plan = Plan(outcome_id=outcome_id, version=1, status="approved", content={})
        s.add(plan)
        s.flush()
        task = Task(workspace_id=factory["ws"], outcome_id=outcome_id, plan_id=plan.id, key="T1", title="Greeting",
                    repo_id=factory["repo"], status="failed", base_sha=base, attempt=3)
        s.add(task)
        s.flush()
        s.add(Artifact(workspace_id=factory["ws"], outcome_id=outcome_id, task_id=task.id, kind="diff", name="d",
                       content="diff --git a/greet.sh b/greet.sh\n--- a/greet.sh\n+++ b/greet.sh\n@@ -1 +1 @@\n"
                               "-echo 'Hello'\n+echo 'Hello, Factory!'\n", data={"files": ["greet.sh"]}))
        decision = Decision(workspace_id=factory["ws"], outcome_id=outcome_id, task_id=task.id, kind="unblock",
                            title="T1 did not pass after 3 attempts",
                            options=[{"value": "retry", "label": "Try again"}, {"value": "take_over", "label": "I'll take it"},
                                     {"value": "abandon", "label": "Abandon"}])
        s.add(decision)
        s.flush()
        task_id, decision_id = task.id, decision.id
        user = s.get(User, factory["user"])
        email = user.email
    assert client.post("/auth/dev", json={"email": email, "name": "Dev"}).status_code == 200
    r = client.post(f"/api/workspaces/{factory['ws']}/decisions/{decision_id}/resolve", json={"resolution": "take_over"})
    assert r.status_code == 200, r.text
    assert r.json()["handover"]["published"] is True
    with session_scope() as s:
        assert s.get(Task, task_id).status == "handed_over"
        assert s.get(Decision, decision_id).status == "resolved"


def test_builder_can_work_in_an_isolated_vm(factory, monkeypatch, tmp_path):
    from aifactory import vm as vm_module

    from .test_vm import FakeBridge

    bridge = FakeBridge(tmp_path / "buildvm")
    monkeypatch.setattr(vm_module.VMBridge, "get", classmethod(lambda cls: bridge))
    monkeypatch.setattr(vm_module, "vm_available", lambda: (True, ""))
    with session_scope() as s:
        repo = s.get(Repository, factory["repo"])
        repo.config = {**repo.config, "build": {"isolation": "vm"}}
    outcome_id = new_outcome(factory)
    assert drive(lambda: outcome_status(outcome_id) in ("completed", "attention"), timeout=120)
    with session_scope() as s:
        task = s.query(Task).filter(Task.outcome_id == outcome_id).one()
        assert task.status == "completed", task.blocked_reason
        kinds = [e.kind for e in s.query(Event).filter(Event.task_id == task.id)]
        assert "build_vm_ready" in kinds
    assert "create" in bridge.calls and "destroy" in bridge.calls
    # The builder's edit happened inside the VM and still reached main through the normal publish path.
    assert git("show", "main:greet.sh", cwd=factory["remotes"] / "acme/demo.git").strip() == "echo 'Hello, Factory!'"


def test_a_conflict_with_newer_work_gets_a_fresh_attempt_budget(factory):
    def planner(messages):
        if step(messages) == 0:
            return {"content": "", "tool_calls": [call("list_files", {})]}
        return {"content": "", "tool_calls": [call("finish", {"summary": "Two edits", "questions": [], "tasks": [
            {"key": "T1", "title": "Write A", "repo": "acme/demo", "spec": "a", "acceptance": ["A written"], "depends_on": []},
            {"key": "T2", "title": "Write B", "repo": "acme/demo", "spec": "b", "acceptance": ["B written"], "depends_on": []}]})]}

    def builder(messages):
        name = "A" if "Write A" in messages[1]["content"] else "B"
        if step(messages) == 0:  # both tasks rewrite the same line: whichever merges second conflicts
            return {"content": "", "tool_calls": [call("write_file", {"path": "shared.txt", "content": f"{name}\n"}),
                                                  call("write_file", {"path": f"{name}.txt", "content": name}, 1)]}
        return {"content": "", "tool_calls": [call("finish", {"status": "done", "summary": f"wrote {name}"})]}

    def verifier(messages):
        criterion = "A written" if "A written" in messages[1]["content"] else "B written"
        if step(messages) == 0:
            return {"content": "", "tool_calls": [call("run", {"command": "ls"})]}
        return {"content": "", "tool_calls": [call("finish", {"verdict": "pass", "summary": "ok",
                "criteria": [{"criterion": criterion, "status": "pass", "evidence": "ls"}], "findings": []})]}

    ScriptedModel.scripts.update(planner=planner, builder=builder, verifier=verifier)
    with session_scope() as s:
        repo = s.get(Repository, factory["repo"])
        repo.config = {**repo.config, "checks": [], "smoke": ["true"]}
        ws = s.get(Workspace, factory["ws"])
        ws.settings = {**ws.settings, "max_attempts": 1}
    with session_scope() as s:  # seed the shared file so both edits touch the same line
        pass
    seed = factory["remotes"] / "seed" / "acme" / "demo"
    (seed / "shared.txt").write_text("base\n")
    git("add", "-A", cwd=seed)
    git("commit", "-qm", "shared", cwd=seed)
    git("push", "-q", str(factory["remotes"] / "acme/demo.git"), "main", cwd=seed)

    outcome_id = new_outcome(factory)
    assert drive(lambda: outcome_status(outcome_id) in ("completed", "attention"), timeout=120)
    with session_scope() as s:
        tasks = s.query(Task).filter(Task.outcome_id == outcome_id).all()
        assert outcome_status(outcome_id) == "completed", [(t.key, t.status, t.blocked_reason) for t in tasks]
        kinds = [e.kind for e in s.query(Event).filter(Event.outcome_id == outcome_id)]
        assert "rebuild_after_base_moved" in kinds
        rebuilt = [t for t in tasks if (t.evidence or {}).get("rebase_rebuilds")]
        assert len(rebuilt) == 1 and rebuilt[0].attempt == 2
    tree = git("ls-tree", "--name-only", "main", cwd=factory["remotes"] / "acme/demo.git").split()
    assert "A.txt" in tree and "B.txt" in tree
