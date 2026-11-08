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
