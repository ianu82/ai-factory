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
        branch = task.branch
    assert FakeGitHub.merged == [1]
    main_greet = git("show", "main:greet.sh", cwd=factory["remotes"] / "acme/demo.git")
    assert "Hello, Factory!" in main_greet
    # Commits are signed "MindsHub AI Factory": a bare "AI Factory" / ai-factory identity is a real GitHub user.
    signed = git("log", "-1", "--format=%an <%ae>|%cn <%ce>", branch, cwd=factory["remotes"] / "acme/demo.git")
    assert signed == "MindsHub AI Factory <factory@mindshub.ai>|MindsHub AI Factory <factory@mindshub.ai>"
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

    verifier_heard = []

    def listening_verifier(messages):
        if step(messages) == 0:
            prompt = messages[1]["content"]
            verifier_heard.append("## What people decided or asked for on this work" in prompt
                                  and "Use 'Hello, Factory!'" in prompt)
        return passing_verifier(messages)

    ScriptedModel.scripts["builder"] = listening_builder
    ScriptedModel.scripts["verifier"] = listening_verifier
    db = SessionLocal()
    resolve(db, db.get(Decision, decision_id), db.get(User, factory["user"]), "answer",
            comment="Use 'Hello, Factory!'", reusable=True, scope="workspace")
    db.close()
    assert drive(lambda: outcome_status(outcome_id) == "completed")
    assert heard, "the answer was not delivered to the builder"
    assert verifier_heard and all(verifier_heard), "the verifier must judge by the same decisions as the builder"
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


def _land_on_main(factory, path, content):
    seed = factory["remotes"] / "seed" / "acme" / "demo"
    (seed / path).write_text(content)
    git("add", "-A", cwd=seed)
    git("commit", "-qm", f"land {path}", cwd=seed)
    git("push", "-q", str(factory["remotes"] / "acme/demo.git"), "main", cwd=seed)
    return git("rev-parse", "HEAD", cwd=seed)


def _event_kinds(outcome_id):
    with session_scope() as s:
        return [e.kind for e in s.query(Event).filter(Event.outcome_id == outcome_id).order_by(Event.id)]


def _request_changes_after_main_moves(factory, move):
    """Merge needs approval; while it waits, ``move()`` moves main; the reviewer requests changes.
    Returns (outcome, main sha)."""
    from aifactory.db import SessionLocal
    from aifactory.decisions import resolve

    with session_scope() as s:
        ws = s.get(Workspace, factory["ws"])
        ws.settings = {**ws.settings, "autonomy": {"plan": "auto", "merge": "approval", "release": "auto"}}
    outcome_id = new_outcome(factory)

    def merge_decision():
        with session_scope() as s:
            d = s.query(Decision).filter(Decision.outcome_id == outcome_id, Decision.kind == "approve_merge",
                                         Decision.status == "open").one_or_none()
            return d.id if d else None

    assert drive(lambda: merge_decision() is not None)
    main = move()
    db = SessionLocal()
    resolve(db, db.get(Decision, merge_decision()), db.get(User, factory["user"]), "reject",
            comment="Please also keep the script POSIX-only.")
    db.close()
    assert drive(lambda: merge_decision() is not None)
    return outcome_id, main


def _approve_and_finish(factory, outcome_id):
    from aifactory.db import SessionLocal
    from aifactory.decisions import resolve

    with session_scope() as s:
        decision = s.query(Decision).filter(Decision.outcome_id == outcome_id, Decision.kind == "approve_merge",
                                            Decision.status == "open").one()
    db = SessionLocal()
    resolve(db, db.get(Decision, decision.id), db.get(User, factory["user"]), "approve")
    db.close()
    finished = drive(lambda: outcome_status(outcome_id) == "completed")
    with session_scope() as s:
        task = s.query(Task).filter(Task.outcome_id == outcome_id).one()
        assert finished, (task.status, task.blocked_reason, " ".join(_event_kinds(outcome_id)))


def test_a_requested_revision_is_rebased_onto_newer_main_first(factory):
    outcome_id, main = _request_changes_after_main_moves(
        factory, lambda: _land_on_main(factory, "other.txt", "landed meanwhile\n"))
    kinds = _event_kinds(outcome_id)
    assert "rebased" in kinds and "rebuild_on_main" not in kinds, " ".join(kinds)
    with session_scope() as s:
        assert s.query(Task).filter(Task.outcome_id == outcome_id).one().base_sha == main
    _approve_and_finish(factory, outcome_id)
    bare = factory["remotes"] / "acme/demo.git"
    assert git("show", "main:greet.sh", cwd=bare) == "echo 'Hello, Factory!'"
    assert git("show", "main:other.txt", cwd=bare) == "landed meanwhile"


def test_a_conflicting_revision_keeps_the_earlier_change_and_resolves_only_the_overlap(factory):
    told, saw_markers, rejected = [], [], []

    def builder(messages):
        prompt = messages[1]["content"]
        if "Unresolved merge conflict markers" in prompt:
            rejected.append(True)
        elif "contain conflict markers" in prompt and "greet.sh" in prompt:
            told.append(True)
            if step(messages) == 0:
                return {"content": "", "tool_calls": [call("read_file", {"path": "greet.sh"})]}
            saw_markers.append(any("<<<<<<< ours" in str(m.get("content")) for m in messages))
            # first try: finish without resolving, so the policy check must catch the markers
            return {"content": "", "tool_calls": [call("finish", {"status": "done", "summary": "left as is"})]}
        return good_builder(messages)

    ScriptedModel.scripts["builder"] = builder
    outcome_id, main = _request_changes_after_main_moves(
        factory, lambda: _land_on_main(factory, "greet.sh", "echo 'Hi'\n"))
    assert told and saw_markers == [True] and rejected
    kinds = _event_kinds(outcome_id)
    assert "rebase_conflicts" in kinds and "rebuild_on_main" not in kinds, " ".join(kinds)
    assert "candidate_rejected" in kinds
    with session_scope() as s:
        assert s.query(Task).filter(Task.outcome_id == outcome_id).one().base_sha == main
    _approve_and_finish(factory, outcome_id)
    bare = factory["remotes"] / "acme/demo.git"
    assert git("show", "main:greet.sh", cwd=bare) == "echo 'Hello, Factory!'"
    assert "land greet.sh" in git("log", "--format=%s", "main", cwd=bare)


def test_a_binary_conflict_is_never_settled_silently_in_mains_favour(factory):
    seed = factory["remotes"] / "seed" / "acme" / "demo"
    (seed / "logo.bin").write_bytes(b"A\x00base")
    git("add", "-A", cwd=seed)
    git("commit", "-qm", "logo", cwd=seed)
    git("push", "-q", str(factory["remotes"] / "acme/demo.git"), "main", cwd=seed)
    told = []

    def builder(messages):
        if "cannot be merged line by line" in messages[1]["content"]:
            told.append(True)
        n = step(messages)
        if n == 0:
            return {"content": "", "tool_calls": [
                call("write_file", {"path": "greet.sh", "content": "echo 'Hello, Factory!'\n"}),
                call("write_file", {"path": "logo.bin", "content": "A\x00task"}, 1)]}
        return {"content": "", "tool_calls": [call("finish", {"status": "done", "summary": "greeting and logo"})]}

    def move():
        (seed / "logo.bin").write_bytes(b"A\x00main")
        git("commit", "-qam", "new logo", cwd=seed)
        git("push", "-q", str(factory["remotes"] / "acme/demo.git"), "main", cwd=seed)
        return git("rev-parse", "HEAD", cwd=seed)

    ScriptedModel.scripts["builder"] = builder
    outcome_id, main = _request_changes_after_main_moves(factory, move)
    assert told
    kinds = _event_kinds(outcome_id)
    assert "rebuild_on_main" in kinds and "rebase_conflicts" not in kinds, " ".join(kinds)
    _approve_and_finish(factory, outcome_id)
    bare = factory["remotes"] / "acme/demo.git"
    assert git("show", "main:logo.bin", cwd=bare) == "A\x00task"


def test_conflict_markers_are_found_only_at_the_start_of_added_lines():
    diff = (b"diff --git a/m.py b/m.py\n--- a/m.py\n+++ b/m.py\n@@ -1 +1,5 @@\n+<<<<<<< ours\n+x\n+=======\n"
            b"+y\n+>>>>>>> theirs\ndiff --git a/t.py b/t.py\n--- a/t.py\n+++ b/t.py\n@@ -1 +1 @@\n"
            b"+    MARK = '<<<<<<< ours'\ndiff --git a/gone.py b/gone.py\n--- a/gone.py\n+++ /dev/null\n")
    patch = gitops.Patch(diff=diff, files=["m.py", "t.py", "gone.py"], additions=6, deletions=0)
    assert gitops.conflict_marker_files(patch) == ["m.py"]


def test_a_revision_replays_only_its_own_commits_when_main_was_rewritten(factory):
    _land_on_main(factory, "dropme.txt", "reverted by force-push later\n")
    seed = factory["remotes"] / "seed" / "acme" / "demo"

    def rewrite_main():  # drop the dropme.txt commit from history, then land something new
        git("reset", "-q", "--hard", "HEAD~1", cwd=seed)
        git("push", "-q", "-f", str(factory["remotes"] / "acme/demo.git"), "main", cwd=seed)
        return _land_on_main(factory, "other.txt", "landed meanwhile\n")

    outcome_id, main = _request_changes_after_main_moves(factory, rewrite_main)
    assert "rebased" in _event_kinds(outcome_id)
    _approve_and_finish(factory, outcome_id)
    tree = git("ls-tree", "--name-only", "main", cwd=factory["remotes"] / "acme/demo.git").split()
    assert "other.txt" in tree and "dropme.txt" not in tree


def test_rebasing_a_published_branch_replays_only_its_own_commits(factory):
    seed = factory["remotes"] / "seed" / "acme" / "demo"
    bare = factory["remotes"] / "acme/demo.git"
    base = _land_on_main(factory, "dropme.txt", "removed by a force-push later\n")
    git("checkout", "-q", "-b", "factory/9-t1", cwd=seed)
    (seed / "task.txt").write_text("task\n")
    git("add", "-A", cwd=seed)
    git("commit", "-qm", "task", cwd=seed)
    git("push", "-q", str(bare), "factory/9-t1", cwd=seed)
    git("checkout", "-q", "main", cwd=seed)
    git("reset", "-q", "--hard", "HEAD~1", cwd=seed)
    git("push", "-q", "-f", str(bare), "main", cwd=seed)
    _land_on_main(factory, "other.txt", "landed\n")

    clean, head = asyncio.run(gitops.rebase_branch("acme/demo", "token", branch="factory/9-t1", onto="main",
                                                   task_id="t-rebase", base=base))
    assert clean
    tree = git("ls-tree", "--name-only", head, cwd=bare).split()
    assert "task.txt" in tree and "other.txt" in tree and "dropme.txt" not in tree


def test_a_branch_someone_else_pushed_to_is_never_rebased_into_the_candidate(factory):
    states = iter(["none"])
    original = FakeGitHub.checks
    seed = factory["remotes"] / "seed" / "acme" / "demo"
    bare = factory["remotes"] / "acme/demo.git"

    async def checks(self, full_name, sha):
        state = next(states, "none")
        if state == "none" and not getattr(checks, "moved", False):
            checks.moved = True
            _land_on_main(factory, "other.txt", "landed meanwhile\n")
            branch = git("for-each-ref", "--format=%(refname:short)", "refs/heads/factory/", cwd=bare).split()[0]
            git("fetch", "-q", str(bare), f"{branch}:pushed", cwd=seed)
            git("checkout", "-q", "pushed", cwd=seed)
            (seed / "sneaky.txt").write_text("unverified\n")
            git("add", "-A", cwd=seed)
            git("commit", "-qm", "unverified push", cwd=seed)
            git("push", "-q", str(bare), f"pushed:{branch}", cwd=seed)
            git("checkout", "-q", "main", cwd=seed)
        FakeGitHub.check_state = state
        return await original(self, full_name, sha)

    FakeGitHub.checks = checks
    with session_scope() as s:
        repo = s.get(Repository, factory["repo"])
        repo.config = {**repo.config, "require_ci": True}
    try:
        outcome_id = new_outcome(factory)
        assert drive(lambda: outcome_status(outcome_id) in ("completed", "attention", "needs_input"))
    finally:
        FakeGitHub.checks = original
    with session_scope() as s:
        task = s.query(Task).filter(Task.outcome_id == outcome_id).one()
        assert task.status == "needs_input" and "no longer matches the verified candidate" in task.blocked_reason
    assert "sneaky.txt" not in git("ls-tree", "--name-only", "main", cwd=bare).split()


def test_a_fix_that_gets_no_ci_because_main_moved_is_revalidated_not_blocked(factory):
    states = iter(["failure", "none"])
    original = FakeGitHub.checks

    async def checks(self, full_name, sha):
        state = next(states, "success")
        if state == "failure":
            _land_on_main(factory, "other.txt", "landed meanwhile\n")
        FakeGitHub.check_state = state
        return await original(self, full_name, sha)

    FakeGitHub.checks = checks
    with session_scope() as s:
        repo = s.get(Repository, factory["repo"])
        repo.config = {**repo.config, "require_ci": True}
    try:
        outcome_id = new_outcome(factory)
        assert drive(lambda: outcome_status(outcome_id) in ("completed", "attention"))
    finally:
        FakeGitHub.checks = original
    with session_scope() as s:
        task = s.query(Task).filter(Task.outcome_id == outcome_id).one()
        assert outcome_status(outcome_id) == "completed", task.blocked_reason
    kinds = _event_kinds(outcome_id)
    assert kinds.index("revalidate") < kinds.index("merged"), " ".join(kinds)


def _always_failing_verifier(messages):
    if step(messages) == 0:
        return {"content": "", "tool_calls": [call("run", {"command": "sh test.sh"})]}
    return {"content": "", "tool_calls": [call("finish", {
        "verdict": "fail", "summary": "Greeting still wrong.",
        "criteria": [{"criterion": CRITERION, "status": "fail", "evidence": "prints Hello"}], "findings": []})]}


def _attempts_and_block(outcome_id):
    with session_scope() as s:
        task = s.query(Task).filter(Task.outcome_id == outcome_id).one()
        decision = s.query(Decision).filter(Decision.task_id == task.id, Decision.status == "open").one()
        return task.id, [a["result"] for a in task.evidence["attempts"]], decision.title, decision.id


def test_attempts_exhausted_on_concrete_findings_get_one_automatic_retry(factory):
    from aifactory.db import SessionLocal
    from aifactory.decisions import resolve

    ScriptedModel.scripts["verifier"] = _always_failing_verifier
    with session_scope() as s:
        ws = s.get(Workspace, factory["ws"])
        ws.settings = {**ws.settings, "max_attempts": 1}
    outcome_id = new_outcome(factory)
    assert drive(lambda: outcome_status(outcome_id) in ("attention", "completed", "needs_input"))
    task_id, results, title, decision_id = _attempts_and_block(outcome_id)
    assert results == ["verification_failed", "verification_failed"]
    assert title == "T1 did not pass after 1 attempts and an automatic retry"
    assert "auto_retry" in _event_kinds(outcome_id)

    # A person's retry restores the automatic round for the next budget.
    db = SessionLocal()
    resolve(db, db.get(Decision, decision_id), db.get(User, factory["user"]), "retry", comment="Try harder.")
    db.close()
    with session_scope() as s:
        assert "auto_retries" not in (s.get(Task, task_id).evidence or {})


def test_an_attempt_that_ended_in_an_infrastructure_failure_is_not_retried_automatically(factory, monkeypatch):
    from aifactory.sandbox import SandboxError

    ScriptedModel.scripts["verifier"] = _always_failing_verifier
    original = delivery.extract_patch
    calls = []

    async def flaky_extract(sandbox, base_sha):
        calls.append(1)
        if len(calls) >= 2:  # the last attempt cannot even read the sandbox: nothing is recorded for it
            raise SandboxError("sandbox went away")
        return await original(sandbox, base_sha)

    monkeypatch.setattr(delivery, "extract_patch", flaky_extract)
    with session_scope() as s:
        ws = s.get(Workspace, factory["ws"])
        ws.settings = {**ws.settings, "max_attempts": 2}
    outcome_id = new_outcome(factory)
    assert drive(lambda: outcome_status(outcome_id) in ("attention", "completed", "needs_input"))
    _, results, title, _ = _attempts_and_block(outcome_id)
    assert results == ["verification_failed"] and title == "T1 did not pass after 2 attempts"
    assert "auto_retry" not in _event_kinds(outcome_id)


def test_automatic_retry_rounds_from_bootstrap_are_clamped(factory):
    ScriptedModel.scripts["verifier"] = _always_failing_verifier
    with session_scope() as s:  # bootstrap writes settings directly, without the API's clamp
        ws = s.get(Workspace, factory["ws"])
        ws.settings = {**ws.settings, "max_attempts": 1, "auto_retry_rounds": 50}
    outcome_id = new_outcome(factory)
    assert drive(lambda: outcome_status(outcome_id) in ("attention", "completed", "needs_input"), timeout=120)
    _, results, title, _ = _attempts_and_block(outcome_id)
    assert len(results) == 4 and title == "T1 did not pass after 1 attempts and 3 automatic retries"


def test_failed_check_output_survives_a_restart_into_the_next_attempt(factory):
    from aifactory.pipeline.delivery import TaskRunner

    outcome_id = new_outcome(factory)
    with session_scope() as s:
        from aifactory.models import Plan

        plan = Plan(outcome_id=outcome_id, version=1, status="approved", content={})
        s.add(plan)
        s.flush()
        task = Task(workspace_id=factory["ws"], outcome_id=outcome_id, plan_id=plan.id, key="T1", title="t", spec="s",
                    acceptance=["c"], repo_id=factory["repo"], status="queued",
                    evidence={"attempts": [{"n": 1, "result": "checks_failed", "checks": delivery._brief([
                        {"command": "sh test.sh", "ok": False, "exit_code": 1, "duration": 0.1,
                         "tail": "expected greeting, got: Hello"},
                        {"command": "true", "ok": True, "exit_code": 0, "duration": 0.0, "tail": ""}])}]})
        s.add(task)
        s.flush()
        task_id = task.id
    notes = TaskRunner(task_id, asyncio.Event(), lambda _: asyncio.Lock())._prior_feedback()
    assert len(notes) == 1 and "`sh test.sh` failed (exit 1)" in notes[0] and "expected greeting, got: Hello" in notes[0]


def test_a_requested_revision_starts_with_a_fresh_budget_and_automatic_round(factory):
    from aifactory.db import SessionLocal
    from aifactory.decisions import resolve
    from aifactory.models import Plan

    outcome_id = new_outcome(factory)
    with session_scope() as s:
        plan = Plan(outcome_id=outcome_id, version=1, status="approved", content={})
        s.add(plan)
        s.flush()
        task = Task(workspace_id=factory["ws"], outcome_id=outcome_id, plan_id=plan.id, key="T1", title="t",
                    repo_id=factory["repo"], status="awaiting_approval", stage="merge", attempt=5, pr_number=3,
                    evidence={"attempt_base": 3, "auto_retries": 1, "attempts": []})
        s.add(task)
        s.flush()
        decision = Decision(workspace_id=factory["ws"], outcome_id=outcome_id, task_id=task.id, kind="approve_merge",
                            title="Approve merge", options=[{"value": "approve", "label": "Merge"},
                                                            {"value": "reject", "label": "Request changes"}])
        s.add(decision)
        s.flush()
        task_id, decision_id = task.id, decision.id
    db = SessionLocal()
    resolve(db, db.get(Decision, decision_id), db.get(User, factory["user"]), "reject", comment="Rename the flag.")
    db.close()
    with session_scope() as s:
        task = s.get(Task, task_id)
        assert task.stage == "revise" and task.attempt == 0
        assert "attempt_base" not in task.evidence and "auto_retries" not in task.evidence


def test_only_actionable_failures_earn_an_automatic_retry():
    assert delivery._actionable({"result": "verification_failed", "verdict": {
        "criteria": [{"criterion": "c", "status": "fail"}], "findings": []}})
    assert delivery._actionable({"result": "verification_failed", "verdict": {
        "criteria": [{"criterion": "c", "status": "pass"}], "findings": [{"severity": "major", "description": "x"}]}})
    assert not delivery._actionable({"result": "verification_failed", "verdict": {  # minor findings are not relayed
        "criteria": [{"criterion": "c", "status": "pass"}], "findings": [{"severity": "minor", "description": "x"}]}})
    assert not delivery._actionable({"result": "verification_failed", "verdict": {
        "verdict": "fail", "criteria": [{"criterion": "c", "status": "pass"}], "findings": []}})
    assert not delivery._actionable({"result": "verification_failed", "verdict": {"verdict": "fail"}})
    assert not delivery._actionable({"result": "verification_failed", "verdict": {"criteria": None, "findings": None}})
    assert delivery._actionable({"result": "verification_failed", "verdict": {
        "criteria": None, "findings": [{"severity": "major", "description": "x"}]}})
    assert delivery._actionable({"result": "checks_failed", "checks": [{"command": "t", "ok": False}]})
    assert not delivery._actionable({"result": "checks_failed", "checks": []})
    assert delivery._actionable({"result": "policy", "detail": "- Too big"})
    assert not delivery._actionable({"result": "builder_failed"}) and not delivery._actionable({})


def test_a_required_runtime_qa_failure_is_recorded_as_a_finding_and_retried(factory):
    def verifier_with_null_findings(messages):  # findings is optional and may be null in a valid report
        reply = passing_verifier(messages)
        for tool_call in reply.get("tool_calls", []):
            if tool_call["function"]["name"] == "finish":
                args = json.loads(tool_call["function"]["arguments"])
                args["findings"] = None
                tool_call["function"]["arguments"] = json.dumps(args)
        return reply

    ScriptedModel.scripts["verifier"] = verifier_with_null_findings
    with session_scope() as s:
        repo = s.get(Repository, factory["repo"])
        repo.config = {**repo.config, "qa": {"start": "exec sleep 30", "ready_path": "/", "ready_timeout": 2,
                                             "pages": [], "required": True}}
        ws = s.get(Workspace, factory["ws"])
        ws.settings = {**ws.settings, "max_attempts": 1}
    outcome_id = new_outcome(factory)
    assert drive(lambda: outcome_status(outcome_id) in ("attention", "completed", "needs_input"), timeout=120)
    task_id, results, title, _ = _attempts_and_block(outcome_id)
    assert results == ["verification_failed", "verification_failed"]
    assert title == "T1 did not pass after 1 attempts and an automatic retry"
    with session_scope() as s:
        verdict = s.get(Task, task_id).evidence["attempts"][-1]["verdict"]
        assert any("Required runtime QA failed: the app did not start" in f["description"] for f in verdict["findings"])


def test_a_runtime_qa_failure_is_recorded_even_when_the_verifier_already_failed(factory):
    def bare_failing_verifier(messages):  # fails with all criteria passing and no findings
        if step(messages) == 0:
            return {"content": "", "tool_calls": [call("run", {"command": "true"})]}
        return {"content": "", "tool_calls": [call("finish", {
            "verdict": "fail", "summary": "Not convinced.",
            "criteria": [{"criterion": CRITERION, "status": "pass", "evidence": "ok"}], "findings": []})]}

    ScriptedModel.scripts["verifier"] = bare_failing_verifier
    with session_scope() as s:
        repo = s.get(Repository, factory["repo"])
        repo.config = {**repo.config, "qa": {"start": "exec sleep 30", "ready_path": "/", "ready_timeout": 2,
                                             "pages": [], "required": True}}
        ws = s.get(Workspace, factory["ws"])
        ws.settings = {**ws.settings, "max_attempts": 1}
    outcome_id = new_outcome(factory)
    assert drive(lambda: outcome_status(outcome_id) in ("attention", "completed", "needs_input"), timeout=120)
    task_id, results, title, _ = _attempts_and_block(outcome_id)
    assert results == ["verification_failed", "verification_failed"] and "automatic retry" in title
    with session_scope() as s:
        verdict = s.get(Task, task_id).evidence["attempts"][0]["verdict"]
        assert verdict["summary"] == "Not convinced." and verdict["findings"][-1]["location"] == "runtime QA"


def test_automatic_retries_can_be_turned_off(factory):
    ScriptedModel.scripts["verifier"] = _always_failing_verifier
    with session_scope() as s:
        ws = s.get(Workspace, factory["ws"])
        ws.settings = {**ws.settings, "max_attempts": 1, "auto_retry_rounds": 0}
    outcome_id = new_outcome(factory)
    assert drive(lambda: outcome_status(outcome_id) in ("attention", "completed", "needs_input"))
    _, results, title, _ = _attempts_and_block(outcome_id)
    assert results == ["verification_failed"] and title == "T1 did not pass after 1 attempts"
    assert "auto_retry" not in _event_kinds(outcome_id)


def test_an_attempt_interrupted_after_a_retry_is_redone_not_counted(factory):
    """Attempt numbers restart after a person's retry; old records with the same numbers must not make an
    interrupted attempt look finished (seen live: a restart skipped attempt 1 of a fresh budget)."""
    from aifactory.db import SessionLocal
    from aifactory.decisions import resolve
    from aifactory.models import Plan

    outcome_id = new_outcome(factory)
    with session_scope() as s:
        plan = Plan(outcome_id=outcome_id, version=1, status="approved", content={})
        s.add(plan)
        s.flush()
        old = [{"n": n, "result": "verification_failed", "verdict": {"verdict": "fail", "summary": "old"}} for n in (1, 2, 3)]
        task = Task(workspace_id=factory["ws"], outcome_id=outcome_id, plan_id=plan.id, key="T1", title="Update greeting",
                    spec="Make greet.sh print Hello, Factory!", acceptance=[CRITERION], repo_id=factory["repo"],
                    status="failed", attempt=3, evidence={"attempts": old})
        s.add(task)
        s.flush()
        s.add(Decision(workspace_id=factory["ws"], outcome_id=outcome_id, task_id=task.id, kind="unblock",
                       title="T1 did not pass after 3 attempts", options=[{"value": "retry", "label": "Try again"}]))
        s.get(Outcome, outcome_id).status = "running"
        task_id = task.id
    with session_scope() as s:
        decision_id = s.query(Decision).filter(Decision.task_id == task_id).one().id
    db = SessionLocal()
    resolve(db, db.get(Decision, decision_id), db.get(User, factory["user"]), "retry", comment="Try again.")
    db.close()
    with session_scope() as s:  # attempt 1 of the new budget started, then a restart interrupted it
        task = s.get(Task, task_id)
        task.attempt = 1
        assert task.evidence["budget_epoch"] == 1
    assert drive(lambda: outcome_status(outcome_id) in ("completed", "attention"))
    with session_scope() as s:
        task = s.get(Task, task_id)
        assert task.status == "completed", task.blocked_reason
        current = [a for a in task.evidence["attempts"] if a.get("epoch") == 1]
        assert [a["n"] for a in current] == [1] and current[0]["result"] == "verified"
    assert "attempt_resumed" in _event_kinds(outcome_id)


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
        kinds = [e.kind for e in s.query(Event).filter(Event.outcome_id == outcome_id).order_by(Event.id)]
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
        kinds = [e.kind for e in s.query(Event).filter(Event.outcome_id == outcome_id).order_by(Event.id)]
        assert "rebuild_after_base_moved" in kinds
        rebuilt = [t for t in tasks if (t.evidence or {}).get("rebase_rebuilds")]
        assert len(rebuilt) == 1 and rebuilt[0].attempt == 2
    tree = git("ls-tree", "--name-only", "main", cwd=factory["remotes"] / "acme/demo.git").split()
    assert "A.txt" in tree and "B.txt" in tree


def test_abandoning_a_task_closes_its_pull_request_with_a_note(factory, client, monkeypatch):
    from aifactory.models import Plan
    from aifactory.pipeline import handover

    monkeypatch.setattr(handover, "GitHub", FakeGitHub)
    monkeypatch.setattr(handover, "resolve_token", lambda ws: "token")
    FakeGitHub.comments = []
    git("push", "-q", str(factory["remotes"] / "acme/demo.git"), "main:refs/heads/factory/1-t1",
        cwd=factory["remotes"] / "seed" / "acme" / "demo")
    pr = asyncio.run(FakeGitHub("t").create_pull("acme/demo", head="factory/1-t1", base="main", title="t", body="b"))
    outcome_id = new_outcome(factory)
    with session_scope() as s:
        plan = Plan(outcome_id=outcome_id, version=1, status="approved", content={})
        s.add(plan)
        s.flush()
        task = Task(workspace_id=factory["ws"], outcome_id=outcome_id, plan_id=plan.id, key="T1", title="t",
                    repo_id=factory["repo"], status="failed", pr_number=pr["number"], branch="factory/1-t1")
        s.add(task)
        s.flush()
        decision = Decision(workspace_id=factory["ws"], outcome_id=outcome_id, task_id=task.id, kind="unblock",
                            title="T1 did not pass", options=[{"value": "abandon", "label": "Abandon"}])
        s.add(decision)
        s.flush()
        task_id, decision_id, email = task.id, decision.id, s.get(User, factory["user"]).email
    assert client.post("/auth/dev", json={"email": email, "name": "Dev"}).status_code == 200
    r = client.post(f"/api/workspaces/{factory['ws']}/decisions/{decision_id}/resolve", json={"resolution": "abandon"})
    assert r.status_code == 200 and r.json()["pr_closed"] is True
    assert FakeGitHub.pulls[pr["number"]]["state"] == "closed"
    assert FakeGitHub.comments and "Abandoned in MindsHub AI Factory by Dev" in FakeGitHub.comments[-1][1]
    with session_scope() as s:
        assert s.get(Task, task_id).status == "abandoned" and s.get(Task, task_id).evidence["pr_closed"]


def test_a_split_outcome_closes_its_issue_with_the_last_merge(factory):
    def planner(messages):
        if step(messages) == 0:
            return {"content": "", "tool_calls": [call("list_files", {})]}
        return {"content": "", "tool_calls": [call("finish", {"summary": "Two files", "questions": [], "tasks": [
            {"key": "T1", "title": "Add a.txt", "repo": "acme/demo", "spec": "a", "acceptance": ["a.txt exists"], "depends_on": []},
            {"key": "T2", "title": "Add b.txt", "repo": "acme/demo", "spec": "b", "acceptance": ["b.txt exists"], "depends_on": []}]})]}

    def builder(messages):
        name = "a.txt" if "Add a.txt" in messages[1]["content"] else "b.txt"
        if step(messages) == 0:
            return {"content": "", "tool_calls": [call("write_file", {"path": name, "content": name})]}
        return {"content": "", "tool_calls": [call("finish", {"status": "done", "summary": f"added {name}"})]}

    def verifier(messages):
        criterion = "a.txt exists" if "a.txt exists" in messages[1]["content"] else "b.txt exists"
        if step(messages) == 0:
            return {"content": "", "tool_calls": [call("run", {"command": "ls"})]}
        return {"content": "", "tool_calls": [call("finish", {"verdict": "pass", "summary": "ok",
                "criteria": [{"criterion": criterion, "status": "pass", "evidence": "ls"}], "findings": []})]}

    ScriptedModel.scripts.update(planner=planner, builder=builder, verifier=verifier)
    FakeGitHub.issues_state = {}
    with session_scope() as s:
        repo = s.get(Repository, factory["repo"])
        repo.config = {**repo.config, "checks": [], "smoke": ["true"]}
    outcome_id = new_outcome(factory)
    with session_scope() as s:
        s.get(Outcome, outcome_id).brief = "Two files.\n\nSource: https://github.com/acme/demo/issues/7"
    assert drive(lambda: outcome_status(outcome_id) in ("completed", "attention"), timeout=120)
    assert outcome_status(outcome_id) == "completed"
    bodies = [pr["body"] for pr in FakeGitHub.pulls.values()]
    assert sum("Closes #7" in b for b in bodies) == 1, bodies
    assert FakeGitHub.issues_state.get(7) == "closed"
    with session_scope() as s:
        kinds = [e.kind for e in s.query(Event).filter(Event.outcome_id == outcome_id).order_by(Event.id)]
    assert "issue_closed" in kinds and "issue_close_failed" not in kinds


def test_an_issue_the_factory_cannot_close_is_reported_plainly(factory, monkeypatch):
    from aifactory.github import GitHubError

    original_merge, original_request = FakeGitHub.merge, FakeGitHub.request

    async def merge_without_keywords(self, full_name, number, **kwargs):  # e.g. the base is not the default branch
        state = dict(FakeGitHub.issues_state)
        result = await original_merge(self, full_name, number, **kwargs)
        FakeGitHub.issues_state = state
        return result

    async def read_only_issues(self, method, path, **kwargs):
        if "/issues/" in path and method == "PATCH":
            raise GitHubError("Resource not accessible by personal access token", 403)
        return await original_request(self, method, path, **kwargs)

    monkeypatch.setattr(FakeGitHub, "merge", merge_without_keywords)
    monkeypatch.setattr(FakeGitHub, "request", read_only_issues)
    FakeGitHub.issues_state = {}
    outcome_id = new_outcome(factory)
    with session_scope() as s:
        s.get(Outcome, outcome_id).brief = "Greeting.\n\nSource: https://github.com/acme/demo/issues/9"
    assert drive(lambda: outcome_status(outcome_id) in ("completed", "attention"), timeout=120)
    assert outcome_status(outcome_id) == "completed"  # still delivered
    with session_scope() as s:
        failed = s.query(Event).filter(Event.outcome_id == outcome_id, Event.kind == "issue_close_failed").one()
    assert "issue #9 is still open" in failed.message and "cannot write issues" in failed.message


def _waiting_task_with_pr(factory, title="Greeting"):
    from aifactory.models import Plan

    base = git("rev-parse", "refs/heads/main", cwd=factory["remotes"] / "acme/demo.git")
    seed = factory["remotes"] / "seed" / "acme" / "demo"
    git("checkout", "-q", "-B", "factory/1-t1", cwd=seed)
    (seed / "greet.sh").write_text("echo 'Hello, Factory!'\n")
    git("commit", "-qam", "greeting", cwd=seed)
    git("push", "-q", "-f", str(factory["remotes"] / "acme/demo.git"), "factory/1-t1", cwd=seed)
    head = git("rev-parse", "HEAD", cwd=seed)
    git("checkout", "-q", "main", cwd=seed)
    pr = asyncio.run(FakeGitHub("t").create_pull("acme/demo", head="factory/1-t1", base="main", title=title, body="b"))
    outcome_id = new_outcome(factory)
    with session_scope() as s:
        s.get(Outcome, outcome_id).status = "attention"  # already planned: the task below waits on a person
        plan = Plan(outcome_id=outcome_id, version=1, status="approved", content={})
        s.add(plan)
        s.flush()
        task = Task(workspace_id=factory["ws"], outcome_id=outcome_id, plan_id=plan.id, key="T1", title=title,
                    repo_id=factory["repo"], status="awaiting_approval", stage="merge", base_sha=base, head_sha=head,
                    branch="factory/1-t1", pr_number=pr["number"], pr_url=pr["html_url"])
        s.add(task)
        s.flush()
        s.add(Decision(workspace_id=factory["ws"], outcome_id=outcome_id, task_id=task.id, kind="approve_merge",
                       title="Approve merge", options=[{"value": "approve", "label": "Merge"}]))
        return outcome_id, task.id, pr["number"]


def test_a_pr_merged_on_github_finishes_its_task_without_asking_again(factory, monkeypatch):
    from aifactory import reconcile

    monkeypatch.setattr(reconcile, "GitHub", FakeGitHub)
    monkeypatch.setattr(reconcile, "resolve_token", lambda ws: "token")
    with session_scope() as s:
        ws = s.get(Workspace, factory["ws"])
        ws.settings = {**ws.settings, "autonomy": {**ws.settings["autonomy"], "merge": "approval"}}
    outcome_id, task_id, number = _waiting_task_with_pr(factory)
    asyncio.run(FakeGitHub("t").merge("acme/demo", number, sha="", method="squash", title="merged by a person"))
    assert asyncio.run(reconcile.reconcile_pull_requests()) == 1

    def task_settled():
        with session_scope() as s:
            return s.get(Task, task_id).status in ("completed", "failed", "needs_input", "awaiting_approval")

    assert drive(task_settled, timeout=60)
    with session_scope() as s:
        task = s.get(Task, task_id)
        assert task.status == "completed", (task.status, task.blocked_reason)
        assert task.merge_sha and task.evidence["merge"]["outside"]
        decisions = s.query(Decision).filter(Decision.task_id == task_id).all()
        assert [(d.kind, d.status) for d in decisions] == [("approve_merge", "superseded")]  # no new approval card


def test_a_pr_closed_on_github_becomes_a_rebuild_decision(factory, monkeypatch):
    from aifactory import reconcile
    from aifactory.decisions import resolve

    monkeypatch.setattr(reconcile, "GitHub", FakeGitHub)
    monkeypatch.setattr(reconcile, "resolve_token", lambda ws: "token")
    outcome_id, task_id, number = _waiting_task_with_pr(factory)
    FakeGitHub.pulls[number]["state"] = "closed"
    assert asyncio.run(reconcile.reconcile_pull_requests()) == 1
    assert asyncio.run(reconcile.reconcile_pull_requests()) == 0  # handled once
    with session_scope() as s:
        task = s.get(Task, task_id)
        assert task.status == "failed" and task.pr_number is None and task.evidence["closed_prs"] == [number]
        open_ = s.query(Decision).filter(Decision.task_id == task_id, Decision.status == "open").one()
        assert open_.kind == "unblock" and f"#{number} was closed" in open_.title
        resolve(s, open_, s.get(User, factory["user"]), "retry")

    def task_settled():
        with session_scope() as s:
            return s.get(Task, task_id).status in ("completed", "failed", "needs_input", "awaiting_approval")

    assert drive(task_settled, timeout=90)
    with session_scope() as s:
        task = s.get(Task, task_id)
        assert task.status == "completed" and task.pr_number != number  # rebuilt with a new pull request


def test_reconciliation_leaves_a_task_people_resolved_meanwhile_alone(factory, monkeypatch):
    from aifactory import reconcile

    outcome_id, task_id, number = _waiting_task_with_pr(factory)
    with session_scope() as s:  # someone abandons the task while GitHub is being asked
        s.get(Task, task_id).status = "abandoned"
    pr = {"number": number, "merged": True, "merged_by": {"login": "lead"}, "state": "closed"}
    assert reconcile._handle_merged(task_id, pr) is False
    assert reconcile._handle_closed(task_id, {**pr, "merged": False}) is False
    with session_scope() as s:
        task = s.get(Task, task_id)
        assert task.status == "abandoned" and task.pr_number == number
        assert s.query(Decision).filter(Decision.task_id == task_id).one().status == "open"


def test_all_agent_roles_receive_attachments_and_publish_only_code(factory):
    import base64
    import shlex

    from .test_attachments import seed_references

    outcome_id = new_outcome(factory)
    rows = seed_references(factory["ws"], outcome_id)
    seen = set()

    def wrap(role, script):
        def checked(messages):
            prompt = messages[1]["content"]
            for aid, name, raw, mime in rows:
                path = f".factory/attachments/{aid}-{name}"
                assert name in prompt
                assert f"MIME: {mime}; decoded size: {len(raw)} bytes; path: {path}" in prompt
            if step(messages) == 0:
                seen.add(role)
                checks = [f'test "$(base64 < {shlex.quote(f".factory/attachments/{aid}-{name}")} | tr -d \'\\n\')" = '
                          + shlex.quote(base64.b64encode(raw).decode()) for aid, name, raw, _ in rows]
                if role == "builder":
                    # Simulate the accidental force-add that gitignore cannot prevent.
                    checks += ["git add -f .factory/attachments", "printf \"echo 'Hello, Factory!'\\n\" > greet.sh"]
                return {"content": "", "tool_calls": [call("run", {"command": " && ".join(checks)})]}
            # The first command must actually have succeeded, not just been requested.
            tool = next(m for m in messages if m["role"] == "tool")
            assert "[exit code 0," in tool["content"], tool
            return script(messages)
        return checked

    ScriptedModel.scripts = {"planner": wrap("planner", planner_script), "builder": wrap("builder", good_builder),
                             "verifier": wrap("verifier", passing_verifier)}
    assert drive(lambda: outcome_status(outcome_id) in ("completed", "attention"), timeout=90)
    with session_scope() as s:
        task = s.query(Task).filter(Task.outcome_id == outcome_id).one()
        assert task.status == "completed", task.blocked_reason
        branch = task.branch
    assert seen == {"planner", "builder", "verifier"}
    assert FakeGitHub.pulls
    for ref in ("main", branch):
        tree = git("ls-tree", "-r", "--name-only", ref, cwd=factory["remotes"] / "acme/demo.git")
        assert ".factory/attachments" not in tree
        assert "Factory!" in git("show", f"{ref}:greet.sh", cwd=factory["remotes"] / "acme/demo.git")


@pytest.mark.parametrize("harness_name", ["native", "codex", "anton"])
def test_shared_agent_hook_rehydrates_every_role_attempt_and_resume(factory, tmp_path, monkeypatch, harness_name):
    from aifactory.harness import HarnessResult
    from aifactory.pipeline.runtime import SessionSpec, run_agent
    from aifactory.sandbox import LocalSandbox

    from .test_attachments import seed_references

    outcome_id = new_outcome(factory)
    rows = seed_references(factory["ws"], outcome_id)
    sb = LocalSandbox("retry-refs", root=tmp_path / "retry-refs").create()
    sb.work.mkdir()
    prompts = []

    class FakeHarness:
        label = "Fake non-native harness"

        async def run(self, session):
            assert len(session.image_inputs) == 1
            for aid, name, raw, mime in rows:
                path = f".factory/attachments/{aid}-{name}"
                assert (sb.work / path).read_bytes() == raw
                assert f"MIME: {mime}; decoded size: {len(raw)} bytes; path: {path}" in session.prompt
                sb.write_bytes(path, b"builder-modified copy")
            prompts.append(session.prompt)
            return HarnessResult(report={}, turns=1)

    monkeypatch.setattr(runtime, "get_harness", lambda _: FakeHarness())

    async def go():
        for role, attempt in [("planner", 1), ("builder", 1), ("builder", 1), ("builder", 2),
                              ("verifier", 1), ("verifier", 2)]:
            run, result, error = await run_agent(SessionSpec(factory["ws"], outcome_id, None, role, harness_name,
                                                            "task", "system", sb, attempt=attempt), asyncio.Event())
            assert run.status == "succeeded" and result and not error
        assert len(prompts) == 6 and len(set(prompts)) == 1

    asyncio.run(go())


@pytest.mark.parametrize("failure", ["base64", "symlink", "size", "html",
                                      "image/png", "image/jpeg", "image/gif", "image/webp", "wrong-mime"])
def test_attachment_setup_failure_finishes_run_and_revokes_gateway(factory, tmp_path, monkeypatch, failure):
    from aifactory.models import Artifact
    from aifactory.pipeline.runtime import SessionSpec, run_agent
    from aifactory.sandbox import LocalSandbox

    from .test_attachments import attachment

    outcome_id = new_outcome(factory)
    sb = LocalSandbox("failure", root=tmp_path / "failure").create()
    sb.work.mkdir()
    with session_scope() as s:
        row = attachment(id="broken")
        row.workspace_id, row.outcome_id = factory["ws"], outcome_id
        if failure == "base64":
            row.content = "broken!"
        elif failure == "size":
            row.data = {**row.data, "size_bytes": 999}
        elif failure == "html":
            import base64

            raw = b"<!doctype html><html>not markdown</html>"
            row.content = base64.b64encode(raw).decode()
            row.data = {"mime_type": "text/markdown", "size_bytes": len(raw)}
        elif failure.startswith("image/") or failure == "wrong-mime":
            import base64

            from .test_attachments import MALFORMED_IMAGES, PNG

            raw = PNG if failure == "wrong-mime" else MALFORMED_IMAGES[failure]
            row.content = base64.b64encode(raw).decode()
            row.data = {"mime_type": "image/jpeg" if failure == "wrong-mime" else failure,
                        "size_bytes": len(raw)}
        else:
            outside = tmp_path / "outside"
            outside.mkdir()
            (sb.work / ".factory").symlink_to(outside, target_is_directory=True)
        s.add(row)
    granted, revoked = [], []
    monkeypatch.setattr(runtime, "issue", lambda grant: granted.append(grant.run_id) or "grant")
    monkeypatch.setattr(runtime, "revoke", revoked.append)

    class NeverHarness:
        label = "Never"

        async def run(self, session):
            pytest.fail("invalid attachment reached the harness")

    monkeypatch.setattr(runtime, "get_harness", lambda _: NeverHarness())
    run, result, error = asyncio.run(run_agent(SessionSpec(factory["ws"], outcome_id, None, "builder", "codex",
                                                          "task", "system", sb), asyncio.Event()))
    assert error and result is None and run.status == "failed" and run.finished_at
    assert granted == revoked == [run.id]
    with session_scope() as s:
        stored = s.get(Run, run.id)
        assert stored.status == "failed" and stored.finished_at and stored.error == error
        assert s.query(Event).filter(Event.run_id == run.id, Event.kind == "run_finished").count() == 1
    if failure == "symlink":
        assert list(outside.iterdir()) == []
    else:
        assert list(sb.work.iterdir()) == []


def test_no_attachments_leave_shared_session_unchanged(factory, tmp_path, monkeypatch):
    from aifactory.harness import HarnessResult
    from aifactory.pipeline.runtime import SessionSpec, run_agent
    from aifactory.sandbox import LocalSandbox

    outcome_id = new_outcome(factory)
    sb = LocalSandbox("no-refs", root=tmp_path / "no-refs").create()
    sb.work.mkdir()

    class NoAttachmentsHarness:
        label = "No attachments"

        async def run(self, session):
            assert session.prompt == "original prompt"
            assert session.image_inputs == []
            assert not (sb.work / ".factory/attachments").exists()
            session.image_inputs.append({"name": "private", "mime_type": "image/png", "content": ""})
            return HarnessResult(report={}, turns=1)

    monkeypatch.setattr(runtime, "get_harness", lambda _: NoAttachmentsHarness())

    async def go():
        for _ in range(2):
            run, result, error = await run_agent(SessionSpec(factory["ws"], outcome_id, None, "builder", "anton",
                                                            "original prompt", "system", sb), asyncio.Event())
            assert run.status == "succeeded" and result and not error

    asyncio.run(go())
