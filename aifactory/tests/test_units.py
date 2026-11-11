from __future__ import annotations

import asyncio
import os

import pytest

from aifactory.harness.schemas import validate_report
from aifactory.sandbox import LocalSandbox, SandboxError
from aifactory.security import decrypt, encrypt, mask, redact


def test_encrypt_round_trip_and_mask():
    token = encrypt("mdb_abc123.secretvalue")
    assert "secretvalue" not in token
    assert decrypt(token) == "mdb_abc123.secretvalue"
    assert mask("mdb_abc123.secretvalue") == "mdb_abc123…alue"
    assert "ghp" not in mask("ghp_" + "x" * 30)[-4:]


def test_redact_removes_credentials():
    text = "key mdb_abc.def123 and ghp_" + "a" * 30 + " and Authorization: Bearer xyz.abc"
    out = redact(text)
    assert "def123" not in out and "aaaaaaaa" not in out and "xyz.abc" not in out


def test_planner_report_rejects_cycles_and_unknown_deps():
    task = {"key": "T1", "title": "x", "repo": "a/b", "spec": "s", "acceptance": ["a"], "depends_on": ["T2"]}
    ok, err = validate_report("planner", {"summary": "s", "tasks": [task], "questions": []})
    assert ok is None and "unknown" in err
    t2 = dict(task, key="T2", depends_on=["T1"])
    ok, err = validate_report("planner", {"summary": "s", "tasks": [task, t2], "questions": []})
    assert ok is None and "cycle" in err


def test_verifier_cannot_pass_with_failing_criterion():
    report = {"verdict": "pass", "summary": "ok", "criteria": [{"criterion": "c", "status": "fail", "evidence": "e"}]}
    ok, err = validate_report("verifier", report)
    assert ok is None and "every criterion" in err


def test_verifier_reports_need_lists_of_objects():
    for criteria in (["the endpoint returns 200"], "all criteria pass"):
        ok, err = validate_report("verifier", {"verdict": "pass", "summary": "ok", "criteria": criteria})
        assert ok is None and "criteria must be a list of objects" in err
    ok, err = validate_report("verifier", {"verdict": "fail", "summary": "no", "criteria": [], "findings": ["bad"]})
    assert ok is None and "findings must be a list of objects" in err


def test_stored_verdicts_with_malformed_lists_are_read_without_crashing():
    from aifactory.pipeline.delivery import _actionable, _objects

    assert _objects(["c", {"criterion": "d"}, None]) == [{"criterion": "d"}]
    assert _objects("text") == [] and _objects(None) == []
    verdict = {"criteria": ["c", {"criterion": "d", "status": "fail"}], "findings": "none"}
    assert _actionable({"result": "verification_failed", "verdict": verdict})


def test_sandbox_exec_timeout_and_clipping(tmp_path):
    sb = LocalSandbox("t1", root=tmp_path).create()
    sb.work.mkdir()

    async def go():
        r = await sb.exec("echo hi; exit 3")
        assert r.exit_code == 3 and "hi" in r.output
        r = await sb.exec("sleep 5", timeout=0.5)
        assert r.timed_out and not r.ok
        r = await sb.exec("python3 -c \"print('x'*100000)\"")
        assert r.truncated and "omitted" in r.output
        env = await sb.exec("env")
        assert "AIFACTORY_SECRET_KEY" not in env.output
        await sb.close()

    asyncio.run(go())


def test_sandbox_refuses_path_escape(tmp_path):
    sb = LocalSandbox("t2", root=tmp_path).create()
    sb.work.mkdir()
    secret = tmp_path / "secret.txt"
    secret.write_text("top secret")
    os.symlink(secret, sb.work / "link.txt")
    with pytest.raises(SandboxError):
        sb.resolve("../secret.txt")
    with pytest.raises(SandboxError):
        sb.read_text("link.txt")
    sb.write_text("dir/new.txt", "ok")
    assert sb.read_text("dir/new.txt") == "ok"


def test_candidate_patch_with_new_files_is_visible_to_the_verifier(tmp_path):
    import subprocess

    from aifactory.gitops import Patch, apply_in_sandbox

    sb = LocalSandbox("v", root=tmp_path).create()
    sb.work.mkdir()
    env = {"GIT_AUTHOR_NAME": "t", "GIT_AUTHOR_EMAIL": "t@t", "GIT_COMMITTER_NAME": "t", "GIT_COMMITTER_EMAIL": "t@t",
           "PATH": os.environ["PATH"], "HOME": str(tmp_path)}
    run = lambda *a: subprocess.run(a, cwd=sb.work, check=True, capture_output=True, text=True, env=env).stdout
    run("git", "init", "-q")
    (sb.work / "a.txt").write_text("a\n")
    run("git", "add", ".")
    run("git", "commit", "-qm", "base")
    base = run("git", "rev-parse", "HEAD").strip()
    diff = (b"diff --git a/a.txt b/a.txt\nindex 7898192..422c2b7 100644\n--- a/a.txt\n+++ b/a.txt\n@@ -1 +1,2 @@\n a\n+b\n"
            b"diff --git a/new.py b/new.py\nnew file mode 100644\nindex 0000000..3e75765\n--- /dev/null\n"
            b"+++ b/new.py\n@@ -0,0 +1 @@\n+new\n")
    # Stale index stat data (as after re-owning the checkout to the job identity).
    os.utime(sb.work / "a.txt", (1, 1))
    asyncio.run(apply_in_sandbox(sb, Patch(diff=diff, files=["a.txt", "new.py"], additions=2, deletions=0)))
    stat = run("git", "diff", "--stat", base)
    assert "new.py" in stat and "a.txt" in stat


def test_job_cpu_sets_are_small_and_spread_by_uid(monkeypatch):
    import os

    from aifactory import sandbox

    monkeypatch.setattr(os, "sched_getaffinity", lambda pid: set(range(48)), raising=False)
    monkeypatch.setenv("AIFACTORY_SANDBOX_CPUS", "4")
    a, b = sandbox.job_cpus(20000), sandbox.job_cpus(20001)
    assert len(a) == len(b) == 4 and a.isdisjoint(b)
    monkeypatch.setenv("AIFACTORY_SANDBOX_CPUS", "0")
    assert sandbox.job_cpus(20000) is None
    monkeypatch.setattr(os, "sched_getaffinity", lambda pid: {0, 1}, raising=False)
    monkeypatch.setenv("AIFACTORY_SANDBOX_CPUS", "4")
    assert sandbox.job_cpus(20000) is None  # a small host is left alone


def test_unpriced_runs_are_priced_later_from_provider_usage(monkeypatch):
    import asyncio

    from aifactory import inference
    from aifactory.db import session_scope, utcnow
    from aifactory.models import Run
    from aifactory.pipeline.orchestrator import Orchestrator

    async def session_cost(self, session_id):
        return 0.0731 if session_id == "run-late" else None

    monkeypatch.setattr(inference.ModelClient, "session_cost", session_cost)
    monkeypatch.setattr(inference, "resolve_provider",
                        lambda ws: inference.ProviderConfig("mindshub", "http://model.test/v1", "k", "MindsHub"))
    from aifactory.models import Organization, Outcome, Workspace

    with session_scope() as s:
        org = Organization(name="Acme")
        s.add(org)
        s.flush()
        ws = Workspace(org_id=org.id, name="Acme", slug="acme", settings={})
        s.add(ws)
        s.flush()
        outcome = Outcome(workspace_id=ws.id, number=1, title="t", brief="", repo_ids=[], status="running")
        s.add(outcome)
        s.flush()
        for rid in ("run-late", "run-never"):
            s.add(Run(id=rid, workspace_id=ws.id, outcome_id=outcome.id, role="builder", harness="native",
                      provider="mindshub", status="cancelled", cost_known=False, finished_at=utcnow()))
    asyncio.run(Orchestrator()._maybe_reconcile_costs())
    with session_scope() as s:
        late, never = s.get(Run, "run-late"), s.get(Run, "run-never")
        assert late.cost_known and late.cost_usd == 0.0731
        assert not never.cost_known


def test_mirror_fetches_and_job_clones_never_overlap(tmp_path, monkeypatch):
    import asyncio

    from aifactory import gitops

    active, overlaps = set(), []

    async def fake_git(*args, **kwargs):
        op = "fetch" if "fetch" in args else "clone" if "clone" in args else None
        if op:
            if active:
                overlaps.append((op, set(active)))
            active.add(op + str(id(args)))
            await asyncio.sleep(0.05)
            active.discard(op + str(id(args)))
        return ""

    monkeypatch.setattr(gitops, "git", fake_git)
    monkeypatch.setattr(gitops, "_mirror_path", lambda name: tmp_path / "m.git")
    (tmp_path / "m.git").mkdir()
    (tmp_path / "m.git" / "HEAD").write_text("ref: refs/heads/main\n")

    async def go():
        await asyncio.gather(gitops.mirror("acme/app", "t"), gitops.clone_mirror(tmp_path / "m.git", tmp_path / "a", {}),
                             gitops.mirror("acme/app", "t"), gitops.clone_mirror(tmp_path / "m.git", tmp_path / "b", {}))

    asyncio.run(go())
    assert overlaps == []


def test_verifiers_are_told_to_separate_pre_existing_failures_from_the_change():
    from aifactory.prompts import BUILDER, verifier_system

    prompt = verifier_system("abc1234")
    assert "clean checkout of abc1234" in prompt and "pre-existing" in prompt
    assert "do not fail a criterion" in prompt and "Opt-in test suites" in prompt
    assert "unrelated to the task" in prompt and "that failure still occurring is a real failure" in prompt
    assert "unless the change touches the integration they" in prompt
    assert "pre-existing" in BUILDER


def test_repository_precedents_apply_only_to_their_own_repository():
    from aifactory.db import session_scope, utcnow
    from aifactory.models import Decision, Organization, Outcome, Plan, Repository, Task, Workspace
    from aifactory.pipeline.runtime import precedents

    with session_scope() as s:
        org = Organization(name="Acme")
        s.add(org)
        s.flush()
        ws = Workspace(org_id=org.id, name="Acme", slug="acme-precedents", settings={})
        s.add(ws)
        s.flush()
        a, b = Repository(workspace_id=ws.id, full_name="acme/a"), Repository(workspace_id=ws.id, full_name="acme/b")
        s.add_all([a, b])
        s.flush()
        outcome = Outcome(workspace_id=ws.id, number=1, title="t", repo_ids=[a.id, b.id], status="running")
        s.add(outcome)
        s.flush()
        plan = Plan(outcome_id=outcome.id, status="approved", content={})
        s.add(plan)
        s.flush()
        task = Task(workspace_id=ws.id, outcome_id=outcome.id, plan_id=plan.id, key="T1", title="t", repo_id=a.id)
        s.add(task)
        s.flush()
        for title, scope, task_id in (("Keep trailing slashes", "repository", task.id), ("Use UK English", "workspace", None)):
            s.add(Decision(workspace_id=ws.id, outcome_id=outcome.id, task_id=task_id, kind="question", title=title,
                           status="resolved", resolution="answer", reusable=True, scope=scope, resolved_at=utcnow()))
        ws_id, a_id, b_id, outcome_id, task_id = ws.id, a.id, b.id, outcome.id, task.id

    titles = lambda repo=None: sorted(d.title for d in precedents(ws_id, repo))  # noqa: E731
    assert titles(a_id) == ["Keep trailing slashes", "Use UK English"]
    assert titles(b_id) == ["Use UK English"]
    assert titles() == ["Keep trailing slashes", "Use UK English"]

    # Many newer decisions about repository A must not crowd out what applies to B.
    with session_scope() as s:
        for i in range(60):
            s.add(Decision(workspace_id=ws_id, outcome_id=outcome_id, task_id=task_id, kind="question", title=f"A{i}",
                           status="resolved", resolution="answer", reusable=True, scope="repository",
                           resolved_at=utcnow()))
    assert titles(b_id) == ["Use UK English"]


@pytest.mark.parametrize("scratch_state", ["unignored", "force-staged", "committed"])
def test_extract_patch_excludes_all_root_scratch_and_external_files(tmp_path, scratch_state):
    import subprocess

    from aifactory.gitops import extract_patch

    sb = LocalSandbox("patch", root=tmp_path).create()
    sb.work.mkdir()
    env = {**os.environ, "GIT_AUTHOR_NAME": "t", "GIT_AUTHOR_EMAIL": "t@t",
           "GIT_COMMITTER_NAME": "t", "GIT_COMMITTER_EMAIL": "t@t"}
    def git(*args, cwd=sb.work, data=None):
        return subprocess.run(["git", *args], cwd=cwd, env=env, input=data,
                              check=True, capture_output=True).stdout
    git("init", "-q")
    (sb.work / "source.py").write_text("before\n")
    if scratch_state == "force-staged":
        (sb.work / ".gitignore").write_text("/.factory/\n")
    git("add", ".")
    git("commit", "-qm", "base")
    base = git("rev-parse", "HEAD").decode().strip()
    (sb.work / ".factory" / "deep").mkdir(parents=True)
    (sb.work / ".factory" / "deep" / "private.bin").write_bytes(b"\0private screenshot")
    (sb.work / ".factory" / "notes.txt").write_text("private note\n" * 20)
    if scratch_state in {"force-staged", "committed"}:
        git("add", "-f", ".factory")
    if scratch_state == "committed":
        git("commit", "-qm", "agent scratch commit")
    # Simulate cells creating source in the checkout and scratch outside it.
    async def go():
        result = await sb.exec("printf 'after\\n' > source.py; printf 'new source\\n' > cell.py; "
                               "mkdir -p nested/.factory; printf 'keep\\n' > nested/.factory/ordinary.txt; "
                               'printf secret > "$HOME/home.txt"; printf secret > "$TMPDIR/tmp.txt"')
        assert result.ok
        return await extract_patch(sb, base)
    patch = asyncio.run(go())
    assert set(patch.files) == {"source.py", "cell.py", "nested/.factory/ordinary.txt"}
    assert patch.additions == 3 and patch.deletions == 1
    assert b"private" not in patch.diff and b"home.txt" not in patch.diff and b"tmp.txt" not in patch.diff
    assert b"a/.factory" not in patch.diff and b"b/.factory" not in patch.diff
    # Publication still applies the extracted binary patch to a pristine base.
    clean = tmp_path / "clean"
    git("clone", "-q", str(sb.work), str(clean))
    git("checkout", "-q", base, cwd=clean)
    git("apply", "--index", "--binary", "-", cwd=clean, data=patch.diff)
    assert (clean / "cell.py").read_text() == "new source\n"
    assert (clean / "source.py").read_text() == "after\n"
    assert not (clean / ".factory").exists()
    asyncio.run(sb.close())


def test_rate_limit_settings_parse_and_fall_back(monkeypatch):
    from aifactory.config import get_settings, reset_settings_cache

    monkeypatch.setenv("AIFACTORY_MINDSHUB_REQUESTS_PER_MINUTE", "120")
    monkeypatch.setenv("AIFACTORY_OPENAI_REQUESTS_PER_MINUTE", "not-a-number")
    reset_settings_cache()
    try:
        settings = get_settings()
        assert settings.mindshub_requests_per_minute == 120 and settings.openai_requests_per_minute == 50
    finally:
        reset_settings_cache()


def test_outcome_progress_distinguishes_queued_from_started_work():
    from aifactory.models import Outcome, Task
    from aifactory.serializers import outcome

    o = Outcome(id="o1", workspace_id="w", number=31, title="t", repo_ids=[], status="running")
    queued = [Task(id=f"t{i}", key=f"T{i}", status=s, attempt=0) for i, s in enumerate(["queued", "pending", "pending"])]
    assert outcome(o, queued)["progress"] == {"done": 0, "total": 3, "started": 0, "active": 0}
    mixed = queued[:2] + [Task(id="t9", key="T9", status="running", attempt=1)]
    assert outcome(o, mixed)["progress"] == {"done": 0, "total": 3, "started": 1, "active": 1}
    retried = [Task(id="t8", key="T8", status="queued", attempt=2, pr_number=4)]  # back in the queue after work
    assert outcome(o, retried)["progress"]["started"] == 1
