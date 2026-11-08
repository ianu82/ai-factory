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
