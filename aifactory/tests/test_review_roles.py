from __future__ import annotations

import asyncio
import copy
import json

import pytest

from aifactory.harness.native import NativeHarness
from aifactory.harness.schemas import RISK_DIMENSIONS, SCHEMAS, validate_report
from aifactory.harness.tools import tool_specs
from aifactory.prompts import RISK_JUDGE, SECURITY_REVIEWER, risk_judge_prompt
from .fakes import ScriptedModel, call, low_risk_review, step
from .test_harness import make_session


def risk_report():
    return {"level": "low", "reasons": {d: "No impact." for d in RISK_DIMENSIONS}}


@pytest.mark.parametrize("level", ["low", "medium", "high", "critical"])
def test_risk_levels(level):
    report = risk_report()
    report["level"] = level
    assert validate_report("risk_judge", report) == (report, "")


@pytest.mark.parametrize("mutation", [
    lambda r: r.update(level="urgent"),
    lambda r: r.update(level=None),
    lambda r: r["reasons"].pop("deployment"),
    lambda r: r["reasons"].update(deployment=" "),
    lambda r: r.update(reasons=[]),
])
def test_bad_risk_reports(mutation):
    report = risk_report()
    mutation(report)
    assert validate_report("risk_judge", report)[0] is None


@pytest.mark.parametrize("verdict,severity,location,valid", [
    ("pass", "blocker", "src/a:1", False),
    ("fail", "blocker", "src/a:1", True),
    ("pass", "minor", "src/a:1", True),
    ("maybe", "minor", "src/a:1", False),
    ("fail", "urgent", "src/a:1", False),
    ("fail", "major", "", False),
])
def test_security_reports(verdict, severity, location, valid):
    report = {"verdict": verdict, "summary": "Review", "findings": [
        {"severity": severity, "location": location, "description": "Concrete evidence"}]}
    assert (validate_report("security_reviewer", report)[0] is not None) == valid


def test_judge_prompt_has_narrow_contract():
    prompt = risk_judge_prompt(diff="+safe", task_spec="spec", acceptance=["works"], protected_paths=["deploy/"])
    assert all(value in prompt for value in ("+safe", "spec", "works", "deploy/"))
    with pytest.raises(TypeError):
        risk_judge_prompt(diff="", task_spec=object(), acceptance=[], protected_paths=[])
    with pytest.raises(TypeError):
        risk_judge_prompt(diff="", task_spec="", acceptance=[], protected_paths=[], builder_report={})


async def test_judge_tools_and_every_message_are_blind(tmp_path):
    secret = "AUTHOR_WORKER_BUILDER_CLAIM_VERIFIER_PASS"
    calls = [
        ("list_files", {}), ("search", {"pattern": "AUTHOR_WORKER"}),
        ("search", {"pattern": "one"}), ("read_file", {"path": "a.txt"}),
        *[("read_file", {"path": path}) for path in (
            ".git/config", ".factory/build.json", "home/credentials", "sessions/other.json",
            "builder-report.json", "other.log", "../home/secret", "/etc/passwd", "link",
            "linked/secret", "a/../../home/secret")],
        ("list_files", {"path": ".git"}), ("search", {"path": ".factory", "pattern": "."}),
        ("search", {"pattern": "$(touch compromised); \""}),
        ("read_file", {"path": "a.txt; touch compromised"}),
        ("run", {"command": "touch compromised"}),
        ("write_file", {"path": "a.txt", "content": "bad"}),
        ("edit_file", {"path": "a.txt", "old_text": "one", "new_text": "bad"}),
    ]

    def fn(messages):
        n = step(messages)
        if n < len(calls):
            name, args = calls[n]
            return {"tool_calls": [call(name, args, n)]}
        return low_risk_review(messages)

    session, client, sb = make_session(tmp_path, fn, take_guidance=lambda: [secret])
    session.role = "risk_judge"
    session.system = RISK_JUDGE
    session.report_schema = SCHEMAS[session.role]
    session.prompt = risk_judge_prompt(diff="+safe", task_spec="inspect", acceptance=[], protected_paths=[])
    for path in (".git/config", ".factory/build.json", "home/credentials", "sessions/other.json",
                 "builder-report.json", "other.log"):
        target = sb.work / path
        target.parent.mkdir(exist_ok=True)
        target.write_text(secret)
    (sb.home / "secret").write_text(secret)
    (sb.work / "link").symlink_to(sb.home / "secret")
    (sb.work / "linked").symlink_to(sb.home, target_is_directory=True)
    (sb.work / "internal").symlink_to(sb.work / ".git", target_is_directory=True)
    result = await NativeHarness().run(session)
    assert result.report["level"] == "low"
    # Inspect every message on every model request, not just the initial prompt.
    assert secret not in json.dumps(client.seen)
    outputs = [m["content"] for m in client.seen[-1] if m["role"] == "tool"]
    assert "a.txt" in outputs[0] and ".git" not in outputs[0] and "internal" not in outputs[0]
    assert "no results" in outputs[1]
    assert "a.txt:1:one" in outputs[2] and "one" in outputs[3]
    assert all("Error:" in output or "no results" in output for output in outputs[4:])
    assert (sb.work / "a.txt").read_text() == "one\ntwo\n"
    assert not (sb.work / "compromised").exists()
    assert {t["function"]["name"] for t in tool_specs(True, role="risk_judge")} == {
        "list_files", "read_file", "search"}


@pytest.fixture
def review_runtime(client, signed_in, monkeypatch, tmp_path):
    from aifactory.db import session_scope
    from aifactory.models import Outcome
    from aifactory.inference import ProviderConfig
    from aifactory.pipeline import runtime
    from aifactory.sandbox import LocalSandbox

    ws = signed_in["workspaces"][0]["id"]
    with session_scope() as s:
        outcome = Outcome(workspace_id=ws, number=1, title="PRIVATE_BUILDER_CLAIM")
        s.add(outcome)
        s.flush()
        oid = outcome.id
    monkeypatch.setattr(runtime, "resolve_provider", lambda *a, **kw: ProviderConfig("openai", "http://test", "key", "Test"))
    monkeypatch.setattr(runtime, "ModelClient", ScriptedModel)
    from aifactory import inference
    monkeypatch.setattr(inference, "DEFAULT_PRICING", {"gpt-terra": (2, 8)})
    monkeypatch.setattr(ScriptedModel, "scripts", {})
    monkeypatch.setattr(ScriptedModel, "calls", [])
    sb = LocalSandbox("reviews", root=tmp_path).create()
    sb.work.mkdir()
    (sb.work / "code.py").write_text("print('hello')\n")
    return runtime, ws, oid, sb


@pytest.mark.parametrize("role", ["risk_judge", "security_reviewer"])
async def test_review_run_uses_verifier_model_usage_and_fresh_session(review_runtime, monkeypatch, role):
    runtime, ws, oid, sb = review_runtime
    seen = []
    original = ScriptedModel.chat

    async def chat(self, **kwargs):
        seen.append(copy.deepcopy(kwargs["messages"]))
        return await original(self, **kwargs)
    monkeypatch.setattr(ScriptedModel, "chat", chat)
    settings = {"models": {"verifier": "gpt-terra"}, "limits": {f"{role}_turns": 9999}}
    system = RISK_JUDGE if role == "risk_judge" else SECURITY_REVIEWER
    if role == "risk_judge":
        def forbidden(*a, **kw):
            raise AssertionError("Blind judge must never request guidance/history/attachments")
        for name in ("take_guidance", "guidance_history", "precedents", "prepare_attachments", "issue"):
            monkeypatch.setattr(runtime, name, forbidden)
    spec = runtime.SessionSpec(ws, oid, None, role, "codex" if role == "risk_judge" else "",
                               risk_judge_prompt(diff="+hello", task_spec="safe", acceptance=[], protected_paths=[]),
                               system, sb, effective_settings=settings)
    for _ in range(2):
        run, result, error = await runtime.run_agent(spec, asyncio.Event())
        assert not error and result and run.status == "succeeded"
        assert run.role == role and run.harness == "native" and run.model == "gpt-terra"
        assert run.input_tokens == 1000 and run.output_tokens == 200 and run.cost_known and run.cost_usd > 0
        assert run.result == result.report
    assert len(seen) == 2 and all(len(messages) == 2 for messages in seen)
    assert "PRIVATE_BUILDER_CLAIM" not in json.dumps(seen)


@pytest.mark.parametrize("budgets", [{"run_tokens": 0}, {"run_usd": 0}, {"outcome_usd": 0}])
async def test_preflight_zero_cost_does_not_poison_later_reviews(review_runtime, budgets):
    runtime, ws, oid, sb = review_runtime
    spec = runtime.SessionSpec(ws, oid, None, "risk_judge", "codex", "candidate", RISK_JUDGE, sb,
                               effective_settings={"budgets": budgets, "models": {"verifier": "gpt-terra"}})
    run, result, error = await runtime.run_agent(spec, asyncio.Event())
    assert error and result is None and run.status == "failed"
    assert run.cost_known and run.cost_usd == 0 and run.turns == 0
    assert ScriptedModel.calls == []
    assert runtime.outcome_cost(oid) == (0, True)
    spec = runtime.SessionSpec(ws, oid, None, "risk_judge", "native", "candidate", RISK_JUDGE, sb,
                               effective_settings={"models": {"verifier": "gpt-terra"}})
    run, result, error = await runtime.run_agent(spec, asyncio.Event())
    assert not error and result and run.cost_known


async def test_review_refuses_unknown_and_spent_outcome_budget(review_runtime):
    runtime, ws, oid, sb = review_runtime
    from aifactory.db import session_scope
    from aifactory.models import Run
    with session_scope() as s:
        previous = Run(workspace_id=ws, outcome_id=oid, role="builder", harness="native", provider="openai",
                       model="gpt-terra", cost_usd=50, cost_known=True)
        s.add(previous)
        s.flush()
        previous_id = previous.id
    spec = runtime.SessionSpec(ws, oid, None, "security_reviewer", "native", "candidate", SECURITY_REVIEWER, sb)
    run, _, error = await runtime.run_agent(spec, asyncio.Event())
    assert "budget exhausted" in error and run.cost_known and not ScriptedModel.calls
    with session_scope() as s:
        row = s.get(Run, previous_id)
        row.cost_known = False
        row.cost_usd = 0
    run, _, error = await runtime.run_agent(spec, asyncio.Event())
    assert "unknown" in error and run.cost_known and not ScriptedModel.calls


async def test_review_turns_are_bounded(review_runtime, monkeypatch):
    from aifactory.harness import HarnessResult
    runtime, ws, oid, sb = review_runtime
    seen = []

    class Harness:
        label = "Native"

        async def run(self, session):
            seen.append(session.max_turns)
            session.ledger.add("gpt-terra", (1, 1))
            return HarnessResult(report=risk_report(), turns=0)

    monkeypatch.setattr(runtime, "get_harness", lambda _: Harness())
    for role in ("risk_judge", "security_reviewer"):
        for configured, expected in ((-1, 1), (999999, 60), (7, 7)):
            spec = runtime.SessionSpec(ws, oid, None, role, "native", "candidate", RISK_JUDGE, sb,
                                       effective_settings={"limits": {f"{role}_turns": configured}})
            _, _, error = await runtime.run_agent(spec, asyncio.Event())
            assert not error and seen[-1] == expected
