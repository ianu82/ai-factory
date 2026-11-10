"""The plan reviewer checks every automatically approved plan before building starts."""
from __future__ import annotations

import asyncio
import json

import pytest

from aifactory.db import session_scope
from aifactory.harness.schemas import validate_report
from aifactory.inference import PLAN_REVIEWER_MODEL, workspace_models
from aifactory.models import Decision, Event, Outcome, Plan, Run, Task, Workspace
from aifactory.pipeline import planning

from .fakes import ScriptedModel, call
from .test_pipeline import factory, new_outcome, planner_script  # noqa: F401 - workspace, repository and user fixture

REVIEW_PROMPTS: list[str] = []
PLANNER_PROMPTS: list[str] = []


def finish(report):
    return {"content": "", "tool_calls": [call("finish", report)]}


def revise(messages):
    REVIEW_PROMPTS.append(messages[1]["content"])
    return finish({"verdict": "revise", "summary": "T1 has no test.",
                   "findings": [{"severity": "major", "task": "T1", "description": "Add a test for the new greeting."}]})


def approve(messages):
    REVIEW_PROMPTS.append(messages[1]["content"])
    return finish({"verdict": "approve", "summary": "Covers the request.",
                   "findings": [{"severity": "minor", "task": "T1", "description": "Name the test file."}]})


def recording_planner(messages):
    if len(messages) == 2:
        PLANNER_PROMPTS.append(messages[1]["content"])
    return planner_script(messages)


@pytest.fixture(autouse=True)
def recorded(factory):
    REVIEW_PROMPTS.clear()
    PLANNER_PROMPTS.clear()
    ScriptedModel.scripts["planner"] = recording_planner
    return factory


def plan(outcome_id):
    asyncio.run(planning.plan_outcome(outcome_id, asyncio.Event()))
    with session_scope() as s:
        outcome = s.get(Outcome, outcome_id)
        plans = s.query(Plan).filter_by(outcome_id=outcome_id).order_by(Plan.version).all()
        tasks = s.query(Task).filter_by(outcome_id=outcome_id).count()
        decision = s.query(Decision).filter_by(outcome_id=outcome_id, status="open").one_or_none()
        roles = [r.role for r in s.query(Run).filter_by(outcome_id=outcome_id).order_by(Run.started_at)]
        return (outcome.status, [(p.version, p.status, (p.content.get("review") or {}).get("verdict")) for p in plans],
                tasks, decision and (decision.kind, decision.body), roles)


def test_an_approved_plan_starts_building(factory):
    ScriptedModel.scripts["plan_reviewer"] = approve
    oid = new_outcome(factory)
    status, plans, tasks, decision, roles = plan(oid)
    assert status == "running" and tasks == 1 and decision is None
    assert plans == [(1, "approved", "approve")]
    assert roles == ["planner", "plan_reviewer"]
    with session_scope() as s:
        review = s.query(Plan).filter_by(outcome_id=oid).one().content["review"]
        assert review["model"] == PLAN_REVIEWER_MODEL and review["findings"][0]["severity"] == "minor"
        approved = s.query(Event).filter_by(outcome_id=oid, kind="plan_approved").one()
        assert approved.message == f"Plan approved by the plan reviewer ({PLAN_REVIEWER_MODEL})"
        assert approved.actor == "policy"
    # The reviewer sees the request, the plan and the repository's limits.
    assert "Say hello to the Factory." in REVIEW_PROMPTS[0]
    assert "### T1: Update greeting" in REVIEW_PROMPTS[0] and "greet.sh prints exactly" in REVIEW_PROMPTS[0]
    assert "Protected paths (no change may touch them): .github/workflows/**" in REVIEW_PROMPTS[0]


def test_a_revision_goes_back_to_the_planner_with_the_findings(factory):
    verdicts = iter([revise, approve])
    ScriptedModel.scripts["plan_reviewer"] = lambda messages: next(verdicts)(messages)
    oid = new_outcome(factory)
    status, plans, tasks, decision, roles = plan(oid)
    assert status == "running" and tasks == 1 and decision is None
    assert plans == [(1, "superseded", "revise"), (2, "approved", "approve")]
    assert roles == ["planner", "plan_reviewer", "planner", "plan_reviewer"]
    assert "needs changes" not in PLANNER_PROMPTS[0]
    revised = PLANNER_PROMPTS[1]
    assert "## Your previous plan was reviewed and needs changes" in revised
    assert "- major (T1): Add a test for the new greeting." in revised
    assert "## The previous plan" in revised and "### T1: Update greeting" in revised
    with session_scope() as s:
        reviewed = [e.message for e in s.query(Event).filter_by(outcome_id=oid, kind="plan_reviewed").order_by(Event.id)]
    assert reviewed[0].startswith("Plan v1 sent back to the planner by the plan reviewer: T1 has no test.")
    assert reviewed[1].startswith("Plan v2 approved by the plan reviewer")


def test_a_plan_the_reviewer_never_approves_goes_to_a_person(factory):
    ScriptedModel.scripts["plan_reviewer"] = revise
    oid = new_outcome(factory)
    status, plans, tasks, decision, roles = plan(oid)
    rounds = planning.PLAN_REVIEW_ROUNDS
    assert status == "awaiting_approval" and tasks == 0
    assert [p[2] for p in plans] == ["revise"] * (rounds + 1)
    assert [p[1] for p in plans] == ["superseded"] * rounds + ["proposed"]
    assert roles == ["planner", "plan_reviewer"] * (rounds + 1)
    kind, body = decision
    assert kind == "approve_plan"
    assert f"it still had concerns after {rounds} revisions" in body
    assert "- major (T1): Add a test for the new greeting." in body


def test_an_escalation_asks_a_person_at_once(factory):
    ScriptedModel.scripts["plan_reviewer"] = lambda messages: finish({
        "verdict": "escalate", "summary": "Outcome #7 already delivers this greeting.", "findings": []})
    oid = new_outcome(factory)
    status, plans, tasks, decision, roles = plan(oid)
    assert status == "awaiting_approval" and tasks == 0 and roles == ["planner", "plan_reviewer"]
    assert plans == [(1, "proposed", "escalate")]
    assert "it needs a person to decide" in decision[1] and "Outcome #7 already delivers" in decision[1]


def test_a_review_that_cannot_finish_never_approves(factory):
    def unavailable(messages):
        raise RuntimeError("The model provider is momentarily overloaded")

    ScriptedModel.scripts["plan_reviewer"] = unavailable
    oid = new_outcome(factory)
    status, plans, tasks, decision, _ = plan(oid)
    assert status == "awaiting_approval" and tasks == 0
    assert plans == [(1, "proposed", "error")]
    assert "the review could not finish" in decision[1] and "momentarily overloaded" in decision[1]


def test_a_failed_revision_leaves_the_reviewed_plan_with_a_person(factory):
    def planner(messages):
        if "needs changes" in messages[1]["content"]:
            raise RuntimeError("planner crashed")
        return planner_script(messages)

    ScriptedModel.scripts["planner"] = planner
    ScriptedModel.scripts["plan_reviewer"] = revise
    oid = new_outcome(factory)
    status, plans, tasks, decision, _ = plan(oid)
    assert status == "awaiting_approval" and tasks == 0
    assert plans == [(1, "proposed", "revise")]
    assert decision[0] == "approve_plan"
    assert "could not revise this plan after the review: planner crashed" in decision[1]
    assert "- major (T1): Add a test for the new greeting." in decision[1]


def test_approve_first_skips_the_reviewer(factory):
    with session_scope() as s:
        ws = s.get(Workspace, factory["ws"])
        ws.settings = {**ws.settings, "autonomy": {**ws.settings["autonomy"], "plan": "approval"}}
    oid = new_outcome(factory)
    status, plans, tasks, decision, roles = plan(oid)
    assert status == "awaiting_approval" and tasks == 0 and roles == ["planner"]
    assert plans == [(1, "proposed", None)]
    assert decision[0] == "approve_plan" and "plan reviewer" not in decision[1]


def test_planner_questions_go_to_a_person_without_a_review(factory):
    def asking(messages):
        result = planner_script(messages)
        if result["tool_calls"][0]["function"]["name"] == "finish":
            report = json.loads(result["tool_calls"][0]["function"]["arguments"])
            report["questions"] = [{"question": "Exclamation mark or full stop?", "why": "Brand voice"}]
            return finish(report)
        return result

    ScriptedModel.scripts["planner"] = asking
    oid = new_outcome(factory)
    status, _, _, decision, roles = plan(oid)
    assert status == "needs_input" and roles == ["planner"] and decision[0] == "question"


def test_plans_see_other_work_in_the_workspace(factory):
    other = new_outcome(factory, title="Translate the greeting")
    finished = new_outcome(factory, title="Old greeting work")
    with session_scope() as s:
        s.get(Outcome, other).status = "running"
        s.get(Outcome, finished).status = "cancelled"
        p = Plan(outcome_id=other, version=1, status="approved")
        s.add(p)
        s.flush()
        s.add(Task(workspace_id=factory["ws"], outcome_id=other, plan_id=p.id, repo_id=factory["repo"], key="T1",
                   title="Add French greeting", status="running"))
    ScriptedModel.scripts["plan_reviewer"] = approve
    oid = new_outcome(factory)
    plan(oid)
    for prompt in (PLANNER_PROMPTS[0], REVIEW_PROMPTS[0]):
        assert "- #1 Translate the greeting (running)" in prompt
        assert "  - T1 Add French greeting [acme/demo] (running)" in prompt
        assert "Old greeting work" not in prompt


def test_review_reports_are_validated():
    ok = {"verdict": "approve", "summary": "s", "findings": [{"severity": "minor", "description": "d"}]}
    assert validate_report("plan_reviewer", ok)[0] is not None
    assert "cannot include blocker or major" in validate_report(
        "plan_reviewer", {**ok, "findings": [{"severity": "major", "description": "d"}]})[1]
    assert "needs at least one blocker or major" in validate_report("plan_reviewer", {**ok, "verdict": "revise"})[1]
    assert "verdict must be" in validate_report("plan_reviewer", {**ok, "verdict": "pass"})[1]


def test_the_plan_reviewer_model_defaults_to_opus_on_mindshub():
    assert workspace_models({"models": {"default": "gpt-terra"}}).plan_reviewer == PLAN_REVIEWER_MODEL == "opus-5-5"
    assert workspace_models({"models": {"default": "a", "verifier": "b"}, "provider": "openai"}).plan_reviewer == "b"
    assert workspace_models({"models": {"plan_reviewer": "c"}, "provider": "openai"}).plan_reviewer == "c"


def test_revisions_stop_at_the_outcome_budget(factory, monkeypatch):
    spend = iter([0.0, 0.0, 45.0])  # before planning, before the first review, then before the revision
    monkeypatch.setattr(planning, "outcome_cost", lambda _oid: (next(spend), False))
    ScriptedModel.scripts["plan_reviewer"] = revise
    oid = new_outcome(factory)
    status, plans, tasks, decision, roles = plan(oid)
    assert status == "awaiting_approval" and tasks == 0
    assert roles == ["planner", "plan_reviewer"]  # no revision was started
    assert plans == [(1, "proposed", "revise")]
    assert "The outcome reached its $40 budget ($45.00 spent) before the planner could revise this plan." in decision[1]
    assert "- major (T1): Add a test for the new greeting." in decision[1]


def test_a_plan_is_not_reviewed_past_the_outcome_budget(factory, monkeypatch):
    spend = iter([0.0, 40.0])  # before planning, then before the review
    monkeypatch.setattr(planning, "outcome_cost", lambda _oid: (next(spend), True))
    oid = new_outcome(factory)
    status, plans, tasks, decision, roles = plan(oid)
    assert status == "awaiting_approval" and tasks == 0 and roles == ["planner"]
    assert plans == [(1, "proposed", "error")]
    assert "the review could not finish" in decision[1] and "so the plan was not reviewed" in decision[1]


def test_a_review_that_cannot_start_keeps_the_plan(factory, monkeypatch):
    from aifactory.inference import ProviderConfig
    from aifactory.pipeline import runtime

    calls = []

    def provider(_workspace_id):
        calls.append(1)
        if len(calls) > 1:  # the planner gets a provider; the reviewer's setup fails
            raise RuntimeError("No model provider key")
        return ProviderConfig("openai", "http://model.test", "k", "Test")

    monkeypatch.setattr(runtime, "resolve_provider", provider)
    oid = new_outcome(factory)
    status, plans, tasks, decision, roles = plan(oid)
    assert status == "awaiting_approval" and tasks == 0 and roles == ["planner"]
    assert plans == [(1, "proposed", "error")]
    assert decision[0] == "approve_plan"
    assert "The plan review could not start: No model provider key" in decision[1]


def test_an_outcome_over_budget_is_not_planned_until_a_person_continues(factory, monkeypatch):
    from aifactory.decisions import resolve
    from aifactory.models import User

    monkeypatch.setattr(planning, "outcome_cost", lambda _oid: (50.0, True))
    oid = new_outcome(factory)
    status, plans, tasks, decision, roles = plan(oid)
    assert status == "blocked" and plans == [] and roles == []
    kind, body = decision
    assert kind == "budget" and "The outcome reached its $40 budget ($50.00 spent)" in body
    with session_scope() as s:
        budget = s.query(Decision).filter_by(outcome_id=oid, kind="budget", status="open").one()
        resolve(s, budget, s.get(User, factory["user"]), "retry")
    assert planning._continued_past_budget(oid)
    status, plans, tasks, decision, roles = plan(oid)  # one full planning run, reviewed
    assert status == "running" and tasks == 1 and roles == ["planner", "plan_reviewer"]
    with session_scope() as s:
        s.get(Outcome, oid).status = "planning"  # e.g. a later revision request
    status, *_, roles = plan(oid)
    assert status == "blocked" and roles == ["planner", "plan_reviewer"]  # the grant covered one run only
