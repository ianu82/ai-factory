"""What the planner and the plan reviewer are told about scope: the whole request, as the team's latest note sets it."""
from __future__ import annotations

from types import SimpleNamespace

from aifactory.prompts import PLAN_REVIEWER, PLANNER, plan_prompt, plan_review_prompt


def test_guidance_is_dated_by_order_and_the_later_note_wins():
    outcome = SimpleNamespace(number=57, title="Automations", brief="Schedules, triggers and actions", acceptance=[])
    repo = SimpleNamespace(full_name="team/app", default_branch="main", config={})
    guidance = ["Plan all six triggers.", "Scope cut: this outcome is phase 1 only."]  # oldest first
    plan = {"summary": "Phase 1", "tasks": [{"key": "T1", "title": "Cron", "repo": "team/app", "spec": "Parse cron",
                                             "acceptance": ["tests pass"], "depends_on": []}]}
    for text in (plan_prompt(outcome, [repo], [], guidance), plan_review_prompt(outcome, [repo], [], guidance, None,
                                                                                plan)):
        heading = text.index("## Guidance from the team on this outcome (oldest first")
        assert heading < text.index("Plan all six triggers.") < text.index("Scope cut: this outcome is phase 1 only.")
    assert "the later one wins" in PLANNER and "the later one wins" in PLAN_REVIEWER
    # Requested work is never parked in risks, and a large request gets more tasks or a proposed split.
    assert "never move it into risks" in PLANNER and "propose" in PLANNER and "small set" not in PLANNER
