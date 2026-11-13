"""Flows 1–4 against the real SPA, independently seeded at both viewport sizes.

Outcomes search is desktop-only: main deliberately hides it on small screens.
The current Outcomes route opens a conversation; Full activity opens its detail.
"""
from __future__ import annotations

import re

import pytest
from playwright.sync_api import expect

from tests.browser.seed import STATUSES


@pytest.fixture
def workflow_page(unsigned_page, scenario, no_overflow, checkpoint):
    """Exercise real dev sign-in and diagnose the sign-in/landing states too."""
    page = unsigned_page
    page.goto("/signin")
    expect(page.get_by_role("heading", name="Sign in to Factory", exact=True)).to_be_visible()
    page.get_by_placeholder("Local development email").fill(scenario["email"])
    expect(page.get_by_role("button", name="Dev sign-in", exact=True)).to_be_visible()
    no_overflow(page)
    checkpoint("signin")
    page.get_by_role("button", name="Dev sign-in", exact=True).click()
    expect(page).to_have_url(re.compile(f"/w/{scenario['workspace_id']}/inbox$"))
    expect(page.get_by_role("heading", name="Inbox", exact=True)).to_be_visible()
    expect(page.get_by_role("region", name="Questions", exact=True)).to_be_visible()
    no_overflow(page)
    checkpoint("inbox-landing")
    return page


def inbox_row(page, group, title):
    # Compact rows put the kind badge and title in the same text container.
    return page.get_by_role("region", name=group, exact=True).get_by_role("listitem").filter(
        has_text=title)


def outcome_row(page, title):
    return page.get_by_role("listitem").filter(has=page.get_by_text(title, exact=True))


def test_inbox_inline_plan_approval(workflow_page, no_overflow, checkpoint):
    page = workflow_page
    groups = {
        "Merge approvals": ("approve_merge", "approve_release"),
        "Plans to approve": ("approve_plan",),
        "Questions": ("question",),
        "Stuck": ("unblock", "budget"),
    }
    for group, kinds in groups.items():
        expect(page.get_by_role("region", name=group, exact=True)).to_be_visible()
        for kind in kinds:
            expect(inbox_row(page, group, f"Browser {kind}")).to_be_visible()
    no_overflow(page)
    checkpoint("inbox-groups")

    plan = inbox_row(page, "Plans to approve", "Browser approve_plan")
    plan.get_by_role("button", name="Approve", exact=True).click()
    expect(plan, "Browser approve_plan is still visible in Inbox after its inline Approve action").to_have_count(0)
    # An inline approval must not open the detail pane or clear unrelated work.
    expect(page.get_by_role("complementary", name="Decision detail", exact=True)).to_have_count(0)
    for group, kinds in groups.items():
        for kind in kinds:
            if kind != "approve_plan":
                expect(inbox_row(page, group, f"Browser {kind}")).to_be_visible()
    no_overflow(page)
    checkpoint("inbox-plan-approved")


def test_answer_question_and_remember_precedent(workflow_page, navigate, no_overflow, checkpoint):
    page = workflow_page
    question = inbox_row(page, "Questions", "Browser question")
    question.get_by_role("button", name="Answer", exact=True).click()
    pane = page.get_by_role("complementary", name="Decision detail", exact=True)
    card = pane.get_by_role("article", name="Browser question", exact=True)
    expect(card).to_be_visible()
    no_overflow(page)
    checkpoint("question-detail")
    note = "Keep CSV exports in UTC and include the timezone in every timestamp header."
    card.get_by_role("textbox", name="Comment", exact=True).fill(note)
    card.get_by_role("checkbox", name="Remember for future work", exact=True).check()
    expect(card.get_by_role("checkbox", name="Remember for future work", exact=True)).to_be_checked()
    no_overflow(page)
    checkpoint("question-remember")
    card.get_by_role("button", name="Answer", exact=True).click()
    expect(question, "Browser question must leave Inbox after answering in Decision detail").to_have_count(0)
    expect(pane).to_have_count(0)
    expect(inbox_row(page, "Plans to approve", "Browser approve_plan")).to_be_visible()
    no_overflow(page)
    checkpoint("question-answered")

    navigate(page, "Decisions")
    expect(page.get_by_role("heading", name="Decisions", exact=True)).to_be_visible()
    precedent = page.get_by_role("listitem").filter(has=page.get_by_text("Browser question", exact=True))
    expect(precedent.get_by_text("answer", exact=True)).to_be_visible()
    expect(precedent.get_by_text(f"“{note}”", exact=True)).to_be_visible()
    # Retire (rather than Make precedent) is the visible standing-precedent affordance.
    expect(precedent.get_by_role("button", name="Retire", exact=True)).to_be_visible()
    expect(precedent.get_by_role("button", name="Make precedent", exact=True)).to_have_count(0)
    expect(page.get_by_text("Team rules and precedents the Factory follows · 1", exact=True)).to_be_visible()
    no_overflow(page)
    checkpoint("decisions-precedent")


def test_capture_outcome_with_keyboard_shortcut(workflow_page, navigate, no_overflow, checkpoint):
    page = workflow_page
    # Focus a real non-input control; send the actual lowercase shortcut, not a capture-button click.
    page.get_by_role("button", name="Priority", exact=True).focus()
    page.keyboard.press("c")
    dialog = page.get_by_role("dialog", name="New outcome", exact=True)
    expect(dialog).to_be_visible()
    no_overflow(page)
    checkpoint("capture-open")
    title = "Export the audit trail as a timezone-aware CSV"
    description = "Let workspace admins export the audit trail for compliance reviews without losing timestamp precision."
    criteria = ("The CSV includes every audit entry in chronological order.",
                "Timestamp headers explicitly identify UTC.")
    dialog.get_by_role("textbox", name="Outcome title", exact=True).fill(title)
    dialog.get_by_role("textbox", name="Description", exact=True).fill(description)
    repository = dialog.get_by_role("button", name="acme/app", exact=True)
    # With one seeded repository the real form selects it automatically.
    expect(repository).to_have_attribute("aria-pressed", "true")
    dialog.get_by_role("button", name="Acceptance criteria, worker and priority", exact=True).click()
    dialog.get_by_role("textbox", name=re.compile("^Acceptance criteria")).fill("\n".join(criteria))
    no_overflow(page)
    checkpoint("capture-filled")
    dialog.get_by_role("button", name="Start", exact=True).click()
    expect(dialog).to_have_count(0)
    expect(page.get_by_role("heading", name=title, exact=True)).to_be_visible()
    expect(page.get_by_role("navigation", name="Breadcrumb").get_by_text("#10", exact=True)).to_be_visible()
    no_overflow(page)
    checkpoint("captured-conversation")

    navigate(page, "Outcomes")
    expect(page.get_by_role("heading", name="Outcomes", exact=True)).to_be_visible()
    created = outcome_row(page, title)
    expect(created).to_be_visible()
    expect(created.get_by_text("#10", exact=True)).to_be_visible()
    no_overflow(page)
    checkpoint("captured-outcomes-row")
    created.click()
    expect(page.get_by_role("heading", name=title, exact=True)).to_be_visible()
    no_overflow(page)
    checkpoint("captured-reopened")
    page.get_by_role("button", name="Full activity").click()
    expect(page.get_by_role("heading", name=title, exact=True, level=2)).to_be_visible()
    expect(page.get_by_role("button", name="Close panel", exact=True)).to_be_visible()
    expect(page.get_by_text(description, exact=True)).to_be_visible()
    for criterion in criteria:
        expect(page.get_by_role("listitem").filter(has_text=criterion)).to_be_visible()
    no_overflow(page)
    checkpoint("captured-outcome-detail")


def test_outcomes_filters_search_and_evidence(workflow_page, scenario, navigate, no_overflow, checkpoint):
    page = workflow_page
    navigate(page, "Outcomes")
    expect(page.get_by_role("heading", name="Outcomes", exact=True)).to_be_visible()
    tabs = page.get_by_role("tablist", name="Filter outcomes", exact=True)
    memberships = {
        "Active": set(STATUSES[:6]),
        # Release approval belongs to completed, proving Needs you is not a status alias.
        "Needs you": {"awaiting_approval", "needs_input", "running", "attention", "blocked", "completed"},
        "Delivered": {"completed", "partially_completed"},
        "All": set(STATUSES),
    }
    for label, included in memberships.items():
        tab = tabs.get_by_role("tab", name=re.compile(f"^{label} "))
        tab.click()
        expect(tab).to_have_attribute("aria-selected", "true")
        for status in STATUSES:
            row = outcome_row(page, f"Scenario {status}")
            if status in included:
                expect(row).to_be_visible()
            else:
                expect(row).to_have_count(0)
        no_overflow(page)
        checkpoint(f"outcomes-{label.lower().replace(' ', '-')}")

    search = page.get_by_role("textbox", name="Search outcomes", exact=True)
    if page.viewport_size["width"] >= 600:
        # Desktop-only: the product's hide-sm search is intentionally absent on mobile.
        for query, found in (("Scenario partially_completed", "partially_completed"), ("7", "completed")):
            search.fill(query)
            expect(outcome_row(page, f"Scenario {found}")).to_be_visible()
            expect(page.get_by_role("listitem")).to_have_count(1)
            for status in set(STATUSES) - {found}:
                expect(outcome_row(page, f"Scenario {status}")).to_have_count(0)
            no_overflow(page)
            checkpoint(f"outcomes-search-{found}")
        search.fill("")
        expect(page.get_by_role("listitem")).to_have_count(len(STATUSES))
        no_overflow(page)
    else:
        expect(search).to_be_hidden()

    outcome_row(page, "Scenario completed").click()
    expect(page.get_by_role("heading", name="Scenario completed", exact=True)).to_be_visible()
    no_overflow(page)
    checkpoint("evidence-conversation")
    page.get_by_role("button", name="Full activity").click()
    expect(page.get_by_role("heading", name="Scenario completed", exact=True, level=2)).to_be_visible()
    task = page.get_by_test_id(f"task-{scenario['tasks']['completed']}")
    toggle = task.get_by_role("button", name="T1 Implement the seeded change", exact=True)
    # Single-task details start expanded on main; collapse then exercise expansion explicitly.
    expect(toggle).to_have_attribute("aria-expanded", "true")
    no_overflow(page)
    checkpoint("evidence-detail")
    toggle.click()
    expect(toggle).to_have_attribute("aria-expanded", "false")
    expect(task.get_by_text("pytest: 3 passed", exact=True)).to_have_count(0)
    no_overflow(page)
    checkpoint("evidence-task-collapsed")
    toggle.click()
    expect(toggle).to_have_attribute("aria-expanded", "true")
    criterion = task.get_by_role("listitem").filter(has_text="Visible evidence is available")
    expect(criterion).to_be_visible()
    expect(criterion).to_contain_text("pytest: 3 passed")
    expect(task.get_by_text("Passed checks and independent verification", exact=True)).to_be_visible()
    no_overflow(page)
    checkpoint("evidence-task-expanded")
    task.get_by_role("button", name="QA report", exact=True).click()
    expect(task.get_by_text("pytest: 3 passed; all criteria verified", exact=True)).to_be_visible()
    no_overflow(page)
    checkpoint("evidence-artifact-content")
