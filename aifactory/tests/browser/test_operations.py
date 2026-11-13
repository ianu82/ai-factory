"""Flows 5–8 through the built SPA, real backend and native EventSource."""
from __future__ import annotations

import re
import uuid

from playwright.sync_api import expect


def test_settings_and_signals(signed_in_page, navigate, no_overflow, checkpoint):
    page = signed_in_page

    def checked(label):
        no_overflow(page)
        checkpoint(label)

    navigate(page, "Settings")
    expect(page.get_by_role("heading", name="Settings", exact=True)).to_be_visible()
    # Scope repeated radio labels to the named policy, not their DOM order.
    merges = page.locator(".s-line").filter(has=page.get_by_text("Merges", exact=True))
    releases = page.locator(".s-line").filter(has=page.get_by_text("Releases", exact=True))
    expect(merges.get_by_role("radio", name="Approve first", exact=True)).to_be_checked()
    merges.get_by_role("radio", name="Automatic", exact=True).click()
    expect(merges.get_by_role("radio", name="Automatic", exact=True)).to_be_checked()
    expect(releases.get_by_role("radio", name="Approve first", exact=True)).to_be_checked()
    checked("settings-policy")
    sections = page.get_by_role("navigation", name="Settings sections")
    sections.get_by_role("link", name="Repositories", exact=True).click()
    expect(page.get_by_role("heading", name="Repositories", exact=True)).to_be_visible()
    checked("repositories-collapsed")
    page.get_by_text("acme/app", exact=True).click()
    notes = page.get_by_role("textbox", name="Team notes for agents", exact=True)
    expect(notes).to_have_value("Use pytest.")
    saved_context = "Use pytest.\nKeep repository changes focused."
    notes.fill(saved_context)
    with page.expect_response(lambda r: r.request.method == "PATCH" and "/repositories/" in r.url) as saved:
        page.get_by_role("button", name="Save", exact=True).click()
    assert saved.value.status == 200
    checked("repository-saved")
    sections.get_by_role("link", name="Autonomy & limits", exact=True).click()
    page.reload()  # Persistence must survive a fresh document, not just cached state.
    expect(merges.get_by_role("radio", name="Automatic", exact=True)).to_be_checked()
    expect(releases.get_by_role("radio", name="Approve first", exact=True)).to_be_checked()
    checked("policy-persisted")
    sections.get_by_role("link", name="Repositories", exact=True).click()
    page.get_by_text("acme/app", exact=True).click()
    expect(notes).to_have_value(saved_context)
    checked("repository-reopened")

    sections.get_by_role("link", name="Workspace", exact=True).click()
    feedback = page.locator("section").filter(has=page.get_by_role("heading", name="Product feedback", exact=True))
    expect(feedback.get_by_role("button", name="Replace key", exact=True)).to_be_visible()
    expect(feedback.locator(".masked")).to_have_text(re.compile(r".+"))
    checked("feedback-key-configured")

    def replace_key(dialog):
        assert dialog.type == "confirm"
        assert dialog.message.startswith("Replace the feedback key?")
        dialog.accept()

    page.once("dialog", replace_key)
    feedback.get_by_role("button", name="Replace key", exact=True).click()
    new_key = page.get_by_label("New feedback key", exact=True)
    # Do not use expect(locator) while the key might be displayed: a failing
    # Playwright assertion prints an aria snapshot containing credential values.
    page.wait_for_function("document.querySelector('[aria-label=\"New feedback key\"]')?.value.length > 0")
    # Only booleans cross the browser boundary: neither pytest's assertion output
    # nor Playwright's call log can contain the one-time key (even on failure).
    assert new_key.evaluate("el => el.readOnly && el.value.length > 0"), "A nonempty read-only key must be shown once"
    assert feedback.evaluate("el => el.innerText.includes('Copy it now. It is stored only as a hash and cannot be shown again.')"), "Copy-once explanation must be visible"
    page.evaluate("""() => {
        const value = document.querySelector('[aria-label="New feedback key"]').value;
        window.__feedbackHidden = () => !document.body.textContent.includes(value)
            && ![...document.querySelectorAll('input, textarea')].some(el => el.value.includes(value));
    }""")
    checked("feedback-key-once-masked")  # diagnostics mask the entire intake section
    sections.get_by_role("link", name="Repositories", exact=True).click()
    page.wait_for_function("!document.querySelector('[aria-label=\"New feedback key\"]')")
    assert page.evaluate("window.__feedbackHidden()"), "The full key must disappear when leaving the section"
    checked("feedback-key-left")
    sections.get_by_role("link", name="Workspace", exact=True).click()
    page.wait_for_function("window.__feedbackHidden()")
    expect(feedback.get_by_role("button", name="Replace key", exact=True)).to_be_visible()
    expect(feedback.locator(".masked")).to_have_text(re.compile(r".+"))
    # Keep this boolean-only even if a regression leaves the key input present:
    # locator assertions would include its value in the failure aria snapshot.
    page.wait_for_function("!document.querySelector('[aria-label=\"New feedback key\"]')")
    assert feedback.evaluate("el => !el.innerText.includes('Copy it now.')"), "Copy-once UI must disappear on reopen"
    assert page.evaluate("window.__feedbackHidden()"), "The full key must not survive in displayed text or inputs"
    page.evaluate("delete window.__feedbackHidden")
    checked("feedback-key-reopened")

    navigate(page, "Signals")
    expect(page.get_by_role("heading", name="Signals", exact=True)).to_be_visible()
    proposal = page.get_by_role("article", name="Teach builders to test empty states", exact=True)
    expect(proposal).to_be_visible()
    checked("signals-proposal")
    page.get_by_role("button", name="Log feedback", exact=True).click()
    dialog = page.get_by_role("dialog", name="Log product feedback", exact=True)
    expect(dialog).to_be_visible()
    dialog.get_by_label("What did someone report?", exact=True).fill("Browser export loses empty rows")
    dialog.get_by_placeholder("Who saw it, where, how often, what they expected").fill("An empty CSV row should be preserved.")
    checked("feedback-dialog-filled")
    dialog.get_by_role("button", name="Log feedback", exact=True).click()
    expect(dialog).to_have_count(0)
    row = page.get_by_role("listitem").filter(has_text="Browser export loses empty rows")
    expect(row).to_be_visible()
    expect(row).to_contain_text("Product feedback")
    expect(row).to_contain_text("acme/app")
    checked("feedback-logged")

    edited = "Every schema change ships with a migration and a test for empty states."
    proposal.get_by_role("textbox").fill(edited)
    checked("proposal-edited")
    proposal.get_by_role("button", name="Add to team notes", exact=True).click()
    expect(proposal).to_have_count(0)
    handled = page.get_by_role("listitem").filter(has_text="Teach builders to test empty states")
    expect(page.get_by_text("Handled", exact=True)).to_be_visible()
    expect(handled.get_by_text("applied", exact=True)).to_be_visible()
    checked("proposal-applied")
    navigate(page, "Settings")
    sections.get_by_role("link", name="Repositories", exact=True).click()
    # A fresh document avoids initializing RepoCard's local form from stale
    # React Query data while the background repository refetch is in flight.
    page.reload()
    page.get_by_text("acme/app", exact=True).click()
    # Mirror test_insights: application appends the EDITED note, retaining context.
    expect(notes).to_have_value(saved_context + "\n\n" + edited)
    checked("edited-note-persisted")
    navigate(page, "Runs")
    expect(page.get_by_role("heading", name="Runs", exact=True)).to_be_visible()
    expect(page.get_by_role("table").get_by_role("row")).to_have_count(9)
    expect(page.get_by_role("table").get_by_text("verifier", exact=True)).to_have_count(8)
    checked("runs-seeded")


def test_pull_request_groups_filters_and_factory_link(signed_in_page, scenario, navigate, no_overflow, checkpoint):
    page = signed_in_page

    def checked(label):
        no_overflow(page)
        checkpoint(label)

    navigate(page, "Pull requests")
    expect(page.get_by_role("heading", name="Pull requests", exact=True)).to_be_visible()
    for label in ("Ready to merge", "Needs attention", "In progress", "Draft"):
        expect(page.get_by_role("button", name=re.compile(r"^" + label + r"\s*\d+$"))).to_be_visible()
    expect(page.locator(".pr-error")).to_have_count(0)
    for title, reason in (("Failing CI", "Checks failed: test"), ("Conflicted", "Merge conflict"),
                          ("Reviewed", "Changes requested by lead"),
                          ("Factory awaiting approval", "needs approval in AI Factory")):
        row = page.get_by_role("listitem").filter(has=page.get_by_text(title, exact=True))
        expect(row).to_be_visible()
        expect(row.locator(".pr-reason")).to_contain_text(reason)
    checked("pulls-open")
    attention = page.get_by_role("button", name=re.compile(r"^Needs attention\s*\d+$"))
    attention.click()
    expect(attention).to_have_attribute("aria-expanded", "false")
    expect(page.get_by_text("Failing CI", exact=True)).to_have_count(0)
    checked("pulls-attention-collapsed")
    attention.click()
    expect(page.get_by_text("Failing CI", exact=True)).to_be_visible()
    draft = page.get_by_role("button", name=re.compile(r"^Draft\s*\d+$"))
    expect(draft).to_have_attribute("aria-expanded", "false")
    draft.click()
    expect(page.get_by_text("Work in progress", exact=True)).to_be_visible()
    checked("pulls-draft-expanded")

    tabs = page.get_by_role("tablist", name="Filter pull requests")
    tabs.get_by_role("tab", name="Ready to merge", exact=True).click()
    expect(tabs.get_by_role("tab", name="Ready to merge", exact=True)).to_have_attribute("aria-selected", "true")
    expect(page.locator(".pr-row")).to_have_count(2)
    expect(page.get_by_text("Ready one", exact=True)).to_be_visible()
    expect(page.get_by_text("Factory awaiting approval", exact=True)).to_be_visible()
    expect(page.get_by_text("Failing CI", exact=True)).to_have_count(0)
    checked("pulls-ready-filter")
    tabs.get_by_role("tab", name="Merged", exact=True).click()
    expect(page.get_by_text("Shipped", exact=True)).to_be_visible()
    expect(page.locator(".pr-row")).to_have_count(1)
    expect(page.get_by_text("Closed unmerged", exact=True)).to_have_count(0)
    checked("pulls-merged-filter")
    tabs.get_by_role("tab", name="All", exact=True).click()
    expect(page.get_by_text("Shipped", exact=True)).to_be_visible()
    expect(page.get_by_text("Failing CI", exact=True)).to_be_visible()
    expect(page.get_by_text("Closed unmerged", exact=True)).to_have_count(0)
    checked("pulls-all-filter")
    tabs.get_by_role("tab", name="Open", exact=True).click()
    expect(page.get_by_text("Shipped", exact=True)).to_have_count(0)
    checked("pulls-open-restored")
    factory_row = page.get_by_role("listitem").filter(has=page.get_by_text("Factory awaiting approval", exact=True))
    link = factory_row.get_by_role("link", name="#4 T1", exact=True)
    expect(link).to_have_attribute("href", f"/w/{scenario['workspace_id']}/outcomes/{scenario['outcomes']['running']}")
    link.click()
    expect(page).to_have_url(re.compile(f"/w/{scenario['workspace_id']}/outcomes/{scenario['outcomes']['running']}$"))
    expect(page.get_by_role("heading", name="Scenario running", exact=True)).to_be_visible()
    details = page.get_by_role("complementary", name="Outcome details", exact=True)
    expect(details.get_by_role("heading", name="Tasks", exact=True)).to_be_visible()
    expect(details.get_by_role("listitem").filter(has_text="T1")).to_contain_text("Implement the seeded change")
    expect(details.get_by_role("listitem").filter(has_text="T1")).to_contain_text("PR #6")
    assert len(page.context.pages) == 1, "Factory link must stay inside the app, not open GitHub"
    checked("pulls-factory-outcome")
    page.get_by_role("button", name="Full activity", exact=False).click()
    expect(page.get_by_role("button", name="Close panel", exact=True)).to_be_visible()
    expect(page.locator(".task-head").filter(has_text="T1")).to_contain_text("Implement the seeded change")
    checked("pulls-factory-task-panel")


def test_live_signal_event_and_reconnect_resync(signed_in_page, scenario, navigate, control, no_overflow, checkpoint):
    page = signed_in_page
    navigate(page, "Signals")
    expect(page.get_by_text("Make empty states clearer", exact=True)).to_be_visible()
    first = control("wait_stream")
    # A per-document marker plus observed document requests rules out a reload,
    # including a same-URL reload that ordinary URL assertions would miss.
    sentinel = uuid.uuid4().hex
    page.evaluate("value => { window.__operationsDocument = value; }", sentinel)
    documents = []
    page.on("request", lambda request: documents.append(request.url) if request.resource_type == "document" else None)
    url = page.url

    def same_document():
        assert page.url == url
        assert page.evaluate("window.__operationsDocument") == sentinel, "Live recovery replaced the document"
        assert not documents, "Live recovery must not navigate or reload"

    def checked(label):
        same_document()
        no_overflow(page)
        checkpoint(label)

    checked("live-signals-subscribed")
    signal = control("create_signal", title="Delivered by the live signal event")
    expect(page.get_by_text("Delivered by the live signal event", exact=True)).to_have_count(0)
    control("notify_signal", signal_id=signal["signal_id"])
    # Signals has NO polling. This bound is also below the shortest production
    # polling interval (10s), so polling cannot rescue broken event invalidation.
    expect(page.get_by_text("Delivered by the live signal event", exact=True)).to_be_visible(timeout=5000)
    checked("live-signal-event")

    assert control("interrupt_stream", reject_reconnects=True)["interrupted"] == 1
    banner = page.get_by_text("Live updates disconnected. Reconnecting…", exact=True)
    try:
        # Native EventSource first retries the broken stream, receives a real 503,
        # closes, and production startLive enters its offline/backoff branch.
        expect(banner).to_be_visible(timeout=7000)
        state = control("stream_state")
        assert state["active"] == 0 and state["rejected"] >= 1
        checked("live-offline-retry")
        control("create_signal", title="Missed while the stream was disconnected")
        expect(page.get_by_text("Missed while the stream was disconnected", exact=True)).to_have_count(0)
        same_document()
    finally:
        control("restore_stream")
    control("wait_stream", connections=first["connections"] + 1)
    expect(banner).to_have_count(0)
    # No event was emitted for this mutation, even after reconnection: only the
    # real onopen resync can recover it. No refresh/focus/invalidation test hook.
    expect(page.get_by_text("Missed while the stream was disconnected", exact=True)).to_be_visible(timeout=5000)
    expect(page.get_by_text("Delivered by the live signal event", exact=True)).to_be_visible()
    checked("live-reconnected-resynced")
