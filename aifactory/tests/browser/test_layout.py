"""Outcome scrolling is contained in the main pane, not the document."""

from playwright.sync_api import expect


def test_outcome_scroll_containment(signed_in_page, scenario, browser_server, viewport, no_overflow):
    page = signed_in_page
    page.goto(f"/w/{scenario['workspace_id']}/outcomes/{scenario['outcomes']['running']}")
    conversation = page.locator('section[aria-label="Conversation"]')
    expect(conversation).to_be_visible()
    expect(conversation.locator(".othread-list")).to_be_visible()
    sidebar = page.locator('aside[aria-label="Main"]')
    desktop = viewport["width"] >= 1100
    if desktop:
        before = sidebar.bounding_box()
        assert before["y"] == 0

    # Add deterministic height without depending on the amount of seeded activity.
    conversation.locator(".othread-list").evaluate("""list => {
        const spacer = document.createElement('li');
        spacer.style.cssText = 'height: 4000px; flex-shrink: 0';
        list.append(spacer);
    }""")
    pane = page.locator(".othread")
    box = pane.bounding_box()
    page.mouse.move(box["x"] + 100, box["y"] + box["height"] - 100)
    page.mouse.wheel(0, 1200)
    page.wait_for_function("document.querySelector('.othread').scrollTop > 0")
    assert page.evaluate("document.scrollingElement.scrollTop") == 0

    if desktop:
        assert sidebar.bounding_box()["y"] == before["y"] == 0
        expect(sidebar.get_by_role("link", name="Settings")).to_be_in_viewport()
        # This checks actual sticky positioning, not just the CSS declaration.
        page.wait_for_function("Math.abs(document.querySelector('.othread-rail').getBoundingClientRect().top - 16) < 1")
        composer = pane.locator(".ocomposer")
        if composer.count():
            expect(composer).to_be_in_viewport(ratio=1)
        pane.evaluate("el => { el.scrollTop = el.scrollHeight; }")
        page.wait_for_function("""() => {
            const el = document.querySelector('.othread');
            return Math.abs(el.scrollHeight - el.clientHeight - el.scrollTop) <= 1;
        }""")
        if composer.count():
            expect(composer).to_be_in_viewport(ratio=1)
    else:
        expect(page.locator(".mobile-bar")).to_be_in_viewport(ratio=1)
        assert page.locator(".mobile-bar").bounding_box()["y"] == 0
    no_overflow(page)

    # The alternate detail view must use the same bounded scrolling behaviour.
    page.get_by_role("button", name="Full activity").click()
    full = page.locator(".othread-full")
    expect(full).to_be_visible()
    full.evaluate("""el => {
        const spacer = document.createElement('div');
        spacer.style.height = '4000px';
        el.append(spacer);
        el.scrollTop = 0;
    }""")
    box = full.bounding_box()
    page.mouse.move(box["x"] + 100, box["y"] + 100)
    page.mouse.wheel(0, 1200)
    page.wait_for_function("document.querySelector('.othread-full').scrollTop > 0")
    assert page.evaluate("document.scrollingElement.scrollTop") == 0
    if desktop:
        assert sidebar.bounding_box()["y"] == 0
        expect(sidebar.get_by_role("link", name="Settings")).to_be_in_viewport()
    else:
        assert page.locator(".mobile-bar").bounding_box()["y"] == 0
    no_overflow(page)


def test_tall_sidebar_scrolls_independently(signed_in_page, viewport, no_overflow):
    page = signed_in_page
    if viewport["width"] < 760:
        page.get_by_role("button", name="Open navigation", exact=True).click()
    sidebar = page.locator('aside[aria-label="Main"]')
    expect(sidebar).to_be_in_viewport()
    # Intersection alone can pass while the mobile drawer is still sliding in.
    # Wait for its final position before aiming the wheel event, or it can hit
    # the scrim rather than the sidebar on a slower browser/CI worker.
    page.wait_for_function("document.querySelector('.sidebar').getBoundingClientRect().left === 0")
    sidebar.evaluate("""el => {
        const spacer = document.createElement('div');
        spacer.style.cssText = 'height: 4000px; flex-shrink: 0';
        el.insertBefore(spacer, el.querySelector('.sidebar-foot'));
    }""")
    box = sidebar.bounding_box()
    page.mouse.move(box["x"] + 100, box["y"] + 200)
    page.mouse.wheel(0, 1200)
    page.wait_for_function("document.querySelector('.sidebar').scrollTop > 0")
    sidebar.get_by_role("link", name="Settings").scroll_into_view_if_needed()
    expect(sidebar.get_by_role("link", name="Settings")).to_be_in_viewport()
    assert sidebar.bounding_box()["y"] == 0
    assert page.evaluate("document.scrollingElement.scrollTop") == 0
    no_overflow(page)
