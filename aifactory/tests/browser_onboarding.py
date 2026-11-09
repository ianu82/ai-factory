"""Dependency-free frontend regression check using the existing Python Playwright.

Build the web app, serve it with Vite preview, then run:
    .venv/bin/python tests/browser_onboarding.py --base-url http://127.0.0.1:4173

API fixtures deliberately exercise browser-only behavior without credentials.
Server persistence and authorization are covered by test_onboarding.py.
"""
from __future__ import annotations

import argparse
from copy import deepcopy

from playwright.sync_api import expect, sync_playwright


def check(base_url: str) -> None:
    initial = {
        "current_step": "models", "completed_steps": ["team"], "deferred": False,
        "first_outcome_id": None, "ready": False,
        "missing_steps": ["models", "github", "repositories"], "validation_uncertain_steps": [],
    }
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True, args=["--no-sandbox"])
        for role in ("owner", "admin", "member", "viewer"):
            for storage_failure in ("normal", "quota", "unavailable"):
                context = browser.new_context()
                context.add_init_script(
                    """const failure = %r;
                    if (failure === 'quota') {
                      Storage.prototype.setItem = () => { throw new DOMException('Full', 'QuotaExceededError'); };
                    } else if (failure === 'unavailable') {
                      Object.defineProperty(window, 'localStorage', { get() { throw new Error('Storage disabled'); } });
                    }
                    """ % storage_failure
                )
                states = {"one": deepcopy(initial), "two": deepcopy(initial)}
                user = {"id": "alice", "name": "Alice", "email": "alice@example.com"}
                signed_in = True
                mutations = []
                requests = []

                def route_api(route):
                    nonlocal signed_in
                    request = route.request
                    path = request.url.split(base_url)[-1].split("?")[0]
                    requests.append((request.method, path))
                    if path == "/auth/logout":
                        signed_in = False
                        return route.fulfill(json={})
                    if path == "/api/me":
                        if not signed_in:
                            return route.fulfill(status=401, json={"detail": "Sign in"})
                        return route.fulfill(json={"user": user, "organizations": [], "workspaces": [
                            {"id": wid, "name": f"Workspace {wid}", "org_name": "Acme", "role": role,
                             "paused": False, "settings": {
                                 "autonomy": {"plan": "approval", "merge": "approval", "release": "approval"},
                                 "budgets": {"run_usd": 10, "outcome_usd": 20}, "limits": {},
                                 "concurrency": 2, "max_attempts": 3,
                             }} for wid in states
                        ]})
                    if path == "/api/meta":
                        return route.fulfill(json={"auth": {"dev": True}, "harnesses": [], "sandbox": {}, "version": "test"})
                    if path.endswith("/onboarding"):
                        wid = path.split("/")[3]
                        if request.method == "PATCH":
                            assert role in ("owner", "admin"), "Read-only roles must never mutate onboarding"
                            action = request.post_data_json
                            mutations.append(action)
                            if action["action"] == "navigate":
                                states[wid]["current_step"] = action["step"]
                            elif action["action"] == "defer":
                                states[wid]["deferred"] = action["deferred"]
                        return route.fulfill(json=states[wid])
                    if path.endswith("/inbox"):
                        return route.fulfill(json={"decisions": [], "since": None, "changes": []})
                    if path.endswith("/pulls"):
                        return route.fulfill(json={"counts": {"ready": 0, "attention": 0}, "items": []})
                    if path.endswith("/outcomes/deep-link"):
                        return route.fulfill(status=404, json={"detail": "Not found"})
                    if path.endswith("/events"):
                        return route.fulfill(content_type="text/event-stream", body="")
                    return route.fulfill(json=[])

                context.route("**/api/**", route_api)
                context.route("**/auth/logout", route_api)
                page = context.new_page()
                errors = []
                page.on("pageerror", lambda error: errors.append(str(error)))
                pending = []
                page.route("**/api/workspaces/one/onboarding", lambda route: pending.append(route), times=1)
                page.goto(base_url + "/")
                page.wait_for_function("document.body.innerText.includes('Loading')")
                expect(page.get_by_role("heading", name="Inbox", exact=True)).to_have_count(0)
                page.wait_for_timeout(100)
                assert pending, "Expected readiness request"
                pending[0].fulfill(json=states["one"])
                expect(page).to_have_url(base_url + "/w/one/onboarding")
                expect(page.locator(".sidebar, .mobile-bar")).to_have_count(0)
                expect(page.locator(".onboarding-nav li")).to_have_count(6)
                page.keyboard.press("c")
                expect(page.get_by_role("dialog")).to_have_count(0)
                expect(page.locator(".onboarding-frame")).to_be_visible()
                if role in ("owner", "admin"):
                    for width in (375, 1440):
                        page.set_viewport_size({"width": width, "height": 900})
                        bounds = page.locator(".onboarding-actions").evaluate("el => el.getBoundingClientRect().top + scrollY")
                        page.get_by_role("button", name="Continue", exact=True).focus()
                        page.keyboard.press("Enter")
                        expect(page.get_by_role("heading", name="GitHub", exact=True)).to_be_focused()
                        assert page.evaluate("document.documentElement.scrollWidth <= innerWidth")
                        assert page.locator(".onboarding-actions").evaluate("el => el.getBoundingClientRect().top + scrollY") == bounds
                        page.get_by_role("button", name="Back", exact=True).click()
                        expect(page.get_by_role("heading", name="AI models", exact=True)).to_be_focused()
                    page.get_by_role("button", name="5 Team").click()
                    expect(page.get_by_role("heading", name="Team", exact=True)).to_be_focused()
                    page.reload()
                    expect(page.get_by_role("heading", name="Team", exact=True)).to_be_visible()
                    assert states["one"]["completed_steps"] == ["team"]
                else:
                    for width in (375, 1440):
                        page.set_viewport_size({"width": width, "height": 900})
                        assert page.evaluate("document.documentElement.scrollWidth <= innerWidth")
                    expect(page.get_by_role("heading", name="An admin needs to finish workspace setup")).to_be_visible()
                    for label in ("AI models", "GitHub", "Repositories"):
                        expect(page.locator("main li").filter(has_text=label)).to_be_visible()
                    expect(page.get_by_role("button", name="Continue", exact=True)).to_have_count(0)
                    expect(page.locator(".onboarding-nav button:enabled")).to_have_count(0)

                # Regression: a failed setItem/storage getter must not trap members.
                page.set_viewport_size({"width": 1440, "height": 900})
                page.get_by_role("button", name="Leave for now", exact=True).click()
                expect(page).to_have_url(base_url + "/w/one/inbox")
                expect(page.get_by_role("heading", name="Inbox", exact=True)).to_be_visible()
                page.locator(".sidebar a[href='/w/one/onboarding']").click()
                expect(page.locator(".onboarding-frame")).to_be_visible()
                page.get_by_role("button", name="Leave for now", exact=True).click()
                expect(page.get_by_role("heading", name="Inbox", exact=True)).to_be_visible()
                if role in ("owner", "admin") or storage_failure == "normal":
                    page.reload()
                    expect(page.get_by_role("heading", name="Inbox", exact=True)).to_be_visible()
                page.goto(base_url + "/w/one/settings")
                expect(page.locator(".sidebar")).to_be_visible()
                expect(page).to_have_url(base_url + "/w/one/settings")
                page.goto(base_url + "/w/one/outcomes/deep-link")
                expect(page.locator(".sidebar")).to_be_visible()
                expect(page).to_have_url(base_url + "/w/one/outcomes/deep-link")
                page.goto(base_url + "/w/one/onboarding")
                page.get_by_label("Workspace", exact=True).select_option("two")
                expect(page).to_have_url(base_url + "/w/two/onboarding")
                assert states["two"]["current_step"] == "models"
                if role not in ("owner", "admin"):
                    assert not mutations
                assert not any("/test" in path or "/credentials" in path for _, path in requests)

                page.get_by_role("button", name="Sign out", exact=True).click()
                expect(page).to_have_url(base_url + "/signin")
                signed_in = True
                page.goto(base_url + "/")
                if role in ("owner", "admin") or storage_failure == "normal":
                    expect(page).to_have_url(base_url + "/w/one/inbox")
                else:
                    # A reload loses memory-only deferral, but never loses server progress.
                    expect(page).to_have_url(base_url + "/w/one/onboarding")
                # A different user's Inbox must not inherit Alice's personal deferral.
                user = {**user, "id": "bob"}
                states["one"]["deferred"] = False
                page.goto(base_url + "/")
                expect(page).to_have_url(base_url + "/w/one/onboarding")
                states["one"]["ready"] = True
                page.goto(base_url + "/")
                expect(page).to_have_url(base_url + "/w/one/inbox")
                assert not errors, errors
                context.close()
                print(f"PASS {role}, storage={storage_failure}: entry, leave, resume, isolation, deep links, ready entry")
        check_settings_readiness(browser, base_url, initial)
        browser.close()



def check_settings_readiness(browser, base_url: str, initial: dict) -> None:
    """Settings mutations must refresh the mounted gate without a page reload."""
    for action in ("credential-save", "credential-test", "credential-remove",
                   "repository-connect", "repository-save", "repository-remove"):
        context = browser.new_context()
        state = deepcopy(initial)
        state["ready"] = action.endswith("remove")
        credential = {"kind": "github_token", "label": "GitHub token", "configured": True,
                      "status": "valid", "hint": "••••1234", "detail": "Working"}
        repository = {"id": "repo", "full_name": "acme/app", "default_branch": "main", "config": {}}
        repositories = [] if action == "repository-connect" else [repository]
        reads = []
        mutations = []

        def route_api(route):
            request = route.request
            path = request.url.split(base_url)[-1].split("?")[0]
            if path == "/api/me":
                return route.fulfill(json={"user": {"id": "admin", "name": "Admin"}, "organizations": [],
                    "workspaces": [{"id": "one", "name": "Workspace", "org_name": "Acme", "role": "admin",
                                    "paused": False, "settings": {"autonomy": {"merge": "approval"}}}]})
            if path == "/api/meta":
                return route.fulfill(json={"auth": {"dev": True}, "harnesses": [], "sandbox": {}})
            if path.endswith("/onboarding"):
                assert request.method == "GET", "Settings must not acknowledge or defer onboarding"
                reads.append(deepcopy(state))
                return route.fulfill(json=state)
            if path.endswith("/repositories/preview"):
                return route.fulfill(json=repository)
            if "/credentials" in path or "/repositories" in path:
                if request.method != "GET":
                    mutations.append((request.method, path))
                    state["ready"] = not action.endswith("remove")
                    state["missing_steps"] = [] if state["ready"] else ["github", "repositories"]
                    if action == "repository-connect":
                        repositories.append(repository)
                    elif action == "repository-remove":
                        repositories.clear()
                    elif action == "credential-remove":
                        credential["configured"] = False
                    return route.fulfill(json=credential if "/credentials" in path else repository)
                return route.fulfill(json={"items": [credential], "server_fallbacks": {}}
                                     if "/credentials" in path else repositories)
            if path.endswith("/github/repos"):
                return route.fulfill(json={"items": [], "next_page": None})
            if path.endswith("/inbox"):
                return route.fulfill(json={"decisions": [], "since": None, "changes": []})
            if path.endswith("/pulls"):
                return route.fulfill(json={"counts": {}, "items": []})
            if path.endswith("/events"):
                return route.fulfill(content_type="text/event-stream", body="")
            return route.fulfill(json=[])

        context.route("**/api/**", route_api)
        page = context.new_page()
        page.on("dialog", lambda dialog: dialog.accept())
        section = "credentials" if action.startswith("credential") else "repositories"
        page.goto(base_url + "/w/one/settings/" + section)
        expect(page.get_by_role("heading", name=section.title(), exact=True)).to_be_visible()
        expect(page.locator(".sidebar")).to_be_visible()
        page.wait_for_timeout(100)
        assert reads, "Settings should mount the shared readiness query"
        with page.expect_response(lambda response: response.url.endswith("/onboarding")):
            if action == "credential-save":
                page.get_by_role("button", name="Replace", exact=True).click()
                page.get_by_label("GitHub token value").fill("test-secret-not-real")
                page.get_by_role("button", name="Save & test", exact=True).click()
            elif action == "credential-test":
                page.get_by_role("button", name="Test", exact=True).click()
            elif action == "credential-remove":
                page.get_by_role("button", name="Remove GitHub token", exact=True).click()
            elif action == "repository-connect":
                page.get_by_role("button", name="Add repository", exact=True).click()
                page.get_by_label("Repository name or search").fill("acme/app")
                page.get_by_role("button", name="Preview", exact=True).click()
                page.get_by_role("button", name="Confirm & connect", exact=True).click()
            else:
                page.locator(".repo-head").click()
                page.get_by_role("button", name="Save" if action == "repository-save" else "Disconnect", exact=True).click()
        # Navigation must use the refreshed server snapshot, not a reload or deferral.
        assert len(mutations) == 1, mutations
        assert len(reads) >= 2, f"{action} did not invalidate onboarding readiness"
        assert reads[-1]["ready"] == state["ready"]
        page.locator(".sidebar a[href='/w/one/inbox']").click()
        expect(page).to_have_url(base_url + ("/w/one/inbox" if state["ready"] else "/w/one/onboarding"))
        if state["ready"]:
            expect(page.get_by_role("heading", name="Inbox", exact=True)).to_be_visible()
        context.close()
        print(f"PASS Settings {action}: refreshed readiness before Inbox navigation")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-url", default="http://127.0.0.1:4173")
    check(parser.parse_args().base_url.rstrip("/"))
