"""Opt-in browser regressions against a built web app (no upstream services).

    .venv/bin/python tests/browser_learning_policy.py --base-url http://127.0.0.1:4173
"""
from __future__ import annotations

import argparse
from copy import deepcopy
from urllib.parse import urlparse

from playwright.sync_api import expect, sync_playwright


def check(base_url):
    with sync_playwright() as p:
        browser = p.chromium.launch(args=["--no-sandbox"])
        context = browser.new_context(viewport={"width": 1440, "height": 1000})
        settings = {"provider": "mindshub", "models": {}, "harness": {}, "limits": {},
                    "concurrency": 3, "planning_concurrency": 2, "max_attempts": 3,
                    "budgets": {"run_usd": 8, "outcome_usd": 40},
                    "autonomy": {"plan": "approval", "merge": "approval", "release": "approval"}}
        preset = {"plan": "auto", "merge": "auto", "release": "approval"}
        policy = {"learning": True, "eligible": False, "applied": False,
                  "before": deepcopy(settings["autonomy"]), "after": preset,
                  "first_completed_outcome_id": None,
                  "changes": [{"policy": key, "before": "approval", "after": "auto"} for key in ("plan", "merge")]}
        mode = "pending"
        held = []
        patches = []
        applies = []
        role = "admin"
        starts = []
        start_mode = "pending"

        def route_api(route):
            path = urlparse(route.request.url).path
            if path == "/api/me":
                return route.fulfill(json={"user": {"id": "u", "name": "Admin"}, "organizations": [],
                                           "workspaces": [{"id": "one", "name": "Team", "role": role,
                                                           "settings": settings}]})
            if path == "/api/meta":
                return route.fulfill(json={"harnesses": [], "sandbox": {}, "auth": {"dev": True}})
            if path.endswith("/onboarding/start"):
                assert route.request.method == "POST"
                starts.append(route)
                if start_mode == "pending":
                    return
                return route.fulfill(json={})
            if path.endswith("/onboarding"):
                return route.fulfill(json={"current_step": "autonomy", "completed_steps": [], "deferred": False,
                                           "first_outcome_id": None, "ready": True, "missing_steps": [],
                                           "validation_uncertain_steps": []})
            if path.endswith("/onboarding/policy"):
                if mode == "pending":
                    held.append(route)
                    return
                if mode == "error":
                    return route.fulfill(status=503, json={"detail": "Policy unavailable"})
                return route.fulfill(json=policy)
            if path.endswith("/policy/apply"):
                applies.append(route.request.method)
                policy.update(eligible=False, applied=True, learning=False)
                settings["autonomy"] = deepcopy(preset)
                return route.fulfill(json=policy)
            if path == "/api/workspaces/one":
                patch = route.request.post_data_json["settings"]
                patches.append(patch)
                preset.update(patch["autonomy"])
                return route.fulfill(json={"id": "one", "settings": settings})
            if path.endswith("/inbox"):
                return route.fulfill(json={"decisions": [], "since": None, "changes": []})
            if path.endswith("/pulls"):
                return route.fulfill(json={"counts": {}, "items": []})
            if path.endswith("/stream"):
                return route.fulfill(body="", content_type="text/event-stream")
            return route.fulfill(json=[])

        context.route("**/api/**", route_api)
        page = context.new_page()
        errors = []
        page.on("pageerror", lambda e: errors.append(str(e)))
        page.goto(base_url + "/w/one/settings/policies")
        expect(page.get_by_text("Loading saved policy…")).to_be_visible()
        radios = page.get_by_role("radio")
        assert radios.count() == 6
        for radio in radios.all():
            expect(radio).to_be_disabled()
        radios.first.focus()
        page.keyboard.press("Enter")
        assert patches == []
        mode = "error"
        for route in held:
            route.fulfill(status=503, json={"detail": "Policy unavailable"})
        expect(page.get_by_role("alert")).to_contain_text("Policy unavailable", timeout=20000)
        for radio in radios.all():
            expect(radio).to_be_disabled()
        mode = "success"
        page.get_by_role("button", name="Retry", exact=True).click()
        expect(radios.first).to_be_enabled()
        expect(radios.nth(0)).to_have_attribute("aria-checked", "true")
        expect(radios.nth(2)).to_have_attribute("aria-checked", "true")
        radios.nth(4).click()  # Releases -> Automatic, not an entire fallback policy.
        expect(radios.nth(4)).to_have_attribute("aria-checked", "true")
        assert patches == [{"autonomy": {"release": "auto"}}]
        assert preset["plan"] == preset["merge"] == "auto"
        policy["eligible"] = True
        policy["first_completed_outcome_id"] = "delivered"
        page.goto(base_url + "/w/one/inbox")
        offer = page.get_by_role("region", name="Post-learning preset")
        expect(offer).to_contain_text("plan: Approve first → Automatic")
        expect(offer).to_contain_text("merge: Approve first → Automatic")
        page.get_by_role("button", name="Apply saved preset").click()
        expect(offer).to_have_count(0)
        assert applies == ["POST"]
        role = "member"
        policy.update(eligible=True, learning=True, applied=False)
        page.reload()
        expect(page.get_by_text("Ask a workspace admin to apply this preset.")).to_be_visible()
        expect(page.get_by_role("button", name="Apply saved preset")).to_have_count(0)
        role = "admin"
        page.goto(base_url + "/w/one/onboarding")
        expect(page.get_by_text("Starting learning mode…")).to_be_visible()
        expect(page.get_by_role("heading", name="How autonomous", exact=True)).to_have_count(0)
        for button in page.locator(".onboarding-nav button").all():
            expect(button).to_be_disabled()
        assert len(starts) == 1
        starts[0].fulfill(status=503, json={"detail": "Start unavailable"})
        expect(page.get_by_role("alert")).to_contain_text("Could not start learning mode")
        start_mode = "success"
        page.get_by_role("button", name="Retry", exact=True).click()
        expect(page.get_by_role("heading", name="How autonomous", exact=True)).to_be_visible()
        assert len(starts) == 2
        assert errors == [], errors
        browser.close()
        print("PASS: disabled pending/error controls, Retry, partial policy save, exact Inbox offer, admin apply and member restriction")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-url", default="http://127.0.0.1:4173")
    check(parser.parse_args().base_url)
