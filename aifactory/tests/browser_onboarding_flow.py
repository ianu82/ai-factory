"""Opt-in six-step journey, mocked upstreams, no credentials or extra dependencies.

Build/serve web with Vite preview, then:
    .venv/bin/python tests/browser_onboarding_flow.py --base-url http://127.0.0.1:4173
Also runs the entry/member/leave-resume suite and detailed connection/repo suites.
"""
from __future__ import annotations

import argparse
import re
from copy import deepcopy
from urllib.parse import urlparse

from playwright.sync_api import expect, sync_playwright


def check(base_url):
    with sync_playwright() as p:
        browser = p.chromium.launch(args=["--no-sandbox"])
        for width in (375, 1440):
            context = browser.new_context(viewport={"width": width, "height": 900})
            state = {"current_step": "models", "completed_steps": [], "deferred": False,
                     "first_outcome_id": None, "ready": False, "missing_steps": ["repositories"],
                     "validation_uncertain_steps": []}
            settings = {"provider": "mindshub", "models": {"default": "model"},
                        "harness": {"builder": "native"}, "limits": {}, "concurrency": 2,
                        "budgets": {"outcome_usd": 20, "run_usd": 7, "run_tokens": 12345},
                        "autonomy": {"plan": "approval", "merge": "auto", "release": "auto"}}
            repo = {"id": "repo", "full_name": "acme/widget", "default_branch": "main",
                    "config": {"detected": ["Python"], "setup": ["pip install -e ."], "checks": ["pytest"],
                               "qa": {"start": "python app.py"}, "release": {"mode": "on_merge"}}}
            outcomes = []
            requests = []
            by_request = {}
            fail_progress = True
            roles = {"alice": "admin", "bob": "member", "owner": "owner"}
            credentials = [{"kind": "mindshub_api_key", "configured": True, "hint": "mdb_••••last", "status": "valid",
                            "metadata": {"models": ["model"], "test_status": "valid", "diagnostic_code": "models_available"}},
                           {"kind": "github_token", "configured": True, "hint": "gh_••••last", "status": "valid",
                            "metadata": {"login": "octocat", "repository": "acme/widget", "test_status": "valid", "permissions": []}}]

            def route_api(route):
                nonlocal fail_progress
                req = route.request
                path = urlparse(req.url).path
                if path == "/api/me":
                    return route.fulfill(json={"user": {"id": "alice", "name": "Alice"}, "organizations": [], "workspaces": [
                        {"id": "one", "name": "Workspace", "org_name": "Acme", "role": "admin", "settings": deepcopy(settings)}]})
                if path == "/api/meta":
                    return route.fulfill(json={"auth": {"dev": True}, "harnesses": [], "sandbox": {}})
                if path.endswith("/onboarding"):
                    if req.method == "PATCH":
                        action = req.post_data_json
                        if action["action"] == "navigate":
                            state["current_step"] = action["step"]
                        elif action["action"] == "defer":
                            state["deferred"] = action["deferred"]
                        else:
                            if action["step"] == "outcome":
                                if fail_progress:
                                    fail_progress = False
                                    return route.fulfill(status=503, json={"detail": "Progress unavailable"})
                                state["first_outcome_id"] = action["first_outcome_id"]
                            if action["step"] not in state["completed_steps"]:
                                state["completed_steps"].append(action["step"])
                    return route.fulfill(json=state)
                if path.endswith("/credentials"):
                    return route.fulfill(json={"items": credentials, "server_fallbacks": {}, "github_permissions": []})
                if "/credentials/" in path:
                    return route.fulfill(json=credentials[1 if "github_token" in path else 0])
                if path.endswith("/models"):
                    return route.fulfill(json={"provider": "mindshub", "models": ["model"]})
                if path == "/api/workspaces/one":
                    settings.update(req.post_data_json["settings"])
                    return route.fulfill(json={"id": "one", "settings": settings})
                if path.endswith("/repositories"):
                    return route.fulfill(json=[repo])
                if path.endswith("/github/repos"):
                    return route.fulfill(json={"items": [], "next_page": None})
                if path.endswith("/members"):
                    return route.fulfill(json=[{"user": {"id": uid, "name": uid.title(), "email": uid + "@acme.test"}, "role": role} for uid, role in roles.items()])
                if "/members/" in path:
                    uid = path.rsplit("/", 1)[1]
                    assert uid != "alice"
                    assert req.post_data_json["role"] != "owner"
                    roles[uid] = req.post_data_json["role"]
                    return route.fulfill(json={"ok": True})
                if path.endswith("/outcomes"):
                    if req.method == "POST":
                        body = req.post_data_json
                        requests.append(body)
                        assert body["request_id"]
                        if body["request_id"] not in by_request:
                            outcome = {"id": "created" if not outcomes else f"created-{len(outcomes)}", "number": len(outcomes) + 1,
                                       "title": body["title"], "brief": body["brief"], "status": "planning", "repo_ids": ["repo"],
                                       "progress": {"done": 0, "total": 0}, "acceptance": [], "track": [], "prs": [],
                                       "priority": 2, "open_decisions": 0, "cost_usd": 0, "cost_known": True, "source": "user",
                                       "created_at": "2026-11-09T12:00:00", "updated_at": "2026-11-09T12:00:00"}
                            outcomes.append(outcome)
                            by_request[body["request_id"]] = outcome
                            # The server committed but the transport lost its response.
                            return route.abort("failed")
                        assert body["request_id"] == requests[-2]["request_id"]
                        return route.fulfill(json=by_request[body["request_id"]])
                    return route.fulfill(json=outcomes)
                if "/outcomes/created" in path:
                    outcome = next(o for o in outcomes if o["id"] == path.rsplit("/", 1)[1])
                    return route.fulfill(json={"outcome": outcome, "repositories": [repo], "plans": [], "tasks": [],
                                               "runs": [], "decisions": [], "messages": [], "events": [], "attachments": []})
                if path.endswith("/inbox"):
                    return route.fulfill(json={"decisions": [], "since": None, "changes": []})
                if path.endswith("/pulls"):
                    return route.fulfill(json={"counts": {}, "items": []})
                if path.endswith("/events"):
                    return route.fulfill(body="", content_type="text/event-stream")
                return route.fulfill(json=[])

            context.route("**/api/**", route_api)
            page = context.new_page()
            errors = []
            page.on("pageerror", lambda error: errors.append(str(error)))

            def keyboard(button):
                expect(button).to_be_enabled()
                button.focus()
                page.keyboard.press("Enter")

            def heading(name):
                expect(page.get_by_role("heading", name=name, exact=True)).to_be_visible()
                assert page.evaluate("document.documentElement.scrollWidth <= innerWidth")

            page.goto(base_url + "/w/one/onboarding")
            heading("AI models")
            keyboard(page.get_by_role("button", name="Save models and continue"))
            heading("GitHub")
            keyboard(page.get_by_role("button", name="Continue to repository selection"))
            heading("Repositories")
            keyboard(page.get_by_role("button", name="Continue with saved repositories"))
            heading("How autonomous")
            expect(page.get_by_role("radio", name="Custom/current", exact=False)).to_be_checked()
            expect(page.get_by_role("radio", name="Autonomous", exact=False)).not_to_be_checked()
            page.get_by_label("Budget per outcome (USD)").fill("0")
            expect(page.get_by_role("button", name="Save autonomy and continue")).to_be_disabled()
            page.get_by_label("Budget per outcome (USD)").fill("35")
            keyboard(page.get_by_role("button", name="Save autonomy and continue"))
            heading("Team")
            assert settings["autonomy"] == {"plan": "approval", "merge": "auto", "release": "auto"}
            keyboard(page.get_by_role("button", name="4 How autonomous", exact=False))
            heading("How autonomous")
            for name, plan, merge in [("Cautious", "approval", "approval"), ("Balanced", "auto", "approval"), ("Autonomous", "auto", "auto")]:
                page.get_by_role("radio", name=name, exact=False).focus()
                page.keyboard.press("Space")
                page.get_by_label("Budget per outcome (USD)").fill("42.5")
                keyboard(page.get_by_role("button", name="Save autonomy and continue"))
                heading("Team")
                assert settings["autonomy"] == {"plan": plan, "merge": merge, "release": "approval"}
                assert settings["budgets"] == {"outcome_usd": 42.5, "run_usd": 7, "run_tokens": 12345}
                if name != "Autonomous":
                    keyboard(page.get_by_role("button", name="4 How autonomous", exact=False))
                    heading("How autonomous")
            expect(page.get_by_text("own MindsHub API keys", exact=False)).to_be_visible()
            expect(page.get_by_role("combobox", name="Role for Alice", exact=True)).to_be_disabled()
            expect(page.get_by_role("combobox", name="Role for Owner", exact=True).locator('option[value="owner"]')).to_have_attribute("disabled", "")
            page.get_by_role("combobox", name="Role for Bob", exact=True).select_option("viewer")
            expect(page.get_by_role("combobox", name="Role for Bob", exact=True)).to_have_value("viewer")
            keyboard(page.get_by_role("button", name="Leave for now", exact=True))
            expect(page).to_have_url(base_url + "/w/one/inbox")
            page.reload()
            expect(page.get_by_role("heading", name="Inbox", exact=True)).to_be_visible()
            page.goto(base_url + "/w/one/onboarding")
            heading("Team")
            keyboard(page.get_by_role("button", name="Continue to first outcome"))
            heading("First outcome")
            expect(page.locator(".onboarding-examples button")).to_have_count(3)
            for button in page.locator(".onboarding-examples button").all():
                keyboard(button)
                expect(page.get_by_role("textbox", name="Description", exact=True)).to_have_value(re.compile(".*Python.*"))
                assert not requests
            repo["config"]["detected"] = []
            page.reload()
            heading("First outcome")
            keyboard(page.locator(".onboarding-examples button").first)
            expect(page.get_by_role("textbox", name="Description", exact=True)).to_have_value(re.compile(".*has not been detected.*"))
            page.get_by_role("textbox", name="Outcome title", exact=True).fill("Improve widget documentation")
            keyboard(page.get_by_role("button", name="Create first outcome", exact=True))
            expect(page.get_by_role("alert")).to_be_visible()
            keyboard(page.get_by_role("button", name="Create first outcome", exact=True))
            expect(page.get_by_role("alert")).to_have_text("Progress unavailable")
            assert len(requests) == 2 and len(outcomes) == 1
            expect(page.get_by_role("link", name="Open outcome", exact=True)).to_be_visible()
            # Reload discards client state; the server list still offers recovery.
            page.reload()
            heading("First outcome")
            expect(page.get_by_role("region", name="Recover an existing outcome")).to_be_visible()
            keyboard(page.get_by_role("button", name="Use this outcome", exact=True))
            expect(page).to_have_url(base_url + "/w/one/outcomes/created")
            expect(page.get_by_role("heading", name="Improve widget documentation", exact=True)).to_be_visible()
            assert state["first_outcome_id"] == "created" and len(requests) == 2
            assert page.evaluate("document.documentElement.scrollWidth <= innerWidth")
            # The main New outcome form shares the same lost-response protection.
            keyboard(page.get_by_role("button", name="New" if width == 375 else "New outcome C", exact=True))
            dialog = page.get_by_role("dialog")
            dialog.get_by_role("textbox", name="Outcome title", exact=True).fill("Second outcome from main form")
            keyboard(dialog.get_by_role("button", name="Start", exact=True))
            expect(dialog.get_by_role("button", name="Start", exact=True)).to_be_enabled()
            assert len(outcomes) == 2
            keyboard(dialog.get_by_role("button", name="Start", exact=True))
            expect(page).to_have_url(base_url + "/w/one/outcomes/created-1")
            expect(page.get_by_role("heading", name="Second outcome from main form", exact=True)).to_be_visible()
            assert len(requests) == 4 and len(outcomes) == 2
            assert requests[2]["request_id"] == requests[3]["request_id"] != requests[0]["request_id"]
            assert not errors, errors
            context.close()
        browser.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-url", default="http://127.0.0.1:4173")
    base_url = parser.parse_args().base_url.rstrip("/")
    check(base_url)
    from browser_onboarding import check as entry
    from browser_onboarding_connections import check as connections
    from browser_onboarding_repositories import check as repositories
    entry(base_url)
    connections(base_url)
    repositories(base_url)
    print("All onboarding browser regressions passed (375/1440px, keyboard, read-only, retry and recovery)")
