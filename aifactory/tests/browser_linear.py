"""Exercise Linear UI against local fake APIs using the existing Playwright setup.

    .venv/bin/python tests/browser_linear.py --base-url http://127.0.0.1:4173
"""
from __future__ import annotations

import argparse
from copy import deepcopy
from urllib.parse import urlparse

from playwright.sync_api import expect, sync_playwright


STAGES = ("queued", "building", "verifying", "merging", "done", "blocked")


def check(base_url: str) -> None:
    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(headless=True, args=["--no-sandbox"])
        for width in (375, 1440):
            context = browser.new_context(viewport={"width": width, "height": 900})
            page = context.new_page()
            errors = []
            page.on("pageerror", lambda error: errors.append(str(error)))
            role = "owner"
            credential = {"kind": "linear_api_key", "label": "Linear", "configured": False,
                          "status": "missing", "hint": "", "detail": ""}
            config = {"enabled": False, "team_id": None, "intake_label": "factory-auto",
                      "repo_ids": [], "states": dict.fromkeys(STAGES)}
            writes = []
            reads = {"me": 0, "linear": 0}
            discovery_failure = False
            save_failure = False
            resolved = False
            repo = {"id": "repo", "full_name": "acme/app", "config": {}}
            outcome = {"id": "outcome", "number": 1, "title": "Linear work", "brief": "Request",
                       "acceptance": [], "repo_ids": ["repo"], "harness": None, "status": "running",
                       "priority": 1, "source": "linear_issue", "summary": "", "blocked_reason": "",
                       "created_at": "2026-01-01T00:00:00Z", "updated_at": "2026-01-01T00:00:00Z",
                       "completed_at": None, "progress": {"done": 0, "total": 0}, "cost_usd": 0,
                       "cost_known": True, "open_decisions": 1, "prs": [], "track": [],
                       "linear_issue": {"id": "issue", "identifier": "ENG-42", "url": "https://linear.app/acme/issue/ENG-42"}}
            plain = {**outcome, "id": "plain", "number": 2, "title": "App work", "source": "user", "linear_issue": None}

            def route_api(route):
                nonlocal config, discovery_failure
                request = route.request
                path = urlparse(request.url).path
                if path == "/api/me":
                    reads["me"] += 1
                    return route.fulfill(json={"user": {"id": "alice", "name": "Alice"}, "workspaces": [
                        {"id": "one", "name": "Workspace", "org_name": "Acme", "role": role,
                         "paused": False, "settings": {"linear": config}}]})
                if path == "/api/meta":
                    return route.fulfill(json={"auth": {"dev": True}, "harnesses": [], "sandbox": {}})
                if path.endswith("/onboarding"):
                    return route.fulfill(json={"ready": True, "deferred": True, "completed_steps": [], "missing_steps": []})
                if path.endswith("/stream"):
                    return route.fulfill(body=": connected\n\n", content_type="text/event-stream")
                if path.endswith("/credentials"):
                    return route.fulfill(json={"items": [credential], "server_fallbacks": {}})
                if "/credentials/linear_api_key" in path:
                    assert role in ("owner", "admin")
                    writes.append((request.method, path))
                    if request.method == "DELETE":
                        credential.update(configured=False, status="missing", hint="", detail="")
                        return route.fulfill(status=204)
                    if request.method == "PUT":
                        assert request.post_data_json == {"value": "lin_api_private_test_key"}
                        if save_failure:
                            return route.fulfill(status=503, json={"detail": "Temporary credential failure"})
                    credential.update(configured=True, status="valid", hint="••••_key", detail="Connected as Alice Linear")
                    return route.fulfill(json=credential)
                if path.endswith("/linear"):
                    if request.method == "PUT":
                        assert role in ("owner", "admin")
                        body = request.post_data_json
                        assert set(body) == {"enabled", "team_id", "intake_label", "repo_ids", "states"}
                        assert "lin_api" not in request.post_data
                        if body["enabled"]:
                            assert body["repo_ids"] == ["repo"]
                            assert body["states"] == {stage: f"{body['team_id']}-{stage}" for stage in STAGES}
                        writes.append(deepcopy(body))
                        config = body
                    reads["linear"] += 1
                    return route.fulfill(json=config)
                if path.endswith("/linear/teams") or path.endswith("/states"):
                    assert role in ("owner", "admin")
                    if discovery_failure:
                        return route.fulfill(status=503, json={"detail": "Linear discovery unavailable"})
                    if path.endswith("/teams"):
                        return route.fulfill(json={"items": [{"id": "alpha", "name": "Alpha"}, {"id": "beta", "name": "Beta"}]})
                    team = path.split("/")[-2]
                    return route.fulfill(json={"items": [{"id": f"{team}-{stage}", "name": f"Custom {stage}", "type": "unstarted"} for stage in STAGES]})
                if path.endswith("/repositories"):
                    return route.fulfill(json=[repo])
                if path.endswith("/outcomes"):
                    return route.fulfill(json=[outcome, plain])
                if path.endswith("/outcomes/outcome") or path.endswith("/outcomes/plain"):
                    decision = {"id": "question", "kind": "question", "title": "Choose a direction", "body": "Which approach?",
                                "status": "resolved" if resolved else "open", "created_at": "2026-01-01T00:00:00Z", "options": [{"value": "answer", "label": "Answer"}]}
                    return route.fulfill(json={"outcome": plain if path.endswith("/plain") else outcome, "repositories": [repo],
                                              "tasks": [], "runs": [], "decisions": [decision], "plans": [], "messages": [], "events": []})
                if path.endswith("/inbox"):
                    return route.fulfill(json={"decisions": [], "activity": []})
                if path.endswith("/pulls"):
                    return route.fulfill(json={"items": [], "counts": {"ready": 0, "attention": 0}})
                return route.fulfill(json={})

            context.route("**/api/**", route_api)
            context.route("https://linear.app/**", lambda route: route.fulfill(body="Fake Linear issue"))
            page.goto(f"{base_url}/w/one/settings/linear")
            expect(page.get_by_role("alert")).to_contain_text("Add a Linear API key")
            expect(page.get_by_role("combobox", name="Team", exact=True)).to_be_disabled()
            page.get_by_role("link", name="Credentials", exact=True).first.click()
            key = page.get_by_label("Linear value")
            expect(key).to_have_attribute("type", "password")
            expect(key).to_have_attribute("placeholder", "lin_api_…")
            key.fill("lin_api_private_test_key")
            page.get_by_role("button", name="Save & test").click()
            expect(page.locator(".cred")).to_contain_text("Connected as Alice Linear")
            expect(page.locator(".masked")).to_have_text("••••_key")
            expect(key).to_have_count(0)
            page.get_by_role("button", name="Test", exact=True).click()
            expect(page.get_by_role("button", name="Test", exact=True)).to_be_enabled()
            page.get_by_role("button", name="Replace", exact=True).click()
            expect(key).to_have_value("")
            save_failure = True
            key.fill("lin_api_private_test_key")
            page.get_by_role("button", name="Save & test").click()
            expect(key).to_have_value("")
            save_failure = False
            page.get_by_role("button", name="Cancel", exact=True).click()
            page.get_by_role("link", name="Linear", exact=True).click()
            team_select = page.get_by_role("combobox", name="Team", exact=True)
            team_select.select_option("alpha")
            for stage in STAGES:
                page.get_by_role("combobox", name=stage.capitalize(), exact=True).select_option(f"alpha-{stage}")
            team_select.select_option("beta")
            for stage in STAGES:
                expect(page.get_by_role("combobox", name=stage.capitalize(), exact=True)).to_have_value("")
            page.get_by_label("Enable Linear intake").check()
            page.get_by_label("acme/app", exact=True).check()
            save = page.get_by_role("button", name="Save Linear settings")
            expect(save).to_be_disabled()
            for stage in STAGES:
                page.get_by_role("combobox", name=stage.capitalize(), exact=True).select_option(f"beta-{stage}")
            page.get_by_label("Intake label").fill("factory-custom")
            prior_reads = deepcopy(reads)
            save.click()
            expect(save).to_be_enabled()
            assert reads["linear"] > prior_reads["linear"] and reads["me"] > prior_reads["me"]
            page.reload()
            expect(team_select).to_have_value("beta")
            expect(page.get_by_label("Intake label")).to_have_value("factory-custom")
            for stage in STAGES:
                expect(page.get_by_role("combobox", name=stage.capitalize(), exact=True)).to_have_value(f"beta-{stage}")
            expect(page.get_by_label("acme/app", exact=True)).to_be_checked()
            discovery_failure = True
            page.reload()
            expect(page.get_by_text("Could not discover teams:")).to_be_visible()
            expect(save).to_be_disabled()
            discovery_failure = False
            page.get_by_role("button", name="Retry teams").click()
            page.get_by_role("button", name="Retry states").click()
            expect(save).to_be_enabled()
            page.get_by_role("link", name="Credentials", exact=True).click()
            page.on("dialog", lambda dialog: dialog.accept())
            page.get_by_role("button", name="Remove Linear", exact=True).click()
            expect(page.get_by_label("Linear value")).to_have_value("")
            page.get_by_role("link", name="Linear", exact=True).click()
            expect(page.get_by_role("alert")).to_contain_text("Add a Linear API key")
            expect(save).to_be_disabled()
            page.get_by_label("Enable Linear intake").uncheck()
            save.click()
            expect(save).to_be_enabled()
            assert not config["enabled"]

            credential.update(configured=True, status="valid", hint="••••_key", detail="Connected as Alice Linear")
            for role in ("member", "viewer"):
                before = len(writes)
                page.reload()
                expect(page.get_by_text("Only workspace admins")).to_be_visible()
                expect(save).to_have_count(0)
                expect(team_select).to_be_disabled()
                page.goto(f"{base_url}/w/one/settings/credentials")
                expect(page.get_by_role("heading", name="Credentials", exact=True)).to_be_visible()
                expect(page.get_by_label("Linear value")).to_have_count(0)
                expect(page.get_by_role("button", name="Save & test")).to_have_count(0)
                expect(page.get_by_role("button", name="Remove Linear")).to_have_count(0)
                expect(page.get_by_role("button", name="Test", exact=True)).to_have_count(0)
                expect(page.get_by_role("button", name="Replace", exact=True)).to_have_count(0)
                assert len(writes) == before
                page.goto(f"{base_url}/w/one/settings/linear")

            page.goto(f"{base_url}/w/one/outcomes")
            link = page.get_by_role("link", name="Open ENG-42 in Linear")
            expect(link).to_have_attribute("rel", "noopener noreferrer")
            with page.expect_popup() as popup_info:
                link.click()
            popup_info.value.close()
            assert page.url.endswith("/outcomes")
            expect(page.get_by_label("Outcome detail")).to_have_count(0)
            link.focus()
            with page.expect_popup() as popup_info:
                page.keyboard.press("Enter")
            popup_info.value.close()
            assert page.url.endswith("/outcomes")
            for resolved in (False, True):
                page.goto(f"{base_url}/w/one/outcomes/outcome#decision-question")
                card = page.locator("#decision-question")
                expect(card).to_be_visible()
                expect(card).to_be_focused()
                expect(card).to_have_class("decide focused")
                expect(page.get_by_label("Outcome detail").get_by_role("link", name="Open ENG-42 in Linear")).to_be_visible()
            page.goto(f"{base_url}/w/one/outcomes/plain")
            expect(page.get_by_label("Outcome detail").get_by_role("heading", name="App work")).to_be_visible()
            expect(page.get_by_label("Outcome detail").get_by_role("link", name="Open ENG-42 in Linear")).to_have_count(0)
            assert not errors, errors
            context.close()
        browser.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-url", default="http://127.0.0.1:4173")
    check(parser.parse_args().base_url.rstrip("/"))
    print("Linear browser checks passed at 375px and 1440px")
