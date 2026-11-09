"""Run against Vite preview with existing Playwright; no GitHub credentials needed.

    .venv/bin/python tests/browser_onboarding_repositories.py --base-url http://127.0.0.1:4173
"""
from __future__ import annotations

import argparse
from copy import deepcopy
from urllib.parse import parse_qs, urlparse

from playwright.sync_api import expect, sync_playwright


def check(base_url: str) -> None:
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True, args=["--no-sandbox"])
        for width in (375, 1440):
            context = browser.new_context(viewport={"width": width, "height": 900})
            state = {"current_step": "repositories", "completed_steps": [], "deferred": False,
                     "first_outcome_id": None, "ready": False, "missing_steps": ["repositories"],
                     "validation_uncertain_steps": []}
            saved = []
            connections = []
            diagnostics = []
            fail_library = True
            refreshed = set()
            detected = {"setup": ["npm ci"], "checks": ["npm test"], "smoke": ["npm run build"],
                        "detected": ["Vite", "Node"], "context": "keep", "protected_paths": ["auth/**"],
                        "qa": {"start": "npm run dev -- --port $PORT", "pages": ["/login"]},
                        "release": {"health_url": "https://example.test"}}

            def route_api(route):
                nonlocal fail_library
                req = route.request
                url = urlparse(req.url)
                path = url.path
                query = parse_qs(url.query)
                if path == "/api/me":
                    return route.fulfill(json={"user": {"id": "alice", "name": "Alice"}, "workspaces": [
                        {"id": "one", "name": "Workspace", "org_name": "Acme", "role": "owner",
                         "paused": False, "settings": {}}]})
                if path == "/api/meta":
                    return route.fulfill(json={"auth": {"dev": True}, "harnesses": [], "sandbox": {}})
                if path.endswith("/onboarding"):
                    if req.method == "PATCH":
                        action = req.post_data_json
                        if action["action"] == "complete":
                            assert refreshed == {"repos", "credentials", "onboarding"}
                            state["completed_steps"].append(action["step"])
                        else:
                            state["current_step"] = action["step"]
                    else:
                        refreshed.add("onboarding")
                    return route.fulfill(json=state)
                if path.endswith("/github/repos"):
                    if query.get("q") == ["missing"]:
                        return route.fulfill(json={"items": [], "next_page": None})
                    if query.get("q") == ["error"]:
                        return route.fulfill(json={"items": [], "next_page": None, "error": "Discovery unavailable"})
                    names = ["acme/library"] if query.get("page") == ["2"] else ["acme/app", "acme/readonly"]
                    return route.fulfill(json={"items": [{"full_name": name, "private": True,
                        "can_push": name != "acme/readonly", "description": "Accessible repository"} for name in names],
                        "next_page": None if query.get("page") == ["2"] else 2})
                if path.endswith("/repositories/preview"):
                    name = req.post_data_json["full_name"]
                    config = deepcopy(detected)
                    if name.endswith("library"):
                        config["qa"].pop("start")
                        config["start_detection"] = {"available": False, "reason": "No known server"}
                    return route.fulfill(json={"full_name": name, "default_branch": "main", "config": config})
                if path.endswith("/repositories"):
                    if req.method == "POST":
                        body = req.post_data_json
                        connections.append(body["full_name"])
                        if body["full_name"].endswith("library") and fail_library:
                            fail_library = False
                            return route.fulfill(status=503, json={"detail": "Temporary save failure"})
                        config = deepcopy(detected)
                        patch = body["config"]
                        config.update({"setup": patch["setup"], "checks": patch["checks"]})
                        config["qa"].update(patch["qa"])
                        config["release"].update(patch["release"])
                        repo = {"id": str(len(saved)), "full_name": body["full_name"], "default_branch": "main", "config": config}
                        saved.append(repo)
                        return route.fulfill(json=repo)
                    refreshed.add("repos")
                    return route.fulfill(json=saved)
                if path.endswith("/credentials/github_token/test"):
                    diagnostics.append(query["repository"][0])
                    return route.fulfill(json={"kind": "github_token", "status": "valid", "detail": "Permissions need review",
                        "metadata": {"permissions": [{"permission": "contents", "required": "write", "state": "unverified", "instruction": "Select this repository in your token."}]}})
                if path.endswith("/credentials"):
                    refreshed.add("credentials")
                    return route.fulfill(json={"items": [], "server_fallbacks": {}})
                return route.fulfill(json=[])

            context.route("**/api/**", route_api)
            page = context.new_page()
            errors = []
            page.on("pageerror", lambda error: errors.append(str(error)))
            page.goto(base_url + "/w/one/onboarding")
            search = page.get_by_label("Search accessible GitHub repositories")
            search.fill("missing")
            expect(page.get_by_text("No accessible repositories match", exact=False)).to_be_visible()
            search.fill("error")
            expect(page.get_by_text("Discovery unavailable", exact=True)).to_be_visible()
            search.fill("")
            readonly = page.get_by_role("checkbox", name="acme/readonly", exact=False)
            expect(readonly).to_be_disabled()
            expect(page.get_by_text("Read-only access", exact=False)).to_be_visible()
            page.get_by_role("checkbox", name="acme/app", exact=False).focus()
            page.keyboard.press("Space")
            app = page.get_by_role("region", name="Review acme/app", exact=True)
            expect(app.get_by_label("Setup commands", exact=False)).to_have_value("npm ci")
            assert connections == []
            expect(app.get_by_role("button", name="Confirm & connect repository")).to_be_disabled()
            app.get_by_label("Setup commands", exact=False).fill("edited install")
            app.get_by_label("Test / check commands", exact=False).fill("edited test")
            app.get_by_label("Start command (config.qa.start)").fill("edited server --port $PORT")
            app.get_by_label("This repository deploys", exact=False).check()
            app.get_by_label("I have reviewed", exact=False).focus()
            page.keyboard.press("Space")
            page.get_by_role("button", name="Load more repositories").focus()
            page.keyboard.press("Enter")
            page.get_by_role("checkbox", name="acme/library", exact=False).focus()
            page.keyboard.press("Space")
            library = page.get_by_role("region", name="Review acme/library", exact=True)
            expect(library.get_by_label("Start command (config.qa.start)")).to_have_value("")
            expect(library.get_by_text("Start command unknown / not configured", exact=False)).to_be_visible()
            library.get_by_label("I have reviewed", exact=False).check()
            expect(library.get_by_role("button", name="Confirm & connect repository")).to_be_disabled()
            library.get_by_label("This repository has no runnable app", exact=False).check()
            library.get_by_label("I have reviewed", exact=False).check()
            library.get_by_label("Setup commands", exact=False).fill("library install")
            library.get_by_label("I have reviewed", exact=False).check()
            assert page.evaluate("document.documentElement.scrollWidth <= innerWidth")
            app.get_by_role("button", name="Confirm & connect repository").focus()
            page.keyboard.press("Enter")
            expect(app.get_by_text("Connected · configuration saved")).to_be_visible()
            library.get_by_role("button", name="Confirm & connect repository").focus()
            page.keyboard.press("Enter")
            expect(library.get_by_text("Temporary save failure", exact=True)).to_be_visible()
            expect(app.get_by_label("Setup commands", exact=False)).to_have_value("edited install")
            expect(library.get_by_label("Setup commands", exact=False)).to_have_value("library install")
            library.get_by_role("button", name="Retry save").focus()
            page.keyboard.press("Enter")
            expect(library.get_by_text("Connected · configuration saved")).to_be_visible()
            assert connections.count("acme/app") == 1
            assert connections.count("acme/library") == 2
            assert set(diagnostics) == {"acme/app", "acme/library"}
            assert saved[0]["config"]["qa"] == {"start": "edited server --port $PORT", "pages": ["/login"]}
            assert saved[0]["config"]["release"]["mode"] == "on_merge"
            assert saved[0]["config"]["smoke"] == ["npm run build"]
            page.reload()
            app = page.get_by_role("region", name="Review acme/app", exact=True)
            expect(app.get_by_label("Setup commands", exact=False)).to_have_value("edited install")
            expect(app.get_by_label("This repository deploys", exact=False)).to_be_checked()
            assert page.evaluate("document.documentElement.scrollWidth <= innerWidth")
            refreshed.clear()
            page.get_by_role("button", name="Continue with saved repositories").focus()
            page.keyboard.press("Enter")
            expect(page.get_by_role("heading", name="How autonomous", exact=True)).to_be_visible()
            assert state["completed_steps"] == ["repositories"]
            assert not errors, errors
            context.close()
        browser.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-url", default="http://127.0.0.1:4173")
    check(parser.parse_args().base_url.rstrip("/"))
    print("Repository onboarding browser checks passed at 375px and 1440px")
