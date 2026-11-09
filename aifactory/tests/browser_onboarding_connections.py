"""Keyboard-only connection checks against a Vite preview; no external services."""
from __future__ import annotations

import argparse
from copy import deepcopy
from urllib.parse import urlparse

from playwright.sync_api import expect, sync_playwright


PERMISSIONS = [("contents", "Contents", "write"), ("pull_requests", "Pull requests", "write"),
               ("issues", "Issues", "write"), ("statuses", "Commit statuses", "write"),
               ("checks", "Checks", "read"), ("actions", "Actions", "read"), ("metadata", "Metadata", "read")]


def check(base_url):
    with sync_playwright() as p:
        browser = p.chromium.launch(args=["--no-sandbox"])
        for width in (375, 1440):
            context = browser.new_context(viewport={"width": width, "height": 900})
            settings = {"provider": "mindshub", "models": {"default": "old-default", "builder": "build"},
                        "budgets": {"outcome_usd": 45}, "autonomy": {"merge": "approval"}}
            state = {"current_step": "models", "completed_steps": [], "deferred": False,
                     "first_outcome_id": None, "ready": False, "missing_steps": ["repositories"],
                     "validation_uncertain_steps": []}
            model = {"kind": "mindshub_api_key", "label": "MindsHub", "configured": True, "hint": "mdb_••••last",
                     "updated_at": "initial", "status": "valid", "metadata": {}}
            github = {"kind": "github_token", "label": "GitHub", "configured": False, "status": "missing"}
            tests = []
            mode = "identity_invalid"
            submitted = []
            invalidations = set()

            def diagnostic(c, code):
                c.update(status="valid" if code in ("models_available", "github_permissions_unverified") else
                         "untested" if code == "network_error" else "invalid", updated_at=str(len(tests) + len(submitted)))
                c["detail"] = {"identity_invalid": "MindsHub did not accept this key.", "inference_denied": "This key cannot access inference.",
                               "network_error": "The service could not be reached.", "models_available": "Model access verified.",
                               "github_permissions_unverified": "GitHub account verified."}[code]
                c["metadata"] = {"diagnostic_code": code, "test_status": c["status"]}
                if code == "models_available":
                    c["metadata"]["models"] = ["plan", "build"]
                if code.startswith("github"):
                    c["metadata"].update(login="octocat", permissions=[{"permission": key, "state": "unverified"} for key, _, _ in PERMISSIONS])

            def route_api(route):
                req = route.request
                path = urlparse(req.url).path
                assert "secret-value" not in req.url
                if path == "/api/me":
                    return route.fulfill(json={"user": {"id": "u", "name": "Owner"}, "workspaces": [{"id": "one", "role": "owner", "name": "Test workspace", "org_name": "Acme", "settings": deepcopy(settings)}]})
                if path == "/api/meta":
                    return route.fulfill(json={"auth": {"dev": True}, "harnesses": [], "sandbox": {}})
                if path.endswith("/onboarding"):
                    if req.method == "PATCH":
                        body = req.post_data_json
                        assert "secret-value" not in req.post_data
                        if body["action"] == "navigate":
                            state["current_step"] = body["step"]
                        elif body["action"] == "complete":
                            state["completed_steps"].append(body["step"])
                    return route.fulfill(json=state)
                if path.endswith("/credentials"):
                    invalidations.add("credentials")
                    return route.fulfill(json={"items": [deepcopy(model), deepcopy(github)], "server_fallbacks": {},
                                               "github_permissions": [{"permission": key, "label": label, "required": level} for key, label, level in PERMISSIONS]})
                if "/credentials/" in path:
                    c = github if "github_token" in path else model
                    if req.method == "PUT":
                        submitted.append(req.post_data_json["value"])
                        diagnostic(c, "github_permissions_unverified" if c is github else mode)
                    else:
                        tests.append(path)
                        diagnostic(c, mode)
                    return route.fulfill(json=deepcopy(c))
                if path.endswith("/models"):
                    invalidations.add("models")
                    return route.fulfill(json={"provider": "mindshub", "models": ["plan", "build"]})
                if path == "/api/workspaces/one" and req.method == "PATCH":
                    settings.update(req.post_data_json["settings"])
                    return route.fulfill(json={"id": "one", "settings": settings})
                if path.endswith("/repositories"):
                    return route.fulfill(json=[])
                if path.endswith("/github/repos"):
                    return route.fulfill(json={"items": [], "next_page": None})
                if path.endswith("/events"):
                    return route.fulfill(body="", content_type="text/event-stream")
                return route.fulfill(json={})

            context.route("**/api/**", route_api)
            page = context.new_page()
            page.goto(base_url + "/w/one/onboarding")
            expect(page.get_by_text("MindsHub did not accept this key.", exact=True)).to_be_visible()
            assert len(tests) == 1
            expect(page.get_by_text("mdb_••••last", exact=True)).to_be_visible()
            expect(page.get_by_label("Planner model", exact=True)).to_have_value("old-default")
            expect(page.get_by_label("Builder model", exact=True)).to_have_value("build")
            expect(page.get_by_role("button", name="Save models and continue")).to_be_disabled()
            page.get_by_label("Model provider", exact=True).select_option("openai")
            expect(page.get_by_text("Create an API key in your OpenAI project", exact=False)).to_be_visible()
            expect(page.get_by_text("starting with mdb_", exact=False)).to_have_count(0)
            page.get_by_label("Model provider", exact=True).select_option("mindshub")
            page.get_by_role("button", name="Replace MindsHub API key").press("Enter")
            field = page.get_by_label("MindsHub API key", exact=True)
            field.fill("mdb_secret-value")
            mode = "inference_denied"
            field.press("Enter")
            expect(page.locator('input[type="password"]')).to_have_count(0)
            expect(page.get_by_text("This key cannot access inference.", exact=True)).to_be_visible()
            mode = "network_error"
            page.get_by_role("button", name="Retry test", exact=True).press("Enter")
            expect(page.get_by_text("The service could not be reached.", exact=True)).to_be_visible()
            mode = "models_available"
            page.get_by_role("button", name="Retry test", exact=True).press("Enter")
            expect(page.get_by_text("Model access verified.", exact=True)).to_be_visible()
            # Native selects and form submission are fully keyboard operable.
            for role in ("Planner", "Verifier"):
                select = page.get_by_label(role + " model", exact=True)
                select.press("ArrowDown")
            expect(page.get_by_label("Planner model", exact=True)).to_have_value("plan")
            page.get_by_role("button", name="Save models and continue").press("Enter")
            expect(page.get_by_role("heading", name="GitHub", exact=True)).to_be_visible()
            assert settings["models"] == {"default": "old-default", "planner": "plan", "builder": "build", "verifier": "plan"}
            assert settings["budgets"]["outcome_usd"] == 45
            page.get_by_label("GitHub token", exact=True).fill("ghp_secret-value")
            page.get_by_label("GitHub token", exact=True).press("Enter")
            expect(page.get_by_text("octocat", exact=True)).to_be_visible()
            for _, label, level in PERMISSIONS:
                expect(page.get_by_text(f"{label}: {'Read and write' if level == 'write' else 'Read-only'} — unverified", exact=True)).to_be_visible()
            assert "github" not in state["completed_steps"]
            assert page.evaluate("document.documentElement.scrollWidth <= innerWidth")
            assert "secret-value" not in page.locator("body").inner_text()
            assert "secret-value" not in page.evaluate("JSON.stringify(localStorage)")
            page.get_by_role("button", name="Continue to repository selection").press("Enter")
            expect(page.get_by_role("heading", name="Repositories", exact=True)).to_be_visible()
            # A repository-verified token is not automatically downgraded on revisit.
            github["metadata"]["repository"] = "acme/app"
            page.get_by_role("button", name="2 GitHub", exact=False).press("Enter")
            expect(page.get_by_text("octocat", exact=True)).to_be_visible()
            assert not any("github_token/test" in path for path in tests)
            page.get_by_role("button", name="1 AI models", exact=False).press("Enter")
            expect(page.get_by_label("Planner model", exact=True)).to_have_value("plan")
            page.reload()
            expect(page.get_by_label("Builder model", exact=True)).to_have_value("build")
            expect(page.get_by_text("mdb_••••last", exact=True)).to_be_visible()
            expect(page.locator('input[type="password"]')).to_have_count(0)
            assert invalidations == {"models", "credentials"}
            print(f"PASS {width}px: masked reload, diagnostic recovery, keyboard save, persisted models, permissions and evidence revisit")
            context.close()
        browser.close()


def check_provider_recovery(base_url):
    """An unavailable saved provider must not gate a tested replacement provider."""
    with sync_playwright() as p:
        browser = p.chromium.launch(args=["--no-sandbox"])
        for width, failure in ((375, "inference_denied"), (1440, "network_error")):
            context = browser.new_context(viewport={"width": width, "height": 900})
            settings = {"provider": "mindshub", "models": {"default": "old-default"},
                        "budgets": {"outcome_usd": 45}}
            state = {"current_step": "models", "completed_steps": [], "deferred": False,
                     "first_outcome_id": None, "ready": False, "missing_steps": ["models"],
                     "validation_uncertain_steps": []}
            credentials = [
                {"kind": "mindshub_api_key", "configured": True, "hint": "mdb_••••last",
                 "updated_at": "initial", "status": "untested", "metadata": {}},
                {"kind": "openai_api_key", "configured": False, "status": "missing"},
            ]
            saves = []

            def route_api(route):
                req = route.request
                path = urlparse(req.url).path
                if path == "/api/me":
                    return route.fulfill(json={"user": {"id": "u", "name": "Owner"}, "workspaces": [
                        {"id": "one", "role": "owner", "name": "Recovery", "org_name": "Acme", "settings": deepcopy(settings)}]})
                if path == "/api/meta":
                    return route.fulfill(json={"auth": {"dev": True}, "harnesses": [], "sandbox": {}})
                if path.endswith("/onboarding"):
                    if req.method == "PATCH":
                        body = req.post_data_json
                        if body["action"] == "navigate":
                            state["current_step"] = body["step"]
                        elif body["action"] == "complete":
                            state["completed_steps"].append(body["step"])
                    return route.fulfill(json=state)
                if path.endswith("/credentials"):
                    return route.fulfill(json={"items": deepcopy(credentials), "server_fallbacks": {}, "github_permissions": []})
                if "/credentials/" in path:
                    if "openai_api_key" in path:
                        c = credentials[1]
                        c.update(configured=True, status="valid", hint="sk-••••last", updated_at="tested",
                                 detail="OpenAI model access verified.", metadata={"diagnostic_code": "models_available",
                                 "test_status": "valid", "models": ["replacement-model"]})
                    else:
                        c = credentials[0]
                        c.update(updated_at="tested", metadata={"diagnostic_code": failure, "test_status": "invalid"})
                    return route.fulfill(json=deepcopy(c))
                if path.endswith("/models"):
                    if settings["provider"] == "openai":
                        return route.fulfill(json={"provider": "openai", "models": ["replacement-model"]})
                    if failure == "network_error":
                        return route.fulfill(status=503, json={"detail": "Provider unreachable"})
                    return route.fulfill(json={"provider": "mindshub", "models": [], "error": "Inference access denied"})
                if path == "/api/workspaces/one" and req.method == "PATCH":
                    saves.append(req.post_data_json["settings"])
                    settings.update(saves[-1])
                    return route.fulfill(json={"id": "one", "settings": settings})
                if path.endswith("/events"):
                    return route.fulfill(body="", content_type="text/event-stream")
                return route.fulfill(json=[])

            context.route("**/api/**", route_api)
            page = context.new_page()
            page.goto(base_url + "/w/one/onboarding")
            expect(page.get_by_text("Could not load the workspace model catalog.", exact=False)).to_be_visible(timeout=15000)
            expect(page.get_by_role("button", name="Save models and continue")).to_be_disabled()
            page.get_by_label("Model provider", exact=True).select_option("openai")
            field = page.get_by_label("OpenAI API key", exact=True)
            field.fill("sk-secret-value")
            field.press("Enter")
            expect(page.get_by_text("OpenAI model access verified.", exact=True)).to_be_visible()
            expect(page.get_by_role("button", name="Retry models")).to_have_count(0)
            # Successful credential validation alone must not silently replace defaults.
            expect(page.get_by_role("button", name="Save models and continue")).to_be_disabled()
            for role in ("Planner", "Builder", "Verifier"):
                page.get_by_label(role + " model", exact=True).press("ArrowDown")
            button = page.get_by_role("button", name="Save models and continue")
            expect(button).to_be_enabled()
            button.press("Enter")
            expect(page.get_by_role("heading", name="GitHub", exact=True)).to_be_visible()
            assert saves == [{"provider": "openai", "models": {"default": "old-default",
                              "planner": "replacement-model", "builder": "replacement-model", "verifier": "replacement-model"}}]
            assert settings["budgets"] == {"outcome_usd": 45}
            page.get_by_role("button", name="1 AI models", exact=False).press("Enter")
            page.reload()
            expect(page.get_by_label("Model provider", exact=True)).to_have_value("openai")
            for role in ("Planner", "Builder", "Verifier"):
                expect(page.get_by_label(role + " model", exact=True)).to_have_value("replacement-model")
            expect(page.get_by_role("button", name="Save models and continue")).to_be_enabled()
            assert "secret-value" not in page.locator("body").inner_text()
            print(f"PASS {width}px: switch provider after {failure}, keyboard save and reload")
            context.close()
        browser.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-url", default="http://127.0.0.1:4173")
    base_url = parser.parse_args().base_url.rstrip("/")
    check(base_url)
    check_provider_recovery(base_url)
