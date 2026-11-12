"""Shared App/PAT controls in the real SPA, with deterministic API responses."""
from copy import deepcopy
from urllib.parse import urlparse

import pytest
from playwright.sync_api import expect


@pytest.mark.parametrize("configured", [False, True], ids=["token", "app"])
def test_github_connection_keyboard_and_repositories(browser_context, configured, no_overflow, checkpoint):
    context = browser_context
    state = {"current_step": "github", "completed_steps": [], "deferred": False,
             "first_outcome_id": None, "ready": False, "missing_steps": ["repositories"],
             "validation_uncertain_steps": []}
    permissions = {"contents": "write", "pull_requests": "write", "issues": "write",
                   "checks": "read", "actions": "read", "deployments": "read",
                   "metadata": "read", "statuses": "read"}
    connection = {"configured": configured, "installation_id": "42" if configured else None,
                  "ready": configured, "status": "active" if configured else "disconnected",
                  "account": "acme", "granted_permissions": permissions if configured else {},
                  "missing_permissions": {}, "manage_url": "https://github.com/settings/installations/42" if configured else None}
    requests = []
    started = []
    credentials = {"items": [{"kind": "github_token", "label": "GitHub", "configured": False,
                              "status": "missing", "metadata": {}}],
                   "server_fallbacks": {}, "github_permissions": []}

    def route_api(route):
        request = route.request
        path = urlparse(request.url).path
        requests.append((request.method, path))
        if path == "/api/me":
            return route.fulfill(json={"user": {"id": "u", "name": "Owner"}, "workspaces": [
                {"id": "one", "role": "owner", "name": "Test workspace", "org_name": "Acme", "settings": {}}]})
        if path == "/api/meta":
            return route.fulfill(json={"auth": {"dev": True, "github_app": configured}, "harnesses": [], "sandbox": {}})
        if path.endswith("/onboarding/start"):
            return route.fulfill(json=state)
        if path.endswith("/onboarding"):
            if request.method == "PATCH":
                state["current_step"] = request.post_data_json["step"]
            return route.fulfill(json=deepcopy(state))
        if path.endswith("/github/connection/start"):
            started.append(request.url)
            # Return to this same-origin preview in place of visiting external GitHub.
            origin = urlparse(request.url)
            return route.fulfill(json={"url": f"{origin.scheme}://{origin.netloc}/w/one/onboarding"})
        if path.endswith("/github/connection"):
            if request.method == "DELETE":
                connection.update(installation_id=None, ready=False, status="disconnected", manage_url=None)
            return route.fulfill(json=deepcopy(connection))
        if path.endswith("/inbox"):
            return route.fulfill(json={"decisions": [], "alerts": []})
        if path.endswith("/pulls"):
            return route.fulfill(json={"counts": {"ready": 0, "attention": 0}, "items": []})
        if path.endswith("/credentials"):
            return route.fulfill(json=credentials)
        if path.endswith("/repositories"):
            return route.fulfill(json=[])
        if path.endswith("/github/repos"):
            return route.fulfill(json={"items": [{"full_name": "acme/installation-repo", "private": True,
                                                 "can_push": True, "description": "Installed repository"}], "next_page": None})
        if path.endswith("/events"):
            return route.fulfill(body="", content_type="text/event-stream")
        return route.fulfill(json={})

    context.route("**/api/**", route_api)
    page = context.new_page()
    errors = []
    page.on("pageerror", lambda error: errors.append(str(error)))
    page.goto("/w/one/onboarding")
    expect(page.get_by_role("heading", name="GitHub", exact=True)).to_be_visible()
    install = page.get_by_role("button", name="Install the GitHub App", exact=True)
    if configured:
        expect(install).to_be_visible()
        expect(page.get_by_label("GitHub token", exact=True)).to_have_count(0)
        expect(page.get_by_role("list", name="GitHub App permissions")).to_contain_text("statuses: read")
        manage = page.get_by_role("link", name="Manage GitHub access")
        expect(manage).to_have_attribute("href", "https://github.com/settings/installations/42")
        expect(manage).to_have_attribute("rel", "noreferrer")
        no_overflow(page)
        checkpoint("github-app")
        install.focus()
        page.keyboard.press("Enter")
        expect(install).to_be_visible()
        expect(page.get_by_role("button", name="Continue to repository selection")).to_be_visible()
        assert len(started) == 1 and "destination=setup" in started[0]
        page.get_by_role("button", name="Continue to repository selection").focus()
        page.keyboard.press("Enter")
        expect(page.get_by_role("heading", name="Repositories", exact=True)).to_be_visible()
        expect(page.get_by_role("checkbox", name="acme/installation-repo", exact=False)).to_be_visible()
        assert ("GET", "/api/workspaces/one/github/repos") in requests
        no_overflow(page)
        checkpoint("installation-repositories")
    else:
        expect(install).to_have_count(0)
        token = page.get_by_label("GitHub token", exact=True)
        expect(token).to_be_visible()
        token.focus()
        expect(token).to_be_focused()
        no_overflow(page)
        checkpoint("github-token")

    # Settings must reuse the same component and preserve the PAT fallback.
    page.goto("/w/one/settings/credentials")
    expect(page.get_by_role("heading", name="Credentials", exact=True)).to_be_visible()
    if configured:
        expect(install).to_be_visible()
        expect(page.get_by_role("link", name="Manage GitHub access")).to_be_visible()
        disconnect = page.get_by_role("button", name="Disconnect GitHub App", exact=True)
        disconnect.focus()
        page.keyboard.press("Enter")
        expect(disconnect).to_have_count(0)
        assert ("DELETE", "/api/workspaces/one/github/connection") in requests
    else:
        expect(install).to_have_count(0)
        expect(page.get_by_label("GitHub value", exact=True)).to_be_visible()
    no_overflow(page)
    checkpoint("github-settings")
    assert not errors
