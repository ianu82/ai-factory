"""Repository release settings against the real backend and built SPA."""
import re

from playwright.sync_api import expect


def test_release_configuration_round_trip_and_write_only_secret(signed_in_page, scenario):
    page = signed_in_page
    ws = scenario["workspace_id"]
    base = f"/api/workspaces/{ws}/repositories"
    repos = page.request.get(base).json()
    repo_id = repos[0]["id"]
    path = f"{base}/{repo_id}"
    legacy = {"mode": "on_merge", "health_url": "https://legacy.example.test/health?probe=1",
              "expect_text": "healthy", "custom": {"keep": [1, 2]}}
    assert page.request.patch(path, data={"config": {"release": legacy}}).status == 200
    page.goto(f"/w/{ws}/settings/repositories")
    page.get_by_text("acme/app", exact=True).click()
    editor = page.get_by_role("region", name="Release configuration")
    expect(editor.get_by_label("Release mode", exact=True)).to_have_value("on_merge")
    expect(editor.get_by_label("Health check after release (legacy)")).to_have_value(legacy["health_url"])
    page.get_by_role("button", name="Save", exact=True).click()
    expect(page.get_by_text("Repository saved", exact=True)).to_be_visible()
    assert page.request.get(base).json()[0]["config"]["release"] == legacy

    editor.get_by_label("Release mode", exact=True).select_option("train")
    editor.get_by_label("IANA timezone", exact=True).fill("Europe/London")
    editor.get_by_label("Local release time", exact=True).fill("15:30")
    editor.get_by_label("Cut release from main", exact=True).select_option("version_pr")
    editor.get_by_label("Version file path", exact=True).fill("package.json")
    editor.get_by_label("Version file format", exact=True).select_option("json")
    editor.get_by_label("Production approval policy", exact=True).select_option("approval")
    for stage in ("Staging", "Production", "Rollback"):
        details = editor.locator("details").filter(has=page.locator("summary", has_text=re.compile(f"^{stage} deployment")))
        details.locator("summary").click()
        details.get_by_label("Workflow filename or ID", exact=True).fill(f"{stage.lower()}.yml")
        details.get_by_label("Workflow inputs (JSON object of strings)", exact=True).fill('{"environment":"test"}')
    for stage in ("Staging", "Production"):
        details = editor.locator("details").filter(has=page.locator("summary", has_text=re.compile(f"^{stage} smoke")))
        details.locator("summary").click()
        details.get_by_label(f"{stage} smoke commands (one per line)", exact=True).fill("npm test\n./smoke.sh")
        details.get_by_label(f"{stage} smoke URLs (one HTTPS URL per line)", exact=True).fill(f"https://{stage.lower()}.example.test/health")

    staging = editor.locator("details").filter(has=page.locator("summary", has_text=re.compile("^Staging deployment")))
    staging.get_by_label("Workflow inputs (JSON object of strings)", exact=True).fill('{"private-paste":')
    updates = []
    page.on("request", lambda request: updates.append(request) if request.method == "PATCH" else None)
    editor.get_by_role("button", name="Save release configuration", exact=True).click()
    expect(editor.get_by_role("alert")).to_contain_text("Staging: Workflow inputs must be valid JSON")
    assert not updates
    assert "private-paste" not in editor.get_by_role("alert").inner_text()
    staging.get_by_label("Workflow inputs (JSON object of strings)", exact=True).fill('{"environment":"staging"}')

    secret_details = editor.locator("details").filter(has=page.locator("summary", has_text=re.compile("^Release webhook secret")))
    secret_details.locator("summary").click()
    expect(secret_details.locator("summary")).to_contain_text("Not configured")
    secret_input = editor.get_by_label("New webhook secret", exact=True)
    expect(secret_input).to_have_attribute("type", "password")
    secret_input.fill("browser-only-dummy-webhook-credential")
    editor.get_by_role("button", name="Save webhook secret", exact=True).click()
    expect(secret_input).to_have_value("")
    expect(secret_details.locator("summary")).to_contain_text("Configured")
    expect(editor).not_to_contain_text("browser-only-dummy-webhook-credential")

    production = editor.locator("details").filter(has=page.locator("summary", has_text=re.compile("^Production deployment")))
    production.get_by_label("Production target", exact=True).select_option("webhook")
    production.get_by_label("Webhook endpoint (HTTPS)", exact=True).fill("https://example.test/deploy")
    production.get_by_label("Webhook completion", exact=True).select_option("poll")
    production.get_by_label("Polling status URL (HTTPS)", exact=True).fill("https://example.test/status")
    rollback = editor.locator("details").filter(has=page.locator("summary", has_text=re.compile("^Rollback deployment")))
    rollback.get_by_label("Rollback target", exact=True).select_option("webhook")
    rollback.get_by_label("Webhook endpoint (HTTPS)", exact=True).fill("https://example.test/rollback")
    rollback.get_by_label("Final success HTTP statuses (comma separated; never 202)", exact=True).fill("201, 204")
    editor.get_by_role("button", name="Save release configuration", exact=True).click()
    expect(editor.get_by_role("button", name="Save release configuration", exact=True)).to_be_disabled()
    saved = page.request.get(base).json()[0]["config"]["release"]
    assert saved["schedule"]["timezone"] == "Europe/London"
    assert saved["schedule"]["time"] == "15:30"
    assert saved["cut"]["version_file"] == {"path": "package.json", "format": "json", "key": "version"}
    assert saved["production"]["completion"]["mode"] == "poll"
    assert saved["production"]["secret_configured"] is True
    assert saved["rollback"]["completion"] == {"mode": "synchronous", "success_statuses": [201, 204]}
    assert "secret_ref" not in str(saved)
    assert saved["staging"]["inputs"] == {"environment": "staging"}
    assert saved["staging_smoke"]["commands"] == ["npm test", "./smoke.sh"]
    assert saved["production_smoke"]["urls"] == ["https://production.example.test/health"]
    page.get_by_label("Team notes for agents", exact=False).fill("Changed notes")
    with page.expect_response(lambda r: r.request.method == "PATCH" and r.url.endswith(repo_id)):
        page.get_by_role("button", name="Save", exact=True).click()
    assert page.request.get(base).json()[0]["config"]["release"] == saved
    page.reload()
    page.get_by_text("acme/app", exact=True).click()
    expect(editor.get_by_label("Release mode", exact=True)).to_have_value("train")
    expect(editor.get_by_label("IANA timezone", exact=True)).to_have_value("Europe/London")
    rollback = editor.locator("details").filter(has=page.locator("summary", has_text=re.compile("^Rollback deployment")))
    rollback.locator("summary").click()
    expect(rollback.get_by_label("Final success HTTP statuses (comma separated; never 202)", exact=True)).to_have_value("201, 204")
    assert page.evaluate("document.documentElement.scrollWidth <= innerWidth + 1")


def test_non_admin_release_controls_disabled(signed_in_page, scenario):
    page = signed_in_page
    ws = scenario["workspace_id"]

    def member_view(route):
        response = route.fetch()
        body = response.json()
        for workspace in body["workspaces"]:
            workspace["role"] = "viewer"
        route.fulfill(response=response, json=body)

    # Backend role enforcement is covered by test_release_config; this checks the
    # same read-only projection in the browser without adding test control APIs.
    page.route("**/api/me", member_view)
    page.goto(f"/w/{ws}/settings/repositories")
    page.get_by_text("acme/app", exact=True).click()
    editor = page.get_by_role("region", name="Release configuration")
    expect(editor.get_by_label("Release mode", exact=True)).to_be_disabled()
    expect(editor.get_by_label("Health check after release (legacy)")).to_be_disabled()
    expect(editor.get_by_role("button", name="Save release configuration", exact=True)).to_be_disabled()
    editor.locator("summary", has_text=re.compile("^Release webhook secret")).click()
    expect(editor.get_by_label("New webhook secret", exact=True)).to_be_disabled()
    expect(editor.get_by_role("button", name="Save webhook secret", exact=True)).to_be_disabled()
