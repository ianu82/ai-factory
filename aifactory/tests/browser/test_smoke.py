"""Harness contract, not the full T2/T3 interaction suite."""


def test_seeded_workspace(signed_in_page, scenario, navigate, no_overflow, checkpoint):
    from playwright.sync_api import expect

    page = signed_in_page
    navigate(page, "Inbox")
    expect(page.get_by_role("region", name="Plans to approve").get_by_role("listitem").filter(has_text="Browser approve_plan")).to_be_visible()
    if page.viewport_size["width"] < 600:
        expect(page.locator(".mobile-bar").get_by_text(scenario["name"], exact=True)).to_be_visible()
    else:
        expect(page.get_by_role("complementary", name="Main").get_by_text(scenario["name"], exact=True)).to_be_visible()
    no_overflow(page)
    checkpoint("inbox")
    navigate(page, "Outcomes")
    expect(page.get_by_role("heading", name="Outcomes", exact=True)).to_be_visible()
    expect(page.get_by_text("Scenario running", exact=True)).to_be_visible()
    no_overflow(page)
    checkpoint("outcomes")
    navigate(page, "Pull requests")
    expect(page.get_by_text("Factory awaiting approval", exact=True)).to_be_visible()
    expect(page.get_by_text("Failing CI", exact=True)).to_be_visible()
    no_overflow(page)
    checkpoint("pulls")


def test_fresh_identity_and_materializable_plan(signed_in_page, scenario, browser_server):
    """Each independent test starts with untouched decisions and its own settings."""
    page = signed_in_page
    api = page.context.request
    base = f"/api/workspaces/{scenario['workspace_id']}"
    me = api.get("/api/me").json()
    assert [w["id"] for w in me["workspaces"]] == [scenario["workspace_id"]]
    assert me["workspaces"][0]["settings"]["autonomy"]["plan"] == "approval"
    inbox = api.get(f"{base}/inbox").json()
    assert {d["kind"] for d in inbox["decisions"]} == {
        "approve_plan", "approve_merge", "question", "unblock", "budget", "approve_release"}
    outcomes = api.get(f"{base}/outcomes").json()
    assert {o["status"] for o in outcomes} == {
        "planning", "awaiting_approval", "needs_input", "running", "attention", "blocked", "completed",
        "partially_completed", "cancelled"}
    evidence = api.get(f"{base}/outcomes/{scenario['outcomes']['completed']}").json()
    assert evidence["tasks"][0]["evidence"]["attempts"][0]["verdict"]["criteria"][0]["status"] == "pass"
    assert evidence["runs"]
    artifacts = api.get(f"{base}/tasks/{scenario['tasks']['completed']}/artifacts").json()
    assert any(a["name"] == "Verification evidence" for a in artifacts)
    pulls = api.get(f"{base}/pulls").json()
    assert set(pulls["counts"]) == {"ready", "attention", "checks", "draft"}
    linked = next(p for p in pulls["items"] if p["number"] == 6)
    assert linked["factory"]["decision_id"] == scenario["decisions"]["approve_merge"]
    proposal = api.post(f"{base}/signals/{scenario['improvement_id']}",
                        data={"action": "accept", "note": "Edited browser team note"})
    assert proposal.status == 200, proposal.text()
    repos = api.get(f"{base}/repositories").json()
    assert "Edited browser team note" in repos[0]["config"]["context"]
    response = api.post(f"{base}/decisions/{scenario['decisions']['approve_plan']}/resolve",
                        data={"resolution": "approve"})
    assert response.status == 200, response.text()
    detail = api.get(f"{base}/outcomes/{scenario['outcomes']['awaiting_approval']}").json()
    assert [t["key"] for t in detail["tasks"]] == ["T1"]
    # The counter must not collide with seeded outcome numbers.
    created = browser_server.control("emit", workspace_id=scenario["workspace_id"], title="Counter smoke")
    detail = api.get(f"{base}/outcomes/{created['outcome_id']}").json()
    assert detail["outcome"]["number"] == 10


def test_private_control_stream_reconnect(signed_in_page, navigate, control):
    from playwright.sync_api import expect

    page = signed_in_page
    navigate(page, "Outcomes")
    expect(page.get_by_text("Scenario running", exact=True)).to_be_visible()
    first = control("wait_stream")
    assert control("interrupt_stream")["interrupted"] == 1
    control("wait_stream", connections=first["connections"] + 1)
    control("emit", title="Arrived through the real event stream")
    expect(page.get_by_text("Arrived through the real event stream", exact=True)).to_be_visible(timeout=8000)
