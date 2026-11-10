"""Opt-in deterministic regression using the built web app and intercepted APIs.

    cd web && npm run build -- --outDir /tmp/guided-web
    # Serve /tmp/guided-web with a SPA fallback (e.g. vite preview).
    .venv/bin/python tests/browser_guided_outcome.py --base-url http://127.0.0.1:4173
"""
from __future__ import annotations

import argparse
from urllib.parse import urlparse

from playwright.sync_api import expect, sync_playwright


STAMP = "2026-11-10T12:00:00Z"


def task(task_id="t", **overrides):
    return {"id": task_id, "key": task_id.upper(), "outcome_id": "o", "title": "README contribution instructions",
            "spec": "Documentation only", "acceptance": ["README only"], "repo": "org/repo", "depends_on": [],
            "harness": "codex", "verifier_harness": "codex", "status": "running", "stage": "verify", "attempt": 1,
            "branch": "factory/readme", "head_sha": "head", "pr_number": None, "pr_url": "", "merge_sha": "",
            "summary": "", "blocked_reason": "", "evidence": {}, "cost_usd": 0, "cost_known": True, "tokens": 0,
            "created_at": STAMP, "updated_at": STAMP, "started_at": STAMP, "finished_at": None, **overrides}


def decision(kind, task_id=None):
    return {"id": kind, "kind": kind, "title": "Review " + kind, "body": "Check scope and evidence", "options": [
        {"value": "approve", "label": "Approve"}], "status": "open", "resolution": "", "comment": "",
        "resolved_by": None, "resolved_at": None, "reusable": False, "scope": "item", "outcome_id": "o",
        "task_id": task_id, "created_at": STAMP, "judge": None, "actionable": True}


def check(base_url):
    with sync_playwright() as p:
        browser = p.chromium.launch(args=["--no-sandbox"])
        context = browser.new_context(viewport={"width": 1440, "height": 1000})
        settings = {"harness": {"builder": "codex"}, "autonomy": {"plan": "approval", "merge": "approval"}}
        outcome = {"id": "o", "number": 1, "title": "Guided README", "brief": "Contributing", "acceptance": [],
                   "repo_ids": ["r"], "status": "planning", "priority": 2, "source": "user", "summary": "",
                   "blocked_reason": "", "created_at": STAMP, "updated_at": STAMP, "completed_at": None,
                   "progress": {"done": 0, "total": 0}, "cost_usd": 0, "cost_known": True,
                   "open_decisions": 0, "prs": [], "track": [], "linear_issue": None}
        detail = {"outcome": outcome, "guidance": {"template": "readme-contributing", "repo_id": "r"},
                  "repositories": [{"id": "r", "full_name": "org/repo"}], "attachments": [], "tasks": [],
                  "plans": [], "runs": [], "decisions": [], "messages": [], "events": []}
        repo = {"id": "r", "full_name": "org/repo", "default_branch": "main", "config": {}}
        role, user_id = "admin", "u"
        writes = []
        artifact_requests = []
        long_name = "check-" + "very-long-evidence-label-" * 25
        artifacts = [{"id": kind, "kind": kind, "name": long_name if kind == "check" else kind,
                      "task_id": "t", "run_id": "run", "data": {}, "size": 50, "created_at": STAMP}
                     for kind in ("diff", "check", "report", "smoke")]

        def route_api(route):
            path = urlparse(route.request.url).path
            if route.request.method != "GET":
                writes.append((path, route.request.post_data_json))
            if path == "/api/me":
                return route.fulfill(json={"user": {"id": user_id, "name": "Tester"}, "organizations": [],
                                           "workspaces": [{"id": "one", "name": "Team", "role": role, "settings": settings}]})
            if path == "/api/meta":
                return route.fulfill(json={"harnesses": [], "sandbox": {}, "auth": {"dev": True}})
            if path.endswith("/onboarding") or path.endswith("/onboarding/start"):
                return route.fulfill(json={"ready": True, "current_step": "outcome", "completed_steps": ["models", "github", "repositories", "autonomy", "team"],
                                           "first_outcome_id": None, "missing_steps": [], "validation_uncertain_steps": []})
            if path.endswith("/inbox"):
                return route.fulfill(json={"decisions": detail["decisions"]})
            if path.endswith("/pulls"):
                return route.fulfill(json={"items": [], "counts": {}})
            if path.endswith("/stream"):
                return route.fulfill(body="", content_type="text/event-stream")
            if path.endswith("/repositories"):
                return route.fulfill(json=[repo])
            if path.endswith("/outcomes"):
                return route.fulfill(json=outcome if route.request.method == "POST" else [outcome])
            if path.endswith("/outcomes/o"):
                return route.fulfill(json=detail)
            if "/decisions/" in path and path.endswith("/resolve"):
                next(d for d in detail["decisions"] if d["id"] == path.split("/")[-2])["status"] = "resolved"
                return route.fulfill(json={})
            if path.endswith("/artifacts"):
                return route.fulfill(json=artifacts)
            if "/artifacts/" in path:
                kind = path.split("/")[-1]
                artifact_requests.append(kind)
                return route.fulfill(json={**next(a for a in artifacts if a["id"] == kind), "content": "Evidence body " + kind})
            return route.fulfill(json=[])

        context.route("**/api/**", route_api)
        page = context.new_page()
        errors = []
        page.on("pageerror", lambda e: errors.append(str(e)))
        page.goto(base_url + "/w/one/outcomes/o")
        guide = page.get_by_role("region", name="Guided outcome")
        expect(guide.get_by_role("article", name="Planning", exact=True)).to_contain_text("In progress")
        for name in ("Plan review", "Building", "Checks", "Independent verification", "Pull request", "CI", "Merge / approval", "Smoke"):
            expect(guide.get_by_role("article", name=name, exact=True)).to_be_attached()
        detail["plans"] = [{"id": "p", "version": 1, "status": "proposed", "summary": "Only README",
                            "content": {}, "approved_by": None, "approved_at": None, "created_at": STAMP}]
        detail["decisions"] = [decision("approve_plan")]
        outcome["status"] = "awaiting_approval"
        page.reload()
        guide.get_by_role("link", name="Inspect plan v1").click()
        expect(page.locator("#guided-plan")).to_be_focused()
        page.get_by_role("button", name="Dismiss guide").click()
        expect(page.get_by_role("article", name="Review approve_plan")).to_be_visible()
        page.reload()
        expect(page.get_by_role("button", name="Reopen guide")).to_be_visible()
        assert writes == []  # Dismissal cannot resolve decisions.
        page.get_by_role("button", name="Reopen guide").focus()
        page.keyboard.press("Enter")
        expect(guide.get_by_role("article", name="Plan review")).to_contain_text("Action needed")
        page.get_by_role("article", name="Review approve_plan").get_by_role("button", name="Approve", exact=True).click()
        expect(page.get_by_role("article", name="Review approve_plan")).to_have_count(0)
        assert writes[-1][0].endswith("/decisions/approve_plan/resolve")
        assert writes[-1][1]["resolution"] == "approve"

        passed = {"n": 1, "result": "verified", "checks": [{"command": "tests", "ok": True, "exit_code": 0}],
                  "verdict": {"verdict": "pass", "summary": "Independent README review", "criteria": [
                      {"criterion": "README only", "status": "pass", "evidence": "diff"}]}}
        detail["tasks"] = [task(evidence={"attempts": [passed]}), task("second", attempt=2, evidence={"attempts": [passed]})]
        detail["runs"] = [{"id": "run", "task_id": "t", "outcome_id": "o", "role": "builder", "attempt": 1,
                           "status": "succeeded", "harness": "codex", "model": "test", "started_at": STAMP,
                           "finished_at": STAMP, "input_tokens": 0, "output_tokens": 0, "cost_usd": 0}]
        outcome.update(status="running", progress={"done": 0, "total": 2})
        page.reload()
        verification = guide.get_by_role("article", name="Independent verification", exact=True)
        expect(verification).to_contain_text("T · Passed")
        expect(verification).to_contain_text("SECOND · Pending / unavailable")
        for name in ("CI", "Smoke"):
            expect(guide.get_by_role("article", name=name, exact=True)).to_contain_text("Not configured / unavailable")
        guide.get_by_role("link", name="Inspect T building evidence", exact=True).focus()
        page.keyboard.press("Enter")
        expect(page.get_by_label("Opened evidence")).to_contain_text("Evidence body diff")
        guide.get_by_role("link", name="Inspect T independent verification evidence", exact=True).click()
        expect(page.get_by_label("Opened evidence")).to_contain_text("Evidence body report")
        assert "diff" in artifact_requests and "report" in artifact_requests
        page.get_by_role("button", name=long_name, exact=True).click()
        expect(page.get_by_label("Opened evidence")).to_contain_text("Evidence body check")
        for width in (375, 1440):
            page.set_viewport_size({"width": width, "height": 1000})
            assert page.evaluate("document.documentElement.scrollWidth <= innerWidth"), width
            assert guide.evaluate("e => e.scrollWidth <= e.clientWidth"), width

        # All delivery evidence must refer to the current candidate/merge.
        detail["tasks"][0].update(status="completed", stage="done", merge_sha="merge-sha", evidence={
            "attempts": [passed], "ci": {"state": "success", "head": "head", "checks": [
                {"name": "CI", "conclusion": "success", "url": "https://example.test/check"}]},
            "smoke": {"state": "passed", "ref": "merge-sha", "checks": [{"command": "smoke", "ok": True}]}})
        page.reload()
        for name in ("CI", "Smoke"):
            expect(guide.get_by_role("article", name=name, exact=True)).to_contain_text("T · Passed")
        detail["tasks"][0]["evidence"]["ci"]["head"] = "older-head"
        detail["tasks"][0]["evidence"]["smoke"]["ref"] = "older-merge"
        page.reload()
        for name in ("CI", "Smoke"):
            expect(guide.get_by_role("article", name=name, exact=True)).not_to_contain_text("T · Passed")

        # A new budget reuses attempt 1; old success must not leak into it.
        detail["tasks"][0].update(attempt=1, stage="build", evidence={"budget_epoch": 1, "attempts": [passed]})
        page.reload()
        expect(verification).not_to_contain_text("T · Passed")
        expect(verification).to_contain_text("No independent verdict for the current attempt")
        for state in ("failed", "blocked", "cancelled", "reverted"):
            detail["tasks"][0].update(status=state, stage="done", evidence={"attempts": [passed]})
            page.reload()
            expect(verification).not_to_contain_text("T · Passed")
            expect(guide.get_by_role("article", name="Building", exact=True)).to_contain_text("T · " + state)
        # Rebase verification supersedes the old candidate verdict.
        detail["tasks"][0].update(status="running", stage="verify", evidence={"attempts": [passed],
            "revalidation": {"phase": "verify", "head": "new-head", "run": True}})
        page.reload()
        expect(verification).not_to_contain_text("T · Passed")

        # Backend persists successful revalidation as complete, not just the
        # transient passed phase. Exercise both verifier policies after merge.
        rebased_verdict = {**passed["verdict"], "summary": "Independent rebased README review",
                           "criteria": [{"criterion": "README only", "status": "pass", "evidence": "rebased diff"}]}
        for phase in ("passed", "complete"):
            for run in (True, False):
                revalidation = {"phase": phase, "head": "new-head", "run": run,
                                "checks": [{"command": "rebased tests", "ok": True, "exit_code": 0}]}
                if run:
                    revalidation["verifier"] = rebased_verdict
                detail["tasks"][0] = task(status="completed", stage="done", head_sha="new-head",
                    pr_url="https://example.test/pr/1", pr_number=1, merge_sha="new-merge", evidence={
                        "attempts": [passed], "revalidation": revalidation,
                        "ci": {"state": "success", "head": "new-head", "checks": [
                            {"name": "CI", "conclusion": "success", "url": "https://example.test/check"}]},
                        "smoke": {"state": "passed", "ref": "new-merge", "checks": [{"command": "smoke", "ok": True}]}})
                page.reload()
                for name in ("Checks", "Independent verification", "CI", "Smoke"):
                    expect(guide.get_by_role("article", name=name, exact=True)).to_contain_text("T · Passed")
                expect(guide.get_by_role("article", name="Independent verification", exact=True)).to_contain_text(
                    "Independent rebased README review" if run else "Independent README review")
                expect(guide.get_by_role("article", name="Pull request", exact=True)).to_contain_text("T · Published")
                expect(guide.get_by_role("article", name="Merge / approval", exact=True)).to_contain_text("T · Merge recorded")
                guide.get_by_role("link", name="Inspect T checks evidence", exact=True).click()
                expect(page.locator("#task-t-checks")).to_contain_text("Passed: rebased tests")
                expect(page.locator("#task-t-verification")).to_contain_text("Acceptance · passed")
                expect(page.locator("#task-t-verification .ev")).to_have_text("rebased diff" if run else "diff")
                page.get_by_text("Current verifier report", exact=True).click()
                expect(page.locator("details").filter(has=page.get_by_text("Current verifier report", exact=True))).to_contain_text(
                    "Independent rebased README review" if run else "Independent README review")

        # Completion alone cannot substitute for head-matched evidence or a
        # required verifier. Nor may a handled rejection override a new review.
        revalidation = detail["tasks"][0]["evidence"]["revalidation"]
        revalidation.update(run=True, verifier=rebased_verdict, head="older-head")
        page.reload()
        for name in ("Checks", "Independent verification", "CI"):
            expect(guide.get_by_role("article", name=name, exact=True)).not_to_contain_text("T · Passed")
        revalidation.update(head="new-head")
        revalidation.pop("verifier")
        page.reload()
        expect(verification).to_contain_text("No independent verdict for the current attempt")
        guide.get_by_role("link", name="Inspect T independent verification evidence", exact=True).click()
        expect(page.locator("#task-t-verification")).to_contain_text("not yet verified")
        revalidation.update(phase="handled", verifier={**rebased_verdict, "verdict": "fail"})
        page.reload()
        expect(verification).to_contain_text("T · Passed")
        guide.get_by_role("link", name="Inspect T independent verification evidence", exact=True).click()
        expect(page.locator("#task-t-verification")).to_contain_text("Acceptance · passed")
        expect(page.locator("#task-t-verification .ev")).to_have_text("diff")
        # A new budget with no verdict must not reuse even a completed rebase's
        # retained original verdict when re-verification was skipped.
        revalidation.update(phase="complete", run=False)
        detail["tasks"][0]["evidence"]["budget_epoch"] = 1
        page.reload()
        expect(verification).not_to_contain_text("T · Passed")

        detail["tasks"][0] = task(status="awaiting_approval", stage="merge", pr_url="https://example.test/pr/1",
            pr_number=1, evidence={"attempts": [passed], "ci": {"state": "none"}})
        detail["decisions"] = [decision("approve_merge", "t")]
        page.reload()
        page.get_by_role("button", name="Dismiss guide").click()
        expect(page.get_by_role("article", name="Review approve_merge")).to_be_visible()
        page.get_by_role("article", name="Review approve_merge").get_by_role("button", name="Approve", exact=True).click()
        assert writes[-1][0].endswith("/decisions/approve_merge/resolve")
        role = "viewer"
        detail["decisions"] = [decision("approve_merge", "t")]
        page.reload()
        expect(page.get_by_role("article", name="Review approve_merge").get_by_role("button", name="Approve", exact=True)).to_have_count(0)
        # Dismissal is scoped to the user. Any ordinary outcome can reopen the same guide.
        user_id = "another-user"
        detail["guidance"] = None
        page.reload()
        page.get_by_role("button", name="Reopen guide").click()
        expect(guide.get_by_role("article", name="Building", exact=True)).to_be_visible()
        # A fresh user's guided marker automatically opens the same outcome screen.
        role = "admin"
        user_id = "fresh-user"
        detail["guidance"] = {"template": "readme-contributing", "repo_id": "r"}
        page.goto(base_url + "/w/one/onboarding")
        page.get_by_role("button", name="Try guided README example").click()
        page.get_by_role("button", name="Start", exact=True).click()
        expect(page).to_have_url(base_url + "/w/one/outcomes/o")
        expect(page.get_by_role("button", name="Dismiss guide")).to_be_visible()
        creation = next(body for path, body in writes if path.endswith("/outcomes"))
        assert creation["template"] == "readme-contributing"
        assert errors == [], errors
        browser.close()
        print("PASS: lifecycle, retries, multiple tasks, missing gates, evidence viewer, approval APIs, dismissal and responsive keyboard access")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-url", default="http://127.0.0.1:4173")
    check(parser.parse_args().base_url)
