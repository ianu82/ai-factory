"""Local UI fixture: a synthetic workspace covering every outcome/task state.

For rendering checks only. Run against a throwaway local database with the orchestrator
disabled (AIFACTORY_ORCHESTRATOR=0); never against a deployed Factory.
"""

from __future__ import annotations

import os
import sys
from datetime import timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from aifactory.db import init_engine, session_scope, utcnow  # noqa: E402
from aifactory.models import (Artifact, Decision, Event, Membership, Message, Organization, Outcome, Plan,  # noqa: E402
                              Repository, Run, Secret, Signal, Task, User, Workspace)
from aifactory.pipeline.runtime import DEFAULT_SETTINGS  # noqa: E402
from aifactory.security import encrypt  # noqa: E402

assert os.environ.get("AIFACTORY_ORCHESTRATOR") == "0", "Disable the orchestrator before seeding fixtures"
init_engine()
now = utcnow()


def ago(minutes: float):
    return now - timedelta(minutes=minutes)


DIFF = """diff --git a/app/api/weight/[id]/route.ts b/app/api/weight/[id]/route.ts
index 3f1a2b4..9c0d1e2 100644
--- a/app/api/weight/[id]/route.ts
+++ b/app/api/weight/[id]/route.ts
@@ -18,6 +18,14 @@ export async function PATCH(request: Request, { params }: { params: { id: string } }) {
   const body = await request.json();
+  if (body.date !== undefined) {
+    if (typeof body.date !== "string" || !isCanonicalDate(body.date)) {
+      return NextResponse.json({ error: "date must be YYYY-MM-DD" }, { status: 400 });
+    }
+    if (body.date > today()) {
+      return NextResponse.json({ error: "date cannot be in the future" }, { status: 400 });
+    }
+  }
   const { data: entry } = await supabase.from("weight_entries").select("*").eq("id", params.id).single();
"""

with session_scope() as s:
    user = s.query(User).filter(User.email == "ian@mindsdb.com").one_or_none()
    if user is None:
        print("Sign in once with dev sign-in as ian@mindsdb.com (organization 'MindsDB') first.")
        sys.exit(1)
    member = s.query(Membership).filter(Membership.user_id == user.id).first()
    ws = s.get(Workspace, member.workspace_id)
    ws.name = "Calorie Tracker"
    ws.settings = DEFAULT_SETTINGS
    for kind, value, detail in [("mindshub_api_key", "mdb_fixture1.localonlyvalue", "Connected to MindsHub · 27 models available"),
                                ("github_token", "github_pat_fixture_local_only_value", "Connected as ianu82")]:
        s.add(Secret(workspace_id=ws.id, kind=kind, ciphertext=encrypt(value), hint=value[:12] + "…" + value[-4:],
                     status="valid", status_detail=detail, tested_at=ago(30)))
    app = Repository(workspace_id=ws.id, full_name="ianu82/calorie-tracker", default_branch="main",
                     description="Calorie tracking app",
                     config={"setup": ["npm ci --ignore-scripts --no-audit --no-fund"], "checks": ["npm run typecheck", "npm test"],
                             "smoke": ["npm run build", "npm test"], "detected": ["Node project (npm)"]})
    api = Repository(workspace_id=ws.id, full_name="ianu82/nutrition-api", default_branch="main",
                     config={"setup": ["uv sync"], "checks": ["uv run pytest -q"], "detected": ["Python project"]})
    s.add_all([app, api])
    s.flush()

    def outcome(n, title, status, brief, minutes, **kw):
        o = Outcome(workspace_id=ws.id, number=n, title=title, status=status, brief=brief, repo_ids=[app.id],
                    created_by=user.id, created_at=ago(minutes), updated_at=ago(kw.pop("updated", minutes / 3)), **kw)
        s.add(o)
        s.flush()
        return o

    def plan(o, summary, status="approved", approved_by=None):
        p = Plan(outcome_id=o.id, version=1, status=status, summary=summary,
                 content={"summary": "", "tasks": [], "risks": ["Fixture clock is 2026-11-07; date tests pin it."]},
                 approved_by=approved_by, approved_at=ago(40) if status == "approved" else None)
        s.add(p)
        s.flush()
        return p

    def task(o, p, key, title, status, stage, attempt=1, deps=(), **kw):
        t = Task(workspace_id=ws.id, outcome_id=o.id, plan_id=p.id, key=key, title=title, status=status, stage=stage,
                 attempt=attempt, repo_id=app.id, depends_on=list(deps), harness=kw.pop("harness", "native"),
                 verifier_harness=kw.pop("verifier", "native"), spec=kw.pop("spec", "Implement as described; add tests."),
                 acceptance=kw.pop("acceptance", ["Behaviour matches the request", "Tests cover the change"]),
                 started_at=ago(50) if status != "pending" else None, updated_at=ago(kw.pop("updated", 2)), **kw)
        s.add(t)
        s.flush()
        return t

    def run(o, t, role, status, cost, minutes, harness="native", tokens=(180_000, 9_000), err=""):
        s.add(Run(workspace_id=ws.id, outcome_id=o.id, task_id=t.id if t else None, role=role, harness=harness,
                  provider="mindshub", model="gpt-terra", status=status, turns=14, input_tokens=tokens[0],
                  output_tokens=tokens[1], cost_usd=cost, cost_known=True, started_at=ago(minutes),
                  finished_at=None if status == "running" else ago(minutes - 6), error=err))

    def ev(o, t, kind, message, minutes, level="info", **data):
        s.add(Event(workspace_id=ws.id, outcome_id=o.id, task_id=t.id if t else None, kind=kind, message=message,
                    level=level, data=data, created_at=ago(minutes)))

    crit = ["PATCH rejects non-string, non-canonical and impossible dates with 400 before any database call",
            "Valid past dates including 2024-02-29 are saved unchanged",
            "Existing weight-only updates and DELETE behave exactly as before"]
    o1 = outcome(1, "Reject invalid weight entry dates before saving", "completed",
                 "Weight PATCH currently stores whatever date string arrives. Validate strictly per PRD FR-5.1/5.3.", 300,
                 completed_at=ago(190), summary="All 1 task(s) merged and verified.", updated=190)
    p1 = plan(o1, "Add strict canonical date validation to the weight PATCH route before any lookup, with a focused regression test file. No schema or dependency changes.")
    t1 = task(o1, p1, "T1", "Validate weight PATCH dates and add regression tests", "completed", "done", attempt=2,
              acceptance=crit, branch="factory/1-t1-a8f3c1", pr_number=13, pr_url="https://github.com/ianu82/calorie-tracker/pull/13",
              head_sha="9c0d1e2a7b", merge_sha="4be81f09c2d3", summary="Merged and smoke-tested", finished_at=ago(190),
              evidence={"attempts": [
                  {"n": 1, "result": "verification_failed", "checks": [{"command": "npm run typecheck", "ok": True, "exit_code": 0, "duration": 21.4}, {"command": "npm test", "ok": True, "exit_code": 0, "duration": 8.2}]},
                  {"n": 2, "result": "verified", "checks": [{"command": "npm run typecheck", "ok": True, "exit_code": 0, "duration": 20.9}, {"command": "npm test", "ok": True, "exit_code": 0, "duration": 8.6}],
                   "verdict": {"verdict": "pass", "summary": "All three criteria verified.", "criteria": [
                       {"criterion": crit[0], "status": "pass", "evidence": "tests/weight-date-validation.test.cjs: 11 invalid inputs → 400; supabase mock asserts 0 calls"},
                       {"criterion": crit[1], "status": "pass", "evidence": "2024-02-29 and 2000-02-29 accepted; stored value unchanged"},
                       {"criterion": crit[2], "status": "pass", "evidence": "Existing suite 46/46 passing; DELETE test untouched"}], "findings": []}}],
                  "ci": {"state": "success", "checks": [{"name": "validate", "conclusion": "success", "url": "https://github.com/ianu82/calorie-tracker/actions"}, {"name": "sandbox", "conclusion": "success", "url": ""}]},
                  "merge": {"sha": "4be81f09c2d3", "pr": 13, "at": ago(195).isoformat()}, "smoke": {"state": "passed"}})
    run(o1, None, "planner", "succeeded", 0.41, 298)
    run(o1, t1, "builder", "succeeded", 1.12, 290)
    run(o1, t1, "verifier", "succeeded", 0.58, 270)
    run(o1, t1, "builder", "succeeded", 0.47, 250)
    run(o1, t1, "verifier", "succeeded", 0.52, 230)
    s.add(Artifact(workspace_id=ws.id, outcome_id=o1.id, task_id=t1.id, kind="diff", name="Attempt 2 diff", content=DIFF,
                   data={"additions": 8, "deletions": 0, "files": ["app/api/weight/[id]/route.ts"]}))
    for m, (kind, msg, lvl) in zip([299, 296, 290, 262, 258, 230, 225, 214, 196, 192], [
        ("outcome_created", "#1 Reject invalid weight entry dates before saving", "info"),
        ("plan_proposed", "Plan v1: 1 task(s)", "success"), ("plan_approved", "Plan approved by policy (automatic)", "success"),
        ("verification_failed", "Missing test for year 0000 and 13th month", "warn"),
        ("stage", "Attempt 2: building with native", "info"), ("verified", "Independent verification passed: All three criteria verified.", "success"),
        ("published", "Opened PR #13", "success"), ("ci_status", "CI success", "success"),
        ("merged", "Merged PR #13 (4be81f09)", "success"), ("smoke_passed", "Post-merge smoke checks passed", "success")]):
        ev(o1, t1, kind, msg, m, lvl)
    ev(o1, None, "outcome_completed", "Outcome #1 delivered", 190, "success")

    o2 = outcome(2, "Export the daily food log as CSV", "running",
                 "People want to take their log to a dietitian. Add a CSV download on the log page.", 70, updated=1)
    p2 = plan(o2, "Stream entries for a date range from a new authenticated endpoint, then add an Export button on the log page. README documents the format.")
    t2a = task(o2, p2, "T1", "Add /api/log/export CSV endpoint with streaming and tests", "running", "verify",
               acceptance=["GET /api/log/export?from&to returns text/csv with one row per entry", "Requires auth and only returns the caller's entries", "Unit tests cover escaping of commas and quotes"])
    t2b = task(o2, p2, "T2", "Add Export button to the log page", "pending", "", deps=["T1"], harness="codex", verifier="codex")
    t2c = task(o2, p2, "T3", "Document CSV export in README", "running", "ci", harness="anton", verifier="anton",
               pr_number=15, pr_url="https://github.com/ianu82/calorie-tracker/pull/15",
               evidence={"attempts": [{"n": 1, "result": "verified", "checks": []}]})
    run(o2, None, "planner", "succeeded", 0.39, 69)
    run(o2, t2a, "builder", "succeeded", 0.94, 60)
    run(o2, t2a, "verifier", "running", 0.21, 6)
    run(o2, t2c, "builder", "succeeded", 0.33, 55, harness="anton")
    run(o2, t2c, "verifier", "succeeded", 0.19, 40, harness="anton")
    for m, (kind, msg, lvl) in zip([69, 66, 60, 52, 50, 49, 40, 30, 7, 5, 3, 1], [
        ("outcome_created", "#2 Export the daily food log as CSV", "info"), ("plan_proposed", "Plan v1: 3 task(s)", "success"),
        ("plan_approved", "Plan approved by policy (automatic)", "success"), ("agent_command", "npm test -- log-export", "info"),
        ("check_result", "✓ npm run typecheck", "success"), ("check_result", "✓ npm test", "success"),
        ("published", "Opened PR #15", "success"), ("ci_status", "CI pending", "info"),
        ("stage", "Independent verification by a fresh native session", "info"), ("agent_command", "git diff 3f1a2b4 --stat", "info"),
        ("agent_command", "npm test -- --runInBand tests/log-export.test.cjs", "info"), ("guidance", "Ian: Use ISO dates in the header row", "info")]):
        ev(o2, t2a, kind, msg, m, lvl)
    s.add(Message(workspace_id=ws.id, outcome_id=o2.id, author_id=user.id, author_kind="user",
                  body="Use ISO dates (YYYY-MM-DD) in the CSV, not locale formats.", delivered=True, created_at=ago(1)))

    o3 = outcome(3, "Show a weekly calorie trend on the dashboard", "awaiting_approval",
                 "Replace the static weekly card with a 7-day trend against target.", 25, updated=12)
    plan(o3, "Aggregate the last 7 days by date in a new stats endpoint and replace the static weekly card with a trend line against the current target.", status="proposed")
    run(o3, None, "planner", "succeeded", 0.44, 24)
    s.add(Decision(workspace_id=ws.id, outcome_id=o3.id, kind="approve_plan", title="Approve plan for #3: Show a weekly calorie trend on the dashboard",
                   body="Two tasks: (T1) add a /api/stats/weekly endpoint aggregating log entries by day; (T2) replace WeeklyCard with a Recharts line chart using the existing theme tokens. Assumes the target comes from the user's current target, not historical targets.",
                   options=[{"value": "approve", "label": "Approve"}, {"value": "replan", "label": "Revise"}, {"value": "cancel", "label": "Cancel"}], created_at=ago(12)))

    o4 = outcome(4, "Fix log date navigation across timezones", "attention",
                 "Users in UTC-8 see the next day's log after 4pm.", 140, priority=1, updated=33)
    p4 = plan(o4, "Compute log day boundaries in the user's timezone in the log routes and date navigation.")
    t4 = task(o4, p4, "T1", "Use the user's timezone for log date boundaries", "needs_input", "build", attempt=0,
              blocked_reason="The profile has no timezone field. Should I add one to the profile schema (needs a migration), or derive it from the browser on each request?")
    run(o4, t4, "builder", "succeeded", 0.62, 60)
    s.add(Decision(workspace_id=ws.id, outcome_id=o4.id, task_id=t4.id, kind="question", title="T1 needs a decision",
                   body=t4.blocked_reason, options=[{"value": "answer", "label": "Answer and continue"}, {"value": "abandon", "label": "Abandon task"}], created_at=ago(33)))

    o5 = outcome(5, "Upgrade Next.js to 16 and fix deprecations", "attention", "Keep behaviour identical.", 400, updated=8)
    p5 = plan(o5, "Single upgrade task; fix deprecation warnings surfaced by the build without behaviour changes.")
    t5 = task(o5, p5, "T1", "Upgrade next, react and eslint-config-next; fix build warnings", "awaiting_approval", "merge",
              harness="codex", verifier="codex", pr_number=14, pr_url="https://github.com/ianu82/calorie-tracker/pull/14",
              evidence={"attempts": [{"n": 1, "result": "verified", "checks": [{"command": "npm run typecheck", "ok": True, "exit_code": 0, "duration": 24.0}, {"command": "npm test", "ok": True, "exit_code": 0, "duration": 9.1}]}],
                        "ci": {"state": "success", "checks": [{"name": "validate", "conclusion": "success", "url": ""}]}})
    run(o5, t5, "builder", "succeeded", 2.31, 390, harness="codex", tokens=(820_000, 31_000))
    run(o5, t5, "verifier", "succeeded", 0.88, 350, harness="codex")
    s.add(Decision(workspace_id=ws.id, outcome_id=o5.id, task_id=t5.id, kind="approve_merge", title="Approve merge: Upgrade next, react and eslint-config-next",
                   body="PR #14 passed independent verification and CI. https://github.com/ianu82/calorie-tracker/pull/14",
                   options=[{"value": "approve", "label": "Merge"}, {"value": "reject", "label": "Request changes"}], created_at=ago(8)))

    o6 = outcome(6, "Remove the unused chart dependency", "attention", "", 1500, priority=3, updated=900)
    p6 = plan(o6, "Remove the dependency and dead imports.")
    t6 = task(o6, p6, "T1", "Remove react-chartjs-2 and its imports", "failed", "checks", attempt=3,
              blocked_reason="Check `npm run typecheck` failed (exit 2): components/dashboard/WeightProgressWidget.tsx still imports Line from react-chartjs-2.")
    for i in range(3):
        run(o6, t6, "builder", "succeeded", 0.35, 1400 - i * 60)
    s.add(Decision(workspace_id=ws.id, outcome_id=o6.id, task_id=t6.id, kind="unblock", title="T1 did not pass after 3 attempts",
                   body="The task could not be completed within the attempt limit. Latest feedback:\n\n- Check `npm run typecheck` failed (exit 2): WeightProgressWidget.tsx imports Line from react-chartjs-2.",
                   options=[{"value": "retry", "label": "Try again (fresh attempts)"}, {"value": "abandon", "label": "Abandon task"}], created_at=ago(900)))

    s.add(Decision(workspace_id=ws.id, outcome_id=o1.id, kind="question", title="Planner needs a decision for #1", status="resolved",
                   resolution="answer", comment="Accept only CE 0001–9999; reject year 0000.", resolved_by=user.id, resolved_at=ago(280),
                   reusable=True, scope="workspace", created_at=ago(285)))
    s.add(Signal(workspace_id=ws.id, repo_id=app.id, source="github_issue", fingerprint="f1", title="Log page is slow with 500+ entries",
                 body="Scrolling stutters on the log page once a month has more than 500 entries.", url="https://github.com/ianu82/calorie-tracker/issues/21",
                 count=3, first_seen_at=ago(3000), last_seen_at=ago(45)))
    s.add(Signal(workspace_id=ws.id, repo_id=app.id, source="ci_failure", fingerprint="f2", title="validate failing on main in ianu82/calorie-tracker",
                 body="Run failed at 4be81f09.", url="https://github.com/ianu82/calorie-tracker/actions", count=1, first_seen_at=ago(20), last_seen_at=ago(20)))
    # History the Factory learns from: recurring review findings, a check builders keep failing, runtime QA that
    # cannot boot, and product feedback sent by the app itself.
    findings = [(o1, t1, "Weight PATCH accepts a date string with trailing whitespace; validate the trimmed canonical form"),
                (o6, t6, "No regression test covers the removed import path; the typecheck failure would have been caught"),
                (o5, t5, "Changed API behaviour without updating the route's integration test")]
    for i, (o, t, text) in enumerate(findings):
        s.add(Run(workspace_id=ws.id, outcome_id=o.id, task_id=t.id, role="verifier", harness="native", provider="mindshub",
                  model="gpt-terra", status="succeeded", cost_usd=0.06, started_at=ago(600 + i * 90),
                  result={"verdict": "fail", "summary": "Rejected", "criteria": [],
                          "findings": [{"severity": "blocker", "description": text}]}))
    for i, t, o in ((0, t6, o6), (1, t5, o5), (2, t1, o1)):
        ev(o, t, "check_result", "✗ npm run typecheck", 700 + i * 50, level="warn", command="npm run typecheck", exit_code=2)
    for i, (o, t) in enumerate(((o6, t6), (o5, t5))):
        ev(o, t, "qa_app_failed", "The app did not become ready for runtime QA: Error: connect ECONNREFUSED 127.0.0.1:54321 "
           "(Supabase is not running in the QA environment)", 500 + i * 120, level="warn")
    s.add(Signal(workspace_id=ws.id, repo_id=app.id, source="feedback", fingerprint="f3", title="Can't edit a food after logging it",
                 body="Tapping a logged food opens nothing on iPhone Safari.\n\nReported by a customer via the product.",
                 count=2, first_seen_at=ago(400), last_seen_at=ago(30)))
    ws.outcome_seq = 6
    member.last_viewed_at = ago(240)

from aifactory import insights  # noqa: E402

with session_scope() as s:
    ws_id = s.query(Workspace).first().id
    print("improvements", insights.analyze(s, ws_id))
print("Seeded local fixture workspace")
