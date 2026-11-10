"""Deterministic, workspace-local scenarios. Imported only in the browser server."""
from __future__ import annotations

import hashlib
import uuid

from fastapi import Response

from aifactory.auth import DevSignIn, dev_sign_in
from aifactory.db import session_scope, utcnow
from aifactory.models import Artifact, Decision, Membership, Organization, Outcome, Plan, Repository, Run, Secret, Signal, Workspace
from aifactory.models import Task
from aifactory.security import encrypt
from aifactory.github import REQUIRED_PERMISSIONS

STATUSES = ("planning", "awaiting_approval", "needs_input", "running", "attention", "blocked", "completed",
            "partially_completed", "cancelled")
KINDS = ("approve_plan", "approve_merge", "question", "unblock", "budget", "approve_release")


async def create_scenario() -> dict:
    nonce = uuid.uuid4().hex
    email = f"browser-{nonce}@example.test"
    # The UI omits organization. Retire the previous test's dev-org binding before
    # invoking the real handler, otherwise upsert_identity grants its workspaces too.
    org_key = "dev-" + hashlib.sha256(b"Local development").hexdigest()[:24]
    with session_scope() as s:
        old = s.query(Organization).filter_by(mindshub_org_id=org_key).one_or_none()
        if old:
            old.mindshub_org_id = "retired-" + uuid.uuid4().hex
    with session_scope() as s:
        await dev_sign_in(DevSignIn(email=email, name="Browser tester"), Response(), s)
        org = s.query(Organization).filter_by(mindshub_org_id=org_key).one()
        ws = s.query(Workspace).filter_by(org_id=org.id).one()
        ws.name = f"Browser workspace {nonce[:8]}"
        ws.settings = {"provider": "openai", "models": {"default": "gpt-test"}, "decision_judges": "off", "autonomy": {"plan": "approval", "merge": "approval", "release": "approval"}}
        ws.onboarding = {"deferred": True}
        repo = Repository(workspace_id=ws.id, full_name="acme/app", config={"context": "Use pytest.", "checks": ["pytest -q"]})
        s.add(repo)
        s.flush()
        user_id = s.query(Membership).filter_by(workspace_id=ws.id).one().user_id
        for kind in ("openai_api_key", "github_token"):
            s.add(Secret(workspace_id=ws.id, kind=kind, ciphertext=encrypt("browser-dummy-not-a-real-credential"),
                         hint="browser dummy", status="valid", metadata_json={"models": ["gpt-test"], "login": "browser",
                         "repository": "acme/app", "test_status": "valid", "diagnostic_code": "github_verified",
                         "permissions": [{"permission": p, "state": "present"} for p in REQUIRED_PERMISSIONS]}))
        outcomes, plans, tasks = {}, {}, {}
        for number, status in enumerate(STATUSES, 1):
            outcome = Outcome(workspace_id=ws.id, number=number, title=f"Scenario {status}", status=status,
                              brief="A realistic browser fixture", acceptance=["Visible evidence is available"],
                              repo_ids=[repo.id], created_by=user_id)
            s.add(outcome)
            s.flush()
            plan = Plan(outcome_id=outcome.id, status="proposed" if status == "awaiting_approval" else "approved",
                        summary="Implement and verify the change", content={"tasks": [{"key": "T1", "repo": "acme/app",
                        "title": "Implement the seeded change", "spec": "Add the change and regression tests.",
                        "acceptance": ["Regression tests pass"], "depends_on": []}]})
            s.add(plan)
            s.flush()
            outcomes[status], plans[status] = outcome.id, plan.id
            if status == "awaiting_approval":
                continue  # approval must materialize T1, not collide with a pre-created task
            task = Task(workspace_id=ws.id, outcome_id=outcome.id, plan_id=plan.id, repo_id=repo.id,
                        key="T1", title="Implement the seeded change", spec="Add regression coverage",
                        acceptance=["Visible evidence is available"], verifier_harness="native",
                        status={"running": "awaiting_approval", "blocked": "failed", "attention": "failed"}.get(status, "completed"),
                        stage="merge" if status == "running" else "verify",
                        pr_number=6 if status == "running" else None,
                        evidence={"verdict": "pass", "summary": "Regression tests pass", "criteria": [
                            {"criterion": "Visible evidence is available", "status": "pass", "evidence": "pytest: 3 passed"}]})
            verdict = task.evidence
            task.evidence = {"attempts": [{"n": 1, "result": "verified", "verdict": verdict,
                              "checks": [{"command": "pytest -q", "ok": True}]}],
                             "ci": {"state": "success", "checks": []}}
            s.add(task)
            s.flush()
            tasks[status] = task.id
            run = Run(workspace_id=ws.id, outcome_id=outcome.id, task_id=task.id, role="verifier", harness="native",
                      model="gpt-test", status="succeeded", turns=3, input_tokens=1000, output_tokens=200,
                      cost_usd=0.02, result=verdict, finished_at=utcnow())
            s.add(run)
            s.flush()
            s.add(Artifact(workspace_id=ws.id, outcome_id=outcome.id, task_id=task.id, run_id=run.id,
                           kind="report", name="Verification evidence", content="pytest: 3 passed; all criteria verified"))
        ws.outcome_seq = len(STATUSES)
        decisions = {}
        for kind in KINDS:
            state = {"approve_plan": "awaiting_approval", "approve_merge": "running", "question": "needs_input",
                     "unblock": "blocked", "budget": "attention", "approve_release": "completed"}[kind]
            options = [{"value": "approve", "label": "Approve"}, {"value": "reject", "label": "Reject"}]
            if kind == "question":
                options = [{"value": "answer", "label": "Answer"}, {"value": "cancel", "label": "Cancel"}]
            elif kind in ("unblock", "budget"):
                options = [{"value": "retry", "label": "Retry"}, {"value": "abandon", "label": "Abandon"}]
            elif kind == "approve_plan":
                options = [{"value": "approve", "label": "Approve"}, {"value": "replan", "label": "Revise"},
                           {"value": "cancel", "label": "Cancel"}]
            decision = Decision(workspace_id=ws.id, outcome_id=outcomes[state],
                                task_id=tasks[state] if kind in ("approve_merge", "budget", "unblock") else None, kind=kind,
                                title=f"Browser {kind}", body="Choose how this seeded work should proceed.", options=options)
            s.add(decision)
            s.flush()
            decisions[kind] = decision.id
        feedback = Signal(workspace_id=ws.id, repo_id=repo.id, source="feedback", fingerprint="browser-feedback",
                          title="Make empty states clearer", body="Explain what to do next.")
        improvement = Signal(workspace_id=ws.id, repo_id=repo.id, source="improvement", fingerprint="browser-team-note",
                             title="Teach builders to test empty states", body="Edit this team note before applying.",
                             status="proposed", data={"rule": "review_themes", "action": {"type": "team_note",
                             "repo_id": repo.id, "note": "Test empty states before shipping.", "label": "Add to team notes"},
                             "evidence": [{"label": "Verified task", "detail": "Missing empty-state coverage",
                                           "outcome_id": outcomes["completed"], "task_id": tasks["completed"]}]})
        s.add_all([feedback, improvement])
        s.flush()
        return {"email": email, "name": ws.name, "workspace_id": ws.id, "repo_id": repo.id, "outcomes": outcomes,
                "plans": plans, "tasks": tasks, "decisions": decisions, "improvement_id": improvement.id,
                "feedback_id": feedback.id}
