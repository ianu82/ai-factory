"""The Message Log reads the event journal as messages between agents and people."""

from __future__ import annotations

from aifactory.db import SessionLocal
from aifactory.events import record
from aifactory.messages import message_data, party
from aifactory.models import Outcome, Plan, Repository, Task, User


def world(signed_in):
    ws = signed_in["workspaces"][0]["id"]
    user = signed_in["user"]["id"]
    with SessionLocal() as db:
        repo = Repository(workspace_id=ws, full_name="org/app")
        outcome = Outcome(workspace_id=ws, number=53, title="Supervisor")
        db.add_all([repo, outcome]); db.flush()
        plan = Plan(outcome_id=outcome.id, content={})
        db.add(plan); db.flush()
        task = Task(workspace_id=ws, outcome_id=outcome.id, plan_id=plan.id, repo_id=repo.id, key="T1", title="Engine",
                    status="running", pr_number=402)
        db.add(task); db.commit()
        return ws, user, outcome.id, task.id


def messages(client, ws, **params):
    response = client.get(f"/api/workspaces/{ws}/messages", params=params)
    assert response.status_code == 200, response.text
    return response.json()


def test_recorded_events_read_as_messages_between_parties(client, signed_in):
    ws, user, outcome, task = world(signed_in)
    record(ws, "plan_proposed", "Plan v1: 3 task(s)", outcome_id=outcome, data={"plan_id": "p"})
    record(ws, "stage", "Checking the candidate change", outcome_id=outcome, task_id=task)  # activity, not a message
    record(ws, "verification_failed", "Verification failed: 2 findings", outcome_id=outcome, task_id=task)
    record(ws, "guidance", "Ian via Mind: use the cached client", outcome_id=outcome, actor=user,
           data={"intervention": True, "source": "mind", "action_id": "a1"})
    record(ws, "decision_resolved", "Ian: retry — T1 did not pass", outcome_id=outcome, task_id=task, actor=user,
           data={"decision_id": "d1", "resolution": "retry"})
    record(ws, "operator_relay", "Operator relay from Ian: resolve #53 T1 retry: applied", actor="operator")
    record(ws, "message", "Main is red; holding T1's merge turn", outcome_id=outcome, task_id=task,
           data=message_data(party("merge_manager", "Merge Manager"), party("mind", "Ian's Mind", "Ian"), "inform"))
    record(ws, "message", "malformed", data={"message": {"from": "Supervisor", "to": {}, "intent": "shout"}})

    items = messages(client, ws)["items"]
    said = [(m["from"]["label"], m["to"]["label"], m["intent"]) for m in items]
    assert said == [
        ("Merge Manager", "Ian's Mind", "inform"),
        ("Operator", "Factory", "request"),
        ("Ian", "Factory", "answer"),
        ("Ian's Mind", "Planning, builders and verifiers", "inform"),
        ("Verifier · T1", "Builder · T1", "answer"),
        ("Planner", "Factory", "propose"),
    ]
    mind = items[3]
    assert mind["from"] == {"role": "mind", "label": "Ian's Mind", "for": "Ian"}
    failed = items[4]
    assert failed["says"] == "Verification failed: 2 findings"
    assert failed["about"] == {"task": {"id": task, "key": "T1"}, "pr": 402,
                               "outcome": {"id": outcome, "number": 53, "title": "Supervisor"}}
    assert items[0]["to"]["for"] == "Ian"


def test_a_plan_worked_out_with_a_mind_is_handed_to_the_factory(client, signed_in):
    ws, user, outcome, _ = world(signed_in)
    record(ws, "plan_proposed", "Plan v1: 2 task(s)", outcome_id=outcome, actor=user,
           data={"plan_id": "p", "source": "mind", "action_id": "a1"})
    record(ws, "plan_approved", "Plan approved by Ian via Mind", outcome_id=outcome, actor=user,
           data={"plan_id": "p", "source": "mind", "action_id": "a1"})
    said = [(m["from"]["label"], m["to"]["label"], m["intent"]) for m in messages(client, ws)["items"]]
    assert said == [("Ian's Mind", "Factory", "handoff")]  # the approval is part of the hand-off


def test_each_plan_review_or_approval_is_one_answer(client, signed_in):
    ws, user, outcome, _ = world(signed_in)
    # A review that never ran is not an answer from the reviewer.
    record(ws, "plan_reviewed", "Plan v1 not reviewed by the plan reviewer: over budget", outcome_id=outcome,
           data={"verdict": "error"})
    # An automated approval: the review, then the transition it causes.
    record(ws, "plan_reviewed", "Plan v2 approved by the plan reviewer: covers the outcome", outcome_id=outcome,
           data={"verdict": "approve"})
    record(ws, "plan_approved", "Plan approved by the plan reviewer (opus-5-5)", outcome_id=outcome, actor="policy")
    # A person's approval: the transition, then their decision.
    record(ws, "plan_approved", "Plan approved by ian", outcome_id=outcome, actor=user, data={"plan_id": "p"})
    record(ws, "decision_resolved", "ian: approve — Approve plan v3", outcome_id=outcome, actor=user,
           data={"decision_id": "d1", "resolution": "approve"})
    said = [(m["kind"], m["from"]["role"], m["to"]["role"]) for m in messages(client, ws)["items"]]
    assert said == [("decision_resolved", "person", "factory"), ("plan_reviewed", "plan_reviewer", "planner")]


def test_an_operator_decision_is_one_answer_from_the_operator(client, signed_in):
    ws, _, outcome, task = world(signed_in)
    with SessionLocal() as db:
        operator = User(email="operator+abc@factory.local", name="Operator (delegated by Ian)", is_system=True)
        db.add(operator); db.commit()
        operator_id = operator.id
    # A relayed resolve records the decision as the operator's system user, then the relay's own receipt.
    record(ws, "decision_resolved", "Operator (delegated by Ian): retry — T1 did not pass", outcome_id=outcome,
           task_id=task, actor=operator_id, data={"decision_id": "d1", "resolution": "retry"})
    record(ws, "operator_decision", "Operator (delegated by Ian) resolved 'T1 did not pass': retry",
           outcome_id=outcome, actor="operator", data={"nonce": "n1", "decision_id": "d1"})
    items = messages(client, ws)["items"]
    assert [(m["kind"], m["from"]["label"], m["to"]["label"], m["intent"]) for m in items] == [
        ("decision_resolved", "Operator", "Factory", "answer")]
    assert items[0]["about"]["task"] == {"id": task, "key": "T1"}


def test_a_takeover_reads_once_as_the_factory_handing_the_task_over(client, signed_in):
    ws, user, outcome, task = world(signed_in)
    # From a decision: the answer, then the hand-over it causes.
    record(ws, "decision_resolved", "ian: take_over — T1 did not pass", outcome_id=outcome, task_id=task, actor=user,
           data={"decision_id": "d1", "resolution": "take_over"})
    record(ws, "task_handed_over", "ian took over T1; the Factory stopped working on it", outcome_id=outcome,
           task_id=task, actor=user)
    # Through a Mind: the hand-over alone.
    record(ws, "task_handed_over", "ian via Mind took over T1; the Factory stopped working on it", outcome_id=outcome,
           task_id=task, actor=user, data={"source": "mind", "action_id": "a1"})
    with SessionLocal() as db:
        db.get(Task, task).status = "handed_over"
        db.commit()
    said = [(m["kind"], m["from"]["label"], m["to"]["label"], m["intent"]) for m in messages(client, ws)["items"]]
    assert said == [("task_handed_over", "Factory · T1", "Ian", "handoff"),
                    ("task_handed_over", "Factory · T1", "Ian", "handoff")]


def test_a_takeover_that_could_not_complete_keeps_its_answer(client, signed_in):
    ws, user, outcome, task = world(signed_in)  # the task finished first, so no hand-over was recorded
    record(ws, "decision_resolved", "Ian: take_over — T1 did not pass", outcome_id=outcome, task_id=task, actor=user,
           data={"decision_id": "d1", "resolution": "take_over"})
    said = [(m["kind"], m["from"]["label"], m["to"]["label"]) for m in messages(client, ws)["items"]]
    assert said == [("decision_resolved", "Ian", "Factory")]


def test_a_decision_answered_in_words_shows_the_answer(client, signed_in):
    ws, user, outcome, task = world(signed_in)
    # The event's message names the decision (its question); what the person said is in the comment.
    record(ws, "decision_resolved", "Ian: answer — Which cache should T1 use?", outcome_id=outcome, task_id=task,
           actor=user, data={"decision_id": "d1", "resolution": "answer", "comment": "  The shared Redis client. "})
    record(ws, "decision_resolved", "Ian: retry — T1 did not pass", outcome_id=outcome, task_id=task, actor=user,
           data={"decision_id": "d2", "resolution": "retry", "comment": ""})
    said = [m["says"] for m in messages(client, ws)["items"]]
    assert said == ["Ian: retry — T1 did not pass", "answer: The shared Redis client."]


def test_the_lead_speaks_as_the_lead(client, signed_in):
    ws, _, outcome, task = world(signed_in)
    with SessionLocal() as db:
        lead = User(email="lead@factory.local", name="Lead", is_system=True)
        db.add(lead); db.commit()
        lead_id = lead.id
    source = {"source": "lead", "run_id": "r1"}
    record(ws, "decision_resolved", "Lead: answer — Which cache?", outcome_id=outcome, task_id=task, actor=lead_id,
           data={"decision_id": "d1", "resolution": "answer", **source})
    record(ws, "guidance", "Lead: use the cached client", outcome_id=outcome, task_id=task, actor=lead_id, data=source)
    record(ws, "lead_note", "Lead: approve; CI is green and every finding is fixed", outcome_id=outcome,
           actor=lead_id, data={**source, "decision_id": "d2"})
    record(ws, "lead_note", "Lead: waiting on CI", outcome_id=outcome, actor=lead_id, data=source)
    record(ws, "lead_update", "Lead: T1 is verified; T2 starts now", outcome_id=outcome, actor=lead_id,
           data={**source, "kind": "update"})  # its post in the outcome's conversation
    said = [(m["kind"], m["from"]["label"], m["to"]["label"], m["intent"]) for m in messages(client, ws)["items"]]
    assert said == [("lead_update", "Lead", "People", "inform"), ("lead_note", "Lead", "People", "inform"),
                    ("lead_note", "Lead", "People", "propose"),
                    ("guidance", "Lead", "Builder and verifier · T1", "inform"),
                    ("decision_resolved", "Lead", "Factory", "answer")]
    assert len(messages(client, ws, party="lead")["items"]) == 5


def test_filters_by_party_intent_and_outcome(client, signed_in):
    ws, user, outcome, task = world(signed_in)
    record(ws, "verified", "Verified", outcome_id=outcome, task_id=task)
    record(ws, "guidance", "Ian via Mind: ship it", outcome_id=outcome, actor=user, data={"source": "mind"})
    record(ws, "operator_retry", "Operator retried T2", actor="operator")
    record(ws, "decision_requested", "T1 needs a decision", outcome_id=outcome, task_id=task, data={"kind": "question"})

    assert [m["from"]["role"] for m in messages(client, ws, party="minds")["items"]] == ["mind"]
    # Guidance for the whole outcome is read by planning, builders and verifiers, so it shows under each of them.
    assert [m["kind"] for m in messages(client, ws, party="verifier")["items"]] == ["guidance", "verified"]
    assert [m["kind"] for m in messages(client, ws, party="planning")["items"]] == ["guidance"]
    assert [m["kind"] for m in messages(client, ws, party="builders")["items"]] == ["guidance", "verified"]
    # A party matches as sender or receiver: People receive the decision request; the operator sends a retry.
    assert {m["kind"] for m in messages(client, ws, party="people")["items"]} == {"decision_requested", "operator_retry"}
    assert [m["kind"] for m in messages(client, ws, intent="request")["items"]] == ["decision_requested", "operator_retry"]
    assert [m["kind"] for m in messages(client, ws, outcome_id=outcome)["items"]] == ["decision_requested", "guidance",
                                                                                   "verified"]


def test_pages_continue_where_the_last_left_off(client, signed_in):
    ws, *_ = world(signed_in)
    for n in range(5):
        record(ws, "operator_retry", f"retry {n}", actor="operator")
    first = messages(client, ws, limit=2)
    second = messages(client, ws, limit=2, before=first["next_cursor"])
    third = messages(client, ws, limit=2, before=second["next_cursor"])
    said = [m["says"] for page in (first, second, third) for m in page["items"]]
    assert said == ["retry 4", "retry 3", "retry 2", "retry 1", "retry 0"]


def test_validation_and_membership(client, signed_in):
    ws, *_ = world(signed_in)
    assert client.get(f"/api/workspaces/{ws}/messages", params={"party": "everyone"}).status_code == 422
    assert client.get(f"/api/workspaces/{ws}/messages", params={"intent": "shout"}).status_code == 422
    client.post("/auth/dev", json={"email": "eve@example.com", "name": "Eve", "organization": "Elsewhere"})
    assert client.get(f"/api/workspaces/{ws}/messages").status_code in (403, 404)
