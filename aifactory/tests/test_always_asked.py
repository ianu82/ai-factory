"""Whatever waits on a person has a decision to answer: tasks behind work that ended undelivered, a task left
waiting with nothing asking, a run that ended without saying how, and planning that cannot start."""
from __future__ import annotations

import asyncio

import pytest

import aifactory.pipeline.orchestrator as scheduler
from aifactory.db import session_scope
from aifactory.decisions import DecisionError, resolve
from aifactory.models import Decision, Event, Message, Outcome, Plan, Repository, Task, User
from aifactory.pipeline import planning
from aifactory.pipeline.lifecycle import dependents
from aifactory.pipeline.orchestrator import Orchestrator
from aifactory.work_services import cancel_task

from .fakes import ScriptedModel, call, make_remote
from .test_pipeline import CRITERION, FILES, factory, new_outcome, outcome_status  # noqa: F401 - the workspace fixture

RETRY_OR_ABANDON = ["retry", "abandon"]


def outcome_with(ids, *tasks, status="running"):
    """An outcome with these (key, status, depends_on) tasks. Returns its id and the task ids by key."""
    oid = new_outcome(ids)
    made = {}
    with session_scope() as s:
        s.get(Outcome, oid).status = status
        plan = Plan(outcome_id=oid)
        s.add(plan)
        s.flush()
        for key, state, deps in tasks:
            task = Task(workspace_id=ids["ws"], outcome_id=oid, plan_id=plan.id, repo_id=ids["repo"], key=key,
                        title=f"Part {key}", status=state, depends_on=deps)
            s.add(task)
            s.flush()
            made[key] = task.id
    return oid, made


def asked(oid, status="open"):
    """The outcome's decisions in this status, oldest first."""
    with session_scope() as s:
        return [{"id": d.id, "kind": d.kind, "task_id": d.task_id, "title": d.title, "body": d.body,
                 "options": [o["value"] for o in d.options or []], "metadata": d.metadata_json, "comment": d.comment}
                for d in s.query(Decision).filter(Decision.outcome_id == oid, Decision.status == status)
                .order_by(Decision.created_at, Decision.id)]


def count(oid, kind):
    with session_scope() as s:
        return s.query(Event).filter(Event.outcome_id == oid, Event.kind == kind).count()


def tasks_of(oid):
    with session_scope() as s:
        return {t.key: (t.status, list(t.depends_on or [])) for t in s.query(Task).filter(Task.outcome_id == oid)}


def answer(ids, decision_id, resolution, comment=""):
    with session_scope() as s:
        resolve(s, s.get(Decision, decision_id), s.get(User, ids["user"]), resolution, comment)


def decide(ids, oid, *, kind="unblock", task_id=None, options=("retry", "abandon")):
    with session_scope() as s:
        d = Decision(workspace_id=ids["ws"], outcome_id=oid, task_id=task_id, kind=kind, title="What now?",
                     options=[{"value": o, "label": o.title()} for o in options])
        s.add(d)
        s.flush()
        return d.id


# Tasks behind work that ended without being delivered ------------------------------------------------------------

@pytest.mark.parametrize("ended, how", [("abandoned", "was abandoned"), ("cancelled", "was cancelled"),
                                        ("reverted", "was merged and then reverted"),
                                        ("handed_over", "was handed over to a person")])
def test_tasks_behind_an_ended_task_get_one_decision_for_the_outcome(factory, ended, how):
    oid, _ = outcome_with(factory, ("T1", ended, []), ("T2", "pending", ["T1"]), ("T3", "pending", ["T2"]),
                          ("T4", "completed", []))
    orch = Orchestrator()
    orch._promote()
    orch._promote()  # asked once, however often the scheduler passes
    [decision] = asked(oid)
    assert decision["kind"] == "unblock" and decision["task_id"] is None
    assert decision["options"] == ["continue", "drop"]
    with session_scope() as s:  # each option says what it does
        assert s.get(Decision, decision["id"]).options == scheduler.STRANDED_OPTIONS
    assert all(o["effect"] for o in scheduler.STRANDED_OPTIONS)
    assert decision["metadata"] == {"stranded": {"waiting": ["T2", "T3"], "ended": ["T1"]}}
    assert decision["title"] == "T2, T3 cannot start: T1 ended without being delivered"
    assert "- T2 (Part T2) waits for T1." in decision["body"] and "- T3 (Part T3) waits for T2." in decision["body"]
    assert f"- T1 (Part T1) {how}." in decision["body"]
    assert outcome_status(oid) == "attention"
    assert count(oid, "tasks_stranded") == 1 and count(oid, "decision_requested") == 1
    assert tasks_of(oid)["T2"] == ("pending", ["T1"])  # nothing moves until a person answers


def test_a_task_behind_one_that_waits_on_a_person_is_not_stranded(factory):
    oid, ids = outcome_with(factory, ("T1", "blocked", []), ("T2", "pending", ["T1"]), status="attention")
    waiting = decide(factory, oid, task_id=ids["T1"])
    Orchestrator()._promote()
    assert [d["id"] for d in asked(oid)] == [waiting]  # answering T1's decision is what moves T2
    assert count(oid, "tasks_stranded") == 0


def test_the_stranded_decision_follows_the_tasks(factory):
    oid, ids = outcome_with(factory, ("T1", "abandoned", []), ("T2", "pending", ["T1"]), ("T3", "running", []))
    orch = Orchestrator()
    orch._promote()
    [first] = asked(oid)
    assert first["metadata"]["stranded"] == {"waiting": ["T2"], "ended": ["T1"]}
    with session_scope() as s:  # another task ends, with one more behind it
        s.get(Task, ids["T3"]).status = "cancelled"
        task = s.get(Task, ids["T2"])
        s.add(Task(workspace_id=task.workspace_id, outcome_id=oid, plan_id=task.plan_id, repo_id=task.repo_id,
                   key="T10", title="Part T10", status="pending", depends_on=["T3"]))
    orch._promote()
    [grown] = asked(oid)
    assert grown["id"] == first["id"]  # the same question, kept current
    assert grown["metadata"]["stranded"] == {"waiting": ["T2", "T10"], "ended": ["T1", "T3"]}
    assert grown["title"] == "T2, T10 cannot start: T1, T3 ended without being delivered"
    assert count(oid, "tasks_stranded") == 1 and count(oid, "decision_requested") == 1
    with session_scope() as s:  # the waiting tasks are dropped some other way: nothing is left to ask
        for t in s.query(Task).filter(Task.outcome_id == oid, Task.status == "pending"):
            t.status = "cancelled"
    orch._promote()
    assert asked(oid) == []
    [closed] = asked(oid, "superseded")
    assert closed["id"] == first["id"] and closed["comment"] == "Nothing waits behind an ended task any more."


def test_starting_the_waiting_tasks_waives_what_ended(factory):
    oid, ids = outcome_with(factory, ("T1", "handed_over", []), ("T2", "pending", ["T1"]), ("T3", "pending", ["T2"]),
                            ("T4", "completed", []), ("T5", "pending", ["T1", "T4"]))
    orch = Orchestrator()
    orch._promote()
    [decision] = asked(oid)
    assert decision["metadata"]["stranded"] == {"waiting": ["T2", "T3", "T5"], "ended": ["T1"]}
    answer(factory, decision["id"], "continue", "Pull request 12 was finished and merged by hand.")
    assert tasks_of(oid) == {"T1": ("handed_over", []), "T2": ("pending", []), "T3": ("pending", ["T2"]),
                             "T4": ("completed", []), "T5": ("pending", ["T4"])}
    with session_scope() as s:
        assert s.get(Task, ids["T2"]).evidence["waived_deps"] == ["T1"]
        assert s.get(Task, ids["T5"]).evidence["waived_deps"] == ["T1"]
        assert "waived_deps" not in (s.get(Task, ids["T3"]).evidence or {})  # it still waits for T2, which will run
        [note] = s.query(Message).filter(Message.outcome_id == oid, Message.author_kind == "user").all()
        assert note.body == "Pull request 12 was finished and merged by hand." and note.task_id is None
    orch._promote()
    assert {k: v[0] for k, v in tasks_of(oid).items()} == {
        "T1": "handed_over", "T2": "queued", "T3": "pending", "T4": "completed", "T5": "queued"}
    assert outcome_status(oid) == "running" and asked(oid) == []
    assert count(oid, "tasks_stranded") == 1  # answered once, not asked again


def test_dropping_the_waiting_tasks_ends_the_outcome_with_what_was_delivered(factory):
    oid, _ = outcome_with(factory, ("T1", "abandoned", []), ("T2", "pending", ["T1"]), ("T3", "pending", ["T2"]),
                          ("T4", "completed", []))
    orch = Orchestrator()
    orch._promote()
    [decision] = asked(oid)
    answer(factory, decision["id"], "drop")
    assert {k: v[0] for k, v in tasks_of(oid).items()} == {
        "T1": "abandoned", "T2": "abandoned", "T3": "abandoned", "T4": "completed"}
    orch._promote()
    assert outcome_status(oid) == "partially_completed" and asked(oid) == []
    with session_scope() as s:
        assert s.get(Outcome, oid).summary == "1 of 4 task(s) delivered."


# A cancel says what it strands ------------------------------------------------------------------------------------

@pytest.fixture
def person(signed_in):
    """The signed-in person's workspace with a repository, in the shape outcome_with takes."""
    ws = signed_in["workspaces"][0]["id"]
    with session_scope() as s:
        repo = Repository(workspace_id=ws, full_name="acme/demo", default_branch="main")
        s.add(repo)
        s.flush()
        return {"ws": ws, "repo": repo.id, "user": signed_in["user"]["id"]}


def test_the_tasks_waiting_for_one_are_found_through_others_and_finished_ones_left_out():
    def t(key, status, deps):
        return Task(key=key, title=f"Part {key}", status=status, depends_on=deps)

    tasks = [t("T1", "running", []), t("T2", "pending", ["T1"]), t("T3", "pending", ["T2"]),
             t("T4", "completed", ["T1"]), t("T5", "pending", []), t("T6", "pending", ["T5", "T3"]),
             t("T7", "cancelled", ["T1"]), t("T10", "pending", ["T1"])]
    assert [w.key for w in dependents(tasks, "T1")] == ["T2", "T3", "T6", "T10"]  # directly, or through T2 and T3
    assert [w.key for w in dependents(tasks, "T5")] == ["T6"]
    assert dependents(tasks, "T6") == []
    assert dependents(tasks, "T4") == dependents(tasks, "T7") == []  # finished: cancelling it strands nothing new


def test_a_cancel_shows_what_waits_and_can_drop_it_in_the_same_step(person, client):
    oid, ids = outcome_with(person, ("T1", "running", []), ("T2", "pending", ["T1"]), ("T3", "pending", ["T2"]),
                            ("T4", "completed", []), ("T5", "pending", []))
    detail = client.get(f"/api/workspaces/{person['ws']}/outcomes/{oid}").json()
    assert {t["key"]: [w["key"] for w in t["dependents"]] for t in detail["tasks"]} == {
        "T1": ["T2", "T3"], "T2": ["T3"], "T3": [], "T4": [], "T5": []}
    assert detail["tasks"][0]["dependents"][0] == {"id": ids["T2"], "key": "T2", "title": "Part T2"}
    r = client.post(f"/api/workspaces/{person['ws']}/tasks/{ids['T1']}/cancel", json={"dependents": "drop"})
    assert r.status_code == 200, r.text
    assert r.json() == {"ok": True, "dropped": [ids["T2"], ids["T3"]]}
    assert {k: v[0] for k, v in tasks_of(oid).items()} == {
        "T1": "cancelled", "T2": "cancelled", "T3": "cancelled", "T4": "completed", "T5": "pending"}
    with session_scope() as s:  # one event each, saying why
        said = {e.task_id: (e.message, (e.data or {}).get("with")) for e in s.query(Event).filter(
            Event.outcome_id == oid, Event.kind == "task_cancelled")}
    assert said == {ids["T1"]: ("Ian stopped T1", None),
                     ids["T2"]: ("Ian dropped T2: it waits for T1, which was cancelled", "T1"),
                     ids["T3"]: ("Ian dropped T3: it waits for T1, which was cancelled", "T1")}
    bad = client.post(f"/api/workspaces/{person['ws']}/tasks/{ids['T5']}/cancel", json={"dependents": "all"})
    assert bad.status_code == 422 and tasks_of(oid)["T5"][0] == "pending"


def test_a_cancel_that_keeps_what_waits_behaves_as_before(person, client):
    oid, ids = outcome_with(person, ("T1", "running", []), ("T2", "pending", ["T1"]))
    r = client.post(f"/api/workspaces/{person['ws']}/tasks/{ids['T1']}/cancel")  # no choice: keep
    assert r.status_code == 200 and r.json() == {"ok": True, "dropped": []}
    assert tasks_of(oid) == {"T1": ("cancelled", []), "T2": ("pending", ["T1"])}
    assert count(oid, "task_cancelled") == 1


def test_kept_tasks_ask_what_to_do_once_their_task_is_cancelled(factory):
    oid, ids = outcome_with(factory, ("T1", "running", []), ("T2", "pending", ["T1"]))
    with session_scope() as s:
        payloads, later = cancel_task(s, s.get(User, factory["user"]), factory["ws"], ids["T1"])
        assert len(payloads) == 1 and later == [("stop_task", ids["T1"], factory["ws"], oid)]
    Orchestrator()._promote()
    [decision] = asked(oid)
    assert decision["metadata"] == {"stranded": {"waiting": ["T2"], "ended": ["T1"]}}


# Decisions with one cause are answered in one go ------------------------------------------------------------------

def stranded_question(ids, oid, title, *, status="open", options=None, metadata=None, kind="unblock"):
    with session_scope() as s:
        d = Decision(workspace_id=ids["ws"], outcome_id=oid, kind=kind, title=title, status=status,
                     options=options if options is not None else scheduler.STRANDED_OPTIONS,
                     metadata_json=metadata if metadata is not None else {"stranded": {}})
        s.add(d)
        s.flush()
        return d.id


def test_the_inbox_keys_the_same_question_about_different_work_alike(person, client):
    first, _ = outcome_with(person, ("T1", "cancelled", []), ("T2", "pending", ["T1"]), ("T3", "pending", ["T1"]),
                            status="attention")
    second, _ = outcome_with(person, ("T1", "cancelled", []), ("T2", "pending", ["T1"]), status="attention")
    a = stranded_question(person, first, "T2, T3 cannot start: T1 ended without being delivered")
    b = stranded_question(person, second, "T2 cannot start: T1 ended without being delivered")
    other = stranded_question(person, second, "T2 cannot start: T1 ended without being delivered",
                              options=[{"value": "retry", "label": "Retry"}, {"value": "abandon", "label": "Abandon"}])
    words = stranded_question(person, first, "Which queue?", kind="question",
                              options=[{"value": "answer", "label": "Answer"}])
    words_too = stranded_question(person, second, "Which queue?", kind="question",
                                  options=[{"value": "answer", "label": "Answer"}])
    keys = {d["id"]: d["question"] for d in client.get(f"/api/workspaces/{person['ws']}/inbox").json()["decisions"]}
    shape = "T# cannot start: T# ended without being delivered"  # the task keys left out
    assert keys[a] == keys[b] == f'["unblock", ["continue", "drop"], "{shape}"]'
    assert keys[other] not in (None, keys[a])  # another set of answers is another question
    assert keys[words] is None and keys[words_too] is None  # answered in words: never grouped


def test_a_group_is_answered_one_decision_at_a_time_and_each_reports_how_it_went(person, client):
    outcomes = [outcome_with(person, ("T1", "cancelled", []), ("T2", "pending", ["T1"]), status="attention")
                for _ in range(3)]
    title = "T2 cannot start: T1 ended without being delivered"
    first = stranded_question(person, outcomes[0][0], title)
    done = stranded_question(person, outcomes[1][0], title, status="resolved")
    refused = stranded_question(person, outcomes[1][0], title, options=[{"value": "retry", "label": "Retry"}])
    for_people = stranded_question(person, outcomes[2][0], title, metadata={"stranded": {}, "people_only": "x"})
    r = client.post(f"/api/workspaces/{person['ws']}/decisions/resolve", json={
        "decision_ids": [first, done, refused, "missing", for_people], "resolution": "drop"})
    assert r.status_code == 200, r.text
    assert [(x["id"], x["status"], x.get("reason")) for x in r.json()["results"]] == [
        (first, "resolved", None), (done, "already_resolved", None), (refused, "refused", "Choose one of: retry"),
        ("missing", "refused", "Decision not found"), (for_people, "resolved", None)]
    with session_scope() as s:
        for decision_id in (first, for_people):  # each its own record and event, attributed to the person
            d = s.get(Decision, decision_id)
            assert (d.status, d.resolution, d.resolved_by) == ("resolved", "drop", person["user"])
            assert s.query(Event).filter(Event.kind == "decision_resolved",
                                         Event.outcome_id == d.outcome_id).one().data["decision_id"] == decision_id
        assert s.get(Decision, refused).status == "open"
    assert tasks_of(outcomes[0][0])["T2"][0] == tasks_of(outcomes[2][0])["T2"][0] == "abandoned"
    assert tasks_of(outcomes[1][0])["T2"][0] == "pending"


# A task left waiting on a person with nothing asking -------------------------------------------------------------

def test_a_task_waiting_with_no_decision_gets_one_after_a_grace(factory, monkeypatch):
    oid, ids = outcome_with(factory, ("T1", "blocked", []), status="attention")
    with session_scope() as s:
        s.get(Task, ids["T1"]).blocked_reason = "Could not push the branch"
    orch = Orchestrator()
    orch._promote()
    assert asked(oid) == []  # whoever set the status may still be writing its own decision
    monkeypatch.setattr(scheduler, "UNASKED_AFTER", 0)
    orch._promote()
    orch._promote()  # once it asks, it does not ask again
    [decision] = asked(oid)
    assert decision["kind"] == "unblock" and decision["task_id"] == ids["T1"]
    assert decision["options"] == RETRY_OR_ABANDON and decision["metadata"] == {"unasked": "blocked"}
    assert decision["title"] == "T1 is blocked, and nothing was asking what to do"
    assert "Recorded reason: Could not push the branch" in decision["body"]
    assert count(oid, "decision_missing") == 1 and count(oid, "decision_requested") == 1
    answer(factory, decision["id"], "retry")
    assert tasks_of(oid)["T1"][0] == "queued"


def merged(task_id, stage, **evidence):
    with session_scope() as s:
        task = s.get(Task, task_id)
        task.merge_sha, task.stage, task.evidence = "a" * 40, stage, evidence


def test_a_merged_task_left_with_failed_checks_is_asked_what_a_failed_smoke_asks(factory, monkeypatch):
    monkeypatch.setattr(scheduler, "UNASKED_AFTER", 0)
    oid, ids = outcome_with(factory, ("T1", "smoke_failed", []), status="attention")
    merged(ids["T1"], "smoke")
    Orchestrator()._promote()
    [decision] = asked(oid)
    assert decision["task_id"] == ids["T1"] and decision["options"] == ["revert", "fix", "acknowledge"]
    assert decision["title"] == ("T1 was merged, and its checks after the merge failed, "
                                 "and nothing was asking what to do")


@pytest.mark.parametrize("stage, evidence", [("revert", {}), ("revert_merge", {"revert_candidate": {"pr": 7}}),
                                             ("smoke", {"revert_candidate": {"pr": 7}})])
def test_a_revert_that_did_not_go_through_is_asked_about_with_answers_that_can_end_it(factory, monkeypatch, stage,
                                                                                    evidence):
    # Accepting cannot complete a task whose revert is under way, so the sweep does not offer it: an acknowledgement
    # would leave the task where it is, to be asked about again.
    monkeypatch.setattr(scheduler, "UNASKED_AFTER", 0)
    oid, ids = outcome_with(factory, ("T1", "smoke_failed", []), status="attention")
    merged(ids["T1"], stage, **evidence)
    Orchestrator()._promote()
    [decision] = asked(oid)
    assert decision["task_id"] == ids["T1"] and decision["options"] == ["revert", "abandon"]
    assert decision["title"] == "T1 was merged, and reverting it did not go through, and nothing was asking what to do"
    with pytest.raises(DecisionError):
        answer(factory, decision["id"], "acknowledge")
    answer(factory, decision["id"], "revert")
    with session_scope() as s:
        task = s.get(Task, ids["T1"])
        assert (task.status, task.stage) == ("queued", "revert")


def test_a_supervisor_finding_about_a_task_is_not_its_decision(factory, monkeypatch):
    # Acknowledging or dismissing the finding moves nothing: the task still needs something to answer.
    monkeypatch.setattr(scheduler, "UNASKED_AFTER", 0)
    oid, ids = outcome_with(factory, ("T1", "blocked", []), status="attention")
    finding = decide(factory, oid, kind="supervisor", task_id=ids["T1"], options=("acknowledge", "dismiss"))
    Orchestrator()._promote()
    opened = [d for d in asked(oid) if d["id"] != finding]
    assert [(d["kind"], d["task_id"], d["options"]) for d in opened] == [("unblock", ids["T1"], RETRY_OR_ABANDON)]
    Orchestrator()._promote()  # and only once
    assert len(asked(oid)) == 2 and count(oid, "decision_missing") == 1


@pytest.mark.parametrize("kind, swept", [("budget", False), ("supervisor", True)])
def test_a_question_about_the_whole_outcome_covers_its_tasks(factory, monkeypatch, kind, swept):
    # A budget question covers the task it stopped. A supervisor's finding does not: acknowledging it moves nothing.
    monkeypatch.setattr(scheduler, "UNASKED_AFTER", 0)
    oid, ids = outcome_with(factory, ("T1", "blocked", []), status="attention")
    decide(factory, oid, kind=kind, options=("acknowledge",))
    Orchestrator()._promote()
    assert [d["task_id"] for d in asked(oid) if d["task_id"]] == ([ids["T1"]] if swept else [])


def test_a_task_that_is_already_asked_about_is_left_alone(factory, monkeypatch):
    monkeypatch.setattr(scheduler, "UNASKED_AFTER", 0)
    oid, ids = outcome_with(factory, ("T1", "needs_input", []), ("T2", "awaiting_approval", []), status="attention")
    question = decide(factory, oid, kind="question", task_id=ids["T1"], options=("answer",))
    approval = decide(factory, oid, kind="approve_merge", task_id=ids["T2"], options=("approve", "reject"))
    Orchestrator()._promote()
    assert sorted(d["id"] for d in asked(oid)) == sorted([question, approval])
    assert count(oid, "decision_missing") == 0


def test_a_decision_another_instance_just_opened_is_not_opened_twice(factory, monkeypatch):
    # Two instances' passes can both read the open decisions before either writes one. Whichever writes second reads
    # them again under the workspace's lock, and finds what the first opened.
    monkeypatch.setattr(scheduler, "UNASKED_AFTER", 0)
    oid, ids = outcome_with(factory, ("T1", "abandoned", []), ("T2", "pending", ["T1"]), ("T3", "blocked", []),
                            status="attention")
    Orchestrator()._promote()  # the first instance: one decision for T2 behind T1, one for T3
    first = asked(oid)
    assert sorted(d["task_id"] or "" for d in first) == ["", ids["T3"]]
    with session_scope() as s:  # the second instance, with the (empty) list it read before the first wrote
        tasks = s.query(Task).filter(Task.outcome_id == oid).all()
        assert Orchestrator()._ask(s, s.get(Outcome, oid), tasks, [], set()) == []
    assert asked(oid) == first
    assert count(oid, "tasks_stranded") == 1 and count(oid, "decision_missing") == 1


def test_a_run_that_ends_without_saying_how_asks_what_to_do(factory, monkeypatch):
    class Silent:
        def __init__(self, *_args):
            pass

        async def run(self):
            return None

    monkeypatch.setattr(scheduler, "TaskRunner", Silent)
    oid, ids = outcome_with(factory, ("T1", "queued", []))

    async def go():
        orch = Orchestrator()
        orch._spawn_task(ids["T1"])
        job, _ = orch._tasks[ids["T1"]]
        await job

    asyncio.run(go())
    with session_scope() as s:
        task = s.get(Task, ids["T1"])
        assert task.status == "blocked" and task.blocked_reason == "The task stopped without recording an outcome."
        assert task.lease_owner == ""
    [decision] = asked(oid)  # written with the status, not left for the sweep
    assert decision["kind"] == "unblock" and decision["task_id"] == ids["T1"]
    assert decision["title"] == "T1 stopped without recording an outcome" and decision["options"] == RETRY_OR_ABANDON
    assert "ended at stage 'start'" in decision["body"]
    assert count(oid, "decision_requested") == 1


# Decisions cannot be closed without an answer --------------------------------------------------------------------

def test_a_decision_that_offers_choices_cannot_be_dismissed(factory):
    oid, ids = outcome_with(factory, ("T1", "blocked", []), status="attention")
    did = decide(factory, oid, task_id=ids["T1"])
    with pytest.raises(DecisionError, match="Choose one of: abandon, retry"):
        answer(factory, did, "dismiss")
    assert [d["id"] for d in asked(oid)] == [did] and tasks_of(oid)["T1"][0] == "blocked"


def test_a_decision_that_offers_nothing_can_be_dismissed(factory):
    oid, _ = outcome_with(factory, ("T1", "running", []))
    did = decide(factory, oid, kind="notice", options=())
    answer(factory, did, "dismiss")
    assert asked(oid) == [] and [d["id"] for d in asked(oid, "dismissed")] == [did]


# Planning that cannot start, or that planned for the wrong repositories ------------------------------------------

def test_planning_with_no_repositories_selected_asks_and_a_retry_plans_again(factory):
    oid = new_outcome(factory)
    with session_scope() as s:
        outcome = s.get(Outcome, oid)
        outcome.repo_ids, number = [], outcome.number
    asyncio.run(planning.plan_outcome(oid, asyncio.Event()))
    assert outcome_status(oid) == "blocked"
    [decision] = asked(oid)
    assert decision["kind"] == "unblock" and decision["task_id"] is None
    assert decision["title"] == f"#{number} has no repositories selected"
    assert decision["options"] == ["retry", "cancel"]
    with session_scope() as s:
        s.get(Outcome, oid).repo_ids = [factory["repo"]]
    answer(factory, decision["id"], "retry")
    assert outcome_status(oid) == "planning" and asked(oid) == []
    with session_scope() as s:
        assert s.get(Outcome, oid).blocked_reason == ""


def test_a_plan_for_repositories_the_outcome_did_not_select_asks(factory):
    make_remote(factory["remotes"], "acme/site", FILES)
    with session_scope() as s:
        site = Repository(workspace_id=factory["ws"], full_name="acme/site", default_branch="main",
                          config={"setup": [], "checks": [], "smoke": []})
        s.add(site)
        s.flush()
        site_id = site.id

    def planner(_messages):
        return {"content": "", "tool_calls": [call("finish", {
            "summary": "Change the greeting.",
            "tasks": [{"key": "T1", "title": "Update greeting", "repo": "acme/billing",
                       "spec": "Make greet.sh print Hello, Factory!", "acceptance": [CRITERION], "depends_on": []}],
            "questions": [], "risks": []})]}

    ScriptedModel.scripts["planner"] = planner
    oid = new_outcome(factory)
    with session_scope() as s:
        outcome = s.get(Outcome, oid)
        outcome.repo_ids, number = [factory["repo"], site_id], outcome.number
    asyncio.run(planning.plan_outcome(oid, asyncio.Event()))
    assert outcome_status(oid) == "blocked"
    [decision] = asked(oid)
    assert decision["title"] == f"The plan for #{number} names repositories it may not use"
    assert "acme/billing" in decision["body"] and decision["options"] == ["retry", "cancel"]
