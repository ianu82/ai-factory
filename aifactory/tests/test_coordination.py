"""Coordination between outcomes (overlapping work wakes both Leads) and team rulings (rules every agent follows)."""
from __future__ import annotations

from datetime import timedelta

from aifactory import coordination, db
from aifactory.models import Artifact, Decision, Event, Message, Outcome, Plan, Task
from aifactory.pipeline import lead
from aifactory.pipeline.runtime import precedents

from .test_lead import world  # noqa: F401 - fixture


def second_outcome(world, *, number=8, files=("app/search.py",), status="running", stage="build"):
    with db.session_scope() as s:
        o = Outcome(workspace_id=world["ws"], number=number, title=f"Outcome {number}", brief="", status="running",
                    repo_ids=[world["repo"]])
        s.add(o)
        s.flush()
        plan = Plan(outcome_id=o.id, status="approved", content={"tasks": []})
        s.add(plan)
        s.flush()
        t = Task(workspace_id=world["ws"], outcome_id=o.id, plan_id=plan.id, repo_id=world["repo"], key="T1",
                 title="Other work", status=status, stage=stage, pr_number=40)
        s.add(t)
        s.flush()
        s.add(Artifact(workspace_id=world["ws"], outcome_id=o.id, task_id=t.id, kind="diff", name="diff",
                       data={"files": list(files)}))
        return o.id, t.id


def diff(world, files, task=None):
    with db.session_scope() as s:
        s.add(Artifact(workspace_id=world["ws"], outcome_id=world["outcome"], task_id=task or world["task"],
                       kind="diff", name="diff", data={"files": list(files)}))


def overlap_events(outcome_id):
    with db.session_scope() as s:
        return [e.data for e in s.query(Event).filter(Event.outcome_id == outcome_id, Event.kind == "work_overlap")]


def test_overlapping_work_in_two_outcomes_is_recorded_once_on_both(world):
    diff(world, ["app/search.py", "app/api.py"])
    other_outcome, other_task = second_outcome(world)
    assert coordination.sync(world["ws"]) == 1
    mine, theirs = overlap_events(world["outcome"]), overlap_events(other_outcome)
    assert len(mine) == len(theirs) == 1
    assert mine[0]["files"] == ["app/search.py"] and mine[0]["other"]["outcome_number"] == 8
    assert theirs[0]["other"]["outcome_number"] == 7
    assert coordination.sync(world["ws"]) == 0  # once per overlap, not every sweep
    assert len(overlap_events(world["outcome"])) == 1


def test_disjoint_files_same_outcome_and_merged_work_are_not_overlaps(world):
    diff(world, ["app/search.py"])
    second_outcome(world, files=("app/other.py",))
    assert coordination.sync(world["ws"]) == 0
    # Two tasks of the same outcome are ordered by the planner, not here.
    with db.session_scope() as s:
        t = Task(workspace_id=world["ws"], outcome_id=world["outcome"], plan_id=world["plan"], repo_id=world["repo"],
                 key="T2", title="Same outcome", status="running", stage="build")
        s.add(t)
        s.flush()
        same = t.id
    diff(world, ["app/search.py"], task=same)
    assert coordination.sync(world["ws"]) == 0
    # Merged work no longer collides.
    _, other_task = second_outcome(world, number=9, files=("app/search.py",))
    with db.session_scope() as s:
        s.get(Task, other_task).merge_sha = "abc"
    assert coordination.sync(world["ws"]) == 0


def test_only_the_newest_candidate_counts(world):
    diff(world, ["app/search.py"])
    second_outcome(world)
    with db.session_scope() as s:
        newest = Artifact(workspace_id=world["ws"], outcome_id=world["outcome"], task_id=world["task"], kind="diff",
                          name="diff", data={"files": ["app/elsewhere.py"]},
                          created_at=db.utcnow() + timedelta(seconds=5))
        s.add(newest)
    assert coordination.sync(world["ws"]) == 0


def test_an_overlap_wakes_the_lead_and_its_record_lists_the_other_work(world):
    diff(world, ["app/search.py"])
    second_outcome(world)
    coordination.sync(world["ws"])
    due = {d["outcome_id"]: d for d in lead.due()}[world["outcome"]]
    assert any("progress" in r for r in due["reasons"])
    rec = lead.outcome_record(world["outcome"])
    [item] = rec["overlapping_work"]
    assert item["your_task"] == "T1" and item["shared_files"] == ["app/search.py"]
    assert item["other"]["outcome_number"] == 8 and item["other"]["pr_number"] == 40
    assert "overlapping_work" in lead.SYSTEM and "team_rules" in lead.SYSTEM


def test_the_overlaps_api(world, client):
    diff(world, ["app/search.py"])
    second_outcome(world)
    body = client.get(f"/api/workspaces/{world['ws']}/overlaps").json()
    [item] = body["overlaps"]
    assert item["files"] == ["app/search.py"] and {w["outcome_number"] for w in item["work"]} == {7, 8}


# Team rulings ---------------------------------------------------------------------------------------------------

def test_a_ruling_reaches_every_agent_and_the_lead(world, client):
    r = client.post(f"/api/workspaces/{world['ws']}/rulings",
                    json={"text": "Build token-shaped test strings at runtime, never as literals."})
    assert r.status_code == 200, r.text
    ruling = r.json()
    assert ruling["scope"] == "workspace" and ruling["by"]
    # Planner, builders and verifiers are given precedents: a ruling is one.
    texts = [d.comment for d in precedents(world["ws"], world["repo"])]
    assert "Build token-shaped test strings at runtime, never as literals." in texts
    other_outcome, _ = second_outcome(world)
    rules = lead.outcome_record(other_outcome)["team_rules"]
    assert rules and rules[0]["detail"].startswith("Build token-shaped")
    listed = client.get(f"/api/workspaces/{world['ws']}/rulings").json()["rulings"]
    assert [x["id"] for x in listed] == [ruling["id"]]


def test_guidance_becomes_a_ruling(world, client):
    with db.session_scope() as s:
        m = Message(workspace_id=world["ws"], outcome_id=world["outcome"], task_id=world["task"],
                    author_id=world["uid"], author_kind="user", body="Use the durable queue for anything billable.")
        s.add(m)
        s.flush()
        mid = m.id
    r = client.post(f"/api/workspaces/{world['ws']}/rulings", json={"message_id": mid, "scope": "repository"})
    assert r.status_code == 200, r.text
    with db.session_scope() as s:
        d = s.get(Decision, r.json()["id"])
        assert (d.kind, d.status, d.reusable, d.scope, d.task_id) == ("ruling", "resolved", True, "repository",
                                                                       world["task"])
        assert d.attribution == {"source": "ruling", "message_id": mid}
    # Repository scope: given to work in that repository only.
    assert any(x.id == r.json()["id"] for x in precedents(world["ws"], world["repo"]))
    assert not any(x.id == r.json()["id"] for x in precedents(world["ws"], "another-repo"))


def test_ruling_validation_and_isolation(world, client):
    ws = world["ws"]
    assert client.post(f"/api/workspaces/{ws}/rulings", json={"text": "  "}).status_code == 400
    assert client.post(f"/api/workspaces/{ws}/rulings", json={"text": "x", "scope": "item"}).status_code == 400
    assert client.post(f"/api/workspaces/{ws}/rulings", json={"text": "x", "scope": "repository"}).status_code == 400
    assert client.post(f"/api/workspaces/{ws}/rulings", json={"message_id": "nope"}).status_code == 404
    assert client.post(f"/api/workspaces/{ws}/rulings", json={"text": "x", "outcome_id": "nope"}).status_code == 404
    # A Factory note is not a person's guidance and can't be promoted.
    with db.session_scope() as s:
        m = Message(workspace_id=ws, outcome_id=world["outcome"], author_kind="factory", body="Build failed")
        s.add(m)
        s.flush()
        mid = m.id
    assert client.post(f"/api/workspaces/{ws}/rulings", json={"message_id": mid}).status_code == 404


def test_retiring_a_ruling_stops_it_being_given(world, client):
    rid = client.post(f"/api/workspaces/{world['ws']}/rulings", json={"text": "Prefer small PRs."}).json()["id"]
    assert client.delete(f"/api/workspaces/{world['ws']}/rulings/{rid}").status_code == 200
    assert not any(x.id == rid for x in precedents(world["ws"], world["repo"]))
    assert client.get(f"/api/workspaces/{world['ws']}/rulings").json()["rulings"] == []
    with db.session_scope() as s:
        assert s.get(Decision, rid).status == "resolved"  # kept in history


def test_every_ruling_is_given_beyond_the_precedent_limit(world, client):
    ids = [client.post(f"/api/workspaces/{world['ws']}/rulings", json={"text": f"Rule {i}"}).json()["id"]
           for i in range(15)]
    given = {d.id for d in precedents(world["ws"], world["repo"])}
    assert set(ids) <= given
    for i in range(10):  # more than 20 in all: the Lead still gets every ruling
        client.post(f"/api/workspaces/{world['ws']}/rulings", json={"text": f"Rule {15 + i}"})
    assert len(lead.outcome_record(world["outcome"])["team_rules"]) == 25


def test_ordinary_precedents_survive_many_rulings(world, client):
    with db.session_scope() as s:
        d = Decision(workspace_id=world["ws"], outcome_id=world["outcome"], kind="question", title="Old answer",
                     status="resolved", resolution="answer", comment="Use Postgres", reusable=True, scope="workspace",
                     resolved_at=db.utcnow() - timedelta(days=30))
        s.add(d)
        s.flush()
        old = d.id
    for i in range(50):
        client.post(f"/api/workspaces/{world['ws']}/rulings", json={"text": f"Rule {i}"})
    assert old in {d.id for d in precedents(world["ws"], world["repo"])}


def test_a_missing_side_of_an_overlap_is_recorded_on_the_next_sweep(world):
    diff(world, ["app/search.py"])
    other_outcome, _ = second_outcome(world)
    coordination.sync(world["ws"])
    with db.session_scope() as s:
        s.query(Event).filter(Event.outcome_id == other_outcome, Event.kind == "work_overlap").delete()
    assert coordination.sync(world["ws"]) == 1
    assert len(overlap_events(other_outcome)) == 1 and len(overlap_events(world["outcome"])) == 1


def test_promoting_the_same_guidance_twice_makes_one_rule(world, client):
    with db.session_scope() as s:
        m = Message(workspace_id=world["ws"], outcome_id=world["outcome"], author_id=world["uid"],
                    author_kind="user", body="Keep PRs under 400 lines.")
        s.add(m)
        s.flush()
        mid = m.id
    first = client.post(f"/api/workspaces/{world['ws']}/rulings", json={"message_id": mid}).json()["id"]
    second = client.post(f"/api/workspaces/{world['ws']}/rulings", json={"message_id": mid}).json()["id"]
    assert first == second
    assert len(client.get(f"/api/workspaces/{world['ws']}/rulings").json()["rulings"]) == 1
