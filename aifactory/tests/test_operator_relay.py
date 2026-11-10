"""Operator commands on the bootstrap's control issue apply on the next poll, without a restart."""
from __future__ import annotations

import asyncio
import json
from datetime import datetime, timedelta, timezone

import pytest

from aifactory import github as github_module
from aifactory.bootstrap import apply_bootstrap
from aifactory.db import session_scope
from aifactory.models import Decision, Event, Message, Organization, Outcome, Plan, Repository, Task, Workspace
from aifactory.operator_relay import REPLY_MARKER, Relay, control, parse

NOW = datetime.now(timezone.utc)
CONTROL = {"repository": "acme/app", "issue": 5, "operators": ["IanU82"]}


def iso(moment: datetime) -> str:
    return moment.strftime("%Y-%m-%dT%H:%M:%SZ")


class FakeGH:
    comments: list[dict] = []
    replies: list[str] = []
    permissions: dict[str, str] = {}
    lists = 0

    def __init__(self, token):
        pass

    async def request(self, method, path, *, params=None, **_):
        assert method == "GET" and path == "/repos/acme/app/issues/5/comments" and params["since"]
        FakeGH.lists += 1
        return list(FakeGH.comments)

    async def repository_author_permission(self, full_name, author):
        assert full_name == "acme/app"
        return FakeGH.permissions.get(author)

    async def comment(self, full_name, number, body):
        assert (full_name, number) == ("acme/app", 5)
        FakeGH.replies.append(body)
        FakeGH.comments.append({"id": 9000 + len(FakeGH.replies), "body": body, "user": {"login": "ianu82"},
                                "created_at": iso(NOW), "updated_at": iso(NOW), "html_url": ""})


def command(comment_id: int, payload=None, *, login: str = "ianu82", age=timedelta(minutes=1), raw=None) -> dict:
    body = raw if raw is not None else "/factory\n```json\n" + json.dumps(payload) + "\n```"
    return {"id": comment_id, "body": body, "user": {"login": login}, "created_at": iso(NOW - age),
            "updated_at": iso(NOW - age), "html_url": f"https://github.com/acme/app/issues/5#issuecomment-{comment_id}"}


REQUEST_CHANGES = {"resolve": [{"outcome": 7, "task": "T1", "kind": "approve_merge", "resolution": "reject",
                                "comment": "Fix the P1: scrub the whole payload"}],
                   "guidance": [{"outcome": 7, "task": "T1", "body": "Scrub once, at the response boundary."}]}


@pytest.fixture
def factory(monkeypatch):
    with session_scope() as s:
        org = Organization(name="Acme")
        s.add(org)
        s.flush()
        ws = Workspace(org_id=org.id, name="Acme", slug="acme", settings={})
        s.add(ws)
        s.flush()
        repo = Repository(workspace_id=ws.id, full_name="acme/app", default_branch="main", config={})
        s.add(repo)
        s.flush()
        o = Outcome(workspace_id=ws.id, number=7, title="Self change", repo_ids=[repo.id], status="attention")
        s.add(o)
        s.flush()
        plan = Plan(outcome_id=o.id, status="approved", content={})
        s.add(plan)
        s.flush()
        t = Task(workspace_id=ws.id, outcome_id=o.id, plan_id=plan.id, key="T1", title="t", repo_id=repo.id,
                 status="awaiting_approval", stage="merge", pr_number=196, head_sha="abc1234def")
        s.add(t)
        s.flush()
        s.add(Decision(workspace_id=ws.id, outcome_id=o.id, task_id=t.id, kind="approve_merge", title="Approve merge",
                       options=[{"value": "approve", "label": "Merge"}, {"value": "reject", "label": "Request changes"}]))
        ids = {"ws": ws.id, "task": t.id}
    monkeypatch.setenv("AIFACTORY_BOOTSTRAP", json.dumps({
        "organization": "Acme", "workspace": "Acme", "delegated_by": "ian@mindsdb.com", "control": CONTROL}))

    async def boot():
        return apply_bootstrap()

    assert asyncio.run(boot()) == ids["ws"]
    monkeypatch.setattr(github_module, "GitHub", FakeGH)
    monkeypatch.setattr(github_module, "resolve_credential", lambda workspace_id: "token")
    FakeGH.comments, FakeGH.replies, FakeGH.permissions, FakeGH.lists = [], [], {"ianu82": "admin"}, 0
    return ids


def poll(relay: Relay | None = None) -> list[int]:
    return asyncio.run((relay or Relay()).poll(control(), now=NOW))


def decision_state() -> tuple[str, str | None]:
    with session_scope() as s:
        d = s.query(Decision).one()
        return d.status, d.resolution


def test_a_command_applies_on_the_next_poll_and_only_once(factory):
    FakeGH.comments = [command(1, REQUEST_CHANGES)]
    assert poll() == [1]
    assert decision_state() == ("resolved", "reject")
    with session_scope() as s:
        task = s.get(Task, factory["task"])
        assert task.status == "queued" and task.stage == "revise"  # the fix round starts now, not at a restart
        assert [m.body for m in s.query(Message).all() if "boundary" in m.body] == ["Scrub once, at the response boundary."]
        relay_event = s.query(Event).filter(Event.kind == "operator_relay").one()
        assert relay_event.data["comment_id"] == 1 and relay_event.data["author"] == "ianu82"
    [reply] = FakeGH.replies
    assert reply.startswith(REPLY_MARKER) and "approve_merge reject: applied" in reply and "T1: posted" in reply
    # A restarted process (fresh relay, same events table) and the Factory's own reply change nothing.
    assert poll() == []
    assert len(FakeGH.replies) == 1
    with session_scope() as s:
        assert s.query(Event).filter(Event.kind == "operator_decision").count() == 1


def test_commands_from_others_without_write_access_or_too_old_do_not_apply(factory):
    FakeGH.permissions = {"ianu82": "admin", "mallory": "admin", "reader": "read"}
    FakeGH.comments = [command(2, REQUEST_CHANGES, login="mallory"),
                       command(3, REQUEST_CHANGES, age=timedelta(hours=25))]
    assert poll() == [3, 2]  # oldest first
    assert decision_state() == ("open", "")
    assert "more than 24 hours" in FakeGH.replies[0]
    assert "mallory is not a listed operator" in FakeGH.replies[1]
    FakeGH.permissions = {"ianu82": "read"}
    FakeGH.comments = [command(4, REQUEST_CHANGES)]
    assert poll() == [4]
    assert decision_state() == ("open", "")
    assert "ianu82 has no write access to acme/app" in FakeGH.replies[-1]


def test_plain_comments_are_ignored_and_bad_commands_are_answered(factory):
    FakeGH.comments = [command(5, raw="Looks good to me"),
                       command(6, raw=REPLY_MARKER + "\n/factory\n```json\n{\"retry\": [{\"outcome\": 7}]}\n```"),
                       command(7, raw="/factory\n```json\n{\"resolve\": [\n```"),
                       command(8, {"merge": [{"outcome": 7}]})]
    assert poll() == [7, 8]
    assert decision_state() == ("open", "")
    assert "not valid JSON" in FakeGH.replies[0]
    assert "any of cancel, retry, resolve, guidance" in FakeGH.replies[1]


def test_a_malformed_item_fails_alone(factory):
    mixed = {"guidance": [{"outcome": "seven", "body": "broken"},
                          {"outcome": 7, "task": "T1", "body": "Still delivered."}]}
    FakeGH.comments = [command(12, mixed)]
    assert poll() == [12]
    with session_scope() as s:
        assert [m.body for m in s.query(Message).all()] == ["Still delivered."]
    assert "guidance item 1: not applied, outcome must be an outcome number" in FakeGH.replies[0]
    assert "guidance #7 T1: posted" in FakeGH.replies[0]


def test_an_unexpected_failure_leaves_the_command_for_the_next_poll(factory, monkeypatch):
    from aifactory import bootstrap

    real, calls = bootstrap._guidance_delegated, []

    def flaky(*args, **kwargs):
        calls.append(1)
        if len(calls) == 1:
            raise RuntimeError("database is locked")
        return real(*args, **kwargs)

    monkeypatch.setattr(bootstrap, "_guidance_delegated", flaky)
    FakeGH.comments = [command(13, {"guidance": [{"outcome": 7, "task": "T1", "body": "Once."}]})]
    relay = Relay()
    with pytest.raises(RuntimeError):
        poll(relay)
    assert FakeGH.replies == []  # not answered and not marked handled
    assert poll(relay) == [13]
    assert "guidance #7 T1: posted" in FakeGH.replies[0]
    with session_scope() as s:
        assert [m.body for m in s.query(Message).all()] == ["Once."]


def test_item_errors_name_what_is_malformed():
    from aifactory.operator_relay import item_error

    assert item_error("resolve", {"outcome": 7, "kind": "approve_merge", "resolution": "reject"}) is None
    assert item_error("resolve", {"outcome": 7, "kind": "approve_merge"}) == "a resolve item needs kind and resolution"
    assert item_error("guidance", {"outcome": 7, "body": "x", "task": 1}) == "task must be a string"
    assert item_error("retry", {"key": "dup"}) is None and item_error("retry", {}) is not None
    assert item_error("cancel", "#4") is None and item_error("cancel", {"outcome": "x"}) is not None


def test_a_head_guard_skips_a_decision_judged_on_an_older_commit(factory):
    stale = {"resolve": [{"outcome": 7, "task": "T1", "kind": "approve_merge", "resolution": "approve",
                          "head": "0123456"}]}
    FakeGH.comments = [command(9, stale)]
    assert poll() == [9]
    assert decision_state() == ("open", "")
    assert "judged on 0123456 but the head is abc1234def" in FakeGH.replies[0]


def test_a_live_cancel_stops_the_planner_of_the_outcome(factory, monkeypatch):
    from aifactory.pipeline.orchestrator import orchestrator

    from aifactory import events

    stopped, changed = [], []
    # get_running_loop() raises outside the event loop's thread: the planner is stopped on its own loop.
    monkeypatch.setattr(orchestrator, "cancel_outcome",
                        lambda outcome_id: stopped.append((outcome_id, asyncio.get_running_loop() is not None)))
    monkeypatch.setattr(events, "notify", lambda workspace_id, entity, entity_id, **_: changed.append((entity, entity_id)))
    with session_scope() as s:
        planning = Outcome(workspace_id=factory["ws"], number=8, title="Still planning", repo_ids=[], status="planning")
        s.add(planning)
        s.flush()
        planning_id = planning.id
    FakeGH.comments = [command(11, {"cancel": ["#8"]})]
    assert poll() == [11]
    with session_scope() as s:
        assert s.get(Outcome, planning_id).status == "cancelled"
    assert stopped == [(planning_id, True)]  # the active planner is stopped, as the app's own cancel does
    assert ("outcome", planning_id) in changed  # and open dashboards refetch it
    assert "cancel #8: cancelled" in FakeGH.replies[0]


def test_a_live_cancel_never_discards_started_work(factory):
    FakeGH.comments = [command(10, {"cancel": [{"outcome": 7, "discard_started": True}]})]
    assert poll() == [10]
    with session_scope() as s:
        assert s.query(Outcome).one().status == "attention"
    assert "#7: not cancelled, open pull requests on T1" in FakeGH.replies[0]


def test_nothing_is_polled_without_a_control_issue(factory, monkeypatch):
    monkeypatch.setenv("AIFACTORY_BOOTSTRAP", json.dumps({"organization": "Acme", "workspace": "Acme"}))
    assert control() is None
    asyncio.run(Relay().maybe_poll())
    assert FakeGH.lists == 0


def test_parse_accepts_only_the_bootstrap_item_kinds():
    items, error = parse("/factory\n```json\n{\"retry\": [{\"outcome\": 3}], \"cancel\": [\"#4\"]}\n```")
    assert error == "" and items == {"retry": [{"outcome": 3}], "cancel": ["#4"]}
    assert parse("/factory no block")[0] is None
    assert parse("/factory\n```json\n{\"retry\": []}\n```")[1] == "retry must be a non-empty list"
    assert parse("/factory\n```json\n{\"guidance\": [\"text\"]}\n```")[1] == "every guidance item must be an object"
