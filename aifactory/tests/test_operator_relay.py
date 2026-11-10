"""Operator commands on the bootstrap's control issue apply on the next poll, without a restart."""
from __future__ import annotations

import asyncio
import json
import logging
from datetime import datetime, timedelta, timezone

import pytest

from aifactory import github as github_module
from aifactory import linear as linear_module
from aifactory.bootstrap import apply_bootstrap
from aifactory.db import session_scope
from aifactory.linear import LinearError
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
    rate_limited = False
    comment_fails = 0
    comment_calls = 0
    lists = 0

    def __init__(self, token):
        pass

    async def request(self, method, path, *, params=None, **_):
        assert method == "GET"
        if path.startswith("/repos/acme/app/collaborators/") and path.endswith("/permission"):
            if FakeGH.rate_limited:
                raise github_module.GitHubError("API rate limit exceeded", 403)
            login = path.split("/")[-2]
            if login not in FakeGH.permissions:
                raise github_module.GitHubError("Not Found", 404)
            return {"permission": FakeGH.permissions[login]}
        assert path == "/repos/acme/app/issues/5/comments" and params["since"]
        FakeGH.lists += 1
        return list(FakeGH.comments)

    async def comment(self, full_name, number, body):
        assert (full_name, number) == ("acme/app", 5)
        FakeGH.comment_calls += 1
        if FakeGH.comment_fails:
            FakeGH.comment_fails -= 1
            raise github_module.GitHubError("GitHub unreachable", 502)
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
    FakeGH.rate_limited, FakeGH.comment_fails, FakeGH.comment_calls = False, 0, 0
    return ids


def poll(relay: Relay | None = None, at: datetime = NOW) -> list:
    return asyncio.run((relay or Relay()).poll(control(), now=at))


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


def test_a_rate_limited_permission_check_retries_instead_of_refusing(factory):
    FakeGH.rate_limited = True
    FakeGH.comments = [command(14, REQUEST_CHANGES)]
    relay = Relay()
    with pytest.raises(github_module.GitHubError):
        poll(relay)
    assert FakeGH.replies == [] and decision_state() == ("open", "")
    FakeGH.rate_limited = False
    assert poll(relay) == [14]
    assert decision_state() == ("resolved", "reject")


def test_a_failed_answer_is_retried_without_applying_again(factory):
    FakeGH.comment_fails = 2
    FakeGH.comments = [command(16, REQUEST_CHANGES)]
    relay = Relay()
    assert poll(relay) == [16]  # applied; the answer failed
    assert decision_state() == ("resolved", "reject") and FakeGH.replies == []
    FakeGH.comments = []  # later polls no longer list it (GitHub's since window has moved on)
    poll(relay, at=NOW + timedelta(seconds=30))  # the second answer attempt fails too
    poll(relay, at=NOW + timedelta(seconds=60))  # backing off: the next try is a minute after the second
    assert FakeGH.comment_calls == 2
    poll(relay, at=NOW + timedelta(seconds=90))
    assert len(FakeGH.replies) == 1 and "approve_merge reject: applied" in FakeGH.replies[0]
    with session_scope() as s:
        assert s.query(Event).filter(Event.kind == "operator_relay").one().data["acknowledged"] is True
        assert s.query(Event).filter(Event.kind == "operator_decision").count() == 1


def test_an_answer_lost_in_a_restart_is_posted_by_the_next_process(factory):
    FakeGH.comment_fails = 1
    FakeGH.comments = [command(17, REQUEST_CHANGES)]
    assert poll() == [17]
    assert FakeGH.replies == []
    assert poll(Relay()) == []  # a new process finds the record: answers it, applies nothing again
    assert len(FakeGH.replies) == 1
    with session_scope() as s:
        assert s.query(Event).filter(Event.kind == "operator_decision").count() == 1


def test_an_old_unanswered_record_is_still_answered_after_a_restart(factory):
    from aifactory.db import utcnow

    with session_scope() as s:
        s.add(Event(workspace_id=factory["ws"], kind="operator_relay", message="old", actor="operator",
                    created_at=utcnow() - timedelta(days=3),
                    data={"comment_id": 99, "author": "ianu82", "results": ["retry #7: retrying T1"],
                          "html_url": "https://github.com/acme/app/issues/5#issuecomment-99", "acknowledged": False}))
    assert poll(Relay()) == []
    assert len(FakeGH.replies) == 1 and "retry #7: retrying T1" in FakeGH.replies[0]
    assert "issuecomment-99" in FakeGH.replies[0]
    with session_scope() as s:
        assert s.query(Event).filter(Event.kind == "operator_relay").one().data["acknowledged"] is True
    assert poll(Relay()) == [] and len(FakeGH.replies) == 1  # answered once


def test_a_login_that_is_not_a_collaborator_is_refused(factory):
    FakeGH.permissions = {}
    FakeGH.comments = [command(15, REQUEST_CHANGES)]
    assert poll() == [15]
    assert decision_state() == ("open", "")
    assert "ianu82 has no write access" in FakeGH.replies[0]


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
    assert item_error("retry", {"outcome": 7, "include_cancelled": "false"}) == "include_cancelled must be true or false"
    assert item_error("resolve", {"outcome": 7, "kind": "question", "resolution": "answer", "reusable": "false"}) \
        == "reusable must be true or false"
    assert item_error("cancel", {"outcome": 7, "discard_started": "yes"}) == "discard_started must be true or false"
    assert item_error("retry", {"outcome": 7, "include_cancelled": True}) is None


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


def test_failed_answers_back_off_to_one_try_every_ten_minutes(factory, caplog):
    FakeGH.comment_fails = 1000  # the token cannot comment on issues
    FakeGH.comments = [command(18, REQUEST_CHANGES)]
    relay = Relay()
    caplog.set_level(logging.WARNING, logger="aifactory.operator_relay")
    tries = []
    for second in range(0, 3601, 30):  # an hour of polls
        before = FakeGH.comment_calls
        poll(relay, at=NOW + timedelta(seconds=second))
        if FakeGH.comment_calls > before:
            tries.append(second)
    assert tries[:7] == [0, 30, 90, 210, 450, 930, 1530]  # doubling from one poll, then every ten minutes
    assert {b - a for a, b in zip(tries[5:], tries[6:])} == {600}
    assert len(tries) == 10  # not 121
    assert len([r for r in caplog.records if "could not answer comment 18" in r.getMessage()]) == len(tries)
    with session_scope() as s:
        assert s.query(Event).filter(Event.kind == "operator_decision").count() == 1  # applied once throughout


IAN = {"id": "user-ian", "name": "Ian", "email": "ian@example.com", "active": True, "guest": False, "app": False}
LINEAR_CONTROL = {"linear": {"issue": "ENG-5"}, "operators": ["Ian@Example.com", "user-deputy"]}


class FakeLinear:
    comments: list[dict] = []
    replies: list[tuple[str, str]] = []  # (comment id, body)
    create_fails = 0
    posts_then_fails = False
    lists = 0

    def __init__(self, token, **_):
        assert token == "lin_api_key"

    async def issue(self, issue_id):
        assert issue_id == "ENG-5"
        return {"id": "issue-uuid", "identifier": "ENG-5", "url": "https://linear.app/acme/issue/ENG-5"}

    async def comments_since(self, issue_id, since):
        assert issue_id == "issue-uuid" and since.endswith("Z")
        FakeLinear.lists += 1
        return list(FakeLinear.comments)

    async def create_comment(self, issue_id, body, *, comment_id=None):
        assert issue_id == "issue-uuid" and comment_id
        if FakeLinear.create_fails and not FakeLinear.posts_then_fails:
            FakeLinear.create_fails -= 1
            raise LinearError("authentication")
        FakeLinear.replies.append((comment_id, body))
        FakeLinear.comments.append({"id": comment_id, "body": body, "createdAt": NOW.isoformat(), "url": "", "user": IAN})
        if FakeLinear.create_fails:  # it posted, but the response never arrived
            FakeLinear.create_fails -= 1
            raise LinearError("timeout")
        return {"id": comment_id, "body": body}


def linear_command(comment_id: str, payload, *, user=IAN, age=timedelta(minutes=1)) -> dict:
    return {"id": comment_id, "body": "/factory\n```json\n" + json.dumps(payload) + "\n```",
            "createdAt": (NOW - age).isoformat(timespec="milliseconds").replace("+00:00", "Z"),
            "url": f"https://linear.app/acme/issue/ENG-5#comment-{comment_id}", "user": user}


@pytest.fixture
def linear_factory(factory, monkeypatch):
    monkeypatch.setenv("AIFACTORY_BOOTSTRAP", json.dumps({
        "organization": "Acme", "workspace": "Acme", "delegated_by": "ian@mindsdb.com", "control": LINEAR_CONTROL}))
    monkeypatch.setattr(linear_module, "Linear", FakeLinear)
    monkeypatch.setattr(linear_module, "resolve_token", lambda workspace_id: "lin_api_key")
    FakeLinear.comments, FakeLinear.replies, FakeLinear.lists = [], [], 0
    FakeLinear.create_fails, FakeLinear.posts_then_fails = 0, False
    return factory


def test_control_names_a_github_or_a_linear_issue(factory, monkeypatch):
    assert control()["channel"] == "github"

    def with_control(value):
        monkeypatch.setenv("AIFACTORY_BOOTSTRAP", json.dumps({"organization": "Acme", "workspace": "Acme",
                                                              "control": value}))
        return control()

    cfg = with_control(LINEAR_CONTROL)
    assert (cfg["channel"], cfg["issue"], cfg["operators"]) == ("linear", "ENG-5", {"ian@example.com", "user-deputy"})
    assert with_control({**LINEAR_CONTROL, "repository": "acme/app"}) is None  # one channel, not both
    assert with_control({"linear": {"issue": " "}, "operators": ["ian@example.com"]}) is None
    assert with_control({"linear": {"issue": "ENG-5"}, "operators": []}) is None


def test_a_linear_command_applies_once_and_is_answered_on_linear(linear_factory):
    FakeLinear.comments = [linear_command("c-1", REQUEST_CHANGES)]
    relay = Relay()
    assert poll(relay) == ["c-1"]
    assert decision_state() == ("resolved", "reject")
    [(reply_id, reply)] = FakeLinear.replies
    assert reply.startswith("Factory relay for https://linear.app/acme/issue/ENG-5#comment-c-1 (by Ian):")
    assert "approve_merge reject: applied" in reply and "guidance #7 T1: posted" in reply
    assert reply_id == relay.channel(control()).reply_id("c-1")  # fixed per command
    assert FakeGH.lists == 0 and FakeGH.replies == []  # nothing on GitHub
    with session_scope() as s:
        relay_event = s.query(Event).filter(Event.kind == "operator_relay").one()
        assert relay_event.data["channel"] == "linear:ENG-5" and relay_event.data["acknowledged"] is True
    # The same process, a restarted one and the Factory's own reply change nothing.
    assert poll(relay, at=NOW + timedelta(seconds=30)) == [] and poll(Relay()) == []
    assert len(FakeLinear.replies) == 1
    with session_scope() as s:
        assert s.query(Event).filter(Event.kind == "operator_decision").count() == 1


def test_only_listed_active_linear_members_count(linear_factory):
    deputy = {**IAN, "id": "user-deputy", "name": "Deputy", "email": "deputy@example.com"}
    FakeLinear.comments = [
        linear_command("c-2", REQUEST_CHANGES, user={**IAN, "id": "user-m", "name": "Mallory", "email": "m@example.com"}),
        linear_command("c-3", REQUEST_CHANGES, user={**IAN, "active": False}),
        linear_command("c-4", REQUEST_CHANGES, user={**IAN, "guest": True}),
        linear_command("c-5", REQUEST_CHANGES, user=None),  # an integration or a synced external user
        linear_command("c-6", REQUEST_CHANGES, age=timedelta(hours=25)),
        linear_command("c-7", {"guidance": [{"outcome": 7, "task": "T1", "body": "From the deputy."}]}, user=deputy),
    ]
    assert sorted(poll()) == ["c-2", "c-3", "c-4", "c-5", "c-6", "c-7"]
    assert decision_state() == ("open", "")
    replies = {reply.split("#comment-")[1].split(" ")[0]: reply for _, reply in FakeLinear.replies}
    assert "Mallory is not a listed operator" in replies["c-2"]
    assert "Ian is not an active member of the Linear workspace" in replies["c-3"]
    assert "Ian is not an active member of the Linear workspace" in replies["c-4"]
    assert "unknown is not a listed operator" in replies["c-5"]
    assert "more than 24 hours" in replies["c-6"]
    assert "guidance #7 T1: posted" in replies["c-7"]  # listed by user id
    with session_scope() as s:
        assert [m.body for m in s.query(Message).all()] == ["From the deputy."]


def test_a_linear_answer_that_did_post_is_not_posted_twice(linear_factory):
    FakeLinear.create_fails, FakeLinear.posts_then_fails = 1, True
    FakeLinear.comments = [linear_command("c-8", REQUEST_CHANGES)]
    relay = Relay()
    assert poll(relay) == ["c-8"]  # applied; the answer posted but its response was lost
    assert relay.unanswered
    poll(relay, at=NOW + timedelta(seconds=30))  # the next listing shows the answer's fixed id
    assert len(FakeLinear.replies) == 1 and not relay.unanswered
    with session_scope() as s:
        assert s.query(Event).filter(Event.kind == "operator_relay").one().data["acknowledged"] is True


def test_github_answers_are_not_posted_on_linear(linear_factory):
    with session_scope() as s:
        s.add(Event(workspace_id=linear_factory["ws"], kind="operator_relay", message="old", actor="operator",
                    data={"comment_id": 98, "author": "ianu82", "results": ["retry #7: retrying T1"],
                          "html_url": "https://github.com/acme/app/issues/5#issuecomment-98", "acknowledged": False}))
    assert poll(Relay()) == []
    assert FakeLinear.replies == [] and FakeGH.replies == []


def test_a_failing_channel_is_polled_less_often(linear_factory, monkeypatch, caplog):
    calls = []

    def missing_key(workspace_id):
        calls.append(workspace_id)
        raise LinearError("not_configured")

    monkeypatch.setattr(linear_module, "resolve_token", missing_key)
    caplog.set_level(logging.WARNING, logger="aifactory.operator_relay")
    relay = Relay()
    asyncio.run(relay.maybe_poll())
    assert relay.failures == 1 and len(calls) == 1
    relay.last_poll = float("-inf")  # the poll interval has passed, but the backoff has not
    asyncio.run(relay.maybe_poll())
    assert len(calls) == 1
    relay.last_poll = relay.resume_at = float("-inf")
    asyncio.run(relay.maybe_poll())
    assert relay.failures == 2 and len(calls) == 2
    warnings = [r for r in caplog.records if "poll failed" in r.getMessage()]
    assert [bool(w.exc_info) for w in warnings] == [True, False]  # one line per step, one traceback
    assert "next poll in 60s" in warnings[1].getMessage()
    monkeypatch.setattr(linear_module, "resolve_token", lambda workspace_id: "lin_api_key")
    relay.last_poll = relay.resume_at = float("-inf")
    asyncio.run(relay.maybe_poll())
    assert relay.failures == 0 and FakeLinear.lists == 1
