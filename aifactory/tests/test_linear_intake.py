from __future__ import annotations

import asyncio
import json
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier

import httpx
import pytest
from sqlalchemy import event
from sqlalchemy.exc import IntegrityError

from aifactory import linear_intake
from aifactory.db import SessionLocal, engine, session_scope
from aifactory.linear import Linear
from aifactory.models import Event, Outcome, Repository, Signal, Workspace
from aifactory.signals import fingerprint, outcome_from_text, upsert_signal
from .linear_fake import connection


def issue(number=1, **changes):
    return {"id": f"immutable-uuid-{number}", "identifier": f"ENG-{number}", "title": f"Task {number}",
            "description": "Full description", "url": f"https://linear.app/eng/issue/ENG-{number}",
            "priority": 0, "team_id": "team-1", "state_id": "queued", "labels": ["factory-auto"], **changes}


class IntakeEndpoint(httpx.MockTransport):
    def __init__(self, issues):
        self.issues = issues
        self.requests = []
        super().__init__(self.handle)

    def handle(self, request):
        payload = json.loads(request.content)
        self.requests.append(payload)
        variables = payload["variables"]
        assert variables["filter"] == {"team": {"id": {"eq": "team-1"}},
            "state": {"id": {"eq": "queued"}}, "labels": {"some": {"name": {"eq": "factory-auto"}}}}
        assert "pageInfo { hasNextPage endCursor }" in payload["query"]
        matching = [item for item in self.issues if item["team_id"] == "team-1"
                    and item["state_id"] == "queued" and "factory-auto" in item["labels"]]
        offset = int(variables["after"] or 0)
        cursor = str(offset + 1) if offset + 1 < len(matching) else None
        return httpx.Response(200, json={"data": {"issues": connection(matching[offset:offset + 1], cursor)}})

    def provider(self):
        return Linear("not-a-real-key", transport=self)


@pytest.fixture
def configured(signed_in):
    workspace_id = signed_in["workspaces"][0]["id"]
    with session_scope() as db:
        excluded = Repository(workspace_id=workspace_id, full_name="org/excluded")
        allowed = Repository(workspace_id=workspace_id, full_name="org/allowed")
        second = Repository(workspace_id=workspace_id, full_name="org/second")
        db.add_all([excluded, allowed, second])
        db.flush()
        repo_ids = [second.id, allowed.id]
        workspace = db.get(Workspace, workspace_id)
        workspace.settings = {**workspace.settings, "linear": {"enabled": True, "team_id": "team-1",
            "repo_ids": repo_ids, "intake_label": "factory-auto",
            "states": {stage: stage for stage in ("queued", "building", "verifying", "merging", "done", "blocked")}}}
    return workspace_id, repo_ids


def sync(workspace_id, endpoint):
    with session_scope() as db:
        return asyncio.run(linear_intake.sync_linear(db, workspace_id, endpoint.provider()))


@pytest.mark.parametrize("heading", ["## Acceptance criteria", "# ACCEPTANCE CRITERIA:",
    "Acceptance criteria", "**Acceptance criteria**", "Acceptance criteria\n-------------------"])
def test_acceptance_parsing(heading):
    description = f"Intro\n- Not acceptance\n\n{heading}\n- First\n* [ ] Second\n+ [X] Third\n1. Fourth\n2) Fifth\n\n## Notes\n- Not criteria"
    assert linear_intake.acceptance_from_description(description) == ["First", "Second", "Third", "Fourth", "Fifth"]


@pytest.mark.parametrize("description", ["", "No heading\n- not criteria", "## Acceptance criteria\n## Notes\n- nope"])
def test_absent_acceptance(description):
    assert linear_intake.acceptance_from_description(description) == []


def test_setext_heading_boundary():
    assert linear_intake.acceptance_from_description(
        "## Acceptance criteria\n- yes\nNotes\n=====\n- no") == ["yes"]


def test_filters_pagination_priorities_and_full_text(configured):
    workspace_id, repo_ids = configured
    brief = "  Intro\n" + "long description " * 700 + "\n## Acceptance criteria\n- [ ] Ship it\n## Notes\n- no\n  "
    priorities = [1, 2, 3, 4, 0, None]
    issues = [issue(index, priority=priority, description=brief) for index, priority in enumerate(priorities)]
    del issues[-1]["priority"]
    endpoint = IntakeEndpoint([*issues, issue(10, team_id="other"), issue(11, state_id="building"),
        issue(12, labels=["Factory-auto"]), issue(13, labels=["factory-auto-extra"])])
    assert sync(workspace_id, endpoint) == {"created": 6, "accepted": 6}
    assert len(endpoint.requests) == 6
    assert [request["variables"]["after"] for request in endpoint.requests] == [None, "1", "2", "3", "4", "5"]
    with session_scope() as db:
        outcomes = db.query(Outcome).order_by(Outcome.number).all()
        assert [outcome.priority for outcome in outcomes] == [0, 1, 2, 3, 2, 2]
        for outcome, item in zip(outcomes, issues):
            assert outcome.title == item["title"]
            assert outcome.brief == brief
            assert outcome.acceptance == ["Ship it"]
            assert outcome.repo_ids == repo_ids
            assert outcome.status == "planning"
            signal = db.get(Signal, outcome.signal_id)
            assert signal.status == "accepted" and signal.outcome_id == outcome.id
            assert signal.source == "linear_issue" and signal.external_id == item["id"]
            assert signal.fingerprint == fingerprint("linear_issue", item["id"])
            assert signal.url == item["url"]
            assert signal.data == {"identifier": item["identifier"], "team_id": "team-1"}


def test_restart_cancel_and_reentry(configured, client):
    workspace_id, _ = configured
    endpoint = IntakeEndpoint([issue()])
    assert sync(workspace_id, endpoint)["accepted"] == 1
    with session_scope() as db:
        outcome_id = db.query(Outcome).one().id
    assert client.post(f"/api/workspaces/{workspace_id}/outcomes/{outcome_id}/cancel").status_code == 200
    endpoint.issues[0]["state_id"] = "done"
    assert sync(workspace_id, endpoint)["accepted"] == 0
    endpoint.issues[0]["state_id"] = "queued"
    for _attempt in range(3):
        assert sync(workspace_id, endpoint)["accepted"] == 0
    with session_scope() as db:
        assert db.query(Outcome).count() == 1
        assert db.query(Outcome).one().status == "cancelled"
        assert db.query(Signal).one().outcome_id == outcome_id


@pytest.mark.parametrize("failure", ["creation", "commit", "unique"])
def test_transaction_rollback(configured, monkeypatch, failure):
    workspace_id, _ = configured
    original = linear_intake.outcome_from_text

    def fail(db, *args, **kwargs):
        original(db, *args, **kwargs)
        if failure == "unique":
            raise IntegrityError("forced unique conflict", {}, Exception())
        raise RuntimeError("creation failed")

    with SessionLocal() as db:
        if failure == "commit":
            original_commit = db.commit
            calls = 0

            def fail_commit():
                nonlocal calls
                calls += 1
                if calls == 2:
                    raise RuntimeError("commit failed")
                original_commit()

            monkeypatch.setattr(db, "commit", fail_commit)
        else:
            monkeypatch.setattr(linear_intake, "outcome_from_text", fail)
        with pytest.raises((RuntimeError, IntegrityError)):
            asyncio.run(linear_intake.sync_linear(db, workspace_id, IntakeEndpoint([issue()]).provider()))
    with session_scope() as db:
        assert db.query(Signal).count() == db.query(Outcome).count() == 0
        assert db.get(Workspace, workspace_id).outcome_seq == 0
        assert not db.query(Event).filter(Event.kind.in_(["signal_accepted", "outcome_created"])).all()


def test_concurrent_claims_and_sequence_allocation(configured):
    workspace_id, _ = configured
    barrier = Barrier(3)

    class OverlappingEndpoint(IntakeEndpoint):
        def handle(self, request):
            barrier.wait(timeout=10)
            return super().handle(request)

    endpoint = OverlappingEndpoint([issue()])

    def create_manual():
        with session_scope() as db:
            db.get(Workspace, workspace_id)
            barrier.wait(timeout=10)
            return outcome_from_text(db, workspace_id, None, "Manual", "", []).number

    with ThreadPoolExecutor(max_workers=3) as pool:
        futures = [pool.submit(sync, workspace_id, endpoint) for _attempt in range(2)]
        manual = pool.submit(create_manual)
        assert sum(future.result()["accepted"] for future in futures) == 1
        assert manual.result() in (1, 2)
    with session_scope() as db:
        assert db.query(Signal).count() == 1
        assert [row.number for row in db.query(Outcome).order_by(Outcome.number)] == [1, 2]
        assert db.get(Workspace, workspace_id).outcome_seq == 2


def test_stale_session_cannot_replace_link(configured):
    workspace_id, _ = configured
    with session_scope() as db:
        signal, _created = upsert_signal(db, workspace_id, source="linear_issue", external_id=issue()["id"],
            title="Old", body="", url=issue()["url"], repo_id=None)
        signal_id = signal.id
    with SessionLocal() as stale:
        cached = stale.get(Signal, signal_id)
        stale.get(Workspace, workspace_id)
        assert cached.outcome_id is None
        assert sync(workspace_id, IntakeEndpoint([issue()]))["accepted"] == 1
        assert asyncio.run(linear_intake.sync_linear(stale, workspace_id,
            IntakeEndpoint([issue()]).provider()))["accepted"] == 0
    with session_scope() as db:
        assert db.query(Outcome).count() == 1


def test_unique_conflict_retries_entire_transaction(configured, monkeypatch):
    workspace_id, _ = configured
    original = linear_intake.upsert_signal
    attempts = 0

    def conflict_once(*args, **kwargs):
        nonlocal attempts
        attempts += 1
        result = original(*args, **kwargs)
        if attempts == 1:
            raise IntegrityError("forced concurrent insert", {}, Exception())
        return result

    monkeypatch.setattr(linear_intake, "upsert_signal", conflict_once)
    assert sync(workspace_id, IntakeEndpoint([issue()])) == {"created": 1, "accepted": 1}
    assert attempts == 2
    with session_scope() as db:
        assert db.query(Signal).count() == db.query(Outcome).count() == 1
        assert db.get(Workspace, workspace_id).outcome_seq == 1


def test_configuration_is_rechecked_after_fetch(configured):
    workspace_id, _ = configured

    class ChangedConfigEndpoint(IntakeEndpoint):
        def handle(self, request):
            with session_scope() as db:
                workspace = db.get(Workspace, workspace_id)
                workspace.settings = {**workspace.settings, "linear": {
                    **workspace.settings["linear"], "enabled": False}}
            return super().handle(request)

    assert sync(workspace_id, ChangedConfigEndpoint([issue()]))["accepted"] == 0
    with session_scope() as db:
        assert db.query(Signal).count() == db.query(Outcome).count() == 0


def test_late_page_failure_is_safe_and_visible_once(configured):
    workspace_id, _ = configured

    class FailingEndpoint(IntakeEndpoint):
        def handle(self, request):
            payload = json.loads(request.content)
            if payload["variables"]["after"]:
                return httpx.Response(429, text="credential-must-not-leak")
            return super().handle(request)

    endpoint = FailingEndpoint([issue(), issue(2)])
    for _attempt in range(2):
        assert sync(workspace_id, endpoint)["accepted"] == 0
    with session_scope() as db:
        assert db.query(Signal).count() == db.query(Outcome).count() == 0
        warning = db.query(Event).filter_by(kind="linear_intake_warning").one()
        assert warning.data == {"category": "rate_limit"}
        assert "credential-must-not-leak" not in warning.message
    assert sync(workspace_id, IntakeEndpoint([issue(), issue(2)]))["accepted"] == 2


@pytest.mark.parametrize("cancelled", [False, True])
def test_generic_accept_preserves_permanent_link_and_allowlist(configured, client, cancelled):
    workspace_id, repo_ids = configured
    sync(workspace_id, IntakeEndpoint([issue()]))
    with session_scope() as db:
        signal_id, outcome_id = db.query(Signal).one().id, db.query(Outcome).one().id
    if cancelled:
        assert client.post(f"/api/workspaces/{workspace_id}/outcomes/{outcome_id}/cancel").status_code == 200
    path = f"/api/workspaces/{workspace_id}/signals/{signal_id}"
    for _attempt in range(2):
        response = client.post(path, json={"action": "accept", "title": "Override", "brief": "Override"})
        assert response.status_code == 200
        assert response.json()["outcome_id"] == outcome_id
    assert client.post(path, json={"action": "ignore"}).status_code == 409
    with session_scope() as db:
        assert db.query(Outcome).count() == 1
        assert db.query(Outcome).one().repo_ids == repo_ids
        assert db.query(Signal).one().outcome_id == outcome_id


def test_generic_accept_cannot_admit_unlinked_linear_signal(configured, client):
    workspace_id, _ = configured
    with session_scope() as db:
        signal, _created = upsert_signal(db, workspace_id, source="linear_issue", external_id=issue()["id"],
            title="Old", body="", url=issue()["url"], repo_id=None)
        signal_id = signal.id
    response = client.post(f"/api/workspaces/{workspace_id}/signals/{signal_id}", json={"action": "accept"})
    assert response.status_code == 409
    with session_scope() as db:
        assert db.query(Outcome).count() == 0
        assert db.get(Signal, signal_id).outcome_id is None


@pytest.mark.parametrize("remaining", [False, True])
def test_stale_allowlist_is_resolved_at_admission(configured, remaining):
    workspace_id, repo_ids = configured
    with session_scope() as db:
        for repo_id in repo_ids[:1] if remaining else repo_ids:
            db.delete(db.get(Repository, repo_id))
    for _attempt in range(2):
        sync(workspace_id, IntakeEndpoint([issue()]))
    with session_scope() as db:
        if remaining:
            assert db.query(Outcome).one().repo_ids == repo_ids[1:]
        else:
            assert db.query(Outcome).count() == db.query(Signal).count() == 0
            warning = db.query(Event).filter_by(kind="linear_intake_warning").one()
            assert warning.level == "warn" and "repositories" in warning.message


def test_disconnect_updates_configuration(configured, client):
    workspace_id, repo_ids = configured
    path = f"/api/workspaces/{workspace_id}"
    assert client.delete(f"{path}/repositories/{repo_ids[0]}").status_code == 200
    config = client.get(f"{path}/linear").json()
    assert config["repo_ids"] == repo_ids[1:] and config["enabled"] is True
    assert client.delete(f"{path}/repositories/{repo_ids[1]}").status_code == 200
    config = client.get(f"{path}/linear").json()
    assert config["repo_ids"] == [] and config["enabled"] is False
    assert sync(workspace_id, IntakeEndpoint([issue()]))["accepted"] == 0


def test_periodic_intake_runs_without_github_credentials(configured, monkeypatch):
    from aifactory import linear_config
    from aifactory.linear_sync import LinearSync
    from aifactory.pipeline.orchestrator import Orchestrator

    workspace_id, _ = configured
    endpoint = IntakeEndpoint([issue()])
    monkeypatch.setenv("AIFACTORY_SIGNAL_INTERVAL_SECONDS", "180")
    monkeypatch.setattr(linear_config, "provider", lambda workspace_id: endpoint.provider())
    orchestrator = Orchestrator()
    asyncio.run(LinearSync(wake=orchestrator.wake).cycle())
    with session_scope() as db:
        assert db.query(Outcome).filter_by(workspace_id=workspace_id).count() == 1
    assert orchestrator._wake.is_set()


def test_list_detail_source_serialization_is_batched(configured, client):
    workspace_id, _ = configured
    sync(workspace_id, IntakeEndpoint([issue(), issue(2)]))
    with session_scope() as db:
        manual = outcome_from_text(db, workspace_id, None, "Existing outcome", "", [])
        manual_id = manual.id
    queries = []

    def capture(connection, cursor, statement, parameters, context, executemany):
        if statement.lstrip().upper().startswith("SELECT") and "FROM signals" in statement:
            queries.append(statement)

    event.listen(engine(), "before_cursor_execute", capture)
    try:
        response = client.get(f"/api/workspaces/{workspace_id}/outcomes")
        assert response.status_code == 200
        assert len(queries) == 1
    finally:
        event.remove(engine(), "before_cursor_execute", capture)
    for row in response.json():
        detail = client.get(f"/api/workspaces/{workspace_id}/outcomes/{row['id']}")
        assert detail.status_code == 200
        assert detail.json()["outcome"]["linear_issue"] == row["linear_issue"]
        if row["id"] == manual_id:
            assert row["linear_issue"] is None
        else:
            number = row["number"]
            expected = issue(number)
            assert row["linear_issue"] == {"id": expected["id"], "identifier": expected["identifier"], "url": expected["url"]}


def test_linear_event_publications_wait_for_commit_in_order(configured, monkeypatch):
    from aifactory import events
    workspace_id, _ = configured
    published = []
    monkeypatch.setattr(events.bus, 'publish', published.append)
    with session_scope() as db:
        config = linear_intake.linear_config.saved_config(db.get(Workspace, workspace_id))
        assert linear_intake._admit(db, workspace_id, issue(), config) == (True, True)
        assert published == []
    assert [(p['type'], p.get('kind', p.get('entity'))) for p in published] == [
        ('event', 'outcome_created'), ('changed', 'outcome'), ('event', 'signal_accepted')]
