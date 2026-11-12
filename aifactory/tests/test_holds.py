"""A provider hold says why the provider refused, and what lifts it, wherever it shows."""
from __future__ import annotations

import asyncio
from datetime import timedelta

import pytest

from aifactory import db
from aifactory.inference import InferenceError, ProviderConfig
from aifactory.models import Decision, Event, User, Workspace
from aifactory.pipeline import holds
from aifactory.pipeline.holds import CAUSES, hold_cause, hold_details


@pytest.mark.parametrize("error, cause", [
    ("MindsHub: no credit remaining (insufficient balance)", "credit"),
    ("Error code: 402 - Payment Required", "credit"),
    ("Provider returned 402: wallet_empty", "credit"),
    ("You exceeded your current quota, please check your plan and billing details.", "credit"),
    ("MindsHub refused the credential (401): invalid key", "credential"),
    ("Error code: 403 - Key disabled", "credential"),
    ("No model provider is configured for this workspace.", "no_provider"),
    ("MindsHub limit reached (429): Monthly limit reached", "limit"),
    ("Rate limit reached for gpt-terra", "rate_limit"),
    ("Error code: 429 - Too many requests", "rate_limit"),
    ("The model provider is momentarily overloaded. It did not recover after 3 attempts.", "overloaded"),
    ("MindsHub error 503: upstream unavailable", "overloaded"),
    ("The provider said no, in words no rule here knows", "unknown"),
    ("Used 1,402,000 tokens before the refusal", "unknown"),  # digits inside a number are not a status
])
def test_a_refusal_names_its_cause(error, cause):
    assert hold_cause(error) == cause
    label, advice = CAUSES[cause]
    assert label and advice.endswith("resume work.")


def test_a_hold_opened_before_causes_were_kept_is_read_from_its_text():
    # The wording of the hold that stopped the AI Factory workspace on 12 Nov 2026.
    hold = Decision(kind="provider_hold", title="Model provider refused: work is on hold", body=(
        "The model provider refused a request (planning outcome #59, the planner):\n\nThe model provider is "
        "momentarily overloaded. It did not recover after 3 attempts.\n\nNothing new starts in this workspace, neither "
        "planning nor tasks, until someone chooses Resume work. Work that is refused the same way waits in the queue; "
        "no task is charged an attempt for it.\n\nWhat to do: add credits or fix the provider credential in Settings, "
        "then choose Resume work. If the provider still refuses, this hold opens again."))
    assert hold_details(hold) == {
        "cause": "overloaded", "label": CAUSES["overloaded"][0], "advice": CAUSES["overloaded"][1],
        "source": "planning outcome #59, the planner",
        "error": "The model provider is momentarily overloaded. It did not recover after 3 attempts."}
    # One opened since keeps its cause: its text is never read for it.
    kept = Decision(kind="provider_hold", title="t", body="anything",
                    metadata_json={"cause": "credential", "source": "T2 of outcome #4, the builder session",
                                   "error": "Bad credentials"})
    assert hold_details(kept)["cause"] == "credential" and hold_details(kept)["source"].startswith("T2")


class Provider:
    """Stands in for ModelClient in a hold's test: records each request and answers with ``reply(model)``, which
    returns the provider's answer or raises its refusal."""
    sent: list = []
    options: dict = {}
    reply = None

    def __init__(self, provider, **options):
        Provider.options = options

    async def _post(self, path, body):
        Provider.sent.append((path, body))
        return Provider.reply(body["model"]) if Provider.reply else {"output": []}


@pytest.fixture
def held(signed_in, monkeypatch):
    """A workspace held for credit, whose planner and builder use one model and its verifier another."""
    ws = signed_in["workspaces"][0]["id"]
    with db.session_scope() as s:
        w = s.get(Workspace, ws)
        w.settings = {**(w.settings or {}), "models": {"default": "gpt-terra", "verifier": "check-model"}}
    monkeypatch.setattr(holds, "PROBED", {})
    monkeypatch.setattr(holds, "ModelClient", Provider)
    monkeypatch.setattr(holds, "resolve_provider", lambda workspace_id: ProviderConfig("mindshub", "unused", "SECRET",
                                                                                      "MindsHub"))
    monkeypatch.setattr(Provider, "sent", [])
    monkeypatch.setattr(Provider, "reply", None)
    hold = holds.open_provider_hold(ws, "MindsHub: no credit remaining (balance 0)", "planning outcome #3")
    with db.session_scope() as s:
        opened = s.get(Decision, hold).created_at
    return {"ws": ws, "hold": hold, "opened": opened, "uid": signed_in["user"]["id"]}


def hold_row(held):
    with db.session_scope() as s:
        d = s.get(Decision, held["hold"])
        s.expunge(d)
        return d


def events(held, kind):
    with db.session_scope() as s:
        return [(e.level, e.message, e.data) for e in s.query(Event).filter_by(workspace_id=held["ws"], kind=kind)
                .order_by(Event.id)]


def test_a_hold_is_tested_two_minutes_after_it_opens_then_every_five_minutes_and_never_more_often(held, client):
    ws, opened, due = held["ws"], held["opened"], [{"workspace_id": held["ws"], "decision_id": held["hold"]}]
    assert holds.due_probes({}, now=opened + timedelta(minutes=1, seconds=59)) == []
    assert holds.due_probes({}, now=opened + timedelta(minutes=2)) == due
    tested = {ws: opened + timedelta(minutes=2)}
    assert holds.due_probes(tested, now=opened + timedelta(minutes=6, seconds=59)) == []
    assert holds.due_probes(tested, now=opened + timedelta(minutes=7)) == due
    assert holds.due_probes(tested, frozenset({ws}), now=opened + timedelta(hours=1)) == []  # one test at a time
    for bad in ({"test_minutes": 61}, {"test_minutes": -1}, {"test_minutes": "5"}, {"every": 5}, 5):
        assert client.patch(f"/api/workspaces/{ws}", json={"settings": {"holds": bad}}).status_code == 422, bad
    r = client.patch(f"/api/workspaces/{ws}", json={"settings": {"holds": {"test_minutes": 0}}})
    assert r.status_code == 200 and r.json()["settings"]["holds"] == {"test_minutes": 0}
    assert holds.due_probes({}, now=opened + timedelta(hours=1)) == []  # 0: never tested


def test_the_orchestrator_tests_a_hold_beside_the_scheduler_and_remembers_when(held, monkeypatch):
    from aifactory.pipeline import orchestrator as orchestration

    started = []

    async def probe(workspace_id, decision_id):
        started.append((workspace_id, decision_id))
        return "failed"

    monkeypatch.setattr(holds, "probe_provider", probe)
    with db.session_scope() as s:
        s.get(Decision, held["hold"]).created_at = db.utcnow() - timedelta(minutes=3)

    async def go():
        orchestrator = orchestration.Orchestrator()
        await orchestrator._maybe_probe_holds()
        assert set(orchestrator._hold_probes) == {held["ws"]}
        await asyncio.gather(*orchestrator._hold_probes.values())
        await asyncio.sleep(0)
        assert orchestrator._hold_probes == {}
        orchestrator._hold_check = float("-inf")
        await orchestrator._maybe_probe_holds()  # tested a moment ago: not again for five minutes
        assert orchestrator._hold_probes == {}

    asyncio.run(go())
    assert started == [(held["ws"], held["hold"])] and set(holds.PROBED) == {held["ws"]}
    # Screens that show the hold say how often the Factory tests the provider, and when it last did.
    with db.session_scope() as s:
        from aifactory import serializers

        shown = serializers.decision(s.get(Decision, held["hold"]))["hold"]
    assert shown["tests_every_minutes"] == 5 and shown["last_tested_at"] == holds.PROBED[held["ws"]].isoformat() + "Z"


def test_every_model_answering_lifts_the_hold_as_a_resume_by_the_factory(held):
    assert asyncio.run(holds.probe_provider(held["ws"], held["hold"])) == "lifted"
    # One minimal request per distinct model of the planner, builder and verifier: no tools, no retries.
    assert [body["model"] for _, body in Provider.sent] == ["gpt-terra", "check-model"]
    assert all(path == "/responses" and body["max_output_tokens"] == holds.TEST_TOKENS and "tools" not in body
               for path, body in Provider.sent)
    assert Provider.options["max_retries"] == 0
    from aifactory import serializers

    with db.session_scope() as s:
        d = s.get(Decision, held["hold"])
        assert (d.status, d.resolution, d.attribution) == ("resolved", "retry", {"source": "factory"})
        assert d.comment == "The provider answered a test request (gpt-terra, check-model). Resumed by the Factory."
        factory = s.get(User, d.resolved_by)
        assert (factory.name, factory.email, factory.is_system) == ("Factory", holds.FACTORY_EMAIL, True)
        assert serializers.decision(d)["resolved_by_name"] == "Factory"
        assert holds.held_workspaces(s) == set()  # the scheduler starts the workspace's queued work again
        [resolved] = s.query(Event).filter_by(workspace_id=held["ws"], kind="decision_resolved").all()
        assert resolved.actor == factory.id and resolved.data["source"] == "factory"
    [(level, message, data)] = events(held, "provider_hold_lifted")
    assert level == "success" and "(gpt-terra, check-model)" in message
    assert data == {"decision_id": held["hold"], "models": ["gpt-terra", "check-model"], "source": "factory"}


def test_a_billing_or_credential_refusal_leaves_the_hold_and_says_why_once_per_reason(held):
    def refuse(model):
        raise InferenceError("MindsHub: no credit remaining (balance 0)", status=402)

    Provider.reply = refuse
    for _ in range(3):
        assert asyncio.run(holds.probe_provider(held["ws"], held["hold"])) == "refused"
    assert hold_row(held).status == "open"
    [(level, message, data)] = events(held, "provider_hold_tested")
    assert level == "warn" and "out of credit" in message
    assert (data["decision_id"], data["cause"], data["reason"]) == (held["hold"], "credit", CAUSES["credit"][0])

    def refuse_key(model):
        raise InferenceError("MindsHub refused the credential (401): invalid key", status=401)

    Provider.reply = refuse_key
    assert asyncio.run(holds.probe_provider(held["ws"], held["hold"])) == "refused"
    assert asyncio.run(holds.probe_provider(held["ws"], held["hold"])) == "refused"
    assert [data["cause"] for _, _, data in events(held, "provider_hold_tested")] == ["credit", "credential"]
    assert events(held, "provider_hold_lifted") == [] and hold_row(held).status == "open"


@pytest.mark.parametrize("failure", [
    InferenceError("MindsHub error 503: upstream unavailable", retryable=True, status=503),
    InferenceError("The model provider is momentarily overloaded", cause="provider"),  # its own words: it passes
    asyncio.TimeoutError(),
])
def test_any_other_failure_is_tried_again_at_the_next_interval_silently(held, failure):
    def fail(model):
        raise failure

    Provider.reply = fail
    assert asyncio.run(holds.probe_provider(held["ws"], held["hold"])) == "failed"
    assert hold_row(held).status == "open"
    assert events(held, "provider_hold_tested") == [] and events(held, "provider_hold_lifted") == []
    Provider.reply = None  # the next interval: the provider answers
    assert asyncio.run(holds.probe_provider(held["ws"], held["hold"])) == "lifted"


def test_a_hold_a_person_resumes_during_the_test_is_resolved_once(held):
    from aifactory.decisions import resolve

    def person_resumes(model):
        if len(Provider.sent) == 1:
            with db.session_scope() as s:
                resolve(s, s.get(Decision, held["hold"]), s.get(User, held["uid"]), "retry", "Topped up")
        return {"output": []}

    Provider.reply = person_resumes
    assert asyncio.run(holds.probe_provider(held["ws"], held["hold"])) == "resolved"
    d = hold_row(held)
    assert (d.status, d.resolved_by, d.comment, d.attribution) == ("resolved", held["uid"], "Topped up", None)
    assert len(events(held, "decision_resolved")) == 1 and events(held, "provider_hold_lifted") == []
