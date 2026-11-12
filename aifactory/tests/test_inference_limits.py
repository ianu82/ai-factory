"""Admission contracts without an installed agent SDK, CLI, or live provider."""
from __future__ import annotations

import asyncio
import json
from contextlib import asynccontextmanager
from types import SimpleNamespace

import httpx
import pytest
from starlette.requests import Request

from aifactory import config, gateway, inference, inference_limits as limits
from aifactory.harness.anton import _create_admitted_response
from aifactory.inference import ModelClient, ProviderConfig, UsageLedger


@pytest.fixture(autouse=True)
def fresh_registry(monkeypatch):
    monkeypatch.setattr(limits, "_registry", {})


@pytest.fixture
def provider():
    return ProviderConfig("mindshub", "https://provider.test/v1", "private-credential", "Fake")


@pytest.fixture
def clocked(provider, monkeypatch):
    now = [0.0]
    guard = limits.AdmissionGuard(clock=lambda: now[0])
    monkeypatch.setattr(limits, "provider_guard", lambda p: guard)
    return now, guard


def test_default_and_positive_provider_configuration(monkeypatch):
    assert config.get_settings().mindshub_requests_per_minute == 50
    assert config.get_settings().openai_requests_per_minute == 50
    monkeypatch.setenv("AIFACTORY_MINDSHUB_REQUESTS_PER_MINUTE", "2")
    monkeypatch.setenv("AIFACTORY_OPENAI_REQUESTS_PER_MINUTE", "0")
    config.reset_settings_cache()
    assert config.get_settings().mindshub_requests_per_minute == 2
    assert config.get_settings().openai_requests_per_minute == 1


def test_full_credential_identity_and_separate_event_loops(provider):
    guard = limits.provider_guard(provider)
    same = ProviderConfig("mindshub", provider.base_url + "/", provider.api_key, "Other workspace")
    assert limits.provider_guard(same) is guard
    # Identical suffixes must not share capacity.
    other = ProviderConfig("mindshub", provider.base_url, "another-" + provider.api_key, "Other")
    assert limits.provider_guard(other) is not guard
    assert limits.provider_guard(ProviderConfig("openai", provider.base_url, provider.api_key, "Other")) is not guard
    assert limits.provider_guard(ProviderConfig("mindshub", "https://elsewhere/v1", provider.api_key, "Other")) is not guard
    assert provider.api_key not in repr(limits._registry)
    for _ in range(3):
        permit = asyncio.run(guard.acquire(50))
        permit.release()
    assert guard.active == 0 and len(guard._sends) == 3


async def test_rolling_window_wait_and_no_event_spam():
    now = [0.0]
    guard = limits.AdmissionGuard(clock=lambda: now[0])
    events = []
    report = lambda *args: events.append(args)
    (await guard.acquire(2)).release()
    now[0] = 10
    (await guard.acquire(2)).release()
    task = asyncio.create_task(guard.acquire(2, report))
    await asyncio.sleep(0)
    assert not task.done() and guard.active == 0
    now[0] = 59.9
    await asyncio.sleep(0.11)
    assert not task.done() and len(events) == 1
    now[0] = 60
    permit = await asyncio.wait_for(task, 1)
    permit.release()
    assert list(guard._sends) == [10, 60]
    assert [e[0] for e in events] == ["inference_throttled", "inference_admitted"]
    assert events[-1][2]["wait_seconds"] == 50


async def test_atomic_admission_after_long_concurrency_wait_and_cancellation():
    now = [0.0]
    guard = limits.AdmissionGuard(clock=lambda: now[0])
    permits = [await guard.acquire(50) for _ in range(16)]
    waiting = [asyncio.create_task(guard.acquire(50)) for _ in range(20)]
    await asyncio.sleep(0)
    assert guard.active == 16 and len(guard._sends) == 16
    waiting[0].cancel()
    with pytest.raises(asyncio.CancelledError):
        await waiting[0]
    now[0] = 120  # Old rate capacity must not be reserved before concurrency is available.
    permits[0].release()
    await asyncio.sleep(0.11)
    done = [t for t in waiting[1:] if t.done()]
    assert len(done) == 1 and guard.active == 16
    assert list(guard._sends) == [120]
    for t in waiting[1:]:
        if t.done():
            t.result().release()
        else:
            t.cancel()
    await asyncio.gather(*waiting, return_exceptions=True)
    for permit in permits:
        permit.release()
    assert guard.active == 0
    (await guard.acquire(50)).release()


async def test_cancelled_rate_waiter_owns_nothing():
    guard = limits.AdmissionGuard(clock=lambda: 0)
    (await guard.acquire(1)).release()
    task = asyncio.create_task(guard.acquire(1))
    await asyncio.sleep(0)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert guard.active == 0 and len(guard._sends) == 1


def fake_http(monkeypatch, handler):
    real = httpx.AsyncClient
    monkeypatch.setattr(httpx, "AsyncClient", lambda **kw: real(transport=httpx.MockTransport(handler), **kw))


def grant_for(provider, monkeypatch):
    ledger = UsageLedger("missing-run", {})
    monkeypatch.setattr(ledger, "persist", lambda: None)
    grant = gateway.GatewayGrant("missing-run", "workspace", "pinned", provider, ledger)
    monkeypatch.setattr(gateway, "_authenticate", lambda request: grant)
    return grant


def request(body):
    async def receive():
        return {"type": "http.request", "body": json.dumps(body).encode()}
    return Request({"type": "http", "method": "POST"}, receive)


async def test_native_anton_gateway_share_rate_capacity_and_anton_retry(provider, clocked, monkeypatch):
    now, guard = clocked
    monkeypatch.setenv("AIFACTORY_MINDSHUB_REQUESTS_PER_MINUTE", "2")
    config.reset_settings_cache()
    fake_http(monkeypatch, lambda req: httpx.Response(200, json={"output": []}))
    grant_for(provider, monkeypatch)
    await ModelClient(provider)._post("/responses", {})
    sends = []

    class Responses:
        async def create(self, **body):
            sends.append(now[0])
            return {}

    sdk = SimpleNamespace(_client=SimpleNamespace(responses=Responses()))

    async def sdk_hook(p, body):
        await p._client.responses.create(**body)
        # Mimics the SDK's direct reasoning-summary compatibility retry.
        return await p._client.responses.create(**body)

    anton = asyncio.create_task(_create_admitted_response(provider, None, sdk_hook, sdk, {}))
    await asyncio.sleep(0)
    assert sends == [0] and not anton.done()
    now[0] = 60
    await asyncio.wait_for(anton, 1)
    assert sends == [0, 60]
    response = await gateway.proxy("responses/compact", request({}))
    assert response.status_code == 200
    assert list(guard._sends) == [60, 60] and guard.active == 0


async def test_native_and_gateway_retries_reacquire_without_holding_backoff(provider, clocked, monkeypatch):
    now, guard = clocked
    sends = []

    def upstream(req):
        sends.append(guard.active)
        return httpx.Response(429 if len(sends) % 2 else 200, headers={"retry-after": "0.01"}, json={})

    fake_http(monkeypatch, upstream)
    grant_for(provider, monkeypatch)
    real_sleep = asyncio.sleep

    async def backoff(delay):
        assert guard.active == 0
        now[0] += delay
        await real_sleep(0)

    monkeypatch.setattr(asyncio, "sleep", backoff)
    await ModelClient(provider, max_retries=1)._post("/responses", {})
    assert (await gateway.proxy("responses", request({}))).status_code == 200
    assert sends == [1, 1, 1, 1]
    assert len(guard._sends) == 4 and guard.active == 0


def retry_clock(monkeypatch, now, guard):
    """Retry waits without waiting: each is recorded, moves the shared clock on, and must hold no permit."""
    waits = []
    real_sleep = asyncio.sleep

    async def backoff(delay):
        assert guard.active == 0
        waits.append(delay)
        now[0] += delay
        await real_sleep(0)

    monkeypatch.setattr(inference, "_clock", lambda: now[0])
    monkeypatch.setattr(inference.random, "random", lambda: 0.0)
    monkeypatch.setattr(asyncio, "sleep", backoff)
    return waits


async def test_a_short_provider_outage_is_retried_inside_the_request(provider, clocked, monkeypatch):
    now, guard = clocked
    waits = retry_clock(monkeypatch, now, guard)
    answers = [(429, {"retry-after": "7"}, {"error": {"message": "Rate limit reached"}}), (502, {}, {}),
               (200, {}, {"output": []})]
    sends, notes = [], []

    def upstream(req):
        sends.append(guard.active)
        status, headers, body = answers[len(sends) - 1]
        return httpx.Response(status, headers=headers, json=body)

    fake_http(monkeypatch, upstream)
    assert await ModelClient(provider)._post("/responses", {}, notice=notes.append) == {"output": []}
    # The provider's own retry-after is honoured, then the backoff; no permit is held during either wait.
    assert sends == [1, 1, 1] and waits == [7, 2] and guard.active == 0
    assert notes == ["Model provider unavailable (502); retried for 9 s, then succeeded"]


async def test_an_outage_longer_than_the_retry_budget_fails_the_request(provider, clocked, monkeypatch):
    now, guard = clocked
    waits = retry_clock(monkeypatch, now, guard)
    sends, notes = [], []

    def upstream(req):
        sends.append(now[0])
        return httpx.Response(503, json={"error": {"message": "Service temporarily unavailable"}})

    fake_http(monkeypatch, upstream)
    with pytest.raises(inference.InferenceError) as failed:
        await ModelClient(provider)._post("/responses", {}, notice=notes.append)
    # Doubling, capped at a minute. With 57 s left the next wait would end at the budget, so it is not taken and
    # nothing is sent there: such a request could run for its whole timeout past the budget.
    assert waits == [1, 2, 4, 8, 16, 32, 60, 60, 60]
    assert len(sends) == 10 and sends[-1] - sends[0] == 243 < inference.RETRY_BUDGET_SECONDS
    assert (failed.value.status, failed.value.cause) == (503, "transient")
    assert notes == ["Model provider unavailable (503); gave up after 243 s"]


@pytest.mark.parametrize("limit, sent, waited", [(0, 1, []), (2, 3, [1, 2])])
async def test_a_client_with_a_retry_limit_fails_fast(provider, clocked, monkeypatch, limit, sent, waited):
    now, guard = clocked
    waits = retry_clock(monkeypatch, now, guard)
    sends = []

    def upstream(req):
        sends.append(guard.active)
        return httpx.Response(503, json={})

    fake_http(monkeypatch, upstream)
    with pytest.raises(inference.InferenceError) as failed:
        await ModelClient(provider, max_retries=limit)._post("/responses", {})
    assert len(sends) == sent and waits == waited and failed.value.cause == "transient"


@pytest.mark.parametrize("status, headers, message, said", [
    (402, {}, "Payment required", "no credit remaining"),
    (401, {}, "Invalid API key", "refused the credential"),
    (403, {}, "Key disabled", "refused the credential"),
    (429, {}, "You exceeded your current quota, please check your plan and billing details.", "error 429"),
    (429, {"x-should-retry": "false"}, "Monthly limit reached", "limit reached"),
])
async def test_a_provider_refusal_is_never_retried(provider, clocked, monkeypatch, status, headers, message, said):
    now, guard = clocked
    waits = retry_clock(monkeypatch, now, guard)
    sends = []

    def upstream(req):
        sends.append(guard.active)
        return httpx.Response(status, headers=headers, json={"error": {"message": message}})

    fake_http(monkeypatch, upstream)
    with pytest.raises(inference.InferenceError) as failed:
        await ModelClient(provider)._post("/responses", {})
    assert len(sends) == 1 and waits == [] and guard.active == 0
    assert (failed.value.status, failed.value.cause) == (status, "provider") and said in str(failed.value)


@pytest.mark.parametrize("status, message", [
    (503, "The model provider is momentarily overloaded. It did not recover after 3 attempts."),
    (529, "Overloaded"),
    (429, "The model provider is momentarily overloaded. It did not recover after 3 attempts."),
])
async def test_an_overload_the_provider_gave_up_on_is_retried_like_any_outage_not_refused(provider, clocked,
                                                                                          monkeypatch, status, message):
    now, guard = clocked
    waits = retry_clock(monkeypatch, now, guard)
    sends = []

    def upstream(req):
        sends.append(now[0])
        if len(sends) != 2:  # "do not send this again": the provider retried on its side and gave up for now
            return httpx.Response(status, headers={"x-should-retry": "false"}, json={"error": {"message": message}})
        return httpx.Response(200, json={"ok": True})

    fake_http(monkeypatch, upstream)
    assert await ModelClient(provider)._post("/responses", {}) == {"ok": True}
    assert sends == [0, 1] and waits == [1] and guard.active == 0
    # Where it does not pass in time, the failure is the provider being unavailable: work that is redone later,
    # never a refusal that holds the workspace until a person resumes it.
    with pytest.raises(inference.InferenceError) as failed:
        await ModelClient(provider, max_retries=0)._post("/responses", {})
    assert (failed.value.status, failed.value.cause) == (status, "transient")
    assert "limit reached" not in str(failed.value)


def test_do_not_send_this_again_is_a_refusal_only_on_a_limit():
    from aifactory.failures import failure_cause

    never = {"x-should-retry": "false"}
    assert failure_cause(sdk_error("RateLimitError", 429, "Too many requests", never)) == "provider"
    # About an overload the provider retried itself, it means "not now": no limit a person lifts.
    assert failure_cause(sdk_error("InternalServerError", 503, "Error code: 503 - upstream failed",
                                   never)) == "transient"
    assert failure_cause(sdk_error("RateLimitError", 429, "The model provider is momentarily overloaded. It did not "
                                   "recover after 3 attempts.", never)) == "transient"


async def test_retries_stop_at_the_session_deadline_and_when_it_is_cancelled(provider, clocked, monkeypatch):
    now, guard = clocked
    waits = retry_clock(monkeypatch, now, guard)
    sends = []

    def upstream(req):
        sends.append(now[0])
        return httpx.Response(503, json={})

    fake_http(monkeypatch, upstream)
    client = ModelClient(provider)
    with pytest.raises(inference.InferenceError):
        await client._post("/responses", {}, deadline=now[0] + 10)
    # With 3 s of the session left the next wait (8 s) does not fit: the request fails, nothing is sent at the deadline.
    assert waits == [1, 2, 4] and sends == [0, 1, 3, 7]
    stopped = asyncio.Event()
    stopped.set()
    with pytest.raises(inference.InferenceError):
        await client._post("/responses", {}, cancelled=stopped)
    assert len(sends) == 5 and len(waits) == 3  # a cancelled session sends once and never waits


async def test_a_resend_is_given_only_the_time_its_retries_have_left(provider, clocked, monkeypatch):
    now, guard = clocked
    retry_clock(monkeypatch, now, guard)
    allowed = []

    def upstream(req):
        allowed.append((now[0], req.extensions["timeout"]["read"], req.extensions["timeout"]["connect"]))
        return httpx.Response(503, json={})

    fake_http(monkeypatch, upstream)
    with pytest.raises(inference.InferenceError):
        await ModelClient(provider)._post("/responses", {}, deadline=now[0] + 10)
    # The first send has the client's whole timeout; each resend only what is left of the session's 10 s, so one
    # sent 3 s before the end cannot run for five minutes past it.
    assert allowed == [(0, 300, 20), (1, 9, 9), (3, 7, 7), (7, 3, 3)]
    del allowed[:]
    now[0] = 1000.0
    with pytest.raises(inference.InferenceError):
        await ModelClient(provider)._post("/responses", {})
    # Without a session the end is the retry budget, counted from the request's first failure.
    assert allowed[0] == (1000, 300, 20) and allowed[1] == (1001, 299, 20) and allowed[-1] == (1243, 57, 20)
    del allowed[:]
    with pytest.raises(inference.InferenceError):
        await ModelClient(provider, max_retries=2)._post("/responses", {})
    # A client with a fixed number of resends has no such end: every send keeps the whole timeout.
    assert [read for _, read, _ in allowed] == [300, 300, 300]


async def test_a_resend_cut_short_fails_with_the_answer_that_was_being_retried(provider, clocked, monkeypatch):
    now, guard = clocked
    waits = retry_clock(monkeypatch, now, guard)
    sends, notes = [], []

    def upstream(req):
        sends.append(now[0])
        if len(sends) == 1:
            return httpx.Response(503, json={"error": {"message": "Service temporarily unavailable"}})
        now[0] += req.extensions["timeout"]["read"]  # no answer: the request runs out the time it was given
        raise httpx.ReadTimeout("timed out", request=req)

    fake_http(monkeypatch, upstream)
    with pytest.raises(inference.InferenceError) as failed:
        await ModelClient(provider)._post("/responses", {}, deadline=now[0] + 10, notice=notes.append)
    # The resend at 1 s had the 9 s that were left and used them all: the session's time is up, nothing more is
    # sent, and the failure is the provider's 503, not a provider that could not be reached.
    assert sends == [0, 1] and waits == [1] and now[0] == 10
    assert failed.value.status == 503 and "unreachable" not in str(failed.value)
    assert notes == ["Model provider unavailable (503); gave up after 10 s"]


async def test_a_resends_wait_for_its_permit_counts_against_the_time_that_is_left(provider, clocked, monkeypatch):
    now, guard = clocked
    waits = retry_clock(monkeypatch, now, guard)
    queued = [0, 4, 20]  # how long each send waits for its permit
    allowed, notes = [], []

    @asynccontextmanager
    async def admitted(provider, run_id=None):
        now[0] += queued.pop(0)
        yield

    def upstream(req):
        allowed.append((now[0], req.extensions["timeout"]["read"]))
        return httpx.Response(503, json={})

    monkeypatch.setattr(inference, "admission", admitted)
    fake_http(monkeypatch, upstream)
    with pytest.raises(inference.InferenceError) as failed:
        await ModelClient(provider)._post("/responses", {}, deadline=now[0] + 10, notice=notes.append)
    # The resend at 1 s waited 4 s for its permit, so it is given the 5 s left then, not the 9 s left when it
    # queued. The next one's permit comes after the session's time is up: it is not sent at all.
    assert allowed == [(0, 300), (5, 5)] and waits == [1, 2] and not queued
    assert failed.value.status == 503
    assert notes == ["Model provider unavailable (503); gave up after 27 s"]


async def test_a_request_that_times_out_with_its_whole_timeout_is_an_unreachable_provider(provider, clocked,
                                                                                         monkeypatch):
    now, guard = clocked
    waits = retry_clock(monkeypatch, now, guard)
    sends = []

    def upstream(req):
        sends.append(req.extensions["timeout"]["read"])
        raise httpx.ReadTimeout("timed out", request=req)

    fake_http(monkeypatch, upstream)
    with pytest.raises(inference.InferenceError) as failed:
        await ModelClient(provider, max_retries=1)._post("/responses", {})
    assert sends == [300, 300] and waits == [1] and "unreachable: ReadTimeout" in str(failed.value)


async def test_cancelling_the_session_ends_a_retry_wait(provider, clocked, monkeypatch):
    _, guard = clocked
    sends, waiting, stopped = [], asyncio.Event(), asyncio.Event()

    def upstream(req):
        sends.append(guard.active)
        return httpx.Response(503, json={})

    async def backoff(delay):
        waiting.set()
        await asyncio.Event().wait()  # this wait ends only by being cancelled

    fake_http(monkeypatch, upstream)
    monkeypatch.setattr(asyncio, "sleep", backoff)
    pending = asyncio.create_task(ModelClient(provider)._post("/responses", {}, cancelled=stopped))
    await asyncio.wait_for(waiting.wait(), 1)
    assert sends == [1] and not pending.done() and guard.active == 0
    stopped.set()
    with pytest.raises(inference.InferenceError):
        await asyncio.wait_for(pending, 1)
    assert sends == [1]


def sdk_error(name, status, message="", headers=None):
    """An exception shaped like the OpenAI SDK's (a status_code, the response's headers), without the SDK."""
    exc = type(name, (Exception,), {})(message or f"Error code: {status}")
    exc.status_code, exc.response = status, SimpleNamespace(headers=headers or {})
    return exc


async def test_anton_retries_a_short_outage_outside_the_permit_and_never_a_refusal(provider, clocked, monkeypatch):
    now, guard = clocked
    waits = retry_clock(monkeypatch, now, guard)
    refusal = sdk_error("APIStatusError", 402, "Error code: 402 - {'error': {'message': 'Payment Required'}}")
    answers = [sdk_error("InternalServerError", 503), sdk_error("RateLimitError", 429, headers={"retry-after": "5"}),
               {"id": "resp_1"}, refusal]
    sends, notes = [], []

    class Responses:
        async def create(self, **body):
            sends.append(guard.active)
            answer = answers[len(sends) - 1]
            if isinstance(answer, Exception):
                raise answer
            return answer

    sdk = SimpleNamespace(_client=SimpleNamespace(responses=Responses()))

    async def sdk_hook(p, body):
        return await p._client.responses.create(**body)

    def session_retries():
        return inference.Retries(notice=notes.append)

    assert await _create_admitted_response(provider, None, sdk_hook, sdk, {}, session_retries) == {"id": "resp_1"}
    assert sends == [1, 1, 1] and waits == [1, 5] and guard.active == 0
    assert notes == ["Model provider unavailable (429); retried for 6 s, then succeeded"]
    with pytest.raises(type(refusal)) as failed:
        await _create_admitted_response(provider, None, sdk_hook, sdk, {}, session_retries)
    assert failed.value is refusal and len(sends) == 4 and waits == [1, 5] and len(notes) == 1


@pytest.mark.parametrize("failure", ["send", "read", "close", "cancel"])
async def test_gateway_failure_releases_capacity(provider, clocked, monkeypatch, failure):
    _, guard = clocked
    grant_for(provider, monkeypatch)

    class Broken(httpx.AsyncByteStream):
        async def __aiter__(self):
            if failure == "cancel":
                raise asyncio.CancelledError()
            raise RuntimeError("read failed")
            yield b""

        async def aclose(self):
            if failure == "close":
                raise RuntimeError("close failed")

    def upstream(req):
        if failure == "send":
            raise httpx.ConnectError("offline")
        return httpx.Response(200, stream=Broken())

    fake_http(monkeypatch, upstream)
    with pytest.raises((RuntimeError, httpx.ConnectError, asyncio.CancelledError)):
        await gateway.proxy("responses", request({}))
    assert guard.active == 0
    (await guard.acquire(50)).release()


@pytest.mark.parametrize("disconnect", [False, True])
async def test_gateway_stream_holds_permit_and_cleans_up(provider, clocked, monkeypatch, disconnect):
    _, guard = clocked
    grant = grant_for(provider, monkeypatch)
    closed = []

    class Stream(httpx.AsyncByteStream):
        async def __aiter__(self):
            yield b'data: {"usage":{"input_tokens":3,"output_tokens":2}}\n\n'
            if disconnect:
                await asyncio.Event().wait()

        async def aclose(self):
            closed.append(True)

    fake_http(monkeypatch, lambda req: httpx.Response(200, stream=Stream()))
    response = await gateway.proxy("responses", request({"stream": True}))
    assert guard.active == 1
    first = asyncio.Event()

    async def send(message):
        if message["type"] == "http.response.body" and message.get("body"):
            first.set()
        assert guard.active == 1 or message.get("more_body") is False

    async def receive():
        await first.wait()
        return {"type": "http.disconnect"}

    await asyncio.wait_for(response({"type": "http", "asgi": {"spec_version": "2.0"}}, receive, send), 1)
    assert guard.active == 0 and closed
    assert grant.ledger.calls == 1
    if not disconnect:
        assert grant.ledger.input_tokens == 3


async def test_run_activity_is_associated_and_contains_no_credential(provider, clocked, signed_in):
    from aifactory.db import session_scope
    from aifactory.models import Event, Outcome, Run

    now, guard = clocked
    ws = signed_in["workspaces"][0]["id"]
    with session_scope() as s:
        outcome = Outcome(workspace_id=ws, number=1, title="Test", repo_ids=[])
        s.add(outcome)
        s.flush()
        run = Run(workspace_id=ws, outcome_id=outcome.id, role="builder", harness="native")
        s.add(run)
        s.flush()
        run_id = run.id
    (await guard.acquire(1)).release()
    task = asyncio.create_task(guard.acquire(1, limits.run_reporter(run_id)))
    await asyncio.sleep(0)
    now[0] = 60
    (await asyncio.wait_for(task, 1)).release()
    with session_scope() as s:
        events = s.query(Event).filter(Event.run_id == run_id).order_by(Event.id).all()
        assert [e.kind for e in events] == ["inference_throttled", "inference_admitted"]
        assert all(e.workspace_id == ws for e in events)
        assert events[-1].data["wait_seconds"] == 60
        assert provider.api_key not in json.dumps([(e.message, e.data) for e in events])


async def test_all_send_paths_obey_same_concurrency_cap(provider, monkeypatch):
    grant_for(provider, monkeypatch)
    guard = limits.provider_guard(provider)
    entered = 0
    peak = 0
    full = asyncio.Event()
    release = asyncio.Event()

    async def outbound():
        nonlocal entered, peak
        entered += 1
        peak = max(peak, guard.active)
        if entered == 16:
            full.set()
        await release.wait()
        return {}

    async def upstream(req):
        await outbound()
        return httpx.Response(200, json={})

    fake_http(monkeypatch, upstream)

    class Responses:
        async def create(self, **body):
            return await outbound()

    sdk = SimpleNamespace(_client=SimpleNamespace(responses=Responses()))

    async def hook(p, body):
        return await p._client.responses.create(**body)

    tasks = []
    for _ in range(8):
        tasks.extend([
            asyncio.create_task(ModelClient(provider)._post("/responses", {})),
            asyncio.create_task(_create_admitted_response(provider, None, hook, sdk, {})),
            asyncio.create_task(gateway.proxy("responses/compact", request({}))),
        ])
    await asyncio.wait_for(full.wait(), 1)
    await asyncio.sleep(0)
    assert entered == 16 and guard.active == 16
    release.set()
    await asyncio.wait_for(asyncio.gather(*tasks), 2)
    assert peak == 16 and guard.active == 0 and len(guard._sends) == 24


async def test_stream_downstream_send_failure_closes_suspended_iterator(provider, monkeypatch):
    grant = grant_for(provider, monkeypatch)
    guard = limits.provider_guard(provider)
    closed = []

    class Stream(httpx.AsyncByteStream):
        async def __aiter__(self):
            yield b"data: {}\n\n"
            await asyncio.Event().wait()

        async def aclose(self):
            closed.append(True)

    fake_http(monkeypatch, lambda req: httpx.Response(200, stream=Stream()))
    response = await gateway.proxy("responses", request({"stream": True}))

    async def send(message):
        if message["type"] == "http.response.body":
            raise OSError("downstream disconnected")

    async def receive():
        await asyncio.Event().wait()

    with pytest.raises(Exception):
        await response({"type": "http", "asgi": {"spec_version": "2.4"}}, receive, send)
    assert guard.active == 0 and closed and grant.ledger.calls == 1


def test_capacity_shared_with_separate_testclient_event_loops(provider, monkeypatch):
    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    grant_for(provider, monkeypatch)
    fake_http(monkeypatch, lambda req: httpx.Response(200, json={}))
    app = FastAPI()
    app.include_router(gateway.router)
    asyncio.run(ModelClient(provider)._post("/responses", {}))
    for _ in range(2):
        with TestClient(app) as client:
            assert client.post("/gateway/v1/responses/compact", json={}).status_code == 200
    guard = limits.provider_guard(provider)
    assert guard.active == 0 and len(guard._sends) == 3
