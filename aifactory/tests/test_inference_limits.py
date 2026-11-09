"""Admission contracts without an installed agent SDK, CLI, or live provider."""
from __future__ import annotations

import asyncio
import json
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
