"""Transport/adapter contracts: no provider, Anton SDK, or Codex CLI required."""
from __future__ import annotations

import asyncio
import json
import sys
from dataclasses import dataclass
from types import ModuleType, SimpleNamespace

import httpx
import pytest
from starlette.requests import Request

from aifactory import gateway, inference_limits
from aifactory.harness import anton, codex
from aifactory.harness.base import HarnessFailed
from aifactory.harness.native import NativeHarness
from aifactory.inference import BudgetExceeded, InferenceError, ModelClient, ProviderConfig, UsageLedger
from aifactory.provider_errors import (
    BILLING_CODES, MAX_ERROR_BODY, ProviderStop, classify_provider_error, read_error_body,
)

from .test_harness import make_session
from .test_scorers import judge, reservation  # shared scorer record fixture
from .test_director import world as director_world
from .test_lead import world
from .test_decision_judge_runner import data, model  # shared decision judge record fixtures


# Mapping and documentation are recorded in provider_errors.py. Intentionally no
# invented MindsHub code aliases or generic quota/rate-limit message matching.
STOPS = [
    ("mindshub", 402, {"detail": "your MindsHub credits are used up"}),
    ("openai", 402, "Payment Required"),
    ("openai", 429, {"error": {"code": "insufficient_quota", "message": "Check billing"}}),
    ("openai", 429, {"error": {"type": "insufficient_quota", "message": "Check billing"}}),
    ("openai", 400, {"error": {"code": "insufficient_quota", "message": "Check billing"}}),
]


def provider_for(name):
    # Not a recognisable sk-/mdb_ pattern: redaction must use the real key.
    return ProviderConfig(name, "https://provider.test/v1", "nonstandard-private-key-value", name)


def mock_http(monkeypatch, handler):
    real = httpx.AsyncClient
    monkeypatch.setattr(httpx, "AsyncClient", lambda **kw: real(transport=httpx.MockTransport(handler), **kw))


def request(body=None, method="POST", headers=None):
    async def receive():
        return {"type": "http.request", "body": json.dumps(body or {}).encode()}
    return Request({"type": "http", "method": method, "headers": headers or []}, receive)


def grant_for(provider, monkeypatch):
    ledger = UsageLedger("r", {})
    monkeypatch.setattr(ledger, "persist", lambda: None)
    grant = gateway.GatewayGrant("r", "workspace", "m", provider, ledger)
    monkeypatch.setattr(gateway, "_authenticate", lambda req: grant)
    monkeypatch.setattr(gateway, "_grants", {"r": grant})
    return grant


class SDKError(Exception):
    def __init__(self, status, body):
        super().__init__("SDK exception text must not be used for diagnostics")
        self.status_code = status
        self.body = body


@pytest.mark.parametrize("name,status,body", STOPS)
async def test_same_stop_on_direct_anton_gateway_without_retries(monkeypatch, name, status, body):
    provider = provider_for(name)
    detected = []
    provider.on_provider_stop = lambda stop, run_id: detected.append((stop, run_id))
    sent = []

    def upstream(req):
        sent.append(req)
        return httpx.Response(status, json=body)

    async def no_backoff(*args):
        pytest.fail("billing stops must not back off")

    mock_http(monkeypatch, upstream)
    monkeypatch.setattr(asyncio, "sleep", no_backoff)
    grant = grant_for(provider, monkeypatch)
    with pytest.raises(ProviderStop) as direct:
        await ModelClient(provider, session_id="r")._post("/responses", {})

    class Responses:
        async def create(self, **body):
            raise SDKError(status, globals_body)

    globals_body = body
    sdk = SimpleNamespace(_client=SimpleNamespace(responses=Responses()))
    repairs = []

    async def repair_hook(p, body):
        try:
            return await p._client.responses.create(**body)
        except Exception:
            repairs.append(True)
            return await p._client.responses.create(**body)

    with pytest.raises(anton._ProviderStopped) as sdk_stop:
        await anton._create_admitted_response(provider, "r", repair_hook, sdk, {})
    assert not repairs
    response = await gateway.proxy("responses", request({"stream": True}))
    assert response.status_code == 400
    assert json.loads(response.body)["error"]["code"] == "provider_billing_stop"
    assert grant.provider_stop.diagnostics() == direct.value.diagnostics() == sdk_stop.value.stop.diagnostics()
    assert len(sent) == 2  # One direct and one gateway request, no SDK HTTP here.
    assert len(detected) == 3 and all(run == "r" for _, run in detected)
    assert grant.ledger.calls == 0
    assert inference_limits.provider_guard(provider).active == 0
    # A misbehaving CLI cannot cause more upstream sends after the terminal 400.
    assert (await gateway.proxy("responses", request())).status_code == 400
    assert len(sent) == 2 and len(detected) == 3
    with pytest.raises(ProviderStop) as host:
        gateway.raise_if_provider_stopped("r")
    assert host.value is grant.provider_stop


@pytest.mark.parametrize("status,body", [
    (401, {"error": {"code": "insufficient_quota"}}),
    (403, {"error": {"code": "insufficient_quota"}}),
    (400, {"error": {"code": "invalid_request_error", "message": "quota parameter invalid"}}),
    (422, {"error": {"message": "billing field missing"}}),
    (429, {"error": {"code": "rate_limit_exceeded", "message": "quota per minute reached"}}),
    (429, {"error": {"code": "quota_exceeded"}}),
    (500, {"error": {"code": "insufficient_quota"}}),
])
async def test_nonbilling_errors_keep_existing_types(monkeypatch, status, body):
    provider = provider_for("openai")
    provider.on_provider_stop = lambda *_: pytest.fail("not a billing stop")
    mock_http(monkeypatch, lambda req: httpx.Response(status, json=body, headers={"x-should-retry": "false"}))
    with pytest.raises(InferenceError) as direct:
        await ModelClient(provider, max_retries=0)._post("/responses", {})
    assert direct.value.status == status
    error = SDKError(status, body)

    class Responses:
        async def create(self, **_):
            raise error

    with pytest.raises(SDKError) as sdk:
        await anton._AdmittedResponses(Responses(), provider, None,
                                       retries=lambda: anton.Retries(limit=0)).create()
    assert sdk.value is error
    grant = grant_for(provider, monkeypatch)
    response = await gateway.proxy("responses", request())
    assert response.status_code in (400, 502)
    assert grant.provider_stop is None


def test_explicit_mapping_and_no_message_guessing():
    assert BILLING_CODES == {"mindshub": frozenset(), "openai": {"insufficient_quota"}}
    for name in BILLING_CODES:
        provider = provider_for(name)
        assert classify_provider_error(provider, 429, {"message": "credits used up billing quota"}) is None
    assert classify_provider_error(provider_for("mindshub"), 429,
                                   {"error": {"code": "insufficient_quota"}}) is None


@pytest.mark.parametrize("field", ["message", "detail"])
@pytest.mark.parametrize("boundary", [500, 800])
async def test_redacts_complete_upstream_message_before_truncation(monkeypatch, field, boundary):
    provider = provider_for("openai")
    # The key straddles the old 500/800 character diagnostic truncation boundary.
    body = {"error": {field: "x" * (boundary - 10) + provider.api_key + " suffix",
                      "code": provider.api_key, "type": "insufficient_quota",
                      "credential_fingerprint": "do-not-expose-fingerprint"}}
    stop = classify_provider_error(provider, 429, body)
    serialized = json.dumps(stop.diagnostics()) + str(stop) + repr(stop)
    assert provider.api_key not in serialized
    assert provider.api_key[:10] not in serialized
    assert "do-not-expose-fingerprint" not in serialized
    assert stop.code == "insufficient_quota"
    mock_http(monkeypatch, lambda req: httpx.Response(402, json=body))
    grant = grant_for(provider, monkeypatch)
    response = await gateway.proxy("responses", request())
    assert provider.api_key[:10] not in response.body.decode()
    assert "do-not-expose-fingerprint" not in response.body.decode()
    assert grant.provider_stop.status == 402


async def test_gateway_redacts_nonbilling_and_get_errors(monkeypatch):
    provider = provider_for("mindshub")
    grant = grant_for(provider, monkeypatch)
    mock_http(monkeypatch, lambda req: httpx.Response(401, json={"error": {"message": provider.api_key}}))
    for method in ("POST", "GET"):
        response = await gateway.proxy("responses" if method == "POST" else "models", request(method=method))
        assert response.status_code == 400
        assert provider.api_key not in response.body.decode()
        assert "[redacted]" in response.body.decode()
        assert grant.provider_stop is None


async def test_bounded_streamed_error_discards_partial_credentials():
    class Stream(httpx.AsyncByteStream):
        async def __aiter__(self):
            yield b"x" * (MAX_ERROR_BODY - 4)
            yield b"private-credential-straddling-boundary"
            pytest.fail("must stop reading an oversized upstream body")

    response = httpx.Response(402, stream=Stream())
    body = await read_error_body(response)
    assert b"private" not in body and len(body) < 100
    stop = classify_provider_error(provider_for("mindshub"), 402, body)
    assert stop is not None


async def test_transport_and_factory_budget_are_distinct(monkeypatch, tmp_path):
    provider = provider_for("openai")
    provider.on_provider_stop = lambda *_: pytest.fail("not a billing stop")

    def offline(req):
        raise httpx.ConnectError("offline")

    mock_http(monkeypatch, offline)
    with pytest.raises(InferenceError) as direct:
        await ModelClient(provider, max_retries=0)._post("/responses", {})
    assert direct.value.retryable and direct.value.status is None
    grant = grant_for(provider, monkeypatch)
    grant.ledger.max_tokens = 0
    response = await gateway.proxy("responses", request())
    assert json.loads(response.body)["error"]["code"] == "budget_exhausted"
    assert grant.provider_stop is None
    session, _, _ = make_session(tmp_path, lambda _: None)
    session.client = ModelClient(provider)
    session.ledger.max_tokens = 0
    with pytest.raises(BudgetExceeded):
        await session.client.chat(model="m", messages=[], ledger=session.ledger)
    with pytest.raises(HarnessFailed) as native:
        await NativeHarness().run(session)
    assert isinstance(native.value.__cause__, BudgetExceeded)


async def test_native_preserves_stop_identity(tmp_path):
    stop = classify_provider_error(provider_for("mindshub"), 402, "credits used up")

    def fail(_):
        raise stop

    session, client, _ = make_session(tmp_path, fail)
    with pytest.raises(ProviderStop) as caught:
        await NativeHarness().run(session)
    assert caught.value is stop and len(client.seen) == 1


@pytest.mark.parametrize("exec_raises", [False, True])
async def test_codex_recovers_host_stop_without_cli(tmp_path, monkeypatch, exec_raises):
    session, _, _ = make_session(tmp_path, lambda _: None, gateway_url="http://gateway/v1", gateway_token="run-token")
    provider = provider_for("mindshub")
    grant = grant_for(provider, monkeypatch)
    monkeypatch.setattr(codex.CodexHarness, "availability", lambda _: (True, "fake"))
    mock_http(monkeypatch, lambda req: httpx.Response(402, text="credits used up"))

    async def write(*_):
        pass

    async def execute(*args, **kwargs):
        assert (await gateway.proxy("responses", request())).status_code == 400
        if exec_raises:
            raise RuntimeError("sandbox process failed")
        return SimpleNamespace(timed_out=True)  # Stop takes precedence over a generic failure.

    monkeypatch.setattr(codex, "write_text", write)
    monkeypatch.setattr(session.sandbox, "exec", execute)
    with pytest.raises(ProviderStop) as caught:
        await codex.CodexHarness().run(session)
    assert caught.value is grant.provider_stop


def fake_anton_sdk(monkeypatch, status, body):
    """Minimal SDK surface exercises the real AntonHarness.run host boundary."""
    counts = {"sends": 0, "repairs": 0, "turns": 0, "closed": 0}

    class Responses:
        async def create(self, **_):
            counts["sends"] += 1
            raise SDKError(status, body)

    class SDKClient:
        def __init__(self):
            self.responses = Responses()
            self._client = SimpleNamespace(follow_redirects=True)

        def with_options(self, **kwargs):
            assert kwargs["max_retries"] == 0
            return self

    class Provider:
        FLAVOR_OPENAI = "openai"

        def __init__(self, **_):
            self._client = SDKClient()

        async def _create_response(self, body):
            try:
                return await self._client.responses.create(**body)
            except Exception:
                counts["repairs"] += 1
                return await self._client.responses.create(**body)

    class Client:
        def __init__(self, **kwargs):
            self.provider = kwargs["planning_provider"]

        async def aclose(self):
            counts["closed"] += 1

    class Settings:
        @classmethod
        def model_construct(cls, **_):
            return cls()

        def model_copy(self, **_):
            return self

    class Chat:
        def __init__(self, config):
            self.provider = config.llm_client.provider

        async def turn_stream(self, *_args, **_kwargs):
            counts["turns"] += 1
            await self.provider._create_response({})
            yield {}  # async generator, not a coroutine

        async def close(self):
            counts["closed"] += 1

    @dataclass
    class Tool:
        name: str
        description: str
        input_schema: dict
        handler: object

    modules = {
        "anton.core.llm.client": {"LLMClient": Client},
        "anton.core.llm.openai": {"OpenAIProvider": Provider},
        "anton.core.llm.provider": {"LLMProvider": SimpleNamespace(), "ProviderConnectionInfo": SimpleNamespace},
        "anton.core.llm.prompt_builder": {"SystemPromptContext": SimpleNamespace},
        "anton.core.session": {"ChatSession": Chat, "ChatSessionConfig": SimpleNamespace},
        "anton.core.settings": {"CoreSettings": Settings},
        "anton.core.tools.tool_defs": {"ToolDef": Tool},
    }
    for name, attributes in modules.items():
        module = ModuleType(name)
        module.__dict__.update(attributes)
        monkeypatch.setitem(sys.modules, name, module)
    monkeypatch.setattr(anton.AntonHarness, "availability", lambda _: (True, "stub SDK"))
    monkeypatch.setattr(anton, "replace_session_tools", lambda tools, _: tools)
    monkeypatch.setattr(anton, "sandbox_web_tools", lambda _: [])
    return counts


@pytest.mark.parametrize("name,status,body", STOPS)
async def test_anton_host_boundary_unwraps_and_skips_continuations(tmp_path, monkeypatch, name, status, body):
    counts = fake_anton_sdk(monkeypatch, status, body)
    provider = provider_for(name)
    detections = []
    provider.on_provider_stop = lambda *args: detections.append(args)
    session, _, _ = make_session(tmp_path, lambda _: None)
    session.client = ModelClient(provider)
    with pytest.raises(ProviderStop) as caught:
        await anton.AntonHarness().run(session)
    assert caught.value is detections[0][0]
    assert counts == {"sends": 1, "repairs": 0, "turns": 1, "closed": 2}
    assert session.ledger.calls == 0


async def test_anton_ordinary_rate_limit_keeps_admitted_retry_behavior(monkeypatch):
    provider = provider_for("openai")
    provider.on_provider_stop = lambda *_: pytest.fail("rate limits are not account stops")
    sends = []

    class Responses:
        async def create(self, **_):
            sends.append(True)
            if len(sends) == 1:
                raise SDKError(429, {"error": {"code": "rate_limit_exceeded"}})
            return {"output": []}

    sdk = SimpleNamespace(_client=SimpleNamespace(responses=Responses()))

    async def retry(p, body):
        try:
            return await p._client.responses.create(**body)
        except SDKError:
            assert inference_limits.provider_guard(provider).active == 0
            return await p._client.responses.create(**body)

    waits = []

    class Retries:
        async def wait(self, what, delay):
            assert inference_limits.provider_guard(provider).active == 0
            waits.append(what)
            return True

        def report(self, ok):
            assert ok

    assert await anton._create_admitted_response(provider, None, retry, sdk, {}, Retries) == {"output": []}
    assert waits == ["429"]
    assert len(sends) == 2 and inference_limits.provider_guard(provider).active == 0


@pytest.mark.parametrize("method,path", [("POST", "responses"), ("GET", "models")])
async def test_only_authenticated_grant_retains_stop(monkeypatch, method, path):
    from fastapi import FastAPI

    app = FastAPI()
    app.include_router(gateway.router)
    provider = provider_for("mindshub")
    ledger = UsageLedger("test-run", {})
    grant = gateway.GatewayGrant("test-run", "workspace", "m", provider, ledger)
    monkeypatch.setattr(gateway, "_grants", {})
    token = gateway.issue(grant)
    other = gateway.GatewayGrant("other-run", "workspace", "m", provider, UsageLedger("other-run", {}))
    gateway.issue(other)
    sends = []

    def upstream(req):
        sends.append(True)
        return httpx.Response(402, json={"error": {"message": "credits used up " + provider.api_key}})

    # Keep the ASGI test client independent of the patched upstream constructor.
    real = httpx.AsyncClient
    mock_http(monkeypatch, upstream)
    async with real(transport=httpx.ASGITransport(app), base_url="http://factory") as client:
        url = "/gateway/v1/" + path
        for headers in ({}, {"Authorization": "Bearer invalid"}):
            response = await client.request(method, url, headers=headers)
            assert response.status_code == 401
            assert grant.provider_stop is None and not sends
        response = await client.request(method, url, headers={"Authorization": "Bearer " + token}, json={})
        assert response.status_code == 400
        assert response.json()["error"]["code"] == "provider_billing_stop"
        assert provider.api_key not in response.text
        assert token not in response.text
        assert grant.provider_stop.status == 402 and other.provider_stop is None
        gateway.revoke(grant.run_id)
        response = await client.request(method, url, headers={"Authorization": "Bearer " + token}, json={})
        assert response.status_code == 401
        assert len(sends) == 1


@pytest.mark.parametrize("wire", ["responses", "chat"])
@pytest.mark.parametrize("status,body", [(402, "your credits are used up"), (429, '{"error":{"code":"insufficient_quota"}}')])
async def test_native_transport_preserves_stop_without_optional_parameter_repair(tmp_path, monkeypatch, wire, status, body):
    provider = provider_for("openai")
    sent = []
    detections = []
    provider.on_provider_stop = lambda stop, run: detections.append(stop)

    def upstream(req):
        sent.append(req)
        return httpx.Response(status, text=body)

    mock_http(monkeypatch, upstream)
    session, _, _ = make_session(tmp_path, lambda _: None)
    session.client = ModelClient(provider, wire=wire, session_id=session.run_id)
    with pytest.raises(ProviderStop) as caught:
        await NativeHarness().run(session)
    assert len(sent) == 1 and caught.value is detections[0]
    assert session.ledger.calls == 0


async def test_redacts_direct_error_and_log_with_actual_short_key(monkeypatch, caplog):
    provider = provider_for("openai")
    provider.api_key = "q7!z"
    body = {"error": {"message": "rejected credential " + provider.api_key,
                      "code": provider.api_key, "credentialFingerprint": "private-hash"}}
    mock_http(monkeypatch, lambda req: httpx.Response(400, json=body))
    with pytest.raises(InferenceError) as caught:
        await ModelClient(provider)._post("/responses", {})
    assert provider.api_key not in str(caught.value) + caplog.text
    assert "private-hash" not in str(caught.value) + caplog.text
    assert "[redacted]" in str(caught.value)


@pytest.mark.parametrize("body", [b"not json", b"{broken", b"[" * 2000, b'\xff\xfe'])
def test_malformed_billing_body_still_stops_without_echoing_unsafe_fields(body):
    stop = classify_provider_error(provider_for("mindshub"), 402, body)
    assert stop.status == 402 and stop.code is None


@pytest.mark.parametrize("name,status,body", STOPS)
async def test_scorer_stop_is_rejected_known_zero_and_propagates(judge, monkeypatch, name, status, body):
    from aifactory import scorers
    from aifactory.db import session_scope
    from aifactory.failures import failure_cause
    from aifactory.models import ScorerAttempt, ScorerResult
    from aifactory.pipeline.runtime import outcome_cost

    provider = provider_for(name)
    detected = []
    provider.on_provider_stop = lambda stop, run: detected.append((stop, run))
    monkeypatch.setattr(scorers, "resolve_provider", lambda _: provider)
    monkeypatch.setattr(scorers, "ModelClient", ModelClient)
    sends = []

    def upstream(req):
        sends.append(req)
        return httpx.Response(status, json=body)

    mock_http(monkeypatch, upstream)
    result_id = reservation(judge)
    with pytest.raises(ProviderStop) as caught:
        await scorers.execute(result_id)
    assert len(sends) == 1 and caught.value is detected[0][0]
    assert failure_cause(caught.value) == "provider"
    with session_scope() as s:
        attempt = s.query(ScorerAttempt).filter_by(result_id=result_id).one()
        result = s.get(ScorerResult, result_id)
        assert detected[0][1] == attempt.id
        assert attempt.finished_at and attempt.cost_known and attempt.cost_usd == 0
        assert attempt.input_tokens == attempt.output_tokens == 0
        assert result.status == "error" and result.cost_known and result.cost_usd == 0
        assert provider.api_key not in result.error
    assert outcome_cost(judge["outcome"]) == (1, True)


@pytest.mark.parametrize("name,status,body", STOPS)
async def test_lead_stop_keeps_classification_and_zero_cost(world, monkeypatch, name, status, body):
    from aifactory.db import session_scope
    from aifactory.models import Run
    from aifactory.pipeline import lead

    provider = provider_for(name)
    detected = []
    provider.on_provider_stop = lambda stop, run: detected.append((stop, run))
    monkeypatch.setattr(lead, "resolve_provider", lambda _: provider)
    mock_http(monkeypatch, lambda req: httpx.Response(status, json=body))
    with pytest.raises(ProviderStop) as caught:
        await lead.run_lead(world["ws"], world["outcome"], ["test"], {})
    assert caught.value is detected[0][0]
    with session_scope() as s:
        row = s.get(Run, detected[0][1])
        assert row.finished_at and row.turns == 0
        assert row.cost_known and row.cost_usd == 0
        assert row.error == f"Model unavailable or request rejected (status {status})"
        assert provider.api_key not in row.error


@pytest.mark.parametrize("name,status,body", STOPS)
async def test_decision_judge_stop_keeps_classification_and_zero_cost(data, model, monkeypatch, name, status, body):
    from aifactory import decision_judge_runner as runner
    from aifactory.db import session_scope
    from aifactory.models import Run

    provider = provider_for(name)
    detected = []
    provider.on_provider_stop = lambda stop, run: detected.append((stop, run))
    monkeypatch.setattr(runner, "resolve_provider", lambda _: provider)
    monkeypatch.setattr(runner, "ModelClient", ModelClient)
    mock_http(monkeypatch, lambda req: httpx.Response(status, json=body))
    with pytest.raises(ProviderStop) as caught:
        await runner.run_judge(data[0], data[-1])
    assert caught.value is detected[0][0]
    with session_scope() as s:
        row = s.get(Run, detected[0][1])
        assert row.finished_at and row.turns == 0
        assert row.cost_known and row.cost_usd == 0
        assert row.error == f"Judge provider unavailable or request rejected (status {status})"
        assert row.result["verdict"] == "hold"
        assert provider.api_key not in row.error


async def test_run_agent_propagates_stop_after_accounting_and_gateway_cleanup(judge, monkeypatch):
    from aifactory.db import session_scope
    from aifactory.models import Run
    from aifactory.pipeline import runtime

    stop = classify_provider_error(provider_for("openai"), 429, {"error": {"code": "insufficient_quota"}})

    class Harness:
        label = "Test Codex"

        async def run(self, session):
            gateway._grants[session.run_id].provider_stop = stop
            gateway.raise_if_provider_stopped(session.run_id)

    async def attachments(*args):
        return "", []

    monkeypatch.setattr(runtime, "resolve_provider", lambda _: provider_for("openai"))
    monkeypatch.setattr(runtime, "get_harness", lambda _: Harness())
    monkeypatch.setattr(runtime, "prepare_attachments", attachments)
    monkeypatch.setattr(gateway, "_grants", {})
    spec = runtime.SessionSpec(judge["ws"], judge["outcome"], None, "planner", "codex", "prompt", "system", None)
    with pytest.raises(ProviderStop) as caught:
        await runtime.run_agent(spec, asyncio.Event())
    assert caught.value is stop and not gateway._grants
    with session_scope() as s:
        row = s.query(Run).filter_by(harness="codex").one()
        assert row.finished_at and row.cost_known and row.cost_usd == 0
        assert row.config_snapshot["failure"] == {"cause": "provider", "status": 429}


@pytest.mark.parametrize("status,body", [
    (402, {"detail": "credits used up"}),
    (429, {"error": {"code": "insufficient_quota"}}),
])
async def test_anton_status_and_body_fallback_to_http_response(monkeypatch, status, body):
    provider = provider_for("openai")
    detections = []
    provider.on_provider_stop = lambda stop, run: detections.append((stop, run))
    message = "x" * 490 + provider.api_key
    body = {**body, "error": {**body.get("error", {}), "message": message}}

    class Responses:
        async def create(self, **_):
            error = RuntimeError("SDK wrapper text is not a public diagnostic")
            error.response = httpx.Response(status, json=body)
            raise error

    with pytest.raises(anton._ProviderStopped) as caught:
        await anton._AdmittedResponses(Responses(), provider, "run").create()
    stop = caught.value.stop
    assert detections == [(stop, "run")]
    assert stop.status == status and stop.provider == "openai"
    assert provider.api_key[:10] not in str(stop)
    assert "SDK wrapper" not in str(stop)
    assert inference_limits.provider_guard(provider).active == 0


@pytest.mark.parametrize("name,status,body", STOPS)
async def test_director_stop_keeps_classification_and_zero_cost(director_world, monkeypatch, name, status, body):
    from aifactory.db import session_scope
    from aifactory.models import Event
    from aifactory.pipeline import director

    provider = provider_for(name)
    detected = []
    provider.on_provider_stop = lambda stop, run: detected.append((stop, run))
    monkeypatch.setattr(director, "resolve_provider", lambda _: provider)
    mock_http(monkeypatch, lambda req: httpx.Response(status, json=body))
    item = next(item for item in director.due() if item["workspace_id"] == director_world["ws"])
    with pytest.raises(ProviderStop) as caught:
        await director.run_director(item)
    assert caught.value is detected[0][0]
    with session_scope() as s:
        row = s.query(Event).filter_by(kind="director_run_finished").one()
        assert row.data["run_id"] == detected[0][1]
        assert row.data["turns"] == 0 and row.data["cost_known"] and row.data["cost_usd"] == 0
        assert row.data["reason"] == f"Model unavailable or request rejected (status {status})"
        assert provider.api_key not in row.message


def resolved_workspace_provider(workspace_id, name="openai"):
    """Exercise production resolution, not a test-supplied stop callback."""
    from aifactory.db import session_scope
    from aifactory.inference import resolve_provider
    from aifactory.models import Secret, Workspace
    from aifactory.security import encrypt

    with session_scope() as s:
        ws = s.get(Workspace, workspace_id)
        ws.settings = {**(ws.settings or {}), "provider": name}
        s.add(Secret(workspace_id=workspace_id, kind=f"{name}_api_key",
                     ciphertext=encrypt("nonstandard-private-key-value")))
    return resolve_provider(workspace_id)


def assert_workspace_held(workspace_id, provider):
    from aifactory.db import session_scope
    from aifactory.models import Decision
    from aifactory.pipeline.holds import held_workspaces

    with session_scope() as s:
        hold = s.query(Decision).filter_by(workspace_id=workspace_id, kind="provider_hold", status="open").one()
        assert hold.outcome_id is None and hold.task_id is None
        assert hold.options[0]["value"] == "retry"
        assert provider.provider in hold.body and "billing/quota stop" in hold.body
        assert provider.api_key not in hold.body + json.dumps(hold.metadata_json)
        assert workspace_id in held_workspaces(s)
        return hold.id


@pytest.mark.parametrize("transport", ["direct", "anton", "gateway"])
@pytest.mark.parametrize("name,status,body", STOPS)
async def test_resolved_provider_persists_hold_at_detection(signed_in, monkeypatch, transport, name, status, body):
    workspace_id = signed_in["workspaces"][0]["id"]
    provider = resolved_workspace_provider(workspace_id, name)
    body = json.loads(json.dumps(body))
    if isinstance(body, dict):
        body.get("error", body)["message"] = "Check billing " + provider.api_key
    else:
        body = "No credits " + provider.api_key
    sends = []

    def upstream(req):
        sends.append(req)
        return httpx.Response(status, json=body)

    mock_http(monkeypatch, upstream)
    grant = grant_for(provider, monkeypatch)
    grant.workspace_id = workspace_id

    class Responses:
        async def create(self, **_):
            raise SDKError(status, body)

    async def detect():
        if transport == "gateway":
            response = await gateway.proxy("responses", request())
            assert response.status_code == 400
            assert grant.provider_stop.status == status
        elif transport == "anton":
            with pytest.raises(anton._ProviderStopped):
                await anton._AdmittedResponses(Responses(), provider, "r").create()
        else:
            with pytest.raises(ProviderStop):
                await ModelClient(provider)._post("/responses", {})

    await detect()
    hold_id = assert_workspace_held(workspace_id, provider)
    # Another in-flight consumer shares the idempotent workspace hold.
    await detect()
    assert assert_workspace_held(workspace_id, provider) == hold_id
    assert len(sends) == {"direct": 2, "anton": 0, "gateway": 1}[transport]


async def test_scorer_production_resolution_opens_hold_and_preserves_stop(judge, monkeypatch):
    from aifactory import scorers
    from aifactory.inference import resolve_provider

    provider = resolved_workspace_provider(judge["ws"])
    monkeypatch.setattr(scorers, "resolve_provider", resolve_provider)
    monkeypatch.setattr(scorers, "ModelClient", ModelClient)
    mock_http(monkeypatch, lambda req: httpx.Response(429, json={"error": {"code": "insufficient_quota"}}))
    with pytest.raises(ProviderStop):
        await scorers.execute(reservation(judge))
    assert_workspace_held(judge["ws"], provider)


async def test_lead_production_resolution_opens_hold_and_preserves_stop(world, monkeypatch):
    from aifactory.pipeline import lead

    provider = resolved_workspace_provider(world["ws"])
    mock_http(monkeypatch, lambda req: httpx.Response(402, text="No credits"))
    with pytest.raises(ProviderStop):
        await lead.run_lead(world["ws"], world["outcome"], ["test"], {})
    assert_workspace_held(world["ws"], provider)


async def test_director_production_resolution_opens_hold_and_preserves_stop(director_world, monkeypatch):
    from aifactory.pipeline import director

    provider = resolved_workspace_provider(director_world["ws"])
    mock_http(monkeypatch, lambda req: httpx.Response(402, text="No credits"))
    item = next(item for item in director.due() if item["workspace_id"] == director_world["ws"])
    with pytest.raises(ProviderStop):
        await director.run_director(item)
    assert_workspace_held(director_world["ws"], provider)
    assert not director.due()


@pytest.mark.parametrize("status,body", [
    (402, {"error": {"message": "No credits"}}),
    (429, {"error": {"code": "insufficient_quota"}}),
])
def test_manual_scorer_returns_structured_conflict_with_durable_hold(client, judge, monkeypatch, status, body):
    from aifactory import scorers
    from aifactory.db import session_scope
    from aifactory.inference import resolve_provider
    from aifactory.models import ScorerAttempt

    provider = resolved_workspace_provider(judge["ws"])
    monkeypatch.setattr(scorers, "resolve_provider", resolve_provider)
    monkeypatch.setattr(scorers, "ModelClient", ModelClient)
    body["error"]["message"] = "No credits " + provider.api_key
    mock_http(monkeypatch, lambda req: httpx.Response(status, json=body))
    response = client.post(f"/api/workspaces/{judge['ws']}/runs/{judge['run']}/scores",
                           json={"scorer_id": "planner-quality"})
    assert response.status_code == 409
    assert response.json()["detail"] == {
        "provider": "openai", "status": status, "code": body["error"].get("code"),
        "reason": "billing_quota", "message": "No credits [redacted]",
    }
    assert provider.api_key not in response.text
    assert_workspace_held(judge["ws"], provider)
    with session_scope() as s:
        attempt = s.query(ScorerAttempt).one()
        assert attempt.finished_at and attempt.cost_known and attempt.cost_usd == 0


@pytest.mark.parametrize("status,body", [
    (401, {"error": {"code": "insufficient_quota"}}),
    (403, {"error": {"code": "insufficient_quota"}}),
    (400, {"error": {"code": "invalid_request_error"}}),
    (429, {"error": {"code": "rate_limit_exceeded"}}),
])
async def test_resolved_provider_nonbilling_errors_do_not_open_hold(signed_in, monkeypatch, status, body):
    from aifactory.db import session_scope
    from aifactory.pipeline.holds import held_workspaces

    workspace_id = signed_in["workspaces"][0]["id"]
    provider = resolved_workspace_provider(workspace_id)
    mock_http(monkeypatch, lambda req: httpx.Response(status, json=body))
    with pytest.raises(InferenceError):
        await ModelClient(provider, max_retries=0)._post("/responses", {})
    with session_scope() as s:
        assert workspace_id not in held_workspaces(s)


async def test_decision_judge_production_resolution_opens_hold_and_preserves_stop(data, model, monkeypatch):
    from aifactory import decision_judge_runner as runner
    from aifactory.inference import resolve_provider

    provider = resolved_workspace_provider(data[0])
    monkeypatch.setattr(runner, "resolve_provider", resolve_provider)
    monkeypatch.setattr(runner, "ModelClient", ModelClient)
    mock_http(monkeypatch, lambda req: httpx.Response(402, text="No credits"))
    with pytest.raises(ProviderStop):
        await runner.run_judge(data[0], data[-1])
    assert_workspace_held(data[0], provider)


@pytest.mark.parametrize("name,status,body", STOPS)
async def test_catalog_preserves_stop_and_notifies_host(monkeypatch, name, status, body):
    provider = provider_for(name)
    detected, sent = [], []
    provider.on_provider_stop = lambda stop, run: detected.append((stop, run))

    def upstream(req):
        sent.append(req)
        assert req.method == "GET" and req.url.path == "/v1/models"
        return httpx.Response(status, json=body)

    mock_http(monkeypatch, upstream)
    with pytest.raises(ProviderStop) as caught:
        await ModelClient(provider, session_id="catalog").list_models()
    assert caught.value.diagnostics() == classify_provider_error(provider, status, body).diagnostics()
    assert detected == [(caught.value, "catalog")]
    assert len(sent) == 1


@pytest.mark.parametrize("name,status,body", STOPS)
async def test_catalog_stop_uses_definitive_credential_diagnostic(monkeypatch, name, status, body):
    from aifactory.api import _test_secret
    from aifactory.credential_diagnostics import MESSAGES

    key = "mdb_catalog-private-key" if name == "mindshub" else provider_for(name).api_key
    sent = []

    def upstream(req):
        sent.append(req)
        assert req.method == "GET"
        if req.url.path.endswith("/models"):
            error = dict(body.get("error", body)) if isinstance(body, dict) else {}
            error["message"] = "Upstream account diagnostic " + key
            return httpx.Response(status, json={"error": error})
        return httpx.Response(200, json={"user_id": "catalog-user", "organization_id": "catalog-org"})

    mock_http(monkeypatch, upstream)
    result, detail, metadata = await _test_secret(f"{name}_api_key", key)
    assert result == "invalid"
    assert detail == MESSAGES["inference_denied"]
    assert metadata["diagnostic_code"] == "inference_denied"
    assert metadata["test_status"] == "invalid"
    assert key not in detail + json.dumps(metadata)
    assert len(sent) == (2 if name == "mindshub" else 1)


@pytest.mark.parametrize("name,status,body", STOPS)
def test_models_endpoint_preserves_public_contract_on_catalog_stop(signed_in, client, monkeypatch, name, status, body):
    workspace_id = signed_in["workspaces"][0]["id"]
    provider = resolved_workspace_provider(workspace_id, name)
    error = dict(body.get("error", body)) if isinstance(body, dict) else {}
    error["message"] = "Upstream account diagnostic " + provider.api_key
    sent = []

    def upstream(req):
        sent.append(req)
        return httpx.Response(status, json={"error": error})

    mock_http(monkeypatch, upstream)
    response = client.get(f"/api/workspaces/{workspace_id}/models")
    assert response.status_code == 200
    assert response.json() == {"provider": None, "models": [], "error": "Model catalog request failed"}
    assert provider.api_key not in response.text
    assert "Upstream account diagnostic" not in response.text
    assert len(sent) == 1
    assert_workspace_held(workspace_id, provider)


def test_valid_identity_can_sign_in_with_exhausted_catalog_quota(client, monkeypatch):
    from aifactory.credential_diagnostics import MESSAGES
    from aifactory.db import session_scope
    from aifactory.models import Secret
    from aifactory.security import decrypt

    key = "mdb_catalog-private-key"
    sent = []

    def upstream(req):
        sent.append(req)
        assert req.method == "GET"
        if req.url.path.endswith("/models"):
            return httpx.Response(402, json={"error": {"message": "No credits " + key}})
        return httpx.Response(200, json={"user_id": "catalog-user", "organization_id": "catalog-org",
                                       "email": "catalog@example.com"})

    mock_http(monkeypatch, upstream)
    response = client.post("/auth/api-key", json={"key": key})
    assert response.status_code == 200 and response.json() == {"ok": True}
    me = client.get("/api/me")
    assert me.status_code == 200
    workspace_id = me.json()["workspaces"][0]["id"]
    with session_scope() as s:
        secret = s.query(Secret).filter_by(workspace_id=workspace_id, kind="mindshub_api_key").one()
        assert decrypt(secret.ciphertext) == key
        assert secret.status == "invalid"
        assert secret.status_detail == MESSAGES["inference_denied"]
        assert secret.metadata_json["diagnostic_code"] == "inference_denied"
        assert secret.metadata_json["test_status"] == "invalid"
        assert key not in secret.status_detail + json.dumps(secret.metadata_json)
    assert len(sent) == 2  # Identity and catalog only; no paid generation or retries.


@pytest.mark.parametrize("status,body", [
    (401, {"error": {"code": "insufficient_quota"}}),
    (403, {"error": {"code": "insufficient_quota"}}),
    (400, {"error": {"code": "invalid_request_error"}}),
    (429, {"error": {"code": "rate_limit_exceeded"}}),
])
async def test_catalog_nonbilling_errors_remain_inference_errors(monkeypatch, status, body):
    provider = provider_for("openai")
    provider.on_provider_stop = lambda *_: pytest.fail("not a billing stop")
    mock_http(monkeypatch, lambda req: httpx.Response(status, json=body))
    with pytest.raises(InferenceError) as caught:
        await ModelClient(provider).list_models()
    assert caught.value.status == status
    assert caught.value.retryable == (status == 429)
