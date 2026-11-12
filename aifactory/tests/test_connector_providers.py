import httpx
import pytest

from aifactory.connectors.core import ConnectorError, store_credential
from aifactory.connectors.providers import resolve_token
from aifactory.db import session_scope
from aifactory.models import Event, Secret
from aifactory.security import encrypt
from tests.test_connector_core import setup_connector


@pytest.fixture
def mock_http(monkeypatch):
    original = httpx.AsyncClient
    calls = []

    def install(handler):
        def wrapped(request):
            calls.append(request)
            return handler(request)

        def client(**kwargs):
            assert kwargs["follow_redirects"] is False
            assert kwargs["timeout"].connect == 3 and kwargs["timeout"].read == 10
            return original(transport=httpx.MockTransport(wrapped), **kwargs)

        monkeypatch.setattr(httpx, "AsyncClient", client)
        return calls
    return install


@pytest.mark.parametrize("provider,opt_in", [("workspace", False), ("workspace", True),
                                             ("mindshub", False), ("mindshub", "true")])
async def test_no_oauth_without_both_opt_ins(mock_http, provider, opt_in):
    def forbidden(request):
        pytest.fail("MindsHub must not be called")

    calls = mock_http(forbidden)
    with session_scope() as db:
        ws, connector, _ = setup_connector(db, opted_in=opt_in)
        connector.provider = provider
        store_credential(db, connector, "slack_bot_token", "local-token")
        db.add(Secret(workspace_id=ws.id, kind="mindshub_api_key", ciphertext=encrypt("workspace-key")))
        result = await resolve_token(db, ws.id, connector)
        assert result.token == "local-token" and result.provider == "workspace"
        assert result.fallback_reason == ("oauth_disabled" if provider == "mindshub" else None)
        assert "local-token" not in repr(result)
        assert calls == []


@pytest.mark.parametrize("service,kind", [("slack", "slack_bot_token"), ("sentry", "sentry_token")])
async def test_oauth_success_transient_and_fixed_destination(mock_http, monkeypatch, service, kind):
    monkeypatch.setenv("MINDSHUB_AUTH_URL", "https://attacker.invalid")
    monkeypatch.setenv("AIFACTORY_BOOTSTRAP_MINDSHUB_API_KEY", "bootstrap-key")

    def success(request):
        assert str(request.url) == f"https://auth.mindshub.ai/v1/oauth/{service}/token"
        assert request.method == "POST"
        assert request.headers["Authorization"] == "Bearer workspace-key"
        return httpx.Response(200, json={"access_token": "ephemeral-token", "expires_in": 300})

    calls = mock_http(success)
    with session_scope() as db:
        ws, connector, _ = setup_connector(db, kind=service, opted_in=True)
        connector.provider = "mindshub"
        store_credential(db, connector, kind, "local-token")
        db.add(Secret(workspace_id=ws.id, kind="mindshub_api_key", ciphertext=encrypt("workspace-key")))
        result = await resolve_token(db, ws.id, connector)
        assert result.require_token() == "ephemeral-token"
        assert result.provider == "mindshub" and result.status == "ready" and not result.fallback_reason
        assert "ephemeral-token" not in repr(result)
        assert "ephemeral-token" not in repr((connector.config, connector.runtime_state, connector.status_detail))
        assert db.query(Secret).count() == 2 and db.query(Event).count() == 0
        # No cached/persisted OAuth token is reused on a second call.
        await resolve_token(db, ws.id, connector)
        assert len(calls) == 2


CASES = [
    ("401", "oauth_unauthorized"), ("404", "missing_connection"), ("409", "missing_connection"),
    ("500", "oauth_http"), ("302", "oauth_http"), ("timeout", "oauth_timeout"),
    ("network", "oauth_network"), ("malformed", "oauth_response"),
    ("empty", "oauth_response"), ("list", "oauth_response"), ("no_expiry", "oauth_response"),
    ("long_expiry", "oauth_response"), ("bool_expiry", "oauth_response"),
    ("nan_expiry", "oauth_response"), ("bad_token", "oauth_response"), ("large", "oauth_response"),
]


@pytest.mark.parametrize("case,reason", CASES)
@pytest.mark.parametrize("fallback", [True, False])
async def test_explicit_fallback_and_actionable_failure(mock_http, case, reason, fallback):
    def response(request):
        if case.isdecimal():
            return httpx.Response(int(case), text="arbitrary-signing-secret")
        if case == "timeout":
            raise httpx.ReadTimeout("arbitrary-signing-secret", request=request)
        if case == "network":
            raise httpx.ConnectError("arbitrary-signing-secret", request=request)
        if case == "malformed":
            return httpx.Response(200, text="arbitrary-signing-secret")
        if case == "large":
            return httpx.Response(200, content=b"x" * 20000)
        data = {
            "empty": {"access_token": "", "expires_in": 300},
            "list": [],
            "no_expiry": {"access_token": "oauth-secret"},
            "long_expiry": {"access_token": "oauth-secret", "expires_in": 999999},
            "bool_expiry": {"access_token": "oauth-secret", "expires_in": True},
            "nan_expiry": {"access_token": "oauth-secret", "expires_in": "NaN"},
            "bad_token": {"access_token": "oauth-secret\r\n", "expires_in": 300},
        }[case]
        return httpx.Response(200, json=data)

    mock_http(response)
    with session_scope() as db:
        ws, connector, _ = setup_connector(db, opted_in=True)
        connector.provider = "mindshub"
        db.add(Secret(workspace_id=ws.id, kind="mindshub_api_key", ciphertext=encrypt("workspace-key")))
        if fallback:
            store_credential(db, connector, "slack_bot_token", "local-token")
        result = await resolve_token(db, ws.id, connector)
        assert "secret" not in repr(result) and "secret" not in result.detail
        assert db.query(Event).count() == 0
        if fallback:
            assert result.require_token() == "local-token"
            assert result.provider == "workspace" and result.fallback_reason == reason
            assert "Using workspace token" in result.detail
        else:
            assert result.token is None and result.status == reason
            with pytest.raises(ConnectorError) as exc:
                result.require_token()
            assert "secret" not in str(exc.value)


@pytest.mark.parametrize("fallback", [True, False])
async def test_missing_workspace_key_never_uses_bootstrap_or_other_workspace(mock_http, monkeypatch, fallback):
    monkeypatch.setenv("AIFACTORY_BOOTSTRAP_MINDSHUB_API_KEY", "bootstrap-key")

    def forbidden(request):
        pytest.fail("Never use bootstrap or another workspace's key")

    calls = mock_http(forbidden)
    with session_scope() as db:
        other, _, _ = setup_connector(db)
        db.add(Secret(workspace_id=other.id, kind="mindshub_api_key", ciphertext=encrypt("other-key")))
        ws, connector, _ = setup_connector(db, opted_in=True)
        connector.provider = "mindshub"
        if fallback:
            store_credential(db, connector, "slack_bot_token", "local-token")
        result = await resolve_token(db, ws.id, connector)
        assert result.fallback_reason == ("missing_key" if fallback else None)
        assert result.status == ("ready" if fallback else "missing_key")
        assert calls == []


async def test_missing_token_and_corrupt_credential():
    with session_scope() as db:
        ws, connector, _ = setup_connector(db)
        result = await resolve_token(db, ws.id, connector)
        assert result.status == "missing_token"
        with pytest.raises(ConnectorError, match="Save a workspace credential"):
            result.require_token()
        db.add(Secret(workspace_id=ws.id, kind="slack_bot_token", ciphertext="not-encrypted"))
        assert (await resolve_token(db, ws.id, connector)).status == "unreadable_secret"
        other, _, _ = setup_connector(db)
        with pytest.raises(ConnectorError):
            await resolve_token(db, other.id, connector)


async def test_oauth_rejects_huge_expiry_without_crashing(mock_http):
    mock_http(lambda request: httpx.Response(200, json={"access_token": "token", "expires_in": 10**400}))
    with session_scope() as db:
        ws, connector, _ = setup_connector(db, opted_in=True)
        connector.provider = "mindshub"
        db.add(Secret(workspace_id=ws.id, kind="mindshub_api_key", ciphertext=encrypt("workspace-key")))
        assert (await resolve_token(db, ws.id, connector)).status == "oauth_response"
