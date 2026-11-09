import json

import httpx
import pytest

from aifactory import api
from aifactory.db import session_scope
from aifactory.linear import Linear, LinearError, LinearProvider, resolve_token
from aifactory.models import Event, Membership, Secret
from aifactory.security import decrypt, redact
from .linear_fake import FakeLinear, connection


@pytest.mark.asyncio
async def test_provider_operations_and_variables():
    fake = FakeLinear()
    provider: LinearProvider = Linear("shortkey", transport=fake)
    assert (await provider.viewer())["name"] == "Fake Account"
    request, payload = fake.requests[-1]
    assert request.headers["Authorization"] == "shortkey"
    assert payload == {"query": "query { viewer { id name } }", "variables": {}}
    assert request.extensions["timeout"] == {"connect": 5, "read": 15, "write": 15, "pool": 15}
    fake.data = {"teams": connection([{"id": "t", "name": "Team"}], "next")}
    def page(request):
        response = fake.handle(request)
        fake.data = {"teams": connection([{"id": "t2", "name": "Team 2"}])}
        return response
    provider = Linear("shortkey", transport=httpx.MockTransport(page))
    assert len(await provider.teams()) == 2
    assert fake.requests[-1][1]["variables"]["after"] == "next"
    fake.data = {"team": {"states": connection([{"id": "s", "name": "Ready", "type": "unstarted"}])}}
    assert (await provider.team_states('team"id'))[0]["id"] == "s"
    fake.data = {"issues": connection([{"id": "i", "identifier": "T-1", "title": "Task", "url": "https://linear.app/i"}])}
    assert (await provider.issues('team"id', "s", 'label"'))[0]["id"] == "i"
    query = fake.requests[-1][1]
    assert 'label"' not in query["query"]
    assert query["variables"]["filter"]["labels"]["some"]["name"]["eq"] == 'label"'
    fake.data = {"issue": {"state": {"id": "s", "name": "Ready"}}}
    assert (await provider.issue_state("i"))["id"] == "s"
    fake.data = {"issueUpdate": {"success": True}}
    await provider.update_issue_state("i", "s")
    assert fake.requests[-1][1]["variables"] == {"id": "i", "input": {"stateId": "s"}}
    fake.data = {"issue": {"comments": connection([{"id": "c", "body": "hello"}])}}
    assert (await provider.comments("i"))[0]["body"] == "hello"
    fake.data = {"commentCreate": {"success": True, "comment": {"id": "c", "body": "hello"}}}
    assert (await provider.create_comment("i", "hello"))["id"] == "c"
    assert fake.requests[-1][1]["variables"] == {"input": {"issueId": "i", "body": "hello"}}


@pytest.mark.parametrize("mode,category", [
    (401, "authentication"), (403, "authentication"), (429, "rate_limit"), (503, "unavailable"),
    (400, "http"), ("graphql", "graphql"), ("timeout", "timeout"), ("network", "network"),
    ("malformed", "malformed"), ("json", "malformed"), ("mutation", "mutation_failed"),
    ("comment", "mutation_failed"),
])
@pytest.mark.asyncio
async def test_failures_are_sanitized_and_not_retried(mode, category):
    key = "shortkey"
    fake = FakeLinear(status=mode if isinstance(mode, int) else 200,
                      errors=[{"message": key}] if mode == "graphql" else None,
                      failure={"timeout": httpx.ReadTimeout(key), "network": httpx.ConnectError(key)}.get(mode))
    if mode == "malformed":
        fake.data = {"viewer": {"id": "v", "name": None}}
    if mode == "mutation":
        fake.data = {"issueUpdate": {"success": False}}
    if mode == "comment":
        fake.data = {"commentCreate": {"success": False}}
    def handle(request):
        response = fake.handle(request)
        return httpx.Response(200, text=key) if mode == "json" else response
    provider = Linear(key, transport=httpx.MockTransport(handle))
    with pytest.raises(LinearError) as caught:
        if mode == "comment":
            await provider.create_comment("i", "body")
        else:
            await (provider.update_issue_state("i", "s") if mode == "mutation" else provider.viewer())
    assert caught.value.category == category and key not in str(caught.value)
    assert len(fake.requests) == 1


def install(monkeypatch, fake):
    monkeypatch.setattr(api, "Linear", lambda token: Linear(token, transport=fake))


@pytest.mark.parametrize("role", ["owner", "admin"])
def test_credential_lifecycle_and_isolation(client, signed_in, monkeypatch, caplog, role, tmp_path):
    workspace = signed_in["workspaces"][0]["id"]
    with session_scope() as db:
        db.query(Membership).filter_by(workspace_id=workspace).one().role = role
    path = f"/api/workspaces/{workspace}/credentials/linear_api_key"
    fake = FakeLinear()
    install(monkeypatch, fake)
    for key in ("shortkey", "lin_api_" + "secret" * 8):
        response = client.put(path, json={"value": key})
        assert response.status_code == 200 and response.json()["detail"] == "Connected as Fake Account"
        assert client.post(path + "/test").json()["status"] == "valid"
        assert key not in response.text + client.get(path.rsplit("/", 1)[0]).text + caplog.text
        with session_scope() as db:
            secret = db.query(Secret).filter_by(workspace_id=workspace, kind="linear_api_key").one()
            assert secret.ciphertext != key and decrypt(secret.ciphertext) == key
            assert key not in json.dumps([event.message + json.dumps(event.data) for event in db.query(Event)])
        assert resolve_token(workspace) == key
    monkeypatch.setenv("LINEAR_API_KEY", key)
    monkeypatch.setenv("AIFACTORY_BOOTSTRAP_LINEAR_API_KEY", key)
    with pytest.raises(LinearError):
        resolve_token("another-workspace")
    from aifactory.sandbox import LocalSandbox
    from aifactory.vm import RailwayVMSandbox
    assert key not in json.dumps(LocalSandbox("linear", root=tmp_path).env())
    class Bridge:
        async def call(self, op, args, **kwargs):
            assert key not in json.dumps(args)
            assert "LINEAR" not in json.dumps(args)
            return {"exitCode": 0}
    import asyncio
    asyncio.run(RailwayVMSandbox("vm", Bridge()).exec("true"))
    assert client.delete(path).status_code == 200
    with pytest.raises(LinearError):
        resolve_token(workspace)


@pytest.mark.parametrize("role", ["member", "viewer", "outsider"])
def test_credential_permissions(client, signed_in, monkeypatch, role):
    workspace = signed_in["workspaces"][0]["id"]
    path = f"/api/workspaces/{workspace}/credentials/linear_api_key"
    install(monkeypatch, FakeLinear())
    assert client.put(path, json={"value": "shortkey"}).status_code == 200
    with session_scope() as db:
        member = db.query(Membership).filter_by(workspace_id=workspace).one()
        if role == "outsider":
            db.delete(member)
        else:
            member.role = role
    expected = 404 if role == "outsider" else 403
    assert client.put(path, json={"value": "newtoken"}).status_code == expected
    assert client.post(path + "/test").status_code == expected
    assert client.delete(path).status_code == expected
    if role == "outsider":
        assert client.get(path.rsplit("/", 1)[0]).status_code == 404


@pytest.mark.parametrize("key", ["1234567", "lin_api_" + "x" * 8192, {"value": "sensitive"}, "shortkey"])
def test_validation_and_upstream_echo_never_expose_key(client, signed_in, monkeypatch, caplog, key):
    path = f'/api/workspaces/{signed_in["workspaces"][0]["id"]}/credentials/linear_api_key'
    install(monkeypatch, FakeLinear(errors=[{"message": str(key)}]))
    response = client.put(path, json={"value": key})
    assert response.status_code == (200 if key == "shortkey" else 422)
    assert str(key) not in response.text + caplog.text
    if key == "shortkey":
        assert response.json()["status"] == "untested"
        install(monkeypatch, FakeLinear(data={"viewer": {"id": key, "name": key}}))
        assert key not in client.post(path + "/test").text
    assert redact("lin_api_secret lin_oauth_secret tiny", ("tiny",)) == "[redacted] [redacted] [redacted]"


@pytest.mark.parametrize("failure", [401, 429, 503, "timeout"])
def test_transient_failures_do_not_disable_credentials(client, signed_in, monkeypatch, failure):
    workspace = signed_in["workspaces"][0]["id"]
    path = f"/api/workspaces/{workspace}/credentials/linear_api_key"
    fake = FakeLinear()
    install(monkeypatch, fake)
    assert client.put(path, json={"value": "shortkey"}).json()["status"] == "valid"
    if failure == "timeout":
        fake.failure = httpx.ReadTimeout("shortkey")
    else:
        fake.status = failure
    for response in (client.post(path + "/test"), client.put(path, json={"value": "shortkey"})):
        assert response.json()["status"] == ("invalid" if failure == 401 else "untested")
    if failure == 401:
        with pytest.raises(LinearError):
            resolve_token(workspace)
    else:
        assert resolve_token(workspace) == "shortkey"
