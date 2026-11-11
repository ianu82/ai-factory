"""Preview policy validation and private account API contracts."""
import json

import pytest
from fastapi.testclient import TestClient

from aifactory import api, preview_config as P
from aifactory.db import session_scope
from aifactory.models import Event, Membership, Repository, Secret
from aifactory.security import decrypt


@pytest.fixture
def repo(client, signed_in):
    ws = signed_in["workspaces"][0]["id"]
    with session_scope() as db:
        row = Repository(workspace_id=ws, full_name="acme/app", config={"context": "keep", "qa": {"start": "npm start"}})
        db.add(row)
        db.flush()
        rid = row.id
    return ws, rid, f"/api/workspaces/{ws}/repositories/{rid}"


def test_default_and_policy_round_trip(client, repo):
    ws, rid, root = repo
    assert not P.preview_qa()["enabled"]
    with session_scope() as db:
        legacy = db.get(Repository, rid).config
        assert "preview_qa" not in legacy
        assert not P.preview_qa(legacy)["enabled"]
    policy = {"enabled": True, "required": True, "environment_pattern": "preview-*",
              "url_template": "https://pr-{pr_number}.example.test/{head_sha}/{branch}",
              "pages": ["/", "/account?view=full"], "probes": [{"path": "/health", "expect_status": 200}],
              "login_steps": [{"action": "goto", "path": "/login"},
                              {"action": "fill", "selector": "#email", "secret_field": "email"},
                              {"action": "click", "selector": "button"},
                              {"action": "wait_for_selector", "selector": "#account"}]}
    r = client.patch(root, json={"config": {"preview_qa": policy}})
    assert r.status_code == 200, r.text
    config = r.json()["config"]
    assert config["context"] == "keep" and config["qa"] == {"start": "npm start"}
    assert config["preview_qa"]["timeout_seconds"] == 600
    assert config["preview_qa"] == P.validate_config(policy)
    assert client.get(f"/api/workspaces/{ws}/repositories").json()[0]["config"] == config
    r = client.patch(root, json={"config": {"preview_qa": {"required": False}}})
    assert r.json()["config"]["preview_qa"]["enabled"]
    assert not r.json()["config"]["preview_qa"]["required"]
    assert P.render_url_template(policy["url_template"], pr_number=23, head_sha="abc1234", branch="feature/foo") == (
        "https://pr-23.example.test/abc1234/feature%2Ffoo")


@pytest.mark.parametrize("branch, escaped", [
    ("feature/foo", "feature%2Ffoo"),
    ("feature/100%done", "feature%2F100%25done"),
    ("feature/#123", "feature%2F%23123"),
    ("feature/what?next", "feature%2Fwhat%3Fnext"),
    ("feature/%0a", "feature%2F%250a"),
    ("feature/%2e%2e", "feature%2F%252e%252e"),
    ("feature/%252f", "feature%2F%25252f"),
])
@pytest.mark.parametrize("template", ["https://example.test/{branch}", "https://example.test/?branch={branch}"])
def test_render_escaped_branch_characters(branch, escaped, template):
    assert P.render_url_template(template, pr_number=23, head_sha="abc1234", branch=branch) == (
        template.replace("{branch}", escaped))


@pytest.mark.parametrize("template", [
    "https://{branch}.example.test/", "https://pr-{branch}.example.test/",
    "https://{branch}/", "https://example.test:{branch}/",
])
def test_branch_cannot_be_used_in_authority(template):
    with pytest.raises(ValueError, match="Invalid preview URL template"):
        P.validate_url_template(template)


@pytest.mark.parametrize("patch", [
    {"timeout_seconds": x} for x in [0, -1, True, 1.5, "600", None, 3601]
] + [
    {"url_template": x} for x in ["https://{branch}.example.test/", "https://{pr_number:04}.test/",
        "https://{unknown}.test/", "https://{head_sha!r}.test/", "https://user:password@test/",
        "javascript:alert(1)", "ftp://test/", "https://test/../secret", "https://test/%2e%2e/secret",
        "https://test:invalid/", "https://test:{pr_number}/", "https://test/#secret", "https://test/#"]
] + [
    {"pages": [x]} for x in ["//evil.test", "https://evil.test/", "relative", "/../secret", "/%252e%252e/", "/%25252e%25252e/", "/%252f%252fevil", "/%250a", "/%bad%", "/\\evil", "/x\n"]
] + [
    {"login_steps": [x]} for x in [{"action": "evaluate", "script": "secret-sentinel"},
        {"action": "fill", "selector": "input", "value": "secret-sentinel"},
        {"action": "goto", "path": "https://evil.test"},
        {"action": "click", "selector": "button", "script": "secret-sentinel"}]
] + [{"password": "secret-sentinel"}, {"enabled": "yes"}, {"probes": [{"path": "/", "body": "secret-sentinel"}]}])
def test_invalid_configuration_is_value_free(client, repo, patch):
    _, _, root = repo
    r = client.patch(root, json={"config": {"preview_qa": patch}})
    assert r.status_code == 422
    assert r.json() == {"detail": "Invalid Preview QA configuration"}
    with session_scope() as db:
        assert "preview_qa" not in db.get(Repository, repo[1]).config


def test_encrypted_account_round_trip_replace_delete(client, repo, monkeypatch):
    ws, rid, root = repo
    path = root + "/preview-qa/account"
    def no_provider(*args):
        pytest.fail("Preview accounts must not be tested by credential providers")
    monkeypatch.setattr(api, "_test_secret", no_provider)
    assert client.get(path).json() == {"configured": False, "status": "missing", "fields": []}
    fields = {"email": "  exact@example.test \n", "password": "xy"}
    r = client.put(path, json={"fields": fields})
    expected = {"configured": True, "status": "configured", "fields": ["email", "password"]}
    assert r.status_code == 200 and r.json() == expected
    assert client.get(path).json() == expected
    with session_scope() as db:
        sec = db.query(Secret).filter_by(workspace_id=ws).one()
        assert sec.kind == P.account_kind(rid) and len(sec.kind) <= 40
        assert fields["email"] not in sec.ciphertext and json.dumps(fields) not in sec.ciphertext
        assert json.loads(decrypt(sec.ciphertext)) == fields
        assert P.load_account(db, ws, rid) == fields
        assert not sec.hint and not sec.status_detail and sec.tested_at is None
        assert "xy" not in json.dumps([e.data for e in db.query(Event)])
        assert "exact@example" not in json.dumps([e.message for e in db.query(Event)])
    assert "preview_" not in client.get(f"/api/workspaces/{ws}/credentials").text
    assert client.post(f"/api/workspaces/{ws}/credentials/{P.account_kind(rid)}/test").status_code == 404
    assert client.delete(f"/api/workspaces/{ws}/credentials/{P.account_kind(rid)}").status_code == 404
    assert client.get(path).json() == expected
    assert client.put(path, json={"fields": {"pin": "a"}}).json()["fields"] == ["pin"]
    assert client.delete(path).json() == {"configured": False, "status": "missing", "fields": []}
    assert client.delete(path).status_code == 200
    with session_scope() as db:
        assert db.query(Secret).count() == 0


@pytest.mark.parametrize("body", [None, [], {"fields": {}}, {"fields": {"password": 1}},
    {"fields": {"password": ""}}, {"fields": {"password": "s" * 8193}},
    {"fields": {"bad-name": "secret-sentinel"}}, {"fields": {"password": "secret-sentinel"}, "other": 1}])
def test_account_invalid_input_is_value_free(client, repo, body):
    r = client.put(repo[2] + "/preview-qa/account", content=json.dumps(body), headers={"content-type": "application/json"})
    assert r.status_code == 422
    assert r.json() == {"detail": "Invalid preview account request"}


def test_malformed_and_oversized_requests(client, repo):
    path = repo[2] + "/preview-qa/account"
    for raw in [b'{"fields":{"password":"secret-sentinel"', b"x" * (P.MAX_ACCOUNT_BYTES + 1), b"\xff"]:
        r = client.put(path, content=raw)
        assert r.status_code == 422 and "secret-sentinel" not in r.text
    r = client.patch(repo[2], json={"config": ["secret-sentinel"]})
    assert r.status_code == 422 and "secret-sentinel" not in r.text


@pytest.mark.parametrize("role", ["viewer", "member"])
def test_workspace_and_admin_boundaries(client, repo, role):
    ws, rid, root = repo
    path = root + "/preview-qa/account"
    assert client.put(path, json={"fields": {"password": "secret-sentinel"}}).status_code == 200
    other = TestClient(client.app)
    other.post("/auth/dev", json={"email": "other@example.test", "organization": "Other"})
    me = other.get("/api/me").json()
    own_ws = me["workspaces"][0]["id"]
    for target in [path, f"/api/workspaces/{own_ws}/repositories/{rid}/preview-qa/account"]:
        assert other.get(target).status_code == 404
        assert other.put(target, json={"fields": {"password": "x"}}).status_code == 404
        assert other.delete(target).status_code == 404
    with session_scope() as db:
        db.add(Membership(workspace_id=ws, user_id=me["user"]["id"], role=role))
    assert other.get(path).json()["fields"] == ["password"]
    assert "secret-sentinel" not in other.get(path).text
    assert other.put(path, json={"fields": {"password": "x"}}).status_code == 403
    assert other.delete(path).status_code == 403
    assert other.patch(root, json={"config": {"preview_qa": {"enabled": True}}}).status_code == 403
    # A separate repository in the same workspace has a separate account.
    with session_scope() as db:
        row = Repository(workspace_id=ws, full_name="acme/second")
        db.add(row)
        db.flush()
        second = row.id
    assert client.get(f"/api/workspaces/{ws}/repositories/{second}/preview-qa/account").json()["configured"] is False
    assert client.delete(root).status_code == 200
    with session_scope() as db:
        assert P.account(db, ws, rid) is None


@pytest.mark.parametrize("template", [
    "https://{head_sha}{head_sha}.example.test/",
    "https://" + "a" * 24 + "{head_sha}.example.test/",
    "https://{pr_number}{pr_number}{pr_number}{pr_number}.example.test/",
    "https://" + ".".join(["a" * 63] * 4) + "/",
])
def test_template_rejects_oversized_rendered_dns_names(client, repo, template):
    with pytest.raises(ValueError):
        P.validate_url_template(template)
    r = client.patch(repo[2], json={"config": {"preview_qa": {"url_template": template}}})
    assert r.status_code == 422
    assert r.json() == {"detail": "Invalid Preview QA configuration"}


def test_template_maximum_host_label_sizes():
    # A 40-character GitHub SHA plus a 19-digit PR number leaves four label bytes.
    template = "https://abcd{head_sha}{pr_number}.example.test/{branch}"
    assert P.validate_url_template(template) == template
    result = P.render_url_template(template, pr_number=P.MAX_PR_NUMBER, head_sha="f" * 40, branch="feature/#100%")
    assert result == "https://abcd" + "f" * 40 + str(P.MAX_PR_NUMBER) + ".example.test/feature%2F%23100%25"
    for revision in [{"pr_number": P.MAX_PR_NUMBER + 1, "head_sha": "f" * 40},
                     {"pr_number": 1, "head_sha": "f" * 41}]:
        with pytest.raises(ValueError, match="Invalid preview revision"):
            P.render_url_template(template, branch="main", **revision)


def test_account_fields_are_individual_egress_redaction_values(client, repo, signed_in):
    from aifactory import linear_outbox
    from aifactory.mind import api as mind_api
    from aifactory.security import encrypt

    ws, rid, root = repo
    fields = {"email": "  exact@example.test \n", "password": "xy", "pin": "q"}
    assert client.put(root + "/preview-qa/account", json={"fields": fields}).status_code == 200
    with session_scope() as db:
        # Existing non-preview credentials remain a single, exact token even if JSON-shaped.
        provider_value = '{"token":"provider-only"}'
        db.add(Secret(workspace_id=ws, kind="github_token", ciphertext=encrypt(provider_value)))
        db.flush()
        tokens = mind_api.secrets_for(db, signed_in["user"]["id"])
        assert all(value in tokens for value in fields.values())
        assert provider_value in tokens and "provider-only" not in tokens
        for value in fields.values():
            assert mind_api.scrub({"text": value}, tokens) == {"text": "[redacted]"}
            assert linear_outbox._body(db, ws, value) == "[redacted]"
        assert P.load_account(db, ws, rid) == fields


def test_account_password_scrubbed_on_mind_submit(client, repo, monkeypatch):
    from aifactory.models import MindMessage

    class Runtime:
        history = None

        async def readiness(self, workspace_id):
            return {"status": "ready", "model_id": "scripted"}

        async def session_cost(self, workspace_id, session_id):
            return 0

        async def run_turn(self, context, history, emit, tools, ledger):
            self.history = history
            await emit({"type": "token", "data": {"text": "ok"}})

    runtime = Runtime()
    monkeypatch.setattr(client.app.state, "mind_runtime", runtime)
    password = "opaque-test-password-8451"
    assert client.put(repo[2] + "/preview-qa/account", json={"fields": {"password": password}}).status_code == 200
    thread = client.post("/api/mind/threads", json={"billing_workspace_id": repo[0]}).json()["id"]
    response = client.post(f"/api/mind/threads/{thread}/messages", json={
        "text": password, "client_message_id": "preview-secret"})
    assert response.status_code == 200, response.text
    assert runtime.history is not None and password not in json.dumps(runtime.history)
    assert password not in response.text
    assert password not in client.get(f"/api/mind/threads/{thread}").text
    with session_scope() as db:
        prompt = db.query(MindMessage).filter_by(thread_id=thread, role="user").one()
        assert prompt.text == "[redacted]"


@pytest.mark.parametrize("method", ["put", "delete"])
def test_account_write_rechecks_repository_after_lock(client, repo, monkeypatch, method):
    from sqlalchemy import delete

    ws, rid, root = repo
    original_lock = api.definitions.lock_workspace
    cached = []

    def disconnect_before_lock(db, workspace_id):
        # Keep the request session's identity-map object alive to reproduce the
        # stale db.get case. Complete a disconnect before its write lock, without
        # sleeps or backend-dependent thread timing.
        cached.append(db.get(Repository, rid))
        with session_scope() as other:
            original_lock(other, workspace_id)
            other.execute(delete(Repository).where(Repository.id == rid))
        original_lock(db, workspace_id)

    monkeypatch.setattr(api.definitions, "lock_workspace", disconnect_before_lock)
    kwargs = {"json": {"fields": {"password": "secret-sentinel"}}} if method == "put" else {}
    r = getattr(client, method)(root + "/preview-qa/account", **kwargs)
    assert r.status_code == 404, r.text
    assert r.json() == {"detail": "Repository not found"}
    with session_scope() as db:
        assert db.get(Repository, rid) is None
        assert P.account(db, ws, rid) is None


def test_disconnect_locks_before_repository_and_account_reads(client, repo, monkeypatch):
    ws, rid, root = repo
    original_lock = api.definitions.lock_workspace
    original_repository = api._preview_repository
    original_account = P.account
    locked = []

    def lock(db, workspace_id):
        original_lock(db, workspace_id)
        locked.append(workspace_id)

    def repository(*args, **kwargs):
        assert locked and locked[0] == ws
        return original_repository(*args, **kwargs)

    def account(*args, **kwargs):
        assert locked and locked[0] == ws
        return original_account(*args, **kwargs)

    monkeypatch.setattr(api.definitions, "lock_workspace", lock)
    monkeypatch.setattr(api, "_preview_repository", repository)
    monkeypatch.setattr(P, "account", account)
    assert client.delete(root).status_code == 200
    with session_scope() as db:
        assert db.get(Repository, rid) is None
        assert P.account(db, ws, rid) is None


def test_account_fields_scrubbed_from_linear_audit_events(client, repo, signed_in):
    from aifactory import linear_config
    from aifactory.models import Workspace

    password = "opaque-linear-label-8451"
    assert client.put(repo[2] + "/preview-qa/account", json={"fields": {"password": password}}).status_code == 200
    with session_scope() as db:
        linear_config.save_config(db, db.get(Workspace, repo[0]),
                                  linear_config.LinearConfig(intake_label=password), signed_in["user"]["id"])
        events = db.query(Event).filter_by(workspace_id=repo[0], kind="settings_changed").all()
        assert events and password not in json.dumps([e.data for e in events])
        assert events[-1].data["changes"]["linear"]["intake_label"] == "[redacted]"


def test_invalid_stored_account_redaction_fails_closed(client, repo, signed_in):
    from aifactory import linear_outbox
    from aifactory.mind import api as mind_api
    from aifactory.security import SecretError, encrypt

    with session_scope() as db:
        sec = Secret(workspace_id=repo[0], kind=P.account_kind(repo[1]),
                     ciphertext=encrypt('{"password": 123}'))
        db.add(sec)
        db.flush()
        with pytest.raises(SecretError, match="Stored preview account is invalid"):
            mind_api.secrets_for(db, signed_in["user"]["id"])
        assert linear_outbox._body(db, repo[0], "private body") == "[redacted]"
