"""Release configuration boundaries, legacy compatibility and encrypted credentials."""
from copy import deepcopy
import json

import pytest
from pydantic import ValidationError

from aifactory import db
from aifactory.models import (Event, Membership, Release, ReleaseStep, ReleaseTrainCursor,
                              Repository, Secret, Workspace)
from aifactory.release_config import (ReleaseCut, ReleaseSchedule, TrainReleaseConfig,
                                      VersionFile, effective_release_approval, webhook_secret_kind)
from aifactory.security import decrypt, encrypt


def train():
    workflow = {"kind": "workflow", "workflow": "deploy.yml", "correlation_input": "release_key"}
    return {"mode": "train", "staging": deepcopy(workflow), "production": deepcopy(workflow),
            "rollback": deepcopy(workflow), "staging_smoke": {"commands": ["npm test"]},
            "production_smoke": {"urls": ["https://app.example.test/health"]}}


@pytest.fixture
def repo(signed_in):
    ws = signed_in["workspaces"][0]["id"]
    with db.session_scope() as s:
        r = Repository(workspace_id=ws, full_name="acme/release-test", config={})
        s.add(r)
        s.flush()
        return ws, r.id, f"/api/workspaces/{ws}/repositories/{r.id}"


@pytest.mark.parametrize("field,value", [("weekdays", []), ("weekdays", ["monday"]),
    ("time", "24:00"), ("time", "9:00"), ("time", "12:60"), ("timezone", "Not/AZone")])
def test_schedule_validation(field, value):
    with pytest.raises(ValidationError):
        ReleaseSchedule.model_validate({field: value})


def test_defaults_and_versions():
    schedule = ReleaseSchedule()
    assert schedule.weekdays == {"mon", "tue", "wed", "thu", "fri"}
    assert schedule.zone.key == "UTC"
    cut = ReleaseCut()
    assert cut.next_version() == "0.1.0"
    assert cut.next_version("1.2.9") == "1.2.10"
    assert VersionFile(path="package.json", format="json", key="version").key == "version"
    cfg = TrainReleaseConfig.model_validate(train())
    assert effective_release_approval(cfg, {}) == "approval"
    assert effective_release_approval(cfg, {"autonomy": {"release": "auto"}}) == "auto"
    cfg.approval = "approval"
    assert effective_release_approval(cfg, {"autonomy": {"release": "auto"}}) == "approval"


@pytest.mark.parametrize("path", ["../VERSION", "/VERSION", "a/../VERSION", "a//VERSION", "a/./v",
                                  ".git/config", "a\\VERSION", "C:VERSION", "a\x00b"])
def test_unsafe_version_path(path):
    with pytest.raises(ValidationError):
        VersionFile(path=path)


@pytest.mark.parametrize("change", [
    {"schedule": {"weekdays": ["bad"]}}, {"schedule": {"time": "25:00"}},
    {"schedule": {"timezone": "Mars/City"}}, {"rollback": None}, {"staging_smoke": {}},
    {"production_smoke": None}, {"staging": {"kind": "workflow", "workflow": "deploy"}},
    {"cut": {"mode": "version_pr"}},
    {"cut": {"mode": "version_pr", "version_file": {"path": "../v"}}},
    {"cut": {"initial_version": "v1.0"}},
    {"production": {"kind": "webhook", "endpoint": "http://example.test", "secret_ref": "a" * 32}},
])
def test_rejected_patch_is_atomic(client, repo, change):
    ws, rid, path = repo
    assert client.patch(path, json={"config": {"release": train()}}).status_code == 200
    invalid = {**train(), **change}
    response = client.patch(path, json={"config": {"release": invalid, "build": {"max_tokens": 99}}})
    assert response.status_code == 422, response.text
    with db.session_scope() as s:
        assert s.get(Repository, rid).config == {"release": train()}


@pytest.mark.parametrize("legacy", [None, {}, {"health_url": "https://app.example.test/healthz?probe=deep#ok"},
    {"mode": "on_merge", "health_url": "https://app.example.test/healthz?probe=deep", "custom": [1, 2]},
    {"mode": "continuous", "custom": {"anything": True}}])
def test_legacy_round_trip(client, repo, legacy):
    ws, rid, path = repo
    config = {"build": {"max_tokens": 5}}
    if legacy is not None:
        config["release"] = legacy
    with db.session_scope() as s:
        s.get(Repository, rid).config = config
    public = next(r for r in client.get(f"/api/workspaces/{ws}/repositories").json() if r["id"] == rid)["config"]
    assert public.get("release") == config.get("release")
    assert public["build"] == {"max_tokens": 5}
    response = client.patch(path, json={"config": public})
    assert response.status_code == 200, response.text
    with db.session_scope() as s:
        stored = s.get(Repository, rid).config
        assert stored.get("release") == config.get("release")
        assert ("release" in stored) == ("release" in config)
        assert stored["build"] == {"max_tokens": 5}


def webhook(ref=None):
    target = {"kind": "webhook", "endpoint": "https://deploy.example.test/hook", "completion": {"mode": "synchronous"}}
    if ref:
        target["secret_ref"] = ref
    return target


def test_secret_write_only_encrypted_and_snapshot_redaction(client, repo):
    ws, rid, path = repo
    secret_path = path + "/release/webhook-secret"
    value = "arbitrary-private-webhook-credential"
    response = client.put(secret_path, json={"value": value})
    assert response.status_code == 200, response.text
    assert response.json() == {"configured": True}
    read = client.get(secret_path)
    assert read.status_code in (404, 405)
    assert value not in read.text
    with db.session_scope() as s:
        secret = s.query(Secret).filter_by(workspace_id=ws, kind=webhook_secret_kind(rid)).one()
        assert len(secret.kind) <= 40
        assert secret.ciphertext != value and decrypt(secret.ciphertext) == value
        secret_id = secret.id
        assert value not in json.dumps([e.data for e in s.query(Event)])
    config = train()
    config["staging"] = webhook()  # resolve this repository's configured credential
    response = client.patch(path, json={"config": {"release": config}})
    assert response.status_code == 200, response.text
    public = response.json()["config"]
    assert public["release"]["staging"]["secret_configured"] is True
    assert "secret_ref" not in json.dumps(public) and secret_id not in json.dumps(public)
    assert client.patch(path, json={"config": public}).status_code == 200
    with db.session_scope() as s:
        release = Release(workspace_id=ws, repo_id=rid, scheduled_slot="2026-11-11T09:00[UTC]",
                          scheduled_at=db.utcnow(), config_snapshot=s.get(Repository, rid).config,
                          notes=value, control_history=[{"actor_id": "user", "comment": value}])
        s.add(release)
        s.flush()
        step = ReleaseStep(workspace_id=ws, repo_id=rid, release_id=release.id, step="staging",
                           operation_key="staging-1", logs=value + "x" * 20000,
                           external_handle={"authorization": value})
        s.add(step)
        s.flush()
        assert value not in release.notes and value not in json.dumps(release.config_snapshot)
        assert secret_id not in json.dumps(release.config_snapshot)
        assert value not in step.logs and len(step.logs) <= 16000
        assert value not in json.dumps(step.external_handle)
        assert value not in json.dumps(release.control_history)


def test_foreign_secret_rejected_and_admin_required(client, repo, signed_in):
    ws, rid, path = repo
    with db.session_scope() as s:
        own = s.get(Workspace, ws)
        other = Workspace(org_id=own.org_id, name="Other", slug="other")
        s.add(other)
        s.flush()
        foreign = Secret(workspace_id=other.id, kind=webhook_secret_kind(rid), ciphertext=encrypt("foreign"))
        s.add(foreign)
        s.flush()
        foreign_id = foreign.id
    config = train()
    config["production"] = webhook(foreign_id)
    assert client.patch(path, json={"config": {"release": config}}).status_code == 422
    with db.session_scope() as s:
        assert s.get(Repository, rid).config == {}
        s.query(Membership).filter_by(workspace_id=ws, user_id=signed_in["user"]["id"]).one().role = "member"
    assert client.put(path + "/release/webhook-secret", json={"value": "private"}).status_code == 403
    assert client.patch(path, json={"config": {"release": train()}}).status_code == 403


def test_webhook_contract_and_validation_errors_never_echo(client, repo):
    ws, rid, path = repo
    secret_path = path + "/release/webhook-secret"
    assert client.put(secret_path, json={"value": "private-credential"}).status_code == 200
    for target in [
        {**webhook(), "endpoint": "https://example.test/?token=private"},
        {**webhook(), "timeout_seconds": 121},
        {**webhook(), "completion": {"mode": "synchronous", "success_statuses": [202]}},
        {**webhook(), "completion": {"mode": "poll"}},
        {k: v for k, v in webhook().items() if k != "completion"},
        {**webhook(), "secret": "private"},
    ]:
        config = {**train(), "staging": target}
        response = client.patch(path, json={"config": {"release": config}})
        assert response.status_code == 422
        assert "private" not in response.text
    for body in [{"value": {"password": "private"}}, ["private"]]:
        response = client.put(secret_path, json=body)
        assert response.status_code == 422 and "private" not in response.text
    response = client.post(f"/api/workspaces/{ws}/repositories/preview",
                           json={"full_name": "acme/app", "config": {"release": {"mode": "private"}}})
    assert response.status_code == 422 and "private" not in response.text
    poll = {"mode": "poll", "status_url": "https://example.test/status", "handle_key": "id",
            "state_key": "state", "success_values": ["done"], "failure_values": ["failed"]}
    assert client.patch(path, json={"config": {"release": {**train(), "staging": {**webhook(), "completion": poll}}}}).status_code == 200


def test_repository_secret_scope_and_known_inline_credentials(client, repo):
    ws, rid, path = repo
    value = "webhook-credential-not-a-recognized-token-format"
    assert client.put(path + "/release/webhook-secret", json={"value": value}).status_code == 200
    with db.session_scope() as s:
        other = Repository(workspace_id=ws, full_name="acme/other")
        s.add(other)
        s.flush()
        secret = Secret(workspace_id=ws, kind=webhook_secret_kind(other.id), ciphertext=encrypt("other-secret"))
        s.add(secret)
        s.flush()
        other_ref = secret.id
    for target in [webhook(other_ref), {"kind": "workflow", "workflow": "deploy.yml",
                    "correlation_input": "operation", "inputs": {"credential": value}}]:
        response = client.patch(path, json={"config": {"release": {**train(), "staging": target}}})
        assert response.status_code == 422
        assert value not in response.text
    with db.session_scope() as s:
        assert s.get(Repository, rid).config == {}


def test_snapshot_scrubber_bounds_and_normalizes_credentials():
    from aifactory.release_config import release_redacted

    snapshot = release_redacted({"webhookSecret": "private", "api-key": "private", "accessToken": "private",
                                 "url": "https://example.test/status?signature=private",
                                 "log": "token=private", "large": "x" * 20000})
    assert "private" not in json.dumps(snapshot)
    assert len(snapshot["large"]) == 16000
    assert snapshot["webhookSecret"] is True


@pytest.mark.parametrize("abort", [False, True])
def test_disconnect_deletes_release_records_and_secret_atomically(client, repo, monkeypatch, abort):
    from sqlalchemy.orm import Session

    ws, rid, path = repo
    assert client.put(path + "/release/webhook-secret", json={"value": "removed-secret"}).status_code == 200
    with db.session_scope() as s:
        other_ws = Workspace(org_id=s.get(Workspace, ws).org_id, name="Other", slug="other")
        s.add(other_ws)
        s.flush()
        other_repo = Repository(workspace_id=ws, full_name="acme/retained")
        s.add(other_repo)
        s.flush()
        # A different repository's credential and even a matching kind in another
        # workspace must survive this disconnect.
        retained = [Secret(workspace_id=ws, kind=webhook_secret_kind(other_repo.id),
                           ciphertext=encrypt("retained-secret")),
                    Secret(workspace_id=other_ws.id, kind=webhook_secret_kind(rid),
                           ciphertext=encrypt("foreign-secret")),
                    Secret(workspace_id=ws, kind="github_token", ciphertext=encrypt("github-secret"))]
        s.add_all(retained)
        releases = []
        steps = []
        for repo_id in (rid, other_repo.id):
            release = Release(workspace_id=ws, repo_id=repo_id,
                              scheduled_slot="2026-11-11T09:00[UTC]", scheduled_at=db.utcnow(), state="succeeded")
            s.add(release)
            s.flush()
            step = ReleaseStep(workspace_id=ws, repo_id=repo_id, release_id=release.id,
                               step="staging", operation_key=f"{release.id}-staging-1", state="succeeded")
            s.add(step)
            s.add(ReleaseTrainCursor(workspace_id=ws, repo_id=repo_id, last_release_id=release.id))
            s.flush()
            releases.append(release.id)
            steps.append(step.id)
        retained_ids = [secret.id for secret in retained]
        other_id = other_repo.id

    def fail_commit(session):
        session.flush()
        assert session.get(Repository, rid) is None
        assert session.get(Release, releases[0]) is None
        assert session.get(ReleaseStep, steps[0]) is None
        assert session.get(ReleaseTrainCursor, rid) is None
        assert session.query(Secret).filter_by(workspace_id=ws, kind=webhook_secret_kind(rid)).count() == 0
        raise RuntimeError("abort disconnect")

    with monkeypatch.context() as m:
        if abort:
            m.setattr(Session, "commit", fail_commit)
            with pytest.raises(RuntimeError, match="abort disconnect"):
                client.delete(path)
        else:
            response = client.delete(path)
            assert response.status_code == 200, response.text
    with db.session_scope() as s:
        assert (s.get(Repository, rid) is not None) == abort
        assert (s.get(Release, releases[0]) is not None) == abort
        assert (s.get(ReleaseStep, steps[0]) is not None) == abort
        assert (s.get(ReleaseTrainCursor, rid) is not None) == abort
        assert s.query(Secret).filter_by(workspace_id=ws, kind=webhook_secret_kind(rid)).count() == int(abort)
        assert s.get(Repository, other_id) is not None
        assert s.get(Release, releases[1]) is not None
        assert s.get(ReleaseStep, steps[1]) is not None
        assert s.get(ReleaseTrainCursor, other_id) is not None
        assert all(s.get(Secret, secret_id) is not None for secret_id in retained_ids)


@pytest.mark.parametrize("field", ["workflow", "correlation_input", "input_name",
                                    "handle_key", "state_key", "success_values", "failure_values"])
@pytest.mark.parametrize("blank", [" ", "\t\n"])
def test_blank_target_identifiers_rejected_atomically(client, repo, field, blank):
    ws, rid, path = repo
    assert client.patch(path, json={"config": {"release": train()}}).status_code == 200
    config = train()
    if field in ("workflow", "correlation_input"):
        config["staging"][field] = blank
    elif field == "input_name":
        config["staging"]["inputs"] = {blank: "value"}
    else:
        assert client.put(path + "/release/webhook-secret", json={"value": "private-credential"}).status_code == 200
        poll = {"mode": "poll", "status_url": "https://example.test/status", "handle_key": "id",
                "state_key": "state", "success_values": ["done"], "failure_values": ["failed"]}
        poll[field] = [blank] if field.endswith("values") else blank
        config["staging"] = {**webhook(), "completion": poll}
    with pytest.raises(ValidationError):
        # Supply a syntactically valid secret reference for model-only validation.
        candidate = deepcopy(config)
        if candidate["staging"]["kind"] == "webhook":
            candidate["staging"]["secret_ref"] = "a" * 32
        TrainReleaseConfig.model_validate(candidate)
    response = client.patch(path, json={"config": {"release": config}})
    assert response.status_code == 422, response.text
    with db.session_scope() as s:
        assert s.get(Repository, rid).config == {"release": train()}


def test_null_release_mode_accepted_on_connect_and_patch(client, signed_in, monkeypatch):
    from aifactory import api

    ws = signed_in["workspaces"][0]["id"]
    health_url = "https://app.example.test/healthz?probe=deep#ready"

    async def detect_repository(workspace_id, full_name):
        assert workspace_id == ws
        return {"full_name": full_name}, {"release": {"health_url": health_url}}

    monkeypatch.setattr(api, "_detect_repository", detect_repository)
    response = client.post(f"/api/workspaces/{ws}/repositories", json={
        "full_name": "acme/null-mode", "config": {"release": {"mode": None}}})
    assert response.status_code == 200, response.text
    repo = response.json()
    assert repo["config"]["release"] == {"mode": None, "health_url": health_url}
    release = {"mode": None, "health_url": health_url, "custom": "preserved"}
    response = client.patch(f"/api/workspaces/{ws}/repositories/{repo['id']}",
                            json={"config": {"release": release}})
    assert response.status_code == 200, response.text
    assert response.json()["config"]["release"] == release
    with db.session_scope() as s:
        assert s.get(Repository, repo["id"]).config["release"] == release


@pytest.mark.parametrize("value", ["a", "staging", "rollback"[:7], "        a        ", "       "])
def test_short_webhook_secret_rejected_without_changing_config_or_credential(client, repo, value):
    ws, rid, path = repo
    secret_path = path + "/release/webhook-secret"
    assert client.put(secret_path, json={"value": "retained-credential"}).status_code == 200
    assert client.patch(path, json={"config": {"release": train()}}).status_code == 200
    with db.session_scope() as s:
        original_ciphertext = s.query(Secret).filter_by(
            workspace_id=ws, kind=webhook_secret_kind(rid)).one().ciphertext
        original_events = s.query(Event).count()
    response = client.put(secret_path, json={"value": value})
    assert response.status_code == 422
    assert response.json() == {"detail": "Invalid webhook secret request"}
    with db.session_scope() as s:
        assert s.get(Repository, rid).config == {"release": train()}
        assert s.query(Secret).filter_by(workspace_id=ws, kind=webhook_secret_kind(rid)).one().ciphertext == original_ciphertext
        assert s.query(Event).count() == original_events
    # A rejected short secret must not make ordinary target names fail redaction.
    assert client.patch(path, json={"config": {"release": train()}}).status_code == 200


def test_minimum_webhook_secret_is_trimmed_and_accepts_train_configuration(client, repo):
    ws, rid, path = repo
    assert client.put(path + "/release/webhook-secret", json={"value": "  abcdefgh  "}).status_code == 200
    with db.session_scope() as s:
        secret = s.query(Secret).filter_by(workspace_id=ws, kind=webhook_secret_kind(rid)).one()
        assert decrypt(secret.ciphertext) == "abcdefgh"
    response = client.patch(path, json={"config": {"release": {**train(), "staging": webhook()}}})
    assert response.status_code == 200, response.text
    assert "abcdefgh" not in response.text


def test_secret_status_is_existence_only_and_generic_deletion_is_blocked(client, repo, monkeypatch):
    ws, rid, path = repo
    status_path = path + "/release/webhook-secret/status"
    assert client.get(status_path).json() == {"configured": False}
    assert client.put(path + "/release/webhook-secret", json={"value": "private-credential"}).status_code == 200
    config = {**train(), "staging": webhook()}
    assert client.patch(path, json={"config": {"release": config}}).status_code == 200

    def no_decrypt(*_args):
        raise AssertionError("Configured-state checks must never decrypt secrets")

    monkeypatch.setattr("aifactory.api.decrypt", no_decrypt)
    response = client.get(status_path)
    assert response.status_code == 200 and response.json() == {"configured": True}
    # The generic credential route cannot invalidate an active train (or remove an
    # unused release credential behind the release configuration UI's back).
    response = client.delete(f"/api/workspaces/{ws}/credentials/{webhook_secret_kind(rid)}")
    assert response.status_code == 404
    with db.session_scope() as s:
        assert s.query(Secret).filter_by(workspace_id=ws, kind=webhook_secret_kind(rid)).count() == 1
        assert s.get(Repository, rid).config["release"]["staging"]["secret_ref"]
    assert client.patch(path, json={"config": {"context": "changed"}}).status_code == 200
    assert client.get(status_path).json() == {"configured": True}


@pytest.mark.parametrize("role", ["member", "viewer"])
def test_release_status_readable_but_config_and_secret_are_admin_only(client, repo, signed_in, role):
    ws, rid, path = repo
    assert client.put(path + "/release/webhook-secret", json={"value": "private-credential"}).status_code == 200
    with db.session_scope() as s:
        s.query(Membership).filter_by(workspace_id=ws, user_id=signed_in["user"]["id"]).one().role = role
    response = client.get(path + "/release/webhook-secret/status")
    assert response.status_code == 200 and response.json() == {"configured": True}
    assert client.get(f"/api/workspaces/{ws}/repositories").status_code == 200
    assert client.put(path + "/release/webhook-secret", json={"value": "another-private-credential"}).status_code == 403
    assert client.patch(path, json={"config": {"release": train()}}).status_code == 403


def test_release_secret_status_is_workspace_scoped(client, repo):
    ws, rid, path = repo
    with db.session_scope() as s:
        workspace = s.get(Workspace, ws)
        other = Workspace(org_id=workspace.org_id, name="Other", slug="release-status-other")
        s.add(other)
        s.flush()
        other_repo = Repository(workspace_id=other.id, full_name="acme/other-release")
        s.add(other_repo)
        s.flush()
        other_rid = other_repo.id
    assert client.get(f"/api/workspaces/{ws}/repositories/{other_rid}/release/webhook-secret/status").status_code == 404
    assert client.get(f"/api/workspaces/{ws}/repositories/missing/release/webhook-secret/status").status_code == 404


@pytest.mark.parametrize("release", [train(), {"mode": "on_merge", "health_url": "https://legacy.test/?probe=1",
                                          "expect_text": "healthy", "custom": {"keep": True}}])
def test_general_repo_settings_do_not_erase_release_or_unrelated_config(client, repo, release):
    ws, rid, path = repo
    saved = {"release": release, "qa": {"start": "npm run dev", "pages": ["/"]},
             "build": {"max_tokens": 123}, "unknown": {"nested": [1, 2]}, "context": "old"}
    with db.session_scope() as s:
        s.get(Repository, rid).config = saved
    response = client.patch(path, json={"config": {"context": "new", "checks": ["npm test"],
                                                  "merge": {"method": "squash"}}})
    assert response.status_code == 200, response.text
    with db.session_scope() as s:
        stored = s.get(Repository, rid).config
        for key in ("release", "qa", "build", "unknown"):
            assert stored[key] == saved[key]
        assert stored["context"] == "new" and stored["checks"] == ["npm test"]
