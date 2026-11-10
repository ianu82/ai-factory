"""Onboarding stores navigation, not credentials or client-asserted readiness."""

from __future__ import annotations

import asyncio
import json
import subprocess

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, inspect, text

from aifactory import config, db
from aifactory.github import REQUIRED_PERMISSIONS
from aifactory.models import Membership, Outcome, Repository, Secret, Workspace
from aifactory.onboarding import STEPS


def url(signed_in):
    return f"/api/workspaces/{signed_in['workspaces'][0]['id']}/onboarding"


def seed_ready(ws_id, *, provider="mindshub", status="valid", metadata=True):
    with db.session_scope() as s:
        s.add_all([
            Secret(workspace_id=ws_id, kind=f"{provider}_api_key", ciphertext="encrypted-model-secret",
                   hint="secret-hint", status=status, status_detail="private error detail",
                   metadata_json={"models": ["model"], "secret": "private metadata"} if metadata else {}),
            Secret(workspace_id=ws_id, kind="github_token", ciphertext="encrypted-github-secret", status=status,
                   metadata_json={"login": "octocat", "repository": "acme/app", "test_status": "valid",
                                  "diagnostic_code": "github_verified",
                                  "permissions": [{"permission": name, "state": "present"} for name in REQUIRED_PERMISSIONS]} if metadata else {}),
            Repository(workspace_id=ws_id, full_name="acme/app", config={"checks": ["pytest"]}),
        ])


def test_null_get_is_local_safe_and_does_not_mutate(client, signed_in, monkeypatch):
    def forbidden(*args, **kwargs):
        raise AssertionError("GET must not test or decrypt credentials")

    monkeypatch.setattr("httpx.AsyncClient.request", forbidden)
    monkeypatch.setattr("aifactory.security.decrypt", forbidden)
    ws_id = signed_in["workspaces"][0]["id"]
    with db.session_scope() as s:
        settings = s.get(Workspace, ws_id).settings
    response = client.get(url(signed_in))
    assert response.status_code == 200
    assert response.json() == {
        "current_step": "models", "completed_steps": [], "deferred": False, "first_outcome_id": None,
        "ready": False, "missing_steps": ["models", "github", "repositories"], "validation_uncertain_steps": [],
    }
    with db.session_scope() as s:
        ws = s.get(Workspace, ws_id)
        assert ws.onboarding is None
        assert ws.settings == settings


def test_progress_survives_sign_out_and_new_database_sessions(client, signed_in):
    path = url(signed_in)
    ws_id = signed_in["workspaces"][0]["id"]
    with db.session_scope() as s:
        settings = s.get(Workspace, ws_id).settings
    for action in [
        {"action": "navigate", "step": "team"},
        {"action": "complete", "step": "autonomy", "acknowledged": True},
        {"action": "complete", "step": "team", "acknowledged": True},
        {"action": "defer", "deferred": True},
    ]:
        assert client.patch(path, json=action).status_code == 200
    expected = client.get(path).json()
    assert expected["current_step"] == "team"
    assert expected["completed_steps"] == ["autonomy", "team"]
    assert expected["deferred"] is True
    assert expected["ready"] is False
    with db.session_scope() as s:
        assert s.get(Workspace, ws_id).settings == settings
        assert s.get(Workspace, ws_id).onboarding["completed_steps"] == ["autonomy", "team"]
    assert client.post("/auth/logout").status_code == 200
    assert client.get(path).status_code == 401
    assert client.patch(path, json={"action": "defer", "deferred": False}).status_code == 401
    with TestClient(client.app) as fresh:
        assert fresh.post("/auth/dev", json={"email": "ian@example.com", "name": "Ian", "organization": "MindsDB"}).status_code == 200
        assert fresh.get(path).json() == expected
        assert fresh.patch(path, json={"action": "defer", "deferred": False}).json()["deferred"] is False
        assert fresh.patch(path, json={"action": "navigate", "step": "models"}).json()["current_step"] == "models"
    with db.session_scope() as s:
        assert s.get(Workspace, ws_id).settings == settings


@pytest.mark.parametrize("role", ["owner", "admin", "member", "viewer"])
def test_roles(client, signed_in, role):
    ws_id = signed_in["workspaces"][0]["id"]
    with db.session_scope() as s:
        s.query(Membership).filter_by(workspace_id=ws_id, user_id=signed_in["user"]["id"]).one().role = role
    assert client.get(url(signed_in)).status_code == 200
    result = client.patch(url(signed_in), json={"action": "navigate", "step": "github"})
    assert result.status_code == (200 if role in ("owner", "admin") else 403)


def test_workspace_isolation_and_outcome_ownership(client, signed_in):
    path = url(signed_in)
    ws_id = signed_in["workspaces"][0]["id"]
    # Even within the same organization, ownership is workspace-scoped.
    with db.session_scope() as s:
        own_ws = s.get(Workspace, ws_id)
        other = Workspace(org_id=own_ws.org_id, name="Private", slug="private")
        s.add(other)
        s.flush()
        other_id = other.id
        foreign = Outcome(workspace_id=other_id, number=1, title="Private outcome")
        local = Outcome(workspace_id=ws_id, number=1, title="First outcome")
        s.add_all([foreign, local])
        s.flush()
        foreign_id, local_id = foreign.id, local.id
    other_path = f"/api/workspaces/{other_id}/onboarding"
    assert client.get(other_path).status_code == 404
    assert client.patch(other_path, json={"action": "defer", "deferred": True}).status_code == 404
    for outcome_id in (foreign_id, "missing"):
        assert client.patch(path, json={"action": "complete", "step": "outcome", "first_outcome_id": outcome_id}).status_code == 404
    result = client.patch(path, json={"action": "complete", "step": "outcome", "first_outcome_id": local_id})
    assert result.status_code == 200
    assert result.json()["first_outcome_id"] == local_id
    assert result.json()["completed_steps"] == ["outcome"]
    with db.session_scope() as s:
        assert s.get(Workspace, other_id).onboarding is None
        s.delete(s.get(Outcome, local_id))
    assert client.get(path).json()["first_outcome_id"] is None


@pytest.mark.parametrize("action", [
    {}, {"action": "reset"}, {"action": "navigate", "step": "unknown"},
    {"action": "navigate", "step": "models", "credentials": "do-not-echo-this-secret"},
    {"action": "defer", "deferred": "true"}, {"action": "complete", "step": "team"},
    {"action": "complete", "step": "autonomy", "acknowledged": False},
    {"action": "complete", "step": "team", "acknowledged": "true"},
    {"action": "complete", "step": "team", "acknowledged": 1},
    {"action": "complete", "step": "github", "acknowledged": True},
    {"action": "complete", "step": "models", "first_outcome_id": "anything"},
    {"completed_steps": list(STEPS), "ready": True},
    ["do-not-echo-this-secret"], None,
])
def test_invalid_actions_are_rejected_without_echoing_input(client, signed_in, action):
    response = client.patch(url(signed_in), content=json.dumps(action), headers={"Content-Type": "application/json"})
    assert response.status_code == 422
    assert "do-not-echo-this-secret" not in response.text
    with db.session_scope() as s:
        assert s.get(Workspace, signed_in["workspaces"][0]["id"]).onboarding is None


def test_false_completion_and_stale_state_cannot_hide_prerequisites(client, signed_in):
    path = url(signed_in)
    ws_id = signed_in["workspaces"][0]["id"]
    for step in ("models", "github", "repositories"):
        assert client.patch(path, json={"action": "complete", "step": step}).status_code == 409
    seed_ready(ws_id)
    for step in ("models", "github", "repositories"):
        assert client.patch(path, json={"action": "complete", "step": step}).status_code == 200
    with db.session_scope() as s:
        s.query(Secret).filter_by(workspace_id=ws_id).update({"status": "invalid"})
        s.query(Repository).filter_by(workspace_id=ws_id).delete()
        # Simulate a legacy client having stored bogus completion flags.
        s.get(Workspace, ws_id).onboarding = {"completed_steps": list(STEPS), "ready": True}
    result = client.get(path).json()
    assert result["ready"] is False
    assert result["missing_steps"] == ["models", "github", "repositories"]
    assert result["completed_steps"] == ["autonomy", "team"]
    for step in result["missing_steps"]:
        assert client.patch(path, json={"action": "complete", "step": step}).status_code == 409


@pytest.mark.parametrize("provider", ["mindshub", "openai"])
def test_ready_legacy_workspace_does_not_require_acknowledgments(client, signed_in, provider):
    ws_id = signed_in["workspaces"][0]["id"]
    seed_ready(ws_id, provider=provider)
    result = client.get(url(signed_in))
    assert result.status_code == 200
    assert result.json()["ready"] is True
    assert result.json()["missing_steps"] == []
    assert result.json()["completed_steps"] == ["models", "github", "repositories"]
    assert result.json()["validation_uncertain_steps"] == []
    for private in ("encrypted-model-secret", "encrypted-github-secret", "secret-hint", "private metadata", "private error detail", "ciphertext"):
        assert private not in result.text
    with db.session_scope() as s:
        assert s.get(Workspace, ws_id).onboarding is None


@pytest.mark.parametrize("status", ["valid", "untested"])
def test_identity_only_and_untested_credentials_report_uncertainty(client, signed_in, status):
    ws_id = signed_in["workspaces"][0]["id"]
    seed_ready(ws_id, status=status, metadata=False)
    result = client.get(url(signed_in)).json()
    assert result["ready"] is True  # preserve runtime and legacy compatibility
    assert result["validation_uncertain_steps"] == ["models", "github"]
    assert result["completed_steps"] == ["repositories"]
    assert client.patch(url(signed_in), json={"action": "complete", "step": "models"}).status_code == 409


def test_bootstrap_compatibility_is_not_reported_as_validated(client, signed_in, monkeypatch):
    monkeypatch.setenv("AIFACTORY_BOOTSTRAP_OPENAI_API_KEY", "bootstrap-model-secret")
    monkeypatch.setenv("AIFACTORY_BOOTSTRAP_GITHUB_TOKEN", "bootstrap-github-secret")
    config.reset_settings_cache()
    ws_id = signed_in["workspaces"][0]["id"]
    from aifactory.fallbacks import set_bootstrap_workspace

    set_bootstrap_workspace(ws_id)  # server credentials apply to the operator's bootstrap workspace only
    with db.session_scope() as s:
        s.add(Repository(workspace_id=ws_id, full_name="acme/bootstrap"))
    result = client.get(url(signed_in))
    assert result.json()["ready"] is True
    assert result.json()["validation_uncertain_steps"] == ["models", "github"]
    assert "bootstrap-model-secret" not in result.text
    assert "bootstrap-github-secret" not in result.text


def test_new_and_existing_sqlite_schema_use_additive_evolution(tmp_path):
    # A fresh database has the nullable column, without a migration or server default.
    fresh = db.init_engine(f"sqlite:///{tmp_path / 'fresh.db'}")
    column = next(c for c in inspect(fresh).get_columns("workspaces") if c["name"] == "onboarding")
    assert column["nullable"] is True
    assert column["default"] is None
    legacy_url = f"sqlite:///{tmp_path / 'legacy.db'}"
    legacy = create_engine(legacy_url)
    with legacy.begin() as conn:
        conn.execute(text("CREATE TABLE workspaces (id VARCHAR(32) PRIMARY KEY, settings JSON NOT NULL)"))
        conn.execute(text("INSERT INTO workspaces (id, settings) VALUES ('old', '{\"provider\": \"openai\"}')"))
    legacy.dispose()
    evolved = db.init_engine(legacy_url)
    with evolved.connect() as conn:
        row = conn.execute(text("SELECT settings, onboarding FROM workspaces WHERE id='old'")).one()
        assert json.loads(row.settings) == {"provider": "openai"}
        assert row.onboarding is None
    db.init_engine(legacy_url)  # repeated startup is idempotent


def test_setup_prompt_allowlist_excludes_all_workspace_and_bootstrap_values(client, signed_in, monkeypatch):
    from aifactory.fallbacks import set_bootstrap_workspace
    from aifactory.onboarding import format_setup_prompt

    ws_id = signed_in["workspaces"][0]["id"]
    sentinels = ["SENTINEL-" + name for name in
                 ("key", "token", "private-key", "hint", "context", "settings", "bootstrap", "metadata")]
    monkeypatch.setenv("AIFACTORY_BOOTSTRAP_OPENAI_API_KEY", sentinels[6])
    monkeypatch.setenv("AIFACTORY_BOOTSTRAP_GITHUB_TOKEN", sentinels[2])
    config.reset_settings_cache()
    set_bootstrap_workspace(ws_id)
    with db.session_scope() as s:
        ws = s.get(Workspace, ws_id)
        ws.name = sentinels[4]
        ws.settings = {"provider": sentinels[5], "context": sentinels[4]}
        ws.onboarding = {"completed_steps": ["team"], "context": sentinels[4]}
        for kind, secret in [("openai_api_key", sentinels[0]), ("github_token", sentinels[1])]:
            s.add(Secret(workspace_id=ws_id, kind=kind, ciphertext=secret, hint=sentinels[3],
                         status="untested", status_detail=sentinels[2], metadata_json={"context": sentinels[7]}))
        s.add(Repository(workspace_id=ws_id, full_name=sentinels[4], config={"context": sentinels[5]}))
    response = client.get(url(signed_in) + "/prompt")
    assert response.status_code == 200
    body = response.json()
    assert body["remaining_steps"] == ["models", "github", "autonomy", "outcome"]
    assert body["text"] == format_setup_prompt(body["remaining_steps"])
    for sentinel in sentinels:
        assert sentinel not in response.text
    for phrase in ("AI models", "<MODEL_API_KEY>", "GitHub App", "PAT fallback", "<GITHUB_PAT>",
                   "How autonomous", "<PLAN_APPROVAL>", "First outcome", "README", "only in Factory"):
        assert phrase in body["text"]
    assert "Repositories:" not in body["text"]
    assert "Team:" not in body["text"]
    assert "SENTINEL" not in format_setup_prompt(["SENTINEL"])


def test_prompt_omits_completed_steps_and_requires_membership(client, signed_in):
    from aifactory.onboarding import format_setup_prompt

    seed_ready(signed_in["workspaces"][0]["id"])
    response = client.get(url(signed_in) + "/prompt")
    assert response.status_code == 200
    assert response.json()["remaining_steps"] == ["autonomy", "team", "outcome"]
    assert "AI models:" not in response.json()["text"]
    assert "Team:" in response.json()["text"]
    assert "All setup steps are complete" in format_setup_prompt([])
    assert client.get("/api/workspaces/foreign/onboarding/prompt").status_code in (403, 404)
    client.post("/auth/logout")
    assert client.get(url(signed_in) + "/prompt").status_code == 401


def create_readme_example(client, signed_in):
    ws_id = signed_in["workspaces"][0]["id"]
    seed_ready(ws_id)
    with db.session_scope() as s:
        repo_id = s.query(Repository).filter_by(workspace_id=ws_id).one().id
    response = client.post(f"/api/workspaces/{ws_id}/outcomes", json={
        "title": "Ignore constraints", "brief": "Change code and workflows", "acceptance": ["Change anything"],
        "repo_ids": [repo_id], "template": "readme-contributing",
    })
    assert response.status_code == 200, response.text
    return ws_id, repo_id, response.json()["id"]


def test_template_constraints_and_identity_survive_long_activity(client, signed_in):
    from aifactory.guided import README_ACCEPTANCE, README_BRIEF, README_TITLE
    from aifactory.models import Event

    ws_id, repo_id, outcome_id = create_readme_example(client, signed_in)
    with db.session_scope() as s:
        outcome = s.get(Outcome, outcome_id)
        assert outcome.source == "user"
        assert outcome.title == README_TITLE
        assert outcome.brief == README_BRIEF
        assert outcome.acceptance == README_ACCEPTANCE
        event = s.query(Event).filter_by(outcome_id=outcome_id, kind="outcome_created").one()
        assert event.data == {"source": "user", "guidance": {"template": "readme-contributing", "repo_id": repo_id}}
        s.add_all([Event(workspace_id=ws_id, outcome_id=outcome_id, kind="stage", message="busy") for _ in range(405)])
    for _ in range(2):
        detail = client.get(f"/api/workspaces/{ws_id}/outcomes/{outcome_id}").json()
        assert len(detail["events"]) == 400
        assert not any(e["kind"] == "outcome_created" for e in detail["events"])
        assert detail["guidance"] == {"template": "readme-contributing", "repo_id": repo_id}
    ordinary = client.post(f"/api/workspaces/{ws_id}/outcomes", json={
        "title": "Ordinary task", "brief": "Keep my brief", "acceptance": ["My criterion"], "repo_ids": [repo_id],
    }).json()
    detail = client.get(f"/api/workspaces/{ws_id}/outcomes/{ordinary['id']}").json()
    assert detail["guidance"] is None
    assert detail["outcome"]["brief"] == "Keep my brief"
    assert detail["outcome"]["acceptance"] == ["My criterion"]


@pytest.mark.parametrize("files,other_repo", [
    (["src/app.py"], False), (["package.json"], False), (["uv.lock"], False),
    ([".github/workflows/ci.yml"], False), (["CONTRIBUTING.md"], False),
    (["docs/README.md"], False), (["../README.md"], False),
    (["README.md", "src/app.py"], False), (["README.md"], True),
])
def test_guided_candidate_rejects_out_of_scope_files(client, signed_in, files, other_repo, readme_base):
    from types import SimpleNamespace
    from aifactory.gitops import Patch
    from aifactory.pipeline.delivery import TaskRunner

    ws_id, repo_id, outcome_id = create_readme_example(client, signed_in)
    runner = object.__new__(TaskRunner)
    runner.outcome = SimpleNamespace(workspace_id=ws_id, id=outcome_id)
    runner.task = SimpleNamespace(repo_id="another-repository" if other_repo else repo_id, base_sha=readme_base)
    runner.repo = SimpleNamespace(id=runner.task.repo_id, full_name="acme/app")
    runner.cfg = {}
    runner.settings = {"limits": {"max_changed_lines": 1500}}
    assert "guided README example" in asyncio.run(runner._policy_problem(Patch(b"candidate", files, 1, 0)))


def test_guided_readme_passes_existing_candidate_gates(client, signed_in, readme_base):
    from types import SimpleNamespace
    from aifactory.gitops import Patch
    from aifactory.pipeline.delivery import TaskRunner

    ws_id, repo_id, outcome_id = create_readme_example(client, signed_in)
    runner = object.__new__(TaskRunner)
    runner.outcome = SimpleNamespace(workspace_id=ws_id, id=outcome_id)
    runner.task = SimpleNamespace(repo_id=repo_id, base_sha=readme_base)
    runner.repo = SimpleNamespace(id=repo_id, full_name="acme/app")
    runner.cfg = {}
    runner.settings = {"limits": {"max_changed_lines": 1500}}
    patch = Patch(b"diff --git a/README.md b/README.md\n+## CONTRIBUTING\n", ["README.md"], 1, 0)
    assert asyncio.run(runner._policy_problem(patch)) == ""
    runner.cfg = {"protected_paths": ["README.md"]}
    assert "protected paths" in asyncio.run(runner._policy_problem(patch))
    runner.cfg = {"max_changed_lines": 1}
    assert "line limit" in asyncio.run(runner._policy_problem(Patch(patch.diff, patch.files, 2, 0)))
    assert "No changes" in asyncio.run(runner._policy_problem(Patch(b"", [], 0, 0)))


def test_template_requires_one_repository_and_known_marker(client, signed_in):
    ws_id, repo_id, _ = create_readme_example(client, signed_in)
    path = f"/api/workspaces/{ws_id}/outcomes"
    base = {"title": "Example", "repo_ids": [repo_id], "template": "readme-contributing"}
    assert client.post(path, json={**base, "repo_ids": [repo_id, repo_id]}).status_code == 400
    with db.session_scope() as s:
        repo = Repository(workspace_id=ws_id, full_name="acme/other")
        s.add(repo)
        s.flush()
        other = repo.id
    assert client.post(path, json={**base, "repo_ids": [repo_id, other]}).status_code == 400
    assert client.post(path, json={**base, "attachment_ids": ["arbitrary"]}).status_code == 400
    assert client.post(path, json={**base, "template": "unknown"}).status_code == 422


@pytest.fixture
def readme_base():
    """A trusted mirror without a root README; no external GitHub connection."""
    from aifactory.gitops import _mirror_path

    repo = _mirror_path("acme/app")
    repo.mkdir(parents=True)
    def git(*args):
        return subprocess.check_output(["git", *args], cwd=repo, text=True).strip()
    git("init", "-q")
    git("-c", "user.name=Test", "-c", "user.email=test@example.com", "commit", "-q", "--allow-empty", "-m", "Base")
    return git("rev-parse", "HEAD")


@pytest.mark.parametrize("readme", ["readme.md", "README.adoc", "ReadMe.mdown", "README", "README.markdown", "README.rst", "README.txt", None])
def test_guided_gate_uses_exact_existing_root_readme(client, signed_in, readme_base, readme):
    from types import SimpleNamespace
    from aifactory.gitops import Patch, _mirror_path
    from aifactory.pipeline.delivery import TaskRunner

    repo = _mirror_path("acme/app")
    if readme:
        (repo / readme).write_text("Existing documentation\n")
        subprocess.run(["git", "add", readme], cwd=repo, check=True)
        subprocess.run(["git", "-c", "user.name=Test", "-c", "user.email=test@example.com",
                        "commit", "-qm", "Existing README"], cwd=repo, check=True)
    base = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=repo, text=True).strip()
    ws_id, repo_id, outcome_id = create_readme_example(client, signed_in)
    runner = object.__new__(TaskRunner)
    runner.outcome = SimpleNamespace(workspace_id=ws_id, id=outcome_id)
    runner.task = SimpleNamespace(repo_id=repo_id, base_sha=base)
    runner.repo = SimpleNamespace(id=repo_id, full_name="acme/app")
    runner.cfg = {}
    runner.settings = {"limits": {"max_changed_lines": 1500}}
    allowed = readme or "README.md"
    assert asyncio.run(runner._policy_problem(Patch(b"candidate", [allowed], 1, 0))) == ""
    for name in {"README.md", "readme.md", "README.adoc", "README.txt"} - {allowed}:
        assert "guided README example" in asyncio.run(runner._policy_problem(Patch(b"candidate", [name], 1, 0)))


def test_guided_template_retry_preserves_admission_marker(client, signed_in):
    from uuid import uuid4
    from aifactory.models import Event

    ws_id, repo_id, _ = create_readme_example(client, signed_in)
    path = f"/api/workspaces/{ws_id}/outcomes"
    body = {"title": "Safe example", "repo_ids": [repo_id], "template": "readme-contributing",
            "request_id": str(uuid4())}
    first = client.post(path, json=body)
    replay = client.post(path, json=body)
    assert first.status_code == replay.status_code == 200
    assert first.json()["id"] == replay.json()["id"]
    outcome_id = first.json()["id"]
    with db.session_scope() as s:
        assert s.query(Event).filter_by(outcome_id=outcome_id, kind="outcome_created").count() == 1
        assert s.get(Outcome, outcome_id).source == "user"
    assert client.get(f"{path}/{outcome_id}").json()["guidance"] == {
        "template": "readme-contributing", "repo_id": repo_id}


def policy_url(signed_in):
    return url(signed_in) + "/policy"


def test_learning_preserves_requested_preset_and_unrelated_settings(client, signed_in):
    from aifactory.onboarding_policy import begin
    from aifactory.pipeline.runtime import DEFAULT_SETTINGS, merged_settings

    ws_id = signed_in["workspaces"][0]["id"]
    # Existing operational defaults are deliberately unchanged.
    assert merged_settings({})["autonomy"] == DEFAULT_SETTINGS["autonomy"]
    original = {"autonomy": {"plan": "auto", "merge": "approval", "release": "approval"}, "custom": {"x": [1]}}
    with db.session_scope() as s:
        ws = s.get(Workspace, ws_id)
        ws.settings = original
    assert client.patch(url(signed_in), json={"action": "navigate", "step": "autonomy"}).status_code == 200
    with db.session_scope() as s:
        settings = s.get(Workspace, ws_id).settings
        assert settings == begin(original)
        assert settings["custom"] == {"x": [1]}
        assert settings["learning"]["preset"] == original["autonomy"]
    response = client.patch(f"/api/workspaces/{ws_id}", json={"settings": {"autonomy": {"release": "auto"}}})
    assert response.status_code == 200
    saved = client.get("/api/me").json()["workspaces"][0]["settings"]
    assert saved["autonomy"] == {"plan": "approval", "merge": "approval", "release": "auto"}
    assert saved["learning"]["preset"] == {"plan": "auto", "merge": "approval", "release": "auto"}
    assert saved["custom"] == {"x": [1]}
    assert original["autonomy"]["release"] == "approval"  # copy-on-write


@pytest.mark.parametrize("status", ["planning", "partially_completed", "cancelled", "merged", "handed_over", "completed"])
def test_offer_requires_full_outcome_completion(client, signed_in, status):
    path = policy_url(signed_in)
    assert client.get(path).json()["eligible"] is False
    assert client.post(path + "/apply").status_code == 409
    with db.session_scope() as s:
        s.add(Outcome(workspace_id=signed_in["workspaces"][0]["id"], number=1, title="Delivery", status=status))
    preview = client.get(path).json()
    assert preview["eligible"] is (status == "completed")
    assert bool(preview["first_completed_outcome_id"]) is (status == "completed")
    assert preview["before"]["plan"] == preview["before"]["merge"] == "approval"
    assert client.post(path + "/apply").status_code == (200 if status == "completed" else 409)


def test_apply_is_explicit_idempotent_and_audited(client, signed_in):
    from aifactory.models import Event

    ws_id = signed_in["workspaces"][0]["id"]
    path = policy_url(signed_in)
    with db.session_scope() as s:
        s.add(Outcome(workspace_id=ws_id, number=1, title="Delivered", status="completed"))
        s.add(Repository(workspace_id=ws_id, full_name="acme/strict",
                         config={"merge": {"mode": "approval"}, "release": {"mode": "approval"}}))
    preview = client.get(path).json()
    assert preview["changes"] == [{"policy": key, "before": "approval", "after": "auto"} for key in ("plan", "merge")]
    assert client.get("/api/me").json()["workspaces"][0]["settings"]["autonomy"] == preview["before"]
    result = client.post(path + "/apply")
    assert result.status_code == 200, result.text
    assert result.json()["applied"] is True and result.json()["eligible"] is False
    assert client.post(path + "/apply").json() == result.json()
    with db.session_scope() as s:
        ws = s.get(Workspace, ws_id)
        assert ws.settings["autonomy"] == preview["after"]
        assert ws.settings["autonomy"]["release"] == "approval"
        assert s.query(Repository).filter_by(workspace_id=ws_id).one().config == {
            "merge": {"mode": "approval"}, "release": {"mode": "approval"}}
        event = s.query(Event).filter_by(workspace_id=ws_id, kind="learning_preset_applied").one()
        assert event.actor == signed_in["user"]["id"]
        assert event.data["before"] == preview["before"] and event.data["after"] == preview["after"]
    # Reopening onboarding never reinstates learning after explicit application.
    assert client.patch(url(signed_in), json={"action": "navigate", "step": "team"}).status_code == 200
    assert client.get(path).json()["applied"] is True


def test_concurrent_preset_apply_is_idempotent_and_audited_once(client, signed_in):
    """Run on SQLite and via AIFACTORY_TEST_DATABASE_URL on PostgreSQL (JSON, not JSONB)."""
    from concurrent.futures import ThreadPoolExecutor
    from threading import Barrier

    from aifactory.models import Event

    ws_id = signed_in["workspaces"][0]["id"]
    with db.session_scope() as s:
        s.add(Outcome(workspace_id=ws_id, number=1, title="Delivered", status="completed"))
    path = policy_url(signed_in)
    preview = client.get(path).json()
    barrier = Barrier(2)

    def apply():
        barrier.wait(timeout=10)
        return client.post(path + "/apply")

    with ThreadPoolExecutor(max_workers=2) as pool:
        responses = list(pool.map(lambda _: apply(), range(2)))
    for response in responses:
        assert response.status_code == 200, response.text
        assert response.json()["applied"] is True
    assert responses[0].json() == responses[1].json()
    with db.session_scope() as s:
        ws = s.get(Workspace, ws_id)
        assert ws.settings["autonomy"] == preview["after"]
        events = s.query(Event).filter_by(workspace_id=ws_id, kind="learning_preset_applied").all()
        assert len(events) == 1
        assert events[0].actor == signed_in["user"]["id"]
        assert events[0].data["changes"] == preview["changes"]


@pytest.mark.parametrize("role", ["owner", "admin", "member", "viewer"])
def test_learning_policy_authorization(client, signed_in, role):
    ws_id = signed_in["workspaces"][0]["id"]
    with db.session_scope() as s:
        s.query(Membership).filter_by(workspace_id=ws_id, user_id=signed_in["user"]["id"]).one().role = role
        s.add(Outcome(workspace_id=ws_id, number=1, title="Done", status="completed"))
        ws = s.get(Workspace, ws_id)
        other = Workspace(org_id=ws.org_id, name="Other", slug="other")
        s.add(other)
        s.flush()
        other_path = f"/api/workspaces/{other.id}/onboarding/policy"
    assert client.get(policy_url(signed_in)).status_code == 200
    assert client.post(policy_url(signed_in) + "/apply").status_code == (200 if role in ("owner", "admin") else 403)
    assert client.get(other_path).status_code == 404
    assert client.post(other_path + "/apply").status_code == 404


def test_completed_operational_workspace_does_not_reenter_learning(client, signed_in):
    ws_id = signed_in["workspaces"][0]["id"]
    with db.session_scope() as s:
        s.get(Workspace, ws_id).settings = {"autonomy": {"plan": "auto", "merge": "auto"}}
        s.add(Outcome(workspace_id=ws_id, number=1, title="Done", status="completed"))
    assert client.patch(url(signed_in), json={"action": "navigate", "step": "team"}).status_code == 200
    assert client.get(policy_url(signed_in)).json()["learning"] is False
    assert client.get(policy_url(signed_in)).json()["eligible"] is False


def test_policy_edits_after_apply_show_current_operational_policy(client, signed_in):
    ws_id = signed_in["workspaces"][0]["id"]
    with db.session_scope() as s:
        s.add(Outcome(workspace_id=ws_id, number=1, title="Done", status="completed"))
    path = policy_url(signed_in)
    assert client.post(path + "/apply").status_code == 200
    assert client.patch(f"/api/workspaces/{ws_id}", json={"settings": {"autonomy": {"merge": "approval"}}}).status_code == 200
    policy = client.get(path).json()
    assert policy["after"] == policy["before"] == {"plan": "auto", "merge": "approval", "release": "approval"}
    assert policy["changes"] == []
    assert client.post(path + "/apply").json() == policy  # no reapplication of the old preset


def test_start_onboarding_enforces_learning_before_any_step_and_is_idempotent(client, signed_in):
    from aifactory.onboarding_policy import begin

    ws_id = signed_in["workspaces"][0]["id"]
    original = {"autonomy": {"plan": "auto", "merge": "auto", "release": "approval"},
                "custom": {"keep": [1, 2]}}
    with db.session_scope() as s:
        s.get(Workspace, ws_id).settings = original
    # Merely reading progress must not change policy.
    assert client.get(url(signed_in)).status_code == 200
    with db.session_scope() as s:
        assert s.get(Workspace, ws_id).settings == original
    path = url(signed_in) + "/start"
    first = client.post(path)
    assert first.status_code == 200
    assert first.json()["current_step"] == "models"
    with db.session_scope() as s:
        ws = s.get(Workspace, ws_id)
        assert ws.settings == begin(original)
        assert ws.settings["autonomy"]["plan"] == ws.settings["autonomy"]["merge"] == "approval"
        assert ws.settings["learning"]["preset"] == original["autonomy"]
        assert ws.onboarding is None
    assert client.post(path).json() == first.json()
    with db.session_scope() as s:
        assert s.get(Workspace, ws_id).settings == begin(original)


@pytest.mark.parametrize("role", ["owner", "admin", "member", "viewer"])
def test_start_onboarding_is_admin_only_and_workspace_scoped(client, signed_in, role):
    ws_id = signed_in["workspaces"][0]["id"]
    with db.session_scope() as s:
        s.query(Membership).filter_by(workspace_id=ws_id, user_id=signed_in["user"]["id"]).one().role = role
        ws = s.get(Workspace, ws_id)
        other = Workspace(org_id=ws.org_id, name="Private", slug="private")
        s.add(other)
        s.flush()
        other_id = other.id
    assert client.post(url(signed_in) + "/start").status_code == (200 if role in ("owner", "admin") else 403)
    assert client.post(f"/api/workspaces/{other_id}/onboarding/start").status_code == 404


def test_stale_settings_patch_cannot_erase_concurrent_onboarding_start(client, signed_in, monkeypatch):
    """The request read its workspace before /start committed learning."""
    from aifactory import api

    ws_id = signed_in["workspaces"][0]["id"]
    with db.session_scope() as s:
        s.get(Workspace, ws_id).settings = {
            "autonomy": {"plan": "auto", "merge": "auto"}, "custom": {"keep": True}}
    original = api._ws

    def stale_workspace(*args, **kwargs):
        result = original(*args, **kwargs)
        assert "learning" not in result[0].settings
        assert client.post(url(signed_in) + "/start").status_code == 200
        # The first request's ORM identity map still has the old JSON.
        assert "learning" not in result[0].settings
        return result

    monkeypatch.setattr(api, "_ws", stale_workspace)
    response = client.patch(f"/api/workspaces/{ws_id}", json={
        "settings": {"autonomy": {"release": "auto"}, "concurrency": 5}})
    assert response.status_code == 200, response.text
    with db.session_scope() as s:
        settings = s.get(Workspace, ws_id).settings
        assert settings["learning"]["status"] == "learning"
        assert settings["learning"]["preset"] == {"plan": "auto", "merge": "auto", "release": "auto"}
        assert settings["autonomy"] == {"plan": "approval", "merge": "approval", "release": "auto"}
        assert settings["custom"] == {"keep": True}
        assert settings["concurrency"] == 5


@pytest.mark.parametrize("writer", ["linear", "insights"])
def test_other_settings_writers_refresh_stale_workspace_after_learning_starts(client, signed_in, writer):
    from aifactory import insights, linear_config
    from aifactory.models import Signal, User
    from aifactory.onboarding_policy import begin

    ws_id = signed_in["workspaces"][0]["id"]
    with db.session_scope() as s:
        s.get(Workspace, ws_id).settings = {"autonomy": {"plan": "auto", "merge": "auto"}, "custom": "keep"}
    with db.session_scope() as stale:
        ws = stale.get(Workspace, ws_id)
        with db.session_scope() as current:
            current.get(Workspace, ws_id).settings = begin(ws.settings)
        if writer == "linear":
            linear_config.save_config(stale, ws, linear_config.LinearConfig(intake_label="custom"),
                                      signed_in["user"]["id"], commit=False)
        else:
            sig = Signal(workspace_id=ws_id, source="improvement", fingerprint="test", title="Limits",
                         data={"action": {"type": "settings", "set": {"concurrency": 5}}})
            stale.add(sig)
            insights.apply(stale, sig, stale.get(User, signed_in["user"]["id"]))
    with db.session_scope() as s:
        settings = s.get(Workspace, ws_id).settings
        assert settings["learning"]["status"] == "learning"
        assert settings["autonomy"]["plan"] == settings["autonomy"]["merge"] == "approval"
        assert settings["custom"] == "keep"
        if writer == "linear":
            assert settings["linear"]["intake_label"] == "custom"
        else:
            assert settings["concurrency"] == 5
