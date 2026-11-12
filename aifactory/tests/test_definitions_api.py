from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from aifactory.db import session_scope
from aifactory.models import DefinitionState, Membership, Repository, Signal


def setup(client, signed_in):
    ws = signed_in["workspaces"][0]["id"]
    return ws, f"/api/workspaces/{ws}/definitions"


def bundle(body="Hello %_ WORLD"):
    return {"knowledge": [{"id": "page-one", "name": "Architecture", "description": "Reference", "body": body}]}


def test_save_history_restore_and_conflicts(client, signed_in):
    ws, root = setup(client, signed_in)
    assert client.get(root).json()["revision"] == 0
    first = client.put(root, json={"expected_revision": 0, "bundle": bundle()})
    assert first.status_code == 200, first.text
    one = first.json()
    assert one["author"] == signed_in["user"]["id"]
    invalid = bundle()
    invalid["knowledge"][0]["description"] = ""
    response = client.put(root, json={"expected_revision": 1, "bundle": invalid})
    assert response.status_code == 422
    assert response.json()["detail"]["errors"][0]["path"] == "knowledge.0.description"
    assert client.put(root, json={"expected_revision": 0, "bundle": bundle("stale")}).status_code == 409
    assert client.get(root).json() == one
    two = client.put(root, json={"expected_revision": 1, "bundle": bundle("Second")}).json()
    assert client.get(root + "/versions/" + one["revision_id"]).json()["bundle"] == one["bundle"]
    versions = client.get(root + "/versions?limit=1&offset=1").json()["versions"]
    assert len(versions) == 1 and versions[0]["id"] == one["revision_id"]
    restored = client.post(root + "/restore", json={"expected_revision": 2, "revision_id": one["revision_id"]})
    assert restored.status_code == 200, restored.text
    assert restored.json()["revision"] == 3
    assert restored.json()["bundle"] == one["bundle"]
    assert restored.json()["author"] == signed_in["user"]["id"]
    assert client.get(root + "/versions/" + restored.json()["revision_id"]).json()["restored_from"] == 1
    assert client.post(root + "/restore", json={"expected_revision": 2, "revision_id": two["revision_id"]}).status_code == 409
    assert client.get(root + "/versions/missing").status_code == 404


@pytest.mark.parametrize("field,source", [
    ("source.path", {"kind": "managed", "path": "../secret"}),
    ("source.repository_id", {"kind": "repository", "path": "knowledge/a.md"}),
])
def test_source_errors_have_explicit_field_context(client, signed_in, field, source):
    _, root = setup(client, signed_in)
    body = bundle()
    body["knowledge"][0]["source"] = source
    response = client.put(root, json={"expected_revision": 0, "bundle": body})
    assert response.status_code == 422
    error = response.json()["detail"]["errors"][0]
    assert error["field"] == field
    assert error["path"] == "knowledge.0." + field
    assert client.get(root).json()["revision"] == 0


def test_source_selection_cas_read_only_and_last_valid(client, signed_in):
    ws, root = setup(client, signed_in)
    one = client.put(root, json={"expected_revision": 0, "bundle": bundle()}).json()
    with session_scope() as db:
        repo = Repository(workspace_id=ws, full_name="org/source")
        db.add(repo)
        db.flush()
        rid = repo.id
        state = db.get(DefinitionState, ws)
        state.sync_diagnostics = {"status": "invalid", "errors": [{"path": "skills/a", "message": "bad"}]}
    response = client.put(root + "/source", json={"expected_revision": 1,
                          "source": {"kind": "repository", "repository_id": rid, "folder": ".factory/"}})
    assert response.status_code == 200, response.text
    selected = response.json()
    assert selected["revision"] == 2
    assert selected["desired_source"]["folder"] == ".factory"
    assert selected["active_source"] == {"kind": "managed"}
    assert selected["bundle"] == one["bundle"]
    assert selected["sync_diagnostics"]["status"] == "invalid"
    # Two admins loaded revision 1. A source-only write must invalidate it.
    assert client.put(root + "/source", json={"expected_revision": 1, "source": {"kind": "managed"}}).status_code == 409
    assert client.put(root, json={"expected_revision": 2, "bundle": bundle()}).status_code == 409
    assert client.post(root + "/restore", json={"expected_revision": 2, "revision_id": one["revision_id"]}).status_code == 409
    assert client.get(root).json() == selected
    assert client.put(root + "/source", json={"expected_revision": 2,
                      "source": {"kind": "repository", "repository_id": rid, "folder": "../bad"}}).status_code == 422
    assert client.put(root + "/source", json={"expected_revision": 2, "source": {"kind": "managed"}}).status_code == 200
    assert client.post(root + "/restore", json={"expected_revision": 3, "revision_id": one["revision_id"]}).status_code == 200
    assert len(client.get(root + "/versions").json()["versions"]) == 4


def test_viewer_can_inspect_but_not_mutate(client, signed_in):
    ws, root = setup(client, signed_in)
    one = client.put(root, json={"expected_revision": 0, "bundle": bundle()}).json()
    viewer = TestClient(client.app)
    viewer.post("/auth/dev", json={"email": "viewer@example.test", "organization": "Other"})
    uid = viewer.get("/api/me").json()["user"]["id"]
    with session_scope() as db:
        db.add(Membership(workspace_id=ws, user_id=uid, role="viewer"))
    for path in ["", "/versions", "/versions/" + one["revision_id"], "/knowledge", "/knowledge/page-one"]:
        assert viewer.get(root + path).status_code == 200
    assert viewer.put(root, json={"expected_revision": 1, "bundle": bundle()}).status_code == 403
    assert viewer.put(root + "/source", json={"expected_revision": 1, "source": {"kind": "managed"}}).status_code == 403
    assert viewer.post(root + "/restore", json={"expected_revision": 1, "revision_id": one["revision_id"]}).status_code == 403


def test_notes_versioned_searchable_and_restorable(client, signed_in):
    ws, root = setup(client, signed_in)
    with session_scope() as db:
        repo = Repository(workspace_id=ws, full_name="org/notes", config={"context": "Original notes"})
        db.add(repo)
        db.flush()
        rid = repo.id
    one = client.put(root, json={"expected_revision": 0, "bundle": bundle()}).json()
    assert client.patch(f"/api/workspaces/{ws}/repositories/{rid}", json={"config": {"context": "Changed notes"}}).status_code == 200
    two = client.get(root).json()
    assert two["revision"] == 2 and two["author"] == signed_in["user"]["id"]
    with session_scope() as db:
        sig = Signal(workspace_id=ws, source="improvement", fingerprint="note", title="Note",
                     data={"action": {"type": "team_note", "repo_id": rid, "note": "Extra literal %_"}})
        db.add(sig)
        db.flush()
        sid = sig.id
    assert client.post(f"/api/workspaces/{ws}/signals/{sid}", json={"action": "accept"}).status_code == 200
    assert client.get(root).json()["revision"] == 3
    pages = client.get(root + "/knowledge", params={"q": "LITERAL %_"}).json()["pages"]
    assert len(pages) == 1 and pages[0]["id"] == "repository-context-" + rid
    assert client.get(root + "/knowledge", params={"q": "%not-a-wildcard%"}).json()["pages"] == []
    assert "Changed notes" in client.get(root + "/knowledge/" + pages[0]["id"]).json()["body"]
    assert client.post(root + "/restore", json={"expected_revision": 3, "revision_id": one["revision_id"]}).status_code == 200
    with session_scope() as db:
        assert db.get(Repository, rid).config["context"] == "Original notes"


def test_search_deterministic_bounded_and_scoped():
    from aifactory.definitions import Bundle, KnowledgePage, Snapshot
    from aifactory.definitions.knowledge import read_knowledge, search_knowledge
    from aifactory.definitions.validation import DefinitionError
    snapshot = Snapshot("ws", 1, Bundle(knowledge=[
        KnowledgePage(id="b", name="Z", description="d", body="x" * 1000),
        KnowledgePage(id="a", name="A", description="d", body="a"),
    ]))
    assert [p["id"] for p in search_knowledge(snapshot, "")] == ["a", "b"]
    assert len(search_knowledge(snapshot, "x")[0]["snippet"]) == 320
    assert read_knowledge(snapshot, "ws/knowledge/a").id == "a"
    with pytest.raises(DefinitionError):
        read_knowledge(snapshot, "foreign/knowledge/a")
    with pytest.raises(DefinitionError):
        search_knowledge(snapshot, "", limit=51)


def test_schema_and_request_bounds(client, signed_in):
    _, root = setup(client, signed_in)
    bad = bundle()
    bad["knowledge"][0]["role"] = "invented"
    response = client.put(root, json={"expected_revision": 0, "bundle": bad})
    assert response.status_code == 422
    assert response.json()["detail"]["errors"][0]["path"] == "bundle.knowledge.0.role"
    response = client.put(root, content=b" " * (8 * 1024 * 1024 + 1))
    assert response.status_code == 422
    assert response.json()["detail"]["errors"][0]["field"] == "body"
    assert client.get(root).json()["revision"] == 0


def test_source_selection_from_empty_workspace_consumes_revision(client, signed_in):
    _, root = setup(client, signed_in)
    assert client.put(root + "/source", json={"expected_revision": 0, "source": {"kind": "managed"}}).status_code == 200
    assert client.get(root).json()["revision"] == 1
    assert client.put(root + "/source", json={"expected_revision": 0, "source": {"kind": "managed"}}).status_code == 409


def test_repository_snapshot_provenance_links_and_restore_guard(client, signed_in):
    from aifactory.definitions import Bundle, KnowledgePage, Source, save_bundle
    ws, root = setup(client, signed_in)
    with session_scope() as db:
        repo = Repository(workspace_id=ws, full_name="org/code", default_branch="main")
        db.add(repo)
        db.flush()
        rid = repo.id
        save_bundle(db, ws, Bundle(knowledge=[KnowledgePage(
            id="from-code", name="code", description="d", body="Validated content",
            source=Source(kind="repository", repository_id=rid, path=".factory/knowledge/page.md", commit="abc"))]),
            expected_revision=0, author="sync", active_source={"kind": "repository", "repository_id": rid,
                                                              "folder": ".factory", "commit": "abc"})
        db.get(DefinitionState, ws).desired_source = {"kind": "repository", "repository_id": rid, "folder": ".factory"}
    active = client.get(root).json()
    assert any(link["url"] == "https://github.com/org/code/blob/abc/.factory/knowledge/page.md" for link in active["source_links"])
    assert client.delete(f"/api/workspaces/{ws}/repositories/{rid}").status_code == 409
    assert client.put(root, json={"expected_revision": 1, "bundle": bundle()}).status_code == 409
    assert client.put(root + "/source", json={"expected_revision": 1, "source": {"kind": "managed"}}).status_code == 200
    assert client.get(root).json()["active_source"] == {"kind": "managed"}
    assert client.get(root).json()["bundle"] == {"schema_version": 1, "skills": [], "instructions": [], "knowledge": []}
    assert client.post(root + "/restore", json={"expected_revision": 2, "revision_id": active["revision_id"]}).status_code == 409
    saved = client.put(root, json={"expected_revision": 2, "bundle": bundle()})
    assert saved.status_code == 200, saved.text
    assert saved.json()["active_source"] == {"kind": "managed"}
    assert client.get(root + "/versions/" + saved.json()["revision_id"]).json()["active_source"] == {"kind": "managed"}


def test_search_service_respects_role_and_repository_scope(client, signed_in):
    from aifactory.definitions import Bundle, KnowledgePage, resolve_definitions, save_bundle
    from aifactory.definitions.knowledge import read_knowledge, search_knowledge
    from aifactory.definitions.validation import DefinitionError
    ws, _ = setup(client, signed_in)
    with session_scope() as db:
        a, b = Repository(workspace_id=ws, full_name="org/a"), Repository(workspace_id=ws, full_name="org/b")
        db.add_all([a, b])
        db.flush()
        save_bundle(db, ws, Bundle(knowledge=[
            KnowledgePage(id="visible", name="visible", description="d", repository_id=a.id),
            KnowledgePage(id="other-repo", name="hidden", description="d", repository_id=b.id),
            KnowledgePage(id="other-role", name="hidden-role", description="d", role="verifier"),
        ]), expected_revision=0, author="test")
        scope = resolve_definitions(db, ws, "builder", task_repository_id=a.id)
        assert [p["id"] for p in search_knowledge(scope, "")] == ["visible"]
        for page_id in ["other-repo", "other-role"]:
            with pytest.raises(DefinitionError):
                read_knowledge(scope, page_id)


def test_generated_team_notes_are_not_a_second_editor(client, signed_in):
    ws, root = setup(client, signed_in)
    with session_scope() as db:
        repo = Repository(workspace_id=ws, full_name="org/notes", config={"context": "Do not duplicate"})
        db.add(repo)
        db.flush()
        rid = repo.id
    snapshot = client.get(root).json()
    projected = snapshot["bundle"]
    projected["knowledge"][0]["body"] = "Attempted app edit"
    response = client.put(root, json={"expected_revision": 0, "bundle": projected})
    assert response.status_code == 422
    assert "repository configuration" in response.text
    with session_scope() as db:
        assert db.get(Repository, rid).config["context"] == "Do not duplicate"
    assert client.get(root).json()["revision"] == 0


def test_versioned_team_notes_keep_prompt_behavior(client, signed_in):
    from types import SimpleNamespace
    from aifactory.prompts import build_prompt, plan_prompt, verify_prompt
    ws, _ = setup(client, signed_in)
    with session_scope() as db:
        repo = Repository(workspace_id=ws, full_name="org/prompts", config={})
        db.add(repo)
        db.flush()
        rid = repo.id
    assert client.patch(f"/api/workspaces/{ws}/repositories/{rid}", json={"config": {"context": "Use the team convention"}}).status_code == 200
    with session_scope() as db:
        repo = db.get(Repository, rid)
        task = SimpleNamespace(key="T", title="Title", spec="Spec", acceptance=[], id="task")
        outcome = SimpleNamespace(number=1, title="Title", brief="Brief", acceptance=[], repo_ids=[rid])
        assert "Use the team convention" in build_prompt(task, outcome, repo, [], [])
        assert "Use the team convention" in verify_prompt(task, repo, "abc", {}, [])
        assert "Use the team convention" in plan_prompt(outcome, [repo], [], [])


def test_skills_instructions_and_knowledge_share_managed_history(client, signed_in):
    _, root = setup(client, signed_in)
    body = {"skills": [{"id": "skill", "name": "testing", "description": "When testing", "body": "Run checks",
                        "files": {"examples.md": "Example"}}],
            "instructions": [{"id": "instructions", "name": "builder-rules", "description": "Build rules",
                              "role": "builder", "body": "Keep changes focused"}],
            "knowledge": [{"id": "knowledge", "name": "architecture", "description": "Architecture", "body": "Notes"}]}
    saved = client.put(root, json={"expected_revision": 0, "bundle": body})
    assert saved.status_code == 200, saved.text
    active = saved.json()
    assert len(active["entries"]) == 3
    assert client.get(root + "/versions/" + active["revision_id"]).json()["bundle"] == active["bundle"]
