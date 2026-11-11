"""Definition foundation contracts; no network, agent execution or new dependencies."""
from __future__ import annotations

import os
from unittest.mock import patch

import pytest
from pydantic import ValidationError
from sqlalchemy import select
from sqlalchemy.orm import Session

from aifactory import db
from aifactory.definitions import (
    BUNDLE_LIMIT, Bundle, DefinitionError, KnowledgePage, RevisionConflict, RoleInstruction,
    Skill, Source, bundle_size, get_snapshot, guard_disconnect, history, parse_frontmatter,
    read_skill_folder, render_skill, resolve_definitions, restore_revision, save_bundle, validate_bundle,
)
from aifactory.models import DefinitionRevision, Organization, Repository, Workspace


@pytest.fixture
def workspace():
    with db.session_scope() as session:
        org = Organization(name="Definitions")
        session.add(org)
        session.flush()
        ws = Workspace(org_id=org.id, name="one", slug="one")
        other = Workspace(org_id=org.id, name="two", slug="two")
        session.add_all([ws, other])
        session.flush()
        repos = [Repository(workspace_id=w.id, full_name=f"org/{n}", config={})
                 for w, n in ((ws, "a"), (ws, "b"), (other, "c"))]
        session.add_all(repos)
        session.flush()
        return ws.id, other.id, [r.id for r in repos]


def skill(**kwargs):
    return Skill(name="testing", description="When testing", **kwargs)


def save(session, ws, bundle, expected=0):
    return save_bundle(session, ws, bundle, expected_revision=expected, author="alice")


def fill(bundle, char="x"):
    page = bundle.knowledge[0]
    remaining = BUNDLE_LIMIT - bundle_size(bundle)
    width = len(char.encode())
    page.body += char * (remaining // width) + "x" * (remaining % width)
    assert bundle_size(bundle) == BUNDLE_LIMIT
    return bundle


def test_minimal_and_support_roundtrip(tmp_path, workspace):
    tmp_path = tmp_path / "skill"
    tmp_path.mkdir()
    text = "---\nname: testing\ndescription: 'When: it''s needed'\n---\n# Instructions\n"
    (tmp_path / "SKILL.md").write_text(text)
    (tmp_path / "docs").mkdir()
    (tmp_path / "docs/example.txt").write_text("é🦊")
    s = read_skill_folder(tmp_path)
    fields, body = parse_frontmatter(render_skill(s))
    assert fields == {"name": s.name, "description": s.description}
    assert body == s.body
    with db.session_scope() as session:
        snapshot = save(session, workspace[0], Bundle(skills=[s]))
        assert snapshot.bundle.skills[0].files == {"docs/example.txt": "é🦊"}
    with db.session_scope() as session:
        assert get_snapshot(session, workspace[0], 1).bundle.skills == [s]


@pytest.mark.parametrize("text", [
    "name: foo", "---\nname: a\ndescription: b", "---\nname: a\nname: b\ndescription: c\n---\n",
    "---\nname: a\ndescription: b\nunknown: c\n---", "---\nname: a\ndescription: ''\n---",
    *[f"---\nname: a\ndescription: {v}\n---" for v in
      ("[a]", "{a: b}", "&anchor", "*alias", "|", ">", "!str a", "a #comment", '"unclosed', "'bad' tail", "true")],
    "---\n name: a\ndescription: b\n---", "---\nname: ../bad\ndescription: b\n---",
])
def test_frontmatter_rejections(text):
    with pytest.raises(DefinitionError):
        parse_frontmatter(text)


@pytest.mark.parametrize("char", ["x", "é", "🦊"])
def test_exact_byte_limits(char, workspace):
    body = char * (65_536 // len(char.encode()))
    s = skill(body=body)
    validate_bundle(Bundle(skills=[s]), set())
    assert parse_frontmatter(render_skill(s))[1] == body
    s.body += "x"
    with pytest.raises(DefinitionError, match="65536"):
        validate_bundle(Bundle(skills=[s]), set())
    bundle = fill(Bundle(knowledge=[KnowledgePage(name="notes", description="Notes")]), char)
    with db.session_scope() as session:
        assert save(session, workspace[0], bundle).revision == 1
    bundle.knowledge[0].body += "x"
    with db.session_scope() as session:
        with pytest.raises(DefinitionError, match="1048576"):
            save(session, workspace[0], bundle, 1)
        assert get_snapshot(session, workspace[0]).revision == 1


@pytest.mark.parametrize("target", ["knowledge-description", "instruction-description", "source-commit", "support"])
def test_every_metadata_field_consumes_quota(target):
    s = skill()
    b = Bundle(skills=[s], knowledge=[KnowledgePage(name="notes", description="Notes")],
               instructions=[RoleInstruction(name="build", description="Build", role="builder")])
    if target == "knowledge-description":
        b.knowledge[0].description = "é" * (BUNDLE_LIMIT // 2 + 1)
    elif target == "instruction-description":
        b.instructions[0].description = "x" * (BUNDLE_LIMIT + 1)
    elif target == "source-commit":
        b.skills[0].source.commit = "x" * (BUNDLE_LIMIT + 1)
    else:
        b.skills[0].files["support.txt"] = "x" * (BUNDLE_LIMIT + 1)
    with pytest.raises(DefinitionError, match="1048576"):
        validate_bundle(b, set())


def test_support_exact_limit():
    b = Bundle(skills=[skill(files={"support.txt": ""})])
    b.skills[0].files["support.txt"] = "x" * (BUNDLE_LIMIT - bundle_size(b))
    validate_bundle(b, set())
    b.skills[0].files["support.txt"] += "x"
    with pytest.raises(DefinitionError, match="1048576"):
        validate_bundle(b, set())


@pytest.mark.parametrize("path", ["../a.md", "/a.md", "a/../b.md", "a//b.md", "a\\b.md", "C:/a.md", "./a.md", "SKILL.md", "payload.exe"])
def test_unsafe_support_paths(path):
    with pytest.raises(DefinitionError):
        validate_bundle(Bundle(skills=[skill(files={path: "x"})]), set())


@pytest.mark.skipif(not hasattr(os, "O_PATH"), reason="search-only traversal needs O_PATH (Linux)")
def test_skill_folder_under_search_only_ancestor(tmp_path):
    # An isolated sandbox user may traverse a parent it cannot list (mode 0o100 here).
    parent = tmp_path / "parent"
    folder = parent / "inner" / "skill"
    folder.mkdir(parents=True)
    (folder / "SKILL.md").write_text(render_skill(skill()))
    parent.chmod(0o100)
    try:
        if os.geteuid() != 0:
            with pytest.raises(PermissionError):
                os.listdir(parent)
        assert read_skill_folder(folder).name == skill().name
    finally:
        parent.chmod(0o700)


def test_skill_folder_rejects_symlinked_ancestor(tmp_path):
    real = tmp_path / "real"
    (real / "skill").mkdir(parents=True)
    (real / "skill" / "SKILL.md").write_text(render_skill(skill()))
    (tmp_path / "link").symlink_to(real, target_is_directory=True)
    with pytest.raises(DefinitionError):
        read_skill_folder(tmp_path / "link" / "skill")


@pytest.mark.parametrize("kind", ["symlink", "directory-link", "fifo", "binary", "invalid-utf8"])
def test_reject_unsupported_files(tmp_path, kind):
    tmp_path = tmp_path / "skill"
    tmp_path.mkdir()
    (tmp_path / "SKILL.md").write_text(render_skill(skill()))
    path = tmp_path / "support.txt"
    if kind == "symlink":
        path.symlink_to(tmp_path / "SKILL.md")
    elif kind == "directory-link":
        path.symlink_to(tmp_path, target_is_directory=True)
    elif kind == "fifo":
        os.mkfifo(path)
    else:
        path.write_bytes(b"\x00" if kind == "binary" else b"\xff")
    with pytest.raises(DefinitionError):
        read_skill_folder(tmp_path)


def test_bounded_ingestion_stops_before_decode_or_next_file(tmp_path):
    tmp_path = tmp_path / "skill"
    tmp_path.mkdir()
    # Each file fits alone. Aggregate exceeds quota at b.txt, whose invalid UTF-8
    # must never be decoded; c.txt must never be opened.
    (tmp_path / "SKILL.md").write_text(render_skill(skill()))
    (tmp_path / "a.txt").write_bytes(b"x" * 600_000)
    (tmp_path / "b.txt").write_bytes(b"\xff" * 600_000)
    (tmp_path / "c.txt").write_text("never read")
    real_open = os.open
    opened = []

    def tracked(path, *args, **kwargs):
        opened.append(str(path))
        return real_open(path, *args, **kwargs)

    with patch("os.open", side_effect=tracked):
        with pytest.raises(DefinitionError, match="1048576"):
            read_skill_folder(tmp_path)
    assert "b.txt" in opened and "c.txt" not in opened


def test_scoped_duplicates_and_references(workspace):
    ws, other, (a, b, foreign) = workspace
    with db.session_scope() as session:
        with pytest.raises(DefinitionError, match="Duplicate"):
            save(session, ws, Bundle(skills=[skill(), skill()]))
        assert get_snapshot(session, ws).revision == 0
        for entry in [skill(repository_id=foreign), skill(source=Source(kind="repository", repository_id=foreign))]:
            with pytest.raises(DefinitionError, match="does not belong"):
                save(session, ws, Bundle(skills=[entry]))
        assert get_snapshot(session, ws).revision == 0
    with pytest.raises(ValidationError):
        skill(role="admin")


@pytest.mark.parametrize("scope", [None, "other", "role"])
def test_context_forged_scope_rejected(workspace, scope):
    ws, _, (a, b, _) = workspace
    page = KnowledgePage(name="notes", description="Notes", body="secret", repository_id=b if scope == "other" else None,
                         role="planner" if scope == "role" else None,
                         source=Source(kind="repository_context", repository_id=a))
    with db.session_scope() as session:
        with pytest.raises(DefinitionError, match="scoped only"):
            save(session, ws, Bundle(knowledge=[page]))
        assert not resolve_definitions(session, ws, "builder", task_repository_id=b).knowledge


@pytest.mark.parametrize("role", ["planner", "builder", "verifier"])
@pytest.mark.parametrize("multi", [False, True])
def test_scope_union_and_workspace_qualified_lookup(workspace, role, multi):
    ws, other, (a, b, foreign) = workspace
    entries = [skill(id="shared"), skill(id="repo-a", repository_id=a), skill(id="repo-b", repository_id=b)]
    for r in ("planner", "builder", "verifier"):
        entries += [skill(id=r, role=r), skill(id=r + "-a", role=r, repository_id=a),
                    skill(id=r + "-b", role=r, repository_id=b)]
    with db.session_scope() as session:
        save(session, ws, Bundle(skills=entries, instructions=[RoleInstruction(name="rules", description="Rules", role=role)],
                                knowledge=[KnowledgePage(name="arch", description="Architecture")]))
        save(session, other, Bundle(skills=[skill(id="shared")]))
        args = {"repository_ids": [a, b] if multi else [a]} if role == "planner" else {"task_repository_id": a}
        result = resolve_definitions(session, ws, role, **args)
        expected = {"shared", "repo-a", role, role + "-a"}
        if role == "planner" and multi:
            expected |= {"repo-b", role + "-b"}
        assert {e.id for e in result.skills.values()} == expected
        assert len(result.instructions) == len(result.knowledge) == 1
        assert result.lookup(f"{ws}/skill/shared").id == "shared"
        with pytest.raises(DefinitionError, match="Ambiguous"):
            result.lookup("testing")
        with pytest.raises(DefinitionError, match="not found"):
            result.lookup(f"{other}/skill/shared")
        with pytest.raises(DefinitionError, match="does not belong"):
            resolve_definitions(session, ws, "planner", repository_ids=[foreign])


def test_history_restore_and_live_notes(workspace):
    ws, _, (a, _, _) = workspace
    with db.session_scope() as session:
        session.get(Repository, a).config = {"context": "Original notes"}
        first = save(session, ws, Bundle(skills=[skill(body="first")]))
        assert first.created_at and first.author == "alice"
    with db.session_scope() as session:
        session.get(Repository, a).config = {"context": "Updated notes"}
        session.flush()
        assert get_snapshot(session, ws).bundle.knowledge[0].body == "Updated notes"
        assert get_snapshot(session, ws, 1).bundle.knowledge[0].body == "Original notes"
        save(session, ws, Bundle(skills=[skill(body="second")]), 1)
    with db.session_scope() as session:
        restored = restore_revision(session, ws, 1, expected_revision=2, author="bob")
        assert restored.revision == 3 and restored.author == "bob"
        assert restored.bundle.skills[0].body == "first"
        assert restored.bundle.knowledge[0].body == "Original notes"
        assert [s.revision for s in history(session, ws)] == [1, 2, 3]
        assert get_snapshot(session, ws, 2).bundle.skills[0].body == "second"
        with pytest.raises(DefinitionError, match="Stale"):
            restore_revision(session, ws, 1, expected_revision=2, author="bob")
        assert get_snapshot(session, ws).revision == 3
    with db.session_scope() as session:
        row = session.scalar(select(DefinitionRevision).where(DefinitionRevision.workspace_id == ws))
        row.author = "mallory"
        with pytest.raises(ValueError, match="immutable"):
            session.flush()
        session.rollback()


def test_two_session_stale_save(workspace):
    ws = workspace[0]
    # Both sessions read revision zero; only one can advance that head.
    with Session(db._engine) as one, Session(db._engine) as two:
        assert get_snapshot(one, ws).revision == get_snapshot(two, ws).revision == 0
        one.rollback()
        two.rollback()
        save(one, ws, Bundle(skills=[skill(body="winner")]))
        one.commit()
        with pytest.raises(RevisionConflict):
            save(two, ws, Bundle(skills=[skill(body="loser")]))
        two.rollback()
        assert get_snapshot(two, ws).bundle.skills[0].body == "winner"


def test_disconnect_guard_and_orphan_read(workspace):
    ws, _, (a, b, _) = workspace
    with db.session_scope() as session:
        save(session, ws, Bundle(skills=[skill(id="shared"), skill(id="scoped", repository_id=a)]))
        with pytest.raises(DefinitionError, match="testing"):
            guard_disconnect(session, ws, a)
        # Simulate an older/external disconnect bypassing the guard.
        session.delete(session.get(Repository, a))
    with db.session_scope() as session:
        snapshot = get_snapshot(session, ws)
        assert [s.id for s in snapshot.bundle.skills] == ["shared"]
        assert "scoped" in snapshot.warnings[0]
        assert resolve_definitions(session, ws, "builder", task_repository_id=b).lookup("testing").id == "shared"
        assert len(get_snapshot(session, ws, 1).bundle.skills) == 2


def test_both_save_and_disconnect_take_repository_row_locks(workspace):
    ws, _, (a, _, _) = workspace
    from sqlalchemy import event
    statements = []

    def capture(state):
        stmt = state.statement
        if state.is_select and "repositories" in str(stmt):
            statements.append(stmt._for_update_arg is not None)

    with db.session_scope() as session:
        event.listen(session, "do_orm_execute", capture)
        save(session, ws, Bundle())
        assert any(statements)
        statements.clear()
        guard_disconnect(session, ws, a)
        assert any(statements)


def test_disconnect_then_read_unrelated_http(client, signed_in):
    ws = signed_in["workspaces"][0]["id"]
    with db.session_scope() as session:
        repo = Repository(workspace_id=ws, full_name="org/repo", config={"context": "notes"})
        session.add(repo)
        session.flush()
        rid = repo.id
        save(session, ws, Bundle(skills=[skill()]))
    assert client.delete(f"/api/workspaces/{ws}/repositories/{rid}").status_code == 200
    with db.session_scope() as session:
        assert resolve_definitions(session, ws, "planner").lookup("testing").name == "testing"
        assert not get_snapshot(session, ws).bundle.knowledge


def test_connect_and_note_update_enforce_projected_quota(client, signed_in, monkeypatch):
    ws = signed_in["workspaces"][0]["id"]
    from aifactory import api

    async def detect(*args):
        return {"full_name": "org/new", "default_branch": "main"}, {"context": "Detected AGENTS.md notes"}

    monkeypatch.setattr(api, "_detect_repository", detect)
    with db.session_scope() as session:
        repo = Repository(workspace_id=ws, full_name="org/existing", config={})
        session.add(repo)
        session.flush()
        rid = repo.id
        save(session, ws, fill(Bundle(knowledge=[KnowledgePage(name="quota", description="Quota")])))
    response = client.post(f"/api/workspaces/{ws}/repositories", json={"full_name": "org/new"})
    assert response.status_code == 422, response.text
    assert "1048576" in response.text
    response = client.patch(f"/api/workspaces/{ws}/repositories/{rid}", json={"config": {"context": "New notes"}})
    assert response.status_code == 422, response.text
    with db.session_scope() as session:
        assert not session.scalar(select(Repository).where(Repository.full_name == "org/new"))
        assert not session.get(Repository, rid).config.get("context")
        snapshot = get_snapshot(session, ws)
        assert snapshot.revision == 1 and bundle_size(snapshot.bundle) == BUNDLE_LIMIT


def test_connect_with_notes_below_quota(client, signed_in, monkeypatch):
    ws = signed_in["workspaces"][0]["id"]
    from aifactory import api

    async def detect(*args):
        return {"full_name": "org/new"}, {"context": "Detected notes"}

    monkeypatch.setattr(api, "_detect_repository", detect)
    response = client.post(f"/api/workspaces/{ws}/repositories", json={"full_name": "org/new"})
    assert response.status_code == 200, response.text
    with db.session_scope() as session:
        notes = get_snapshot(session, ws).bundle.knowledge
        assert len(notes) == 1 and notes[0].body == "Detected notes"
        assert notes[0].repository_id == response.json()["id"]


@pytest.mark.parametrize("kind", ["knowledge", "instruction", "source"])
def test_oversized_metadata_cannot_be_persisted(workspace, kind):
    ws = workspace[0]
    bundle = Bundle()
    huge = "🦊" * (BUNDLE_LIMIT // 4 + 1)
    if kind == "knowledge":
        bundle.knowledge.append(KnowledgePage(name="notes", description=huge, body="small"))
    elif kind == "instruction":
        bundle.instructions.append(RoleInstruction(name="rules", description=huge, role="builder"))
    else:
        bundle.skills.append(skill(source=Source(commit=huge)))
    with db.session_scope() as session:
        with pytest.raises(DefinitionError, match="1048576"):
            save(session, ws, bundle)
        assert get_snapshot(session, ws).revision == 0
        assert history(session, ws) == []


def test_existing_sqlite_schema_initialization_is_additive(workspace):
    from sqlalchemy import inspect
    from aifactory.models import DefinitionState

    if db._engine.dialect.name != "sqlite":
        pytest.skip("SQLite additive schema setup contract")
    # Emulate a pre-definitions database; no other table needs alteration.
    DefinitionRevision.__table__.drop(db._engine)
    DefinitionState.__table__.drop(db._engine)
    engine = db.init_engine()
    assert {"definition_states", "definition_revisions"} <= set(inspect(engine).get_table_names())
    with db.session_scope() as session:
        assert save(session, workspace[0], Bundle()).revision == 1


def test_referenced_disconnect_http_is_actionable(client, signed_in):
    ws = signed_in["workspaces"][0]["id"]
    with db.session_scope() as session:
        repo = Repository(workspace_id=ws, full_name="org/referenced", config={})
        session.add(repo)
        session.flush()
        rid = repo.id
        save(session, ws, Bundle(skills=[skill(repository_id=rid)]))
    response = client.delete(f"/api/workspaces/{ws}/repositories/{rid}")
    assert response.status_code == 409
    assert "testing" in response.text
    with db.session_scope() as session:
        assert session.get(Repository, rid) is not None
        assert get_snapshot(session, ws).revision == 1


def test_history_does_not_consume_active_quota(workspace):
    bundle = fill(Bundle(knowledge=[KnowledgePage(name="large", description="Large page")]))
    with db.session_scope() as session:
        save(session, workspace[0], bundle)
    with db.session_scope() as session:
        assert save(session, workspace[0], bundle, 1).revision == 2
        assert restore_revision(session, workspace[0], 1, expected_revision=2, author="bob").revision == 3
        assert len(history(session, workspace[0])) == 3


def test_folder_entry_limit_stops_lazy_enumeration(tmp_path):
    from aifactory.definitions.validation import MAX_FOLDER_ENTRIES

    for i in range(MAX_FOLDER_ENTRIES + 10):
        (tmp_path / f"{i}.txt").touch()
    real_scandir = os.scandir
    visited = []

    class TrackedEntries:
        def __enter__(self):
            self.entries = real_scandir(tmp_path)
            return self

        def __iter__(self):
            for entry in self.entries:
                visited.append(entry.name)
                yield entry

        def __exit__(self, *args):
            self.entries.close()

    with patch("os.scandir", side_effect=lambda fd: TrackedEntries()), patch("os.fdopen") as read:
        with pytest.raises(DefinitionError, match="2000 directory entries"):
            read_skill_folder(tmp_path)
    assert len(visited) == MAX_FOLDER_ENTRIES + 1
    read.assert_not_called()


def test_folder_paths_consume_ingestion_budget(tmp_path, monkeypatch):
    from aifactory.definitions import validation

    # Each path is below the budget; their sum is not. Empty files still cost
    # bytes and enumeration must stop before opening/retaining any contents.
    monkeypatch.setattr(validation, "BUNDLE_LIMIT", 100)
    for i in range(10):
        (tmp_path / (str(i) + "x" * 45 + ".txt")).touch()
    with patch("os.fdopen") as read:
        with pytest.raises(DefinitionError, match="100 UTF-8 bytes"):
            read_skill_folder(tmp_path)
    read.assert_not_called()


def test_connection_locks_before_adding_projected_notes(client, signed_in, monkeypatch):
    from aifactory import api, definitions

    ws = signed_in["workspaces"][0]["id"]
    calls = []

    async def detect(*args):
        return {"full_name": "org/locked"}, {"context": "Detected notes"}

    monkeypatch.setattr(api, "_detect_repository", detect)
    for name in ("lock_workspace", "lock_repositories", "validate_repository_change"):
        original = getattr(definitions, name)

        def tracked(*args, _name=name, _original=original, **kwargs):
            calls.append(_name)
            return _original(*args, **kwargs)

        monkeypatch.setattr(definitions, name, tracked)
    response = client.post(f"/api/workspaces/{ws}/repositories", json={"full_name": "org/locked"})
    assert response.status_code == 200, response.text
    assert calls == ["lock_workspace", "lock_repositories", "validate_repository_change"]


@pytest.mark.parametrize("notes_file", ["AGENTS.md", "CLAUDE.md"])
@pytest.mark.parametrize("char", ["x", "é", "🦊"])
@pytest.mark.parametrize("excess", [0, 1])
def test_detected_connection_notes_exact_projected_limit(client, signed_in, monkeypatch, notes_file, char, excess):
    import base64
    from aifactory import api

    ws = signed_in["workspaces"][0]["id"]
    context = char * 10

    class GitHub:
        def __init__(self, token):
            pass

        async def repo(self, name):
            return {"full_name": name, "permissions": {"push": True}}

        async def request(self, method, path, **kwargs):
            if path.endswith("/" + notes_file):
                return {"encoding": "base64", "content": base64.b64encode(context.encode()).decode()}
            return {}

    monkeypatch.setattr(api, "GitHub", GitHub)
    monkeypatch.setattr(api, "resolve_token", lambda ws: "fake")
    # Repository IDs have a fixed UTF-8 width; include the projected page's
    # metadata as well as the detected body when reserving exactly enough space.
    rid = "0" * 32
    projected = KnowledgePage(id=f"repository-context-{rid}", name="team-notes",
                              description="Team notes for org/detected", body=context, repository_id=rid,
                              source=Source(kind="repository_context", repository_id=rid, path="config/context"))
    cost = bundle_size(Bundle(knowledge=[projected]))
    bundle = Bundle(knowledge=[KnowledgePage(name="quota", description="Quota")])
    available = BUNDLE_LIMIT - cost + excess - bundle_size(bundle)
    width = len(char.encode())
    bundle.knowledge[0].body = char * (available // width) + "x" * (available % width)
    with db.session_scope() as session:
        save(session, ws, bundle)
    response = client.post(f"/api/workspaces/{ws}/repositories", json={"full_name": "org/detected"})
    assert response.status_code == (422 if excess else 200), response.text
    with db.session_scope() as session:
        snapshot = get_snapshot(session, ws)
        assert snapshot.revision == (1 if excess else 2)
        assert len(history(session, ws)) == (1 if excess else 2)
        if excess:
            assert "1048576" in response.text
            assert not session.scalar(select(Repository).where(Repository.workspace_id == ws))
            assert snapshot.bundle == bundle
        else:
            assert bundle_size(snapshot.bundle) == BUNDLE_LIMIT
            assert snapshot.bundle.knowledge[-1].body == context


@pytest.mark.parametrize("kind", ["team_note", "repo_config"])
def test_insight_note_mutation_validates_under_locks(client, signed_in, monkeypatch, kind):
    from aifactory import definitions, insights
    from aifactory.models import Event, Signal, User

    ws = signed_in["workspaces"][0]["id"]
    with db.session_scope() as session:
        repo = Repository(workspace_id=ws, full_name="org/notes", config={"context": "Original notes"})
        session.add(repo)
        session.flush()
        rid = repo.id
        # Leave 1 KB of headroom: valid before applying, too small for an 8 KB note.
        bundle = get_snapshot(session, ws).bundle
        bundle.knowledge.insert(0, KnowledgePage(name="quota", description="Quota"))
        fill(bundle)
        bundle.knowledge[0].body = bundle.knowledge[0].body[:-1024]
        save(session, ws, bundle)
        action = {"type": kind, "repo_id": rid}
        action.update({"note": "x" * 8000} if kind == "team_note" else {"set": {"context": "x" * 8000}})
        signal = Signal(workspace_id=ws, repo_id=rid, source="improvement", fingerprint="review-quota",
                        title="Review themes", data={"rule": "review_themes", "action": action})
        session.add(signal)
        session.flush()
        sid = signal.id
    calls = []
    for name in ("lock_workspace", "lock_repositories", "validate_repository_change"):
        original = getattr(definitions, name)

        def tracked(*args, _name=name, _original=original, **kwargs):
            calls.append(_name)
            return _original(*args, **kwargs)

        monkeypatch.setattr(definitions, name, tracked)
    response = client.post(f"/api/workspaces/{ws}/signals/{sid}", json={"action": "accept"})
    assert response.status_code == 400, response.text
    assert "1048576" in response.text
    assert calls == ["lock_workspace", "lock_repositories", "validate_repository_change"]
    with db.session_scope() as session:
        assert session.get(Repository, rid).config["context"] == "Original notes"
        assert session.get(Signal, sid).status == "new"
        assert get_snapshot(session, ws).revision == 1
        assert bundle_size(get_snapshot(session, ws).bundle) == BUNDLE_LIMIT - 1024
        assert not session.scalar(select(Event).where(Event.kind == "improvement_applied"))
        # The savepoint also protects direct callers that catch the error and
        # commit their outer transaction instead of rolling the session back.
        sig = session.get(Signal, sid)
        user = session.scalar(select(User))
        with pytest.raises(DefinitionError, match="1048576"):
            insights.apply(session, sig, user)
        assert session.get(Repository, rid).config["context"] == "Original notes"
        assert sig.status == "new"
        session.commit()
    with db.session_scope() as session:
        assert session.get(Repository, rid).config["context"] == "Original notes"
        assert session.get(Signal, sid).status == "new"


@pytest.mark.parametrize("path", ["existing", "connect-detected", "connect-override"])
@pytest.mark.parametrize("oversized", [False, True])
def test_bootstrap_context_writes_validate_under_locks(workspace, monkeypatch, caplog, path, oversized):
    import asyncio
    import json
    from aifactory import bootstrap, definitions, github, repo_detect

    ws, _, (a, _, _) = workspace
    with db.session_scope() as session:
        session.get(Repository, a).config = {"context": "Original notes"}
        bundle = get_snapshot(session, ws).bundle
        bundle.knowledge.insert(0, KnowledgePage(name="quota", description="Quota"))
        fill(bundle)
        bundle.knowledge[0].body = bundle.knowledge[0].body[:-1024]
        save(session, ws, bundle)
    context = "x" * 8000 if oversized else "Short notes"
    calls = []
    for name in ("lock_workspace", "lock_repositories", "validate_repository_change"):
        original = getattr(definitions, name)

        def tracked(*args, _name=name, _original=original, **kwargs):
            calls.append(_name)
            return _original(*args, **kwargs)

        monkeypatch.setattr(definitions, name, tracked)

    if path == "existing":
        monkeypatch.setenv("AIFACTORY_BOOTSTRAP", json.dumps({
            "organization": "Definitions", "workspace": "one",
            "settings": {"bootstrap_test": True},
            "repositories": [{"full_name": "org/a", "config": {"context": context}}],
        }))

        async def run():
            return bootstrap.apply_bootstrap()

        assert asyncio.run(run()) == ws
    else:
        class GitHub:
            def __init__(self, token):
                pass

            async def repo(self, name):
                return {"full_name": name, "default_branch": "main"}

        async def detect(*args):
            return {"context": context if path == "connect-detected" else "Detected notes"}

        monkeypatch.setattr(github, "GitHub", GitHub)
        monkeypatch.setattr(github, "resolve_credential", lambda ws: "fake")
        monkeypatch.setattr(repo_detect, "detect", detect)
        overrides = {"context": context} if path == "connect-override" else {}
        asyncio.run(bootstrap._connect(ws, [("org/new", overrides)]))

    assert calls == ["lock_workspace", "lock_repositories", "validate_repository_change"]
    with db.session_scope() as session:
        snapshot = get_snapshot(session, ws)
        assert snapshot.revision == (1 if oversized else 2)
        assert len(history(session, ws)) == (1 if oversized else 2)
        assert get_snapshot(session, ws, 1).bundle.knowledge[-1].body == "Original notes"
        if path == "existing":
            repo = session.get(Repository, a)
            assert repo.config["context"] == ("Original notes" if oversized else context)
            assert ("context" in repo.config.get("_bootstrap", {})) == (not oversized)
            # Rejected config must not roll back startup's other work.
            assert session.get(Workspace, ws).settings["bootstrap_test"] is True
        else:
            repo = session.scalar(select(Repository).where(Repository.full_name == "org/new"))
            if oversized:
                assert repo is None
            else:
                assert repo.config["context"] == context
        if oversized:
            assert "1048576" in caplog.text
            assert bundle_size(snapshot.bundle) == BUNDLE_LIMIT - 1024
        else:
            assert snapshot.author == "bootstrap"
            assert any(p.body == context for p in get_snapshot(session, ws, 2).bundle.knowledge)
            assert any(p.body == context for p in snapshot.bundle.knowledge)


def test_restore_drops_disconnected_context_projections(workspace):
    ws, _, (a, b, _) = workspace
    with db.session_scope() as session:
        session.get(Repository, a).config = {"context": "Disconnected notes"}
        session.get(Repository, b).config = {"context": "Original connected notes"}
        first = save(session, ws, Bundle(skills=[skill(body="Original instructions")]))
        original = first.bundle.model_dump()
    with db.session_scope() as session:
        guard_disconnect(session, ws, a)
        session.delete(session.get(Repository, a))
        session.get(Repository, b).config = {"context": "Updated notes", "other": True}
        session.flush()
        save(session, ws, Bundle(skills=[skill(body="Updated instructions")]), 1)
    with db.session_scope() as session:
        restored = restore_revision(session, ws, 1, expected_revision=2, author="bob")
        assert restored.revision == 3 and restored.author == "bob"
        assert restored.bundle.skills[0].body == "Original instructions"
        assert [(p.repository_id, p.body) for p in restored.bundle.knowledge] == [(b, "Original connected notes")]
        assert session.get(Repository, b).config == {"context": "Original connected notes", "other": True}
        assert get_snapshot(session, ws, 1).bundle.model_dump() == original
        row = session.scalar(select(DefinitionRevision).where(
            DefinitionRevision.workspace_id == ws, DefinitionRevision.revision == 3))
        assert row.restored_from == 1
        assert resolve_definitions(session, ws, "builder", task_repository_id=b).lookup("testing").body == "Original instructions"


def test_restore_still_rejects_disconnected_explicit_definitions(workspace):
    ws, _, (a, _, _) = workspace
    with db.session_scope() as session:
        session.get(Repository, a).config = {"context": "Old notes"}
        save(session, ws, Bundle(skills=[skill(repository_id=a)]))
        # An external/legacy disconnect must not make an explicit definition restorable.
        session.delete(session.get(Repository, a))
    with db.session_scope() as session:
        with pytest.raises(DefinitionError, match="does not belong"):
            restore_revision(session, ws, 1, expected_revision=1, author="bob")
        assert get_snapshot(session, ws).revision == 1
        assert len(history(session, ws)) == 1
