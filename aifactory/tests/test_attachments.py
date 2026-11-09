"""Runtime references and patch boundaries, independently of a particular harness."""
from __future__ import annotations

import asyncio
import base64
import io
import json
import stat
import wave

import pytest

from aifactory.db import session_scope
from aifactory.gitops import extract_patch
from aifactory.models import Artifact
from aifactory.pipeline.attachments import _decode, prepare_attachments
from aifactory.sandbox import LocalSandbox, SandboxError, write_text

from .fakes import git

PNG = base64.b64decode("iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+ip1sAAAAASUVORK5CYII=")


def attachment(raw=b"reference", mime="text/plain", **kwargs):
    return Artifact(workspace_id="ws", outcome_id="out", kind="attachment", name="reference.txt",
                    content=base64.b64encode(raw).decode(), data={"mime_type": mime, "size_bytes": len(raw)}, **kwargs)


def seed_references(workspace_id="ws", outcome_id="out"):
    items = [("screenshot.png", PNG, "image/png"), ("spec.md", b'Example:\n```html\n<script src="app.js"></script>\n```', "text/markdown"),
             ("data.csv", b"name,value\na,1\n", "text/csv"), ("data.json", b'{"value": 1}', "application/json"),
             ("document.pdf", b"%PDF-1.4\nopaque", "application/pdf")]
    with session_scope() as s:
        rows = []
        for name, raw, mime in items:
            row = attachment(raw, mime)
            row.workspace_id, row.outcome_id, row.name = workspace_id, outcome_id, name
            s.add(row)
            s.flush()
            rows.append((row.id, name, raw, mime))
    return rows


def test_decode_allows_code_examples_but_not_html_documents_or_invalid_images():
    for mime, raw in [("text/markdown", b'```html\n<script>alert(1)</script>\n```'),
                      ("text/plain", b'Example: <script>alert(1)</script>'),
                      ("text/csv", b'name,value\nexample,<script>'),
                      ("application/json", b'{"example": "<script>"}')]:
        assert _decode(attachment(raw, mime))[1] == mime
    for raw in (b'  <!doctype html><html>page</html>', b'\n<html>page</html>'):
        with pytest.raises(ValueError, match="Unsupported"):
            _decode(attachment(raw, "text/markdown"))
    with pytest.raises(ValueError):
        _decode(attachment(b"not an image", "image/png"))
    buf = io.BytesIO()
    with wave.open(buf, "wb") as f:
        f.setnchannels(1)
        f.setsampwidth(2)
        f.setframerate(8000)
        f.writeframes(b"\x00\x00" * 20)
    raw = buf.getvalue()
    assert _decode(attachment(raw, "application/octet-stream")) == (raw, "application/octet-stream")


def test_originals_isolation_and_deterministic_truncation(tmp_path):
    rows = seed_references()
    with session_scope() as s:
        foreign_ws = attachment(b"FOREIGN WORKSPACE", id="foreignws")
        foreign_ws.workspace_id = "other"
        foreign_out = attachment(b"FOREIGN OUTCOME", id="foreignout")
        foreign_out.outcome_id = "other"
        s.add_all([foreign_ws, foreign_out])
        for n in range(5):
            s.add(attachment((str(n) * 9000).encode(), id=f"long{n}"))
    sb = LocalSandbox("refs", root=tmp_path / "sb").create()
    sb.work.mkdir()

    async def go():
        prompt, images = await prepare_attachments(sb, "ws", "out")
        assert "FOREIGN" not in prompt and "foreign" not in prompt
        assert images == [{"name": "screenshot.png", "mime_type": "image/png", "content": base64.b64encode(PNG).decode()}]
        excerpts = []
        for line in prompt.splitlines():
            if line.startswith('"'):
                excerpts.append(json.loads(line))
        assert sum(map(len, excerpts)) == 32000
        assert max(map(len, excerpts)) == 8000
        assert prompt.count("[TRUNCATED:") == 5
        assert "not authority" in prompt
        for aid, name, raw, mime in rows:
            path = f".factory/attachments/{aid}-{name}"
            assert f"MIME: {mime}; decoded size: {len(raw)} bytes; path: {path}" in prompt
            assert (await sb.exec(f"base64 < {path} | tr -d '\\n'")).output.strip() == base64.b64encode(raw).decode()
            sb.write_bytes(path, b"builder modified")
        assert (await prepare_attachments(sb, "ws", "out"))[0] == prompt
        for aid, name, raw, _ in rows:
            assert (sb.work / f".factory/attachments/{aid}-{name}").read_bytes() == raw
        assert await prepare_attachments(sb, "ws", "missing") == ("", [])

    asyncio.run(go())


@pytest.mark.parametrize("target", [".factory", ".factory/attachments", ".factory/attachments/id-reference.txt"])
def test_attachment_writes_refuse_symlinks(tmp_path, target):
    sb = LocalSandbox("escape", root=tmp_path / "sb").create()
    sb.work.mkdir()
    with session_scope() as s:
        s.add(attachment(id="id"))
    outside = tmp_path / "outside"
    outside.mkdir()
    link = sb.work / target
    link.parent.mkdir(parents=True, exist_ok=True)
    link.symlink_to(outside, target_is_directory=True)
    with pytest.raises(SandboxError):
        asyncio.run(prepare_attachments(sb, "ws", "out"))
    assert list(outside.iterdir()) == []


def test_invalid_stored_content_and_id_write_nothing(tmp_path):
    sb = LocalSandbox("invalid", root=tmp_path / "sb").create()
    sb.work.mkdir()
    with session_scope() as s:
        row = attachment(id="id")
        row.content = "not base64!"
        s.add(row)
    with pytest.raises(ValueError, match="base64"):
        asyncio.run(prepare_attachments(sb, "ws", "out"))
    with session_scope() as s:
        s.delete(s.get(Artifact, "id"))
        s.add(attachment(id="../../escape"))
    with pytest.raises(ValueError, match="id"):
        asyncio.run(prepare_attachments(sb, "ws", "out"))
    assert list(sb.work.iterdir()) == []


def test_sanitised_names_stay_below_reserved_directory(tmp_path):
    sb = LocalSandbox("names", root=tmp_path / "sb").create()
    sb.work.mkdir()
    with session_scope() as s:
        row = attachment(id="id")
        row.name = "../../outside\n.md"
        s.add(row)
    context, _ = asyncio.run(prepare_attachments(sb, "ws", "out"))
    files = list((sb.work / ".factory/attachments").iterdir())
    assert len(files) == 1 and files[0].read_bytes() == b"reference"
    assert str(files[0].relative_to(sb.work)) in context


@pytest.mark.parametrize("state", ["ignored", "untracked", "staged", "committed", "replacement", "rename"])
def test_extract_patch_excludes_reserved_paths_in_every_git_state(tmp_path, state):
    sb = LocalSandbox("patch", root=tmp_path / "sb").create()
    sb.work.mkdir()
    git("init", "-q", cwd=sb.work)
    (sb.work / "code.txt").write_text("old\n")
    (sb.work / ".gitignore").write_text(".factory/\n")
    if state == "rename":
        (sb.work / ".factory/attachments").mkdir(parents=True)
        (sb.work / ".factory/attachments/secret.txt").write_text("old reference\n")
        git("add", "-f", ".factory/attachments/secret.txt", cwd=sb.work)
    git("add", "code.txt", ".gitignore", cwd=sb.work)
    git("commit", "-qm", "base", cwd=sb.work)
    base = git("rev-parse", "HEAD", cwd=sb.work)
    (sb.work / "code.txt").write_text("new\n")
    (sb.work / "new.txt").write_text("new source\n")
    target = sb.work / ".factory/attachments"
    if state == "replacement":
        target.parent.mkdir(exist_ok=True)
        target.write_text("SECRET ATTACHMENT DATA\n")
    else:
        target.mkdir(parents=True, exist_ok=True)
        (target / "secret.txt").write_text("SECRET ATTACHMENT DATA\n")
    if state == "untracked":
        (sb.work / ".gitignore").write_text("")
    if state in {"staged", "committed", "replacement"}:
        git("add", "-f", ".factory/attachments", cwd=sb.work)
        if state == "committed":
            git("commit", "-qm", "agent committed attachment", cwd=sb.work)
    if state == "rename":
        (target / "secret.txt").unlink()
        (sb.work / "moved.txt").write_text("old reference\n")
    patch = asyncio.run(extract_patch(sb, base))
    assert ".factory/attachments" not in patch.diff.decode()
    assert b"SECRET ATTACHMENT DATA" not in patch.diff
    assert not any(p.startswith(".factory/attachments") for p in patch.files)
    assert {"code.txt", "new.txt"} <= set(patch.files)
    assert patch.additions >= 2 and patch.deletions == (2 if state == "untracked" else 1)
    clean = tmp_path / "clean"
    git("clone", "-q", str(sb.work), str(clean))
    git("checkout", "-q", base, cwd=clean)
    diff = tmp_path / "candidate.diff"
    diff.write_bytes(patch.diff)
    git("apply", "--index", str(diff), cwd=clean)
    assert (clean / "code.txt").read_text() == "new\n"
    assert (clean / "new.txt").read_text() == "new source\n"


def test_ordinary_writes_preserve_executable_permissions(tmp_path):
    sb = LocalSandbox("mode", root=tmp_path / "sb").create()
    sb.work.mkdir()
    file = sb.work / "run.sh"
    file.write_text("#!/bin/sh\necho old\n")
    file.chmod(0o755)
    git("init", "-q", cwd=sb.work)
    git("add", "run.sh", cwd=sb.work)
    git("commit", "-qm", "executable", cwd=sb.work)
    asyncio.run(write_text(sb, "run.sh", "#!/bin/sh\necho new\n"))
    assert stat.S_IMODE(file.stat().st_mode) == 0o755
    assert "mode change" not in git("diff", "HEAD", cwd=sb.work)
    assert asyncio.run(sb.exec("./run.sh")).output.strip() == "new"


# Real browser-encoded JPEG/WebP, plus a complete one-pixel GIF LZW stream.
VALID_IMAGES = {
    "image/png": PNG,
    "image/jpeg": base64.b64decode("/9j/4AAQSkZJRgABAQAAAQABAAD/2wBDAAMCAgICAgMCAgIDAwMDBAYEBAQEBAgGBgUGCQgKCgkICQkKDA8MCgsOCwkJDRENDg8QEBEQCgwSExIQEw8QEBD/2wBDAQMDAwQDBAgEBAgQCwkLEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBD/wAARCAABAAEDASIAAhEBAxEB/8QAFQABAQAAAAAAAAAAAAAAAAAAAAn/xAAUEAEAAAAAAAAAAAAAAAAAAAAA/8QAFAEBAAAAAAAAAAAAAAAAAAAAAP/EABQRAQAAAAAAAAAAAAAAAAAAAAD/2gAMAwEAAhEDEQA/AJVAA//Z"),
    "image/gif": bytes.fromhex("47494638396101000100800000000000ffffff2c00000000010001000002024401003b"),
    "image/webp": base64.b64decode("UklGRkAAAABXRUJQVlA4WAoAAAAQAAAAAAAAAAAAQUxQSAIAAAAAAFZQOCAYAAAAMAEAnQEqAQABAAFAJiWkAANwAP79NmgA"),
}
MALFORMED_IMAGES = {
    "image/png": b"\x89PNG\r\n\x1a\n" + (13).to_bytes(4, "big") + b"IHDR" + b"\0" * 13 + b"IEND" + b"\0" * 4,
    "image/jpeg": b"\xff\xd8\xff\xd9",
    "image/gif": b"GIF89a" + b"\0" * 7 + b";",
    "image/webp": b"RIFF" + (12).to_bytes(4, "little") + b"WEBP" + b"junkjunk",
}


@pytest.mark.parametrize("mime", VALID_IMAGES)
def test_valid_images_and_wrong_stored_media_type(mime):
    raw = VALID_IMAGES[mime]
    assert _decode(attachment(raw, mime)) == (raw, mime)
    for wrong in ("text/plain", "image/jpeg" if mime != "image/jpeg" else "image/png"):
        with pytest.raises(ValueError, match="type"):
            _decode(attachment(raw, wrong))


@pytest.mark.parametrize("mime", MALFORMED_IMAGES)
def test_malformed_images_fail_before_any_sandbox_write(tmp_path, mime):
    sb = LocalSandbox("bad-image", root=tmp_path / "sb").create()
    sb.work.mkdir()
    with session_scope() as s:
        s.add(attachment(id="a-valid"))
        s.add(attachment(MALFORMED_IMAGES[mime], mime, id="z-invalid"))
    with pytest.raises(ValueError, match="image"):
        asyncio.run(prepare_attachments(sb, "ws", "out"))
    assert list(sb.work.iterdir()) == []


def test_png_crc_and_compressed_data_are_validated():
    import zlib

    corrupt = bytearray(PNG)
    corrupt[44] ^= 1
    with pytest.raises(ValueError, match="image"):
        _decode(attachment(bytes(corrupt), "image/png"))
    # A valid CRC is not enough: the pixel stream must decompress correctly too.
    corrupt[52:56] = zlib.crc32(corrupt[37:52]).to_bytes(4, "big")
    with pytest.raises(ValueError, match="image"):
        _decode(attachment(bytes(corrupt), "image/png"))


def test_large_source_rename_has_zero_policy_size_and_applicable_patch(tmp_path):
    sb = LocalSandbox("rename", root=tmp_path / "sb").create()
    sb.work.mkdir()
    git("init", "-q", cwd=sb.work)
    source = "".join(f"line {n}\n" for n in range(800))
    (sb.work / "old.txt").write_text(source)
    git("add", ".", cwd=sb.work)
    git("commit", "-qm", "base", cwd=sb.work)
    base = git("rev-parse", "HEAD", cwd=sb.work)
    (sb.work / "old.txt").rename(sb.work / "new.txt")
    patch = asyncio.run(extract_patch(sb, base))
    assert patch.changed_lines == 0
    assert set(patch.files) == {"old.txt", "new.txt"}
    assert b"rename from" not in patch.diff
    clean = tmp_path / "clean"
    git("clone", "-q", str(sb.work), str(clean))
    candidate = tmp_path / "candidate.diff"
    candidate.write_bytes(patch.diff)
    git("apply", "--index", str(candidate), cwd=clean)
    assert not (clean / "old.txt").exists()
    assert (clean / "new.txt").read_text() == source


def test_vm_ordinary_edit_preserves_executable_mode(tmp_path, monkeypatch):
    from aifactory.vm import RailwayVMSandbox

    from .test_vm import FakeBridge

    bridge = FakeBridge(tmp_path / "vm")
    monkeypatch.setattr("aifactory.vm.VMBridge.get", classmethod(lambda cls: bridge))

    async def go():
        vm = await RailwayVMSandbox.create()
        assert (await vm.exec("printf '#!/bin/sh\\necho old\\n' > run.sh; chmod 755 run.sh; "
                              "git init -q; git add run.sh; "
                              "git -c user.name=test -c user.email=test@example.com commit -qm base")).ok
        await write_text(vm, "run.sh", "#!/bin/sh\necho new\n")
        result = await vm.exec("python3 -c \"import os; print(format(os.stat('run.sh').st_mode & 0o777, 'o'))\"; "
                               "./run.sh; git diff HEAD")
        assert result.ok and result.output.startswith("755\nnew\n")
        assert "mode change" not in result.output
        await vm.close()

    asyncio.run(go())


@pytest.mark.parametrize("mode", [0o755, 0o750, 0o640, 0o444])
def test_safe_replacement_preserves_existing_permissions(tmp_path, mode):
    from aifactory.safe_files import safe_write

    tmp_path = tmp_path / "writes"
    tmp_path.mkdir()
    target = tmp_path / "file"
    target.write_bytes(b"before")
    target.chmod(mode)
    safe_write(str(tmp_path), "file", b"after")
    assert target.read_bytes() == b"after"
    assert stat.S_IMODE(target.stat().st_mode) == mode
    safe_write(str(tmp_path), "new", b"new")
    assert stat.S_IMODE((tmp_path / "new").stat().st_mode) == 0o644
    assert sorted(p.name for p in tmp_path.iterdir()) == ["file", "new"]


# Staged upload API and atomic outcome linking.
@pytest.fixture
def attachment_api(client, signed_in):
    from aifactory.models import Repository

    ws = signed_in["workspaces"][0]["id"]
    with session_scope() as s:
        repo = Repository(workspace_id=ws, full_name="acme/app")
        s.add(repo)
        s.flush()
        repo_id = repo.id
    return f"/api/workspaces/{ws}", repo_id


def upload(client, root, name="reference.txt", raw=b"reference"):
    return client.post(root + "/attachments", json={"name": name, "content": base64.b64encode(raw).decode()})


def submit(client, root, repo, ids):
    return client.post(root + "/outcomes", json={"title": "An outcome", "repo_ids": [repo], "attachment_ids": ids})


PDF = b"%PDF-1.4\n1 0 obj\n<< /Type /Catalog >>\nendobj\nstartxref\n9\n%%EOF\n"
SUPPORTED_UPLOADS = [("a.png", PNG, "image/png"),
                     ("a.jpg", VALID_IMAGES["image/jpeg"], "image/jpeg"),
                     ("a.gif", VALID_IMAGES["image/gif"], "image/gif"),
                     ("a.webp", VALID_IMAGES["image/webp"], "image/webp"),
                     ("a.pdf", PDF, "application/pdf"), ("a.txt", b"hello\n", "text/plain"),
                     ("a.md", b"# A heading", "text/markdown"),
                     ("a.csv", b'key,value\na,"one,two"\n', "text/csv"),
                     ("a.json", b'{"value":1}', "application/json")]


@pytest.mark.parametrize("name,raw,mime", SUPPORTED_UPLOADS)
def test_api_accepts_formats_and_roundtrips_metadata(client, signed_in, attachment_api, name, raw, mime):
    root, _ = attachment_api
    result = upload(client, root, name, raw)
    assert result.status_code == 200, result.text
    view = result.json()
    assert "content" not in view and view["size"] == len(raw)
    assert view["data"] == {"mime_type": mime, "size_bytes": len(raw), "uploaded_by": signed_in["user"]["id"]}
    stored = client.get(root + "/artifacts/" + view["id"]).json()
    assert base64.b64decode(stored["content"]) == raw
    assert client.delete(root + "/attachments/" + view["id"]).status_code == 200
    assert client.get(root + "/artifacts/" + view["id"]).status_code == 404


@pytest.mark.parametrize("extension", ["txt", "md", "csv", "json"])
@pytest.mark.parametrize("markup", ["<svg/onload=alert(1)>", "<HtMl/lang=en><BoDy/onload=alert(1)>",
                                    "<SVG", "<svg >", "<svg/>", "<!DOCTYPE html>"])
def test_disguised_markup_rejected_anywhere(client, attachment_api, extension, markup):
    root, _ = attachment_api
    text = json.dumps({"example": markup}) if extension == "json" else "some text\n" + markup
    assert upload(client, root, "disguised." + extension, text.encode()).status_code == 400


@pytest.mark.parametrize("name,raw", [("bad.json", b'{"value":}'), ("bad.json", b'NaN'),
                                      ("bad.txt", b"binary\x00data"), ("bad.txt", b"binary\x01data"),
                                      ("bad.txt", b"\xff\xfe\x80"), ("bad.png", b"plain text"),
                                      ("bad.jpg", PNG), ("bad.txt", PNG), ("bad.html", b"hello"),
                                      ("bad.svg", PNG), ("bad.csv", b'a,"unterminated'),
                                      ("bad.json", b'{"x":"\\u003csvg/onload=x>"}')])
def test_api_rejects_spoofing_binary_and_invalid_documents(client, attachment_api, name, raw):
    root, _ = attachment_api
    assert upload(client, root, name, raw).status_code == 400


@pytest.mark.parametrize("content", ["data:text/plain;base64,aGk=", "a Gk=", "!!!!", "aGk", "", "é"])
def test_strict_base64(client, attachment_api, content):
    root, _ = attachment_api
    assert client.post(root + "/attachments", json={"name": "a.txt", "content": content}).status_code == 400


def test_bom_json_is_usable_by_runtime_without_changing_original(client, attachment_api, tmp_path):
    root, repo = attachment_api
    raw = b'\xef\xbb\xbf{"value":1}'
    view = upload(client, root, "bom.json", raw).json()
    outcome = submit(client, root, repo, [view["id"]]).json()
    with session_scope() as s:
        assert _decode(s.get(Artifact, view["id"])) == (raw, "application/json")
    sb = LocalSandbox("bom", root=tmp_path / "sb").create()
    sb.work.mkdir()
    context, _ = asyncio.run(prepare_attachments(sb, root.rsplit("/", 1)[-1], outcome["id"]))
    assert '\\ufeff' not in context and '{\\"value\\":1}' in context
    assert (sb.work / f'.factory/attachments/{view["id"]}-bom.json').read_bytes() == raw


def test_sanitised_metadata_and_attribution_committed_before_wake(client, signed_in, attachment_api, monkeypatch):
    from aifactory.models import Event, Outcome
    from aifactory.pipeline.orchestrator import orchestrator

    root, repo = attachment_api
    views = [upload(client, root, "../folder\\..\\unsafe\n<>name.txt").json(),
             upload(client, root, "x" * 250 + ".txt").json()]
    assert views[0]["name"] == "unsafe___name.txt"
    assert len(views[1]["name"]) == 180 and views[1]["name"].endswith(".txt")
    ids = [v["id"] for v in views]
    observed = []

    def wake():
        with session_scope() as s:
            rows = s.query(Artifact).filter(Artifact.id.in_(ids)).all()
            assert len({a.outcome_id for a in rows}) == 1 and rows[0].outcome_id
            assert s.get(Outcome, rows[0].outcome_id) is not None
            event = s.query(Event).filter_by(kind="attachments_added").one()
            assert event.actor == signed_in["user"]["id"]
            assert event.data == {"ids": ids, "names": [v["name"] for v in views], "count": 2}
            assert "content" not in json.dumps(event.data)
            observed.append(True)

    monkeypatch.setattr(orchestrator, "wake", wake)
    response = submit(client, root, repo, ids)
    assert response.status_code == 200, response.text
    assert observed == [True]
    detail = client.get(root + "/outcomes/" + response.json()["id"]).json()
    assert [a["id"] for a in detail["attachments"]] == ids
    assert all("content" not in a for a in detail["attachments"])
    assert client.delete(root + "/attachments/" + ids[0]).status_code == 404
    assert client.get(root + "/artifacts/" + ids[0]).status_code == 200
    assert submit(client, root, repo, ids).status_code == 404


def test_decoded_size_boundaries(client, attachment_api):
    from aifactory.attachments import MAX_FILE_BYTES, MAX_TOTAL_BYTES

    root, repo = attachment_api
    ids = []
    for size in [MAX_FILE_BYTES, MAX_FILE_BYTES, MAX_TOTAL_BYTES - 2 * MAX_FILE_BYTES]:
        response = upload(client, root, raw=b"a" * size)
        assert response.status_code == 200, response.text
        assert response.json()["size"] == size
        ids.append(response.json()["id"])
    assert submit(client, root, repo, ids).status_code == 200
    assert upload(client, root, raw=b"a" * (MAX_FILE_BYTES + 1)).status_code == 400
    # The encoded bound alone is insufficient: these lengths share a base64 size.
    assert upload(client, root, raw=b"a" * (MAX_FILE_BYTES + 2)).status_code == 400
    ids = [upload(client, root, raw=b"a" * size).json()["id"] for size in
           [MAX_FILE_BYTES, MAX_FILE_BYTES, MAX_TOTAL_BYTES - 2 * MAX_FILE_BYTES]]
    assert upload(client, root, raw=b"a").status_code == 413
    # Legacy/corrupt storage cannot bypass the outcome's independent total limit.
    with session_scope() as s:
        row = s.get(Artifact, ids[-1])
        row.content = base64.b64encode(b"a" * (MAX_TOTAL_BYTES - 2 * MAX_FILE_BYTES + 1)).decode()
    assert submit(client, root, repo, ids).status_code == 400


def test_count_duplicate_and_invalid_ids_leave_no_outcome_or_events(client, attachment_api):
    from aifactory.models import Event, Outcome

    root, repo = attachment_api
    ids = [upload(client, root).json()["id"] for _ in range(10)]
    assert upload(client, root).status_code == 413
    for bad in [ids + ["extra"], [ids[0], ids[0]], [ids[0], "missing"]]:
        assert submit(client, root, repo, bad).status_code in (400, 404)
        with session_scope() as s:
            assert s.query(Outcome).count() == 0
            assert s.query(Event).filter(Event.kind.in_(["outcome_created", "attachments_added"])).count() == 0
            assert all(a.outcome_id is None for a in s.query(Artifact))
    assert submit(client, root, repo, ids[:10]).status_code == 200
    assert submit(client, root, repo, []).status_code == 200


def test_expired_staging_and_cleanup_preserve_linked(client, attachment_api):
    from datetime import timedelta
    from aifactory.db import utcnow

    root, repo = attachment_api
    linked = upload(client, root).json()["id"]
    assert submit(client, root, repo, [linked]).status_code == 200
    expired = upload(client, root).json()["id"]
    with session_scope() as s:
        for aid in [linked, expired]:
            s.get(Artifact, aid).created_at = utcnow() - timedelta(hours=25)
    assert submit(client, root, repo, [expired]).status_code == 404
    assert client.get(root + "/artifacts/" + expired).status_code == 404
    assert upload(client, root).status_code == 200
    with session_scope() as s:
        assert s.get(Artifact, expired) is None
        assert s.get(Artifact, linked) is not None
    assert client.get(root + "/artifacts/" + linked).status_code == 200


def test_concurrent_submissions_cannot_reuse_upload(client, attachment_api, monkeypatch):
    from concurrent.futures import ThreadPoolExecutor
    from threading import Barrier
    from aifactory import attachments
    from aifactory.models import Event, Outcome

    root, repo = attachment_api
    aid = upload(client, root).json()["id"]
    barrier = Barrier(2)
    original = attachments.validate_staged

    def validate(*args):
        result = original(*args)
        barrier.wait(timeout=10)
        return result

    monkeypatch.setattr(attachments, "validate_staged", validate)
    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(lambda _: submit(client, root, repo, [aid]), range(2)))
    assert sorted(r.status_code for r in results) == [200, 409]
    with session_scope() as s:
        outcome = s.query(Outcome).one()
        assert s.get(Artifact, aid).outcome_id == outcome.id
        assert s.query(Event).filter_by(kind="outcome_created").count() == 1
        assert s.query(Event).filter_by(kind="attachments_added").count() == 1


def _riff(tag, data):
    chunk = tag + len(data).to_bytes(4, "little") + data + (b"\0" if len(data) % 2 else b"")
    return b"RIFF" + (len(chunk) + 4).to_bytes(4, "little") + b"WEBP" + chunk


def _bomb_png():
    import struct
    import zlib

    def chunk(tag, body):
        return struct.pack(">I", len(body)) + tag + body + struct.pack(">I", zlib.crc32(tag + body))
    return (b"\x89PNG\r\n\x1a\n" + chunk(b"IHDR", struct.pack(">IIBBBBB", 100000, 100000, 8, 6, 0, 0, 0))
            + chunk(b"IDAT", zlib.compress(b"\0" * 1024)) + chunk(b"IEND", b""))


BAD_RASTERS = [
    ("bomb.png", _bomb_png()),
    ("bad.gif", bytes.fromhex("47494638396101000100800000000000ffffff2c0000000001000100000201ff003b")),
    ("bad.webp", _riff(b"VP8L", b"\x2f\0\0\0\xe0")),
    ("bad.webp", _riff(b"VP8L", b"\x2f\0\0\0\0")),
    ("bad.webp", _riff(b"VP8L", b"\x2f\0\0\0\0\0\0\0")),
    ("bad.webp", _riff(b"VP8 ", b"\x20\0\0\x9d\x01\x2a\x01\0\x01\0\0")),
    ("bad.webp", _riff(b"VP8 ", b"\x20\0\0\x9d\x01\x2a\0\0\0\0\0")),
    ("bad.webp", _riff(b"VP8X", b"\xc1" + b"\0" * 9)),
    ("bad.webp", _riff(b"JUNK", b"arbitrary bytes")),
    ("bad.webp", VALID_IMAGES["image/webp"] + b"trailing"),
    ("bad.jpg", b"\xff\xd8\xff\xc0\x00\x08\x08\x00\x01\x00\x01\x00\xff\xd9"),
]


def _jpeg_without_quantization():
    raw = VALID_IMAGES["image/jpeg"]
    while b"\xff\xdb" in raw:
        pos = raw.index(b"\xff\xdb")
        size = int.from_bytes(raw[pos + 2:pos + 4], "big")
        raw = raw[:pos] + raw[pos + 2 + size:]
    return raw


@pytest.mark.parametrize("name,raw", BAD_RASTERS + [("bad.jpg", _jpeg_without_quantization())])
def test_verifier_malformed_rasters_are_rejected(client, attachment_api, name, raw):
    root, _ = attachment_api
    assert upload(client, root, name, raw).status_code == 400


def test_screenshot_serialization_unchanged():
    from aifactory.serializers import artifact as view

    screenshot = Artifact(kind="runtime_qa_screenshot", name="screenshot.png", content=base64.b64encode(PNG).decode())
    assert view(screenshot)["size"] == len(screenshot.content)
    assert "content" not in view(screenshot)
    assert view(screenshot, content=True)["content"] == screenshot.content


def test_png_bomb_rejected_before_decompression(monkeypatch):
    from aifactory.pipeline import image_validation

    def forbidden():
        pytest.fail("Oversized PNG must be rejected before decompression")

    monkeypatch.setattr(image_validation.zlib, "decompressobj", forbidden)
    with pytest.raises(ValueError):
        image_validation.validate_image(_bomb_png(), "image/png")


def test_failure_after_outcome_creation_rolls_back_claims_and_events(client, attachment_api, monkeypatch):
    from aifactory import api
    from aifactory.models import Event, Outcome

    root, repo = attachment_api
    aid = upload(client, root).json()["id"]
    original = api.record

    def fail(*args, **kwargs):
        if args[1] == "attachments_added":
            raise RuntimeError("transaction interrupted")
        return original(*args, **kwargs)

    monkeypatch.setattr(api, "record", fail)
    with pytest.raises(RuntimeError, match="transaction interrupted"):
        submit(client, root, repo, [aid])
    with session_scope() as s:
        assert s.get(Artifact, aid).outcome_id is None
        assert s.query(Outcome).count() == 0
        assert s.query(Event).filter(Event.kind.in_(["outcome_created", "attachments_added"])).count() == 0
    monkeypatch.setattr(api, "record", original)
    assert submit(client, root, repo, [aid]).status_code == 200


@pytest.mark.parametrize("modification", ["reserved", "canvas", "start_code", "riff_size"])
def test_webp_container_mutations(client, attachment_api, modification):
    root, _ = attachment_api
    raw = bytearray(VALID_IMAGES["image/webp"])
    if modification == "reserved":
        raw[20] |= 0x40
    elif modification == "canvas":
        raw[24:30] = b"\xff" * 6
    elif modification == "start_code":
        offset = raw.index(b"VP8 ") + 8
        raw[offset + 3] = 0
    else:
        raw[4:8] = (len(raw) - 9).to_bytes(4, "little")
    assert upload(client, root, "invalid.webp", raw).status_code == 400


@pytest.mark.parametrize("size", [131074, 10 * 1024 * 1024])
def test_csv_large_fields_use_advertised_file_limit(client, attachment_api, size):
    root, _ = attachment_api
    raw = b"a" * (size - 1) + b"\n"
    response = upload(client, root, "large.csv", raw)
    assert response.status_code == 200, response.text
    assert response.json()["size"] == size
    stored = client.get(root + "/artifacts/" + response.json()["id"]).json()
    assert base64.b64decode(stored["content"]) == raw


def test_staged_quota_delete_link_and_expiry_free_capacity(client, attachment_api):
    from datetime import timedelta
    from aifactory.db import utcnow

    root, repo = attachment_api
    ids = [upload(client, root).json()["id"] for _ in range(10)]
    rejected = upload(client, root)
    assert rejected.status_code == 413 and "quota" in rejected.json()["detail"]
    assert client.delete(root + "/attachments/" + ids.pop()).status_code == 200
    replacement = upload(client, root).json()["id"]
    ids.append(replacement)
    assert upload(client, root).status_code == 413
    assert submit(client, root, repo, ids).status_code == 200
    staged = [upload(client, root).json()["id"] for _ in range(10)]
    with session_scope() as s:
        s.get(Artifact, staged[0]).created_at = utcnow() - timedelta(hours=25)
    assert upload(client, root).status_code == 200
    with session_scope() as s:
        assert s.get(Artifact, staged[0]) is None
        assert all(s.get(Artifact, aid).outcome_id for aid in ids)


@pytest.mark.parametrize("quota", ["count", "bytes"])
def test_concurrent_uploads_cannot_exceed_staged_quota(client, attachment_api, monkeypatch, quota):
    from concurrent.futures import ThreadPoolExecutor
    from threading import Barrier
    from aifactory import attachments

    root, _ = attachment_api
    if quota == "count":
        for _ in range(9):
            assert upload(client, root).status_code == 200
        raw = b"a"
    else:
        for _ in range(2):
            assert upload(client, root, raw=b"a" * attachments.MAX_FILE_BYTES).status_code == 200
        raw = b"a" * (5 * 1024 * 1024)
    barrier = Barrier(2)
    original = attachments.validate_upload

    def validate(*args):
        result = original(*args)
        barrier.wait(timeout=30)
        return result

    monkeypatch.setattr(attachments, "validate_upload", validate)
    with ThreadPoolExecutor(max_workers=2) as pool:
        responses = list(pool.map(lambda _: upload(client, root, raw=raw), range(2)))
    assert sorted(r.status_code for r in responses) == [200, 413]
    with session_scope() as s:
        rows = s.query(Artifact).filter_by(kind="attachment", outcome_id=None).all()
        assert len(rows) <= attachments.MAX_FILES
        assert sum(len(base64.b64decode(row.content)) for row in rows) <= attachments.MAX_TOTAL_BYTES
