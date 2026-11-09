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
            assert (await sb.exec(f"base64 -w0 {path}")).output.strip() == base64.b64encode(raw).decode()
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
        result = await vm.exec("stat -c %a run.sh; ./run.sh; git diff HEAD")
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
