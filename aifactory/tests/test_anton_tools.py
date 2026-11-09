"""Protected Anton image reads and session-local SDK replacement."""
from __future__ import annotations

import asyncio
import base64
import io
import os

import pytest
from PIL import Image

from aifactory.sandbox import LocalSandbox, SandboxError, read_image_bytes


def png_bytes():
    stream = io.BytesIO()
    Image.new("RGB", (2, 2), "red").save(stream, format="PNG")
    return stream.getvalue()


def test_local_image_read_is_bounded_and_refuses_all_symlinks(tmp_path):
    sb = LocalSandbox("image", root=tmp_path).create()
    sb.work.mkdir()
    (sb.work / ".factory").mkdir()
    image = sb.work / ".factory" / "screen.png"
    image.write_bytes(png_bytes())
    outside = sb.home / "secret.png"
    outside.write_bytes(png_bytes())
    (sb.work / "outside.png").symlink_to(outside)
    (sb.work / "outside-dir").symlink_to(sb.home, target_is_directory=True)
    (sb.work / "inside.png").symlink_to(image)
    (sb.work / "inside-dir").symlink_to(image.parent, target_is_directory=True)
    os.mkfifo(sb.work / "pipe")

    async def go():
        for path in (".factory/screen.png", str(image)):
            assert await read_image_bytes(sb, path, 1000) == png_bytes()
        for path in ("../home/secret.png", str(outside), "outside.png", "outside-dir/secret.png",
                     "inside.png", "inside-dir/screen.png", "pipe", ".factory", "missing.png"):
            with pytest.raises(SandboxError):
                await read_image_bytes(sb, path, 1000)
        with pytest.raises(SandboxError, match="too large"):
            await read_image_bytes(sb, ".factory/screen.png", 10)
        await sb.close()

    asyncio.run(go())


def test_anton_image_outcome_and_session_replacement(tmp_path, monkeypatch):
    pytest.importorskip("anton.core.tools.tool_defs")
    from anton.core.tools.tool_defs import READ_IMAGE_TOOL
    from anton.utils.clipboard import MAX_IMAGE_BYTES

    from aifactory.harness.anton_tools import replace_session_tools

    sb = LocalSandbox("image", root=tmp_path).create()
    sb.work.mkdir()
    (sb.work / "image.jpg").write_bytes(png_bytes())  # MIME must come from bytes.
    (sb.work / "bad.png").write_bytes(b"\x89PNG\r\n\x1a\nnot really an image")
    with (sb.work / "big.png").open("wb") as handle:
        handle.truncate(MAX_IMAGE_BYTES)
    original = READ_IMAGE_TOOL.handler
    tools = [READ_IMAGE_TOOL]
    replaced = replace_session_tools(tools, sb)
    assert tools == [READ_IMAGE_TOOL] and READ_IMAGE_TOOL.handler is original
    assert replaced[0] is not READ_IMAGE_TOOL
    assert replaced[0].input_schema == READ_IMAGE_TOOL.input_schema
    assert replaced[0].input_schema is not READ_IMAGE_TOOL.input_schema

    async def go():
        result = await replaced[0].handler(None, {"file_path": "image.jpg"})
        assert result.ok is True
        assert result.content[0]["source"]["media_type"] == "image/png"
        assert base64.b64decode(result.content[0]["source"]["data"]) == png_bytes()
        assert result.content[1]["type"] == "text"
        for args in ({}, {"file_path": 1}, {"file_path": "bad.png"}, {"file_path": "big.png"}):
            error = await replaced[0].handler(None, args)
            assert error.ok is False and isinstance(error.content, str) and "Error:" in error.content
        def broken(*args):
            raise RecursionError("malicious image")
        monkeypatch.setattr("aifactory.harness.anton_tools.validate_image", broken)
        error = await replaced[0].handler(None, {"file_path": "image.jpg"})
        assert error.ok is False and isinstance(error.content, str)
        await sb.close()

    asyncio.run(go())


def test_nested_anmf_and_chunk_budget_are_recoverable(tmp_path):
    from aifactory.pipeline.image_validation import _webp

    def riff(body):
        return b"RIFF" + (len(body) + 4).to_bytes(4, "little") + b"WEBP" + body

    body = b"VP8L" + (6).to_bytes(4, "little") + b"\x2f\x00\x00\x00\x00\x00"
    for _ in range(1200):
        body = b"ANMF" + (len(body) + 16).to_bytes(4, "little") + b"\x00" * 16 + body
    nested = riff(body)
    with pytest.raises(ValueError):
        _webp(nested)
    with pytest.raises(ValueError):
        _webp(riff(b"JUNK\x00\x00\x00\x00" * 10_001 + body))
    pytest.importorskip("anton.core.tools.tool_defs")
    from aifactory.harness.anton_tools import sandbox_image_tool

    sb = LocalSandbox("nested", root=tmp_path).create()
    sb.work.mkdir()
    (sb.work / "nested.webp").write_bytes(nested)
    result = asyncio.run(sandbox_image_tool(sb).handler(None, {"file_path": "nested.webp"}))
    assert result.ok is False and isinstance(result.content, str) and "Error:" in result.content
    asyncio.run(sb.close())


@pytest.mark.parametrize("fmt,mime", [("PNG", "image/png"), ("JPEG", "image/jpeg"),
                                     ("GIF", "image/gif"), ("WEBP", "image/webp")])
def test_image_media_types_from_bytes(tmp_path, fmt, mime):
    pytest.importorskip("anton.core.tools.tool_defs")
    from aifactory.harness.anton_tools import sandbox_image_tool

    sb = LocalSandbox("formats", root=tmp_path).create()
    sb.work.mkdir()
    stream = io.BytesIO()
    Image.new("RGB", (2, 2), "red").save(stream, format=fmt)
    (sb.work / "screen.data").write_bytes(stream.getvalue())
    outcome = asyncio.run(sandbox_image_tool(sb).handler(None, {"file_path": "screen.data"}))
    assert outcome.ok is True
    assert outcome.content[0]["source"]["media_type"] == mime
    assert base64.b64decode(outcome.content[0]["source"]["data"]) == stream.getvalue()
    asyncio.run(sb.close())
