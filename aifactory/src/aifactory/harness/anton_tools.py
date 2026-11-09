"""Session-local Anton tools; no SDK globals or other harnesses are changed."""

from __future__ import annotations

import base64
from copy import deepcopy
from dataclasses import replace

from ..pipeline.image_validation import validate_image
from ..sandbox import read_image_bytes


def sandbox_image_tool(sandbox):
    from anton.core.tools.registry import ToolOutcome
    from anton.core.tools.tool_defs import READ_IMAGE_TOOL
    from anton.utils.clipboard import MAX_IMAGE_BYTES, _media_type_for, human_size, sniff_image_format

    async def handler(_session, arguments):
        try:
            path = arguments.get("file_path")
            if not isinstance(path, str) or not path.strip():
                raise ValueError("file_path is required")
            path = path.strip()
            # Exact base64 ceiling (including padding), enforced before reading.
            raw = await read_image_bytes(sandbox, path, (MAX_IMAGE_BYTES // 4) * 3)
            fmt = sniff_image_format(raw[:16])
            if fmt not in ("PNG", "JPEG", "GIF", "WEBP"):
                raise ValueError("Unsupported image; expected PNG, JPEG, GIF or WebP")
            mime = _media_type_for(fmt)
            validate_image(raw, mime)
        except Exception as exc:
            # Validation libraries may raise more than ValueError (including
            # RecursionError on malicious containers). Never end a tool turn or
            # emit vision blocks when any part of reading/validation fails.
            return ToolOutcome(content=f"Error: cannot read image: {exc}", ok=False, reason="image_read_error")
        return ToolOutcome(content=[
            {"type": "image", "source": {"type": "base64", "media_type": mime,
                                          "data": base64.b64encode(raw).decode("ascii")}},
            {"type": "text", "text": f"Loaded {path} ({human_size(len(raw))})."},
        ], ok=True)

    return replace(READ_IMAGE_TOOL, input_schema=deepcopy(READ_IMAGE_TOOL.input_schema), handler=handler,
                   description="Read a PNG, JPEG, GIF or WebP inside the sandbox checkout, including .factory screenshots. "
                               "Paths may be repository-relative or checkout-absolute; symlinks are refused.")


def replace_session_tools(tools, sandbox):
    """Return a fresh tool list with the protected reader added/replaced."""
    image = sandbox_image_tool(sandbox)
    return [tool for tool in tools if tool.name != image.name] + [image]
