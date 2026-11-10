"""Shared image delivery, narrowly scoped fallback and session-local diagnostics."""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any

MAX_IMAGE_BYTES = 5 * 1024 * 1024
MAX_IMAGES = 12
IMAGE_FAILURE = "Image input is unavailable for this model; continue using files and text only."
_DATA_URL = re.compile(r"data:image/[\w.+-]+;base64,[A-Za-z0-9+/=]+", re.I)
_BINARY = re.compile(r"https?://[^\s\"<>]+|[A-Za-z0-9+/=]{512,}")


@dataclass(frozen=True)
class ImageResult:
    description: str
    mime_type: str
    data: str = field(repr=False)

    def parts(self) -> list[dict[str, Any]]:
        return [{"type": "text", "text": self.description},
                {"type": "image_url", "image_url": {"url": f"data:{self.mime_type};base64,{self.data}"}}]


def image_mime(raw: bytes, path: str) -> str:
    from pathlib import PurePosixPath

    mime = None
    if raw.startswith(b"\x89PNG\r\n\x1a\n"):
        mime = "image/png"
    elif raw.startswith(b"\xff\xd8\xff"):
        mime = "image/jpeg"
    elif raw.startswith((b"GIF87a", b"GIF89a")):
        mime = "image/gif"
    elif len(raw) >= 12 and raw[:4] == b"RIFF" and raw[8:12] == b"WEBP":
        mime = "image/webp"
    suffix = PurePosixPath(path).suffix.lower()
    expected = {".png": "image/png", ".jpg": "image/jpeg", ".jpeg": "image/jpeg",
                ".gif": "image/gif", ".webp": "image/webp"}.get(suffix)
    if mime is None or expected != mime:
        raise ValueError("Unsupported or mismatched image; expected PNG, JPEG, GIF or WebP")
    return mime


def is_image_rejection(error: Exception) -> bool:
    """Only input-capability rejections, never auth, capacity or tool definitions."""
    if getattr(error, "status", None) not in (400, 404, 415, 422):
        return False
    text = str(error).lower()
    # A mention of image_url inside a tool definition is not image input.
    if re.search(r"(?:tool|function)\s+(?:schema|definition|parameters)|function parameters", text):
        return False
    image = r"(?:image(?:\s+inputs?)?|images|vision|image_url)"
    capability = re.compile(
        rf"\b(?:does not|doesn't|do not|cannot|can't)\s+support\s+{image}\s*$"
        rf"|\b{image}\s+(?:is|are)\s+not\s+supported(?:\s+(?:by|for)\s+(?:this|the)\s+model)?\s*$")
    representation = re.compile(
        r"\b(?:format|encoding|encoded|base64|data[\s-]+(?:urls?|uris?)|inline|mime|"
        r"type|size|bytes|large|larger|smaller|resolution|dimensions|pixels|tiff|png|jpe?g|gif|webp)\b")
    for clause in re.split(r"[.;!\n]", text):
        if re.search(r"\bimage_url is only supported by certain models\s*$", clause):
            return True
        if not representation.search(clause) and capability.search(clause):
            return True
    return False


def strip_image_parts(messages: list[dict[str, Any]], tool_ids: set[str] | None = None) -> None:
    """Strip replay images in place without disturbing assistant/tool batch ordering."""
    for message in messages:
        content = message.get("content")
        if isinstance(content, list):
            message["content"] = [p for p in content if p.get("type") not in ("image_url", "input_image", "image")]
            if not message["content"]:
                message["content"] = IMAGE_FAILURE
        if message.get("role") == "tool" and message.get("tool_call_id") in (tool_ids or set()):
            message["content"] = IMAGE_FAILURE


class ImageRedactor:
    """One incremental matcher per session. Large payloads use sparse fixed anchors.

    Every >=96-character fragment of a large payload contains a complete indexed
    32-character anchor. No quadratic shingle index or copies of prior matchers.
    """
    def __init__(self) -> None:
        self.payloads: set[str] = set()
        self.small: list[str] = []
        self.anchors: dict[str, tuple[str, int] | list[tuple[str, int]]] = {}

    def add(self, payload: str) -> None:
        if not payload or payload in self.payloads:
            return
        self.payloads.add(payload)
        if len(payload) <= 256:
            self.small.append(payload)
        else:
            for offset in range(0, len(payload) - 31, 64):
                anchor = payload[offset:offset + 32]
                previous = self.anchors.get(anchor)
                entry = (payload, offset)
                if previous is None:
                    self.anchors[anchor] = entry
                elif isinstance(previous, tuple):
                    self.anchors[anchor] = [previous, entry]
                else:
                    previous.append(entry)

    def __call__(self, text: str, *, preserve_length: bool = False) -> str:
        def omitted(match):
            return "*" * len(match.group()) if preserve_length else "[image data omitted]"
        # Do not rewrite ordinary output when no image was delivered.
        if not self.payloads:
            return _DATA_URL.sub(omitted, text)
        text = _DATA_URL.sub(omitted, text)
        for payload in self.small:
            text = text.replace(payload, "*" * len(payload) if preserve_length else "[image data omitted]")
        # Backstop also avoids scanning enormous, contiguous binary runs.
        text = _BINARY.sub(lambda m: m.group() if m.group().startswith(("http://", "https://"))
                           or re.fullmatch(r"[0-9a-fA-F]+", m.group()) else omitted(m), text)
        spans: list[tuple[int, int]] = []
        i, end = 0, len(text) - 31
        while self.anchors and i < end:
            candidates = self.anchors.get(text[i:i + 32])
            if not candidates:
                i += 1
                continue
            best = (i, i)
            for payload, offset in ([candidates] if isinstance(candidates, tuple) else candidates):
                left, right = 0, 32
                while i - left > 0 and offset - left > 0 and text[i - left - 1] == payload[offset - left - 1]:
                    left += 1
                while i + right < len(text) and offset + right < len(payload) and text[i + right] == payload[offset + right]:
                    right += 1
                if right + left > best[1] - best[0]:
                    best = (i - left, i + right)
                if best[1] == len(text):
                    break
            spans.append(best)
            i = best[1]
        for start, stop in reversed(spans):
            text = text[:start] + ("*" * (stop - start) if preserve_length else "[image data omitted]") + text[stop:]
        return text


class DiagnosticStream:
    """Bounded diagnostic tail with raw look-behind, never masked matching input."""
    LIMIT = 64 * 1024
    WINDOW = 512

    def __init__(self, redactor: ImageRedactor):
        self.redactor = redactor
        self.pending = ""
        self.tail = ""
        self.mask = ""

    def feed(self, chunk: str) -> None:
        # Process bounded pieces even when a VM gives us an entire command at once.
        for offset in range(0, len(chunk), self.LIMIT):
            raw = self.pending + chunk[offset:offset + self.LIMIT]
            cut = max(0, len(raw) - self.WINDOW)
            masked = self.redactor(raw, preserve_length=True)
            # Previously recognized spans remain masked, but matching always sees raw.
            masked = "".join("*" if old == "*" else new
                             for old, new in zip(self.mask, masked)) + masked[len(self.mask):]
            self.tail = (self.tail + masked[:cut])[-self.LIMIT:]
            self.pending = raw[cut:]
            self.mask = masked[cut:]

    def finish(self) -> str:
        masked = self.redactor(self.pending, preserve_length=True)
        masked = "".join("*" if old == "*" else new for old, new in zip(self.mask, masked))
        self.tail = (self.tail + masked)[-self.LIMIT:]
        self.pending = self.mask = ""
        return self.tail


def collect_image_payloads(value: Any, redactor: ImageRedactor) -> None:
    """Inspect request bodies, not their string representation."""
    if isinstance(value, dict):
        if value.get("type") == "base64" and isinstance(value.get("data"), str):
            redactor.add(value["data"])
        for child in value.values():
            collect_image_payloads(child, redactor)
    elif isinstance(value, list):
        for child in value:
            collect_image_payloads(child, redactor)
    elif isinstance(value, str) and value.startswith("data:image/") and ";base64," in value:
        redactor.add(value.split(";base64,", 1)[1])


def disable_images(session) -> None:
    session.images_disabled = True
    if not session.image_warning_sent:
        session.image_warning_sent = True
        session.on_progress("image_fallback", IMAGE_FAILURE, {})
