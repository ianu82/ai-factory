"""Shared image capability classification and bounded payload-safe output handling."""
from __future__ import annotations

import bisect
import json
import re

OMITTED = "[image omitted]"


def rejects_image_input(exc: Exception) -> bool:
    # Import lazily: inference also uses this rule before logging provider errors.
    from .inference import BudgetExceeded, InferenceError

    if not isinstance(exc, InferenceError) or isinstance(exc, BudgetExceeded):
        return False
    if exc.retryable or exc.status not in (400, 415, 422):
        return False
    text = str(exc).lower().replace("’", "'")
    for stem, expanded in (("does", "does not"), ("do", "do not"), ("is", "is not"),
                           ("are", "are not"), ("ca", "can not")):
        text = re.sub(rf"\b{stem}\s*n\s*[' ]?\s*t\b", expanded, text)
    text = re.sub(r"\bcannot\b", "can not", text)
    for clause in re.split(r"[.;!\n]", text):
        clause = " ".join(re.findall(r"[a-z0-9_]+", clause))
        if re.search(r"\b(tool|tools|function|functions|schema|parameters?|format|encoding|encoded|base64|"
                     r"data urls?|data uris?|inline|mime|type|size|larger?|pixels?|resolution|dimensions|"
                     r"count|frames?|tiff|png|jpeg|jpg|gif|webp)\b", clause):
            continue
        image = r"(?:images?|image inputs?|vision|image_url)"
        if (re.search(rf"\b(?:does not|do not|can not) support {image}$", clause)
                or re.search(rf"\b{image} (?:is|are) not supported(?: (?:by|for) (?:this|the) model)?$", clause)
                or clause == "image_url is only supported by certain models"):
            return True
    return False


class ImagePayloadRedactor:
    """Index once per session. Catch any known fragment >=96 non-space chars.

    32-character anchors spaced by 64 guarantee an anchor in each such fragment.
    Compact run offsets map matches back across arbitrary base64 line wrapping.
    """
    def __init__(self, images=()):
        self.payloads = [re.sub(r"\s+", "", i["content"]) for i in images]
        self.anchors = {}
        for index, payload in enumerate(self.payloads):
            for offset in range(0, len(payload) - 31, 64):
                anchor = payload[offset:offset + 32]
                hit = (index, offset)
                previous = self.anchors.get(anchor)
                if previous is None:
                    self.anchors[anchor] = hit
                elif isinstance(previous, tuple):
                    self.anchors[anchor] = [previous, hit]
                else:
                    previous.append(hit)

    def spans(self, text: str) -> list[tuple[int, int]]:
        clean = re.sub(r"\s+", "", text)
        spans = []
        i = 0
        while i <= len(clean) - 32:
            hit = self.anchors.get(clean[i:i + 32])
            if hit is None:
                i += 1
                continue
            candidates = [hit] if isinstance(hit, tuple) else hit
            matched = False
            for index, offset in candidates:
                payload = self.payloads[index]
                left, right = i, i + 32
                pleft, pright = offset, offset + 32
                while left and pleft and clean[left - 1] == payload[pleft - 1]:
                    left -= 1
                    pleft -= 1
                while (right + 1024 <= len(clean) and pright + 1024 <= len(payload)
                       and clean[right:right + 1024] == payload[pright:pright + 1024]):
                    right += 1024
                    pright += 1024
                while right < len(clean) and pright < len(payload) and clean[right] == payload[pright]:
                    right += 1
                    pright += 1
                if right - left >= 96 or right - left == len(payload):
                    spans.append((left, right))
                    i = right
                    matched = True
                    break
            if not matched:
                i += 1
        # Small images still need exact redaction.
        for payload in self.payloads:
            if len(payload) < 96 and payload:
                spans.extend((m.start(), m.end()) for m in re.finditer(re.escape(payload), clean))
        if spans:
            runs, starts, n = [], [], 0
            for m in re.finditer(r"\S+", text):
                starts.append(n)
                runs.append(m.start())
                n += m.end() - m.start()
            def original(pos):
                j = bisect.bisect_right(starts, pos) - 1
                return runs[j] + pos - starts[j]
            spans = [(original(a), original(b - 1) + 1) for a, b in spans]
        # Scan only unmatched regions: known multi-megabyte payloads are already
        # covered and must not pay for a second per-character regex search.
        for m in re.finditer(r"data:image/[^\s;,]+;base64,", text, re.IGNORECASE):
            for index, (a, b) in enumerate(spans):
                if a >= m.end() and not text[m.end():a].strip():
                    spans[index] = (m.start(), b)
                    break
        cursor = 0
        gaps = []
        for a, b in sorted(spans):
            if a > cursor:
                gaps.append((cursor, a))
            cursor = max(cursor, b)
        if cursor < len(text):
            gaps.append((cursor, len(text)))
        for a, b in gaps:
            spans.extend((a + m.start(), a + m.end()) for m in re.finditer(
                r"data:image/[^\s;,]+;base64,(?:[A-Za-z0-9+/=]\s*)+|[A-Za-z0-9+/=]{512,}",
                text[a:b], re.IGNORECASE))
        merged = []
        for a, b in sorted(spans):
            if merged and a <= merged[-1][1]:
                merged[-1] = (merged[-1][0], max(b, merged[-1][1]))
            else:
                merged.append((a, b))
        return merged

    def text(self, text: str) -> str:
        for a, b in reversed(self.spans(text)):
            text = text[:a] + OMITTED + text[b:]
        return text

    def value(self, value):
        if isinstance(value, str):
            if value.lstrip().startswith(("{", "[")):
                try:
                    parsed = json.loads(value)
                except (ValueError, RecursionError):
                    pass
                else:
                    if isinstance(parsed, (dict, list)):
                        safe = self.value(parsed)
                        return json.dumps(safe) if safe != parsed else value
            return self.text(value)
        if isinstance(value, list):
            return [self.value(v) for v in value]
        if isinstance(value, dict):
            return {self.text(k): self.value(v) for k, v in value.items()}
        return value


class ImageOutputRedactor:
    """Keep an overlap until matches spanning incoming chunks can be classified."""
    def __init__(self, matcher: ImagePayloadRedactor):
        self.matcher = matcher
        self.pending = ""

    def feed(self, text: str, *, final: bool = False) -> str:
        self.pending += text
        if final:
            result = self.matcher.text(self.pending)
            self.pending = ""
            return result
        # Hold enough non-whitespace characters for both anchors and the backstop.
        end = len(self.pending)
        count = 0
        # Bound raw overlap too: whitespace-only (or very sparsely wrapped)
        # output must not accumulate or rescan an unbounded pending buffer.
        while end and count < 1024 and len(self.pending) - end < 8192:
            end -= 1
            count += not self.pending[end].isspace()
        spans = self.matcher.spans(self.pending)
        for a, b in spans:
            if a < end < b:
                end = a
        result = self.matcher.text(self.pending[:end])
        self.pending = self.pending[end:]
        # Avoid retaining multi-megabyte matched runs until EOF.
        if spans and spans[-1][0] == end and spans[-1][1] == len(self.pending) + end:
            # Retain a small raw overlap so later wrapped continuations still match.
            raw = self.pending
            if len(raw) > 8192:
                self.pending = raw[-2048:]
                if re.match(r"data:image/[^\s;,]+;base64,", raw, re.IGNORECASE):
                    self.pending = "data:image/png;base64," + self.pending
                result += OMITTED
        return result
