"""Bounded image validation before storage and model hand-off.

Do not treat magic bytes as validation: walk complete, bounded records and require
image data and dimensions. PNG additionally checks CRCs and its compressed pixels.
Pillow verifies and fully decodes the stream after the container checks.
"""
from __future__ import annotations

import io
import struct
import warnings
import zlib

from PIL import Image

# Fixed process-wide budget; warnings are promoted locally during validation.
Image.MAX_IMAGE_PIXELS = 50_000_000
MAX_DECODED_BYTES = 128 * 1024 * 1024


def _require(condition: bool) -> None:
    if not condition:
        raise ValueError("Invalid attachment image content")


def validate_image(raw: bytes, mime: str) -> None:
    try:
        _check_dimensions(raw, mime)
        {"image/png": _png, "image/jpeg": _jpeg, "image/gif": _gif,
         "image/webp": _webp}[mime](raw)
        _fully_decode(raw, mime)
    except (IndexError, struct.error, zlib.error) as exc:
        raise ValueError("Invalid attachment image content") from exc


def _png(raw: bytes) -> None:
    _require(raw[:8] == b"\x89PNG\r\n\x1a\n")
    pos, chunks, compressed = 8, [], bytearray()
    width = height = depth = colour = interlace = 0
    while pos < len(raw):
        size, tag = struct.unpack_from(">I4s", raw, pos)
        end = pos + 12 + size
        _require(end <= len(raw))
        data = raw[pos + 8:end - 4]
        _require(zlib.crc32(tag + data) == int.from_bytes(raw[end - 4:end], "big"))
        if not chunks:
            _require(tag == b"IHDR" and size == 13)
            width, height, depth, colour, compression, filtering, interlace = struct.unpack(">IIBBBBB", data)
            depths = {0: (1, 2, 4, 8, 16), 2: (8, 16), 3: (1, 2, 4, 8), 4: (8, 16), 6: (8, 16)}
            _require(width > 0 and height > 0 and depth in depths.get(colour, ())
                     and compression == filtering == 0 and interlace in (0, 1))
        elif tag == b"IHDR":
            _require(False)
        if tag == b"PLTE":
            _require(size > 0 and size % 3 == 0 and size <= 768 and b"IDAT" not in chunks)
        if tag == b"IDAT":
            _require(not chunks or chunks[-1] in (b"IDAT", b"IHDR", b"PLTE") or b"IDAT" not in chunks)
            compressed.extend(data)
        chunks.append(tag)
        pos = end
        if tag == b"IEND":
            _require(size == 0 and pos == len(raw))
            break
    _require(chunks[-1:] == [b"IEND"] and compressed and (colour != 3 or b"PLTE" in chunks))
    channels = {0: 1, 2: 3, 3: 1, 4: 2, 6: 4}[colour]
    passes = [(0, 0, 1, 1)] if not interlace else [
        (0, 0, 8, 8), (4, 0, 8, 8), (0, 4, 4, 8), (2, 0, 4, 4),
        (0, 2, 2, 4), (1, 0, 2, 2), (0, 1, 1, 2)]
    rows = []
    for x, y, dx, dy in passes:
        w, h = max(0, (width - x + dx - 1) // dx), max(0, (height - y + dy - 1) // dy)
        if w and h:
            rows.append((h, (w * channels * depth + 7) // 8 + 1))
    expected = sum(h * stride for h, stride in rows)
    # Bound decompression even for a tiny compressed image claiming huge dimensions.
    _require(expected <= 128 * 1024 * 1024)
    decoder = zlib.decompressobj()
    pixels = decoder.decompress(compressed, expected + 1)
    _require(len(pixels) == expected and decoder.eof and not decoder.unused_data and not decoder.unconsumed_tail)
    offset = 0
    for h, stride in rows:
        _require(all(pixels[offset + n * stride] <= 4 for n in range(h)))
        offset += h * stride


def _jpeg(raw: bytes) -> None:
    _require(raw[:2] == b"\xff\xd8")
    pos, frame, tables, scan = 2, False, set(), False
    while pos < len(raw):
        _require(raw[pos] == 255)
        while pos < len(raw) and raw[pos] == 255:
            pos += 1
        marker = raw[pos]
        pos += 1
        if marker == 0xD9:
            _require(frame and scan and pos == len(raw))
            return
        _require(marker not in (0, 0xD8) and not 0xD0 <= marker <= 0xD7)
        size = int.from_bytes(raw[pos:pos + 2], "big")
        _require(size >= 2 and pos + size <= len(raw))
        data = raw[pos + 2:pos + size]
        pos += size
        if marker in (0xC0, 0xC1, 0xC2):
            _require(len(data) >= 6 and data[0] in (8, 12) and int.from_bytes(data[1:3], "big") > 0
                     and int.from_bytes(data[3:5], "big") > 0 and data[5] in (1, 3, 4)
                     and len(data) == 6 + 3 * data[5])
            frame = True
        elif marker in (0xDB, 0xC4):
            _require(len(data) > 16)
            tables.add(marker)
        elif marker == 0xDA:
            _require(frame and tables == {0xDB, 0xC4} and len(data) >= 6
                     and data[0] > 0 and len(data) == 4 + 2 * data[0])
            start = pos
            while pos < len(raw):
                if raw[pos] != 255:
                    pos += 1
                elif pos + 1 < len(raw) and (raw[pos + 1] == 0 or 0xD0 <= raw[pos + 1] <= 0xD7):
                    pos += 2
                else:
                    break
            _require(pos > start)
            scan = True
    _require(False)


def _gif(raw: bytes) -> None:
    _require(raw[:6] in (b"GIF87a", b"GIF89a") and len(raw) >= 13)
    width, height = struct.unpack_from("<HH", raw, 6)
    _require(width > 0 and height > 0)
    pos = 13 + (3 * (2 << (raw[10] & 7)) if raw[10] & 128 else 0)
    frames = 0

    def blocks(start: int) -> tuple[int, bytes]:
        data = bytearray()
        while True:
            _require(start < len(raw))
            size = raw[start]
            start += 1
            if not size:
                return start, bytes(data)
            _require(start + size <= len(raw))
            data.extend(raw[start:start + size])
            start += size

    while pos < len(raw):
        tag = raw[pos]
        pos += 1
        if tag == 0x3B:
            _require(frames > 0 and pos == len(raw))
            return
        if tag == 0x21:
            pos, _ = blocks(pos + 1)  # extension label, then sub-blocks
            continue
        _require(tag == 0x2C and pos + 9 <= len(raw))
        x, y, w, h, packed = struct.unpack_from("<HHHHB", raw, pos)
        _require(w > 0 and h > 0 and x + w <= width and y + h <= height)
        pos += 9 + (3 * (2 << (packed & 7)) if packed & 128 else 0)
        minimum = raw[pos]
        _require(2 <= minimum <= 8)
        pos, data = blocks(pos + 1)
        # Decode LZW lengths (not pixel buffers), checking dictionary references,
        # the end code and exact pixel count without allocating a decompressed image.
        clear, bits, bit, count, previous = 1 << minimum, minimum + 1, 0, 0, 0
        table = list(range(1 << minimum))  # replaced by lengths on clear
        seen_clear = False
        while bit + bits <= len(data) * 8:
            code = (int.from_bytes(data[bit // 8:bit // 8 + 3], "little") >> (bit % 8)) & ((1 << bits) - 1)
            bit += bits
            if code == clear:
                table = [1] * clear + [0, 0]
                bits, previous, seen_clear = minimum + 1, 0, True
                continue
            _require(seen_clear)
            if code == clear + 1:
                _require(count == w * h)
                break
            _require(code < len(table) or (code == len(table) and previous > 0))
            length = table[code] if code < len(table) else previous + 1
            _require(length > 0)
            count += length
            _require(count <= w * h)
            if previous and len(table) < 4096:
                table.append(previous + 1)
                if len(table) == 1 << bits and bits < 12:
                    bits += 1
            previous = length
        else:
            _require(False)
        frames += 1
    _require(False)


def _webp(raw: bytes, depth: int = 0, budget: list[int] | None = None) -> None:
    # ANMF may contain only image chunks, not arbitrarily nested animations.
    # Bound both stack depth and cumulative work even for hostile containers.
    _require(depth <= 1)
    if budget is None:
        budget = [10_000]
    _require(raw[:4] == b"RIFF" and raw[8:12] == b"WEBP"
             and int.from_bytes(raw[4:8], "little") + 8 == len(raw))
    pos, images = 12, 0
    while pos < len(raw):
        budget[0] -= 1
        _require(budget[0] >= 0)
        _require(pos + 8 <= len(raw))
        tag, size = raw[pos:pos + 4], int.from_bytes(raw[pos + 4:pos + 8], "little")
        data = raw[pos + 8:pos + 8 + size]
        pos += 8 + size + (size & 1)
        _require(len(data) == size and pos <= len(raw))
        if tag == b"VP8 ":
            _require(size > 10 and data[0] & 1 == 0 and data[3:6] == b"\x9d\x01\x2a"
                     and int.from_bytes(data[6:8], "little") & 0x3FFF > 0
                     and int.from_bytes(data[8:10], "little") & 0x3FFF > 0)
            partition = int.from_bytes(data[:3], "little") >> 5
            _require(partition > 0 and partition + 10 <= size)
            images += 1
        elif tag == b"VP8L":
            _require(size > 5 and data[0] == 0x2F and data[4] >> 5 == 0)
            images += 1
        elif tag == b"VP8X":
            _require(size == 10 and data[0] & 0xC1 == 0 and data[1:4] == b"\x00\x00\x00")
        elif tag == b"ANMF":
            _require(size > 16)
            # Animation frames contain their own padded image chunks.
            body = data[16:]
            _webp(b"RIFF" + (len(body) + 4).to_bytes(4, "little") + b"WEBP" + body, depth + 1, budget)
            images += 1
    _require(pos == len(raw) and images > 0)


def _check_dimensions(raw: bytes, mime: str) -> None:
    try:
        with warnings.catch_warnings():
            warnings.simplefilter("error", Image.DecompressionBombWarning)
            with Image.open(io.BytesIO(raw)) as image:
                _require(image.get_format_mimetype() == mime)
                width, height = image.size
                _require(0 < width * height <= Image.MAX_IMAGE_PIXELS)
                _require(width * height * max(4, len(image.getbands())) <= MAX_DECODED_BYTES)
    except (OSError, ValueError, SyntaxError, EOFError, Image.DecompressionBombWarning,
            Image.DecompressionBombError) as exc:
        raise ValueError("Invalid attachment image content") from exc


def _fully_decode(raw: bytes, mime: str) -> None:
    """Container checks alone cannot prove an encoded raster stream is valid."""
    formats = {"image/png": "PNG", "image/jpeg": "JPEG", "image/gif": "GIF", "image/webp": "WEBP"}
    try:
        with warnings.catch_warnings():
            warnings.simplefilter("error", Image.DecompressionBombWarning)
            with Image.open(io.BytesIO(raw)) as image:
                _require(image.format == formats[mime])
                width, height = image.size
                _require(0 < width * height <= Image.MAX_IMAGE_PIXELS)
                _require(width * height * max(4, len(image.getbands())) <= MAX_DECODED_BYTES)
                image.verify()
            with Image.open(io.BytesIO(raw)) as image:
                total_pixels = 0
                # Validate every animation frame and bound cumulative decode work.
                for frame in range(getattr(image, "n_frames", 1)):
                    image.seek(frame)
                    total_pixels += image.width * image.height
                    _require(total_pixels <= Image.MAX_IMAGE_PIXELS)
                    image.load()
    except (OSError, ValueError, SyntaxError, EOFError, Image.DecompressionBombWarning,
            Image.DecompressionBombError) as exc:
        raise ValueError("Invalid attachment image content") from exc
