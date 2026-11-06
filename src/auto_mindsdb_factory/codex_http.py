"""Bounded loopback HTTP framing; inference policy remains in CodexInference."""

import asyncio
import hmac
import json
import math
import re


class CodexHTTPError(ValueError):
    pass


def _object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("Duplicate JSON key")
        result[key] = value
    return result


def _constant(value):
    raise ValueError("Non-JSON constant")


def _float(value):
    number = float(value)
    if not math.isfinite(number):
        raise ValueError("Non-finite JSON number")
    return number


def _check_depth(payload):
    depth = 0
    for match in re.finditer(rb'"(?:[^"\\]|\\.)*"|[{}\[\]]', payload, re.DOTALL):
        if match.end() - match.start() != 1: continue  # Ignore complete JSON strings.
        depth += 1 if payload[match.start()] in (91, 123) else -1
        if not 0 <= depth <= 128:
            raise ValueError("JSON nesting bound exceeded")


async def read_codex_http(reader, *, token, host, timeout):
    """Read one authenticated bounded POST, without retaining raw headers/body.

    token is a new per-session local capability, never a provider API key. The
    caller owns the loopback listener (reader limit 16KiB), overall lifetime and
    cancellation. The returned object still requires exact inference admission.
    No provider request or executable tool can run through this parser alone.
    """
    if (type(token) is not str or re.fullmatch(r"[A-Za-z0-9_-]{43}", token) is None
            or type(host) is not str or re.fullmatch(r"127\.0\.0\.1:[1-9][0-9]{0,4}", host) is None
            or int(host.split(":")[1]) > 65535 or type(timeout) not in (int, float)
            or not math.isfinite(timeout) or timeout <= 0):
        raise CodexHTTPError("Invalid local proxy configuration")
    try:
        async with asyncio.timeout(timeout):
            raw = await reader.readuntil(b"\r\n\r\n")
            if len(raw) > 16384:
                raise ValueError("Header bound exceeded")
            lines = raw[:-4].decode("ascii").split("\r\n")
            if lines[0] != "POST /v1/responses HTTP/1.1":
                raise ValueError("Unsupported request")
            headers = {}
            for line in lines[1:]:
                key, separator, value = line.partition(":")
                if (not separator or re.fullmatch(r"[!#$%&'*+\-.^_`|~0-9A-Za-z]+", key) is None
                        or key.lower() in headers or any(c != "\t" and not 32 <= ord(c) < 127 for c in value)):
                    raise ValueError("Invalid headers")
                headers[key.lower()] = value.strip(" \t")
            if (headers.get("host") != host
                    or not hmac.compare_digest(headers.get("x-factory-session", ""), token)
                    or any(key in headers for key in ("transfer-encoding", "content-encoding", "upgrade", "origin", "referer", "sec-fetch-site"))
                    or headers.get("content-type", "").split(";", 1)[0].strip().lower() != "application/json"):
                raise ValueError("Unauthorized or unsupported request")
            length = headers.get("content-length", "")
            if re.fullmatch(r"[1-9][0-9]{0,6}", length) is None or int(length) > 8_000_000:
                raise ValueError("Body bound exceeded")
            payload = await reader.readexactly(int(length))
            _check_depth(payload)
            body = json.loads(payload.decode("utf-8"),
                              object_pairs_hook=_object, parse_constant=_constant, parse_float=_float)
            if type(body) is not dict:
                raise ValueError("Invalid JSON object")
            return body
    except (ValueError, RecursionError, asyncio.IncompleteReadError,
            asyncio.LimitOverrunError, TimeoutError):
        raise CodexHTTPError("Local inference request was rejected") from None
