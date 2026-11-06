import asyncio
import json

import pytest

from auto_mindsdb_factory.codex_http import CodexHTTPError, read_codex_http


TOKEN, HOST = "t" * 43, "127.0.0.1:43123"


def request(body=b'{}'):
    return (f"POST /v1/responses HTTP/1.1\r\nHost: {HOST}\r\nX-Factory-Session: {TOKEN}\r\n"
            f"Content-Type: application/json\r\nContent-Length: {len(body)}\r\n\r\n").encode() + body


def parse(raw, **settings):
    async def run():
        reader = asyncio.StreamReader(limit=16384)
        reader.feed_data(raw); reader.feed_eof()
        return await read_codex_http(reader, **{"token": TOKEN, "host": HOST, "timeout": 1, **settings})
    return asyncio.run(run())


def test_authenticated_utf8_body_is_preserved_without_transport_fields():
    body = {"model": "gpt-6.1-terra", "input": "Untrusted café context"}
    assert parse(request(json.dumps(body, ensure_ascii=False).encode())) == body


def test_exact_maximum_body_size_remains_supported():
    prefix, suffix = b'{"text":"', b'"}'
    payload = prefix + b'x' * (8_000_000 - len(prefix) - len(suffix)) + suffix
    assert len(payload) == 8_000_000
    assert len(parse(request(payload))["text"]) == 8_000_000 - len(prefix) - len(suffix)


@pytest.mark.parametrize("old,new", [(b"POST ", b"GET "), (b"/v1/responses", b"/v1/compact"),
    (b"HTTP/1.1", b"HTTP/1.0"), (HOST.encode(), b"attacker.invalid"), (TOKEN.encode(), b"incorrect"),
    (b"Content-Type: application/json", b"Content-Type: text/plain"),
    (b"Content-Length: 2", b"Content-Length: -2"), (b"Content-Length: 2", b"Content-Length: 8000001"),
    (b"Host:", b" Host:"), (b"Content-Type:", b"Content-Type:\nInjected:")])
def test_other_endpoints_identity_and_invalid_framing_fail_closed(old, new):
    with pytest.raises(CodexHTTPError): parse(request().replace(old, new))


@pytest.mark.parametrize("header", [b"Transfer-Encoding: chunked", b"Content-Encoding: gzip",
    b"Upgrade: websocket", b"Origin: https://attacker.invalid", b"Referer: https://attacker.invalid",
    b"Sec-Fetch-Site: cross-site", b"Content-Length: 2", b"x-Factory-session: " + TOKEN.encode()])
def test_hidden_or_duplicate_header_policy_is_rejected(header):
    with pytest.raises(CodexHTTPError): parse(request().replace(b"\r\n\r\n", b"\r\n" + header + b"\r\n\r\n"))


@pytest.mark.parametrize("body", [b'[]', b'null', b'{"model":"a","model":"b"}', b'{"value":NaN}',
    b'{"value":Infinity}', b'{"value":1e1000}', b'\xff', b'{', b'{"nested":' + b'[' * 2000 + b'0' + b']' * 2000 + b'}'],
    ids=["array", "null", "duplicate-key", "NaN", "Infinity", "numeric-overflow", "invalid-UTF8", "truncated", "excessive-depth"])
def test_malformed_or_ambiguous_json_never_reaches_inference(body):
    with pytest.raises(CodexHTTPError): parse(request(body))


def test_depth_bound_ignores_brackets_and_escaped_quotes_in_strings():
    body = {"text": '[{}]"\\' * 2000}
    assert parse(request(json.dumps(body).encode())) == body
    assert parse(request(b'{"nested":' + b'[' * 127 + b'0' + b']' * 127 + b'}'))
    with pytest.raises(CodexHTTPError):
        parse(request(b'{"nested":' + b'[' * 128 + b'0' + b']' * 128 + b'}'))


def test_truncated_body_oversized_header_and_missing_token_are_rejected():
    for raw in [request()[:-1], request().replace(b"Content-Type:", b"Large: " + b"x" * 17000 + b"\r\nContent-Type:"),
                request().replace(f"X-Factory-Session: {TOKEN}\r\n".encode(), b"")]:
        with pytest.raises(CodexHTTPError): parse(raw)


def test_unauthenticated_headers_are_rejected_before_body_read():
    async def run():
        reader = asyncio.StreamReader(limit=16384)
        reader.feed_data(request().replace(TOKEN.encode(), b"incorrect"))
        async def forbidden_read(size): pytest.fail("Unauthenticated body read")
        reader.readexactly = forbidden_read
        with pytest.raises(CodexHTTPError): await read_codex_http(reader, token=TOKEN, host=HOST, timeout=1)
    asyncio.run(run())


def test_slow_peer_times_out_without_disclosing_capability_or_body():
    async def run():
        reader = asyncio.StreamReader(limit=16384)
        reader.feed_data(b"POST /v1/responses ")
        with pytest.raises(CodexHTTPError) as error:
            await read_codex_http(reader, token=TOKEN, host=HOST, timeout=0.01)
        assert TOKEN not in str(error.value) and str(error.value) == "Local inference request was rejected"
    asyncio.run(run())


def test_external_cancellation_is_not_relabelled_as_a_parse_failure():
    async def run():
        reader = asyncio.StreamReader(limit=16384)
        task = asyncio.create_task(read_codex_http(reader, token=TOKEN, host=HOST, timeout=10))
        await asyncio.sleep(0); task.cancel()
        with pytest.raises(asyncio.CancelledError): await task
    asyncio.run(run())


@pytest.mark.parametrize("setting", [{"token": "provider-key"}, {"host": "127.0.0.1:99999"},
    {"timeout": 0}, {"timeout": True}, {"timeout": float("nan")}])
def test_invalid_proxy_configuration_is_rejected(setting):
    with pytest.raises(CodexHTTPError): parse(request(), **setting)
