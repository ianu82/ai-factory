"""Deterministic HTTP fixtures; no public service or controller HTTP client used."""
from __future__ import annotations

import asyncio
import json
import subprocess
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from types import SimpleNamespace

import pytest

from aifactory.harness import anton_web as web
from aifactory.sandbox import ExecResult, LocalSandbox


@pytest.fixture
def http_site():
    requests = []

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass

        def do_GET(self):
            requests.append((self.path, dict(self.headers)))
            if self.path == '/disconnect':
                self.close_connection = True
                return
            if self.path.startswith('/redirect'):
                self.send_response(302)
                self.send_header('Location', '/page?token=redirect-secret')
                self.end_headers()
                return
            if self.path == '/loop':
                self.send_response(302)
                self.send_header('Location', '/loop')
                self.end_headers()
                return
            if self.path == '/denied':
                self.send_response(302)
                self.send_header('Location', 'file:///etc/passwd')
                self.end_headers()
                return
            if self.path.startswith('/search'):
                body = b'<a class="result__a" href="//duckduckgo.com/l/?uddg=https%3A%2F%2Fexample.org%2Fa">First hit</a>' * 30
            elif self.path == '/large':
                body = b'x' * 300000
            elif self.path == '/output':
                body = b'x' * 100000
            elif self.path.startswith('/unavailable'):
                self.send_response(503)
                self.end_headers()
                self.wfile.write(b'provider-private-response')
                return
            else:
                body = b'<html><script>hidden</script><p>Fixture content</p></html>'
            self.send_response(200)
            self.send_header('Content-Type', 'text/html')
            self.end_headers()
            self.wfile.write(body)

    server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    yield f'http://127.0.0.1:{server.server_port}', requests
    server.shutdown()
    server.server_close()
    thread.join()


class FakeSandbox:
    """Executes the exact shipped worker, with the run tool's sanitized env.

    Same exec contract for local and VM; the adapter cannot use either one's
    controller transport or filesystem as an HTTP fallback.
    """
    def __init__(self, env, kind='local'):
        self.calls = []
        self.env = env
        self.kind = kind

    async def exec(self, command, *, stdin, timeout):
        self.calls.append((command, json.loads(stdin), timeout))
        def run():
            result = subprocess.run(command, shell=True, input=stdin, env=self.env,
                                    stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=timeout)
            return ExecResult(command, result.returncode, result.stdout.decode(), 0)
        return await asyncio.to_thread(run)


def session(sandbox, policy=None, command_timeout=10):
    events = []
    return SimpleNamespace(sandbox=sandbox, tool_policy=policy, command_timeout=command_timeout,
                           remaining=lambda: 10, cancelled=asyncio.Event(),
                           on_progress=lambda *event: events.append(event), events=events)


@pytest.fixture
def scrubbed_env(tmp_path, monkeypatch):
    monkeypatch.setenv('FACTORY_SECRET', 'controller-secret')
    monkeypatch.setenv('BRAVE_API_KEY', 'controller-search-key')
    monkeypatch.setenv('HTTP_PROXY', 'http://controller:proxy-password@127.0.0.1:1')
    sb = LocalSandbox('web-env', root=tmp_path)
    env = sb.env()
    assert not {'FACTORY_SECRET', 'BRAVE_API_KEY', 'HTTP_PROXY'} & env.keys()
    return env


@pytest.mark.parametrize('policy', [None, {}, False, {'web': False}, {'web': 'true'}, {'web': 1}])
def test_policy_off_and_direct_invocation(policy):
    s = session(None, policy)
    assert web.sandbox_web_tools(s) == []
    assert 'disabled' in asyncio.run(web.SandboxWeb(s).call('web_fetch', {'url': 'https://example.org'}))
    assert s.events[-1][2]['outcome'] == 'policy_denied'
    assert not web.web_enabled(policy)


@pytest.mark.parametrize('kind', ['local', 'vm'])
def test_fetch_and_search_execute_in_selected_sandbox(http_site, scrubbed_env, monkeypatch, kind):
    base, requests = http_site
    sandbox = FakeSandbox(scrubbed_env, kind)
    s = session(sandbox, {'web': True})
    monkeypatch.setattr(web, 'SEARCH_ENDPOINT', base + '/search')

    async def run():
        result = await web.SandboxWeb(s).call('web_fetch', {'url': base + '/redirect?key=initial-secret'})
        assert result == 'Fixture content'
        result = await web.SandboxWeb(s).call('web_search', {'query': 'private query', 'max_results': 2})
        assert result.count('https://example.org/a') == 2
        assert 'First hit' in result
    asyncio.run(run())
    assert len(sandbox.calls) == len(requests) == 3
    assert all('python3 -I' in cmd for cmd, _, _ in sandbox.calls)
    assert all(timeout <= 10 for _, _, timeout in sandbox.calls)
    assert [e[2]['outcome'] for e in s.events] == ['attempt', 'redirect', 'attempt', 'success', 'attempt', 'success']
    audit = json.dumps(s.events)
    for secret in ('private query', 'initial-secret', 'redirect-secret', 'Fixture content', 'controller-secret'):
        assert secret not in audit
    for _, headers in requests:
        assert not {'authorization', 'cookie', 'proxy-authorization'} & {h.lower() for h in headers}


def test_limits_denials_and_provider_failure(http_site, scrubbed_env, monkeypatch):
    base, requests = http_site
    sandbox = FakeSandbox(scrubbed_env)
    s = session(sandbox, {'web': True})

    async def run():
        for url in ('file:///etc/passwd', 'ftp://example.org', 'https://user:password@example.org/path?token=secret',
                    'http://example.org/\nsecret', 'http://example.org:bad'):
            before = len(sandbox.calls)
            assert 'Error:' in await web.SandboxWeb(s).call('web_fetch', {'url': url})
            assert len(sandbox.calls) == before
        assert 'network_error' in await web.SandboxWeb(s).call('web_fetch', {'url': base + '/disconnect'})
        result = await web.SandboxWeb(s).call('web_fetch', {'url': base + '/large'})
        assert 'response_limit' in result
        result = await web.SandboxWeb(s).call('web_fetch', {'url': base + '/output', 'max_chars': 200000})
        assert len(result.encode()) == web.MAX_OUTPUT
        result = await web.SandboxWeb(s).call('web_fetch', {'url': base + '/output', 'max_chars': 40})
        assert len(result) == 40
        before = len(sandbox.calls)
        assert 'redirect limit' in await web.SandboxWeb(s).call('web_fetch', {'url': base + '/loop'})
        assert len(sandbox.calls) - before == web.MAX_REDIRECTS + 1
        before = len(sandbox.calls)
        assert 'HTTP(S)' in await web.SandboxWeb(s).call('web_fetch', {'url': base + '/denied'})
        assert len(sandbox.calls) - before == 1
        monkeypatch.setattr(web, 'SEARCH_ENDPOINT', base + '/unavailable')
        assert 'http_error' in await web.SandboxWeb(s).call('web_search', {'query': 'test'})
        monkeypatch.setattr(web, 'SEARCH_ENDPOINT', base + '/page')
        assert 'search_unavailable' in await web.SandboxWeb(s).call('web_search', {'query': 'test'})
    asyncio.run(run())
    audit = json.dumps(s.events)
    assert 'password' not in audit and 'token' not in audit and 'provider-private-response' not in audit
    assert all(len(e[2]['url']) <= 200 for e in s.events)
    assert any(e[2]['outcome'] == 'redirect_limit' for e in s.events)
    assert any(e[2]['status'] == 503 for e in s.events)


@pytest.mark.parametrize('failure', ['exception', 'timeout', 'truncated', 'oversized', 'malformed', 'network'])
def test_sandbox_failure_has_no_controller_fallback(failure):
    class Broken:
        async def exec(self, command, *, stdin, timeout):
            if failure == 'exception':
                raise OSError('https://user:credential@example.org')
            return ExecResult(command, 0, {'oversized': 'x' * 30001, 'malformed': 'bad json',
                                          'network': '{"error":"network_error"}'}.get(failure, ''), 0,
                              timed_out=failure == 'timeout', truncated=failure == 'truncated')
    s = session(Broken(), {'web': True})
    result = asyncio.run(web.SandboxWeb(s).call('web_fetch', {'url': 'https://example.org'}))
    assert result.startswith('Error:')
    assert len(s.events) == 2
    assert 'credential' not in json.dumps(s.events) + result


def test_deadlines_and_cancellation():
    class Sleeping:
        calls = 0
        cancelled = False
        async def exec(self, command, *, stdin, timeout):
            self.calls += 1
            assert timeout <= 0.05
            try:
                await asyncio.sleep(60)
            finally:
                self.cancelled = True
    async def run():
        sb = Sleeping()
        s = session(sb, {'web': True}, command_timeout=0.05)
        assert 'timed out' in await web.SandboxWeb(s).call('web_fetch', {'url': 'https://example.org'})
        assert sb.cancelled
        s.remaining = lambda: 0
        assert 'budget' in await web.SandboxWeb(s).call('web_fetch', {'url': 'https://example.org'})
        assert sb.calls == 1
        s.remaining = lambda: 10
        s.cancelled.set()
        assert 'cancelled' in await web.SandboxWeb(s).call('web_fetch', {'url': 'https://example.org'})
        assert sb.calls == 1
        s.cancelled.clear()
        sb.cancelled = False
        task = asyncio.create_task(web.SandboxWeb(s).call('web_fetch', {'url': 'https://example.org'}))
        await asyncio.sleep(0.005)
        s.cancelled.set()
        assert 'cancelled' in await task
        assert sb.cancelled and s.events[-1][2]['outcome'] == 'cancelled'
    asyncio.run(run())


def test_enabled_tooldefs_and_policy_revocation():
    pytest.importorskip('anton.core.tools.tool_defs')
    s = session(None, {'web': True})
    tools = web.sandbox_web_tools(s)
    assert {t.name for t in tools} == {'web_search', 'web_fetch'}
    assert tools[0].input_schema['required'] == ['query']
    assert tools[1].input_schema['required'] == ['url']
    s.tool_policy['web'] = False
    for tool in tools:
        assert 'disabled' in asyncio.run(tool.handler(None, {'url': 'https://example.org', 'query': 'test'}))
