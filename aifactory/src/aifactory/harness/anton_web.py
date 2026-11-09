"""Policy-gated, session-bound web tools. All HTTP runs in sandbox.exec.

T6 can pass a workspace-derived tool_policy into HarnessSession. Only the literal
boolean {"web": True} enables these tools; SDK web flags must remain false.
"""
from __future__ import annotations

import asyncio
import base64
import json
import shlex
import time
from pathlib import Path
from urllib.parse import urlencode, urljoin, urlsplit, urlunsplit

MAX_OUTPUT = 16000
MAX_REDIRECTS = 5
SEARCH_ENDPOINT = 'https://html.duckduckgo.com/html/'
_WORKER = Path(__file__).with_name('_anton_web_worker.py').read_text()
_COMMAND = 'python3 -I -c ' + shlex.quote(_WORKER)


def web_enabled(policy) -> bool:
    """Fail closed, reusable policy interpretation (not Python truthiness)."""
    return isinstance(policy, dict) and policy.get('web') is True


def sanitized_url(url) -> str:
    """Audit origin only: paths, queries, fragments and userinfo can all be secrets."""
    if not isinstance(url, str):
        return '[invalid URL]'
    try:
        p = urlsplit(url)
        if p.scheme not in ('http', 'https') or not p.hostname:
            return '[invalid URL]'
        host = p.hostname
        if ':' in host:
            host = f'[{host}]'
        origin = f'{p.scheme}://{host}' + (f':{p.port}' if p.port else '')
        return ''.join(c for c in origin if c.isprintable())[:200]
    except (TypeError, ValueError):
        return '[invalid URL]'


def _url(url):
    if not isinstance(url, str) or len(url) > 4096 or any(ord(c) <= 32 or ord(c) == 127 for c in url):
        raise ValueError()
    p = urlsplit(url)
    if p.scheme not in ('http', 'https') or not p.hostname or p.username is not None or p.password is not None:
        raise ValueError()
    p.port  # Validate before giving it to the worker.
    return urlunsplit((p.scheme, p.netloc, p.path, p.query, ''))


class SandboxWeb:
    def __init__(self, session):
        self.session = session

    def audit(self, tool, url, outcome, status=None):
        tool = tool if tool in ('web_search', 'web_fetch') else 'web'
        self.session.on_progress('web_request', f'{tool}: {outcome}', {
            'tool': tool, 'url': sanitized_url(url), 'outcome': outcome,
            'status': status if type(status) is int and 100 <= status <= 599 else None,
        })

    async def call(self, tool, arguments):
        url = arguments.get('url', '') if isinstance(arguments, dict) else ''
        if not web_enabled(getattr(self.session, 'tool_policy', None)):
            self.audit(tool, url, 'policy_denied')
            return 'Error: web tools are disabled by policy.'
        try:
            if not isinstance(arguments, dict) or tool not in ('web_search', 'web_fetch'):
                raise ValueError()
            max_results = max(1, min(20, int(arguments.get('max_results', 5))))
            max_chars = max(1, min(MAX_OUTPUT, int(arguments.get('max_chars', MAX_OUTPUT))))
            if tool == 'web_search':
                query = arguments.get('query')
                if not isinstance(query, str) or not query.strip() or len(query) > 1000:
                    raise ValueError()
                url = SEARCH_ENDPOINT + '?' + urlencode({'q': query})
        except (TypeError, ValueError, OverflowError):
            self.audit(tool, url, 'invalid_arguments')
            return 'Error: invalid web tool arguments.'

        deadline = time.monotonic() + min(30.0, self.session.command_timeout, self.session.remaining())
        for hop in range(MAX_REDIRECTS + 1):
            try:
                url = _url(url)
            except (TypeError, ValueError):
                self.audit(tool, url, 'url_denied')
                return 'Error: only absolute HTTP(S) URLs without credentials are allowed.'
            timeout = min(deadline - time.monotonic(), self.session.remaining())
            if self.session.cancelled.is_set() or timeout <= 0:
                self.audit(tool, url, 'budget_denied')
                return 'Error: web request cancelled or time budget exhausted.'
            self.audit(tool, url, 'attempt')
            outcome, status = 'execution_error', None
            execution = cancellation = None
            try:
                payload = json.dumps({'url': url, 'tool': tool, 'timeout': timeout,
                                      'max_results': max_results, 'max_chars': max_chars}).encode()
                # No env overrides: LocalSandbox scrubs the controller environment;
                # VMSandbox supplies the VM's own environment and egress rules.
                execution = asyncio.create_task(self.session.sandbox.exec(_COMMAND, stdin=payload, timeout=timeout))
                cancellation = asyncio.create_task(self.session.cancelled.wait())
                done, _ = await asyncio.wait((execution, cancellation), timeout=timeout,
                                             return_when=asyncio.FIRST_COMPLETED)
                if cancellation in done:
                    outcome = 'cancelled'
                    return 'Error: web request cancelled.'
                if execution not in done:
                    outcome = 'timeout'
                    return 'Error: web request timed out in sandbox.'
                result = execution.result()
                if result.timed_out:
                    outcome = 'timeout'
                    return 'Error: web request timed out in sandbox.'
                if not result.ok or result.truncated or len(result.output.encode()) > 28000:
                    outcome = 'output_limit' if result.truncated or len(result.output.encode()) > 28000 else 'execution_error'
                    return 'Error: sandbox web worker failed or exceeded output limits.'
                data = json.loads(result.output)
                status = data.get('status')
                if status in (301, 302, 303, 307, 308):
                    outcome = 'redirect' if hop < MAX_REDIRECTS else 'redirect_limit'
                    if hop == MAX_REDIRECTS:
                        return 'Error: web redirect limit exceeded.'
                    url_next = urljoin(url, data.get('location', ''))
                    if not data.get('location'):
                        outcome = 'invalid_redirect'
                        return 'Error: web redirect has no usable location.'
                    continue_redirect = url_next
                else:
                    error = data.get('error')
                    if error or type(status) is not int or not 200 <= status < 300:
                        outcome = error if error in ('network_error', 'response_limit', 'search_unavailable', 'http_error') else 'worker_error'
                        return f'Error: {tool} unavailable ({outcome}); retry or use another source.'
                    content = base64.b64decode(data['content'], validate=True)
                    if len(content) > MAX_OUTPUT:
                        outcome = 'output_limit'
                        return 'Error: web output limit exceeded.'
                    outcome = 'success'
                    return content.decode('utf-8', 'replace')[:max_chars]
            except asyncio.CancelledError:
                outcome = 'cancelled'
                raise
            except Exception:
                return 'Error: sandbox web execution unavailable; retry or use another source.'
            finally:
                for task in (execution, cancellation):
                    if task is not None and not task.done():
                        task.cancel()
                await asyncio.gather(*(t for t in (execution, cancellation) if t is not None), return_exceptions=True)
                self.audit(tool, url, outcome, status)
            url = continue_redirect
        return 'Error: web redirect limit exceeded.'


def sandbox_web_tools(session):
    """No SDK imports or exposed tools unless the session explicitly opts in."""
    if not web_enabled(getattr(session, 'tool_policy', None)):
        return []
    from anton.core.tools.tool_defs import ToolDef

    web = SandboxWeb(session)

    def handler(name):
        async def call(_chat, arguments):
            return await web.call(name, arguments)
        return call

    return [
        ToolDef('web_search', 'Search the web using a free no-key endpoint in the sandbox. Returns ranked titles and URLs; availability may vary.',
                {'type': 'object', 'properties': {'query': {'type': 'string'}, 'max_results': {'type': 'integer'}},
                 'required': ['query']}, handler('web_search')),
        ToolDef('web_fetch', 'Fetch HTTP(S) text in the sandbox, with bounded response size and redirects.',
                {'type': 'object', 'properties': {'url': {'type': 'string'}, 'max_chars': {'type': 'integer'}},
                 'required': ['url']}, handler('web_fetch')),
    ]
