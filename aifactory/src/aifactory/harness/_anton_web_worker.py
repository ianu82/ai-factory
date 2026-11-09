"""Stdlib-only worker, passed to sandbox Python with -I (never imported there).

One HTTP hop only. Redirect decisions and auditing belong to the session adapter.
No cookies, netrc, SDK, credentials, automatic redirects, or controller environment.
"""
import base64
import json
import sys
import time
from html.parser import HTMLParser
from urllib.error import HTTPError
from urllib.parse import parse_qs, urljoin, urlsplit
from urllib.request import HTTPRedirectHandler, Request, build_opener

MAX_BYTES = 262144
MAX_TEXT_BYTES = 16000


class NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


class Text(HTMLParser):
    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.parts = []
        self.hidden = 0

    def handle_starttag(self, tag, attrs):
        if tag in ('script', 'style'):
            self.hidden += 1

    def handle_endtag(self, tag):
        if tag in ('script', 'style'):
            self.hidden = max(0, self.hidden - 1)

    def handle_data(self, data):
        if not self.hidden:
            self.parts.append(data)


class Search(HTMLParser):
    def __init__(self, limit):
        super().__init__(convert_charrefs=True)
        self.limit = limit
        self.results = []
        self.current = None

    def handle_starttag(self, tag, attrs):
        attrs = dict(attrs)
        if tag == 'a' and 'result__a' in attrs.get('class', '').split() and len(self.results) < self.limit:
            href = urljoin('https://html.duckduckgo.com/', attrs.get('href', ''))
            parsed = urlsplit(href)
            if parsed.hostname in ('duckduckgo.com', 'html.duckduckgo.com'):
                href = parse_qs(parsed.query).get('uddg', [''])[0]
            p = urlsplit(href)
            if p.scheme in ('http', 'https') and p.hostname and not p.username and not p.password:
                self.current = [href[:2048], '']

    def handle_data(self, data):
        if self.current is not None:
            self.current[1] = (self.current[1] + data)[:300]

    def handle_endtag(self, tag):
        if tag == 'a' and self.current is not None:
            self.results.append(self.current)
            self.current = None


def main():
    args = json.loads(sys.stdin.read(16384))
    opener = build_opener(NoRedirect())  # Inherits sandbox proxy/egress settings only.
    request = Request(args['url'], headers={'User-Agent': 'Factory-Anton/1.0', 'Accept-Encoding': 'identity'})
    started = time.monotonic()
    try:
        try:
            response = opener.open(request, timeout=args['timeout'])
        except HTTPError as exc:
            response = exc
        with response:
            status = response.code
            result = {'status': status}
            if status in (301, 302, 303, 307, 308):
                location = response.headers.get('Location', '')
                result['location'] = location if len(location) <= 4096 else ''
            elif 200 <= status < 300:
                chunks = []
                size = 0
                while size <= MAX_BYTES:
                    if time.monotonic() - started >= args['timeout']:
                        raise TimeoutError()
                    chunk = response.read(min(8192, MAX_BYTES + 1 - size))
                    if not chunk:
                        break
                    chunks.append(chunk)
                    size += len(chunk)
                if size > MAX_BYTES:
                    result['error'] = 'response_limit'
                else:
                    raw = b''.join(chunks)
                    text = raw.decode('utf-8', 'replace')
                    if args['tool'] == 'web_search':
                        parser = Search(args['max_results'])
                        parser.feed(text)
                        if not parser.results:
                            result['error'] = 'search_unavailable'
                        else:
                            text = '\n\n'.join(f'{i}. {title.strip()}\n{url}'
                                               for i, (url, title) in enumerate(parser.results, 1))
                    elif 'html' in response.headers.get('Content-Type', '').lower():
                        parser = Text()
                        parser.feed(text)
                        text = ' '.join(' '.join(parser.parts).split())
                    text = text[:args['max_chars']].encode('utf-8')[:MAX_TEXT_BYTES]
                    text = text.decode('utf-8', 'ignore').encode('utf-8')
                    result['content'] = base64.b64encode(text).decode('ascii')
            else:
                result['error'] = 'http_error'
        print(json.dumps(result))
    except Exception:
        # Exception strings may contain URL credentials, proxy auth or remote bodies.
        print(json.dumps({'error': 'network_error'}))


if __name__ == '__main__':
    main()
