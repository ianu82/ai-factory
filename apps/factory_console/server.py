"""Small isolated console service; Factory execution remains on its controller."""
from http import HTTPStatus
from http.cookies import SimpleCookie
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import mimetypes
import os
from pathlib import Path
import secrets
import sys
import time
from urllib.parse import urlsplit

if __package__ in {None, ''}:
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
    from apps.factory_console.store import Store
else:
    from .store import Store

STATIC = Path(__file__).with_name('static').resolve()


class ConsoleServer(ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = True

    def __init__(self, address, store, access_key, bridge_token, *, secure=True):
        if len(access_key)<20 or len(bridge_token)<20:
            raise ValueError('Configure independent preview and bridge credentials of at least 20 characters')
        self.store, self.access_key, self.bridge_token = store, access_key, bridge_token
        self.secure, self.attempts = secure, {}
        super().__init__(address, Handler)


class Handler(BaseHTTPRequestHandler):
    server_version = 'FactoryConsole'

    def log_message(self, *_):
        pass  # No query strings, request bodies or credentials in server logs.

    def send(self, status, value, content_type='application/json; charset=utf-8', cookie=None):
        body = value if isinstance(value, bytes) else json.dumps(value, allow_nan=False).encode()
        self.send_response(status)
        self.send_header('Content-Type', content_type)
        self.send_header('Content-Length',str(len(body)))
        self.send_header('Cache-Control','no-store' if not self.path.startswith('/assets/') else 'public,max-age=3600')
        self.send_header('X-Content-Type-Options','nosniff')
        self.send_header('Referrer-Policy','no-referrer')
        self.send_header('Content-Security-Policy',"default-src 'self'; script-src 'self'; style-src 'self'; img-src 'self'; font-src 'self'; connect-src 'self'; frame-ancestors 'none'; base-uri 'self'; form-action 'self'")
        if cookie:
            self.send_header('Set-Cookie',cookie)
        self.end_headers()
        self.wfile.write(body)

    def body(self):
        raw = self.headers.get('Content-Length','0')
        if not raw.isdigit() or not 0 < int(raw) <= 2*1024*1024:
            raise ValueError('Invalid request size')
        if self.headers.get('Content-Type','').split(';')[0]!='application/json':
            raise ValueError('JSON required')
        value = json.loads(self.rfile.read(int(raw)))
        if not isinstance(value,dict):
            raise ValueError('A JSON object is required')
        return value

    def session_token(self):
        try:
            cookie = SimpleCookie(self.headers.get('Cookie',''))
            return cookie['factory_session'].value if 'factory_session' in cookie else ''
        except Exception:
            return ''

    def authorized(self):
        token = self.session_token()
        return bool(token and len(token)<=100 and self.server.store.session(token))

    def bridge_authorized(self):
        supplied = self.headers.get('Authorization','')
        return secrets.compare_digest(supplied, 'Bearer '+self.server.bridge_token)

    def same_origin(self):
        origin = self.headers.get('Origin','')
        parsed = urlsplit(origin)
        return parsed.scheme in {'http','https'} and parsed.netloc == self.headers.get('Host','')

    def do_GET(self):
        path = urlsplit(self.path).path
        if path == '/health':
            return self.send(200,{'status':'ok','service':'factory-console'})
        if path == '/api/session':
            return self.send(200,{'authenticated':self.authorized()})
        if path == '/api/state':
            return self.send(200,self.server.store.state()) if self.authorized() else self.send(401,{'error':'Preview access required'})
        target = STATIC/'index.html' if path == '/' else STATIC/path.removeprefix('/assets/') if path.startswith('/assets/') else None
        if not target or target.is_symlink() or not target.resolve().is_relative_to(STATIC) or not target.is_file():
            return self.send(404,{'error':'Not found'})
        content_type = mimetypes.guess_type(target.name)[0] or 'application/octet-stream'
        self.send(200,target.read_bytes(),content_type)

    def do_POST(self):
        path = urlsplit(self.path).path
        try:
            if path.startswith('/bridge/'):
                if not self.bridge_authorized():
                    return self.send(401,{'error':'Bridge authorization required'})
                body = self.body()
                if path == '/bridge/snapshot':
                    return self.send(200,self.server.store.ingest(body))
                if path == '/bridge/next-message':
                    return self.send(200,{'request':self.server.store.claim_message()})
                if path.startswith('/bridge/replies/'):
                    return self.send(200,self.server.store.reply(path.split('/')[-1],body))
                return self.send(404,{'error':'Not found'})
            if not self.same_origin():
                return self.send(403,{'error':'Same-origin request required'})
            body = self.body()
            if path == '/api/access':
                return self.access(body)
            if not self.authorized():
                return self.send(401,{'error':'Preview access required'})
            if path == '/api/logout':
                with self.server.store.connect() as db:
                    import hashlib
                    db.execute('DELETE FROM sessions WHERE token_hash=?',(hashlib.sha256(self.session_token().encode()).hexdigest(),))
                return self.send(200,{'ok':True},cookie=self.cookie('',0))
            if path == '/api/workspaces' or path.startswith('/api/workspaces/'):
                key = path.split('/')[-1] if path!='/api/workspaces' else None
                return self.send(200,self.server.store.workspace(body,key))
            if path == '/api/items' or path.startswith('/api/items/'):
                key = path.split('/')[-1] if path!='/api/items' else None
                return self.send(200,self.server.store.item(body,key))
            if path == '/api/messages':
                return self.send(200,self.server.store.message(body.get('workspace_id'),body.get('content','')))
            self.send(404,{'error':'Not found'})
        except (ValueError, TypeError, KeyError, AttributeError):
            self.send(400,{'error':'The request is invalid. Check the fields and try again.'})
        except Exception:
            self.send(503,{'error':'Console storage is unavailable. Your Factory run has not been changed.'})

    def cookie(self, token, age):
        return f'factory_session={token}; Path=/; Max-Age={age}; HttpOnly; SameSite=Strict'+('; Secure' if self.server.secure else '')

    def access(self, body):
        ip, current = self.client_address[0], time.monotonic()
        recent = [stamp for stamp in self.server.attempts.get(ip,[]) if stamp>current-300]
        if len(recent)>=10:
            return self.send(429,{'error':'Too many attempts. Try again in five minutes.'})
        recent.append(current)
        if len(self.server.attempts)>1000:
            self.server.attempts.clear()
        self.server.attempts[ip]=recent
        key = body.get('access_key')
        if not isinstance(key,str) or not secrets.compare_digest(key,self.server.access_key):
            return self.send(401,{'error':'That preview code is not correct.'})
        token = secrets.token_urlsafe(32)
        self.server.store.session(token,time.time()+7*86400)
        self.send(200,{'authenticated':True},cookie=self.cookie(token,7*86400))


def main():
    store = Store(os.environ.get('FACTORY_CONSOLE_DB','/data/console.db'))
    server = ConsoleServer(('0.0.0.0',int(os.environ.get('PORT','8080'))),store,
        os.environ.get('FACTORY_CONSOLE_ACCESS_KEY',''),os.environ.get('FACTORY_CONSOLE_BRIDGE_TOKEN',''),
        secure=os.environ.get('FACTORY_CONSOLE_DEV')!='1')
    print('Factory console listening; data and execution credentials remain private',flush=True)
    server.serve_forever()


if __name__=='__main__':
    main()
