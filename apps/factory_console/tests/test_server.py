from contextlib import contextmanager
import json
import threading
import urllib.error
import urllib.request
from apps.factory_console.server import ConsoleServer
from apps.factory_console.store import Store


@contextmanager
def server(tmp_path):
    app=ConsoleServer(('127.0.0.1',0),Store(tmp_path/'console.db'),'a'*32,'b'*32,secure=False)
    thread=threading.Thread(target=app.serve_forever,daemon=True);thread.start()
    try: yield app
    finally: app.shutdown();app.server_close();thread.join()


def request(app,path,body=None,headers=None):
    host=f'127.0.0.1:{app.server_port}'
    h={'Content-Type':'application/json','Origin':'http://'+host,**(headers or {})}
    req=urllib.request.Request('http://'+host+path,data=json.dumps(body).encode() if body is not None else None,headers=h)
    try:
        with urllib.request.urlopen(req) as response:return response.status,json.load(response),response.headers
    except urllib.error.HTTPError as e:return e.code,json.load(e),e.headers


def test_preview_gate_session_and_cross_origin_mutation(tmp_path):
    with server(tmp_path) as app:
        assert request(app,'/api/state')[0]==401
        assert request(app,'/api/access',{'access_key':'wrong'})[0]==401
        code,body,headers=request(app,'/api/access',{'access_key':'a'*32})
        cookie=headers['Set-Cookie'].split(';')[0]
        assert code==200 and 'HttpOnly' in headers['Set-Cookie']
        assert request(app,'/api/state',headers={'Cookie':cookie})[1]['workspaces'][0]['id']=='factory'
        assert request(app,'/api/items',{'title':'x'},headers={'Cookie':cookie,'Origin':'https://evil.example'})[0]==403
        assert request(app,'/api/logout',{},headers={'Cookie':cookie})[0]==200
        assert request(app,'/api/state',headers={'Cookie':cookie})[0]==401


def test_bridge_requires_independent_token_and_only_accepts_projection(tmp_path):
    with server(tmp_path) as app:
        snapshot={'version':1,'observed_at':'2026-01-01T00:00:00Z','controller':{'mode':'paused'},'runs':[]}
        assert request(app,'/bridge/snapshot',snapshot)[0]==401
        assert request(app,'/bridge/snapshot',snapshot,{'Authorization':'Bearer '+'b'*32})[0]==200
        assert request(app,'/bridge/execute',{}, {'Authorization':'Bearer '+'b'*32})[0]==404
        assert request(app,'/health')[1]=={'status':'ok','service':'factory-console'}
