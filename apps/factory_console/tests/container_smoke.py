"""Qualify the real console container without provider credentials or inference."""
import json
import subprocess
import tempfile
import time
from pathlib import Path
from datetime import datetime,timezone
from urllib.error import HTTPError
from urllib.request import Request,urlopen


def run(*args):
    return subprocess.check_output(args,text=True).strip()


def main():
    container=run('docker','run','-d','--read-only','--tmpfs','/data:rw,nosuid,noexec,size=32m',
        '--tmpfs','/tmp:rw,nosuid,noexec,size=16m','--memory','256m','--cpus','1','--pids-limit','64',
        '-p','127.0.0.1::8080','factory-console-ci')
    try:
        with tempfile.TemporaryDirectory(prefix='console-ci-') as directory:
            config=Path(directory)/'credentials.json'
            config.write_text(json.dumps({'FACTORY_CONSOLE_ACCESS_KEY':'ci-preview-only-never-production',
                'FACTORY_CONSOLE_BRIDGE_TOKEN':'ci-bridge-only-never-production'}))
            config.chmod(0o600)
            run('docker','cp',str(config),container+':/data/credentials.json')
        port=run('docker','port',container,'8080/tcp').split(':')[-1]
        origin='http://127.0.0.1:'+port
        def request(path,body=None,headers=None):
            req=Request(origin+path,data=json.dumps(body).encode() if body is not None else None,
                headers={'Content-Type':'application/json','Origin':origin,**(headers or {})})
            try:
                with urlopen(req,timeout=3) as response:return response.status,response.read(),response.headers
            except HTTPError as error:return error.code,error.read(),error.headers
        for attempt in range(30):
            try:
                if request('/health')[0]==200:break
            except OSError:pass
            time.sleep(1)
        else:raise AssertionError('Console did not become healthy')
        identity=run('docker','exec',container,'python','-c',"from pathlib import Path;print(next(l for l in Path('/proc/1/status').read_text().splitlines() if l.startswith('Uid:')))")
        assert identity.split()[1:]==['10001']*4,identity
        assert request('/api/state')[0]==401
        assert request('/assets/../../../server.py')[0]==404
        assert request('/assets/app.js')[0]==200
        code,body,headers=request('/api/access',{'access_key':'ci-preview-only-never-production'})
        assert code==200 and all(flag in headers['Set-Cookie'] for flag in ('HttpOnly','Secure','SameSite=Strict'))
        cookie=headers['Set-Cookie'].split(';')[0]
        state=json.loads(request('/api/state',headers={'Cookie':cookie})[1])
        assert len(state['workspaces'][0]['repositories'])==2
        snapshot={'version':1,'observed_at':datetime.now(timezone.utc).isoformat(),'controller':{'mode':'paused'},'runs':[]}
        assert request('/bridge/snapshot',snapshot)[0]==401
        assert request('/bridge/snapshot',snapshot,{'Authorization':'Bearer ci-bridge-only-never-production'})[0]==200
        assert json.loads(request('/api/state',headers={'Cookie':cookie})[1])['connection']['connected'] is True
        assert request('/api/items',{'title':'Rejected'}, {'Cookie':cookie,'Origin':'https://another.example'})[0]==403
        print('Actual console container passed: private configuration upload, nonroot, read-only root, private state, scoped ingestion, secure cookies and assets')
    finally:
        subprocess.run(['docker','rm','-f',container],check=True,stdout=subprocess.DEVNULL)


if __name__=='__main__':main()
