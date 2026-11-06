import json
import hashlib
from pathlib import Path
from types import SimpleNamespace
import pytest
from apps.factory_console.bridge import Publisher, publish


def test_projection_reads_journal_without_mutations_or_payload_disclosure(tmp_path):
    store=tmp_path/'evidence';run=store/'delivery'/'run';run.mkdir(parents=True)
    (run/'manifest.json').write_text(json.dumps({'created_at':'2026-01-01T00:00:00Z',
        'task':{'title':'A real fix','goal':'private prompt'},'profile':{'repository':'owner/repo'},'credentials':'private'}))
    events=[{'kind':'attempt_started','at':'2026-01-01T00:00:01Z','data':{}},
        {'kind':'verification_check','at':'2026-01-01T00:00:02Z','data':{'name':'unit','exit_code':1,'stdout':'private output'}},
        {'kind':'run_finished','at':'2026-01-01T00:00:03Z','data':{'status':'blocked','reason':'private exception'}}]
    for i,event in enumerate(events): (run/f'event-{i:06d}.json').write_text(json.dumps(event))
    class Journal:
        def __init__(self,s,r,*,read_only):assert read_only is True and s==store and r=='run'
        def events(self):return events
        def summary(self):return {'total_tokens':123,'usage_complete':False,'cost_usd':None}
    before={p.name:hashlib.sha256(p.read_bytes()).hexdigest() for p in run.iterdir()}
    publisher=Publisher([store],tmp_path/'status.json',journal_factory=Journal)
    snapshot=publisher.snapshot()
    assert snapshot['controller']['mode']=='unknown'
    assert snapshot['controller']['chat_available'] is False
    assert snapshot['runs'][0]['status']=='blocked'
    assert snapshot['runs'][0]['stage']=='verifying'
    assert snapshot['runs'][0]['checks'][0]['status']=='failed'
    assert snapshot['runs'][0]['cost_usd'] is None
    assert 'private' not in str(snapshot)
    assert before=={p.name:hashlib.sha256(p.read_bytes()).hexdigest() for p in run.iterdir()}
    assert publisher.snapshot()['runs']==snapshot['runs']


def test_publisher_is_https_projection_only_and_never_returns_provider_errors():
    calls=[]
    class Response:
        def __enter__(self):return self
        def __exit__(self,*a):pass
        def read(self,n):return b'{"accepted":true}'
    def opener(request,timeout):
        calls.append(request.full_url)
        assert request.headers['Authorization']=='Bearer ui-only-token'
        assert timeout==10
        return Response()
    publish('https://console.example','ui-only-token',{'version':1},opener=opener)
    assert calls==['https://console.example/bridge/snapshot']
    for endpoint in ['http://console.example','https://user:password@console.example','https://console.example?token=x']:
        with pytest.raises(ValueError):publish(endpoint,'ui-only-token',{},opener=opener)


@pytest.mark.parametrize('code',[302,303])
def test_redirect_cannot_forward_token_or_projection_to_another_https_origin(code):
    from email.message import Message
    from io import BytesIO
    from urllib.error import HTTPError
    from urllib.request import HTTPSHandler, build_opener
    from urllib.response import addinfourl
    from apps.factory_console.bridge import NoRedirect
    calls=[]
    class Network(HTTPSHandler):
        def https_open(self,request):
            calls.append((request.full_url,request.headers.get('Authorization'),request.data))
            headers=Message();headers['Location']='https://another.example/steal'
            response=addinfourl(BytesIO(b''),headers,request.full_url,code)
            response.msg='Redirect'
            return response
    with pytest.raises(HTTPError):
        publish('https://console.example','private-ui-token',{'private':'projection'},
                opener=build_opener(NoRedirect(),Network()).open)
    assert len(calls)==1 and calls[0][0]=='https://console.example/bridge/snapshot'
    assert not any('another.example' in url for url,_,_ in calls)


@pytest.mark.parametrize('exit_code,expected',[(0,'passed'),(1,'failed'),(False,'unverified'),(None,'unverified'),('0','unverified')])
def test_check_outcomes_require_exact_integer_evidence(tmp_path,exit_code,expected):
    from auto_mindsdb_factory.run_journal import RunJournal
    from datetime import datetime,timezone
    store=tmp_path/'evidence'
    journal=RunJournal(store,'checks')
    journal.create({'task':{'title':'Check provenance'},'profile':{'repository':'owner/repo'}})
    journal.append('verification_check',name='unit',exit_code=exit_code,stdout='not projected')
    publisher=Publisher([store],tmp_path/'absent-status')
    before={p.name:hashlib.sha256(p.read_bytes()).hexdigest() for p in journal.root.iterdir()}
    run=publisher.snapshot()['runs'][0]
    assert run['checks'][0]['status']==expected
    assert before=={p.name:hashlib.sha256(p.read_bytes()).hexdigest() for p in journal.root.iterdir()}
