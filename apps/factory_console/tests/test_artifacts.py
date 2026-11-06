import base64
import hashlib
import pytest
from apps.factory_console.store import Store
from apps.factory_console.tests.test_server import server, request


def document(body=b'evidence report', name='report.txt'):
    return {'name':name,'content_base64':base64.b64encode(body).decode(),'caption':'VM capture','candidate_sha':'a'*40}


def test_attachment_is_immutable_scoped_checksum_verified_and_restart_safe(tmp_path):
    store=Store(tmp_path/'console.db')
    item=store.item({'workspace_id':'factory','title':'Actual work'})
    source=document()
    value=store.attach('item',item['id'],source)
    assert store.attach('item',item['id'],source)==value
    assert value['source']=='manual' and value['sha256']==hashlib.sha256(b'evidence report').hexdigest()
    assert Store(tmp_path/'console.db').attachment(value['id'])==(value,b'evidence report')
    assert store.state()['artifacts']==[value]
    assert store.state()['items'][0]['status']=='backlog'
    with store.connect() as db:
        db.execute('UPDATE artifacts SET body=? WHERE id=?',(b'tampered',value['id']))
    with pytest.raises(ValueError,match='integrity'):store.attachment(value['id'])


@pytest.mark.parametrize('source',[document(name='../file.txt'),document(b'<svg/>','image.svg'),
    document(b'\xff','report.txt'),document(b'x'*(1024*1024+1)),{'name':'report.txt','content_base64':'%bad'}])
def test_attachments_reject_paths_active_documents_invalid_text_and_oversize(tmp_path,source):
    store=Store(tmp_path/'console.db');item=store.item({'workspace_id':'factory','title':'Actual work'})
    with pytest.raises((ValueError,UnicodeError)):store.attach('item',item['id'],source)
    assert store.state()['artifacts']==[]


def test_attachment_requires_real_target_and_private_same_origin_access(tmp_path):
    with server(tmp_path) as app:
        item=app.store.item({'workspace_id':'factory','title':'Actual work'})
        payload={**document(),'target_kind':'item','target_id':item['id']}
        assert request(app,'/api/evidence',payload)[0]==401
        _,_,headers=request(app,'/api/access',{'access_key':'a'*32})
        cookie={'Cookie':headers['Set-Cookie'].split(';')[0]}
        assert request(app,'/api/evidence',payload,{**cookie,'Origin':'https://evil.example'})[0]==403
        code,value,_=request(app,'/api/evidence',payload,cookie)
        assert code==200
        assert request(app,'/api/evidence/'+value['id'])[0]==401
        assert request(app,'/api/evidence',{**payload,'target_id':'absent'},cookie)[0]==400


def test_omitted_runs_keep_ticket_evidence_after_feed_rotates(tmp_path):
    store=Store(tmp_path/'console.db')
    run={'id':'real-run','title':'Actual delivery','repository':'owner/repo','status':'blocked',
         'created_at':'2026-01-01T00:00:00Z','updated_at':'2026-01-01T00:00:00Z'}
    store.ingest({'version':1,'observed_at':'2026-01-01T00:00:00Z','controller':{},'runs':[run]})
    attachment=store.attach('run','real-run',document())
    store.ingest({'version':1,'observed_at':'2026-01-02T00:00:00Z','controller':{},'runs':[]})
    assert store.state()['runs'][0]['id']=='real-run'
    assert store.attachment(attachment['id'])[1]==b'evidence report'
