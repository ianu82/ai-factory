from apps.factory_console.model import attention_sort, projection, repositories
import pytest


def test_global_priority_then_original_attention_age():
    items = [{'id':'old-review','priority':'medium','attention_at':'2026-01-01'},
             {'id':'new-bug','priority':'urgent','attention_at':'2026-03-01'},
             {'id':'old-bug','priority':'urgent','attention_at':'2026-02-01'}]
    assert [i['id'] for i in attention_sort(items)] == ['old-bug','new-bug','old-review']
    assert [i['id'] for i in attention_sort(items, 'oldest')] == ['old-review','old-bug','new-bug']
    assert [i['id'] for i in attention_sort(items, 'newest')] == ['new-bug','old-bug','old-review']


def test_multiple_repositories_are_explicit_and_unique():
    assert len(repositories(['mindsdb/cowork','mindsdb/cowork-server'])) == 2
    for value in ([], ['../repo'], ['owner/repo','OWNER/REPO']):
        with pytest.raises(ValueError): repositories(value)


def test_projection_ignores_credentials_paths_and_provider_payloads():
    source = {'version':1,'observed_at':'2026-01-01T00:00:00Z','controller':{
        'mode':'paused','api_key':'private','profile':{'secret':'private'}},'runs':[{
        'id':'run-1','title':'Fix sk-abcdefghijklmnop','repository':'owner/repo','status':'blocked',
        'created_at':'2026-01-01T00:00:00Z','updated_at':'2026-01-01T00:00:00Z',
        'command':'private command','stdout':'private output','summary':'Bearer very-private-token'}]}
    result = projection(source)
    assert result['runs'][0]['title'] == 'Fix [redacted]'
    assert result['runs'][0]['summary'] == '[redacted]'
    assert 'private' not in str(result)
    assert result['runs'][0]['cost_usd'] is None


def test_evidence_receipts_are_bounded_and_cannot_leak_raw_data():
    from apps.factory_console.evidence import receipt, environment, review
    assert receipt({'exit_code':True,'duration_seconds':float('nan'),'sequence':'1','commit_sha':'secret'}) == {
        'exit_code':None,'duration_seconds':None,'sequence':None,'commit_sha':None,'record_sha256':None}
    result=receipt({'exit_code':0,'duration_seconds':1.2,'commit_sha':'a'*40,'record_sha256':'b'*64,'sequence':4,'stdout':'secret'})
    assert result['exit_code']==0 and result['sequence']==4 and 'secret' not in str(result)
    assert environment({'image':'https://secret@example.com','platform':'password','runtime_sha':'secret'})['image'] is None
    result=review({'status':'passed','at':'2026-01-01T00:00:00Z','candidate_sha':'a'*40,'accepted':True,
                   'criteria':[{'id':'criterion','status':'passed','evidence':'private prose'}]})
    assert result['criteria']==[{'id':'criterion','status':'passed'}] and 'private' not in str(result)


@pytest.mark.parametrize('fields,expected',[
    ({},'passed'), ({'exit_code':0},'passed'), ({'exit_code':False},'unverified'),
    ({'exit_code':1},'failed'), ({'exit_code':'0'},'unverified'),
    ({'duration_seconds':-1},'unverified'), ({'commit_sha':'bad'},'unverified'),
    ({'record_sha256':None},'unverified')])
def test_explicit_check_receipts_cannot_retain_contradictory_success(fields,expected):
    from apps.factory_console.evidence import check
    assert check({'name':'check','status':'passed',**fields})['status']==expected


@pytest.mark.parametrize('change',[
    {'candidate_sha':None},{'record_sha256':'bad'},{'criteria':[]},
    {'criteria':[{'id':'one','status':'passed'},{'id':'one','status':'passed'}]},
    {'criteria':[{'id':'one','status':'unverified'}]}])
def test_model_never_accepts_unbound_or_invalid_review_receipts(change):
    from apps.factory_console.evidence import review
    value={'status':'passed','at':'2026-01-01T00:00:00Z','candidate_sha':'a'*40,
           'record_sha256':'b'*64,'accepted':True,'criteria':[{'id':'one','status':'passed'}]}
    assert review({**value,**change})['accepted'] is False


def test_legacy_projection_and_explicit_receipts_are_idempotent():
    from apps.factory_console.evidence import check
    legacy={'name':'contracts','status':'passed','at':'2026-01-01T00:00:00Z'}
    assert 'exit_code' not in check(legacy)
    assert check(check(legacy))==check(legacy)
    typed={**legacy,'commit_sha':'a'*40,'record_sha256':'b'*64,'exit_code':0,'duration_seconds':1.5,'sequence':4}
    assert check(check(typed))==check(typed)
