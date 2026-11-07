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
