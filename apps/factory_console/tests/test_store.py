from datetime import datetime, timezone, timedelta
import pytest
from apps.factory_console.store import Store


def test_workspace_and_draft_survive_restart_without_dispatch(tmp_path):
    path = tmp_path/'console.db'
    store = Store(path)
    workspace = store.workspace({'name':'Cowork','repositories':['mindsdb/cowork','mindsdb/cowork-server']})
    item = store.item({'workspace_id':workspace['id'],'title':'Investigate onboarding','priority':'urgent','needs_attention':True})
    restarted = Store(path).state()
    assert len(next(w for w in restarted['workspaces'] if w['id']==workspace['id'])['repositories'])==2
    assert restarted['items'][0]['status']=='backlog'
    updated = store.item({**item,'priority':'medium','description':'More detail'},item['id'])
    assert updated['attention_at']==item['attention_at']
    assert updated['created_at']==item['created_at']
    assert restarted['connection']['connected'] is False


def test_canonical_run_outcome_cannot_be_changed_from_ui(tmp_path):
    store = Store(tmp_path/'console.db')
    with store.connect() as db:
        store.put(db,'item','attention',{'id':'attention','workspace_id':'factory','title':'Failure',
            'status':'blocked','priority':'high','mode':'engineering','kind':'failure','run_id':'run',
            'created_at':'2026-01-01T00:00:00+00:00','attention_at':'2026-01-01T00:00:00+00:00'})
    with pytest.raises(ValueError,match='outcome'):
        store.item({'workspace_id':'factory','title':'Failure','status':'complete'},'attention')
    with pytest.raises(ValueError,match='evidence'):
        store.item({'workspace_id':'factory','title':'Fake progress','status':'building'})


def test_bridge_cache_rejects_old_observations_and_marks_stale(tmp_path):
    store = Store(tmp_path/'console.db')
    snapshot = {'version':1,'observed_at':'2026-01-01T00:00:00Z','controller':{'mode':'paused'},'runs':[]}
    store.ingest(snapshot)
    with pytest.raises(ValueError,match='stale'):
        store.ingest(snapshot)
    assert store.state()['connection']['stale'] is True
    assert store.state()['connection']['connected'] is False


def test_chat_claim_is_exclusive_and_history_is_workspace_scoped(tmp_path):
    store = Store(tmp_path/'console.db')
    store.ingest({'version':1,'observed_at':datetime.now(timezone.utc).isoformat(),
        'controller':{'mode':'paused','chat_available':True},'runs':[]})
    other = store.workspace({'name':'Other','repositories':['owner/other']})
    message = store.message('factory','Summarise the current blockers')
    store.message(other['id'],'Private other context')
    claimed = store.claim_message()
    assert claimed['message']['id']==message['id']
    assert all(m['workspace_id']=='factory' for m in claimed['history'])
    assert store.claim_message()['message']['workspace_id']==other['id']
    assert store.claim_message() is None
    reply = store.reply(message['id'],{'content':'The controller is deliberately paused.','model':'gpt-6.1-terra'})
    assert reply['reply_to']==message['id']
    with pytest.raises(ValueError,match='already'):
        store.reply(message['id'],{'content':'Duplicate'})


def test_session_is_hashed_and_expires(tmp_path):
    store = Store(tmp_path/'console.db')
    store.session('preview-secret', datetime.now(timezone.utc).timestamp()+10)
    assert store.session('preview-secret') and not store.session('wrong')
    with store.connect() as db:
        assert db.execute('SELECT token_hash FROM sessions').fetchone()[0]!='preview-secret'
