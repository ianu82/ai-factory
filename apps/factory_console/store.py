"""Console-owned state, separate from authoritative Factory delivery evidence."""
from contextlib import contextmanager
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import secrets
import sqlite3

from .model import MODES, PRIORITIES, attention_sort, identifier, now, projection, repositories, text


class Store:
    def __init__(self, path):
        self.path = Path(path)
        self.path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        db = sqlite3.connect(self.path, timeout=10)
        try:
            db.execute('PRAGMA journal_mode=WAL')
        finally:
            db.close()
        with self.connect() as db:
            db.execute('CREATE TABLE IF NOT EXISTS records(kind TEXT,id TEXT,document TEXT,PRIMARY KEY(kind,id))')
            db.execute('CREATE TABLE IF NOT EXISTS sessions(token_hash TEXT PRIMARY KEY,expires REAL)')
            if not self.get(db, 'meta', 'created'):
                self.put(db, 'meta', 'created', {'at': now()})
                self.put(db, 'workspace', 'factory', {'id':'factory','name':'AI Factory',
                    'description':'Factory core and real coding certification', 'mode':'engineering',
                    'repositories':['ianu82/ai-factory','ianu82/calorie-tracker'], 'created_at':now()})

    @contextmanager
    def connect(self):
        db = sqlite3.connect(self.path, timeout=10)
        try:
            db.execute('BEGIN IMMEDIATE')
            yield db
            db.commit()
        except BaseException:
            db.rollback()
            raise
        finally:
            db.close()

    @staticmethod
    def put(db, kind, key, value):
        db.execute('INSERT INTO records VALUES(?,?,?) ON CONFLICT(kind,id) DO UPDATE SET document=excluded.document',
                   (kind, key, json.dumps(value, allow_nan=False)))

    @staticmethod
    def get(db, kind, key):
        row = db.execute('SELECT document FROM records WHERE kind=? AND id=?', (kind,key)).fetchone()
        return json.loads(row[0]) if row else None

    @staticmethod
    def all(db, kind):
        return [json.loads(r[0]) for r in db.execute('SELECT document FROM records WHERE kind=?', (kind,))]

    def state(self):
        with self.connect() as db:
            snapshot = self.get(db, 'snapshot', 'factory')
            workspaces, items = self.all(db, 'workspace'), self.all(db, 'item')
            run_map = {r['id']:r for r in snapshot['runs']} if snapshot else {}
            for item in items:
                if item.get('run_id') in run_map:
                    item['run'] = run_map[item['run_id']]
            age = max(0, datetime.now(timezone.utc).timestamp() - datetime.fromisoformat(snapshot['observed_at']).timestamp()) if snapshot else None
            return {'workspaces':workspaces, 'items':items, 'runs':list(run_map.values()),
                'connection':{'connected':snapshot is not None and age < 90,
                    'observed_at':snapshot['observed_at'] if snapshot else None,
                    'stale':age is not None and age >= 90,
                    'controller':snapshot['controller'] if snapshot else {'mode':'unknown','chat_available':False}},
                'messages':sorted(self.all(db, 'message'), key=lambda m:(m['created_at'],m['id']))[-200:]}

    def ingest(self, document):
        snapshot = projection(document)
        with self.connect() as db:
            old = self.get(db, 'snapshot', 'factory')
            if old and snapshot['observed_at'] <= old['observed_at']:
                raise ValueError('Factory observation is stale or duplicated')
            started = self.get(db, 'meta', 'created')['at']
            workspaces = self.all(db, 'workspace')
            for run in snapshot['runs']:
                if run['status'] != 'blocked' or not run['finished_at'] or run['finished_at'] < started:
                    continue
                key = 'run-' + run['id']
                if self.get(db, 'item', key):
                    continue
                workspace = next((w for w in workspaces if run['repository'].lower() in [r.lower() for r in w['repositories']]), None)
                if workspace:
                    self.put(db,'item',key,{'id':key,'workspace_id':workspace['id'],'title':run['title'],
                        'description':'Inspect the retained run evidence and decide the next step.',
                        'kind':'failure','mode':'engineering','priority':'high','status':'blocked',
                        'attention_at':run['finished_at'],'created_at':run['created_at'],
                        'updated_at':run['finished_at'],'run_id':run['id'],'resolved':False})
            self.put(db, 'snapshot', 'factory', snapshot)
        return {'accepted':True,'observed_at':snapshot['observed_at'],'run_count':len(snapshot['runs'])}

    def workspace(self, document, key=None):
        name = document.get('name','').strip()
        if not name or len(name) > 100:
            raise ValueError('Give the workspace a name of up to 100 characters')
        mode = document.get('mode','engineering')
        if mode not in MODES:
            raise ValueError('Unknown workspace mode')
        repos = repositories(document.get('repositories'))
        key = identifier(key) if key else secrets.token_hex(8)
        with self.connect() as db:
            old = self.get(db,'workspace',key)
            value = {'id':key,'name':text(name,100),'description':text(document.get('description'),1000),
                'mode':mode,'repositories':repos,'created_at':old['created_at'] if old else now()}
            self.put(db,'workspace',key,value)
        return value

    def item(self, document, key=None):
        key = identifier(key) if key else secrets.token_hex(8)
        with self.connect() as db:
            old = self.get(db,'item',key)
            if not self.get(db,'workspace',document.get('workspace_id')):
                raise ValueError('Select an existing workspace')
            title, priority = document.get('title','').strip(), document.get('priority','medium')
            status, mode = document.get('status','backlog'), document.get('mode','engineering')
            if not title or len(title)>500 or priority not in PRIORITIES or mode not in MODES:
                raise ValueError('Invalid work item title, priority or mode')
            if status not in {'backlog','ready','review','complete','blocked'}:
                raise ValueError('Execution progress comes from Factory evidence')
            if old and old.get('run_id') and status != old['status']:
                raise ValueError('A run outcome cannot be changed in the console')
            stamp = now()
            attention = old.get('attention_at') if old else None
            if document.get('needs_attention') and attention is None:
                attention = stamp
            value = {'id':key,'workspace_id':document['workspace_id'],'title':text(title),
                'description':text(document.get('description'),8000),'mode':mode,'priority':priority,
                'status':status,'kind':old.get('kind','task') if old else 'task',
                'attention_at':attention,'created_at':old['created_at'] if old else stamp,'updated_at':stamp,
                'run_id':old.get('run_id') if old else None,'resolved':document.get('resolved') is True}
            self.put(db,'item',key,value)
        return value

    def message(self, workspace_id, content):
        content = content.strip()
        if not content or len(content)>8000:
            raise ValueError('Write a message of up to 8,000 characters')
        with self.connect() as db:
            if not self.get(db,'workspace',workspace_id):
                raise ValueError('Workspace not found')
            pending = [m for m in self.all(db,'message') if m['status'] in {'pending','claimed'}]
            if len(pending)>=20:
                raise ValueError('The Orchestrator queue is full; wait for a reply')
            value = {'id':secrets.token_hex(12),'workspace_id':workspace_id,'role':'user',
                'content':text(content,8000),'created_at':now(),'status':'saved'}
            snapshot = self.get(db, 'snapshot', 'factory')
            if (snapshot and snapshot['controller']['chat_available']
                    and datetime.now(timezone.utc).timestamp()-datetime.fromisoformat(snapshot['observed_at']).timestamp()<90):
                value['status']='pending'
            self.put(db,'message',value['id'],value)
        return value

    def claim_message(self):
        with self.connect() as db:
            pending = sorted([m for m in self.all(db,'message') if m['status']=='pending'],key=lambda m:m['created_at'])
            if not pending:
                return None
            message = pending[0]
            message['status']='claimed'
            self.put(db,'message',message['id'],message)
            workspace = self.get(db,'workspace',message['workspace_id'])
            history = sorted([m for m in self.all(db,'message') if m['workspace_id']==message['workspace_id']],key=lambda m:m['created_at'])[-16:]
            return {'message':message,'workspace':workspace,'history':history}

    def reply(self, key, document):
        with self.connect() as db:
            request = self.get(db,'message',identifier(key))
            if not request or request['status']!='claimed':
                raise ValueError('Message is missing or already answered')
            content = document.get('content','').strip()
            if not content or len(content)>16000:
                raise ValueError('Invalid Orchestrator reply')
            request['status']='answered'
            self.put(db,'message',key,request)
            reply = {'id':'reply-'+key,'workspace_id':request['workspace_id'],'role':'assistant',
                'content':text(content,16000),'created_at':now(),'status':'complete','reply_to':key,
                'model':text(document.get('model'),100),'usage':document.get('usage') if isinstance(document.get('usage'),dict) else None}
            self.put(db,'message',reply['id'],reply)
        return reply

    def session(self, token, expires=None):
        fingerprint = hashlib.sha256(token.encode()).hexdigest()
        with self.connect() as db:
            if expires is not None:
                db.execute('INSERT INTO sessions VALUES(?,?)',(fingerprint,expires))
                return True
            row = db.execute('SELECT expires FROM sessions WHERE token_hash=?',(fingerprint,)).fetchone()
            return bool(row and row[0] > datetime.now(timezone.utc).timestamp())
