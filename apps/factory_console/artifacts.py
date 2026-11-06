"""Private, immutable console attachments, separate from Factory certification."""
import base64
import hashlib
import json
import re

from .model import identifier, now, text
from .evidence import digest

MAX_BYTES = 1024 * 1024


def install(db):
    db.execute('CREATE TABLE IF NOT EXISTS artifacts(id TEXT PRIMARY KEY,kind TEXT,target TEXT,metadata TEXT,body BLOB)')


def metadata(db):
    return [json.loads(row[0]) for row in db.execute('SELECT metadata FROM artifacts ORDER BY rowid')]


def save(db, kind, target, document):
    identifier(target)
    row = db.execute('SELECT 1 FROM records WHERE kind=? AND id=?', (kind, target)).fetchone()
    if kind not in {'item', 'run'} or not row:
        raise ValueError('Evidence target not found')
    name = document.get('name', '')
    if not isinstance(name, str) or not re.fullmatch(r'[^/\\\x00-\x1f]{1,120}', name):
        raise ValueError('Invalid evidence filename')
    encoded = document.get('content_base64', '')
    if not isinstance(encoded, str) or len(encoded) > (MAX_BYTES + 2) // 3 * 4:
        raise ValueError('Evidence exceeds one megabyte')
    body = base64.b64decode(encoded, validate=True)
    if not 0 < len(body) <= MAX_BYTES:
        raise ValueError('Invalid evidence size')
    if body.startswith(b'\x89PNG\r\n\x1a\n'):
        mime = 'image/png'
    elif body.startswith(b'\xff\xd8\xff'):
        mime = 'image/jpeg'
    elif name.lower().endswith(('.txt', '.log', '.md', '.json')):
        body.decode('utf-8')
        mime = 'text/plain; charset=utf-8'
    else:
        raise ValueError('Attach a PNG, JPEG or UTF-8 text report')
    sha = hashlib.sha256(body).hexdigest()
    key = hashlib.sha256(f'{kind}:{target}:{sha}'.encode()).hexdigest()
    existing = db.execute('SELECT metadata FROM artifacts WHERE id=?', (key,)).fetchone()
    if existing:
        return json.loads(existing[0])
    if db.execute('SELECT COUNT(*) FROM artifacts WHERE kind=? AND target=?', (kind, target)).fetchone()[0] >= 20:
        raise ValueError('This item already has twenty evidence attachments')
    value = {'id':key, 'target_kind':kind, 'target_id':target, 'name':text(name,120),
             'caption':text(document.get('caption'),200), 'source':'manual', 'at':now(),
             'candidate_sha':digest(document.get('candidate_sha'),40), 'sha256':sha, 'size':len(body), 'mime':mime}
    db.execute('INSERT INTO artifacts VALUES(?,?,?,?,?)', (key,kind,target,json.dumps(value),body))
    return value


def load(db, key):
    if not digest(key):
        return None
    row = db.execute('SELECT metadata,body FROM artifacts WHERE id=?', (key,)).fetchone()
    if not row:
        return None
    value, body = json.loads(row[0]), row[1]
    if hashlib.sha256(body).hexdigest() != value['sha256']:
        raise ValueError('Evidence integrity check failed')
    return value, body
