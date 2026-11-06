"""Allowlisted, candidate-bound receipts; never raw commands or agent prose."""
import hashlib
import json
import math
import re

from .model import check_state, text, timestamp


def digest(value, size=64):
    return value if isinstance(value, str) and re.fullmatch('[0-9a-f]{'+str(size)+'}', value) else None


def record_hash(event):
    return hashlib.sha256(json.dumps(event, sort_keys=True, separators=(',', ':'), ensure_ascii=False).encode()).hexdigest()


def receipt(value):
    code, duration, sequence = value.get('exit_code'), value.get('duration_seconds'), value.get('sequence')
    return {'commit_sha': digest(value.get('commit_sha'), 40),
            'exit_code': code if type(code) is int and -65536 <= code <= 65536 else None,
            'duration_seconds': duration if type(duration) in (float, int) and math.isfinite(duration) and 0 <= duration <= 86400 else None,
            'sequence': sequence if type(sequence) is int and 0 <= sequence <= 1000000 else None,
            'record_sha256': digest(value.get('record_sha256'))}


def check(value):
    fields = {k:v for k,v in receipt(value).items() if k in value}
    status = check_state(value.get('status'))
    if 'exit_code' in value:
        code = fields['exit_code']
        status = 'unverified' if code is None else 'failed' if code != 0 else status
    if status == 'passed' and any(key in value and fields[key] is None for key in fields):
        status = 'unverified'
    return {**fields, 'name':text(value.get('name'),150), 'status':status,
            'at':timestamp(value['at']) if value.get('at') else None}


def passed_criteria(value, expected=None):
    if not isinstance(value, list) or not 1 <= len(value) <= 30:
        return False
    ids = [c.get('id') for c in value if isinstance(c, dict) and c.get('status') == 'passed']
    if len(ids) != len(value) or any(not isinstance(i,str) or not i or len(i)>100 or text(i,100)!=i for i in ids):
        return False
    return len(set(ids)) == len(ids) and (expected is None or set(ids) == set(expected) and len(ids) == len(expected))


def environment(value):
    if not isinstance(value, dict):
        return None
    # Only pinned configuration identifiers, never credentials, task prose or paths.
    image, platform = value.get('image'), value.get('platform')
    return {'image': image if isinstance(image, str) and re.fullmatch(r'[a-zA-Z0-9/_.:-]+@sha256:[a-f0-9]{64}', image) else None,
            'platform': platform if platform in {'linux/amd64', 'linux/arm64'} else None,
            'profile_sha256': digest(value.get('profile_sha256')),
            'runtime_sha': digest(value.get('runtime_sha'), 40)}


def review(value):
    if not isinstance(value, dict):
        return None
    accepted = (value.get('accepted') is True and value.get('status') == 'passed'
                and bool(digest(value.get('candidate_sha'),40)) and bool(digest(value.get('record_sha256')))
                and passed_criteria(value.get('criteria')))
    return {'status': check_state(value.get('status')), 'at': timestamp(value['at']),
            'candidate_sha': digest(value.get('candidate_sha'), 40),
            'record_sha256': digest(value.get('record_sha256')),
            'accepted': bool(accepted),
            'criteria': [{'id': text(c.get('id'), 100), 'status': check_state(c.get('status'))}
                         for c in value.get('criteria', [])[:30] if isinstance(c, dict)]}
