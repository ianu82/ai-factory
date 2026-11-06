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
    return {'status': check_state(value.get('status')), 'at': timestamp(value['at']),
            'candidate_sha': digest(value.get('candidate_sha'), 40),
            'record_sha256': digest(value.get('record_sha256')),
            'accepted': value.get('accepted') is True,
            'criteria': [{'id': text(c.get('id'), 100), 'status': check_state(c.get('status'))}
                         for c in value.get('criteria', [])[:30] if isinstance(c, dict)]}
