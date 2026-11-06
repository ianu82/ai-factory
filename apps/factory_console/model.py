"""Explicit UI projection: no credentials, commands, prompts or journal paths."""
from datetime import datetime, timezone
import re

PRIORITIES = {'urgent': 0, 'high': 1, 'medium': 2, 'low': 3}
STATES = {'backlog', 'ready', 'planning', 'building', 'verifying', 'ci', 'smoking', 'review', 'complete', 'blocked', 'unknown'}
MODES = {'engineering', 'non-coding'}
SECRET = re.compile(r'(?:sk-[A-Za-z0-9_-]{12,}|gh[pousr]_[A-Za-z0-9_]{12,}|github_pat_[A-Za-z0-9_]{12,}|Bearer\s+\S+)', re.I)


def now():
    return datetime.now(timezone.utc).isoformat()


def text(value, limit=500):
    return SECRET.sub('[redacted]', str(value or ''))[:limit]


def timestamp(value):
    if not isinstance(value, str) or len(value) > 40:
        raise ValueError('Invalid timestamp')
    parsed = datetime.fromisoformat(value.replace('Z', '+00:00'))
    if parsed.tzinfo is None:
        raise ValueError('Timestamp requires a timezone')
    return parsed.astimezone(timezone.utc).isoformat()


def identifier(value):
    if not isinstance(value, str) or not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_-]{0,95}', value):
        raise ValueError('Invalid identifier')
    return value


def repositories(value):
    if not isinstance(value, list) or not 1 <= len(value) <= 20:
        raise ValueError('Add between one and twenty repositories')
    if any(not isinstance(v, str) or not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_-]*/[A-Za-z0-9_.-]+', v)
           or v.split('/')[1] in {'.', '..'} for v in value):
        raise ValueError('Use repository names in owner/repository form')
    if len(set(v.lower() for v in value)) != len(value):
        raise ValueError('Each repository must be unique')
    return value


def projection(value):
    if not isinstance(value, dict) or value.get('version') != 1:
        raise ValueError('Unsupported Factory projection')
    observed = timestamp(value.get('observed_at'))
    if datetime.fromisoformat(observed).timestamp() > datetime.now(timezone.utc).timestamp() + 30:
        raise ValueError('Factory observation is in the future')
    controller = value.get('controller', {})
    if not isinstance(controller, dict):
        raise ValueError('Invalid controller observation')
    mode = controller.get('mode')
    if mode not in {'paused', 'busy', 'idle', 'polling', 'unknown'}:
        mode = 'unknown'
    runs = value.get('runs', [])
    if not isinstance(runs, list) or len(runs) > 100:
        raise ValueError('Factory projection exceeds the run limit')
    clean = []
    for run in runs:
        if not isinstance(run, dict):
            raise ValueError('Invalid run projection')
        sha = run.get('candidate_sha')
        clean.append({'id': identifier(run.get('id')), 'title': text(run.get('title')),
            'repository': repositories([run.get('repository')])[0],
            'status': run.get('status') if run.get('status') in STATES else 'unknown',
            'stage': run.get('stage') if run.get('stage') in STATES else 'unknown',
            'created_at': timestamp(run.get('created_at')), 'updated_at': timestamp(run.get('updated_at')),
            'finished_at': timestamp(run['finished_at']) if run.get('finished_at') else None,
            'candidate_sha': sha if isinstance(sha, str) and re.fullmatch(r'[0-9a-f]{40}', sha) else None,
            'tokens': run.get('tokens') if type(run.get('tokens')) is int and run['tokens'] >= 0 else None,
            'cost_usd': run.get('cost_usd') if type(run.get('cost_usd')) in (int, float) and 0 <= run['cost_usd'] < 1e6 else None,
            'usage_complete': run.get('usage_complete') is True,
            'summary': text(run.get('summary'), 2000),
            'pull_request_url': github_url(run.get('pull_request_url')),
            'checks': [{'name': text(c.get('name'), 150), 'status': check_state(c.get('status')),
                        'at': timestamp(c['at']) if c.get('at') else None}
                       for c in run.get('checks', [])[:50] if isinstance(c, dict)],
            'timeline': [{'kind': text(e.get('kind'), 100), 'label': text(e.get('label'), 300),
                          'at': timestamp(e.get('at')), 'status': check_state(e.get('status'))}
                         for e in run.get('timeline', [])[-100:] if isinstance(e, dict)]})
    if len({r['id'] for r in clean}) != len(clean):
        raise ValueError('Duplicate run identifiers')
    runtime = controller.get('runtime_sha', '')
    return {'version': 1, 'observed_at': observed, 'controller': {'mode': mode,
        'runtime_sha': runtime if isinstance(runtime, str) and re.fullmatch(r'[0-9a-f]{40}', runtime) else None,
        'unreadable_runs': controller.get('unreadable_runs') if type(controller.get('unreadable_runs')) is int and 0 <= controller['unreadable_runs'] <= 1000 else 0,
        'chat_available': controller.get('chat_available') is True}, 'runs': clean}


def github_url(value):
    return value if isinstance(value, str) and re.fullmatch(r'https://github.com/[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+/pull/[1-9][0-9]*', value) else None


def check_state(value):
    return value if value in {'passed', 'failed', 'running', 'unverified', 'blocked'} else 'unverified'


def attention_sort(items, order='priority'):
    if order == 'newest':
        return sorted(items, key=lambda i: (i['attention_at'], i['id']), reverse=True)
    if order == 'oldest':
        return sorted(items, key=lambda i: (i['attention_at'], i['id']))
    if order != 'priority':
        raise ValueError('Unknown attention order')
    return sorted(items, key=lambda i: (PRIORITIES[i['priority']], i['attention_at'], i['id']))
