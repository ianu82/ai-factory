"""Bounded read-only journal publisher. No inference, execution or claim mutation."""
import argparse
from collections import Counter
from datetime import datetime, timezone
import json
from pathlib import Path
import sys
import time
from urllib.parse import urlsplit
from urllib.error import HTTPError
from urllib.request import HTTPRedirectHandler, Request, build_opener

if __package__ in {None, ''}:
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
    from apps.factory_console.model import now, projection
    from apps.factory_console.evidence import record_hash
else:
    from .model import now, projection
    from .evidence import record_hash

LABELS = {'base_resolved':'Source pinned','plan_frozen':'Plan retained',
    'attempt_started':'Implementation attempt started','candidate_created':'Candidate retained',
    'candidate_verified':'Independent verification passed','publication_requested':'Publication requested',
    'pull_request_published':'Pull request published','github_checks_passed':'GitHub checks passed',
    'promotion_requested':'Exact-base promotion requested','merge_confirmed':'Exact merge confirmed',
    'post_merge_smoke':'Post-merge smoke check','run_finished':'Run finished'}
STAGES = {'base_resolved':'planning','plan_frozen':'planning','attempt_started':'building',
    'candidate_created':'verifying','verification_check':'verifying','candidate_verified':'verifying',
    'publication_requested':'ci','pull_request_published':'ci','github_checks_passed':'ci',
    'promotion_requested':'ci','merge_confirmed':'smoking','post_merge_smoke':'smoking'}


class NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, request, response, code, message, headers, new_url):
        raise HTTPError(request.full_url, code, 'Console redirects are refused', headers, response)


class Publisher:
    def __init__(self, stores, status_file, runtime_sha=None, *, journal_factory=None):
        if journal_factory is None:
            from auto_mindsdb_factory.run_journal import RunJournal
            journal_factory = RunJournal
        self.stores, self.status_file = [Path(s).resolve() for s in stores], Path(status_file)
        self.runtime_sha, self.journal_factory, self.cache = runtime_sha, journal_factory, {}

    def run(self, store, directory):
        manifest_file = directory/'manifest.json'
        if directory.is_symlink() or manifest_file.is_symlink() or manifest_file.stat().st_size>1024*1024:
            raise ValueError('Unsupported journal')
        files = list(directory.glob('event-*.json'))
        if len(files)>3000 or any(p.is_symlink() or not p.is_file() for p in files) or sum(p.stat().st_size for p in files)>16*1024*1024:
            raise ValueError('Journal exceeds observation limits')
        fingerprint = (len(files),max((p.stat().st_mtime_ns for p in files),default=0),manifest_file.stat().st_mtime_ns)
        key = str(directory)
        if key in self.cache and self.cache[key][0]==fingerprint:
            return self.cache[key][1]
        manifest = json.loads(manifest_file.read_text())
        journal = self.journal_factory(store,directory.name,read_only=True)
        events, summary = journal.events(), journal.summary()
        current_files = list(directory.glob('event-*.json'))
        current = (len(current_files), max((p.stat().st_mtime_ns for p in current_files), default=0),
                   manifest_file.stat().st_mtime_ns)
        if current != fingerprint:
            raise ValueError('Journal changed during observation')
        if not events:
            return None
        created = manifest.get('created_at',events[0]['at'])
        stage, terminal, checks, timeline, candidate, pr = 'planning',None,[],[],None,None
        review, review_report = None, None
        for event in events:
            kind, data, at = event['kind'], event['data'], event['at']
            stage = STAGES.get(kind,stage)
            if kind=='candidate_created':
                candidate=data.get('commit_sha')
                review, review_report = None, None
            if kind=='pull_request_published':pr=data.get('url')
            if kind=='run_finished':terminal=event
            status = 'passed'
            if kind in {'verification_check','post_merge_smoke'}:
                result = data.get('exit_code')
                status = ('passed' if result==0 else 'failed') if type(result) is int else 'unverified'
                checks.append({'name':data.get('name') if kind=='verification_check' else 'Smoke '+str(data.get('index')),
                               'status':status,'at':at, 'commit_sha': data.get('commit_sha'),
                               'exit_code': result, 'duration_seconds': data.get('duration_seconds'),
                               'sequence': event.get('sequence'), 'record_sha256': record_hash(event)})
            if kind=='agent_report' and data.get('role')=='verifier' and isinstance(data.get('report'),dict):
                report=review_report=data['report']
                review={'status':report.get('status'),'at':at,'candidate_sha':candidate,
                        'record_sha256':record_hash(event),'accepted':False,'criteria':report.get('criteria',[])}
            if kind=='candidate_verified' and review and candidate and data.get('commit_sha')==candidate and data.get('report')==review_report:
                review['accepted']=review['status']=='passed' and bool(review['criteria']) and all(
                    c.get('status')=='passed' for c in review['criteria'])
            if kind=='plan_frozen':status='passed' if data.get('status')=='ready' else 'unverified'
            if kind=='run_finished':status='passed' if data.get('status')=='complete' else 'blocked'
            if kind in LABELS:
                timeline.append({'kind':kind,'label':LABELS[kind],'at':at,'status':status})
        status = {'complete':'complete','blocked':'blocked','verified':'review','pr_ready':'review'}.get(
            terminal['data'].get('status'),'unknown') if terminal else stage
        reason = ('Controller recorded complete; inspect retained verification and delivery evidence.' if status=='complete' else
                  'The run ended blocked during '+stage+'. Its original evidence is retained.' if status=='blocked' else
                  'The candidate is ready for review; complete delivery is not yet recorded.' if status=='review' else
                  'No terminal outcome has been recorded. Activity does not establish process liveness.')
        value = {'id':directory.name,'title':manifest.get('task',{}).get('title',directory.name),
            'repository':manifest.get('profile',{}).get('repository'),'status':status,'stage':stage,
            'created_at':created,'updated_at':events[-1]['at'],'finished_at':terminal['at'] if terminal else None,
            'candidate_sha':candidate,'tokens':summary.get('total_tokens'),'cost_usd':summary.get('cost_usd'),
            'usage_complete':summary.get('usage_complete') is True,'summary':reason,'pull_request_url':pr,
            'environment':{'image':manifest.get('profile',{}).get('image'),
                           'platform':manifest.get('profile',{}).get('platform'),
                           'profile_sha256':manifest.get('profile_sha'),'runtime_sha':manifest.get('runtime_sha')},
            'review':review, 'checks':checks[-50:],'timeline':timeline[-100:]}
        self.cache[key]=(fingerprint,value)
        return value

    def snapshot(self):
        stamp = now()
        mode = 'unknown'
        try:
            if self.status_file.is_symlink() or self.status_file.stat().st_size>4096:
                raise ValueError('Invalid controller status')
            status = json.loads(self.status_file.read_text())
            age = time.time()-status['at']
            if 0<=age<180:
                mode = status['mode'] if status['mode'] in {'paused','busy','idle','polling'} else 'unknown'
        except (OSError, ValueError, KeyError, TypeError):
            pass
        directories=[]
        for store in self.stores:
            root=store/'delivery'
            if not root.is_dir() or root.is_symlink():continue
            directories.extend((store,p) for p in root.iterdir() if p.is_dir() and not p.is_symlink() and (p/'manifest.json').is_file())
        directories.sort(key=lambda pair:(pair[1]/'manifest.json').stat().st_mtime_ns,reverse=True)
        identities=Counter(directory.name for _,directory in directories)
        runs, unavailable = [], 0
        for store,directory in directories[:50]:
            try:
                if identities[directory.name]>1:
                    raise ValueError('Ambiguous run identity across configured stores')
                value=self.run(store,directory)
                if value:
                    checked=projection({'version':1,'observed_at':stamp,'controller':{'mode':'unknown'},'runs':[value]})['runs'][0]
                    runs.append(checked)
            except (OSError, ValueError, KeyError, TypeError, RuntimeError):
                unavailable+=1
        return projection({'version':1,'observed_at':stamp,'controller':{'mode':mode,
            'runtime_sha':self.runtime_sha,'chat_available':False,'unreadable_runs':unavailable},'runs':runs})


def publish(endpoint, token, document, *, opener=None):
    parsed=urlsplit(endpoint)
    if (parsed.scheme!='https' or not parsed.hostname or parsed.username or parsed.password
            or parsed.query or parsed.fragment or parsed.path not in {'','/'}):
        raise ValueError('Use the exact HTTPS console origin')
    opener = opener or build_opener(NoRedirect()).open
    request=Request(endpoint.rstrip('/')+'/bridge/snapshot',data=json.dumps(document,allow_nan=False).encode(),
                    headers={'Content-Type':'application/json','Authorization':'Bearer '+token},method='POST')
    with opener(request,timeout=10) as response:
        result=json.loads(response.read(4096))
        if not result.get('accepted'):raise ValueError('Observation was not accepted')


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--store',type=Path,action='append',required=True)
    parser.add_argument('--status-file',type=Path,required=True)
    parser.add_argument('--runtime-sha')
    parser.add_argument('--endpoint',required=True)
    parser.add_argument('--token-file',type=Path,required=True)
    parser.add_argument('--interval',type=int,default=15)
    parser.add_argument('--max-seconds',type=int,default=3600)
    args=parser.parse_args()
    if not 5<=args.interval<=300 or not 10<=args.max_seconds<=86400:
        parser.error('Use a bounded observation interval and lifetime')
    if args.token_file.is_symlink() or args.token_file.stat().st_size>256:
        raise ValueError('Invalid UI-only bridge credential file')
    token=args.token_file.read_text().strip()
    if len(token)<20:raise ValueError('Bridge credential is unavailable')
    publisher=Publisher(args.store,args.status_file,args.runtime_sha)
    deadline=time.monotonic()+args.max_seconds
    while time.monotonic()<deadline:
        try:
            snapshot=publisher.snapshot()
            publish(args.endpoint,token,snapshot)
            print(json.dumps({'console_observation':'accepted','at':snapshot['observed_at'],'runs':len(snapshot['runs'])}),flush=True)
        except Exception:
            print(json.dumps({'console_observation':'unavailable','at':now()}),flush=True)
        time.sleep(min(args.interval,max(0,deadline-time.monotonic())))


if __name__=='__main__':main()
