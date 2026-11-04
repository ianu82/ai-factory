from dataclasses import replace
import pytest
from auto_mindsdb_factory.linear_trigger import LinearIssueSnapshot
from auto_mindsdb_factory.product_linear_intake import LinearProductRoute, task_from_issue
from auto_mindsdb_factory.product_linear_worker import LinearProductWorker
from auto_mindsdb_factory.product_profile import ProductProfile
from auto_mindsdb_factory.run_journal import RunJournal
SHA = 'a' * 40
URL = 'https://github.com/owner/product/pull/1'
class Client:
    def __init__(self, issue):
        self.issue, self.writes, self.fail_done = issue, [], False
    def validate_route(self, route):
        self.writes.append('validated')
    def queued_issue_ids(self, route, limit, *, exclude_ids):
        return [self.issue.id] if self.issue.state['id'] == 'queued' and self.issue.id not in exclude_ids else []
    def fetch_issue_snapshot(self, issue_id):
        return self.issue
    def create_comment(self, issue_id, body):
        self.writes.append('comment')
        if 'outcome]' in body and self.fail_done:
            self.fail_done = False
            raise RuntimeError('secret provider response')
        self.issue = replace(self.issue, comments=self.issue.comments + [{'body': body}])
@pytest.fixture
def env(tmp_path):
    issue = LinearIssueSnapshot(id='b894af1a-d838-4bc8-bddf-a2800cdd0cde', identifier='ENG-1',
        title='Feature', description='details', url='https://linear.app/team/issue/ENG-1',
        team={'id': 'team'}, state={'id': 'queued'}, labels=['factory'], priority=None,
        project=None, creator=None, assignee=None, created_at='then', updated_at='now', comments=[])
    route = LinearProductRoute.from_document({'version': 2, 'team_id': 'team', 'queued_state_id': 'queued',
        'required_label': 'factory', 'acceptance_criteria': [{'id': 'test', 'description': 'test', 'command': ['pytest']}]})
    profile = ProductProfile.from_document({'version': 1, 'name': 'sample', 'repository': 'owner/product',
        'image': 'python@sha256:' + 'a' * 64, 'checks': {'unit': ['pytest']}, 'smoke': [['true']], 'auto_merge': True, 'required_status_checks': ['validate', 'sandbox']})
    client = Client(issue)
    class Harness:
        builds = 0
        promotions = 0
        merges = 0
        before_publish = None
        during_ci = None
        status = 'complete'
        def delivery(self, profile, target, store):
            harness = self
            class Delivery:
                deadline = 123
                def smoke(self, sha, journal): pass
                def run(self, task, *, promote):
                    harness.builds += 1
                    journal = RunJournal(store, task.document['id'])
                    journal.create({'purpose': 'delivery', 'task': task.document, 'task_sha': task.digest,
                                    'profile': profile.document, 'profile_sha': profile.digest})
                    if harness.before_publish: harness.before_publish()
                    result = promote({}, journal)
                    journal.append('run_finished', status=result['status'])
                    return result
            return Delivery()
        def promotion(self, profile, task, target, store, *, smoke, deadline, before_merge):
            harness = self
            def promote(candidate, journal):
                harness.promotions += 1
                journal.append('pull_request_published', number=1, url=URL, commit_sha=SHA)
                if harness.during_ci: harness.during_ci()
                before_merge()
                harness.merges += 1
                journal.append('merge_confirmed', commit_sha=SHA, pr_number=1)
                journal.append('post_merge_smoke', commit_sha=SHA, index=1, command=['true'], exit_code=0)
                return {'status': harness.status, 'merge_sha': SHA, 'pull_request_url': URL}
            return promote
    h = Harness()
    def worker(p=profile, r=route):
        return LinearProductWorker(p, tmp_path / 'target', tmp_path / 'state', r, client,
            delivery_factory=h.delivery, promotion_factory=h.promotion)
    return client, h, worker, profile, route
def test_success_and_no_rebuild(env):
    client, h, worker, _, _ = env
    w = worker()
    assert w.run_cycle()[0]['status'] == 'complete'
    assert client.writes == ['validated', 'comment', 'comment']
    assert 'started]' in client.issue.comments[0]['body']
    assert SHA in client.issue.comments[1]['body'] and URL in client.issue.comments[1]['body']
    assert client.issue.state['id'] == 'queued'
    assert w.run_cycle() == [] and h.builds == h.merges == 1
def test_retry_sync_after_restart_without_duplicate_comment(env):
    client, h, worker, _, _ = env
    client.fail_done = True
    assert worker().run_cycle()[0]['status'] == 'pending'
    assert len(client.issue.comments) == 1 and client.issue.state['id'] == 'queued'
    assert worker().run_cycle()[0]['status'] == 'complete'
    assert client.issue.state['id'] == 'queued' and len(client.issue.comments) == 2
    assert h.builds == h.merges == 1
@pytest.mark.parametrize('phase', ['before_publish', 'during_ci'])
@pytest.mark.parametrize('change', ['cancel', 'edit', 'label', 'team', 'assignee'])
def test_source_changes_stop_publication_or_merge(env, phase, change):
    client, h, worker, _, _ = env
    def mutate():
        fields = {'cancel': {'state': {'id': 'canceled'}}, 'edit': {'description': 'changed'},
                  'label': {'labels': []}, 'team': {'team': {'id': 'elsewhere'}},
                  'assignee': {'assignee': {'id': 'someone'}}}
        client.issue = replace(client.issue, **fields[change])
    setattr(h, phase, mutate)
    assert worker().run_cycle()[0]['status'] == ('blocked' if change == 'cancel' else 'conflict')
    assert h.builds == 1 and h.merges == 0
    assert h.promotions == (phase == 'during_ci')
    assert client.issue.state['id'] == ('canceled' if change == 'cancel' else 'queued')
    w = worker()
    if change != 'cancel':
        journal = w.state.pending()[0]
        assert w.state.outcome(journal)['status'] == 'blocked'
        assert w.run_cycle()[0]['status'] == 'conflict'
    assert h.builds == 1
def test_queued_stale_snapshot_never_claimed(env):
    client, h, worker, _, _ = env
    client.queued_issue_ids = lambda route, limit, *, exclude_ids: [client.issue.id]
    client.issue = replace(client.issue, labels=[])
    assert worker().run_cycle()[0]['status'] == 'ineligible'
    assert h.builds == 0
def test_interrupted_claim_blocks_without_build(env):
    client, h, worker, profile, route = env
    w = worker()
    with w.state.lease(client.issue.id):
        journal = w.state.claim(task_from_issue(client.issue, route), client.issue, route, profile)
        w._freeze_owner(journal, client.issue)
    assert worker().run_cycle()[0]['status'] == 'blocked'
    assert client.issue.state['id'] == 'queued' and h.builds == 0
def test_recover_completed_product_after_outcome_write_failure(env, monkeypatch):
    client, h, worker, _, _ = env
    w = worker()
    monkeypatch.setattr(w.state, 'record_outcome', lambda *a: (_ for _ in ()).throw(OSError('disk')))
    assert w.run_cycle()[0]['status'] == 'pending'
    assert h.builds == h.merges == 1 and client.issue.state['id'] == 'queued'
    assert worker().run_cycle()[0]['status'] == 'complete'
    assert client.issue.state['id'] == 'queued' and h.builds == h.merges == 1
@pytest.mark.parametrize('corrupt', ['unfinished', 'smoke', 'sha', 'profile'])
def test_recovery_requires_exact_complete_evidence(env, monkeypatch, corrupt):
    client, h, worker, _, _ = env
    w = worker()
    monkeypatch.setattr(w.state, 'record_outcome', lambda *a: (_ for _ in ()).throw(OSError()))
    w.run_cycle()
    path = w.store / 'delivery' / ('linear-' + client.issue.id)
    import json
    if corrupt == 'profile':
        p = path / 'manifest.json'
        data = json.loads(p.read_text()); data['profile_sha'] = 'b' * 64
    else:
        p = path / ('event-000004.json' if corrupt == 'unfinished' else 'event-000003.json')
        data = json.loads(p.read_text())
        if corrupt == 'unfinished': data['data']['status'] = 'blocked'
        elif corrupt == 'smoke': data['data']['exit_code'] = 1
        else: data['data']['commit_sha'] = 'b' * 40
    p.write_text(json.dumps(data))
    assert worker().run_cycle()[0]['status'] == 'blocked'
    assert client.issue.state['id'] == 'queued' and h.builds == h.merges == 1
def test_pending_binding_change_conflicts(env):
    client, h, worker, profile, route = env
    client.fail_done = True
    worker().run_cycle()
    other = ProductProfile.from_document({**profile.document, 'name': 'different'})
    assert worker(other).run_cycle()[0]['status'] == 'conflict'
    assert h.builds == h.merges == 1 and client.issue.state['id'] == 'queued'
def test_noncomplete_never_merged(env):
    client, h, worker, _, _ = env
    h.status = 'pr_ready'
    assert worker().run_cycle()[0]['status'] == 'blocked'
    assert client.issue.state['id'] == 'queued'
@pytest.mark.parametrize('value', [0, 101, True, 1.0, '1', None])
def test_bounds_and_opt_in(env, value):
    client, _, worker, profile, _ = env
    with pytest.raises(ValueError): worker().run_cycle(value)
    with pytest.raises(ValueError): worker(ProductProfile.from_document({**profile.document, 'auto_merge': False}))
    assert client.writes == []
def test_sync_does_not_overwrite_cancellation_after_merge(env):
    client, h, worker, _, _ = env
    client.fail_done = True
    worker().run_cycle()
    client.issue = replace(client.issue, state={'id': 'canceled'})
    assert worker().run_cycle()[0]['status'] == 'complete'
    assert client.issue.state['id'] == 'canceled' and h.builds == h.merges == 1
    assert 'Code verified and merged' in client.issue.comments[-1]['body']
def test_missing_finished_event_after_merge_is_blocked(env, monkeypatch):
    client, h, worker, _, _ = env
    w = worker()
    monkeypatch.setattr(w.state, 'record_outcome', lambda *a: (_ for _ in ()).throw(OSError()))
    w.run_cycle()
    (w.store / 'delivery' / ('linear-' + client.issue.id) / 'event-000004.json').unlink()
    assert worker().run_cycle()[0]['status'] == 'blocked'
    assert client.issue.state['id'] == 'queued' and h.builds == h.merges == 1
@pytest.mark.parametrize('corrupt', ['wrong_repo', 'wrong_number', 'bool_number', 'merge_number', 'manifest_version', 'duplicate_terminal', 'shifted_files', 'after_terminal'])
def test_recovery_rejects_mismatched_publication(env, monkeypatch, corrupt):
    import json
    client, h, worker, _, _ = env
    w = worker()
    monkeypatch.setattr(w.state, 'record_outcome', lambda *a: (_ for _ in ()).throw(OSError()))
    assert w.run_cycle()[0]['status'] == 'pending'
    path = w.store / 'delivery' / ('linear-' + client.issue.id)
    if corrupt == 'shifted_files':
        for file in sorted(path.glob('event-*.json'), reverse=True):
            file.rename(path / f"event-{int(file.stem[6:]) + 1:06d}.json")
    elif corrupt == 'after_terminal':
        RunJournal(w.store, 'linear-' + client.issue.id).append('model_requested', role='builder')
    elif corrupt == 'duplicate_terminal':
        journal = RunJournal(w.store, 'linear-' + client.issue.id)
        journal.append('run_finished', status='complete')
    else:
        name = {'manifest_version': 'manifest.json', 'merge_number': 'event-000002.json'}.get(corrupt, 'event-000001.json')
        file = path / name
        doc = json.loads(file.read_text())
        data = doc if corrupt == 'manifest_version' else doc['data']
        if corrupt == 'wrong_repo': data['url'] = 'https://github.com/other/product/pull/1'
        if corrupt == 'wrong_number': data['url'] = 'https://github.com/owner/product/pull/2'
        if corrupt == 'bool_number': data['number'] = True
        if corrupt == 'merge_number': data['pr_number'] = 2
        if corrupt == 'manifest_version': data['version'] = 2
        file.write_text(json.dumps(doc))
    assert worker().run_cycle()[0]['status'] == 'blocked'
    assert h.builds == h.merges == 1
def test_old_synced_claim_excluded_before_queue_limit(env):
    client, h, worker, _, _ = env
    old = client.issue
    assert worker().run_cycle()[0]['status'] == 'complete'
    new = replace(old, id='c894af1a-d838-4bc8-bddf-a2800cdd0cde', identifier='ENG-2')
    def queue(route, limit, *, exclude_ids):
        assert old.id in exclude_ids
        return [i.id for i in (old, new) if i.id not in exclude_ids][:limit]
    client.issue = new
    client.queued_issue_ids = queue
    assert worker().run_cycle(1)[0]['status'] == 'complete'
    assert h.builds == h.merges == 2
def test_ambiguous_comment_failure_deduplicates_on_restart(env):
    client, h, worker, _, _ = env
    create = client.create_comment
    def ambiguous(issue_id, body):
        create(issue_id, body)
        if 'outcome]' in body: raise RuntimeError('secret')
    client.create_comment = ambiguous
    assert worker().run_cycle()[0]['status'] == 'pending'
    client.create_comment = create
    assert worker().run_cycle()[0]['status'] == 'complete'
    assert len(client.issue.comments) == 2 and h.builds == h.merges == 1
def test_concurrent_duplicate_queue_claim_never_rebuilds(env):
    client, h, worker, _, _ = env
    assert worker().run_cycle()[0]['status'] == 'complete'
    client.queued_issue_ids = lambda route, limit, *, exclude_ids: [client.issue.id]
    assert worker().run_cycle()[0]['status'] == 'already claimed'
    assert h.builds == h.merges == 1
def test_cancellation_immediately_before_reporting(env):
    client, h, worker, _, _ = env
    create = client.create_comment
    def cancel(issue_id, body):
        if 'outcome]' in body: client.issue = replace(client.issue, state={'id': 'canceled'})
        create(issue_id, body)
    client.create_comment = cancel
    assert worker().run_cycle()[0]['status'] == 'complete'
    assert client.issue.state['id'] == 'canceled' and h.merges == 1
def test_delivery_cannot_complete_without_promotion_callback(env):
    client, h, worker, _, _ = env
    w = worker()
    class Delivery:
        def run(self, task, *, promote):
            return {'status': 'complete', 'merge_sha': SHA, 'pull_request_url': URL}
    w.delivery_factory = lambda *a: Delivery()
    assert w.run_cycle()[0]['status'] == 'blocked'
    assert h.merges == 0 and 'Blocked' in client.issue.comments[-1]['body']
def test_lost_source_lease_stops_publication_and_outcome(env):
    from contextlib import contextmanager
    client, h, worker, _, _ = env
    w = worker()
    class Lease:
        lost = False
        def heartbeat(self):
            if self.lost: raise RuntimeError('lease lost')
    held = Lease()
    h.before_publish = lambda: setattr(held, 'lost', True)
    @contextmanager
    def lease(issue_id): yield held
    w.state.lease = lease
    assert w.run_cycle()[0]['status'] == 'pending'
    assert h.builds == 1 and h.promotions == 0
    assert w.state.outcome(w.state.pending()[0]) is None


@pytest.mark.parametrize("checks", [[], [" "], ["validate", "\t"]])
def test_automatic_delivery_requires_named_ci_policy(env, checks):
    client, _, worker, profile, _ = env
    with pytest.raises(ValueError, match="named GitHub CI checks"):
        worker(ProductProfile.from_document({**profile.document, 'required_status_checks': checks}))
    assert client.writes == []
