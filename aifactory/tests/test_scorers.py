from __future__ import annotations

import asyncio
import json
from datetime import timedelta

import pytest

from aifactory import scorers
from aifactory.db import session_scope, utcnow
from aifactory.inference import ProviderConfig
from aifactory.models import Outcome, Run, ScorerAttempt, ScorerResult, Workspace
from aifactory.pipeline.orchestrator import Orchestrator
from aifactory.pipeline.runtime import DEFAULT_SETTINGS, outcome_cost
from .fakes import ScriptedModel


@pytest.fixture
def judge(signed_in, monkeypatch):
    ws = signed_in['workspaces'][0]['id']
    with session_scope() as s:
        settings = json.loads(json.dumps(DEFAULT_SETTINGS))
        settings['provider'] = 'openai'
        settings['pricing'] = {'gpt-6.1-terra': {'input': 1, 'output': 2}}
        s.get(Workspace, ws).settings = settings
        outcome = Outcome(workspace_id=ws, number=1, title='Plan greeting', acceptance=['Test greeting'])
        s.add(outcome)
        s.flush()
        run = Run(workspace_id=ws, outcome_id=outcome.id, role='planner', harness='native',
                  status='succeeded', result={'tasks': [{'spec': 'Change greeting'}]}, cost_usd=1)
        s.add(run)
        s.flush()
        ids = {'ws': ws, 'run': run.id, 'outcome': outcome.id}
    ScriptedModel.calls = []
    ScriptedModel.scripts = {'scorer': lambda messages: {'content': json.dumps({
        'classification': 'good', 'score': 9999, 'passed': False, 'reasoning': 'Specific, testable, focused.'})}}
    monkeypatch.setattr(scorers, 'ModelClient', ScriptedModel)
    monkeypatch.setattr(scorers, 'resolve_provider', lambda ws: ProviderConfig('openai', 'http://test', 'SECRET', 'Test'))
    return ids


def reservation(judge, automatic=False):
    with session_scope() as s:
        return scorers.reserve(s, s.get(Run, judge['run']), scorers.BUILTIN, automatic=automatic).id


def test_builtin_on_demand_storage_and_accounting(client, judge):
    root = f"/api/workspaces/{judge['ws']}"
    definition_response = client.get(root + '/scorers')
    assert definition_response.status_code == 200
    definition = definition_response.json()[0]
    assert definition == scorers.BUILTIN.model_dump() | {'version': scorers.BUILTIN.version}
    response = client.post(root + f"/runs/{judge['run']}/scores", json={'scorer_id': 'planner-quality'})
    assert response.status_code == 200
    score = response.json()
    assert (score['classification'], score['score'], score['passed']) == ('good', 1, True)
    assert score['reasoning'] == 'Specific, testable, focused.'
    assert score['cost_known'] and score['cost_usd'] == .0014
    assert ScriptedModel.calls == ['scorer']
    with session_scope() as s:
        attempt = s.query(ScorerAttempt).one()
        assert attempt.model == 'gpt-6.1-terra'  # DEFAULT_SETTINGS contains gpt-terra, even with an own OpenAI key
        assert s.get(Run, judge['run']).result == {'tasks': [{'spec': 'Change greeting'}]}
    stored = client.get(root + f"/runs/{judge['run']}/scores").json()[0]
    assert stored == score
    assert stored['definition'] == scorers.BUILTIN.model_dump()
    assert stored['definition_version'] == definition['version']
    summary = client.get(root + '/runs').json()[0]['scores'][0]
    assert summary['id'] == score['id']
    assert (summary['classification'], summary['score'], summary['passed']) == ('good', 1, True)
    assert 'reasoning' not in summary and 'definition' not in summary
    assert outcome_cost(judge['outcome']) == (1.0014, True)
    stats = client.get(root + '/stats').json()
    assert stats['tokens'] == 1200
    assert stats['cost']['known']


@pytest.mark.parametrize('bad', [ {'classification': 'forged', 'score': 999, 'reasoning': 'x'},
                                  {'classification': [], 'reasoning': 'x'}, {}, None])
async def test_invalid_judge_is_isolated(judge, bad):
    ScriptedModel.scripts['scorer'] = lambda _: {'content': json.dumps(bad)}
    rid = reservation(judge)
    await scorers.execute(rid)
    with session_scope() as s:
        row = s.get(ScorerResult, rid)
        assert row.status == 'error' and row.score is None and row.passed is None
        assert s.get(Run, judge['run']).status == 'succeeded'


async def test_provider_errors_are_sanitized_and_predispatch_free(judge, monkeypatch):
    def fail(_):
        raise RuntimeError('SECRET')
    monkeypatch.setattr(scorers, 'resolve_provider', fail)
    rid = reservation(judge)
    await scorers.execute(rid)
    with session_scope() as s:
        row = s.get(ScorerResult, rid)
        assert row.cost_known and row.cost_usd == 0
        assert 'SECRET' not in row.error and row.status == 'error'


def test_sampling_and_idempotent_reservations(judge):
    zero = scorers.BUILTIN
    one = zero.model_copy(update={'sample_rate': 1})
    middle = zero.model_copy(update={'sample_rate': .37})
    values = [scorers.selected(str(i), middle) for i in range(1000)]
    assert values == [scorers.selected(str(i), middle) for i in range(1000)]
    assert 300 < sum(values) < 440
    assert not scorers.selected(judge['run'], zero)
    assert scorers.selected(judge['run'], one)
    with session_scope() as s:
        run = s.get(Run, judge['run'])
        assert scorers.reserve_automatic(s, run) == []
        ws = s.get(Workspace, judge['ws'])
        ws.settings = {**ws.settings, 'scorers': [one.model_dump()]}
        first = scorers.reserve_automatic(s, run)
        assert first == scorers.reserve_automatic(s, run)
        run.role = 'builder'
        assert scorers.reserve_automatic(s, run) == []
        assert s.query(ScorerResult).count() == 1


@pytest.mark.parametrize('patch', [{'sample_rate': float('nan')}, {'pass_threshold': float('inf')},
                                  {'classifications': {'bad': float('inf')}}, {'roles': ['scorer']},
                                  {'id': ''}, {'model': ''}])
def test_validation(patch):
    with pytest.raises(ValueError):
        scorers.ScorerDefinition.model_validate(scorers.BUILTIN.model_dump() | patch)


@pytest.mark.parametrize('raw', [None, {}, [None], [scorers.BUILTIN.model_dump()] * 2])
def test_invalid_config_returns_422(client, judge, raw):
    root = f"/api/workspaces/{judge['ws']}"
    assert client.patch(root, json={'settings': {'scorers': raw}}).status_code == 422
    # Legacy/malformed stored settings also return a controlled response.
    with session_scope() as s:
        ws = s.get(Workspace, judge['ws'])
        ws.settings = {**ws.settings, 'scorers': raw}
    assert client.get(root + '/scorers').status_code == 422
    assert client.post(root + f"/runs/{judge['run']}/scores", json={'scorer_id': 'planner-quality'}).status_code == 422


@pytest.mark.parametrize('credential_removed', [False, True])
async def test_interrupted_paid_attempt_survives_recovery(judge, monkeypatch, credential_removed):
    entered = asyncio.Event()
    async def slow_cost(self, session_id):
        entered.set()
        await asyncio.Event().wait()
    monkeypatch.setattr(scorers, 'resolve_provider', lambda _: ProviderConfig('mindshub', 'http://test', 'SECRET', 'Test'))
    monkeypatch.setattr(ScriptedModel, 'session_cost', slow_cost)
    rid = reservation(judge, automatic=True)
    job = asyncio.create_task(scorers.execute(rid))
    await entered.wait()
    job.cancel()
    with pytest.raises(asyncio.CancelledError):
        await job
    with session_scope() as s:
        row = s.get(ScorerResult, rid)
        assert row.status == 'queued' and row.input_tokens == 1000 and not row.cost_known
    if credential_removed:
        def fail(_):
            raise RuntimeError('credential removed')
        monkeypatch.setattr(scorers, 'resolve_provider', fail)
    else:
        monkeypatch.setattr(scorers, 'resolve_provider', lambda _: ProviderConfig('openai', 'http://test', 'SECRET', 'Test'))
    await scorers.execute(rid)
    with session_scope() as s:
        row = s.get(ScorerResult, rid)
        assert row.input_tokens == (1000 if credential_removed else 2000)
        assert row.output_tokens == (200 if credential_removed else 400)
        assert not row.cost_known
        first = s.query(ScorerAttempt).filter_by(provider='mindshub').one()
        assert first.finished_at is not None
        assert first.provider == 'mindshub' and not first.cost_known
    async def priced(self, session_id):
        return .5
    monkeypatch.setattr(ScriptedModel, 'session_cost', priced)
    import aifactory.inference as inference
    monkeypatch.setattr(inference, 'ModelClient', ScriptedModel)
    monkeypatch.setattr(inference, 'resolve_provider', lambda _, **kwargs: ProviderConfig('mindshub', 'http://test', 'SECRET', 'Test'))
    await Orchestrator()._maybe_reconcile_costs()
    with session_scope() as s:
        row = s.get(ScorerResult, rid)
        assert row.cost_known and row.cost_usd == (.5 if credential_removed else .5014)
    assert outcome_cost(judge['outcome'])[1]


@pytest.mark.parametrize('state', ['queued', 'running'])
async def test_restart_recovers_reservations(judge, state):
    rid = reservation(judge, automatic=True)
    with session_scope() as s:
        row = s.get(ScorerResult, rid)
        row.status, row.started_at = state, utcnow() - timedelta(seconds=200)
    scorers.recover()
    await asyncio.gather(*list(scorers._jobs.values()))
    with session_scope() as s:
        assert s.get(ScorerResult, rid).status == 'succeeded'
        assert s.query(ScorerResult).count() == 1
    scorers.recover()
    assert not scorers._jobs


async def test_judge_is_bounded_and_tool_free(judge, monkeypatch):
    async def chat(self, **kwargs):
        assert kwargs['tools'] is None and kwargs['max_tokens'] == 2000
        assert 'SECRET' not in json.dumps(kwargs['messages'])
        await asyncio.sleep(10)
    monkeypatch.setattr(ScriptedModel, 'chat', chat)
    monkeypatch.setattr(scorers, 'JUDGE_TIMEOUT', .01)
    rid = reservation(judge)
    await scorers.execute(rid)
    with session_scope() as s:
        assert s.get(ScorerResult, rid).status == 'error'


async def test_slow_judge_does_not_block_delivery(judge, monkeypatch):
    from aifactory.harness import HarnessResult
    from aifactory.pipeline import runtime
    entered = asyncio.Event()
    async def slow(self, **kwargs):
        entered.set()
        await asyncio.Event().wait()
    class Harness:
        label = 'Test'
        async def run(self, session):
            return HarnessResult(report={'tasks': []}, turns=1)
    async def attachments(*args):
        return '', []
    monkeypatch.setattr(ScriptedModel, 'chat', slow)
    monkeypatch.setattr(runtime, 'ModelClient', ScriptedModel)
    monkeypatch.setattr(runtime, 'resolve_provider', scorers.resolve_provider)
    monkeypatch.setattr(runtime, 'get_harness', lambda _: Harness())
    monkeypatch.setattr(runtime, 'prepare_attachments', attachments)
    with session_scope() as s:
        ws = s.get(Workspace, judge['ws'])
        ws.settings = {**ws.settings, 'scorers': [scorers.BUILTIN.model_copy(update={'sample_rate': 1}).model_dump()]}
    spec = runtime.SessionSpec(judge['ws'], judge['outcome'], None, 'planner', 'native', 'prompt', 'system', None)
    run, result, error = await asyncio.wait_for(runtime.run_agent(spec, asyncio.Event()), .5)
    assert run.status == 'succeeded' and not error and result.report == {'tasks': []}
    scorers.recover()
    await asyncio.wait_for(entered.wait(), .5)
    assert not list(scorers._jobs.values())[0].done()
    await scorers.stop()


def test_judge_spend_in_stats_and_outcome_budget(client, judge):
    rid = reservation(judge)
    with session_scope() as s:
        row = s.get(ScorerResult, rid)
        row.cost_usd, row.cost_known = 2.5, False
        row.input_tokens, row.output_tokens = 400, 100
    stats = client.get(f"/api/workspaces/{judge['ws']}/stats").json()
    assert stats['cost']['usd'] == 3.5 and not stats['cost']['known']
    assert stats['cost']['per_completed_task'] == 3.5
    assert stats['tokens'] == 500
    assert outcome_cost(judge['outcome']) == (3.5, False)


async def test_explicit_judge_model_is_preserved(judge):
    with session_scope() as s:
        definition = scorers.BUILTIN.model_copy(update={'model': 'custom-judge'})
        rid = scorers.reserve(s, s.get(Run, judge['run']), definition).id
    await scorers.execute(rid)
    with session_scope() as s:
        assert s.query(ScorerAttempt).one().model == 'custom-judge'


async def test_hard_kill_retains_attempt_and_reconciles_after_retry(judge, monkeypatch):
    rid = reservation(judge, automatic=True)
    with session_scope() as s:
        row = s.get(ScorerResult, rid)
        row.status, row.started_at = 'running', utcnow() - timedelta(seconds=200)
        s.add(ScorerAttempt(result_id=rid, workspace_id=judge['ws'], provider='mindshub', model='gpt-terra',
                            input_tokens=1000, output_tokens=200, cost_known=False))
        s.flush()
        scorers.aggregate(s, rid)
    scorers.recover()
    await asyncio.gather(*list(scorers._jobs.values()))
    with session_scope() as s:
        row = s.get(ScorerResult, rid)
        assert row.input_tokens == 2000 and row.output_tokens == 400 and not row.cost_known
        assert all(a.finished_at for a in s.query(ScorerAttempt))


async def test_builtin_with_initialized_settings_and_own_openai_key(judge, monkeypatch):
    from aifactory.inference import resolve_provider
    from aifactory.models import Secret
    from aifactory.security import encrypt

    with session_scope() as s:
        assert s.get(Workspace, judge['ws']).settings['models']['default'] == 'gpt-terra'
        s.add(Secret(workspace_id=judge['ws'], kind='openai_api_key', ciphertext=encrypt('own-key')))
    monkeypatch.setattr(scorers, 'resolve_provider', resolve_provider)
    provider = resolve_provider(judge['ws'])
    assert provider.provider == 'openai' and provider.model_override is None

    async def chat(self, **kwargs):
        # A native OpenAI endpoint rejects the gateway-only alias.
        assert self.provider.api_key == 'own-key'
        assert kwargs['model'] == 'gpt-6.1-terra'
        return await original(self, **kwargs)

    original = ScriptedModel.chat
    monkeypatch.setattr(ScriptedModel, 'chat', chat)
    rid = reservation(judge)
    await scorers.execute(rid)
    with session_scope() as s:
        row = s.get(ScorerResult, rid)
        assert (row.status, row.classification, row.score, row.passed) == ('succeeded', 'good', 1, True)


@pytest.mark.parametrize('status', [400, 401, 402, 403, 404, 422, 429])
async def test_rejected_request_is_known_zero(judge, monkeypatch, status):
    from aifactory.inference import InferenceError

    async def reject(self, **kwargs):
        raise InferenceError('SECRET', status=status)

    monkeypatch.setattr(ScriptedModel, 'chat', reject)
    rid = reservation(judge)
    await scorers.execute(rid)
    with session_scope() as s:
        row = s.get(ScorerResult, rid)
        assert row.status == 'error' and row.cost_known and row.cost_usd == 0
        assert 'SECRET' not in row.error
    assert outcome_cost(judge['outcome']) == (1, True)


@pytest.mark.parametrize('credential_removed', [False, True])
async def test_delayed_cost_uses_recorded_provider_after_workspace_switch(judge, monkeypatch, credential_removed):
    import aifactory.inference as inference
    from aifactory.models import Secret
    from aifactory.security import encrypt

    rid = reservation(judge)
    with session_scope() as s:
        s.add(ScorerAttempt(result_id=rid, workspace_id=judge['ws'], provider='mindshub', model='gpt-terra',
                            cost_known=False, finished_at=utcnow()))
        s.add(Secret(workspace_id=judge['ws'], kind='openai_api_key', ciphertext=encrypt('openai-key')))
        if not credential_removed:
            s.add(Secret(workspace_id=judge['ws'], kind='mindshub_api_key', ciphertext=encrypt('mindshub-key')))
        s.flush()
        scorers.aggregate(s, rid)

    async def cost(self, session_id):
        assert self.provider.provider == 'mindshub' and self.provider.api_key == 'mindshub-key'
        return .25

    monkeypatch.setattr(ScriptedModel, 'session_cost', cost)
    monkeypatch.setattr(inference, 'ModelClient', ScriptedModel)
    await Orchestrator()._maybe_reconcile_costs()
    with session_scope() as s:
        row = s.get(ScorerResult, rid)
        assert row.cost_known == (not credential_removed)
        assert row.cost_usd == (0 if credential_removed else .25)


@pytest.mark.parametrize('failure', ['transport', 'timeout', 'interrupted', 'missing_usage', 'missing_price'])
async def test_openai_unknown_cost_has_honest_reconciliation_path(client, judge, monkeypatch, failure):
    from aifactory.inference import InferenceError
    from aifactory.models import Event

    async def chat(self, **kwargs):
        if failure == 'transport':
            raise InferenceError('post-dispatch transport error', retryable=True)
        if failure == 'timeout':
            raise TimeoutError()
        if failure == 'interrupted':
            raise asyncio.CancelledError()
        kwargs['ledger'].add(kwargs['model'], None if failure == 'missing_usage' else (1000, 200))
        return {'content': json.dumps({'classification': 'good', 'reasoning': 'Focused'})}

    if failure == 'missing_price':
        with session_scope() as s:
            ws = s.get(Workspace, judge['ws'])
            ws.settings = {**ws.settings, 'pricing': {}}
    monkeypatch.setattr(ScriptedModel, 'chat', chat)
    rid = reservation(judge)
    if failure == 'interrupted':
        with pytest.raises(asyncio.CancelledError):
            await scorers.execute(rid)
    else:
        await scorers.execute(rid)
    # Reconciliation must not invent a session lookup that OpenAI doesn't support,
    # nor price missing usage as zero even when model pricing is configured.
    await Orchestrator()._maybe_reconcile_costs()
    root = f"/api/workspaces/{judge['ws']}/runs/{judge['run']}/scores/{rid}/attempts"
    attempts = client.get(root).json()
    assert len(attempts) == 1
    attempt = attempts[0]
    assert attempt['provider'] == 'openai' and not attempt['cost_known']
    assert attempt['reconciliation_required']
    assert outcome_cost(judge['outcome']) == (1, False)
    response = client.post(root + f"/{attempt['id']}/cost", json={
        'cost_usd': .17, 'evidence': 'OpenAI billing export, isolated judge request'})
    assert response.status_code == 200
    assert response.json()['cost_known']
    assert response.json()['cost_reconciliation']['evidence'].startswith('OpenAI billing')
    assert outcome_cost(judge['outcome']) == (1.17, True)
    stats = client.get(f"/api/workspaces/{judge['ws']}/stats").json()
    assert stats['cost']['known'] and stats['cost']['usd'] == 1.17
    with session_scope() as s:
        event = s.query(Event).filter_by(kind='scorer_cost_reconciled').one()
        assert event.actor == response.json()['cost_reconciliation']['actor']
    assert client.post(root + f"/{attempt['id']}/cost", json={'cost_usd': 0, 'evidence': 'overwrite'}).status_code == 409


async def test_openai_missing_price_retries_recorded_model_with_known_usage(judge, monkeypatch):
    with session_scope() as s:
        ws = s.get(Workspace, judge['ws'])
        ws.settings = {**ws.settings, 'pricing': {}}
    rid = reservation(judge)
    await scorers.execute(rid)
    with session_scope() as s:
        row = s.get(ScorerResult, rid)
        assert not row.cost_known
        ws = s.get(Workspace, judge['ws'])
        # A provider/model switch cannot reattribute the original usage.
        ws.settings = {**ws.settings, 'provider': 'mindshub', 'models': {'default': 'new-model'},
                       'pricing': {'gpt-6.1-terra': {'input': 1, 'output': 2}}}
    def forbidden(*args, **kwargs):
        raise AssertionError('OpenAI usage repricing needs no credential or session lookup')
    import aifactory.inference as inference
    monkeypatch.setattr(inference, 'resolve_provider', forbidden)
    await Orchestrator()._maybe_reconcile_costs()
    with session_scope() as s:
        row = s.get(ScorerResult, rid)
        assert row.cost_known and row.cost_usd == .0014
    assert outcome_cost(judge['outcome']) == (1.0014, True)


@pytest.mark.parametrize('cost,evidence', [(-1, 'bill'), (1, ''), (1, '   ')])
def test_manual_cost_validation(client, judge, cost, evidence):
    root = f"/api/workspaces/{judge['ws']}/runs/{judge['run']}/scores/missing/attempts/missing/cost"
    assert client.post(root, json={'cost_usd': cost, 'evidence': evidence}).status_code == 422


async def test_cost_reconciliation_pages_past_unpriceable_attempts(judge, monkeypatch):
    import aifactory.inference as inference

    rid = reservation(judge)
    with session_scope() as s:
        # Neither OpenAI calls without usage nor attempts whose retry isn't due
        # may monopolize the limited provider lookup page.
        for _ in range(55):
            s.add(ScorerAttempt(result_id=rid, workspace_id=judge['ws'], provider='openai',
                                cost_known=False, finished_at=utcnow()))
        for _ in range(50):
            s.add(ScorerAttempt(result_id=rid, workspace_id=judge['ws'], provider='mindshub',
                                cost_known=False, finished_at=utcnow() - timedelta(days=10)))
        last = ScorerAttempt(result_id=rid, workspace_id=judge['ws'], provider='mindshub',
                             cost_known=False, finished_at=utcnow())
        s.add(last)
        s.flush()
        last_id = last.id
    lookups = []
    async def cost(self, session_id):
        lookups.append(session_id)
        return .23 if session_id == last_id else None
    monkeypatch.setattr(inference, 'resolve_provider', lambda _, **kw: ProviderConfig('mindshub', 'http://test', 'key', 'Test'))
    monkeypatch.setattr(inference, 'ModelClient', ScriptedModel)
    monkeypatch.setattr(ScriptedModel, 'session_cost', cost)
    await Orchestrator()._maybe_reconcile_costs()
    assert len(lookups) == 50 and last_id not in lookups
    with session_scope() as s:
        for a in s.query(ScorerAttempt).filter(ScorerAttempt.id.in_(lookups)):
            assert a.cost_retry_count == 1 and a.cost_retry_at > utcnow()
    await Orchestrator()._maybe_reconcile_costs()
    assert len(lookups) == 51 and lookups[-1] == last_id
    with session_scope() as s:
        assert s.get(ScorerAttempt, last_id).cost_known
        assert s.get(ScorerAttempt, last_id).cost_usd == .23


async def test_missing_credentials_back_off_without_losing_provider(judge, monkeypatch):
    import aifactory.inference as inference

    rid = reservation(judge)
    with session_scope() as s:
        a = ScorerAttempt(result_id=rid, workspace_id=judge['ws'], provider='mindshub',
                          model='original', cost_known=False, finished_at=utcnow())
        s.add(a)
        s.flush()
        aid = a.id
    def unavailable(ws, *, required_provider):
        assert required_provider == 'mindshub'
        raise RuntimeError('credential gone')
    monkeypatch.setattr(inference, 'resolve_provider', unavailable)
    await Orchestrator()._maybe_reconcile_costs()
    await Orchestrator()._maybe_reconcile_costs()
    with session_scope() as s:
        a = s.get(ScorerAttempt, aid)
        assert a.cost_retry_count == 1 and not a.cost_known
        assert a.provider == 'mindshub' and a.model == 'original'
        a.cost_retry_at = utcnow() - timedelta(seconds=1)
    await Orchestrator()._maybe_reconcile_costs()
    with session_scope() as s:
        a = s.get(ScorerAttempt, aid)
        assert a.cost_retry_count == 2
        assert a.cost_retry_at > utcnow() + timedelta(seconds=1100)


def test_runs_have_bounded_score_summaries_and_paginated_details(client, judge):
    with session_scope() as s:
        for n in range(250):
            s.add(ScorerResult(workspace_id=judge['ws'], run_id=judge['run'], scorer_id='planner-quality',
                               definition_version=str(n), definition={'instructions': 'D' * 16000},
                               status='succeeded', classification='good', score=1, passed=True,
                               reasoning='R' * 16000, cost_usd=.01, cost_known=n != 0,
                               created_at=utcnow() + timedelta(seconds=n)))
    root = f"/api/workspaces/{judge['ws']}"
    response = client.get(root + '/runs?limit=200')
    assert response.status_code == 200 and len(response.content) < 5000
    run = response.json()[0]
    assert len(run['scores']) == 1
    assert run['scores'][0]['definition_version'] == '249'
    assert 'definition' not in run['scores'][0] and 'reasoning' not in run['scores'][0]
    assert run['score_summary'] == {'count': 250, 'cost_usd': 2.5, 'cost_known': False}
    url = root + f"/runs/{judge['run']}/scores"
    first = client.get(url).json()
    second = client.get(url + '?offset=20').json()
    assert len(first) == len(second) == 20
    assert not ({s['id'] for s in first} & {s['id'] for s in second})
    assert first[0]['reasoning'] == 'R' * 16000
    assert first[0]['definition'] == {'instructions': 'D' * 16000}
    assert len(client.get(url + '?limit=50').json()) == 50
    assert client.get(url + '?limit=10000').status_code == 422
    assert client.get(url + '?offset=-1').status_code == 422


def test_historic_scorer_ids_are_bounded_in_runs(client, judge):
    with session_scope() as s:
        for n in range(100):
            s.add(ScorerResult(workspace_id=judge['ws'], run_id=judge['run'], scorer_id=f'historic-{n}',
                               definition_version='v', definition={}, reasoning='R' * 16000))
    response = client.get(f"/api/workspaces/{judge['ws']}/runs")
    assert len(response.content) < 20000
    run = response.json()[0]
    assert len(run['scores']) == 40 and run['score_summary']['count'] == 100


async def test_openai_repricing_backoff_does_not_starve_provider_lookup(judge, monkeypatch):
    import aifactory.inference as inference

    rid = reservation(judge)
    with session_scope() as s:
        for _ in range(50):
            s.add(ScorerAttempt(result_id=rid, workspace_id=judge['ws'], provider='openai',
                                model='unpriced-model', usage_known=True, input_tokens=100,
                                cost_known=False, finished_at=utcnow() - timedelta(days=10)))
        a = ScorerAttempt(result_id=rid, workspace_id=judge['ws'], provider='mindshub',
                          cost_known=False, finished_at=utcnow())
        s.add(a)
        s.flush()
        aid = a.id
    calls = []
    async def cost(self, session_id):
        calls.append(session_id)
        return .1
    monkeypatch.setattr(inference, 'resolve_provider', lambda _, **kw: ProviderConfig('mindshub', 'http://test', 'key', 'Test'))
    monkeypatch.setattr(inference, 'ModelClient', ScriptedModel)
    monkeypatch.setattr(ScriptedModel, 'session_cost', cost)
    await Orchestrator()._maybe_reconcile_costs()
    assert calls == []
    await Orchestrator()._maybe_reconcile_costs()
    assert calls == [aid]
    # Adding prices later must resolve old OpenAI charges, without credentials
    # or changing the recorded provider/model, even beyond seven days.
    with session_scope() as s:
        ws = s.get(Workspace, judge['ws'])
        ws.settings = {**ws.settings, 'pricing': {'unpriced-model': {'input': 1, 'output': 2}}}
        for a in s.query(ScorerAttempt).filter_by(provider='openai'):
            a.cost_retry_at = utcnow() - timedelta(seconds=1)
    await Orchestrator()._maybe_reconcile_costs()
    with session_scope() as s:
        assert all(a.cost_known for a in s.query(ScorerAttempt))
        assert s.get(ScorerResult, rid).cost_usd == .105


def test_runs_negative_limit_cannot_disable_payload_bound(client, judge):
    with session_scope() as s:
        for _ in range(3):
            s.add(Run(workspace_id=judge['ws'], outcome_id=judge['outcome'], role='planner',
                      harness='native', status='succeeded'))
    assert len(client.get(f"/api/workspaces/{judge['ws']}/runs?limit=-1").json()) == 1


@pytest.mark.parametrize('endpoint', ['stats', 'outcome_cost'])
@pytest.mark.parametrize('unknown', [False, True])
def test_usage_hot_paths_aggregate_score_history_in_sql(client, judge, endpoint, unknown):
    from sqlalchemy import event

    with session_scope() as s:
        engine = s.get_bind()
        for n in range(120):
            s.add(ScorerResult(workspace_id=judge['ws'], run_id=judge['run'], scorer_id='planner-quality',
                               definition_version=str(n), definition={'instructions': 'D' * 16000},
                               reasoning='R' * 16000, cost_usd=.125, cost_known=not (unknown and n == 0),
                               input_tokens=10, output_tokens=5))
        other = Outcome(workspace_id=judge['ws'], number=2, title='Unrelated')
        s.add(other)
        s.flush()
        other_run = Run(workspace_id=judge['ws'], outcome_id=other.id, role='planner', harness='native',
                        status='succeeded', cost_usd=2)
        s.add(other_run)
        s.flush()
        # Only the selected outcome belongs in its budget; the workspace includes both.
        s.add(ScorerResult(workspace_id=judge['ws'], run_id=other_run.id, scorer_id='planner-quality',
                           definition_version='other', definition={}, cost_usd=3, cost_known=True,
                           input_tokens=7, output_tokens=3))
        other_id = other.id
    statements = []
    def capture(conn, cursor, statement, parameters, context, executemany):
        if statement.lstrip().upper().startswith('SELECT'):
            statements.append(statement.lower())
    event.listen(engine, 'before_cursor_execute', capture)
    try:
        if endpoint == 'stats':
            response = client.get(f"/api/workspaces/{judge['ws']}/stats")
            assert response.status_code == 200
            data = response.json()
            assert data['cost'] == {'usd': 21, 'known': not unknown, 'per_completed_task': 21}
            assert data['tokens'] == 1810
        else:
            assert outcome_cost(judge['outcome']) == (16, not unknown)
    finally:
        event.remove(engine, 'before_cursor_execute', capture)
    score_queries = [sql for sql in statements if 'from scorer_results' in sql]
    assert len(score_queries) == 1
    assert 'sum(scorer_results.cost_usd)' in score_queries[0]
    assert 'count(scorer_results.id) filter' in score_queries[0]
    assert 'scorer_results.definition' not in score_queries[0]
    assert 'scorer_results.reasoning' not in score_queries[0]
    assert outcome_cost(other_id) == (5, True)
    assert outcome_cost('nonexistent') == (0, True)


def test_empty_workspace_score_usage_is_known_zero(client, judge):
    with session_scope() as s:
        s.delete(s.get(Run, judge['run']))
    response = client.get(f"/api/workspaces/{judge['ws']}/stats")
    assert response.status_code == 200
    assert response.json()['cost'] == {'usd': 0, 'known': True, 'per_completed_task': 0}
    assert response.json()['tokens'] == 0


def test_runs_ui_score_permissions_applicability_and_errors(client, judge):
    from aifactory.models import Membership

    root = f"/api/workspaces/{judge['ws']}"
    path = root + f"/runs/{judge['run']}/scores"
    # Successful HTTP transport can carry an error verdict; the details remain readable.
    ScriptedModel.scripts['scorer'] = lambda _: {'content': json.dumps({'classification': 'not-configured'})}
    response = client.post(path, json={'scorer_id': 'planner-quality'})
    assert response.status_code == 200
    score = response.json()
    assert score['status'] == 'error' and score['error']
    assert score['classification'] is None and score['score'] is None and score['passed'] is None
    assert client.get(path).json()[0] == score
    assert client.get(root + '/runs').json()[0]['status'] == 'succeeded'

    with session_scope() as s:
        membership = s.query(Membership).filter_by(workspace_id=judge['ws']).one()
        membership.role = 'viewer'
    assert client.get(root + '/scorers').status_code == 200
    assert client.get(path).json()[0]['error'] == score['error']
    assert client.post(path, json={'scorer_id': 'planner-quality'}).status_code == 403
    with session_scope() as s:
        assert s.query(ScorerResult).count() == 1
        s.query(Membership).filter_by(workspace_id=judge['ws']).one().role = 'member'
        s.get(Run, judge['run']).role = 'builder'
    response = client.post(path, json={'scorer_id': 'planner-quality'})
    assert response.status_code == 422
    assert 'applicable role' in response.json()['detail']
    with session_scope() as s:
        run = s.get(Run, judge['run'])
        run.role, run.status = 'planner', 'running'
    assert client.post(path, json={'scorer_id': 'planner-quality'}).status_code == 422
    with session_scope() as s:
        assert s.query(ScorerResult).count() == 1
