"""Pinned runtime provenance, tool evidence, authorization and lazy query contracts."""
from __future__ import annotations

import asyncio
import json
from dataclasses import replace

import pytest
from sqlalchemy import event, inspect
from sqlalchemy.orm import selectinload

from aifactory import db, prompts, serializers
from aifactory.definitions import Bundle, Skill, RoleInstruction, KnowledgePage, save_bundle, restore_revision, DefinitionError
from aifactory.definitions.sessions import pin_definitions, append_instructions
from aifactory.harness import HarnessResult
from aifactory.harness.tools import ToolBox
from aifactory.inference import ProviderConfig
from aifactory.models import Event, Outcome, Task, Plan, Repository, Run, RunDefinition, Workspace
from aifactory.pipeline import runtime


@pytest.fixture
def setup(signed_in, monkeypatch):
    ws = signed_in['workspaces'][0]['id']
    with db.session_scope() as s:
        repo = Repository(workspace_id=ws, full_name='org/one', config={'context': 'old notes'})
        second = Repository(workspace_id=ws, full_name='org/two', config={})
        s.add_all([repo, second])
        s.flush()
        outcome = Outcome(workspace_id=ws, number=1, title='example', repo_ids=[repo.id, second.id])
        s.add(outcome)
        s.flush()
        plan = Plan(outcome_id=outcome.id, version=1)
        s.add(plan)
        s.flush()
        task = Task(workspace_id=ws, outcome_id=outcome.id, plan_id=plan.id, repo_id=repo.id, key='T1', title='example')
        s.add(task)
        s.flush()
        bundle = Bundle(skills=[Skill(id='shared', name='shared', description='when testing', body='x' * 65536,
                                      files={'docs/a.txt': 'support'}),
                                Skill(id='excluded', name='excluded', description='not now', body='secret'),
                                Skill(id='other-repo', name='other-repo', description='other', body='other', repository_id=second.id)],
                        knowledge=[KnowledgePage(id='page', name='page', description='arch', body='architecture')],
                        instructions=[RoleInstruction(id=f'{role}-{scope}', name=f'{role}-{scope}',
                                                      description='instructions', body=f'{role} {scope}', role=role,
                                                      repository_id=repo.id if scope == 'repository' else None)
                                      for role in ('planner', 'builder', 'verifier') for scope in ('workspace', 'repository')])
        save_bundle(s, ws, bundle, expected_revision=0, author='test')
        spec = runtime.SessionSpec(ws, outcome.id, task.id, 'builder', 'native', 'work', prompts.BUILDER, None,
                                   excluded_skill_ids=[f'{ws}/skill/excluded'])
        ids = (repo.id, second.id)
    provider = ProviderConfig('openai', 'http://model.test', 'key', 'Test')
    monkeypatch.setattr(runtime, 'resolve_provider', lambda *args, **kw: provider)
    async def attachments(*args):
        return '', []
    monkeypatch.setattr(runtime, 'prepare_attachments', attachments)
    monkeypatch.setattr(runtime, 'configuration_version', lambda **kw: ('old', {'system': kw['system']}))
    return spec, ids


@pytest.mark.parametrize('role', ['planner', 'builder', 'verifier', 'plan_reviewer'])
def test_scope_append_order_and_base_prompt(setup, role):
    spec, repos = setup
    system = {'planner': prompts.PLANNER, 'builder': prompts.BUILDER, 'verifier': prompts.VERIFIER,
              'plan_reviewer': 'review base'}[role]
    spec = replace(spec, role=role, task_id=None if role in ('planner', 'plan_reviewer') else spec.task_id)
    with db.session_scope() as s:
        context = pin_definitions(s, spec)
    result = append_instructions(system, context, 'native')
    assert result.startswith(system + '\n\n')
    effective_role = 'planner' if role == 'plan_reviewer' else role
    assert result.index(f'{effective_role} workspace') < result.index(f'{effective_role} repository') < result.index('Applicable skills')
    assert 'Repository role instructions: org/one' in result
    assert 'x' * 100 not in result and 'support' not in json.dumps(context['skills'][f'{spec.workspace_id}/skill/shared']['source'])
    assert f'{spec.workspace_id}/skill/excluded' not in result
    assert (f'{spec.workspace_id}/skill/other-repo' in result) == (effective_role == 'planner')
    codex = append_instructions(system, context, 'codex')
    assert 'read_skill' not in codex and 'search_knowledge' not in codex and 'Applicable skills' not in codex
    assert f'{effective_role} workspace' in codex


@pytest.mark.parametrize('role', ['builder', 'verifier'])
def test_taskless_sessions_receive_only_workspace_definitions(setup, role):
    spec, _ = setup
    with db.session_scope() as s:
        context = pin_definitions(s, replace(spec, role=role, task_id=None))
    assert context['repository_ids'] == []
    assert [item['body'] for item in context['instructions']] == [f'{role} workspace']
    assert set(context['skills']) == {f'{spec.workspace_id}/skill/shared'}
    assert set(context['knowledge']) == {f'{spec.workspace_id}/knowledge/page'}
    assert all(item['repository_id'] is None for kind in ('skills', 'knowledge')
               for item in context[kind].values())


async def test_runtime_freezes_restore_notes_and_records_every_body_load(setup, monkeypatch, client):
    spec, repos = setup
    ws = spec.workspace_id
    from types import SimpleNamespace
    clock = {"now": 10.0}
    monkeypatch.setattr(runtime, "time", SimpleNamespace(monotonic=lambda: clock["now"]))
    seen = []
    class Harness:
        label = 'Script'
        async def run(self, session):
            seen.append(session)
            toolbox = ToolBox(session)
            assert not session.loaded_skills
            notes_id = f'{ws}/knowledge/repository-context-{repos[0]}'
            with db.session_scope() as s:
                new = Bundle(skills=[Skill(id='shared', name='shared', description='changed', body='new body')])
                save_bundle(s, ws, new, expected_revision=1, author='other')
                restore_revision(s, ws, 1, expected_revision=2, author='restore')
                repo = s.get(Repository, repos[0])
                repo.config = {'context': 'changed notes'}
            session.on_progress('tool', 'progress', {})
            assert json.loads(await toolbox.call('search_knowledge', {'page_id': notes_id}))['body'] == 'old notes'
            assert len(json.loads(await toolbox.call('search_knowledge', {}))['pages']) == 2
            assert not session.loaded_skills
            for args in ({'skill_id': f'{ws}/skill/excluded'}, {'skill_id': f'{ws}/skill/other-repo'},
                         {'skill_id': 'foreign/skill/shared'}):
                assert (await toolbox.call('read_skill', args)).startswith('Error:')
            assert await toolbox.call('read_skill', {'skill_id': f'{ws}/skill/shared', 'path': 'docs/a.txt'})
            assert not session.loaded_skills
            clock['now'] = 15.0
            for _ in range(2):
                assert await toolbox.call('read_skill', {'skill_id': f'{ws}/skill/shared'}) == 'x' * 65536
            session.on_progress('tool', 'progress after skill loads', {})
            return HarnessResult(report={}, turns=1)
    monkeypatch.setattr(runtime, 'get_harness', lambda _: Harness())
    run, _, error = await runtime.run_agent(spec, asyncio.Event())
    assert not error
    data = serializers.run(run)
    assert data['definition_revision'] == 1
    assert len(data['applicable_skills']) == len(data['loaded_skills']) == 1
    assert data['definition_provenance']['excluded_skill_ids'] == list(spec.excluded_skill_ids)
    assert len(data['applied_instructions']) == 2
    with db.session_scope() as s:
        events = s.query(Event).filter_by(run_id=run.id, kind='agent_skill_loaded').all()
        assert len(events) == 2
        progress = s.query(Event).filter_by(run_id=run.id, kind='agent_tool').all()
        assert [e.message for e in progress] == ['progress', 'progress after skill loads']
        assert all(e.data['revision'] == 1 and e.data['source'] == {'kind': 'managed', 'repository_id': None,
                                                                 'path': None, 'commit': None} for e in events)
        pinned = s.get(RunDefinition, run.id)
        assert pinned.snapshot['knowledge'][f'{ws}/knowledge/repository-context-{repos[0]}']['body'] == 'old notes'
    response = client.get(f'/api/workspaces/{ws}/runs').json()[0]
    assert response['loaded_skills'] == data['loaded_skills']
    outcome_run = client.get(f'/api/workspaces/{ws}/outcomes/{spec.outcome_id}').json()['runs'][0]
    for collection_run in (response, outcome_run):
        assert collection_run['applied_instructions'] == data['applied_instructions']
        assert all('body' not in item for item in collection_run['applied_instructions'])
    detail_response = client.get(f'/api/workspaces/{ws}/runs/{run.id}')
    assert detail_response.status_code == 200
    detail = detail_response.json()
    assert detail == serializers.run(run, include_instruction_content=True)
    assert [item['body'] for item in detail['applied_instructions']] == ['builder workspace', 'builder repository']
    assert [{k: v for k, v in item.items() if k != 'body'} for item in detail['applied_instructions']] == data['applied_instructions']
    # Serializing a collection must not mutate the pinned instruction snapshot.
    with db.session_scope() as s:
        assert s.get(RunDefinition, run.id).instructions == detail['applied_instructions']
    assert client.get(f'/api/workspaces/{ws}/runs/missing').status_code == 404
    with db.session_scope() as s:
        foreign_ws = Workspace(org_id=s.get(Workspace, ws).org_id, name='Foreign', slug='foreign')
        s.add(foreign_ws)
        s.flush()
        foreign_outcome = Outcome(workspace_id=foreign_ws.id, number=1, title='Foreign')
        s.add(foreign_outcome)
        s.flush()
        foreign = Run(workspace_id=foreign_ws.id, outcome_id=foreign_outcome.id, role='builder', harness='native')
        s.add(foreign)
        s.flush()
        foreign_id = foreign.id
    assert client.get(f'/api/workspaces/{ws}/runs/{foreign_id}').status_code == 404


async def test_configuration_tracks_all_effective_definitions_and_exclusions(setup, monkeypatch):
    spec, repos = setup
    class Harness:
        label = 'Script'
        async def run(self, session):
            return HarnessResult(report={}, turns=1)
    monkeypatch.setattr(runtime, 'get_harness', lambda _: Harness())
    async def version(current):
        run, _, error = await runtime.run_agent(current, asyncio.Event())
        assert not error
        return run.config_version
    first = await version(spec)
    assert first == await version(spec)
    assert first != await version(replace(spec, excluded_skill_ids=()))
    with db.session_scope() as s:
        s.get(Repository, repos[0]).config = {'context': 'new notes'}
    assert first != await version(spec)
    with db.session_scope() as s:
        bundle = Bundle(skills=[Skill(name='new', description='when new', body='new')])
        save_bundle(s, spec.workspace_id, bundle, expected_revision=1, author='test')
    assert first != await version(spec)


def test_fail_closed_ownership_and_lazy_snapshots(setup, client):
    spec, repos = setup
    with db.session_scope() as s:
        for invalid in (replace(spec, workspace_id='foreign'), replace(spec, task_id='missing')):
            with pytest.raises(DefinitionError):
                pin_definitions(s, invalid)
        run = Run(workspace_id=spec.workspace_id, outcome_id=spec.outcome_id, role='builder', harness='native')
        s.add(run)
        s.flush()
        assert serializers.run(run)['definition_revision'] is None
        for key in ('applicable_skills', 'loaded_skills', 'applied_instructions'):
            assert serializers.run(run)[key] == []
        historical_id = run.id
    response = client.get(f'/api/workspaces/{spec.workspace_id}/runs/{historical_id}')
    assert response.status_code == 200
    assert response.json()['definition_revision'] is None
    for key in ('applicable_skills', 'loaded_skills', 'applied_instructions'):
        assert response.json()[key] == []
    assert response.json()['definition_provenance'] == {}
    with db.session_scope() as s:
        s.add(RunDefinition(run_id=historical_id, revision=1, snapshot={'large': 'x' * 1000000}))
    statements = []
    engine = db.init_engine()
    def capture(conn, cursor, statement, parameters, context, executemany):
        statements.append(statement)
    event.listen(engine, 'before_cursor_execute', capture)
    try:
        with db.session_scope() as s:
            runs = s.query(Run).all()
            assert 'definition' in inspect(runs[0]).unloaded
            assert not any('run_definitions' in q for q in statements)
        statements.clear()
        with db.session_scope() as s:
            run = s.query(Run).options(selectinload(Run.definition)).one()
            assert serializers.run(run)['definition_revision'] == 1
            assert 'snapshot' in inspect(run.definition).unloaded
            assert not any('run_definitions.snapshot' in q for q in statements)
    finally:
        event.remove(engine, 'before_cursor_execute', capture)


def test_collection_queries_do_not_select_instruction_bodies(setup, client):
    spec, _ = setup
    metadata = {'id': 'test', 'label': 'Workspace role instructions (builder)', 'source': {'kind': 'managed'}}
    instructions = [{**metadata, 'body': 'large instruction body' * 10000}]
    with db.session_scope() as s:
        run = Run(workspace_id=spec.workspace_id, outcome_id=spec.outcome_id, role='builder', harness='native')
        s.add(run)
        s.flush()
        run_id = run.id
        s.add(RunDefinition(run_id=run_id, revision=1, snapshot={'instructions': instructions},
                            instructions=instructions, instruction_metadata=[metadata]))
    statements = []
    engine = db._engine

    def capture(conn, cursor, statement, parameters, context, executemany):
        statements.append(statement)

    event.listen(engine, 'before_cursor_execute', capture)
    try:
        for path in (f'/api/workspaces/{spec.workspace_id}/runs',
                     f'/api/workspaces/{spec.workspace_id}/outcomes/{spec.outcome_id}'):
            statements.clear()
            response = client.get(path)
            assert response.status_code == 200
            data = response.json()
            runs = data if isinstance(data, list) else data['runs']
            assert runs[0]['applied_instructions'] == [metadata]
            queries = [sql for sql in statements if 'FROM run_definitions' in sql]
            assert len(queries) == 1
            assert 'run_definitions.instruction_metadata' in queries[0]
            assert 'run_definitions.instructions' not in queries[0]
            assert 'run_definitions.snapshot' not in queries[0]
        statements.clear()
        response = client.get(f'/api/workspaces/{spec.workspace_id}/runs/{run_id}')
        assert response.status_code == 200
        assert response.json()['applied_instructions'] == instructions
        queries = [sql for sql in statements if 'FROM run_definitions' in sql]
        assert len(queries) == 1 and 'run_definitions.instructions' in queries[0]
        assert 'run_definitions.snapshot' not in queries[0]
    finally:
        event.remove(engine, 'before_cursor_execute', capture)


async def test_codex_configuration_hashes_only_consumable_instructions(setup, monkeypatch):
    spec, repos = setup
    spec = replace(spec, harness='codex')
    sessions = []

    class Harness:
        label = 'Script Codex'
        async def run(self, session):
            sessions.append(session)
            assert 'read_skill' not in session.system and 'search_knowledge' not in session.system
            return HarnessResult(report={}, turns=1)

    monkeypatch.setattr(runtime, 'get_harness', lambda _: Harness())
    monkeypatch.setattr(runtime, 'issue', lambda _: '')

    async def run(current=spec):
        row, _, error = await runtime.run_agent(current, asyncio.Event())
        assert not error
        return row

    first = await run()
    assert first.config_version == (await run(replace(spec, excluded_skill_ids=()))).config_version
    with db.session_scope() as s:
        from aifactory.definitions.store import get_snapshot
        bundle = get_snapshot(s, spec.workspace_id).bundle.model_copy(deep=True)
        bundle.skills[0].body = 'updated unused skill'
        bundle.knowledge[0].body = 'updated unused knowledge'
        save_bundle(s, spec.workspace_id, bundle, expected_revision=1, author='test')
        s.get(Repository, repos[0]).config = {'context': 'updated unused team notes'}
    second = await run()
    assert first.config_version == second.config_version
    assert serializers.run(first)['definition_revision'] == 1
    assert serializers.run(second)['definition_revision'] == 2
    # Pinning still retains the actual snapshot and exclusions for audit/future adapters.
    assert json.loads(sessions[-1].definition_context)['skills'][f'{spec.workspace_id}/skill/shared']['body'] == 'updated unused skill'
    with db.session_scope() as s:
        bundle.instructions[0].body = 'updated planner instruction outside builder scope'
        save_bundle(s, spec.workspace_id, bundle, expected_revision=2, author='test')
    assert first.config_version == (await run()).config_version
    with db.session_scope() as s:
        next(item for item in bundle.instructions if item.role == 'builder').body = 'updated consumed instruction'
        save_bundle(s, spec.workspace_id, bundle, expected_revision=3, author='test')
    assert first.config_version != (await run()).config_version


@pytest.mark.parametrize('harness', ['native', 'anton', 'codex'])
@pytest.mark.parametrize('large_field', ['instructions', 'catalogue', 'base_system'])
def test_appended_definitions_fail_closed_at_prompt_budget(setup, harness, large_field):
    from aifactory.definitions.sessions import DEFINITION_PROMPT_BYTE_BUDGET, SYSTEM_CHAR_BUDGET
    spec, _ = setup
    with db.session_scope() as s:
        context = pin_definitions(s, spec)
    system = spec.system
    if large_field == 'instructions':
        context['instructions'][0]['body'] = 'x' * DEFINITION_PROMPT_BYTE_BUDGET
    elif large_field == 'catalogue':
        # Exercise the serialized size, including escaped control characters.
        context['skills'][f'{spec.workspace_id}/skill/shared']['description'] = '\x01' * (DEFINITION_PROMPT_BYTE_BUDGET // 5)
    else:
        system = 'base' * (SYSTEM_CHAR_BUDGET // 4)
    if harness == 'codex' and large_field == 'catalogue':
        assert append_instructions(system, context, harness).startswith(system)
    else:
        with pytest.raises(DefinitionError, match='session prompt budget'):
            append_instructions(system, context, harness)


def test_prompt_budget_counts_utf8_and_preserves_full_instructions(setup):
    from aifactory.definitions.sessions import DEFINITION_PROMPT_BYTE_BUDGET
    spec, _ = setup
    with db.session_scope() as s:
        context = pin_definitions(s, spec)
    body = 'é' * (DEFINITION_PROMPT_BYTE_BUDGET // 4)
    context['instructions'][0]['body'] = body
    result = append_instructions(spec.system, context, 'native')
    assert result.startswith(spec.system + '\n\n') and body in result
    context['instructions'][0]['body'] = body * 2
    with pytest.raises(DefinitionError, match='session prompt budget'):
        append_instructions(spec.system, context, 'native')
