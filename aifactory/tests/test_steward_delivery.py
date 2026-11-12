"""Steward candidates use ordinary delivery, not smoke-recovery's direct revert."""
from __future__ import annotations

import ast
import asyncio
import inspect

import pytest

from aifactory.db import session_scope, utcnow
from aifactory.models import Decision, Outcome, Plan, Repository, Run, Task
from aifactory.pipeline import delivery
from aifactory import merge_manager
from aifactory.gitops import extract_patch

from .fakes import FakeGitHub, ScriptedModel, call, git, step
from .test_pipeline import factory, new_outcome, _runner, prompt_criteria  # shared local remote/model conventions


def land(factory, path, content):
    seed = factory['remotes'] / 'seed/acme/demo'
    target = seed / path
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(content)
    git('add', '-A', cwd=seed)
    git('commit', '-qm', 'change ' + path, cwd=seed)
    git('push', '-q', str(factory['remotes'] / 'acme/demo.git'), 'main', cwd=seed)
    return git('rev-parse', 'HEAD', cwd=seed)


def task(factory, sha, action='revert'):
    outcome_id = new_outcome(factory)
    with session_scope() as s:
        s.get(Outcome, outcome_id).status = 'executing'
        plan = Plan(outcome_id=outcome_id, version=1, status='approved', content={})
        s.add(plan)
        s.flush()
        row = Task(workspace_id=factory['ws'], outcome_id=outcome_id, plan_id=plan.id, key='T1',
                   title='Repair main', repo_id=factory['repo'], status='running',
                   evidence={'steward': {'episode_id': 'episode-1', 'action': action,
                                         'authorized': True, 'culprit_sha': sha}})
        s.add(row)
        s.flush()
        return row.id


def run(task_id):
    asyncio.run(_runner(task_id).run())


def passing_verifier(messages):
    if step(messages) == 0:
        assert delivery.STEWARD_TEST_CRITERION in str(messages)
        return {'content': '', 'tool_calls': [call('run', {'command': 'sh test.sh'})]}
    return {'content': '', 'tool_calls': [call('finish', {
        'verdict': 'pass', 'summary': 'Exact inverse checked', 'findings': [],
        'criteria': [{'criterion': criterion, 'status': 'pass', 'evidence': 'Exact inverse and tests inspected'}
                     for criterion in prompt_criteria(messages)]})]}


def prepared_revert(factory):
    land(factory, 'greet.sh', "echo 'Hello, Factory!'\n")
    sha = land(factory, 'greet.sh', "echo 'broken'\n")
    ScriptedModel.scripts['verifier'] = passing_verifier
    return task(factory, sha), sha


def test_revert_creates_pr_checks_and_fresh_verifier_preserves_original_merge(factory):
    task_id, sha = prepared_revert(factory)
    # An original Factory merge record belongs to a different task and must never be changed.
    original = task(factory, sha, 'fix')
    with session_scope() as s:
        s.get(Task, original).merge_sha = sha
        s.get(Task, original).status = 'completed'
    run(task_id)
    with session_scope() as s:
        row = s.get(Task, task_id)
        assert row.status == 'waiting_ci' and row.pr_number == 1 and not row.merge_sha
        assert row.evidence['steward_candidate']['identity']['culprit_sha'] == sha
        assert row.evidence['attempts'][-1]['checks'][0]['ok']
        assert s.query(Run).filter_by(task_id=task_id, role='verifier').count() == 1
        assert s.query(Run).filter_by(task_id=task_id, role='builder').count() == 0
        assert s.get(Task, original).merge_sha == sha
    assert not FakeGitHub.merged
    assert git('rev-parse', 'main', cwd=factory['remotes'] / 'acme/demo.git') == sha
    run(task_id)  # a restart while CI is pending never republishes
    assert len(FakeGitHub.pulls) == 1
    asyncio.run(_runner(task_id).poll_ci())
    run(task_id)
    assert FakeGitHub.merged == [1]


def test_revert_merge_policy_requires_approval(factory):
    task_id, _ = prepared_revert(factory)
    with session_scope() as s:
        repo = s.get(Repository, factory['repo'])
        repo.config = {**repo.config, 'merge': {'mode': 'approval'}}
    run(task_id)
    asyncio.run(_runner(task_id).poll_ci())
    run(task_id)
    with session_scope() as s:
        assert s.get(Task, task_id).status == 'awaiting_approval'
        assert s.query(Decision).filter_by(task_id=task_id, kind='approve_merge', status='open').count() == 1
    assert not FakeGitHub.merged


@pytest.mark.parametrize('state', ['pending', 'failure'])
def test_ci_must_still_pass_at_merge(factory, state):
    task_id, _ = prepared_revert(factory)
    run(task_id)
    asyncio.run(_runner(task_id).poll_ci())
    FakeGitHub.check_state = state
    run(task_id)
    assert not FakeGitHub.merged
    with session_scope() as s:
        assert s.get(Task, task_id).status == 'waiting_ci'


@pytest.mark.parametrize('mode', ['conflict', 'empty', 'nonancestor', 'merge', 'unauthorized', 'protected'])
def test_unsafe_reverts_need_human_decision(factory, mode):
    task_id, sha = prepared_revert(factory)
    seed = factory['remotes'] / 'seed/acme/demo'
    if mode == 'conflict':
        land(factory, 'greet.sh', "echo 'newer'\n")
    elif mode == 'empty':
        git('revert', '--no-edit', sha, cwd=seed)
        git('push', '-q', str(factory['remotes'] / 'acme/demo.git'), 'main', cwd=seed)
    elif mode == 'nonancestor':
        git('checkout', '-qb', 'elsewhere', cwd=seed)
        (seed / 'else.txt').write_text('side\n')
        git('add', '-A', cwd=seed)
        git('commit', '-qm', 'side', cwd=seed)
        sha = git('rev-parse', 'HEAD', cwd=seed)
        git('push', '-q', str(factory['remotes'] / 'acme/demo.git'), 'elsewhere', cwd=seed)
    elif mode == 'merge':
        git('checkout', '-qb', 'side', cwd=seed)
        (seed / 'side.txt').write_text('side\n')
        git('add', '-A', cwd=seed)
        git('commit', '-qm', 'side', cwd=seed)
        git('checkout', '-q', 'main', cwd=seed)
        git('merge', '--no-ff', '-m', 'merge side', 'side', cwd=seed)
        git('push', '-q', str(factory['remotes'] / 'acme/demo.git'), 'main', cwd=seed)
        sha = git('rev-parse', 'HEAD', cwd=seed)
    with session_scope() as s:
        row = s.get(Task, task_id)
        row.evidence = {'steward': {**row.evidence['steward'], 'culprit_sha': sha,
                                    'authorized': mode != 'unauthorized'}}
        if mode == 'protected':
            repo = s.get(Repository, factory['repo'])
            repo.config = {**repo.config, 'protected_paths': ['greet.sh']}
    run(task_id)
    with session_scope() as s:
        row = s.get(Task, task_id)
        assert row.status == 'needs_input', row.blocked_reason
        assert not row.pr_number and not row.merge_sha
        decision = s.query(Decision).filter_by(task_id=task_id, status='open').one()
        decision_id = decision.id
    from aifactory.decisions import resolve
    from aifactory.models import User
    with session_scope() as s:
        resolve(s, s.get(Decision, decision_id), s.get(User, factory['user']), 'answer', 'Review and retry repair')
        assert s.get(Task, task_id).status == 'queued'
        assert s.query(Decision).filter_by(task_id=task_id, status='open').count() == 0
    assert not FakeGitHub.pulls


def test_stale_pr_head_cannot_merge(factory):
    task_id, _ = prepared_revert(factory)
    run(task_id)
    asyncio.run(_runner(task_id).poll_ci())
    with session_scope() as s:
        branch = s.get(Task, task_id).branch
    seed = factory['remotes'] / 'seed/acme/demo'
    git('push', '-q', '-f', str(factory['remotes'] / 'acme/demo.git'), 'main:' + branch, cwd=seed)
    run(task_id)
    assert not FakeGitHub.merged
    with session_scope() as s:
        assert 'head' in s.get(Task, task_id).blocked_reason


@pytest.mark.parametrize('action', ['fix', 'revert'])
@pytest.mark.parametrize('change', ['culprit_sha', 'action', 'episode_id'])
def test_restart_candidate_is_bound_to_authorization(factory, monkeypatch, action, change):
    task_id, _ = prepared_revert(factory)
    with session_scope() as s:
        row = s.get(Task, task_id)
        row.evidence = {'steward': {**row.evidence['steward'], 'action': action}}
    original = delivery.TaskRunner._publish
    async def interrupt(self, token):
        raise asyncio.CancelledError()
    monkeypatch.setattr(delivery.TaskRunner, '_publish', interrupt)
    with pytest.raises(asyncio.CancelledError):
        run(task_id)
    with session_scope() as s:
        row = s.get(Task, task_id)
        meta = row.evidence['steward']
        replacement = {'culprit_sha': 'a' * 40, 'episode_id': 'episode-2',
                       'action': 'fix' if action == 'revert' else 'revert'}[change]
        row.evidence = {**row.evidence, 'steward': {**meta, change: replacement}}
    runner = _runner(task_id)
    assert not runner._restore_verified_candidate()
    monkeypatch.setattr(delivery.TaskRunner, '_publish', original)
    assert not FakeGitHub.pulls


@pytest.mark.parametrize('action', ['fix', 'revert'])
@pytest.mark.parametrize('change', ['episode_id', 'culprit_sha', 'action'])
def test_authorization_change_during_preparation_blocks_before_build(factory, monkeypatch, action, change):
    task_id, _ = prepared_revert(factory)
    with session_scope() as s:
        row = s.get(Task, task_id)
        row.evidence = {'steward': {**row.evidence['steward'], 'action': action}}
    original = delivery.TaskRunner._sections

    async def changed(self, verifier=False):
        sections = await original(self, verifier)
        with session_scope() as s:
            row = s.get(Task, task_id)
            replacement = {'episode_id': 'episode-2', 'culprit_sha': 'a' * 40,
                           'action': 'fix' if action == 'revert' else 'revert'}[change]
            row.evidence = {**row.evidence, 'steward': {**row.evidence['steward'], change: replacement}}
        return sections

    monkeypatch.setattr(delivery.TaskRunner, '_sections', changed)
    run(task_id)
    with session_scope() as s:
        row = s.get(Task, task_id)
        assert row.status == 'needs_input' and not row.pr_number and not row.merge_sha
        decision = s.query(Decision).filter_by(task_id=task_id, status='open').one()
        assert decision.title == 'Steward authorization changed during preparation'
        assert s.query(Run).filter_by(task_id=task_id).count() == 0
    assert not FakeGitHub.pulls


def test_restart_after_pr_side_effect_does_not_duplicate(factory, monkeypatch):
    task_id, _ = prepared_revert(factory)
    original = FakeGitHub.create_pull
    calls = []
    async def interrupted(self, *args, **kwargs):
        result = await original(self, *args, **kwargs)
        if not calls:
            calls.append(True)
            raise asyncio.CancelledError()
        return result
    monkeypatch.setattr(FakeGitHub, 'create_pull', interrupted)
    with pytest.raises(asyncio.CancelledError):
        run(task_id)
    run(task_id)
    assert len(FakeGitHub.pulls) == 1
    assert ScriptedModel.calls.count('verifier') == 2  # two turns, only one fresh review
    with session_scope() as s:
        row = s.get(Task, task_id)
        assert row.pr_number == 1 and not row.merge_sha and row.attempt == 1


@pytest.mark.parametrize('replacement', [
    'def test_flaky():\n    return\n',
    'def test_flaky():\n    assert True\n',
    'import pytest\n@pytest.mark.skip\ndef test_flaky():\n    assert False\n',
    '__test__ = False\ndef test_flaky():\n    assert False\n',
    'def test_flaky():\n    pass\n',
])
def test_existing_test_edits_need_exact_human_approval(factory, replacement):
    land(factory, 'tests/test_flaky.py', 'def test_flaky():\n    assert False\n')
    task_id = task(factory, None, 'fix')
    async def scenario():
        runner = _runner(task_id)
        await runner._prepare_builder('token', False)
        try:
            runner.builder.write_text('tests/test_flaky.py', replacement)
            patch = await extract_patch(runner.builder, runner.task.base_sha)
            with pytest.raises(delivery.Stop):
                await runner._steward_test_gate(patch)
            with session_scope() as s:
                decision = s.query(Decision).filter_by(task_id=task_id, status='open').one()
                decision.status, decision.resolution = 'resolved', 'answer'
                decision.resolved_by, decision.resolved_at = factory['user'], utcnow()
            await runner._steward_test_gate(patch)
            # A changed candidate cannot reuse approval.
            runner.builder.write_text('tests/test_flaky.py', replacement + '\n# changed candidate\n')
            changed = await extract_patch(runner.builder, runner.task.base_sha)
            with pytest.raises(delivery.Stop):
                await runner._steward_test_gate(changed)
        finally:
            await runner.builder.close()
    asyncio.run(scenario())
    assert not FakeGitHub.pulls


@pytest.mark.parametrize('path,checks', [
    ('backend/scripts/check.sh', ['cd backend && bash scripts/check.sh']),
    ('jest.config.js', ['npm test']), ('vitest.config.ts', ['npm test']),
    ('playwright.config.ts', ['npm test']), ('pytest.ini', ['pytest']),
])
def test_check_configuration_and_cd_scripts_need_approval(factory, path, checks):
    land(factory, path, 'original\n')
    task_id = task(factory, None, 'fix')
    with session_scope() as s:
        repo = s.get(Repository, factory['repo'])
        repo.config = {**repo.config, 'checks': checks}
    async def scenario():
        runner = _runner(task_id)
        await runner._prepare_builder('token', False)
        try:
            runner.builder.write_text(path, 'weakened\n')
            patch = await extract_patch(runner.builder, runner.task.base_sha)
            with pytest.raises(delivery.Stop):
                await runner._steward_test_gate(patch)
        finally:
            await runner.builder.close()
    asyncio.run(scenario())


def test_only_new_tests_and_production_code_proceed_without_test_approval(factory):
    before = 'def test_original():\n    assert True\n'
    land(factory, 'tests/test_good.py', before)
    task_id = task(factory, None, 'fix')
    async def scenario():
        runner = _runner(task_id)
        await runner._prepare_builder('token', False)
        try:
            runner.builder.write_text('tests/test_good.py', before + '\ndef test_added():\n    assert 1 == 1\n')
            runner.builder.write_text('greet.sh', "echo 'Hello, Factory!'\n")
            await runner._steward_test_gate(await extract_patch(runner.builder, runner.task.base_sha))
        finally:
            await runner.builder.close()
    asyncio.run(scenario())
    with session_scope() as s:
        assert s.query(Decision).filter_by(task_id=task_id).count() == 0


@pytest.mark.parametrize('action', ['fix', 'revert'])
def test_github_queue_forces_steward_revalidation_even_when_disabled(factory, monkeypatch, action):
    task_id, _ = prepared_revert(factory)
    run(task_id)
    with session_scope() as s:
        row = s.get(Task, task_id)
        ev = row.evidence
        identity = {**ev['steward_candidate']['identity'], 'action': action}
        row.status, row.stage = 'running', 'merge'
        row.evidence = {**ev, 'steward': {**ev['steward'], 'action': action},
                        'steward_candidate': {**ev['steward_candidate'], 'identity': identity}}
        repo = s.get(Repository, factory['repo'])
        repo.config = {**repo.config, 'merge': {'revalidate': False, 'reverify': 'never'}}
    land(factory, 'other.txt', 'new main\n')
    calls = []
    async def take(*args):
        pass
    async def queue(*args):
        return {'entry': None}
    async def refresh(self, token, gh, main):
        calls.append('refresh')
    async def enqueue(*args):
        pytest.fail('stale candidate was enqueued')
    monkeypatch.setattr(merge_manager, 'take_turn', take)
    monkeypatch.setattr(merge_manager, 'github_queue_state', queue)
    monkeypatch.setattr(merge_manager, 'merge_via_github_queue', enqueue)
    monkeypatch.setattr(delivery.TaskRunner, '_refresh_onto_main', refresh)
    run(task_id)
    assert calls == ['refresh']


def test_fix_disabling_test_blocks_until_person_approves_and_then_resumes(factory):
    land(factory, 'tests/test_flaky.py', 'def test_flaky():\n    assert False\n')
    task_id = task(factory, None, 'fix')
    def builder(messages):
        if step(messages) == 0:
            return {'content': '', 'tool_calls': [call('write_file', {
                'path': 'tests/test_flaky.py', 'content': 'def test_flaky():\n    return\n'})]}
        return {'content': '', 'tool_calls': [call('finish', {'status': 'done', 'summary': 'Skip flaky test'})]}
    ScriptedModel.scripts['builder'] = builder
    ScriptedModel.scripts['verifier'] = passing_verifier
    run(task_id)
    with session_scope() as s:
        row = s.get(Task, task_id)
        assert row.status == 'needs_input' and not row.pr_number
        assert delivery.STEWARD_TEST_CRITERION in row.acceptance
        decision = s.query(Decision).filter_by(task_id=task_id, status='open').one()
        assert decision.metadata_json['steward_test_approval']['episode_id'] == 'episode-1'
        assert 'tests/test_flaky.py' in decision.body
        # An agent-attributed answer, even under a user id, is not human consent.
        decision.status, decision.resolution = 'resolved', 'answer'
        decision.resolved_by, decision.resolved_at = factory['user'], utcnow()
        decision.attribution = {'source': 'lead'}
        row.status = 'queued'
    run(task_id)
    assert not FakeGitHub.pulls
    with session_scope() as s:
        row = s.get(Task, task_id)
        decision = s.query(Decision).filter_by(task_id=task_id, status='open').one()
        decision_id = decision.id
    # Exercise the ordinary decision service, including its durable requeue.
    from aifactory.decisions import resolve
    from aifactory.models import User
    with session_scope() as s:
        resolve(s, s.get(Decision, decision_id), s.get(User, factory['user']), 'answer', 'Reviewed this exact diff')
    run(task_id)
    with session_scope() as s:
        row = s.get(Task, task_id)
        assert row.status == 'waiting_ci', row.blocked_reason
        assert row.evidence['steward_test_gate']['approved']
        assert row.pr_number == 1


@pytest.mark.parametrize('action', ['fix', 'revert'])
def test_changed_authorization_after_publication_cannot_merge(factory, action):
    task_id, _ = prepared_revert(factory)
    with session_scope() as s:
        row = s.get(Task, task_id)
        row.evidence = {'steward': {**row.evidence['steward'], 'action': action}}
    run(task_id)
    asyncio.run(_runner(task_id).poll_ci())
    with session_scope() as s:
        row = s.get(Task, task_id)
        row.evidence = {**row.evidence, 'steward': {**row.evidence['steward'], 'culprit_sha': 'a' * 40}}
    run(task_id)
    with session_scope() as s:
        row = s.get(Task, task_id)
        assert row.status == 'needs_input' and not row.merge_sha
        assert 'current episode/action/culprit' in row.blocked_reason
    assert not FakeGitHub.merged


def test_new_protected_path_policy_at_merge_cannot_be_bypassed(factory):
    task_id, _ = prepared_revert(factory)
    run(task_id)
    asyncio.run(_runner(task_id).poll_ci())
    with session_scope() as s:
        repo = s.get(Repository, factory['repo'])
        repo.config = {**repo.config, 'protected_paths': ['greet.sh']}
    run(task_id)
    assert not FakeGitHub.merged
    with session_scope() as s:
        assert 'protected paths' in s.get(Task, task_id).blocked_reason


def test_confirmed_first_parent_allows_exact_merge_inverse(factory):
    land(factory, 'greet.sh', "echo 'Hello, Factory!'\n")
    seed = factory['remotes'] / 'seed/acme/demo'
    first_parent = git('rev-parse', 'HEAD', cwd=seed)
    git('checkout', '-qb', 'side', cwd=seed)
    (seed / 'greet.sh').write_text("echo 'broken'\n")
    git('commit', '-qam', 'break greeting', cwd=seed)
    git('checkout', '-q', 'main', cwd=seed)
    git('merge', '--no-ff', '-m', 'merge breaking side', 'side', cwd=seed)
    git('push', '-q', str(factory['remotes'] / 'acme/demo.git'), 'main', cwd=seed)
    sha = git('rev-parse', 'HEAD', cwd=seed)
    task_id = task(factory, sha)
    with session_scope() as s:
        row = s.get(Task, task_id)
        row.evidence = {'steward': {**row.evidence['steward'], 'first_parent_confirmed': True,
                                    'first_parent_sha': first_parent}}
    ScriptedModel.scripts['verifier'] = passing_verifier
    run(task_id)
    with session_scope() as s:
        row = s.get(Task, task_id)
        assert row.status == 'waiting_ci', row.blocked_reason
        assert row.evidence['attempts'][-1]['checks'][0]['ok']
    assert len(FakeGitHub.pulls) == 1


@pytest.mark.parametrize('command', ['cd "$DIR" && bash check.sh', 'pushd backend && bash check.sh',
                                     'cd backend; bash scripts/check.sh', '(cd backend && bash check.sh)'])
def test_unresolved_shell_working_directory_is_conservatively_gated(command):
    assert delivery._check_paths([command])[1]


def test_revert_base_movement_requires_decision_then_rebuilds_same_pr(factory):
    task_id, _ = prepared_revert(factory)
    run(task_id)
    asyncio.run(_runner(task_id).poll_ci())
    main = land(factory, 'other.txt', 'new main\n')
    run(task_id)
    with session_scope() as s:
        row = s.get(Task, task_id)
        assert row.status == 'needs_input' and row.stage == 'prepare'
        assert 'latest default branch' in row.blocked_reason
        decision = s.query(Decision).filter_by(task_id=task_id, status='open').one()
        decision_id = decision.id
    from aifactory.decisions import resolve
    from aifactory.models import User
    with session_scope() as s:
        resolve(s, s.get(Decision, decision_id), s.get(User, factory['user']), 'answer', 'Rebuild on latest main')
        assert s.get(Task, task_id).status == 'queued'
    run(task_id)
    with session_scope() as s:
        row = s.get(Task, task_id)
        assert row.base_sha == main and row.status == 'waiting_ci'
        assert row.pr_number == 1 and not row.merge_sha
        assert s.query(Run).filter_by(task_id=task_id, role='verifier').count() == 2
    assert len(FakeGitHub.pulls) == 1 and not FakeGitHub.merged


def test_setup_cannot_contaminate_deterministic_inverse(factory):
    task_id, _ = prepared_revert(factory)
    with session_scope() as s:
        repo = s.get(Repository, factory['repo'])
        repo.config = {**repo.config, 'setup': ["echo changed > unrelated.txt"]}
    run(task_id)
    with session_scope() as s:
        row = s.get(Task, task_id)
        assert row.status == 'needs_input' and 'setup changed tracked files' in row.blocked_reason
    assert not FakeGitHub.pulls


def test_fix_revalidation_always_uses_test_gate_and_fresh_verifier(factory, monkeypatch):
    task_id, _ = prepared_revert(factory)
    with session_scope() as s:
        row = s.get(Task, task_id)
        row.evidence = {'steward': {**row.evidence['steward'], 'action': 'fix'}}
    run(task_id)
    asyncio.run(_runner(task_id).poll_ci())
    main = land(factory, 'other.txt', 'new main\n')
    with session_scope() as s:
        repo = s.get(Repository, factory['repo'])
        repo.config = {**repo.config, 'merge': {'revalidate': False, 'reverify': 'never'}}
    calls = []
    original = delivery.TaskRunner._steward_test_gate
    async def gate(self, patch):
        calls.append('gate')
        await original(self, patch)
    monkeypatch.setattr(delivery.TaskRunner, '_steward_test_gate', gate)
    run(task_id)
    with session_scope() as s:
        row = s.get(Task, task_id)
        assert row.status == 'waiting_ci' and row.base_sha == main, row.blocked_reason
        assert row.evidence['revalidation']['run'] is True
        assert row.evidence['revalidation']['phase'] == 'complete'
        assert row.evidence['steward_candidate']['base'] == main
        assert s.query(Run).filter_by(task_id=task_id, role='verifier').count() == 2
    assert calls == ['gate', 'gate']  # merge boundary and rebased candidate before checks


@pytest.mark.parametrize('definition', [
    '@globals().__setitem__("__test__", False)\ndef test_added():\n    assert True\n',
    'def test_added(value=globals().__setitem__("__test__", False)):\n    assert True\n',
    'def test_added(value: globals().__setitem__("__test__", False)):\n    assert True\n',
    'def test_added() -> globals().__setitem__("__test__", False):\n    assert True\n',
])
@pytest.mark.parametrize('existing', [False, True])
def test_added_test_import_time_expressions_require_approval(factory, definition, existing):
    before = 'def test_original():\n    assert True\n' if existing else ''
    if existing:
        land(factory, 'tests/test_good.py', before)
    task_id = task(factory, None, 'fix')

    async def scenario():
        runner = _runner(task_id)
        await runner._prepare_builder('token', False)
        try:
            runner.builder.write_text('tests/test_good.py', before + '\n' + definition)
            patch = await extract_patch(runner.builder, runner.task.base_sha)
            with pytest.raises(delivery.Stop):
                await runner._steward_test_gate(patch)
            with session_scope() as s:
                decision = s.query(Decision).filter_by(task_id=task_id, status='open').one()
                assert decision.metadata_json['steward_test_approval']['paths'] == ['tests/test_good.py']
        finally:
            await runner.builder.close()

    asyncio.run(scenario())
    assert not FakeGitHub.pulls


@pytest.mark.parametrize('stage', ['publish', 'merge'])
@pytest.mark.parametrize('configuration', ['checks', 'setup'])
def test_changed_check_configuration_invalidates_candidate_and_rebuilds(factory, monkeypatch, stage, configuration):
    task_id, _ = prepared_revert(factory)
    original = delivery.TaskRunner._publish
    if stage == 'publish':
        async def interrupt(self, token):
            self.stage('publish')
            raise asyncio.CancelledError()
        monkeypatch.setattr(delivery.TaskRunner, '_publish', interrupt)
        with pytest.raises(asyncio.CancelledError):
            run(task_id)
        monkeypatch.setattr(delivery.TaskRunner, '_publish', original)
    else:
        run(task_id)
        asyncio.run(_runner(task_id).poll_ci())
    with session_scope() as s:
        repo = s.get(Repository, factory['repo'])
        commands = ['sh test.sh', 'sh -c true'] if configuration == 'checks' else ['sh -c true']
        repo.config = {**repo.config, configuration: commands}
    run(task_id)
    with session_scope() as s:
        row = s.get(Task, task_id)
        assert row.stage == 'prepare' and row.status == 'needs_input'
        assert row.evidence['steward_candidate'] is None
        assert row.evidence['steward_test_gate'] is None
        assert row.evidence['revalidation'] is None
        assert 'current configuration' in row.blocked_reason
        decision = s.query(Decision).filter_by(task_id=task_id, status='open').one()
        decision_id = decision.id
    assert not FakeGitHub.merged
    from aifactory.decisions import resolve
    from aifactory.models import User
    with session_scope() as s:
        resolve(s, s.get(Decision, decision_id), s.get(User, factory['user']), 'answer', 'Rebuild with current checks')
        assert s.get(Task, task_id).status == 'queued'
    run(task_id)
    with session_scope() as s:
        row = s.get(Task, task_id)
        assert row.status == 'waiting_ci', row.blocked_reason
        gate = row.evidence['steward_test_gate']
        assert gate[configuration] == commands
        assert s.query(Run).filter_by(task_id=task_id, role='verifier').count() == 2
    assert len(FakeGitHub.pulls) == 1 and not FakeGitHub.merged


def steward_question_sites():
    """Exercise each steward question's actual option declaration, including approval.

    Enumerating call sites prevents a new error path from silently acquiring the
    question/unblock retry mismatch, without mocking the decision service.
    """
    tree = ast.parse(inspect.getsource(delivery.TaskRunner))
    sites = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call) or not isinstance(node.func, ast.Attribute):
            continue
        if node.func.attr == 'block' and node.args and isinstance(node.args[0], ast.Constant):
            if node.args[0].value == 'question':
                # Only the helper and the builder's already-explicit answer may
                # create task questions directly.
                assert any(kw.arg == 'options' for kw in node.keywords)
        if node.func.attr != '_steward_question':
            continue
        title = ast.literal_eval(node.args[0])
        options = next((ast.literal_eval(kw.value) for kw in node.keywords if kw.arg == 'options'), None)
        sites.append(pytest.param(title, options, id=f'{node.lineno}-{title}'))
    assert sites
    return sites


@pytest.mark.parametrize('title,options', steward_question_sites())
def test_every_steward_question_answer_requeues_task(factory, title, options):
    from aifactory.decisions import resolve
    from aifactory.models import User

    task_id = task(factory, 'a' * 40)
    runner = _runner(task_id)
    metadata = {'steward_test_approval': {'episode_id': 'episode-1', 'patch_sha256': 'candidate'}}
    with pytest.raises(delivery.Stop):
        runner._steward_question(title, 'Review this repair before continuing.', options=options, metadata=metadata)
    with session_scope() as s:
        row = s.get(Task, task_id)
        assert row.status == 'needs_input'
        decision = s.query(Decision).filter_by(task_id=task_id, status='open').one()
        assert {option['value'] for option in decision.options} == {'answer', 'abandon'}
        assert decision.metadata_json == metadata
        resolve(s, decision, s.get(User, factory['user']), 'answer', 'Reviewed; continue through all gates')
        assert row.status == 'queued' and not row.blocked_reason
        assert decision.resolved_by == factory['user'] and decision.resolved_at
    with session_scope() as s:
        assert s.get(Task, task_id).status == 'queued'
        assert s.query(Decision).filter_by(task_id=task_id, status='open').count() == 0
    assert not FakeGitHub.pulls and not FakeGitHub.merged


@pytest.mark.parametrize('action', ['fix', 'revert'])
@pytest.mark.parametrize('phase', ['checks', 'verify'])
def test_interrupted_steward_candidate_resumes_same_attempt(factory, monkeypatch, action, phase):
    from .test_pipeline import _stop_at_the_checks, _stop_once

    task_id, _ = prepared_revert(factory)
    with session_scope() as s:
        row = s.get(Task, task_id)
        row.evidence = {'steward': {**row.evidence['steward'], 'action': action}}
    if phase == 'checks':
        _stop_at_the_checks(monkeypatch)
    else:
        _stop_once(monkeypatch, '_verifier_sandbox')
    with pytest.raises(asyncio.CancelledError):
        run(task_id)
    run(task_id)
    with session_scope() as s:
        row = s.get(Task, task_id)
        assert row.status == 'waiting_ci', row.blocked_reason
        assert row.attempt == 1 and row.pr_number == 1 and not row.merge_sha
        assert row.evidence['attempts'][-1]['result'] == 'verified'
        assert s.query(Run).filter_by(task_id=task_id, role='builder').count() == (action == 'fix')
        assert s.query(Run).filter_by(task_id=task_id, role='verifier').count() == 1
    assert len(FakeGitHub.pulls) == 1


@pytest.mark.parametrize('action', ['fix', 'revert'])
@pytest.mark.parametrize('change', ['action', 'culprit_sha', 'episode_id', 'checks', 'setup'])
def test_interrupted_candidate_cannot_resume_changed_authorization_or_checks(factory, monkeypatch, action, change):
    from .test_pipeline import _stop_once
    from aifactory.decisions import resolve
    from aifactory.models import User

    task_id, _ = prepared_revert(factory)
    with session_scope() as s:
        row = s.get(Task, task_id)
        row.evidence = {'steward': {**row.evidence['steward'], 'action': action}}
    _stop_once(monkeypatch, '_verifier_sandbox')
    with pytest.raises(asyncio.CancelledError):
        run(task_id)
    with session_scope() as s:
        row = s.get(Task, task_id)
        assert row.evidence['candidate']['checks'][0]['ok']
        if change in ('checks', 'setup'):
            repo = s.get(Repository, factory['repo'])
            commands = [*repo.config[change], 'sh -c true']
            repo.config = {**repo.config, change: commands}
        else:
            value = {'action': 'fix' if action == 'revert' else 'revert',
                     'culprit_sha': 'a' * 40, 'episode_id': 'episode-2'}[change]
            row.evidence = {**row.evidence, 'steward': {**row.evidence['steward'], change: value}}
    run(task_id)
    with session_scope() as s:
        row = s.get(Task, task_id)
        assert row.status == 'needs_input' and row.stage == 'prepare'
        assert row.evidence['candidate'] is None and row.evidence['steward_candidate'] is None
        assert not row.pr_number and not row.merge_sha
        assert s.query(Run).filter_by(task_id=task_id, role='verifier').count() == 0
        decision = s.query(Decision).filter_by(task_id=task_id, status='open').one()
        resolve(s, decision, s.get(User, factory['user']), 'answer', 'Rebuild under current authorization')
        assert row.status == 'queued'
    if change in ('checks', 'setup', 'episode_id'):
        run(task_id)
        with session_scope() as s:
            row = s.get(Task, task_id)
            assert row.status == 'waiting_ci', row.blocked_reason
            assert s.query(Run).filter_by(task_id=task_id, role='verifier').count() == 1
        assert len(FakeGitHub.pulls) == 1
    else:
        assert not FakeGitHub.pulls


def test_interrupted_revert_rebuilds_on_new_main_before_checks(factory, monkeypatch):
    from .test_pipeline import _stop_at_the_checks
    from aifactory.decisions import resolve
    from aifactory.models import User

    task_id, _ = prepared_revert(factory)
    _stop_at_the_checks(monkeypatch)
    with pytest.raises(asyncio.CancelledError):
        run(task_id)
    main = land(factory, 'unrelated.txt', 'new main\n')
    run(task_id)
    with session_scope() as s:
        row = s.get(Task, task_id)
        assert row.stage == 'prepare' and row.status == 'needs_input'
        assert row.evidence['candidate'] is None
        decision = s.query(Decision).filter_by(task_id=task_id, status='open').one()
        resolve(s, decision, s.get(User, factory['user']), 'answer', 'Regenerate on current main')
    run(task_id)
    with session_scope() as s:
        row = s.get(Task, task_id)
        assert row.status == 'waiting_ci' and row.base_sha == main, row.blocked_reason
    assert len(FakeGitHub.pulls) == 1
