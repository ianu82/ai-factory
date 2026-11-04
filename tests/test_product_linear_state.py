import json
from dataclasses import replace

import pytest

from auto_mindsdb_factory.automation import FactoryRunStore, RunLeaseBusyError
from auto_mindsdb_factory.linear_trigger import LinearIssueSnapshot
from auto_mindsdb_factory.product_linear_intake import LinearProductRoute, task_from_issue
from auto_mindsdb_factory.product_linear_state import LinearDeliveryState, LinearStateError
from auto_mindsdb_factory.product_profile import ProductProfile
from auto_mindsdb_factory.product_task import ProductTask
from auto_mindsdb_factory.run_journal import JournalError


@pytest.fixture
def inputs(tmp_path):
    issue = LinearIssueSnapshot(id="b894af1a-d838-4bc8-bddf-a2800cdd0cde", identifier="ENG-1",
        title="Feature", description="details", url="https://linear.app/team/issue/ENG-1",
        team={"id": "team"}, state={"id": "queued"}, labels=["factory"], priority=None,
        project=None, creator=None, assignee=None, created_at="then", updated_at="now", comments=[])
    route = LinearProductRoute.from_document({"version": 1, "team_id": "team", "queued_state_id": "queued",
        "building_state_id": "building", "completed_state_id": "done", "blocked_state_id": "blocked",
        "required_label": "factory", "acceptance_criteria": [{"id": "test", "description": "test", "command": ["pytest"]}]})
    profile = ProductProfile.from_document({"version": 1, "name": "sample", "repository": "owner/product",
        "image": "python@sha256:" + "a" * 64, "checks": {"unit": ["pytest"]}, "smoke": [["true"]]})
    state = LinearDeliveryState(tmp_path / "state", tmp_path / "target")
    return state, issue, route, profile, task_from_issue(issue, route)


def test_leases_are_independent_and_exclusive(inputs):
    state, issue, _, _, task = inputs
    locks = FactoryRunStore(state.store)
    with state.lease(issue.id, ttl_seconds=2, renew_interval_seconds=.1) as lease:
        lease.heartbeat()
        with locks.run_lease(task.document["id"]):
            with pytest.raises(RunLeaseBusyError):
                with state.lease(issue.id.upper()):
                    pass
    with pytest.raises(LinearStateError):
        state.lease("../../escape")


def test_replay_restart_and_retry(inputs):
    state, issue, route, profile, task = inputs
    with state.lease(issue.id):
        journal = state.claim(task, issue, route, profile)
        assert journal.root == state.store / "linear-intake" / "delivery" / task.document["id"]
        binding = state.get_binding(journal)
        assert binding["task"] == task.document and binding["task_digest"] == task.digest
        assert binding["route_digest"] == route.digest and binding["profile_digest"] == profile.digest
        assert binding["issue_id"] == issue.id and binding["identifier"] == issue.identifier
        assert binding["url"] == issue.url
        assert state.outcome(journal) is None and [j.root for j in state.pending()] == [journal.root]
        with pytest.raises(LinearStateError):
            state.mark_synced(journal)
    state = LinearDeliveryState(state.store, state.store.parent / "target")
    with state.lease(issue.id):
        journal = state.claim(task, replace(issue, state={"id": "building"}), route, profile)
        assert state.get_binding(journal) == binding
        blocked = {"status": "blocked", "reason": "test failed"}
        state.record_outcome(journal, blocked)
        state.record_outcome(journal, blocked)
        assert len(journal.events()) == 1
        with pytest.raises(LinearStateError):
            state.record_outcome(journal, {"status": "complete", "merge_sha": "a" * 40,
                                           "pull_request_url": "https://github.com/owner/repo/pull/1"})
        assert state.pending() and state.outcome(journal) == blocked
    state = LinearDeliveryState(state.store, state.store.parent / "target")
    with state.lease(issue.id):
        journal = state.pending()[0]
        assert state.outcome(journal) == blocked
        state.mark_synced(journal)
        state.mark_synced(journal)
        assert len(journal.events()) == 2 and state.pending() == []
        assert state.claim(task, issue, route, profile).root == journal.root


def test_conflicting_claims_fail(inputs):
    state, issue, route, profile, task = inputs
    with state.lease(issue.id):
        state.claim(task, issue, route, profile)
        for changed in (replace(issue, identifier="ENG-2"), replace(issue, description="other"),
                        replace(issue, url="https://linear.app/new")):
            with pytest.raises((LinearStateError, ValueError)):
                state.claim(task_from_issue(changed, route), changed, route, profile)
        other = ProductProfile.from_document({**profile.document, "name": "another"})
        with pytest.raises(LinearStateError):
            state.claim(task, issue, route, other)
        doc = route.document
        doc["acceptance_criteria"][0]["command"] = ["false"]
        other_route = LinearProductRoute.from_document(doc)
        with pytest.raises(LinearStateError):
            state.claim(task_from_issue(issue, other_route), issue, other_route, profile)


@pytest.mark.parametrize("result", [
    {}, {"status": "complete"}, {"status": "complete", "merge_sha": "a" * 39, "pull_request_url": "https://github.com/a/b/pull/1"},
    {"status": "complete", "merge_sha": "a" * 40, "pull_request_url": "https://github.com.evil/a/b/pull/1"},
    {"status": "blocked", "extra": 1}, {"status": "blocked", "reason": None},
])
def test_invalid_outcomes(inputs, result):
    state, issue, route, profile, task = inputs
    with state.lease(issue.id):
        journal = state.claim(task, issue, route, profile)
        with pytest.raises(LinearStateError):
            state.record_outcome(journal, result)
        assert state.outcome(journal) is None


def test_complete_is_durable(inputs):
    state, issue, route, profile, task = inputs
    result = {"status": "complete", "merge_sha": "a" * 40, "pull_request_url": "https://github.com/a/b/pull/12"}
    with state.lease(issue.id):
        journal = state.claim(task, issue, route, profile)
        state.record_outcome(journal, result)
    state = LinearDeliveryState(state.store, state.store.parent / "target")
    with state.lease(issue.id):
        journal = state.pending()[0]
        assert state.outcome(journal) == result
        state.mark_synced(journal)
        assert not state.pending()


@pytest.mark.parametrize("damage", ["manifest_missing", "manifest_invalid", "task_digest", "event_missing", "event_invalid", "event_name", "event_bool_sequence", "synced_without_outcome"])
def test_corruption_fails_closed(inputs, damage):
    state, issue, route, profile, task = inputs
    with state.lease(issue.id):
        journal = state.claim(task, issue, route, profile)
        state.record_outcome(journal, {"status": "blocked"})
        state.mark_synced(journal)
        manifest = journal.root / "manifest.json"
        first = journal.root / "event-000001.json"
        if damage == "manifest_missing": manifest.unlink()
        elif damage == "manifest_invalid": manifest.write_text("{")
        elif damage == "task_digest":
            doc = json.loads(manifest.read_text()); doc["binding"]["task"]["title"] = "changed"; manifest.write_text(json.dumps(doc))
        elif damage == "event_missing": first.unlink()
        elif damage == "event_invalid": first.write_text("{}")
        elif damage == "event_bool_sequence":
            doc = json.loads(first.read_text()); doc["sequence"] = True; first.write_text(json.dumps(doc))
        elif damage == "event_name": first.rename(journal.root / "event-bad.json")
        else:
            doc = json.loads(first.read_text()); doc["kind"] = "synced"; doc["data"] = {}; first.write_text(json.dumps(doc))
        with pytest.raises(JournalError): state.pending()
        with pytest.raises(JournalError): state.claim(task, issue, route, profile)


def test_unsafe_paths_and_invalid_task(inputs, tmp_path):
    state, issue, route, profile, task = inputs
    target = tmp_path / "target"
    target.mkdir()
    with pytest.raises(LinearStateError): LinearDeliveryState(target / "state", target)
    (tmp_path / "alias").symlink_to(target, target_is_directory=True)
    with pytest.raises(LinearStateError): LinearDeliveryState(tmp_path / "alias" / "state", target)
    bad = ProductTask(json.dumps({**task.document, "id": "../../escape"}, sort_keys=True))
    with state.lease(issue.id):
        with pytest.raises(LinearStateError): state.claim(bad, issue, route, profile)
        journal = state.claim(task, issue, route, profile)
        (journal.root / "event-000001.json").symlink_to(tmp_path / "outside")
        with pytest.raises(LinearStateError): state.pending()



def test_no_initial_claim_after_issue_leaves_queue(inputs):
    state, issue, route, profile, task = inputs
    with state.lease(issue.id):
        with pytest.raises(LinearStateError):
            state.claim(task, replace(issue, state={"id": "building"}), route, profile)
        assert state.pending() == []


def test_unsafe_directory_and_lease_paths(inputs, tmp_path):
    state, issue, route, profile, task = inputs
    (state.store / "runs").mkdir(parents=True)
    (state.store / "runs" / ("linear-source-" + issue.id)).symlink_to(tmp_path, target_is_directory=True)
    with pytest.raises(LinearStateError): state.lease(issue.id)
    state.base.mkdir(parents=True)
    state.delivery.symlink_to(tmp_path, target_is_directory=True)
    with pytest.raises(LinearStateError): state.pending()
    with pytest.raises(LinearStateError): state.claim(task, issue, route, profile)


@pytest.mark.parametrize("missing", ["event-000001.json", "outcome.json"])
def test_lost_terminal_evidence_cannot_replace_blocked_with_complete(inputs, missing):
    state, issue, route, profile, task = inputs
    with state.lease(issue.id):
        journal = state.claim(task, issue, route, profile)
        state.record_outcome(journal, {"status": "blocked"})
        (journal.root / missing).unlink()
        assert state.outcome(journal) == {"status": "blocked"}
        state.record_outcome(journal, {"status": "blocked"})
        assert (journal.root / missing).exists()
        with pytest.raises(LinearStateError):
            state.record_outcome(journal, {"status": "complete", "merge_sha": "a" * 40,
                                           "pull_request_url": "https://github.com/a/b/pull/1"})


@pytest.mark.parametrize("suffix", ["linear-intake", "linear-intake/delivery", "runs"])
def test_target_must_be_disjoint_from_control_store(tmp_path, suffix):
    with pytest.raises(LinearStateError):
        LinearDeliveryState(tmp_path / "store", tmp_path / "store" / suffix)


@pytest.mark.parametrize("repo", ["a/..", "../b", "a/.", "./b"])
def test_dot_segment_is_not_completion_evidence(inputs, repo):
    state, issue, route, profile, task = inputs
    with state.lease(issue.id):
        journal = state.claim(task, issue, route, profile)
        with pytest.raises(LinearStateError):
            state.record_outcome(journal, {"status": "complete", "merge_sha": "a" * 40,
                                           "pull_request_url": f"https://github.com/{repo}/pull/1"})


def test_interrupted_terminal_append_recovers_same_outcome(inputs, monkeypatch):
    state, issue, route, profile, task = inputs
    with state.lease(issue.id):
        journal = state.claim(task, issue, route, profile)
        def interrupted(*args, **kwargs):
            assert state.outcome(state.pending()[0]) == {"status": "blocked"}
            raise OSError("interrupted append")
        monkeypatch.setattr(journal, "append", interrupted)
        with pytest.raises(OSError):
            state.record_outcome(journal, {"status": "blocked"})
    restarted = LinearDeliveryState(state.store, state.store.parent / "target")
    with restarted.lease(issue.id):
        journal = restarted.pending()[0]
        assert restarted.outcome(journal) == {"status": "blocked"}
        restarted.record_outcome(journal, {"status": "blocked"})
        restarted.mark_synced(journal)
        assert not restarted.pending()


def test_lease_rechecks_paths_at_context_entry(inputs, tmp_path):
    state, issue, _, _, _ = inputs
    context = state.lease(issue.id)
    run = state.store / "runs" / ("linear-source-" + issue.id)
    run.mkdir(parents=True, exist_ok=True)
    outside = tmp_path / "outside-guard"
    (run / ".automation.lock.guard").symlink_to(outside)
    with pytest.raises(LinearStateError):
        with context:
            pass
    assert not outside.exists()


def test_claimed_issue_ids_include_synced_and_validate_all(inputs):
    state, issue, route, profile, task = inputs
    assert state.claimed_issue_ids() == []
    with state.lease(issue.id):
        journal = state.claim(task, issue, route, profile)
        assert state.claimed_issue_ids() == [issue.id]
        state.record_outcome(journal, {'status': 'blocked'})
        state.mark_synced(journal)
    assert state.pending() == [] and state.claimed_issue_ids() == [issue.id]
    (journal.root / 'event-000001.json').write_text('{}')
    with pytest.raises(JournalError):
        state.claimed_issue_ids()


def test_claimed_issue_ids_reject_unsafe_directory(inputs, tmp_path):
    state, issue, route, profile, task = inputs
    state.delivery.mkdir(parents=True)
    (state.delivery / ('linear-' + issue.id)).symlink_to(tmp_path, target_is_directory=True)
    with pytest.raises(LinearStateError):
        state.claimed_issue_ids()
