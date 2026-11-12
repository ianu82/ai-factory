"""Scoped autonomy metric: deterministic local evidence, no providers or GitHub."""
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import event

from aifactory import db
from aifactory.models import Event, Organization, Outcome, Plan, Repository, Signal, Task, Workspace
from aifactory.regret import link_critical_regression, regret_free_merge_rate
from aifactory.regrets import record_regret

NOW = datetime(2026, 11, 11, 20, 0)
START = NOW - timedelta(days=30)


@pytest.fixture
def scope():
    with db.session_scope() as s:
        org = Organization(name="test")
        s.add(org)
        s.flush()
        ws = Workspace(org_id=org.id, name="test", slug="test")
        s.add(ws)
        s.flush()
        repo = Repository(workspace_id=ws.id, full_name="acme/test")
        s.add(repo)
        s.flush()
        outcome = Outcome(workspace_id=ws.id, number=1, title="Ship", repo_ids=[repo.id])
        s.add(outcome)
        s.flush()
        plan = Plan(outcome_id=outcome.id)
        s.add(plan)
        s.flush()
        yield s, ws, repo, outcome, plan


def task(scope, *, at=NOW, status="completed", merged=True, repo=None, evidence=None):
    s, ws, default_repo, outcome, plan = scope
    number = s.query(Task).count() + 1
    row = Task(workspace_id=ws.id, repo_id=(repo or default_repo).id, outcome_id=outcome.id,
               plan_id=plan.id, key=f"T{number}", title="ship", status=status,
               merge_sha=f"merge-{number}" if merged else "", pr_number=number,
               pr_url=f"https://github.com/acme/test/pull/{number}", finished_at=NOW,
               evidence=evidence if evidence is not None else (
                   {"merge": {"at": at.isoformat() if at else None}} if merged else {}))
    s.add(row)
    s.flush()
    return row


def signal(scope, row, **kwargs):
    s, ws, repo, _, _ = scope
    sig = Signal(workspace_id=ws.id, repo_id=repo.id, source="smoke_failure", title="failure",
                 fingerprint=f"s-{s.query(Signal).count()}", url=row.pr_url, **kwargs)
    s.add(sig)
    s.flush()
    return sig


def metric(scope, **kwargs):
    s, ws, *_ = scope
    return regret_free_merge_rate(s, ws.id, now=NOW, **kwargs)


def test_exact_closed_window_and_pending_unmerged_unknown(scope):
    inside = [task(scope, at=START), task(scope, at=NOW)]
    task(scope, at=START - timedelta(microseconds=1))
    task(scope, at=NOW + timedelta(microseconds=1))
    pending = task(scope, status="running")
    task(scope, merged=False)  # even 'completed' is not merge proof
    unknown = task(scope, at=None)
    result = metric(scope)
    assert result["window_start"] == "2026-10-12T20:00:00Z"
    assert result["window_end"] == "2026-11-11T20:00:00Z"
    assert result["settled_count"] == result["regret_free_count"] == 2
    assert result["regretted_count"] == 0 and result["rate"] == 1
    assert result["pending_count"] == result["unknown_count"] == 1
    assert {s["task_id"] for s in result["samples"]} == {r.id for r in [*inside, pending]}
    assert result["unknown_samples"][0]["task_id"] == unknown.id
    for sample in result["samples"]:
        row = scope[0].get(Task, sample["task_id"])
        assert sample["pr_url"] == row.pr_url and sample["pr_number"] == row.pr_number
        assert sample["merge_reference"] == {"task_id": row.id, "path": "evidence.merge"}
    assert regret_free_merge_rate(scope[0], scope[1].id, now=NOW.replace(tzinfo=UTC)) == result


def test_empty_and_only_pending_are_unknown(scope):
    assert metric(scope)["rate"] is None
    task(scope, status="queued")
    result = metric(scope)
    assert result["rate"] is None and result["settled_count"] == 0


def test_scope_isolation(scope):
    s, ws, repo, outcome, plan = scope
    other_repo = Repository(workspace_id=ws.id, full_name="acme/other")
    other_ws = Workspace(org_id=ws.org_id, name="other", slug="other")
    s.add_all([other_repo, other_ws])
    s.flush()
    foreign_repo = Repository(workspace_id=other_ws.id, full_name="foreign/repo")
    s.add(foreign_repo)
    s.flush()
    own = task(scope)
    second = task(scope, repo=other_repo)
    foreign = task(scope, repo=foreign_repo)
    foreign.workspace_id = other_ws.id
    signal(scope, own, data={"task_id": second.id})  # wrong repository for target
    sig = signal(scope, own)
    sig.workspace_id = other_ws.id  # cross-workspace URL must not taint own task
    assert metric(scope)["settled_count"] == 2
    result = metric(scope, repo_id=repo.id)
    assert result["settled_count"] == 1 and result["regretted_count"] == 0
    assert {r["task_id"] for r in result["samples"]} == {own.id}
    with pytest.raises(ValueError, match="Repository"):
        metric(scope, repo_id=foreign_repo.id)


def test_smoke_signal_survives_acknowledgment_and_revert_counted_once(scope):
    s, _, _, outcome, _ = scope
    row = task(scope, status="smoke_failed")
    sig = signal(scope, row)
    assert metric(scope)["regretted_count"] == 1
    sig.status, sig.outcome_id = "resolved", outcome.id
    row.status = "completed"
    row.evidence = {**row.evidence, "smoke": {"state": "passed"},
                    "revert": {"sha": "revert-sha", "pr": 99}}
    result = metric(scope)
    assert (result["settled_count"], result["regretted_count"], result["regret_free_count"], result["rate"]) == (1, 1, 0, 0)
    refs = result["samples"][0]["evidence"]
    assert {"signal_id": sig.id, "kind": "smoke_failure", "url": row.pr_url} in refs
    row.status = "abandoned"
    assert metric(scope)["rate"] == 0


@pytest.mark.parametrize("source", ["smoke", "history", "ledger", "event", "revert"])
def test_durable_sources_settle_even_nonterminal_merge(scope, source):
    s, ws, repo, _, _ = scope
    row = task(scope, status="running")
    if source == "smoke":
        row.evidence = {**row.evidence, "smoke": {"state": "failed", "ref": row.merge_sha}}
    elif source == "history":
        row.evidence = {**row.evidence, "smoke": {"state": "passed"}, "failure_facts": [
            {"kind": "ci", "state": "failed"},
            {"kind": "smoke", "state": "failed", "ref": row.merge_sha}]}
    elif source == "ledger":
        regret, _ = record_regret(s, ws.id, row.id, kind="smoke_failure", source_key="s1", reporter="factory")
        regret.fix_url = "https://fixed"
        s.get(Signal, regret.signal_id).status = "ignored"
    elif source == "event":
        s.add(Event(workspace_id=ws.id, task_id=row.id, kind="reverted"))
    else:
        row.evidence = {**row.evidence, "revert": {"sha": "reverted"}}
    result = metric(scope)
    assert result["settled_count"] == result["regretted_count"] == 1
    assert result["samples"][0]["evidence"]


def test_premerge_smoke_ci_and_revert_candidate_are_not_regrets(scope):
    row = task(scope)
    row.evidence = {**row.evidence, "smoke": {"state": "failed", "ref": "old-head"},
                    "failure_facts": [{"kind": "ci", "state": "failed"}],
                    "revert_candidate": {"sha": "candidate", "pr": 99}}
    assert metric(scope)["rate"] == 1


def test_external_merge_uses_recorded_time_never_finished_or_now(scope):
    old = task(scope, at=START - timedelta(days=1))
    old.evidence = {**old.evidence, "merge": {"outside": True, "sha": old.merge_sha,
                                            "at": (START - timedelta(days=1)).isoformat()}}
    row = task(scope, at=START)
    row.evidence = {**row.evidence, "merge": {"outside": True, "sha": row.merge_sha, "at": START.isoformat()}}
    for value in (None, "", "not-a-date"):
        task(scope, evidence={"merge": {"outside": True, "at": value}})
    result = metric(scope)
    assert result["settled_count"] == 1 and result["unknown_count"] == 3
    assert result["samples"][0]["task_id"] == row.id


def test_legacy_signals_loaded_once_not_per_task(scope):
    s, ws, *_ = scope
    rows = [task(scope) for _ in range(25)]
    for row in rows:
        signal(scope, row)
    for _ in range(25):
        row = task(scope)
        sig = signal(scope, row)
        sig.source = "feedback"
        link_critical_regression(s, ws.id, sig.id, row.id)
    s.flush()
    queries = []

    def observe(conn, cursor, statement, parameters, context, executemany):
        if "FROM signals" in statement:
            queries.append(statement)

    event.listen(s.bind, "before_cursor_execute", observe)
    try:
        result = metric(scope)
    finally:
        event.remove(s.bind, "before_cursor_execute", observe)
    assert result["regretted_count"] == 50 and len(queries) == 1


def test_critical_link_explicit_validated_idempotent_and_durable(scope):
    s, ws, repo, _, _ = scope
    row = task(scope)
    sig = signal(scope, row, data={"keep": "value", "task_id": row.id})
    link = link_critical_regression(s, ws.id, sig.id, row.id, repo_id=repo.id, actor="admin")
    assert link["severity"] == "critical" and link["merge_sha"] == row.merge_sha
    assert sig.data == {"keep": "value", "task_id": row.id, "critical_regression": link}
    assert link_critical_regression(s, ws.id, sig.id, row.id) == link
    audit = s.query(Event).filter_by(kind="critical_regression_linked").one()
    assert audit.data["signal_id"] == sig.id and audit.data["repo_id"] == repo.id
    assert audit.task_id == row.id and audit.actor == "admin"
    sig.status = "resolved"
    assert s.get(Event, audit.id).data["severity"] == "critical"


@pytest.mark.parametrize("evidence_source", ["both", "signal", "event"])
@pytest.mark.parametrize("status", ["completed", "running"])
def test_non_smoke_critical_regression_is_durable_regret(scope, evidence_source, status):
    s, ws, repo, outcome, _ = scope
    row = task(scope, status=status)
    healthy = task(scope)
    sig = signal(scope, row)
    sig.source, sig.url = "feedback", "https://example.test/incidents/1"
    link_critical_regression(s, ws.id, sig.id, row.id, actor="admin")
    audit = s.query(Event).filter_by(kind="critical_regression_linked").one()
    audit_id = audit.id
    if evidence_source == "signal":
        s.delete(audit)
    elif evidence_source == "event":
        sig.data = {}  # the append-only event survives edits to the feedback
    sig.status, sig.outcome_id = "resolved", outcome.id
    row.evidence = {**row.evidence, "smoke": {"state": "passed"}}
    result = metric(scope, repo_id=repo.id)
    assert (result["settled_count"], result["regretted_count"],
            result["regret_free_count"], result["rate"]) == (2, 1, 1, 0.5)
    sample = next(sample for sample in result["samples"] if sample["task_id"] == row.id)
    assert sample["settled"] and sample["regretted"]
    refs = sample["evidence"]
    if evidence_source != "event":
        assert {"signal_id": sig.id, "kind": "critical_regression",
                "path": "data.critical_regression", "url": sig.url} in refs
    if evidence_source != "signal":
        assert {"event_id": audit_id, "kind": "critical_regression_linked"} in refs
    assert next(sample for sample in result["samples"] if sample["task_id"] == healthy.id)["regretted"] is False
    # Completion, acknowledgment and a successful revert cannot erase or double-count it.
    row.status = "abandoned"
    row.evidence = {**row.evidence, "revert": {"sha": "revert-sha", "pr": 99}}
    assert metric(scope)["regretted_count"] == 1


@pytest.mark.parametrize("field,value", [
    ("workspace_id", "foreign-workspace"), ("repo_id", "foreign-repo"),
    ("task_id", "unrelated-task"), ("merge_sha", "another-merge"),
    ("severity", "high"),
])
def test_critical_evidence_must_match_exact_merged_scope(scope, field, value):
    s, ws, repo, _, _ = scope
    row = task(scope)
    sig = signal(scope, row)
    sig.source = "feedback"
    link = link_critical_regression(s, ws.id, sig.id, row.id)
    audit = s.query(Event).filter_by(kind="critical_regression_linked").one()
    sig.data = {"critical_regression": {**link, field: value}}
    audit.data = {**audit.data, field: value}
    assert metric(scope)["rate"] == 1


@pytest.mark.parametrize("field", ["workspace_id", "repo_id"])
def test_critical_signal_and_event_scope_isolation(scope, field):
    s, ws, repo, _, _ = scope
    row = task(scope)
    sig = signal(scope, row)
    sig.source = "feedback"
    link_critical_regression(s, ws.id, sig.id, row.id)
    audit = s.query(Event).filter_by(kind="critical_regression_linked").one()
    setattr(sig, field, "foreign-scope")
    if field == "workspace_id":
        audit.workspace_id = "foreign-scope"
    else:
        audit.data = {**audit.data, "repo_id": "foreign-scope"}
    for repo_id in (None, repo.id):
        assert metric(scope, repo_id=repo_id)["rate"] == 1


def test_critical_text_without_explicit_link_is_not_regret(scope):
    row = task(scope)
    sig = signal(scope, row, data={"task_id": row.id, "severity": "critical"})
    sig.source, sig.title = "feedback", "critical production regression"
    assert metric(scope)["rate"] == 1


@pytest.mark.parametrize("mismatch", ["workspace", "repo", "task", "pr", "legacy_pr", "unmerged", "relink"])
def test_critical_link_rejects_unrelated_references_without_writes(scope, mismatch):
    s, ws, repo, _, _ = scope
    row = task(scope)
    other = task(scope)
    sig = signal(scope, row)
    if mismatch == "workspace":
        sig.workspace_id = "another-workspace"
    elif mismatch == "repo":
        sig.repo_id = "another-repo"
    elif mismatch == "task":
        sig.data = {"task_id": other.id}
    elif mismatch == "pr":
        sig.data = {"pr_url": other.pr_url}
    elif mismatch == "legacy_pr":
        sig.url = other.pr_url
    elif mismatch == "unmerged":
        row.merge_sha, row.evidence = "", {}
    else:
        sig.url = ""
        link_critical_regression(s, ws.id, sig.id, other.id)
    before = s.query(Event).count()
    data_before = dict(sig.data or {})
    with pytest.raises(ValueError):
        link_critical_regression(s, ws.id, sig.id, row.id, repo_id=repo.id)
    assert s.query(Event).count() == before and sig.data == data_before


@pytest.mark.parametrize("days", [0, -1, float("inf"), float("nan"), True])
def test_invalid_window(scope, days):
    with pytest.raises(ValueError):
        metric(scope, window_days=days)


@pytest.mark.parametrize("merged_at", ["2026-10-01T12:00:00Z", None])
def test_human_merge_delivery_records_provider_time_not_reconciliation_time(scope, monkeypatch, merged_at):
    import asyncio
    from types import SimpleNamespace

    from aifactory.pipeline import delivery

    s, ws, repo, _, _ = scope
    row = task(scope, status="queued", merged=False)

    class GitHub:
        def __init__(self, token):
            pass

        async def pull(self, full_name, number):
            return {"merged": True, "merged_at": merged_at, "merge_commit_sha": "human-sha",
                    "merged_by": {"login": "human"}}

    def update(**values):
        for key, value in values.items():
            setattr(row, key, value)

    async def smoke():
        row.status = "completed"

    runner = SimpleNamespace(ws=ws.id, repo=repo, task=row, _guard=lambda: None,
                             _guard_steward_candidate=lambda: None, update=update,
                             evidence=lambda key, value: setattr(row, "evidence", {key: value}),
                             event=lambda *args, **kwargs: None, smoke=smoke)
    monkeypatch.setattr(delivery, "GitHub", GitHub)
    monkeypatch.setattr(delivery, "resolve_token", lambda ws: "fake")
    asyncio.run(delivery.TaskRunner.merge(runner))
    assert row.evidence["merge"]["at"] == merged_at
    result = metric(scope)
    assert result["settled_count"] == 0
    assert result["unknown_count"] == (1 if merged_at is None else 0)
